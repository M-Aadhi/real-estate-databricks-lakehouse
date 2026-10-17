"""Shared Silver transformation: Bronze batch -> validated, deduplicated Silver table.

Steps for one entity and one batch:
 1. read the batch from Bronze (incremental: only rows with this batch_id)
 2. safe-cast to the registry types and standardise text
 3. evaluate rules: required/type rules + business rules + referential integrity
 4. split valid / invalid; invalid rows -> quarantine.<entity> with error_reason
 5. deduplicate valid rows (exact dupes dropped, conflicting keys quarantined)
 6. stop if the quarantine rate exceeds data_quality.max_quarantine_pct
 7. Delta MERGE into silver.<entity> (or SCD2 merge for tenants)
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.config import Config
from src.common.exceptions import DataQualityThresholdError
from src.common.logging_utils import PipelineRun
from src.common.schemas import ENTITIES, Entity
from src.common.validation import (
    Rule,
    apply_rules,
    cast_to_schema,
    foreign_key_rule,
    row_hash,
    schema_rules,
    split_valid_invalid,
)
from src.silver.dedup import deduplicate

LINEAGE_COLS = ["batch_id", "source_file", "ingestion_timestamp"]


@dataclass
class ForeignKey:
    column: str
    parent: str                 # parent entity name
    optional: bool = False      # null allowed (e.g. maintenance.unit_id for common areas)

    @property
    def reason(self) -> str:
        return f"unknown_{self.column}"


@dataclass
class SilverSpec:
    entity: Entity
    standardize: Callable[[DataFrame], DataFrame] = lambda df: df
    rules: Callable[[Config], list[Rule]] = lambda cfg: []
    foreign_keys: list[ForeignKey] = field(default_factory=list)
    derive: Callable[[DataFrame, Config], DataFrame] = lambda df, cfg: df
    # Extra checks needing a join (e.g. unit belongs to property): (lakehouse, df) -> df with _fk_* column
    join_checks: list[Callable] = field(default_factory=list)


def parent_keys(lakehouse, parent: str) -> DataFrame:
    """Current keys of a parent Silver table (tenants is SCD2 -> current versions only)."""
    pk = ENTITIES[parent].primary_key
    if not lakehouse.exists("silver", parent):
        return lakehouse.spark.createDataFrame([], f"{pk} string")
    df = lakehouse.read("silver", parent)
    if "is_current" in df.columns:
        df = df.filter("is_current")
    return df.select(pk)


def validate_batch(lakehouse, spec: SilverSpec, bronze_batch: DataFrame, cfg: Config) -> tuple[DataFrame, DataFrame]:
    """Return (valid_rows_with_row_hash, invalid_rows). Pure apart from reading parent keys."""
    entity = spec.entity
    df = spec.standardize(cast_to_schema(bronze_batch, entity))
    for fk in spec.foreign_keys:
        df = foreign_key_rule(df, fk.column, parent_keys(lakehouse, fk.parent),
                              ENTITIES[fk.parent].primary_key, fk.reason, optional=fk.optional)
    for check in spec.join_checks:
        df = check(lakehouse, df)
    df = apply_rules(df, schema_rules(entity) + spec.rules(cfg))
    valid, invalid = split_valid_invalid(df)
    valid = valid.withColumn("row_hash", row_hash(entity.column_names))
    return valid, invalid


def to_quarantine(df: DataFrame, entity: Entity) -> DataFrame:
    """Quarantine keeps the ORIGINAL source strings, never the cast values."""
    raw_cols = [F.col(f"_raw_{c}").alias(c) for c in entity.column_names]
    return df.select(*raw_cols, "error_reason", "validation_timestamp", *LINEAGE_COLS)


def transform_entity(lakehouse, spec: SilverSpec, batch_id: str, cfg: Config,
                     writer: Callable[[DataFrame], None] | None = None) -> dict:
    entity = spec.entity
    with PipelineRun(lakehouse, f"silver.{entity.name}", batch_id) as run:
        bronze = lakehouse.read("bronze", entity.name).filter(F.col("batch_id") == batch_id)
        run.records_read = bronze.count()
        if run.records_read == 0:
            run.log.info("no rows for %s in %s; nothing to do", entity.name, batch_id)
            return run.as_record()

        valid, invalid = validate_batch(lakehouse, spec, bronze, cfg)
        unique, conflicts, exact_removed = deduplicate(valid, entity.primary_key)
        if exact_removed:
            run.log.info("removed %d exact duplicate row(s)", exact_removed)

        quarantine = to_quarantine(invalid, entity).unionByName(to_quarantine(conflicts, entity))
        lakehouse.write_batch(quarantine, "quarantine", entity.name, batch_id)
        rejected = lakehouse.read("quarantine", entity.name).filter(F.col("batch_id") == batch_id).count()
        run.records_rejected = rejected

        pct = rejected / run.records_read * 100
        if pct > cfg.max_quarantine_pct:
            raise DataQualityThresholdError(
                f"{entity.name}/{batch_id}: {rejected} of {run.records_read} rows quarantined "
                f"({pct:.1f}% > {cfg.max_quarantine_pct}%). Silver not updated; review quarantine.{entity.name}.")

        silver = spec.derive(unique.select(*entity.column_names, "row_hash", *LINEAGE_COLS), cfg)
        silver = silver.withColumn("silver_updated_at", F.current_timestamp())
        if writer:
            writer(silver)
        else:
            lakehouse.merge_upsert(silver, "silver", entity.name, [entity.primary_key])
        run.records_written = unique.count()
    return run.as_record()
