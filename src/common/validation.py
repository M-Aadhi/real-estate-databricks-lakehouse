"""Row-level validation used by the Silver layer.

Rules never fix data. Each failed rule adds a reason; rows with at least one reason
are routed to quarantine with `error_reason` and `validation_timestamp`.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column as SparkColumn
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .schemas import Entity

REASONS_COL = "_reasons"


@dataclass
class Rule:
    """A business rule: rows where `failed` is true get `reason`."""
    reason: str
    failed: SparkColumn


def _blank_to_null(col: str) -> SparkColumn:
    trimmed = F.trim(F.col(col))
    return F.when(trimmed == "", None).otherwise(trimmed)


def cast_column(raw: str, spark_type: str) -> SparkColumn:
    """Safe cast: invalid values become null instead of failing the job (ANSI-safe)."""
    value = _blank_to_null(raw)
    if spark_type == "date":
        return F.to_date(F.expr(f"try_to_timestamp(nullif(trim(`{raw}`), ''), 'yyyy-MM-dd')"))
    if spark_type == "string":
        return value
    return F.expr(f"try_cast(nullif(trim(`{raw}`), '') AS {spark_type})")


def cast_to_schema(df: DataFrame, entity: Entity) -> DataFrame:
    """Cast every source column. The original string is kept as `_raw_<col>` for type checks."""
    out = df
    for c in entity.columns:
        out = out.withColumn(f"_raw_{c.name}", F.col(c.name))
    for c in entity.columns:
        out = out.withColumn(c.name, cast_column(f"_raw_{c.name}", c.spark_type))
    return out


def schema_rules(entity: Entity) -> list[Rule]:
    """Generic rules from the registry: required columns present, values parse to their type."""
    rules: list[Rule] = []
    for c in entity.columns:
        raw = F.col(f"_raw_{c.name}")
        raw_present = raw.isNotNull() & (F.trim(raw) != "")
        if not c.nullable:
            rules.append(Rule(f"missing_required:{c.name}", ~raw_present | raw.isNull()))
        if c.spark_type != "string":
            rules.append(Rule(f"invalid_{c.spark_type.split('(')[0]}:{c.name}", raw_present & F.col(c.name).isNull()))
    return rules


def foreign_key_rule(df: DataFrame, column: str, parent_keys: DataFrame, parent_key: str,
                     reason: str, optional: bool = False) -> DataFrame:
    """Flag rows whose `column` has no match in `parent_keys`. Adds a boolean `_fk_<column>` column."""
    keys = parent_keys.select(F.col(parent_key).alias("_fk_key")).distinct().withColumn("_fk_hit", F.lit(True))
    joined = df.join(keys, df[column] == keys["_fk_key"], "left")
    missing = F.col("_fk_hit").isNull() & F.col(column).isNotNull()
    if not optional:
        missing = missing | F.col(column).isNull()
    return (joined.withColumn(f"_fk_{column}", F.when(missing, F.lit(reason)))
            .drop("_fk_key", "_fk_hit"))


def apply_rules(df: DataFrame, rules: list[Rule]) -> DataFrame:
    """Collect failed-rule reasons into `_reasons` (array<string>), also picking up `_fk_*` columns."""
    fk_cols = [c for c in df.columns if c.startswith("_fk_")]
    parts = [F.when(r.failed, F.lit(r.reason)) for r in rules] + [F.col(c) for c in fk_cols]
    reasons = F.filter(F.array(*parts), lambda x: x.isNotNull()) if parts else F.array().cast("array<string>")
    return df.withColumn(REASONS_COL, reasons).drop(*fk_cols)


def split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = df.filter(F.size(REASONS_COL) == 0)
    invalid = (df.filter(F.size(REASONS_COL) > 0)
               .withColumn("error_reason", F.concat_ws("; ", F.col(REASONS_COL)))
               .withColumn("validation_timestamp", F.current_timestamp()))
    return valid, invalid


def row_hash(cols: list[str]) -> SparkColumn:
    """Deterministic hash of business columns; used for change detection and dedup."""
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]), 256)
