"""Generic Bronze ingestion: raw CSV -> bronze.<entity> (Delta).

- Every source column is read as STRING, values are preserved exactly (no business logic).
- Adds ingestion_timestamp, source_file, batch_id.
- Malformed CSV rows (wrong number of fields) go to quarantine.bronze_rejects.
- File-level problems (missing / empty / missing columns) raise and stop the load.
- Idempotent: re-running a batch replaces that batch's rows (Delta replaceWhere).
"""
from __future__ import annotations

import csv
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common.exceptions import EmptySourceFileError, MissingSourceFileError, SchemaValidationError
from src.common.logging_utils import PipelineRun
from src.common.schemas import ENTITIES

CORRUPT_COL = "_corrupt_record"


def check_source_file(path: str, expected_columns: list[str]) -> list[str]:
    """Validate the file before Spark reads it. Returns the header columns."""
    if not os.path.isfile(path):
        raise MissingSourceFileError(f"Source file not found: {path}")
    if os.path.getsize(path) == 0:
        raise EmptySourceFileError(f"Source file is empty: {path}")
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = [h.strip().lower() for h in next(reader, [])]
        first_row = next(reader, None)
    if not header:
        raise EmptySourceFileError(f"Source file has no header: {path}")
    missing = [c for c in expected_columns if c not in header]
    if missing:
        raise SchemaValidationError(f"{os.path.basename(path)} is missing required columns: {missing}")
    if first_row is None:
        raise EmptySourceFileError(f"Source file has a header but no data rows: {path}")
    return header


def read_raw_csv(spark, path: str, header: list[str]) -> DataFrame:
    """Read with an all-string schema in the file's own column order, plus a corrupt-record column."""
    schema = T.StructType([T.StructField(c, T.StringType()) for c in header]
                          + [T.StructField(CORRUPT_COL, T.StringType())])
    return (spark.read.format("csv")
            .option("header", True)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_COL)
            .option("enforceSchema", False)
            .schema(schema)
            .load(path)
            .withColumn("source_file", F.col("_metadata.file_path")))


def ingest_entity(lakehouse, entity_name: str, batch_id: str, source_dir: str) -> dict:
    """Load one entity file of one batch into Bronze. Returns run metrics."""
    entity = ENTITIES[entity_name]
    path = os.path.join(source_dir, f"{entity_name}.csv")
    with PipelineRun(lakehouse, f"bronze.{entity_name}", batch_id) as run:
        header = check_source_file(path, entity.column_names)
        extra = [c for c in header if c not in entity.column_names]
        if extra:
            run.log.warning("ignoring unexpected columns in %s: %s", path, extra)

        raw = (read_raw_csv(lakehouse.spark, path, header)
               .withColumn("ingestion_timestamp", F.current_timestamp())
               .withColumn("batch_id", F.lit(batch_id)))

        good = raw.filter(F.col(CORRUPT_COL).isNull()).select(
            *entity.column_names, "ingestion_timestamp", "source_file", "batch_id")
        # Two Spark details matter here:
        # 1. Queries on raw CSV may not reference ONLY the corrupt-record column (and serverless
        #    compute cannot cache the parsed result).
        # 2. CSV column pruning parses only the referenced columns, so a row with extra fields is
        #    flagged as corrupt only when every column is parsed.
        # Referencing all source columns in the filter handles both, on Databricks and locally.
        key = entity.primary_key
        all_cols = F.coalesce(*[F.col(c) for c in header], F.col(CORRUPT_COL))
        is_corrupt = F.col(CORRUPT_COL).isNotNull() & all_cols.isNotNull()
        rejects = raw.filter(is_corrupt).select(
            F.lit(entity_name).alias("entity"),
            F.col(key).alias("record_key"),
            F.col(CORRUPT_COL).alias("raw_record"),
            F.lit("malformed_csv_row").alias("error_reason"),
            F.current_timestamp().alias("validation_timestamp"),
            "source_file", "batch_id")

        run.records_read = raw.count()
        lakehouse.write_batch(good, "bronze", entity_name, batch_id)
        run.records_written = lakehouse.read("bronze", entity_name).filter(F.col("batch_id") == batch_id).count()

        reject_count = rejects.count()
        run.records_rejected = reject_count
        if reject_count or lakehouse.exists("quarantine", "bronze_rejects"):
            # replace this entity+batch slice so reruns stay idempotent
            _write_rejects(lakehouse, rejects, entity_name, batch_id)
        if reject_count:
            run.log.warning("%d malformed row(s) in %s sent to quarantine.bronze_rejects", reject_count, path)
    return run.as_record()


def _write_rejects(lakehouse, rejects: DataFrame, entity_name: str, batch_id: str) -> None:
    if not lakehouse.exists("quarantine", "bronze_rejects"):
        lakehouse.append(rejects, "quarantine", "bronze_rejects")
        return
    (lakehouse.delta_table("quarantine", "bronze_rejects")
     .delete(f"entity = '{entity_name}' AND batch_id = '{batch_id}'"))
    lakehouse.append(rejects, "quarantine", "bronze_rejects")
