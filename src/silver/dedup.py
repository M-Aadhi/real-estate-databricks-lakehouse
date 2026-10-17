"""Deduplication strategy (documented in docs/architecture.md).

Within one batch, rows are grouped by primary key:
1. Exact duplicates (same key, same business values) -> keep one row. Typical cause:
   source system re-sending the same record. Counted and logged, not an error.
2. Conflicting duplicates (same key, different values) -> we cannot know which one is
   correct, so ALL versions are quarantined with reason `conflicting_duplicate_key`.
   We never guess.
Across batches, a later batch with changed values is an update and goes through MERGE.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def deduplicate(df: DataFrame, key: str, hash_col: str = "row_hash",
                order_col: str = "ingestion_timestamp") -> tuple[DataFrame, DataFrame, int]:
    """Return (unique_rows, conflicting_rows, exact_duplicates_removed)."""
    by_key = Window.partitionBy(key)
    marked = df.withColumn("_versions", F.size(F.collect_set(hash_col).over(by_key)))

    conflicts = (marked.filter(F.col("_versions") > 1).drop("_versions")
                 .withColumn("error_reason", F.lit("conflicting_duplicate_key"))
                 .withColumn("validation_timestamp", F.current_timestamp()))

    single = marked.filter(F.col("_versions") == 1).drop("_versions")
    ranked = single.withColumn(
        "_rn", F.row_number().over(Window.partitionBy(key).orderBy(F.col(order_col).desc(), F.col("source_file")))
    )
    unique = ranked.filter(F.col("_rn") == 1).drop("_rn")
    exact_removed = ranked.filter(F.col("_rn") > 1).count()
    return unique, conflicts, exact_removed
