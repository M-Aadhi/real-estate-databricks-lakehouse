"""SCD Type 2 merge for slowly changing attributes (used for tenants).

Target columns added: effective_from, effective_to, is_current, row_hash.
- New key            -> insert version (effective_from = created/effective date, is_current = true)
- Changed attributes -> close current version (effective_to = new effective_from - 1 day,
                        is_current = false) and insert a new current version
- Unchanged          -> nothing happens (so re-running a batch is idempotent)

Implemented with the standard "staged union" MERGE: changed rows appear twice in the
staged source, once with a NULL merge key (forces an insert) and once with the real
key (matches and closes the old version).
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

OPEN_END = "9999-12-31"


def prepare_scd2_source(source: DataFrame, effective_from: F.Column) -> DataFrame:
    return (source.withColumn("effective_from", effective_from)
            .withColumn("effective_to", F.to_date(F.lit(OPEN_END)))
            .withColumn("is_current", F.lit(True)))


def stage_scd2(source: DataFrame, current: DataFrame | None, key: str) -> DataFrame:
    """Build the staged MERGE source. `current` = target rows where is_current."""
    keyed = source.withColumn("_merge_key", F.col(key))
    if current is None:
        return keyed
    cur = current.select(F.col(key).alias("_cur_key"), F.col("row_hash").alias("_cur_hash"))
    changed = (source.join(cur, source[key] == cur["_cur_key"])
               .filter(F.col("row_hash") != F.col("_cur_hash"))
               .drop("_cur_key", "_cur_hash")
               .withColumn("_merge_key", F.lit(None).cast("string")))
    return keyed.unionByName(changed)


def merge_scd2(lakehouse, source: DataFrame, layer: str, table: str, key: str) -> None:
    """Apply SCD2 changes from `source` (already has effective_from/to, is_current, row_hash)."""
    if not lakehouse.exists(layer, table):
        lakehouse._save(source.write.format("delta").mode("append"), layer, table)
        return
    current = lakehouse.read(layer, table).filter("is_current")
    staged = stage_scd2(source, current, key)
    cols = [c for c in source.columns]
    insert_values = {c: f"s.{c}" for c in cols}
    (lakehouse.delta_table(layer, table).alias("t")
     .merge(staged.alias("s"), f"t.{key} = s._merge_key AND t.is_current = true")
     .whenMatchedUpdate(
         condition="t.row_hash <> s.row_hash",
         set={"is_current": "false", "effective_to": "date_sub(s.effective_from, 1)"})
     .whenNotMatchedInsert(values=insert_values)
     .execute())
