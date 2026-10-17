"""Delta table access for two storage modes.

- path mode (local):       <lakehouse_root>/<layer>/<table>      e.g. lakehouse/silver/leases
- catalog mode (Databricks): <catalog>.<layer>.<table>          e.g. real_estate.silver.leases

Every write is idempotent:
- write_batch()   replaces only the rows of one batch_id (Bronze, Quarantine)
- merge_upsert()  Delta MERGE on business keys (Silver)
- overwrite()     full refresh (Gold, recomputed from Silver)
"""
from __future__ import annotations

import logging
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from .config import Config
from .exceptions import TableWriteError

log = logging.getLogger(__name__)


class Lakehouse:
    def __init__(self, spark: SparkSession, cfg: Config):
        self.spark = spark
        self.cfg = cfg

    # ---------- naming ----------
    def table_id(self, layer: str, table: str) -> str:
        if self.cfg.storage_mode == "catalog":
            return f"{self.cfg.catalog}.{layer}.{table}"
        return str(Path(self.cfg.lakehouse_root) / layer / table)

    def setup(self) -> None:
        """Create schemas in catalog mode (guarded, see docs/databricks_setup.md)."""
        if self.cfg.storage_mode != "catalog":
            return
        for layer in self.cfg.raw.get("layers", ["bronze", "silver", "gold", "quarantine", "ops"]):
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.cfg.catalog}.{layer}")

    # ---------- reads ----------
    def exists(self, layer: str, table: str) -> bool:
        tid = self.table_id(layer, table)
        if self.cfg.storage_mode == "catalog":
            return self.spark.catalog.tableExists(tid)
        return (Path(tid) / "_delta_log").is_dir()

    def read(self, layer: str, table: str) -> DataFrame:
        tid = self.table_id(layer, table)
        if self.cfg.storage_mode == "catalog":
            return self.spark.table(tid)
        return self.spark.read.format("delta").load(tid)

    def delta_table(self, layer: str, table: str) -> DeltaTable:
        tid = self.table_id(layer, table)
        if self.cfg.storage_mode == "catalog":
            return DeltaTable.forName(self.spark, tid)
        return DeltaTable.forPath(self.spark, tid)

    # ---------- writes ----------
    def _save(self, writer, layer: str, table: str) -> None:
        tid = self.table_id(layer, table)
        if self.cfg.storage_mode == "catalog":
            writer.saveAsTable(tid)
        else:
            writer.save(tid)

    def overwrite(self, df: DataFrame, layer: str, table: str) -> int:
        """Full refresh. Returns rows written."""
        try:
            writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            self._save(writer, layer, table)
        except Exception as exc:  # surface as a pipeline error with context
            raise TableWriteError(f"Overwrite of {layer}.{table} failed: {exc}") from exc
        return self.read(layer, table).count()

    def write_batch(self, df: DataFrame, layer: str, table: str, batch_id: str) -> None:
        """Replace the rows of `batch_id` only, so re-running a batch never duplicates data."""
        try:
            if not self.exists(layer, table):
                self._save(df.write.format("delta").mode("append"), layer, table)
                return
            writer = (df.write.format("delta").mode("overwrite")
                      .option("replaceWhere", f"batch_id = '{batch_id}'"))
            self._save(writer, layer, table)
        except Exception as exc:
            raise TableWriteError(f"Batch write of {layer}.{table} ({batch_id}) failed: {exc}") from exc

    def merge_upsert(self, df: DataFrame, layer: str, table: str, keys: list[str],
                     change_col: str = "row_hash") -> None:
        """Delta MERGE: insert new keys, update keys whose content changed, ignore identical rows."""
        try:
            if not self.exists(layer, table):
                self._save(df.write.format("delta").mode("append"), layer, table)
                return
            on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
            (self.delta_table(layer, table).alias("t")
             .merge(df.alias("s"), on)
             .whenMatchedUpdateAll(condition=f"t.{change_col} <> s.{change_col}")
             .whenNotMatchedInsertAll()
             .execute())
        except Exception as exc:
            raise TableWriteError(f"MERGE into {layer}.{table} failed: {exc}") from exc

    def append(self, df: DataFrame, layer: str, table: str) -> None:
        try:
            self._save(df.write.format("delta").mode("append").option("mergeSchema", "true"), layer, table)
        except Exception as exc:
            raise TableWriteError(f"Append to {layer}.{table} failed: {exc}") from exc
