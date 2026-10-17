"""Helpers shared by the Gold table builders."""
from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import DataFrame

from src.common.config import Config
from src.common.logging_utils import PipelineRun


def silver(lakehouse, name: str) -> DataFrame:
    return lakehouse.read("silver", name)


def build_and_write(lakehouse, cfg: Config, table: str, builder: Callable[[], DataFrame]) -> dict:
    """Gold tables are full refreshes of Silver -> always consistent and idempotent."""
    with PipelineRun(lakehouse, f"gold.{table}") as run:
        df = builder()
        run.records_written = lakehouse.overwrite(df, "gold", table)
        run.records_read = run.records_written
    return run.as_record()
