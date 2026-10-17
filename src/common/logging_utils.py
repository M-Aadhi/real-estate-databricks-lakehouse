"""Structured pipeline logging.

    with PipelineRun(lh, "silver.leases", batch_id="batch_001") as run:
        run.records_read = 1000
        ...
        run.records_written = 990
        run.records_rejected = 10

On exit one record is written to ops.pipeline_runs (Delta) and logged as JSON:
pipeline, batch_id, start/end time, read/written/rejected counts, status, error message.
"""
from __future__ import annotations

import json
import logging
import sys
import traceback
from datetime import datetime, timezone

_CONFIGURED = False


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    global _CONFIGURED
    if not _CONFIGURED:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(name)s | %(message)s"))
        root = logging.getLogger("lakehouse")
        root.addHandler(handler)
        root.setLevel(level)
        root.propagate = False
        _CONFIGURED = True
    return logging.getLogger(f"lakehouse.{name}")


class PipelineRun:
    def __init__(self, lakehouse, pipeline: str, batch_id: str | None = None, persist: bool = True):
        self.lh = lakehouse
        self.pipeline = pipeline
        self.batch_id = batch_id
        self.persist = persist
        self.records_read = 0
        self.records_written = 0
        self.records_rejected = 0
        self.status = "RUNNING"
        self.error_message: str | None = None
        self.start_time: datetime | None = None
        self.end_time: datetime | None = None
        self.log = get_logger(pipeline)

    def __enter__(self) -> "PipelineRun":
        self.start_time = datetime.now(timezone.utc)
        self.log.info("started batch=%s", self.batch_id)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.end_time = datetime.now(timezone.utc)
        if exc is None:
            self.status = "SUCCESS"
        else:
            self.status = "FAILED"
            self.error_message = f"{exc_type.__name__}: {exc}"
            self.log.error("failed: %s\n%s", self.error_message, "".join(traceback.format_tb(tb)[-3:]))
        record = self.as_record()
        self.log.info("finished %s", json.dumps(record, default=str))
        if self.persist and self.lh is not None:
            self._persist(record)
        return False  # never swallow the exception

    def as_record(self) -> dict:
        duration = (self.end_time - self.start_time).total_seconds() if self.end_time else None
        return {
            "pipeline": self.pipeline,
            "batch_id": self.batch_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": round(duration, 2) if duration is not None else None,
            "records_read": int(self.records_read),
            "records_written": int(self.records_written),
            "records_rejected": int(self.records_rejected),
            "status": self.status,
            "error_message": self.error_message,
        }

    def _persist(self, record: dict) -> None:
        from pyspark.sql import types as T

        schema = T.StructType([
            T.StructField("pipeline", T.StringType()), T.StructField("batch_id", T.StringType()),
            T.StructField("start_time", T.TimestampType()), T.StructField("end_time", T.TimestampType()),
            T.StructField("duration_seconds", T.DoubleType()), T.StructField("records_read", T.LongType()),
            T.StructField("records_written", T.LongType()), T.StructField("records_rejected", T.LongType()),
            T.StructField("status", T.StringType()), T.StructField("error_message", T.StringType()),
        ])
        try:
            df = self.lh.spark.createDataFrame([record], schema)
            self.lh.append(df, "ops", "pipeline_runs")
        except Exception as exc:  # logging must not hide the real result, but we do report it
            self.log.warning("could not persist run log: %s", exc)
