"""Unit tests for config loading, file checks, storage idempotency and run logging."""
import pytest

from src.bronze.ingest import check_source_file
from src.common.config import load_config
from src.common.exceptions import ConfigError, EmptySourceFileError, MissingSourceFileError, SchemaValidationError
from src.common.logging_utils import PipelineRun


def test_config_defaults():
    cfg = load_config(env="local")
    assert cfg.storage_mode == "path" and str(cfg.reporting_as_of) == "2026-09-30"
    assert list(cfg.batches) == ["batch_001", "batch_002"]
    assert cfg.batch("batch_002").entities == ["tenants", "leases", "payments", "valuations"]


def test_config_databricks_uses_catalog_and_volume():
    cfg = load_config(env="databricks")
    assert cfg.storage_mode == "catalog" and cfg.catalog == "workspace"
    assert cfg.batch("batch_002").path == "/Volumes/workspace/raw/landing/incremental/batch_002"


def test_config_errors(tmp_path):
    with pytest.raises(ConfigError):
        load_config(tmp_path / "missing.yaml")
    with pytest.raises(ConfigError):
        load_config(env="nope")
    with pytest.raises(ConfigError):
        load_config(env="local").batch("batch_999")


def test_source_file_checks(tmp_path):
    cols = ["a", "b"]
    with pytest.raises(MissingSourceFileError):
        check_source_file(str(tmp_path / "x.csv"), cols)
    empty = tmp_path / "empty.csv"
    empty.write_text("")
    with pytest.raises(EmptySourceFileError):
        check_source_file(str(empty), cols)
    header_only = tmp_path / "h.csv"
    header_only.write_text("a,b\n")
    with pytest.raises(EmptySourceFileError):
        check_source_file(str(header_only), cols)
    missing = tmp_path / "m.csv"
    missing.write_text("a,c\n1,2\n")
    with pytest.raises(SchemaValidationError, match="missing required columns"):
        check_source_file(str(missing), cols)
    ok = tmp_path / "ok.csv"
    ok.write_text("A, b ,extra\n1,2,3\n")
    assert check_source_file(str(ok), cols) == ["a", "b", "extra"]


def test_write_batch_and_merge_are_idempotent(make_lakehouse):
    lh, _ = make_lakehouse("storage")
    df = lh.spark.createDataFrame([("1", "x", "b1"), ("2", "y", "b1")], "id string, row_hash string, batch_id string")
    lh.write_batch(df, "bronze", "t", "b1")
    lh.write_batch(df, "bronze", "t", "b1")
    assert lh.read("bronze", "t").count() == 2
    lh.merge_upsert(df, "silver", "t", ["id"])
    changed = lh.spark.createDataFrame([("2", "y2", "b2"), ("3", "z", "b2")], "id string, row_hash string, batch_id string")
    lh.merge_upsert(changed, "silver", "t", ["id"])
    lh.merge_upsert(changed, "silver", "t", ["id"])
    rows = {r.id: r.row_hash for r in lh.read("silver", "t").collect()}
    assert rows == {"1": "x", "2": "y2", "3": "z"}


def test_pipeline_run_logs_failure_and_reraises(make_lakehouse):
    lh, _ = make_lakehouse("runlog")
    with pytest.raises(ValueError):
        with PipelineRun(lh, "unit.test", "b1") as run:
            run.records_read = 5
            raise ValueError("boom")
    with PipelineRun(lh, "unit.test", "b2") as run:
        run.records_written = 3
    rows = {r.batch_id: r for r in lh.read("ops", "pipeline_runs").collect()}
    assert rows["b1"].status == "FAILED" and "boom" in rows["b1"].error_message and rows["b1"].records_read == 5
    assert rows["b2"].status == "SUCCESS" and rows["b2"].records_written == 3
