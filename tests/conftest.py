"""Shared pytest fixtures.

- `spark`: one local SparkSession with Delta for the whole test run.
- `fixture_lakehouse`: runs the real pipeline over tests/fixtures (batch_001, rerun, batch_002,
  rerun) into a temp folder and records results after each step. Integration and
  data-quality tests read from it, so the expensive Spark work happens once.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"
sys.path.insert(0, str(ROOT))
os.environ.setdefault("SPARK_MASTER", "local[1]")
os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "1")


@pytest.fixture(scope="session")
def spark():
    from src.common.spark import get_spark

    session = get_spark("lakehouse-tests")
    yield session
    session.stop()


def fixture_overrides(root: Path) -> dict:
    return {
        "environments": {"local": {"lakehouse_root": str(root / "lakehouse")}},
        # Fixtures are deliberately dirty (e.g. 3 of 7 leases invalid), so allow a high rate here.
        # tests/integration::test_quarantine_threshold_stops_load checks the limit itself.
        "data_quality": {"max_quarantine_pct": 60.0},
        "batches": {
            "batch_001": {"path": str(FIXTURES / "batch_001"), "effective_date": "2026-09-01",
                          "entities": ["properties", "units", "tenants", "leases", "payments", "valuations",
                                       "maintenance"]},
            "batch_002": {"path": str(FIXTURES / "batch_002"), "effective_date": "2026-10-01",
                          "entities": ["tenants", "leases", "payments"]},
        },
    }


@pytest.fixture(scope="session")
def make_lakehouse(spark, tmp_path_factory):
    """Factory for an isolated Lakehouse pointing at a fresh temp folder."""
    from src.common.config import load_config
    from src.common.storage import Lakehouse

    def _make(name: str = "lh", extra: dict | None = None):
        root = tmp_path_factory.mktemp(name)
        overrides = fixture_overrides(root)
        if extra:
            overrides.update(extra)
        cfg = load_config(env="local", overrides=overrides)
        return Lakehouse(spark, cfg), cfg

    return _make


def _snapshot(lh) -> dict:
    tables = {}
    for layer, names in {
        "bronze": ["properties", "units", "tenants", "leases", "payments", "valuations", "maintenance"],
        "silver": ["properties", "units", "tenants", "leases", "payments", "valuations", "maintenance"],
        "quarantine": ["units", "tenants", "leases", "payments", "valuations", "maintenance", "bronze_rejects"],
    }.items():
        for n in names:
            tables[f"{layer}.{n}"] = lh.read(layer, n).count() if lh.exists(layer, n) else 0
    summary = lh.read("gold", "portfolio_summary").first().asDict() if lh.exists("gold", "portfolio_summary") else {}
    return {"counts": tables, "summary": summary}


@pytest.fixture(scope="session")
def fixture_lakehouse(make_lakehouse):
    from src.pipeline import run_bronze, run_gold, run_silver

    lh, cfg = make_lakehouse("pipeline")
    steps = {}
    run_bronze(lh, cfg, "batch_001")
    run_silver(lh, cfg, "batch_001")
    run_gold(lh, cfg)
    steps["batch_001"] = _snapshot(lh)
    run_bronze(lh, cfg, "batch_001")          # rerun the same batch: must be a no-op
    run_silver(lh, cfg, "batch_001")
    steps["batch_001_rerun"] = _snapshot(lh)  # gold is a full refresh; rechecked after batch_002 rerun
    run_bronze(lh, cfg, "batch_002")          # incremental batch
    run_silver(lh, cfg, "batch_002")
    run_gold(lh, cfg)
    steps["batch_002"] = _snapshot(lh)
    run_bronze(lh, cfg, "batch_002")          # rerun incremental batch
    run_silver(lh, cfg, "batch_002")
    run_gold(lh, cfg)
    steps["batch_002_rerun"] = _snapshot(lh)
    return lh, cfg, steps
