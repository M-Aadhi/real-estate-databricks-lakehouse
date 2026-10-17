"""Pipeline orchestrator (local CLI and Databricks notebooks use the same functions).

Examples:
    python -m src.pipeline --batch batch_001                 # bronze + silver for one batch, then gold
    python -m src.pipeline --all                             # every configured batch in order, gold, dashboard
    python -m src.pipeline --batch batch_002 --layers bronze,silver
    python -m src.pipeline --layers gold                     # rebuild gold only
"""
from __future__ import annotations

import argparse
import importlib
import sys

from src.common.config import load_config
from src.common.data_quality import gold_checks, record_results, silver_checks
from src.common.exceptions import PipelineError
from src.common.logging_utils import get_logger
from src.common.spark import get_spark
from src.common.storage import Lakehouse

GOLD_TABLES = ["occupancy", "rent_roll", "lease_expiry", "tenant_delinquency", "maintenance_summary",
               "collection_trend", "property_performance", "portfolio_summary"]

log = get_logger("pipeline")


def build_lakehouse(env: str | None = None, config_path: str | None = None, overrides: dict | None = None):
    cfg = load_config(config_path, env=env, overrides=overrides)
    get_logger("pipeline", cfg.log_level)
    lh = Lakehouse(get_spark(), cfg)
    lh.setup()
    return lh, cfg


def run_bronze(lh, cfg, batch_id: str) -> list[dict]:
    batch = cfg.batch(batch_id)
    out = []
    for entity in batch.entities:
        module = importlib.import_module(f"src.bronze.ingest_{entity}")
        out.append(module.run(lh, batch_id, batch.path))
    return out


def run_silver(lh, cfg, batch_id: str, check: bool = True) -> list[dict]:
    out = []
    for entity in cfg.batch(batch_id).entities:   # entities are in dependency order
        module = importlib.import_module(f"src.silver.transform_{entity}")
        out.append(module.run(lh, batch_id, cfg))
    if check:
        enforce(lh, silver_checks(lh), "silver", batch_id)
    return out


def run_gold(lh, cfg, check: bool = True) -> list[dict]:
    out = [importlib.import_module(f"src.gold.{t}").run(lh, cfg) for t in GOLD_TABLES]
    if check:
        enforce(lh, gold_checks(lh), "gold")
    return out


def enforce(lh, results, stage: str, batch_id: str | None = None) -> None:
    record_results(lh, results, stage, batch_id)
    failed = [r for r in results if not r.passed and r.critical]
    for r in results:
        (log.info if r.passed else log.error)("DQ %-7s %-28s %-30s failed_rows=%d",
                                               "PASS" if r.passed else "FAIL", r.table, r.check, r.failed_rows)
    if failed:
        raise PipelineError(f"{len(failed)} critical {stage} data quality check(s) failed: "
                            + ", ".join(f"{r.table}:{r.check}" for r in failed))


def run_all(lh, cfg, export: bool = True) -> None:
    for batch_id in cfg.batches:
        run_bronze(lh, cfg, batch_id)
        run_silver(lh, cfg, batch_id)
    run_gold(lh, cfg)
    if export:
        from src.dashboard.export_dashboard import export
        export(lh, cfg)


def entity_cli(layer: str, entity: str) -> None:
    """Shared CLI for the per-entity modules (src/bronze/ingest_*.py, src/silver/transform_*.py)."""
    parser = argparse.ArgumentParser(description=f"{layer} {entity}")
    parser.add_argument("--batch", required=True)
    parser.add_argument("--env", default=None)
    args = parser.parse_args()
    lh, cfg = build_lakehouse(args.env)
    if layer == "bronze":
        importlib.import_module(f"src.bronze.ingest_{entity}").run(lh, args.batch, cfg.batch(args.batch).path)
    else:
        importlib.import_module(f"src.silver.transform_{entity}").run(lh, args.batch, cfg)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Real estate lakehouse pipeline")
    parser.add_argument("--env", default=None, help="local (default) or databricks")
    parser.add_argument("--config", default=None)
    parser.add_argument("--batch", help="batch id from config, e.g. batch_001")
    parser.add_argument("--layers", default="bronze,silver,gold")
    parser.add_argument("--all", action="store_true", help="run every batch in order, then gold and export")
    parser.add_argument("--no-export", action="store_true")
    args = parser.parse_args(argv)

    lh, cfg = build_lakehouse(args.env, args.config)
    layers = [x.strip() for x in args.layers.split(",") if x.strip()]
    try:
        if args.all:
            run_all(lh, cfg, export=not args.no_export)
            return 0
        if ("bronze" in layers or "silver" in layers) and not args.batch:
            parser.error("--batch is required for bronze/silver")
        if "bronze" in layers:
            run_bronze(lh, cfg, args.batch)
        if "silver" in layers:
            run_silver(lh, cfg, args.batch)
        if "gold" in layers:
            run_gold(lh, cfg)
            if not args.no_export:
                from src.dashboard.export_dashboard import export
                export(lh, cfg)
    except PipelineError as exc:
        log.error("pipeline stopped: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
