"""Export Gold metrics to dashboard/data/dashboard_data.json (+ dashboard_data.js).

Gold Delta tables -> export_dashboard.py -> dashboard_data.json -> dashboard/index.html

The .js copy (window.DASHBOARD_DATA = {...}) lets index.html work when opened straight
from disk (file://), where browsers block fetch() of local JSON.

    python -m src.dashboard.export_dashboard
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from pyspark.sql import functions as F

from src.common.config import REPO_ROOT
from src.common.logging_utils import get_logger

OUT_DIR = REPO_ROOT / "dashboard" / "data"
log = get_logger("dashboard.export")


def _clean(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _rows(df, cols=None) -> list[dict]:
    df = df.select(*cols) if cols else df
    return [{k: _clean(v) for k, v in r.asDict().items()} for r in df.collect()]


def build_payload(lh, cfg) -> dict:
    gold = lambda t: lh.read("gold", t)  # noqa: E731
    summary = _rows(gold("portfolio_summary"))[0]
    perf = gold("property_performance")

    by_type = (perf.groupBy("property_type").agg(
        F.count("*").alias("properties"), F.sum("current_value").alias("portfolio_value"),
        F.sum("total_units").alias("units")).orderBy(F.desc("portfolio_value")))
    expiry_timeline = (gold("lease_expiry").filter(F.col("days_to_expiry") <= 365)
                       .groupBy("expiry_month").agg(F.count("*").alias("leases"),
                                                    F.sum("monthly_rent").alias("monthly_rent_at_risk"))
                       .orderBy("expiry_month"))
    maint = (gold("maintenance_summary").groupBy("property_id", "property_name")
             .agg(F.sum("total_cost").alias("ttm_maintenance_cost"), F.sum("work_orders").alias("work_orders"))
             .orderBy(F.desc("ttm_maintenance_cost")).limit(10))
    delinquency = gold("tenant_delinquency")
    aging = (delinquency.groupBy("aging_bucket").agg(F.sum("outstanding_balance").alias("balance"),
                                                     F.countDistinct("tenant_id").alias("tenants"))
             .orderBy("aging_bucket"))
    prop_cols = ["property_id", "property_name", "property_type", "city", "total_units", "occupied_units",
                 "occupancy_rate", "scheduled_monthly_rent", "collected_rent", "collection_rate",
                 "ttm_collected_rent", "ttm_maintenance_cost", "current_value", "value_growth_pct",
                 "estimated_noi"]

    return {
        "meta": {
            "title": "Real Estate Portfolio Dashboard",
            "as_of_date": cfg.reporting_as_of.isoformat(),
            "reporting_month": cfg.reporting_month,
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "data_note": "Synthetic demo data. No real company, tenant or property information.",
            "source": "Gold Delta tables (real-estate-databricks-lakehouse)",
        },
        "kpis": summary,
        "occupancy_by_property": _rows(perf.orderBy("occupancy_rate", "property_id").limit(15),
                                       ["property_name", "occupancy_rate", "occupied_units", "total_units"]),
        "revenue_by_property": _rows(perf.orderBy(F.desc("ttm_collected_rent")).limit(10),
                                     ["property_name", "ttm_collected_rent", "property_type"]),
        "value_by_property_type": _rows(by_type),
        "lease_expiry_timeline": _rows(expiry_timeline),
        "collection_trend": _rows(gold("collection_trend")),
        "maintenance_by_property": _rows(maint),
        "delinquency_aging": _rows(aging),
        "top_delinquent_tenants": _rows(delinquency.filter("is_delinquent").orderBy(F.desc("outstanding_balance"))
                                        .limit(10), ["tenant_name", "property_name", "outstanding_balance",
                                                     "days_past_due", "aging_bucket"]),
        "properties": _rows(perf.orderBy(F.desc("current_value")), prop_cols),
    }


def export(lh, cfg, out_dir: Path | str = OUT_DIR) -> Path:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = build_payload(lh, cfg)
    text = json.dumps(payload, indent=2, default=str)
    (out_dir / "dashboard_data.json").write_text(text, encoding="utf-8")
    (out_dir / "dashboard_data.js").write_text(f"window.DASHBOARD_DATA = {text};\n", encoding="utf-8")
    log.info("dashboard data written to %s (%d properties)", out_dir, len(payload["properties"]))
    return out_dir / "dashboard_data.json"


if __name__ == "__main__":
    from src.pipeline import build_lakehouse

    lh, cfg = build_lakehouse()
    export(lh, cfg)
