"""Print a readable summary of the local lakehouse: run log, quarantine reasons, Gold KPIs.

    python scripts/show_results.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F  # noqa: E402

from src.pipeline import build_lakehouse  # noqa: E402


def main() -> None:
    lh, cfg = build_lakehouse()
    print("\n=== Pipeline runs (latest per pipeline/batch) ===")
    runs = lh.read("ops", "pipeline_runs")
    latest = runs.groupBy("pipeline", "batch_id").agg(F.max("end_time").alias("end_time"))
    (runs.join(latest, ["pipeline", "batch_id", "end_time"])
     .select("pipeline", "batch_id", "records_read", "records_written", "records_rejected", "status")
     .orderBy("batch_id", "pipeline").show(60, truncate=False))

    print("=== Quarantine reasons ===")
    for entity in ["properties", "units", "tenants", "leases", "payments", "valuations", "maintenance"]:
        if lh.exists("quarantine", entity):
            q = lh.read("quarantine", entity)
            if q.count():
                (q.withColumn("reason", F.explode(F.split("error_reason", "; ")))
                 .groupBy(F.lit(entity).alias("entity"), "batch_id", "reason").count()
                 .orderBy("batch_id", "reason").show(truncate=False))
    if lh.exists("quarantine", "bronze_rejects"):
        lh.read("quarantine", "bronze_rejects").groupBy("entity", "batch_id", "error_reason").count().show()

    print("=== Silver row counts ===")
    for t in ["properties", "units", "tenants", "leases", "payments", "valuations", "maintenance",
              "property_value_history"]:
        df = lh.read("silver", t)
        extra = f" (current: {df.filter('is_current').count()})" if "is_current" in df.columns else ""
        print(f"  silver.{t:<24} {df.count():>6}{extra}")

    print("\n=== Portfolio summary (gold.portfolio_summary) ===")
    row = lh.read("gold", "portfolio_summary").first().asDict()
    for k, v in row.items():
        print(f"  {k:<30} {v}")


if __name__ == "__main__":
    main()
