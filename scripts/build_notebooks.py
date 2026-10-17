"""Generate the Databricks notebooks in notebooks/ (keeps them small and in sync with src/).

Notebooks only orchestrate and display; all logic lives in the src/ package.

    python scripts/build_notebooks.py
"""
from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "notebooks"

SETUP = '''# Setup: make the repo's `src` package importable and select the Databricks environment.
# The notebook runs from <repo>/notebooks inside a Databricks Git folder.
import os, sys
repo_root = os.path.abspath("..")
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)
os.environ["LAKEHOUSE_ENV"] = "databricks"

dbutils.widgets.text("catalog", "workspace")          # Free Edition default; production: real_estate
catalog = dbutils.widgets.get("catalog")
overrides = {"environments": {"databricks": {"catalog": catalog,
                                             "raw_volume_root": f"/Volumes/{catalog}/raw/landing"}}}

from src.pipeline import build_lakehouse
lh, cfg = build_lakehouse(env="databricks", overrides=overrides)
print(f"catalog={cfg.catalog}  as_of={cfg.reporting_as_of}  batches={list(cfg.batches)}")'''

PIP = "%pip install -q pyyaml python-dateutil"


def md(text: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": text.strip("\n").splitlines(keepends=True)}


def code(text: str) -> dict:
    return {"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [],
            "source": text.strip("\n").splitlines(keepends=True)}


def notebook(cells: list[dict]) -> dict:
    return {"cells": cells, "metadata": {"language_info": {"name": "python"},
                                         "application/vnd.databricks.v1+notebook": {"language": "python"}},
            "nbformat": 4, "nbformat_minor": 5}


NOTEBOOKS = {
    "01_bronze_ingestion.ipynb": [
        md("""# 01 · Bronze ingestion
Raw CSV extracts -> `bronze.<entity>` Delta tables. Source values are kept as strings, with
`ingestion_timestamp`, `source_file` and `batch_id` added. Malformed rows go to
`quarantine.bronze_rejects`. Re-running a batch replaces that batch only (idempotent)."""),
        code(PIP), code(SETUP),
        md("""## Create schemas and stage the raw files in a Unity Catalog Volume
Serverless compute reads files from Volumes (not DBFS). We copy the repo's `data/raw` folder into
`/Volumes/<catalog>/raw/landing`."""),
        code('''import shutil
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.raw")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.raw.landing")
lh.setup()   # bronze, silver, gold, quarantine, ops schemas
shutil.copytree(os.path.join(repo_root, "data", "raw"), cfg.raw_volume_root, dirs_exist_ok=True)
display(dbutils.fs.ls(cfg.raw_volume_root))'''),
        md("## Ingest batch_001 (initial load)"),
        code('''from src.pipeline import run_bronze
results = run_bronze(lh, cfg, "batch_001")
display(spark.createDataFrame([{k: str(v) for k, v in r.items()} for r in results]))'''),
        code('''display(spark.table(f"{catalog}.quarantine.bronze_rejects"))'''),
        code('''display(spark.table(f"{catalog}.bronze.payments").limit(10))'''),
    ],
    "02_silver_transformations.ipynb": [
        md("""# 02 · Silver transformations
Type casting, standardisation, business rules, referential integrity, deduplication and
quarantine. Invalid rows are never corrected: they are written to `quarantine.<entity>` with
`error_reason` and `validation_timestamp`. Valid rows are MERGEd into `silver.<entity>`;
tenants use SCD Type 2."""),
        code(PIP), code(SETUP),
        code('''from src.pipeline import run_silver
results = run_silver(lh, cfg, "batch_001")     # also runs the Silver data-quality suite
display(spark.createDataFrame([{k: str(v) for k, v in r.items()} for r in results]))'''),
        md("## Why rows were quarantined"),
        code('''from pyspark.sql import functions as F
frames = []
for t in ["units", "tenants", "leases", "payments", "valuations", "maintenance"]:
    q = spark.table(f"{catalog}.quarantine.{t}")
    frames.append(q.select(F.lit(t).alias("entity"), "batch_id", F.explode(F.split("error_reason", "; ")).alias("reason")))
from functools import reduce
display(reduce(lambda a, b: a.unionByName(b), frames).groupBy("entity", "batch_id", "reason").count().orderBy("entity"))'''),
        code('''display(spark.table(f"{catalog}.ops.dq_results").orderBy(F.desc("checked_at")).limit(40))'''),
    ],
    "03_gold_models.ipynb": [
        md("""# 03 · Gold models
Business-ready tables rebuilt from Silver: occupancy, rent roll, lease expiry, delinquency,
maintenance, collection trend, property performance and the portfolio KPI summary."""),
        code(PIP), code(SETUP),
        code('''from src.pipeline import run_gold
run_gold(lh, cfg)    # also runs the Gold data-quality suite
display(spark.table(f"{catalog}.gold.portfolio_summary"))'''),
        code('''display(spark.table(f"{catalog}.gold.property_performance").orderBy("occupancy_rate"))'''),
        code('''display(spark.table(f"{catalog}.gold.collection_trend"))'''),
        md("## Export the dashboard data (writes dashboard/data/dashboard_data.json in the Git folder)"),
        code('''from src.dashboard.export_dashboard import export
print(export(lh, cfg))'''),
    ],
    "04_business_analysis.ipynb": [
        md("""# 04 · Business analysis
Questions management asks, answered with SQL on the Gold layer."""),
        code(PIP), code(SETUP),
        md("### Occupancy and value by property type"),
        code('''display(spark.sql(f"""
SELECT property_type, COUNT(*) AS properties, SUM(total_units) AS units,
       ROUND(SUM(occupied_units) * 100.0 / SUM(total_units), 1) AS occupancy_pct,
       ROUND(SUM(current_value) / 1e6, 1) AS value_musd,
       ROUND(AVG(value_growth_pct), 1) AS avg_value_growth_pct
FROM {catalog}.gold.property_performance GROUP BY property_type ORDER BY value_musd DESC"""))'''),
        md("### Leases expiring in the next 90 days (renewal pipeline)"),
        code('''display(spark.sql(f"""
SELECT property_name, COUNT(*) AS leases, ROUND(SUM(monthly_rent), 0) AS monthly_rent_at_risk
FROM {catalog}.gold.lease_expiry WHERE expires_within_90_days
GROUP BY property_name ORDER BY monthly_rent_at_risk DESC LIMIT 15"""))'''),
        md("### Delinquency by aging bucket"),
        code('''display(spark.sql(f"""
SELECT aging_bucket, COUNT(DISTINCT tenant_id) AS tenants, ROUND(SUM(outstanding_balance), 0) AS balance
FROM {catalog}.gold.tenant_delinquency GROUP BY aging_bucket ORDER BY aging_bucket"""))'''),
        md("### Properties with collection rate below 90% this month"),
        code('''display(spark.sql(f"""
SELECT property_name, property_type, collection_rate, outstanding_rent, occupancy_rate
FROM {catalog}.gold.property_performance WHERE collection_rate < 90 ORDER BY outstanding_rent DESC"""))'''),
        md("### Estimated NOI yield by property (assumption-based, see docs/architecture.md)"),
        code('''display(spark.sql(f"""
SELECT property_name, property_type, ttm_collected_rent, ttm_maintenance_cost, estimated_noi,
       current_value, estimated_noi_yield_pct
FROM {catalog}.gold.property_performance ORDER BY estimated_noi_yield_pct DESC LIMIT 15"""))'''),
    ],
    "05_incremental_merge_demo.ipynb": [
        md("""# 05 · Incremental load with Delta MERGE
batch_002 contains new leases and tenants, renewals, late payments, re-sent unchanged rows and
a few invalid rows. We load it, inspect what changed, then run it again to prove the load is
idempotent."""),
        code(PIP), code(SETUP),
        code('''def counts():
    return {t: spark.table(f"{catalog}.silver.{t}").count() for t in ["tenants", "leases", "payments", "valuations"]}
before = counts(); before'''),
        code('''from src.pipeline import run_bronze, run_silver, run_gold
run_bronze(lh, cfg, "batch_002")
run_silver(lh, cfg, "batch_002")
after = counts()
{t: (before[t], after[t]) for t in before}'''),
        md("### MERGE updated renewed leases in place (same lease_id, new end date and rent)"),
        code('''display(spark.table(f"{catalog}.silver.leases").filter("batch_id = 'batch_002'").orderBy("lease_id"))'''),
        md("### SCD Type 2: tenants with a contact change now have two versions"),
        code('''display(spark.sql(f"""
SELECT tenant_id, email, phone, effective_from, effective_to, is_current
FROM {catalog}.silver.tenants
WHERE tenant_id IN (SELECT tenant_id FROM {catalog}.silver.tenants GROUP BY tenant_id HAVING COUNT(*) > 1)
ORDER BY tenant_id, effective_from"""))'''),
        md("### Delta history shows the MERGE operations"),
        code('''display(spark.sql(f"DESCRIBE HISTORY {catalog}.silver.leases").select("version", "timestamp", "operation", "operationMetrics"))'''),
        md("### Run batch_002 again: nothing changes"),
        code('''run_bronze(lh, cfg, "batch_002")
run_silver(lh, cfg, "batch_002")
assert counts() == after, "re-running a batch must not change Silver"
print("idempotent:", counts())'''),
        code('''run_gold(lh, cfg)
display(spark.table(f"{catalog}.gold.portfolio_summary"))'''),
    ],
}


def main() -> None:
    OUT.mkdir(exist_ok=True)
    for name, cells in NOTEBOOKS.items():
        (OUT / name).write_text(json.dumps(notebook(cells), indent=1) + "\n", encoding="utf-8")
        print("wrote", OUT / name)


if __name__ == "__main__":
    main()
