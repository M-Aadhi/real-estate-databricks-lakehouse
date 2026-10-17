"""Generate docs/data_dictionary.md from the schema registry and the local Delta tables.

Types and example values come from the actual tables (run the pipeline first);
descriptions and rules come from src/common/schemas.py and the DESCRIPTIONS map below.

    python -m src.pipeline --all && python scripts/generate_data_dictionary.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.common.schemas import BRONZE_METADATA, ENTITIES  # noqa: E402
from src.pipeline import GOLD_TABLES, build_lakehouse  # noqa: E402

LINEAGE = {
    "ingestion_timestamp": ("When the row was loaded into Bronze", ""),
    "source_file": ("Full path of the source file", ""),
    "batch_id": ("Source batch (extract) the row arrived in", "Bronze/Quarantine are replaced per batch_id"),
    "row_hash": ("SHA-256 of the business columns", "Used for change detection in MERGE / SCD2"),
    "silver_updated_at": ("When the Silver row was last inserted/updated", ""),
    "effective_from": ("First day this version is valid", "SCD2"),
    "effective_to": ("Last day this version is valid (9999-12-31 = open)", "SCD2"),
    "is_current": ("True for the current version", "Exactly one current version per key"),
    "error_reason": ("Why the row failed validation (semicolon separated)", "Quarantine only"),
    "validation_timestamp": ("When validation ran", "Quarantine only"),
}

DESCRIPTIONS = {
    "lease_term_months": ("Lease length in months", "Derived from start/end dates"),
    "derived_payment_status": ("Status computed from amounts and dates", "Paid / Paid Late / Partial / Unpaid"),
    "days_late": ("Days between due date and payment date", "0 if paid early"),
    "outstanding_amount": ("amount_due - amount_paid (min 0)", ""),
    "days_to_complete": ("Days from request to completion", ""),
    "previous_value": ("Previous valuation of the property", ""),
    "valuation_id": ("Valuation that defines this version", ""),
    "total_units": ("Units in the property", ""),
    "occupied_units": ("Units with an active lease at the as-of date", "Derived from leases"),
    "vacant_units": ("total_units - occupied_units", ""),
    "occupancy_rate": ("occupied_units / total_units * 100", "0-100"),
    "as_of_date": ("Snapshot date (config reporting.as_of_date)", ""),
    "reporting_month": ("Month used for billed vs collected", ""),
    "tenant_name": ("First + last name (fake)", ""),
    "rent_to_market_pct": ("monthly_rent / market_rent * 100", ""),
    "rent_per_sqft": ("monthly_rent / square_feet", ""),
    "days_to_expiry": ("lease_end_date - as_of_date in days", ""),
    "expiry_bucket": ("Days-to-expiry band", "0-30, 31-60, 61-90, 91-180, 181-365, > 365 days"),
    "expiry_month": ("Month the lease ends (yyyy-MM)", ""),
    "expires_within_30_days": ("Lease ends within 30 days of as-of", ""),
    "expires_within_90_days": ("Lease ends within 90 days of as-of", ""),
    "outstanding_balance": ("Sum of open charges due on or before as-of", ""),
    "open_charges": ("Number of open charges", ""),
    "oldest_open_due_date": ("Due date of the oldest open charge", ""),
    "days_past_due": ("as_of_date - oldest_open_due_date", ""),
    "aging_bucket": ("Aging band of the oldest open charge", "0-30, 31-60, 61-90, 90+"),
    "is_delinquent": ("Balance >= min balance and > N days past due", "config data_quality.delinquency"),
    "category": ("Work category", ""),
    "work_orders": ("Work orders in the trailing 12 months", "Cancelled excluded"),
    "total_cost": ("Cost in the trailing 12 months", "Cancelled excluded"),
    "avg_cost": ("Average cost per work order", ""),
    "open_work_orders": ("Open or in-progress work orders", ""),
    "avg_days_to_complete": ("Average completion time in days", ""),
    "month": ("Due month (yyyy-MM)", ""),
    "billed_rent": ("Sum of amount_due for charges due in the month", ""),
    "collected_rent": ("Sum of amount_paid against those charges", ""),
    "outstanding_rent": ("billed_rent - collected_rent", ""),
    "collection_rate": ("collected_rent / billed_rent * 100", "0-100"),
    "scheduled_monthly_rent": ("Sum of monthly_rent of active leases", ""),
    "ttm_collected_rent": ("Collected rent, trailing 12 months", "Revenue by property"),
    "ttm_maintenance_cost": ("Maintenance cost, trailing 12 months", "Cancelled excluded"),
    "current_value": ("Valuation effective at as-of", ""),
    "value_12m_ago": ("Valuation effective 12 months before as-of", "Null if none"),
    "comparison_value": ("value_12m_ago, else earliest valuation", ""),
    "value_growth_pct": ("(current - comparison) / comparison * 100", ""),
    "avg_rent_per_occupied_unit": ("scheduled_monthly_rent / occupied_units", ""),
    "net_rent_after_maintenance": ("ttm_collected_rent - ttm_maintenance_cost", "Not NOI"),
    "estimated_noi": ("ttm_collected_rent * (1 - opex ratio) - ttm_maintenance_cost",
                      "Assumption-based (config assumptions.other_operating_expense_ratio)"),
    "estimated_noi_yield_pct": ("estimated_noi / current_value * 100", "Assumption-based"),
    "total_properties": ("Properties in the portfolio", ""),
    "portfolio_value": ("Sum of current_value", ""),
    "portfolio_value_12m_ago": ("Sum of comparison_value", ""),
    "portfolio_value_growth_pct": ("Portfolio value growth over 12 months", ""),
    "leases_expiring_30_days": ("Active leases ending within 30 days", ""),
    "leases_expiring_90_days": ("Active leases ending within 90 days", ""),
    "delinquent_tenants": ("Distinct delinquent tenants", ""),
    "delinquent_balance": ("Open balance of delinquent tenants", ""),
    "total_open_balance": ("All open balances (AR) at as-of", ""),
}


def column_info(entity_col_map: dict, name: str) -> tuple[str, bool | None, str]:
    if name in entity_col_map:
        c = entity_col_map[name]
        return c.description, c.nullable, c.rule
    if name in LINEAGE:
        return LINEAGE[name][0], None, LINEAGE[name][1]
    if name in DESCRIPTIONS:
        return DESCRIPTIONS[name][0], None, DESCRIPTIONS[name][1]
    return "", None, ""


def table_md(lh, layer: str, table: str, description: str, entity=None) -> list[str]:
    if not lh.exists(layer, table):
        return []
    df = lh.read(layer, table)
    first = df.first()
    col_map = {c.name: c for c in entity.columns} if entity else {}
    all_cols = {c.name: c for e in ENTITIES.values() for c in e.columns}
    lines = [f"### `{layer}.{table}`", "", description, "",
             "| Column | Type | Nullable | Description | Example | Business rule |",
             "|---|---|---|---|---|---|"]
    for field in df.schema.fields:
        desc, nullable, rule = column_info(col_map or all_cols, field.name)
        nullable = field.nullable if nullable is None else nullable
        example = "" if first is None or first[field.name] is None else str(first[field.name])
        example = (example[:40] + "…") if len(example) > 40 else example
        lines.append(f"| `{field.name}` | {field.dataType.simpleString()} | {'yes' if nullable else 'no'} | "
                     f"{desc} | {example.replace('|', '/')} | {rule} |")
    return lines + [""]


def main() -> None:
    lh, _ = build_lakehouse()
    out = ["# Data dictionary", "",
           "Generated by `scripts/generate_data_dictionary.py` from the schema registry",
           "(`src/common/schemas.py`) and the local Delta tables. Examples are synthetic.", "",
           "Production naming: `real_estate.<layer>.<table>` (e.g. `real_estate.silver.leases`).", "",
           "## Bronze", "",
           f"Every Bronze table has all source columns as `string` plus {', '.join(f'`{m}`' for m in BRONZE_METADATA)}.",
           "Example:", ""]
    out += table_md(lh, "bronze", "leases", "Raw leases, source values preserved.", ENTITIES["leases"])
    out += ["## Silver", ""]
    for name, entity in ENTITIES.items():
        out += table_md(lh, "silver", name, entity.description + f" Primary key: `{entity.primary_key}`.", entity)
    out += table_md(lh, "silver", "property_value_history",
                    "SCD2-style value history per property derived from valuations.")
    out += ["## Quarantine", "", "Each `quarantine.<entity>` table has the entity's source columns (original strings) "
            "plus `error_reason`, `validation_timestamp`, `batch_id`, `source_file`, `ingestion_timestamp`.", ""]
    out += table_md(lh, "quarantine", "leases", "Rejected lease rows.", ENTITIES["leases"])
    out += table_md(lh, "quarantine", "bronze_rejects", "Malformed CSV rows that could not be parsed.")
    out += ["## Gold", ""]
    for t in GOLD_TABLES:
        out += table_md(lh, "gold", t, f"Business table `{t}` (full refresh from Silver).")
    out += ["## Ops", ""]
    out += table_md(lh, "ops", "pipeline_runs", "One row per pipeline step run (logging).")
    out += table_md(lh, "ops", "dq_results", "Results of the Silver and Gold data-quality suites.")
    (ROOT / "docs" / "data_dictionary.md").write_text("\n".join(out), encoding="utf-8")
    print("wrote docs/data_dictionary.md")


if __name__ == "__main__":
    main()
