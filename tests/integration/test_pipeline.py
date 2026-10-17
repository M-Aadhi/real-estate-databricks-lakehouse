"""End-to-end tests: Bronze -> Silver -> Gold on the hand-checked fixtures.

Expected values below are worked out by hand from tests/fixtures (see the comments).
"""
from datetime import date

import pytest


def counts(steps, step):
    return steps[step]["counts"]


def summary(steps, step):
    return steps[step]["summary"]


def test_bronze_preserves_all_rows_and_rejects_malformed(fixture_lakehouse):
    _, _, steps = fixture_lakehouse
    c = counts(steps, "batch_001")
    assert c["bronze.properties"] == 3      # duplicate kept in Bronze (raw history)
    assert c["bronze.payments"] == 9        # 10 data lines - 1 malformed
    assert c["quarantine.bronze_rejects"] == 1


def test_bronze_keeps_source_values_and_metadata(fixture_lakehouse):
    lh, _, _ = fixture_lakehouse
    row = lh.read("bronze", "properties").filter("property_id = 'P1'").first()
    assert row.state == "tx"                                   # not standardised in Bronze
    assert row.batch_id == "batch_001" and row.source_file.endswith("properties.csv")
    assert row.ingestion_timestamp is not None


def test_valid_rows_reach_silver_invalid_rows_quarantined(fixture_lakehouse):
    lh, _, steps = fixture_lakehouse
    c = counts(steps, "batch_001")
    assert (c["silver.properties"], c["silver.units"], c["silver.tenants"], c["silver.leases"]) == (2, 4, 3, 3)
    assert (c["silver.payments"], c["silver.valuations"], c["silver.maintenance"]) == (6, 4, 5)
    assert (c["quarantine.units"], c["quarantine.tenants"], c["quarantine.leases"]) == (1, 1, 3)
    assert (c["quarantine.payments"], c["quarantine.valuations"], c["quarantine.maintenance"]) == (2, 1, 1)

    reasons = {r.lease_id: r.error_reason for r in lh.read("quarantine", "leases").collect()}
    assert reasons == {"L4": "lease_end_before_start", "L5": "unknown_tenant_id", "L6": "unit_property_mismatch"}
    pay = {r.payment_id: r.error_reason for r in lh.read("quarantine", "payments").collect()}
    assert pay["PMT7"] == "unknown_lease_id" and pay["PMT8"] == "negative_amount_paid"
    q = lh.read("quarantine", "payments").filter("payment_id = 'PMT8'").first()
    assert q.amount_paid == "-5.00" and q.validation_timestamp is not None   # original value, not "fixed"


def test_silver_standardises_and_derives(fixture_lakehouse):
    lh, _, _ = fixture_lakehouse
    assert lh.read("silver", "properties").filter("property_id = 'P1'").first().state == "TX"
    t1 = lh.read("silver", "tenants").filter("tenant_id = 'T1'").first()
    assert t1.email == "ana@example.com" and t1.tenant_status == "Active"
    pays = {r.payment_id: r for r in lh.read("silver", "payments").collect()}
    assert pays["PMT3"].derived_payment_status == "Paid Late" and pays["PMT5"].derived_payment_status == "Partial"
    assert pays["PMT2"].payment_method == "ACH"


def test_duplicates_do_not_multiply(fixture_lakehouse):
    lh, _, _ = fixture_lakehouse
    for table, key in [("properties", "property_id"), ("leases", "lease_id"), ("payments", "payment_id")]:
        df = lh.read("silver", table)
        assert df.count() == df.select(key).distinct().count()


def test_reruns_are_idempotent(fixture_lakehouse):
    _, _, steps = fixture_lakehouse
    assert steps["batch_001_rerun"] == steps["batch_001"]
    assert steps["batch_002_rerun"] == steps["batch_002"]


def test_gold_kpis_after_initial_load(fixture_lakehouse):
    _, _, steps = fixture_lakehouse
    s = summary(steps, "batch_001")
    assert (s["total_properties"], s["total_units"], s["occupied_units"], s["vacant_units"]) == (2, 4, 3, 1)
    assert s["occupancy_rate"] == 75.0
    assert float(s["scheduled_monthly_rent"]) == 7500          # L1 1000 + L2 1500 + L3 5000
    assert float(s["billed_rent"]) == 7500 and float(s["collected_rent"]) == 6000   # PMT4 unpaid
    assert float(s["outstanding_rent"]) == 1500 and s["collection_rate"] == 80.0
    assert float(s["portfolio_value"]) == 3_000_000 and s["portfolio_value_growth_pct"] == 0.0
    assert float(s["ttm_collected_rent"]) == 10_500             # P1 3,500 + P2 7,000
    assert float(s["ttm_maintenance_cost"]) == 1_050            # M1 500 + M2 300 + M4 250 (M3 old, M5 cancelled)
    assert float(s["net_rent_after_maintenance"]) == 9_450
    assert float(s["estimated_noi"]) == 6_300                   # 10,500 * 0.7 - 1,050
    assert (s["leases_expiring_30_days"], s["leases_expiring_90_days"]) == (1, 1)   # L2 ends 2026-10-20
    assert s["delinquent_tenants"] == 1                         # T3: 3,000 open for 60 days
    assert float(s["delinquent_balance"]) == 3000 and float(s["total_open_balance"]) == 4500


def test_property_level_kpis(fixture_lakehouse):
    lh, _, _ = fixture_lakehouse
    perf = {r.property_id: r for r in lh.read("gold", "property_performance").collect()}
    # state after batch_002
    assert perf["P1"].occupancy_rate == 100.0 and perf["P2"].occupancy_rate == 100.0
    assert perf["P1"].value_growth_pct == 10.0 and perf["P2"].value_growth_pct == -5.0
    assert float(perf["P2"].ttm_maintenance_cost) == 250


def test_incremental_load_updates_and_inserts(fixture_lakehouse):
    lh, _, steps = fixture_lakehouse
    c = counts(steps, "batch_002")
    assert c["silver.leases"] == 4 and c["silver.payments"] == 7 and c["quarantine.payments"] == 3
    l2 = lh.read("silver", "leases").filter("lease_id = 'L2'").first()
    assert l2.lease_end_date == date(2027, 10, 20) and float(l2.monthly_rent) == 1545 and l2.batch_id == "batch_002"
    l3 = lh.read("silver", "leases").filter("lease_id = 'L3'").first()
    assert l3.batch_id == "batch_001"                           # unchanged re-send did not update the row
    pmt4 = lh.read("silver", "payments").filter("payment_id = 'PMT4'").first()
    assert float(pmt4.amount_paid) == 1500 and pmt4.derived_payment_status == "Paid Late"

    s = summary(steps, "batch_002")
    assert s["occupied_units"] == 4 and s["occupancy_rate"] == 100.0       # new lease L7 on U3
    assert float(s["scheduled_monthly_rent"]) == 8645                      # 1000 + 1545 + 5000 + 1100
    assert float(s["collected_rent"]) == 8600 and s["collection_rate"] == 100.0
    assert (s["leases_expiring_30_days"], s["leases_expiring_90_days"]) == (0, 0)   # L2 renewed
    assert s["delinquent_tenants"] == 1 and float(s["total_open_balance"]) == 3000


def test_scd2_tenant_history(fixture_lakehouse):
    lh, _, steps = fixture_lakehouse
    assert counts(steps, "batch_002")["silver.tenants"] == 5      # T1, T2(old), T2(new), T3, T5
    t2 = lh.read("silver", "tenants").filter("tenant_id = 'T2'").orderBy("effective_from").collect()
    assert [(r.email, r.is_current) for r in t2] == [("ben@example.com", False), ("ben.new@example.com", True)]
    assert t2[0].effective_to == date(2026, 9, 30) and t2[1].effective_from == date(2026, 10, 1)
    assert lh.read("silver", "tenants").filter("tenant_id = 'T1'").count() == 1   # unchanged re-send


def test_run_log_records_every_step(fixture_lakehouse):
    lh, _, _ = fixture_lakehouse
    runs = lh.read("ops", "pipeline_runs")
    assert runs.filter("status != 'SUCCESS'").count() == 0
    row = runs.filter("pipeline = 'silver.payments' AND batch_id = 'batch_001'").first()
    assert (row.records_read, row.records_written, row.records_rejected) == (9, 6, 2)


def test_quarantine_threshold_stops_load(make_lakehouse):
    from src.common.exceptions import DataQualityThresholdError
    from src.pipeline import run_bronze, run_silver

    lh, cfg = make_lakehouse("strict", extra={"data_quality": {"max_quarantine_pct": 5.0,
                                                               "delinquency": {}}})
    run_bronze(lh, cfg, "batch_001")
    run_silver_props = __import__("src.silver.transform_properties", fromlist=["run"])
    run_silver_props.run(lh, "batch_001", cfg)
    with pytest.raises(DataQualityThresholdError):
        run_silver(lh, cfg, "batch_001", check=False)   # units: 1 of 5 rows invalid = 20% > 5%
    assert not lh.exists("silver", "units")             # Silver not written when the batch is rejected
    assert lh.read("quarantine", "units").count() == 1  # but the bad rows are kept for review
