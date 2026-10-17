"""Unit tests for the Gold KPI logic on tiny in-memory DataFrames."""
from datetime import date

import pytest

from src.gold import metrics
from tests.helpers import make_df

AS_OF = date(2026, 9, 30)


def leases_df(spark, rows):
    return make_df(spark, rows, "lease_id string, tenant_id string, unit_id string, property_id string, "
                                       "lease_start_date date, lease_end_date date, monthly_rent decimal(14,2), "
                                       "security_deposit decimal(14,2), lease_status string")


def payments_df(spark, rows):
    return make_df(spark, rows, "payment_id string, lease_id string, tenant_id string, property_id string, "
                                       "payment_date date, payment_due_date date, amount_due decimal(14,2), "
                                       "amount_paid decimal(14,2)")


@pytest.fixture
def props(spark):
    return make_df(spark, [("P1", "Alpha", "Multifamily", "Houston", "TX", "Active"),
                                  ("P2", "Beta", "Office", "Dallas", "TX", "Active"),
                                  ("P3", "Gamma", "Retail", "Austin", "TX", "Active")],
                                 "property_id string, property_name string, property_type string, city string, "
                                 "state string, status string")


@pytest.fixture
def units(spark):
    return make_df(spark, [("U1", "P1"), ("U2", "P1"), ("U3", "P1"), ("U4", "P2")],
                                 "unit_id string, property_id string")


def test_active_lease_rules(spark):
    d = date
    leases = leases_df(spark, [
        ("A", "T", "U1", "P1", d(2026, 1, 1), d(2026, 12, 31), 1, 0, "Active"),       # active
        ("B", "T", "U2", "P1", d(2026, 9, 30), d(2027, 9, 29), 1, 0, "Active"),       # starts on as-of -> active
        ("C", "T", "U3", "P1", d(2025, 1, 1), d(2026, 9, 30), 1, 0, "Active"),        # ends on as-of -> active
        ("D", "T", "U3", "P1", d(2025, 1, 1), d(2026, 9, 29), 1, 0, "Active"),        # ended yesterday
        ("E", "T", "U4", "P2", d(2026, 1, 1), d(2026, 12, 31), 1, 0, "Terminated"),   # terminated
        ("F", "T", "U4", "P2", d(2026, 10, 1), d(2027, 9, 30), 1, 0, "Future"),       # not started
    ])
    assert sorted(r.lease_id for r in metrics.active_leases(leases, AS_OF).collect()) == ["A", "B", "C"]


def test_occupancy_calculation(spark, props, units):
    d = date
    leases = leases_df(spark, [
        ("L1", "T1", "U1", "P1", d(2026, 1, 1), d(2026, 12, 31), 1000, 0, "Active"),
        ("L2", "T2", "U2", "P1", d(2025, 1, 1), d(2025, 12, 31), 1000, 0, "Expired"),   # past lease
        ("L3", "T3", "U4", "P2", d(2026, 1, 1), d(2026, 12, 31), 5000, 0, "Active"),
        ("L4", "T4", "U1", "P1", d(2026, 6, 1), d(2027, 5, 31), 1000, 0, "Active"),     # 2nd lease same unit
    ])
    occ = {r.property_id: r for r in metrics.occupancy_by_property(props, units, leases, AS_OF).collect()}
    assert (occ["P1"].total_units, occ["P1"].occupied_units, occ["P1"].vacant_units) == (3, 1, 2)
    assert occ["P1"].occupancy_rate == pytest.approx(33.33)
    assert occ["P2"].occupancy_rate == 100.0
    assert occ["P3"].total_units == 0 and occ["P3"].occupancy_rate is None   # no units -> no divide by zero


def test_collection_rate(spark):
    d = date
    pays = payments_df(spark, [
        ("1", "L1", "T1", "P1", d(2026, 9, 2), d(2026, 9, 1), 1000, 1000),
        ("2", "L2", "T2", "P1", None, d(2026, 9, 1), 1000, 0),
        ("3", "L3", "T3", "P1", d(2026, 8, 2), d(2026, 8, 1), 1000, 1000),    # other month, ignored
        ("4", "L4", "T4", "P2", d(2026, 9, 3), d(2026, 9, 1), 0, 0),          # zero billed
    ])
    rows = {r.property_id: r for r in metrics.collections_by_property(pays, "2026-09").collect()}
    assert float(rows["P1"].billed_rent) == 2000 and float(rows["P1"].collected_rent) == 1000
    assert float(rows["P1"].outstanding_rent) == 1000
    assert rows["P1"].collection_rate == 50.0
    assert rows["P2"].collection_rate is None


def test_rent_roll_calculation(spark, props):
    d = date
    leases = leases_df(spark, [("L1", "T1", "U1", "P1", d(2026, 1, 1), d(2026, 12, 31), 900, 900, "Active"),
                               ("L2", "T2", "U2", "P1", d(2024, 1, 1), d(2024, 12, 31), 800, 800, "Expired")])
    units = make_df(spark, [("U1", "101", 600, 1000.0), ("U2", "102", 800, 1200.0)],
                                  "unit_id string, unit_number string, square_feet int, market_rent decimal(14,2)")
    tenants = make_df(spark, [("T1", "Ana", "Smith", True), ("T2", "Ben", "Jones", True)],
                                    "tenant_id string, first_name string, last_name string, is_current boolean")
    rows = metrics.rent_roll(leases, units, tenants, props, AS_OF).collect()
    assert len(rows) == 1
    r = rows[0]
    assert r.tenant_name == "Ana Smith" and r.unit_number == "101"
    assert r.rent_to_market_pct == 90.0 and r.rent_per_sqft == 1.5 and r.days_to_expiry == 92


def test_lease_expiry_logic(spark, props):
    d = date
    leases = leases_df(spark, [
        ("A", "T", "U1", "P1", d(2026, 1, 1), d(2026, 10, 30), 1, 0, "Active"),   # 30 days
        ("B", "T", "U2", "P1", d(2026, 1, 1), d(2026, 10, 31), 1, 0, "Active"),   # 31 days
        ("C", "T", "U3", "P1", d(2026, 1, 1), d(2026, 12, 29), 1, 0, "Active"),   # 90 days
        ("D", "T", "U4", "P2", d(2026, 1, 1), d(2027, 6, 30), 1, 0, "Active"),    # 273 days
    ])
    rows = {r.lease_id: r for r in metrics.lease_expiry(leases, props, AS_OF).collect()}
    assert rows["A"].expires_within_30_days and rows["A"].expiry_bucket == "0-30 days"
    assert not rows["B"].expires_within_30_days and rows["B"].expires_within_90_days
    assert rows["C"].expires_within_90_days and rows["C"].expiry_bucket == "61-90 days"
    assert not rows["D"].expires_within_90_days and rows["D"].expiry_bucket == "181-365 days"


def test_tenant_delinquency(spark, props):
    d = date
    pays = payments_df(spark, [
        ("1", "L1", "T1", "P1", None, d(2026, 8, 1), 1000, 0),        # 60 days open
        ("2", "L1", "T1", "P1", None, d(2026, 9, 1), 1000, 0),
        ("3", "L2", "T2", "P1", None, d(2026, 9, 1), 1000, 0),        # 29 days -> not yet delinquent
        ("4", "L3", "T3", "P2", d(2026, 7, 5), d(2026, 7, 1), 1000, 1000),   # fully paid
        ("5", "L4", "T4", "P2", None, d(2026, 10, 1), 1000, 0),       # not due yet
    ])
    tenants = make_df(spark, [(t, t, "X", True) for t in ["T1", "T2", "T3", "T4"]],
                                    "tenant_id string, first_name string, last_name string, is_current boolean")
    rows = {r.tenant_id: r for r in metrics.tenant_delinquency(pays, tenants, props, AS_OF, 1.0, 30).collect()}
    assert set(rows) == {"T1", "T2"}
    assert float(rows["T1"].outstanding_balance) == 2000 and rows["T1"].days_past_due == 60
    assert rows["T1"].is_delinquent and rows["T1"].aging_bucket == "31-60"
    assert not rows["T2"].is_delinquent


def test_property_value_growth(spark):
    d = date
    from src.silver.transform_valuations import build_value_history
    vals = make_df(spark, [("V1", "P1", d(2025, 6, 30), 1000.0, "m"), ("V2", "P1", d(2026, 6, 30), 1100.0, "m"),
                                  ("V3", "P2", d(2026, 3, 31), 500.0, "m")],
                                 "valuation_id string, property_id string, valuation_date date, "
                                 "property_value decimal(14,2), valuation_method string")
    rows = {r.property_id: r for r in metrics.property_values(build_value_history(vals), AS_OF).collect()}
    assert float(rows["P1"].current_value) == 1100 and rows["P1"].value_growth_pct == 10.0
    assert rows["P2"].value_12m_ago is None and rows["P2"].value_growth_pct == 0.0   # falls back to earliest


def test_month_bounds():
    assert metrics.month_bounds("2026-02") == (date(2026, 2, 1), date(2026, 2, 28))
    assert metrics.month_bounds("2024-02") == (date(2024, 2, 1), date(2024, 2, 29))
