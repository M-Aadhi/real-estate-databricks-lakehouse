"""Data-quality tests on the published Silver and Gold tables (fixture lakehouse)."""
import pytest

from src.common.data_quality import gold_checks, silver_checks


@pytest.fixture(scope="module")
def lh(fixture_lakehouse):
    return fixture_lakehouse[0]


@pytest.mark.parametrize("table,key", [("properties", "property_id"), ("units", "unit_id"),
                                       ("leases", "lease_id"), ("payments", "payment_id"),
                                       ("valuations", "valuation_id"), ("maintenance", "maintenance_id")])
def test_primary_keys_unique(lh, table, key):
    df = lh.read("silver", table)
    assert df.count() == df.select(key).distinct().count()


def test_tenant_has_one_current_version(lh):
    t = lh.read("silver", "tenants").filter("is_current")
    assert t.count() == t.select("tenant_id").distinct().count()


@pytest.mark.parametrize("table,cols", [
    ("properties", ["property_id", "property_type", "acquisition_price"]),
    ("units", ["unit_id", "property_id", "square_feet"]),
    ("leases", ["lease_id", "tenant_id", "unit_id", "lease_start_date", "lease_end_date", "monthly_rent"]),
    ("payments", ["payment_id", "lease_id", "payment_due_date", "amount_due", "amount_paid"]),
])
def test_required_columns_not_null(lh, table, cols):
    df = lh.read("silver", table)
    for c in cols:
        assert df.filter(f"{c} IS NULL").count() == 0, f"{table}.{c} has nulls"


@pytest.mark.parametrize("child,col,parent,pk", [
    ("units", "property_id", "properties", "property_id"),
    ("leases", "unit_id", "units", "unit_id"),
    ("leases", "tenant_id", "tenants", "tenant_id"),
    ("leases", "property_id", "properties", "property_id"),
    ("payments", "lease_id", "leases", "lease_id"),
])
def test_relationships_valid(lh, child, col, parent, pk):
    orphans = lh.read("silver", child).join(lh.read("silver", parent).select(pk).distinct()
                                            .withColumnRenamed(pk, col), col, "left_anti")
    assert orphans.count() == 0


@pytest.mark.parametrize("table,bad_rows", [
    ("leases", "lease_start_date > lease_end_date"),
    ("leases", "monthly_rent < 0"),
    ("payments", "amount_paid < 0 OR amount_due < 0"),
    ("valuations", "property_value <= 0"),
])
def test_business_rules_hold(lh, table, bad_rows):
    assert lh.read("silver", table).filter(bad_rows).count() == 0


@pytest.mark.parametrize("col", ["occupancy_rate", "collection_rate"])
def test_rates_between_0_and_100(lh, col):
    perf = lh.read("gold", "property_performance")
    assert perf.filter(f"{col} < 0 OR {col} > 100").count() == 0
    s = lh.read("gold", "portfolio_summary").first()
    assert 0 <= s[col] <= 100


def test_pipeline_dq_suites_pass(lh):
    failed = [r for r in silver_checks(lh) + gold_checks(lh) if not r.passed]
    assert failed == []
