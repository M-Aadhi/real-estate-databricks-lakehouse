"""Post-load data quality checks on Silver and Gold tables.

Unlike row-level validation (which quarantines bad input rows), these checks assert that
the published tables are trustworthy: unique keys, required values present, valid
relationships, KPI ranges. Results are written to ops.dq_results and a failing critical
check stops the pipeline. The same functions are reused by tests/data_quality.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class CheckResult:
    table: str
    check: str
    failed_rows: int
    critical: bool = True

    @property
    def passed(self) -> bool:
        return self.failed_rows == 0


def check_unique(df: DataFrame, table: str, cols: list[str]) -> CheckResult:
    dupes = df.groupBy(*cols).count().filter("count > 1").count()
    return CheckResult(table, f"unique({', '.join(cols)})", dupes)


def check_not_null(df: DataFrame, table: str, cols: list[str]) -> CheckResult:
    cond = None
    for c in cols:
        cond = F.col(c).isNull() if cond is None else cond | F.col(c).isNull()
    return CheckResult(table, f"not_null({', '.join(cols)})", df.filter(cond).count())


def check_fk(child: DataFrame, table: str, col: str, parent: DataFrame, parent_col: str,
             optional: bool = False) -> CheckResult:
    c = child.filter(F.col(col).isNotNull()) if optional else child
    orphans = c.join(parent.select(F.col(parent_col).alias(col)).distinct(), col, "left_anti").count()
    return CheckResult(table, f"fk({col})", orphans)


def check_condition(df: DataFrame, table: str, name: str, fail_condition: str, critical: bool = True) -> CheckResult:
    """`fail_condition` is a SQL expression that is TRUE for bad rows."""
    return CheckResult(table, name, df.filter(F.expr(fail_condition)).count(), critical)


def silver_checks(lh) -> list[CheckResult]:
    r = lambda t: lh.read("silver", t)  # noqa: E731
    tenants_current = r("tenants").filter("is_current")
    results = [
        check_unique(r("properties"), "silver.properties", ["property_id"]),
        check_unique(r("units"), "silver.units", ["unit_id"]),
        check_unique(tenants_current, "silver.tenants(current)", ["tenant_id"]),
        check_unique(r("leases"), "silver.leases", ["lease_id"]),
        check_unique(r("payments"), "silver.payments", ["payment_id"]),
        check_unique(r("valuations"), "silver.valuations", ["valuation_id"]),
        check_unique(r("maintenance"), "silver.maintenance", ["maintenance_id"]),
        check_not_null(r("leases"), "silver.leases", ["lease_id", "tenant_id", "unit_id", "lease_start_date",
                                                      "lease_end_date", "monthly_rent"]),
        check_not_null(r("payments"), "silver.payments", ["payment_id", "lease_id", "payment_due_date",
                                                          "amount_due", "amount_paid"]),
        check_fk(r("units"), "silver.units", "property_id", r("properties"), "property_id"),
        check_fk(r("leases"), "silver.leases", "unit_id", r("units"), "unit_id"),
        check_fk(r("leases"), "silver.leases", "tenant_id", r("tenants"), "tenant_id"),
        check_fk(r("payments"), "silver.payments", "lease_id", r("leases"), "lease_id"),
        check_fk(r("maintenance"), "silver.maintenance", "unit_id", r("units"), "unit_id", optional=True),
        check_condition(r("leases"), "silver.leases", "lease_start<=lease_end", "lease_start_date > lease_end_date"),
        check_condition(r("leases"), "silver.leases", "monthly_rent>=0", "monthly_rent < 0"),
        check_condition(r("payments"), "silver.payments", "amount_paid>=0", "amount_paid < 0"),
        check_condition(r("valuations"), "silver.valuations", "property_value>0", "property_value <= 0"),
        check_condition(r("tenants"), "silver.tenants", "one_current_version_rule",
                        "effective_to < effective_from"),
    ]
    return results


def gold_checks(lh) -> list[CheckResult]:
    perf = lh.read("gold", "property_performance")
    summary = lh.read("gold", "portfolio_summary")
    return [
        check_unique(perf, "gold.property_performance", ["property_id"]),
        check_condition(perf, "gold.property_performance", "occupancy_rate_0_100",
                        "occupancy_rate < 0 OR occupancy_rate > 100"),
        check_condition(perf, "gold.property_performance", "collection_rate_0_100",
                        "collection_rate < 0 OR collection_rate > 100"),
        check_condition(perf, "gold.property_performance", "occupied<=total", "occupied_units > total_units"),
        CheckResult("gold.portfolio_summary", "row_count==1", abs(summary.count() - 1)),
        check_unique(lh.read("gold", "rent_roll"), "gold.rent_roll", ["unit_id"]),
    ]


def record_results(lh, results: list[CheckResult], stage: str, batch_id: str | None = None) -> None:
    rows = [{**asdict(r), "passed": r.passed, "stage": stage, "batch_id": batch_id} for r in results]
    df = lh.spark.createDataFrame(
        rows, "table string, check string, failed_rows long, critical boolean, passed boolean, stage string, "
              "batch_id string").withColumn("checked_at", F.current_timestamp())
    lh.append(df, "ops", "dq_results")
