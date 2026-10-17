"""Business metric logic for the Gold layer, as pure DataFrame functions.

Every function takes Silver DataFrames plus explicit dates, so it can be unit-tested with
tiny in-memory data. Definitions (also in docs/architecture.md):

- Active lease at D:      lease_start_date <= D <= lease_end_date and status != 'Terminated'
- Occupied unit:          a unit with an active lease at the as-of date (derived from leases,
                          not from the source unit_status flag)
- Occupancy rate:         occupied_units / total_units * 100
- Scheduled rent:         sum of monthly_rent of active leases at the as-of date
- Billed / collected:     amount_due / amount_paid of charges due in the reporting month
- Outstanding (month):    billed - collected for the reporting month
- Collection rate:        collected / billed * 100
- Open balance (AR):      sum of (amount_due - amount_paid) for charges due on or before as-of
- Delinquent tenant:      open balance >= min balance AND oldest open charge > N days past due
- Value growth:           value at as-of vs value 12 months earlier (earliest if none older)
- Net rent after maint.:  TTM collected rent - TTM maintenance cost
- Estimated NOI:          TTM collected rent * (1 - other_opex_ratio) - TTM maintenance cost
                          (assumption-based, NOT an audited NOI)
"""
from __future__ import annotations

from datetime import date

from dateutil.relativedelta import relativedelta
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def safe_pct(numerator: Column, denominator: Column) -> Column:
    """numerator / denominator * 100, rounded to 2 dp; null when the denominator is 0."""
    return F.when(denominator > 0, F.round(numerator / denominator * 100, 2).cast("double"))


def month_bounds(reporting_month: str) -> tuple[date, date]:
    start = date.fromisoformat(f"{reporting_month}-01")
    return start, start + relativedelta(months=1) - relativedelta(days=1)


def active_leases(leases: DataFrame, as_of: date) -> DataFrame:
    d = F.lit(as_of)
    return leases.filter((F.col("lease_start_date") <= d) & (F.col("lease_end_date") >= d)
                         & (F.col("lease_status") != "Terminated"))


def current_tenants(tenants: DataFrame) -> DataFrame:
    return tenants.filter("is_current") if "is_current" in tenants.columns else tenants


# ---------------- occupancy ----------------
def occupancy_by_property(properties: DataFrame, units: DataFrame, leases: DataFrame, as_of: date) -> DataFrame:
    occupied = active_leases(leases, as_of).select("unit_id").distinct().withColumn("_occ", F.lit(1))
    per_unit = units.select("unit_id", "property_id").join(occupied, "unit_id", "left")
    agg = per_unit.groupBy("property_id").agg(
        F.count("unit_id").alias("total_units"),
        F.coalesce(F.sum("_occ"), F.lit(0)).cast("long").alias("occupied_units"))
    return (properties.select("property_id", "property_name", "property_type", "city", "state", "status")
            .join(agg, "property_id", "left")
            .fillna(0, ["total_units", "occupied_units"])
            .withColumn("vacant_units", F.col("total_units") - F.col("occupied_units"))
            .withColumn("occupancy_rate", safe_pct(F.col("occupied_units"), F.col("total_units")))
            .withColumn("as_of_date", F.lit(as_of)))


# ---------------- rent roll ----------------
def rent_roll(leases: DataFrame, units: DataFrame, tenants: DataFrame, properties: DataFrame, as_of: date) -> DataFrame:
    t = current_tenants(tenants).select("tenant_id", F.concat_ws(" ", "first_name", "last_name").alias("tenant_name"))
    u = units.select("unit_id", "unit_number", "square_feet", "market_rent")
    p = properties.select("property_id", "property_name", "property_type")
    return (active_leases(leases, as_of)
            .select("lease_id", "property_id", "unit_id", "tenant_id", "lease_start_date", "lease_end_date",
                    "monthly_rent", "security_deposit", "lease_status")
            .join(p, "property_id", "left").join(u, "unit_id", "left").join(t, "tenant_id", "left")
            .withColumn("rent_to_market_pct", safe_pct(F.col("monthly_rent"), F.col("market_rent")))
            .withColumn("rent_per_sqft", F.round(F.col("monthly_rent") / F.col("square_feet"), 2).cast("double"))
            .withColumn("days_to_expiry", F.datediff("lease_end_date", F.lit(as_of)))
            .withColumn("as_of_date", F.lit(as_of)))


# ---------------- lease expiry ----------------
def expiry_bucket(days: Column) -> Column:
    return (F.when(days <= 30, "0-30 days").when(days <= 60, "31-60 days").when(days <= 90, "61-90 days")
             .when(days <= 180, "91-180 days").when(days <= 365, "181-365 days").otherwise("> 365 days"))


def lease_expiry(leases: DataFrame, properties: DataFrame, as_of: date) -> DataFrame:
    days = F.datediff("lease_end_date", F.lit(as_of))
    return (active_leases(leases, as_of)
            .select("lease_id", "property_id", "unit_id", "tenant_id", "lease_end_date", "monthly_rent")
            .join(properties.select("property_id", "property_name"), "property_id", "left")
            .withColumn("days_to_expiry", days)
            .withColumn("expiry_bucket", expiry_bucket(days))
            .withColumn("expiry_month", F.date_format("lease_end_date", "yyyy-MM"))
            .withColumn("expires_within_30_days", days <= 30)
            .withColumn("expires_within_90_days", days <= 90)
            .withColumn("as_of_date", F.lit(as_of)))


# ---------------- collections ----------------
def charges_in_period(payments: DataFrame, start: date, end: date) -> DataFrame:
    return payments.filter((F.col("payment_due_date") >= F.lit(start)) & (F.col("payment_due_date") <= F.lit(end)))


def collections_by_property(payments: DataFrame, reporting_month: str) -> DataFrame:
    start, end = month_bounds(reporting_month)
    return (charges_in_period(payments, start, end).groupBy("property_id").agg(
                F.sum("amount_due").alias("billed_rent"), F.sum("amount_paid").alias("collected_rent"))
            .withColumn("outstanding_rent", F.col("billed_rent") - F.col("collected_rent"))
            .withColumn("collection_rate", safe_pct(F.col("collected_rent"), F.col("billed_rent"))))


def collection_trend(payments: DataFrame, reporting_month: str, months: int) -> DataFrame:
    end_start, end = month_bounds(reporting_month)
    start = end_start - relativedelta(months=months - 1)
    return (charges_in_period(payments, start, end)
            .withColumn("month", F.date_format("payment_due_date", "yyyy-MM"))
            .groupBy("month").agg(F.sum("amount_due").alias("billed_rent"), F.sum("amount_paid").alias("collected_rent"))
            .withColumn("outstanding_rent", F.col("billed_rent") - F.col("collected_rent"))
            .withColumn("collection_rate", safe_pct(F.col("collected_rent"), F.col("billed_rent")))
            .orderBy("month"))


# ---------------- delinquency ----------------
def tenant_delinquency(payments: DataFrame, tenants: DataFrame, properties: DataFrame, as_of: date,
                       min_balance: float, min_days: int) -> DataFrame:
    open_charges = (payments.filter(F.col("payment_due_date") <= F.lit(as_of))
                    .withColumn("open_amount", F.col("amount_due") - F.col("amount_paid"))
                    .filter(F.col("open_amount") > 0))
    t = current_tenants(tenants).select("tenant_id", F.concat_ws(" ", "first_name", "last_name").alias("tenant_name"))
    per_tenant = (open_charges.groupBy("tenant_id", "property_id").agg(
                      F.sum("open_amount").alias("outstanding_balance"),
                      F.count("*").alias("open_charges"),
                      F.min("payment_due_date").alias("oldest_open_due_date"))
                  .withColumn("days_past_due", F.datediff(F.lit(as_of), "oldest_open_due_date")))
    bucket = (F.when(F.col("days_past_due") <= 30, "0-30").when(F.col("days_past_due") <= 60, "31-60")
               .when(F.col("days_past_due") <= 90, "61-90").otherwise("90+"))
    return (per_tenant
            .withColumn("aging_bucket", bucket)
            .withColumn("is_delinquent", (F.col("outstanding_balance") >= min_balance)
                        & (F.col("days_past_due") > min_days))
            .join(t, "tenant_id", "left")
            .join(properties.select("property_id", "property_name"), "property_id", "left")
            .withColumn("as_of_date", F.lit(as_of)))


# ---------------- valuation ----------------
def value_as_of(history: DataFrame, d: date, alias: str) -> DataFrame:
    """Value effective on date d per property (from silver.property_value_history)."""
    lit = F.lit(d)
    return (history.filter((F.col("effective_from") <= lit) & (F.col("effective_to") >= lit))
            .select("property_id", F.col("property_value").alias(alias)))


def property_values(history: DataFrame, as_of: date) -> DataFrame:
    current = value_as_of(history, as_of, "current_value")
    prior = value_as_of(history, as_of - relativedelta(years=1), "value_12m_ago")
    earliest = (history.groupBy("property_id").agg(F.min_by("property_value", "effective_from").alias("earliest_value")))
    return (current.join(prior, "property_id", "left").join(earliest, "property_id", "left")
            .withColumn("comparison_value", F.coalesce("value_12m_ago", "earliest_value"))
            .withColumn("value_growth_pct", safe_pct(F.col("current_value") - F.col("comparison_value"),
                                                     F.col("comparison_value")))
            .drop("earliest_value"))


# ---------------- maintenance ----------------
def maintenance_ttm(maintenance: DataFrame, as_of: date) -> DataFrame:
    start = as_of - relativedelta(years=1) + relativedelta(days=1)
    return maintenance.filter((F.col("request_date") >= F.lit(start)) & (F.col("request_date") <= F.lit(as_of))
                              & (F.col("status") != "Cancelled"))


def maintenance_summary(maintenance: DataFrame, properties: DataFrame, as_of: date) -> DataFrame:
    return (maintenance_ttm(maintenance, as_of).groupBy("property_id", "category").agg(
                F.count("*").alias("work_orders"),
                F.sum("cost").alias("total_cost"),
                F.round(F.avg("cost"), 2).alias("avg_cost"),
                F.sum(F.when(F.col("status").isin("Open", "In Progress"), 1).otherwise(0)).alias("open_work_orders"),
                F.round(F.avg("days_to_complete"), 1).alias("avg_days_to_complete"))
            .join(properties.select("property_id", "property_name", "property_type"), "property_id", "left")
            .withColumn("as_of_date", F.lit(as_of)))


# ---------------- property performance ----------------
def property_performance(occupancy: DataFrame, leases: DataFrame, payments: DataFrame, maintenance: DataFrame,
                         value_history: DataFrame, as_of: date, reporting_month: str,
                         other_opex_ratio: float) -> DataFrame:
    scheduled = (active_leases(leases, as_of).groupBy("property_id")
                 .agg(F.sum("monthly_rent").alias("scheduled_monthly_rent")))
    month = collections_by_property(payments, reporting_month)
    _, month_end = month_bounds(reporting_month)
    ttm_start = month_end - relativedelta(years=1) + relativedelta(days=1)
    ttm_rent = (charges_in_period(payments, ttm_start, month_end).groupBy("property_id")
                .agg(F.sum("amount_paid").alias("ttm_collected_rent")))
    ttm_maint = maintenance_ttm(maintenance, as_of).groupBy("property_id").agg(F.sum("cost").alias("ttm_maintenance_cost"))
    values = property_values(value_history, as_of)

    money_cols = ["scheduled_monthly_rent", "billed_rent", "collected_rent", "outstanding_rent",
                  "ttm_collected_rent", "ttm_maintenance_cost"]
    df = (occupancy.join(scheduled, "property_id", "left").join(month, "property_id", "left")
          .join(ttm_rent, "property_id", "left").join(ttm_maint, "property_id", "left")
          .join(values, "property_id", "left"))
    for c in money_cols:
        df = df.withColumn(c, F.coalesce(F.col(c), F.lit(0)).cast("decimal(16,2)"))
    return (df
            .withColumn("avg_rent_per_occupied_unit",
                        F.when(F.col("occupied_units") > 0,
                               F.round(F.col("scheduled_monthly_rent") / F.col("occupied_units"), 2)))
            .withColumn("net_rent_after_maintenance", F.col("ttm_collected_rent") - F.col("ttm_maintenance_cost"))
            .withColumn("estimated_noi", F.round(F.col("ttm_collected_rent") * F.lit(1 - other_opex_ratio)
                                                 - F.col("ttm_maintenance_cost"), 2).cast("decimal(16,2)"))
            .withColumn("estimated_noi_yield_pct", safe_pct(F.col("estimated_noi"), F.col("current_value")))
            .withColumn("reporting_month", F.lit(reporting_month)))


# ---------------- portfolio summary ----------------
def portfolio_summary(performance: DataFrame, expiry: DataFrame, delinquency: DataFrame, payments: DataFrame,
                      as_of: date, reporting_month: str) -> DataFrame:
    p = performance.agg(
        F.count("property_id").alias("total_properties"),
        F.sum("total_units").alias("total_units"),
        F.sum("occupied_units").alias("occupied_units"),
        F.sum("vacant_units").alias("vacant_units"),
        F.sum("scheduled_monthly_rent").alias("scheduled_monthly_rent"),
        F.sum("billed_rent").alias("billed_rent"),
        F.sum("collected_rent").alias("collected_rent"),
        F.sum("outstanding_rent").alias("outstanding_rent"),
        F.sum("current_value").alias("portfolio_value"),
        F.sum("comparison_value").alias("portfolio_value_12m_ago"),
        F.sum("ttm_collected_rent").alias("ttm_collected_rent"),
        F.sum("ttm_maintenance_cost").alias("ttm_maintenance_cost"),
        F.sum("net_rent_after_maintenance").alias("net_rent_after_maintenance"),
        F.sum("estimated_noi").alias("estimated_noi"))
    e = expiry.agg(F.sum(F.col("expires_within_30_days").cast("int")).alias("leases_expiring_30_days"),
                   F.sum(F.col("expires_within_90_days").cast("int")).alias("leases_expiring_90_days"))
    d = delinquency.agg(
        F.countDistinct(F.when(F.col("is_delinquent"), F.col("tenant_id"))).alias("delinquent_tenants"),
        F.sum(F.when(F.col("is_delinquent"), F.col("outstanding_balance")).otherwise(0)).alias("delinquent_balance"),
        F.sum("outstanding_balance").alias("total_open_balance"))
    return (p.crossJoin(e).crossJoin(d)
            .fillna(0, ["leases_expiring_30_days", "leases_expiring_90_days", "delinquent_tenants"])
            .withColumn("occupancy_rate", safe_pct(F.col("occupied_units"), F.col("total_units")))
            .withColumn("collection_rate", safe_pct(F.col("collected_rent"), F.col("billed_rent")))
            .withColumn("avg_rent_per_occupied_unit",
                        F.when(F.col("occupied_units") > 0,
                               F.round(F.col("scheduled_monthly_rent") / F.col("occupied_units"), 2)))
            .withColumn("portfolio_value_growth_pct",
                        safe_pct(F.col("portfolio_value") - F.col("portfolio_value_12m_ago"),
                                 F.col("portfolio_value_12m_ago")))
            .withColumn("as_of_date", F.lit(as_of))
            .withColumn("reporting_month", F.lit(reporting_month)))
