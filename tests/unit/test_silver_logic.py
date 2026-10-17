"""Unit tests for validation, deduplication, SCD2 and payment-status logic."""
from datetime import date

import pytest
from pyspark.sql import functions as F

from src.common.schemas import LEASES
from src.common.validation import apply_rules, cast_column, row_hash, schema_rules, split_valid_invalid
from src.silver.dedup import deduplicate
from src.silver.scd2 import merge_scd2, prepare_scd2_source
from src.silver.transform_leases import rules as lease_rules
from src.silver.transform_payments import payment_status_expr
from tests.helpers import make_df


def test_safe_casts_never_fail(spark):
    df = spark.createDataFrame([("2026-02-28", "12.50", "7"), ("2026-02-30", "abc", ""), ("09/01/2026", " 3 ", None)],
                               "d string, m string, i string")
    out = df.select(cast_column("d", "date").alias("d"), cast_column("m", "decimal(14,2)").alias("m"),
                    cast_column("i", "int").alias("i")).collect()
    assert out[0].d == date(2026, 2, 28) and float(out[0].m) == 12.5 and out[0].i == 7
    assert out[1].d is None and out[1].m is None and out[1].i is None       # invalid -> null, no exception
    assert out[2].d is None and float(out[2].m) == 3.0


def _lease_rows(spark, rows):
    cols = LEASES.column_names
    df = spark.createDataFrame(rows, ", ".join(f"{c} string" for c in cols))
    from src.common.validation import cast_to_schema
    return cast_to_schema(df, LEASES)


def test_lease_rules_quarantine_with_reasons(spark):
    df = _lease_rows(spark, [
        ("L1", "T1", "U1", "P1", "2026-01-01", "2026-12-31", "1000", "1000", "Active"),
        ("L2", "T1", "U1", "P1", "2026-03-01", "2026-02-01", "-5", "0", "Active"),
        ("L3", None, "U1", "P1", "2026-02-30", "2026-12-31", "1000", "0", "Pending"),
    ])
    valid, invalid = split_valid_invalid(apply_rules(df, schema_rules(LEASES) + lease_rules(None)))
    assert [r.lease_id for r in valid.collect()] == ["L1"]
    reasons = {r.lease_id: r.error_reason for r in invalid.collect()}
    assert "lease_end_before_start" in reasons["L2"] and "negative_monthly_rent" in reasons["L2"]
    assert "missing_required:tenant_id" in reasons["L3"]
    assert "invalid_date:lease_start_date" in reasons["L3"] and "invalid_lease_status" in reasons["L3"]


def test_deduplication(spark):
    df = spark.createDataFrame([
        ("P1", "a", "f1"), ("P1", "a", "f1"), ("P1", "a", "f1"),   # exact duplicates -> keep 1
        ("P2", "b", "f1"), ("P2", "c", "f1"),                       # conflicting -> quarantine both
        ("P3", "d", "f1"),
    ], "payment_id string, val string, source_file string")
    df = (df.withColumn("row_hash", row_hash(["payment_id", "val"]))
            .withColumn("ingestion_timestamp", F.current_timestamp()))
    unique, conflicts, removed = deduplicate(df, "payment_id")
    assert sorted(r.payment_id for r in unique.collect()) == ["P1", "P3"]
    assert removed == 2
    assert conflicts.count() == 2 and {r.error_reason for r in conflicts.collect()} == {"conflicting_duplicate_key"}


@pytest.mark.parametrize("due,paid,pay_date,expected", [
    (1000, 1000, date(2026, 9, 3), "Paid"),
    (1000, 1000, date(2026, 9, 6), "Paid"),        # exactly at grace limit (5 days)
    (1000, 1000, date(2026, 9, 7), "Paid Late"),
    (1000, 1200, date(2026, 9, 1), "Paid"),        # overpayment counts as paid
    (1000, 400, date(2026, 9, 2), "Partial"),
    (1000, 0, None, "Unpaid"),
])
def test_payment_status_logic(spark, due, paid, pay_date, expected):
    df = make_df(spark, [(due, paid, pay_date, date(2026, 9, 1))],
                               "amount_due decimal(14,2), amount_paid decimal(14,2), payment_date date, "
                               "payment_due_date date")
    assert df.select(payment_status_expr(5).alias("s")).first().s == expected


def test_scd2_logic(make_lakehouse):
    lh, _ = make_lakehouse("scd2")
    spark = lh.spark
    schema = "tenant_id string, email string, created_date date"

    def load(rows, eff):
        src = spark.createDataFrame(rows, schema).withColumn("row_hash", row_hash(["tenant_id", "email"]))
        if lh.exists("silver", "dim"):
            known = lh.read("silver", "dim").filter("is_current").select(F.col("tenant_id").alias("_k"))
            src = src.join(known, src.tenant_id == known._k, "left")
            src = prepare_scd2_source(src, F.when(F.col("_k").isNotNull(), F.lit(eff)).otherwise(F.col("created_date"))).drop("_k")
        else:
            src = prepare_scd2_source(src, F.col("created_date"))
        merge_scd2(lh, src, "silver", "dim", "tenant_id")

    load([("T1", "a@x.com", date(2025, 1, 1)), ("T2", "b@x.com", date(2025, 2, 1))], date(2026, 9, 1))
    load([("T1", "a2@x.com", date(2025, 1, 1)), ("T2", "b@x.com", date(2025, 2, 1)),
          ("T3", "c@x.com", date(2026, 9, 20))], date(2026, 10, 1))
    load([("T1", "a2@x.com", date(2025, 1, 1))], date(2026, 10, 1))      # rerun -> no change

    rows = lh.read("silver", "dim").orderBy("tenant_id", "effective_from").collect()
    assert [(r.tenant_id, r.email, r.is_current) for r in rows] == [
        ("T1", "a@x.com", False), ("T1", "a2@x.com", True), ("T2", "b@x.com", True), ("T3", "c@x.com", True)]
    closed, new = rows[0], rows[1]
    assert closed.effective_to == date(2026, 9, 30) and new.effective_from == date(2026, 10, 1)
    assert str(new.effective_to) == "9999-12-31"
