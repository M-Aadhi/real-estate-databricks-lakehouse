"""Silver: payments (rent charges and collections).

The source `payment_status` is kept unchanged. We add `derived_payment_status`
computed from amounts and dates so reporting does not depend on inconsistent source
statuses:
  Paid       amount_paid >= amount_due and paid within the grace period
  Paid Late  amount_paid >= amount_due but after the grace period
  Partial    0 < amount_paid < amount_due
  Unpaid     amount_paid = 0
"""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

from src.common.schemas import PAYMENTS
from src.common.validation import Rule
from src.pipeline import entity_cli
from src.silver.base import ForeignKey, SilverSpec, transform_entity


def payment_status_expr(grace_days: int) -> Column:
    paid_in_full = F.col("amount_paid") >= F.col("amount_due")
    on_time = F.col("payment_date") <= F.date_add("payment_due_date", grace_days)
    return (F.when(paid_in_full & on_time, "Paid")
             .when(paid_in_full, "Paid Late")
             .when(F.col("amount_paid") > 0, "Partial")
             .otherwise("Unpaid"))


def standardize(df):
    return (df.withColumn("payment_status", F.initcap("payment_status"))
              .withColumn("payment_method", F.upper("payment_method"))
              .withColumn("payment_method", F.when(F.col("payment_method").isin("CARD", "CHECK", "CASH"),
                                                   F.initcap("payment_method")).otherwise(F.col("payment_method"))))


def rules(cfg):
    return [
        Rule("negative_amount_due", F.col("amount_due") < 0),
        Rule("negative_amount_paid", F.col("amount_paid") < 0),
        Rule("paid_without_payment_date", (F.col("amount_paid") > 0) & F.col("payment_date").isNull()),
    ]


def derive(df, cfg):
    return (df.withColumn("derived_payment_status", payment_status_expr(cfg.payment_grace_days))
              .withColumn("days_late", F.when(F.col("payment_date").isNotNull(),
                                              F.greatest(F.lit(0), F.datediff("payment_date", "payment_due_date"))))
              .withColumn("outstanding_amount", F.greatest(F.lit(0), F.col("amount_due") - F.col("amount_paid"))))


SPEC = SilverSpec(PAYMENTS, standardize=standardize, rules=rules, derive=derive,
                  foreign_keys=[ForeignKey("lease_id", "leases"), ForeignKey("tenant_id", "tenants"),
                                ForeignKey("property_id", "properties")])


def run(lakehouse, batch_id, cfg):
    return transform_entity(lakehouse, SPEC, batch_id, cfg)


if __name__ == "__main__":
    entity_cli("silver", "payments")
