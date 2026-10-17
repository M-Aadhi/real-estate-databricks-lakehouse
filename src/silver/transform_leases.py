"""Silver: leases. Dates must be ordered, rent non-negative, tenant/unit/property must exist
and the unit must belong to the lease's property."""
from __future__ import annotations

from pyspark.sql import functions as F

from src.common.schemas import LEASES
from src.common.validation import Rule
from src.pipeline import entity_cli
from src.silver.base import ForeignKey, SilverSpec, transform_entity

LEASE_STATUSES = ["Active", "Expired", "Terminated", "Future"]


def standardize(df):
    return df.withColumn("lease_status", F.initcap("lease_status"))


def rules(cfg):
    return [
        Rule("lease_end_before_start", F.col("lease_end_date") < F.col("lease_start_date")),
        Rule("negative_monthly_rent", F.col("monthly_rent") < 0),
        Rule("negative_security_deposit", F.col("security_deposit") < 0),
        Rule("invalid_lease_status", F.col("lease_status").isNotNull() & ~F.col("lease_status").isin(LEASE_STATUSES)),
    ]


def unit_property_check(lakehouse, df):
    """The unit must belong to the property on the lease (only checked when the unit exists)."""
    if not lakehouse.exists("silver", "units"):
        return df
    units = lakehouse.read("silver", "units").select(
        F.col("unit_id").alias("_u_id"), F.col("property_id").alias("_u_property"))
    joined = df.join(units, df.unit_id == units._u_id, "left")
    mismatch = F.col("_u_id").isNotNull() & (F.col("_u_property") != F.col("property_id"))
    return joined.withColumn("_fk_unit_property", F.when(mismatch, F.lit("unit_property_mismatch"))) \
                 .drop("_u_id", "_u_property")


def derive(df, cfg):
    return df.withColumn("lease_term_months",
                         F.round(F.months_between(F.date_add("lease_end_date", 1), "lease_start_date")).cast("int"))


SPEC = SilverSpec(LEASES, standardize=standardize, rules=rules,
                  foreign_keys=[ForeignKey("tenant_id", "tenants"), ForeignKey("unit_id", "units"),
                                ForeignKey("property_id", "properties")],
                  join_checks=[unit_property_check], derive=derive)


def run(lakehouse, batch_id, cfg):
    return transform_entity(lakehouse, SPEC, batch_id, cfg)


if __name__ == "__main__":
    entity_cli("silver", "leases")
