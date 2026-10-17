"""Silver: tenants as an SCD Type 2 dimension (history of contact/status changes).

effective_from = created_date for a tenant's first version, or the batch effective date
when an existing tenant's attributes change. See src/silver/scd2.py.
"""
from __future__ import annotations

from pyspark.sql import functions as F

from src.common.schemas import TENANTS
from src.common.validation import Rule
from src.pipeline import entity_cli
from src.silver.base import SilverSpec, transform_entity
from src.silver.scd2 import merge_scd2, prepare_scd2_source

TENANT_STATUSES = ["Active", "Former", "Prospect"]


def standardize(df):
    return (df.withColumn("email", F.lower("email"))
              .withColumn("first_name", F.initcap("first_name"))
              .withColumn("last_name", F.initcap("last_name"))
              .withColumn("tenant_status", F.initcap("tenant_status")))


def rules(cfg):
    return [
        Rule("invalid_email", F.col("email").isNotNull() & ~F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")),
        Rule("invalid_tenant_status", F.col("tenant_status").isNotNull()
             & ~F.col("tenant_status").isin(TENANT_STATUSES)),
    ]


SPEC = SilverSpec(TENANTS, standardize=standardize, rules=rules)


def run(lakehouse, batch_id, cfg):
    effective_date = cfg.batch(batch_id).effective_date

    def write_scd2(silver):
        if lakehouse.exists("silver", "tenants"):
            known = (lakehouse.read("silver", "tenants").filter("is_current")
                     .select(F.col("tenant_id").alias("_known_id")))
            silver = silver.join(known, silver.tenant_id == known._known_id, "left")
            eff = F.when(F.col("_known_id").isNotNull(), F.lit(effective_date)).otherwise(F.col("created_date"))
            source = prepare_scd2_source(silver, eff).drop("_known_id")
        else:
            source = prepare_scd2_source(silver, F.col("created_date"))
        merge_scd2(lakehouse, source, "silver", "tenants", "tenant_id")

    return transform_entity(lakehouse, SPEC, batch_id, cfg, writer=write_scd2)


if __name__ == "__main__":
    entity_cli("silver", "tenants")
