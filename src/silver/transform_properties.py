"""Silver: properties. Standardise names/state/type, validate price and allowed values."""
from __future__ import annotations

from pyspark.sql import functions as F

from src.common.schemas import PROPERTIES
from src.common.validation import Rule
from src.pipeline import entity_cli
from src.silver.base import SilverSpec, transform_entity

PROPERTY_TYPES = ["Multifamily", "Office", "Retail", "Industrial", "Mixed Use"]
PROPERTY_STATUSES = ["Active", "Under Renovation", "Disposed"]


def standardize(df):
    return (df.withColumn("state", F.upper("state"))
              .withColumn("property_type", F.initcap("property_type"))
              .withColumn("property_type", F.when(F.col("property_type") == "Mixed-use", "Mixed Use")
                          .otherwise(F.col("property_type")))
              .withColumn("status", F.initcap("status")))


def rules(cfg):
    return [
        Rule("acquisition_price_not_positive", F.col("acquisition_price") <= 0),
        Rule("invalid_property_type", F.col("property_type").isNotNull() & ~F.col("property_type").isin(PROPERTY_TYPES)),
        Rule("invalid_status", F.col("status").isNotNull() & ~F.col("status").isin(PROPERTY_STATUSES)),
        Rule("invalid_state_code", F.col("state").isNotNull() & ~F.col("state").rlike("^[A-Z]{2}$")),
    ]


SPEC = SilverSpec(PROPERTIES, standardize=standardize, rules=rules)


def run(lakehouse, batch_id, cfg):
    return transform_entity(lakehouse, SPEC, batch_id, cfg)


if __name__ == "__main__":
    entity_cli("silver", "properties")
