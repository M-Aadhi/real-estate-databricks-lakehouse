"""Silver: units. Must belong to a known property; size and rent must be sensible."""
from __future__ import annotations

from pyspark.sql import functions as F

from src.common.schemas import UNITS
from src.common.validation import Rule
from src.pipeline import entity_cli
from src.silver.base import ForeignKey, SilverSpec, transform_entity


def standardize(df):
    return df.withColumn("unit_status", F.initcap("unit_status"))


def rules(cfg):
    return [
        Rule("square_feet_not_positive", F.col("square_feet") <= 0),
        Rule("negative_market_rent", F.col("market_rent") < 0),
        Rule("negative_bedrooms", F.col("bedrooms") < 0),
        Rule("negative_bathrooms", F.col("bathrooms") < 0),
    ]


SPEC = SilverSpec(UNITS, standardize=standardize, rules=rules,
                  foreign_keys=[ForeignKey("property_id", "properties")])


def run(lakehouse, batch_id, cfg):
    return transform_entity(lakehouse, SPEC, batch_id, cfg)


if __name__ == "__main__":
    entity_cli("silver", "units")
