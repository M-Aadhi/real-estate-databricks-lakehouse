"""Silver: maintenance work orders. Unit is optional (common areas) but must exist if given."""
from __future__ import annotations

from pyspark.sql import functions as F

from src.common.schemas import MAINTENANCE
from src.common.validation import Rule
from src.pipeline import entity_cli
from src.silver.base import ForeignKey, SilverSpec, transform_entity

STATUSES = ["Open", "In Progress", "Completed", "Cancelled"]


def standardize(df):
    return (df.withColumn("status", F.initcap("status"))
              .withColumn("category", F.when(F.upper("category") == "HVAC", "HVAC").otherwise(F.initcap("category"))))


def rules(cfg):
    return [
        Rule("negative_cost", F.col("cost") < 0),
        Rule("completion_before_request", F.col("completion_date") < F.col("request_date")),
        Rule("invalid_status", F.col("status").isNotNull() & ~F.col("status").isin(STATUSES)),
    ]


def derive(df, cfg):
    return df.withColumn("days_to_complete", F.datediff("completion_date", "request_date"))


SPEC = SilverSpec(MAINTENANCE, standardize=standardize, rules=rules, derive=derive,
                  foreign_keys=[ForeignKey("property_id", "properties"),
                                ForeignKey("unit_id", "units", optional=True)])


def run(lakehouse, batch_id, cfg):
    return transform_entity(lakehouse, SPEC, batch_id, cfg)


if __name__ == "__main__":
    entity_cli("silver", "maintenance")
