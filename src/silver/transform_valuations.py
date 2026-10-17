"""Silver: valuations, plus an SCD2-style value history per property.

silver.valuations keeps every appraisal (MERGE on valuation_id).
silver.property_value_history derives effective_from / effective_to / is_current per
property from the appraisal dates, so "value as of any date" is a simple range filter.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.common.schemas import VALUATIONS
from src.common.validation import Rule
from src.pipeline import entity_cli
from src.silver.base import ForeignKey, SilverSpec, transform_entity
from src.silver.scd2 import OPEN_END


def rules(cfg):
    return [Rule("property_value_not_positive", F.col("property_value") <= 0)]


SPEC = SilverSpec(VALUATIONS, rules=rules, foreign_keys=[ForeignKey("property_id", "properties")])


def build_value_history(valuations: DataFrame) -> DataFrame:
    w = Window.partitionBy("property_id").orderBy("valuation_date", "valuation_id")
    nxt = F.lead("valuation_date").over(w)
    return (valuations.select("property_id", "valuation_id", "valuation_date", "property_value", "valuation_method")
            .withColumn("effective_from", F.col("valuation_date"))
            .withColumn("effective_to", F.coalesce(F.date_sub(nxt, 1), F.to_date(F.lit(OPEN_END))))
            .withColumn("is_current", nxt.isNull())
            .withColumn("previous_value", F.lag("property_value").over(w)))


def run(lakehouse, batch_id, cfg):
    record = transform_entity(lakehouse, SPEC, batch_id, cfg)
    if lakehouse.exists("silver", "valuations"):
        history = build_value_history(lakehouse.read("silver", "valuations"))
        lakehouse.overwrite(history, "silver", "property_value_history")
    return record


if __name__ == "__main__":
    entity_cli("silver", "valuations")
