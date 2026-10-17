"""Gold: one row per property with occupancy, rent, collections, TTM revenue, maintenance,
valuation, value growth, net rent after maintenance and assumption-based estimated NOI."""
from src.gold import metrics
from src.gold._base import build_and_write, silver

TABLE = "property_performance"


def build(lakehouse, cfg):
    occupancy = lakehouse.read("gold", "occupancy") if lakehouse.exists("gold", "occupancy") else \
        metrics.occupancy_by_property(silver(lakehouse, "properties"), silver(lakehouse, "units"),
                                      silver(lakehouse, "leases"), cfg.reporting_as_of)
    return metrics.property_performance(
        occupancy.drop("as_of_date"), silver(lakehouse, "leases"), silver(lakehouse, "payments"),
        silver(lakehouse, "maintenance"), silver(lakehouse, "property_value_history"),
        cfg.reporting_as_of, cfg.reporting_month, cfg.other_opex_ratio)


def run(lakehouse, cfg):
    return build_and_write(lakehouse, cfg, TABLE, lambda: build(lakehouse, cfg))
