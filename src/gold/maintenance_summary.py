"""Gold: trailing-12-month maintenance cost and work orders by property and category."""
from src.gold import metrics
from src.gold._base import build_and_write, silver

TABLE = "maintenance_summary"


def build(lakehouse, cfg):
    return metrics.maintenance_summary(silver(lakehouse, "maintenance"), silver(lakehouse, "properties"),
                                       cfg.reporting_as_of)


def run(lakehouse, cfg):
    return build_and_write(lakehouse, cfg, TABLE, lambda: build(lakehouse, cfg))
