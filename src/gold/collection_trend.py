"""Gold: monthly billed vs collected rent for the trailing N months (portfolio level)."""
from src.gold import metrics
from src.gold._base import build_and_write, silver

TABLE = "collection_trend"


def build(lakehouse, cfg):
    return metrics.collection_trend(silver(lakehouse, "payments"), cfg.reporting_month, cfg.trend_months)


def run(lakehouse, cfg):
    return build_and_write(lakehouse, cfg, TABLE, lambda: build(lakehouse, cfg))
