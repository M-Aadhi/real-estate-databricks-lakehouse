"""Gold: single-row portfolio KPI summary built from the other Gold tables."""
from src.gold import metrics
from src.gold._base import build_and_write, silver

TABLE = "portfolio_summary"


def build(lakehouse, cfg):
    return metrics.portfolio_summary(
        lakehouse.read("gold", "property_performance"), lakehouse.read("gold", "lease_expiry"),
        lakehouse.read("gold", "tenant_delinquency"), silver(lakehouse, "payments"),
        cfg.reporting_as_of, cfg.reporting_month)


def run(lakehouse, cfg):
    return build_and_write(lakehouse, cfg, TABLE, lambda: build(lakehouse, cfg))
