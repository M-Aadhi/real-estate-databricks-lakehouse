"""Gold: open balances per tenant and property, aging buckets and delinquency flag."""
from src.gold import metrics
from src.gold._base import build_and_write, silver

TABLE = "tenant_delinquency"


def build(lakehouse, cfg):
    return metrics.tenant_delinquency(silver(lakehouse, "payments"), silver(lakehouse, "tenants"),
                                      silver(lakehouse, "properties"), cfg.reporting_as_of,
                                      cfg.delinquency_min_balance, cfg.delinquency_min_days)


def run(lakehouse, cfg):
    return build_and_write(lakehouse, cfg, TABLE, lambda: build(lakehouse, cfg))
