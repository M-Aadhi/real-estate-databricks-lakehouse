"""Gold: lease expiry schedule with 30/90-day flags and expiry buckets."""
from src.gold import metrics
from src.gold._base import build_and_write, silver

TABLE = "lease_expiry"


def build(lakehouse, cfg):
    return metrics.lease_expiry(silver(lakehouse, "leases"), silver(lakehouse, "properties"), cfg.reporting_as_of)


def run(lakehouse, cfg):
    return build_and_write(lakehouse, cfg, TABLE, lambda: build(lakehouse, cfg))
