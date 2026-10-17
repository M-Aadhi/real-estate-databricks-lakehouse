"""Gold: rent roll, one row per active lease with tenant, unit and rent-vs-market details."""
from src.gold import metrics
from src.gold._base import build_and_write, silver

TABLE = "rent_roll"


def build(lakehouse, cfg):
    return metrics.rent_roll(silver(lakehouse, "leases"), silver(lakehouse, "units"), silver(lakehouse, "tenants"),
                             silver(lakehouse, "properties"), cfg.reporting_as_of)


def run(lakehouse, cfg):
    return build_and_write(lakehouse, cfg, TABLE, lambda: build(lakehouse, cfg))
