"""Gold: occupancy by property (occupied / vacant units, occupancy rate) at the as-of date."""
from src.gold import metrics
from src.gold._base import build_and_write, silver

TABLE = "occupancy"


def build(lakehouse, cfg):
    return metrics.occupancy_by_property(silver(lakehouse, "properties"), silver(lakehouse, "units"),
                                         silver(lakehouse, "leases"), cfg.reporting_as_of)


def run(lakehouse, cfg):
    return build_and_write(lakehouse, cfg, TABLE, lambda: build(lakehouse, cfg))
