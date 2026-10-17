"""Load config/config.yaml and resolve the active environment.

Usage:
    cfg = load_config()                 # env from LAKEHOUSE_ENV, default "local"
    cfg = load_config(env="databricks")
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from .exceptions import ConfigError

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "config.yaml"
ENTITY_ORDER = ["properties", "units", "tenants", "leases", "payments", "valuations", "maintenance"]


@dataclass
class BatchConfig:
    batch_id: str
    path: str
    effective_date: date
    entities: list[str]


@dataclass
class Config:
    env: str
    storage_mode: str
    reporting_as_of: date
    reporting_month: str
    trend_months: int
    batches: dict[str, BatchConfig]
    max_quarantine_pct: float
    payment_grace_days: int
    delinquency_min_balance: float
    delinquency_min_days: int
    other_opex_ratio: float
    lakehouse_root: str | None = None
    catalog: str | None = None
    raw_volume_root: str | None = None
    log_level: str = "INFO"
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    def batch(self, batch_id: str) -> BatchConfig:
        if batch_id not in self.batches:
            raise ConfigError(f"Unknown batch '{batch_id}'. Known: {', '.join(self.batches)}")
        return self.batches[batch_id]


def _require(section: dict, key: str, where: str) -> Any:
    if key not in section or section[key] in (None, ""):
        raise ConfigError(f"Missing config value '{where}.{key}'")
    return section[key]


def _resolve(path: str, base: Path) -> str:
    """Absolute paths and /Volumes paths are kept; relative paths are resolved from base."""
    p = Path(path)
    return str(p if p.is_absolute() else (base / p).resolve())


def load_config(path: str | Path | None = None, env: str | None = None,
                overrides: dict[str, Any] | None = None) -> Config:
    path = Path(path) if path else DEFAULT_CONFIG_PATH
    if not path.is_file():
        raise ConfigError(f"Config file not found: {path}")
    with path.open(encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    if overrides:
        raw = _deep_merge(raw, overrides)

    env = env or os.environ.get("LAKEHOUSE_ENV", "local")
    envs = _require(raw, "environments", "root")
    if env not in envs:
        raise ConfigError(f"Unknown environment '{env}'. Known: {', '.join(envs)}")
    env_cfg = envs[env]
    base = path.parent.parent  # repo root (config/ lives at the root)

    reporting = _require(raw, "reporting", "root")
    dq = _require(raw, "data_quality", "root")
    delinquency = _require(dq, "delinquency", "data_quality")

    batches = {}
    for batch_id, b in _require(raw, "batches", "root").items():
        unknown = set(b.get("entities", [])) - set(ENTITY_ORDER)
        if unknown:
            raise ConfigError(f"Batch {batch_id} lists unknown entities: {sorted(unknown)}")
        entities = [e for e in ENTITY_ORDER if e in b.get("entities", [])]
        source = _require(b, "path", f"batches.{batch_id}")
        if env_cfg.get("storage_mode") == "catalog" and env_cfg.get("raw_volume_root"):
            # On Databricks raw files are staged in a UC Volume under the same relative folder.
            rel = Path(source).relative_to("data/raw") if source.startswith("data/raw") else Path(source)
            source = str(Path(env_cfg["raw_volume_root"]) / rel)
        else:
            source = _resolve(source, base)
        batches[batch_id] = BatchConfig(batch_id, source,
                                        date.fromisoformat(str(_require(b, "effective_date", batch_id))),
                                        entities)

    mode = _require(env_cfg, "storage_mode", f"environments.{env}")
    if mode not in ("path", "catalog"):
        raise ConfigError(f"storage_mode must be 'path' or 'catalog', got '{mode}'")
    return Config(
        env=env,
        storage_mode=mode,
        reporting_as_of=date.fromisoformat(str(_require(reporting, "as_of_date", "reporting"))),
        reporting_month=str(_require(reporting, "reporting_month", "reporting")),
        trend_months=int(reporting.get("trend_months", 12)),
        batches=batches,
        max_quarantine_pct=float(_require(dq, "max_quarantine_pct", "data_quality")),
        payment_grace_days=int(dq.get("payment_grace_days", 5)),
        delinquency_min_balance=float(delinquency.get("min_outstanding_balance", 1.0)),
        delinquency_min_days=int(delinquency.get("min_days_past_due", 30)),
        other_opex_ratio=float(raw.get("assumptions", {}).get("other_operating_expense_ratio", 0.30)),
        lakehouse_root=_resolve(env_cfg["lakehouse_root"], base) if mode == "path" else None,
        catalog=env_cfg.get("catalog") if mode == "catalog" else None,
        raw_volume_root=env_cfg.get("raw_volume_root"),
        log_level=env_cfg.get("log_level", "INFO"),
        raw=raw,
    )


def _deep_merge(base: dict, extra: dict) -> dict:
    out = dict(base)
    for key, value in extra.items():
        out[key] = _deep_merge(out[key], value) if isinstance(value, dict) and isinstance(out.get(key), dict) else value
    return out
