"""Bronze ingestion for tenants. Thin entry point over the generic loader.

    python -m src.bronze.ingest_tenants --batch batch_001
"""
from __future__ import annotations

from src.bronze.ingest import ingest_entity
from src.pipeline import entity_cli

ENTITY = "tenants"


def run(lakehouse, batch_id: str, source_dir: str) -> dict:
    return ingest_entity(lakehouse, ENTITY, batch_id, source_dir)


if __name__ == "__main__":
    entity_cli("bronze", ENTITY)
