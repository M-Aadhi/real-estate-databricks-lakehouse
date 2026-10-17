# Testing

Tests run locally and in CI with open-source PySpark + Delta Lake. **No Databricks credentials needed.**

```bash
pip install -r requirements.txt        # needs Java 17+
pytest                                  # all tests
pytest tests/unit                       # fast logic tests
pytest --cov=src --cov-report=term-missing
```

## Layout

| Folder | What it tests | Data |
|---|---|---|
| `tests/unit` | KPI formulas (occupancy, collection rate, rent roll, lease expiry, delinquency, value growth), safe casting, validation reasons, deduplication, SCD2 merge, payment status, config errors, file checks, idempotent writes, run logging | tiny in-memory DataFrames |
| `tests/data_quality` | PK uniqueness, required columns, relationships, business rules, rates in 0-100, the pipeline DQ suites | fixture lakehouse |
| `tests/integration` | Bronze → Silver → Gold end to end: valid rows reach Gold, invalid rows are quarantined with the right reason, duplicates don't multiply, reruns are idempotent, incremental MERGE + SCD2 work, Gold KPIs equal hand-calculated values, the quarantine threshold stops a bad batch | `tests/fixtures` |

`tests/fixtures/` is a deliberately small, dirty dataset (see its README) so every expected KPI can be
checked by hand. The large generated dataset is not used in tests.

The `fixture_lakehouse` fixture (in `tests/conftest.py`) runs the real pipeline once per session:
batch_001 → rerun → batch_002 → rerun, snapshotting row counts and KPIs after each step.

## Databricks
CI does not touch Databricks. To validate in a workspace, run notebooks 01–05; notebook 05 asserts
that re-running batch_002 leaves Silver unchanged, and the pipeline's DQ suites raise on failure.

## Latest results

Local run on Python 3.13, PySpark 4.0.1, Delta 4.0.0 (`pytest --cov=src --cov-report=term-missing tests/`):

```
tests/data_quality/test_data_quality.py .......................          [ 38%]
tests/integration/test_dashboard_export.py .                             [ 40%]
tests/integration/test_pipeline.py ............                          [ 60%]
tests/unit/test_framework.py ......                                      [ 70%]
tests/unit/test_metrics.py ........                                      [ 83%]
tests/unit/test_silver_logic.py ..........                               [100%]
TOTAL                                   940     81    91%
======================== 60 passed in 487.52s (0:08:07) ========================
```
