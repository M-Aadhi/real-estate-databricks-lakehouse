# Run on Databricks (Free Edition)

> The notebooks and modules are written for serverless compute (no RDDs, no `sparkContext`, no
> caching, no DBFS paths, Volumes for files, only supported Spark configs). They have **not** been
> executed by the author in a Databricks workspace for this release; the local pipeline and tests
> were. If something differs in your workspace, see Troubleshooting below.

## 1. Workspace
1. Sign up for **Databricks Free Edition** (free, serverless only).
2. Note your catalog: Free Edition provides the `workspace` catalog. The notebooks default to it.
   On a paid workspace you can create `real_estate` and pass it in the `catalog` widget.

## 2. Bring the code in
1. **Workspace → Create → Git folder** → paste `https://github.com/M-Aadhi/real-estate-databricks-lakehouse`.
2. Open the folder `notebooks/`.

## 3. Run the notebooks in order (serverless compute)
| Notebook | What it does |
|---|---|
| `01_bronze_ingestion` | creates schemas `raw, bronze, silver, gold, quarantine, ops` and the Volume `raw.landing`, copies `data/raw` into the Volume, loads batch_001 into Bronze |
| `02_silver_transformations` | validation, quarantine, dedup, MERGE, SCD2, Silver DQ checks |
| `03_gold_models` | builds Gold tables + Gold DQ checks, exports dashboard JSON |
| `04_business_analysis` | SQL answers to management questions |
| `05_incremental_merge_demo` | loads batch_002, shows MERGE/SCD2 results and Delta history, reruns to prove idempotency |

Each notebook starts with `%pip install pyyaml python-dateutil` (small, pinned by the environment)
and a setup cell that adds the repo root to `sys.path` and sets `LAKEHOUSE_ENV=databricks`.

## 4. Optional: schedule it as a job
Create a Job with tasks 01 → 02 → 03 on serverless compute, parameter `catalog`. For production,
deploy with Declarative Automation Bundles and Auto Loader (see `docs/architecture.md`).

## What runs where

| Feature | Local (PySpark + delta-spark) | Databricks Free Edition |
|---|---|---|
| Bronze / Silver / Gold, MERGE, SCD2, replaceWhere | ✅ verified | designed for it (UC managed tables) |
| Quarantine + DQ + run logs | ✅ verified | ✅ same code |
| Unity Catalog 3-level names, Volumes | n/a (path mode) | ✅ `workspace.<layer>.<table>` |
| Creating a new catalog (`real_estate`) | n/a | ❌ usually not allowed on Free Edition → use `workspace` |
| Lineage / permissions demo | n/a | partial (Free Edition limits); documented as production design |
| Scheduled jobs | n/a | limited quota on Free Edition |

## Troubleshooting
- `PERMISSION_DENIED ... CREATE SCHEMA` → use a catalog you own (`workspace`), or ask an admin.
- `ModuleNotFoundError: src` → run the notebook from inside the Git folder (`notebooks/`), so `..` is the repo root.
- `TABLE_OR_VIEW_NOT_FOUND` in 02/03 → run 01 first (and 02 before 03).
