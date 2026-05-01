# BankingSample - Databricks Lakehouse Pipeline

A production-grade medallion architecture pipeline built on Databricks, implementing both **imperative (notebooks)** and **declarative (Lakeflow Declarative Pipelines)** patterns. Demonstrates end-to-end ETL with PII governance, failure isolation, and operational diagnostics — packaged as a deployable Databricks Asset Bundle.

---

## What this project demonstrates

This repo is a complete portfolio piece showing:

- **Medallion architecture** (Bronze / Silver / Gold) with proper layer separation
- **Two parallel implementations** of the same pipeline:
  - Imperative notebooks (Python + SQL) with explicit MERGE upserts
  - Lakeflow Declarative Pipelines using `@dp.table` decorators and `@dp.expect` data quality rules
- **PII governance** via Unity Catalog SQL UDFs and column-level masking
- **Failure isolation** with per-table `safe_run()` wrappers and a standalone diagnostics notebook
- **Star schema modeling** for analytics-ready facts and dimensions
- **Idempotent re-runs** using Delta Lake MERGE operations
- **CI/CD-ready packaging** as a Databricks Asset Bundle

---

## Architecture

The pipeline follows a classic medallion pattern:

**Source** — 4 CSVs in a Unity Catalog Volume (customers, accounts, transactions, branches)

**Bronze** — Raw, timestamped Delta tables. Idempotent MERGE upserts. processDate lineage stamping. No cleaning.

**Silver** — Cleaned and enriched data. Type casting, casing standardization, derived columns (full_name, age, email_domain, is_credit). Joins-ready.

**Gold** — Business-ready outputs:
- `customers_secure_v` — PII-masked view for analyst access
- `transactions_enriched` — Star schema fact table joining all 4 entities
- `txn_summary_by_region` — Pre-aggregated regional KPIs
- `customer_lifetime_value` — Per-customer activity rollup
- `dormant_account_alerts` — Compliance/outreach flags

---

## Repository structure

```
BankingSample/
  databricks.yml                     -- Asset Bundle entry point
  notebooks/                         -- Imperative implementation
    0_troubleshoot_BankingSample.py     -- Diagnostics (7 health checks)
    1_bronze_BankingSample.py           -- Raw CSV ingestion + MERGE
    2_silver_BankingSample.py           -- Cleaning + enrichment
    3_gold_BankingSample.py             -- PII masking + marts
  pipelines/
    transformations/                 -- Declarative implementation (LDP)
      1_bronze.py                       -- @dp.table streaming reads
      2_silver.py                       -- @dp.expect data quality
      3_gold.py                         -- @dp.materialized_view aggregates
  resources/
    jobs/banking_job.yml             -- Notebook orchestration
    pipelines/banking_pipeline.yml   -- LDP definition
  data/sample/                       -- Sample CSVs for fresh deploys
  docs/                              -- Additional documentation
```

---

## Failure isolation strategy

This pipeline implements a three-layer failure strategy:

| Layer | Where | Catches |
|---|---|---|
| 1. Pre-flight checks | Top of each notebook | Missing source data, missing upstream tables |
| 2. Per-table isolation | `safe_run()` wrapper around each table | Mid-run crashes — one bad table doesn't kill the others |
| 3. On-demand diagnostics | `0_troubleshoot_BankingSample` | Cross-layer issues, orphan FKs, PII leakage, freshness |

Example output when one table fails:

    [PRE-FLIGHT OK] All 4 source paths verified.
    [OK]   customers
    [FAIL] accounts - AnalysisException: malformed CSV
    [OK]   transactions
    [OK]   branches

    [RUN FAILED] 1 table(s) failed:
      - accounts: AnalysisException: malformed CSV

3 of 4 tables landed successfully. Fix the source and re-run — MERGE handles recovery cleanly.

---

## PII governance

Customer PII is preserved in clear text only in Bronze and Silver (trusted internal layers). Gold exposes only masked views via Unity Catalog SQL UDFs:

| Field | Bronze/Silver | Gold |
|---|---|---|
| SSN | `821-18-1750` | `XXX-XX-1750` |
| Email | `donna.green@gmail.com` | `do***@gmail.com` |
| Phone | `(570) 791-4150` | `(***) ***-4150` |
| Account number | `123456789012` | `********9012` |
| Address | `3833 Hill St` | `Hill St` |
| DOB | `2004-04-29` | `2004` (year only) |

The `0_troubleshoot` notebook includes a PII leakage check that fails the build if any clear-text PII appears in Gold.

---

## Deployment

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks CLI v0.218+ installed and configured
- Catalog `databricks_clar` created (or override via bundle variable)

### Setup

1. Clone the repo and enter the directory.

2. Update `databricks.yml` with your workspace URL and notification email:

        variables:
          workspace_host:
            default: "https://your-workspace.cloud.databricks.com"
          notification_email:
            default: "you@example.com"

3. Upload sample CSVs to your Unity Catalog Volume at:

        /Volumes/databricks_clar/bronze/bronze_volume/banking_data/customers/customers.csv
        /Volumes/databricks_clar/bronze/bronze_volume/banking_data/accounts/accounts.csv
        /Volumes/databricks_clar/bronze/bronze_volume/banking_data/transactions/transactions.csv
        /Volumes/databricks_clar/bronze/bronze_volume/banking_data/branches/branches.csv

   Source CSVs are in `data/sample/` for reference.

4. Validate and deploy:

        databricks bundle validate
        databricks bundle deploy --target dev

5. Run the job:

        databricks bundle run banking_pipeline_job --target dev

---

## Sample queries (Gold layer)

After running the pipeline, try these on the Gold tables.

Top 10 customers by total inflow:

    SELECT customer_name, total_inflow, total_transactions
    FROM databricks_clar.gold.customer_lifetime_value
    ORDER BY total_inflow DESC
    LIMIT 10;

Regional transaction volume:

    SELECT branch_region, total_transactions, net_flow, unique_customers
    FROM databricks_clar.gold.txn_summary_by_region
    ORDER BY total_transactions DESC;

Accounts requiring outreach (no activity > 90 days):

    SELECT customer_name, account_type, balance, days_since_last_txn, activity_flag
    FROM databricks_clar.gold.dormant_account_alerts
    WHERE activity_flag IN ('AT_RISK', 'DORMANT', 'NEVER_USED')
    ORDER BY days_since_last_txn DESC NULLS FIRST;

---

## Implementation notes

### Why two implementations?

Real teams maintain legacy notebooks while migrating to declarative pipelines. This project demonstrates fluency in both paradigms:

- **Notebooks** are imperative — explicit reads, transforms, MERGE writes. Easier to debug step-by-step. Better for ad-hoc exploration.
- **LDP** is declarative — define what each table is, let Databricks handle execution order, dependencies, and incremental processing. Better for production with built-in DQ and lineage.

### Key design decisions

- **Bronze is immutable**: never mutated in place. If Silver/Gold logic is wrong, rebuild from Bronze without re-ingesting source.
- **processDate on every row**: per-layer lineage timestamp, useful for incremental processing and freshness diagnostics.
- **CREATE OR REPLACE in Gold**: aggregates are full rebuilds, not upserts. Simpler to reason about, faster to recover.
- **Star schema in transactions_enriched**: denormalized fact table joining all 4 entities. Pre-computed once, queried many times.

---

## Free tier notes

This project runs on Databricks free tier with serverless compute. Some considerations:

- Job runs may queue under load
- Lakeflow Declarative Pipelines runs may be limited per day
- All compute is serverless — no cluster configuration required

---

## License

MIT — use freely for learning, adapt for your own portfolio.