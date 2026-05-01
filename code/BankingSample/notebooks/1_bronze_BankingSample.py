# Databricks notebook source
# MAGIC %md
# MAGIC # BankingSample — Bronze Layer
# MAGIC
# MAGIC **Pipeline:** BankingSample | **Layer:** Bronze | **Catalog:** `databricks_clar`
# MAGIC
# MAGIC ## Overview
# MAGIC 1. Verify catalog/schemas exist (`bronze`, `silver`, `gold`)
# MAGIC 2. Pre-flight: confirm source CSVs are present
# MAGIC 3. Read 4 raw CSVs from `/Volumes/databricks_clar/bronze/bronze_volume/banking_data/`
# MAGIC 4. Stamp each record with `processDate = current_timestamp()`
# MAGIC 5. Upsert (MERGE) into Bronze Delta tables — idempotent re-runs
# MAGIC
# MAGIC **Bronze tables produced:**
# MAGIC - `banking_customers_raw`
# MAGIC - `banking_accounts_raw`
# MAGIC - `banking_transactions_raw`
# MAGIC - `banking_branches_raw`
# MAGIC
# MAGIC > **Bronze principle:** Preserve raw structure. NO cleaning here. Just land + timestamp + dedupe-on-PK.
# MAGIC
# MAGIC > **Failure isolation:** Each table runs through `safe_run()` so one bad CSV doesn't kill the others. Notebook fails overall at the end if any table failed.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Schemas
# MAGIC Doing this for bundling purposes. You never know who might need to install from scratch! It's expected to be working in existing schemas or only creating what you need.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS databricks_clar.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS databricks_clar.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS databricks_clar.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters
# MAGIC I like to set these up so I'm not doing a lot of typing and keeps things clean.

# COMMAND ----------

BRONZE_VOLUME_PATH = "/Volumes/databricks_clar/bronze/bronze_volume/banking_data"
BRONZE_SCHEMA      = "databricks_clar.bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions
# MAGIC
# MAGIC Three helpers power this notebook:
# MAGIC - `assert_source_exists` — pre-flight check before any reads
# MAGIC - `upsert_bronze` — idempotent MERGE into Delta (creates table on first run)
# MAGIC - `safe_run` — wraps each table's logic so one failure doesn't kill the rest

# COMMAND ----------

# MAGIC %md
# MAGIC ### Source Check

# COMMAND ----------

def assert_source_exists(volume_path: str, expected_files: list):
    """Pre-flight check: confirm source data is present before reading."""
    for f in expected_files:
        full_path = f"{volume_path}/{f}/"
        try:
            # dbutils throws if path doesn't exist
            files = dbutils.fs.ls(full_path)
            if not files:
                raise FileNotFoundError(f"Source folder is empty: {full_path}")
        except Exception as e:
            raise RuntimeError(
                f"PRE-FLIGHT FAILED: {full_path} not accessible. "
                f"Check Volume mount and CSV uploads. Original error: {e}"
            )
    print(f"[PRE-FLIGHT OK] All {len(expected_files)} source paths verified.")

# Run guard
assert_source_exists(
    BRONZE_VOLUME_PATH,
    ["customers", "accounts", "transactions", "branches"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upsert Helper

# COMMAND ----------

def upsert_bronze(df, table_fqn: str, pk_cols: list):
    """Idempotent MERGE — creates table on first run, upserts on PK after."""
    if spark.catalog.tableExists(table_fqn):
        merge_cond = " AND ".join([f"trg.{c} = src.{c}" for c in pk_cols])
        dt = DeltaTable.forName(spark, table_fqn)
        (dt.alias("trg")
           .merge(df.alias("src"), merge_cond)
           .whenMatchedUpdateAll()
           .whenNotMatchedInsertAll()
           .execute())
        print(f"[MERGE]  {table_fqn}  on  {pk_cols}")
    else:
        (df.write.format("delta")
           .mode("append")
           .saveAsTable(table_fqn))
        print(f"[CREATE] {table_fqn}  (first run)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Safe Run Wrapper

# COMMAND ----------

# Tracks failures across the run — populated by safe_run, checked at the end
run_errors = []

def safe_run(table_label: str, fn):
    """Execute a table's logic; capture failures so other tables still run."""
    try:
        fn()
        print(f"[OK]   {table_label}")
    except Exception as e:
        run_errors.append((table_label, str(e)))
        print(f"[FAIL] {table_label} — {type(e).__name__}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-flight Check
# MAGIC
# MAGIC Verify all 4 source folders exist and contain files before any processing begins.

# COMMAND ----------

assert_source_exists(
    BRONZE_VOLUME_PATH,
    ["customers", "accounts", "transactions", "branches"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-Table Processing Functions
# MAGIC
# MAGIC Each table's logic is wrapped in its own function so `safe_run` can isolate failures.

# COMMAND ----------

def process_customers():
    df = (spark.read.format("csv")
          .option("header", True)
          .option("inferSchema", True)
          .load(f"{BRONZE_VOLUME_PATH}/customers/")
          .withColumn("processDate", current_timestamp()))
    upsert_bronze(df, f"{BRONZE_SCHEMA}.banking_customers_raw", ["customer_id"])

def process_accounts():
    df = (spark.read.format("csv")
          .option("header", True)
          .option("inferSchema", True)
          .load(f"{BRONZE_VOLUME_PATH}/accounts/")
          .withColumn("processDate", current_timestamp()))
    upsert_bronze(df, f"{BRONZE_SCHEMA}.banking_accounts_raw", ["account_id"])

def process_transactions():
    df = (spark.read.format("csv")
          .option("header", True)
          .option("inferSchema", True)
          .load(f"{BRONZE_VOLUME_PATH}/transactions/")
          .withColumn("processDate", current_timestamp()))
    upsert_bronze(df, f"{BRONZE_SCHEMA}.banking_transactions_raw", ["transaction_id"])

def process_branches():
    df = (spark.read.format("csv")
          .option("header", True)
          .option("inferSchema", True)
          .load(f"{BRONZE_VOLUME_PATH}/branches/")
          .withColumn("processDate", current_timestamp()))
    upsert_bronze(df, f"{BRONZE_SCHEMA}.banking_branches_raw", ["branch_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Pipeline (with isolation)
# MAGIC
# MAGIC Each table runs through `safe_run()`. Failures are captured but don't stop subsequent tables.

# COMMAND ----------

safe_run("customers",    process_customers)
safe_run("accounts",     process_accounts)
safe_run("transactions", process_transactions)
safe_run("branches",     process_branches)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Run Report
# MAGIC
# MAGIC If any table failed, the notebook fails overall here so the orchestrator knows.
# MAGIC Successful tables stay loaded — re-run only what failed.

# COMMAND ----------

if run_errors:
    print(f"\n[RUN FAILED] {len(run_errors)} table(s) failed:")
    for tbl, err in run_errors:
        print(f"  - {tbl}: {err}")
    raise RuntimeError(f"Bronze run had {len(run_errors)} failure(s). See log above.")
else:
    print(f"\n[RUN OK] All 4 tables processed successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation — Row Counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'banking_customers_raw'    AS table_name, COUNT(*) AS row_count FROM databricks_clar.bronze.banking_customers_raw
# MAGIC UNION ALL
# MAGIC SELECT 'banking_accounts_raw',     COUNT(*) FROM databricks_clar.bronze.banking_accounts_raw
# MAGIC UNION ALL
# MAGIC SELECT 'banking_transactions_raw', COUNT(*) FROM databricks_clar.bronze.banking_transactions_raw
# MAGIC UNION ALL
# MAGIC SELECT 'banking_branches_raw',     COUNT(*) FROM databricks_clar.bronze.banking_branches_raw
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Expected Output
# MAGIC
# MAGIC When this notebook runs successfully, you should see:
# MAGIC
# MAGIC ### Pre-flight check (Cell 10)
# MAGIC ```
# MAGIC [PRE-FLIGHT OK] All 4 source paths verified.
# MAGIC ```
# MAGIC
# MAGIC ### Per-table execution (Cell 14)
# MAGIC
# MAGIC **First run:**
# MAGIC ```
# MAGIC [CREATE] databricks_clar.bronze.banking_customers_raw  (first run)
# MAGIC [OK]   customers
# MAGIC [CREATE] databricks_clar.bronze.banking_accounts_raw   (first run)
# MAGIC [OK]   accounts
# MAGIC [CREATE] databricks_clar.bronze.banking_transactions_raw  (first run)
# MAGIC [OK]   transactions
# MAGIC [CREATE] databricks_clar.bronze.banking_branches_raw   (first run)
# MAGIC [OK]   branches
# MAGIC ```
# MAGIC
# MAGIC **Subsequent runs (idempotent):**
# MAGIC ```
# MAGIC [MERGE]  databricks_clar.bronze.banking_customers_raw  on  ['customer_id']
# MAGIC [OK]   customers
# MAGIC [MERGE]  databricks_clar.bronze.banking_accounts_raw   on  ['account_id']
# MAGIC [OK]   accounts
# MAGIC [MERGE]  databricks_clar.bronze.banking_transactions_raw  on  ['transaction_id']
# MAGIC [OK]   transactions
# MAGIC [MERGE]  databricks_clar.bronze.banking_branches_raw   on  ['branch_id']
# MAGIC [OK]   branches
# MAGIC ```
# MAGIC
# MAGIC ### Final report (Cell 16)
# MAGIC ```
# MAGIC [RUN OK] All 4 tables processed successfully.
# MAGIC ```
# MAGIC
# MAGIC ### Validation query (Cell 18)
# MAGIC
# MAGIC | table_name | row_count |
# MAGIC |---|---|
# MAGIC | banking_accounts_raw | 150 |
# MAGIC | banking_branches_raw | 15 |
# MAGIC | banking_customers_raw | 100 |
# MAGIC | banking_transactions_raw | 800 |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What a partial failure looks like
# MAGIC
# MAGIC If `accounts/accounts.csv` is corrupted but the others are fine:
# MAGIC
# MAGIC ```
# MAGIC [PRE-FLIGHT OK] All 4 source paths verified.
# MAGIC [CREATE] databricks_clar.bronze.banking_customers_raw  (first run)
# MAGIC [OK]   customers
# MAGIC [FAIL] accounts — AnalysisException: malformed CSV
# MAGIC [CREATE] databricks_clar.bronze.banking_transactions_raw  (first run)
# MAGIC [OK]   transactions
# MAGIC [CREATE] databricks_clar.bronze.banking_branches_raw   (first run)
# MAGIC [OK]   branches
# MAGIC
# MAGIC [RUN FAILED] 1 table(s) failed:
# MAGIC   - accounts: AnalysisException: malformed CSV
# MAGIC RuntimeError: Bronze run had 1 failure(s). See log above.
# MAGIC ```
# MAGIC
# MAGIC 3 of 4 tables loaded. Fix `accounts.csv` and re-run — MERGE handles the recovery cleanly.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Troubleshooting
# MAGIC
# MAGIC | Issue | Likely cause | Fix |
# MAGIC |---|---|---|
# MAGIC | `PRE-FLIGHT FAILED` | CSVs not uploaded | Verify files at `/Volumes/databricks_clar/bronze/bronze_volume/banking_data/<table>/` |
# MAGIC | `[FAIL] <table>` mid-run | Bad CSV, schema issue, or upstream change | Check the captured error message; re-run after fix |
# MAGIC | `Permission denied` on CREATE SCHEMA | Missing UC privileges | Need `USE CATALOG` + `CREATE SCHEMA` on `databricks_clar` |
# MAGIC | Row counts don't match | Partial CSV upload | Re-upload and re-run; MERGE is idempotent |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Next Step
# MAGIC
# MAGIC Run **`2_silver_BankingSample`** next — cleans, enriches, and prepares data for joins.