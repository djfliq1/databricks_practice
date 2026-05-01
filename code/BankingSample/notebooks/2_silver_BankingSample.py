# Databricks notebook source
# MAGIC %md
# MAGIC # BankingSample — Silver Layer
# MAGIC
# MAGIC **Pipeline:** BankingSample | **Layer:** Silver | **Catalog:** `databricks_clar`
# MAGIC
# MAGIC ## Overview
# MAGIC 1. Pre-flight: confirm Bronze tables exist
# MAGIC 2. Read Bronze tables
# MAGIC 3. Clean (trim, UPPER, null handling, type casting)
# MAGIC 4. Enrich (derived columns: `full_name`, `email_domain`, `age`, etc.)
# MAGIC 5. Upsert to Silver Delta tables — idempotent re-runs
# MAGIC
# MAGIC **Silver tables produced:**
# MAGIC - `databricks_clar.silver.banking_customers_clean`
# MAGIC - `databricks_clar.silver.banking_accounts_clean`
# MAGIC - `databricks_clar.silver.banking_transactions_clean`
# MAGIC - `databricks_clar.silver.banking_branches_clean`
# MAGIC
# MAGIC > **Silver principle:** Trustworthy, queryable, joined-ready data. Still has PII in clear — masking happens in Gold.
# MAGIC
# MAGIC > **Failure isolation:** Each table runs through `safe_run()` so one bad transform doesn't kill the others. Notebook fails overall at the end if any table failed.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

from pyspark.sql.functions import (
    col, upper, trim, lower, current_timestamp,
    split, concat_ws, datediff, current_date,
    floor, year, month, when, to_timestamp, to_date,
    date_format
)
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters

# COMMAND ----------

BRONZE_SCHEMA = "databricks_clar.bronze"
SILVER_SCHEMA = "databricks_clar.silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions
# MAGIC
# MAGIC Three helpers power this notebook:
# MAGIC - `assert_bronze_tables_exist` — pre-flight check before any reads
# MAGIC - `upsert_silver` — idempotent MERGE into Delta (creates table on first run)
# MAGIC - `safe_run` — wraps each table's logic so one failure doesn't kill the rest

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pre-flights

# COMMAND ----------

def assert_bronze_tables_exist(schema: str, expected_tables: list):
    """Pre-flight check: confirm Bronze tables are present before transforming."""
    missing = []
    for tbl in expected_tables:
        fqn = f"{schema}.{tbl}"
        if not spark.catalog.tableExists(fqn):
            missing.append(fqn)
    if missing:
        raise RuntimeError(
            f"PRE-FLIGHT FAILED: {len(missing)} Bronze table(s) missing: {missing}. "
            f"Run 1_bronze_BankingSample first."
        )
    print(f"[PRE-FLIGHT OK] All {len(expected_tables)} Bronze tables verified.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upsert Helper

# COMMAND ----------

def upsert_silver(df, table_fqn: str, pk_cols: list):
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
# MAGIC Verify all 4 Bronze tables exist before transforming.

# COMMAND ----------

assert_bronze_tables_exist(
    BRONZE_SCHEMA,
    [
        "banking_customers_raw",
        "banking_accounts_raw",
        "banking_transactions_raw",
        "banking_branches_raw"
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-Table Processing Functions
# MAGIC
# MAGIC Each table's transform + upsert logic lives in its own function so `safe_run` can isolate failures.
# MAGIC
# MAGIC **What each function does:**
# MAGIC
# MAGIC | Table | Cleanups | New Columns |
# MAGIC |---|---|---|
# MAGIC | `customers` | UPPER names, lowercase email, UPPER city/state | `full_name`, `email_domain`, `age` |
# MAGIC | `accounts` | UPPER status, account_type | `account_age_days`, `is_active` |
# MAGIC | `transactions` | Cast date, UPPER merchant/type/channel, null merchants → "UNKNOWN" | `is_credit`, `txn_year_month` |
# MAGIC | `branches` | UPPER branch_name, city, state, region | (none — just standardization) |

# COMMAND ----------

from pyspark.sql.functions import (
    col, upper, trim, lower, current_timestamp,
    split, concat_ws, datediff, current_date,
    floor, year, month, when, to_timestamp, to_date,
    date_format
)

def process_customers():
    df_bronze = spark.read.table(f"{BRONZE_SCHEMA}.banking_customers_raw")
    df_silver = (df_bronze
        # Clean: standardize casing
        .withColumn("first_name",  upper(trim(col("first_name"))))
        .withColumn("last_name",   upper(trim(col("last_name"))))
        .withColumn("email",       lower(trim(col("email"))))
        .withColumn("city",        upper(trim(col("city"))))
        .withColumn("state",       upper(trim(col("state"))))
        # Enrich: derived columns
        .withColumn("full_name",     concat_ws(" ", col("first_name"), col("last_name")))
        .withColumn("email_domain",  split(col("email"), "@")[1])
        .withColumn("age",           floor(datediff(current_date(), col("date_of_birth")) / 365.25).cast("int"))
        # Refresh processDate for Silver lineage
        .withColumn("processDate",   current_timestamp())
    )
    upsert_silver(df_silver, f"{SILVER_SCHEMA}.banking_customers_clean", ["customer_id"])


def process_accounts():
    df_bronze = spark.read.table(f"{BRONZE_SCHEMA}.banking_accounts_raw")
    df_silver = (df_bronze
        # Clean
        .withColumn("account_type", upper(trim(col("account_type"))))
        .withColumn("status",       upper(trim(col("status"))))
        # Enrich
        .withColumn("account_age_days", datediff(current_date(), col("open_date")))
        .withColumn("is_active",        when(col("status") == "ACTIVE", True).otherwise(False))
        .withColumn("processDate",      current_timestamp())
    )
    upsert_silver(df_silver, f"{SILVER_SCHEMA}.banking_accounts_clean", ["account_id"])


def process_transactions():
    df_bronze = spark.read.table(f"{BRONZE_SCHEMA}.banking_transactions_raw")
    df_silver = (df_bronze
        # Clean: cast string ISO timestamp -> real timestamp
        .withColumn("transaction_date", to_timestamp(col("transaction_date")))
        # Clean: standardize merchant (handles '  AMAZON  ' and nulls)
        .withColumn("merchant",
            when((col("merchant").isNull()) | (trim(col("merchant")) == ""), "UNKNOWN")
            .otherwise(upper(trim(col("merchant"))))
        )
        .withColumn("transaction_type", upper(trim(col("transaction_type"))))
        .withColumn("channel",          upper(trim(col("channel"))))
        # Enrich
        .withColumn("is_credit",        when(col("amount") > 0, True).otherwise(False))
        .withColumn("txn_year_month",   date_format(col("transaction_date"), "yyyy-MM"))
        .withColumn("processDate",      current_timestamp())
    )
    upsert_silver(df_silver, f"{SILVER_SCHEMA}.banking_transactions_clean", ["transaction_id"])


def process_branches():
    df_bronze = spark.read.table(f"{BRONZE_SCHEMA}.banking_branches_raw")
    df_silver = (df_bronze
        .withColumn("branch_name", upper(trim(col("branch_name"))))
        .withColumn("city",        upper(trim(col("city"))))
        .withColumn("state",       upper(trim(col("state"))))
        .withColumn("region",      upper(trim(col("region"))))
        .withColumn("processDate", current_timestamp())
    )
    upsert_silver(df_silver, f"{SILVER_SCHEMA}.banking_branches_clean", ["branch_id"])

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
    raise RuntimeError(f"Silver run had {len(run_errors)} failure(s). See log above.")
else:
    print(f"\n[RUN OK] All 4 tables processed successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation — Silver Row Counts & Quality Checks

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row Counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'banking_customers_clean'    AS table_name, COUNT(*) AS row_count FROM databricks_clar.silver.banking_customers_clean
# MAGIC UNION ALL
# MAGIC SELECT 'banking_accounts_clean',     COUNT(*) FROM databricks_clar.silver.banking_accounts_clean
# MAGIC UNION ALL
# MAGIC SELECT 'banking_transactions_clean', COUNT(*) FROM databricks_clar.silver.banking_transactions_clean
# MAGIC UNION ALL
# MAGIC SELECT 'banking_branches_clean',     COUNT(*) FROM databricks_clar.silver.banking_branches_clean
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quality checks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Should be 0 dirty merchants left (no leading/trailing spaces, no nulls)
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN merchant != TRIM(merchant)        THEN 1 ELSE 0 END) AS dirty_whitespace,
# MAGIC   SUM(CASE WHEN merchant IS NULL OR merchant = '' THEN 1 ELSE 0 END) AS null_or_empty,
# MAGIC   COUNT(DISTINCT merchant)                                            AS unique_merchants,
# MAGIC   COUNT(*)                                                            AS total_rows
# MAGIC FROM databricks_clar.silver.banking_transactions_clean;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sanity check - a peek at the data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm enrichments landed cleanly
# MAGIC SELECT customer_id, full_name, email, email_domain, age, state
# MAGIC FROM databricks_clar.silver.banking_customers_clean
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Expected Output
# MAGIC
# MAGIC When this notebook runs successfully, you should see:
# MAGIC
# MAGIC ### Pre-flight check (Cell 9)
# MAGIC ```
# MAGIC [PRE-FLIGHT OK] All 4 Bronze tables verified.
# MAGIC ```
# MAGIC
# MAGIC ### Per-table execution (Cell 13)
# MAGIC
# MAGIC **First run:**
# MAGIC ```
# MAGIC [CREATE] databricks_clar.silver.banking_customers_clean  (first run)
# MAGIC [OK]   customers
# MAGIC [CREATE] databricks_clar.silver.banking_accounts_clean   (first run)
# MAGIC [OK]   accounts
# MAGIC [CREATE] databricks_clar.silver.banking_transactions_clean  (first run)
# MAGIC [OK]   transactions
# MAGIC [CREATE] databricks_clar.silver.banking_branches_clean   (first run)
# MAGIC [OK]   branches
# MAGIC ```
# MAGIC
# MAGIC **Subsequent runs (idempotent):**
# MAGIC ```
# MAGIC [MERGE]  databricks_clar.silver.banking_customers_clean  on  ['customer_id']
# MAGIC [OK]   customers
# MAGIC [MERGE]  databricks_clar.silver.banking_accounts_clean   on  ['account_id']
# MAGIC [OK]   accounts
# MAGIC [MERGE]  databricks_clar.silver.banking_transactions_clean  on  ['transaction_id']
# MAGIC [OK]   transactions
# MAGIC [MERGE]  databricks_clar.silver.banking_branches_clean   on  ['branch_id']
# MAGIC [OK]   branches
# MAGIC ```
# MAGIC
# MAGIC ### Final report (Cell 15)
# MAGIC ```
# MAGIC [RUN OK] All 4 tables processed successfully.
# MAGIC ```
# MAGIC
# MAGIC ### Row count validation (Cell 17)
# MAGIC
# MAGIC | table_name | row_count |
# MAGIC |---|---|
# MAGIC | banking_accounts_clean | 150 |
# MAGIC | banking_branches_clean | 15 |
# MAGIC | banking_customers_clean | 100 |
# MAGIC | banking_transactions_clean | 800 |
# MAGIC
# MAGIC > Silver should match Bronze counts — we're enriching, not filtering. If counts differ, something dropped rows unintentionally.
# MAGIC
# MAGIC ### Quality check (Cell 18)
# MAGIC
# MAGIC | dirty_whitespace | null_or_empty | unique_merchants | total_rows |
# MAGIC |---|---|---|---|
# MAGIC | 0 | 0 | ~13 | 800 |
# MAGIC
# MAGIC > Zero dirty whitespace and zero null/empty merchants confirms the cleanup logic worked.
# MAGIC
# MAGIC ### Sanity peek (Cell 19)
# MAGIC
# MAGIC 10 rows showing:
# MAGIC - `full_name` populated (e.g., `"DONNA GREEN"`)
# MAGIC - `email` lowercase (e.g., `"donna.green80@proton.me"`)
# MAGIC - `email_domain` extracted (e.g., `"proton.me"`)
# MAGIC - `age` calculated as integer
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What a partial failure looks like
# MAGIC
# MAGIC If transactions has a transform bug but the others are fine:
# MAGIC
# MAGIC ```
# MAGIC [PRE-FLIGHT OK] All 4 Bronze tables verified.
# MAGIC [CREATE] databricks_clar.silver.banking_customers_clean  (first run)
# MAGIC [OK]   customers
# MAGIC [CREATE] databricks_clar.silver.banking_accounts_clean   (first run)
# MAGIC [OK]   accounts
# MAGIC [FAIL] transactions — AnalysisException: cannot resolve 'merchant' given input columns
# MAGIC [CREATE] databricks_clar.silver.banking_branches_clean   (first run)
# MAGIC [OK]   branches
# MAGIC
# MAGIC [RUN FAILED] 1 table(s) failed:
# MAGIC   - transactions: AnalysisException: cannot resolve 'merchant'
# MAGIC RuntimeError: Silver run had 1 failure(s). See log above.
# MAGIC ```
# MAGIC
# MAGIC 3 of 4 tables loaded. Fix the transform and re-run — MERGE handles the recovery.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Troubleshooting
# MAGIC
# MAGIC | Issue | Likely cause | Fix |
# MAGIC |---|---|---|
# MAGIC | `PRE-FLIGHT FAILED` | Bronze didn't run or partially failed | Run `1_bronze_BankingSample` first; verify all 4 raw tables exist |
# MAGIC | `[FAIL] <table>` mid-run | Schema drift, type mismatch, bad transform | Check captured error; fix transform in `process_<table>()`; re-run |
# MAGIC | Row counts lower than Bronze | Filter accidentally added | Review `process_*()` — should NOT have `.filter()` calls |
# MAGIC | `transaction_date` still string | `to_timestamp` failed silently | Check format string in `process_transactions` |
# MAGIC | `is_active` is all `False` | Status values not matching `"ACTIVE"` exactly | UPPER + TRIM should fix it; verify Bronze data |
# MAGIC | `NameError: name '<func>' is not defined` | Notebook state lost | Re-run from top, or check Cell 11 imports |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Next Step
# MAGIC
# MAGIC Run **`3_gold_BankingSample`** next — applies PII masking, builds aggregated marts, and creates business-ready views for dashboards.