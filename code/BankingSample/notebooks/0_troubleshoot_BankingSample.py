# Databricks notebook source
# MAGIC %md
# MAGIC # BankingSample - Pipeline Diagnostics
# MAGIC
# MAGIC Run this notebook on demand when something looks off in Bronze, Silver, or Gold.
# MAGIC
# MAGIC **Purpose:** Detect, diagnose, and report issues across all three layers.
# MAGIC
# MAGIC **Checks performed:**
# MAGIC 1. Object existence (tables and views in all 3 schemas)
# MAGIC 2. Row count parity (Bronze vs Silver, Silver vs Gold)
# MAGIC 3. Freshness (when was each table last written?)
# MAGIC 4. Null/empty checks on critical columns
# MAGIC 5. Referential integrity (orphan FKs)
# MAGIC 6. PII leakage check (no clear-text in Gold)
# MAGIC 7. Delta health (file count, history depth)
# MAGIC
# MAGIC **How to use:** Run All. Any cell with red output = investigate that area.

# COMMAND ----------

from pyspark.sql.functions import col, count, sum as spark_sum, max as spark_max, current_timestamp

CATALOG = "databricks_clar"
BRONZE_SCHEMA = f"{CATALOG}.bronze"
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA   = f"{CATALOG}.gold"

print(f"Diagnostics run at: {spark.sql('SELECT current_timestamp()').collect()[0][0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 1: Object existence across all layers
# MAGIC
# MAGIC If any expected object is missing, the corresponding pipeline layer never finished.

# COMMAND ----------

expected_objects = [
    # Bronze
    ("bronze", "banking_customers_raw"),
    ("bronze", "banking_accounts_raw"),
    ("bronze", "banking_transactions_raw"),
    ("bronze", "banking_branches_raw"),
    # Silver
    ("silver", "banking_customers_clean"),
    ("silver", "banking_accounts_clean"),
    ("silver", "banking_transactions_clean"),
    ("silver", "banking_branches_clean"),
    # Gold
    ("gold", "transactions_enriched"),
    ("gold", "txn_summary_by_region"),
    ("gold", "customer_lifetime_value"),
    ("gold", "dormant_account_alerts"),
    ("gold", "customers_secure_v"),
]

results = []
for schema, obj_name in expected_objects:
    fqn = f"{CATALOG}.{schema}.{obj_name}"
    exists = spark.catalog.tableExists(fqn)
    status = "OK" if exists else "MISSING"
    results.append((schema, obj_name, status))

for schema, obj_name, status in results:
    marker = "  " if status == "OK" else ">>"
    print(f"{marker} [{status:7}] {schema}.{obj_name}")

missing = [r for r in results if r[2] == "MISSING"]
print(f"\nSummary: {len(results) - len(missing)}/{len(results)} objects present")
if missing:
    print(f"ACTION REQUIRED: Re-run the notebook(s) that produce: {[m[1] for m in missing]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 2: Row count parity across layers
# MAGIC
# MAGIC Silver and Bronze should match. Gold fact (`transactions_enriched`) should match Silver transactions.
# MAGIC Mismatches indicate filters dropped rows, joins lost rows, or upstream layer is stale.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH counts AS (
# MAGIC   SELECT 'customers'    AS entity,
# MAGIC          (SELECT COUNT(*) FROM databricks_clar.bronze.banking_customers_raw)    AS bronze,
# MAGIC          (SELECT COUNT(*) FROM databricks_clar.silver.banking_customers_clean)  AS silver,
# MAGIC          NULL AS gold
# MAGIC   UNION ALL
# MAGIC   SELECT 'accounts',
# MAGIC          (SELECT COUNT(*) FROM databricks_clar.bronze.banking_accounts_raw),
# MAGIC          (SELECT COUNT(*) FROM databricks_clar.silver.banking_accounts_clean),
# MAGIC          NULL
# MAGIC   UNION ALL
# MAGIC   SELECT 'transactions',
# MAGIC          (SELECT COUNT(*) FROM databricks_clar.bronze.banking_transactions_raw),
# MAGIC          (SELECT COUNT(*) FROM databricks_clar.silver.banking_transactions_clean),
# MAGIC          (SELECT COUNT(*) FROM databricks_clar.gold.transactions_enriched)
# MAGIC   UNION ALL
# MAGIC   SELECT 'branches',
# MAGIC          (SELECT COUNT(*) FROM databricks_clar.bronze.banking_branches_raw),
# MAGIC          (SELECT COUNT(*) FROM databricks_clar.silver.banking_branches_clean),
# MAGIC          NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   entity,
# MAGIC   bronze,
# MAGIC   silver,
# MAGIC   gold,
# MAGIC   CASE
# MAGIC     WHEN bronze = silver AND (gold IS NULL OR silver = gold) THEN 'OK'
# MAGIC     WHEN bronze != silver                                    THEN 'BRONZE_SILVER_MISMATCH'
# MAGIC     WHEN gold IS NOT NULL AND silver != gold                 THEN 'SILVER_GOLD_MISMATCH'
# MAGIC     ELSE 'CHECK_MANUALLY'
# MAGIC   END AS status
# MAGIC FROM counts
# MAGIC ORDER BY entity;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 3: Freshness - when was each table last written?
# MAGIC
# MAGIC Stale tables = pipeline didn't run, or only some layers ran.
# MAGIC Look for tables where `processDate` is older than expected.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bronze.banking_customers_raw'        AS table_name, MAX(processDate) AS last_updated FROM databricks_clar.bronze.banking_customers_raw
# MAGIC UNION ALL
# MAGIC SELECT 'bronze.banking_accounts_raw',        MAX(processDate) FROM databricks_clar.bronze.banking_accounts_raw
# MAGIC UNION ALL
# MAGIC SELECT 'bronze.banking_transactions_raw',    MAX(processDate) FROM databricks_clar.bronze.banking_transactions_raw
# MAGIC UNION ALL
# MAGIC SELECT 'bronze.banking_branches_raw',        MAX(processDate) FROM databricks_clar.bronze.banking_branches_raw
# MAGIC UNION ALL
# MAGIC SELECT 'silver.banking_customers_clean',     MAX(processDate) FROM databricks_clar.silver.banking_customers_clean
# MAGIC UNION ALL
# MAGIC SELECT 'silver.banking_accounts_clean',      MAX(processDate) FROM databricks_clar.silver.banking_accounts_clean
# MAGIC UNION ALL
# MAGIC SELECT 'silver.banking_transactions_clean',  MAX(processDate) FROM databricks_clar.silver.banking_transactions_clean
# MAGIC UNION ALL
# MAGIC SELECT 'silver.banking_branches_clean',      MAX(processDate) FROM databricks_clar.silver.banking_branches_clean
# MAGIC UNION ALL
# MAGIC SELECT 'gold.transactions_enriched',         MAX(processDate) FROM databricks_clar.gold.transactions_enriched
# MAGIC ORDER BY last_updated;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 4: Null and empty checks on critical columns
# MAGIC
# MAGIC Catches data quality issues that would break downstream joins or aggregations.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'customers' AS entity,
# MAGIC        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)                 AS null_pks,
# MAGIC        SUM(CASE WHEN email IS NULL OR TRIM(email) = '' THEN 1 ELSE 0 END)   AS null_emails,
# MAGIC        SUM(CASE WHEN ssn IS NULL OR TRIM(ssn) = '' THEN 1 ELSE 0 END)       AS null_ssns,
# MAGIC        COUNT(*) AS total_rows
# MAGIC FROM databricks_clar.silver.banking_customers_clean
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'accounts',
# MAGIC        SUM(CASE WHEN account_id IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN balance IS NULL THEN 1 ELSE 0 END),
# MAGIC        COUNT(*)
# MAGIC FROM databricks_clar.silver.banking_accounts_clean
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'transactions',
# MAGIC        SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN account_id IS NULL THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END),
# MAGIC        COUNT(*)
# MAGIC FROM databricks_clar.silver.banking_transactions_clean;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 5: Referential integrity (orphan FKs)
# MAGIC
# MAGIC Catches rows in child tables pointing to non-existent parent keys.
# MAGIC Common cause: incremental loads where parent didn't update before child.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Accounts pointing to non-existent customers
# MAGIC SELECT 'accounts -> customers' AS relationship,
# MAGIC        COUNT(*) AS orphan_count
# MAGIC FROM databricks_clar.silver.banking_accounts_clean a
# MAGIC LEFT ANTI JOIN databricks_clar.silver.banking_customers_clean c
# MAGIC   ON a.customer_id = c.customer_id
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Accounts pointing to non-existent branches
# MAGIC SELECT 'accounts -> branches',
# MAGIC        COUNT(*)
# MAGIC FROM databricks_clar.silver.banking_accounts_clean a
# MAGIC LEFT ANTI JOIN databricks_clar.silver.banking_branches_clean b
# MAGIC   ON a.branch_id = b.branch_id
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- Transactions pointing to non-existent accounts
# MAGIC SELECT 'transactions -> accounts',
# MAGIC        COUNT(*)
# MAGIC FROM databricks_clar.silver.banking_transactions_clean t
# MAGIC LEFT ANTI JOIN databricks_clar.silver.banking_accounts_clean a
# MAGIC   ON t.account_id = a.account_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 6: PII leakage check
# MAGIC
# MAGIC Confirms no clear-text PII appears in Gold.
# MAGIC Critical for compliance - this check should NEVER fail in production.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   -- Should be 0 - all SSNs in Gold should start with 'XXX-XX-'
# MAGIC   SUM(CASE WHEN ssn NOT LIKE 'XXX-XX-%' THEN 1 ELSE 0 END) AS unmasked_ssn_count,
# MAGIC
# MAGIC   -- Should be 0 - all emails should contain '***@'
# MAGIC   SUM(CASE WHEN email NOT LIKE '%***@%' THEN 1 ELSE 0 END) AS unmasked_email_count,
# MAGIC
# MAGIC   -- Should be 0 - all phones should start with '(***)'
# MAGIC   SUM(CASE WHEN phone NOT LIKE '(***)%' THEN 1 ELSE 0 END) AS unmasked_phone_count,
# MAGIC
# MAGIC   COUNT(*) AS total_rows_checked
# MAGIC FROM databricks_clar.gold.customers_secure_v;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 7: Delta table health
# MAGIC
# MAGIC Counts files and history depth. Too many small files = OPTIMIZE candidate.
# MAGIC Excessive history = VACUUM candidate.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- File count and recent history for the largest table
# MAGIC DESCRIBE DETAIL databricks_clar.silver.banking_transactions_clean;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recent operations on transactions table (last 5 commits)
# MAGIC DESCRIBE HISTORY databricks_clar.silver.banking_transactions_clean LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagnostic Summary
# MAGIC
# MAGIC If all checks above show green/zero/OK:
# MAGIC - Pipeline is healthy
# MAGIC - Data quality is intact
# MAGIC - PII governance is enforced
# MAGIC
# MAGIC If any check failed:
# MAGIC
# MAGIC | Check | Failure means | Recovery |
# MAGIC |---|---|---|
# MAGIC | 1. Existence | A layer didn't complete | Re-run the missing layer's notebook |
# MAGIC | 2. Row counts | Filter or join issue | Investigate the layer where counts diverge |
# MAGIC | 3. Freshness | Layer is stale | Re-run from the oldest layer downward |
# MAGIC | 4. Nulls | Bad source data | Check Bronze, fix at source if possible |
# MAGIC | 5. Orphans | FK integrity broken | Reload parent before child |
# MAGIC | 6. PII leak | UDF or view broken | Re-run Gold UDFs and view |
# MAGIC | 7. Delta health | Performance degradation | Run OPTIMIZE / VACUUM as needed |
# MAGIC
# MAGIC **Recovery cheat sheet:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Restore a table to a prior version (Time Travel)
# MAGIC RESTORE TABLE databricks_clar.silver.banking_customers_clean TO VERSION AS OF 5;
# MAGIC
# MAGIC -- See available versions
# MAGIC DESCRIBE HISTORY databricks_clar.silver.banking_customers_clean;
# MAGIC
# MAGIC -- Optimize a table with too many small files
# MAGIC OPTIMIZE databricks_clar.silver.banking_transactions_clean;
# MAGIC
# MAGIC -- Clean up old files (production: retain at least 7 days)
# MAGIC VACUUM databricks_clar.silver.banking_transactions_clean RETAIN 168 HOURS;
# MAGIC ```