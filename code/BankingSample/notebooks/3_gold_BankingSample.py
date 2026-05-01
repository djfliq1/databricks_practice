# Databricks notebook source
# MAGIC %md
# MAGIC # BankingSample — Gold Layer
# MAGIC
# MAGIC **Pipeline:** BankingSample | **Layer:** Gold | **Catalog:** `databricks_clar`
# MAGIC
# MAGIC 1. Create UC SQL UDFs for PII masking
# MAGIC 2. Build secure views (mask PII for analysts)
# MAGIC 3. Build the **star schema fact table** — `transactions_enriched`
# MAGIC 4. Build aggregated marts for dashboards
# MAGIC
# MAGIC **Gold artifacts produced:**
# MAGIC
# MAGIC **Secure Views (mask PII):**
# MAGIC - `databricks_clar.gold.customers_secure_v`
# MAGIC
# MAGIC **Star Schema Fact:**
# MAGIC - `databricks_clar.gold.transactions_enriched`
# MAGIC
# MAGIC **Aggregated Marts:**
# MAGIC - `databricks_clar.gold.txn_summary_by_region`
# MAGIC - `databricks_clar.gold.customer_lifetime_value`
# MAGIC - `databricks_clar.gold.dormant_account_alerts`
# MAGIC
# MAGIC > **Gold principle (For me to remember):** Business-ready, governed, query-optimized. PII never appears in clear text in Gold. I didn't have to worry about this much in my last role as I dealt mostly with engine data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters

# COMMAND ----------

SILVER_SCHEMA = "databricks_clar.silver"
GOLD_SCHEMA   = "databricks_clar.gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. PII Masking Functions (Unity Catalog SQL UDFs)
# MAGIC
# MAGIC These functions live in `databricks_clar.gold` and can be called from any SQL query in this catalog.
# MAGIC
# MAGIC **Performance ranking:** SQL UDFs > Python UDFs (cert exam fact)
# MAGIC **Permissions required:** `USE CATALOG` + `USE SCHEMA` + `EXECUTE`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the masking UDFs

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Mask SSN: keep only last 4 digits
# MAGIC CREATE OR REPLACE FUNCTION databricks_clar.gold.mask_ssn(ssn STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CONCAT('XXX-XX-', RIGHT(ssn, 4));
# MAGIC
# MAGIC -- Mask email: keep first 2 chars + domain
# MAGIC CREATE OR REPLACE FUNCTION databricks_clar.gold.mask_email(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CONCAT(LEFT(email, 2), '***@', SPLIT(email, '@')[1]);
# MAGIC
# MAGIC -- Mask phone: keep only last 4 digits
# MAGIC CREATE OR REPLACE FUNCTION databricks_clar.gold.mask_phone(phone STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CONCAT('(***) ***-', RIGHT(phone, 4));
# MAGIC
# MAGIC -- Mask account number: keep only last 4 digits
# MAGIC CREATE OR REPLACE FUNCTION databricks_clar.gold.mask_account(acct STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CONCAT('********', RIGHT(acct, 4));
# MAGIC
# MAGIC -- Mask address: drop street number, keep street name only
# MAGIC CREATE OR REPLACE FUNCTION databricks_clar.gold.mask_address(addr STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN REGEXP_REPLACE(addr, '^[0-9]+\\s+', '');
# MAGIC
# MAGIC -- Test the functions
# MAGIC SELECT
# MAGIC   databricks_clar.gold.mask_ssn('821-18-1750')             AS masked_ssn,
# MAGIC   databricks_clar.gold.mask_email('donna.green@gmail.com') AS masked_email,
# MAGIC   databricks_clar.gold.mask_phone('(570) 791-4150')        AS masked_phone,
# MAGIC   databricks_clar.gold.mask_account('123456789012')        AS masked_account,
# MAGIC   databricks_clar.gold.mask_address('3833 Hill St')        AS masked_address;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Secure Customer View (PII Masked)
# MAGIC PII never leaves Silver in clear text.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW databricks_clar.gold.customers_secure_v AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   full_name,
# MAGIC   databricks_clar.gold.mask_email(email)     AS email,
# MAGIC   databricks_clar.gold.mask_phone(phone)     AS phone,
# MAGIC   databricks_clar.gold.mask_ssn(ssn)         AS ssn,
# MAGIC   YEAR(date_of_birth)                        AS birth_year,  -- drop full DOB, keep year only
# MAGIC   databricks_clar.gold.mask_address(address) AS address,
# MAGIC   city,
# MAGIC   state,
# MAGIC   zip_code,
# MAGIC   email_domain,
# MAGIC   age,
# MAGIC   signup_date
# MAGIC FROM databricks_clar.silver.banking_customers_clean;
# MAGIC
# MAGIC -- Verify masking worked
# MAGIC SELECT * FROM databricks_clar.gold.customers_secure_v LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Star Schema Fact Table — `transactions_enriched`
# MAGIC
# MAGIC This is the central denormalized fact table — joins all 4 entities into a single row per transaction.
# MAGIC Account number is masked, customer PII flows through the secure view pattern.
# MAGIC
# MAGIC **Joins:**
# MAGIC - transactions to accounts (account_id)
# MAGIC - accounts to customers (customer_id)
# MAGIC - accounts to branches (branch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Building the fact table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE databricks_clar.gold.transactions_enriched AS
# MAGIC SELECT
# MAGIC   -- Transaction facts
# MAGIC   t.transaction_id,
# MAGIC   t.transaction_date,
# MAGIC   t.txn_year_month,
# MAGIC   t.transaction_type,
# MAGIC   t.amount,
# MAGIC   t.is_credit,
# MAGIC   t.merchant,
# MAGIC   t.channel,
# MAGIC   t.description,
# MAGIC
# MAGIC   -- Account dim (masked)
# MAGIC   a.account_id,
# MAGIC   databricks_clar.gold.mask_account(CAST(a.account_number AS STRING)) AS account_number_masked,
# MAGIC   a.account_type,
# MAGIC   a.status         AS account_status,
# MAGIC   a.is_active      AS account_is_active,
# MAGIC
# MAGIC   -- Customer dim
# MAGIC   c.customer_id,
# MAGIC   c.full_name      AS customer_name,
# MAGIC   c.email_domain   AS customer_email_domain,
# MAGIC   c.age            AS customer_age,
# MAGIC   c.state          AS customer_state,
# MAGIC
# MAGIC   -- Branch dim
# MAGIC   b.branch_id,
# MAGIC   b.branch_name,
# MAGIC   b.region         AS branch_region,
# MAGIC   b.state          AS branch_state,
# MAGIC
# MAGIC   -- Lineage
# MAGIC   CURRENT_TIMESTAMP() AS processDate
# MAGIC
# MAGIC FROM databricks_clar.silver.banking_transactions_clean t
# MAGIC LEFT JOIN databricks_clar.silver.banking_accounts_clean   a ON t.account_id  = a.account_id
# MAGIC LEFT JOIN databricks_clar.silver.banking_customers_clean  c ON a.customer_id = c.customer_id
# MAGIC LEFT JOIN databricks_clar.silver.banking_branches_clean   b ON a.branch_id   = b.branch_id;
# MAGIC
# MAGIC SELECT COUNT(*) AS row_count FROM databricks_clar.gold.transactions_enriched;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Aggregated Mart — Transaction Summary by Region
# MAGIC
# MAGIC Dashboard-ready KPIs sliced by branch region. Pre-computed for fast queries.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Regional Summary Mart

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE databricks_clar.gold.txn_summary_by_region AS
# MAGIC SELECT
# MAGIC   branch_region,
# MAGIC   COUNT(DISTINCT customer_id)                                    AS unique_customers,
# MAGIC   COUNT(DISTINCT account_id)                                     AS unique_accounts,
# MAGIC   COUNT(*)                                                       AS total_transactions,
# MAGIC   ROUND(SUM(CASE WHEN is_credit THEN amount ELSE 0 END), 2)      AS total_credits,
# MAGIC   ROUND(SUM(CASE WHEN NOT is_credit THEN amount ELSE 0 END), 2)  AS total_debits,
# MAGIC   ROUND(AVG(amount), 2)                                          AS avg_txn_amount,
# MAGIC   ROUND(SUM(amount), 2)                                          AS net_flow,
# MAGIC   CURRENT_TIMESTAMP()                                            AS processDate
# MAGIC FROM databricks_clar.gold.transactions_enriched
# MAGIC WHERE branch_region IS NOT NULL
# MAGIC GROUP BY branch_region
# MAGIC ORDER BY total_transactions DESC;
# MAGIC
# MAGIC SELECT * FROM databricks_clar.gold.txn_summary_by_region;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Aggregated Mart — Customer Lifetime Value (CLV)
# MAGIC
# MAGIC One row per customer with their full transaction footprint. Ranked for marketing/retention targeting.

# COMMAND ----------

# MAGIC %md
# MAGIC ### CLV Mart

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE databricks_clar.gold.customer_lifetime_value AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   customer_state,
# MAGIC   customer_age,
# MAGIC   COUNT(DISTINCT account_id)                                          AS account_count,
# MAGIC   COUNT(*)                                                            AS total_transactions,
# MAGIC   ROUND(SUM(amount), 2)                                               AS net_balance_change,
# MAGIC   ROUND(SUM(CASE WHEN is_credit THEN amount ELSE 0 END), 2)           AS total_inflow,
# MAGIC   ROUND(ABS(SUM(CASE WHEN NOT is_credit THEN amount ELSE 0 END)), 2)  AS total_outflow,
# MAGIC   MIN(transaction_date)                                               AS first_transaction_date,
# MAGIC   MAX(transaction_date)                                               AS last_transaction_date,
# MAGIC   DATEDIFF(MAX(transaction_date), MIN(transaction_date))              AS days_active,
# MAGIC   CURRENT_TIMESTAMP()                                                 AS processDate
# MAGIC FROM databricks_clar.gold.transactions_enriched
# MAGIC WHERE customer_id IS NOT NULL
# MAGIC GROUP BY customer_id, customer_name, customer_state, customer_age
# MAGIC ORDER BY total_inflow DESC;
# MAGIC
# MAGIC SELECT * FROM databricks_clar.gold.customer_lifetime_value LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Aggregated Mart — Dormant Account Alerts
# MAGIC
# MAGIC Flags accounts with no recent activity. Operational table for compliance and outreach teams.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE databricks_clar.gold.dormant_account_alerts AS
# MAGIC WITH last_activity AS (
# MAGIC   SELECT
# MAGIC     a.account_id,
# MAGIC     a.customer_id,
# MAGIC     a.account_type,
# MAGIC     a.status,
# MAGIC     a.balance,
# MAGIC     MAX(t.transaction_date) AS last_txn_date
# MAGIC   FROM databricks_clar.silver.banking_accounts_clean a
# MAGIC   LEFT JOIN databricks_clar.silver.banking_transactions_clean t
# MAGIC     ON a.account_id = t.account_id
# MAGIC   GROUP BY a.account_id, a.customer_id, a.account_type, a.status, a.balance
# MAGIC )
# MAGIC SELECT
# MAGIC   la.account_id,
# MAGIC   la.customer_id,
# MAGIC   c.full_name AS customer_name,
# MAGIC   la.account_type,
# MAGIC   la.status,
# MAGIC   la.balance,
# MAGIC   la.last_txn_date,
# MAGIC   DATEDIFF(CURRENT_DATE(), la.last_txn_date) AS days_since_last_txn,
# MAGIC   CASE
# MAGIC     WHEN la.last_txn_date IS NULL                          THEN 'NEVER_USED'
# MAGIC     WHEN DATEDIFF(CURRENT_DATE(), la.last_txn_date) > 180  THEN 'DORMANT'
# MAGIC     WHEN DATEDIFF(CURRENT_DATE(), la.last_txn_date) > 90   THEN 'AT_RISK'
# MAGIC     ELSE 'ACTIVE'
# MAGIC   END AS activity_flag,
# MAGIC   CURRENT_TIMESTAMP() AS processDate
# MAGIC FROM last_activity la
# MAGIC LEFT JOIN databricks_clar.silver.banking_customers_clean c
# MAGIC   ON la.customer_id = c.customer_id
# MAGIC ORDER BY days_since_last_txn DESC NULLS FIRST;
# MAGIC
# MAGIC SELECT activity_flag, COUNT(*) AS account_count
# MAGIC FROM databricks_clar.gold.dormant_account_alerts
# MAGIC GROUP BY activity_flag
# MAGIC ORDER BY account_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validation — Gold Layer Inventory

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inventory Check
# MAGIC I don't think is needed here but I need that muscle memory. I never know when I will get a tasking that needs it.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'transactions_enriched'      AS object_name, COUNT(*) AS row_count FROM databricks_clar.gold.transactions_enriched
# MAGIC UNION ALL
# MAGIC SELECT 'txn_summary_by_region',     COUNT(*) FROM databricks_clar.gold.txn_summary_by_region
# MAGIC UNION ALL
# MAGIC SELECT 'customer_lifetime_value',   COUNT(*) FROM databricks_clar.gold.customer_lifetime_value
# MAGIC UNION ALL
# MAGIC SELECT 'dormant_account_alerts',    COUNT(*) FROM databricks_clar.gold.dormant_account_alerts
# MAGIC UNION ALL
# MAGIC SELECT 'customers_secure_v (view)', COUNT(*) FROM databricks_clar.gold.customers_secure_v
# MAGIC ORDER BY object_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### PII Masking Verification - Gut check from the Silver layer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm no clear-text PII leaked into Gold
# MAGIC SELECT 'customers_secure_v' AS object_name, ssn, email, phone, address
# MAGIC FROM databricks_clar.gold.customers_secure_v
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Expected Output
# MAGIC
# MAGIC When this notebook runs successfully, you should see:
# MAGIC
# MAGIC ### UDF test (Cell 5)
# MAGIC
# MAGIC | masked_ssn | masked_email | masked_phone | masked_account | masked_address |
# MAGIC |---|---|---|---|---|
# MAGIC | XXX-XX-1750 | do***@gmail.com | (***) ***-4150 | ********9012 | Hill St |
# MAGIC
# MAGIC ### Secure customer view (Cell 7)
# MAGIC 5 rows showing **masked** PII — no clear-text SSN, email, phone, or street numbers.
# MAGIC
# MAGIC ### Fact table row count (Cell 9)
# MAGIC ```
# MAGIC row_count: 800
# MAGIC ```
# MAGIC > Should match Silver transactions count exactly. If lower, joins lost rows (check FK integrity).
# MAGIC
# MAGIC ### Region summary (Cell 11)
# MAGIC ~5 rows (one per region: Northeast, Southeast, Midwest, West, Southwest).
# MAGIC
# MAGIC ### CLV preview (Cell 13)
# MAGIC Top 10 customers by `total_inflow`, one row per customer.
# MAGIC
# MAGIC ### Dormant alerts breakdown (Cell 15)
# MAGIC
# MAGIC | activity_flag | account_count |
# MAGIC |---|---|
# MAGIC | ACTIVE | (varies) |
# MAGIC | AT_RISK | (varies) |
# MAGIC | DORMANT | (varies) |
# MAGIC | NEVER_USED | (rare) |
# MAGIC
# MAGIC ### Final inventory (Cell 17)
# MAGIC
# MAGIC | object_name | row_count |
# MAGIC |---|---|
# MAGIC | customer_lifetime_value | <=100 |
# MAGIC | customers_secure_v (view) | 100 |
# MAGIC | dormant_account_alerts | 150 |
# MAGIC | transactions_enriched | 800 |
# MAGIC | txn_summary_by_region | ~5 |
# MAGIC
# MAGIC ### Tables/views in Unity Catalog
# MAGIC - 4 managed Delta tables in `databricks_clar.gold`
# MAGIC - 1 view: `customers_secure_v`
# MAGIC - 5 SQL UDFs: `mask_ssn`, `mask_email`, `mask_phone`, `mask_account`, `mask_address`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Troubleshooting
# MAGIC
# MAGIC | Issue | Likely cause | Fix |
# MAGIC |---|---|---|
# MAGIC | `Function not found` | UDFs not created | Run Cell 5 first |
# MAGIC | `Permission denied` on UDF | Missing `EXECUTE` privilege | Grant: `GRANT EXECUTE ON FUNCTION ... TO <principal>` |
# MAGIC | Fact table count < 800 | Inner join used somewhere | Code uses LEFT JOINs — verify if you modified |
# MAGIC | `mask_ssn` returns NULL | SSN column has actual NULLs | Wrap in COALESCE or handle nulls in UDF |
# MAGIC | Dormant counts all `NEVER_USED` | Transactions didn't join | Check `account_id` types match between tables |
# MAGIC | CLV missing customers | LEFT JOIN dropped them | Customers without transactions won't appear (by design) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Cert Exam Concepts in This Notebook
# MAGIC
# MAGIC - Unity Catalog SQL UDFs (3-level naming, EXECUTE permission)
# MAGIC - Views vs Tables (no data duplication for secure views)
# MAGIC - Star schema denormalization
# MAGIC - `CREATE OR REPLACE` idempotency pattern
# MAGIC - PII masking strategy (column-level transformation)
# MAGIC - CTEs with `WITH` clause
# MAGIC - Aggregations with `GROUP BY`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Next Step
# MAGIC
# MAGIC Pipeline logic complete. Next we package this as a Databricks Asset Bundle for GitHub:
# MAGIC - `databricks.yml` config
# MAGIC - Job definitions chaining Bronze to Silver to Gold
# MAGIC - CI/CD-ready deployment