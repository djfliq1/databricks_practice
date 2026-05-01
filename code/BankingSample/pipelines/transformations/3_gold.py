# Gold Layer - Lakeflow Declarative Pipeline
# PII masking via inline transforms, star schema fact, aggregated business marts.
# Note: DLT pipelines cannot create UC SQL UDFs, so masking is done inline.

from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col, concat, lit, year, split, regexp_replace, current_timestamp,
    count, countDistinct, sum as spark_sum, avg, round as spark_round,
    abs as spark_abs, min as spark_min, max as spark_max, datediff,
    current_date, when
)


# ============================================================
# Secure Customer View - PII Masked
# ============================================================
@dp.materialized_view(
    name="customers_secure",
    comment="Customer dimension with PII masked. Safe for analyst access."
)
def customers_secure():
    return (
        spark.read.table("banking_customers_clean")
            .withColumn("ssn",           concat(lit("XXX-XX-"), col("ssn").substr(-4, 4)))
            .withColumn("email_masked",  concat(col("email").substr(1, 2), lit("***@"), split(col("email"), "@")[1]))
            .withColumn("phone",         concat(lit("(***) ***-"), col("phone").substr(-4, 4)))
            .withColumn("birth_year",    year(col("date_of_birth")))
            .withColumn("address",       regexp_replace(col("address"), "^[0-9]+\\s+", ""))
            .drop("date_of_birth", "email")
            .withColumnRenamed("email_masked", "email")
            .withColumn("processDate",   current_timestamp())
    )


# ============================================================
# Star Schema Fact Table - transactions_enriched
# ============================================================
@dp.materialized_view(
    name="transactions_enriched",
    comment="Denormalized fact table joining transactions, accounts, customers, and branches."
)
def transactions_enriched():
    txn = spark.read.table("banking_transactions_clean").alias("t")
    acc = spark.read.table("banking_accounts_clean").alias("a")
    cust = spark.read.table("banking_customers_clean").alias("c")
    br = spark.read.table("banking_branches_clean").alias("b")

    return (
        txn
            .join(acc,  col("t.account_id")  == col("a.account_id"),  "left")
            .join(cust, col("a.customer_id") == col("c.customer_id"), "left")
            .join(br,   col("a.branch_id")   == col("b.branch_id"),   "left")
            .select(
                # Transaction facts
                col("t.transaction_id"),
                col("t.transaction_date"),
                col("t.txn_year_month"),
                col("t.transaction_type"),
                col("t.amount"),
                col("t.is_credit"),
                col("t.merchant"),
                col("t.channel"),
                col("t.description"),
                # Account dim (masked account number)
                col("a.account_id"),
                concat(lit("********"), col("a.account_number").cast("string").substr(-4, 4)).alias("account_number_masked"),
                col("a.account_type"),
                col("a.status").alias("account_status"),
                col("a.is_active").alias("account_is_active"),
                # Customer dim
                col("c.customer_id"),
                col("c.full_name").alias("customer_name"),
                col("c.email_domain").alias("customer_email_domain"),
                col("c.age").alias("customer_age"),
                col("c.state").alias("customer_state"),
                # Branch dim
                col("b.branch_id"),
                col("b.branch_name"),
                col("b.region").alias("branch_region"),
                col("b.state").alias("branch_state"),
                # Lineage
                current_timestamp().alias("processDate"),
            )
    )


# ============================================================
# Aggregated Mart - Transaction Summary by Region
# ============================================================
@dp.materialized_view(
    name="txn_summary_by_region",
    comment="KPIs sliced by branch region for dashboard consumption."
)
def txn_summary_by_region():
    return (
        spark.read.table("transactions_enriched")
            .filter(col("branch_region").isNotNull())
            .groupBy("branch_region")
            .agg(
                countDistinct("customer_id").alias("unique_customers"),
                countDistinct("account_id").alias("unique_accounts"),
                count("*").alias("total_transactions"),
                spark_round(spark_sum(when(col("is_credit"), col("amount")).otherwise(0)), 2).alias("total_credits"),
                spark_round(spark_sum(when(~col("is_credit"), col("amount")).otherwise(0)), 2).alias("total_debits"),
                spark_round(avg("amount"), 2).alias("avg_txn_amount"),
                spark_round(spark_sum("amount"), 2).alias("net_flow"),
            )
            .withColumn("processDate", current_timestamp())
    )


# ============================================================
# Aggregated Mart - Customer Lifetime Value (CLV)
# ============================================================
@dp.materialized_view(
    name="customer_lifetime_value",
    comment="One row per customer with full transaction footprint and activity span."
)
def customer_lifetime_value():
    return (
        spark.read.table("transactions_enriched")
            .filter(col("customer_id").isNotNull())
            .groupBy("customer_id", "customer_name", "customer_state", "customer_age")
            .agg(
                countDistinct("account_id").alias("account_count"),
                count("*").alias("total_transactions"),
                spark_round(spark_sum("amount"), 2).alias("net_balance_change"),
                spark_round(spark_sum(when(col("is_credit"), col("amount")).otherwise(0)), 2).alias("total_inflow"),
                spark_round(spark_abs(spark_sum(when(~col("is_credit"), col("amount")).otherwise(0))), 2).alias("total_outflow"),
                spark_min("transaction_date").alias("first_transaction_date"),
                spark_max("transaction_date").alias("last_transaction_date"),
            )
            .withColumn("days_active", datediff(col("last_transaction_date"), col("first_transaction_date")))
            .withColumn("processDate", current_timestamp())
    )


# ============================================================
# Aggregated Mart - Dormant Account Alerts
# ============================================================
@dp.materialized_view(
    name="dormant_account_alerts",
    comment="Account activity flags for compliance and outreach teams."
)
def dormant_account_alerts():
    accounts = spark.read.table("banking_accounts_clean").alias("a")
    txns     = spark.read.table("banking_transactions_clean").alias("t")
    customers = spark.read.table("banking_customers_clean").alias("c")

    last_activity = (
        accounts
            .join(txns, col("a.account_id") == col("t.account_id"), "left")
            .groupBy("a.account_id", "a.customer_id", "a.account_type", "a.status", "a.balance")
            .agg(spark_max("t.transaction_date").alias("last_txn_date"))
    )

    return (
        last_activity
            .join(customers, last_activity["customer_id"] == col("c.customer_id"), "left")
            .select(
                last_activity["account_id"],
                last_activity["customer_id"],
                col("c.full_name").alias("customer_name"),
                last_activity["account_type"],
                last_activity["status"],
                last_activity["balance"],
                last_activity["last_txn_date"],
                datediff(current_date(), last_activity["last_txn_date"]).alias("days_since_last_txn"),
                when(last_activity["last_txn_date"].isNull(),                                     "NEVER_USED")
                    .when(datediff(current_date(), last_activity["last_txn_date"]) > 180,        "DORMANT")
                    .when(datediff(current_date(), last_activity["last_txn_date"]) > 90,         "AT_RISK")
                    .otherwise("ACTIVE")
                    .alias("activity_flag"),
                current_timestamp().alias("processDate"),
            )
    )