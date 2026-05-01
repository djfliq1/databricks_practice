# Silver Layer - Lakeflow Declarative Pipeline
# Cleans + enriches Bronze streaming tables.
# Uses @dp.expect for declarative data quality validation.

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, upper, trim, lower, current_timestamp,
    split, concat_ws, datediff, current_date,
    floor, when, to_timestamp, date_format
)


@dp.table(
    name="banking_customers_clean",
    comment="Customers with standardized casing and enriched columns (full_name, email_domain, age)."
)
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect("valid_email_format", "email LIKE '%@%'")
def banking_customers_clean():
    return (
        dp.read_stream("banking_customers_raw")
            .withColumn("first_name",   upper(trim(col("first_name"))))
            .withColumn("last_name",    upper(trim(col("last_name"))))
            .withColumn("email",        lower(trim(col("email"))))
            .withColumn("city",         upper(trim(col("city"))))
            .withColumn("state",        upper(trim(col("state"))))
            .withColumn("full_name",    concat_ws(" ", col("first_name"), col("last_name")))
            .withColumn("email_domain", split(col("email"), "@")[1])
            .withColumn("age",          floor(datediff(current_date(), col("date_of_birth")) / 365.25).cast("int"))
            .withColumn("processDate",  current_timestamp())
    )


@dp.table(
    name="banking_accounts_clean",
    comment="Accounts with standardized status/type and enriched is_active flag."
)
@dp.expect_or_drop("valid_account_id", "account_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_fk", "customer_id IS NOT NULL")
@dp.expect("non_negative_balance", "balance >= 0 OR account_type = 'CREDIT'")
def banking_accounts_clean():
    return (
        dp.read_stream("banking_accounts_raw")
            .withColumn("account_type",     upper(trim(col("account_type"))))
            .withColumn("status",           upper(trim(col("status"))))
            .withColumn("account_age_days", datediff(current_date(), col("open_date")))
            .withColumn("is_active",        when(col("status") == "ACTIVE", True).otherwise(False))
            .withColumn("processDate",      current_timestamp())
    )


@dp.table(
    name="banking_transactions_clean",
    comment="Transactions with cast timestamps, cleaned merchant strings, and is_credit flag."
)
@dp.expect_or_drop("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_account_fk",     "account_id IS NOT NULL")
@dp.expect("non_zero_amount",              "amount != 0")
def banking_transactions_clean():
    return (
        dp.read_stream("banking_transactions_raw")
            .withColumn("transaction_date", to_timestamp(col("transaction_date")))
            .withColumn("merchant",
                when((col("merchant").isNull()) | (trim(col("merchant")) == ""), "UNKNOWN")
                .otherwise(upper(trim(col("merchant"))))
            )
            .withColumn("transaction_type", upper(trim(col("transaction_type"))))
            .withColumn("channel",          upper(trim(col("channel"))))
            .withColumn("is_credit",        when(col("amount") > 0, True).otherwise(False))
            .withColumn("txn_year_month",   date_format(col("transaction_date"), "yyyy-MM"))
            .withColumn("processDate",      current_timestamp())
    )


@dp.table(
    name="banking_branches_clean",
    comment="Branch dimension with standardized casing for clean joins downstream."
)
@dp.expect_or_drop("valid_branch_id", "branch_id IS NOT NULL")
def banking_branches_clean():
    return (
        dp.read_stream("banking_branches_raw")
            .withColumn("branch_name", upper(trim(col("branch_name"))))
            .withColumn("city",        upper(trim(col("city"))))
            .withColumn("state",       upper(trim(col("state"))))
            .withColumn("region",      upper(trim(col("region"))))
            .withColumn("processDate", current_timestamp())
    )