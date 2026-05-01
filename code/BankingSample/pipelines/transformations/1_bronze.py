# Bronze Layer - Lakeflow Declarative Pipeline
# Reads raw CSVs from Volume and lands as streaming tables.
# DLT handles isolation, retries, and incremental processing automatically.

from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

VOLUME_PATH = "/Volumes/databricks_clar/bronze/bronze_volume/banking_data"


@dp.table(
    name="banking_customers_raw",
    comment="Raw customer data ingested from CSV. PII intact for downstream masking."
)
def banking_customers_raw():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("header", "true")
            .load(f"{VOLUME_PATH}/customers/")
            .withColumn("processDate", current_timestamp())
    )


@dp.table(
    name="banking_accounts_raw",
    comment="Raw account data ingested from CSV."
)
def banking_accounts_raw():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("header", "true")
            .load(f"{VOLUME_PATH}/accounts/")
            .withColumn("processDate", current_timestamp())
    )


@dp.table(
    name="banking_transactions_raw",
    comment="Raw transaction facts ingested from CSV. Largest dataset."
)
def banking_transactions_raw():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("header", "true")
            .load(f"{VOLUME_PATH}/transactions/")
            .withColumn("processDate", current_timestamp())
    )


@dp.table(
    name="banking_branches_raw",
    comment="Raw branch dimension data ingested from CSV."
)
def banking_branches_raw():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("header", "true")
            .load(f"{VOLUME_PATH}/branches/")
            .withColumn("processDate", current_timestamp())
    )