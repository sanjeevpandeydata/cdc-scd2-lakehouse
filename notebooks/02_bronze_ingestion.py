# Databricks notebook source
# Notebook 02 — Bronze ingestion
#
# What it does:
#   Reads staged CDC events and appends them to bronze_customer_events,
#   adding only ingestion metadata (ingest_ts, source_file). No business
#   transformation happens in Bronze: the raw event is preserved exactly
#   as received, so any downstream question can always be re-answered
#   from Bronze.
#
# Exactly-once:
#   A processed-files guard table (bronze_processed_files) records every
#   staged file already ingested. A re-run reads only files not in the
#   guard, so re-running this notebook never duplicates Bronze rows.
#   If a run fails after the append but before the guard write, the next
#   run may re-read the file — Silver dedup on (customer_id, seq) still
#   protects the downstream tables.
#
# Input : Parquet files under STAGING_PATH (written by notebook 01).
# Output: main.cdc_lakehouse.bronze_customer_events (append)
#         main.cdc_lakehouse.bronze_processed_files (append, guard)

from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
)

# COMMAND ----------

CATALOG = "main"
SCHEMA = "cdc_lakehouse"
STAGING_PATH = "/Volumes/main/cdc_lakehouse/staging/cdc_events"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_customer_events"
GUARD_TABLE = f"{CATALOG}.{SCHEMA}.bronze_processed_files"

EVENT_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("full_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("tier", StringType(), True),
    StructField("loyalty_points", IntegerType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("operation", StringType(), False),
    StructField("seq", LongType(), False),
    StructField("source_ts", TimestampType(), False),
])

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
        customer_id STRING,
        full_name STRING,
        email STRING,
        city STRING,
        tier STRING,
        loyalty_points INT,
        updated_at TIMESTAMP,
        operation STRING,
        seq BIGINT,
        source_ts TIMESTAMP,
        ingest_ts TIMESTAMP,
        source_file STRING
    ) USING DELTA
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GUARD_TABLE} (
        source_file STRING,
        rows_ingested BIGINT,
        ingested_at TIMESTAMP
    ) USING DELTA
""")

# COMMAND ----------

# Files already ingested, from the guard table.
processed = {
    row["source_file"]
    for row in spark.table(GUARD_TABLE).select("source_file").collect()
}

staged = (
    spark.read.schema(EVENT_SCHEMA)
    .parquet(STAGING_PATH)
    .withColumn("source_file", input_file_name())
)

new_files = sorted({
    row["source_file"]
    for row in staged.select("source_file").distinct().collect()
} - processed)

if not new_files:
    print("NO_NEW_FILES: every staged file has already been ingested.")
else:
    to_ingest = staged.filter(staged["source_file"].isin(new_files))
    bronze_rows = to_ingest.withColumn("ingest_ts", current_timestamp())
    row_count = bronze_rows.count()

    bronze_rows.write.format("delta").mode("append").saveAsTable(BRONZE_TABLE)

    guard_df = spark.createDataFrame(
        [(f, bronze_rows.filter(bronze_rows["source_file"] == f).count())
         for f in new_files],
        ["source_file", "rows_ingested"],
    ).withColumn("ingested_at", current_timestamp())
    guard_df.write.format("delta").mode("append").saveAsTable(GUARD_TABLE)

    print(f"files ingested : {len(new_files)}")
    print(f"rows appended  : {row_count}")

print(f"bronze rows total: {spark.table(BRONZE_TABLE).count()}")
