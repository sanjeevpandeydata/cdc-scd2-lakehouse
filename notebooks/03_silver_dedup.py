# Databricks notebook source
# Notebook 03 — Silver dedup, watermarking, quarantine
#
# What it does:
#   Rebuilds silver_customers from bronze_customer_events:
#     1. Deduplicates on (customer_id, seq). A duplicate delivery of the
#        same logical change carries the same seq, so it collapses to one
#        row. A conflicting payload for the same (customer_id, seq) is
#        quarantined with reason SEQ_CONFLICT instead of guessing.
#     2. Applies the watermark: the highest source_ts already applied to
#        Gold. Events older than the watermark are late — they go to
#        silver_quarantine with reason LATE_ARRIVAL, never to Silver.
#     3. Writes silver_customers (one row per (customer_id, seq)) and
#        silver_quarantine (late / conflicting events).
#
# Idempotency: Silver and quarantine are rebuilt in full from Bronze on
#   every run (overwrite), so re-running this notebook produces the same
#   result. Nothing is appended blindly.
#
# Input : main.cdc_lakehouse.bronze_customer_events
#         main.cdc_lakehouse.gold_dim_customer (watermark only; optional)
# Output: main.cdc_lakehouse.silver_customers (overwrite)
#         main.cdc_lakehouse.silver_quarantine (overwrite)

from pyspark.sql.functions import col, current_timestamp, lit, row_number
from pyspark.sql.window import Window

# COMMAND ----------

CATALOG = "main"
SCHEMA = "cdc_lakehouse"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_customer_events"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_customers"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA}.silver_quarantine"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.gold_dim_customer"

TRACKED_COLUMNS = ["full_name", "email", "city", "tier", "loyalty_points"]

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Watermark: the newest source_ts already applied to Gold. On the first run
# there is no Gold table yet, so the watermark is the epoch — nothing is late.
if spark.catalog.tableExists(GOLD_TABLE):
    watermark_row = spark.sql(
        f"SELECT max(valid_from) AS watermark FROM {GOLD_TABLE}"
    ).collect()[0]
    watermark = watermark_row["watermark"]
else:
    watermark = None

print(f"watermark (max valid_from in Gold): {watermark}")

# COMMAND ----------

bronze = spark.table(BRONZE_TABLE)

# 1. Deduplicate on (customer_id, seq), keeping the first-seen row.
#    A conflicting payload for the same key is flagged, not merged.
dedup_window = Window.partitionBy("customer_id", "seq").orderBy("ingest_ts")
ranked = bronze.withColumn("rn", row_number().over(dedup_window))

deduped = ranked.filter(col("rn") == 1).drop("rn")

conflicts = (
    ranked.filter(col("rn") > 1)
    .withColumn("quarantine_reason", lit("SEQ_CONFLICT"))
    .withColumn("quarantined_at", current_timestamp())
)

# 2. Watermark: events older than the watermark are late.
if watermark is not None:
    late = deduped.filter(col("source_ts") < lit(watermark)).withColumn(
        "quarantine_reason", lit("LATE_ARRIVAL")
    ).withColumn("quarantined_at", current_timestamp())
    on_time = deduped.filter(col("source_ts") >= lit(watermark))
else:
    late = deduped.limit(0).withColumn("quarantine_reason", lit("LATE_ARRIVAL")).withColumn(
        "quarantined_at", current_timestamp()
    )
    on_time = deduped

silver = on_time.withColumn("processed_at", current_timestamp())

quarantine = (
    late.select(
        "customer_id",
        *TRACKED_COLUMNS,
        "updated_at",
        "operation",
        "seq",
        "source_ts",
        "quarantine_reason",
        "quarantined_at",
    ).unionByName(
        conflicts.select(
            "customer_id",
            *TRACKED_COLUMNS,
            "updated_at",
            "operation",
            "seq",
            "source_ts",
            "quarantine_reason",
            "quarantined_at",
        )
    )
)

# COMMAND ----------

silver.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)
quarantine.write.format("delta").mode("overwrite").saveAsTable(QUARANTINE_TABLE)

print(f"bronze rows     : {bronze.count()}")
print(f"silver rows     : {silver.count()}")
print(f"quarantined rows: {quarantine.count()}")
print("quarantine reasons:")
quarantine.groupBy("quarantine_reason").count().show()
