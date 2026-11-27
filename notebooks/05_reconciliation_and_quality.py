# Databricks notebook source
# Notebook 05 — Reconciliation and quality
#
# What it does:
#   Runs the acceptance checks for the whole pipeline and writes one row
#   per check to reconciliation_report (run_id, check_name,
#   expected_value, actual_value, status, checked_at):
#
#     bronze_vs_source
#         Bronze rows = staged source events. A mismatch means ingestion
#         lost or duplicated events.
#     silver_plus_quarantine_vs_bronze
#         Silver rows + quarantine rows = Bronze rows. Every event is
#         either applied or quarantined — none dropped.
#     gold_versions_vs_silver_applied
#         Gold versions = Silver applied events that changed state. The
#         expected count is derived from Silver: every applied INSERT or
#         UPDATE with a real attribute change opens one version, every
#         applied DELETE closes one without opening one, and no-op
#         UPDATEs open none. (Computed here by replaying the same
#         change-detection logic against Silver and Gold history.)
#     gold_current_vs_live_keys
#         Current Gold rows = distinct live business keys (customers with
#         at least one version and whose latest event is not a DELETE).
#         No customer may have zero or two current rows.
#     gold_hash_vs_silver_sample
#         For a sample of business keys, the hash of the tracked
#         attributes in Gold's current row must equal the hash of the
#         latest applied Silver event. Attribute values must agree.
#
# Failing loudly:
#   Any check with status FAIL raises an exception at the end of the
#   notebook, so a bad run is never silent and downstream consumers are
#   never fed unchecked data. The report is append-only: past runs
#   remain for audit.
#
# Input : staging events, bronze_customer_events, silver_customers,
#         silver_quarantine, gold_dim_customer
# Output: main.cdc_lakehouse.reconciliation_report (append)

import uuid
from datetime import datetime

from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, sha2

# COMMAND ----------

CATALOG = "main"
SCHEMA = "cdc_lakehouse"
STAGING_PATH = "/Volumes/main/cdc_lakehouse/staging/cdc_events"
RUN_ID = str(uuid.uuid4())

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_customer_events"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_customers"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA}.silver_quarantine"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.gold_dim_customer"
REPORT_TABLE = f"{CATALOG}.{SCHEMA}.reconciliation_report"

TRACKED_COLUMNS = ["full_name", "email", "city", "tier", "loyalty_points"]
SAMPLE_SIZE = 50

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {REPORT_TABLE} (
        run_id STRING,
        check_name STRING,
        expected_value BIGINT,
        actual_value BIGINT,
        status STRING,
        checked_at TIMESTAMP
    ) USING DELTA
""")

results = []


def record_check(name, expected, actual):
    status = "PASS" if expected == actual else "FAIL"
    results.append((RUN_ID, name, int(expected), int(actual), status))
    print(f"{status}  {name}: expected={expected} actual={actual}")


# COMMAND ----------

# Check 1 — Bronze vs source: nothing lost or duplicated in ingestion.
source_count = spark.read.parquet(STAGING_PATH).count()
bronze_count = spark.table(BRONZE_TABLE).count()
record_check("bronze_vs_source", source_count, bronze_count)

# Check 2 — Silver + quarantine vs Bronze: every event is either applied
# or quarantined, none dropped.
silver_count = spark.table(SILVER_TABLE).count()
quarantine_count = spark.table(QUARANTINE_TABLE).count()
record_check("silver_plus_quarantine_vs_bronze", bronze_count, silver_count + quarantine_count)

# COMMAND ----------

# Check 3 — Gold versions vs Silver applied events that changed state.
# Replay the change logic: walk Silver events per customer in seq order and
# count how many versions the rules should have opened. A no-op UPDATE
# opens none; a DELETE opens none; every other INSERT/UPDATE opens one.
silver = spark.table(SILVER_TABLE).orderBy("customer_id", "seq")
silver.createOrReplaceTempView("silver_ordered")

expected_versions = spark.sql(f"""
    WITH ordered AS (
        SELECT *,
               LAG(operation) OVER w AS prev_operation,
               LAG(full_name) OVER w AS prev_full_name,
               LAG(email) OVER w AS prev_email,
               LAG(city) OVER w AS prev_city,
               LAG(tier) OVER w AS prev_tier,
               LAG(loyalty_points) OVER w AS prev_loyalty_points
        FROM silver_ordered
        WINDOW w AS (PARTITION BY customer_id ORDER BY seq)
    )
    SELECT COUNT(*) AS expected_versions
    FROM ordered
    WHERE operation IN ('INSERT', 'UPDATE')
      AND (
          prev_operation IS NULL
          OR prev_operation = 'DELETE'
          OR full_name IS DISTINCT FROM prev_full_name
          OR email IS DISTINCT FROM prev_email
          OR city IS DISTINCT FROM prev_city
          OR tier IS DISTINCT FROM prev_tier
          OR loyalty_points IS DISTINCT FROM prev_loyalty_points
      )
""").collect()[0]["expected_versions"]

gold_versions = spark.table(GOLD_TABLE).count()
record_check("gold_versions_vs_silver_applied", expected_versions, gold_versions)

# COMMAND ----------

# Check 4 — Gold current rows vs live business keys.
live_keys = spark.sql(f"""
    WITH latest AS (
        SELECT customer_id,
               operation,
               ROW_NUMBER() OVER (
                   PARTITION BY customer_id ORDER BY seq DESC
               ) AS rn
        FROM {SILVER_TABLE}
    )
    SELECT COUNT(*) AS live_keys FROM latest
    WHERE rn = 1 AND operation != 'DELETE'
""").collect()[0]["live_keys"]

gold_current = spark.sql(
    f"SELECT COUNT(*) AS c FROM {GOLD_TABLE} WHERE is_current = true"
).collect()[0]["c"]
record_check("gold_current_vs_live_keys", live_keys, gold_current)

# COMMAND ----------

# Check 5 — Hash comparison on a sample of business keys: the current
# Gold row's tracked attributes must hash to the same value as the latest
# applied Silver event for that customer.
# Sample only live customers (latest Silver event is not a DELETE):
# deleted customers have no current Gold row, so they cannot be compared.
sample_keys = [
    row["customer_id"]
    for row in spark.sql(f"""
        SELECT customer_id FROM (
            SELECT customer_id, operation,
                   ROW_NUMBER() OVER (
                       PARTITION BY customer_id ORDER BY seq DESC
                   ) AS rn
            FROM {SILVER_TABLE}
        ) WHERE rn = 1 AND operation != 'DELETE'
        LIMIT {SAMPLE_SIZE}
    """).collect()
]

hash_expr = "sha2(concat_ws('|', full_name, email, city, tier, "
hash_expr += "CAST(loyalty_points AS STRING)), 256)"

silver_hashes = {
    row["customer_id"]: row["h"]
    for row in spark.sql(f"""
        SELECT customer_id, {hash_expr} AS h
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY customer_id ORDER BY seq DESC
            ) AS rn
            FROM {SILVER_TABLE}
            WHERE operation != 'DELETE'
        ) WHERE rn = 1 AND customer_id IN ({",".join(repr(k) for k in sample_keys)})
    """).collect()
}

gold_hashes = {
    row["customer_id"]: row["h"]
    for row in spark.sql(f"""
        SELECT customer_id, {hash_expr} AS h
        FROM {GOLD_TABLE}
        WHERE is_current = true
          AND customer_id IN ({",".join(repr(k) for k in sample_keys)})
    """).collect()
}

matching = sum(
    1 for k in sample_keys
    if silver_hashes.get(k) is not None and silver_hashes.get(k) == gold_hashes.get(k)
)
record_check("gold_hash_vs_silver_sample", len(silver_hashes), matching)

# COMMAND ----------

report_df = spark.createDataFrame(
    results, ["run_id", "check_name", "expected_value", "actual_value", "status"]
).withColumn("checked_at", current_timestamp())
report_df.write.format("delta").mode("append").saveAsTable(REPORT_TABLE)

failures = [r for r in results if r[4] == "FAIL"]
print(f"run_id: {RUN_ID}")
print(f"checks passed: {len(results) - len(failures)} / {len(results)}")
if failures:
    failed_names = ", ".join(r[1] for r in failures)
    raise RuntimeError(
        f"RECONCILIATION FAILED for checks: {failed_names}. "
        "See reconciliation_report for details. Do not use this run's Gold data."
    )
print("All reconciliation checks PASSED.")
