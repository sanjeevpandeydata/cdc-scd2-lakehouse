# Databricks notebook source
# Notebook 04 — Gold SCD Type 2
#
# What it does:
#   Applies silver_customers to gold_dim_customer as SCD Type 2, in
#   ascending seq order, using a Delta Lake MERGE plus an insert step:
#
#   Step 0 — Out-of-order guard: a Silver row whose source_ts is before
#     the current version's valid_from cannot be inserted into the middle
#     of history. It is appended to silver_quarantine with reason
#     OUT_OF_ORDER and excluded from this run.
#   Steps 1 + 2 — Changes are applied in ordered rounds (earliest
#     unapplied event per customer first), because a Delta MERGE fails if
#     two source rows match the same target row. Each round MERGE-closes
#     the current version when a tracked attribute changed or the
#     operation is DELETE (a no-op UPDATE closes nothing), then INSERTs
#     a new version for INSERTs and real-change UPDATEs that have no
#     current version with equal attributes after the close.
#   Step 3 — Rebuild the gold_dim_customer_current view.
#
# Rules enforced (see docs/architecture.md for the full algorithm):
#   - A new version opens only when a tracked attribute actually changed.
#   - An UPDATE with no attribute change opens no new version.
#   - A DELETE closes the current version and opens no new row.
#   - A late UPDATE (source_ts before the current valid_from) is
#     quarantined, never applied out of order.
#   - surrogate_key = sha2(customer_id + valid_from) — stable across re-runs.
#
# Idempotency: re-applying the same Silver batch is a no-op. The MERGE
#   matches current rows whose attributes already equal the incoming row,
#   so nothing closes and Step 2 finds a current version with equal
#   attributes, so nothing inserts.
#
# Input : main.cdc_lakehouse.silver_customers
# Output: main.cdc_lakehouse.gold_dim_customer (MERGE + insert)
#         main.cdc_lakehouse.gold_dim_customer_current (view)
#         main.cdc_lakehouse.silver_quarantine (append, out-of-order rows)

from pyspark.sql.functions import col, current_timestamp, lit, row_number, sha2, concat_ws

# COMMAND ----------

CATALOG = "main"
SCHEMA = "cdc_lakehouse"

SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_customers"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA}.silver_quarantine"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.gold_dim_customer"
CURRENT_VIEW = f"{CATALOG}.{SCHEMA}.gold_dim_customer_current"

TRACKED_COLUMNS = ["full_name", "email", "city", "tier", "loyalty_points"]

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
        surrogate_key STRING,
        customer_id STRING,
        full_name STRING,
        email STRING,
        city STRING,
        tier STRING,
        loyalty_points INT,
        valid_from TIMESTAMP,
        valid_to TIMESTAMP,
        is_current BOOLEAN
    ) USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# COMMAND ----------

silver = spark.table(SILVER_TABLE).orderBy("seq")
silver.createOrReplaceTempView("silver_changes")

# Step 0 — Out-of-order guard. A Silver row older than the current version
# it would update cannot be applied without rewriting history.
out_of_order = spark.sql(f"""
    SELECT s.*
    FROM silver_changes s
    JOIN {GOLD_TABLE} g
      ON s.customer_id = g.customer_id
     AND g.is_current = true
     AND s.source_ts < g.valid_from
""")

out_of_order_count = out_of_order.count()
if out_of_order_count > 0:
    (out_of_order
     .withColumn("quarantine_reason", lit("OUT_OF_ORDER"))
     .withColumn("quarantined_at", current_timestamp())
     .select("customer_id", *TRACKED_COLUMNS, "updated_at", "operation",
             "seq", "source_ts", "quarantine_reason", "quarantined_at")
     .write.format("delta").mode("append").saveAsTable(QUARANTINE_TABLE))
    print(f"out-of-order rows quarantined: {out_of_order_count}")

# A DELETE for a customer that has no Gold row at all invents nothing:
# quarantine it instead of consuming it silently.
known_customers = spark.table(GOLD_TABLE).select("customer_id").distinct()
orphan_deletes = (
    silver.filter(col("operation") == "DELETE")
    .join(known_customers, on="customer_id", how="left_anti")
)
orphan_count = orphan_deletes.count()
if orphan_count > 0:
    (orphan_deletes
     .withColumn("quarantine_reason", lit("NO_CURRENT_ROW"))
     .withColumn("quarantined_at", current_timestamp())
     .select("customer_id", *TRACKED_COLUMNS, "updated_at", "operation",
             "seq", "source_ts", "quarantine_reason", "quarantined_at")
     .write.format("delta").mode("append").saveAsTable(QUARANTINE_TABLE))
    print(f"orphan DELETEs quarantined: {orphan_count}")

quarantined_keys = out_of_order.select("customer_id", "seq").unionByName(
    orphan_deletes.select("customer_id", "seq")
)
appliable = silver.join(
    quarantined_keys,
    on=["customer_id", "seq"],
    how="left_anti",
)
appliable.createOrReplaceTempView("appliable_changes")

# COMMAND ----------

# Step 1 + 2 — Apply changes in ordered rounds.
#
# Why rounds: a Delta MERGE fails if two source rows match the same target
# row, and one batch can contain two changes for the same customer (for
# example an INSERT followed by an UPDATE). So each round takes the
# earliest unapplied event per customer (lowest seq), closes the current
# version if needed, opens the new version, and repeats until no Silver
# rows remain unapplied. This preserves every intermediate version and
# keeps the application strictly in source order.
from pyspark.sql.window import Window

work = appliable
total_opened = 0
round_no = 0
while work.count() > 0:
    round_no += 1
    ranked = work.withColumn(
        "rn",
        row_number().over(Window.partitionBy("customer_id").orderBy("seq")),
    )
    batch = ranked.filter(col("rn") == 1).drop("rn")
    batch.createOrReplaceTempView("round_changes")

    # Step 1 — MERGE: close the current version when a tracked attribute
    # changed, or when the operation is a DELETE. A no-op UPDATE (all
    # tracked attributes equal) closes nothing.
    changed_predicate = " OR ".join(
        [f"(g.{c} IS DISTINCT FROM s.{c})" for c in TRACKED_COLUMNS]
    )
    spark.sql(f"""
        MERGE INTO {GOLD_TABLE} g
        USING round_changes s
        ON g.customer_id = s.customer_id AND g.is_current = true
        WHEN MATCHED AND (s.operation = 'DELETE' OR ({changed_predicate}))
        THEN UPDATE SET valid_to = s.source_ts, is_current = false
    """)

    # Step 2 — INSERT: open a new version for INSERTs and for UPDATEs with
    # a real attribute change. A row is inserted only if no current version
    # with equal tracked attributes exists after Step 1 (this is what makes
    # a re-run a no-op, and what keeps no-op UPDATEs from opening versions).
    equal_predicate = " AND ".join(
        [f"(g.{c} IS NOT DISTINCT FROM s.{c})" for c in TRACKED_COLUMNS]
    )
    new_versions = spark.sql(f"""
        SELECT
            sha2(concat_ws('|', s.customer_id, CAST(s.source_ts AS STRING)), 256)
                AS surrogate_key,
            s.customer_id,
            s.full_name,
            s.email,
            s.city,
            s.tier,
            s.loyalty_points,
            s.source_ts AS valid_from,
            CAST(NULL AS TIMESTAMP) AS valid_to,
            true AS is_current
        FROM round_changes s
        WHERE s.operation IN ('INSERT', 'UPDATE')
          AND NOT EXISTS (
              SELECT 1 FROM {GOLD_TABLE} g
              WHERE g.customer_id = s.customer_id
                AND g.is_current = true
                AND ({equal_predicate})
          )
    """)
    opened = new_versions.count()
    new_versions.write.format("delta").mode("append").saveAsTable(GOLD_TABLE)
    total_opened += opened

    work = ranked.filter(col("rn") > 1).drop("rn")

print(f"rounds applied      : {round_no}")
print(f"new versions opened : {total_opened}")

# Step 3 — Rebuild the current-state view.
spark.sql(f"""
    CREATE OR REPLACE VIEW {CURRENT_VIEW} AS
    SELECT * FROM {GOLD_TABLE} WHERE is_current = true
""")

gold = spark.table(GOLD_TABLE)
current = spark.table(CURRENT_VIEW)
print(f"gold versions total: {gold.count()}")
print(f"gold current rows  : {current.count()}")
print(f"gold current by tier:")
current.groupBy("tier").count().orderBy("tier").show()
