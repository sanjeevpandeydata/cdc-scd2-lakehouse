# Databricks notebook source
# Notebook 01 — Generate synthetic CDC source events
#
# What it does:
#   Generates deterministic, seeded synthetic change events for customer records
#   (INSERT / UPDATE / DELETE) and writes them to a staging location as Parquet.
#   The events deliberately include the edge cases the pipeline exists to handle:
#   duplicate deliveries of the same seq, a no-op update, deletes, a
#   re-registration after a delete, one late-arriving event, and a mid-stream
#   schema-evolution event that adds loyalty_points.
#
# What it does NOT do:
#   This is a synthetic data generator. It is not a real CDC source (Debezium,
#   a database log reader, or a cloud CDC service). Replacing it with a real
#   source means writing the real connector's output in the same envelope
#   shape; notebooks 02-05 are unchanged by that swap.
#
# Output: Parquet files under STAGING_PATH, one row per change event.
# Idempotency: the generator is seeded (SEED = 42), so it produces the same
#   events on every run. Re-running overwrites the staging output.

import random
from datetime import datetime, timedelta

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
)

# COMMAND ----------

# Parameters — edit these for your workspace. No other values are hard-coded.
CATALOG = "main"
SCHEMA = "cdc_lakehouse"
STAGING_PATH = "/Volumes/main/cdc_lakehouse/staging/cdc_events"

SEED = 42
NUM_CUSTOMERS = 200
BASE_TS = datetime(2026, 1, 1, 0, 0, 0)
# loyalty_points appears only in events from this seq onwards (schema evolution).
LOYALTY_POINTS_FROM_SEQ = 401

FIRST_NAMES = [
    "Aarav", "Vivaan", "Aditya", "Arjun", "Sai", "Reyansh", "Ayaan", "Krishna",
    "Ishaan", "Rohan", "Ananya", "Diya", "Aadhya", "Myra", "Sara", "Ira",
    "Priya", "Anika", "Navya", "Riya", "Kabir", "Ansh", "Dev", "Aryan",
    "Meera", "Kavya", "Naina", "Tara", "Zara", "Ishita",
]
LAST_NAMES = [
    "Sharma", "Verma", "Patel", "Gupta", "Mehta", "Iyer", "Reddy", "Nair",
    "Singh", "Kumar", "Das", "Joshi", "Agarwal", "Mishra", "Pandey", "Chopra",
    "Bhatt", "Trivedi", "Kapoor", "Malhotra",
]
CITIES = [
    "Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Chennai", "Kolkata",
    "Pune", "Ahmedabad", "Jaipur", "Lucknow", "Ranchi", "Patna",
]
TIERS = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]

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

rng = random.Random(SEED)
events = []
seq = 0


def next_seq():
    global seq
    seq += 1
    return seq


def make_event(customer_id, state, operation, ts):
    """Build one event. loyalty_points is only populated from LOYALTY_POINTS_FROM_SEQ."""
    s = next_seq()
    points = state["loyalty_points"] if s >= LOYALTY_POINTS_FROM_SEQ else None
    return {
        "customer_id": customer_id,
        "full_name": state["full_name"] if operation != "DELETE" else None,
        "email": state["email"] if operation != "DELETE" else None,
        "city": state["city"] if operation != "DELETE" else None,
        "tier": state["tier"] if operation != "DELETE" else None,
        "loyalty_points": points if operation != "DELETE" else None,
        "updated_at": ts,
        "operation": operation,
        "seq": s,
        "source_ts": ts,
    }


def random_state(customer_id):
    first = rng.choice(FIRST_NAMES)
    last = rng.choice(LAST_NAMES)
    return {
        "full_name": f"{first} {last}",
        "email": f"{first.lower()}.{last.lower()}{rng.randint(1, 99)}@example.com",
        "city": rng.choice(CITIES),
        "tier": rng.choice(["BRONZE", "BRONZE", "SILVER", "SILVER", "GOLD"]),
        "loyalty_points": rng.randint(0, 5000),
    }


# COMMAND ----------

# 1. INSERT one event per customer (seq 1-200).
states = {}
for i in range(1, NUM_CUSTOMERS + 1):
    customer_id = f"CUST-{i:04d}"
    state = random_state(customer_id)
    states[customer_id] = state
    ts = BASE_TS + timedelta(minutes=i)
    events.append(make_event(customer_id, state, "INSERT", ts))

# COMMAND ----------

# 2. UPDATEs: a random subset of customers change city or tier (~250 events).
update_customers = rng.sample(list(states.keys()), 150)
for customer_id in update_customers:
    state = dict(states[customer_id])
    state["city"] = rng.choice(CITIES)
    state["tier"] = rng.choice(TIERS)
    state["loyalty_points"] = rng.randint(0, 9000)
    states[customer_id] = state
    ts = BASE_TS + timedelta(days=1, minutes=rng.randint(0, 1400))
    events.append(make_event(customer_id, state, "UPDATE", ts))
    # A second update for some customers.
    if rng.random() < 0.6:
        state = dict(states[customer_id])
        state["tier"] = rng.choice(TIERS)
        states[customer_id] = state
        ts = BASE_TS + timedelta(days=2, minutes=rng.randint(0, 1400))
        events.append(make_event(customer_id, state, "UPDATE", ts))

# COMMAND ----------

# 3. No-op UPDATEs: same attributes, new seq (~15 events). These must open
#    no new SCD Type 2 version in Gold.
for customer_id in rng.sample(list(states.keys()), 15):
    state = states[customer_id]
    ts = BASE_TS + timedelta(days=3, minutes=rng.randint(0, 1400))
    events.append(make_event(customer_id, state, "UPDATE", ts))

# COMMAND ----------

# 4. DELETEs for 30 customers, then re-registration (INSERT) for 10 of them.
deleted = rng.sample(list(states.keys()), 30)
for customer_id in deleted:
    ts = BASE_TS + timedelta(days=4, minutes=rng.randint(0, 1400))
    events.append(make_event(customer_id, states[customer_id], "DELETE", ts))
for customer_id in deleted[:10]:
    state = random_state(customer_id)
    states[customer_id] = state
    ts = BASE_TS + timedelta(days=5, minutes=rng.randint(0, 1400))
    events.append(make_event(customer_id, state, "INSERT", ts))

# COMMAND ----------

# 5. Duplicate deliveries: the same event object delivered twice (~20 events).
#    Same seq, same payload — Silver dedup must collapse these to one row.
for event in rng.sample(events, 20):
    events.append(dict(event))

# COMMAND ----------

# 6. One late-arriving event: an UPDATE for CUST-0001 whose source_ts is
#    earlier than the watermark, delivered at the end of the file. It carries
#    a low seq (replaying the original sequence number) and must be
#    quarantined, never applied out of order.
late_state = dict(states["CUST-0001"])
late_state["city"] = "Mumbai"
late_event = {
    "customer_id": "CUST-0001",
    "full_name": late_state["full_name"],
    "email": late_state["email"],
    "city": "Mumbai",
    "tier": late_state["tier"],
    "loyalty_points": None,
    "updated_at": BASE_TS + timedelta(minutes=30),
    "operation": "UPDATE",
    "seq": 2,
    "source_ts": BASE_TS + timedelta(minutes=30),
}
events.append(late_event)

# COMMAND ----------

# Sort by seq for the main body of the file, but keep the late event last:
# it is delivered late, so it appears at the end of the file even though its
# seq is low. Duplicate deliveries keep their original seq.
main_events = sorted(events[:-1], key=lambda e: e["seq"])
ordered_events = main_events + [events[-1]]

df = spark.createDataFrame([Row(**e) for e in ordered_events], schema=EVENT_SCHEMA)

df.write.mode("overwrite").parquet(STAGING_PATH)

print(f"customers generated : {NUM_CUSTOMERS}")
print(f"events written      : {df.count()}")
print(f"staging path        : {STAGING_PATH}")
df.groupBy("operation").count().orderBy("operation").show()
