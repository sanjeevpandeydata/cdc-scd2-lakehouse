# Data Dictionary — CDC + SCD Type 2 Lakehouse

Catalog `main`, schema `cdc_lakehouse`. All tables are Delta Lake tables unless noted as a view.

## Source event envelope

Every change event carries the business columns plus the CDC envelope columns below. The envelope is what makes ordering, deduplication, and late-data handling possible; the business columns are what the dimension stores.

| Column | Type | Description |
|---|---|---|
| `customer_id` | STRING | Business key. Stable across all versions of a customer. Never null. |
| `full_name` | STRING | Tracked attribute. |
| `email` | STRING | Tracked attribute. |
| `city` | STRING | Tracked attribute. |
| `tier` | STRING | Tracked attribute. Values: `BRONZE`, `SILVER`, `GOLD`, `PLATINUM`. |
| `loyalty_points` | INT | Tracked attribute. Added mid-stream as a schema-evolution example; null in events produced before it existed. |
| `updated_at` | TIMESTAMP | Timestamp of the change in the source system. Metadata, not a tracked attribute. |
| `operation` | STRING | `INSERT`, `UPDATE`, or `DELETE`. For a DELETE, tracked attributes may be null except `customer_id`. |
| `seq` | BIGINT | Monotonic source sequence number. The ordering authority. Unique per logical change; a duplicate delivery reuses the same `seq`. |
| `source_ts` | TIMESTAMP | Timestamp the event was produced. Monotonic with `seq` in the source system. |

Tracked attributes — a change to any of these opens a new SCD Type 2 version: `full_name`, `email`, `city`, `tier`, `loyalty_points`.

## `bronze_customer_events`

Raw change events exactly as received, plus ingestion metadata. Append-only; rows are never updated or deleted by the pipeline.

| Column | Type | Description |
|---|---|---|
| `customer_id` | STRING | Business key. |
| `full_name` | STRING | As received; null for DELETE events. |
| `email` | STRING | As received; null for DELETE events. |
| `city` | STRING | As received; null for DELETE events. |
| `tier` | STRING | As received; null for DELETE events. |
| `loyalty_points` | INT | As received; null for DELETE events and for events produced before the column existed. |
| `updated_at` | TIMESTAMP | As received. |
| `operation` | STRING | `INSERT`, `UPDATE`, or `DELETE`. |
| `seq` | BIGINT | Source sequence number. |
| `source_ts` | TIMESTAMP | As received. |
| `ingest_ts` | TIMESTAMP | Timestamp the row was written to Bronze. |
| `source_file` | STRING | Name of the staged file the row came from. |

## `bronze_processed_files`

Ingestion guard table. One row per staged file already ingested, so a re-run of notebook 02 skips it.

| Column | Type | Description |
|---|---|---|
| `source_file` | STRING | Staged file name. Primary key. |
| `rows_ingested` | BIGINT | Rows appended to Bronze from this file. |
| `ingested_at` | TIMESTAMP | Timestamp the file was ingested. |

## `silver_customers`

Deduplicated, ordered change events ready to apply to Gold. One row per (`customer_id`, `seq`).

| Column | Type | Description |
|---|---|---|
| `customer_id` | STRING | Business key. |
| `full_name` | STRING | Null for DELETE events. |
| `email` | STRING | Null for DELETE events. |
| `city` | STRING | Null for DELETE events. |
| `tier` | STRING | Null for DELETE events. |
| `loyalty_points` | INT | Null for DELETE events and pre-evolution events. |
| `updated_at` | TIMESTAMP | As received. |
| `operation` | STRING | `INSERT`, `UPDATE`, or `DELETE`. |
| `seq` | BIGINT | Source sequence number. |
| `source_ts` | TIMESTAMP | As received. |
| `ingest_ts` | TIMESTAMP | Carried from Bronze. |
| `processed_at` | TIMESTAMP | Timestamp the row was written to Silver. |

## `silver_quarantine`

Events that were not applied, held for review. Rows are never silently dropped from the pipeline; they land here and are counted in reconciliation.

| Column | Type | Description |
|---|---|---|
| `customer_id` | STRING | Business key. |
| `full_name` | STRING | As received, if present. |
| `email` | STRING | As received, if present. |
| `city` | STRING | As received, if present. |
| `tier` | STRING | As received, if present. |
| `loyalty_points` | INT | As received, if present. |
| `updated_at` | TIMESTAMP | As received. |
| `operation` | STRING | As received. |
| `seq` | BIGINT | Source sequence number. |
| `source_ts` | TIMESTAMP | As received. |
| `quarantine_reason` | STRING | `LATE_ARRIVAL`, `OUT_OF_ORDER`, `NO_CURRENT_ROW`, or `SEQ_CONFLICT`. |
| `quarantined_at` | TIMESTAMP | Timestamp the row was quarantined. |

## `gold_dim_customer`

SCD Type 2 customer dimension. One row per version of a customer.

| Column | Type | Description |
|---|---|---|
| `surrogate_key` | STRING | Stable hash of `customer_id` + `valid_from`. Unique per version. |
| `customer_id` | STRING | Business key. |
| `full_name` | STRING | Value for this version. |
| `email` | STRING | Value for this version. |
| `city` | STRING | Value for this version. |
| `tier` | STRING | Value for this version. |
| `loyalty_points` | INT | Value for this version; null where the version predates the column. |
| `valid_from` | TIMESTAMP | `source_ts` of the event that opened this version. Inclusive. |
| `valid_to` | TIMESTAMP | `source_ts` of the event that closed this version; null while the version is current. Exclusive. |
| `is_current` | BOOLEAN | True for exactly one row per live customer; false for historical rows and for deleted customers. |

Point-in-time query: a version is valid for a date `d` when `valid_from <= d AND (valid_to IS NULL OR valid_to > d)`.

## `gold_dim_customer_current` (view)

Current state of the dimension: `SELECT * FROM gold_dim_customer WHERE is_current = true`. One row per live customer. Use this view — not the raw Gold table — for current-state reporting.

## `reconciliation_report`

One row per check per run. Append-only; past runs remain for audit.

| Column | Type | Description |
|---|---|---|
| `run_id` | STRING | Identifier of the pipeline run (notebook run id or supplied run label). |
| `check_name` | STRING | `bronze_vs_source`, `silver_plus_quarantine_vs_bronze`, `gold_versions_vs_silver_applied`, `gold_current_vs_live_keys`, `gold_hash_vs_silver_sample`. |
| `expected_value` | BIGINT | Value the check expected. |
| `actual_value` | BIGINT | Value actually observed. |
| `status` | STRING | `PASS` or `FAIL`. |
| `checked_at` | TIMESTAMP | Timestamp the check ran. |
