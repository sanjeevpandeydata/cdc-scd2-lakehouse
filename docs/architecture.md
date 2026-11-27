# Architecture — CDC + SCD Type 2 Lakehouse

## Overview

This project builds a change-data-capture pipeline on Azure Databricks (PySpark + Delta Lake) that turns a stream of database change events into a versioned customer dimension with full history, and proves the result correct with reconciliation on every run.

Design goals, in priority order:

1. **No change lost, no change applied twice** — exactly-once semantics across re-runs and duplicate deliveries.
2. **History is auditable** — every real attribute change is a new version; deletes close history instead of erasing it.
3. **Late data is visible, not silent** — out-of-order events are quarantined and reported, never dropped and never applied out of order.
4. **Correctness is measured** — a reconciliation report with PASS / FAIL per check, and a loud failure on any FAIL.

## Components

| Component | Technology | Responsibility |
|---|---|---|
| Source event generator | PySpark, seeded random data | Produces deterministic synthetic CDC events (inserts, updates, deletes, duplicates, one late event, one schema-evolution event). |
| Staging | Volume / DBFS path under the workspace | Holds generated events as JSON / Parquet files until ingested. |
| Bronze ingestion | PySpark, Delta Lake (append) | Reads staged files, adds ingestion metadata, appends to `bronze_customer_events`. A processed-files guard prevents re-ingestion. |
| Silver processing | PySpark, Delta Lake | Deduplicates by `seq`, applies the watermark, quarantines late events to `silver_quarantine`, writes `silver_customers`. |
| Gold SCD Type 2 | PySpark, Delta Lake MERGE | Applies Silver changes to `gold_dim_customer`, maintaining `valid_from` / `valid_to` / `is_current` / `surrogate_key`. |
| Current view | Delta view | `gold_dim_customer_current` — one row per live customer. |
| Reconciliation | PySpark | Count and hash checks across Bronze / Silver / Gold, written to `reconciliation_report`. |

All tables live in catalog `main`, schema `cdc_lakehouse`.

## Data flow

```
source events (JSON/Parquet, staged)
        │
        ▼
bronze_customer_events          append-only, raw + ingest_ts, source_file
        │
        ▼
silver_customers                 deduplicated, ordered, watermarked
        │  ┌─────────────────────► silver_quarantine (late / out-of-order)
        ▼
gold_dim_customer                SCD Type 2 versions
        │
        ▼
gold_dim_customer_current        view: is_current = true
        │
   reconciliation_report         counts + hashes, PASS / FAIL per check
```

## Exactly-once approach

Exactly-once here is achieved by the combination of three mechanisms, not by any single one:

1. **Bronze processed-files guard.** Notebook 02 records every staged file it has ingested (file name + row count) in a `bronze_processed_files` table. A re-run reads only files not in that table, so re-running ingestion never duplicates Bronze rows. The guard write and the Bronze append happen in the same run; if the run fails before the guard is written, the next run re-reads the file — and step 2 still protects the downstream tables.
2. **Silver deduplication on `seq`.** `seq` is a monotonic sequence number assigned by the source; the same logical change always carries the same `seq`. Silver keeps exactly one row per (`customer_id`, `seq`). A duplicate delivery of the same event — same `seq`, same payload — collapses to one row. If a duplicate arrives with a different payload for the same `seq`, Silver keeps the first-seen row and writes the conflict to `silver_quarantine` for review, rather than guessing which payload is right.
3. **Gold MERGE keyed on business key + validity.** The Gold MERGE matches on `customer_id` and `is_current = true`, and only opens a new version when a tracked attribute actually changed. Re-applying the same Silver batch therefore opens no new versions: the current row already matches the incoming attributes, so the MERGE is a no-op for that row. This makes the Gold step idempotent even if Silver is re-processed.

## Watermarking and late data

The watermark is the highest `source_ts` already applied to Gold (equivalently, the highest `seq` applied, since `seq` is monotonic with `source_ts` in the source system). Notebook 03 compares each incoming event against the watermark:

- Event with `source_ts` at or after the watermark: eligible to apply.
- Event with `source_ts` before the watermark: **late**. It is written to `silver_quarantine` with reason `LATE_ARRIVAL`, and counted in the reconciliation report. It is never applied, because applying it would rewrite history that downstream consumers may already have read, and never silently dropped, because the quarantine table and the report make it visible.
- An UPDATE whose `source_ts` is before the current version's `valid_from` is the same case in Gold terms: it cannot be inserted into the middle of history without rewriting existing versions, so it is quarantined with reason `OUT_OF_ORDER`.

See [ADR-003](adr/ADR-003-watermark-and-late-data.md) for the reasoning and the alternatives considered.

## SCD Type 2 algorithm

Gold table `gold_dim_customer` has, in addition to the business columns, `surrogate_key`, `valid_from`, `valid_to`, `is_current`.

- `surrogate_key` is a stable hash of `customer_id` + `valid_from`, so the same version always has the same key across re-runs.
- `valid_from` is the `source_ts` of the event that opened the version.
- `valid_to` is null for the current version, otherwise the `source_ts` of the event that closed it.
- `is_current` is true for exactly one row per live customer, false for all historical rows and for deleted customers (who have no current row).

For each Silver change, processed in ascending `seq` order, the MERGE in notebook 04 does the following. Changes are applied in rounds — each round takes the earliest unapplied event per customer — because a Delta MERGE fails if two source rows match the same target row, and one batch can contain two changes for the same customer:

1. **INSERT** — no current row exists for the `customer_id`. Open a new version: `valid_from = source_ts`, `valid_to = NULL`, `is_current = true`.
2. **UPDATE, attribute changed** — a current row exists and at least one tracked attribute (`full_name`, `email`, `city`, `tier`, `loyalty_points`) differs from the current row. Close the current row (`valid_to = source_ts`, `is_current = false`) and open a new version (`valid_from = source_ts`, `valid_to = NULL`, `is_current = true`).
3. **UPDATE, no attribute change** — a current row exists and all tracked attributes are equal (a no-op update, e.g. a source re-save). Open no new version. The event is consumed and counted, but history is unchanged. This is what keeps the Gold table from growing a version per heartbeat.
4. **DELETE** — a current row exists. Close it (`valid_to = source_ts`, `is_current = false`) and open no new row. The customer disappears from `gold_dim_customer_current` but the full history remains queryable.
5. **DELETE with no current row** — written to `silver_quarantine` with reason `NO_CURRENT_ROW`, rather than inventing a version from a delete. (An UPDATE for a customer with no current row is treated as an upsert and opens a version — the change itself carries the full attribute values.)
6. **Late UPDATE** (`source_ts` before the current version's `valid_from`) — quarantined with reason `OUT_OF_ORDER`, never applied.

Tracked attributes are `full_name`, `email`, `city`, `tier`, and `loyalty_points`. `updated_at` and `source_ts` are metadata, not tracked attributes: a change to them alone opens no new version.

## Delete handling

Deletes are first-class events, not absences. A DELETE closes the current version, so:

- Point-in-time queries ("who was a customer on date X?") still work — the closed version covers its validity window.
- `gold_dim_customer_current` reflects the delete immediately.
- A later INSERT for the same `customer_id` (a re-registration) opens a fresh version; the earlier history is untouched.

Hard-deleting Gold rows is never done by the pipeline.

## Schema evolution

`loyalty_points` is added mid-stream by the generator (notebook 01) as a schema-evolution example: earlier events have no `loyalty_points` column value (null). The pipeline handles this by:

- Bronze storing events with `mergeSchema` enabled, so a new column does not break ingestion.
- Silver coalescing a missing `loyalty_points` to null, and treating null consistently in the change comparison — a null-to-value transition on `loyalty_points` is a tracked attribute change and opens a new version; a null-to-null comparison opens none.
- Gold carrying `loyalty_points` as a nullable column from the start, so no Gold migration is needed when the column appears.

Rules for future columns: a new source column is added to Bronze by `mergeSchema`, added to the Silver select explicitly (never by `SELECT *`), and only added to the tracked-attribute list after a deliberate decision recorded in an ADR — an unreviewed column must never silently start opening versions.

## Reconciliation

Notebook 05 runs after every Gold load and writes one row per check to `reconciliation_report` (`run_id`, `check_name`, `expected_value`, `actual_value`, `status`, `checked_at`):

| Check | Expected | What a FAIL means |
|---|---|---|
| `bronze_vs_source` | Bronze rows = staged source events | Ingestion lost or duplicated events. |
| `silver_plus_quarantine_vs_bronze` | Silver + quarantine rows = Bronze rows | An event was dropped between Bronze and Silver. |
| `gold_versions_vs_silver_applied` | Gold versions opened + closed = Silver applied events that changed state | A Silver change was not reflected in Gold. |
| `gold_current_vs_live_keys` | Current Gold rows = distinct live business keys | A customer has zero or two current rows. |
| `gold_hash_vs_silver_sample` | Hash of tracked attributes agrees on a sampled set of keys | Attribute values disagree between Silver and Gold. |

A FAIL raises an exception in notebook 05, so the run is visibly broken rather than silently wrong. The report is append-only: past runs remain for audit.

## Failure modes

| Failure | What happens | Recovery |
|---|---|---|
| Notebook 02 re-run after a partial write | Processed-files guard skips already-ingested files; Silver dedup absorbs any duplicate that still slips through. | Re-run notebook 02. |
| Duplicate delivery of the same `seq` | Collapses to one Silver row; a conflicting payload for the same `seq` goes to quarantine. | Inspect `silver_quarantine`, resolve the conflict, reprocess manually. |
| Late event arrives | Written to `silver_quarantine`, counted in reconciliation, Gold untouched. | Review the quarantined event; if it must be applied, correct history with an explicit, reviewed backfill — never by re-running the pipeline. |
| DELETE for an unknown customer | Quarantined (`NO_CURRENT_ROW`), no version invented. | Check the source: usually a delete that preceded the first captured INSERT. |
| Schema change in source (new column) | Bronze accepts it via `mergeSchema`; Silver ignores it until explicitly added. | Add the column to Silver and decide whether it is tracked; record the decision. |
| Gold MERGE fails mid-run | Delta Lake transaction is atomic — the Gold table is unchanged. | Fix the cause, re-run notebook 04; idempotency means no double-apply. |
| Reconciliation FAIL | Notebook 05 raises; the report row records the FAIL. | Do not proceed to downstream consumers; investigate the failing check first. |

## Testing approach

- **Deterministic generated data.** Notebook 01 is seeded, so the same input is produced on every run; expected outcomes (version counts, quarantine counts) are therefore stable and checkable.
- **Known edge cases in the data.** The generated events deliberately include duplicate deliveries, a no-op update, a delete, a re-registration after delete, a late-arriving event, and a schema-evolution event — the cases the pipeline exists to handle.
- **Reconciliation as the test.** Notebook 05 is the acceptance test for the whole pipeline: if all checks PASS, the run is correct by construction of the checks.
- **Sample file.** `data/sample_cdc_events.csv` lets a reader inspect the input shape and hand-trace the expected Gold history for 30 events without running anything.

## Limits

- The source data is synthetic, generated sample data. It exercises the edge cases above, but it is not production data and no production behaviour is claimed from it.
- No runtime, throughput, or scale figures are claimed. Measure on your own cluster and record runs in `results/`.
- The notebooks were syntax-checked only and have not been run on Databricks. The first run in a real workspace is a validation run.
- The generator, not a real CDC source (Debezium, database log reader, or cloud CDC service), produces the events. Swapping in a real source means replacing notebook 01's output with the real connector's output in the same envelope shape (`customer_id`, tracked attributes, `operation`, `seq`, `source_ts`); notebooks 02–05 are unchanged by that swap.
- The pipeline is batch-oriented over staged files. A streaming variant (Auto Loader + Structured Streaming with the same Silver/Gold logic) is a natural next step, not part of this repository.
