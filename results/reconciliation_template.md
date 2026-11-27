# Reconciliation Run — Template

Record one real run per section below. Copy this template for each run.
Do not fill in numbers until the run has actually been executed on a
Databricks workspace.

## Run

| Field | Value |
|---|---|
| Date | |
| Cluster / runtime | |
| Run ID (`run_id` in `reconciliation_report`) | |
| Source events generated (notebook 01) | |
| Staging path | |

## Row counts

| Table | Rows |
|---|---|
| `bronze_customer_events` | |
| `silver_customers` | |
| `silver_quarantine` | |
| `gold_dim_customer` (all versions) | |
| `gold_dim_customer_current` | |

## Checks (from `reconciliation_report`)

| Check | Expected | Actual | Status |
|---|---|---|---|
| `bronze_vs_source` | | | |
| `silver_plus_quarantine_vs_bronze` | | | |
| `gold_versions_vs_silver_applied` | | | |
| `gold_current_vs_live_keys` | | | |
| `gold_hash_vs_silver_sample` | | | |

## Quarantine

| Reason | Rows |
|---|---|
| `LATE_ARRIVAL` | |
| `OUT_OF_ORDER` | |
| `NO_CURRENT_ROW` | |
| `SEQ_CONFLICT` | |

## Notes

- Any check that FAILED, and what was done about it:
- Anything unexpected in this run:
