# ADR-001 — MERGE vs overwrite for applying changes

## Status

Accepted.

## Context

The Gold SCD Type 2 table must be updated from Silver change events. Each batch of changes can insert new customers, close and open versions for updated customers, and close versions for deleted customers — all in one pass. The update must be atomic (a failed batch leaves Gold unchanged) and idempotent (re-applying the same batch changes nothing).

## Decision

Apply changes with a Delta Lake `MERGE` keyed on `customer_id` and `is_current = true`, with the SCD Type 2 logic (close current version, insert new version) expressed as the MERGE's matched / not-matched actions, implemented in notebook 04.

## Alternatives considered

- **Overwrite the whole Gold table each run.** Simple and easy to reason about, but it rewrites all history on every run, breaks concurrent readers during the rewrite, loses Delta time-travel continuity for the table, and scales with total history rather than with the size of the change batch.
- **Append-only with a view that resolves "current".** Append every version and compute the current row in a view. This avoids MERGE, but pushes the "which row is current" logic into every query, makes point-in-time queries harder to get right, and leaves `is_current` unenforced — two rows could both claim to be current with nothing to prevent it.
- **Upsert via delete + insert in two statements.** Two separate operations are not atomic together: a failure between them leaves Gold half-updated. MERGE performs the match, close, and insert as a single Delta transaction.

## Consequences

- Gold updates are atomic and idempotent; a failed MERGE leaves the table unchanged and a re-run is safe.
- The MERGE condition must be exactly right — a wrong match predicate would close the wrong version. The reconciliation checks in notebook 05 exist largely to catch this class of error.
- MERGE requires Delta Lake; the pipeline cannot be ported to a plain Parquet sink without replacing this step.
