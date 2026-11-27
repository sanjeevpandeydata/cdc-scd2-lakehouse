# ADR-003 — Watermarking and late-data handling

## Status

Accepted.

## Context

Change events do not always arrive in source order. Network retries, source replays, and clock differences can deliver an event after a newer event for the same customer has already been applied. The pipeline must decide what to do with an event whose `source_ts` is older than the newest data already applied (the watermark).

## Decision

Maintain a watermark equal to the highest `source_ts` (equivalently, highest `seq`) already applied to Gold. Events older than the watermark are written to `silver_quarantine` with a reason (`LATE_ARRIVAL` or `OUT_OF_ORDER`), counted in the reconciliation report, and never applied to Gold. They are never silently dropped.

## Alternatives considered

- **Apply late events by rewriting history.** Insert the late version into the middle of the version chain and shift neighbouring `valid_from` / `valid_to` values. This produces the most "correct" history in theory, but it rewrites rows downstream consumers may already have read, so reports computed before and after the rewrite disagree with no audit trail of why. Rejected for a dimension that is consumed by reporting.
- **Silently drop late events.** Simplest to implement. It loses data with no trace: the source says a change happened, the warehouse says it did not, and nothing records the discrepancy. Rejected — silent data loss is the worst failure mode for a pipeline whose purpose is correctness.
- **Hold all events until a lateness window passes (fixed delay).** Buffer incoming events for a fixed period before applying them. This reduces late arrivals but delays all data by the window length and still needs a policy for events later than the window. Rejected as the primary mechanism; it trades freshness for a problem it does not fully solve.

## Consequences

- Gold history is append-only in time order: once a version is written, it is never rewritten by the pipeline. Downstream reports are stable.
- Late events are visible in `silver_quarantine` and in the reconciliation report, so the scale of the lateness problem is measurable and a human can decide whether a manual, reviewed backfill is warranted.
- The quarantine table must be monitored — a growing quarantine means the source or the delivery path has a real problem, and ignoring it is equivalent to dropping the data.
