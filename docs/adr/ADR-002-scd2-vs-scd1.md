# ADR-002 — SCD Type 2 vs Type 1 for customer history

## Status

Accepted.

## Context

Customer attributes change: city, tier, email, loyalty points. Downstream reporting needs to answer both "what is true now?" and "what was true on a given date?" — for example, revenue attributed to the tier a customer held when the order was placed, not the tier they hold today.

## Decision

Maintain the Gold customer dimension as SCD Type 2: one row per version, with `valid_from`, `valid_to`, `is_current`, and a stable `surrogate_key`. The current view `gold_dim_customer_current` serves "what is true now?" queries.

## Alternatives considered

- **SCD Type 1 (overwrite in place).** Keeps one row per customer and is simpler and smaller. It destroys history: once a tier changes, the previous tier is unrecoverable, and any report that depended on it silently changes. Rejected — it fails the core requirement of the project.
- **SCD Type 3 (previous value column).** Keeps the current and one previous value per attribute. It answers "what was the last value?" but not "what was the value on 1 March?", and it needs a new column per tracked attribute. Rejected — it answers the wrong question and scales badly.
- **Full event log only (no dimension).** Keep every change event and compute state in queries. This preserves everything, but every consumer must re-implement version resolution, and a wrong query silently produces a wrong answer. Rejected as the consumer-facing model; the event log already exists in Bronze/Silver, and Gold is the resolved, queryable form of it.

## Consequences

- History is queryable and auditable; point-in-time joins work with a `valid_from <= date < valid_to` predicate.
- The Gold table grows with the number of real changes, not with the number of events — a no-op update opens no version, by design.
- Consumers must use `gold_dim_customer_current` (or filter `is_current = true`) for current-state queries; querying the raw Gold table without that filter double-counts customers. The view exists to make the correct query the easy one.
