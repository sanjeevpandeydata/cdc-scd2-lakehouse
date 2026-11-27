# ADR-004 — Delta Lake as the storage format

## Status

Accepted.

## Context

The pipeline needs atomic multi-row updates (MERGE), protection against partial writes, a way to inspect a table as of a past point, and schema flexibility as the source evolves — on top of the plain columnar storage that Spark already uses.

## Decision

Store Bronze, Silver, and Gold tables as Delta Lake tables in catalog `main`, schema `cdc_lakehouse`, with Change Data Feed enabled on the Gold table so downstream consumers can subscribe to version changes.

## Alternatives considered

- **Plain Parquet.** The default Spark format: cheap, portable, well understood. It has no transactions, so a failed write can leave a table half-written; no MERGE, so the SCD Type 2 update would need a delete-plus-insert pair with a failure window between them; and no time travel. Rejected — it cannot provide the atomicity this pipeline depends on.
- **Apache Iceberg.** Provides transactions, MERGE-equivalent upserts, and time travel, and is a credible alternative. It was not chosen because the target platform is Azure Databricks, where Delta Lake is native, Change Data Feed and `OPTIMIZE`/`ZORDER` are built in, and Unity Catalog governance applies directly. Rejected on platform fit, not on capability.
- **A relational database for Gold.** A database would give transactions and upserts naturally. It would also reintroduce a serving layer the warehouse is meant to replace, and it does not fit the Medallion lakehouse shape of the rest of the platform. Rejected — Gold belongs in the lakehouse alongside Bronze and Silver.

## Consequences

- MERGE, atomic writes, and time travel are available to every layer; a failed run leaves tables unchanged.
- The pipeline is tied to an engine that supports Delta Lake (Databricks, or Spark with the Delta library). Porting to a plain Parquet or Iceberg environment means replacing the MERGE step and the transaction guarantees around it.
- Small files accumulate from frequent appends; `OPTIMIZE` should be run on Bronze and Gold on a schedule (see `docs/architecture.md` failure modes). This is a maintenance task, not a design flaw.
