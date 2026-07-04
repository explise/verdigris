# Architecture Decision Records

This directory records **architecturally significant decisions** for Verdigris — the
choices that shape the system and would be expensive to reverse. Each ADR captures the
context, the decision, and its consequences at a point in time.

We follow the lightweight ADR convention (Michael Nygard's format). An ADR is immutable
once **Accepted**: if a later decision changes it, add a new ADR that *supersedes* it
rather than editing history.

## Index

| ADR | Title | Status |
|---|---|---|
| [0001](../dst-architecture.md) | Deterministic Simulation Testing as a core constraint (and DataFusion over DuckDB) | Accepted |
| [0002](0002-manifest-as-iceberg-standin.md) | JSON manifest as an Apache Iceberg stand-in | Accepted |
| [0003](0003-ingest-query-role-split.md) | Split ingest (writer) from query (readers) for single-writer safety | Accepted |

> ADR-0001 lives at [`../dst-architecture.md`](../dst-architecture.md) for historical
> reasons (it predates this directory) and is the canonical record for both the DST
> methodology and the DataFusion-not-DuckDB engine choice.

## Writing a new ADR

1. Copy the format below into `NNNN-short-title.md` (zero-padded, next number).
2. Fill in **Context → Decision → Consequences**. Be honest about trade-offs and about
   what is *assumed but unproven*.
3. Set `Status: Proposed`, open a PR, and move it to `Accepted` (or `Rejected`) on merge.
4. Add a row to the index above.

```markdown
# ADR-NNNN: <title>

**Status:** Proposed | Accepted | Rejected | Superseded by ADR-XXXX
**Date:** YYYY-MM-DD

## Context
What forces are at play — technical, product, operational.

## Decision
What we are doing, stated plainly.

## Consequences
What becomes easier, what becomes harder, what we now owe.
```
