# One storage backend per process; backend comparison lives in e2e only

duck-store and ClickHouse are selected by an explicit `--storage-backend=clickhouse|duck` flag, and a
process serves exactly one of them. We deliberately rejected a production dual-write / read-compare
mode, even though it would have made a ClickHouse-to-duck-store migration path and continuous
differential validation nearly free.

## Consequences

The two SQL renderers — the agg's DuckDB renderer and the existing ClickHouse `queryBuilder` — answer
the same semantic request but share no code, so divergence between them is a standing risk. The only
thing that catches it is a **differential conformance run**: an e2e harness mode (`go run ./e2e
--conformance`) that boots ClickHouse plus both daemon stacks over one shared metadata, seeds the
identical deterministic stream to both agents from the harness itself and compares the two APIs'
decoded answers to every query shape (never state bytes — two valid TDigest merge orders serialize
differently), with CH as the reference. That run is therefore load-bearing, not a
nicety, and must not be allowed to rot.

Migrating an existing ClickHouse install to duck-store has no supported online path.
