# One storage backend per process; backend comparison lives in e2e only

duck-store and ClickHouse are selected by an explicit `--storage-backend=clickhouse|duck` flag, and a
process serves exactly one of them. We deliberately rejected a production dual-write / read-compare
mode, even though it would have made a ClickHouse-to-duck-store migration path and continuous
differential validation nearly free.

## Consequences

The two SQL renderers — the agg's DuckDB renderer and the existing ClickHouse `queryBuilder` — answer
the same semantic request but share no code, so divergence between them is a standing risk. The only
thing that catches it is a **differential conformance run**: the e2e suite executing its unchanged
input matrix and assertions against both backends and comparing *decoded values* (never state bytes —
two valid TDigest merge orders serialize differently). That run is therefore load-bearing, not a
nicety, and must not be allowed to rot.

Migrating an existing ClickHouse install to duck-store has no supported online path.
