# StatsHouse

A metrics monitoring system: agents collect samples, aggregators roll them into per-second buckets, and a time-series store serves the API. ClickHouse is the incumbent store; this effort adds DuckDB as an optional alternative for small installations.

## Language

**Storage backend**:
The time-series store that holds aggregated metric data and serves API queries. One of: ClickHouse (incumbent) or duck-store (optional, for small installs). Metadata, mappings, and the journal are NOT part of the storage backend (they live in SQLite/rqlite).
_Avoid_: database, DB, CH (as a synonym for the concept)

**duck-store**:
DuckDB embedded **inside the statshouse-agg binary** — not a separate process. Each aggregator shard owns one DuckDB holding only that shard's data, written by the agg's own insert conveyor and read by the API over RPC. Selected by config; ClickHouse remains the other supported storage backend.
_Avoid_: DuckDB server, storage node, duck-store process

**Shard fan-out**:
The API's replacement for ClickHouse's `_dist` Distributed tables: to answer one query the API issues a structured query RPC to every relevant agg shard in parallel and merges the per-shard results in Go, the way `internal/api/tscache.go` already merges across LODs.
_Avoid_: scatter-gather, distributed query

**Structured query RPC**:
The read protocol between statshouse-api and a duck-store-backed agg. The API sends a typed request (metric, LOD, time range, filters, what-to-compute); the agg builds the DuckDB SQL and returns columnar results. SQL text never crosses the wire.
_Avoid_: query API, SQL RPC

**Aggregate state**:
A serialized partial-aggregation value stored per row instead of raw samples — TDigest centroids for percentiles, HLL/uniq state for unique counts, argMin/argMax host values. ClickHouse uses its own byte formats, which `internal/chutil` already encodes and decodes in Go. The duck-store format is an open question, constrained by **shard fan-out**: percentile and unique states must stay mergeable across shards and LODs, so a finalized number is not sufficient.
_Avoid_: sketch, digest state

**LOD (level of detail)**:
The resolution tier of stored data — 1s, 1m, or 1h — kept in separate tables (`statshouse_v6_1s/1m/1h`); coarser tiers are produced by rolling up finer ones.
_Avoid_: resolution table, downsample tier

**Rollup**:
Producing the 1m and 1h tiers. In ClickHouse this is *not* a cascade: `statshouse_v3_incoming` is `ENGINE = Null`, and three materialized views each read the same incoming rows and write them to `v6_1s`/`1m`/`1h` with only the timestamp truncated — `AggregatingMergeTree` then collapses same-key rows in the background. For duck-store the agg does this itself at insert time, emitting partial 1m/1h rows on its normal conveyor cadence.
_Avoid_: compaction, aggregation (overloaded), downsampling cascade

**Row collapse**:
Folding rows that share a key into one. ClickHouse gets this free from `AggregatingMergeTree`'s background merges. duck-store does it during **compaction** — but queries still always `GROUP BY` the key, so correctness never depends on compaction having run; collapse is a row-count optimization (42× measured on the 1m tier), not the correctness mechanism.
_Avoid_: merge, dedup

**Delta file**:
`delta.duckdb` — the one file per aggregator shard that receives *all* writes, holding a table per LOD. Append-only, unsorted, deliberately small; every query full-scans it, so its size is bounded by how often **compaction** drains it. Chosen on-disk rather than in-memory so that acking a contributor keeps meaning "durable", as it does today when ClickHouse returns 200.
_Avoid_: WAL, L0, buffer table, staging table

**Archive window file**:
`archive/<tier>-<window_start>.duckdb` — a read-optimized file holding one LOD tier's rows for one time window, sorted `(metric, time)`. Rows are routed by their own timestamp, so a file's time span is capped by the window width however late a row arrives. Retention is unlinking whole files, because DuckDB's `DELETE` does not reclaim disk.
_Avoid_: partition, segment, shard (already means something else)

**Compaction**:
Moving rows from the **delta file** into the **archive window files** their timestamps belong to, collapsing them by the full key and sorting by `(metric, time)` on the way. Runs on the order of seconds. Everything but `percentiles`/`uniq_state` collapses in SQL; those two are folded in Go, since DuckDB can neither merge nor re-import those **aggregate states**.
_Avoid_: merge, flush, rollup (already means something else)

**Sealing**:
The once-per-window rewrite of an **archive window file**'s several sorted runs into one, after which the file is reopened `READ_ONLY`. Happens at `window_end + 48h` — past `MaxHistoricWindow`, so no late row can ever invalidate a sealed collapse.
_Avoid_: freezing, finalizing, closing

**Small installation**:
The whole statshouse stack (agent + aggregator + API + storage) on a single node with no ClickHouse process. A motivating case for duck-store, but not the only one — sharded multi-node duck-store deployments are in scope; replication is not.
_Avoid_: tiny install, lightweight deployment
