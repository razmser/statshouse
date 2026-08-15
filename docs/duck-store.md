# duck-store: StatsHouse without ClickHouse (operator guide)

duck-store is a second storage backend for StatsHouse: DuckDB embedded in
`statshouse-agg`, selected by a flag. It exists for small installations — the
deployment that cannot justify provisioning a ClickHouse cluster. ClickHouse
remains the default backend and is unchanged; an operator picks one backend
per process with a single explicit flag, and there is no dual-write or
read-comparison mode.

Because the store is embedded, the topology is: agents send to aggregators as
usual, each aggregator shard owns a directory of DuckDB files holding only its
own data, and `statshouse-api` reads by fanning queries out to the shards over
a structured query RPC. There is no standalone duck-store process, no
replication, and no migration path from existing ClickHouse data — a
duck-store install starts empty.

## Selecting the backend

`--storage-backend=clickhouse|duck` on both `statshouse-agg` and
`statshouse-api` (default `clickhouse`).

Two build requirements:

- the **aggregator** binary must be compiled with the `duckdb` build tag —
  `make build-agg-duckdb` produces it with the verified static link flags. A
  regular (pure Go) aggregator refuses `--storage-backend=duck` at startup
  with an error naming the flag and the build tag.
- the **API** never embeds DuckDB; any `statshouse-api` binary accepts `duck`.

A minimal single-shard setup (all other standard flags — `--agg-addr`,
metadata, and so on — apply as under ClickHouse):

```shell
statshouse-agg \
  --storage-backend=duck \
  --duck-store-dir=/var/lib/statshouse/duck \
  --duck-query-addr=0.0.0.0:9404 \
  --local-shard=1 --local-replica=1

statshouse-api \
  --storage-backend=duck \
  --duck-shard-query-addrs=1=agg1.example.com:9404
```

Under duck there is no ClickHouse cluster to autodetect, so the shard and
replica numbers come from `--local-shard` / `--local-replica` instead. For
several shards, list every shard's query address in
`--duck-shard-query-addrs` as comma-separated `shard=host:port` pairs with
1-based shard numbers.

Nonsensical combinations fail at startup with a message naming the offending
flags: `--kh` with duck, duck without `--duck-store-dir` or
`--duck-query-addr`, any `--duck-*` flag without duck, `--duck-shard-query-addrs`
on the API without duck (and duck on the API without it), and the ClickHouse
v3-to-v6 `--migration` under duck (the migration and the table generator are
ClickHouse-only tooling and hard-error against duck).

## The store directory

`--duck-store-dir` points the aggregator at a directory it owns. Everything
inside is created on first start — there is no init SQL to run by hand — and
the store uses whatever disk the filesystem has rather than demanding a
reservation.

```
<dir>/delta-<generation>.duckdb          all writes; rolled to a new generation for compaction
<dir>/archive/1s-<window_start>.duckdb   one sealed/unsealed window file per tier
<dir>/archive/1m-<window_start>.duckdb
<dir>/archive/1h-<window_start>.duckdb
```

Write acknowledgement means durable: rows are fsynced into the delta file
before contributors are acked, exactly as a ClickHouse 200 does today. A
background compactor moves delta rows into the archive window their own
timestamp belongs to; a window is *sealed* (rewritten into one sorted run,
reopened read-only) at window end plus 48 hours; retention then removes whole
window files. Rows older than three days are dropped at write time, matching
ClickHouse's materialized-view guard.

## Retention

Defaults mirror ClickHouse's TTLs, so switching backends does not change how
much history is kept. Each tier is a separate flag; `0` keeps that tier's
windows forever:

| Flag | Default | Covers |
| --- | --- | --- |
| `--duck-retention-1s` | 52 hours | second-resolution data |
| `--duck-retention-1m` | 33 days | minute-resolution data |
| `--duck-retention-1h` | unbounded | hour-resolution data |

Retention unlinks whole archive window files (it never deletes rows inside
them). `--duck-free-space-watermark` is a safety net, off by default (`0`
disables it): when free space on the store's volume falls below the
watermark, the oldest archive windows are evicted ahead of their age limit so
that **ingestion never stops** — history silently shortens instead, and the
shortening is published as the early-eviction count (see observability below)
so it is visible and alertable rather than discovered.

## Sizing disk

duck-store ships no required disk-cap flag. Disk is bounded upstream by the
same lever as under ClickHouse — the aggregator's insert (sampling) budget,
which the sampler enforces as `max(MinInsertBudget, InsertBudget ×
contributors)` bytes per insert round. Because that bounds the serialized
bytes per second entering the store, and retention bounds how long they stay,
disk per tier is a formula rather than an estimate:

```
disk_per_tier ≈ insert_budget_bytes_per_sec × retention_sec × duck_bytes_per_rowbinary_byte
```

Turning disk down means turning the sampling budget down — the same lever,
with the same meaning, operators already use on ClickHouse.

**Caveat:** the `duck_bytes_per_rowbinary_byte` constant is **unmeasured**.
The ~10 bytes-per-row figure behind it came from synthetic rows with no
percentile or unique sketch payloads. Do not capacity-plan against this
formula until the constant has been measured against the real wide schema
with realistic sketch states; until then treat the formula's output as a
lower bound.

## Resource flags

Defaults target the smallest viable node, not the available envelope:

- `--duck-memory-limit` — DuckDB memory limit per store file, default 256 MB
  (the temp directory spill bound matches it).
- `--duck-query-concurrency` — 2 concurrent queries per shard by default; the
  next concurrent query is refused as overloaded rather than queued.

DuckDB runs single-threaded, compaction and sealing run at lowest priority,
and ingestion never yields to queries — a dashboard cannot starve the insert
conveyor.

## Version stamps, quarantine, upgrades

Every store file carries a version stamp: the duck-store schema version, the
DuckDB storage version, and the StatsHouse version. On any mismatch the
aggregator **quarantines** the file — it is excluded from queries, the rest of
the store keeps serving, the file and the reason are written to the process
log, and the count is published as the `__duck_store_quarantined_files`
metric. There is no in-place upgrade, no compatibility shim; downgrading
StatsHouse is safe but equally lossy (files written by the newer binary are
quarantined by the older one rather than misread — never wrong numbers).
Because retention is bounded, the worst case after an upgrade is that queries
lose coverage of at most one retention window while fresh data accumulates.

## Backup, restore, and starting clean

- **Backup and restore are not supported.** Nothing in duck-store claims them;
  do not build a recovery plan on the assumption that they exist.
- **Sealed archive window files are immutable and copyable.** An operator may
  copy a sealed `archive/*-*.duckdb` file out for archival purposes, but
  nothing supports restoring one into a running shard.
- **Quarantined files are reported for deliberate reclamation**: they appear
  in the process log (file name and reason) and in the
  `__duck_store_quarantined_files` metric, and an operator reclaims their disk
  by hand — stop the aggregator, delete the named files, start it again.
- **To start clean** (the equivalent of dropping and recreating the ClickHouse
  tables): stop the aggregator, remove the store directory, start it again.
  The directory tree is recreated empty on the next start.

## Observability

duck-store monitors itself through builtin metrics, so the system can be
diagnosed from StatsHouse:

- `__duck_store_maintenance_time` — duration of compaction, sealing and
  retention passes, by kind and outcome.
- `__duck_store_windows` — archive windows acted on: sealed, unlinked,
  **early-evicted** (the watermark shortening history) or unlink-deferred by
  a reader's lease, per tier.
- `__duck_store_quarantined_files` — quarantined file count per reason axis.
- `__duck_store_query_time` — store-query latency and errors per verb
  (series, tag values) — the query load.
- `__duck_store_size` — store size measured with DuckDB's database-size
  pragma (used and free), which sees the free blocks DuckDB reuses and file
  length does not.

Ingestion status and the other builtin metrics flow through the duck write
path unchanged; the aggregator's internal insert-error log, which under
ClickHouse goes to a log-buffer table, is written to the process log under
duck.
