# duck-store: StatsHouse without ClickHouse (operator guide)

duck-store is a second storage backend for StatsHouse: DuckDB embedded in
`statshouse-agg`, selected by a flag. It exists for small installations — the
deployment that cannot justify provisioning a ClickHouse cluster. ClickHouse
remains the default backend and behaves as before, with one flag change:
`--kh` no longer defaults to `127.0.0.1:13338,127.0.0.1:13339` — it is now
required with `--storage-backend=clickhouse`. An operator picks one backend
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

Three build requirements:

- the **aggregator** binary must be compiled with the `duckdb` build tag —
  `make build-agg-duckdb` produces it with the verified static link flags. A
  regular (pure Go) aggregator refuses `--storage-backend=duck` at startup
  with an error naming the flag and the build tag.
- that tagged build is a cgo build linking DuckDB's C++ runtime, so the build
  host needs a working C/C++ toolchain; for the static Linux link the
  toolchain must be complete enough to provide `libpthread.a` (the make
  target checks for it and refuses to link otherwise). A naive static link of
  DuckDB produces a binary that segfaults on first use, which is why the
  flags live in the make target and not in ad-hoc `go build` invocations.
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
  --duck-shard-query-addrs=1=agg1.example.com:9404 \
  --shard-by-metric-shards=1
```

Under duck there is no ClickHouse cluster to autodetect, so the shard and
replica numbers come from `--local-shard` / `--local-replica` instead. For
several shards, list every shard's query address in
`--duck-shard-query-addrs` as comma-separated `shard=host:port` pairs with
1-based shard numbers, and copy the aggregator cluster's shard count into
`--shard-by-metric-shards` — the routing modulus the query side prunes by.

On the API, `--clickhouse-v2-addrs` is a ClickHouse-backend flag: under duck
it is not required (the API substitutes an internal placeholder address for
the pool it never queries), and passing it anyway has no effect — every read
fans out to the shard addresses from `--duck-shard-query-addrs` instead.

When the API and the aggregators run on different machines, the store-query
RPC handshakes are encrypted and both sides must present the same key: pass
the key file to the API with `--rpc-crypto-path` and to the aggregators with
`--aes-pwd-file` (the aggregator defaults to reading `/etc/engine/pass`).
Without a shared key the API cannot talk to the shards at all — every query
fails the handshake.

Two caveats for sharded installs:

- the shard count is part of the by-metric-id routing (`metric_id % shards`).
  If the cluster is later resized, rows of a by-metric-id metric written
  before the resize live on their old shard while the API prunes queries to
  the new one, so old data answers as missing until retention expires it.
  Changing the shard count is a fresh-start operation.
- the API routes by the `--shard-by-metric-shards` copy of the aggregator
  cluster's count, so `--duck-shard-query-addrs` must cover shards `1..N` of
  that count — listing fewer fails at startup, because by-metric-id data
  lands on a shard with no address (the default count is 16; a single-shard
  install passes `--shard-by-metric-shards=1`). Listing more than the count
  is fine: the cluster may hold shards beyond the modulus, and fixed-shard
  metrics and fan-outs still read them. The numbering must also be contiguous
  from 1: a gap (say `1=...,3=...`) fails at startup.

Nonsensical combinations fail at startup with a message naming the offending
flags: `--kh` with duck, duck without `--duck-store-dir` or
`--duck-query-addr`, either of those two addressing flags without duck on the
aggregator, `--duck-shard-query-addrs` on the API without duck (and duck on the
API without it), and the ClickHouse v3-to-v6 `--migration` under duck (the
migration and the table generator are ClickHouse-only tooling and hard-error
against duck). The remaining `--duck-*` flags — retention, free-space
watermark, query concurrency, memory limit — are accepted under either backend
and simply have no effect while the aggregator runs ClickHouse.

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
window files. Rows older than the historic window (48 hours) are dropped at
write time: a window seals at its end plus the historic window, so an older
row could only target a sealed window. This is tighter than ClickHouse's
materialized-view guard (three days), which does not seal windows.

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
which the sampler enforces as `max(MinInsertBudget, InsertBudgetFixed +
InsertBudget × contributors)` bytes per insert round (`InsertBudgetFixed` is
300 000 bytes; the terms live beside `MinInsertBudget` in the aggregator
config). Because that bounds the serialized bytes per second entering the
store, and retention bounds how long they stay, disk per tier is a formula
rather than an estimate:

```
disk_per_tier ≈ insert_budget_bytes_per_sec × retention_sec × duck_bytes_per_rowbinary_byte
```

Turning disk down means turning the sampling budget down — the same lever,
with the same meaning, operators already use on ClickHouse.

**Caveat:** the `duck_bytes_per_rowbinary_byte` constant is **unmeasured**.
The ~10 bytes-per-row figure behind it came from synthetic rows with no
percentile or unique aggregate-state payloads. Do not capacity-plan against this
formula until the constant has been measured against the real wide schema
with realistic aggregate states; until then treat the formula's output as a
lower bound.

## Resource flags

Defaults target the smallest viable node, not the available envelope:

- `--duck-memory-limit` — DuckDB memory limit per store file, default 256 MB
  (the temp directory spill bound matches it).
- `--duck-query-concurrency` — 2 concurrent queries per shard by default; a
  query finding every slot busy waits up to 5 seconds for one (so a dashboard's
  tiles all firing at once drain through the slots instead of failing) and is
  then refused as overloaded rather than queued indefinitely.

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
  **early-evicted** (the watermark shortening history), unlink-deferred by
  a reader's lease, or **late-dropped** (a consume found the window already
  sealed and dropped that generation's rows for it), per tier.
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
