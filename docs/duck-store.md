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

Four build requirements:

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
- the static Linux build must also carry the `osusergo` tag (the make target
  does): a statically-linked glibc cannot load its NSS modules, so the cgo
  group lookup behind the privilege drop (`--user`/`--group`, the standard
  start shape for a daemon running as root) fails and the aggregator exits
  fatally at startup. `osusergo` makes the lookup read `/etc/passwd` and
  `/etc/group` directly; the e2e harness builds its duck aggregator with the
  same tag for exactly this reason.
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
<dir>/delta-<generation>.duckdb          all writes, the 1s tier only; rolled to a new generation for compaction
<dir>/archive/1s-<window_start>.duckdb   one sealed/unsealed window file per tier
<dir>/archive/1m-<window_start>.duckdb
<dir>/archive/1h-<window_start>.duckdb
```

Write acknowledgement means durable: rows are fsynced into the delta file
before contributors are acked, exactly as a ClickHouse 200 does today. The
delta stores one tier — second-resolution rows; a background compactor moves
them into the archive window their own timestamp belongs to, deriving the
minute- and hour-resolution archive rows by truncating the second-resolution
ones, so the archive ends up with all three tiers without the coarser two
ever being written at ingest. A window is *sealed* (rewritten into one sorted
run, reopened read-only) at window end plus 48 hours, and until then it is
periodically re-collapsed in place so partial rows from successive
compaction passes do not accumulate; retention then removes whole window
files. Rows older than the historic window (48 hours) are dropped at
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
store, and retention bounds how long the archive keeps them, how the budget
lands on disk is two separate terms — the delta and the archive hold
different shapes of the same rows:

```
archive, per tier ≈ insert_budget_bytes_per_sec × retention_sec × duck_bytes_per_rowbinary_byte
delta             ≈ 1s_rows_per_sec × backlog_sec × duck_bytes_per_delta_row
```

Turning disk down means turning the sampling budget down — the same lever,
with the same meaning, operators already use on ClickHouse.

- The **archive** term is the retention-bounded steady state: whole window
  files per tier, each holding collapsed rows — one per (key, second),
  (key, minute) or (key, hour). How far each tier's collapse shrinks the row
  count is workload-dependent, which is one of the two unknowns below.
- The **delta** term has no retention bound at all: the delta holds the
  uncollapsed 1s rows for exactly as long as compaction takes to drain them.
  Healthy, that is a few seconds of ingest (one generation at the compaction
  cadence plus the pass in flight); when compaction falls behind ingest, the
  term grows without bound. The load-test run that motivated the 2026-08
  compaction rework failed precisely here — 96% of a 1662 MB store was
  undrained delta.

**Measurements, and their limits.** The one load-test run to date
(`20260817-233113`) observed **~32 bytes per physical delta row and ~46 bytes
per collapsed archive row**. These are observations from a single workload,
taken by a throwaway probe that is deliberately not kept in the tree — not
capacity-planning constants. They also cannot be substituted for
`duck_bytes_per_rowbinary_byte`, which remains unmeasured: that constant's
units are duck bytes per serialized RowBinary byte, and the budget it
multiplies bounds serialized bytes, not rows. Converting between per-row
figures and the budget needs rows-per-input-byte (row width varies with tag
population and aggregate-state weight), and a complete formula additionally
needs a workload-dependent collapse ratio per tier. Until those are measured
for an install, size by watching `__duck_store_size` on a pilot rather than
by formula.

## Resource flags

Defaults target the smallest viable node, not the available envelope:

- `--duck-memory-limit` — DuckDB memory limit per store file, default 256 MB
  (the temp directory spill bound matches it).
- `--duck-query-concurrency` — how many store queries execute at once per
  shard, default `max(2, GOMAXPROCS)` — the process's own parallelism, floored
  at two. A query finding every slot busy waits for one toward the request's
  own timeout, never past a 30-second ceiling (so a dashboard's tiles all
  firing at once drain through the slots instead of failing), and the wait
  always ends a full execution-budget second short of the request's deadline,
  so a query is never admitted with no useful time left to run in — a query
  that would only get a slot inside that reserved window is refused as
  overloaded instead. Beyond 4 queries waiting or executing per admission
  slot (the waiter bound), the shard refuses at once rather than growing
  goroutine and longpoll memory under overload.

DuckDB runs single-threaded, compaction and sealing run at lowest priority,
and ingestion never yields to queries — a dashboard cannot starve the insert
conveyor.

## Version stamps, quarantine, upgrades

Every store file carries a version stamp: the duck-store schema version, the
DuckDB storage version, and the StatsHouse version. The schema version is
**scoped by file kind** — delta files are checked against the delta schema
axis, archive windows against the archive schema axis, and the two axes move
independently — so a delta layout change can never evict archive history,
and vice versa. On any mismatch the aggregator **quarantines** the file — it
is excluded from queries, the rest of the store keeps serving, the file and
the reason are written to the process log, and the count is published as the
`__duck_store_quarantined_files` metric with the failing axis named
(`delta_schema`, `archive_schema`, `storage`, `statshouse` or `unreadable`).
What a quarantine costs depends on the axis: a `delta_schema` quarantine
loses only undrained ingest data — rows still waiting for compaction — while
every archive window keeps serving and fresh data accumulates in a new
generation (the 2026-08 single-tier-delta change quarantines delta files
written by older binaries on first start, and that is its whole cost); an
`archive_schema` quarantine loses the history in the affected windows while
ingestion continues. There is no in-place upgrade, no compatibility shim;
downgrading StatsHouse is safe but equally lossy (files written by the newer
binary are quarantined by the older one rather than misread — never wrong
numbers). Because retention is bounded, the worst case after an upgrade is
that queries lose coverage of at most one retention window while fresh data
accumulates.

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
- `__duck_store_backlog` — ingestion backlog, sampled from in-memory state
  only, so it keeps flowing while maintenance holds the store's file locks:
  `generations` counts rolled delta generations still holding rows compaction
  has not taken, and `oldest_age_seconds` is how long the oldest has waited
  (counted from process start for generations recovered from disk). A healthy
  store sits at 0–2 generations; growth means compaction is falling behind
  ingest and the delta term of the disk sizing is growing with it.
- `__duck_store_maintenance_age` — seconds since each maintenance
  (compaction, sealing, retention) last completed a successful pass, counted
  from the component's start until its first success. A pass that never
  returns reads as a growing age instead of as no data; healthy compaction
  hugs its 5-second cadence, so growth here means a pass is stuck or starved.
- `__duck_store_windows` — archive windows acted on: sealed, unlinked,
  **early-evicted** (the watermark shortening history), unlink-deferred by
  a reader's lease, **late-dropped** (a consume found the window already
  sealed and dropped that generation's rows for it — with the seal barrier
  draining every contributing generation before a seal, a nonzero count
  indicts the sender, not the store), or **recollapsed** (an unsealed window
  rewritten in place so partial rows from successive compaction passes do
  not accumulate; a steady rate is the expected shape of a busy store), per
  tier.
- `__duck_store_quarantined_files` — quarantined file count per reason axis.
- `__duck_store_query_time` — store-query latency and errors per verb
  (series, tag values) — the query load. The same metric's status tag also
  carries the admission outcomes: `queued` counts queries that waited for a
  slot (value = the wait), `refused` counts queries shed at admission (value =
  how long they waited first) — the only place an overload shed is visible,
  because a shed query never reaches the renderer that reports executions.
- `__duck_store_size` — store size measured with DuckDB's database-size
  pragma (used and free), which sees the free blocks DuckDB reuses and file
  length does not.

Ingestion status and the other builtin metrics flow through the duck write
path unchanged; the aggregator's internal insert-error log, which under
ClickHouse goes to a log-buffer table, is written to the process log under
duck.
