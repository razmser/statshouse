# duck-store: read collapse and 6× disk, diagnosed

Investigation of load-test run `20260817-233113` (`e2e/artifacts/20260817-233113/REPORT.md`),
which measured the duck-backed stack at **~0 q/s sustained read rate** (0.8% of queries
succeeded) and **1662 MB on disk against ClickHouse's 272 MB** for the same stream.

Both numbers come from one root cause: **compaction is ~16× slower than ingestion, and it
holds a store-global lock while it runs.**

## What the run's data shows

`duck/__duck_store_size.csv` splits the 1662 MB (only 6 samples survived the failing API):

| time | delta used | archive used |
| --- | --- | --- |
| 01:29 | 451 MB | 26 MB |
| 02:11 | 517 MB | 26 MB |
| 04:32 | **1372 MB** | 54 MB |

96% of the store is **undrained delta**. The archive — where data is supposed to live —
grew 28 MB in three hours.

`duck/__duck_store_maintenance_time.csv` carries points for sealing and retention
(microseconds, nothing to do) and **zero points for compaction across the whole 5-hour
run**. A pass is only recorded when it returns; no pass returned once the backlog began
(the addendum corrects "never returned" — the metric's absence alone proved less).

`duck/__duck_store_query_time.csv` — the store queries that did execute averaged
**60–370 ms**. DuckDB's query engine was never the problem. Meanwhile 99.2% of client
queries failed, and the client latency histogram is a spike at ~5000 ms (mean 4729 ms for
the 400s, 4737 ms for the 422s): exactly `DefaultQueryQueueWait`. Queries were not running
slowly, they were never admitted.

## Root cause

### 1. Compaction's insert is row-by-row through `database/sql`

`internal/duckstore/compact.go:232` (`insertCollapsedGroups`) inserts the collapsed groups
one row at a time through a prepared statement with 116 bound parameters.

Measured against the real package (`Writer` + `CompactOnce`, `threads=1`, 256 MB memory
limit — production settings, but on an unconstrained laptop; the load-test aggregator had
0.5 CPU):

```
writer (duckdb.Appender):                22,156 logical rows/s   (66k physical appends/s)
compaction (CompactOnce):                 1,375 logical rows/s   -> 16x slower than the writer
  of which: collapse SQL (GROUP BY ALL)     1.22 s   ( 1% of the pass)
            insertCollapsedGroups          84.93 s   (99% of the pass)
```

The same 126k collapsed rows written other ways:

```
via duckdb.Appender:            0.82 s   -> 104x faster than the prepared-statement loop
via pure SQL INSERT..SELECT:    0.44 s   -> 190x faster (360k rows, no Go round trip)
```

So compaction cannot keep up with any realistic ingest rate. The delta grows without bound
(the 1662 MB), and each pass takes minutes.

`seal.go:237` uses the same function, so sealing carries the same cost — invisible in a
5-hour run because a window seals at window_end + 48 h.

### 2. A compaction pass fences off every read

`consumeWindow` takes the store-wide `s.archiveMu.Lock()` for the whole
open + collapse + insert + commit of one window (`internal/duckstore/generation.go:226`).
A query needs `s.archiveMu.RLock()` for its entire read
(`internal/duckstore/render_series.go:607`) — and it holds one of the two admission slots
while blocked, because `admitQuery` acquires the slot *before* running the executor
(`internal/aggregator/store_query_server.go`). Go's `RWMutex` also blocks new readers as
soon as a writer queues, so one compaction request every 5 s is enough to fence everything
off permanently.

Measured directly:

```
query source acquisition: 1m22.9s   while one compaction pass ran 1m27.8s
```

The store-query RPC gives up after 5 s, and the API surfaces that as `422 overloaded`,
`400` or `500` depending on endpoint. That is the entire read collapse.

### 3. The delta carries 3× the rows

`internal/duckstore/writer.go:332` appends every row to **all three** tier tables.
ClickHouse inserts once into `statshouse_v3_incoming` and three materialized views roll up
(1m ≈ N/60, 1h ≈ N/3600 once `AggregatingMergeTree` merges). Under duck, only compaction
performs that roll-up — so with compaction starved, 1s/1m/1h all hold N rows.
3× fan-out × ~2× uncollapsed-and-unsorted ≈ the 6× measured against ClickHouse's 272 MB.
(The addendum corrects this: the two stacks did not store the same stream, so the 6× is
not a defensible decomposition — the 96% undrained delta above is the solid finding.)

### 4. Unconsumed generations are invisible to queries

`withQuerySources` reads the *active* delta plus archive windows only
(`internal/duckstore/render_series.go:644`); a rolled-but-unconsumed generation is
"consumption input, not a query source". That is sound when consumption takes
milliseconds. With 1.3 GB of backlog, most of the ingested data was simply not queryable —
a data-availability hole the 99% error rate masked.

### 5. Latent: no intra-window merge before seal

Each generation appends its own partial rows into a window and nothing re-collapses until
seal at window_end + 48 h. At a 5 s compaction cadence that is ~12 partial rows per
(key, minute) in a 1m window for ~3 days, and ~720 partial rows per (key, hour) in a 1h
window for up to 32 days. The 5-hour run never reached this; a real deployment would.

### 6. The observability gap that hid it

`__duck_store_maintenance_time` only records passes that **finish**, so a pass that never
finishes reads as "no data" — indistinguishable from a healthy idle store. There is no
gauge for the delta backlog at all. `__duck_store_query_time` counts only queries that
ran, so the store reported a healthy 100 ms p50 while refusing 99% of traffic.

## Proposed solution

Ordered; 1–3 turn this run's numbers around, 4–6 make them survive a real deployment.

1. **Kill the row-by-row insert.** Replace the `tx.Prepare` loop in `insertCollapsedGroups`
   with a `duckdb.Appender` (104× measured; `seal.go` gets it for free). Better still: keep
   the collapse entirely in SQL (`INSERT INTO t SELECT <collapse> FROM delta_src.t WHERE
   time ...`) and round-trip into Go **only** the groups that actually need a fold
   (`len(percentiles_list) > 1 OR len(uniq_state_list) > 1`), taking `list[1]` in SQL for
   the rest. The 1s tier collapses almost nothing (120k rows → 120k groups in the probe),
   so its compaction drops to the 0.44 s SQL path.

2. **Stop fencing readers behind maintenance.** `archiveMu` is one mutex for all window
   files, but the invariant it protects is per-file ("DuckDB allows a file one handle per
   process"). Make it a per-`windowKey` lock, and keep a small cache of open read-only
   window handles instead of open/attach/detach/close per query — that removes the
   per-query `ATTACH` cost and most of the reason the lock exists. Same for
   `SampleStoreSize` (`internal/duckstore/metrics.go:196`), which opens *every* archive
   file every 30 s under the shared lock.

3. **Don't burn an admission slot while blocked.** Acquire the slot around actual
   execution, not around lock waits, and derive the queue wait from the request's own
   timeout instead of a fixed 5 s. Scale `--duck-query-concurrency` off available CPU
   (default `max(2, GOMAXPROCS)`): 2 slots × ~150 ms service time caps a shard at ~13
   store-queries/s even with no blocking at all, which is below what a single dashboard
   produces.

4. **Make rolled-off generations queryable.** They are immutable once rolled, and the
   consumed-generation records give an exact double-count boundary. Union them into the
   read. With fix 1 there will only ever be one or two, so it costs nothing and closes the
   availability hole permanently.

5. **Derive 1m/1h instead of writing them.** Have the writer append only the 1s tier and
   produce 1m/1h by rolling up at compaction/seal time (before the 1s window's 52 h
   retention drops it). Removes two thirds of delta bytes and two thirds of ingest
   appends — the shape ClickHouse's matviews already have.

6. **Add an intra-window re-collapse pass.** The collapse is associative (sum/min/max/
   arg_min plus the two blob folds), so an unsealed window can be rewritten in place
   whenever its physical row count exceeds a factor of its collapsed count. Cheap once
   fix 1 lands.

7. **Close the observability gap.** Two gauges would have made this obvious in minute one:
   *unconsumed delta generations* (and the age of the oldest), and *time since the last
   successful compaction pass*. `__duck_store_query_time` should also count refused and
   queued queries.

`docs/duck-store.md`'s disk formula needs the 3× tier fan-out folded in. The probe also
measured **~32 B per physical delta row and ~46 B per collapsed archive row** — workload
observations from one run that cannot substitute for `duck_bytes_per_rowbinary_byte`
(unit mismatch; see the addendum).

## Reproducing the measurements

The throwaway probe used for the numbers above is kept out of the tree; drop it into
`internal/duckstore/` and run with the build tag:

```shell
go test -tags duckdb ./internal/duckstore/ -run TestZZPerf -v -timeout 30m
```

It contains four probes: writer-vs-compaction throughput, the collapse/insert split, the
pure-SQL append, and the query-blocked-by-compaction demonstration.

## Addendum 2026-08-18: corrections, and what landed

An external review of the fix plan found three claims above weaker than stated. They are
corrected here rather than rewritten in place — the body is the record of what the
investigation believed at the time — and the fixes it proposed are recorded below.

### Corrections

1. **The 6× disk ratio is not a like-for-like comparison.** The store sizes are exact
   `du` measurements, but `20260817-233113/REPORT.md` records that ClickHouse inserts
   were refused from 01:27 to 02:59 during the IP-drift incident, so CH-side data after
   01:27 is confounded — the two stacks did not store the same stream. Delta domination
   (96% undrained) is solidly demonstrated; "3× fan-out × ~2× uncollapsed = 6× vs CH"
   is not a defensible causal decomposition.

2. **Zero compaction points does not prove no pass ever returned.** The maintenance
   loop runs one immediate pass before its ticker; on a store still empty at boot that
   pass returns in microseconds and emits a point, which can land in a generation that
   is then rolled and never consumed — invisible to queries, exactly fix 4's hole.
   Sealing and retention keep emitting every interval, so their points land in the
   queryable active generation, which reproduces "tags 2/3 present, tag 1 absent"
   through the shared pipeline. The supportable claim is that **no compaction pass
   returned once the backlog began**, resting on the direct 1m27.8 s timing probe
   rather than on the metric's absence.

3. **The 32 B / 46 B figures cannot replace `duck_bytes_per_rowbinary_byte`.** Unit
   mismatch — that constant multiplies a RowBinary byte budget, not a row count.
   `docs/duck-store.md` now states the per-row observations separately, marked as
   workload observations from one run rather than capacity-planning constants.

### What landed

All seven fixes landed, plus the foundation work the review required before they could
be built safely: per-file-kind schema axes (ADR-0005), a query-source snapshot with
generation pins, and the per-window lock registry.

- **Fix 1 — the row-by-row insert is gone.** Compaction and sealing transact through
  the writer's connection-level `BEGIN`/`COMMIT` protocol with a `duckdb.Appender`
  inside the transaction, and the collapse itself is one `INSERT INTO .. SELECT`
  (`collapseInsert`, `internal/duckstore/compact.go`); the 116-parameter prepared
  statement and the Go row round-trip were deleted.
- **Fix 2 — readers are no longer fenced.** The store-global `archiveMu` is deleted;
  `internal/duckstore/window_locks.go` keeps one reference-counted lock per archive
  window, so a pass on window A does not fence a query reading window B. The proposed
  cache of read-only window handles was rejected on review: ADR-0004 had already
  measured 60 attaches plus a 60-way `UNION ALL` at 31 ms — per-query `ATTACH` was
  never the cost, the lock wait was.
- **Fix 3 — admission, reframed as policy.** The diagnosis's claim that the slot was
  burned during lock waits was wrong — the clamped deadline was already installed
  before `admitQuery` — and the locking work behind fixes 2 and 4 is what stopped
  slots being held through lock waits. What landed is the policy change: the
  admission wait is `min(30 s ceiling, request deadline − 1 s execution budget)`,
  with a waiter bound of 4× concurrency and `--duck-query-concurrency` defaulting to
  `max(2, GOMAXPROCS)`; ADR-0004 carries the amendment.
- **Fix 4 — rolled-but-unconsumed generations are queryable.** Reads run over a
  query-source snapshot (`internal/duckstore/query_snapshot.go`): the active delta,
  the served windows, and every rolled generation pinned for the read and bounded per
  window by the consumption records, so rows count exactly once however a consume
  interleaves.
- **Fix 5 — the coarser tiers are derived, not written.** The writer appends only
  `rows_1s`; compaction emits the 1s/1m/1h archive rows from the 1s rows by timestamp
  truncation. The delta schema axis moved 4→5 with the archive axis unchanged
  (ADR-0005's per-file-kind axes), so the bump quarantines only undrained delta.
- **Fix 6 — the re-collapse pass.** `RecollapseWindow`
  (`internal/duckstore/seal.go`) rewrites an unsealed window in place, without the
  seal marker, whenever its physical rows exceed 4× its collapsed count.
- **Fix 7 — the observability gap is closed.** `__duck_store_backlog` (unconsumed
  generations and the oldest's age) and `__duck_store_maintenance_age` are sampled
  from in-memory state only — no window lock, no file open — so the gauges cannot
  themselves hang behind a stuck pass; `__duck_store_query_time` counts queued and
  refused queries too.
- **Plus one pre-existing bug the review surfaced:** a row accepted exactly at the
  ingest-guard boundary could be dropped when its window sealed while it sat in an
  unconsumed generation. The seal barrier — a coordinated roll-and-drain before each
  sealing pass — now lands it instead.

`docs/duck-store.md` carries the operator-facing surface of all of the above; ADR-0004
is amended and ADR-0005 added.
