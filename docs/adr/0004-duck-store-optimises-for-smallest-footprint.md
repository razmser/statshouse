# duck-store optimises for smallest footprint, not for the available envelope

duck-store exists to make a **small installation** cheap. Its defaults therefore target the smallest
CPU, RAM and disk it can run on, not the largest it is allowed. A reasonable engineer looking at a
node with 2 cores and 4 GB would tune DuckDB to use them; here that envelope is a *ceiling* the agg
must never exceed, while the defaults sit well below it and only grow when an operator raises them.

Concretely: modest `memory_limit` and query concurrency rather than envelope-filling ones; archive
window files **attached on demand** per query and dropped again (60 attaches plus a 60-way `UNION ALL`
measured at 31 ms, so keeping every window attached buys latency that isn't needed and costs resident
memory that is); the DuckDB dependency behind a `-tags duckdb` build guard so the default agg binary
stays pure-Go and 38.6 MB rather than 73.9 MB; and disk turned down through the existing sampling
budget (ADR-0003) rather than by provisioning more.

## Consequences

Under load duck-store will refuse work — a third concurrent query gets `overloaded` (the default
concurrency is two) — where a
tuned-to-the-box configuration would have served it. That is the intended trade: predictable small
resident cost over peak throughput. Any future benchmark that concludes "duck-store is slower than it
could be" should check whether it is measuring this decision before treating it as a defect.
