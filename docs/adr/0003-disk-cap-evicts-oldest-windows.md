# Disk is sized by the sampling budget, not by a configured cap

The primary control on duck-store's disk usage is the one operators already use: the aggregator's
insert budget, which the sampler enforces as bytes per insert round
(`max(MinInsertBudget, InsertBudget × contributors)`, `aggregator_insert.go:478`). Because that bounds
the serialized bytes/sec entering the store, and **retention** bounds how long they stay, disk is a
formula rather than an estimate:

```
disk_per_tier ≈ insert_budget_bytes_per_sec × retention_sec × duck_bytes_per_rowbinary_byte
```

duck-store therefore ships **no required disk-cap flag**. It assumes it may use whatever the
filesystem has at the moment, and turning disk down means turning the sampling budget down — the same
lever, with the same meaning, as under ClickHouse.

The safety net is a free-space low watermark, not an absolute cap: when free space on the store's
filesystem falls below the watermark, the oldest **archive window files** are unlinked ahead of their
age limit, so ingestion never stops and history silently shortens. We rejected refusing writes when
full, because a monitoring system going blind during the incident that filled its disk is the worse
failure. The shortfall is published as a builtin metric so the loss is visible rather than silent.

## Consequences

`duck_bytes_per_rowbinary_byte` is load-bearing for capacity planning and is currently **unmeasured** —
ticket 03's ~10 B/row came from synthetic data with no sketch blobs. The spec states the formula and
flags the constant as requiring measurement against the flat-96 schema with realistic
`percentiles`/`uniq_state` payloads before anyone plans against it.
