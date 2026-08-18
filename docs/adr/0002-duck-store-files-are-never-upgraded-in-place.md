# duck-store files are never upgraded in place

Embedding DuckDB makes the agg binary own its on-disk format, across two version axes it must track —
duck-store's own schema, and DuckDB's storage format, which changes between DuckDB releases and is not
ours to control. Every delta and archive window file carries a version stamp (duck-store schema
version, DuckDB storage version, statshouse version); on mismatch the agg **refuses to open that file,
quarantines it, and keeps serving the rest**. There is no in-place rewrite and no compatibility shim.

This is only affordable because retention is bounded (ADR-0003): the worst case of an upgrade is that
queries lose coverage of at most one retention window while fresh data accumulates, rather than a
migration that must be written, tested and supported for every schema change.

> Amended by ADR-0005: the duck-store schema version is two axes, one per file kind
> (delta generations, archive windows). The exact-match, never-upgrade rule holds for each
> axis separately, so a layout change to one kind of file quarantines only that kind.

## Consequences

Downgrading statshouse is safe but equally lossy — files written by the newer binary are quarantined
by the older one rather than misread. Backup and restore are explicitly **not** claimed as features;
operators may copy sealed archive window files, which are immutable, but nothing supports restoring
them into a running shard.
