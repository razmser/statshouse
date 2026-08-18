# Schema versions are scoped by file kind

ADR-0002 stamps every store file — delta generations and archive windows alike —
with one duck-store schema version and refuses any exact mismatch. As long as the
two file kinds share one version axis, the two lifetimes they protect are very
different: a delta generation is transient (undrained rows the writer can
re-accept after a restart), while an archive window is the store's whole history
for that window, unrecoverable once quarantined. The first change that touches
only the delta layout — dropping the 1m/1h tables from delta files, the plan's
Task 9 — would have bumped the shared version and taken **all existing archive
history out of service on restart**, not just the undrained delta.

ADR-0002 forbids upgrading files in place, so the fix is not a migration: the
version axis itself is split by file kind.

**Decision.** There are two schema-version axes: `DeltaSchemaVersion` stamps and
verifies delta generation files, `ArchiveSchemaVersion` stamps and verifies
archive window files. Each axis keeps ADR-0002's exact-match,
never-upgrade-in-place, no-compatibility-shim rule independently — a delta
stamped with the archive axis's number is still refused; only a delta stamped
with the delta axis's number opens. A layout change to one kind bumps only that
kind's axis and therefore quarantines only that kind's files.

The file kind is a property of the file's name and directory, not of its
contents: nothing on disk records it, and nothing needs to, because every open
path already knows which kind of file it is opening. The on-disk format is
therefore unchanged — the shared version stood at 4 when the axes split, so both
axes start at 4 and every existing file remains valid on its own axis. The
`__duck_store_quarantined_files` axis tag splits accordingly (`schema` becomes
`delta_schema` and `archive_schema`), so an operator can tell a delta-only bump —
fresh data, recovered by reingestion — from an archive-axis bump, which loses the
quarantined files' history.

## Consequences

A delta-axis bump is now cheap by design: it costs only the undrained delta
generations at the moment of the upgrade, which is the same loss a roll already
imposes. Archive history survives any delta layout change, and vice versa. The
histories of the two axes are kept in one comment at their definition and
diverge from the split on; a change touching both kinds' layouts bumps both
axes, and the quarantines it causes are attributed per kind.
