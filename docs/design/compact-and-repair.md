# Managed Compact & Repair

## Contract and current boundary

This is the implementation and acceptance plan for I17, not an implemented
maintenance API. `ShrinkDatabaseAsync` only removes globally free tail pages.
Native DAO compact tests validate writer output; they do not exercise a managed
compact or repair implementation.

The operation must rebuild into a separate database, validate that result, and
preserve the original until replacement is safe. Preserve the source format,
collation, code page, encryption parameters, security permissions, object
identities, relationships, complex-item identities and counters. Do not execute
queries, expressions, macros, VBA, links or external providers to recover data.

Microsoft's [DAO CompactDatabase contract](https://learn.microsoft.com/en-us/office/client-developer/access/desktop-database-reference/dbengine-compactdatabase-method-dao)
requires exclusive access and a distinct destination and copies security
permissions. Microsoft's [repair guidance](https://support.microsoft.com/en-us/access/compact-and-repair-a-database)
acknowledges partial repairs and records unrepaired objects in
`MSysCompactErrors`. The managed contract must make every loss and recovery
decision explicit. Partial recovery must never authorize automatic source
replacement.

The [native damage controls](compact-and-repair-evidence.md) show why successful
compaction, reopen and row counts cannot establish lossless recovery. The repair
validator must compare actual surviving content and constraint behavior.

## Read-only inventory probe

`JetDatabaseWriter.FormatProbe storage-audit` streams bounded NDJSON observations
for physical pages, catalog objects, TDEF chains and known usage-map references.
It hashes one decrypted physical page at a time and marks every region pinned.
It does not validate row values, LVAL chains, index trees, opaque incoming
references or native constraints, and it does not authorize relocation or repair.

For example, run the built probe with an absolute input and a new absolute output
outside the checkout:

```powershell
$report = Join-Path ([IO.Path]::GetTempPath()) ("jdw-storage-audit-" + [Guid]::NewGuid().ToString('N') + '.ndjson')
dotnet run --project JetDatabaseWriter.FormatProbe -c Release -- storage-audit --input C:\data\source.accdb --output $report
```

The command opens the source read-only, refuses pending recovery journals and
existing output files, and never follows linked-table references or executes database content.
Windows sharing excludes cooperating writers; Unix access still requires
cooperation. The source is not called an immutable snapshot on an unqualified
filesystem. Native page-encrypted inputs can use `--password-environment NAME`;
compound encrypted packages are outside this probe's native-file contract.

Page, catalog-object, reference, diagnostic, schema and UTF-8 output limits apply.
The CLI exposes `--max-pages`, `--max-objects`, `--max-references`,
`--max-diagnostics`, `--max-schema-bytes` and `--max-report-bytes`;
`storage-audit --help` lists defaults. Cancellation,
corruption, I/O failure or a budget refusal returns nonzero and retains any
partial report. A final `audit-terminal` with `InventoryCompleted: true` describes
only the stated inventory coverage; it is never a value-completeness,
relocation, repair or power-loss verdict. A successful process exit also requires
report flush and close to succeed.

## Prerequisites

Dependencies apply to the named requirements, not every unrelated requirement
in the corresponding TODO item.

| Requirement | Existing foundation | Remaining dependency |
|---|---|---|
| Bound discovery before allocating | Per-definition and aggregate catalog limits; bounded usage-map ownership inspection; streaming pinned physical/catalog/TDEF/map inventory | S1: collection overhead, cache lifetime, remaining traversal and output budgets; cancellation throughout |
| Preserve stored values | Strict write-back scans; refusal for unreadable mapped content; unreadable-LVAL refusal | S3/F8 and the bounded raw-copy part of I6: distinguish native error envelopes, Null and damaged values; preserve raw unassigned bytes; stream rows and large LVALs |
| Preserve metadata and identities | Immutable raw TDEF images; original table-root publication; catalog field preservation | S7/S9: complete raw descriptor/header/trailer preservation and inventory application, security, replication and unknown objects |
| Rebuild indexes and maps | Native table/index/LVAL map maintenance; conservative alias proofs | I2: verified tree ownership, shared roots, cross-TDEF index numbers and native key ordering; F9 only where keys must be regenerated |
| Validate native behavior | DAO36 Jet3 and DAO120 Jet4/ACE helpers and Microsoft-authored fixtures | E1/E2: healthy and deliberately damaged fixture comparisons for the managed result, with reopen, reads and constraint checks |
| Replace the source safely | Private staging, flushed backups and process-interruption tests | E4/F6 and the file-recovery part of I15: coordination, recovery discovery, namespace persistence and actual power-interruption evidence |

The compact-copy engine must avoid the existing table rewrite snapshots; it can
implement the required bounded raw copy without completing I6's separate
in-place-update and seek optimizations.

Index prefix compression can include row bookmarks and intermediate child-page
pointers. Changing page numbers can change compressed entry lengths, so a
same-page pointer patch can overflow. The compact-copy engine needs a bounded
raw-key tree builder and disk-backed relocation/fence records, preserving stored
canonical keys when possible instead of regenerating collation-sensitive keys.

Individual attachment editing/history (R8), query planning, cache performance,
benchmarks and the custom-stream recovery contract are not prerequisites for a
path-based compact-copy implementation that preserves existing raw identities.
Unsupported encryption providers cannot be treated as decryptable; refusal must
identify that dependency rather than produce an unencrypted or incomplete copy.

## Work packages

1. **I17.1 — Inventory and preservation proof.** Inventory every catalog object,
   local TDEF chain, known reference, allocation map and opaque region. Include
   system tables, hidden complex tables, linked-table catalog metadata and
   application objects. Retain hashes and byte ranges for opaque content. Prove
   which references can be retargeted and which pages must retain their identity;
   an unknown byte sequence is not proof that it contains no references.
2. **I17.2 — Bounded diagnosis and recovery plan.** Walk independently readable
   storage with explicit byte, object, page, row, chain and diagnostic limits.
   Record conflicting ownership, invalid bounds, cycles, unreadable values,
   missing objects and fallback scans. Distinguish a bad hint/map/index from
   damaged authoritative content. An ambiguous boundary or owner must not
   authorize reclamation. Preserve real I/O failures and cancellation.
3. **I17.3 — Compact copy.** Copy live rows and raw long values without lossy
   CLR conversions or recalculation. Rebuild indexes and usage maps with verified
   references, sharing and counters; omit only storage proven disposable. Use
   bounded buffers and disk spill rather than whole-database/table snapshots.
   Preserve pinned opaque storage or refuse a relocation that cannot preserve it.
   A byte-identical copy or tail shrink alone does not satisfy compaction.
4. **I17.4 — Validation and report.** Reopen the closed destination and validate
   the complete object inventory, stored values, LVAL lengths, identities,
   counters, constraints, index reachability/order and allocation ownership.
   Compare opaque bytes against the preservation proof. Emit a bounded report
   naming every recovered and unrecoverable location and decision. A partial
   salvage result is explicitly incomplete, even when its readable rows reopen.
5. **I17.5 — Publication and recovery.** Connect the validated result to a proven
   replacement protocol under exclusive ownership. Refusal, cancellation and
   validation failure preserve the source. Recovery must identify and validate
   original/staging/replacement evidence before choosing a copy, survive another
   interruption during replay, and retain uncertain evidence. Pending journals
   and stale locks require explicit ownership rules.
6. **I17.6 — Native acceptance.** Compare managed and native compact outcomes on
   Microsoft-authored Jet3, Jet4 and ACE specimens, including supported encrypted
   variants, relationships, complex values, application objects and unknown
   metadata. Introduce independently specified damage in fixture copies and
   record exact damage offsets, expected surviving content, hashes, native
   errors and every managed recovery decision. Check native reopen/read/write,
   uniqueness/FK enforcement and byte preservation, not just compact exit codes.

I17.1 and the bounded-diagnosis foundation can proceed independently of the
filesystem work. I17.3 needs their preservation and ownership proofs; I17.4
needs the resulting copy and report. Native fixture preparation and durability
investigation can proceed in parallel. Automatic replacement waits for I17.4
and the proven I17.5 protocol.

## Durability evidence and completion

The current [transaction/replacement protocol](transaction-crash-recovery.md)
flushes file contents but does not establish persistence of directory entries.
Neither injected exceptions nor killing a process proves power-loss safety.
Retaining a flushed backup until a rename is not by itself a solution to F6.
Any proposed managed protocol must specify recovery discovery when directory
updates are lost, its provisioning preconditions, and a durable commit decision.
Verify actual power interruption during preparation, publication, recovery and
cleanup on every filesystem for which the guarantee is claimed. macOS process
coordination also needs E4 evidence.

An EOF-footer-only journal inside the original file does not establish a durable
replacement protocol either. Its first interrupted append can persist a changed
file length without any valid locator for the original EOF. A design needs a
qualified bootstrap/anchor and recoverable decision/cleanup ordering; neither
page alignment nor a hashed footer resolves those assumptions automatically.
Windows, WSL and ordinary GitHub Actions can supply development, modeled faults,
process termination and cross-platform checks. They do not, by themselves,
provide physical device/controller power-interruption evidence. Keep that boundary
explicit until a qualified rig and storage profile exist.

Remove I17 only after all six packages meet their contracts, the validation
matrix records the native and durability evidence with exact revisions, and
the settled main revision passes the required hosted gates. Preserve residual
external dependencies explicitly; safe refusal or a partial API does not erase
the compatibility requirement.
