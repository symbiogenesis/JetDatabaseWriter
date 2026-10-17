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
`--max-page-reads`, `--max-map-steps`, `--max-records` and
`--max-working-bytes` additionally limit repeated traversal, bitmap bytes and
directory slots (including zeroes), emitted observations and working-storage
admission. Defaults are 8,388,608 page reads, 268,435,456 map steps, 8,388,608
records and 512 MiB of admitted storage. `storage-audit --help` lists all defaults.

Before inventory I/O, storage admission reserves 4 MiB of scratch, 1,024 bytes
per allowed catalog object, and sixteen times the per-definition byte allowance.
These conservative allowances include collection capacity and simultaneous
definition representations. Catalog discovery scans physical owner headers
without constructing a whole-file ownership index or relying on map hints.
Damaged ownership can still hide catalog identities; this scan does not prove
catalog completeness or authorize reclaiming any page.
Admission excludes caller/visitor allocations, runtime overhead and shared buffer-pool
retention, and is not an exact heap guarantee. The scan owns uncached definition
and ownership readers instead of populating caller caches. Report serialization
uses a fixed bounded buffer before writing each record.

Cancellation, corruption, I/O failure or a budget refusal returns nonzero and
retains any partial report. A final `audit-terminal` with `InventoryCompleted: true` describes
only the stated inventory coverage; it is never a value-completeness,
relocation, repair or power-loss verdict. A successful process exit also requires
report flush and close to succeed.

## Prerequisites

Dependencies apply to the named requirements, not every unrelated requirement
in the corresponding TODO item.

| Requirement | Existing foundation | Remaining dependency |
|---|---|---|
| Bound discovery before allocating | Per-definition and aggregate catalog limits; bounded usage-map ownership inspection; scan-owned uncached physical/catalog/TDEF/map inventory; shared raw-row/LVAL/index discovery accounting and simultaneous storage admission | S1: integrate discovery sessions with whole-database enumeration and inventory/catalog lifetime; complete remaining public-scan accounting; qualify runtime/pool and caller-owned memory separately |
| Preserve stored values | Strict write-back scans; retained unknown-page evidence through owner-index fallback; malformed LVAL directory and calculated-slot refusal; bounded raw row and LVAL streaming; complete defined LVAL-chain observations including trailing storage | S3/F8 and the bounded raw-copy part of I6: distinguish native error envelopes, Null and damaged values; preserve raw unassigned row fields; integrate streamed rows and LVALs into compact copy |
| Preserve metadata and identities | Immutable raw TDEF images; original table-root publication; catalog field preservation; retained continuation headers and original trailing padding during chain writes | S7/S9: complete schema-rewrite descriptor/header/trailer preservation and inventory application, security, replication and unknown objects |
| Rebuild indexes and maps | Native table/index/LVAL map maintenance; conservative alias proofs; bounded raw-key and reference observations with complete observed-graph cycle checks | I2: complete tree ownership, shared-root/cross-TDEF proofs, separator fences, global ordering and a bounded raw-key tree builder; F9 only where keys must be regenerated |
| Validate native behavior | DAO36 Jet3 and DAO120 Jet4/ACE helpers and Microsoft-authored fixtures | E1/E2: healthy and deliberately damaged fixture comparisons for the managed result, with reopen, reads and constraint checks |
| Replace the source safely | Private staging, flushed backups and process-interruption tests | E4/F6 and the file-recovery part of I15: coordination, recovery discovery, namespace persistence and actual power-interruption evidence |

The compact-copy engine must avoid the existing table rewrite snapshots; it can
implement the required bounded raw copy without completing I6's separate
in-place-update and seek optimizations.

Raw LVAL streaming copies the descriptor-declared stored payload without text,
OLE or calculated-value interpretation. Byte and chain-row budgets bound the
copy, and each destination write finishes before its source page is released.
This preserves opaque payloads without establishing what a native error envelope
means. Failure can leave a partial destination, which must remain private and
must never be published as a validated value. Following the declared payload
length does not prove that trailing bytes or remaining chain pointers are
disposable.

`CompactRawRowReader` streams one table's stored row slices from physical owner
headers, retaining the original header bookmark and moved-row location. It
validates directory geometry, follows bounded same-owner overflow chains and
refuses live-target aliases. Physical size, repeated reads, emitted rows,
inspected slots and fixed working storage have separate limits. The awaited
visitor borrows the source page; it must not retain that memory. This avoids
field decoding and table snapshots, but does not prove missing ownership,
incoming aliases, field/LVAL integrity or safe relocation. Overflow-header padding
and unreachable deleted payloads require a separate preservation decision.
Standalone budgets apply to one table invocation; a whole-database driver must
enumerate all tables and use the shared session accounting described below.

`CompactIndexAudit` observes one root's raw stored keys, bookmarks and known
index references with bounded collections and entry bytes. It validates page
geometry/type/owner and in-page order without regenerating collation-sensitive
keys. Its terminal record states the limited coverage and retains pinned
storage. Reaching its observed graph is not a proof of separator fences,
bookmark targets, global order, shared roots or incoming-reference ownership.
Every cycle in the observed entry-child/intermediate-tail graph is rejected by a
bounded iterative walk, including cycles crossing tail aliases in separate
branches. Acyclic tail/entry aliases remain observable. Terminal acyclicity does
not prove exclusive page ownership or complete tree validity.
Standalone budgets apply to one root; whole-database orchestration and a raw-key
tree builder remain open. These internal primitives do not extend the CLI
inventory's completion contract.

`CompactLongValueAudit` observes the entire supplied column slice and every
complete row reached through defined external references, following chained
pointers to null even after the descriptor's declared payload is covered. It
retains descriptors, tokens, next pointers and trailing bytes as borrowed raw
observations. Zero-length external descriptors still expose their references.
Dangling references, trailing cycles, malformed directories and missing declared
bytes refuse completion while retaining partial observations. All storage stays
pinned; neither observed trailing bytes nor an acyclic chain establishes
exclusive ownership, native value semantics or safe relocation.

`CompactDiscoverySession` composes repeated raw-row, raw-index and raw-LVAL audit
and payload-copy calls against one uncached page source. Read, object, row,
directory-slot, LVAL byte/row, index byte/entry/page, reference, graph-step and
observation counters persist across calls and failures. Conservative storage
reservations cover simultaneous scans, including a value operation awaited from
a row visitor, and release on completion or failure. Runtime overhead, shared-pool
retention and caller-owned buffers, streams and retained observations are
separate. The caller must enumerate every required object and value: a session
does not discover those identities, prove whole-database coverage or validate a
compact result. Inventory/catalog accounting still has its own lifetime.

Logical TDEF chain writes retain the first four bytes of existing continuation
headers on the same physical pages, while rewriting their next-page pointers.
They preserve original padding beyond both the old declared end and the new
logical end; metadata deliberately removed by a shrink is cleared. New pages use
canonical headers. This storage-level preservation does not prove that an
uninterpreted reference can survive relocation or a generated schema rewrite.

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
