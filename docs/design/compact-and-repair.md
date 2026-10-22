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
dotnet run --project tools/JetDatabaseWriter.FormatProbe -c Release -- storage-audit --input C:\data\source.accdb --output $report
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
Catalog discovery also independently audits the native Id index and compares
its stored keys with resolved live rows' full stored Ids. Matching bookmarks can
retain catalog identities despite damaged physical owners and absent or corrupt
map hints. Bounded moved-row recovery additionally requires deleted payload slots,
independent owner/index evidence along the moved chain and a matching stored Id; cycles,
live payload aliases and conflicting recovered bookmarks refuse that recovery.
When the original header owner is also damaged, moved-payload owner damage can
be retained only after the complete independent Id-index audit establishes its
ordered leaf chain, bookmark order and conservative key routing. A late index
failure discards candidates that relied on damaged moved owners. Deleted-slot
geometry, complete signed Id matching and cross-bookmark alias checks still
apply. An intact original owner with damaged moved ownership now defers that bookmark
to independent index recovery while retaining other readable physical rows.
When the Id index cannot supply complete independent evidence, a declared unique
ParentIdName index can corroborate the full stored ParentId/Name key using its
supported native collation. That path requires complete graph, leaf-chain,
bookmark-order and routing audits and lossless complete stored Name text;
unsupported or damaged text/collation withholds recovery. The signed Id remains
part of the referenced raw row. Every accepted physical or independently recovered identity requires exactly one assigned native-width Id, ParentId, Name, Type and Flags field, with disjoint storage and lossless complete Name decoding under its compression eligibility. Required-field inspections consume shared field admission. Discovery can try later declared Id or unique ParentIdName descriptors after unavailable evidence, retaining cross-pass conflicts and cumulative reference, directory and overflow work.
Recovery observations and
diagnostics retain that ambiguity; they do not repair
page headers or prove exclusive ownership. Positive map conflicts
without sufficient independent evidence still refuse. Unavailable or damaged independent Id and ParentIdName indexes,
unproven moved-payload ownership and missing independent references can still hide
identities. This scan does not prove catalog completeness or authorize
reclaiming any page.
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
| Bound discovery before allocating | Per-definition and aggregate catalog limits; bounded usage-map inspection; independent catalog Id and unique ParentIdName key evidence despite damaged headers/map hints, including intact original headers with damaged moved owners and complete independently audited key/chain/routing evidence; uncached physical/catalog/TDEF/map inventory; one session accounts inventory, catalog and raw-row/field/LVAL/index work and simultaneous storage through local-table orchestration; independent discovered-local-table plan coverage checks original bytes and parsed projections | S1: complete remaining public-scan accounting and catalog recovery when physical ownership and both supported independent key paths are unavailable; qualify runtime/pool and caller-owned memory separately |
| Preserve stored values | Strict write-back scans; retained unknown-page evidence through owner-index fallback; malformed LVAL directory refusal; bounded raw row and field streaming with retained assignment states; bounded calculated ResultType hydration and complete defined LVAL-chain observations, including trailing storage; private dense raw-row packing with original-header bookmark mappings and independent reopened raw-allocation/ledger comparison; complete raw LVAL staging with retargeted descriptor/chain pointers installed in verified packed rows and per-column allocation observations; independent reopened data/LVAL byte and private allocation coverage validation; bounded per-table DATA/LVAL/map assembly with original-column allocation-ledger comparison, independent complete table/map and pointer-only image comparison; bounded serialized whole-database raw assembly and independently reopened active catalog/table/index/global-map/prefix/allocation validation; native-authored calculated Byte error/Null envelope byte preservation through assembly and native compact | S3/F8: interpret remaining native value/error variants independently of Null and damaged values for diagnosis and recovery; complete ownership and source coverage before reclaiming retained storage |
| Preserve metadata and identities | Immutable raw TDEF images; original table-root publication; catalog field preservation; projected raw header/trailer bytes and source index sharing/directions during schema edits; retained continuation headers and original trailing padding during chain writes; complete known table/index/LVAL pointer patching within pinned raw definitions; all-chain-preflight installation on original page identities and independent reopened header/body comparison; exact complete pinned-prefix comparison permitting only independently reconstructed TDEF pointers and validated native global-map bytes | S9: complete application, security, replication and unknown-object inventory and semantic preservation requirements |
| Rebuild indexes and maps | Native table/index/LVAL map maintenance; conservative alias proofs; bounded raw-key graph, entry-edge fence and observed leaf-chain checks; separate complete observed-edge key-envelope, conservative key-routing and bookmark-inclusive order proofs; bounded same-owner bookmark-target validation; cross-root declared-owner and incoming-reference observations; stored-key staging with disk-backed fences and monotone disk-backed row-bookmark lookup; independent reopened private stored-key/bookmark, child-maximum and allocation coverage validation; bounded private native usage-map construction from ordered allocation runs and independent reopened exact membership/allocation validation; composed relocated stored-key tree/map assembly with references returned after reopened validation; disk-backed exact whole-destination allocation ledgers retaining the complete source prefix; composed per-table physical stored indexes, maps and pinned chain installation; independent canonical native global-map installation/comparison with self-covered bitmap storage; independent native descriptor-map membership and exact observed separator/child-maximum comparisons; bounded database-wide private assembly driver and exact independently discovered active destination inventory | I2: prove safe sharing and complete incoming-reference ownership beyond known declarations, complete stored-key/row correspondence beyond supported scalar and positive complex-reference keys against corroborated active rows, plus native sharing validation, and extend the bounded discovered-table driver and independent active destination inventory into a reclaiming compact copy; F9 only where keys must be regenerated |
| Validate native behavior | DAO36 Jet3 and DAO120 Jet4/ACE helpers and Microsoft-authored fixtures | E1/E2: healthy and deliberately damaged fixture comparisons for the managed result, with reopen, reads and constraint checks |
| Replace the source safely | Private staging, flushed backups and process-interruption tests | E4/F6 and the file-recovery part of I15: coordination, recovery discovery, namespace persistence and actual power-interruption evidence |

The compact-copy engine must avoid the existing table rewrite snapshots; it can
implement the required bounded raw copy without completing I6's separate
in-place-update and seek optimizations.

Raw LVAL streaming preserves observed Jet4/ACE page-header tokens, including nonzero tokens; independent reopened validation compares each row's page token with its source observation. Token changes require separate private pages. Jet3 retains its row-count field at offset 8.

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
refuses live-target aliases and shared moved-payload starts across live row
identities. The tracker includes intermediate overflow rows and distinct deleted
slots that name the same physical payload start. Its storage is admitted lazily
before the first overflow target and released with the scan; scans without
overflow retain only fixed storage. Physical size, repeated reads, emitted rows,
inspected slots, tracked overflow targets and working storage have separate
limits. The awaited
visitor borrows the source page; it must not retain that memory. This avoids
field decoding and table snapshots, but does not prove missing ownership,
incoming aliases, field/LVAL integrity or safe relocation. Overflow-header padding
and unreachable deleted payloads require a separate preservation decision.
Standalone budgets apply to one table invocation; a whole-database driver must
enumerate all tables and use the shared session accounting described below.

`CompactIndexAudit` observes one root's raw stored keys, bookmarks and known
index references with bounded collections and entry bytes. It validates page
geometry/type/owner, in-page key order and inclusive entry-edge-only key ranges
without regenerating collation-sensitive keys. Its terminal record separates
the range and sibling-chain proofs from acyclicity and retains pinned storage.
Bookmark targets, missing leaves, shared roots and incoming-reference
ownership remain unverified.
Every cycle in the observed entry-child/intermediate-tail graph is rejected by a
bounded iterative walk, including cycles crossing tail aliases in separate
branches. Acyclic tail/entry aliases remain observable. Terminal acyclicity does
not prove exclusive page ownership or complete tree validity.
Standalone budgets apply to one root. A successful terminal separately records
inclusive entry-edge-only fence checks and whether every observed leaf belongs to
one reciprocal, closed, ordered sibling chain. All tail paths remain outside
the entry-edge-only fences, including tails not proven to be aliases. Missing sibling observations do not establish global order.

`ObservedKeyRoutingValidated` separately certifies that separator/tail descent
for each encoded key present in the observed leaves can start at or before its
first matching leaf
and continue through the observed closed, ordered sibling chain. A bounded
iterative DAG pass propagates conservative key-range hulls through entry and
tail aliases. Incomplete chains, empty-leaf uncertainty, uncovered routing gaps
or over-approximation can withhold this proof without declaring corruption.
`BookmarkInclusiveOrderValidated` separately checks bookmark tie breakers for
in-page order, entry-edge-only fences and that chain. Uncertain native duplicate
bookmark ordering remains readable when this optional proof is false. Neither
flag establishes missing-leaf coverage, stored-key/row correspondence or safe
relocation. These internal primitives do not extend the CLI inventory's
completion contract.

`AllObservedEdgeFencesValidated` separately checks complete observed leaf-key
envelopes on every entry-child and tail edge, including aliases, using a bounded
iterative postorder. It compares encoded keys without requiring bookmark ties
or exact separator/leaf correspondence. A redundant native tail alias can widen
an envelope and withhold this proof even when conservative key routing succeeds.
The flag is therefore not a universal native-tree acceptance requirement.

`ObservedChildMaximumSeparatorsValidated` separately compares every stored
intermediate separator's complete key and bookmark with its child's greatest
observed leaf entry through every child/tail edge, including aliases. Empty
descendants, different keys or different bookmark ties withhold this proof.
Equal-key maxima retain the actual greatest bookmark even when the separate
optional bookmark-order proof is unavailable. Exact separators do not establish
that stored keys describe rows or that all source rows and leaves were found.

`CompactIndexRowAudit` independently derives keys from physically readable
same-owner raw scalar rows and compares every leaf and intermediate key and bookmark. Included
rows must appear exactly once; an IgnoreNulls descriptor omits only all-null
keys. Date keys retain their stored IEEE double rather than converting through
calendar rounding. Strict lossless text decoding and supported stored
collations are required. Positive four-byte complex parent references retain their stored identities without following hidden items. Calculated, long-value and unsupported key
interpretations refuse. Complete observed reciprocal leaf chains, conservative routing, exact child maxima and independently resolved bookmark payloads must agree. The result covers the physically discovered row set;
it does not choose which historical same-owner rows remain active or establish
missing ownership and complete source coverage.

`CompactActiveTableRows` independently reopens one exact ordinary TDEF, strictly
reads its native table-owned map, audits a supported covering index's complete
graph, ordered leaf chain, routing, exact child maxima and bookmarks, and derives
every active scalar key from raw rows. The map, complete covering entries and
declared row count must agree. Every other readable same-owner row is reported
with borrowed raw bytes as unassigned and pinned; it is never declared dead or
reclaimable. Disposable authority is bound to the exact immutable source and
definition. Every consumption rechecks row coordinates and payload hashes,
replays unassigned observations and shares lifetime work/storage admission with
the surrounding session. Failed consumption expires the authority.

`CompactActiveTableRows.ValidateIndexesAsync` compares every physical descriptor with the same independently corroborated active rows. Repeated consumption and index audits serialize and share lifetime work and storage admission; each comparison replays pinned unassigned rows. Unsupported key interpretations refuse the all-index proof. Both database construction and independent reopened validation require this proof for an opted-in active table. This establishes correspondence for the agreed supported rows, while missing owners, unrecovered catalog membership and opaque incoming references remain separate.

The database driver requires an explicit covering-index ordinal and a mandatory
unassigned-row observer before opting a table into this scope. Independent
reopened comparison derives fresh source authority and checks active-table and
unassigned-row counts against construction. Catalog recovery retains its separate
identity proof and conflicting recovered catalog bookmarks refuse ordinary
active ownership. Legacy `mdbtools/nwind.mdb` Umsätze (TDEF753) has 830 corroborated active
rows and 2,049 additional readable same-owner rows. Complete private assembly
retains and reports all 2,049 in its compared original prefix while rebuilding
the 830 active rows. This scoped choice establishes neither global discovery,
exclusive incoming ownership nor permission to reclaim the retained rows.

`CompactIndexBookmarkAudit` composes raw index observations with an explicit
target check for each leaf and intermediate entry. Its ordinary path verifies a
same-owner live row header, complete row-directory geometry and bounded moved-row
chains through deleted payload slots. Verified entries identify the resolved
payload page and slot; successful completion marks all observed bookmark targets
validated only when no target used accepted recovered catalog evidence. Recovered
targets and terminals have separate flags and withhold the same-owner proof. Repeated target reads, directory slots and simultaneous scratch
storage are bounded and can share discovery-session accounting through
`ScanIndexBookmarksAsync`. Raw-only scans retain separate coverage. Neither
target validity nor a matching owner proves that the stored key describes that
row, that all rows are indexed, or that targets have exclusive incoming owners.

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
separate. `ScanInventoryAsync` shares inventory/catalog reads, object admission,
schema
bytes, diagnostics, map steps, references, observations and working storage with
the session. `ScanDatabaseAsync` visits each discovered local definition, its
raw rows and nonzero physical index roots with bookmark-target verification,
including the catalog and hidden
local tables. Inventory reservations remain held through those callbacks.
Linked metadata stays pinned and is not followed. `ScanDatabaseFieldsAsync` uses
`CompactRawFieldReader` to validate each row's complete variable-offset table and
stream its original field slots with absent, Null, Boolean and assigned-storage
states. Null fields retain any physical slot bytes. Calculated envelopes remain
opaque; they are neither decoded nor recalculated. Calculated definitions hydrate
persisted ResultType through bounded matching catalog LvProp reads, with full
signed Id matching and cumulative read, payload, schema and parser-storage
admission. Every repeated ResultType declaration must agree on one supported
type; conflicting, malformed or unsupported declarations remain unknown.
An absent or unknown ResultType still emits the raw assigned
field and refuses automatic discovery rather than guessing from its TDEF storage
discriminator. Hydration consumes both physical catalog rows and final accepted
recovered bookmarks. Recovered payload coordinates and complete signed Ids are rechecked
through bounded reads before field discovery and property hydration. Only exact
accepted catalog bookmarks can bypass the physical-owner check in catalog index
audits; recovered flags disclose this evidence while withholding the ordinary
same-owner bookmark proof. These rows remain pinned and do not establish complete
catalog coverage or exclusive incoming ownership. Each assigned field with a
verified MEMO/OLE value type is automatically passed through the complete raw LVAL
audit with the original table, column and row identities. Field observations have their own cumulative
allowance and also consume the shared observation allowance. No field collection
or table snapshot is retained. The raw-row entry point still allows explicit
value work in its awaited visitor.

Both database entry points hold a `CompactIndexOwnershipAudit` through all
physical descriptor/root scans. Each descriptor declares its root and exact
ownership-map row. Declarations, observed pages and known incoming child, tail
and sibling references are cross-checked for contradictory expected TDEF owners.
An index root, page or reference target cannot also be a declared map data page.
Exact map-row aliases remain explicit, including aliases between tables; distinct
map rows can share a data page. Missing roots/maps, same-owner shared roots/pages
and unobserved reference targets are streamed explicitly. Terminal counts retain
those gaps instead of treating a missing descriptor reference as complete.
The tracker bounds retained page and map-row coordinates, descriptor attempts,
references, inspection work and observations; its conservative storage reservation
remains held alongside inventory and per-root storage. Incoming counts describe
observations, including repeated scans, rather than distinct physical edges.
Its terminal proves only consistency of supplied known observations. Map-content
membership is independently inspected for every supplied nonzero descriptor map.
Complete INLINE bits and REFERENCE pointers/bitmap bytes have separate cumulative
read, inspection, membership and retained-storage admission. Every observed index
page must occur in each declared map for its root. Shared map rows and bitmap
pages remain explicit, DATA/index/bitmap role conflicts refuse, and unused map
members remain pinned gaps. Opaque or missing incoming references and native
sharing semantics remain unverified, so all observed storage stays pinned.

A damaged owner can still hide an object; this orchestration does not prove
complete database coverage or validate a compact result. Cumulative allowances
persist after failures and repeated scans.

Logical TDEF chain writes retain the first four bytes of existing continuation
headers on the same physical pages, while rewriting their next-page pointers.
They preserve original padding beyond both the old declared end and the new
logical end; metadata deliberately removed by a shrink is cleared. New pages use
canonical headers. This storage-level preservation does not prove that an
uninterpreted reference can survive relocation or a generated schema rewrite.

Index prefix compression can include row bookmarks and intermediate child-page
pointers. Changing page numbers can change compressed entry lengths, so a
same-page pointer patch can overflow. `CompactRawIndexBuilder` appends a fresh
tree to an explicitly private, page-aligned destination from ordered stored
canonical keys and caller-supplied relocated bookmarks. It recompresses keys
with the native codecs, holds one page group, and alternates two caller-owned
disk-capable fence streams between levels. Entry, canonical-byte, page, output,
repeated fence-I/O, level and working-storage limits apply. Invalid order,
out-of-range pointers, oversized entries and nonreducing levels refuse.
Completion requires flush and cancellation checks and returns an unvalidated
staging result. Failure may leave private partial bytes. Whole-database
relocation records, index/map ownership, native acceptance beyond controlled
trees and destination validation remain required before publication.

`CompactRawIndexValidator` independently audits a reopened private tree and
compares every leaf's stored key and relocated bookmark with immutable original
observations and a separately reopened row ledger. It also compares every
intermediate entry with its independently read child maximum and requires exact
contiguous level, sibling, tail, root, owner, height and allocation coverage.
Original entries must come from completed independent source observations;
builder output and fence scratch cannot serve as that evidence. Raw graph work,
canonical comparison bytes, repeated destination reads, levels and simultaneous
storage are bounded; row-ledger lookup retains its own cumulative read allowance.
The proof applies to the private builder layout. It preserves supplied keys
without proving source-entry completeness, their semantic correspondence to
rows, global ownership or complete database validity.

`CompactRawDataBuilder` streams one table through the discovery session into
dense direct-row pages appended to a separate private decrypted destination.
It preserves exact row slices and records original header bookmarks, including
moved-row identities, in an eight-byte-per-row disk-capable relocation ledger.
Generated pages retain the native writer's 255-row cap. Source traversal consumes
session allowances; staging separately bounds rows, pages, output, ledger bytes
and its simultaneous storage reservation. Both streams must flush before a
complete allocation is returned. Partial bytes remain private on any failure.

`CompactRowRelocationMap` validates strictly increasing source and destination
bookmarks with fixed storage, then performs serial disk-backed binary searches
under a cumulative I/O allowance. Stored index keys remain verbatim; monotone
relocation preserves bookmark tie order. Missing mappings refuse. The caller
keeps the ledger immutable and distinct from the source and destination.
Raw packing, rebuilt index bookmarks and per-table LVAL pointer staging have
focused composition checks. Catalog/TDEF references, maps and opaque metadata
still require preservation or retargeting. These internal components neither produce a complete database nor
authorize source replacement.

`CompactRawDataValidator` independently scans the immutable source and a reopened
private data allocation/ledger with one destination page and a fixed record buffer.
It compares every original header identity and exact raw payload against the next
dense destination slot, verifies direct-row geometry, native owner and free-space
counts, and requires every declared row, page and ledger record to be covered.
Cumulative read admission includes partial-read retries; real I/O errors propagate.
This validates raw packing before reference patches, not LVAL relocation, maps,
constraints, metadata or the complete destination database.

`CompactRawLongValueBuilder` stages complete referenced external rows into private
LVAL pages and replaces only defined descriptor and chain pointers. It retains
column tokens, raw payloads, whole row slices and storage beyond the declared
payload, including zero-length external values and trailing chain rows. One
pending output page retains a predecessor until its successor's destination
bookmark is known, avoiding whole-value collections and random patch writes.
Output, row, raw-byte and working-storage admission combines with the discovery
session's cumulative traversal limits. Only successful traversal and flush return
an unvalidated allocation. The returned descriptor requires installation in its
staged raw row; native maps and metadata require separate assembly. Incoming ownership,
unreferenced storage and opaque padding remain separate preservation dependencies.

`CompactRawDataLongValueRelocator` composes those primitives for a known table
with verified definitions and persisted calculated result types. It requires a
successful raw data allocation and its validated immutable matching ledger. Each
mapped dense destination row must exactly match its complete source bytes before
that row is patched. Assigned raw long values append to the private destination;
only defined four-byte external descriptor pointers change. Unassigned raw slots,
tokens, envelopes and other bytes remain intact. An awaited allocation visitor
exposes original field/header identities and appended ranges for disk-backed
allocation ledgers or future per-column map assembly without retaining a table.
Aggregate rows, values, output, raw bytes, destination read requests and pointer
writes have limits alongside the session's work and simultaneous storage limits.
Partial reads are bounded; I/O, cancellation, visitor and flush failures return
no completion result. This pass must precede other row-reference patches. It
does not prove incoming ownership, deduplicate shared storage, construct maps or
validate the complete database; source storage remains pinned.

`CompactRawDataLongValueValidator` independently compares immutable source rows
and complete raw LVAL observations with reopened private data/LVAL allocations
and their complete relocation ledger. It permits only defined descriptor and
chain-pointer changes, compares trailing storage, and checks dense private row
coverage and disjoint sequential LVAL allocations. Missing or extra ledger
records, aliased private chains, changed opaque bytes and uncovered allocation
slots refuse completion. Fixed page buffers, destination read allowances and
the session's cumulative work/storage admission bound this pass. This validates
the supplied private allocations, not global incoming ownership, metadata/maps,
constraints or the complete database.

`CompactUsageMapBuilder` streams ordered disjoint allocation runs into an
independently addressed private native map row. It emits an INLINE map when the
range fits and streams REFERENCE bitmap windows otherwise, using fixed page
storage. Run, membership, output and working-storage limits apply. Every member
must precede the map's append point. The returned map and bitmap allocation is
not included in that map's membership; global allocation assembly must account
for it separately. Native representability, cancellation and flush failures
leave private partial bytes and return no completed result.

`CompactUsageMapValidator` independently reads a reopened private map against
immutable ordered allocation runs. It checks native row geometry, every set bit,
exact membership and count agreement, and complete sequential private bitmap
coverage. Missing or extra members, aliased or external bitmap pointers,
uncovered pages and unnecessary empty reference bitmaps refuse completion.
Expected runs are supplied independently; deriving them from the map under test
would not establish the comparison. Fixed page storage, inspected bitmap bytes
(including zeroes), enumeration and requested read bytes (including partial-read
retries) have separate admission. This validates the builder's private layout,
not arbitrary native maps, expected-run ownership or global allocation state.

`CompactTDefReferencePatcher` replaces the table map pair, every physical index
root/map and every existing known column LVAL map pair in a copied raw logical
definition. Complete descriptor/trailer boundaries and ordered complete
replacement lists are required. All other bytes, including counters, sort
metadata, logical relationship descriptors and trailing padding, remain exact.
The TDEF root and continuation identities stay pinned, so catalog identities and
relationship partner roots retain their original meanings. Installation must
preserve the original continuation headers. This bounded pointer patch is not
a schema rewrite, map-content validation or proof that opaque references permit
reclamation. Complete allocation selection and whole-database assembly remain
required. Focused Jet3/Jet4/ACE composition checks clone a writer-created source,
append packed data/maps, patch a pinned definition, then reopen through ordinary
map-selected reads. They preserve the original storage and every unrelated prefix
byte; they do not update global allocation accounting or establish reclamation.

`CompactRawTableBuilder` composes packed DATA rows, the complete row relocation
ledger, raw LVAL relocation, table maps, existing per-column LVAL maps and a
pointer-only patched logical TDEF image. A fixed twelve-byte disk-capable ledger
records each external value's original column and appended page range. Repeated
per-column scans build ordered maps without retaining a table's allocations in
memory; free-space maps use the staged pages' actual row capacity. Output pages
and bytes are admitted across all phases, and private destination/ledger read
requests share cumulative admission. Parser storage, retained map results and
image copies have conservative working-storage admission before staging.
Index replacements remain caller-verified inputs, and an external column without
an existing verified native map record refuses instead of inventing metadata.

`CompactRawTableAllocationValidator` independently rescans source fields and
reopened DATA/LVAL rows, compares every per-column ledger record with the
independently covered destination range, and requires complete ledger coverage.
Changing or swapping column identities, omitting/adding records or changing page
ranges refuses even when all raw row and LVAL bytes still compare. The immutable
validated ledger can then provide expected runs for separate reopened map
validation. Its fixed ledger buffer and nested raw comparison share the local
working allowance; source-session admission and ledger read work remain bounded.
These components return private staging/comparison evidence. The caller still
must install the patched image on its original pinned TDEF chain, assemble
catalog/index/global allocation references, prove reclaimability and validate
the whole destination. Original/global storage remains pinned. Controlled native tests compare original, staged and native-compacted Memo/OLE contents on Jet3/Jet4/ACE; native read/compact does not establish mutation with the unchanged global allocation maps. Raw staging copies stored LVAL bytes without applying the ordinary Memo writer's compression or encoding policy.

`CompactRawTableValidator` composes independent raw DATA/LVAL and original-column
ledger comparison with exact validation of every table and per-column map.
Expected membership comes from the compared allocations; free-space membership
comes from their validated page geometry. The complete table allocation must be
contiguous in staging order, with no gaps or aliased map storage. Map work and
private reads have cumulative limits across all columns. Reconstructing the
pointer-only logical definition patch independently detects unrelated changed
bytes. Installed physical TDEF chains, indexes, catalog completeness, global
allocation and unknown incoming references remain separate requirements.

`CompactTDefChainInstaller` preflights original and private chains before writing
patched bodies on the original pinned page identities. The separate reopened
chain validator compares physical headers and expected logical bytes without
trusting the installer's completion evidence. These private operations preserve
the source and do not establish that the caller's patch or incoming references
are complete.

`CompactRawIndexAssembly` composes stored-key relocation, private tree packing
and its exact ownership map. A separate reopened comparison checks the original
stored entries through the immutable row relocation ledger, tree geometry and
map membership before returning replacement TDEF references. It does not infer
semantic key/row agreement, source-index completeness or exclusive incoming
ownership from a successful private comparison.

`CompactGlobalAllocationBuilder` records a pinned original prefix and each appended
allocation in a disk-capable ledger. Independent expected role, owner and page
ranges supplied to `CompactGlobalAllocationValidator` must cover the destination
through EOF exactly once. This accounts for
map storage as well as its members; it neither installs native global free maps
nor proves that the supplied allocations exhaust unknown incoming references.
Original pages remain pinned. Native global-map installation is a separate
component; complete database assembly, reclamation and reopened database
validation are still required.

`CompactPinnedTableAssembly` composes one table's raw DATA/LVAL/maps, every
physical stored index and pointer-only installation on the original TDEF chain.
Its independent reopened validator compares original stored entries, both
immutable ledgers, exact private allocations and installed definition bytes.
Later table or global-map allocations may follow its exact compared extent;
their coverage belongs to the complete allocation ledger. Private I/O, observed
entries/key bytes, output and simultaneous storage have aggregate admission.
`CompactPinnedTableAssemblySession` retains source discovery, private read/write
requests, original entry/key bytes and output admission across serialized table
operations. Construction counts append high-water growth before writes, including
failed requests; independent validation admits each complete compared extent.
Pinned-chain source reads share the discovery counters. Working reservations
unwind on every exit, while work charges survive failures and repeated calls.
Build and independent validation can use separate sessions with separate phase
allowances. Standalone global-map, prefix and allocation-ledger components still
have separate budgets; the database validation coordinator additionally composes
their requested-read and simultaneous-storage admission.
Caller-owned streams, retained result evidence and runtime/pool
storage remain outside the admitted working-storage estimate. Physical root/map
aliases refuse until sharing is independently supported. Logical sharing within
one physical descriptor retains its original metadata.

`CompactDatabasePlanValidator` independently inventories the immutable source
and requires exactly one matching original definition for every discovered local
table, including `MSysObjects`, hidden and system tables. Missing, duplicate and
extra owners refuse, as do different raw bytes or parsed definition projections.
Linked and other catalog-only objects remain pinned and are never followed.
Membership storage and inventory traversal share cumulative discovery admission.
This binds a proposed table plan to the discovered source; damaged ownership can
still hide objects, so it does not establish complete discovery.

`CompactPinnedDatabaseValidator` composes that plan check with every reopened
table/index/definition comparison, canonical native global-map validation,
complete pinned-prefix comparison and exact whole-destination allocation-ledger
comparison. One requested-read allowance covers source, destination and sidecars,
including partial-read retries and failed requests. Table sidecars open serially
through caller factories and can stay on disk. The coordinator owns no
whole-table snapshots, restores borrowed stream positions and returns no
completion evidence on refusal, cancellation or I/O failure. Each invocation has
its own phase allowance. Caller observations and provider work remain outside
that allowance. Composed comparisons still do not establish source-index
completeness, unknown incoming ownership, native acceptance, reclamation or
permission to publish a compact database.

The coordinator also independently compares the complete discovered source
catalog with the reopened destination's active catalog. Exact signed Id,
ParentId, Name, type, flags and local definition membership must match.
Independently compared raw catalog rows, LVAL/maps, row ledgers and retained
source DATA bytes establish scoped active membership before destination
inventory runs. All inventory and membership reads share the coordinator's
cumulative read admission. Matching discovered inventories do not establish
damaged-source completeness or opaque reference semantics.

`CompactPinnedDatabaseBuilder` validates the complete discovered-local-table plan
before creating output, streams the decrypted source prefix one page at a time,
and serializes all raw table, LVAL, stored-index, map and pinned-definition
assemblies. Caller factories provide distinct private destinations and sidecars,
which may remain on disk. One catalog recovery scope spans every table so a
recovered catalog row with a damaged user-table owner cannot be copied as user
data. The driver installs the canonical global map and complete allocation
ledger, closes every output, then independently reopens them for the composed
private comparison. Construction reads, writes, output growth and retained
evidence are bounded across phases; reopened validation has a separate bounded
allowance. Failures retain private partial output and leave the source untouched.
The driver's internal image is decrypted private staging. Reclamation, complete
native acceptance and durable publication remain required.

`CompactEncodedDatabaseCopy` encodes closed private staging with its exact
original native header and password. It checks the complete staging header and
provider before creating output, preserves every original provider/security
parameter, streams one page at a time and closes construction handles before
independently reopening staging and encoded output. A fresh password codec
compares every decrypted output page with staging. Requested reads, writes,
output size, scratch and aggregate password hashing across both codec opens have
explicit bounds. Factories transfer handle ownership, run serially and retain
partial private output on failure. Supported native Jet3 RC4, Jet4 RC4, ACE
Agile and ACE Standard fixture assemblies pass managed password reopen and
native mutation, primary-key refusal, password compact and readback. This is a
codec-equality proof; it does not establish structural validity, reclaimability,
durability or publication permission. Unsupported providers remain dependencies.

The controlled Access16.0.20430.20182 fixture in
`Databases/ApplicationObjects/NativeInertApplicationObjects.accdb` contains an
inert saved query, blank form/report and one fixed data row. Complete raw catalog,
application storage, index and prefix comparisons retain their stored identities
and bytes through private assembly. `CompactPinnedApplicationObjectsTests`
checks native query metadata and Forms/Reports container names through DAO
compact without executing the saved query or application objects. Separate
native SaveAsText controls retain byte-identical form/report exports through
assembly and native compact. This evidence does not cover macros, VBA modules,
replication, every security configuration or opaque incoming references.

`CompactNativeRowBoundaryTests` uses frozen DAO3.6/DAO120 controls to verify
native fixed-only rows, dropped fixed-slot gaps and actual Jet3 jump markers at
256/512-byte boundaries. Complete private assembly preserves every stored row
byte and visible field; native compact/reopen preserves controlled values.
For Id LONG plus fixed binary fields split at 255 bytes, Jet3 accepts 2000 value
bytes and refuses 2001 at schema append with DAO 3047; Jet4/ACE accept 4051 and
refuse 4052. Accepted raw rows occupy 2003/4056 bytes including native overhead.
These are limits for that schema shape, not universal limits or validation of
writer-emitted rows through physical page capacity. Fixture hashes and producer
details are retained beside `Databases/RowBoundaries/`.
`CompactPinnedSecurityTests` compares the two frozen native Jet4 workgroup
password variants and native-authored restricted copies using an isolated copy
of their MDW. Complete assembly preserves the raw `MSysObjects`/`MSysACEs`
values, owner/SID/ACL and property LVAL bytes, original header and pinned
security storage. DAO120 compares ownership/grants through native compact,
permits owner writes with rollback, distinguishes wrong account password 3029
from wrong database password 3031, and retains read permission 20 with update
denial 3108 for the restricted account. These cases do not implement workgroup
authentication in the managed reader or establish replication and arbitrary
security variants.

`CompactGlobalMapInstaller` replaces only the original page-one global free-map
row in the private destination. It marks every original and appended page used,
retains old bitmap storage and appends canonical REFERENCE bitmaps when needed.
Coverage includes those new bitmap pages themselves, including a boundary that
requires another window. The separate reopened `CompactGlobalMapValidator`
derives the expected row and all bitmap bytes from the original page geometry
and independently recorded pre-map EOF, without trusting installer output.
Every page-one byte outside the map row remains exact. The complete allocation
ledger must also account for appended global-map storage owned by page one.
This conservative map does not reclaim any source page.

`CompactPinnedPrefixValidator` independently reconstructs complete known TDEF
pointer replacements and compares every physical source-prefix byte. Unlisted
definitions, application/security/replication storage, page headers and padding
remain byte-identical. Optional native global-map changes require the concrete
independent global-map validator; arbitrary mutable-page exceptions are not
accepted. Definition traversal, cumulative requested reads and retained patch
storage have bounds. This is a preservation comparison, not proof of complete
catalog discovery, opaque-reference meaning, ownership or safe reclamation.

`CompactPinnedDatabaseCompositionTests` assembles multiple generated tables on
Jet3/Jet4/ACE, then independently compares their reopened table/index allocations,
native global map, complete allocation ledger and entire pinned prefix before
ordinary row reads. These append-only specimens retain all original storage.
They establish component composition, not reclaimed space, complete native
feature coverage or a managed Compact & Repair API.

`CompactPinnedCatalogCompositionTests` additionally assembles every local table
in a generated catalog, including `MSysObjects`, system/security and relationship
tables, hidden local tables and ACE template tables. The catalog's stored Id,
ParentId, Name, type, flags and original TDEF roots survive; physical index keys
retain their bytes while bookmarks follow independently compared row ledgers.
Reopened table/index, global-map, allocation-ledger and complete-prefix checks
precede ordinary schema, row, relationship and linked-metadata reads. Linked
sources are never opened. These specimens establish catalog and multi-object
composition with cumulative phase admission, while complete native object
coverage, source-index completeness, opaque incoming ownership and reclamation
remain open.

A physical recovery scan of an append-only destination can find both pinned
original catalog rows and relocated active catalog rows with the same identities.
Ordinary catalog reads use the installed maps, but physical recovery cannot
discard either copy merely from those hints. `CompactActiveCatalogRows` first
independently compares the reopened raw catalog table, LVAL/maps and complete row
relocation ledger, and compares retained original catalog DATA pages byte for
byte. Its bounded membership evidence is tied to the exact immutable destination
page source and geometry. Only a destination inventory explicitly supplied that
live evidence can exclude the compared retained bookmarks; unverified copies
refuse. Ordinary damaged-source inventory retains strict duplicate-Id refusal.
This is a scoped active-catalog inventory proof, not complete damaged-source
recovery, incoming ownership or a database-validity verdict.

Accepted independent catalog recoveries participate in active membership and
raw assembly through a bounded explicit recovery scope. Physical and recovered
rows merge in original bookmark order without a whole-table snapshot. Their
complete raw identities and payload locations are rechecked, and exact accepted
catalog bookmarks are excluded from conflicting noncatalog owner scans. Active
destination evidence also scopes whole-database row/field discovery; unrelated
physical duplicates still refuse.

Focused native global-map controls force REFERENCE storage and compare the
unchanged original/control content, allocation growth during Memo inserts,
update/delete, duplicate-key refusal, native compact and exact readback.
The DAO3.6 Jet3 control uses a Microsoft-authored host. Fresh Jet3 bootstrap
page-one global-map rows also use the format's native 133-byte width, including
128 bitmap bytes; Jet4/ACE use 69-byte rows. `Jet3NativeExternalMemoTests`
compares that geometry with a frozen DAO3.6 specimen and checks fresh external
Memo mutation, native table allocation, constraints and compact/readback.
Existing imported map rows retain their declared geometry. Controlled native
map acceptance does not establish complete managed compact acceptance or safe
reclamation.

`CompactPinnedTableAssemblyTests` composes these components on generated Jet3,
Jet4 and ACE tables with a primary index and external Memo storage. It validates
stored-index replacements before installing the final pinned definition, then
compares reopened raw tables, chains and full allocation ledgers and checks
ordinary catalog, row and stored-index reads. Those specimens retain original
global maps and every original page. They establish private component composition,
not native mutation acceptance, complete database coverage or reclaimed space.

Query planning, cache performance,
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

The available Windows host and focused Linux/macOS CI now exercise actual
recovery interruptions, repeated replay and modeled namespace survivors through
`RecoveryReplayCrashTests` and `ReplacementNamespaceModelTests`. Preparation-only journals now undergo full validation before replay; invalid evidence refuses. Generation-bound lock receipts permit repeated reopen after recovery and later writer deaths while preserving foreign, live or malformed ownership evidence. Random replacement candidates still need persistent generation discovery and a durable decision; loss of every journal/receipt name remains unqualified. The Windows writable-directory flush probe demonstrates
a runtime-specific managed mechanism worth qualifying; it does not establish a
portable barrier or replace filesystem-qualified generation discovery and commit ordering. See the
[recovery evidence and limits](transaction-crash-recovery.md#regression-evidence).
Remove I17 only after all six packages meet their contracts, the validation
matrix records the native and durability evidence with exact revisions, and
the settled main revision passes the required hosted gates. Preserve residual
external dependencies explicitly; safe refusal or a partial API does not erase
the compatibility requirement.
