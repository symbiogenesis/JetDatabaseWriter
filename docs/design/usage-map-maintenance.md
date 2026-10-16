# Usage-map ownership and maintenance

JET and ACE address a usage-map row by a one-byte row number and a three-byte
little-endian page number. A table has owned-data and free-space pointers;
each physical index has an owned-tree pointer. Long-value column records in
the TDEF trailer contain a two-byte column number followed by owned and free
pointers. The records end at `FFFF`; the remaining trailer bytes are preserved.

New Jet3 rows are 133 bytes, including a 128-byte INLINE bitmap. New Jet4/ACE
rows are 69 bytes, including a 64-byte bitmap. Existing rows retain their actual
bounds. REFERENCE rows contain bitmap-page pointers; bitmap pages begin
`05 01 00 00`. Table, index and column maps grow through `UsageMapEditor`.
Index rows spill onto additional native-width map pages when existing pages
are full, including FK creation. Allocations go through `PageAllocator`.

## Ownership

Physical page owner zero does not prove that a map row is private.
`UsageMapOwnership` checks live local TDEF chains, including opaque bytes and
column-map pointers, other rows on map-storage pages, and the global allocation
map. It detects row aliases and shared REFERENCE bitmap pages. Traversal has
aggregate definition and candidate budgets and observes cancellation. Mutation
proofs validate unrelated map pointers, bitmap headers and member bounds without
collecting their member sets. Reclamation uses a fresh complete membership proof.
Opaque references to nonlive map slots retain their readable payload members
and bitmap storage after live descriptors detach. Invalid retained payload
bounds refuse reclamation.
Several mutation targets can share one read-only inspection, which ends before
any map or definition write. If isolation requires writes, later preparations
inspect the updated graph.

Before editing shared storage, the writer copies the map into private rows and
bitmap pages and retargets its descriptor. For aliased table owned/free maps it
reconstructs membership from allocated data pages with the table's physical
owner, excluding globally free residual pages. Other owners' rows and bitmaps
remain unchanged. Cached table-map decisions are invalidated when definitions
or pointer layouts change and restored or reset with transaction rollback.

Index maps describe the current live tree after insert, delete, merge and
rebuild. On every format, reclamation excludes members retained by other maps
or reachable trees. Table rewrites, table drops and removed FK descriptors release map-row
and bitmap storage only after checking every surviving definition. Uncertain
opaque references retain storage; they do not authorize freeing it.

## Free space and long values

DAO leaves a data page in the free-space map until an attempted row does not
fit. It then clears that candidate. Deleting a row packs its payload space,
preserves surviving row-slot identities, and restores the free-space hint.
An emptied slot is a deleted overflow tombstone at the removed row's end;
insertion appends a new slot. The 255-slot limit still applies. Ordinary and
overflow deletion reclaim only the selected storage, retaining unrelated moved
targets. Secure erase also clears freed gaps. These hints are conservative:
the writer verifies the actual row and slot capacity before reuse.

Each external Memo/OLE column has separate native trailer pointers. Its LVAL
pages stay out of the table's data map. Single-page values share and reuse free
LVAL pages within that column; deletion removes their payload space and updates
the column maps. Empty pages return to the global allocator even without secure
erase. Native chained values list every page in the owned map and leave the
free map empty, including a partially filled final page. Chain chunks are not
used as ordinary single-value row space. Schema rewrites create independent
column maps while preserving surviving column numbers and opaque metadata.
Release preparation leaves unused empty column maps unchanged, except when
shared global bitmap storage requires isolation before allocator writes. A
release-only batch can retain prepared map pointers while clearing membership
bits. Definition, catalog, map-reference or global bitmap topology changes
invalidate that preparation; shared-page refresh prepares fresh maps. The
batch discards all state when it exits.

Calculated Memo columns can retain native Text descriptors. Their persisted
calculated result type determines long storage; schema creation and rewrites
author their map records even while the table is empty.

Prepared surgical index splits reserve their precomputed append addresses
through `PageAllocator`, which updates global allocation bits before publication.
Abandoned, unlinked page runs and failed bitmap allocations are released;
published changes and their allocator state participate in transaction rollback.

## Evidence

`NativeDataPageFreeSpaceTests` uses independently DAO-authored Jet3, Jet4 and
ACE databases to compare deletion, slot counts, failed-fit hints and managed
reuse, followed by native mutation, primary-key checks and compact/reopen.
`NativeFreeMapGrowthTests` verifies free-map REFERENCE promotion across the
native INLINE window and native compact/readback.

`Jet3SystemUsageMapDaoTests` exercises core REFERENCE maps, shared rows and
shared bitmaps through catalog growth, first relationship creation, native
cascades, query creation and compact. `Jet3IndexUsageMapTests` covers a naturally
full map page, FK spill allocation, large tree growth/deletion, native key
order and constraint enforcement through compact. `LongValueUsageMapTests`
compares native column trailers, packed single values, chained-value free-map
behavior, reuse and compact on all three formats.
`CalculatedLongValueMapTests` preserves native calculated descriptors, column
identities and map records through populated and empty rewrites; the native
calculated-column rename tests verify evaluation and compact/readback.

Synthetic ownership and overflow tests separately cover aliases, malformed
storage, global bitmap protection, byte-preserving refusal, cancellation and
rollback. These do not establish exhaustive native feature coverage or full
Compact & Repair; the corpus and remaining storage work retain their separate
requirements.
