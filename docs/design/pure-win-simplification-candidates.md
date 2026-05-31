# Pure-Win Simplification Candidates

Status: architecture scouting note
Date: 2026-05-30

This note captures the remaining simplification opportunities that still look
large enough to matter after the first high-impact backlog closed. The goal is
not formatting, file splitting, or abstraction for its own sake. A candidate
belongs here only if it can plausibly delete or consolidate meaningful code
while preserving performance, readability, Access/DAO compatibility, and the
current public API shape.

The strongest pattern found is not another page codec. It is repeated
writer-side table-row scanning code that could reuse the same owned-page and
live-row infrastructure already used by reader table scans.

## Selection Criteria

- Delete or collapse real algorithms, data structures, or parallel code paths.
- Preserve or improve performance by reusing existing fast paths and caches.
- Keep compatibility-sensitive logic byte-for-byte equivalent unless tests or
  fixtures prove the behavioral change is intentional.
- Avoid broad rewrites whose only benefit is naming, formatting, or moving code
  between files.
- Prefer helpers that clarify ownership boundaries already present in the
  codebase.

## 1. Shared Table Row Walker and Owned-Page Locator

Primary files:

- [../../JetDatabaseWriter/AccessReader.cs](../../JetDatabaseWriter/AccessReader.cs)
- [../../JetDatabaseWriter/AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs)
- [../../JetDatabaseWriter/Catalog/CatalogWriter.cs](../../JetDatabaseWriter/Catalog/CatalogWriter.cs)
- [../../JetDatabaseWriter/Relationships/RelationshipCatalogStore.cs](../../JetDatabaseWriter/Relationships/RelationshipCatalogStore.cs)
- [../../JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs](../../JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs)
- [../../JetDatabaseWriter/Pages/UsageMap.cs](../../JetDatabaseWriter/Pages/UsageMap.cs)

Why this looks like the best next pure win: reader-side scans already have a
usage-map-aware owned-page discovery path in `AccessReader.GetOwnedDataPagesAsync`.
That path reads the table's owned-page usage map, validates data-page ownership,
caches stable results when no journal is active, and falls back to a whole-file
owner index when the map is unfamiliar or corrupt.

Several writer-side catalog and mutation helpers still repeat the lower-level
pattern directly:

- Iterate every physical page from page 3 to EOF.
- Read the page.
- Skip non-data pages.
- Check `DataPage.TDefOff` against the target TDEF page.
- Enumerate live rows with `EnumerateLiveRowLocations`.
- Decode simple columns from each live row.
- Return the page in a `finally` block.

That code appears in catalog reads, non-table object-id allocation, relationship
catalog discovery, complex-column ID and flat-table lookups, cascade cleanup,
ACE cleanup, and writer `GetLiveRowLocationsAsync`.

Target shape:

- Introduce a shared internal row-walk surface, such as `TableRowScanner` or
  `OwnedDataPageLocator`, usable by both `AccessReader` and `AccessWriter`.
- Expose an internal method that returns candidate data pages for a TDEF page,
  using the owned-page usage map when possible and the current full-file owner
  index fallback otherwise.
- Expose row iteration as `(pageNumber, page, RowLocation)` or a narrow visitor
  callback so callers can decode only the columns they need without materializing
  full typed rows.
- Preserve the existing cached reader table-scan path and avoid adding async
  enumerable overhead to hot writer paths where a visitor-style method is
  clearer and cheaper.
- Route writer `GetLiveRowLocationsAsync` through the shared page locator.
- Convert repeated catalog, relationship, and complex-column scans one at a
  time after the shared primitive is covered by tests.

Likely payoff:

- High deletion potential across `AccessWriter`, `CatalogWriter`,
  `RelationshipCatalogStore`, and `ComplexColumnManager`.
- Likely performance improvement for Access-authored or writer-authored tables
  with recognized owned-page maps, because many writer helpers can stop walking
  every physical page.
- Better consistency in page-return and cancellation behavior.

Risks and guardrails:

- Preserve whole-file fallback for corrupt, unfamiliar, or missing owned-page
  maps.
- Preserve journal semantics: cached owned-page lists are safe only when the
  active journal cannot make page ownership stale.
- Keep writer mutation paths careful around pages that are read for inspection
  and then later modified.
- Do not force full typed row decoding where callers only need one or two
  scalar catalog columns.

Proof plan:

- Characterize the shared locator with inline usage maps, reference usage maps,
  missing maps, corrupt maps, and empty tables.
- Run catalog, relationship, complex-column, index, and schema-evolution tests.
- Run DAO CompactDatabase validation for fresh databases, relationships,
  complex columns, linked tables, and catalog mutations.
- Benchmark or at least time representative writer operations on databases with
  many pages but small target system tables, where avoiding full physical scans
  should show up clearly.

## 2. Delete the Stale Index Leaf Facade

Primary files:

- [../../JetDatabaseWriter/Indexes/IndexLeafIncremental.cs](../../JetDatabaseWriter/Indexes/IndexLeafIncremental.cs)
- [../../JetDatabaseWriter/Indexes/IndexPageCodec.cs](../../JetDatabaseWriter/Indexes/IndexPageCodec.cs)
- [../../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs](../../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs)
- [../../JetDatabaseWriter/Indexes/IndexMaintainer.cs](../../JetDatabaseWriter/Indexes/IndexMaintainer.cs)
- [../../JetDatabaseWriter.Tests/Indexes/IndexLeafIncrementalTests.cs](../../JetDatabaseWriter.Tests/Indexes/IndexLeafIncrementalTests.cs)

Why this is interesting: the earlier index simplification work moved most
layout-aware read/write behavior into `IndexPageCodec`, and mutation planning
now lives in `IndexBTreeEditor`. `IndexLeafIncremental` remains as a historical
facade with many pass-through methods to `IndexPageCodec`, plus one meaningful
entry-list splice helper.

Target shape:

- Move callers of page-header reads, entry decoding, child-pointer reads, and
  leaf rebuilds directly to `IndexPageCodec` or `IndexLeafPageBuilder`.
- Preserve the splice algorithm as a small focused helper, such as
  `IndexEntrySplicer` or `IndexEntrySet.Splice`.
- Delete `IndexLeafIncremental` once it no longer owns a distinct concept.
- Rename or retarget tests from `IndexLeafIncrementalTests` to codec/splicer
  tests instead of keeping a test suite for a deleted facade.

Likely payoff:

- Moderate direct deletion.
- Clearer index ownership: codec for page format, cursor for navigation,
  editor for mutation planning, splicer for in-memory entry-list edits.
- Less confusion around the term "incremental," since multi-leaf and multi-level
  mutation now lives elsewhere.

Risks and guardrails:

- Keep Jet3 and Jet4/ACE layout selection explicit through
  `IndexLeafPageBuilder.LeafPageLayout` or a successor descriptor.
- Preserve all existing single-leaf, multi-leaf, and intermediate-page tests.
- Do not change `Splice` ordering semantics: duplicate keys currently retain a
  deterministic tie order by tagging the original entry order.

Proof plan:

- Run the full `Indexes` test namespace.
- Run index relationship and DAO compact tests that exercise maintained B-trees.
- Diff emitted page invariants before and after for representative index pages.

## 3. Unify Insert Batch Mutation Flow

Primary files:

- [../../JetDatabaseWriter/AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs)
- [../../JetDatabaseWriter/Indexes/UniqueIndexChecker.cs](../../JetDatabaseWriter/Indexes/UniqueIndexChecker.cs)
- [../../JetDatabaseWriter/Relationships/RelationshipEnforcer.cs](../../JetDatabaseWriter/Relationships/RelationshipEnforcer.cs)
- [../../JetDatabaseWriter/Schema/ConstraintRegistry.cs](../../JetDatabaseWriter/Schema/ConstraintRegistry.cs)
- [../../JetDatabaseWriter/ValueDecoding/RowMapper.cs](../../JetDatabaseWriter/ValueDecoding/RowMapper.cs)

Why this is interesting: the public row insertion paths differ mostly in how
they convert caller input into `object[]` rows. After mapping, they run the same
pipeline:

- Apply column constraints and collect AutoNumber checkpoints.
- Materialize pending rows so unique checks can see the whole batch.
- Pre-check unique indexes.
- Enforce foreign keys.
- Insert row bytes and record row locations.
- Augment in-memory relationship sets.
- Maintain indexes incrementally or fall back to rebuild.
- Update AutoNumber high-water values.
- Roll back inserted rows and restore AutoNumber counters on failure.

Both `InsertRowsAsync(string, IEnumerable<object?[]>)` and
`InsertRowsAsync<T>(string, IEnumerable<T>)` carry this structure separately,
and the single-row path contains a smaller version of the same mutation and
rollback protocol.

Target shape:

- Introduce an internal prepared-insert-batch helper that accepts a row mapper
  or already-normalized `object[]` rows.
- Keep public overload validation at the overload boundary, but share the
  mutation pipeline after rows are normalized.
- Keep rollback behavior explicit and testable: inserted row rollback,
  TDEF row-count adjustment, and AutoNumber restoration should remain one
  cohesive unit.
- Avoid hiding the ordering of constraint, FK, unique-index, row-write, and
  index-maintenance phases behind overly generic callbacks.

Likely payoff:

- Moderate to high deletion in `AccessWriter`.
- Better reliability: one rollback path to audit for duplicate-key, FK, and
  index-maintenance failures.
- Easier future work on bulk insert performance because there is one main
  insert pipeline.

Risks and guardrails:

- Preserve AutoNumber assignment timing before unique checks.
- Preserve FK parent-set augmentation after each successful insert.
- Preserve current exception behavior and best-effort rollback behavior.
- Do not allocate extra row copies in hot insert paths.

Proof plan:

- Run writer insert, bulk insert, unique-index, AutoNumber, FK, transaction,
  and index-maintenance tests.
- Add focused tests that make each phase fail and assert rows/counters/indexes
  are left in the expected state.
- Benchmark bulk insert with and without indexes to verify allocation and
  throughput stay neutral or better.

## 4. Make Logical TDEF Chains a Small Shared Data Structure

Primary files:

- [../../JetDatabaseWriter/AccessBase.cs](../../JetDatabaseWriter/AccessBase.cs)
- [../../JetDatabaseWriter/AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs)
- [../../JetDatabaseWriter/Relationships/RelationshipManager.cs](../../JetDatabaseWriter/Relationships/RelationshipManager.cs)
- [../../JetDatabaseWriter/Schema/TDefPageBuilder.cs](../../JetDatabaseWriter/Schema/TDefPageBuilder.cs)

Why this is interesting: `AccessBase.ReadTDefBytesAsync` already stitches a
TDEF page chain into a logical byte buffer for readers. Relationship mutation
needs the same logical buffer plus the physical page numbers so it can write
changes back, so `RelationshipManager` carries a parallel
`LogicalTDefChain`, logical-capacity math, page materialization, and write-back
helpers.

Target shape:

- Introduce a schema-level `LogicalTDefChain` helper that can:
  - Read and stitch a TDEF chain.
  - Retain physical page numbers when requested.
  - Compute logical capacity and page counts.
  - Ensure logical buffer capacity.
  - Materialize logical bytes back into physical TDEF pages.
  - Write the chain, allocating or deallocating continuation pages as needed.
- Route `AccessBase.ReadTDefBytesAsync` through the read-only side of that
  helper.
- Route relationship TDEF mutation write-back through the mutable side.

Likely payoff:

- Moderate deletion in `RelationshipManager` and `AccessBase`.
- Fewer independent copies of page-chain math.
- More obvious ownership for future schema mutations that need multi-page TDEF
  write-back.

Risks and guardrails:

- Preserve the logical layout exactly: first page contributes the full page;
  continuation pages contribute bytes after offset 8 only.
- Preserve next-page pointers and first-page free-space/tdef-length fields.
- Preserve deallocation behavior for no-longer-needed continuation pages.
- Keep malformed-chain handling compatible with current `ReadTableDefAsync` and
  relationship mutation behavior.

Proof plan:

- Run schema, relationship, index, and catalog tests covering multi-page TDEFs.
- Add focused tests for growing and shrinking a multi-page TDEF chain.
- Run DAO CompactDatabase validation for relationship create, rename, and drop.

## 5. Promote Numeric Fixed-Point Payload Encoding into `NumericEncoder`

Primary files:

- [../../JetDatabaseWriter/ValueEncoding/NumericEncoder.cs](../../JetDatabaseWriter/ValueEncoding/NumericEncoder.cs)
- [../../JetDatabaseWriter/ValueEncoding/RowEncoder.cs](../../JetDatabaseWriter/ValueEncoding/RowEncoder.cs)
- [../../JetDatabaseWriter/Indexes/IndexKeyEncoder.cs](../../JetDatabaseWriter/Indexes/IndexKeyEncoder.cs)

Why this is interesting: `NumericEncoder` already owns decimal decomposition,
but row storage and index-key encoding both still perform similar work around
natural scale, declared scale, `BigInteger` rescaling, and 16-byte unsigned
mantissa shaping. The output frames differ, but the middle algorithm can be
shared.

Target shape:

- Add a helper that converts a decimal value and target scale into sign,
  natural scale, and a 16-byte big-endian unsigned magnitude.
- Let `RowEncoder.EncodeNumericValue` use it for JET 17-byte column storage.
- Let `IndexKeyEncoder.EncodeNumericEntry` use it before applying
  Access/Jackcess index-key twiddling rules.
- Keep precision checks and exception types compatible with existing callers.

Likely payoff:

- Smaller deletion than the row-walker or insert-flow work, but clean and
  bounded.
- Reduces the chance that future Numeric fixes land in one path and not the
  other.

Risks and guardrails:

- Numeric precision, rounding, and scale behavior is compatibility-sensitive.
- Preserve current differences between row storage and index-key output frames.
- Preserve legacy Jet4 MDB vs newer ACCDB numeric index twiddling rules.

Proof plan:

- Add edge-case tests for zero, negative values, max precision, scale rounding,
  overflow past 16-byte mantissa, and declared-scale index keys.
- Run numeric value-encoding, index seek-key, unique-index, FK, and writer
  round-trip tests.
- Benchmark numeric-heavy row writes and index maintenance for neutral results.

## Lower-Payoff Candidates

These may still be worth doing opportunistically, but they do not meet the
current "large meaningful simplification" bar on their own.

### Binary Slice Helpers

`DirectRowDecoderBuilder`, `RowDecodePlan`, complex-column attachment decoding,
and some tests all contain tiny byte-slice or data-URI parsing helpers. A shared
helper could remove a few lines and standardize edge behavior, but the payoff is
small and it should not distract from the larger row-walker or insert-flow work.

### LVAL Row Location Wrapper

`LongValueDecoder` mostly delegates row-location math to `LongValueStore`. There
may be a small cleanup around passing cached row bounds directly, but this is not
large enough to treat as architecture work unless another LVAL refactor is
already underway.

### Text Linked-Table Enumeration

`LinkedTableManager` has separate surfaces for counting rows, streaming rows,
metadata, and materializing a `DataTable`. Some common source opening and row
normalization is already shared. Further consolidation might be useful, but the
operations are different enough that the likely deletion is modest.

## Candidates Not Recommended as Pure Wins Right Now

### Text Index Encoder Strategy Rewrite

Primary files:

- [../../JetDatabaseWriter/Indexes/Collation/GeneralLegacyTextIndexEncoder.cs](../../JetDatabaseWriter/Indexes/Collation/GeneralLegacyTextIndexEncoder.cs)
- [../../JetDatabaseWriter/Indexes/Collation/GeneralTextIndexEncoder.cs](../../JetDatabaseWriter/Indexes/Collation/GeneralTextIndexEncoder.cs)
- [../../JetDatabaseWriter/Indexes/Collation/General97TextIndexEncoder.cs](../../JetDatabaseWriter/Indexes/Collation/General97TextIndexEncoder.cs)
- [../../JetDatabaseWriter/Indexes/Collation/GeneralTextIndexEncoder.V2010LongRowSuffix.cs](../../JetDatabaseWriter/Indexes/Collation/GeneralTextIndexEncoder.V2010LongRowSuffix.cs)

There may be a strategy-shape simplification hiding here, especially around
shared table loading, framing, and long-row suffix hooks. However, collation
encoding is byte-level compatibility work with a history of fixture-driven
edge cases. The risk-to-deletion ratio is not attractive unless the work begins
with a byte-for-byte characterization harness over existing Access-authored
fixtures and DAO-generated samples.

This is a possible future research project, not the next pure-win deletion
candidate.

### Generic Page Builder Abstraction

TDEF pages, data pages, index leaves, index intermediates, LVAL pages, and CFB
sectors have different headers, trailers, ownership rules, and validation
contracts. A generic page-builder abstraction would likely add indirection and
reduce readability without deleting much real code.

### Compound File Dependency Replacement

The local CFB layer was already reassessed and kept. Its runtime surface is
narrow, internal, and tailored to Office Crypto streams. Adding a dependency
would trade local code for supply-chain, package-size, and attack-surface risk.

### Row Decode Plan Redux

The major row-reader consolidation already landed. Further changes should be
motivated by specific performance data or bug fixes, not another broad pass over
row decoding.

## Suggested Order

1. Shared table row walker and owned-page locator.
2. Delete or shrink `IndexLeafIncremental` into a focused splicer helper.
3. Unify insert batch mutation flow.
4. Extract logical TDEF chain read/write helpers.
5. Promote Numeric fixed-point rescaling and magnitude shaping into
   `NumericEncoder`.

The first item has the best blend of deletion, readability, and likely
performance upside. It also gives later catalog, relationship, and complex-column
work a better foundation because those areas currently pay the most repeated
scan boilerplate.