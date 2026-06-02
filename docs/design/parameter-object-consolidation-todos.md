# Parameter Object Consolidation TODOs

**Status:** Proposed backlog, created 2026-06-01.

This document tracks follow-up work from a production-source scan for methods
with multiple parameters that may be better served by existing or new domain
types. The goal is not to eliminate every long signature. The goal is to reduce
boolean/scalar parameter bundles where the code is already passing a coherent
domain concept through several layers.

## Scan Summary

- Production source had 1,226 members with two or more parameters.
- The high-arity tail was small: 65 production members had seven or more
  parameters.
- Many high-arity entries were already model/layout/descriptor constructors and
  should not be treated as refactoring candidates by arity alone.
- The best candidates were repeated bundles that cross method boundaries,
  especially where a public or nearby internal type already represents the same
  concept.

## Priority Strategy

Work in priority order. Start with changes that reduce high-arity private
helpers without adding new named types, then add internal domain types only
where repeated bundles remain error-prone. Public API additions are deliberately
out of scope for this backlog unless caller workflows show a concrete ergonomic
payoff; arity alone is not enough.

- **P0: No-new-type arity reductions.** Highest impact per unit of risk. These
  should reuse existing domain types, split private helpers, or pass existing
  structs deeper through helper layers.
- **P1: Internal domain-type consolidation.** Use this once P0 leaves a real
  repeated concept crossing method boundaries. New types here should stay
  internal and allocation-free.

## Cross-Cutting Design Constraints

Apply these to every item below so the refactor is idiomatic without
regressing performance or security.

- **Prefer no-new-type fixes first.** If a high-arity method can be made clearer
  by using an existing type such as `RowBound` or `FkSidePlan`, do that before
  adding a new model.
- **Prefer `readonly record struct` for internal bundles.** Match the existing
  peer style (`RowBound`, `RowLocation`, `IndexEntry`, `FkSidePlan`,
  `LongValueDescriptor`, `LockFileSettings`). A class would add a per-call heap
  allocation on hot paths such as index seeks, which is exactly the regression
  this refactor must avoid.
- **Account for the `async` + `in` interaction.**
  `IndexCursor.FindRowLocationsInRangeAsync` is `async ValueTask`, so the C#
  language forbids `in` / `ref` / `ref readonly` parameters there. New bundle
  structs must be passed by value on async signatures. Keep them small (a few
  references plus a few bytes of flags) so the copy is cheap.
- **Do not add defensive copies that were not there before.** Public types like
  `IndexKeyBound` already copy their inputs. Internal encoded buffers produced
  by `EncodeIndexKeyPrefix` are already fresh single-use arrays, so wrapping
  them in a new struct must not allocate a second copy.
- **Benchmark before/after on hot paths.** When a touched method is exercised
  by `JetDatabaseWriter.Benchmarks`, run the relevant benchmark on `main` and
  on the change and attach the deltas to the PR. For hot paths that are not yet
  benchmarked, such as index-range seeks, add a focused benchmark or record an
  equivalent before/after allocation/perf probe. Adaptive job is preferred per
  the repo BenchmarkDotNet conventions.
- **Honor checked arithmetic and BannedSymbols.txt.** Bundles must not
  introduce narrowing casts that could throw at runtime under the repo's
  global `CheckForOverflowUnderflow`, and must not reintroduce banned APIs.

## Prioritized Action Items

### P0. Reduce high arity without adding new types

These are the first changes to take. They reduce real high-arity functions while
reusing types and helpers that already exist.

#### 1. Push `RowBound` through usage-map helpers

- [x] Update `UsageMap.TryEnumerateInlinePages` to take
  `RowBound rowBound` instead of separate `rowStart` and `rowSize`
  parameters.
- [x] Update `UsageMap.TryEnumerateReferencePagesAsync` the same way.
- [x] Keep `UsageMap.TryEnumeratePagesAsync` as the top-level entry point; it
  already receives `RowBound`, so this change should mostly pass the existing
  value deeper instead of unpacking it.
- [x] Keep `List<long> pageNumbers` as a separate output parameter so callers
  continue to control ownership and capacity.
- [x] Preserve the current strict/non-strict behavior for corrupt or unfamiliar
  maps. Strict mode is a parser-hardening contract; it must not be relaxed by
  the signature cleanup.
- [x] Run owned-data-page discovery tests and any usage-map/reference-map tests.

Payoff: this removes a repeated row-slice pair from two helper signatures
without adding any type. It is localized to the usage-map parser and reuses
`RowBound`, which is already the domain type for this slice.

Relevant code:

- [`UsageMap.TryEnumeratePagesAsync`](../../JetDatabaseWriter/Pages/UsageMap.cs)
- [`UsageMap.TryEnumerateInlinePages`](../../JetDatabaseWriter/Pages/UsageMap.cs)
- [`UsageMap.TryEnumerateReferencePagesAsync`](../../JetDatabaseWriter/Pages/UsageMap.cs)
- [`RowBound`](../../JetDatabaseWriter/Pages/Models/RowBound.cs)

#### 2. Use existing `FkSidePlan` in FK logical-index emission

- [x] Replace the four scalar plan parameters on `EmitFkLogicalIdxAsync`
  (`realIdxNumThisSide`, `logicalIdxNumThisSide`, `allocateNewRealIdx`, and
  `preAllocatedLeafPage`) with the existing `FkSidePlan` value.
- [x] Keep `tdefPage`, `columnNumbers`, `indexName`, relationship-side bytes,
  and `CancellationToken` separate in this first pass.
- [x] Update the two call sites to pass `pkPlan` and `fkPlan` directly.
- [x] Do not introduce `FkLogicalIdxSideSpec`. `FkSidePlan` removes the
  highest-risk parallel scalar group; the remaining named relationship-side
  arguments do not justify a new type without new behavior.
- [x] Run or add relationship tests covering same-table relationships, shared
  existing real indexes, newly allocated real indexes, and cascade flags.

Payoff: this reduces the 13-parameter method using an existing domain value and
removes the highest-risk group of parallel scalars without adding a speculative
side-spec type.

Relevant code:

- [`RelationshipManager.EmitFkLogicalIdxAsync`](../../JetDatabaseWriter/Relationships/RelationshipManager.cs)
- [`FkSidePlan`](../../JetDatabaseWriter/Relationships/RelationshipManager.cs)
- [`RelationshipDefinition`](../../JetDatabaseWriter/Models/RelationshipDefinition.cs)

#### 3. Split the index range matcher before adding range types

- [x] Refactor the private 14-parameter `IndexPageCodec.IsRangeMatch` into
  smaller focused checks inside `CollectRangeLeafEntries`, such as lower-bound,
  required-prefix, and upper-bound checks.
- [x] Do not decode `IndexEntry` objects or allocate canonical keys just to make
  the helper signatures shorter. The matcher must keep comparing directly
  against the compressed page bytes.
- [x] Prefer local functions or tightly scoped private helpers that avoid
  threading the same entry-location scalars through every call. If the helper
  extraction still requires passing `page`, `prefixStart`, `entryStart`,
  `suffixLength`, `prefixLength`, and `isFirstEntry` everywhere, stop and move
  to the P1 encoded-range type instead.
- [x] Keep `AccessReader` and `IndexCursor` signatures unchanged in this pass so
  the behavioral risk is isolated to the codec.
- [x] Run focused index range tests. BenchmarkDotNet `--list flat` confirmed
  there is no current index-seek benchmark target to run.

Payoff: this addresses the single highest-arity private method from the scan
without adding a named type. It does not solve the repeated encoded-bound bundle
across `AccessReader` and `IndexCursor`; that remaining cross-layer concern is
handled in P1 if the P0 cleanup is not enough.

Relevant code:

- [`IndexPageCodec.CollectRangeLeafEntries`](../../JetDatabaseWriter/Indexes/IndexPageCodec.cs)
- [`IndexPageCodec.IsRangeMatch`](../../JetDatabaseWriter/Indexes/IndexPageCodec.cs)

#### 4. Prefer the zero-pointer index page-builder overloads at call sites

- [x] Audit every `BuildLeafPage` / `TryBuildLeafPage` /
  `BuildIntermediatePage` / `TryBuildIntermediatePage` call site that passes
  `prevPage: 0, nextPage: 0, tailPage: 0`. Each of these can switch to the
  existing 4-arg convenience overload and drop three arguments without any API
  change.
- [x] Keep the long-form overloads for genuine sibling-preserving rewrites such
  as `IndexBTreeEditor.TryBuildSplitLeafPages`. Those callers must continue to
  pass `prevPage:` / `nextPage:` / `tailPage:` as **named arguments** so a
  swapped pointer is caught at compile / review time.
- [x] Do **not** replace the three `long` parameters with an unnamed
  `(long Prev, long Next, long Tail)` tuple. A plain value tuple loses the
  named-argument guard at the call site, which is the exact transposition
  hazard this entire backlog is trying to reduce.
- [x] Do not add a dedicated sibling-pointer struct (`IndexSiblingPointers`)
  solely to shorten the remaining long-form overloads. The sibling-preserving
  callers already use named arguments, which keeps the transposition guard at
  the actual call site.
- [x] Run index build/edit tests and focused index maintenance tests.

Payoff: the highest-arity sibling-pointer call sites are the ones already
passing all zeros; switching them to the existing convenience overloads removes
those three arguments outright. Sibling-preserving callers keep their explicit
named arguments and stay as resistant to transposition as they are today.

Implementation notes (2026-06-01):

- Added two additive convenience overloads to mirror the existing
  `IndexPageCodec.TryBuildLeafPage(layout, pageSize, parentTdefPage, entries)`
  shape: `IndexPageCodec.BuildLeafPage(..., bool enablePrefixCompression, int? maxPrefixLength = null)`
  and `IndexBTreeBuilder.TryBuildIntermediatePage(..., int? maxPrefixLength = null)`.
  Both forward to the long-form overload with `prevPage: 0, nextPage: 0, tailPage: 0`.
- Switched six production zero-pointer call sites to the convenience overloads:
  `AccessWriter.cs` (×2 empty-leaf bootstrap), `RelationshipManager.cs` (×2 new
  real-idx leaf allocation), `IndexBTreeEditor.TryMeasureLeafFreeSpace`
  (free-space probe), and the two `IndexHelpers.TryGreedySplitIntermediateInN`
  size-probe call sites.
- Left the existing zero-pointer call site in `IndexMaintainer.cs:1004` alone
  (already uses the 4-arg `TryBuildLeafPage` convenience overload).
- Did not refactor test call sites: tests that pass `0, 0, 0` are typically
  asserting the header bytes those zeros produce, so the explicit zeros
  document the under-test header values.
- Did not introduce a `BuildIntermediatePage` convenience overload — no
  production site emits a zero-pointer intermediate via that entry point;
  the only zero-pointer intermediate writers go through
  `IndexBTreeBuilder.TryBuildIntermediatePage`.

Relevant code:

- [`IndexPageCodec.BuildLeafPage`](../../JetDatabaseWriter/Indexes/IndexPageCodec.cs)
- [`IndexPageCodec.TryBuildLeafPage`](../../JetDatabaseWriter/Indexes/IndexPageCodec.cs)
- [`IndexPageCodec.BuildIntermediatePage`](../../JetDatabaseWriter/Indexes/IndexPageCodec.cs)
- [`IndexPageCodec.ReadSiblingPointers`](../../JetDatabaseWriter/Indexes/IndexPageCodec.cs)
- [`IndexBTreeEditor.TryBuildSplitLeafPages`](../../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs)

#### 5. Trim `AccessWriter.CreateTableInternalAsync` and pass through `CatalogTableArtifact`

- [x] Confirm by code search that the only call site of
  `CreateTableInternalAsync` is `AccessWriter.CreateTableAsync`, which passes
  exactly the four required arguments (`tableName`, `columns`, `indexes`,
  `catalogFlags`) plus `CancellationToken`.
- [x] If no internal caller actually overrides `reservedTdefPageNumber`,
  `emitLvProp`, `markSystemTableTdef`, or `emitAceRows`, **delete those
  defaulted parameters outright**. They are dead options today, and removing
  them drops the arity from 9 to 5 with no behavior change and no new type.
- [x] After the dead-default removal, fold the remaining four scalars into a
  `CatalogTableArtifact` built at the caller, so the helper signature becomes
  `(CatalogTableArtifact artifact, CancellationToken cancellationToken)`. The
  artifact already exists, has the right shape, and is what the helper
  constructs internally today.
- [x] If any of the defaulted options does have a legitimate (current or near-
  term) caller, keep that option on the artifact only; do not retain it as a
  separate scalar parameter.
- [x] Keep complex-column code on its existing catalog-artifact path; it
  already constructs `CatalogTableArtifact` values directly.
- [x] Preserve validation and cache invalidation behavior exactly; this is a
  signature cleanup around an already-existing artifact model, not a catalog
  pipeline rewrite.
- [x] Run table-creation, complex-column template-table, and catalog round-trip
  tests.

Implementation notes (2026-06-01):

- Verified the sole call site of `CreateTableInternalAsync` was
  `AccessWriter.CreateTableAsync`, which passed only the four required
  arguments. The four defaulted scalars (`reservedTdefPageNumber`,
  `emitLvProp`, `markSystemTableTdef`, `emitAceRows`) had no callers.
- Replaced the 9-parameter signature with
  `CreateTableInternalAsync(CatalogTableArtifact tableArtifact, CancellationToken cancellationToken)`.
  The caller now constructs the artifact inline with the four required
  fields; the artifact's other options retain their existing defaults.
- All 3594 non-fuzz tests pass.

Payoff: `CreateTableInternalAsync` is a 9-parameter helper that immediately
constructs a `CatalogTableArtifact`, and most of those parameters appear to be
unused defaults. Removing dead defaults is the cleanest first step; the
artifact pass-through then leaves a tiny, readable signature without changing
public APIs.

Relevant code:

- [`AccessWriter.CreateTableInternalAsync`](../../JetDatabaseWriter/AccessWriter.cs)
- [`CatalogTableArtifact`](../../JetDatabaseWriter/Catalog/Models/CatalogTableArtifact.cs)
- [`ComplexColumnManager`](../../JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs)

#### 6. Hoist `RowDecodePlan` out of `MaterializeSeekRowAsync`

- [x] In the index seek path, build `RowDecodePlan.CreateTyped(td, wantedColumns,
  strictParsing)` once before the hit loop and pass the prebuilt plan into
  `MaterializeSeekRowAsync` instead of recreating it for every matched row.
- [x] Drop the redundant `CatalogEntry entry` parameter on
  `MaterializeSeekRowAsync`. The method only uses `entry.TDefPage`; pass
  `long expectedTDefPage` directly, which is more truthful and one parameter
  shorter.
- [x] Drop the `wantedColumns` parameter on `MaterializeSeekRowAsync`. With the
  decode plan hoisted to the caller, `wantedColumns` is already baked into
  `RowDecodePlan` and is not needed by the materializer.
- [x] Keep `td` on `MaterializeSeekRowAsync` because the complex-column and
  hyperlink passes still consume `td.Columns` and `td.ClrTypes`.
- [x] Do **not** introduce a `ResolvedTable` parameter here. `MaterializeSeekRowAsync`
  only needs the TDEF page number plus the table definition, and a
  `ResolvedTable` would carry more than the helper actually consumes.
- [x] Leave `EnumerateMappedRowsPooledAsync` as-is for this pass. `tableName`,
  `entry`, and `td` are all genuinely used (complex data lookup,
  owned-data-page lookup, and decoding respectively), so there is no honest
  no-new-type arity reduction available there.
- [x] Keep the index-hit `(long DataPage, int RowIndex)` tuple unchanged; there
  is no existing row-pointer type for an index hit that lacks `RowStart` /
  `RowSize`.
- [x] Run typed/untyped reader projection tests, index seek tests, hyperlink
  projection tests, and complex-column read tests.
- [x] Spot-check seek-path allocations on a multi-hit query (for example with
  `dotnet-counters` or a quick BenchmarkDotNet allocation sample) to confirm
  the per-hit `RowDecodePlan` allocation has disappeared.

Implementation notes (2026-06-01):

- Hoisted the seek-path typed `RowDecodePlan` construction in
  `EnumerateIndexRowsAsync` so it is built once after the projection is known
  and reused for every index hit.
- Replaced `MaterializeSeekRowAsync(CatalogEntry entry, ..., bool[]? wantedColumns, ...)`
  with `MaterializeSeekRowAsync(long expectedTDefPage, ..., RowDecodePlan decodePlan, ...)`.
  The helper still receives `td` because complex-column resolution and
  hyperlink wrapping consume `td.Columns` and `td.ClrTypes`.
- Left `EnumerateMappedRowsPooledAsync` and the index-hit tuple unchanged.
- Focused validation passed: 158 tests covering index seeks, typed/untyped
  projection, hyperlink projection, and complex-column reads.
- Temporary allocation probe on a 2,000-hit duplicate-key seek showed the
  old per-additional-hit allocation at about 208.67 bytes and the hoisted-plan
  path at about 160.49 bytes, a reduction of roughly 48 bytes per additional
  hit in that sample.

Payoff: the largest concrete reader win is per-hit allocation removal in the
seek path, not the parameter count itself. The signature shrink falls out of
removing the redundant `entry` and the now-superfluous `wantedColumns`.

Relevant code:

- [`AccessReader.MaterializeSeekRowAsync`](../../JetDatabaseWriter/AccessReader.cs)
- [`AccessReader.FindIndexRowLocationsAsync`](../../JetDatabaseWriter/AccessReader.cs)
- [`RowDecodePlan`](../../JetDatabaseWriter/ValueDecoding/RowDecodePlan.cs)
- [`ResolvedTable`](../../JetDatabaseWriter/Catalog/Models/ResolvedTable.cs)

### P1. Add internal domain types only where P0 leaves repeated bundles

These changes add new internal types only where the no-new-type cleanup still
leaves a concrete repeated concept crossing method boundaries. The only current
candidate that meets that bar is the encoded index-range shape used by the
reader, cursor, and page codec.

#### 7. Add an internal encoded index-range type

- [x] Introduce `EncodedIndexBound` as a `readonly record struct` with
  `byte[]? Key`, `bool Inclusive`, and `bool IsPrefix`. Expose a `None` static
  (default value) and an `IsUnbounded` helper so unbounded sides do not require
  `Nullable<EncodedIndexBound>`.
- [x] Introduce `EncodedIndexRange` as a `readonly record struct` holding the
  lower and upper `EncodedIndexBound` plus an optional `byte[]? RequiredPrefix`.
- [x] Do not copy `Key` or `RequiredPrefix` buffers in the constructor. The
  encoder already returns fresh single-use arrays; extra copies would regress
  the seek hot path.
- [x] Convert `AccessReader.FindIndexRowLocationsAsync` from the local
  `lowerKey` / `lowerInclusive` / `lowerIsPrefix` and upper-bound scalars into
  the encoded range type.
- [x] Update `IndexCursor.FindRowLocationsInRangeAsync` to accept the encoded
  range instead of seven separate range/prefix parameters. Pass by value (the
  signature is `async ValueTask`, which forbids `in`).
- [x] Update `IndexPageCodec.CollectRangeLeafEntries` and any remaining range
  matcher helpers to consume the encoded range. These are sync methods, so
  prefer `in EncodedIndexRange` where it avoids repeated struct copies.
- [x] Keep the public `IndexKeyBound` and `IAccessIndexQuery.WhereRange` API
  unchanged unless a separate public-API review chooses otherwise.
- [x] Add or refresh focused index range tests covering inclusive/exclusive
  lower bounds, inclusive/exclusive upper bounds, prefix bounds, and required
  prefix filtering.
- [x] Run a focused index-range/seek BenchmarkDotNet benchmark before and after
  and attach the deltas. If the benchmark suite still lacks one, add the
  benchmark or record an equivalent before/after allocation/perf probe. No
  statistically significant regression is acceptable for this refactor.

Implementation notes (2026-06-01):

- Added internal `EncodedIndexBound` and `EncodedIndexRange` structs under
  `JetDatabaseWriter.Indexes.Models`. Constructors retain the supplied encoded
  buffer references and do not copy `Key` or `RequiredPrefix`.
- `AccessReader.FindIndexRowLocationsAsync` now constructs encoded ranges after
  public `IndexKeyBound` values are encoded. The public `IndexKeyBound` and
  `IAccessIndexQuery.WhereRange` APIs are unchanged.
- `IndexCursor.FindRowLocationsInRangeAsync` now takes the encoded range by
  value, and `IndexPageCodec.CollectRangeLeafEntries` takes it by
  `in EncodedIndexRange` while continuing to compare against compressed page
  bytes directly.
- Added `IndexRangeSeekBenchmarks` because the suite did not have an index-range
  seek target. Short-job BenchmarkDotNet before/after deltas:
  `BoundedRange` 15.525 us / 8.20 KB -> 12.055 us / 8.20 KB;
  `RequiredPrefix` 4.915 us / 4.18 KB -> 5.624 us / 4.18 KB. The
  `RequiredPrefix` confidence intervals overlap, and allocations were unchanged,
  so this run did not show a statistically significant regression.
- Focused validation passed: 27 tests in `IndexCursorTests` and
  `AccessReaderIndexSeekTests`, plus all 455 tests in the
  `JetDatabaseWriter.Tests.Indexes` namespace.

Payoff: public query callers already use `IndexKeyBound`, but the reader
immediately explodes those bounds into repeated `byte[]?` plus boolean triples.
This is the right internal type once the remaining issue is the cross-layer
encoded-bound concept rather than a single oversized helper.

Relevant code:

- [`IndexKeyBound`](../../JetDatabaseWriter/Models/IndexKeyBound.cs)
- [`IndexQueryCriteria`](../../JetDatabaseWriter/Indexes/IndexQueryCriteria.cs)
- [`AccessReader.FindIndexRowLocationsAsync`](../../JetDatabaseWriter/AccessReader.cs)
- [`IndexCursor.FindRowLocationsInRangeAsync`](../../JetDatabaseWriter/Indexes/IndexCursor.cs)
- [`IndexPageCodec.CollectRangeLeafEntries`](../../JetDatabaseWriter/Indexes/IndexPageCodec.cs)

## Explicit Non-Goals

- Do not replace high-arity record constructors that already are the domain type,
  such as catalog rows, relationship snapshots, layout structs, or descriptors.
- Do not refactor row-decoding hot paths solely to reduce parameter count.
  Existing `page`, `rowStart`, and `rowSize` signatures are performance-sensitive;
  any consolidation there should be benchmark-driven.
- Do not change public APIs in a breaking way for this cleanup. Prefer additive
  overloads or internal-only refactors.
- Do not add public request or target types solely to make signatures shorter.
  Reopen those ideas only when concrete caller workflows show the current API is
  awkward enough to justify more public surface area.
- Do not create generic parameter-bag types. Each type should name a real domain
  concept and carry validation or meaning that scalar parameters cannot.

## Validation Notes

- Index range cleanup should run focused index-query tests first, then broader
  index maintenance tests if shared codec behavior changes, and finally a
  focused index-range/seek benchmark or equivalent allocation/perf probe for
  the no-regression guarantee.
- Relationship-side cleanup should run relationship creation/enforcement tests,
  including cascade update/delete coverage.
- Usage-map cleanup should run owned-page discovery tests and writer round-trip
  tests that depend on TDEF owned-page maps.
- Index page-builder sibling-pointer cleanup should run index B-tree build,
  split, merge, and maintenance tests.
- Catalog-table-artifact cleanup should run table creation, complex-column
  backing table, and catalog round-trip tests.
- Reader helper cleanup should run projection, index seek, hyperlink, and
  complex-column read tests, and confirm via an allocation sample that the
  hoisted `RowDecodePlan` removes the per-hit allocation on multi-hit seeks.
- Every refactor must keep a clean Release build under the repo's strict
  analyzer settings (StyleCop, Roslynator, banned APIs, warnings-as-errors).

## Risks and Rejected Alternatives

- **Class-based bundles.** Rejected. The hot paths (index seek, FK emit, usage
  map walk) run repeatedly per query / per relationship; a per-call heap
  allocation would show up in benchmarks and add GC pressure to the reader.
- **Nullable struct bounds (`EncodedIndexBound?`).** Rejected. A `None`
  sentinel plus `IsUnbounded` keeps the type small, avoids the `HasValue`
  branching pattern, and reads more clearly at call sites.
- **Merging `IndexKeyBound` and `EncodedIndexBound`.** Rejected. The public
  type is user-facing (object values, defensive copy, validation); the internal
  type is encoder output (byte buffers, no copy). Conflating them would either
  expose internals or add per-seek allocations.
- **A universal `ParameterBag` / `Context` type per subsystem.** Rejected.
  Generic bags hide intent and grow unbounded over time. Each new type must
  name a real domain concept and replace a recurring bundle, not collect
  unrelated parameters.
- **Unnamed value tuples in place of multi-scalar method parameters.**
  Rejected. A `(long Prev, long Next, long Tail)` tuple keeps element names at
  the declaration but drops them at the call site, replacing a named-argument
  guard with positional packing. That is the same transposition hazard this
  backlog is trying to remove. Either keep named scalar parameters (so callers
  can write `prevPage: a, nextPage: b, tailPage: c`) or introduce a named
  `readonly record struct` whose construction site forces the names. Do not
  use an anonymous tuple as a middle ground.
