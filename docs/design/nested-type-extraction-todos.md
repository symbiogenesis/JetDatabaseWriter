# Nested Type Extraction TODOs

**Status:** Proposed backlog, created 2026-06-01.

This document tracks nested types in `JetDatabaseWriter/` whose declaring class
is **not** the only consumer in production library code. Each candidate is a
good fit for extraction into its own file under the appropriate `Models/` (or
peer) folder per [library-structure.md](library-structure.md).

The scan excluded:

- `private` nested types (cannot be referenced externally).
- Types only referenced within their declaring file.
- Public DTOs already in `Models/`.

## Cross-Cutting Constraints

- One type per file. Filename matches type name.
- Preserve effective visibility exactly. Do not widen access; nested `public`
  types inside an `internal` declaring type should become `internal` top-level
  types.
- Preserve `readonly record struct` shape; do not convert to class
  (per `parameter-object-consolidation-todos.md` allocation guidance).
- Update the namespace to match the destination folder
  (e.g. `JetDatabaseWriter.Pages.Models`).
- Remove now-redundant `using TypeName = Outer.TypeName;` aliases at call
  sites (notably [UniqueIndexChecker.cs](../../JetDatabaseWriter/Indexes/UniqueIndexChecker.cs)
  and [IndexMaintainer.cs](../../JetDatabaseWriter/Indexes/IndexMaintainer.cs)).
- Keep XML doc comments with the moved type.
- Run the full test suite after each cluster; do not batch unrelated moves.

## Priority Strategy

- **P0: High cross-file usage clusters.** Most leverage; the `IndexLayout`
  family and the `AccessBase` row primitives. These already show the smell —
  call sites use long `Outer.Nested` paths or `using` aliases.
- **P1: Two-to-three-callsite extractions.** Mechanical moves with low risk.
- **P2: Single external caller.** Optional; nice for consistency but not
  urgent.

## P0. High-leverage cluster moves

### 1. Extract the `IndexLayout` nested cluster

[IndexLayout.cs](../../JetDatabaseWriter/Indexes/IndexLayout.cs) previously contained seven
nested data shapes consumed across `AccessReader`, `AccessWriter`,
`UniqueIndexChecker`, `TDefPageBuilder`, and `IndexCatalogReader`. Their
declarations were `public`, but `IndexLayout` itself is `internal`; they were
extracted as `internal` top-level types to avoid expanding the public API.
[UniqueIndexChecker.cs](../../JetDatabaseWriter/Indexes/UniqueIndexChecker.cs)
previously aliased `IndexLayout.UniqueIndexDescriptor`, which was the canonical
"should-be-its-own-file" signal.

- [x] Move `IndexLayout.KeyColumn` to `Indexes/Models/KeyColumn.cs`.
- [x] Move `IndexLayout.IndexSectionAnchors` to
  `Indexes/Models/IndexSectionAnchors.cs`.
- [x] Move `IndexLayout.RealIdxSlot` to `Indexes/Models/RealIdxSlot.cs`.
- [x] Move `IndexLayout.LogicalIdxEntry` to `Indexes/Models/LogicalIdxEntry.cs`.
- [x] Move `IndexLayout.RealIdxEntry` to `Indexes/Models/RealIdxEntry.cs`.
- [x] Move `IndexLayout.KeyColumnInfo` to `Indexes/Models/KeyColumnInfo.cs`.
- [x] Move `IndexLayout.UniqueIndexDescriptor` to
  `Indexes/Models/UniqueIndexDescriptor.cs`.
- [x] Delete the `using UniqueIndexDescriptor = IndexLayout.UniqueIndexDescriptor;`
  alias in `UniqueIndexChecker.cs`.
- [x] Delete the `using KeyColumnInfo = IndexLayout.KeyColumnInfo;` and
  `using RealIdxEntry = IndexLayout.RealIdxEntry;` aliases in
  `IndexMaintainer.cs`.
- [x] Update all `IndexLayout.X` references to bare `X`.
- [x] Keep the instance methods (`GetIndexSection`, `TryReadLogicalEntry`,
  `TryReadRealIdxSlot`, etc.) on `IndexLayout`; only the data shapes move.
- [x] Run index, schema, catalog, and round-trip tests.

### 2. Extract the `AccessBase` row primitives

[AccessBase.cs](../../JetDatabaseWriter/AccessBase.cs) is over 1800 lines and
its nested row types are referenced from `Pages/`, `LongValues/`,
`ValueDecoding/`, `Indexes/`, and `Relationships/`.

- [x] Move `AccessBase.RowBound` to `Pages/Models/RowBound.cs`
  (siblings: `RowLocation.cs`, `PageInsertTarget.cs`; even though its current
  external consumer is `LongValueStore`, it describes data-page row bounds).
- [x] Move `AccessBase.RowLayout` to `Pages/Models/RowLayout.cs`.
- [x] Move `AccessBase.ColumnSlice` to `ValueDecoding/Models/ColumnSlice.cs`
  (its external consumers are `RowDecodePlan` and `DirectRowDecoderBuilder`).
- [x] Move `AccessBase.ColumnSliceKind` to
  `ValueDecoding/Models/ColumnSliceKind.cs`.
- [x] Leave `AccessBase.TableRow` nested — only used inside `AccessBase.cs`.
- [x] Leave `AccessBase.TableRowVisitor` nested — only used inside
  `AccessBase.cs` and tied to the nested `TableRow` shape.
- [x] Leave `AccessBase.ParsedColumnDescriptor` nested — `private`.
- [x] Update all `AccessBase.X` references at call sites in
  [RowDecodePlan.cs](../../JetDatabaseWriter/ValueDecoding/RowDecodePlan.cs),
  [DirectRowDecoderBuilder.cs](../../JetDatabaseWriter/ValueDecoding/DirectRowDecoderBuilder.cs),
  [UsageMap.cs](../../JetDatabaseWriter/Pages/UsageMap.cs),
  [LongValueDecoder.cs](../../JetDatabaseWriter/ValueDecoding/LongValueDecoder.cs),
  [LongValueStore.cs](../../JetDatabaseWriter/LongValues/LongValueStore.cs),
  [IndexMaintainer.cs](../../JetDatabaseWriter/Indexes/IndexMaintainer.cs),
  [RelationshipChildRowLocator.cs](../../JetDatabaseWriter/Relationships/RelationshipChildRowLocator.cs).
- [x] Run reader, writer, long-value, and index-maintenance tests.

## P1. Mechanical single-cluster moves

### 3. Extract `CalculatedExpressionEvaluator` nested types

Every `CalculatedExpression*Node.cs` file in
`Schema/Expressions/` consumes `CalculatedExpressionEvaluator.Plan` and
`EvaluationContext`. They are also referenced from
[CalculatedFunctionInvocation.cs](../../JetDatabaseWriter/Schema/Expressions/CalculatedFunctionInvocation.cs)
and [ColumnConstraint.cs](../../JetDatabaseWriter/Schema/Models/ColumnConstraint.cs).

- [x] Move `CalculatedExpressionEvaluator.Plan` to
  `Schema/Expressions/CalculatedExpressionPlan.cs`.
- [x] Move `CalculatedExpressionEvaluator.EvaluationContext` to
  `Schema/Expressions/CalculatedExpressionEvaluationContext.cs`.
- [x] Update `ColumnConstraint.CalculatedExpressionPlan` property type.
- [x] Run calculated-column expression tests.

### 4. Extract `ColumnPropertyBlockBuilder` builder types

[LinkedOdbcLvPropBuilder.cs](../../JetDatabaseWriter/Schema/LinkedOdbcLvPropBuilder.cs)
has 10+ references to `ColumnPropertyBlockBuilder.TargetBuilder`; also used by
[JetExpressionConverter.cs](../../JetDatabaseWriter/Schema/JetExpressionConverter.cs).

- [x] Move `ColumnPropertyBlockBuilder.TargetBuilder` to
  `Schema/Models/ColumnPropertyTargetBuilder.cs`.
- [x] Move `ColumnPropertyBlockBuilder.EntryBuilder` to
  `Schema/Models/ColumnPropertyEntryBuilder.cs`.
- [x] Run schema/LvProp tests.

### 5. Extract `IndexBTreeBuilder.BuildResult`

Used from [IndexMaintainer.cs](../../JetDatabaseWriter/Indexes/IndexMaintainer.cs)
and [IndexBTreeEditor.cs](../../JetDatabaseWriter/Indexes/IndexBTreeEditor.cs).

- [x] Move to `Indexes/Models/IndexBTreeBuildResult.cs`
  (rename to `IndexBTreeBuildResult` to prevent any future ambiguity; there is
  no current `BuildResult` name collision).
- [x] Run index build/edit tests.

### 6. Extract `NumericEncoder.FixedPointPayload`

Used from [RowEncoder.cs](../../JetDatabaseWriter/ValueEncoding/RowEncoder.cs)
and [IndexKeyEncoder.cs](../../JetDatabaseWriter/Indexes/IndexKeyEncoder.cs).

- [ ] Move to `ValueEncoding/Models/FixedPointPayload.cs`.
- [ ] Run numeric/decimal encoding tests.

### 7. Extract `LongValueStore.LvalRowLocation`

Used from [LongValueDecoder.cs](../../JetDatabaseWriter/ValueDecoding/LongValueDecoder.cs)
(8 sites).

- [ ] Move to `LongValues/Models/LvalRowLocation.cs`
  (sibling of `LongValueDescriptor.cs`).
- [ ] Run long-value decode tests.

### 8. Extract `RowDecodePlan.LongValueRef` and `CalculatedLongValueRef`

Used from [AccessReader.cs](../../JetDatabaseWriter/AccessReader.cs)
sentinel-resolution code.

- [ ] Move `LongValueRef` to `ValueDecoding/Models/LongValueRef.cs`.
- [ ] Move `CalculatedLongValueRef` to
  `ValueDecoding/Models/CalculatedLongValueRef.cs`.
- [ ] Run reader projection tests including MEMO/OLE and calculated-column
  columns.

## P2. Optional single-consumer moves

These each have one external caller. Extracting them improves consistency but
adds no real discoverability win.

### 9. `UsageMap.Pointer`

- [x] Move `UsageMap.Pointer` to `Pages/Models/UsageMapPointer.cs`.

### 10. `ComplexColumnManager.ComplexColumnAllocation`

- [x] Used from [AccessWriter.cs](../../JetDatabaseWriter/AccessWriter.cs#L433).
  Optional move to `ComplexColumns/Models/ComplexColumnAllocation.cs`.

### 11. `GeneralLegacyTextIndexEncoder.CharHandlerType`

- [x] Used from [General97TextIndexEncoder.cs](../../JetDatabaseWriter/Indexes/Collation/General97TextIndexEncoder.cs).
  Optional move to `Indexes/Collation/CharHandlerType.cs`.

## Explicit Non-Moves

These nested types are referenced cross-file but should **stay nested** because
their lifetime, disposal, or construction contract is fully owned by the
declaring class:

- `AsyncReentrantOperationGate.Lease` — disposal token tightly coupled to gate.
- `AccessBase.TableRow` — only used inside `AccessBase.cs` (despite being
  `internal`).
- `AccessBase.TableRowVisitor` — only used inside `AccessBase.cs` and coupled
  to `AccessBase.TableRow`.
- `RowMapper.Accessor` — only used inside `RowMapper.cs`.
- Any `private` nested type (e.g. `AccessReader.TableScanPage`,
  `IndexBTreeEditor.LeafGroup`, encoder handler hierarchies).

## Validation Notes

- `IndexLayout` cluster: run the full `Indexes/` test suite plus catalog and
  schema round-trip tests; this touches the most files.
- `AccessBase` row primitives: run reader, writer, long-value, index, and
  relationship tests.
- All other moves: run the test class(es) most directly exercising the
  affected subsystem before running the full suite.
- Every move must keep a clean Release build under the repo's strict analyzer
  settings (StyleCop, Roslynator, banned APIs, warnings-as-errors).

## Risks and Rejected Alternatives

- **Partial-class splits instead of separate types.** Rejected. The goal is
  one type per file under `Models/`, not splitting the declaring class.
- **Promoting effectively internal types to `public`.** Rejected. A `public`
  nested type inside an `internal` container is not public API; extract it as
  `internal`.
- **Moving types that are only referenced inside their declaring file.**
  Rejected. Nested types with no external consumer are correctly nested.
