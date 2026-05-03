# `AccessWriter.cs` extraction plan

`JetDatabaseWriter/Core/AccessWriter.cs` is currently **12,537 lines** with 15 nested types and no `#region` markers. Several large, cohesive subsystems clearly belong in their own files — most have a natural home alongside existing helpers in `Internal/` (`IndexBTreeBuilder`, `IndexLeafIncremental`, `IndexCatalogReader`, etc.).

Phases below are ordered by **biggest line reduction first**. Line numbers reference the current `AccessWriter.cs`.

## Summary of impact

| Phase | Subsystem | ~Lines moved | Cumulative |
|------:|-----------|-------------:|-----------:|
| 1  | Relationships / FK enforcement | 2,870 | 2,870 |
| 2  | Index maintenance               | 2,820 | 5,690 |
| 3  | Complex columns                 | 1,590 | 7,280 |
| 4  | Row encoder + page insert       |   750 | 8,030 |
| 5  | TDEF / empty-DB builders        |   510 | 8,540 |
| 6  | Encryption ops                  |   400 | 8,940 |
| 7  | Transactions                    |   300 | 9,240 |
| 8  | Unique-index checks             |   290 | 9,530 |
| 9  | Catalog access helpers          |   270 | 9,800 |
| 10 | Constraint registry             |   245 | 10,045 |
| 11 | LVAL encoding                   |   200 | 10,245 |
| 12 | Stray nested utilities          |   150 | 10,395 |

After all phases, `AccessWriter.cs` would shrink from ~12,500 lines to roughly **~2,100 lines** of pure orchestration: the constructor, public `OpenAsync`/`CreateDatabaseAsync` factories, the public CRUD/schema entry points, and `DisposeAsync`.

> **No partial classes.** The maintainer dislikes splitting `AccessWriter` across multiple `partial` files — it hides the true size and complexity of the type and makes navigation worse. Every extraction below must land in a **properly-named type** under `Internal/`, with `AccessWriter` holding a private field of that type and forwarding through thin instance methods. Anything that genuinely cannot be lifted off `AccessWriter` (because it touches too much private state) must stay in `AccessWriter.cs` rather than be moved into a partial.
>
> **Prefer existing types over new ones.** Before introducing a new manager / helper / builder, check `Internal/` (and adjacent folders) for a type that is already a natural home for the methods being moved — e.g. extend `IndexBTreeBuilder` / `IndexLeafIncremental` / `IndexCatalogReader` rather than spinning up a parallel `IndexMaintainer` if the existing type fits. Only create a new type when no existing one is a clean fit; the "Suggested home" entries below are starting points, not mandates, and should be replaced with an existing type whenever one applies.

### Suggested execution order rationale
- Phases 1–3 target the three largest cohesive subsystems and yield ~58 % reduction on their own.
- Phases 4–6 are standalone, low-risk extractions (mostly static-feeling code) that don't depend on Phases 1–3.
- Phases 7–12 are smaller, one-type-per-file refactors that can be done piecemeal at any time.
- Recommended warm-up order: **Phase 12 → Phase 6 → Phase 1**, then proceed top-down.

---

## Phase 1 — Foreign-key / Relationship subsystem (~2,870 lines) ✅ DONE

Extracted to [JetDatabaseWriter/Internal/Relationships/RelationshipManager.cs](../../JetDatabaseWriter/Internal/Relationships/RelationshipManager.cs). `AccessWriter` holds a private `_relationships` field and exposes thin public forwarders for `CreateRelationshipAsync` / `DropRelationshipAsync` / `RenameRelationshipAsync`; FK enforcement entry points (`EnforceFkOnInsertAsync`, `EnforceFkOnPrimaryDeleteAsync`, `EnforceFkOnPrimaryUpdateAsync`, `GetEnforcedRelationshipsAsync`, `AugmentParentSetsAfterInsert`) are called directly through `_relationships`.

`AccessWriter.cs` shrank from **12,537 → 9,693 lines** (–2,844). `CatalogEntry`, `RowLocation`, `RowBound`, `CatalogRow` were promoted out of `AccessBase`/`AccessWriter` to top-level `internal` records under `Internal/Models/`. ~22 `private protected` members of `AccessBase` and ~15 `private` members of `AccessWriter` were promoted to `internal` to allow `RelationshipManager` to call them via its `AccessWriter _writer` reference.

- [x] Extract methods at lines **1304–4177** (FK CRUD, FK logical-idx maintenance, runtime enforcement, cascade processing).
- [x] Move nested types: `FkSidePlan`, `RelationshipRowSnapshot`, `FkTDefLayout`, `FkPairContext`, `TablePairComparer`, `FkRelationship`, `FkContext` (now nested in `RelationshipManager`; `FkContext` and `FkRelationship` are `internal` so AccessWriter can reference them as `RelationshipManager.FkContext` / `RelationshipManager.FkRelationship`).
- [x] **Suggested home:** `Internal/Relationships/RelationshipManager.cs` ✓.

## Phase 2 — Index maintenance (~2,820 lines)

- [ ] Extract methods at lines **9380–12200**:
  - `MaintainIndexesAsync`, `TryMaintainIndexesIncrementalAsync`
  - `DescendToLeftmostLeafAsync`, `TryAppendToTailLeafAsync`
  - `TrySurgicalMultiLevelMaintainAsync`, `DescendCapturingAsync`
  - `TrySurgicalCrossLeafMaintainAsync`
  - `GroupChangesByTargetLeafAsync`, `DescendOrLookupGroupAsync`, `GetEffectiveTailPageAsync`
  - `TryStageIntermediateRewritesAsync`, `EncodeHintEntries`
  - `ReadLastChildPointer`, `SelectChildForKey`
  - `TrySpliceCatalogIndexEntryAsync`
- [ ] Move nested types: `LeafGroup` (L10602), `IntermediateStagingState` (L11250).
- [ ] Move diagnostic field `_lastIncrementalBail`.
- [ ] **Suggested home:** `Internal/IndexMaintainer.cs`, alongside existing `IndexLeafIncremental` / `IndexBTreeBuilder`.

## Phase 3 — Complex-column (Attachment / MultiValue) subsystem (~1,590 lines)

- [ ] Extract methods at lines **6626–8213**:
  - `ScaffoldSystemTablesAsync`
  - `CreateMSysComplexTypeTemplatesAsync`, `ResolveComplexTypeTemplateName`
  - `CreateMSysComplexColumnsAsync`
  - `PrepareComplexColumnAllocationsAsync`, `GetNextComplexIdAsync`
  - `EmitComplexColumnArtifactsAsync`, `BuildFlatTableName`
  - `InsertMSysComplexColumnsRowAsync`
  - `AddAttachmentAsync`, `AddMultiValueItemAsync`, `AddComplexItemCoreAsync`
  - `ResolveFlatTableNameAsync`, `ResolveFlatTableTdefPageAsync`
  - `ReadOrAllocateConceptualTableIdAsync`, `PatchParentComplexSlotAsync`, `GetNextConceptualTableIdForFlatAsync`
  - `BuildAttachmentFlatRow`, `BuildMultiValueFlatRow`, `DeriveExtension`
  - `CascadeDeleteComplexChildrenAsync`, `DropSingleComplexChildAsync`
  - `RenameComplexColumnArtifactsAsync`, `DropComplexChildrenForTableAsync`
- [ ] Move nested type: `ComplexColumnAllocation` (L6803).
- [ ] **Suggested home:** `Internal/ComplexColumnManager.cs`.

## Phase 4 — Row encoding / page-level row insertion (~750 lines)

- [ ] Extract methods at lines **8400–9075**:
  - `InsertRowDataAsync`, `InsertSystemRowAndMaintainAsync`, `SystemTableHasMaintainableIndexesAsync`
  - `InsertRowDataLocAsync`, `AdjustTDefRowCountAsync`
  - `FindInsertTargetAsync`, `CanInsertRow`, `GetFirstRowStart`
  - `CreateEmptyDataPage`, `WriteRowToPage`, `WriteRowToPageAsync`
  - `SerializeRow`
  - `CanStoreFixedColumn`, `TryEncodeFixedValue`
  - `EncodeVariableValue`, `EncodeTextValue`, `EncodeBinaryValue`, `EncodeMemoValue`, `EncodeNumericValue`
  - `TryGetCachedInsertPageNumber`, `SetCachedInsertPageNumber`
- [ ] **Suggested split:** `Internal/RowEncoder.cs` (Serialize/Encode*) + `Internal/DataPageInserter.cs` (FindInsertTargetAsync, CanInsertRow, etc.).

## Phase 5 — TDEF / empty-database builders (~510 lines)

- [ ] Extract methods at lines **5527–5666** and **6402–6625**:
  - `BuildTableDefinition`, `GetDeclaredSize`, `BuildTDefPage` (×2)
- [ ] Extract methods at lines **8215–8398**:
  - `BuildEmptyDatabase`, `BuildMSysObjectsTDef`
- [ ] **Suggested home:** `Internal/Builders/TDefPageBuilder.cs` (alongside `IndexBTreeBuilder`, `IndexLeafPageBuilder`, `ColumnPropertyBlockBuilder`).

## Phase 6 — Encryption / re-encryption operations (~400 lines)

- [ ] Extract methods at lines **4178–4580**:
  - `DetectEncryptionFormatAsync` (×2), `ChangePasswordAsync` (×2)
  - `EncryptAsync` (×2), `DecryptAsync` (×2)
  - `ReencryptFileAsync`, `ReplaceFileAtomicAsync`, `ReencryptStreamAsync`, `ReencryptCoreAsync`
- [ ] **Suggested home:** `Internal/AccessFileEncryption.cs`, with thin static wrappers remaining on `AccessWriter`.

## Phase 7 — Transaction lifecycle (~300 lines)

- [ ] Extract methods at lines **4581–4979**:
  - `BeginTransactionAsync`, `RunAutoCommitAsync` (×2)
  - `CommitTransactionAsync`, `BumpCommitLockByteAsync`, `FlushDurableAsync`
  - `RollbackTransactionAsync`
  - `DisposeActiveTransactionAsync`, `RewrapAndCloseOuterEncryptedStreamAsync`, `RewrapAgileOnDisposeAsync`, `DisposeStateLockAsync`
- [ ] **Suggested home:** `Internal/Transactions/TransactionLifecycle.cs` (or similar) — but because these methods need access to `_activeTransaction`, `_outerEncryptedStream`, `_stateLock`, etc., much of this may have to remain in `AccessWriter.cs`. Do **not** use a partial-class file.

## Phase 8 — Unique-index pre-insert/update checks (~290 lines)

- [ ] Extract methods at lines **12204–12490**:
  - `LoadUniqueIndexDescriptorsAsync`, `EncodeCompositeKeyForUniqueCheck`
  - `CheckUniqueIndexesPreInsertAsync`, `CheckUniqueIndexesPreUpdateAsync`, `CheckUniqueIndexesCore`
  - `MarkRowDeletedAsync`
- [ ] **Suggested home:** `Internal/UniqueIndexChecker.cs` (mirror of read-side `IndexCatalogReader`).

## Phase 9 — Catalog/system-row reading helpers (~270 lines)

- [ ] Extract methods at lines **9111–9379**:
  - `GetCatalogRowsAsync`, `ParseInt32`, `ParseInt64`
  - `GetLiveRowLocationsAsync`, `TryReadColumnValuesTypedAsync`, `TryDecodeColumnSlice`
- [ ] Extract upstream snapshot helpers at lines **5947–6105**:
  - `ReadTableSnapshotAsync`, `ReadIndexMetadataSnapshotAsync`, `ReadLvPropBlockAsync`
  - `GetUserTablesAsync`, `GetRequiredCatalogEntryAsync`, `ReadRequiredTableDefAsync`
  - `InsertCatalogEntryAsync`, `RewriteTableAsync`, `RenameTableInCatalogAsync`
- [ ] Move `CatalogRow` (L12508) into a model file.
- [ ] **Suggested home:** `Internal/CatalogAccess.cs` (writer-side counterpart to `IndexCatalogReader`).

## Phase 10 — Constraint registry (~245 lines)

- [ ] Extract methods at lines **4981–5226**:
  - `ToConstraint`, `IsIntegralType`, `ConvertIntegral`
  - `RegisterConstraints`, `UnregisterConstraints`, `RenameConstraintsTable`
  - `HydrateConstraintsFromTableDef`, `TdefTypeToClrType`, `RestoreAutoCounters`
- [ ] Promote nested `ColumnConstraint` (L12517) to a top-level internal type.
- [ ] **Suggested home:** `Internal/ColumnConstraint.cs` + a `ConstraintRegistry` static helper.

## Phase 11 — Long-value (LVAL) encoding (~200 lines)

- [ ] Extract methods at lines **5687–5881**:
  - `WrapInlineLongValue`
  - `PreEncodeLongValuesAsync`, `EncodeAsLvalChainAsync`
  - `BuildSingleLvalPageBuffer`, `BuildChainLvalPageBuffer`
- [ ] Extract `EncodeOleValue` (L5391).
- [ ] Move nested `PreEncodedLongValue` (L5717).
- [ ] **Suggested home:** `Internal/LongValueEncoder.cs`.

## Phase 12 — Trivially-misplaced nested utilities (~150 lines)

- [ ] `ByteArrayEqualityComparer` (L12448) → `Internal/ByteArrayEqualityComparer.cs`.
- [ ] `PageInsertTarget` (L12510) → `Internal/Models/PageInsertTarget.cs`.
- [ ] `CatalogRow` (L12508) → `Internal/Models/CatalogRow.cs` (also referenced by Phase 9).
