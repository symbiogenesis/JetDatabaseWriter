# `AccessWriter.cs` extraction plan

`JetDatabaseWriter/Core/AccessWriter.cs` is currently **~4,923 lines** with no `#region` markers. Several large, cohesive subsystems still belong in their own files — most have a natural home alongside existing helpers in `Internal/` (`IndexBTreeBuilder`, `IndexLeafIncremental`, `IndexCatalogReader`, etc.).

Phases below are ordered by **biggest line reduction first**. Line numbers reference the current `AccessWriter.cs`.

## Summary of impact

| Phase | Subsystem | ~Lines |
|------:|-----------|-------:|
| 1 | Row encoder + page insert | 750 |
| 2 | Transactions | 300 |
| 3 | Unique-index checks | 290 |
| 4 | Catalog access helpers | 270 |
| 5 | Constraint registry | 245 |
| 6 | LVAL encoding | 200 |

After the remaining phases, `AccessWriter.cs` should shrink from ~4,923 lines to roughly **~2,900 lines** of pure orchestration: the constructor, public `OpenAsync`/`CreateDatabaseAsync` factories, the public CRUD/schema entry points, and `DisposeAsync`.

> **No partial classes.** The maintainer dislikes splitting `AccessWriter` across multiple `partial` files — it hides the true size and complexity of the type and makes navigation worse. Every extraction below must land in a **properly-named type** under `Internal/`, with `AccessWriter` holding a private field of that type and forwarding through thin instance methods. Anything that genuinely cannot be lifted off `AccessWriter` (because it touches too much private state) must stay in `AccessWriter.cs` rather than be moved into a partial.
>
> **Prefer existing types over new ones.** Before introducing a new manager / helper / builder, check `Internal/` (and adjacent folders) for a type that is already a natural home for the methods being moved — e.g. extend `IndexBTreeBuilder` / `IndexLeafIncremental` / `IndexCatalogReader` rather than spinning up a parallel `IndexMaintainer` if the existing type fits. Only create a new type when no existing one is a clean fit; the "Suggested home" entries below are starting points, not mandates, and should be replaced with an existing type whenever one applies.

### Completed pre-phase extractions

These cohesive subsystems were extracted before the numbered phases began:

| Extraction | New file | ~Lines removed |
|-----------|----------|---------------:|
| TDef page building | `Internal/Builders/TDefPageBuilder.cs` | 707 |
| Encryption logic | `Internal/EncryptionManager.cs` | 273 |
| ByteArrayEqualityComparer | `Internal/Collections/ByteArrayEqualityComparer.cs` | 46 |
| PageInsertTarget model | `Internal/Models/PageInsertTarget.cs` | 8 |

### Suggested execution order rationale
- Phase 1 is still the biggest remaining cohesive block.
- Phases 2–4 are moderate, self-contained extractions that can proceed independently.
- Phases 5–6 are smaller refactors that can be done piecemeal at any time.
- Recommended warm-up order: **Phase 5 → Phase 2 → Phase 1**, then proceed top-down.

---

## Phase 1 — Row encoding / page-level row insertion (~750 lines)

- [ ] Extract methods at lines **3528–3609**, **3661–3853**, and **3948–4339**:
  - `InsertRowDataAsync`, `InsertSystemRowAndMaintainAsync`, `SystemTableHasMaintainableIndexesAsync`
  - `InsertRowDataLocAsync`, `AdjustTDefRowCountAsync`
  - `FindInsertTargetAsync`, `CanInsertRow`, `GetFirstRowStart`
  - `CreateEmptyDataPage`, `WriteRowToPage`, `WriteRowToPageAsync`
  - `SerializeRow`
  - `CanStoreFixedColumn`, `TryEncodeFixedValue`
  - `EncodeVariableValue`, `EncodeTextValue`, `EncodeBinaryValue`, `EncodeMemoValue`, `EncodeNumericValue`
  - `TryGetCachedInsertPageNumber`, `SetCachedInsertPageNumber`
- [ ] **Suggested split:** `Internal/RowEncoder.cs` (Serialize/Encode*) + `Internal/DataPageInserter.cs` (FindInsertTargetAsync, CanInsertRow, etc.).

## Phase 2 — Transaction lifecycle (~300 lines)

- [ ] Extract methods at lines **1562–1949**:
  - `BeginTransactionAsync`, `RunAutoCommitAsync` (×2)
  - `CommitTransactionAsync`, `BumpCommitLockByteAsync`, `FlushDurableAsync`
  - `RollbackTransactionAsync`
  - `DisposeActiveTransactionAsync`, `RewrapAndCloseOuterEncryptedStreamAsync`, `RewrapAgileOnDisposeAsync`, `DisposeStateLockAsync`
- [ ] **Suggested home:** `Internal/Transactions/TransactionLifecycle.cs` (new file) — but because these methods need access to `_activeTransaction`, `_outerEncryptedStream`, `_stateLock`, etc., much of this may have to remain in `AccessWriter.cs`. Do **not** use a partial-class file. No existing file is a clean fit (`PageJournal` is a buffer data-structure, not a lifecycle manager).

## Phase 3 — Unique-index pre-insert/update checks (~290 lines)

- [ ] Extract methods at lines **4643–4900**:
  - `LoadUniqueIndexDescriptorsAsync`, `EncodeCompositeKeyForUniqueCheck`
  - `CheckUniqueIndexesPreInsertAsync`, `CheckUniqueIndexesPreUpdateAsync`, `CheckUniqueIndexesCore`
  - `MarkRowDeletedAsync`
- [ ] **Suggested home:** `Internal/UniqueIndexChecker.cs` (new file). The state-free `EncodeCompositeKeyForUniqueCheck` could alternatively land in `Internal/Helpers/IndexHelpers.cs` (861 lines, already holds FK composite-key encoding), but the async check methods need I/O through the writer so they require their own type.

## Phase 4 — Catalog/system-row reading helpers (~270 lines)

- [ ] Extract methods at lines **4347–4568**:
  - `GetCatalogRowsAsync`, `ParseInt32`, `ParseInt64`
  - `GetLiveRowLocationsAsync`, `TryReadColumnValuesTypedAsync`, `TryDecodeColumnSlice`
- [ ] Extract upstream snapshot helpers at lines **2754–3233**:
  - `ReadTableSnapshotAsync`, `ReadIndexMetadataSnapshotAsync`, `ReadLvPropBlockAsync`
  - `GetUserTablesAsync`, `GetRequiredCatalogEntryAsync`, `ReadRequiredTableDefAsync`
  - `InsertCatalogEntryAsync`, `RewriteTableAsync`, `RenameTableInCatalogAsync`
- [ ] **Suggested home:** `Internal/CatalogAccess.cs` (new file). `IndexCatalogReader` is specifically index-section decoding, not general system-row reading, so no existing file is a clean fit.

## Phase 5 — Constraint registry (~245 lines)

- [ ] Extract methods at lines **1962–2211**:
  - `ToConstraint`, `IsIntegralType`, `ConvertIntegral`
  - `RegisterConstraints`, `UnregisterConstraints`, `RenameConstraintsTable`
  - `HydrateConstraintsFromTableDef`, `TdefTypeToClrType`, `RestoreAutoCounters`
- [ ] Promote nested `ColumnConstraint` (L12517) to a top-level internal type.
- [ ] **Suggested home:** `Internal/ColumnConstraint.cs` + a `ConstraintRegistry` static helper (new files). No existing file is a clean fit.

## Phase 6 — Long-value (LVAL) encoding (~200 lines)

- [ ] Extract methods at lines **2375–2724**:
  - `EncodeOleValue`
  - `WrapInlineLongValue`
  - `PreEncodeLongValuesAsync`, `EncodeAsLvalChainAsync`
  - `BuildSingleLvalPageBuffer`, `BuildChainLvalPageBuffer`
- [ ] Move nested `PreEncodedLongValue` (around **2564**).
- [ ] **Suggested home:** `Internal/LongValueEncoder.cs` (new file). `LvalChainResult` in `Internal/Models/` is a tiny result record, not a behavioral class — not a suitable home.
