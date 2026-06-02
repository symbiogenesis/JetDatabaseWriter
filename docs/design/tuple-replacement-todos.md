# Tuple Replacement TODOs

Audit date: 2026-06-01

Scope: C# source tuples found in the library, tests, benchmarks, and FormatProbe, excluding `bin` and `obj`. There are no explicit `System.Tuple<>` or `ValueTuple<>` generic uses; the findings are C# tuple syntax, tuple deconstruction syntax, and tuple-shaped local data.

Use this as a prioritized cleanup list, not as a mandate to replace every tuple. Clear wins remove repeated contracts or reuse domain types that already exist. Opportunistic items should wait until the owning module is already being touched.

## Existing Type Already Fits

- [x] Use `ColumnInfo` directly for projected typed reads.
  - Before: `List<(string Name, ColumnInfo Column)> projectedColumns` in `AccessReader.ReadTableAsync<T>` and `ReadProjectedTableAsync<T>`.
  - After: `List<ColumnInfo> projectedColumns`; read `column.Name` and `ResolveClrType(column)` directly.
  - Files: `JetDatabaseWriter/AccessReader.cs` around `ReadTableAsync<T>` and `ReadProjectedTableAsync<T>`.

- [x] Return `RowLocation` from the unique-parent-row lookup.
  - Before: `ValueTask<(long PageNumber, int RowIndex, int RowStart, int RowSize)> FindUniqueParentRowAsync(...)` plus a local `(long, int, int, int) match`.
  - After: `ValueTask<RowLocation> FindUniqueParentRowAsync(...)` and assign `match = row.Location`.
  - File: `JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs`.

- [x] Store `RowLocation` in complex-column catalog rewrite match lists.
  - Before: `List<(long PageNumber, int RowIndex, object[] Values)>` and tuple adds from `row.Location.PageNumber` / `row.Location.RowIndex`.
  - After: `List<(RowLocation Loc, object[] Values)>` as an incremental cleanup, or the `RowMutationHint` / `LocatedRowValues` wrapper below as the final shape.
  - Note: this overlaps with the row-values wrapper task. If that wrapper is implemented first, skip this intermediate tuple shape.
  - Files: `JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs` methods that rename complex-column artifacts and update parent table ids.

- [x] Use `IndexEntry` directly while rebuilding index entries.
  - Before: `List<(byte[] Key, long Page, byte Row)> entries`, then a second pass converting each tuple into `new IndexEntry(key, page, row)`.
  - After: `List<IndexEntry> entries`; sort by `entry.Key` and pass the list onward without conversion.
  - File: `JetDatabaseWriter/Indexes/IndexMaintainer.cs` bulk index rebuild path.

- [x] Keep `IndexLayout.IndexSectionAnchors` as a named value instead of deconstructing it where both retained fields are meaningful.
  - Before: `(int _, int logicalIdxStart, int logicalIdxNamesStart, int _, int _) = this.IndexLayoutInfo.GetIndexSection(...)`.
  - After: `IndexLayout.IndexSectionAnchors anchors = this.IndexLayoutInfo.GetIndexSection(...)`, then use `anchors.LogIdxStart` and `anchors.LogIdxNamesStart`.
  - Files: `JetDatabaseWriter/AccessReader.cs` and `JetDatabaseWriter/Schema/TDefPageBuilder.cs`.

## New Types: Clear Wins

- [x] Add an internal `ResolvedTable` or `TableResolution` type.
  - Before: `(CatalogEntry Entry, TableDef Td)?` returned by `ResolveTableAsync` and repeated at many call sites.
  - After: `ResolvedTable?` with `CatalogEntry Entry` and `TableDef Definition` (or `TableDef Td`, if preserving local naming matters more).
  - Files: `JetDatabaseWriter/AccessReader.cs`, `JetDatabaseWriter/ComplexColumns/ComplexColumnReader.cs`, and related call sites.

- [ ] Add an internal row-values wrapper for row location plus row values.
  - Before: `List<(RowLocation Loc, object[] Row)>` in public mutation, system-table maintenance, index maintenance, and relationship cascade paths.
  - After: `List<RowMutationHint>` or `List<LocatedRowValues>` with `RowLocation Location` and `object[] Values`.
  - Note: this should also subsume the complex-column catalog rewrite lists currently shaped as `(PageNumber, RowIndex, Values)`.
  - Files: `JetDatabaseWriter/AccessWriter.cs`, `JetDatabaseWriter/Catalog/CatalogWriter.cs`, `JetDatabaseWriter/Indexes/IndexMaintainer.cs`, `JetDatabaseWriter/Indexes/UniqueIndexChecker.cs`, `JetDatabaseWriter/Relationships/RelationshipChildRowLocator.cs`, `JetDatabaseWriter/Relationships/RelationshipEnforcer.cs`, and `JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs`.

- [ ] Add an internal `AutoNumberCheckpoint` type.
  - Before: `(ColumnConstraint Constraint, long? PreviousValue)` in constraint application and rollback.
  - After: `AutoNumberCheckpoint` with `ColumnConstraint Constraint` and `long? PreviousValue`.
  - Files: `JetDatabaseWriter/Schema/ConstraintRegistry.cs` and `JetDatabaseWriter/AccessWriter.cs` insert-batch preparation.

- [ ] Add a relationship table-pair type.
  - Before: repeated `(string Pk, string Fk)` tuples for relationship table name pairs.
  - After: `RelationshipTablePair` with `PrimaryTable` and `ForeignTable` (or `Pk` / `Fk` if matching the surrounding shorthand is preferable).
  - File: `JetDatabaseWriter/Relationships/RelationshipManager.cs` around the relationship-pair helper code.

- [x] Add a public `MultiValueItem` model if changing the public API is acceptable.
  - Before: `IReadOnlyList<(int ConceptualTableId, object? Value)>` for multi-value complex-column readback.
  - After: `IReadOnlyList<MultiValueItem>` with `ConceptualTableId` and `Value` properties.
  - Compatibility note: this is a deliberate public API change from tuple-field access to named DTO property access; README usage was updated.
  - Files: `JetDatabaseWriter/Models/MultiValueItem.cs`, `JetDatabaseWriter/Interfaces/IAccessReader.cs`, `JetDatabaseWriter/AccessReader.cs`, `JetDatabaseWriter/ComplexColumns/ComplexColumnReader.cs`, and public API tests.

- [x] Add an Office crypto package result type.
  - Before: `(byte[] EncryptionInfo, byte[] EncryptedPackage)` from Standard and Agile encryption helpers.
  - After: `OfficeEncryptedPackage` or `EncryptedPackageStreams` with `EncryptionInfo` and `EncryptedPackage`.
  - Files: `JetDatabaseWriter/Encryption/OfficeCryptoStandard.cs`, `JetDatabaseWriter/Encryption/OfficeCryptoAgile.cs`, `JetDatabaseWriter/Encryption/EncryptionConverter.cs`, and encryption tests.

- [x] Add an Agile password-key material type.
  - Before: `(byte[] VerifierInput, byte[] VerifierHash, byte[] KeyValue, byte[] HmacKey, byte[] HmacValue)` from `DeriveAllPasswordKeys`.
  - After: `AgilePasswordKeys` or `AgileEncryptionKeyMaterial` with named properties for each derived key.
  - File: `JetDatabaseWriter/Encryption/OfficeCryptoAgile.cs`.

- [x] Add a flat Agile encryption-info result type.
  - Before: `(byte[] EncryptionInfo, byte[] IntermediateKey, byte[] KeyDataSalt)` from flat Agile encryption setup.
  - After: `FlatAgileEncryptionInfo` or `FlatAgileArtifacts`.
  - File: `JetDatabaseWriter/Encryption/OfficeCryptoAgile.cs`.

## New Types: Optional / Opportunistic

- [ ] Consider returning `PageDecryptionKeys` directly from the reader key-resolution path.
  - Before: `ResolveReaderPageKeys(...)` returns `(uint? Rc4DbKey, byte[]? AesPageKey)`, and `CreatePageDecryptionKeys(...)` immediately wraps those values in `PageDecryptionKeys` with the Jet3 mask.
  - After: either keep the tuple because it is only a partial input, or move Jet3 mask resolution into the helper and return `PageDecryptionKeys` directly.
  - File: `JetDatabaseWriter/Encryption/EncryptionManager.cs`.

- [ ] Add a decryption result type only if encryption conversion APIs grow further.
  - Before: `(byte[] Plaintext, AccessEncryptionFormat SourceFormat)` and `(byte[]? Plaintext, AccessEncryptionFormat Format)`.
  - After: `DecryptedDatabase` or `DecryptionResult` with plaintext bytes and detected format.
  - Files: `JetDatabaseWriter/Encryption/EncryptionConverter.cs` and `JetDatabaseWriter/Encryption/EncryptionManager.cs`.

- [ ] Add a CFB chain-run type if the CFB reader gets more chain-walking helpers.
  - Before: `(uint RunStart, int RunSectors, uint Next)` returned by `CoalesceRun`.
  - After: `CfbSectorRun` or `FatChainRun` with the same three fields.
  - File: `JetDatabaseWriter/CompoundFile/CompoundFileReader.cs`.

- [ ] Add an index row-pointer type when pointer-only seek results spread further.
  - Before: `List<(long DataPage, int RowIndex)>`, `List<(long DataPage, byte DataRow)>`, and dictionaries carrying only data page plus row index.
  - After: `IndexRowPointer` or `DataRowPointer`; use `byte` or `int` consistently at the boundary. Do not use `IndexEntry` here unless key bytes are also present.
  - Files: `JetDatabaseWriter/Indexes/IndexCursor.cs`, `JetDatabaseWriter/Indexes/IndexPageCodec.cs`, `JetDatabaseWriter/Indexes/IndexEntrySplicer.cs`, `JetDatabaseWriter/Relationships/RelationshipChildRowLocator.cs`, and seek-related tests.

- [ ] Add a page-write batch item type if staged rewrites gain more behavior.
  - Before: `List<(long PageNum, byte[] Bytes)>`, dictionaries of `(page, bytes)`, and loops deconstructing `(long pn, byte[] bytes)`.
  - After: `PageRewrite` or `StagedPageWrite` with `PageNumber` and `Bytes`.
  - Files: `JetDatabaseWriter/Indexes/IndexBTreeEditor.cs` and `JetDatabaseWriter/Indexes/IndexMaintainer.cs`.

- [ ] Add index-page header and leaf-search result types only if the codec surface keeps expanding.
  - Before: `(long Prev, long Next, long Tail)` and `(bool Found, bool ContinueToNext)`.
  - After: `IndexSiblingPointers` and `LeafSearchResult`.
  - File: `JetDatabaseWriter/Indexes/IndexPageCodec.cs`.

- [ ] Add ordered wrapper types only if the stable-sort helpers stop being one-method locals.
  - Before: `(IndexEntry Entry, int Order)[]` and `(IntermediateOp Op, int Order)[]`.
  - After: `OrderedIndexEntry` and `OrderedIntermediateOp` private record structs.
  - Files: `JetDatabaseWriter/Indexes/IndexEntrySplicer.cs` and `JetDatabaseWriter/Indexes/Helpers/IndexHelpers.cs`.

- [ ] Add a TDEF build-result type when doing TDEF builder work.
  - Before: `(byte[] Page, int[] FirstDpOffsets)`, `(byte[][] Pages, int[] FirstDpLogicalOffsets, int[] UsedPagesLogicalOffsets)`, and `(byte[][] Pages, int[] FirstDpLogicalOffsets)`.
  - After: prefer one cohesive `TDefPageBuildResult` / `TDefPageChainBuildResult`. Avoid proliferating tiny result records such as `SplitTDefPagesResult` unless the split helper becomes independently reused.
  - File: `JetDatabaseWriter/Schema/TDefPageBuilder.cs`.

- [ ] Add a logical TDEF offset type only if physical/logical offset mapping spreads.
  - Before: `(int PageIndex, int PageOffset)` from `LogicalToPhysicalOffset`.
  - After: `LogicalTDefOffset`.
  - Files: `JetDatabaseWriter/Schema/LogicalTDefChain.cs` and `JetDatabaseWriter/Schema/TDefPageBuilder.cs`.

- [ ] Add a catalog-bootstrap column descriptor type only if that code changes again.
  - Before: `(string Name, ColumnType Type, int ColNum, int VarIdx, int FixedOff, int Size, byte Flags)` arrays for slim/full MSysObjects bootstrap columns.
  - After: `CatalogColumnDescriptor` or `BootstrapColumnDescriptor`. Do not use `ColumnInfo`; that is parsed mutable metadata, not a write-template descriptor.
  - File: `JetDatabaseWriter/Schema/TDefPageBuilder.cs`.

- [ ] Add complex-column schema/template types if template generation grows.
  - Before: `(string Name, ColumnDefinition[] Columns)` for complex type templates, and `(ColumnDefinition[] Columns, IndexDefinition[] Indexes)` for flat-table schemas.
  - After: `ComplexTypeTemplate` and `ComplexFlatTableSchema`.
  - File: `JetDatabaseWriter/ComplexColumns/ComplexColumnManager.cs`.

- [ ] Add relationship-planning result types if FK planning grows further.
  - Before: `(FkSidePlan Plan, List<string> ExistingNames)` and `(FkSidePlan PkPlan, FkSidePlan FkPlan, List<string> ExistingNames)`.
  - After: `PreparedFkSide` and `PreparedSelfReferentialFkSides`.
  - File: `JetDatabaseWriter/Relationships/RelationshipManager.cs`.

- [ ] Add a relationship seek-index candidate type if seek planning gains more state.
  - Before: `(long FirstDp, IReadOnlyList<bool> AscendingFlags)?`.
  - After: `SeekIndexCandidate` with root page and ascending flags.
  - File: `JetDatabaseWriter/Relationships/RelationshipSeekPlanner.cs`.

- [ ] Add generic child-row-locator request/result wrappers only if the generic cascade code becomes harder to follow.
  - Before: `IEnumerable<(object?[] OldPk, TPayload Payload)>`, `(long DataPage, int RowIndex, TPayload Payload)`, and `List<(RowLocation Loc, TPayload Payload)>`.
  - After: `ChildRowSeekRequest<TPayload>`, `PendingChildRow<TPayload>`, and `LocatedChildRow<TPayload>`.
  - Files: `JetDatabaseWriter/Relationships/RelationshipChildRowLocator.cs` and `JetDatabaseWriter/Relationships/RelationshipEnforcer.cs`.

- [ ] Add a direct-row-decoder binding type only if more binding metadata is added.
  - Before: `(int Index, RowMapper<T>.Accessor Accessor, ColumnInfo Col)`.
  - After: `BoundRowAccessor<T>` or `DirectRowAccessorBinding<T>` private record struct.
  - File: `JetDatabaseWriter/ValueDecoding/DirectRowDecoderBuilder.cs`.

## Keep As Tuples Or Deconstruction For Now

- [ ] Keep local `string.Create` state tuples.
  - Before: `(Bytes: bytes, Start: start)` and `(Bytes: bytes, Start: start, Length: len)`.
  - After: no change; these are private callback state values and are clearer as inline state than as named types.
  - File: `JetDatabaseWriter/AccessBase.cs`.

- [ ] Keep tuple swaps in crypto and byte-order code.
  - Before: `(h, scratchHash) = (scratchHash, h)`, `(s[i], s[j]) = (s[j], s[i])`, and byte-array swap expressions.
  - After: no change; this is idiomatic swap syntax, not a domain data model.
  - Files: encryption helpers and schema numeric/byte-order helpers.

- [ ] Keep simple dictionary and existing-record deconstruction where the source type is already named.
  - Before: `foreach ((string key, long value) in tableRowCounts)`, `foreach ((int realIdxNum, RealIdxEntry entry) in slots)`, and deconstruction of `LogicalIdxEntry`, `TdefPreamble`, or `KeyColumn`.
  - After: no change unless a specific method reads better with property access. This is deconstruction of named shapes, not an anonymous tuple contract to replace.
  - Files: catalog, index, relationship, and statistics code.

- [ ] Keep LINQ/test tuples used as ephemeral assertion data.
  - Before: `.Select((item, index) => (item, index))`, `(expected, actual)`-style assertion values, test table seeds, and small benchmark scenario pairs.
  - After: no change; these are local test/benchmark data and do not justify new production types.
  - Files: `JetDatabaseWriter.Tests/**`, `JetDatabaseWriter.Benchmarks/**`.

- [ ] Keep FormatProbe exploratory candidate tuples unless that code graduates into the library.
  - Before: large tuple-heavy candidate, corpus, CRC, and diagnostic groupings.
  - After: no change for exploratory diagnostics; add local record structs only when a shape becomes reused enough to obscure the probe.
  - Files: `JetDatabaseWriter.FormatProbe/**`.

- [ ] Keep one-off local algorithm tuples when they are tightly scoped and self-named.
  - Before: small local pairs such as `(groups, lastPerGroup)`, `(plan, existingNames)`, `(firstDataPage, ascending)`, or `(pages, offsets)` in a short helper.
  - After: no change unless the tuple crosses an API boundary, appears in multiple methods, or accumulates more fields.
  - Files: index builder, relationship planner, complex-column builder, and TDEF builder helpers.

## Suggested Order

1. Replace the clear existing-type fits first: `ColumnInfo`, `RowLocation`, `IndexEntry`, and `IndexSectionAnchors`.
2. Add `ResolvedTable`, `RowMutationHint` / `LocatedRowValues`, `AutoNumberCheckpoint`, and `RelationshipTablePair`; these remove the most repeated internal tuple contracts.
3. Public `MultiValueItem` API change is complete; account for the tuple-to-DTO compatibility cost in release communication.
4. Add crypto result/key-material types when touching encryption code.
5. Consider index, page-write, TDEF, relationship-planning, and other helper result types opportunistically when touching those modules next.
6. Leave tests, benchmarks, FormatProbe, tuple swaps, callback state, and opportunistic module-local shapes alone unless local readability has already become a problem.
