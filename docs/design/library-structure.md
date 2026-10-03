# Library Structure

This document describes the architecture and folder organization of the `JetDatabaseWriter` library — a .NET library for reading and writing Microsoft Access (JET/ACE) database files at the binary format level.

---

## Directory layout

```
JetDatabaseWriter/
├── AccessBase.cs                          (public base: format, page size, code page; owns the facade's DatabaseFile)
├── AccessReader.cs                        (public read API — facade; every operation forwards to one reader service)
├── AccessWriter.cs                        (public write API — facade; every operation forwards to one writer service)
├── DatabaseFile.cs                        (one open database: composes its JetFormat and its PageFile or Pager and forwards to them; TDEF parsing, owned-page and live-row enumeration)
├── JetFormat.cs                           (immutable per-file format profile: format, page size, code page, byte layouts, text and name codecs)
├── ReaderServices.cs                      (reader composition root: builds and wires the reader's collaborators)
├── WriterServices.cs                      (writer composition root: builds and wires the writer's collaborators)
├── AccessReaderOptions.cs
├── AccessWriterOptions.cs
├── JetTransaction.cs
├── Constants.cs                           (format constants, magic numbers, page offsets)
├── IsExternalInit.cs                      (compiler shim for init-only properties)
├── JetDatabaseWriter.csproj                (library project and NuGet packaging metadata)
├── packages.lock.json                      (locked NuGet dependency graph)
│
├── Interfaces/
│   ├── IAccessBase.cs
│   ├── IAccessIndexQuery.cs               (fluent exact/prefix/range read queries over a named index)
│   ├── IAccessOptions.cs
│   ├── IAccessReader.cs
│   ├── IAccessSchema.cs                   (DDL: CreateTable, AddColumn, linked tables, relationships)
│   └── IAccessWriter.cs                   (DML: Insert, Update, Delete, complex-row APIs)
│
├── Models/                                (public DTOs — one per file)
│   ├── AttachmentInput.cs
│   ├── AttachmentRecord.cs
│   ├── ColumnDefinition.cs
│   ├── ColumnMetadata.cs
│   ├── ColumnPredicate.cs
│   ├── ColumnPredicateOperator.cs
│   ├── ColumnSize.cs
│   ├── ComplexCellValue.cs                (decodes the complex-column cells that row reads return)
│   ├── ComplexColumnInfo.cs
│   ├── DatabaseStatistics.cs
│   ├── DbDefault.cs                       (stands for a column's default in an insert row, like SQL DEFAULT)
│   ├── Hyperlink.cs
│   ├── IndexColumnReference.cs
│   ├── IndexDefinition.cs
│   ├── IndexKeyBound.cs
│   ├── IndexMetadata.cs
│   ├── LinkedTableInfo.cs
│   ├── MultiValueItem.cs
│   ├── OleObjectContent.cs                (what an OLE Object value holds, as OleObjectValue unwraps it)
│   ├── OleObjectValue.cs                  (unwraps OLE Object values, which reads return as stored bytes)
│   ├── RelationshipDefinition.cs
│   ├── RelationshipMetadata.cs
│   ├── RowCriteria.cs
│   ├── RowValues.cs
│   ├── TableProgress.cs
│   └── TableStat.cs
│
├── Enums/
│   ├── AccessEncryptionFormat.cs
│   ├── ColumnSizeUnit.cs
│   ├── ColumnType.cs                      (public JET column-type discriminator enum)
│   ├── ComplexColumnKind.cs
│   ├── DatabaseFormat.cs
│   ├── DeletedRowDataMode.cs              (internal: whether deleting a row also scrubs its payload bytes)
│   ├── IndexKind.cs
│   ├── IndexQueryKind.cs                  (internal index-query predicate kind)
│   ├── IntermediateOpType.cs
│   ├── LinkedTableKind.cs
│   ├── OleObjectKind.cs                   (NotWrapped, EmbeddedFile, LinkedFile, EmbeddedObject, Unknown)
│   ├── PageReadOptimizationMode.cs        (reader random-access/read-ahead optimization policy)
│   ├── SecureEraseMode.cs
│   ├── SystemTableIndexMaintenancePath.cs (last system-table index-maintenance path used by writer)
│   └── TdefPreambleStatus.cs
│
├── Exceptions/
│   └── JetLimitationException.cs
│
├── Catalog/                               (system-table reading/writing)
│   ├── CatalogArtifactWriter.cs           (executes CatalogArtifactPlans: table artifacts, catalog object rows, fresh MSysObjects bootstrap)
│   ├── CatalogReader.cs                   (read-side MSysObjects queries: user/system table resolution, LvProp, diagnostics)
│   ├── CatalogRowReader.cs                (read-only MSysObjects row scans and system-table lookup)
│   ├── CatalogWriter.cs                   (MSysObjects / MSysACEs row inserts, renames, and deletions)
│   ├── CatalogValueReader.cs              (safe MSys* row access and tolerant invariant scalar parsing)
│   ├── ColumnPropertyReader.cs            (reads a table's LvProp blob and hydrates calculated-column result types)
│   ├── TableCatalog.cs                    (cached user-table catalog and name lookup, shared by reader and writer)
│   └── Models/
│       ├── CatalogArtifactPlan.cs
│       ├── CatalogObjectAcePolicy.cs
│       ├── CatalogObjectArtifact.cs
│       ├── CatalogObjectIdPolicy.cs
│       ├── CatalogTableArtifact.cs
│       ├── CatalogEntry.cs
│       ├── CatalogRow.cs
│       ├── CatalogScanSummary.cs          (what the last user-table scan saw; rendered as LastDiagnostics)
│       ├── ResolvedTable.cs
│       ├── TableDef.cs
│       ├── UserTableCatalogDeletionArtifact.cs
│       ├── UserTableCatalogDeletionResult.cs
│       └── UserTableCatalogReplacementArtifact.cs
│
├── DelimitedText/                         (internal CSV/delimited text parsing for linked text tables)
│   ├── DelimitedTextReader.cs             (buffered record parser with quote, CR/LF, and limit handling)
│   ├── DelimitedTextColumnNames.cs        (header normalization and generated F1/F2/... names)
│   ├── DelimitedTextFormat.cs             (validated delimiter and header-row options)
│   ├── DelimitedTextLimits.cs             (record/field/column parser safety limits)
│   └── DelimitedTextRecord.cs             (parsed row fields plus row/line tracking metadata)
│
├── ValueEncoding/                         (write-path: typed values → bytes)
│   ├── RowEncoder.cs                      (SerializeRow, EncodeFixed/Variable/Text/Binary)
│   ├── LongValueEncoder.cs               (pre-encodes oversized MEMO/OLE/attachment values)
│   ├── NumericEncoder.cs                  (BCD decimal encoding)
│   └── Models/
│       ├── FixedPointPayload.cs
│       └── PreEncodedLongValue.cs
│
├── LongValues/                            (shared LVAL storage codec)
│   ├── LongValueStore.cs                  (descriptor/page helpers, chain traversal, deallocation)
│   └── Models/
│       ├── LongValueDescriptor.cs         (12-byte MEMO/OLE/attachment descriptor parser/serializer)
│       ├── LvalRowLocation.cs
│       └── LvalChainResult.cs             (bounded chain-read result)
│
├── ValueDecoding/                         (read-path: bytes → typed values)
│   ├── RowDecodePlan.cs                   (row-layout preflight, projection masks, string rows, typed/direct slice decoding)
│   ├── RowMapper.cs                       (object-array → POCO mapping and generic write projection, via EntityMap)
│   ├── RowCriteriaEvaluator.cs            (compiles RowCriteria against a table, evaluates decoded rows)
│   ├── TypedRowFallbackPolicy.cs          (strict/lenient malformed-row fallback behavior)
│   ├── OleObjectDecoder.cs                (structured OLE Object parser behind OleObjectValue, media type from a signature at offset 0, data URIs of stored bytes)
│   ├── LongValueDecoder.cs               (typed MEMO/OLE decode over LongValues, through the reader's page cache)
│   ├── RowDecoder.cs                      (decodes a cached data page's rows to typed values or strings, resolving LVAL chains)
│   ├── DirectRowDecoderBuilder.cs         (builds optimized row decode delegates)
│   └── Models/
│       ├── CalculatedLongValueRef.cs
│       ├── ColumnSlice.cs
│       ├── ColumnSliceKind.cs
│       ├── LongValueRef.cs
│       └── UnreadableLongValue.cs         (MEMO/OLE value the writer's snapshot could not read; writing it throws)
│
├── Pages/                                 (page-level I/O & layout)
│   ├── DataPageLayout.cs                  (byte offsets, page structure, format-version layouts)
│   ├── DataPageInserter.cs               (FindInsertTarget, CanInsertRow, WriteRowToPage)
│   ├── PageAllocator.cs                   (global free-map reuse, freed-page scrubbing, tail shrink)
│   ├── ReservedPageRuns.cs                (page runs an index operation reserved but has not linked; released on a bail or throw)
│   ├── ReaderPageCache.cs                 (the reader's page and row-directory LRU caches)
│   ├── UsageMap.cs                        (INLINE/REFERENCE usage-map parsing, bitmaps, pointer/row emission)
│   ├── PageJournal.cs                     (in-memory after-images of a transaction's pages, replayed in place on commit)
│   ├── Paging/                            (one open file's page I/O)
│   │   ├── IPageSource.cs                 (read decrypted pages: the interface read-side code depends on)
│   │   ├── PageBuffers.cs                 (pooled page-buffer return and caller-owned page copies)
│   │   ├── PageFile.cs                    (read-only page I/O: stream, I/O gate, page cipher, RandomAccess reads)
│   │   └── Pager.cs                       (the writer's PageFile: writes, appends, flushes, byte-range locks, the transaction journal and its JournalGate)
│   └── Models/
│       ├── DataPageInserterState.cs       (insert hint + writable owned-map set, restored on rollback)
│       ├── LocatedRow.cs                  (a decoded row paired with the location it was read from)
│       ├── OverflowRowTarget.cs           (the slot an overflow row's header points at, and its page)
│       ├── PageInsertTarget.cs
│       ├── RowBound.cs                    (one row-directory entry; flags an overflow row's header)
│       ├── RowLayout.cs
│       ├── RowLocation.cs                 (a row's identity slot, and the slot that holds its bytes)
│       └── UsageMapPointer.cs
│
├── Indexes/                               (all index concerns — B-tree, key encoding, maintenance)
│   ├── AccessObjectIndexQuery.cs          (fluent object-array index query implementation)
│   ├── AccessTypedIndexQuery.cs           (fluent POCO index query implementation)
│   ├── IndexKeyEncoder.cs                 (column values → sort key bytes)
│   ├── IndexBTreeBuilder.cs               (constructs index B-tree pages)
│   ├── IndexBTreeEditor.cs                (plans/applies in-place B-tree mutations)
│   ├── IndexCursor.cs                     (read-only B-tree descent and exact-key lookups)
│   ├── IndexPageCodec.cs                  (index page build/decode, pointers, entry bitmasks)
│   ├── IndexPageLayout.cs                 (Jet3 / Jet4 index page layout selection)
│   ├── IndexQueryCriteria.cs              (exact, key-prefix, and range predicate descriptor)
│   ├── IndexPredicateTranslator.cs        (extracts index-seekable AND conjuncts from a typed predicate)
│   ├── IndexSeekFilter.cs                 (keeps the conjuncts a seek on the column's keys answers without missing a row)
│   ├── IndexPlanner.cs                    (chooses the best index for a predicate; builds the seek criteria)
│   ├── IndexPlan.cs                       (chosen index + seek criteria + matched key-column count)
│   ├── IndexCatalogReader.cs              (reads index definitions from system tables)
│   ├── IndexEntrySplicer.cs               (stable in-memory index entry add/remove splicing)
│   ├── IndexMaintainer.cs                 (TDEF/catalog orchestration for index maintenance)
│   ├── IndexLayout.cs                     (index page byte-offset structs)
│   ├── UniqueIndexChecker.cs              (validates uniqueness constraints)
│   ├── Helpers/
│   │   └── IndexHelpers.cs
│   ├── Collation/                         (sort-key generation for text indexes)
│   │   ├── CharHandlerType.cs
│   │   ├── GeneralTextIndexEncoder.cs
│   │   ├── GeneralTextIndexEncoder.V2010LongRowSuffix.cs
│   │   ├── GeneralLegacyTextIndexEncoder.cs
│   │   └── General97TextIndexEncoder.cs
│   ├── CodeTables/                        (embedded gzipped collation lookup tables)
│   │   ├── index_codes_ext_gen.txt.gz
│   │   ├── index_codes_ext_genleg.txt.gz
│   │   ├── index_codes_gen.txt.gz
│   │   ├── index_codes_genleg.txt.gz
│   │   ├── index_codes_gen_97.txt.gz
│   │   └── index_mappings_ext_gen_97.txt.gz
│   └── Models/
│       ├── ChildSeekIndex.cs
│       ├── DecodedIntermediateEntry.cs
│       ├── DescentStep.cs
│       ├── EncodedIndexBound.cs
│       ├── EncodedIndexRange.cs
│       ├── IndexBTreeBuildResult.cs
│       ├── IndexEntry.cs
│       ├── IndexSectionAnchors.cs
│       ├── IntermediateOp.cs
│       ├── KeyColumn.cs
│       ├── KeyColumnInfo.cs
│       ├── LogicalIdxEntry.cs
│       ├── ParentSeekIndex.cs
│       ├── RealIdxEntry.cs
│       ├── RealIdxSlot.cs
│       ├── ResolvedIndex.cs
│       ├── SplitPages.cs
│       └── UniqueIndexDescriptor.cs
│
├── Schema/                                (DDL: table/column/index definition & type metadata)
│   ├── TDefPageBuilder.cs                 (constructs Table Definition pages)
│   ├── ColumnPropertyBlockBuilder.cs      (builds column property blocks)
│   ├── ConstraintRegistry.cs              (manages column constraints — auto-increment, defaults, validation)
│   ├── AutoNumberMaintainer.cs            (reads and advances the per-table AutoNumber high-water value)
│   ├── JetTypeInfo.cs                     (column type metadata — sizes, flags, CLR mapping)
│   ├── JetExpressionConverter.cs          (expression parsing for calculated columns)
│   ├── CalculatedColumnUtil.cs            (utility methods for calculated column handling)
│   ├── LinkedOdbcLvPropBuilder.cs         (generated linked-ODBC schema-cache property blocks)
│   ├── LogicalTDefChain.cs                (logical TDEF bytes spanning chained table-definition pages)
│   ├── PersistedPropertyProjector.cs      (carries a table's LvProp blob through AddColumn / DropColumn / RenameColumn)
│   ├── Expressions/
│   │   ├── AccessExpressionKeywords.cs             (Access word operators and literal words, shared by the parser and ExpressionFieldReferences)
│   │   ├── CalculatedExpressionAstFactory.cs       (ClosedXML.Parser adapter for calculated-expression AST nodes)
│   │   ├── CalculatedExpressionBinaryNode.cs       (binary operator AST node)
│   │   ├── CalculatedExpressionCoercion.cs         (central Access null/date/number/text coercion semantics)
│   │   ├── CalculatedExpressionDateTimeFunctions.cs (date/time function catalog)
│   │   ├── CalculatedExpressionEvaluationContext.cs
│   │   ├── CalculatedExpressionEvaluator.cs        (entry point: applies calculated-expression plans to row values)
│   │   ├── CalculatedExpressionFinancialFunctions.cs (financial function catalog)
│   │   ├── CalculatedExpressionFormattingFunctions.cs (formatting function catalog)
│   │   ├── CalculatedExpressionFunctionNode.cs     (function-call AST node)
│   │   ├── CalculatedExpressionFunctionRegistry.cs (descriptor-based function lookup and argument validation)
│   │   ├── CalculatedExpressionLimits.cs           (expression safety caps and generated-text limits)
│   │   ├── CalculatedExpressionLogicalFunctions.cs (logical function catalog)
│   │   ├── CalculatedExpressionMetadataFunctions.cs (metadata function catalog)
│   │   ├── CalculatedExpressionNameNode.cs         (column/name AST node)
│   │   ├── CalculatedExpressionNode.cs             (base calculated-expression AST node)
│   │   ├── CalculatedExpressionNormalizer.cs       (Access-precedence parse of every expression: column brackets, date literals, word operators)
│   │   ├── CalculatedExpressionNumericFunctions.cs (numeric function catalog)
│   │   ├── CalculatedExpressionPlan.cs
│   │   ├── CalculatedExpressionTextFunctions.cs    (text function catalog)
│   │   ├── CalculatedExpressionUnaryNode.cs        (unary operator AST node)
│   │   ├── CalculatedExpressionUnsupportedNode.cs  (unsupported syntax AST sentinel)
│   │   ├── CalculatedExpressionValueNode.cs        (literal value AST node)
│   │   ├── CalculatedFunctionDescriptor.cs         (function alias, domain, and argument metadata)
│   │   ├── CalculatedFunctionDomain.cs             (calculated-function domain enum)
│   │   ├── CalculatedFunctionEvaluator.cs          (function evaluator delegate)
│   │   ├── CalculatedFunctionInvocation.cs         (bound function invocation context)
│   │   ├── ColumnDefaultValue.cs                   (persisted column DefaultValue expression, evaluated on insert)
│   │   ├── ColumnValidationRule.cs                 (persisted column ValidationRule: implicit operand, three-valued logic)
│   │   └── ExpressionFieldReferences.cs            (lexer that finds and renames a field's references in expression text, for column renames and drops)
│   └── Models/
│       ├── ColumnConstraint.cs
│       ├── ConstraintRegistrySnapshot.cs  (registry contents + AutoNumber counters, restored on rollback)
│       ├── ColumnInfo.cs
│       ├── ColumnPropertyBlock.cs
│       ├── ColumnPropertyEntry.cs
│       ├── ColumnPropertyEntryBuilder.cs
│       ├── ColumnPropertyTarget.cs
│       ├── ColumnPropertyTargetBuilder.cs
│       ├── ColumnPropertyChunkType.cs
│       └── ColumnPropertyUnknownChunk.cs
│
├── Transactions/                          (lifecycle, locking, journaling)
│   ├── TransactionLifecycle.cs            (begin/commit/rollback orchestration)
│   ├── LockFileCoordinator.cs             (multi-process lock file management)
│   ├── LockFileSlotWriter.cs              (writes process slot into .ldb/.laccdb)
│   └── JetByteRangeLock.cs                (filesystem byte-range lock primitives)
│
├── Encryption/                            (all cryptographic concerns)
│   ├── EncryptionManager.cs               (key derivation, page encrypt/decrypt dispatch)
│   ├── EncryptionConverter.cs             (format conversion — add/remove/change encryption)
│   ├── OfficeCryptoAgile.cs               (ECMA-376 Agile encryption — AES-256-CBC, SHA-512)
│   ├── OfficeCryptoPrimitives.cs          (shared Office Crypto hashing, HMAC, AES helpers)
│   ├── OfficeCryptoStandard.cs            (MS-OFFCRYPTO §2.3.6 Standard — AES-128-CBC, SHA-1)
│   └── Models/
│       ├── OfficeEncryptedPackage.cs
│       └── PageDecryptionKeys.cs
│
├── Relationships/                         (foreign keys, cascade rules, linked tables)
│   ├── RelationshipManager.cs             (relationship lifecycle and TDEF FK logical-index mutation)
│   ├── RelationshipCatalogStore.cs        (MSysRelationships row emission, loading, and rewrites)
│   ├── RelationshipMetadataAggregator.cs  (groups MSysRelationships rows into per-relationship metadata)
│   ├── RelationshipEnforcer.cs            (runtime FK insert/update/delete referential-integrity enforcement)
│   ├── RelationshipSeekPlanner.cs         (parent/child FK B-tree seek-index resolution)
│   ├── RelationshipChildRowLocator.cs     (child-row location resolution from FK-side index seeks)
│   ├── RelationshipKeyBuilder.cs          (shared FK composite-key projection and fallback key building)
│   ├── RelationshipCascadePolicy.cs       (cascade recursion-depth guard)
│   ├── RelationshipPageReader.cs          (owned page-copy adapter for index cursor reads)
│   ├── FkRelationship.cs                  (enforced FK metadata model)
│   ├── FkContext.cs                       (per-mutation FK lookup cache)
│   ├── RelationshipRowSnapshot.cs         (MSysRelationships row rewrite snapshot)
│   ├── RelationshipRewriteState.cs        (a table's relationship state captured before a column add/drop/rename rewrite)
│   ├── FkLogicalIndexSnapshot.cs          (one FK logical-index entry carried across that rewrite)
│   ├── RelationshipKeyColumn.cs           (a relationship key column named by MSysRelationships)
│   ├── LinkedTableManager.cs              (linked-table catalog scan, source-path policy, delimited-text read-through, link creation)
│   ├── LinkedTableReader.cs               (reader-side link cache and read-through; opens a separate reader for Access-file links)
│   └── LinkedSourcePolicy.cs              (linked-source open options plus the host path relative sources anchor to)
│
├── ComplexColumns/                        (multi-value fields, attachments, versioned columns)
│   ├── ComplexColumnReader.cs             (complex-column metadata, subtypes, flat-table decode, and the row-read cells)
│   ├── ComplexItemReader.cs               (attachment and multi-value item reads from the hidden flat tables)
│   ├── ComplexColumnManager.cs            (write/scaffold/cascade complex column data)
│   ├── ComplexReferenceSeedReader.cs      (flat-table lookup, per-row complex slots, and the seed for new per-row references)
│   └── Models/
│       ├── AttachmentWrapper.cs
│       └── ComplexColumnAllocation.cs
│
├── Tables/                                (table-level workflow services for both facades, plus writer row primitives)
│   ├── TableReader.cs                     (table scans: typed / string / DataTable / POCO reads, live row counts, read-ahead)
│   ├── IndexRowReader.cs                  (index metadata, seeks and range reads, predicate-inferred index reads)
│   ├── SchemaReader.cs                    (column metadata, relationships, table lists and database statistics)
│   ├── TableDataWriter.cs                 (row DML: insert / update / delete batches)
│   ├── TableSchemaEditor.cs               (table DDL: create / drop table, add / drop / rename column, page reclaim)
│   ├── TableRowStore.cs                   (row primitives: write row bytes, mark deleted, adjust TDEF row count)
│   └── TableSnapshotReader.cs             (writer's decoded reads of its own rows, through its own DatabaseFile and journal)
│
├── Linq/                                  (public LINQ surface, namespace JetDatabaseWriter.Linq)
│   ├── AccessQueryExtensions.cs           (Include/ThenInclude + async terminal operators for Query<T>)
│   └── IAccessIncludableQueryable.cs      (the Include/ThenInclude chaining interface)
│
├── Queries/                               (read-path: LINQ IQueryable provider over a single table)
│   ├── AccessQueryable.cs                 (composable, async-enumerable IQueryable<T> over one table)
│   ├── AccessOrderedQueryable.cs          (IOrderedQueryable<T> marker produced only by ordering operators)
│   ├── AccessQueryProvider.cs             (IQueryProvider: translates the expression and runs the stage pipeline)
│   ├── AccessQueryTranslator.cs           (splits the engine-evaluable prefix from the in-memory tail)
│   ├── AccessQueryPlan.cs                 (ordered stage pipeline plus include navigation paths)
│   ├── IAccessQueryEngine.cs              (non-generic execution surface the provider exposes)
│   ├── QueryStage.cs                      (base: one operator applied in written order)
│   ├── FilterStage.cs                     (Where stage; AND-combines a leading run for index push-down)
│   ├── OrderStage.cs                      (OrderBy/ThenBy buffer-and-sort stage)
│   ├── SkipStage.cs                       (Skip paging stage)
│   ├── TakeStage.cs                       (Take paging stage)
│   ├── OrderingKey.cs                     (one sort key: selector plus direction)
│   ├── QueryKeyComparer.cs                (null-first, type-tolerant ordering-key comparison)
│   ├── RuntimeRowMapper.cs                (maps object?[] rows onto a runtime-resolved POCO type)
│   ├── IncludableQueryable.cs             (adapts a composed query to IAccessIncludableQueryable)
│   ├── IncludeLoader.cs                   (relationship-inferred eager loading and stitching)
│   ├── IncludeStep.cs                     (one Include/ThenInclude navigation plus inline operators)
│   ├── IncludeOperation.cs                (base for inline filtered/ordered/paged include operators)
│   ├── IncludeFilterOperation.cs          (Where applied to a parent's loaded children)
│   ├── IncludeOrderOperation.cs           (OrderBy/ThenBy applied to a parent's loaded children)
│   ├── IncludeSkipOperation.cs            (Skip applied to a parent's loaded children)
│   └── IncludeTakeOperation.cs            (Take applied to a parent's loaded children)
│
├── Mapping/                               (POCO mapping model shared by typed reads, writes and LINQ)
│   ├── EntityMap.cs                       (cached per-type property↔column map honouring [Column], [NotMapped], [Table])
│   ├── EntityProperty.cs                  (one mapped property and its column name)
│   └── ValueCoercer.cs                    (the one cell-value → property-type conversion every typed read uses)
│
├── CompoundFile/                          (MS-CFB OLE structured storage)
│   ├── CompoundFileReader.cs              (read .accdb wrapped in CFB container)
│   └── CompoundFileWriter.cs              (write CFB container for Agile-encrypted output)
│
└── Infrastructure/                        (generic utilities — not JET-specific)
    ├── LruCache.cs                        (256-page least-recently-used eviction cache)
    ├── ByteArrayEqualityComparer.cs       (byte[] equality for dictionary keys)
    ├── BinaryBuffer.cs                    (byte-slice copy helpers)
    ├── BinaryStringParser.cs              (base64 and data-URI decoding helpers)
    ├── BoxCache.cs                        (interned boxes for low-cardinality fixed-width cell values)
    ├── FileStreamFactory.cs               (central FileStream construction helpers)
    ├── StreamReadExtensions.cs            (cross-target stream read helpers)
    ├── AsyncLazyInitializer.cs            (thread-safe async lazy initialization)
    ├── AsyncReentrantOperationGate.cs     (reentrant async operation serializer)
    ├── Adler32.cs                         (zlib Adler-32 checksum for attachment zlib streams)
    ├── ClosureValueReader.cs              (reads captured LINQ operands through reflection instead of compiling them)
    └── Guard.cs                           (argument validation helpers)
```

The other projects in the solution are not part of the package: the tests (`JetDatabaseWriter.Tests`, net10.0 and net8.0), the benchmarks, the format probe, the scaffolding CLI, and `JetDatabaseWriter.TestSupport`. TestSupport is a non-packable class library, built for net10.0 and net8.0 like the tests, that holds code the tests and the format probe share but the package must not ship: `DaoPowerShellHostResolver`, which finds a Windows PowerShell host that can load DAO, and `PowerShellProcessRunner`, which runs that host and, when it times out, kills it with every process it started.

---

## Architectural layers

The library follows a **Layered Codec / Service Architecture** — the dominant pattern in binary format libraries (protobuf, MessagePack-CSharp, Apache Parquet, SQLite, System.Text.Json), adapted for a file-format library whose reader and writer services share one open file's page I/O. Four practical layers are visible:

| Layer | Folders | Responsibility |
|-------|---------|----------------|
| **Infrastructure** | `Infrastructure/`, `CompoundFile/` | Generic helpers, stream compatibility shims, CFB container parsing/writing |
| **Storage / Page Services** | `DatabaseFile` (root), `JetFormat` (root), `Pages/`, `Transactions/`, `Encryption/` | One open file's page I/O and format layouts, page caching, usage-map parsing/serialization, allocation/free-list reuse, journaling, locking, page encryption |
| **Codec / Domain Services** | `ValueEncoding/`, `ValueDecoding/`, `DelimitedText/`, `Indexes/`, `Catalog/`, `Schema/`, `Relationships/`, `ComplexColumns/`, `Tables/`, `Queries/` | Encode/decode values, rows, index keys, and linked text records; read/write system tables; run the reader's and writer's table workflows; translate and run LINQ queries; manage feature-specific catalog artifacts |
| **API / Orchestration** | Root (`AccessReader`, `AccessWriter`, `AccessBase`, `ReaderServices`, `WriterServices`), `Interfaces/`, public `Models/`, public `Enums/` | User-facing operations, options, DTOs, and composition |

Both `AccessReader` and `AccessWriter` are **facades** (GoF). Each keeps only what is genuinely its own: opening or creating the database (header read, Agile unwrap, lock-file slot, and for the writer the byte-range lock), disposal order, and, for the writer, the refusal of flat-Agile files on open, the Agile re-wrap, the auto-commit scope, and the static encryption helpers. Every other public method forwards to one service:

- reader: `TableReader`, `IndexRowReader`, `SchemaReader`, or `ComplexItemReader`; `FromIndex` and `Query<T>` return handles built over those services;
- writer: `TableDataWriter`, `TableSchemaEditor`, `RelationshipManager`, `ComplexColumnManager`, `LinkedTableManager`, `PageAllocator`, or `TransactionLifecycle`, inside the auto-commit scope.

Each facade owns one **`DatabaseFile`**, which composes the file's parts and forwards to them:

- its format profile, `JetFormat` (`DatabaseFile.Profile`): the format, page size, code page, byte layouts and text codecs, built once from the header;
- its page I/O (`DatabaseFile.Pages`): a read-only `PageFile` for the reader, which owns the stream, the I/O gate, the page cipher and the positional reads, and cannot write; or, for the writer, a `Pager`, the `PageFile` that also writes, appends, truncates and flushes pages, encrypts on write, takes the cooperative byte-range locks and holds the transaction journal. `TransactionLifecycle` attaches and detaches that journal only through a `Pager.JournalGate` lease, never through the gate or the journal directly. The reader's object graph holds no `Pager`, so its write forwarders throw;
- TDEF parsing, and owned data-page and live-row enumeration.

Its `PageCount` is the one end of file: inside a transaction the `Pager` includes the pages the journal has appended past the physical end, so every page-number bounds check, and every caller that numbers new pages before appending them, sees the transaction's own pages. `AccessBase` holds the `DatabaseFile`, exposes the public format properties over it, and nothing else. The facade object is never handed to a service, so at runtime the facade and its services share the `DatabaseFile`, not the facade.

`ReaderServices` and `WriterServices` are the **composition roots**. Each builds its facade's collaborators once and passes each one two kinds of dependency through its constructor:

- the `DatabaseFile` it reads and writes pages through; and
- the specific sibling services it calls, such as `TableCatalog`, `ReaderPageCache`, `RowDecoder`, `TableRowStore`, `IndexMaintainer`, or `ConstraintRegistry`.

`TableCatalog`, the cached user-table catalog, is shared in shape by both graphs: both facades resolve table names through the same `MSysObjects` scan (`CatalogRowReader`), and system tables by name through its one lookup, `FindSystemTableTdefPageAsync`, which resolves `MSysObjects` itself to TDEF page 2 when no catalog row names it. The reader's operation gate (`AsyncReentrantOperationGate`) is created by `ReaderServices` and entered by each reader service's public operations, so the LINQ provider and index-query handles, which call services directly, are drained on disposal the same way facade calls are.

No collaborator receives a facade, `AccessBase`, or a composition root, and none finds a sibling through another object. `ServiceGraphTests` enforces this:

- no library type outside the facades holds or accepts any of those types, including inside generic arguments;
- the collaborator graph reachable from each composition root is acyclic;
- walking the live object graph from an open facade's services never reaches the facade;
- the writer's services hold exactly one `DatabaseFile`, the writer's own, and no `AccessReader`; and
- the facades declare no internal members.

The writer reads its own file only through its own `DatabaseFile`. Workflows that read a table before changing it (update, delete, cascades, index rebuilds, schema rewrites, constraint seeding, and relationship enforcement) decode rows through `TableSnapshotReader`, which `WriterServices` builds from a `RowDecoder` over a capacity-0 `ReaderPageCache` and a `CatalogReader` over the writer's own `TableCatalog`. Every page therefore comes through the writer's `Pager`, which returns an active transaction's pending pages and decrypts the rest with the writer's page keys, and nothing read this way is cached between calls: a `ReaderPageCache` over a `Pager` refuses any capacity but 0. Workflows that then change those rows read them with `TableSnapshotReader.ReadRowsAsync`, which returns each decoded row as a `LocatedRow` paired with the location it came from, and mutate exactly that location; nothing pairs separately read rows and locations by position.

Two exceptions are deliberate. The public `JetTransaction` handle calls back into the `TransactionLifecycle` that issued it, the same owner/handle shape as `DbConnection` and `DbTransaction`. `LinkedTableReader` opens a separate `AccessReader` on each Access-file link's source database; it does not receive or hold the reader that owns it.

---

## Dependency graph

Three graphs matter. The two collaborator graphs are acyclic and tested; the folder map is not layered.

**Reader collaborator graph (acyclic, tested).** `AccessReader` holds `ReaderServices`; `ReaderServices` constructs every reader collaborator. Roughly, from the top:

```
AccessReader → ReaderServices
  TableReader         → ReaderPageCache, RowDecoder, CatalogReader, ComplexColumnReader, LinkedTableReader
  IndexRowReader      → ReaderPageCache, RowDecoder, CatalogReader, ComplexColumnReader, TableReader
  SchemaReader        → ReaderPageCache, CatalogReader, ComplexColumnReader, LinkedTableReader, TableReader
  ComplexItemReader   → ComplexColumnReader
  LinkedTableReader   → CatalogReader, LinkedSourcePolicy
  ComplexColumnReader → CatalogReader, RowDecoder
  CatalogReader       → TableCatalog, CatalogRowReader, RowDecoder, ColumnPropertyReader
  ColumnPropertyReader → RowDecoder
  TableCatalog        → CatalogRowReader
  RowDecoder          → ReaderPageCache, LongValueDecoder
  LongValueDecoder    → ReaderPageCache
  (services with public operations) → AsyncReentrantOperationGate
  (every collaborator) → DatabaseFile
```

**Writer collaborator graph (acyclic, tested).** `AccessWriter` holds `WriterServices`; `WriterServices` constructs every writer collaborator. Roughly, from the top:

```
AccessWriter → WriterServices
  TableDataWriter     → TableCatalog, TableRowStore, IndexMaintainer, UniqueIndexChecker, AutoNumberMaintainer,
                        ConstraintRegistry, RelationshipEnforcer, ComplexColumnManager, TableSnapshotReader
  TableSchemaEditor   → TableCatalog, TableRowStore, IndexMaintainer, PageAllocator, LongValueEncoder, CatalogWriter,
                        CatalogArtifactWriter, ComplexColumnManager, ConstraintRegistry, RelationshipManager, TableSnapshotReader,
                        AutoNumberMaintainer
  RelationshipManager → TableCatalog, IndexMaintainer, PageAllocator, CatalogArtifactWriter, CatalogRowReader, RelationshipCatalogStore
  RelationshipEnforcer → TableCatalog, TableRowStore, IndexMaintainer, RelationshipCatalogStore, ComplexColumnManager, TableSnapshotReader
  ComplexColumnManager → TableCatalog, TableRowStore, IndexMaintainer, CatalogArtifactWriter, CatalogRowReader, ConstraintRegistry,
                        AutoNumberMaintainer, ComplexReferenceSeedReader
  ComplexReferenceSeedReader → CatalogRowReader, AutoNumberMaintainer
  CatalogArtifactWriter → TableCatalog, PageAllocator, TDefPageBuilder, DataPageInserter, CatalogWriter, ConstraintRegistry
  CatalogWriter       → TableCatalog, TableRowStore, IndexMaintainer, LongValueEncoder, ConstraintRegistry, CatalogRowReader
  IndexMaintainer     → PageAllocator, TableRowStore, DataPageInserter, TableSnapshotReader
  TableSnapshotReader → RowDecoder, CatalogReader
  CatalogReader       → TableCatalog, CatalogRowReader, RowDecoder, ColumnPropertyReader
  ColumnPropertyReader → RowDecoder
  RowDecoder          → ReaderPageCache (capacity 0), LongValueDecoder
  TableRowStore       → LongValueEncoder, RowEncoder, DataPageInserter, TDefPageBuilder
  DataPageInserter    → PageAllocator, CatalogRowReader
  TableCatalog        → CatalogRowReader
  TransactionLifecycle → JetByteRangeLock, TableCatalog, DataPageInserter, ConstraintRegistry
  (every collaborator) → DatabaseFile
```

**Folder map (not layered).** Folders are domain groupings, not layers. Measured from `using` directives (with `Models` sub-namespaces folded into their folder), they reference each other as follows. `DatabaseFile` marks a dependency on the open database file as the page I/O and format context; `opens a reader` marks a class that opens a separate `AccessReader` of its own:

```
Infrastructure/   → (nothing — leaf)
CompoundFile/     → Infrastructure/
DelimitedText/    → Infrastructure/
Mapping/          → Infrastructure/
LongValues/       → Pages/, Schema/
Pages/            → Catalog/, Schema/, Infrastructure/; DatabaseFile
Transactions/     → Catalog/, Pages/, Schema/, Infrastructure/; DatabaseFile
Encryption/       → CompoundFile/, Schema/, Transactions/, Infrastructure/
ValueDecoding/    → Catalog/, LongValues/, Mapping/, Pages/, Schema/, Infrastructure/; DatabaseFile
ValueEncoding/    → Catalog/, LongValues/, Pages/, Schema/, ValueDecoding.Models/; DatabaseFile
Schema/           → Catalog/, Encryption/, Indexes/, Pages/, Infrastructure/; DatabaseFile
Indexes/          → Catalog/, Mapping/, Pages/, Schema/, Tables/, ValueDecoding.Models/, ValueEncoding/, Infrastructure/; DatabaseFile
Catalog/          → Indexes/, Pages/, Schema/, Tables/, ValueDecoding/, ValueEncoding/, Infrastructure/; DatabaseFile
ComplexColumns/   → Catalog/, Encryption/, Indexes/, Pages/, Schema/, Tables/, ValueDecoding/, Infrastructure/; DatabaseFile
Relationships/    → Catalog/, ComplexColumns/, DelimitedText/, Indexes/, Pages/, Schema/, Tables/, ValueDecoding/,
                    Infrastructure/; DatabaseFile; opens a reader (linked sources)
Tables/           → Catalog/, ComplexColumns/, Indexes/, LongValues/, Pages/, Relationships/, Schema/,
                    ValueDecoding/, ValueEncoding/, Infrastructure/; DatabaseFile
Queries/          → Indexes/, Linq/, Mapping/, Tables/, Infrastructure/
Linq/             → Queries/, Infrastructure/
DatabaseFile (root)   → JetFormat, Catalog/, Encryption/, Indexes/, Pages/, Schema/, Transactions/, ValueDecoding/, Infrastructure/
JetFormat (root)      → Encryption/, Indexes/, Pages/, Schema/
AccessBase (root)     → DatabaseFile
AccessReader (root)   → ReaderServices, Indexes/, Queries/, Encryption/, Transactions/
AccessWriter (root)   → WriterServices, Relationships/, Encryption/, Schema/, Transactions/
ReaderServices (root) → every reader collaborator
WriterServices (root) → every writer collaborator
```

Because folders group by domain, several pairs reference each other: `Catalog` ↔ `Indexes`, `Catalog` ↔ `Pages`, `Catalog` ↔ `Schema`, `Catalog` ↔ `Tables`, `Catalog` ↔ `ValueDecoding`, `Catalog` ↔ `ValueEncoding`, `ComplexColumns` ↔ `Tables`, `Encryption` ↔ `Schema`, `Indexes` ↔ `Schema`, `Indexes` ↔ `Tables`, `Linq` ↔ `Queries`, `Pages` ↔ `Schema`, and `Relationships` ↔ `Tables`. Each pair comes from different classes in the two folders using one another (for example, `IndexMaintainer` in `Indexes/` uses `TableRowStore` in `Tables/`, while `TableDataWriter` in `Tables/` uses `IndexMaintainer`; `CatalogReader` decodes `MSysObjects` rows through `RowDecoder`, while the value decoders read `TableDef` from `Catalog.Models`). The reader's and writer's table-level workflow services all live in `Tables/`, so the read path adds only the `Catalog` ↔ `ValueDecoding` pair. The acyclicity guarantee applies to the two collaborator graphs above, not to the folder map. The library is a single project, so there are no project-level cycles. `Infrastructure/` and the pure layout and value helpers remain stable leaf dependencies.

---

## Namespace conventions

Every folder maps 1:1 to a namespace per the .NET Framework Design Guidelines (§3.4):

| Folder | Namespace |
|--------|-----------|
| Root | `JetDatabaseWriter` |
| `Interfaces/` | `JetDatabaseWriter.Interfaces` |
| `Models/` | `JetDatabaseWriter.Models` |
| `Enums/` | `JetDatabaseWriter.Enums` |
| `Exceptions/` | `JetDatabaseWriter.Exceptions` |
| `Catalog/` | `JetDatabaseWriter.Catalog` |
| `Catalog/Models/` | `JetDatabaseWriter.Catalog.Models` |
| `DelimitedText/` | `JetDatabaseWriter.DelimitedText` |
| `LongValues/` | `JetDatabaseWriter.LongValues` |
| `LongValues/Models/` | `JetDatabaseWriter.LongValues.Models` |
| `ValueEncoding/` | `JetDatabaseWriter.ValueEncoding` |
| `ValueEncoding/Models/` | `JetDatabaseWriter.ValueEncoding.Models` |
| `ValueDecoding/` | `JetDatabaseWriter.ValueDecoding` |
| `ValueDecoding/Models/` | `JetDatabaseWriter.ValueDecoding.Models` |
| `Pages/` | `JetDatabaseWriter.Pages` |
| `Pages/Models/` | `JetDatabaseWriter.Pages.Models` |
| `Pages/Paging/` | `JetDatabaseWriter.Pages.Paging` |
| `Indexes/` | `JetDatabaseWriter.Indexes` |
| `Indexes/Helpers/` | `JetDatabaseWriter.Indexes.Helpers` |
| `Indexes/Collation/` | `JetDatabaseWriter.Indexes.Collation` |
| `Indexes/Models/` | `JetDatabaseWriter.Indexes.Models` |
| `Schema/` | `JetDatabaseWriter.Schema` |
| `Schema/Expressions/` | `JetDatabaseWriter.Schema.Expressions` |
| `Schema/Models/` | `JetDatabaseWriter.Schema.Models` |
| `Transactions/` | `JetDatabaseWriter.Transactions` |
| `Encryption/` | `JetDatabaseWriter.Encryption` |
| `Encryption/Models/` | `JetDatabaseWriter.Encryption.Models` |
| `Relationships/` | `JetDatabaseWriter.Relationships` |
| `ComplexColumns/` | `JetDatabaseWriter.ComplexColumns` |
| `ComplexColumns/Models/` | `JetDatabaseWriter.ComplexColumns.Models` |
| `Tables/` | `JetDatabaseWriter.Tables` |
| `Queries/` | `JetDatabaseWriter.Queries` |
| `Linq/` | `JetDatabaseWriter.Linq` |
| `Mapping/` | `JetDatabaseWriter.Mapping` |
| `CompoundFile/` | `JetDatabaseWriter.CompoundFile` |
| `Infrastructure/` | `JetDatabaseWriter.Infrastructure` |

Public API types live at the root namespace (`JetDatabaseWriter`) — no sub-namespace required for consumers to access the main entry points. The LINQ extensions are the exception: `AccessQueryExtensions` and `IAccessIncludableQueryable` live in `JetDatabaseWriter.Linq`, because EF Core declares extension methods (`Include`, `ThenInclude`, `ToListAsync`, …) and an `IIncludableQueryable` with the same names, and a file that imports both libraries' namespaces would otherwise get ambiguity errors.

---

## Interface hierarchy

The public interface uses **Interface Segregation** (ISP) to separate concerns:

```
IAccessBase          (format metadata, page size, code page, async disposal)
├── IAccessReader    (read/stream rows, schema metadata, exact index seek, LINQ Query<T>,
│                    relationship metadata, complex/linked reads)
├── IAccessSchema    (DDL: CreateTable, DropTable, AddColumn, DropColumn, RenameColumn,
│                    CreateLinkedTable, CreateLinkedTextTable, CreateLinkedOdbcTable,
│                    Create/Drop/RenameRelationship)
└── IAccessWriter    (DML: InsertRow, InsertRows, UpdateRows, DeleteRows,
                     AddAttachment, AddMultiValueItem)
```

`IAccessSchema` and `IAccessWriter` are independent peers that both extend `IAccessBase`; the concrete `AccessWriter` implements both. `IAccessReader` is a separate branch extending `IAccessBase`. This split follows ADO.NET precedent — consumers depend only on the surface they need.

---

## Design patterns in use

| Pattern | Where applied | Rationale |
|---------|--------------|-----------|
| **Facade** (GoF) | `AccessReader`, `AccessWriter` | Each public operation forwards to one service (inside the auto-commit scope for the writer); keeps the public API surface small |
| **Composition Root** | `ReaderServices`, `WriterServices` | Builds each facade's collaborators once and injects each one's dependencies through its constructor; no collaborator can reach the facade or another service through a shared object |
| **Symmetric Codec** | `ValueEncoding/` ↔ `ValueDecoding/`, `LongValueEncoder` ↔ `LongValueDecoder` | Matched encode/decode pairs (same pattern as protobuf's `CodedOutputStream`/`CodedInputStream`) |
| **Shared Storage Codec** | `LongValues/LongValueStore`, `LongValueDescriptor` | Centralizes LVAL descriptor parsing, page-buffer emission, chain traversal, and secure-erase page reclamation |
| **Builder** | `TDefPageBuilder`, `IndexBTreeBuilder`, `ColumnPropertyBlockBuilder`, `DirectRowDecoderBuilder` | Constructs complex page buffers incrementally |
| **Cursor / Editor** | `IndexCursor`, `IndexBTreeEditor`, `IndexPageCodec` | Keeps read-only B-tree descent and in-place mutation planning separate from TDEF/catalog orchestration |
| **Strategy via layout structs** | `JetFormat` holding `DataPageLayout`, `LvalPageLayout`, `TDefHeaderLayout`, `ColumnDescriptorLayout`, `RowFieldSizes`, `IndexLayout`, `IndexPageLayout` | Format-version polymorphism (Jet3 vs Jet4 vs ACE) without virtual dispatch; one immutable profile per open file, built from its header |
| **Pager** | `PageFile` / `Pager` + `ReaderPageCache` (`LruCache`) + `PageJournal` | Dedicated page-level I/O: the reader's read-only `PageFile` with its 256-page LRU eviction cache, and the writer's `Pager` with an in-memory transaction journal. Unlike SQLite's pager, the journal holds after-images only and commit writes them in place, so a commit is not crash-atomic |
| **Allocator** | `PageAllocator`, `ReservedPageRuns` | Centralizes Access global free-map reuse, freed-page headers, secure erase, and tail-only shrink; index paths record each run they reserve until a TDEF or usage-map write links it, and give back any run they abandon |
| **Usage Map Codec** | `UsageMap` | Centralizes INLINE/REFERENCE ownership and free-map row parsing, bitmap traversal, bit mutation, pointer emission, and inline row serialization |
| **Row Decode Plan** | `RowDecodePlan` | Centralizes row-layout preflight, projection masks, string-row materialization, typed fixed/variable slice decoding, direct-decoder slice resolution, calculated payload handling, and partial key-column reads |
| **Manager / Coordinator** | `RelationshipManager`, `LinkedTableManager`, `LinkedTableReader`, `ComplexColumnManager`, `ComplexColumnReader` | Keeps feature-specific catalog and child-table workflows out of the public facades |
| **Workflow Service** | `TableReader`, `IndexRowReader`, `SchemaReader`, `TableDataWriter`, `TableSchemaEditor`, `CatalogArtifactWriter` | Own the read paths and the writer's DML, DDL, and catalog-plan workflows so neither facade holds any |
| **Catalog Store** | `RelationshipCatalogStore` | Keeps MSysRelationships row emission/loading/rewrites separate from TDEF logical-index mutation |
| **Runtime Enforcer** | `RelationshipEnforcer` | Keeps FK insert/update/delete referential-integrity checks separate from create/drop/rename workflows |
| **Streaming Parser** | `DelimitedTextReader` | Parses linked CSV/delimited text records one at a time with bounded memory, quote handling, and line tracking |
| **Planner / Locator** | `RelationshipSeekPlanner`, `RelationshipChildRowLocator` | Separates index-backed lookup planning and row-location resolution from FK fallback/enforcement workflow |
| **Policy** | `TypedRowFallbackPolicy`, `RelationshipCascadePolicy` | Encapsulates strict vs lenient malformed-row handling and FK cascade recursion limits |
| **Gateway** (Fowler) | `LockFileCoordinator`, `JetByteRangeLock` | Encapsulates filesystem concurrency primitives behind a clean interface |
| **Registry** | `ConstraintRegistry`, `CalculatedExpressionFunctionRegistry` | Centralized constraint management and calculated-expression function dispatch — decoupled from the writer orchestrator and evaluator entry point |
| **Query Provider / Pipeline** | `AccessQueryProvider`, `AccessQueryTranslator`, `QueryStage` subclasses | LINQ `IQueryable` provider over the reader's `TableReader`, `IndexRowReader`, and `SchemaReader` that translates an expression tree into an ordered stage pipeline, pushes a leading filter run into index inference, and replays the unsupported tail in memory |
| **Specification** | `RowCriteria`, `ColumnPredicate`, `RowCriteriaEvaluator` | Named-column predicate objects expressing writer update/delete filters independently of how they compile and evaluate against decoded rows |
| **Query Planner** | `IndexPlanner`, `IndexPredicateTranslator`, `IndexSeekFilter`, `IndexPlan` | Chooses the best index for a predicate and builds a sound (superset) seek, leaving a residual client-side filter to enforce exactness; a comparison whose property holds a converted value, or whose operand the key cannot hold, is left to that filter |

---

## Design principles applied

### SOLID

| Principle | How applied |
|-----------|-------------|
| **Single Responsibility (SRP)** | Each file/class owns one concern. `RowEncoder` only serializes rows; `UsageMap` only parses/emits usage-map rows and bits; `DataPageInserter` only manages page insertion; `TransactionLifecycle` only handles begin/commit/rollback |
| **Open/Closed (OCP)** | Adding a new column type means extending `JetTypeInfo` (`GetClrType`, `ReadFixedTyped`), `RowEncoder`, and type metadata helpers — not modifying the orchestrator |
| **Interface Segregation (ISP)** | `IAccessReader`, `IAccessSchema` (DDL), and `IAccessWriter` (DML) are separated; consumers depend only on what they use |
| **Dependency Inversion (DIP)** | Reader and writer collaborators receive their dependencies through constructors from `ReaderServices` / `WriterServices`; they depend on the `DatabaseFile` page I/O and format context, not on the facade that owns them |

### Package design principles (Robert C. Martin)

| Principle | How applied |
|-----------|-------------|
| **Common Closure (CCP)** | Classes that change together live together. All index concerns in `Indexes/`; all encryption in `Encryption/` |
| **Common Reuse (CRP)** | Classes used together live together. `CatalogEntry`, `CatalogRow`, `TableDef` always consumed as a group → `Catalog/Models/` |
| **Acyclic Dependencies (ADP)** | Applied at the class level to both facades: the collaborator graphs built by `ReaderServices` and `WriterServices` have no cycles, and `ServiceGraphTests` enforces it. Folders are domain groupings and do reference each other in both directions (see the dependency map). |
| **Stable Dependencies (SDP)** | Pure helpers (`Infrastructure/`, layout structs, codec primitives) stay stable. Reader and writer services depend on `DatabaseFile` for coordinated page I/O and format state, which is more stable than any service and never depends on one. |

---

## Organizational philosophy

### Domain-first folders ("Screaming Architecture")

The folder structure communicates the **domain** — not the technical role of each type:

```
Indexes/              ← "what subsystem" ✓
Encryption/           ← "what subsystem" ✓
Catalog/              ← "what subsystem" ✓
```

Not:

```
Models/               ← "what kind of thing" ✗
Builders/             ← "what kind of thing" ✗
Helpers/              ← "what kind of thing" ✗
```

When a developer opens the solution, the top-level folders **scream** "this is a JET database engine" — you immediately see the major subsystems (indexes, encryption, catalog, pages, transactions, etc.) rather than generic role-based buckets.

Models that belong to a specific domain are co-located with that domain (`Indexes/Models/`, `Schema/Models/`, etc.). Only the public API DTOs live in the root `Models/` folder, since they span multiple subsystems.

### `internal` as an access modifier, not a folder

Visibility is controlled via the C# `internal` keyword on classes — not by stuffing everything into an `Internal/` directory. This eliminates the misleading namespace prefix while maintaining encapsulation. Test projects access internals via `[InternalsVisibleTo]`.

Internal access goes to the internal types, not through the facades. Tests that inspect pages or the catalog, or drive one service, build the layers they need: `ReaderHarness` and `WriterHarness` open a `DatabaseFile` directly and construct `ReaderServices` / `WriterServices` over it. The format probe does the same through its own `ProbeDatabase`. The few tests that check how a facade wires itself at open (page-cache allocation, random-access reads) read its private state by reflection. So the facades carry no members for tests or tools, and `ServiceGraphTests` keeps it that way.

### Naming to avoid BCL shadowing

- **`ValueEncoding/`** not `Encoding/` — avoids shadowing `System.Text.Encoding`
- **`ValueDecoding/`** not `Decoding/` — symmetric with `ValueEncoding/`
- **`Collation/`** not `TextEncoding/` — avoids confusion with character encoding

---

## Key architectural decisions

### 1. Thin facades over composition roots and one database file

`AccessWriter` and `AccessReader` are **facades**. Every public method forwards to one service; the facade keeps only opening and creating databases, the lock-file lifetime, disposal order, and for the writer the byte-range lock, the refusal of flat-Agile files on open, the auto-commit scope, the Agile-encryption re-wrap, and the static encryption helpers.

Both used to be the shared context their collaborators reached through. Writer managers took `AccessWriter` and found their siblings through internal `Relationships`, `ComplexColumns`, and `Constraints` properties. Reader helpers (`ComplexColumnReader`, `LongValueDecoder`, the index queries, the LINQ provider, `IncludeLoader`, and the reader half of `LinkedTableManager`) took `AccessReader` itself, and some called its public methods back. Page I/O lived in the `AccessBase` base class, so even a service that only read pages held the facade object. `ReaderServices` and `WriterServices` now wire each graph explicitly. Page I/O moved out of `AccessBase` into `DatabaseFile`, which every service depends on instead, and the user-table catalog moved into the shared `TableCatalog`.

`DatabaseFile` is the largest shared class: one open file's page I/O, format layouts, TDEF parsing, and owned-page and live-row enumeration. It is cohesive around "the bytes of this file", but it is the next candidate if one service needs only part of it.

### 2. ValueEncoding and ValueDecoding share neutral format domains

These are symmetric but independent. Shared types live in neutral domains such as `Schema/` (`ColumnInfo`, `JetTypeInfo`) and `LongValues/` (`LongValueDescriptor`, `LongValueStore`) so the read path and write path do not depend on each other's implementation folders.

`RowDecodePlan` is the read-side row decode coordinator. It is built from `TableDef`, an optional projection mask or partial-column ordinal list, and strictness requirements. The plan parses/preflights row layout once, resolves per-column slices through `DatabaseFile`, materializes `RowsAsStrings()` rows, decodes typed fixed/variable values, emits async LVAL sentinels for `RowDecoder` to resolve, supplies row-layout and slice resolution to the direct POCO expression-tree decoder, and serves the writer's index/FK partial key-column reader.

### 3. Models co-located with their domain

Internal DTOs live in `{Domain}/Models/` subdirectories. This satisfies CRP — you never need to import a grab-bag `Models/` namespace to get one type; you import the specific domain's models.

### 4. Public and domain DTOs get their own files

Public API types and domain DTOs get their own files. Reusable shapes that are consumed across files are top-level internal types in their domain's folder, including the extracted index layout records, row-layout primitives, column slices, long-value references, property-block builders, numeric payloads, usage-map pointers, and complex-column allocations. Small implementation details that are private or tightly coupled to one algorithm may remain nested inside that algorithm's file.

### 5. Embedded resources follow their consumer

The `CodeTables/` directory (gzipped collation lookup data) lives under `Indexes/` alongside the `Collation/` encoders that consume it — not in a generic resources folder.

### 6. Catalog row parsing stays with catalog ownership

`CatalogValueReader` lives in `Catalog/` because it handles tolerant scalar reads from system-table rows (`MSysObjects`, `MSysRelationships`, `MSysComplexColumns`, etc.): safe `string[]` cell access, missing-column defaults, and invariant integer parsing of catalog metadata. It is not a general user-value parser. User table column values flow through `ValueDecoding/` (`RowDecoder` and `RowDecodePlan`), and write-path values through `ValueEncoding/`.

### 7. Facade-owned services stay in their domain

Classes such as `PageAllocator`, `DataPageInserter`, `TDefPageBuilder`, `RelationshipManager`, `ComplexColumnManager`, `ReaderPageCache`, `CatalogReader`, and `ComplexColumnReader` live beside the disk-format concern they manipulate. Each receives the `DatabaseFile` plus the specific siblings it calls, injected by its composition root. Table-level workflows that span domains live in `Tables/` for both facades: the reader's scans, index reads, and schema reads, and the writer's row DML, table DDL, row-level storage primitives, and snapshot reads. This keeps the folder structure domain-first without making either facade the shared context.

### 8. Usage-map parsing stays with page ownership

`UsageMap` lives in `Pages/` because INLINE and REFERENCE usage-map rows are page-layout structures, not reader-only or writer-only behavior. It owns pointer reads/writes, row-bound lookup, bitmap traversal, point bit checks and mutation, and inline row serialization. Callers keep policy: `DatabaseFile` validates mapped owned data pages before taking the fast path; `DataPageInserter` marks table owned/free rows; `PageAllocator` decides when to promote the global free map and allocate reference pages; `CatalogArtifactWriter`, `TableSchemaEditor`, and `IndexMaintainer` decide which index pages to emit or reclaim.

### 9. Linked-table metadata spans catalog, schema, and delimited text parsing

Linked-table public APIs live on `IAccessSchema`; linked-table catalog scanning, source-path policy, and creation live in `Relationships/LinkedTableManager`, and the reader's link cache and read-through live in `Relationships/LinkedTableReader`, which reads an Access-file link by opening a separate `AccessReader` on its source; ODBC schema-cache property-map generation lives in `Schema/LinkedOdbcLvPropBuilder` because it emits `MSysObjects.LvProp` property blocks using the shared schema property-map builder. Text/CSV linked-table read-through delegates record parsing to `DelimitedText/`, keeping separator, quote, line-ending, header-normalization, and parser-limit behavior reusable outside the linked-table manager.

### 10. Relationship catalog and runtime helpers are split from lifecycle orchestration

`RelationshipManager` owns relationship create/drop/rename workflow and per-TDEF FK logical-index mutation. `RelationshipCatalogStore` owns `MSysRelationships` row emission, loading, and rewrites, while `RelationshipEnforcer` owns insert/update/delete referential-integrity checks. The runtime path uses smaller helpers for reusable policy and lookup work: `RelationshipSeekPlanner` resolves parent/child B-tree seek indexes, `RelationshipChildRowLocator` turns child-side seek hits into live `RowLocation` values, `RelationshipKeyBuilder` keeps seek and snapshot fallback key semantics aligned, and `RelationshipCascadePolicy` owns the cascade-depth guard independently of catalog mutation setup.

### 11. Calculated expressions use explicit helper ownership

`CalculatedExpressionEvaluator` remains the row-local entry point for applying calculated-column expressions, but parsing, normalization, AST nodes, coercion, safety limits, and function dispatch are split into focused internal helpers. Supported Access/VBA functions are registered through `CalculatedExpressionFunctionRegistry` using descriptors for aliases, argument counts, domains, and evaluator delegates; the implementation lives in domain catalogs such as `CalculatedExpressionTextFunctions` and `CalculatedExpressionDateTimeFunctions`. Spreadsheet-only constructs, external references, and domain aggregates stay rejected at the parser/evaluator boundary instead of leaking into row evaluation.

### 12. The LINQ query layer is read-only and degrades gracefully

`Queries/` adds an `IQueryable<T>` over a single table (`AccessReader.Query<T>`), and `Linq/` holds its public extensions (`Include`, `ThenInclude` and the async terminals). The provider and `IncludeLoader` hold the reader's `TableReader`, `IndexRowReader`, and `SchemaReader`, not the facade. `AccessQueryProvider` translates only the operators it can run natively against the storage engine — a leading run of `Where` filters (AND-combined and pushed into index inference by `IndexPredicateTranslator`/`IndexPlanner`), `OrderBy`/`ThenBy`, `Skip`, and `Take` — into an ordered `QueryStage` pipeline that honors written order. `AccessQueryTranslator` marks the engine boundary at the first unsupported operator (notably `Select` projections): the prefix runs in the engine and the tail replays in memory through LINQ-to-Objects. Relationship-inferred eager loading (`Include`/`ThenInclude`, including filtered/ordered/paged collection includes) is a post-materialization step driven by the `MSysRelationships` catalog. Index selection is intentionally sound-but-not-exact — a seek can return a superset — so the compiled residual predicate is always reapplied to every row the seek yields. The seek compares stored keys while that predicate compares mapped properties, so `Indexes/IndexSeekFilter` keeps only the comparisons on which the two agree: the property must hold every stored value exactly (not an enum or `Guid` property bound to a Text column) and the operand must encode to a key that bounds the matches. The values the translators need while planning (the constant side of a pushed comparison, and the `Skip`/`Take` counts of an include) are read by `Infrastructure/ClosureValueReader`, which follows constants, static members and closure field and property chains through reflection and compiles only a computed operand such as `DateTime.Today` or `id + 1`. Typed results convert each value to its property through `Mapping/ValueCoercer`, for the root entities and the included ones alike.

---

## Public API surface

The public entry points are:

| Type | Purpose |
|------|---------|
| `AccessReader` | Open and read .mdb/.accdb files — stream rows, materialize DataTables/POCOs, LINQ `Query<T>` with relationship-inferred eager loading, exact index seek, read schema, relationship metadata, linked-table metadata/read-through, complex-column metadata/items |
| `AccessWriter` | Create/open/write .mdb/.accdb files — CRUD with named-column `RowValues`/`RowCriteria` filters, DDL, linked-table catalog rows, relationships, complex-column row APIs, transactions, storage maintenance, encryption conversion helpers |
| `AccessReaderOptions` | Reader configuration: page cache, validation, strict parsing, password, lock-file settings (the reader never writes, so it takes no byte-range locks and ignores `UseByteRangeLocks`), linked-source path policy, linked-text limits |
| `AccessWriterOptions` | Writer configuration: password, full catalog schema, lock-file/byte-range locking, transaction page budget, secure erase, implicit transactional writes |
| `JetTransaction` | Disposable transaction handle returned by `BeginTransactionAsync` |
| `Linq.AccessQueryExtensions` | LINQ extensions for `Query<T>` results — relationship-inferred `Include`/`ThenInclude` eager loading and async terminal operators |
| `Linq.IAccessIncludableQueryable<TEntity, TProperty>` | Interface returned by `Include`/`ThenInclude` so a chain can carry the most recently included navigation type |
| `Models/*` | Public DTOs for column definitions, index metadata, relationships, etc. |
| `Enums/*` | Public enumerations (database format, encryption format, linked-table kind, secure erase mode, etc.) |
| `Exceptions/*` | Domain-specific exceptions |
| `Interfaces/*` | Abstractions for DI/testing (`IAccessReader`, `IAccessWriter`, `IAccessSchema`, etc.) |

All other types are `internal` — implementation details organized by domain.
