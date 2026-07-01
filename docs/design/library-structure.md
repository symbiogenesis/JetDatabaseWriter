# Library Structure

This document describes the architecture and folder organization of the `JetDatabaseWriter` library — a .NET library for reading and writing Microsoft Access (JET/ACE) database files at the binary format level.

---

## Directory layout

```
JetDatabaseWriter/
├── AccessBase.cs                          (shared I/O, page read, format detection, stream lifecycle)
├── AccessReader.cs                        (public read API — thin orchestrator delegating to domain modules)
├── AccessWriter.cs                        (public write API — thin orchestrator delegating to domain modules)
├── AccessReaderOptions.cs
├── AccessWriterOptions.cs
├── AccessQueryExtensions.cs               (public LINQ Include/ThenInclude + async terminal operators)
├── IIncludableQueryable.cs                (public Include/ThenInclude chaining marker interface)
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
│   ├── ComplexColumnInfo.cs
│   ├── DatabaseStatistics.cs
│   ├── Hyperlink.cs
│   ├── IndexColumnReference.cs
│   ├── IndexDefinition.cs
│   ├── IndexKeyBound.cs
│   ├── IndexMetadata.cs
│   ├── LinkedTableInfo.cs
│   ├── MultiValueItem.cs
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
│   ├── PageReadOptimizationMode.cs        (reader random-access/read-ahead optimization policy)
│   ├── SecureEraseMode.cs
│   ├── SystemTableIndexMaintenancePath.cs (last system-table index-maintenance path used by writer)
│   └── TdefPreambleStatus.cs
│
├── Exceptions/
│   └── JetLimitationException.cs
│
├── Catalog/                               (system-table reading/writing)
│   ├── CatalogWriter.cs                   (InsertCatalogEntry, RewriteTable, RenameInCatalog)
│   ├── CatalogValueReader.cs              (safe MSys* row access and tolerant invariant scalar parsing)
│   └── Models/
│       ├── CatalogArtifactPlan.cs
│       ├── CatalogObjectAcePolicy.cs
│       ├── CatalogObjectArtifact.cs
│       ├── CatalogObjectIdPolicy.cs
│       ├── CatalogTableArtifact.cs
│       ├── CatalogEntry.cs
│       ├── CatalogRow.cs
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
│   ├── RowMapper.cs                       (object-array → POCO mapping and generic write projection)
│   ├── RowCriteriaEvaluator.cs            (compiles RowCriteria against a table, evaluates decoded rows)
│   ├── TypedValueParser.cs                (individual column type parsing)
│   ├── TypedRowFallbackPolicy.cs          (strict/lenient malformed-row fallback behavior)
│   ├── OleObjectDecoder.cs                (unwraps OLE envelopes, detects file signatures and data-URI formatting)
│   ├── LongValueDecoder.cs               (typed MEMO/OLE decode over LongValues)
│   ├── DirectRowDecoderBuilder.cs         (builds optimized row decode delegates)
│   └── Models/
│       ├── CalculatedLongValueRef.cs
│       ├── ColumnSlice.cs
│       ├── ColumnSliceKind.cs
│       └── LongValueRef.cs
│
├── Pages/                                 (page-level I/O & layout)
│   ├── DataPageLayout.cs                  (byte offsets, page structure, format-version layouts)
│   ├── DataPageInserter.cs               (FindInsertTarget, CanInsertRow, WriteRowToPage)
│   ├── PageAllocator.cs                   (global free-map reuse, freed-page scrubbing, tail shrink)
│   ├── UsageMap.cs                        (INLINE/REFERENCE usage-map parsing, bitmaps, pointer/row emission)
│   ├── PageJournal.cs                     (before-image journaling for rollback)
│   └── Models/
│       ├── PageInsertTarget.cs
│       ├── RowBound.cs
│       ├── RowLayout.cs
│       ├── RowLocation.cs
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
│   ├── AutoNumberMaintainer.cs            (advances the per-table AutoNumber high-water value after inserts)
│   ├── JetTypeInfo.cs                     (column type metadata — sizes, flags, CLR mapping)
│   ├── JetExpressionConverter.cs          (expression parsing for calculated columns)
│   ├── CalculatedColumnUtil.cs            (utility methods for calculated column handling)
│   ├── LinkedOdbcLvPropBuilder.cs         (generated linked-ODBC schema-cache property blocks)
│   ├── LogicalTDefChain.cs                (logical TDEF bytes spanning chained table-definition pages)
│   ├── Expressions/
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
│   │   ├── CalculatedExpressionNormalizer.cs       (Access syntax normalization: column brackets, date literals, word operators)
│   │   ├── CalculatedExpressionNumericFunctions.cs (numeric function catalog)
│   │   ├── CalculatedExpressionPlan.cs
│   │   ├── CalculatedExpressionTextFunctions.cs    (text function catalog)
│   │   ├── CalculatedExpressionUnaryNode.cs        (unary operator AST node)
│   │   ├── CalculatedExpressionUnsupportedNode.cs  (unsupported syntax AST sentinel)
│   │   ├── CalculatedExpressionValueNode.cs        (literal value AST node)
│   │   ├── CalculatedFunctionDescriptor.cs         (function alias, domain, and argument metadata)
│   │   ├── CalculatedFunctionDomain.cs             (calculated-function domain enum)
│   │   ├── CalculatedFunctionEvaluator.cs          (function evaluator delegate)
│   │   └── CalculatedFunctionInvocation.cs         (bound function invocation context)
│   └── Models/
│       ├── ColumnConstraint.cs
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
│   └── LinkedTableManager.cs              (Access/text/ODBC linked-table metadata and Access-file read-through)
│
├── ComplexColumns/                        (multi-value fields, attachments, versioned columns)
│   ├── ComplexColumnReader.cs             (complex-column metadata and flat-table read APIs)
│   ├── ComplexColumnManager.cs            (write/scaffold/cascade complex column data)
│   └── Models/
│       ├── AttachmentWrapper.cs
│       └── ComplexColumnAllocation.cs
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
│   ├── IncludableQueryable.cs             (adapts a composed query to IIncludableQueryable)
│   ├── IncludeLoader.cs                   (relationship-inferred eager loading and stitching)
│   ├── IncludeStep.cs                     (one Include/ThenInclude navigation plus inline operators)
│   ├── IncludeOperation.cs                (base for inline filtered/ordered/paged include operators)
│   ├── IncludeFilterOperation.cs          (Where applied to a parent's loaded children)
│   ├── IncludeOrderOperation.cs           (OrderBy/ThenBy applied to a parent's loaded children)
│   ├── IncludeSkipOperation.cs            (Skip applied to a parent's loaded children)
│   └── IncludeTakeOperation.cs            (Take applied to a parent's loaded children)
│
├── CompoundFile/                          (MS-CFB OLE structured storage)
│   ├── CompoundFileReader.cs              (read .accdb wrapped in CFB container)
│   └── CompoundFileWriter.cs              (write CFB container for Agile-encrypted output)
│
└── Infrastructure/                        (generic utilities — not JET-specific)
    ├── LruCache.cs                        (256-page least-recently-used eviction cache)
    ├── ByteArrayEqualityComparer.cs       (byte[] equality for dictionary keys)
    ├── BinaryBuffer.cs                    (byte-slice copy helpers)
    ├── BinaryStringParser.cs              (hex/base64 parsing helpers)
    ├── BoxCache.cs                        (interned boxes for low-cardinality fixed-width cell values)
    ├── DaoPowerShellHostResolver.cs       (test/probe DAO PowerShell host discovery)
    ├── FileStreamFactory.cs               (central FileStream construction helpers)
    ├── StreamReadExtensions.cs            (cross-target stream read helpers)
    ├── AsyncLazyInitializer.cs            (thread-safe async lazy initialization)
    ├── AsyncReentrantOperationGate.cs     (reentrant async operation serializer)
    └── Guard.cs                           (argument validation helpers)
```

---

## Architectural layers

The library follows a **Layered Codec / Service Architecture** — the dominant pattern in binary format libraries (protobuf, MessagePack-CSharp, Apache Parquet, SQLite, System.Text.Json), adapted for a file-format writer that needs a shared writer context while mutating pages. Four practical layers are visible:

| Layer | Folders | Responsibility |
|-------|---------|----------------|
| **Infrastructure** | `Infrastructure/`, `CompoundFile/` | Generic helpers, stream compatibility shims, CFB container parsing/writing |
| **Storage / Page Services** | `Pages/`, `Transactions/`, `Encryption/` | Page layouts, usage-map parsing/serialization, allocation/free-list reuse, journaling, locking, page encryption |
| **Codec / Domain Services** | `ValueEncoding/`, `ValueDecoding/`, `DelimitedText/`, `Indexes/`, `Catalog/`, `Schema/`, `Relationships/`, `ComplexColumns/`, `Queries/` | Encode/decode values, rows, index keys, and linked text records; read/write system tables; translate and run LINQ queries; manage feature-specific catalog artifacts |
| **API / Orchestration** | Root (`AccessReader`, `AccessWriter`, `AccessBase`), `Interfaces/`, public `Models/`, public `Enums/` | User-facing operations, options, DTOs, and orchestration |

The orchestration layer is intentionally thin — `AccessReader` and `AccessWriter` act as **facades** (GoF) that compose domain modules rather than embedding logic directly. Most pure layout/codec helpers keep one-way dependencies. Several writer-owned services (`DataPageInserter`, `PageAllocator`, `TDefPageBuilder`, relationship and complex-column managers) intentionally receive `AccessWriter` as a context object so they can coordinate page I/O, encryption, transactions, catalog caches, and format-specific layouts without duplicating state.

---

## Dependency graph

Dependency flow is acyclic. The current high-level dependency map is:

```
Infrastructure/        → (nothing — leaf)
CompoundFile/          → Infrastructure/
DelimitedText/         → (nothing — leaf parser/codec module)
Pages/                 → Infrastructure/, Catalog.Models/, AccessWriter context for writer-owned services
Encryption/            → CompoundFile/, Infrastructure/, Pages/
Transactions/          → Pages/, Infrastructure/, AccessWriter context for lifecycle orchestration
LongValues/            → Pages/
ValueDecoding/         → Schema/, Catalog.Models/, LongValues/, AccessBase/AccessReader context for row/text/LVAL decoding
ValueEncoding/         → Schema/, LongValues/, ValueEncoding.Models/   [never depends on ValueDecoding/]
Indexes/               → Pages/, ValueEncoding/ (key encoding), Schema/, AccessWriter context for maintenance
Catalog/               → Pages/, ValueDecoding/, Schema/, Indexes/, AccessWriter context for writes
Schema/                → Models/, Indexes/, Pages/, AccessWriter context for writer-owned builders
Relationships/         → Catalog/, DelimitedText/, Indexes/, Pages/, Schema/, AccessReader/AccessWriter context
ComplexColumns/        → Catalog/, Indexes/, Pages/, Schema/, ValueDecoding/, AccessReader/AccessWriter context
Queries/               → Indexes/, Models/, Enums/, Infrastructure/, AccessReader context
AccessBase (root)      → Pages/, Encryption/, Infrastructure/
AccessReader (root)    → ValueDecoding/, Catalog/, Indexes/, Pages/, ComplexColumns/, Relationships/, Queries/
AccessWriter (root)    → ValueEncoding/, Catalog/, Indexes/, Transactions/, Schema/,
                         LongValues/, Relationships/, ComplexColumns/, Pages/, Encryption/
```

No project-level circular dependencies exist. `Infrastructure/` and pure layout/value helpers remain stable dependencies. Some storage, schema, and decode service classes are not leaf packages; they are context-owned collaborators scoped by domain, so they may receive `AccessReader`, `AccessWriter`, or `AccessBase` context when they need coordinated page I/O, text decoding, LVAL resolution, transactions, or format-specific layouts.

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
| `Queries/` | `JetDatabaseWriter.Queries` |
| `CompoundFile/` | `JetDatabaseWriter.CompoundFile` |
| `Infrastructure/` | `JetDatabaseWriter.Infrastructure` |

Public API types live at the root namespace (`JetDatabaseWriter`) — no sub-namespace required for consumers to access the main entry points.

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
| **Facade** (GoF) | `AccessReader`, `AccessWriter` | Thin orchestrators that delegate to domain modules; keeps public API surface small |
| **Symmetric Codec** | `ValueEncoding/` ↔ `ValueDecoding/`, `LongValueEncoder` ↔ `LongValueDecoder` | Matched encode/decode pairs (same pattern as protobuf's `CodedOutputStream`/`CodedInputStream`) |
| **Shared Storage Codec** | `LongValues/LongValueStore`, `LongValueDescriptor` | Centralizes LVAL descriptor parsing, page-buffer emission, chain traversal, and secure-erase page reclamation |
| **Builder** | `TDefPageBuilder`, `IndexBTreeBuilder`, `ColumnPropertyBlockBuilder`, `DirectRowDecoderBuilder` | Constructs complex page buffers incrementally |
| **Cursor / Editor** | `IndexCursor`, `IndexBTreeEditor`, `IndexPageCodec` | Keeps read-only B-tree descent and in-place mutation planning separate from TDEF/catalog orchestration |
| **Strategy via layout structs** | `DataPageLayout`, `IndexLayout`, `IndexPageLayout` | Format-version polymorphism (Jet3 vs Jet4 vs ACE) without virtual dispatch; cache-friendly |
| **Pager** | `AccessBase` + `LruCache` + `PageJournal` | Dedicated page-level I/O with 256-page LRU eviction cache and before-image journaling (same pattern as SQLite's pager) |
| **Allocator** | `PageAllocator` | Centralizes Access global free-map reuse, freed-page headers, secure erase, and tail-only shrink |
| **Usage Map Codec** | `UsageMap` | Centralizes INLINE/REFERENCE ownership and free-map row parsing, bitmap traversal, bit mutation, pointer emission, and inline row serialization |
| **Row Decode Plan** | `RowDecodePlan` | Centralizes row-layout preflight, projection masks, string-row materialization, typed fixed/variable slice decoding, direct-decoder slice resolution, calculated payload handling, and partial key-column reads |
| **Manager / Coordinator** | `RelationshipManager`, `LinkedTableManager`, `ComplexColumnManager`, `ComplexColumnReader` | Keeps feature-specific catalog and child-table workflows out of the public facades |
| **Catalog Store** | `RelationshipCatalogStore` | Keeps MSysRelationships row emission/loading/rewrites separate from TDEF logical-index mutation |
| **Runtime Enforcer** | `RelationshipEnforcer` | Keeps FK insert/update/delete referential-integrity checks separate from create/drop/rename workflows |
| **Streaming Parser** | `DelimitedTextReader` | Parses linked CSV/delimited text records one at a time with bounded memory, quote handling, and line tracking |
| **Planner / Locator** | `RelationshipSeekPlanner`, `RelationshipChildRowLocator` | Separates index-backed lookup planning and row-location resolution from FK fallback/enforcement workflow |
| **Policy** | `TypedRowFallbackPolicy`, `RelationshipCascadePolicy` | Encapsulates strict vs lenient malformed-row handling and FK cascade recursion limits |
| **Gateway** (Fowler) | `LockFileCoordinator`, `JetByteRangeLock` | Encapsulates filesystem concurrency primitives behind a clean interface |
| **Registry** | `ConstraintRegistry`, `CalculatedExpressionFunctionRegistry` | Centralized constraint management and calculated-expression function dispatch — decoupled from the writer orchestrator and evaluator entry point |
| **Query Provider / Pipeline** | `AccessQueryProvider`, `AccessQueryTranslator`, `QueryStage` subclasses | LINQ `IQueryable` provider that translates an expression tree into an ordered stage pipeline, pushes a leading filter run into index inference, and replays the unsupported tail in memory |
| **Specification** | `RowCriteria`, `ColumnPredicate`, `RowCriteriaEvaluator` | Named-column predicate objects expressing writer update/delete filters independently of how they compile and evaluate against decoded rows |
| **Query Planner** | `IndexPlanner`, `IndexPredicateTranslator`, `IndexPlan` | Chooses the best index for a predicate and builds a sound (superset) seek, leaving a residual client-side filter to enforce exactness |

---

## Design principles applied

### SOLID

| Principle | How applied |
|-----------|-------------|
| **Single Responsibility (SRP)** | Each file/class owns one concern. `RowEncoder` only serializes rows; `UsageMap` only parses/emits usage-map rows and bits; `DataPageInserter` only manages page insertion; `TransactionLifecycle` only handles begin/commit/rollback |
| **Open/Closed (OCP)** | Adding a new column type means extending `TypedValueParser`, `RowEncoder`, and type metadata helpers — not modifying the orchestrator |
| **Interface Segregation (ISP)** | `IAccessReader`, `IAccessSchema` (DDL), and `IAccessWriter` (DML) are separated; consumers depend only on what they use |
| **Dependency Inversion (DIP)** | Orchestrators depend on domain modules via composition; codec logic is delegated, not embedded |

### Package design principles (Robert C. Martin)

| Principle | How applied |
|-----------|-------------|
| **Common Closure (CCP)** | Classes that change together live together. All index concerns in `Indexes/`; all encryption in `Encryption/` |
| **Common Reuse (CRP)** | Classes used together live together. `CatalogEntry`, `CatalogRow`, `TableDef` always consumed as a group → `Catalog/Models/` |
| **Acyclic Dependencies (ADP)** | The dependency graph is kept cycle-free; writer-owned services compose through `AccessWriter` rather than through cross-domain back-references |
| **Stable Dependencies (SDP)** | Pure helpers (`Infrastructure/`, layout structs, codec primitives) stay stable. Writer-owned services can depend on the facade context when they need coordinated state. |

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

### Naming to avoid BCL shadowing

- **`ValueEncoding/`** not `Encoding/` — avoids shadowing `System.Text.Encoding`
- **`ValueDecoding/`** not `Decoding/` — symmetric with `ValueEncoding/`
- **`Collation/`** not `TextEncoding/` — avoids confusion with character encoding

---

## Key architectural decisions

### 1. Thin orchestrators over god classes

`AccessReader` and `AccessWriter` are **facades** — they compose and delegate to domain modules (`RowEncoder`, `LongValueEncoder`, `LongValueStore`, `DataPageInserter`, `TransactionLifecycle`, `CatalogWriter`, `UniqueIndexChecker`, `ConstraintRegistry`, `LongValueDecoder`). The orchestrators own the workflow; the domain modules own the logic.

### 2. ValueEncoding and ValueDecoding share neutral format domains

These are symmetric but independent. Shared types live in neutral domains such as `Schema/` (`ColumnInfo`, `JetTypeInfo`) and `LongValues/` (`LongValueDescriptor`, `LongValueStore`) so the read path and write path do not depend on each other's implementation folders.

`RowDecodePlan` is the read-side row decode coordinator. It is built from `TableDef`, an optional projection mask or partial-column ordinal list, and strictness requirements. The plan parses/preflights row layout once, resolves per-column slices through `AccessBase`, materializes `RowsAsStrings()` rows, decodes typed fixed/variable values, emits async LVAL sentinels for `AccessReader` to resolve, supplies row-layout and slice resolution to the direct POCO expression-tree decoder, and serves the writer's index/FK partial key-column reader.

### 3. Models co-located with their domain

Internal DTOs live in `{Domain}/Models/` subdirectories. This satisfies CRP — you never need to import a grab-bag `Models/` namespace to get one type; you import the specific domain's models.

### 4. Public and domain DTOs get their own files

Public API types and domain DTOs get their own files. Reusable shapes that are consumed across files are top-level internal types in their domain's folder, including the extracted index layout records, row-layout primitives, column slices, long-value references, property-block builders, numeric payloads, usage-map pointers, and complex-column allocations. Small implementation details that are private or tightly coupled to one algorithm may remain nested inside that algorithm's file.

### 5. Embedded resources follow their consumer

The `CodeTables/` directory (gzipped collation lookup data) lives under `Indexes/` alongside the `Collation/` encoders that consume it — not in a generic resources folder.

### 6. Catalog row parsing stays with catalog ownership

`CatalogValueReader` lives in `Catalog/` because it handles tolerant scalar reads from system-table rows (`MSysObjects`, `MSysRelationships`, `MSysComplexColumns`, etc.): safe `string[]` cell access, missing-column defaults, and invariant integer parsing of catalog metadata. It is not a general user-value parser. User table column values continue to flow through `ValueDecoding/TypedValueParser`, and write-path values through `ValueEncoding/`.

### 7. Writer-owned services stay in their domain

Classes such as `PageAllocator`, `DataPageInserter`, `TDefPageBuilder`, `RelationshipManager`, and `ComplexColumnManager` live beside the disk-format concern they manipulate, even when they receive `AccessWriter` as a context object. This keeps the folder structure domain-first while avoiding a large writer god class.

### 8. Usage-map parsing stays with page ownership

`UsageMap` lives in `Pages/` because INLINE and REFERENCE usage-map rows are page-layout structures, not reader-only or writer-only behavior. It owns pointer reads/writes, row-bound lookup, bitmap traversal, point bit checks and mutation, and inline row serialization. Callers keep policy: `AccessReader` validates mapped owned data pages before taking the fast path; `DataPageInserter` marks table owned/free rows; `PageAllocator` decides when to promote the global free map and allocate reference pages; `AccessWriter` and `IndexMaintainer` decide which index pages to emit or reclaim.

### 9. Linked-table metadata spans catalog, schema, and delimited text parsing

Linked-table public APIs live on `IAccessSchema`; linked-table discovery and read-through live in `Relationships/LinkedTableManager`; ODBC schema-cache property-map generation lives in `Schema/LinkedOdbcLvPropBuilder` because it emits `MSysObjects.LvProp` property blocks using the shared schema property-map builder. Text/CSV linked-table read-through delegates record parsing to `DelimitedText/`, keeping separator, quote, line-ending, header-normalization, and parser-limit behavior reusable outside the linked-table manager.

### 10. Relationship catalog and runtime helpers are split from lifecycle orchestration

`RelationshipManager` owns relationship create/drop/rename workflow and per-TDEF FK logical-index mutation. `RelationshipCatalogStore` owns `MSysRelationships` row emission, loading, and rewrites, while `RelationshipEnforcer` owns insert/update/delete referential-integrity checks. The runtime path uses smaller helpers for reusable policy and lookup work: `RelationshipSeekPlanner` resolves parent/child B-tree seek indexes, `RelationshipChildRowLocator` turns child-side seek hits into live `RowLocation` values, `RelationshipKeyBuilder` keeps seek and snapshot fallback key semantics aligned, and `RelationshipCascadePolicy` owns the cascade-depth guard independently of catalog mutation setup.

### 11. Calculated expressions use explicit helper ownership

`CalculatedExpressionEvaluator` remains the row-local entry point for applying calculated-column expressions, but parsing, normalization, AST nodes, coercion, safety limits, and function dispatch are split into focused internal helpers. Supported Access/VBA functions are registered through `CalculatedExpressionFunctionRegistry` using descriptors for aliases, argument counts, domains, and evaluator delegates; the implementation lives in domain catalogs such as `CalculatedExpressionTextFunctions` and `CalculatedExpressionDateTimeFunctions`. Spreadsheet-only constructs, external references, and domain aggregates stay rejected at the parser/evaluator boundary instead of leaking into row evaluation.

### 12. The LINQ query layer is read-only and degrades gracefully

`Queries/` adds an `IQueryable<T>` over a single table (`AccessReader.Query<T>`). `AccessQueryProvider` translates only the operators it can run natively against the storage engine — a leading run of `Where` filters (AND-combined and pushed into index inference by `IndexPredicateTranslator`/`IndexPlanner`), `OrderBy`/`ThenBy`, `Skip`, and `Take` — into an ordered `QueryStage` pipeline that honors written order. `AccessQueryTranslator` marks the engine boundary at the first unsupported operator (notably `Select` projections): the prefix runs in the engine and the tail replays in memory through LINQ-to-Objects. Relationship-inferred eager loading (`Include`/`ThenInclude`, including filtered/ordered/paged collection includes) is a post-materialization step driven by the `MSysRelationships` catalog. Index selection is intentionally sound-but-not-exact — a seek can return a superset — so the compiled residual predicate is always reapplied to every row the seek yields.

---

## Public API surface

The public entry points are:

| Type | Purpose |
|------|---------|
| `AccessReader` | Open and read .mdb/.accdb files — stream rows, materialize DataTables/POCOs, LINQ `Query<T>` with relationship-inferred eager loading, exact index seek, read schema, relationship metadata, linked-table metadata/read-through, complex-column metadata/items |
| `AccessWriter` | Create/open/write .mdb/.accdb files — CRUD with named-column `RowValues`/`RowCriteria` filters, DDL, linked-table catalog rows, relationships, complex-column row APIs, transactions, storage maintenance, encryption conversion helpers |
| `AccessReaderOptions` | Reader configuration: page cache, validation, strict parsing, password, lock-file/byte-range locking, linked-source path policy, linked-text limits |
| `AccessWriterOptions` | Writer configuration: password, full catalog schema, lock-file/byte-range locking, transaction page budget, secure erase, implicit transactional writes |
| `JetTransaction` | Disposable transaction handle returned by `BeginTransactionAsync` |
| `AccessQueryExtensions` | LINQ extensions for `Query<T>` results — relationship-inferred `Include`/`ThenInclude` eager loading and async terminal operators |
| `IIncludableQueryable<TEntity, TProperty>` | Marker returned by `Include`/`ThenInclude` so a chain can carry the most recently included navigation type |
| `Models/*` | Public DTOs for column definitions, index metadata, relationships, etc. |
| `Enums/*` | Public enumerations (database format, encryption format, linked-table kind, secure erase mode, etc.) |
| `Exceptions/*` | Domain-specific exceptions |
| `Interfaces/*` | Abstractions for DI/testing (`IAccessReader`, `IAccessWriter`, `IAccessSchema`, etc.) |

All other types are `internal` — implementation details organized by domain.
