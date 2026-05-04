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
├── JetTransaction.cs
├── Constants.cs                           (format constants, magic numbers, page offsets)
├── IsExternalInit.cs                      (compiler shim for init-only properties)
│
├── Interfaces/
│   ├── IAccessBase.cs
│   ├── IAccessOptions.cs
│   ├── IAccessReader.cs
│   ├── IAccessSchema.cs                   (DDL: CreateTable, AddColumn, CreateIndex, etc.)
│   └── IAccessWriter.cs                   (DML: Insert, Update, Delete — extends IAccessSchema)
│
├── Models/                                (public DTOs — one per file)
│   ├── AttachmentInput.cs
│   ├── AttachmentRecord.cs
│   ├── ColumnDefinition.cs
│   ├── ColumnMetadata.cs
│   ├── ColumnSize.cs
│   ├── ComplexColumnInfo.cs
│   ├── DatabaseStatistics.cs
│   ├── Hyperlink.cs
│   ├── IndexColumnReference.cs
│   ├── IndexDefinition.cs
│   ├── IndexMetadata.cs
│   ├── LinkedTableInfo.cs
│   ├── RelationshipDefinition.cs
│   ├── TableProgress.cs
│   └── TableStat.cs
│
├── Enums/
│   ├── AccessEncryptionFormat.cs
│   ├── ColumnSizeUnit.cs
│   ├── ComplexColumnKind.cs
│   ├── DatabaseFormat.cs
│   ├── IndexKind.cs
│   ├── IntermediateOpType.cs
│   └── TdefPreambleStatus.cs
│
├── Exceptions/
│   └── JetLimitationException.cs
│
├── Catalog/                               (system-table reading/writing)
│   ├── CatalogWriter.cs                   (InsertCatalogEntry, RewriteTable, RenameInCatalog)
│   └── Models/
│       ├── CatalogEntry.cs
│       ├── CatalogRow.cs
│       └── TableDef.cs
│
├── ValueEncoding/                         (write-path: typed values → bytes)
│   ├── RowEncoder.cs                      (SerializeRow, EncodeFixed/Variable/Text/Binary)
│   ├── LongValueEncoder.cs               (LVAL chain allocation, OLE wrapping)
│   ├── NumericEncoder.cs                  (BCD decimal encoding)
│   └── Models/
│       ├── LvalChainResult.cs
│       └── PreEncodedLongValue.cs
│
├── ValueDecoding/                         (read-path: bytes → typed values)
│   ├── RowMapper.cs                       (column dispatch — routes to correct decoder)
│   ├── TypedValueParser.cs                (individual column type parsing)
│   ├── LongValueDecoder.cs               (LVAL chain reading)
│   └── DirectRowDecoderBuilder.cs         (builds optimized row decode delegates)
│
├── Pages/                                 (page-level I/O & layout)
│   ├── DataPageLayout.cs                  (byte offsets, page structure, format-version layouts)
│   ├── DataPageInserter.cs               (FindInsertTarget, CanInsertRow, WriteRowToPage)
│   ├── PageJournal.cs                     (before-image journaling for rollback)
│   └── Models/
│       ├── PageInsertTarget.cs
│       └── RowLocation.cs
│
├── Indexes/                               (all index concerns — B-tree, key encoding, maintenance)
│   ├── IndexKeyEncoder.cs                 (column values → sort key bytes)
│   ├── IndexBTreeBuilder.cs               (constructs index B-tree pages)
│   ├── IndexBTreeSeeker.cs                (navigates index B-tree for lookups)
│   ├── IndexCatalogReader.cs              (reads index definitions from system tables)
│   ├── IndexLeafIncremental.cs            (incremental leaf-page writes)
│   ├── IndexLeafPageBuilder.cs            (constructs leaf pages)
│   ├── IndexMaintainer.cs                 (insert/delete/update index entries)
│   ├── IndexLayout.cs                     (index page byte-offset structs)
│   ├── UniqueIndexChecker.cs              (validates uniqueness constraints)
│   ├── Helpers/
│   │   └── IndexHelpers.cs
│   ├── Collation/                         (sort-key generation for text indexes)
│   │   ├── GeneralTextIndexEncoder.cs
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
│       ├── IndexEntry.cs
│       ├── ResolvedIndex.cs
│       ├── DescentStep.cs
│       ├── ChildSeekIndex.cs
│       ├── ParentSeekIndex.cs
│       ├── DecodedIntermediateEntry.cs
│       ├── IntermediateOp.cs
│       └── SplitPages.cs
│
├── Schema/                                (DDL: table/column/index definition & type metadata)
│   ├── TDefPageBuilder.cs                 (constructs Table Definition pages)
│   ├── ColumnPropertyBlockBuilder.cs      (builds column property blocks)
│   ├── ConstraintRegistry.cs              (manages column constraints — auto-increment, defaults, validation)
│   ├── JetTypeInfo.cs                     (column type metadata — sizes, flags, CLR mapping)
│   ├── JetExpressionConverter.cs          (expression parsing for calculated columns)
│   ├── CalculatedColumnUtil.cs            (utility methods for calculated column handling)
│   └── Models/
│       ├── ColumnConstraint.cs
│       ├── ColumnInfo.cs
│       ├── ColumnPropertyBlock.cs
│       ├── ColumnPropertyEntry.cs
│       ├── ColumnPropertyTarget.cs
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
│   └── Models/
│       └── PageDecryptionKeys.cs
│
├── Relationships/                         (foreign keys, cascade rules, linked tables)
│   ├── RelationshipManager.cs             (create/drop/rename relationships, cascade logic)
│   └── LinkedTableManager.cs              (ODBC and Access-linked table operations)
│
├── ComplexColumns/                        (multi-value fields, attachments, versioned columns)
│   ├── ComplexColumnManager.cs            (read/write complex column data)
│   └── Models/
│       └── AttachmentWrapper.cs
│
├── CompoundFile/                          (MS-CFB OLE structured storage)
│   ├── CompoundFileReader.cs              (read .accdb wrapped in CFB container)
│   └── CompoundFileWriter.cs              (write CFB container for Agile-encrypted output)
│
└── Infrastructure/                        (generic utilities — not JET-specific)
    ├── LruCache.cs                        (256-page least-recently-used eviction cache)
    ├── ByteArrayEqualityComparer.cs       (byte[] equality for dictionary keys)
    ├── AsyncLazyInitializer.cs            (thread-safe async lazy initialization)
    ├── AsyncReentrantOperationGate.cs     (reentrant async operation serializer)
    ├── Guard.cs                           (argument validation helpers)
    └── NonClosingStreamWrapper.cs         (stream wrapper that suppresses Dispose)
```

---

## Architectural layers

The library follows a **Layered Codec Architecture** — the dominant pattern in binary format libraries (protobuf, MessagePack-CSharp, Apache Parquet, SQLite, System.Text.Json). Three distinct layers with strict dependency direction:

| Layer | Folders | Responsibility |
|-------|---------|----------------|
| **Infrastructure** | `Infrastructure/`, `Pages/`, `CompoundFile/` | Raw I/O, page buffering, caching, encryption, OLE container |
| **Codec** | `ValueEncoding/`, `ValueDecoding/`, `Indexes/`, `Catalog/`, `Schema/` | Encode/decode values, rows, index keys; read/write system tables |
| **API / Orchestration** | Root (`AccessReader`, `AccessWriter`, `AccessBase`) | User-facing operations, delegates to domain modules |

Each layer depends only on the one below it. The orchestration layer is intentionally thin — `AccessReader` and `AccessWriter` act as **facades** (GoF) that compose domain modules rather than embedding logic directly.

---

## Dependency graph

Dependency flow is acyclic. Each domain folder depends only on peers at the same or lower layer:

```
Infrastructure/        → (nothing — leaf)
Pages/                 → Infrastructure/
Encryption/            → Pages/
CompoundFile/          → (nothing — leaf)
ValueDecoding/         → Pages/, Schema/
ValueEncoding/         → Schema/                        [never depends on ValueDecoding/]
Indexes/               → Pages/, ValueEncoding/ (key encoding), Schema/
Catalog/               → Pages/, ValueDecoding/, Schema/
Transactions/          → Pages/
Relationships/         → Catalog/, Indexes/
ComplexColumns/        → Catalog/, Pages/
Schema/                → (nothing — leaf, defines types used by others)
AccessBase (root)      → Pages/, Encryption/, Infrastructure/
AccessReader (root)    → ValueDecoding/, Catalog/, Indexes/, Pages/
AccessWriter (root)    → ValueEncoding/, Catalog/, Indexes/, Transactions/, Schema/,
                         Relationships/, ComplexColumns/
```

No circular dependencies exist. The **Stable Dependencies Principle** is maintained: `Infrastructure/` and `Pages/` (rarely changing) are depended upon by volatile subsystems like `ValueEncoding/` and `Schema/`.

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
| `ValueEncoding/` | `JetDatabaseWriter.ValueEncoding` |
| `ValueEncoding/Models/` | `JetDatabaseWriter.ValueEncoding.Models` |
| `ValueDecoding/` | `JetDatabaseWriter.ValueDecoding` |
| `Pages/` | `JetDatabaseWriter.Pages` |
| `Pages/Models/` | `JetDatabaseWriter.Pages.Models` |
| `Indexes/` | `JetDatabaseWriter.Indexes` |
| `Indexes/Helpers/` | `JetDatabaseWriter.Indexes.Helpers` |
| `Indexes/Collation/` | `JetDatabaseWriter.Indexes.Collation` |
| `Indexes/Models/` | `JetDatabaseWriter.Indexes.Models` |
| `Schema/` | `JetDatabaseWriter.Schema` |
| `Schema/Models/` | `JetDatabaseWriter.Schema.Models` |
| `Transactions/` | `JetDatabaseWriter.Transactions` |
| `Encryption/` | `JetDatabaseWriter.Encryption` |
| `Encryption/Models/` | `JetDatabaseWriter.Encryption.Models` |
| `Relationships/` | `JetDatabaseWriter.Relationships` |
| `ComplexColumns/` | `JetDatabaseWriter.ComplexColumns` |
| `ComplexColumns/Models/` | `JetDatabaseWriter.ComplexColumns.Models` |
| `CompoundFile/` | `JetDatabaseWriter.CompoundFile` |
| `Infrastructure/` | `JetDatabaseWriter.Infrastructure` |

Public API types live at the root namespace (`JetDatabaseWriter`) — no sub-namespace required for consumers to access the main entry points.

---

## Interface hierarchy

The public interface uses **Interface Segregation** (ISP) to separate concerns:

```
IAccessBase          (stream lifecycle, format detection, page I/O)
    ↓
IAccessSchema        (DDL: CreateTable, DropTable, AddColumn, DropColumn, RenameColumn,
                      CreateLinkedTable, CreateRelationship, etc.)
    ↓
IAccessWriter        (DML: InsertRow, InsertRows, UpdateRows, DeleteRows,
                      AddAttachment, AddMultiValueItem)
```

`IAccessReader` is a separate branch extending `IAccessBase`. This split follows ADO.NET precedent — consumers that only need schema management depend on `IAccessSchema` without pulling in DML operations.

---

## Design patterns in use

| Pattern | Where applied | Rationale |
|---------|--------------|-----------|
| **Facade** (GoF) | `AccessReader`, `AccessWriter` | Thin orchestrators that delegate to domain modules; keeps public API surface small |
| **Symmetric Codec** | `ValueEncoding/` ↔ `ValueDecoding/`, `LongValueEncoder` ↔ `LongValueDecoder` | Matched encode/decode pairs (same pattern as protobuf's `CodedOutputStream`/`CodedInputStream`) |
| **Builder** | `TDefPageBuilder`, `IndexBTreeBuilder`, `IndexLeafPageBuilder`, `ColumnPropertyBlockBuilder`, `DirectRowDecoderBuilder` | Constructs complex page buffers incrementally |
| **Strategy via layout structs** | `DataPageLayout`, `IndexLayout` | Format-version polymorphism (Jet3 vs Jet4 vs ACE) without virtual dispatch; cache-friendly |
| **Pager** | `AccessBase` + `LruCache` + `PageJournal` | Dedicated page-level I/O with 256-page LRU eviction cache and before-image journaling (same pattern as SQLite's pager) |
| **Gateway** (Fowler) | `LockFileCoordinator`, `JetByteRangeLock` | Encapsulates filesystem concurrency primitives behind a clean interface |
| **Registry** | `ConstraintRegistry` | Centralized constraint management — auto-increment, defaults, validation rules — decoupled from the writer orchestrator |

---

## Design principles applied

### SOLID

| Principle | How applied |
|-----------|-------------|
| **Single Responsibility (SRP)** | Each file/class owns one concern. `RowEncoder` only serializes rows; `DataPageInserter` only manages page insertion; `TransactionLifecycle` only handles begin/commit/rollback |
| **Open/Closed (OCP)** | Adding a new column type means extending `TypedValueParser` and `RowEncoder` — not modifying the orchestrator |
| **Interface Segregation (ISP)** | `IAccessSchema` (DDL) separated from `IAccessWriter` (DML); consumers depend only on what they use |
| **Dependency Inversion (DIP)** | Orchestrators depend on domain modules via composition; codec logic is delegated, not embedded |

### Package design principles (Robert C. Martin)

| Principle | How applied |
|-----------|-------------|
| **Common Closure (CCP)** | Classes that change together live together. All index concerns in `Indexes/`; all encryption in `Encryption/` |
| **Common Reuse (CRP)** | Classes used together live together. `CatalogEntry`, `CatalogRow`, `TableDef` always consumed as a group → `Catalog/Models/` |
| **Acyclic Dependencies (ADP)** | The layered structure guarantees no cycles. `Infrastructure/` → `Pages/` → codecs → orchestrators |
| **Stable Dependencies (SDP)** | Volatile packages (`ValueEncoding/`, `Schema/`) depend on stable ones (`Infrastructure/`, `Pages/`) — never the reverse |

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

`AccessReader` and `AccessWriter` are **facades** — they compose and delegate to domain modules (`RowEncoder`, `LongValueEncoder`, `DataPageInserter`, `TransactionLifecycle`, `CatalogWriter`, `UniqueIndexChecker`, `ConstraintRegistry`, `LongValueDecoder`). The orchestrators own the workflow; the domain modules own the logic.

### 2. ValueEncoding and ValueDecoding never depend on each other

These are symmetric but independent. Shared types (like `ColumnInfo`, `JetTypeInfo`) live in `Schema/` which both can depend on. This prevents coupling the read path to the write path.

### 3. Models co-located with their domain

Internal DTOs live in `{Domain}/Models/` subdirectories. This satisfies CRP — you never need to import a grab-bag `Models/` namespace to get one type; you import the specific domain's models.

### 4. One class per file, nested types promoted

Every type gets its own file. Previously-nested types (`ColumnConstraint`, `PreEncodedLongValue`, `PageDecryptionKeys`) are promoted to top-level internal types in their domain's folder. This improves discoverability and follows SRP.

### 5. Embedded resources follow their consumer

The `CodeTables/` directory (gzipped collation lookup data) lives under `Indexes/` alongside the `Collation/` encoders that consume it — not in a generic resources folder.

---

## Public API surface

The public entry points are:

| Type | Purpose |
|------|---------|
| `AccessReader` | Open and read .mdb/.accdb files — stream rows, query by index, read schema |
| `AccessWriter` | Open and write .mdb/.accdb files — CRUD operations, DDL, transactions |
| `AccessReaderOptions` | Configuration for the reader (encoding, buffer size, etc.) |
| `AccessWriterOptions` | Configuration for the writer (encryption, format, etc.) |
| `JetTransaction` | Disposable transaction handle returned by `BeginTransactionAsync` |
| `Models/*` | Public DTOs for column definitions, index metadata, relationships, etc. |
| `Enums/*` | Public enumerations (database format, encryption format, column types, etc.) |
| `Exceptions/*` | Domain-specific exceptions |
| `Interfaces/*` | Abstractions for DI/testing (`IAccessReader`, `IAccessWriter`, `IAccessSchema`, etc.) |

All other types are `internal` — implementation details organized by domain.
