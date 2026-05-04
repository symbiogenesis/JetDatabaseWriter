# Library structure critique & reorganization plan

## Current file/class structure

```
JetDatabaseWriter/
├── Constants.cs                           (663 lines, ~28 nested static classes)
├── IsExternalInit.cs                      (6 lines, shim)
│
├── Core/
│   ├── AccessBase.cs                      (1041 lines — shared I/O, page read, catalog)
│   ├── AccessReader.cs                    (3513 lines — row decoding, streaming, LVAL read)
│   ├── AccessWriter.cs                    (4312 lines — row encoding, CRUD, schema DDL, txn, LVAL write)
│   ├── AccessReaderOptions.cs
│   ├── AccessWriterOptions.cs
│   ├── JetTransaction.cs
│   └── Interfaces/
│       ├── IAccessBase.cs
│       ├── IAccessOptions.cs
│       ├── IAccessReader.cs
│       └── IAccessWriter.cs
│
├── Enums/                                 (7 files — one enum each)
├── Exceptions/                            (1 file)
├── Models/                                (15 files — public DTOs, one per file ✓)
│
└── Internal/
    ├── IndexMaintainer.cs                 (2515 lines)
    ├── ComplexColumnManager.cs            (1344 lines)
    ├── EncryptionManager.cs               (833 lines)
    ├── IndexHelpers.cs (in Helpers/)      (767 lines)
    ├── GeneralLegacyTextIndexEncoder.cs   (620 lines, 12 nested types)
    ├── OfficeCryptoAgile.cs               (617 lines)
    ├── IndexKeyEncoder.cs                 (586 lines)
    ├── IndexBTreeSeeker.cs                (558 lines)
    ├── JetTypeInfo.cs                     (527 lines)
    ├── IndexLeafIncremental.cs            (520 lines)
    ├── IndexLayout.cs                     (456 lines, struct-only layout file)
    ├── ... (+ 30 more files)
    ├── Builders/                          (5 files)
    ├── Collections/                       (2 files)
    ├── Helpers/                           (7 files)
    ├── Models/                            (21 files)
    ├── Relationships/                     (1 file — 2479 lines)
    └── Transactions/                      (1 file — 129 lines)
```

### Namespace mapping

Everything in `Internal/` uses `JetDatabaseWriter.Internal` (flat), regardless of subfolder — except `Builders`, `Collections`, `Helpers`, `Models`, `Relationships`, `Transactions` which use their own sub-namespace.

---

## Critique

### 1. The "god class" problem is symptomatic of missing architectural layers

Binary format libraries (like `System.Text.Json`, `MessagePack`, `protobuf-net`, Apache Arrow, Parquet.NET) almost universally separate these concerns into distinct layers:

| Layer | Responsibility | Current home |
|-------|---------------|--------------|
| **Format/Layout** | Byte offsets, page structures, magic numbers | `Internal/DataPageLayout`, `IndexLayout`, `Constants` ✓ |
| **Low-level I/O** | Page read/write, buffering, encryption | Split across `AccessBase` + `EncryptionManager` ⚠️ |
| **Decoding (Read)** | Raw bytes → typed values, row deserialization | Monolithic in `AccessReader` ✗ |
| **Encoding (Write)** | Typed values → raw bytes, row serialization | Monolithic in `AccessWriter` ✗ |
| **Catalog/Schema** | System table parsing, table-def interpretation | Split across `AccessBase` + `AccessWriter` ✗ |
| **Index** | B-tree navigation, key encoding, maintenance | Good separation (`IndexBTreeSeeker`, `IndexKeyEncoder`, `IndexMaintainer`, etc.) ✓ |
| **Transaction/Concurrency** | Journal, locking, commit/rollback | Partially split (`PageJournal`, `LockFileCoordinator`) ⚠️ |
| **Public API / Orchestration** | User-facing CRUD + streaming | `AccessReader`, `AccessWriter` ✗ |

The extraction plan (in `access-writer-extraction-plan.md`) addresses the symptoms (big file → smaller files) but doesn't fix the structural problem: **`AccessReader` and `AccessWriter` conflate orchestration with encoding/decoding logic.** Extracting 200 lines of LVAL encoding into `LongValueEncoder.cs` helps, but you end up with a flat bag of 40+ files in `Internal/` that have no navigable relationship to each other.

### 2. The `Internal/` folder is a dumping ground

Currently `Internal/` has:
- Index concerns: 9 files (BTree, Leaf, Key, Layout, Catalog, Helpers, Maintainer, Seeker, Incremental)
- Encryption: 3 files
- Compound file: 2 files
- Everything else: loosely related

The only subfolder discipline is `Builders/`, `Models/`, `Helpers/` — which are *role-based* groupings (what kind of thing) rather than *domain-based* (what subsystem does it belong to). This makes discovery hard.

### 3. Namespaces don't match folders consistently

`Internal/Builders/IndexBTreeBuilder.cs` is in `JetDatabaseWriter.Internal.Builders`, but `Internal/IndexBTreeSeeker.cs` is in `JetDatabaseWriter.Internal`. Both are index B-tree concerns. A consumer of `InternalsVisibleTo` sees them in different namespaces despite being the same subsystem.

### 4. The `Core/` folder is misleading

In .NET convention, `Core` usually means "the kernel of a framework". Here it just means "the 3 public entry-point classes." Since these are the public API surface, they belong at the **root namespace** level, not hidden in a sub-namespace that users must `using JetDatabaseWriter.Core` to reach.

### 5. The extraction plan creates more flat files without structure

The plan proposes: `RowEncoder.cs`, `DataPageInserter.cs`, `TransactionLifecycle.cs`, `UniqueIndexChecker.cs`, `CatalogAccess.cs`, `ConstraintRegistry.cs`, `LongValueEncoder.cs` — all dumped into `Internal/`. After completion you'd have ~45 files in `Internal/` with no grouping principle.

---

## Applicable patterns, principles, and standards

### Architectural patterns

#### 1. Layered Codec Architecture

The dominant pattern in binary format libraries (protobuf, MessagePack-CSharp, Apache Parquet, System.Text.Json, SQLite). Three distinct layers:

| Layer | Role | Example in SQLite |
|-------|------|-------------------|
| **Wire/Page layer** | Raw I/O, buffering, encryption | Pager |
| **Codec layer** | Encode/decode individual values and rows | B-tree cell serialization |
| **API/Orchestration layer** | User-facing operations, query planning | SQL VM |

Each layer depends only on the one below it. Our `AccessWriter` currently spans all three.

#### 2. Symmetric Codec Pattern

Reader and writer are **mirror images** with matched type pairs:

- `RowEncoder` ↔ `RowDecoder`
- `LongValueEncoder` ↔ `LongValueDecoder`
- `CompoundFileWriter` ↔ `CompoundFileReader`

This is how `System.Text.Json` (`Utf8JsonWriter` / `Utf8JsonReader`), `MessagePack` (`MessagePackWriter` / `MessagePackReader`), and `protobuf` (`CodedOutputStream` / `CodedInputStream`) organize. We already have this for CompoundFile but not for rows or LVALs.

#### 3. Builder Pattern

Already used (`TDefPageBuilder`, `IndexBTreeBuilder`). Correct for constructing complex page buffers incrementally.

#### 4. Strategy Pattern (via layout structs)

For format-version polymorphism (Jet3 vs Jet4 vs ACE). We use this via the layout structs (`DataPageLayout`, `IndexLayout`). More cache-friendly than a virtual dispatch approach.

#### 5. Facade Pattern (GoF)

What `AccessReader`/`AccessWriter` *should* be — thin facades over domain modules. Currently they are god classes that embed the logic rather than delegating.

#### 6. Pager Pattern

Used by SQLite, LevelDB, RocksDB. A dedicated abstraction for page-level I/O that owns buffering, caching, and journaling. Our `AccessBase` page read/write + `PageJournal` + `LruCache` constitute this layer but are not cleanly separated.

#### 7. Gateway Pattern (Fowler, *PoEAA*)

Encapsulates access to an external system. `LockFileCoordinator` and `JetByteRangeLock` are gateways to filesystem concurrency primitives.

### SOLID principles (Robert C. Martin)

| Principle | Current violation |
|-----------|-------------------|
| **Single Responsibility (SRP)** | `AccessWriter` does row encoding, page insertion, transaction lifecycle, catalog writes, constraint management, LVAL encoding — 6+ responsibilities |
| **Open/Closed (OCP)** | Adding a new column type requires modifying the monolithic encode/decode switch inside the god class |
| **Interface Segregation (ISP)** | `IAccessWriter` is 287 lines — clients must depend on the entire surface |
| **Dependency Inversion (DIP)** | Codec logic is embedded in the orchestrator rather than injected/delegated as a dependency |

### Package/module design principles (Robert C. Martin, *Agile Principles*)

| Principle | Implication |
|-----------|-------------|
| **Common Closure Principle (CCP)** | Classes that change together belong in the same package. Index key encoding + index B-tree building + index leaf building all change when index format changes → they belong together in `Indexes/` |
| **Common Reuse Principle (CRP)** | Classes used together belong together. `CatalogEntry`, `CatalogRow`, `TableDef` are always consumed together → they belong in `Catalog/Models/` |
| **Acyclic Dependencies Principle (ADP)** | No circular dependencies between packages. The layered approach guarantees this: `Infrastructure/` → `Pages/` → `ValueEncoding/`/`ValueDecoding/` → `Indexes/` → `Catalog/` never cycle back |
| **Stable Dependencies Principle (SDP)** | Depend in the direction of stability. `Infrastructure/` and `Pages/` are stable (rarely change); `ValueEncoding/`, `ValueDecoding/`, and `Schema/` are volatile (change with format support) |

### Organizational standards

#### Package-by-Feature ("Vertical Slice" / "Screaming Architecture")

Robert C. Martin's term: the folder structure should **scream the domain**, not the technical role. Looking at folders should tell you "this is a JET database engine" not "this has Models and Helpers and Builders."

**Anti-pattern (current):**
```
Internal/Models/      ← "what kind of thing"
Internal/Builders/    ← "what kind of thing"
Internal/Helpers/     ← "what kind of thing"
```

**Correct:**
```
Indexes/              ← "what domain"
Encryption/           ← "what domain"
Catalog/              ← "what domain"
```

Recommended by:
- Martin Fowler (*Patterns of Enterprise Application Architecture*)
- Robert C. Martin (*Clean Architecture*, Chapter 21)
- Microsoft's .NET architecture guidance for non-trivial libraries

#### Namespace-Folder Correspondence (Microsoft .NET Framework Design Guidelines, §3.4)

> "The namespace should match the folder structure."

Current violation: `Internal/IndexBTreeSeeker.cs` is namespace `JetDatabaseWriter.Internal` while `Internal/Builders/IndexBTreeBuilder.cs` is namespace `JetDatabaseWriter.Internal.Builders` — even though they're the same subsystem.

---

## Proposed ideal structure

Organized by **subsystem/domain** (the standard for binary-format libraries), with `internal` as an access modifier rather than a folder name:

```
JetDatabaseWriter/
├── AccessReader.cs                        (thin orchestrator — public API only, ~300 lines)
├── AccessWriter.cs                        (thin orchestrator — public API only, ~500 lines)
├── AccessBase.cs                          (page I/O, format detection, stream lifecycle)
├── AccessReaderOptions.cs
├── AccessWriterOptions.cs
├── JetTransaction.cs
├── Constants.cs
├── IsExternalInit.cs
│
├── Interfaces/
│   ├── IAccessBase.cs
│   ├── IAccessOptions.cs
│   ├── IAccessReader.cs
│   ├── IAccessWriter.cs                  (DML: Insert, Update, Delete, BulkInsert)
│   └── IAccessSchema.cs                  (DDL: CreateTable, AddColumn, CreateIndex, etc.)
│
├── Models/                                (public DTOs — unchanged ✓)
│   └── ...
│
├── Enums/                                 (unchanged ✓)
│   └── ...
│
├── Exceptions/                            (unchanged ✓)
│   └── ...
│
├── Catalog/                               (system-table reading/writing)
│   ├── CatalogReader.cs                   (from AccessBase + AccessWriter catalog helpers)
│   ├── CatalogWriter.cs                   (InsertCatalogEntry, RewriteTable, RenameInCatalog)
│   └── Models/
│       ├── CatalogEntry.cs
│       ├── CatalogRow.cs
│       └── TableDef.cs
│
├── ValueEncoding/                         (write-path: typed values → bytes; named to avoid shadowing System.Text.Encoding; symmetric with ValueDecoding/)
│   ├── RowEncoder.cs                      (SerializeRow, EncodeFixed/Variable/Text/Binary)
│   ├── LongValueEncoder.cs               (LVAL chains, OLE wrapping)
│   ├── NumericEncoder.cs                  (from DecimalNumeric)
│   ├── TypeEncoder.cs                     (from parts of JetTypeInfo write-side)
│   └── Models/
│       ├── LvalChainResult.cs
│       └── PreEncodedLongValue.cs         (promoted from nested type)
│
├── ValueDecoding/                         (read-path: bytes → typed values)
│   ├── RowDecoder.cs                      (from AccessReader row-decode logic)
│   ├── RowMapper.cs                       (moved from Internal/ — column dispatch)
│   ├── LongValueDecoder.cs               (LVAL chain reading)
│   ├── TypedValueParser.cs               (moved from Internal/)
│   └── DirectRowDecoderBuilder.cs        (moved from Internal/Builders/)
│
├── Pages/                                 (page-level I/O & layout)
│   ├── DataPageLayout.cs                  (moved, now includes layout structs)
│   ├── DataPageInserter.cs               (FindInsertTarget, CanInsertRow, WriteRowToPage)
│   ├── DataPageReader.cs                 (from AccessReader page-scan logic)
│   ├── PageJournal.cs                     (moved from Internal/Transactions/)
│   └── Models/
│       ├── PageInsertTarget.cs
│       └── RowLocation.cs
│
├── Indexes/                               (all index concerns together)
│   ├── IndexKeyEncoder.cs
│   ├── IndexBTreeBuilder.cs              (moved from Internal/Builders/)
│   ├── IndexBTreeSeeker.cs
│   ├── IndexCatalogReader.cs             (moved from Internal/ — CCP: index-specific)
│   ├── IndexLeafIncremental.cs
│   ├── IndexLeafPageBuilder.cs           (moved from Internal/Builders/)
│   ├── IndexMaintainer.cs
│   ├── IndexLayout.cs
│   ├── UniqueIndexChecker.cs             (new, from AccessWriter)
│   ├── Helpers/
│   │   └── IndexHelpers.cs
│   ├── Collation/                         (sort-key generation — avoids shadowing System.Text.Encoding)
│   │   ├── GeneralTextIndexEncoder.cs
│   │   ├── GeneralLegacyTextIndexEncoder.cs
│   │   └── General97TextIndexEncoder.cs
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
├── Schema/                                (DDL: table/column/index definition)
│   ├── TDefPageBuilder.cs                (moved from Internal/Builders/)
│   ├── ColumnPropertyBlockBuilder.cs     (moved from Internal/Builders/)
│   ├── ConstraintRegistry.cs             (new, from AccessWriter)
│   ├── JetTypeInfo.cs                    (column type metadata)
│   ├── JetExpressionConverter.cs
│   ├── CalculatedColumnUtil.cs
│   └── Models/
│       ├── ColumnInfo.cs
│       ├── ColumnConstraint.cs            (promoted from nested type)
│       ├── ColumnPropertyBlock.cs
│       ├── ColumnPropertyEntry.cs
│       ├── ColumnPropertyTarget.cs
│       ├── ColumnPropertyChunkType.cs
│       └── ColumnPropertyUnknownChunk.cs
│
├── Transactions/                          (lifecycle, locking, journaling)
│   ├── TransactionLifecycle.cs           (new, from AccessWriter)
│   ├── LockFileCoordinator.cs
│   ├── LockFileSlotWriter.cs
│   └── JetByteRangeLock.cs
│
├── Encryption/                            (all crypto concerns)
│   ├── EncryptionManager.cs
│   ├── EncryptionConverter.cs
│   ├── OfficeCryptoAgile.cs
│   └── Models/
│       └── PageDecryptionKeys.cs          (promoted from nested class)
│
├── Relationships/                         (FK, cascade, linked tables)
│   ├── RelationshipManager.cs
│   └── LinkedTableManager.cs
│
├── ComplexColumns/                        (attachments, multi-value, versioned)
│   ├── ComplexColumnManager.cs
│   └── Models/
│       └── AttachmentWrapper.cs
│
├── CompoundFile/                          (OLE structured storage)
│   ├── CompoundFileReader.cs
│   └── CompoundFileWriter.cs
│
└── Infrastructure/                        (generic utilities, not JET-specific)
    ├── LruCache.cs
    ├── ByteArrayEqualityComparer.cs
    ├── AsyncLazyInitializer.cs
    ├── AsyncReentrantOperationGate.cs
    ├── Guard.cs
    └── NonClosingStreamWrapper.cs
```

### Key principles (with justification)

| # | Principle | Pattern/standard applied |
|---|-----------|-------------------------|
| 1 | **Domain-first folders** — group by subsystem, not by type-role | Package-by-Feature, CCP, Screaming Architecture |
| 2 | **Reader/Writer stays thin** — orchestration only; logic in domain modules | Facade pattern, SRP, Layered Codec Architecture |
| 3 | **Symmetric encode/decode pairs** — matched types for write-path and read-path | Symmetric Codec pattern (protobuf, System.Text.Json) |
| 4 | **Public types at root namespace** — no `Core` sub-namespace for the main API | .NET Framework Design Guidelines §3.4 |
| 5 | **One class per file** — nested types promoted to own files | SRP, discoverability |
| 6 | **Models co-located with their domain** — not in a single bag | CRP, CCP |
| 7 | **`internal` keyword on classes, not folder** — visibility via access modifier | Namespace-Folder Correspondence standard |
| 8 | **Depend in direction of stability** — volatile packages depend on stable ones | SDP, ADP |
| 9 | **ValueEncoding/ and ValueDecoding/ never depend on each other** — shared types go in Schema/ or root | ADP, Symmetric Codec |
| 10 | **ISP on the public interface** — split DML from DDL surface | ISP (ADO.NET precedent) |
| 11 | **Avoid shadowing BCL names** — use `ValueEncoding/`/`ValueDecoding/` not `Encoding/`/`Decoding/`, `Collation/` not `TextEncoding/` | .NET FDG §3.3 |

---

## Suggested moves/renames (ordered)

Sequenced for minimal merge conflicts — infrastructure/leaf moves first, then structural reorganization, then the big extractions last.

### Phase A — Eliminate `Core/` indirection (pure moves, no logic change) ✅

| # | Action | From | To | Status |
|--:|--------|------|-----|--------|
| 1 | Move | `Core/Interfaces/` (4 files) | `Interfaces/` | ✅ |
| 2 | Move | `Core/AccessBase.cs` | `AccessBase.cs` (root) | ✅ |
| 3 | Move | `Core/AccessReader.cs` | `AccessReader.cs` (root) | ✅ |
| 4 | Move | `Core/AccessWriter.cs` | `AccessWriter.cs` (root) | ✅ |
| 5 | Move | `Core/AccessReaderOptions.cs` | `AccessReaderOptions.cs` (root) | ✅ |
| 6 | Move | `Core/AccessWriterOptions.cs` | `AccessWriterOptions.cs` (root) | ✅ |
| 7 | Move | `Core/JetTransaction.cs` | `JetTransaction.cs` (root) | ✅ |

> **Completed:** Namespace changed from `JetDatabaseWriter.Core` → `JetDatabaseWriter` and `JetDatabaseWriter.Core.Interfaces` → `JetDatabaseWriter.Interfaces`. All `using` statements and `cref` references updated solution-wide. `Core/` directory deleted.

### Phase B — Infrastructure (generic utilities) ✅

| # | Action | From | To | Status |
|--:|--------|------|-----|--------|
| 8 | Move | `Internal/Collections/LruCache.cs` | `Infrastructure/LruCache.cs` | ✅ |
| 9 | Move | `Internal/Collections/ByteArrayEqualityComparer.cs` | `Infrastructure/ByteArrayEqualityComparer.cs` | ✅ |
| 10 | Move | `Internal/Helpers/AsyncLazyInitializer.cs` | `Infrastructure/AsyncLazyInitializer.cs` | ✅ |
| 11 | Move | `Internal/Helpers/AsyncReentrantOperationGate.cs` | `Infrastructure/AsyncReentrantOperationGate.cs` | ✅ |
| 12 | Move | `Internal/Helpers/Guard.cs` | `Infrastructure/Guard.cs` | ✅ |
| 13 | Move | `Internal/Helpers/NonClosingStreamWrapper.cs` | `Infrastructure/NonClosingStreamWrapper.cs` | ✅ |

> **Completed:** Namespace changed from `JetDatabaseWriter.Internal.Collections` and `JetDatabaseWriter.Internal.Helpers` → `JetDatabaseWriter.Infrastructure`. All `using` statements updated solution-wide. Files that still reference `IndexHelpers`, `DecimalNumeric`, or `CalculatedColumnUtil` (which remain in `Internal/Helpers/`) retain their `using JetDatabaseWriter.Internal.Helpers;` import. `Internal/Collections/` directory deleted.

### Phase C — CompoundFile, Encryption, Transactions (self-contained subsystems) ✅

| # | Action | From | To | Status |
|--:|--------|------|-----|--------|
| 14 | Move | `Internal/CompoundFileReader.cs` | `CompoundFile/CompoundFileReader.cs` | ✅ |
| 15 | Move | `Internal/CompoundFileWriter.cs` | `CompoundFile/CompoundFileWriter.cs` | ✅ |
| 16 | Move | `Internal/EncryptionManager.cs` | `Encryption/EncryptionManager.cs` | ✅ |
| 17 | Move | `Internal/EncryptionConverter.cs` | `Encryption/EncryptionConverter.cs` | ✅ |
| 18 | Move | `Internal/OfficeCryptoAgile.cs` | `Encryption/OfficeCryptoAgile.cs` | ✅ |
| 19 | Move | `Internal/LockFileCoordinator.cs` | `Transactions/LockFileCoordinator.cs` | ✅ |
| 20 | Move | `Internal/LockFileSlotWriter.cs` | `Transactions/LockFileSlotWriter.cs` | ✅ |
| 21 | Move | `Internal/JetByteRangeLock.cs` | `Transactions/JetByteRangeLock.cs` | ✅ |
| 22 | Move | `Internal/Transactions/PageJournal.cs` | `Pages/PageJournal.cs` | ✅ |

> **Completed:** Namespaces changed from `JetDatabaseWriter.Internal` → `JetDatabaseWriter.CompoundFile`, `JetDatabaseWriter.Encryption`, `JetDatabaseWriter.Transactions`; and `JetDatabaseWriter.Internal.Transactions` → `JetDatabaseWriter.Pages`. All `using` statements updated in library, test, and benchmark projects. `Internal/Transactions/` directory deleted.

### Phase D — Relationships & complex columns ✅

| # | Action | From | To | Status |
|--:|--------|------|-----|--------|
| 23 | Move | `Internal/Relationships/RelationshipManager.cs` | `Relationships/RelationshipManager.cs` | ✅ |
| 24 | Move | `Internal/LinkedTableManager.cs` | `Relationships/LinkedTableManager.cs` | ✅ |
| 25 | Move | `Internal/ComplexColumnManager.cs` | `ComplexColumns/ComplexColumnManager.cs` | ✅ |
| 26 | Move | `Internal/Models/AttachmentWrapper.cs` | `ComplexColumns/Models/AttachmentWrapper.cs` | ✅ |

> **Completed:** Namespaces changed from `JetDatabaseWriter.Internal.Relationships` → `JetDatabaseWriter.Relationships`, `JetDatabaseWriter.Internal` (LinkedTableManager) → `JetDatabaseWriter.Relationships`, `JetDatabaseWriter.Internal` (ComplexColumnManager) → `JetDatabaseWriter.ComplexColumns`, and `JetDatabaseWriter.Internal.Models` (AttachmentWrapper) → `JetDatabaseWriter.ComplexColumns.Models`. All `using` statements updated in library and test projects. `Internal/Relationships/` directory deleted.

### Phase E — Indexes (consolidate all index concerns) ✅

| # | Action | From | To | Status |
|--:|--------|------|-----|--------|
| 27 | Move | `Internal/IndexKeyEncoder.cs` | `Indexes/IndexKeyEncoder.cs` | ✅ |
| 28 | Move | `Internal/IndexBTreeSeeker.cs` | `Indexes/IndexBTreeSeeker.cs` | ✅ |
| 29 | Move | `Internal/IndexLeafIncremental.cs` | `Indexes/IndexLeafIncremental.cs` | ✅ |
| 30 | Move | `Internal/IndexMaintainer.cs` | `Indexes/IndexMaintainer.cs` | ✅ |
| 31 | Move | `Internal/IndexLayout.cs` | `Indexes/IndexLayout.cs` | ✅ |
| 32 | Move | `Internal/IndexCatalogReader.cs` | `Indexes/IndexCatalogReader.cs` | ✅ |
| 33 | Move | `Internal/Builders/IndexBTreeBuilder.cs` | `Indexes/IndexBTreeBuilder.cs` | ✅ |
| 34 | Move | `Internal/Builders/IndexLeafPageBuilder.cs` | `Indexes/IndexLeafPageBuilder.cs` | ✅ |
| 35 | Move | `Internal/Helpers/IndexHelpers.cs` | `Indexes/Helpers/IndexHelpers.cs` | ✅ |
| 36 | Move | `Internal/GeneralTextIndexEncoder.cs` | `Indexes/Collation/GeneralTextIndexEncoder.cs` | ✅ |
| 37 | Move | `Internal/GeneralLegacyTextIndexEncoder.cs` | `Indexes/Collation/GeneralLegacyTextIndexEncoder.cs` | ✅ |
| 38 | Move | `Internal/General97TextIndexEncoder.cs` | `Indexes/Collation/General97TextIndexEncoder.cs` | ✅ |
| 39 | Move | `Internal/Models/IndexEntry.cs` | `Indexes/Models/IndexEntry.cs` | ✅ |
| 40 | Move | `Internal/Models/ResolvedIndex.cs` | `Indexes/Models/ResolvedIndex.cs` | ✅ |
| 41 | Move | `Internal/Models/DescentStep.cs` | `Indexes/Models/DescentStep.cs` | ✅ |
| 42 | Move | `Internal/Models/ChildSeekIndex.cs` | `Indexes/Models/ChildSeekIndex.cs` | ✅ |
| 43 | Move | `Internal/Models/ParentSeekIndex.cs` | `Indexes/Models/ParentSeekIndex.cs` | ✅ |
| 44 | Move | `Internal/Models/DecodedIntermediateEntry.cs` | `Indexes/Models/DecodedIntermediateEntry.cs` | ✅ |
| 45 | Move | `Internal/Models/IntermediateOp.cs` | `Indexes/Models/IntermediateOp.cs` | ✅ |
| 46 | Move | `Internal/Models/SplitPages.cs` | `Indexes/Models/SplitPages.cs` | ✅ |

> **Completed:** Namespaces changed from `JetDatabaseWriter.Internal` → `JetDatabaseWriter.Indexes`, `JetDatabaseWriter.Internal.Builders` (index builders) → `JetDatabaseWriter.Indexes`, `JetDatabaseWriter.Internal.Helpers` (IndexHelpers) → `JetDatabaseWriter.Indexes.Helpers`, `JetDatabaseWriter.Internal` (collation encoders) → `JetDatabaseWriter.Indexes.Collation`, and `JetDatabaseWriter.Internal.Models` (index models) → `JetDatabaseWriter.Indexes.Models`. All `using` statements updated in library, test, benchmark, and probe projects. Using aliases for `IndexLayout` nested types updated in `AccessWriter` and `IndexMaintainer`.

### Phase F — Schema (DDL, column type info, TDef building) ✅

| # | Action | From | To | Status |
|--:|--------|------|-----|--------|
| 47 | Move | `Internal/Builders/TDefPageBuilder.cs` | `Schema/TDefPageBuilder.cs` | ✅ |
| 48 | Move | `Internal/Builders/ColumnPropertyBlockBuilder.cs` | `Schema/ColumnPropertyBlockBuilder.cs` | ✅ |
| 49 | Move | `Internal/JetTypeInfo.cs` | `Schema/JetTypeInfo.cs` | ✅ |
| 50 | Move | `Internal/JetExpressionConverter.cs` | `Schema/JetExpressionConverter.cs` | ✅ |
| 51 | Move | `Internal/Helpers/CalculatedColumnUtil.cs` | `Schema/CalculatedColumnUtil.cs` | ✅ |
| 52 | Move | `Internal/Models/ColumnInfo.cs` | `Schema/Models/ColumnInfo.cs` | ✅ |
| 53 | Move | `Internal/Models/ColumnPropertyBlock.cs` | `Schema/Models/ColumnPropertyBlock.cs` | ✅ |
| 54 | Move | `Internal/Models/ColumnPropertyEntry.cs` | `Schema/Models/ColumnPropertyEntry.cs` | ✅ |
| 55 | Move | `Internal/Models/ColumnPropertyTarget.cs` | `Schema/Models/ColumnPropertyTarget.cs` | ✅ |
| 56 | Move | `Internal/Models/ColumnPropertyChunkType.cs` | `Schema/Models/ColumnPropertyChunkType.cs` | ✅ |
| 57 | Move | `Internal/Models/ColumnPropertyUnknownChunk.cs` | `Schema/Models/ColumnPropertyUnknownChunk.cs` | ✅ |

> **Completed:** Namespaces changed from `JetDatabaseWriter.Internal.Builders` (TDefPageBuilder, ColumnPropertyBlockBuilder) → `JetDatabaseWriter.Schema`, `JetDatabaseWriter.Internal` (JetTypeInfo, JetExpressionConverter) → `JetDatabaseWriter.Schema`, `JetDatabaseWriter.Internal.Helpers` (CalculatedColumnUtil) → `JetDatabaseWriter.Schema`, and `JetDatabaseWriter.Internal.Models` (ColumnInfo, ColumnProperty* files) → `JetDatabaseWriter.Schema.Models`. All `using` statements updated in library, test, benchmark, and probe projects. `cref` references in Constants.cs updated. `Internal/Helpers/` now contains only `DecimalNumeric.cs`.

### Phase G — Catalog (system-table access) ✅

| # | Action | From | To | Status |
|--:|--------|------|-----|--------|
| 58 | Move | `Internal/Models/CatalogEntry.cs` | `Catalog/Models/CatalogEntry.cs` | ✅ |
| 59 | Move | `Internal/Models/CatalogRow.cs` | `Catalog/Models/CatalogRow.cs` | ✅ |
| 60 | Move | `Internal/Models/TableDef.cs` | `Catalog/Models/TableDef.cs` | ✅ |

> **Completed:** Namespaces changed from `JetDatabaseWriter.Internal.Models` → `JetDatabaseWriter.Catalog.Models`. All `using` statements updated in library, test, benchmark, and probe projects. Fully-qualified reference in `IndexWriterTests.cs` updated. All 3078 tests pass.

### Phase H — ValueDecoding & Pages (read-path plumbing) ✅

| # | Action | From | To | Status |
|--:|--------|------|-----|--------|
| 61 | Move | `Internal/TypedValueParser.cs` | `ValueDecoding/TypedValueParser.cs` | ✅ |
| 62 | Move | `Internal/Builders/DirectRowDecoderBuilder.cs` | `ValueDecoding/DirectRowDecoderBuilder.cs` | ✅ |
| 63 | Move | `Internal/RowMapper.cs` | `ValueDecoding/RowMapper.cs` | ✅ |
| 64 | Move | `Internal/DataPageLayout.cs` | `Pages/DataPageLayout.cs` | ✅ |
| 65 | Move | `Internal/Models/PageInsertTarget.cs` | `Pages/Models/PageInsertTarget.cs` | ✅ |
| 66 | Move | `Internal/Models/RowLocation.cs` | `Pages/Models/RowLocation.cs` | ✅ |
| 67 | Move | `Internal/Models/LvalChainResult.cs` | `ValueEncoding/Models/LvalChainResult.cs` | ✅ |
| 68 | Rename | `Internal/Helpers/DecimalNumeric.cs` | `ValueEncoding/NumericEncoder.cs` | ✅ |

> **Completed:** Namespaces changed from `JetDatabaseWriter.Internal` (TypedValueParser, RowMapper, DataPageLayout) → `JetDatabaseWriter.ValueDecoding` and `JetDatabaseWriter.Pages`, `JetDatabaseWriter.Internal.Builders` (DirectRowDecoderBuilder) → `JetDatabaseWriter.ValueDecoding`, `JetDatabaseWriter.Internal.Models` (PageInsertTarget, RowLocation) → `JetDatabaseWriter.Pages.Models`, `JetDatabaseWriter.Internal.Models` (LvalChainResult) → `JetDatabaseWriter.ValueEncoding.Models`, and `JetDatabaseWriter.Internal.Helpers` (DecimalNumeric) → `JetDatabaseWriter.ValueEncoding` (class renamed to `NumericEncoder`). The `Encoding/` folder name was changed to `ValueEncoding/` to avoid shadowing `System.Text.Encoding`; the `Decoding/` folder was similarly renamed to `ValueDecoding/` for symmetry. All `using` statements updated in library, test, benchmark, and probe projects. `Internal/Builders/`, `Internal/Helpers/`, and `Internal/Models/` directories deleted; `Internal/` now contains only `IndexCodeTables/` (embedded resources). All 3078 tests pass.

### Phase I — Code extractions (substantive refactoring)

| # | Action | Source | Target | Status |
|--:|--------|--------|--------|--------|
| 69 | **Extract** | `AccessWriter` → constraint methods | `Schema/ConstraintRegistry.cs` | ✅ |
| 70 | **Extract** | `AccessWriter` → LVAL encode methods | `ValueEncoding/LongValueEncoder.cs` | ✅ |
| 71 | **Extract** | `AccessWriter` → transaction lifecycle | `Transactions/TransactionLifecycle.cs` | ✅ |
| 72 | **Extract** | `AccessWriter` → unique-index checks | `Indexes/UniqueIndexChecker.cs` | ✅ |
| 73 | **Extract** | `AccessWriter` → catalog helpers | `Catalog/CatalogWriter.cs` | ✅ |
| 74 | **Extract** | `AccessWriter` → row encode + page insert | `ValueEncoding/RowEncoder.cs` + `Pages/DataPageInserter.cs` | ✅ |
| 75 | **Extract** | `AccessReader` → LVAL read logic | `ValueDecoding/LongValueDecoder.cs` | ✅ |

> **Step 69 + 76 completed:** Nested `AccessWriter.ColumnConstraint` promoted to `Schema/Models/ColumnConstraint.cs`. All constraint management logic (`Register`, `Unregister`, `Rename`, `ApplyAsync`, `RestoreAutoCounters`, `HydrateFromTableDef`, `TdefTypeToClrType`, `GetNextAutoValueAsync`, `ToConstraint`, `IsIntegralType`, `ConvertIntegral`) extracted to `Schema/ConstraintRegistry.cs`. `AccessWriter` now holds a `ConstraintRegistry` instance (injected with a `ReadTableSnapshotAsync` delegate) and delegates all constraint operations. `ConstraintRegistry.TryGet` added for schema-evolution paths that need direct constraint list access. All 3078 tests pass.
>
> **Steps 70–75 completed:** All extractable writer helpers (`LongValueEncoder`, `TransactionLifecycle`, `UniqueIndexChecker`, `CatalogWriter`, `RowEncoder`, `DataPageInserter`) and reader helpers (`LongValueDecoder`) extracted into domain-correct folders. Each god class now holds a field for its helper and delegates via thin forwarders. `AccessWriter` reduced from ~4300 → ~3150 lines; `AccessReader` from ~3700 → ~3840 lines (net neutral after LVAL extraction offset by prior growth). All 3081 tests pass.

### Phase J — Promote nested types

| # | Action | Source | Target |
|--:|--------|--------|--------|
| 76 | **Promote** | Nested `AccessWriter.ColumnConstraint` | `Schema/Models/ColumnConstraint.cs` | ✅ |
| 77 | **Promote** | Nested `AccessWriter.PreEncodedLongValue` | `ValueEncoding/Models/PreEncodedLongValue.cs` |
| 78 | **Promote** | Nested `EncryptionManager.PageDecryptionKeys` | `Encryption/Models/PageDecryptionKeys.cs` |

### Phase K — Interface segregation & cleanup

| # | Action | Target |
|--:|--------|--------|
| 79 | **Split** `IAccessWriter` into `IAccessWriter` (DML) + `IAccessSchema` (DDL); `AccessWriter` implements both |
| 80 | Update all `namespace` declarations to match new folder paths |
| 81 | Delete empty `Core/` folder |
| 82 | Delete empty `Internal/` folder |

---

## Dependency direction (ADP verification)

The proposed structure must be acyclic. Expected dependency flow:

```
Infrastructure/        → (nothing — leaf)
Pages/                 → Infrastructure/
Encryption/            → Pages/
ValueDecoding/         → Pages/, Schema/
ValueEncoding/         → Schema/                        [rule: never depends on ValueDecoding/]
Indexes/               → Pages/, ValueEncoding/ (key encoding), Schema/
Catalog/               → Pages/, ValueDecoding/, Schema/
Transactions/          → Pages/
Relationships/         → Catalog/, Indexes/
ComplexColumns/        → Catalog/, Pages/
CompoundFile/          → (nothing — leaf)
AccessBase (root)      → Pages/, Encryption/, Infrastructure/
AccessReader (root)    → ValueDecoding/, Catalog/, Indexes/, Pages/
AccessWriter (root)    → ValueEncoding/, Catalog/, Indexes/, Transactions/, Schema/, Relationships/, ComplexColumns/
```

No cycles exist. Each domain folder depends only on peers at the same or lower layer.

---

## Execution notes

- **Steps 1–68** are mechanical file moves. Each can be done with `git mv` + find-replace on namespace declarations and `using` statements. Commit per phase (A–H) is reasonable.
- **Steps 69–77** are substantive code extractions — these correspond to Phases 1–6 of the existing `access-writer-extraction-plan.md` but land in domain-correct folders instead of a flat `Internal/`.
- **Steps 78–80** promote nested types to top-level internal types in their own files.
- **Each phase should compile and pass tests before proceeding to the next.** Namespace renames propagate through `InternalsVisibleTo` to the test and benchmark projects.
- The `IndexCodeTables/` resource folder (gzipped text files) should move alongside its consumer — likely `Indexes/Collation/` or stay as an embedded-resource folder at `Indexes/CodeTables/`.
