# Design notes: Complex columns (Attachment, Multi-value) — write path

**Status:** All shipped phases listed in §4.2; outstanding work tracked in the same table.
**Empirical appendix:** [`format-probe-appendix-complex.md`](../format-probe/format-probe-appendix-complex.md) — annotated hex dumps of `MSysComplexColumns`, every `MSysComplexType_*` template table, an attachment-bearing parent table (`Documents`), and the hidden flat tables from `ComplexFields.accdb`. Regenerate via `dotnet run --project JetDatabaseWriter.FormatProbe -- complex`.
**Validation requirement:** see [`index-and-relationship-format-notes.md` §8](index-and-relationship-format-notes.md#8-validation-strategy).

> ⚠️ Reverse-engineered notes. mdbtools documents complex columns only superficially. The authoritative open-source reference is [Jackcess](https://github.com/jahlborn/jackcess) (Java, Apache-2.0) — specifically `com.healthmarketscience.jackcess.impl.complex.*`. Field names and offsets in this document are derived from Jackcess source and the existing reader code in this repo (`ComplexColumnReader`, `AttachmentWrapper`).

---

## 1. Background

Access 2007 introduced three "complex column" kinds. All three are stored the same way: a 4-byte per-row complex reference in the parent row, pointing into a hidden child ("flat") table that holds the actual values. **All three kinds share the column-type byte `0x12` (`COMPLEX_TYPE`)** — confirmed both by Jackcess `DataType.java` and by our format-probe across the entire test corpus (no on-disk fixture in `JetDatabaseWriter.Tests/Databases/` carries `0x11` on a complex column). The writer now emits `0x12` for attachment and multi-value parent descriptors; `Attachment = 0x11` in `Constants.cs` is retained only as a legacy/private alias for older reader tolerance.

| Kind | Storage in child table |
|---|---|
| Attachment | One row per attached file. Columns: `FileURL`, `FileName`, `FileType`, `FileFlags`, `FileTimeStamp`, `FileData`. |
| Multi-value | One row per value. Columns: a single value column whose type matches the user-declared element type. |
| Version history | One row per historical edit. Columns: the version's text (a Memo column named after the append-only Memo column) and `Modified_<GUID>` (Date/Time). Only meaningful on memo columns flagged "Append Only" in Access. |

The discrimination between attachment / multi-value / version-history is **not** done by the column-type byte. It is done by the linked flat table's schema and/or the value of `MSysComplexColumns.ComplexTypeObjectID` (which points at one of the `MSysComplexType_*` template tables — see [appendix](../format-probe/format-probe-appendix-complex.md)).

The reader implements all three kinds. Writer support has shipped through phase C10 — see §4.2.

## 2. On-disk layout

### 2.1 Parent column descriptor

Inside the parent table's TDEF column descriptor block (25 bytes per col on Jet4 / ACE), a complex column has:

| Field | Value |
|---|---|
| `col_type` | `0x12` (`COMPLEX_TYPE`) for all three kinds — attachment, multi-value, and version-history. |
| `col_len` | `4` |
| `bitmask` | `0x07` (always — per mdbtools "always have the flag byte set to exactly 0x07") |
| `misc` | The 4-byte **ComplexID** (called "complexid" in mdbtools), stored at the `misc` offset. This is the value used to match the parent descriptor to its `MSysComplexColumns` row. |

Because `bitmask = 0x07`, the column is treated as a fixed-length 4-byte column for row-layout purposes. The 4-byte payload in each data row is the per-row complex reference value that joins this parent row to child rows in the flat table's FK column. The `MSysComplexColumns.ConceptualTableID` catalog column is separate: it identifies the parent table object/TDEF page for the complex-column definition.

Access gives every complex column its own index on that reference: unique, Required, and named `<column>_<32 hex>` (truncated so the name fits in 64 characters), for example `Attachments_CFC7F98A63064F2DBDB6822D653AB763` in `ComplexFields.accdb`, `attach-data_071D71EDD53D45A1A9089929F06857D9` and `VersionHistory_F5F8918F-0A3F-4D_6E54CCBB170741DD8FD837271ED8B90C` in `complexDataTest*.accdb`, and `ProductCategoryImage_5C9A6A17CF9D4E1CA64DB2DECECEBB16` in `NorthwindTraders.accdb`. Its keys use the Long Integer layout (`7F` + big-endian int with the sign bit flipped; `7F 80 00 00 01` for reference 1), as Jackcess `IndexData` maps `COMPLEX_TYPE` to its integer descriptor. `IndexKeyEncoder` encodes `Complex` / `Attachment` keys that way, so updates, deletes and inserts on Access tables with complex columns keep these indexes current (`ComplexColumnIndexFixtureTests`). Users still cannot declare an index on a complex column, and tables the writer creates do not yet get this index.

**Per-row references come from the TDEF complex AutoNumber.** ACE TDEF headers hold a second counter at offset 28 (`TDefHeaderLayout.ComplexAutoNumber`; Jackcess `JetFormat.OFFSET_NEXT_COMPLEX_AUTO_NUMBER`, mdbtools `ct_autonum`; Jet3 and Jet4 have none): the last per-row complex reference handed out. Access gives each row one reference, shared by all its complex columns, even a column with no items; it does not reuse the references of deleted rows. In the fixtures:

| Fixture / table | TDEF@28 | Live slots |
|---|---|---|
| `ComplexFields.accdb` `Documents` | 2 | `[1]`, `[2]` |
| `complexDataTestV2007.accdb` `Table1` | 7 | `[1,1,1]` … `[4,4,4]` (rows deleted in Access) |
| `complexDataTestV2010.accdb` `Table1` | 8 | `[1,1,1]` … `[4,4,4]` |
| `NorthwindTraders.accdb` `ProductCategories` | 17 | 1 … 16 |

Byte 24 is `0x01` in all of them; bytes 32–39 hold values whose meaning is unknown, and the writer does not touch them.

The writer does the same. `ConstraintRegistry.ApplyAsync` gives every inserted row whose complex columns are null one reference, shared by those columns. A reference the caller supplies in any of them becomes the row's, its null complex columns take it, and the counter moves past it; two different supplied references, or one outside 1 to `int.MaxValue`, are rejected with `ArgumentException`, as Jackcess `ComplexTypeAutoNumberGenerator` rejects inconsistent ones. The counter lives on the table's first complex constraint for the writer session, like an AutoNumber counter: it is seeded from one more than the largest of TDEF@28, every live row's complex slots and every foreign key in the table's flat tables (`ComplexReferenceSeedReader`, which covers files whose counter earlier builds of this library left at 0), rewound when a row or batch is rejected, and restored with the registry on rollback. `TableDataWriter` raises TDEF@28 after each batch (`AutoNumberMaintainer.UpdateComplexHighWaterAsync`). When `AddAttachmentAsync` / `AddMultiValueItemAsync` find the parent row's slot null (rows written by earlier builds), `ComplexColumnManager` first verifies that the parent indexes can be maintained, before changing the row or the counter, then takes the next reference from the same counter (`ConstraintRegistry.AllocateComplexReferencesAsync`), raises TDEF@28, and stores it in every null complex slot of the row in one page write; if the table has an index on a complex column, its indexes are then rebuilt. `TableSchemaEditor.RewriteTableAsync` fills each null complex slot of the rows it copies. A complex column it adds has a new, empty flat table, so it takes the reference the row's other complex columns share, when they all hold it and no other row does. Any other row with a null slot (no other complex column, a slot an earlier build left null, or references that differ or repeat another row's) gets a fresh reference in its null slots, because an existing column's flat table may already hold rows for the references its other columns use. The rewrite carries TDEF@28 to the rebuilt TDEF on both the transplant and the copy-and-swap path, as it does the AutoNumber counter at offset 20. `UpdateRowsAsync` refuses to assign a complex column (`ArgumentException`), so an update keeps the row's reference and items; Jackcess, by default, ignores a value given for one and keeps the stored reference. A row with a reference but no items still reads as `DBNull`. `ComplexColumnsReferenceAllocationTests` and `TransactionRollbackStateTests.ComplexReference_AfterRolledBackInsert_IsReused` cover this.

### 2.2 `MSysComplexColumns` catalog table

**Verified against `ComplexFields.accdb`** ([appendix](../format-probe/format-probe-appendix-complex.md#msyscomplexcolumns--tdef-page-18)). Actual schema is **5 columns** (column names and order below are probe-confirmed):

| Column (verified) | Type | Meaning |
|---|---|---|
| `ColumnName` | `Text(510)` | Name of the parent column (e.g. `"Attachments"`). |
| `ComplexID` | `LongInteger` (fixed_off=12), AutoNumber (flags `0x17`) | Per-database ID for this complex column. **Matches the 4-byte value the parent TDEF stores in the column descriptor's `misc`+`misc_ext` slot** (see §2.1). |
| `ComplexTypeObjectID` | `LongInteger` (fixed_off=0) | `MSysObjects.Id` of the **type-template table** — one of `MSysComplexType_Long`, `MSysComplexType_Text`, `MSysComplexType_Attachment`, etc. The template's schema dictates the kind (attachment vs multi-value of a given inner type). |
| `ConceptualTableID` | `LongInteger` (fixed_off=8) | Parent table object/TDEF page for the complex-column definition. Schema rewrite preserves this identity when possible by transplanting rebuilt storage onto the original TDEF page; copy/swap fallbacks patch the value to the replacement TDEF page. |
| `FlatTableID` | `LongInteger` (fixed_off=4) | `MSysObjects.Id` of the hidden child ("flat") table. |

The rows above are in the catalog's logical column order; the `fixed_off` values give each column's offset in the physical fixed-width row area, which Access orders independently (`ComplexTypeObjectID`=0, `FlatTableID`=4, `ConceptualTableID`=8, `ComplexID`=12).

`ComplexID` is an AutoNumber column, and the `MSysComplexColumns` TDEF AutoNumber counter (offset 20) holds the last ID handed out, including IDs of complex columns since dropped: 1 in `ComplexFields.accdb`, 7 in both `complexDataTest*.accdb` (whose largest remaining ID is 4) and 3 in `NorthwindTraders.accdb`. `ComplexColumnManager.GetNextComplexIdAsync` allocates one more than the larger of that counter and the largest `ComplexID` still stored (the scan covers files whose counter earlier builds of this library left at 0), and `InsertMSysComplexColumnsRowAsync` raises the counter after each row, so a dropped column's ID is never handed out again. The counter is on a TDEF page, so a rolled-back transaction discards the raise with the rest of its writes. `ComplexColumnsWriterTests` (`CreateTable_ComplexColumns_RaisesComplexIdCounter`, `DropThenAddComplexColumn_DoesNotReuseComplexId`, `CreateTable_AccessFixture_ContinuesFromComplexIdCounter`, `CreateTable_RolledBackTransaction_RestoresComplexIdCounter`) covers this.

There is **no** `ParentTable` / `ParentColumn` column in `MSysComplexColumns`. The reader joins the requested table's complex descriptors to `ComplexID` and verifies that `ConceptualTableID` names that parent. Duplicate IDs for the parent are ambiguous and yield no descriptor. If the descriptor join fails, a fallback requires one matching `ConceptualTableID` and `ColumnName` catalog row with a positive `FlatTableID`. A flat-table name suffix alone never establishes ownership; missing or ambiguous mappings yield no cell under the existing best-effort read policy.

`MSysComplexColumns` is now created by the ACCDB full-catalog scaffold on every fresh ACCDB built via `CreateDatabaseAsync` (Phase C1, 2026-04-25). That same scaffold also creates the core `MSysACEs`, `MSysQueries`, and `MSysRelationships` tables expected by DAO compatibility paths. The catalog rows carry `Flags = 0x80000000` so the tables are excluded from `ListTablesAsync`. ACE only — Jet3/Jet4 `.mdb` scaffolds skip these tables.

### 2.3 The hidden "flat" child table

**Verified naming and flag conventions** ([appendix](../format-probe/format-probe-appendix-complex.md)):

- Flat-table name pattern: **`f_<32-hex-uppercase>_<userColumnName>`**, e.g. `f_A3DF50CFC033433899AF0AC1A4CF4171_Attachments`. The 32 hex characters are a GUID without dashes. Access truncates the whole name to 64 characters: in `complexDataTestV2007.accdb` the column `VersionHistory_F5F8918F-0A3F-4DA9-AE71-184EE5012880` has the flat table `f_0BFE159C44334BFB968A820EA89C2D7E_VersionHistory_F5F8918F-0A3F-`. That fixture's `_<userColumnName>` and `<parentTable>_<userColumnName>` columns are 52 and 58 characters, so it does not show how Access shortens those. `ComplexColumnManager.BuildFlatTableName` does not truncate.
- Flat-table `MSysObjects.Flags`: **`0x800A0000`** (probe-confirmed in both `NorthwindTraders.accdb` and `ComplexFields.accdb`).
- Template ("type") tables: `MSysComplexType_<TypeName>` (`Long`, `Text`, `Attachment`, `UnsignedByte`, `Short`, `IEEESingle`, `IEEEDouble`, `GUID`, `Decimal`). `Flags = 0x80030000`.
- Stored as a normal user table in `MSysObjects` (Type = 1), but the system-flag bit (`0x80000000`) hides it from Access UI.

The flat table has all the value-bearing columns of the complex type, plus two extra columns:

1. An **autonumber Long primary key** column.
2. A **Long FK column** that holds the same per-row complex reference value used in the parent row. This is what the reader joins on (`ComplexColumnReader.BuildColumnDataAsync`). Both Access and this writer name it `_<userColumnName>`; the reader (`ComplexColumnReader.FindForeignKeyIndex`) takes the `LongInteger` column named exactly `_<userColumnName>`; failing that, a `_`-prefixed `LongInteger` column that is not an AutoNumber and does not end with `_<userColumnName>`; then any `_`-prefixed `LongInteger` column; else the first `LongInteger` column. The other `LongInteger` column, `<parentTable>_<userColumnName>`, is the flat table's own autonumber key and is not a join key.

The flat table also requires:

- A **primary key** (the autonumber column) — needs the index-creation foundation in [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md).
- A **non-unique index** on the FK column — same.

### 2.4 Per-kind flat-table schemas

#### 2.4.1 Attachment (`MSysComplexColumns.Type = 4`, on-disk `col_type = 0x12`)

Per Jackcess [`AttachmentColumnInfoImpl`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/complex/AttachmentColumnInfoImpl.java):

| Flat-table column | Jet type | Notes |
|---|---|---|
| (autonumber PK) | `LongInteger`, autoincrement | Required by complex-column protocol. |
| (FK) | `LongInteger` | Holds the per-row complex reference value from the parent row. |
| `FileURL` | `Memo` | May be empty/null. |
| `FileName` | `Text` (max 255) | Display filename. |
| `FileType` | `Text` (max 255) | Lowercase file extension without leading `.` (Jackcess uses lowercase consistently). Drives the COMPRESSED_FORMATS skip-list (§3.2). |
| `FileFlags` | `LongInteger` | Reserved by Access; Jackcess emits null. |
| `FileTimeStamp` | `DateTime` | When the file was attached. Jackcess uses the system clock. |
| `FileData` | `Ole` | Wrapper-encoded payload. **NOT raw bytes.** See §3. |

If the flat table on disk has columns whose names don't match these exactly, Jackcess assigns by type/order: the LONG column is `FileFlags`; the SHORT_DATE_TIME column is `FileTimeStamp`; the OLE column is `FileData`; the MEMO column is `FileUrl`; the first TEXT column is `FileName`; the second TEXT column is `FileType`. We should write canonical names, but the reader path should be tolerant.

#### 2.4.2 Multi-value (`MSysComplexColumns.Type = 3`, on-disk `col_type = 0x12`)

| Flat-table column | Jet type | Notes |
|---|---|---|
| (autonumber PK) | `LongInteger`, autoincrement | |
| (FK) | `LongInteger` | |
| `value` | varies | Whatever element type the user declared (text, long, etc.). |

The shipped declaration surface is the `ColumnDefinition.IsMultiValue` init-only property (see §4.2 C2 row); the inner value column inherits the `ColumnDefinition`'s declared type and length, and, for `decimal` items, its `NumericPrecision` and `NumericScale` (Access's Decimal(18,0) when none is declared).

#### 2.4.3 Version history (`MSysComplexColumns.Type = 2`, on-disk `col_type = 0x12`)

Verified against `complexDataTest*.accdb` `Table1.VersionHistory_F5F8918F-0A3F-4DA9-AE71-184EE5012880`, whose flat table holds five versions of the append-only Memo column, all modified on 2011-09-12:

| Flat-table column | Jet type | Notes |
|---|---|---|
| `_VersionHistory_<GUID>` (FK) | `LongInteger` | The parent row's per-row complex reference. |
| `<memo column name>` | `Memo` | Historical text snapshot, named after the append-only Memo column. |
| `Modified_<GUID>` | `DateTime` | When Access recorded this version. |
| `<table>_VersionHistory_<GUID>` (PK) | `LongInteger`, autoincrement | |

Only meaningful on memo columns marked "Append Only" in Access. The reader follows Jackcess `VersionHistoryColumnInfoImpl`: the version's text is the first Memo column that is neither the foreign key nor the AutoNumber key, and its timestamp is the first Date/Time column. `GetMultiValueItemsAsync` returns each version as a `MultiValueItem` with `Modified` set, and the row cell uses the `'V'` kind of `ComplexCellValue`, which carries the timestamps (`ComplexCellValue.ReadMultiValueItems` reads both `'M'` and `'V'` cells). Versions keep the flat table's order; Jackcess sorts them newest first. The writer cannot add versions: writes to an append-only Memo column in an Access table do not append a version-history row.

## 3. Attachment payload format

The reader decodes this in `AttachmentWrapper.TryDecode`, from the stored `FileData` bytes (the flat table's OLE column is read as its stored bytes, as every read API returns OLE values). The writer encodes it in `AttachmentWrapper.Encode`.

**Verified against all 24 Access-authored attachments in the fixtures**: 2 `.txt` in `ComplexFields.accdb` `Documents.Attachments`, 3 `.txt` in each of `complexDataTestV2007.accdb` / `complexDataTestV2010.accdb` `Table1.attach-data`, and 16 `.jpg` in `NorthwindTraders.accdb` `ProductCategories.ProductCategoryImage`. The layout below is what all of them hold, and it matches Jackcess `AttachmentColumnInfoImpl.encodeData` / `decodeData`.

### 3.1 Wrapper layout

```text
+0   uint32 LE   typeFlag       0x00 = stored raw, 0x01 = zlib-compressed
+4   uint32 LE   dataLen        length of the UNCOMPRESSED content stream (header + file), for both flags
+8   ----        body           the content stream, as is (typeFlag 0) or as a zlib stream (typeFlag 1)
```

The content stream (after inflating, for typeFlag 1):

```text
+0   uint32 LE   headerLen      length of this header, INCLUDING the 4 bytes of headerLen itself: 12 + 2 * extChars
+4   uint32 LE   flag           always 1 (Jackcess CONTENT_HEADER_FLAG); meaning unknown
+8   uint32 LE   extChars       length of the extension in CHARACTERS, including its NUL ("txt" -> 4)
+12  bytes       fileExtension  UTF-16LE, NUL-terminated (2 * extChars bytes)
+headerLen bytes payload        the file bytes: dataLen - headerLen of them
```

So a `.txt` file has `headerLen` 20 and `dataLen` = 20 + the file length. A compressed body is a full zlib stream: the header `78 5E` (deflate, 32 KB window, FLEVEL 1, the level Jackcess's `Deflater(3)` also announces), the deflate blocks, and the big-endian Adler-32 of the content stream. Jackcess writes an empty extension as just its NUL (`extChars` 1, `headerLen` 14); what Access writes there is unchecked.

`AttachmentWrapper.Encode` writes exactly this, with the extension lowercased (as Jackcess does). It frames the zlib stream by hand (`Infrastructure/Adler32.cs`), because the netstandard2.1 build has no `ZLibStream`, and compresses with `DeflateStream` at `CompressionLevel.Optimal`. Its deflate blocks differ from Access's, which no .NET compressor reproduces (they also differ between the zlib builds of .NET 8 and .NET 10), so tests never pin compressed bytes. Re-encoding each Access attachment gives Access's wrapper byte for byte for the raw `.jpg` files, and the same typeFlag, `dataLen`, zlib header, Adler-32 and inflated content for the compressed `.txt` files (`ComplexColumnsAttachmentFormatTests.AccessAuthoredFileData_ReEncodesToSameWrapper`). `AddAttachment_WriterBytes_DecodeWithJackcessSemantics` decodes the writer's stored bytes the way Jackcess `decodeData` does (zlib, then exactly `dataLen - headerLen` file bytes), including a 20 KB value on a chained LVAL.

`AttachmentWrapper.TryDecode` reads the extension from bytes `[12, headerLen)` up to its NUL and ignores `extChars`, as Jackcess does. It reads a typeFlag-1 body as zlib when it has a zlib header and inflates to `dataLen` bytes. Otherwise it inflates the whole body as raw deflate, which is what earlier builds of this library wrote (with `dataLen` = the compressed length and `extChars` counted in bytes), so their files stay readable.

### 3.2 When to compress

Access stores a small set of formats raw and deflates everything else, including formats that are compressed already, such as gz and mp3. The writer uses Jackcess 4.0.12's list, which its author took from Access ("Use the file formats which ms access stores uncompressed when writing an attachment ... and deflate everything else"):

```java
private static final Set<String> COMPRESSED_FORMATS = new HashSet<>(
    Arrays.asList("jpg", "jpeg", "gif", "png", "zip", "cab", "docx",
                  "xlsx", "xlsb", "pptx"));
```

The match is case-insensitive. The fixtures confirm only `.jpg` (raw) and `.txt` (compressed); the rest of the list is Jackcess's observation, not Microsoft documentation. HACKING.md notes "if a memo field is marked for compression, only at value which is at most 1024 characters when uncompressed can be compressed" — that's a memo-compression rule, not an attachment rule, so it does not apply here.

## 4. Reader / writer phases

### 4.1 Reader

The reader (see §1) implements `Attachment` / `Complex` column-type recognition, the `MSysComplexColumns` join, and attachment payload decode (`AttachmentWrapper.TryDecode`, §3). Schema metadata is exposed via `IAccessReader.GetComplexColumnsAsync`; typed item enumeration is exposed via `GetAttachmentsAsync` / `GetMultiValueItemsAsync` (see §4.2 C4).

`ComplexColumnReader` classifies a column by its `MSysComplexType_*` template name (`ComplexTypeObjectID`). A column with no template (`ComplexTypeObjectID` 0, which files written by builds of this library before C10 hold) is classified from its flat table's schema instead (`ClassifyFlatTable`): a `FileData` column means an attachment; leaving out the foreign key and the AutoNumber key, one value column means multi-value, and a Memo plus a Date/Time column a version history. `GetComplexColumnsAsync`, the row cells and the type names all use that classification. `GetColumnMetadataAsync` reports each complex column's `TypeName`, keyed by `ComplexID` so a rename keeps it (`ReadColumnTypeNamesAsync`): `"Attachment"`, `"Version History"`, or `"Multi-value "` plus the display name of the flat table's value column type (`"Multi-value Text"` for `complexDataTest`'s `multi-value-data`, `"Multi-value Long Integer"` for a writer column of `int`), falling back to the type the template declares when the flat table cannot be read, and `"Complex"` when the column cannot be resolved at all. Before, every complex column that joined to `MSysComplexColumns` reported `"Attachment"`. `ComplexColumnsInfoTests` covers this.

Row reads (`Rows`, `ReadTableAsync`, `Rows<T>`, index seeks and `Query<T>`) replace each complex column's 4-byte reference with a `ComplexCellValue` cell (`byte[]`, since complex columns report `ClrType = byte[]`). `ComplexColumnReader.BuildColumnDataAsync` reads each flat table once per scan, groups its rows by the FK back-reference, and encodes one cell per parent reference holding **every** attachment (decoded payload, file name, type, URL, timestamp) or every multi-value / version-history value. A row whose reference has no flat rows reads as `DBNull`. `ComplexCellValue.ReadAttachments` / `ReadMultiValueItems` decode a cell into the same records `GetAttachmentsAsync` / `GetMultiValueItemsAsync` return, because both paths share `ComplexColumnReader`'s flat-table decode. `RowsAsStrings` and `ReadTableAsStringsAsync` return the same cell as a `data:application/octet-stream;base64,` URI, or an empty string. A version-history cell (kind `'V'`) carries each version's text and its `Modified_<GUID>` timestamp, as `GetMultiValueItemsAsync` returns them in `MultiValueItem.Modified` (§2.4.3).

### 4.2 Writer

| Phase | Scope | Status |
|---|---|---|
| **C1** | Empty-DB scaffold: add ACCDB full-catalog system tables, including `MSysComplexColumns`. | ✅ Shipped. ACCDB full-catalog only. Helper: `ComplexColumnManager.ScaffoldSystemTablesAsync` creates core `MSysACEs` / `MSysQueries` / `MSysRelationships`, `MSysComplexColumns`, and later the complex type-template tables. Catalog rows carry `MSysObjects.Flags = 0x80000000` and are excluded from `ListTablesAsync`. Tests: `ComplexColumnsWriterTests` plus fresh ACCDB DAO compact coverage. |
| **C2** | `ColumnDefinition.IsAttachment` / `IsMultiValue` declaration surface + `ColumnInfo.Misc` round-trip in TDEF emission (the `0x07` bitmask, the 4-byte `misc` ComplexID slot, `col_len = 4`). | ✅ Shipped. Public init-only props on `ColumnDefinition`. Descriptor offset `_colMiscOff` = 11 (Jet4/ACE). Tests: `ComplexColumnsWriterTests`. |
| **C3** | `CreateTableAsync` emits the hidden flat child table, allocates a fresh `ComplexID`, writes the `MSysComplexColumns` row, and patches the parent descriptor. | ✅ Shipped. Helper: `CatalogArtifactWriter.CreateTableAsync` (formerly `AccessWriter.CreateTableInternalAsync`). Flat table named `f_<32-hex>_<userColumnName>`, `MSysObjects.Flags = 0x800A0000`. Per-flat PK/FK indexes are emitted by C7 (§4.3); `ComplexTypeObjectID` is populated by C10 (§4.6). |
| **C4** | Per-kind row APIs (`AddAttachmentAsync`, `AddMultiValueItemAsync`) and attachment payload encode (§3). | ✅ Shipped. Helpers: `AccessWriter.AddAttachmentAsync` / `AddMultiValueItemAsync`, `AttachmentWrapper`. Reader-side: `AccessReader.GetAttachmentsAsync` / `GetMultiValueItemsAsync`. Inline-OLE 256-byte cap was lifted by C8 — oversized payloads now ride on LVAL data pages. |
| **C5** | Cascade flat-table rows on parent delete. | ✅ Shipped. Helpers: `PlanComplexChildDeletesAsync`, which `DeleteRowsAsync` and `RelationshipEnforcer.PlanCascadeDeletesAsync` call for the matching and the cascaded rows before the delete writes anything, collecting the flat rows into one `ComplexChildDeletes` group per cascade and one for the matching rows, each flat row once, and `ApplyComplexChildDeletesAsync`, which tombstones a group's rows just before the delete removes the group's parent rows and lowers each flat table's row count, with no cancellation token (index-and-relationship-format-notes.md §7.11). The flat table's indexes are not maintained on a delete (docs/todo.md). Cost: one O(P) flat-table scan per (parent table × complex column). Tests: `ComplexColumnsCascadeDeleteTests`, `UpdateDeleteCancellationTests`. |
| **C6** | `DropTableAsync` cascade for hidden flat tables and `MSysComplexColumns` rows. | ✅ Shipped. Helper: `DropComplexChildrenForTableAsync`, called from `DropTableAsync`. Removes `MSysComplexColumns` and `MSysObjects` catalog rows for each child flat table. It reclaims each flat table's TDEF, data, reachable index and usage-map pages and releases its live rows' long values through the same storage reclaimer as an ordinary table drop. Dropping a complex column does the same for that column's flat table. |
| **C7** | Per-flat-table indexes Access expects: autoincrement scalar PK column, primary key on the scalar, normal index on the FK back-reference, and (attachment only) a composite secondary index on (FK, FileName). Lifts the C3 "no PK / no autoincrement / no FK back-reference index" caveat. | ✅ Shipped. Helper: `BuildFlatTableSchema` (returns `(ColumnDefinition[], IndexDefinition[])`) wired through `EmitComplexColumnArtifactsAsync` and reused by `AddComplexItemCoreAsync` via `ApplyConstraintsAsync` so the autoincrement scalar PK is seeded per insert. See §4.3. |
| **C8** | LVAL chain emission for oversized MEMO / OLE / Attachment payloads. Lifts the inline-only `Constants.LongValue.MaxInlineMemoBytes = 1024` and `Constants.LongValue.MaxInlineOleBytes = 256` caps. | ✅ Shipped. Helpers: `LongValueEncoder.PrepareLongValues` / `WriteLongValuesAsync` plus shared `LongValueDescriptor` / `LongValueStore` descriptor, page-buffer, and chain helpers. The long-value pass runs once in `InsertRowDataLocAsync`, through `TableRowStore.EncodeRow`: any `Ole` / `Memo` value whose encoded payload exceeds the inline cap is replaced with a pending `PreEncodedLongValue` (a zeroed 12-byte header), and the row is serialized, which checks its values and its size, before any page is written; while the row is too long for a page, the largest inline `Ole` / `Memo` values are replaced the same way (`LongValueEncoder.CollectInlineLongValues`). Only then is each payload staged onto Access-style LVAL pages (single-page bitmask `0x40`; chained bitmask `0x00` with one row per page, walked in reverse so each predecessor row carries its successor's `lval_dp` pointer) and the placeholder replaced with a sentinel carrying the finished header, which the encoders splice through verbatim. Upper limit is the on-disk 24-bit LVAL length field (`MaxPayloadBytes = 16 MiB - 1`). LVAL pages are emitted as page-type `0x01` with the `LVAL` signature and descriptor token Access uses. See §4.4. |
| **C9** | Schema evolution on parent tables that already contain complex columns: lift the `NotSupportedException` thrown by `BuildColumnDefinitionFromInfo` so `AddColumnAsync` / `DropColumnAsync` / `RenameColumnAsync` can run against tables with attachment / multi-value columns. | ✅ **Implemented (2026-04-25; DAO compact-hardened 2026-05-20 and 2026-05-23).** `BuildColumnDefinitionFromInfo` now returns `IsAttachment` / `IsMultiValue` ColumnDefinitions with the original `ComplexId` preserved from `ColumnInfo.Misc`. `RewriteTableAsync` preserves the parent rows' per-row complex references and keeps flat children + `MSysComplexColumns` rows attached. When no complex column is dropped or renamed, the temp table is transplanted onto the original TDEF page so `MSysComplexColumns.ConceptualTableID` continues to point at the original parent identity; copy/swap fallbacks patch surviving rows to the replacement TDEF page. Surgical post-rewrite cleanup runs for dropped/renamed complex columns. The rebuilt TDEF re-emits `Complex` (`0x12`) with the correct misc field, and `HydrateConstraintsFromTableDef` skips the `Flags` flag-bit interpretation for complex columns (the on-disk Flags = 0x07 is a magic marker, not real `IsNullable` / `IsAutoIncrement` bits). 7 round-trip tests in `ComplexColumnsSchemaEvolutionTests`; DAO compact coverage in `DaoCompact_ComplexColumnsWithLvalPayload_SurviveCompactAndRepair` and fresh ACCDB complex storage tests. See §4.5. |
| **C10** | Scaffold the nine `MSysComplexType_*` template tables (`UnsignedByte`, `Short`, `Long`, `IEEESingle`, `IEEEDouble`, `GUID`, `Decimal`, `Text`, `Attachment`) and populate `MSysComplexColumns.ComplexTypeObjectID` with the matching template id instead of the placeholder `0`. | ✅ **Implemented (2026-04-25).** Helpers on `ComplexColumnManager` create the type templates immediately after `MSysComplexColumns`, and the static `ResolveComplexTypeTemplateName(ColumnDefinition)` lookup is wired into `EmitComplexColumnArtifactsAsync`. ACE only (Jet3 / Jet4 `.mdb` skip the templates because complex columns are an Access 2007+ feature). Each template carries `MSysObjects.Flags = 0x80030000` (system + the `0x30000` marker Access uses for type-template tables) so they are excluded from `ListTablesAsync`. 6 round-trip tests in `ComplexColumnsWriterTests`. See §4.6. |

### 4.3 C7 flat-table schema

The flat-child schema emitted by `BuildFlatTableSchema` (called from `EmitComplexColumnArtifactsAsync` during `CreateTableAsync`):

- **Attachment flat table** (8 columns, in Access-authored Northwind order): `_<userColumnName>` (LONG, FK back-reference), `FileData` (OLE), `FileFlags` (LONG), `FileName` (TEXT 255), `FileTimeStamp` (DATETIME), `FileType` (TEXT 255), `FileURL` (MEMO), `<parentTable>_<userColumnName>` (LONG, autoincrement scalar PK). Three indexes ship: `MSysComplexPKIndex` (PK on the scalar), `_<userColumnName>` (normal index on the FK), and `IdxFKPrimaryScalar` (composite normal index on `(_<userColumnName>, FileName)`). The Access-specific descriptor flags/extra flags/misc values for these hidden flat-table columns are emitted through writer-owned `ColumnDefinition` descriptor overrides.
- **Multi-value flat table** (3 columns): `_<userColumnName>` (LONG, FK back-reference), `Value` (CLR type from the user `ColumnDefinition`; a `decimal` value column takes its declared `NumericPrecision` and `NumericScale`, which `TDefPageBuilder.ValidateColumnForFormat` checks before the parent table is written), `<parentTable>_<userColumnName>` (LONG, autoincrement scalar PK). Two indexes: PK on the scalar plus a normal index on the FK back-reference. The composite secondary index is omitted because the format-probe corpus contains no multi-value flat-table fixture and the `Value` column may be a non-indexable type (MEMO, OLE, GUID).

`AddComplexItemCoreAsync` resolves the flat-table name from the catalog (helper `ResolveFlatTableNameAsync`) and calls `ConstraintRegistry.ApplyAsync` before `InsertRowDataAsync`, so the autoincrement scalar PK is seeded from the larger of the flat table's TDEF AutoNumber counter and its existing rows; after the insert, `AutoNumberMaintainer.UpdateHighWaterAsync` raises that counter, so IDs freed by deleting the top flat rows are not reused in a later session. The constraint registry is hydrated from the persisted `FLAG_AUTO_LONG` bit when the writer instance did not declare the table itself, so re-opening a file produced by a previous writer instance still drives the autoincrement correctly.

C7 caveats:

- **Flat-table indexes stay current.** `AddAttachmentAsync` / `AddMultiValueItemAsync` insert the child row and rebuild the emitted flat-table indexes, including the attachment `(FK, FileName)` composite index. A parent-row delete removes its hidden child rows, adjusts each flat table's row count and rebuilds its indexes before returning. Complex table and column drops maintain the `MSysComplexColumns` and `MSysObjects` row counts and indexes; renames and parent-table transplants rebuild the rewritten complex catalog indexes. Under `SecureEraseMode.DeletedRowsAndFreedPages`, deleting flat or catalog rows also scrubs and frees their MEMO and OLE long values.
- **`ComplexTypeObjectID`** is populated by C10 (§4.6) with the matching `MSysComplexType_*` template id; pre-C10 writer-authored files had this field at `0`.
- **Validation.** Reader round-trip coverage lives in the `ComplexColumns*Tests` suite. DAO CompactDatabase coverage now exercises writer-authored attachment rows on the `ComplexFields` fixture in `ComplexFixture_WriterAttachmentRowsSurviveCompactAndRepair` (with `ComplexFixtureCatalog_SurvivesCompactAndRepair` pinning the compacted complex catalog), plus a Northwind-hosted writer-created attachment and multi-value table with flat-table indexes, schema rewrite, and a chained-LVAL attachment payload in `DaoCompact_ComplexColumnsWithLvalPayload_SurviveCompactAndRepair`; see [writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md).

### 4.4 C8 LVAL chain emission

Phase C8 lifts the inline cap that limited C4 attachment payloads to ~256 bytes. The long-value pass (`LongValueEncoder.PrepareLongValues`, then `WriteLongValuesAsync` once the row is known to fit a page) runs in `InsertRowDataLocAsync` and fires for `Ole` / `Memo` columns whose encoded payload exceeds the in-row inline cap (`Constants.LongValue.MaxInlineOleBytes = 256`, `Constants.LongValue.MaxInlineMemoBytes = 1024`). Smaller values keep the inline path (`WrapInlineLongValue`, bitmask `0x80`) while the row fits a data page; when it does not, the largest of them, the first of equal ones in column order, move to LVAL pages until it does (`TableRowStore.EncodeRow`).

12-byte LVAL header layout (matches `LongValueDescriptor` and `AccessReader.ReadLongValueAsync`):

```
+--------+--------+--------+--------+
| memo_len (24 LE)         | bitmask|   bytes 0..3
+--------+--------+--------+--------+
| lval_dp (32 LE)                   |   bytes 4..7  ((page<<8) | row_index)
+--------+--------+--------+--------+
| LVAL token (32 LE)                |   bytes 8..11 (Jet4/ACE: copied to bytes 8..11 of every LVAL page in the value; Jet3: 0)
+--------+--------+--------+--------+
```

`bitmask` values produced by C8:

- `0x80` — inline (small payloads; bytes 4..11 zero, payload follows the header in the row body).
- `0x40` — single LVAL page. `lval_dp` points at one row on a freshly-appended LVAL data page; the row body **is** the payload (no next-pointer prefix).
- `0x00` — chained LVAL pages. `lval_dp` points at the first chained row, whose first 4 bytes are the next-pointer (LE `(page<<8)|row`), followed by that page's chunk of the payload. The terminal row's next-pointer is `0`.

LVAL page layout (one row per page, written by `LongValueStore.BuildSinglePageBuffer` / `BuildChainedPageBuffer` from the per-format `LvalPageLayout`):

- `page_type = 0x01` (ordinary data page with an `LVAL` marker in bytes 4..7; `0x05` is the usage-map page type in this codebase, not the writer's LVAL page form).
- bytes 4..7 are ASCII `LVAL`.
- The row count and the row-offset table sit where every data page of the format keeps them (`DataPageLayout`): `num_rows = 1` at offset 12 and the row offset at 14 on Jet4/ACE, at 8 and 10 on Jet3.
- Jet4/ACE: bytes 8..11 store the descriptor token from header bytes 8..11 (Access-authored pages leave them zero). Rows start at byte 20, so free space is 4; chained rows store the next pointer in bytes 20..23 and the payload chunk at byte 24.
- Jet3: no token; offsets 8..11 are the row count and the row offset. The pages match Access 97's byte for byte (test2V1997.mdb pages 37-49): a full chained row starts at byte 12, right after the one-entry offset table, with free space 0; a single-page row and a chain's last chunk are packed at the end of the page, with free space `row start − 12`.

Allocation order for the chained form is **reverse**: `EncodeAsLvalChainAsync` appends the *last* chunk's page first (next-pointer `= 0`), then walks backwards so each newly-appended page can carry its successor's `lval_dp` as its row-prefix next-pointer. The header's `lval_dp` ends up pointing at whatever page was appended *last* (the highest page number, holding the *first* chunk). Access 97 chains run in ascending page order instead; readers follow the pointers either way.

Chunking math:

- One row per LVAL page.
- Single-page row max = `pgSize − LvalPageLayout.MinRowStart`: `4096 − 20 = 4076` bytes on Jet4/ACE, `2048 − 12 = 2036` on Jet3.
- Chain row max = single-page row max − 4 (the in-row next-pointer prefix): 4072 on Jet4/ACE, 2032 on Jet3 (Access 97's chunk size).
- The inline caps apply on every format. Access 97 itself keeps only values of 32 bytes or less inline; the writer keeps the larger caps because it writes one value per LVAL page.

C8 caveats:

- **Upper limit is `Constants.LongValue.MaxPayloadBytes = (1 << 24) − 1`** (~16 MiB) per single MEMO / OLE / Attachment value, set by the on-disk 24-bit `memo_len` field. Larger payloads throw `JetLimitationException`.
- **Default update/delete do not reclaim old LVAL pages**: `UpdateRowsAsync` rewrites the row through `InsertRowDataAsync` and allocates a fresh LVAL chain; `DeleteRowsAsync` marks the owning row deleted. With `SecureEraseMode.None`, old LVAL pages remain on disk for Compact & Repair or a rebuild. With `SecureEraseMode.DeletedRowsAndFreedPages`, `LongValueEncoder.DeallocateLongValueAsync` walks the single-page or chained descriptor and releases each LVAL row: it zeroes the row and marks it deleted, and returns the page to the Access global free list, scrubbed, once no other live row is left on it (Access packs several values onto one page). `DropTableAsync` and schema rewrites release the old rows the same way in every mode, zeroing them only under secure erase. A chain hop that is not a live row of an LVAL page ends the walk.
- **System-table OLE columns (`MSysObjects.LvProp` / `LvModule` / `LvExtra`) use the same caps.** `MSysObjects` rows are inserted through `TableRowStore`, whose long-value pass moves an `LvProp` blob over 256 bytes to LVAL pages like any other OLE value (`Jet3LongValueTests.CreateTable_LvPropOver256Bytes_PropertiesRoundTrip`).
- **Validation.** Round-trip through this library's reader is verified in `JetDatabaseWriter.Tests/ComplexColumns/ComplexColumnsLvalChainTests.cs` (single-page form, chained form, deflate-compressed text payload). Automated DAO CompactDatabase coverage now includes writer-authored attachment payload bytes on the `ComplexFields` fixture in `ComplexFixture_WriterAttachmentRowsSurviveCompactAndRepair` and a Northwind-hosted writer-created attachment table with a chained-LVAL `.jpg` payload in `DaoCompact_ComplexColumnsWithLvalPayload_SurviveCompactAndRepair`; see [writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md).

### 4.5 C9 schema-evolution on tables containing complex columns

Phase C9 lifts the `NotSupportedException` previously thrown by `BuildColumnDefinitionFromInfo` so that `AddColumnAsync` / `DropColumnAsync` / `RenameColumnAsync` work on parent tables that already carry attachment / multi-value columns. The on-disk byte-format of the parent TDEF and the hidden flat children is unchanged — C9 is a control-flow change in the rewrite path that preserves the existing artifacts when possible and surgically removes / renames them when the user mutation explicitly targets a complex column.

How surviving complex columns ride through `RewriteTableAsync`:

1. **TDEF descriptor reconstruction.** `BuildColumnDefinitionFromInfo` now returns a `ColumnDefinition` flagged with `IsAttachment` / `IsMultiValue` and the original `ComplexId` recovered from `ColumnInfo.Misc`. The default-projection and rename-column projection forward `IsAttachment` / `IsMultiValue` / `MultiValueElementType` / `ComplexId` so the rebuilt TDEF re-emits `Complex` (Flags = 0x07) with the correct misc field.
2. **Allocation skip.** `PrepareComplexColumnAllocationsAsync` only allocates fresh `ComplexId` values for columns whose `ComplexId == 0`; preserved columns bypass it entirely, so no new flat child table is emitted.
3. **Preserve or patch parent identity.** When surviving complex columns are present and none are dropped or renamed, the temp table is transplanted onto the original TDEF page. That keeps `MSysComplexColumns.ConceptualTableID` stable for DAO CompactDatabase. Every complex descriptor in the rebuilt table, including newly added columns emitted under the temporary table, is rebound to the original parent TDEF. When a complex column drop/rename requires the older copy/swap path, the original-table drop routes through `DropTableCoreAsync(tableName, dropComplexChildren: false, ct)` so flat children + surviving `MSysComplexColumns` rows are not cascaded away, then their `ConceptualTableID` values are patched to the replacement TDEF page.
4. **Constraint-registry hydration.** `HydrateConstraintsFromTableDef` now skips the `Flags` flag-bit interpretation for `Complex` columns. The on-disk Flags = 0x07 is a magic marker (per mdbtools docs) — interpreting bit 0x04 as `FLAG_AUTO_LONG` would mark a `byte[]` column as auto-increment and fail the `IsIntegralType` check during the temp-table `RegisterConstraints` pass.
5. **Catalog repair after rebuild.** The transplant path replaces the original catalog row in place and removes only the temporary table catalog/ACE rows. The copy/swap path rewrites surviving `MSysComplexColumns.ConceptualTableID` values to the new parent TDEF page, and old parent ACE/catalog rows are removed without rebuilding Access-authored system indexes wholesale.

Surgical post-rewrite cleanup runs from the rewrite path itself:

- **`DropSingleComplexChildAsync(columnName, complexId)`** — invoked once per dropped complex column (matched by ComplexId between `existingDefs` and `newDefs`). Deletes the matching `MSysComplexColumns` row (matched by ColumnName + ComplexID), adjusts the `MSysComplexColumns` TDEF row count, and drops the hidden flat-table catalog row in `MSysObjects`. The normal table-storage reclaimer releases the child's TDEF, data, reachable index, usage-map and live LVAL storage; secure erase and transaction rollback apply. A candidate must be a catalogued local complex flat table, and no surviving complex catalog row may reference it. Idempotent; tolerates missing rows.
- **`RenameComplexColumnArtifactsAsync(oldName, newName, complexId)`** — invoked once per renamed complex column (matched by ComplexId, with names in `existingDefs` and `newDefs` that differ ordinally, so a case-only rename counts). Mark-deletes the matching `MSysComplexColumns` row, then re-inserts it (`updateTDefRowCount: false`) with `ColumnName` rewritten to the new name. The hidden flat-table's catalog name (`f_<hex>_<oldName>`) is left unchanged — readers resolve the flat name via `FlatTableID` → `MSysObjects.Name`, and the cosmetic suffix carries no semantic meaning.

C9 caveats:

- **Adding a brand-new complex column to an existing table works** because `PrepareComplexColumnAllocationsAsync` allocates fresh IDs for the appended `ColumnDefinition` (its `ComplexId == 0`), and `EmitComplexColumnArtifactsAsync` runs at the end of `CreateTableAsync` (called by the rewrite for the temp table) to emit the new flat child + `MSysComplexColumns` row. The pre-existing complex columns continue to ride through unchanged.
- **`AddAttachmentAsync` / `AddMultiValueItemAsync` after rename still work** because the FK back-reference column on the flat table (`_<userColumnName>`) keeps its original name; the reader resolves the flat table via `MSysComplexColumns.FlatTableID` and `GetAttachmentsAsync` returns every flat row tagged with its FK back-reference; row reads join that FK to the parent's complex slot, which the rebuild preserves (next bullet). The `AddComplexItemCoreAsync` parent-row predicate matches on the user's PK columns, not on the renamed complex column.
- **Per-row complex slots are preserved on rebuild.** The writer's own row snapshot (`TableSnapshotReader.ReadTableSnapshotAsync`) keeps `ComplexIdRef` values for complex columns, and `RowEncoder` writes those values back to the rebuilt parent row so flat-table FK joins continue to resolve after `AddColumnAsync` / `DropColumnAsync` / `RenameColumnAsync`.
- **MEMO / OLE values are carried exactly on rebuild.** The same snapshot (also used by `UpdateRowsAsync` and cascade updates) decodes OLE cells as their stored bytes, as every read API does (`OleObjectValue` unwraps them on request). A MEMO / OLE value whose LVAL data cannot be read becomes an `UnreadableLongValue`, and a rewrite that would store it throws `InvalidDataException` before any page changes, instead of storing placeholder text such as `(memo on LVAL page)`. See `JetDatabaseWriter.Tests/Writer/LongValueWriteBackTests.cs`.
- **Validation.** Round-trip through this library's reader is verified in `JetDatabaseWriter.Tests/ComplexColumns/ComplexColumnsSchemaEvolutionTests.cs` (7 tests covering AddColumn / DropColumn / RenameColumn for both the complex column itself and a non-complex sibling, plus AddColumn of a brand-new attachment column on a table that already has one). Automated DAO CompactDatabase coverage now includes `AddColumnAsync` on a Northwind-hosted writer-created table with attachment and multi-value columns in `DaoCompact_ComplexColumnsWithLvalPayload_SurviveCompactAndRepair`; see [writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md).

### 4.6 C10 `MSysComplexType_*` template tables

Phase C10 lifts the C3 caveat that wrote `MSysComplexColumns.ComplexTypeObjectID = 0`. Every fresh ACCDB built by `CreateDatabaseAsync` now also emits the nine type-template tables Access expects, and the C3 row-emit path resolves the matching template id by name and persists it on the catalog row.

**Templates emitted** (all ACE-only, all carry `MSysObjects.Flags = 0x80030000`):

| Template name | Schema | Used when |
|---|---|---|
| `MSysComplexType_UnsignedByte` | `Value: BYTE` | `MultiValueElementType = typeof(byte)` |
| `MSysComplexType_Short` | `Value: INT` | `MultiValueElementType = typeof(short)` |
| `MSysComplexType_Long` | `Value: LONG` | `MultiValueElementType = typeof(int)` |
| `MSysComplexType_IEEESingle` | `Value: FLOAT` | `MultiValueElementType = typeof(float)` |
| `MSysComplexType_IEEEDouble` | `Value: DOUBLE` | `MultiValueElementType = typeof(double)` |
| `MSysComplexType_GUID` | `Value: GUID` | `MultiValueElementType = typeof(Guid)` |
| `MSysComplexType_Decimal` | `Value: NUMERIC` | `MultiValueElementType = typeof(decimal)` |
| `MSysComplexType_Text` | `Value: TEXT(255)` | `MultiValueElementType = typeof(string)` |
| `MSysComplexType_Attachment` | `FileData OLE`, `FileFlags LONG`, `FileName TEXT(255)`, `FileTimeStamp DATETIME`, `FileType TEXT(255)`, `FileURL MEMO` | `IsAttachment = true` |

Schemas come from [`format-probe-appendix-complex.md`](../format-probe/format-probe-appendix-complex.md) §`MSysComplexType_*` against `ComplexFields.accdb`.

Helpers now live on `ComplexColumnManager` and are called from `AccessWriter`:

- `CreateMSysComplexTypeTemplatesAsync` — emits the nine tables in declaration order (TDEF page + catalog row, no indexes, no rows). Skipped for Jet3 / Jet4 `.mdb` and for the slim-catalog ACCDB (`WriteFullCatalogSchema = false`) per the existing C1 gating.
- `ResolveComplexTypeTemplateName(ColumnDefinition)` (static) — returns the canonical template name for a complex column declaration, or `null` if the element type has no matching template.
- `EmitComplexColumnArtifactsAsync` now calls `ResolveComplexTypeTemplateName` + `FindSystemTableTdefPageAsync` to obtain the template's catalog id (= TDEF page) and passes it to `InsertMSysComplexColumnsRowAsync` instead of `0`.

C10 caveats:

- **Slim-catalog ACCDBs (`WriteFullCatalogSchema = false`) skip the templates by design.** That mode targets byte-hash backward compatibility with the legacy 9-column catalog and must not introduce additional pages; `EmitComplexColumnArtifactsAsync` falls back to `ComplexTypeObjectID = 0` when the template lookup misses. Access-authored files always carry the templates, and fresh `CreateDatabaseAsync` ACCDBs scaffold them, so the fallback only fires on the slim-catalog mode.
- **Decimal template `col_len`.** The format-probe appendix shows `col_len = 9` (precision 9 / scale 0); the C10 implementation emits whatever the writer's default `decimal` mapping produces. The template table is never populated with rows, so the precise `col_len` value carries no observable semantic.
- **Validation status.** Round-trip through this library's reader is verified in 6 tests in `ComplexColumnsWriterTests` (template scaffolding, hidden-from-`ListTablesAsync`, attachment template column list, Jet4 skip, attachment + multi-value `ComplexTypeObjectID` non-zero). Writer-authored complex payloads on the `ComplexFields` fixture have representative DAO CompactDatabase coverage in `ComplexFixture_WriterAttachmentRowsSurviveCompactAndRepair`, with `ComplexFixtureCatalog_SurvivesCompactAndRepair` covering the compacted complex catalog; the Northwind-hosted compact test remains the Access-authored-base coverage for schema evolution and chained-LVAL attachment payloads. See [writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md).

## 5. Validation strategy

General DAO validation rules live in [dao-validation-strategy.md](dao-validation-strategy.md), and cross-feature coverage lives in [writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md). Same as the index doc, with one addition specific to attachments:

- Round-trip through this library: read fixtures (`ComplexFields.accdb`) → re-emit → re-read → byte-compare attachment payloads (post-decode).
- Cross-validate compression: a `.jpg` payload must be stored with `typeFlag=0x00` (raw); a `.txt` payload must be stored with `typeFlag=0x01` (a zlib stream, §3.1). Open in Access and **save the attachment back to disk via the GUI** (or DAO `Field2.SaveToFile`) — verify the saved file is byte-identical to the input. Not yet done: Access is not installed where the tests run, so whether Access reads the writer's deflate blocks is unchecked; any valid zlib stream should inflate.
- Test fixture: `JetDatabaseWriter.Tests/Databases/ComplexFields.accdb`. This is the existing read-side fixture; the writer tests should round-trip it.

## 6. References

- [mdbtools HACKING.md](https://github.com/mdbtools/mdbtools/blob/master/HACKING.md) — complex-column flag byte (`0x07`) and complexid in `misc` field. (HACKING.md attributes type byte `0x11` to attachment, but per Jackcess `DataType.java` and our format-probe corpus Access uses `0x12` / `COMPLEX_TYPE` for **all** complex columns — see §1.)
- [Jackcess `AttachmentColumnInfoImpl.java`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/complex/AttachmentColumnInfoImpl.java) — wrapper-header encoder/decoder, COMPRESSED_FORMATS skip-list
- [Jackcess `ComplexColumnInfoImpl.java`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/complex/ComplexColumnInfoImpl.java) — flat-table protocol (PK + FK columns, `diffFlatColumns`)
- [Jackcess `ComplexDataType.java`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/complex/ComplexDataType.java) — type-discriminator integer values
- This repo: `JetDatabaseWriter/ComplexColumns/ComplexColumnReader.cs` (`BuildColumnDataAsync`, flat-table decode), `JetDatabaseWriter/ComplexColumns/Models/AttachmentWrapper.cs`, `JetDatabaseWriter/Models/ComplexCellValue.cs`
- Companion design doc: [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md)
