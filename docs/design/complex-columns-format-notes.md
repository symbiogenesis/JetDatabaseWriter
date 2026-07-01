# Design notes: Complex columns (Attachment, Multi-value) — write path

**Status:** All shipped phases listed in §4.2; outstanding work tracked in the same table.
**Empirical appendix:** [`format-probe-appendix-complex.md`](../format-probe/format-probe-appendix-complex.md) — annotated hex dumps of `MSysComplexColumns`, every `MSysComplexType_*` template table, an attachment-bearing parent table (`Documents`), and the hidden flat tables from `ComplexFields.accdb`. Regenerate via `dotnet run --project JetDatabaseWriter.FormatProbe -- complex`.
**Validation requirement:** see [`index-and-relationship-format-notes.md` §8](index-and-relationship-format-notes.md#8-validation-strategy).

> ⚠️ Reverse-engineered notes. mdbtools documents complex columns only superficially. The authoritative open-source reference is [Jackcess](https://github.com/jahlborn/jackcess) (Java, Apache-2.0) — specifically `com.healthmarketscience.jackcess.impl.complex.*`. Field names and offsets in this document are derived from Jackcess source and the existing reader code in this repo (`AccessReader.BuildComplexColumnDataAsync`, `DecodeAttachmentFileData`).

---

## 1. Background

Access 2007 introduced three "complex column" kinds. All three are stored the same way: a 4-byte per-row complex reference in the parent row, pointing into a hidden child ("flat") table that holds the actual values. **All three kinds share the column-type byte `0x12` (`COMPLEX_TYPE`)** — confirmed both by Jackcess `DataType.java` and by our format-probe across the entire test corpus (no on-disk fixture in `JetDatabaseWriter.Tests/Databases/` carries `0x11` on a complex column). The writer now emits `0x12` for attachment and multi-value parent descriptors; `Attachment = 0x11` in `Constants.cs` is retained only as a legacy/private alias for older reader tolerance.

| Kind | Storage in child table |
|---|---|
| Attachment | One row per attached file. Columns: `FileURL`, `FileName`, `FileType`, `FileFlags`, `FileTimeStamp`, `FileData`. |
| Multi-value | One row per value. Columns: a single value column whose type matches the user-declared element type. |
| Version history | One row per historical edit. Columns: `value` (memo), `version` (datetime). Only meaningful on memo columns flagged "Append Only" in Access. |

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

### 2.2 `MSysComplexColumns` catalog table

**Verified against `ComplexFields.accdb`** ([appendix](../format-probe/format-probe-appendix-complex.md#msyscomplexcolumns--tdef-page-18)). Actual schema is **5 columns** (column names and order below are probe-confirmed):

| Column (verified) | Type | Meaning |
|---|---|---|
| `ColumnName` | `Text(510)` | Name of the parent column (e.g. `"Attachments"`). |
| `ComplexID` | `LongInteger` (fixed_off=12) | Per-database ID for this complex column. **Matches the 4-byte value the parent TDEF stores in the column descriptor's `misc`+`misc_ext` slot** (see §2.1). |
| `ComplexTypeObjectID` | `LongInteger` (fixed_off=0) | `MSysObjects.Id` of the **type-template table** — one of `MSysComplexType_Long`, `MSysComplexType_Text`, `MSysComplexType_Attachment`, etc. The template's schema dictates the kind (attachment vs multi-value of a given inner type). |
| `ConceptualTableID` | `LongInteger` (fixed_off=8) | Parent table object/TDEF page for the complex-column definition. Schema rewrite preserves this identity when possible by transplanting rebuilt storage onto the original TDEF page; copy/swap fallbacks patch the value to the replacement TDEF page. |
| `FlatTableID` | `LongInteger` (fixed_off=4) | `MSysObjects.Id` of the hidden child ("flat") table. |

The rows above are in the catalog's logical column order; the `fixed_off` values give each column's offset in the physical fixed-width row area, which Access orders independently (`ComplexTypeObjectID`=0, `FlatTableID`=4, `ConceptualTableID`=8, `ComplexID`=12).

There is **no** `ParentTable` / `ParentColumn` column in `MSysComplexColumns`. The parent reference is implicit: it is recovered by scanning every user TDEF for a complex column whose `misc`+`misc_ext` 4-byte slot equals `ComplexID`. The reader (`AccessReader.BuildComplexColumnDataAsync`) already does this scan.

`MSysComplexColumns` is now created by the ACCDB full-catalog scaffold on every fresh ACCDB built via `CreateDatabaseAsync` (Phase C1, 2026-04-25). That same scaffold also creates the core `MSysACEs`, `MSysQueries`, and `MSysRelationships` tables expected by DAO compatibility paths. The catalog rows carry `Flags = 0x80000000` so the tables are excluded from `ListTablesAsync`. ACE only — Jet3/Jet4 `.mdb` scaffolds skip these tables.

### 2.3 The hidden "flat" child table

**Verified naming and flag conventions** ([appendix](../format-probe/format-probe-appendix-complex.md)):

- Flat-table name pattern: **`f_<32-hex-uppercase>_<userColumnName>`**, e.g. `f_A3DF50CFC033433899AF0AC1A4CF4171_Attachments`. The 32 hex characters are a GUID without dashes.
- Flat-table `MSysObjects.Flags`: **`0x800A0000`** (probe-confirmed in both `NorthwindTraders.accdb` and `ComplexFields.accdb`).
- Template ("type") tables: `MSysComplexType_<TypeName>` (`Long`, `Text`, `Attachment`, `UnsignedByte`, `Short`, `IEEESingle`, `IEEEDouble`, `GUID`, `Decimal`). `Flags = 0x80030000`.
- Stored as a normal user table in `MSysObjects` (Type = 1), but the system-flag bit (`0x80000000`) hides it from Access UI.

The flat table has all the value-bearing columns of the complex type, plus two extra columns:

1. An **autonumber Long primary key** column.
2. A **Long FK column** that holds the same per-row complex reference value used in the parent row. This is what the reader joins on (`AccessReader.BuildComplexColumnDataAsync`).

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

The shipped declaration surface is the `ColumnDefinition.IsMultiValue` init-only property (see §4.2 C2 row); the inner value column inherits the `ColumnDefinition`'s declared type and length.

#### 2.4.3 Version history (`MSysComplexColumns.Type = 2`, on-disk `col_type = 0x12`)

| Flat-table column | Jet type | Notes |
|---|---|---|
| (autonumber PK) | `LongInteger`, autoincrement | |
| (FK) | `LongInteger` | |
| `value` | `Memo` | Historical text snapshot. |
| `version` | `DateTime` | When this snapshot was recorded. |

Only meaningful on memo columns marked "Append Only" in Access. Lowest-priority of the three to support — virtually no users opt into this.

## 3. Attachment payload format

The reader already decodes this; see `AccessReader.DecodeAttachmentFileData`. The writer needs to round-trip the encoder.

### 3.1 Wrapper layout

```text
+0   uint32 LE   typeFlag       0x00 = raw, 0x01 = deflate-compressed
+4   uint32 LE   dataLen        length of (header + payload), excluding wrapper
+8   ----        contentStream  raw OR deflate-wrapped bytes follow
```

Inside `contentStream` (after deflate-decompression if applicable):

```text
+0   uint32 LE   headerLen      length of this header block, INCLUDING the 4 bytes for headerLen itself
+4   uint32 LE   unknownFlag    Jackcess writes 1; meaning unknown
+8   uint32 LE   extLen         byte length of the file-extension blob that follows
+12  bytes       fileExtension  null-terminated, encoded with VERSION_12.CHARSET (UCS-2 little-endian, uncompressed). Includes the trailing NUL.
+12+extLen bytes payload        the actual file bytes
```

Total wrapper overhead with no compression: `8 + 12 + extLen` bytes.

### 3.2 When to compress

Per Jackcess, deflate is **skipped** for already-compressed media:

```java
private static final Set<String> COMPRESSED_FORMATS = new HashSet<String>(
    Arrays.asList("jpg", "zip", "gz", "bz2", "z", "7z", "cab", "rar",
                  "mp3", "mpg"));
```

Match case-insensitively against `FileType` (which Jackcess lowercases on store). Jackcess uses `Deflater(3)` (level 3, raw deflate without zlib wrapper). HACKING.md notes "if a memo field is marked for compression, only at value which is at most 1024 characters when uncompressed can be compressed" — that's a memo-compression rule, not an attachment rule, so it does not apply here.

> The compression flag is `0x01 = zlib-deflate`. The existing reader uses `System.IO.Compression.DeflateStream`, which does **raw deflate** (no zlib header). The reader works against fixtures, so raw deflate is correct — Jackcess's "zlib" comment in the field name is a misnomer.

## 4. Reader / writer phases

### 4.1 Reader

The reader (see §1) implements `Attachment` / `Complex` column-type recognition, the `MSysComplexColumns` join (`BuildComplexColumnDataAsync`), and attachment payload decode (raw + deflate; `DecodeAttachmentFileData`). Schema metadata is exposed via `IAccessReader.GetComplexColumnsAsync`; typed item enumeration is exposed via `GetAttachmentsAsync` / `GetMultiValueItemsAsync` (see §4.2 C4).

### 4.2 Writer

| Phase | Scope | Status |
|---|---|---|
| **C1** | Empty-DB scaffold: add ACCDB full-catalog system tables, including `MSysComplexColumns`. | ✅ Shipped. ACCDB full-catalog only. Helper: `ComplexColumnManager.ScaffoldSystemTablesAsync` creates core `MSysACEs` / `MSysQueries` / `MSysRelationships`, `MSysComplexColumns`, and later the complex type-template tables. Catalog rows carry `MSysObjects.Flags = 0x80000000` and are excluded from `ListTablesAsync`. Tests: `ComplexColumnsWriterTests` plus fresh ACCDB DAO compact coverage. |
| **C2** | `ColumnDefinition.IsAttachment` / `IsMultiValue` declaration surface + `ColumnInfo.Misc` round-trip in TDEF emission (the `0x07` bitmask, the 4-byte `misc` ComplexID slot, `col_len = 4`). | ✅ Shipped. Public init-only props on `ColumnDefinition`. Descriptor offset `_colMiscOff` = 11 (Jet4/ACE). Tests: `ComplexColumnsWriterTests`. |
| **C3** | `CreateTableAsync` emits the hidden flat child table, allocates a fresh `ComplexID`, writes the `MSysComplexColumns` row, and patches the parent descriptor. | ✅ Shipped. Helper: `CreateTableInternalAsync`. Flat table named `f_<32-hex>_<userColumnName>`, `MSysObjects.Flags = 0x800A0000`. Per-flat PK/FK indexes are emitted by C7 (§4.3); `ComplexTypeObjectID` is populated by C10 (§4.6). |
| **C4** | Per-kind row APIs (`AddAttachmentAsync`, `AddMultiValueItemAsync`) and attachment payload encode (§3). | ✅ Shipped. Helpers: `AccessWriter.AddAttachmentAsync` / `AddMultiValueItemAsync`, `AttachmentWrapper`. Reader-side: `AccessReader.GetAttachmentsAsync` / `GetMultiValueItemsAsync`. Inline-OLE 256-byte cap was lifted by C8 — oversized payloads now ride on LVAL data pages. |
| **C5** | Cascade flat-table rows on parent delete. | ✅ Shipped. Helper: `CascadeDeleteComplexChildrenAsync`, called from `DeleteRowsAsync` and the W10 `EnforceFkOnPrimaryDeleteAsync` cascade path. Cost: one O(P) flat-table scan per (parent table × complex column). Tests: `ComplexColumnsCascadeDeleteTests`. |
| **C6** | `DropTableAsync` cascade for hidden flat tables and `MSysComplexColumns` rows. | ✅ Shipped. Helper: `DropComplexChildrenForTableAsync`, called from `DropTableAsync`. Removes `MSysComplexColumns` and `MSysObjects` catalog rows for each child flat table, then returns dropped table data/TDEF/usage/index/LVAL pages to the Access global free list. |
| **C7** | Per-flat-table indexes Access expects: autoincrement scalar PK column, primary key on the scalar, normal index on the FK back-reference, and (attachment only) a composite secondary index on (FK, FileName). Lifts the C3 "no PK / no autoincrement / no FK back-reference index" caveat. | ✅ Shipped. Helper: `BuildFlatTableSchema` (returns `(ColumnDefinition[], IndexDefinition[])`) wired through `EmitComplexColumnArtifactsAsync` and reused by `AddComplexItemCoreAsync` via `ApplyConstraintsAsync` so the autoincrement scalar PK is seeded per insert. See §4.3. |
| **C8** | LVAL chain emission for oversized MEMO / OLE / Attachment payloads. Lifts the inline-only `Constants.LongValue.MaxInlineMemoBytes = 1024` and `Constants.LongValue.MaxInlineOleBytes = 256` caps. | ✅ Shipped. Helpers: `LongValueEncoder.PreEncodeLongValuesAsync` plus shared `LongValueDescriptor` / `LongValueStore` descriptor, page-buffer, and chain helpers. Pre-encode pass runs once at the top of `InsertRowDataLocAsync`: any `Ole` / `Memo` value whose encoded payload exceeds the inline cap is staged onto freshly-appended Access-style LVAL pages (single-page bitmask `0x40`; chained bitmask `0x00` with one row per page, walked in reverse so each predecessor row carries its successor's `lval_dp` pointer) and the in-row value is replaced with a `PreEncodedLongValue` sentinel that the encoders splice through verbatim. Upper limit is the on-disk 24-bit LVAL length field (`MaxPayloadBytes = 16 MiB - 1`). LVAL pages are emitted as page-type `0x01` with the `LVAL` signature and descriptor token Access uses. See §4.4. |
| **C9** | Schema evolution on parent tables that already contain complex columns: lift the `NotSupportedException` thrown by `BuildColumnDefinitionFromInfo` so `AddColumnAsync` / `DropColumnAsync` / `RenameColumnAsync` can run against tables with attachment / multi-value columns. | ✅ **Implemented (2026-04-25; DAO compact-hardened 2026-05-20 and 2026-05-23).** `BuildColumnDefinitionFromInfo` now returns `IsAttachment` / `IsMultiValue` ColumnDefinitions with the original `ComplexId` preserved from `ColumnInfo.Misc`. `RewriteTableAsync` preserves the parent rows' per-row complex references and keeps flat children + `MSysComplexColumns` rows attached. When no complex column is dropped or renamed, the temp table is transplanted onto the original TDEF page so `MSysComplexColumns.ConceptualTableID` continues to point at the original parent identity; copy/swap fallbacks patch surviving rows to the replacement TDEF page. Surgical post-rewrite cleanup runs for dropped/renamed complex columns. The rebuilt TDEF re-emits `Complex` (`0x12`) with the correct misc field, and `HydrateConstraintsFromTableDef` skips the `Flags` flag-bit interpretation for complex columns (the on-disk Flags = 0x07 is a magic marker, not real `IsNullable` / `IsAutoIncrement` bits). 7 round-trip tests in `ComplexColumnsSchemaEvolutionTests`; DAO compact coverage in `DaoCompact_ComplexColumnsWithLvalPayload_SurviveCompactAndRepair` and fresh ACCDB complex storage tests. See §4.5. |
| **C10** | Scaffold the nine `MSysComplexType_*` template tables (`UnsignedByte`, `Short`, `Long`, `IEEESingle`, `IEEEDouble`, `GUID`, `Decimal`, `Text`, `Attachment`) and populate `MSysComplexColumns.ComplexTypeObjectID` with the matching template id instead of the placeholder `0`. | ✅ **Implemented (2026-04-25).** Helpers on `ComplexColumnManager` create the type templates immediately after `MSysComplexColumns`, and the static `ResolveComplexTypeTemplateName(ColumnDefinition)` lookup is wired into `EmitComplexColumnArtifactsAsync`. ACE only (Jet3 / Jet4 `.mdb` skip the templates because complex columns are an Access 2007+ feature). Each template carries `MSysObjects.Flags = 0x80030000` (system + the `0x30000` marker Access uses for type-template tables) so they are excluded from `ListTablesAsync`. 6 round-trip tests in `ComplexColumnsWriterTests`. See §4.6. |

### 4.3 C7 flat-table schema

The flat-child schema emitted by `BuildFlatTableSchema` (called from `EmitComplexColumnArtifactsAsync` during `CreateTableAsync`):

- **Attachment flat table** (8 columns, in Access-authored Northwind order): `_<userColumnName>` (LONG, FK back-reference), `FileData` (OLE), `FileFlags` (LONG), `FileName` (TEXT 255), `FileTimeStamp` (DATETIME), `FileType` (TEXT 255), `FileURL` (MEMO), `<parentTable>_<userColumnName>` (LONG, autoincrement scalar PK). Three indexes ship: `MSysComplexPKIndex` (PK on the scalar), `_<userColumnName>` (normal index on the FK), and `IdxFKPrimaryScalar` (composite normal index on `(_<userColumnName>, FileName)`). The Access-specific descriptor flags/extra flags/misc values for these hidden flat-table columns are emitted through writer-owned `ColumnDefinition` descriptor overrides.
- **Multi-value flat table** (3 columns): `_<userColumnName>` (LONG, FK back-reference), `Value` (CLR type from the user `ColumnDefinition`), `<parentTable>_<userColumnName>` (LONG, autoincrement scalar PK). Two indexes: PK on the scalar plus a normal index on the FK back-reference. The composite secondary index is omitted because the format-probe corpus contains no multi-value flat-table fixture and the `Value` column may be a non-indexable type (MEMO, OLE, GUID).

`AddComplexItemCoreAsync` resolves the flat-table name from the catalog (helper `ResolveFlatTableNameAsync`) and calls `ApplyConstraintsAsync` before `InsertRowDataAsync` so the autoincrement scalar PK is seeded from the existing flat-table rows. The constraint registry is hydrated from the persisted `FLAG_AUTO_LONG` bit when the writer instance did not declare the table itself, so re-opening a file produced by a previous writer instance still drives the autoincrement correctly.

C7 caveats:

- **Flat-table index maintenance is incremental.** `AddAttachmentAsync` / `AddMultiValueItemAsync` insert the child row and maintain the emitted flat-table indexes, including the attachment `(FK, FileName)` composite index.
- **`ComplexTypeObjectID`** is populated by C10 (§4.6) with the matching `MSysComplexType_*` template id; pre-C10 writer-authored files had this field at `0`.
- **Validation.** Reader round-trip coverage lives in the `ComplexColumns*Tests` suite. DAO CompactDatabase coverage now exercises writer-authored attachment rows on the `ComplexFields` fixture in `ComplexFixture_WriterAttachmentRowsSurviveCompactAndRepair` (with `ComplexFixtureCatalog_SurvivesCompactAndRepair` pinning the compacted complex catalog), plus a Northwind-hosted writer-created attachment and multi-value table with flat-table indexes, schema rewrite, and a chained-LVAL attachment payload in `DaoCompact_ComplexColumnsWithLvalPayload_SurviveCompactAndRepair`; see [writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md).

### 4.4 C8 LVAL chain emission

Phase C8 lifts the inline cap that limited C4 attachment payloads to ~256 bytes. The pre-encode pass `LongValueEncoder.PreEncodeLongValuesAsync` runs at the top of `InsertRowDataLocAsync` and only fires for `Ole` / `Memo` columns whose encoded payload exceeds the in-row inline cap (`Constants.LongValue.MaxInlineOleBytes = 256`, `Constants.LongValue.MaxInlineMemoBytes = 1024`). Smaller values keep the existing inline path (`WrapInlineLongValue`, bitmask `0x80`) unchanged.

12-byte LVAL header layout (matches `LongValueDescriptor` and `AccessReader.ReadLongValueAsync`):

```
+--------+--------+--------+--------+
| memo_len (24 LE)         | bitmask|   bytes 0..3
+--------+--------+--------+--------+
| lval_dp (32 LE)                   |   bytes 4..7  ((page<<8) | row_index)
+--------+--------+--------+--------+
| LVAL token (32 LE)                |   bytes 8..11 (copied to bytes 8..11 of every LVAL page in the value)
+--------+--------+--------+--------+
```

`bitmask` values produced by C8:

- `0x80` — inline (small payloads; bytes 4..11 zero, payload follows the header in the row body).
- `0x40` — single LVAL page. `lval_dp` points at one row on a freshly-appended LVAL data page; the row body **is** the payload (no next-pointer prefix).
- `0x00` — chained LVAL pages. `lval_dp` points at the first chained row, whose first 4 bytes are the next-pointer (LE `(page<<8)|row`), followed by that page's chunk of the payload. The terminal row's next-pointer is `0`.

LVAL page layout (one row per page, written by `LongValueStore.BuildSinglePageBuffer` / `BuildChainedPageBuffer`):

- `page_type = 0x01` (ordinary data page with an `LVAL` marker in bytes 4..7; `0x05` is the usage-map page type in this codebase, not the writer's LVAL page form).
- bytes 4..7 are ASCII `LVAL`.
- bytes 8..11 store the descriptor token from header bytes 8..11.
- `num_rows = 1`, single row offset entry pointing at byte 20 (`Constants.LongValue.LvalRowStart`).
- Rows grow from byte 20; chained rows store the next pointer in bytes 20..23 and the payload chunk at byte 24.

Allocation order for the chained form is **reverse**: `EncodeAsLvalChainAsync` appends the *last* chunk's page first (next-pointer `= 0`), then walks backwards so each newly-appended page can carry its successor's `lval_dp` as its row-prefix next-pointer. The header's `lval_dp` ends up pointing at whatever page was appended *last* (the highest page number, holding the *first* chunk).

Chunking math:

- One row per LVAL page.
- Single-page row max = `pgSize − Constants.LongValue.LvalRowStart` (Jet4/ACE: `4096 − 20 = 4076` bytes payload).
- Chain row max = single-page row max − 4 (the in-row next-pointer prefix).

C8 caveats:

- **Upper limit is `Constants.LongValue.MaxPayloadBytes = (1 << 24) − 1`** (~16 MiB) per single MEMO / OLE / Attachment value, set by the on-disk 24-bit `memo_len` field. Larger payloads throw `JetLimitationException`.
- **Default update/delete do not reclaim old LVAL pages**: `UpdateRowsAsync` rewrites the row through `InsertRowDataAsync` and allocates a fresh LVAL chain; `DeleteRowsAsync` marks the owning row deleted. With `SecureEraseMode.None`, old LVAL pages remain on disk for Compact & Repair or a rebuild. With `SecureEraseMode.DeletedRowsAndFreedPages`, `LongValueStore.DeallocateExternalPagesAsync` walks the single-page or chained descriptor, scrubs the old pages, and returns them to the Access global free list.
- **System-table OLE columns (`MSysObjects.LvProp` / `LvModule` / `LvExtra`) keep the 256-byte inline cap.** Internal system-table writes bypass the pre-encode hook because the property blobs the writer emits today are well under the inline limit; lifting the cap there would require routing every system-table writer through `InsertRowDataLocAsync`.
- **Validation.** Round-trip through this library's reader is verified in `JetDatabaseWriter.Tests/ComplexColumns/ComplexColumnsLvalChainTests.cs` (single-page form, chained form, deflate-compressed text payload). Automated DAO CompactDatabase coverage now includes writer-authored attachment payload bytes on the `ComplexFields` fixture in `ComplexFixture_WriterAttachmentRowsSurviveCompactAndRepair` and a Northwind-hosted writer-created attachment table with a chained-LVAL `.jpg` payload in `DaoCompact_ComplexColumnsWithLvalPayload_SurviveCompactAndRepair`; see [writer-disk-format-validation-matrix.md](writer-disk-format-validation-matrix.md).

### 4.5 C9 schema-evolution on tables containing complex columns

Phase C9 lifts the `NotSupportedException` previously thrown by `BuildColumnDefinitionFromInfo` so that `AddColumnAsync` / `DropColumnAsync` / `RenameColumnAsync` work on parent tables that already carry attachment / multi-value columns. The on-disk byte-format of the parent TDEF and the hidden flat children is unchanged — C9 is a control-flow change in the rewrite path that preserves the existing artifacts when possible and surgically removes / renames them when the user mutation explicitly targets a complex column.

How surviving complex columns ride through `RewriteTableAsync`:

1. **TDEF descriptor reconstruction.** `BuildColumnDefinitionFromInfo` now returns a `ColumnDefinition` flagged with `IsAttachment` / `IsMultiValue` and the original `ComplexId` recovered from `ColumnInfo.Misc`. The default-projection and rename-column projection forward `IsAttachment` / `IsMultiValue` / `MultiValueElementType` / `ComplexId` so the rebuilt TDEF re-emits `Complex` (Flags = 0x07) with the correct misc field.
2. **Allocation skip.** `PrepareComplexColumnAllocationsAsync` only allocates fresh `ComplexId` values for columns whose `ComplexId == 0`; preserved columns bypass it entirely, so no new flat child table is emitted.
3. **Preserve or patch parent identity.** When surviving complex columns are present and none are dropped or renamed, the temp table is transplanted onto the original TDEF page. That keeps `MSysComplexColumns.ConceptualTableID` stable for DAO CompactDatabase. When a complex column drop/rename requires the older copy/swap path, the original-table drop routes through `DropTableCoreAsync(tableName, dropComplexChildren: false, ct)` so flat children + surviving `MSysComplexColumns` rows are not cascaded away, then their `ConceptualTableID` values are patched to the replacement TDEF page.
4. **Constraint-registry hydration.** `HydrateConstraintsFromTableDef` now skips the `Flags` flag-bit interpretation for `Complex` columns. The on-disk Flags = 0x07 is a magic marker (per mdbtools docs) — interpreting bit 0x04 as `FLAG_AUTO_LONG` would mark a `byte[]` column as auto-increment and fail the `IsIntegralType` check during the temp-table `RegisterConstraints` pass.
5. **Catalog repair after rebuild.** The transplant path replaces the original catalog row in place and removes only the temporary table catalog/ACE rows. The copy/swap path rewrites surviving `MSysComplexColumns.ConceptualTableID` values to the new parent TDEF page, and old parent ACE/catalog rows are removed without rebuilding Access-authored system indexes wholesale.

Surgical post-rewrite cleanup runs from the rewrite path itself:

- **`DropSingleComplexChildAsync(columnName, complexId)`** — invoked once per dropped complex column (matched by ComplexId between `existingDefs` and `newDefs`). Deletes the matching `MSysComplexColumns` row (matched by ColumnName + ComplexID), adjusts the `MSysComplexColumns` TDEF row count, and drops the hidden flat-table catalog row in `MSysObjects`. Idempotent; tolerates missing rows.
- **`RenameComplexColumnArtifactsAsync(oldName, newName, complexId)`** — invoked once per renamed complex column (matched by ComplexId match + name mismatch between `existingDefs` and `newDefs`). Mark-deletes the matching `MSysComplexColumns` row, then re-inserts it (`updateTDefRowCount: false`) with `ColumnName` rewritten to the new name. The hidden flat-table's catalog name (`f_<hex>_<oldName>`) is left unchanged — readers resolve the flat name via `FlatTableID` → `MSysObjects.Name`, and the cosmetic suffix carries no semantic meaning.

C9 caveats:

- **Adding a brand-new complex column to an existing table works** because `PrepareComplexColumnAllocationsAsync` allocates fresh IDs for the appended `ColumnDefinition` (its `ComplexId == 0`), and `EmitComplexColumnArtifactsAsync` runs at the end of `CreateTableAsync` (called by the rewrite for the temp table) to emit the new flat child + `MSysComplexColumns` row. The pre-existing complex columns continue to ride through unchanged.
- **`AddAttachmentAsync` / `AddMultiValueItemAsync` after rename still work** because the FK back-reference column on the flat table (`_<userColumnName>`) keeps its original name; the reader resolves the flat table via `MSysComplexColumns.FlatTableID` and `GetAttachmentsAsync` joins via the parent's auto-number primary key without consulting the parent's complex slot. The `AddComplexItemCoreAsync` parent-row predicate matches on the user's PK columns, not on the renamed complex column.
- **Per-row complex slots are preserved on rebuild.** `ReadDataTableForSchemaRewriteAsync` keeps `ComplexIdRef` values for complex columns, and `RowEncoder` writes those values back to the rebuilt parent row so flat-table FK joins continue to resolve after `AddColumnAsync` / `DropColumnAsync` / `RenameColumnAsync`.
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
- Cross-validate compression: a `.jpg` payload must be stored with `typeFlag=0x00` (raw); a `.txt` payload must be stored with `typeFlag=0x01` (deflate). Open in Access and **save the attachment back to disk via the GUI** — verify the saved file is byte-identical to the input.
- Test fixture: `JetDatabaseWriter.Tests/Databases/ComplexFields.accdb`. This is the existing read-side fixture; the writer tests should round-trip it.

## 6. References

- [mdbtools HACKING.md](https://github.com/mdbtools/mdbtools/blob/master/HACKING.md) — complex-column flag byte (`0x07`) and complexid in `misc` field. (HACKING.md attributes type byte `0x11` to attachment, but per Jackcess `DataType.java` and our format-probe corpus Access uses `0x12` / `COMPLEX_TYPE` for **all** complex columns — see §1.)
- [Jackcess `AttachmentColumnInfoImpl.java`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/complex/AttachmentColumnInfoImpl.java) — wrapper-header encoder/decoder, COMPRESSED_FORMATS skip-list
- [Jackcess `ComplexColumnInfoImpl.java`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/complex/ComplexColumnInfoImpl.java) — flat-table protocol (PK + FK columns, `diffFlatColumns`)
- [Jackcess `ComplexDataType.java`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/complex/ComplexDataType.java) — type-discriminator integer values
- This repo: `JetDatabaseWriter/AccessReader.cs` `BuildComplexColumnDataAsync`, `DecodeAttachmentFileData`, `DecompressAttachmentData`
- Companion design doc: [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md)
