# Design notes: Complex columns (Attachment, Multi-value) — write path

**Status:** All shipped phases listed in §4.2; outstanding work tracked in the same table.
**Empirical appendix:** [`format-probe-appendix-complex.md`](format-probe-appendix-complex.md) — annotated hex dumps of `MSysComplexColumns`, every `MSysComplexType_*` template table, an attachment-bearing parent table (`Documents`), and the hidden flat tables from `ComplexFields.accdb`. Regenerate via `dotnet run --project JetDatabaseWriter.FormatProbe`.
**Validation requirement:** see [`index-and-relationship-format-notes.md` §8](index-and-relationship-format-notes.md#8-validation-strategy).

> ⚠️ Reverse-engineered notes. mdbtools documents complex columns only superficially. The authoritative open-source reference is [Jackcess](https://github.com/jahlborn/jackcess) (Java, Apache-2.0) — specifically `com.healthmarketscience.jackcess.impl.complex.*`. Field names and offsets in this document are derived from Jackcess source and the existing reader code in this repo (`AccessReader.BuildComplexColumnDataAsync`, `DecodeAttachmentFileData`).

---

## 1. Background

Access 2007 introduced three "complex column" kinds. All three are stored the same way: a 4-byte per-row "ConceptualTableID" pseudo-foreign-key in the parent row, pointing into a hidden child ("flat") table that holds the actual values. **All three kinds share the column-type byte `0x12` (`COMPLEX_TYPE`)** — confirmed both by Jackcess `DataType.java` and by our format-probe across the entire test corpus (no on-disk fixture in `JetDatabaseWriter.Tests/Databases/` carries `0x11` on a complex column). The `T_ATTACHMENT = 0x11` constant in `AccessBase.cs` is vestigial: per Jackcess `0x11` is `UNKNOWN_11`, an alternative OLE type used only by `MSysAccessObjects` (fixed length 3992). The writer's IsAttachment-→-`0x11` mapping in `MapToJetType` does not match Access's own emission and is a known divergence to fix.

| Kind | Storage in child table |
|---|---|
| Attachment | One row per attached file. Columns: `FileURL`, `FileName`, `FileType`, `FileFlags`, `FileTimeStamp`, `FileData`. |
| Multi-value | One row per value. Columns: a single value column whose type matches the user-declared element type. |
| Version history | One row per historical edit. Columns: `value` (memo), `version` (datetime). Only meaningful on memo columns flagged "Append Only" in Access. |

The discrimination between attachment / multi-value / version-history is **not** done by the column-type byte. It is done by the linked flat table's schema and/or the value of `MSysComplexColumns.ComplexTypeObjectID` (which points at one of the `MSysComplexType_*` template tables — see [appendix](format-probe-appendix-complex.md)).

The reader implements all three kinds. Writer support has shipped through phase C10 — see §4.2.

## 2. On-disk layout

### 2.1 Parent column descriptor

Inside the parent table's TDEF column descriptor block (25 bytes per col on Jet4 / ACE), a complex column has:

| Field | Value |
|---|---|
| `col_type` | `0x12` (`COMPLEX_TYPE`) for all three kinds — attachment, multi-value, and version-history. See §1 for the `0x11` writer divergence. |
| `col_len` | `4` |
| `bitmask` | `0x07` (always — per mdbtools "always have the flag byte set to exactly 0x07") |
| `misc` | The 4-byte **ComplexID** (called "complexid" in mdbtools), stored at the `misc` offset. This is the same value as the `ConceptualTableID` written into `MSysComplexColumns`. |

Because `bitmask = 0x07`, the column is treated as a fixed-length 4-byte column for row-layout purposes. The 4-byte payload in each data row is the **ConceptualTableID** that joins this row to its child rows in the flat table.

### 2.2 `MSysComplexColumns` catalog table

**Verified against `ComplexFields.accdb`** ([appendix](format-probe-appendix-complex.md#msyscomplexcolumns--tdef-page-18)). Actual schema is **5 columns** (column names and order below are probe-confirmed):

| Column (verified) | Type | Meaning |
|---|---|---|
| `ColumnName` | `T_TEXT(510)` | Name of the parent column (e.g. `"Attachments"`). |
| `ComplexID` | `T_LONG` (fixed_off=12) | Per-database ID for this complex column. **Matches the 4-byte value the parent TDEF stores in the column descriptor's `misc`+`misc_ext` slot** (see §2.1). |
| `ComplexTypeObjectID` | `T_LONG` (fixed_off=0) | `MSysObjects.Id` of the **type-template table** — one of `MSysComplexType_Long`, `MSysComplexType_Text`, `MSysComplexType_Attachment`, etc. The template's schema dictates the kind (attachment vs multi-value of a given inner type). |
| `ConceptualTableID` | `T_LONG` (fixed_off=8) | Per-table cursor source. The next value is taken from the parent TDEF's `ct_autonum` field (TDEF block offset 28). Each parent row's 4-byte payload is one `ConceptualTableID` value, joining the parent row to its child rows. |
| `FlatTableID` | `T_LONG` (fixed_off=4) | `MSysObjects.Id` of the hidden child ("flat") table. |

There is **no** `ParentTable` / `ParentColumn` column in `MSysComplexColumns`. The parent reference is implicit: it is recovered by scanning every user TDEF for a complex column whose `misc`+`misc_ext` 4-byte slot equals `ComplexID`. The reader (`AccessReader.BuildComplexColumnDataAsync`) already does this scan.

`MSysComplexColumns` is now created by [`AccessWriter.CreateMSysComplexColumnsAsync`](../../JetDatabaseWriter/Core/AccessWriter.cs) on every fresh ACCDB built via `CreateDatabaseAsync` (Phase C1, 2026-04-25). The catalog row carries `Flags = 0x80000000` so the table is excluded from `ListTablesAsync`. ACE only — Jet3/Jet4 `.mdb` scaffolds skip the system table because complex columns are an Access 2007+ feature.

### 2.3 The hidden "flat" child table

**Verified naming and flag conventions** ([appendix](format-probe-appendix-complex.md)):

- Flat-table name pattern: **`f_<32-hex-uppercase>_<userColumnName>`**, e.g. `f_A3DF50CFC033433899AF0AC1A4CF4171_Attachments`. The 32 hex characters are a GUID without dashes.
- Flat-table `MSysObjects.Flags`: **`0x800A0000`** (probe-confirmed in both `NorthwindTraders.accdb` and `ComplexFields.accdb`).
- Template ("type") tables: `MSysComplexType_<TypeName>` (`Long`, `Text`, `Attachment`, `UnsignedByte`, `Short`, `IEEESingle`, `IEEEDouble`, `GUID`, `Decimal`). `Flags = 0x80030000`.
- Stored as a normal user table in `MSysObjects` (Type = 1), but the system-flag bit (`0x80000000`) hides it from Access UI.

The flat table has all the value-bearing columns of the complex type, plus two extra columns:

1. An **autonumber Long primary key** column.
2. A **Long FK column** that holds the same `ConceptualTableID` value used in the parent row. This is what the reader joins on (`AccessReader.BuildComplexColumnDataAsync`).

The flat table also requires:

- A **primary key** (the autonumber column) — needs the index-creation foundation in [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md).
- A **non-unique index** on the FK column — same.

### 2.4 Per-kind flat-table schemas

#### 2.4.1 Attachment (`MSysComplexColumns.Type = 4`, on-disk `col_type = 0x12`)

Per Jackcess [`AttachmentColumnInfoImpl`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/complex/AttachmentColumnInfoImpl.java):

| Flat-table column | Jet type | Notes |
|---|---|---|
| (autonumber PK) | `T_LONG`, autoincrement | Required by complex-column protocol. |
| (FK) | `T_LONG` | Holds `ConceptualTableID`. |
| `FileURL` | `T_MEMO` | May be empty/null. |
| `FileName` | `T_TEXT` (max 255) | Display filename. |
| `FileType` | `T_TEXT` (max 255) | Lowercase file extension without leading `.` (Jackcess uses lowercase consistently). Drives the COMPRESSED_FORMATS skip-list (§3.2). |
| `FileFlags` | `T_LONG` | Reserved by Access; Jackcess emits null. |
| `FileTimeStamp` | `T_DATETIME` | When the file was attached. Jackcess uses the system clock. |
| `FileData` | `T_OLE` | Wrapper-encoded payload. **NOT raw bytes.** See §3. |

If the flat table on disk has columns whose names don't match these exactly, Jackcess assigns by type/order: the LONG column is `FileFlags`; the SHORT_DATE_TIME column is `FileTimeStamp`; the OLE column is `FileData`; the MEMO column is `FileUrl`; the first TEXT column is `FileName`; the second TEXT column is `FileType`. We should write canonical names, but the reader path should be tolerant.

#### 2.4.2 Multi-value (`MSysComplexColumns.Type = 3`, on-disk `col_type = 0x12`)

| Flat-table column | Jet type | Notes |
|---|---|---|
| (autonumber PK) | `T_LONG`, autoincrement | |
| (FK) | `T_LONG` | |
| `value` | varies | Whatever element type the user declared (text, long, etc.). |

The shipped declaration surface is the `ColumnDefinition.IsMultiValue` init-only property (see §4.2 C2 row); the inner value column inherits the `ColumnDefinition`'s declared type and length.

#### 2.4.3 Version history (`MSysComplexColumns.Type = 2`, on-disk `col_type = 0x12`)

| Flat-table column | Jet type | Notes |
|---|---|---|
| (autonumber PK) | `T_LONG`, autoincrement | |
| (FK) | `T_LONG` | |
| `value` | `T_MEMO` | Historical text snapshot. |
| `version` | `T_DATETIME` | When this snapshot was recorded. |

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

The reader (see §1) implements `T_ATTACHMENT` / `T_COMPLEX` column-type recognition, the `MSysComplexColumns` join (`BuildComplexColumnDataAsync`), and attachment payload decode (raw + deflate; `DecodeAttachmentFileData`). Schema metadata is exposed via `IAccessReader.GetComplexColumnsAsync`; typed item enumeration is exposed via `GetAttachmentsAsync` / `GetMultiValueItemsAsync` (see §4.2 C4).

### 4.2 Writer

| Phase | Scope | Status |
|---|---|---|
| **C1** | Empty-DB scaffold: add `MSysComplexColumns` to `BuildEmptyDatabase`. | ✅ Shipped. ACCDB only. Helper: `AccessWriter.CreateMSysComplexColumnsAsync` (called from `ScaffoldSystemTablesAsync`). Catalog row carries `MSysObjects.Flags = 0x80000000` and is excluded from `ListTablesAsync`. Tests: `ComplexColumnsWriterTests`. |
| **C2** | `ColumnDefinition.IsAttachment` / `IsMultiValue` declaration surface + `ColumnInfo.Misc` round-trip in TDEF emission (the `0x07` bitmask, the 4-byte `misc` ComplexID slot, `col_len = 4`). | ✅ Shipped. Public init-only props on `ColumnDefinition`. Descriptor offset `_colMiscOff` = 11 (Jet4/ACE). Tests: `ComplexColumnsWriterTests`. |
| **C3** | `CreateTableAsync` emits the hidden flat child table, allocates a fresh `ComplexID`, writes the `MSysComplexColumns` row, and patches the parent descriptor. | ✅ Shipped. Helper: `CreateTableInternalAsync`. Flat table named `f_<32-hex>_<userColumnName>`, `MSysObjects.Flags = 0x800A0000`. Per-flat PK/FK indexes are emitted by C7 (§4.3); `ComplexTypeObjectID` is populated by C10 (§4.6). |
| **C4** | Per-kind row APIs (`AddAttachmentAsync`, `AddMultiValueItemAsync`) and attachment payload encode (§3). | ✅ Shipped. Helpers: `AccessWriter.AddAttachmentAsync` / `AddMultiValueItemAsync`, `AttachmentWrapper`. Reader-side: `AccessReader.GetAttachmentsAsync` / `GetMultiValueItemsAsync`. Inline-OLE 256-byte cap was lifted by C8 — oversized payloads now ride on LVAL data pages. |
| **C5** | Cascade flat-table rows on parent delete. | ✅ Shipped. Helper: `CascadeDeleteComplexChildrenAsync`, called from `DeleteRowsAsync` and the W10 `EnforceFkOnPrimaryDeleteAsync` cascade path. Cost: one O(P) flat-table scan per (parent table × complex column). Tests: `ComplexColumnsCascadeDeleteTests`. |
| **C6** | `DropTableAsync` cascade for hidden flat tables and `MSysComplexColumns` rows. | ✅ Shipped. Helper: `DropComplexChildrenForTableAsync`, called from `DropTableAsync`. Removes `MSysComplexColumns` and `MSysObjects` catalog rows for each child flat table; orphaned data pages are reclaimed by Access on the next Compact & Repair (same model used by W5). |
| **C7** | Per-flat-table indexes Access expects: autoincrement scalar PK column, primary key on the scalar, normal index on the FK back-reference, and (attachment only) a composite secondary index on (FK, FileName). Lifts the C3 "no PK / no autoincrement / no FK back-reference index" caveat. | ✅ Shipped. Helper: `BuildFlatTableSchema` (returns `(ColumnDefinition[], IndexDefinition[])`) wired through `EmitComplexColumnArtifactsAsync` and reused by `AddComplexItemCoreAsync` via `ApplyConstraintsAsync` so the autoincrement scalar PK is seeded per insert. See §4.3. |
| **C8** | LVAL chain emission for oversized MEMO / OLE / Attachment payloads. Lifts the inline-only `MaxInlineMemoBytes = 1024` and `MaxInlineOleBytes = 256` caps. | ✅ Shipped. Helpers: `PreEncodeLongValuesAsync`, `EncodeAsLvalChainAsync`, `BuildSingleLvalPageBuffer`, `BuildChainLvalPageBuffer` on `AccessWriter`. Pre-encode pass runs once at the top of `InsertRowDataLocAsync`: any `T_OLE` / `T_MEMO` value whose encoded payload exceeds the inline cap is staged onto freshly-appended LVAL data pages (single-page bitmask `0x40` for sub-page payloads; chained bitmask `0x00` with one row per page, walked in reverse so each predecessor row carries its successor's `lval_dp` pointer) and the in-row value is replaced with a `PreEncodedLongValue` sentinel that the encoders splice through verbatim. Upper limit is the on-disk 24-bit LVAL length field (`MaxLvalPayloadBytes = 16 MiB - 1`). LVAL pages are emitted as page-type `0x01` with `tdef_page = 0`, matching the format the existing `AccessReader.LocateLvalRowAsync` / `ReadLvalChainAsync` decoders accept. See §4.4. |
| **C9** | Schema evolution on parent tables that already contain complex columns: lift the `NotSupportedException` thrown by `BuildColumnDefinitionFromInfo` so `AddColumnAsync` / `DropColumnAsync` / `RenameColumnAsync` can run against tables with attachment / multi-value columns. | ✅ **Implemented (2026-04-25).** `BuildColumnDefinitionFromInfo` now returns `IsAttachment` / `IsMultiValue` ColumnDefinitions with the original `ComplexId` preserved from `ColumnInfo.Misc`. `RewriteTableAsync` was refactored: the original-table drop now routes through `DropTableCoreAsync(tableName, dropComplexChildren: false, ct)` (new private helper, also backing `DropTableAsync`), so flat children + `MSysComplexColumns` rows of surviving complex columns stay attached to the rebuilt parent. Surgical post-rewrite cleanup runs for the two single-column edge cases: `DropSingleComplexChildAsync(columnName, complexId)` removes the row + flat-table catalog entry when the user drops the complex column itself, and `RenameComplexColumnArtifactsAsync(oldName, newName, complexId)` rewrites `MSysComplexColumns.ColumnName` when the user renames it. The `RenameColumnAsync` projection now forwards `IsAttachment` / `IsMultiValue` / `MultiValueElementType` / `ComplexId` so the rebuilt TDEF re-emits T_ATTACHMENT / T_COMPLEX with the correct misc field. `HydrateConstraintsFromTableDef` skips the `Flags` flag-bit interpretation for complex columns (the on-disk Flags = 0x07 is a magic marker, not real `IsNullable` / `IsAutoIncrement` bits). 7 round-trip tests in `ComplexColumnsSchemaEvolutionTests`. See §4.5. |
| **C10** | Scaffold the nine `MSysComplexType_*` template tables (`UnsignedByte`, `Short`, `Long`, `IEEESingle`, `IEEEDouble`, `GUID`, `Decimal`, `Text`, `Attachment`) and populate `MSysComplexColumns.ComplexTypeObjectID` with the matching template id instead of the placeholder `0`. | ✅ **Implemented (2026-04-25).** Helpers: `AccessWriter.CreateMSysComplexTypeTemplatesAsync` (called from `ScaffoldSystemTablesAsync` immediately after `CreateMSysComplexColumnsAsync`) and the static `ResolveComplexTypeTemplateName(ColumnDefinition)` lookup wired into `EmitComplexColumnArtifactsAsync`. ACE only (Jet3 / Jet4 `.mdb` skip the templates because complex columns are an Access 2007+ feature). Each template carries `MSysObjects.Flags = 0x80030000` (system + the `0x30000` marker Access uses for type-template tables) so they are excluded from `ListTablesAsync`. 6 round-trip tests in `ComplexColumnsWriterTests`. See §4.6. |

### 4.3 C7 flat-table schema

The flat-child schema emitted by `BuildFlatTableSchema` (called from `EmitComplexColumnArtifactsAsync` during `CreateTableAsync`):

- **Attachment flat table** (8 columns, in this declaration order to match the `f_A3DF50CFC033433899AF0AC1A4CF4171_Attachments` probe in `ComplexFields.accdb`): `FileData` (OLE), `FileFlags` (LONG), `FileName` (TEXT 255), `FileTimeStamp` (DATETIME), `FileType` (TEXT 255), `FileURL` (MEMO), `<parentTable>_<userColumnName>` (LONG, autoincrement scalar PK), `_<userColumnName>` (LONG, FK back-reference). Three indexes ship: `MSysComplexPKIndex` (PK on the scalar), `_<userColumnName>` (normal index on the FK), and `IdxFKPrimaryScalar` (composite normal index on `(_<userColumnName>, FileName)`).
- **Multi-value flat table** (3 columns): `value` (CLR type from the user `ColumnDefinition`), `<parentTable>_<userColumnName>` (LONG, autoincrement scalar PK), `_<userColumnName>` (LONG). Two indexes: PK on the scalar plus a normal index on the FK back-reference. The composite secondary index is omitted because the format-probe corpus contains no multi-value flat-table fixture and the `value` column may be a non-indexable type (MEMO, OLE, GUID).

`AddComplexItemCoreAsync` resolves the flat-table name from the catalog (helper `ResolveFlatTableNameAsync`) and calls `ApplyConstraintsAsync` before `InsertRowDataAsync` so the autoincrement scalar PK is seeded from the existing flat-table rows. The constraint registry is hydrated from the persisted `FLAG_AUTO_LONG` bit when the writer instance did not declare the table itself, so re-opening a file produced by a previous writer instance still drives the autoincrement correctly.

C7 caveats:

- **`first_dp` is patched to an empty W3 leaf page** for each emitted index. The leaves are not maintained on `AddAttachmentAsync` / `AddMultiValueItemAsync` (those route through `InsertRowDataAsync`, which bypasses the W5 `MaintainIndexesAsync` hook). Access rebuilds the leaves on the next Compact & Repair pass — same model as W3 placeholder leaves on user tables that were never mutated through a public Insert/Update/Delete entry point.
- **`ComplexTypeObjectID`** is populated by C10 (§4.6) with the matching `MSysComplexType_*` template id; pre-C10 writer-authored files had this field at `0`.
- **Byte-level layout** is taken from the appendix probe of `f_A3DF50CFC033433899AF0AC1A4CF4171_Attachments` (column ordering, index names / kinds / column lists, scalar PK column name, autoincrement bit) and the Jackcess-derived multi-value extrapolation; the per-flat-table layout has not been round-tripped through a real Access install — see [`index-and-relationship-format-notes.md` §8](index-and-relationship-format-notes.md#8-validation-strategy).

### 4.4 C8 LVAL chain emission

Phase C8 lifts the inline cap that limited C4 attachment payloads to ~256 bytes. The pre-encode pass `AccessWriter.PreEncodeLongValuesAsync` runs at the top of `InsertRowDataLocAsync` and only fires for `T_OLE` / `T_MEMO` columns whose encoded payload exceeds the in-row inline cap (`MaxInlineOleBytes = 256`, `MaxInlineMemoBytes = 1024`). Smaller values keep the existing inline path (`WrapInlineLongValue`, bitmask `0x80`) unchanged.

12-byte LVAL header layout (matches `AccessReader.ReadLongValueAsync`):

```
+--------+--------+--------+--------+
| memo_len (24 LE)         | bitmask|   bytes 0..3
+--------+--------+--------+--------+
| lval_dp (32 LE)                   |   bytes 4..7  ((page<<8) | row_index)
+--------+--------+--------+--------+
| reserved (32, zero)               |   bytes 8..11
+--------+--------+--------+--------+
```

`bitmask` values produced by C8:

- `0x80` — inline (small payloads; bytes 4..11 zero, payload follows the header in the row body).
- `0x40` — single LVAL page. `lval_dp` points at one row on a freshly-appended LVAL data page; the row body **is** the payload (no next-pointer prefix).
- `0x00` — chained LVAL pages. `lval_dp` points at the first chained row, whose first 4 bytes are the next-pointer (LE `(page<<8)|row`), followed by that page's chunk of the payload. The terminal row's next-pointer is `0`.

LVAL page layout (one row per page, written by `BuildSingleLvalPageBuffer` / `BuildChainLvalPageBuffer`):

- `page_type = 0x01` (data page; the reader does not treat type `0x05` LVAL pages, so this matches the on-disk reader contract).
- `tdef_page = 0` (LVAL pages are not owned by a TDEF).
- `num_rows = 1`, single row offset entry pointing at the row body that occupies the tail of the page.
- Free-space field at offset 2 reflects the unused middle of the page.

Allocation order for the chained form is **reverse**: `EncodeAsLvalChainAsync` appends the *last* chunk's page first (next-pointer `= 0`), then walks backwards so each newly-appended page can carry its successor's `lval_dp` as its row-prefix next-pointer. The header's `lval_dp` ends up pointing at whatever page was appended *last* (the highest page number, holding the *first* chunk).

Chunking math:

- One row per LVAL page. The row offset table costs 2 bytes for the single offset slot.
- Single-page row max = `pgSize − dpRowsStart − 2` (Jet4/ACE: `4096 − 14 − 2 = 4080` bytes payload).
- Chain row max = single-page row max − 4 (the in-row next-pointer prefix).

C8 caveats:

- **Upper limit is `MaxLvalPayloadBytes = (1 << 24) − 1`** (~16 MiB) per single MEMO / OLE / Attachment value, set by the on-disk 24-bit `memo_len` field. Larger payloads throw `JetLimitationException`.
- **No LVAL page reuse on update/delete**: `UpdateRowsAsync` rewrites the row through `InsertRowDataAsync` and re-allocates a fresh LVAL chain; the old LVAL pages stay on disk and are reclaimed by Access on the next Compact & Repair pass (same model used by the W5 stale-leaf path).
- **System-table OLE columns (`MSysObjects.LvProp` / `LvModule` / `LvExtra`) keep the 256-byte inline cap.** Internal system-table writes bypass the pre-encode hook because the property blobs the writer emits today are well under the inline limit; lifting the cap there would require routing every system-table writer through `InsertRowDataLocAsync`.
- **Validation gap.** Round-trip through this library's reader is verified in `JetDatabaseWriter.Tests/Core/ComplexColumnsLvalChainTests.cs` (single-page form, chained form, deflate-compressed text payload). Microsoft Access compact-and-repair validation is still pending — see [`index-and-relationship-format-notes.md` §8](index-and-relationship-format-notes.md#8-validation-strategy).

### 4.5 C9 schema-evolution on tables containing complex columns

Phase C9 lifts the `NotSupportedException` previously thrown by `BuildColumnDefinitionFromInfo` so that `AddColumnAsync` / `DropColumnAsync` / `RenameColumnAsync` work on parent tables that already carry attachment / multi-value columns. The on-disk byte-format of the parent TDEF and the hidden flat children is unchanged — C9 is a control-flow change in the rewrite path that preserves the existing artifacts when possible and surgically removes / renames them when the user mutation explicitly targets a complex column.

How surviving complex columns ride through `RewriteTableAsync`:

1. **TDEF descriptor reconstruction.** `BuildColumnDefinitionFromInfo` now returns a `ColumnDefinition` flagged with `IsAttachment` / `IsMultiValue` and the original `ComplexId` recovered from `ColumnInfo.Misc`. The default-projection and rename-column projection forward `IsAttachment` / `IsMultiValue` / `MultiValueElementType` / `ComplexId` so the rebuilt TDEF re-emits T_ATTACHMENT / T_COMPLEX (Flags = 0x07) with the correct misc field.
2. **Allocation skip.** `PrepareComplexColumnAllocationsAsync` only allocates fresh `ComplexId` + `ConceptualTableID` for columns whose `ComplexId == 0`; preserved columns bypass it entirely, so no new flat child table is emitted.
3. **Drop without cascade.** The original-table drop now routes through `DropTableCoreAsync(tableName, dropComplexChildren: false, ct)` (new private helper, also backing the public `DropTableAsync` which keeps `dropComplexChildren: true`). Flat children + `MSysComplexColumns` rows of surviving complex columns stay attached to the rebuilt parent because `DropComplexChildrenForTableAsync` is not invoked.
4. **Constraint-registry hydration.** `HydrateConstraintsFromTableDef` now skips the `Flags` flag-bit interpretation for T_ATTACHMENT / T_COMPLEX columns. The on-disk Flags = 0x07 is a magic marker (per mdbtools docs) — interpreting bit 0x04 as `FLAG_AUTO_LONG` would mark a `byte[]` column as auto-increment and fail the `IsIntegralType` check during the temp-table `RegisterConstraints` pass.

Surgical post-rewrite cleanup runs from the rewrite path itself:

- **`DropSingleComplexChildAsync(columnName, complexId)`** — invoked once per dropped complex column (matched by ComplexId between `existingDefs` and `newDefs`). Deletes the matching `MSysComplexColumns` row (matched by ColumnName + ComplexID), adjusts the `MSysComplexColumns` TDEF row count, and drops the hidden flat-table catalog row in `MSysObjects`. Idempotent; tolerates missing rows.
- **`RenameComplexColumnArtifactsAsync(oldName, newName, complexId)`** — invoked once per renamed complex column (matched by ComplexId match + name mismatch between `existingDefs` and `newDefs`). Mark-deletes the matching `MSysComplexColumns` row, then re-inserts it (`updateTDefRowCount: false`) with `ColumnName` rewritten to the new name. The hidden flat-table's catalog name (`f_<hex>_<oldName>`) is left unchanged — readers resolve the flat name via `FlatTableID` → `MSysObjects.Name`, and the cosmetic suffix carries no semantic meaning. Mirrors the W14 `RenameRelationshipAsync` trade-off that leaves stale name cookies until Compact & Repair.

C9 caveats:

- **Adding a brand-new complex column to an existing table works** because `PrepareComplexColumnAllocationsAsync` allocates fresh IDs for the appended `ColumnDefinition` (its `ComplexId == 0`), and `EmitComplexColumnArtifactsAsync` runs at the end of `CreateTableAsync` (called by the rewrite for the temp table) to emit the new flat child + `MSysComplexColumns` row. The pre-existing complex columns continue to ride through unchanged.
- **`AddAttachmentAsync` / `AddMultiValueItemAsync` after rename still work** because the FK back-reference column on the flat table (`_<userColumnName>`) keeps its original name; the reader resolves the flat table via `MSysComplexColumns.FlatTableID` and `GetAttachmentsAsync` joins via the parent's auto-number primary key without consulting the parent's complex slot. The `AddComplexItemCoreAsync` parent-row predicate matches on the user's PK columns, not on the renamed complex column.
- **Per-row complex slot is null on the rebuilt parent.** Same as fresh `Insert` — `EncodeFixedValue` returns `null` for T_ATTACHMENT / T_COMPLEX so the slot is left at the null-mask default. The reader's `ReadDataTableAsync` path resolves the parent → child join via `ExtractParentId` (the parent's first LONG column) against the flat table's `_<columnName>` FK back-reference, which is preserved by the rewrite.
- **Validation gap.** Round-trip through this library's reader is verified in `JetDatabaseWriter.Tests/Core/ComplexColumnsSchemaEvolutionTests.cs` (7 tests covering AddColumn / DropColumn / RenameColumn for both the complex column itself and a non-complex sibling, plus AddColumn of a brand-new attachment column on a table that already has one). Microsoft Access compact-and-repair validation is still pending — see [`index-and-relationship-format-notes.md` §8](index-and-relationship-format-notes.md#8-validation-strategy).

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

Schemas come from `format-probe-appendix-complex.md` §`MSysComplexType_*` against `ComplexFields.accdb`.

Helpers added on `AccessWriter`:

- `CreateMSysComplexTypeTemplatesAsync` — emits the nine tables in declaration order (TDEF page + catalog row, no indexes, no rows). Skipped for Jet3 / Jet4 `.mdb` and for the slim-catalog ACCDB (`WriteFullCatalogSchema = false`) per the existing C1 gating.
- `ResolveComplexTypeTemplateName(ColumnDefinition)` (static) — returns the canonical template name for a complex column declaration, or `null` if the element type has no matching template.
- `EmitComplexColumnArtifactsAsync` now calls `ResolveComplexTypeTemplateName` + `FindSystemTableTdefPageAsync` to obtain the template's catalog id (= TDEF page) and passes it to `InsertMSysComplexColumnsRowAsync` instead of `0`.

C10 caveats:

- **Slim-catalog ACCDBs (`WriteFullCatalogSchema = false`) skip the templates by design.** That mode targets byte-hash backward compatibility with the legacy 9-column catalog and must not introduce additional pages; `EmitComplexColumnArtifactsAsync` falls back to `ComplexTypeObjectID = 0` when the template lookup misses. Access-authored files always carry the templates, and fresh `CreateDatabaseAsync` ACCDBs scaffold them, so the fallback only fires on the slim-catalog mode.
- **Decimal template `col_len`.** The format-probe appendix shows `col_len = 9` (precision 9 / scale 0); the C10 implementation emits whatever the writer's default `decimal` mapping produces. The template table is never populated with rows, so the precise `col_len` value carries no observable semantic.
- **Validation gap.** Round-trip through this library's reader is verified in 6 tests in `ComplexColumnsWriterTests` (template scaffolding, hidden-from-`ListTablesAsync`, attachment template column list, Jet4 skip, attachment + multi-value `ComplexTypeObjectID` non-zero). Microsoft Access compact-and-repair validation is still pending — see [`index-and-relationship-format-notes.md` §8](index-and-relationship-format-notes.md#8-validation-strategy).

## 5. Validation strategy

Same as the index doc, with one addition specific to attachments:

- Round-trip through this library: read fixtures (`ComplexFields.accdb`) → re-emit → re-read → byte-compare attachment payloads (post-decode).
- Cross-validate compression: a `.jpg` payload must be stored with `typeFlag=0x00` (raw); a `.txt` payload must be stored with `typeFlag=0x01` (deflate). Open in Access and **save the attachment back to disk via the GUI** — verify the saved file is byte-identical to the input.
- Test fixture: `JetDatabaseWriter.Tests/Databases/ComplexFields.accdb`. This is the existing read-side fixture; the writer tests should round-trip it.

## 6. References

- [mdbtools HACKING.md](https://github.com/mdbtools/mdbtools/blob/master/HACKING.md) — complex-column flag byte (`0x07`) and complexid in `misc` field. (HACKING.md attributes type byte `0x11` to attachment, but per Jackcess `DataType.java` and our format-probe corpus Access uses `0x12` / `COMPLEX_TYPE` for **all** complex columns — see §1.)
- [Jackcess `AttachmentColumnInfoImpl.java`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/complex/AttachmentColumnInfoImpl.java) — wrapper-header encoder/decoder, COMPRESSED_FORMATS skip-list
- [Jackcess `ComplexColumnInfoImpl.java`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/complex/ComplexColumnInfoImpl.java) — flat-table protocol (PK + FK columns, `diffFlatColumns`)
- [Jackcess `ComplexDataType.java`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/complex/ComplexDataType.java) — type-discriminator integer values
- This repo: `JetDatabaseWriter/Core/AccessReader.cs` `BuildComplexColumnDataAsync`, `DecodeAttachmentFileData`, `DecompressAttachmentData`
- Companion design doc: [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md)
