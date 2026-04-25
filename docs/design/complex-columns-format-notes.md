# Design notes: Complex columns (Attachment, Multi-value) — write path

**Status:** Research / not implemented
**Owner:** TBD
**Related limitations:** [`README.md` §"Limitations"](../../README.md):
- "No attachment columns. Reading attachments via the `MSysComplexColumns` FK lookup is supported, but `CreateTableAsync` cannot declare an Attachment column, and there is no API to add files to one."
- "No multi-value (complex) columns. Same restriction — readable, not writable."
- "Attachment payloads are not Deflate-compressed."

**Empirical appendix:** [`format-probe-appendix-complex.md`](format-probe-appendix-complex.md) — annotated hex dumps of `MSysComplexColumns`, every `MSysComplexType_*` template table, an attachment-bearing parent table (`Documents`), and the hidden flat tables from `ComplexFields.accdb`. Regenerate via `dotnet run --project JetDatabaseWriter.FormatProbe`.

**Validation requirement:** identical to [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md): every PR must round-trip through Microsoft Access on Windows (open + compact + re-open + Design View inspection) before merge.

> ⚠️ Reverse-engineered notes. mdbtools documents complex columns only superficially. The authoritative open-source reference is [Jackcess](https://github.com/jahlborn/jackcess) (Java, Apache-2.0) — specifically `com.healthmarketscience.jackcess.impl.complex.*`. Field names and offsets in this document are derived from Jackcess source and the existing reader code in this repo (`AccessReader.BuildComplexColumnDataAsync`, `DecodeAttachmentFileData`).

---

## 1. Background

Access 2007 introduced three "complex column" kinds. All three are stored the same way: a 4-byte per-row "ConceptualTableID" pseudo-foreign-key in the parent row, pointing into a hidden child ("flat") table that holds the actual values.

| Kind | Column type byte (per probe) | Storage in child table |
|---|---|---|
| Attachment | **`0x12` (T_COMPLEX)** in `ComplexFields.accdb` — see appendix `Documents.Attachments`. The `0x11` (T_ATTACHMENT) constant in `AccessBase.cs` may be vestigial; the probe has not yet observed it on disk. | One row per attached file. Columns: `FileURL`, `FileName`, `FileType`, `FileFlags`, `FileTimeStamp`, `FileData`. |
| Multi-value | `T_COMPLEX = 0x12` | One row per value. Columns: a single value column whose type matches the user-declared element type. |
| Version history | `T_COMPLEX = 0x12` | One row per historical edit. Columns: `value` (memo), `version` (datetime). Only meaningful on memo columns flagged "Append Only" in Access. |

The discrimination between attachment / multi-value / version-history is **not** done by the column-type byte. It is done by the linked flat table's schema and/or the value of `MSysComplexColumns.ComplexTypeObjectID` (which points at one of the `MSysComplexType_*` template tables — see [appendix](format-probe-appendix-complex.md)).

The reader already implements all three kinds. Writer support has never been added.

The existing pinned tests that codify the limitation (in `JetDatabaseWriter.Tests/Core/LimitationsTests.cs`, both should flip when this is implemented):

- `SpecializedColumns_NoPublicAttachmentApi`
- `SpecializedColumns_NoPublicMultiValueApi`

## 2. On-disk layout

### 2.1 Parent column descriptor

Inside the parent table's TDEF column descriptor block (25 bytes per col on Jet4 / ACE), a complex column has:

| Field | Value |
|---|---|
| `col_type` | `0x11` (attachment) or `0x12` (multi-value / version-history) |
| `col_len` | `4` |
| `bitmask` | `0x07` (always — per mdbtools "always have the flag byte set to exactly 0x07") |
| `misc` | The 4-byte **ComplexID** (called "complexid" in mdbtools), stored at the `misc` offset. This is the same value as the `ConceptualTableID` written into `MSysComplexColumns`. |

Because `bitmask = 0x07`, the column is treated as a fixed-length 4-byte column for row-layout purposes. The 4-byte payload in each data row is the **ConceptualTableID** that joins this row to its child rows in the flat table.

> In existing reader code, see `AccessReader.cs` near the `T_COMPLEX or T_ATTACHMENT when sz >= 4 => $"__CX:{Ri32(row, start)}__"` placeholder — that `Ri32` is reading this 4-byte ConceptualTableID.

### 2.2 `MSysComplexColumns` catalog table

**Verified against `ComplexFields.accdb`** ([appendix](format-probe-appendix-complex.md#msyscomplexcolumns--tdef-page-18)). Actual schema is **5 columns** (column names and order below are probe-confirmed):

| Column (verified) | Type | Meaning |
|---|---|---|
| `ColumnName` | `T_TEXT(510)` | Name of the parent column (e.g. `"Attachments"`). |
| `ComplexID` | `T_LONG` (fixed_off=12) | Per-database ID for this complex column. **Matches the 4-byte value the parent TDEF stores in the column descriptor's `misc`+`misc_ext` slot** (see §2.1). |
| `ComplexTypeObjectID` | `T_LONG` (fixed_off=0) | `MSysObjects.Id` of the **type-template table** — one of `MSysComplexType_Long`, `MSysComplexType_Text`, `MSysComplexType_Attachment`, etc. The template's schema dictates the kind (attachment vs multi-value of a given inner type). |
| `ConceptualTableID` | `T_LONG` (fixed_off=8) | Per-table cursor source. The next value is taken from the parent TDEF's `ct_autonum` field (TDEF block offset 28). Each parent row's 4-byte payload is one `ConceptualTableID` value, joining the parent row to its child rows. |
| `FlatTableID` | `T_LONG` (fixed_off=4) | `MSysObjects.Id` of the hidden child ("flat") table. |

**Critical correction from earlier draft:** the `ParentTable` and `ParentColumn` columns I listed previously **do not exist**. The parent reference is implicit: it is recovered by scanning every user TDEF for a complex column whose `misc`+`misc_ext` 4-byte slot equals `ComplexID`. The reader (`AccessReader.BuildComplexColumnDataAsync`) already does this scan.

`MSysComplexColumns` is **not** created today by [`AccessWriter.BuildEmptyDatabase`](../../JetDatabaseWriter/Core/AccessWriter.cs). Implementation must add it to the empty-DB scaffold (with the same hidden/system flag bits Access uses — the appendix shows `Flags = 0x80000000`).

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

> **Implementation tip:** rather than synthesizing a flat-table schema from scratch, copy the schema of the corresponding `MSysComplexType_*` template table and add the extra `_<userColName>` suffix. The templates exist in every fresh ACCDB precisely so that complex-column writers don't have to derive the schema by hand.

### 2.4 Per-kind flat-table schemas

#### 2.4.1 Attachment (`T_ATTACHMENT = 0x11`, `MSysComplexColumns.Type = 4`)

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

#### 2.4.2 Multi-value (`T_COMPLEX = 0x12`, `MSysComplexColumns.Type = 3`)

| Flat-table column | Jet type | Notes |
|---|---|---|
| (autonumber PK) | `T_LONG`, autoincrement | |
| (FK) | `T_LONG` | |
| `value` | varies | Whatever element type the user declared (text, long, etc.). |

The user-facing API on `ColumnDefinition` would need a way to specify the element type. Suggested shape:

```csharp
new ColumnDefinition("Tags", typeof(IList<string>))
{
    MultiValueElementType = typeof(string),
    MaxLength = 50, // applies to the inner value column
}
```

#### 2.4.3 Version history (`T_COMPLEX = 0x12`, `MSysComplexColumns.Type = 2`)

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

## 4. Required reader/writer changes

### 4.1 Reader (already done)

- ✅ `T_ATTACHMENT` / `T_COMPLEX` column-type recognition
- ✅ `MSysComplexColumns` join (`BuildComplexColumnDataAsync`)
- ✅ Attachment payload decode (raw + deflate; `DecodeAttachmentFileData`)

### 4.2 Reader (additions needed for writer round-trip)

- ✅ **Implemented (2026-04-24):** `IAccessReader.GetComplexColumnsAsync(string tableName, CT)` returns `IReadOnlyList<ComplexColumnInfo>` per parent-column metadata (ColumnName, ComplexId, Kind, FlatTableName, FlatTableId, ConceptualTableId, ComplexTypeObjectId, ComplexTypeName) — joins parent TDEF column descriptors with `MSysComplexColumns` and resolves flat-table / template names via `MSysObjects`. Tests in `JetDatabaseWriter.Tests/Core/ComplexColumnInfoTests.cs`.
- `IAccessReader.GetAttachmentsAsync(string tableName, string columnName, IReadOnlyDictionary<string,object> rowKey, CT)` — typed alternative to the current "decode opaque byte[]" surface. Still TODO.

### 4.3 Writer (new)

| Phase | Scope |
|---|---|
| **C1** | Empty-DB scaffold: add `MSysComplexColumns` to `BuildEmptyDatabase`. |
| **C2** | `ColumnDefinition.AsAttachment()` / `.AsMultiValue(Type element)` API + `ColumnInfo` round-trip in TDEF emission (the `0x07` bitmask, the 4-byte `misc` ComplexID slot, `col_len = 4`). |
| **C3** | `CreateTableAsync` + `AddColumnAsync` paths: when emitting a complex column, also create the hidden flat table (with PK + FK + per-kind value columns), reserve a fresh ConceptualTableID, write the `MSysComplexColumns` row, and write the parent column descriptor. |
| **C4** | Writer methods for attachment payload encode (§3) and the per-kind row APIs: `AddAttachmentAsync(string table, rowKey, AttachmentInput)`, `AddMultiValueItemAsync(string table, rowKey, object value)`. |
| **C5** | Cascade on parent delete: the flat-table rows that join to a deleted parent must also be deleted. Index-driven delete needs the index foundation — without indexes, we'd be doing a full child-table scan per parent delete. |
| **C6** | `DropTableAsync` cascade: when dropping a parent table, also drop its hidden flat tables and remove `MSysComplexColumns` rows. |

### 4.4 Hard prerequisites from the index foundation

Phases C2–C6 cannot ship until the index foundation in [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md) reaches at least:

- W1 (`IndexDefinition` + TDEF emission) — for the autonumber PK on the flat table ✅ shipped
- W3 (leaf-page emitter) — same, so the PK / FK indexes have real B-tree leafs and survive Access's compact/repair pass ✅ shipped
- W8 (PK API) — needed to declare the autonumber column as a primary key on the flat table ✅ shipped (2026-04-25); single-column PKs participate in W5 maintenance, multi-column PKs ship the schema only
- W9 (FK + `MSysRelationships`) — needed for the FK back-reference from each flat-table row to its parent (still pending)

> Note on W6 (`MSysIndexes` / `MSysIndexColumns`): these tables are absent from modern ACCDB ([index doc §6](index-and-relationship-format-notes.md#6-msysindexes--msysindexcolumns--msysrelationships-catalog-tables)), so for ACCDB output the flat table's index metadata lives entirely in its own TDEF. Complex columns themselves are an Access 2007+ (ACCDB) feature, so even if Jet3/Jet4 `.mdb` turns out to need W6 for ordinary indexes, that requirement does not propagate here.

## 5. Validation strategy

Same as the index doc, with one addition specific to attachments:

- Round-trip through this library: read fixtures (`ComplexFields.accdb`) → re-emit → re-read → byte-compare attachment payloads (post-decode).
- Cross-validate compression: a `.jpg` payload must be stored with `typeFlag=0x00` (raw); a `.txt` payload must be stored with `typeFlag=0x01` (deflate). Open in Access and **save the attachment back to disk via the GUI** — verify the saved file is byte-identical to the input.
- Test fixture: `JetDatabaseWriter.Tests/Databases/ComplexFields.accdb`. This is the existing read-side fixture; the writer tests should round-trip it.

## 6. References

- [mdbtools HACKING.md](https://github.com/mdbtools/mdbtools/blob/master/HACKING.md) — complex-column flag byte (`0x07`) and `T_COMPLEX = 0x12` / `T_ATTACHMENT = 0x11` type bytes; complexid in `misc` field
- [Jackcess `AttachmentColumnInfoImpl.java`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/complex/AttachmentColumnInfoImpl.java) — wrapper-header encoder/decoder, COMPRESSED_FORMATS skip-list
- [Jackcess `ComplexColumnInfoImpl.java`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/impl/complex/ComplexColumnInfoImpl.java) — flat-table protocol (PK + FK columns, `diffFlatColumns`)
- [Jackcess `ComplexDataType.java`](https://github.com/jahlborn/jackcess/blob/master/src/main/java/com/healthmarketscience/jackcess/complex/ComplexDataType.java) — type-discriminator integer values
- This repo: `JetDatabaseWriter/Core/AccessReader.cs` `BuildComplexColumnDataAsync`, `DecodeAttachmentFileData`, `DecompressAttachmentData`
- Companion design doc: [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md)
