---
description: "Glossary of acronyms, constants, and unusual Access/JET terms used throughout this codebase. Reference when encountering unfamiliar terms in source, docs, or comments."
applyTo: "**"
---

# Glossary

## Database Engine & File Format

| Term | Meaning |
|---------|---------|
| **JET** | Joint Engine Technology — Microsoft's database engine for Access |
| **Jet3** | JET version 3 — Access 97 format (2048-byte pages, ANSI encoding) |
| **Jet4** | JET version 4 — Access 2000–2003 format (4096-byte pages, UCS-2) |
| **ACE** | Access Connectivity Engine — successor to JET used by Access 2007+ |
| **MDB** | Microsoft Database — file extension for Jet3/Jet4 databases |
| **ACCDB** | Access Database — file extension for ACE-format databases (2007+) |
| **DAO** | Data Access Objects — Microsoft COM-based API for Access |
| **OLE** | Object Linking and Embedding |
| **OLE DB** | OLE Database — COM-based data-access API |
| **ODBC** | Open Database Connectivity — linked-table source type represented in `MSysObjects` |
| **CSV** | Comma-Separated Values — text linked-table format with dedicated `MSysObjects.Flags` values |
| **mdbtools** | Open-source Access/MDB reverse-engineering project; many column type names mirror its `MDB_*` symbols |
| **Jackcess** | Java Access database library used as a reference for many on-disk constants and names |
| **V2000+** | Jackcess/format shorthand for Jet4/ACE-style Access 2000 and newer structures |

## Page Types & Structures

| Term | Meaning |
|---------|---------|
| **TDEF** | Table Definition — page type storing column metadata, index defs, and row counts |
| **Data page** | Page type `0x01` that stores table rows and row-offset slots |
| **Usage map / umap** | Bitmap row or page that tracks pages owned by a table, index, or free-space list |
| **Inline usage map** | Usage-map row whose bitmap bytes live directly in the row payload |
| **Reference usage map** | Usage-map row that points to separate page-type `0x05` bitmap pages |
| **Freed page** | Page type `0x09` marking a page available for reuse |
| **LVAL** | Long Value — descriptor/page form for MEMO, OLE, and attachment payloads; oversized payloads live on external data pages marked with the `LVAL` signature |
| **EOD** | End of Data — marker in the variable-length column trailer |
| **Row ID** | Physical row pointer made from a page number plus a per-page row slot |
| **Row-offset slot** | Data-page trailer entry pointing to a row start; high bits mark deleted or non-live rows |
| **Null mask** | Row bitmap indicating which columns are present; BOOL values are stored here rather than in the fixed area |
| **Fixed row area** | Row section containing fixed-width column payloads |
| **Variable-length trailer** | Row tail holding offsets for variable-width columns plus the EOD marker |
| **PK** | Primary Key |
| **FK** | Foreign Key |
| **DDL** | Data Definition Language |
| **DML** | Data Manipulation Language |

## Compound File Binary (MS-CFB)

| Term | Meaning |
|---------|---------|
| **CFB** | Compound File Binary — OLE2 container format used by Office Crypto wrappers and test fixtures; Access-native Agile `.accdb` files use a flat page layout instead |
| **FAT** | File Allocation Table — sector chain mapping in a CFB file |
| **DIFAT** | Double-Indirect FAT — extension array when >109 FAT sectors overflow the header |
| **Mini-FAT** | Mini File Allocation Table — allocation table for streams < 4096 bytes |
| **MSAT** | Master Sector Allocation Table — alternate name for DIFAT |
| **SAT** | Sector Allocation Table — alternate name for FAT |
| **Sector** | CFB allocation unit: 512 bytes in CFB v3, 4096 bytes in CFB v4 |
| **Mini-sector** | Small-stream allocation unit inside the CFB mini-stream |
| **Mini-stream** | CFB stream that stores mini-sector data for streams below the 4096-byte cutoff |
| **FREESECT** | FAT sentinel `0xFFFFFFFF` meaning the sector is free / unused |
| **ENDOFCHAIN** | FAT sentinel `0xFFFFFFFE` marking the end of a sector chain |
| **FATSECT** | FAT sentinel `0xFFFFFFFD` marking a sector that stores FAT entries |
| **DIFSECT** | FAT sentinel `0xFFFFFFFC` marking a sector that stores DIFAT entries |
| **Dir entry** | 128-byte CFB directory entry describing a storage or stream |
| **Header DIFAT** | First 109 FAT-sector pointers embedded directly in the CFB header |
| **Sector shift** | Log2 value for sector size (`9` for 512-byte sectors, `12` for 4096-byte sectors) |

## Encryption & Cryptography

| Term | Meaning |
|---------|---------|
| **RC4** | Rivest Cipher 4 — stream cipher used by Jet4 per-page encryption |
| **AES** | Advanced Encryption Standard — AES-128-ECB (legacy ACCDB) and AES-256-CBC (Agile) |
| **ECB** | Electronic Codebook — AES block mode for legacy encryption |
| **CBC** | Cipher Block Chaining — AES block mode for Agile encryption |
| **Agile** | ECMA-376 password encryption used by Access 2010 SP1+ and Microsoft 365; AES-256-CBC + SHA-512 + PBKDF spin loop |
| **EncryptionInfo** | Office Crypto descriptor stream/XML that records Agile encryption parameters |
| **XOR** | Exclusive OR — Jet3 page obfuscation (128-byte cyclical mask) |
| **HMAC** | Hash-based Message Authentication Code — integrity check in Agile encryption |
| **SHA** | Secure Hash Algorithm — SHA-256 (legacy AES key), SHA-512 (Agile) |
| **MD5** | Message Digest 5 — Jet4 RC4 per-page key derivation |
| **PBKDF** | Password-Based Key Derivation Function — Agile uses SHA-512 spin loop |
| **IV** | Initialization Vector — per-segment AES-CBC IV |
| **Salt** | Random bytes mixed into PBKDF key derivation and Agile IV generation |
| **Spin count** | PBKDF iteration count; Agile encryption constants use 100,000 |
| **Segment** | Independently encrypted 4096-byte chunk of the Agile encrypted package |

## Text Encoding

| Term | Meaning |
|---------|---------|
| **ANSI** | Jet3 text encoding (code-page–dependent) |
| **UCS-2** | Universal Character Set (2-byte) — Jet4/ACE text encoding |
| **UTF-16** | Unicode Transformation Format (16-bit) — password and column-name storage |
| **Compressed Unicode** | Jet4/ACE text optimization enabled by an extra flag; stores `0xFF 0xFE` marker + one byte per Latin-1 character |
| **Latin-1** | ISO-8859-1 character range eligible for Jet4/ACE compressed-unicode storage |
| **LE** | Little-endian byte order; used by UCS-2/UTF-16 text and most numeric on-disk fields |
| **BOM** | Byte Order Mark; the CFB header byte-order field is always `0xFFFE` |

## Data Types & Column Constants

| Term | Meaning |
|---------|---------|
| **GUID** | Globally Unique Identifier — column type `0x0F`, also called REPID by mdbtools |
| **BLOB** | Binary Large Object — large binary payload, often stored through OLE/attachment LVAL storage when it exceeds inline limits |
| **MEMO** | Memo field — column type 0x0C; long text |
| **BCD** | Binary-Coded Decimal — format for `ColumnTypes.NumericType` (0x10) columns |
| **MDB_*** | mdbtools prefix for Access column type identifiers, such as `MDB_TEXT` and `MDB_NUMERIC` |
| **ColumnTypes.BooleanType** | Type Boolean (0x01) |
| **ColumnTypes.ByteType** | Type unsigned byte (0x02) |
| **ColumnTypes.IntegerType** | Type 2-byte signed integer (0x03) |
| **ColumnTypes.LongIntegerType** | Type 4-byte signed integer (0x04) |
| **ColumnTypes.MoneyType** | Type currency value stored as 8-byte integer scaled by 10,000 (0x05) |
| **ColumnTypes.FloatType** | Type IEEE-754 single-precision float (0x06) |
| **ColumnTypes.DoubleType** | Type IEEE-754 double-precision float (0x07) |
| **ColumnTypes.DateTimeType** | Type OLE Automation date/time (0x08) |
| **ColumnTypes.BinaryType** | Type Binary (0x09) |
| **ColumnTypes.TextType** | Type Text (0x0A) |
| **ColumnTypes.OleType** | Type OLE long-value blob (0x0B) |
| **ColumnTypes.MemoType** | Type Memo long-value text (0x0C) |
| **ColumnTypes.GuidType** | Type GUID / REPID (0x0F) |
| **ColumnTypes.NumericType** | Type Numeric/BCD (0x10) |
| **ColumnTypes.AttachmentType** | Legacy/private attachment alias (0x11); Access-authored ACCDB files normally use `ColumnTypes.ComplexType` |
| **ColumnTypes.ComplexType** | Type Complex (0x12) — multi-value/attachment |
| **ColumnTypes.DateTimeExtendedType** | Type DateTime Extended (0x14) — Access 2019+ high-precision |
| **OLE Automation date** | Date/time encoded as a floating-point day count used by Access and COM automation |

## Column Metadata & Properties

| Term | Meaning |
|---------|---------|
| **Column descriptor** | TDEF record for one column, including type, length, flags, offsets, and column number |
| **col_type** | One-byte JET column type discriminator stored in each column descriptor |
| **col_len** | Column length field in a descriptor; calculated columns may include wrapper overhead |
| **flags byte** | Primary column descriptor bitmask for fixed-width, AutoNumber, Hyperlink, and legacy NOT NULL markers |
| **extra flags** | Jet4/ACE descriptor byte used for compressed-unicode and calculated-column markers |
| **AutoNumber** | Access identity column; TDEF stores a high-water value used for the next generated number |
| **Hyperlink** | MEMO column flag indicating Access hyperlink semantics |
| **Calculated column** | ACCDB-only expression column with a cached value wrapped by calculated-column metadata |
| **Expression** | Persisted Jet/VBA expression string for a calculated column |
| **ResultType** | Persisted JET column type code describing the calculated expression result |
| **LvProp** | `MSysObjects` long-value property blob used for catalog and column/table property data |
| **PropertyMap** | Jackcess term for Access property blobs such as `Expression` and `ResultType` |
| **InputMask** | Access column property that constrains interactive text entry formatting |
| **AllowZeroLength** | Access text-column property allowing empty strings distinct from NULL |
| **CRC** | Cyclic Redundancy Check — reserved/version area in calculated-column wrappers references CRC bytes |

## Internal Structures

| Term | Meaning |
|---------|---------|
| **LRU** | Least Recently Used — 256-page eviction cache in `AccessReader` |
| **B-tree** | Balanced tree — index page structure (leaf 0x04, intermediate 0x03) |
| **Magic value / cookie** | Fixed byte or integer value Access expects in a header/descriptor to recognize a valid structure |
| **High-water value** | Cached maximum AutoNumber value used to pick the next generated value |
| **Sentinel** | Reserved value that marks a special state such as free sector, unused column-map slot, or end of chain |
| **Bitmask** | Integer whose individual bits carry independent flags or bitmap state |
| **little-endian** | Multi-byte numeric encoding with the least significant byte first |

## Indexes & Relationships

| Term | Meaning |
|---------|---------|
| **real-idx** | Physical index descriptor in a TDEF; maps indexed columns to the root page of an index B-tree |
| **logical-idx** | Logical index entry in a TDEF; names or relates logical indexes to backing real-idx descriptors |
| **col_map** | Fixed 10-slot column map inside a real-idx descriptor: `{col_num, col_order}` entries |
| **col_num** | Physical column number stored in descriptors and index column maps |
| **col_order** | Sort direction byte inside a `col_map` slot (`0x01` ascending, `0x00` descending) |
| **first_dp** | Root page pointer for an index B-tree in a real-idx descriptor |
| **used_pages** | Usage-map pointer/list for pages owned by an index |
| **pref_len** | Page-shared prefix length used by compressed index key entries |
| **tail_page / childTail** | Index page header pointer used by intermediate index pages to reach the final child branch |
| **grbit** | `MSysRelationships` bitmask column containing referential-integrity and cascade flags |
| **Referential integrity** | Relationship enforcement requiring FK values to match parent PK/unique-index values |
| **Cascade update/delete** | Relationship option that propagates parent key updates or deletes to child rows |
| **Unique index** | Index flag that rejects duplicate non-null key values |
| **Ignore nulls** | Index flag that omits rows whose key columns are NULL |
| **Required index** | Index flag used for NOT NULL / PK enforcement |
| **Collation** | Text comparison and sort-key encoding rules used by text indexes |

## System Tables, Catalog & Security

| Term | Meaning |
|---------|---------|
| **MSysObjects** | Catalog table listing all database objects |
| **MSysACEs** | Access Control Entries (security) |
| **MSysRelationships** | Foreign key relationship definitions |
| **MSysComplexColumns** | Links complex columns to their template tables |
| **MSysIndexes** | Index definitions |
| **MSysIndexColumns** | System table listing columns that participate in indexes |
| **MSysQueries** | System table storing saved query metadata |
| **MSysDb** | Internal Access system object name that appears in DAO error messages when catalog validation fails |
| **MSysComplexType_*** | Per-database template tables Access creates for complex columns |
| **MSysComplexTypeVH_*** | Version-history template table prefix for complex-column history data |
| **Complex type-template table** | Hidden system table describing the shape of attachment or multi-value child rows |
| **Complex flat table** | Hidden child table that stores actual attachment, multi-value, or version-history rows |
| **ParentId** | `MSysObjects` column linking an object row to a catalog container such as Tables or Relationships |
| **Type** | `MSysObjects` object-kind discriminator, such as user table, linked table, or relationship |
| **Flags** | `MSysObjects` bitmask identifying system/hidden objects, linked tables, and complex backing tables |
| **Owner** | `MSysObjects` binary owner token required by DAO Compact & Repair on writer-created rows |
| **ACE** | Access Control Entry row in `MSysACEs` granting a principal permissions on an object |
| **ACM** | Access Control Mask — permission bitmask stored in `MSysACEs` rows |
| **SID** | Security Identifier — compact principal identifier bytes stored in `MSysACEs` |
| **Principal** | Security subject such as owner, Admins group, or Users group |
| **C&R** | Compact & Repair — DAO operation that validates and rewrites Access database files |

## Long Values & Wrapped Payloads

| Term | Meaning |
|---------|---------|
| **Inline LVAL** | Long-value payload stored directly in the owning row |
| **Single-page LVAL** | Long-value payload stored in one external LVAL page |
| **Chained LVAL** | Long-value payload spread across multiple linked LVAL pages |
| **Storage mode** | LVAL header bit pattern selecting inline, single-page, or chained storage |
| **24-bit length** | LVAL payload-length field width; caps addressable MEMO/OLE/attachment payloads at 16,777,215 bytes |
| **Magic bytes** | File-signature bytes used to identify wrapped payload formats inside OLE columns |
| **JPEG** | Joint Photographic Experts Group image format; detected by `FF D8 FF` magic bytes |
| **PNG** | Portable Network Graphics image format; detected by `89 50 4E 47` magic bytes |
| **GIF** | Graphics Interchange Format image format; detected by `GIF` magic bytes |
| **BMP** | Bitmap image format; detected by `BM` magic bytes |
| **TIFF** | Tagged Image File Format; little- and big-endian signatures are both detected |
| **PDF** | Portable Document Format; detected by `%PDF` magic bytes in OLE payloads |
| **ZIP** | ZIP archive/container format; detected by `50 4B 03 04` (`PK\x03\x04`) magic bytes |
| **OOXML** | Office Open XML — ZIP-packaged Office document formats such as DOCX, XLSX, and PPTX |
| **RTF** | Rich Text Format; detected by `{\rt` magic bytes in OLE payloads |

## Standards & Specifications

| Term | Meaning |
|---------|---------|
| **ECMA-376** | Office Open XML standard — defines Agile encryption |
| **MS-CFB** | Microsoft Compound File Binary Format specification |
| **MS-OFFCRYPTO** | Microsoft Office Document Cryptography Structure specification |
| **CVE** | Common Vulnerabilities and Exposures |
| **OOB** | Out of Bounds — memory access vulnerability class |
