namespace JetDatabaseWriter;

using System;

/// <summary>
/// Central container for project-wide constant values, grouped into nested
/// static classes by topic.
/// </summary>
internal static class Constants
{
    /// <summary>
    /// JET column-type discriminator codes as documented in the mdbtools
    /// <c>HACKING.md</c> reference. Stored in the <c>col_type</c> byte of each
    /// TDEF column descriptor; also used as the runtime tag for every value
    /// crack / encode path. Names mirror mdbtools' <c>MDB_*</c> identifiers
    /// (with the customary <c>T_</c> prefix used throughout this codebase).
    /// </summary>
    public static class ColumnTypes
    {
        /// <summary>mdbtools <c>MDB_BOOL</c> (0x01): 1 bit — stored in the row null-mask, never in the fixed area.</summary>
        public const byte T_BOOL = 0x01;

        /// <summary>mdbtools <c>MDB_BYTE</c> (0x02): 1-byte unsigned integer.</summary>
        public const byte T_BYTE = 0x02;

        /// <summary>mdbtools <c>MDB_INT</c> (0x03): 2-byte signed integer.</summary>
        public const byte T_INT = 0x03;

        /// <summary>mdbtools <c>MDB_LONGINT</c> (0x04): 4-byte signed integer.</summary>
        public const byte T_LONG = 0x04;

        /// <summary>mdbtools <c>MDB_MONEY</c> (0x05): 8-byte int64 / 10000 fixed-point currency.</summary>
        public const byte T_MONEY = 0x05;

        /// <summary>mdbtools <c>MDB_FLOAT</c> (0x06): 4-byte IEEE-754 single-precision float.</summary>
        public const byte T_FLOAT = 0x06;

        /// <summary>mdbtools <c>MDB_DOUBLE</c> (0x07): 8-byte IEEE-754 double-precision float.</summary>
        public const byte T_DOUBLE = 0x07;

        /// <summary>mdbtools <c>MDB_SDATETIME</c> (0x08): 8-byte OLE-Automation date.</summary>
        public const byte T_DATETIME = 0x08;

        /// <summary>mdbtools <c>MDB_BINARY</c> (0x09): variable-length binary, ≤ 255 bytes inline.</summary>
        public const byte T_BINARY = 0x09;

        /// <summary>mdbtools <c>MDB_TEXT</c> (0x0A): variable-length string (UCS-2 in Jet4/ACE, ANSI in Jet3).</summary>
        public const byte T_TEXT = 0x0A;

        /// <summary>mdbtools <c>MDB_OLE</c> (0x0B): long-value (LVAL) OLE blob.</summary>
        public const byte T_OLE = 0x0B;

        /// <summary>mdbtools <c>MDB_MEMO</c> (0x0C): long-value (LVAL) text — stored inline when small.</summary>
        public const byte T_MEMO = 0x0C;

        /// <summary>mdbtools <c>MDB_REPID</c> (0x0F): 16-byte GUID (replication identifier).</summary>
        public const byte T_GUID = 0x0F;

        /// <summary>mdbtools <c>MDB_NUMERIC</c> (0x10): 17-byte scaled decimal (precision + scale + 16-byte mantissa).</summary>
        public const byte T_NUMERIC = 0x10;

        /// <summary>mdbtools <c>MDB_COMPLEX</c> attachment subtype (0x11): Access 2007+ attachment column (complex flat-table backing).</summary>
        public const byte T_ATTACHMENT = 0x11;

        /// <summary>mdbtools <c>MDB_COMPLEX</c> (0x12): Access 2007+ multi-value / version-history column (complex flat-table backing).</summary>
        public const byte T_COMPLEX = 0x12;

        /// <summary>Access 2019+ extended Date/Time (0x14): 42-byte fixed string. No mdbtools symbol — post-dates mdbtools.</summary>
        public const byte T_DATETIMEEXT = 0x14;
    }

    /// <summary>
    /// Standard property names that appear in <c>MSysObjects.LvProp</c> blobs.
    /// </summary>
    public static class ColumnPropertyNames
    {
        public const string DefaultValue = "DefaultValue";
        public const string ValidationRule = "ValidationRule";
        public const string ValidationText = "ValidationText";
        public const string Description = "Description";
        public const string Format = "Format";
        public const string DecimalPlaces = "DecimalPlaces";
        public const string InputMask = "InputMask";
        public const string Caption = "Caption";
        public const string Required = "Required";

        /// <summary>
        /// Calculated-column expression string (Access 2010+, ACCDB only). Written as a
        /// <see cref="Internal.Models.ColumnPropertyBlock.DataTypeMemo"/> entry. Contains the Jet/VBA
        /// expression Microsoft Access evaluates to compute the cached column value
        /// (e.g. <c>"[FirstName] &amp; \" \" &amp; [LastName]"</c>). See Jackcess <c>PropertyMap.EXPRESSION_PROP</c>.
        /// </summary>
        public const string Expression = "Expression";

        /// <summary>
        /// Calculated-column result data type (Access 2010+, ACCDB only). Written as a
        /// <see cref="Internal.Models.ColumnPropertyBlock.DataTypeByte"/> entry holding
        /// the JET column-type code (<see cref="ColumnTypes"/>) of the value the
        /// expression produces. Distinguishes the logical CLR type of the column from
        /// the on-disk storage which always carries a 23-byte calculated-value wrapper.
        /// See Jackcess <c>PropertyMap.RESULT_TYPE_PROP</c>.
        /// </summary>
        public const string ResultType = "ResultType";
    }

    /// <summary>
    /// On-disk constants for Access 2010+ calculated (expression) columns —
    /// translated from Jackcess <c>CalculatedColumnUtil</c> and <c>ColumnImpl</c>.
    /// Calculated columns are an ACCDB-only feature; Jet3 / Jet4 .mdb files
    /// have no <c>OFFSET_COLUMN_EXT_FLAGS</c> slot in the column descriptor and
    /// reject the on-disk markers below.
    /// </summary>
    /// <remarks>
    /// See <c>docs/design/calculated-columns-format-notes.md</c> for the full
    /// on-disk layout.
    /// </remarks>
    public static class CalculatedColumn
    {
        /// <summary>
        /// Bitmask written into the column descriptor's <c>extra flags</c> byte
        /// (descriptor-relative offset 16 in the 25-byte ACE column descriptor)
        /// to mark a column as calculated. Jackcess <c>ColumnImpl.CALCULATED_EXT_FLAG_MASK</c>
        /// = <c>0xC0</c>. The high bit alone (<c>0x80</c>) is sometimes documented; Access
        /// always sets both bits, so we test/emit the full <c>0xC0</c> mask.
        /// </summary>
        public const byte ExtFlagMask = 0xC0;

        /// <summary>
        /// Number of overhead bytes prepended to every stored calculated value.
        /// Jackcess <c>CalculatedColumnUtil.CALC_EXTRA_DATA_LEN</c> = 23.
        /// Layout: 16 reserved/version/CRC bytes (zeroed by this library) + 4-byte
        /// little-endian payload length + 3 unused bytes, then the actual value
        /// bytes encoded per the column's result type.
        /// </summary>
        public const int ExtraDataLen = 23;

        /// <summary>
        /// Byte offset within the wrapper at which the 4-byte little-endian payload
        /// length is stored. Jackcess <c>CalculatedColumnUtil.CALC_DATA_LEN_OFFSET</c> = 16.
        /// </summary>
        public const int DataLenOffset = 16;

        /// <summary>
        /// Byte offset within the wrapper at which the actual value bytes start.
        /// Jackcess <c>CalculatedColumnUtil.CALC_DATA_OFFSET</c> = 20
        /// (= <see cref="DataLenOffset"/> + 4).
        /// </summary>
        public const int DataOffset = 20;

        /// <summary>
        /// Column-descriptor <c>col_len</c> Microsoft Access writes for fixed-length
        /// calculated columns regardless of the result type. Jackcess
        /// <c>CalculatedColumnUtil.CALC_FIXED_FIELD_LEN</c> = 39 (= <see cref="ExtraDataLen"/> + 16).
        /// Variable-length result types (TEXT/MEMO) instead carry the original
        /// <c>col_len</c> + <see cref="ExtraDataLen"/>.
        /// </summary>
        public const short FixedFieldLen = 39;
    }

    /// <summary>
    /// Names of the well-known JET system catalog tables (the <c>MSys*</c>
    /// tables that exist at fixed roles inside every Access database).
    /// </summary>
    public static class SystemTableNames
    {
        public const string Objects = "MSysObjects";
        public const string Indexes = "MSysIndexes";
        public const string IndexColumns = "MSysIndexColumns";
        public const string Relationships = "MSysRelationships";
        public const string ComplexColumns = "MSysComplexColumns";
        public const string Queries = "MSysQueries";
    }

    /// <summary>
    /// Names of the per-database complex-column "template" tables that Access
    /// auto-creates to back attachment / multi-value columns. Their names all
    /// start with <see cref="Prefix"/>.
    /// </summary>
    public static class ComplexTypeNames
    {
        public const string Prefix = "MSysComplexType_";

        public const string Attachment = Prefix + "Attachment";
        public const string UnsignedByte = Prefix + "UnsignedByte";
        public const string Short = Prefix + "Short";
        public const string Long = Prefix + "Long";
        public const string IEEESingle = Prefix + "IEEESingle";
        public const string IEEEDouble = Prefix + "IEEEDouble";
        public const string GUID = Prefix + "GUID";
        public const string Decimal = Prefix + "Decimal";
        public const string Text = Prefix + "Text";
    }

    /// <summary>
    /// Constants describing rows in <c>MSysObjects</c> (catalog object types
    /// and the flag bitmask that distinguishes system tables from user tables).
    /// Names in parentheses correspond to the mdbtools <c>HACKING.md</c>
    /// <c>MDB_TABLE_*</c> / <c>OBJ_*</c> nomenclature.
    /// </summary>
    public static class SystemObjects
    {
        /// <summary>MSysObjects.Type value for a regular user table (mdbtools <c>OBJ_TABLE</c> / <c>MDB_TABLE_USER</c>).</summary>
        public const int UserTableType = 1;

        /// <summary>
        /// Well-known <c>MSysObjects.Id</c> of the <c>"Tables"</c>
        /// pseudo-object (Type = 3) that owns every user table. New
        /// user-table catalog rows MUST set <c>ParentId</c> to this value
        /// — Microsoft Access enumerates tables via the
        /// <c>(ParentId, Name)</c> index and silently filters out rows whose
        /// <c>ParentId</c> does not point at one of the well-known
        /// containers, so a row with <c>ParentId = 0</c> is invisible to
        /// Access even though it is physically present in MSysObjects.
        /// Jackcess names this <c>SYS_PARENT_ID_TABLES</c>.
        /// </summary>
        public const int TablesParentId = 0x0F00_0001;

        /// <summary>MSysObjects.Type value for a linked Jet/Access table (mdbtools <c>MDB_TABLE_LINK</c>).</summary>
        public const int LinkedTableType = 4;

        /// <summary>MSysObjects.Type value for a linked ODBC table (mdbtools <c>MDB_TABLE_LINK_ODBC</c>).</summary>
        public const int LinkedOdbcType = 6;

        /// <summary>
        /// Bitmask applied to <c>MSysObjects.Flags</c> to detect system tables;
        /// any row whose flags AND this mask is non-zero is treated as a system
        /// (hidden) object rather than a user table.
        /// </summary>
        public const uint SystemTableMask = 0x80000002U;
    }

    /// <summary>
    /// Database page sizes in bytes for each JET format generation.
    /// </summary>
    public static class PageSizes
    {
        /// <summary>Page size for Jet3 (.mdb, Access 97). 2048 bytes.</summary>
        public const int Jet3 = 2048;

        /// <summary>Page size for Jet4 (.mdb) and ACE (.accdb). 4096 bytes.</summary>
        public const int Jet4 = 4096;
    }

    /// <summary>
    /// Bit flags stored in the <c>grbit</c> column of <c>MSysRelationships</c>
    /// rows. Values per Jackcess <c>RelationshipImpl</c>.
    /// </summary>
    public static class RelationshipFlags
    {
        /// <summary>When set, referential integrity is NOT enforced.</summary>
        public const uint NoRefIntegrity = 0x00000002;

        /// <summary>When set, updates to the primary key cascade to the foreign key.</summary>
        public const uint CascadeUpdates = 0x00000100;

        /// <summary>When set, deletes of the primary row cascade to dependent rows.</summary>
        public const uint CascadeDeletes = 0x00001000;
    }

    /// <summary>
    /// Sentinel values and sizes for the OLE / Microsoft Compound File Binary
    /// (CFB / OLE2, "MS-CFB") format used by Office Crypto API "Agile"
    /// encrypted .accdb files.
    /// </summary>
    public static class CompoundFile
    {
        /// <summary>CFB v4 sector size in bytes (4096).</summary>
        public const int V4SectorSize = 4096;

        /// <summary>Sector shift for v4 (log₂ of <see cref="V4SectorSize"/>): 2¹² = 4096.</summary>
        public const ushort V4SectorShift = 12;

        /// <summary>CFB major version for v4 (4096-byte sectors).</summary>
        public const ushort V4MajorVersion = 4;

        /// <summary>Size of a single directory entry in bytes (128).</summary>
        public const int DirEntrySize = 128;

        /// <summary>FAT entry value indicating a free / unused sector.</summary>
        public const uint FreeSect = 0xFFFFFFFFu;

        /// <summary>FAT entry value marking the end of a sector chain.</summary>
        public const uint EndOfChain = 0xFFFFFFFEu;

        /// <summary>FAT entry value marking a sector that itself holds FAT data.</summary>
        public const uint FatSect = 0xFFFFFFFDu;

        /// <summary>
        /// Lowest reserved-marker FAT entry value. Any entry &gt;= this value is
        /// a reserved sentinel rather than an addressable sector index.
        /// </summary>
        public const uint FatSectMin = 0xFFFFFFFAu;
    }

    /// <summary>
    /// Magic-byte signatures used to identify wrapped file payloads stored inside
    /// JET OLE columns. Access embeds files in an OLE container header
    /// (~78 bytes) before the actual file bytes, so the scanner probes a sliding
    /// window over the first 512 bytes rather than checking only offset 0.
    /// Patterns are ordered longest-first so the most-specific match wins when
    /// two signatures share a common prefix.
    /// </summary>
    public static class OleMagicBytes
    {
        /// <summary>Gets the JPEG magic bytes: <c>FF D8 FF</c>.</summary>
        public static ReadOnlySpan<byte> Jpeg => [0xFF, 0xD8, 0xFF];

        /// <summary>Gets the PNG magic bytes: <c>89 50 4E 47</c> (<c>\x89PNG</c>).</summary>
        public static ReadOnlySpan<byte> Png => [0x89, 0x50, 0x4E, 0x47];

        /// <summary>Gets the GIF magic bytes: <c>47 49 46</c> (<c>GIF</c>).</summary>
        public static ReadOnlySpan<byte> Gif => [0x47, 0x49, 0x46];

        /// <summary>Gets the BMP magic bytes: <c>42 4D</c> (<c>BM</c>).</summary>
        public static ReadOnlySpan<byte> Bmp => [0x42, 0x4D];

        /// <summary>Gets the little-endian TIFF magic bytes: <c>49 49 2A 00</c>.</summary>
        public static ReadOnlySpan<byte> TiffLittleEndian => [0x49, 0x49, 0x2A, 0x00];

        /// <summary>Gets the big-endian TIFF magic bytes: <c>4D 4D 00 2A</c>.</summary>
        public static ReadOnlySpan<byte> TiffBigEndian => [0x4D, 0x4D, 0x00, 0x2A];

        /// <summary>Gets the PDF magic bytes: <c>25 50 44 46</c> (<c>%PDF</c>).</summary>
        public static ReadOnlySpan<byte> Pdf => [0x25, 0x50, 0x44, 0x46];

        /// <summary>Gets the ZIP / OOXML (DOCX, XLSX, PPTX) magic bytes: <c>50 4B 03 04</c> (<c>PK\x03\x04</c>).</summary>
        public static ReadOnlySpan<byte> Zip => [0x50, 0x4B, 0x03, 0x04];

        /// <summary>Gets the OLE Compound File (DOC, XLS, PPT, …) magic bytes: <c>D0 CF 11 E0</c>.</summary>
        public static ReadOnlySpan<byte> OleCompound => [0xD0, 0xCF, 0x11, 0xE0];

        /// <summary>Gets the RTF magic bytes: <c>7B 5C 72 74</c> (<c>{\rt</c>).</summary>
        public static ReadOnlySpan<byte> Rtf => [0x7B, 0x5C, 0x72, 0x74];
    }

    /// <summary>
    /// Page-layout constants for JET index pages
    /// (leaf <c>0x04</c> and intermediate <c>0x03</c>) as documented in
    /// <c>docs/design/index-and-relationship-format-notes.md</c>
    /// §4.1 (header) and §4.2 (entry-start bitmask + first entry).
    /// The bitmask / first-entry offsets are shared by both page types
    /// because the §4.1 header layout is identical between them.
    /// </summary>
    public static class IndexLeafPage
    {
        /// <summary>Page type byte for index leaf pages.</summary>
        public const byte PageTypeLeaf = 0x04;

        /// <summary>Page type byte for index intermediate pages
        /// (sibling of <see cref="PageTypeLeaf"/> in the same B-tree).</summary>
        public const byte PageTypeIntermediate = 0x03;

        /// <summary>Bitmask offset on a Jet4 leaf page (§4.2).</summary>
        public const int Jet4BitmaskOffset = 0x1B;

        /// <summary>First-entry offset on a Jet4 leaf page (§4.2).</summary>
        public const int Jet4FirstEntryOffset = 0x1E0;

        /// <summary>Bitmask offset on a Jet3 leaf page (§4.2).</summary>
        public const int Jet3BitmaskOffset = 0x16;

        /// <summary>First-entry offset on a Jet3 leaf page (§4.2).</summary>
        public const int Jet3FirstEntryOffset = 0xF8;
    }

    /// <summary>
    /// Fixed algorithm parameters for ECMA-376 §2.3.4 "Agile" encryption
    /// (AES-256-CBC + SHA-512 + PBKDF spin loop) as used by Access 2010 SP1+
    /// and Microsoft 365. These values must agree with the XML descriptor
    /// emitted into the <c>EncryptionInfo</c> stream.
    /// </summary>
    public static class AgileEncryption
    {
        /// <summary>Salt length in bytes (16).</summary>
        public const int SaltSize = 16;

        /// <summary>AES block size in bytes (16).</summary>
        public const int BlockSize = 16;

        /// <summary>AES-256 key length in bytes (32).</summary>
        public const int KeyBytes = 32;

        /// <summary>SHA-512 digest length in bytes (64).</summary>
        public const int HashBytes = 64;

        /// <summary>PBKDF spin count (100 000 iterations).</summary>
        public const int SpinCount = 100_000;

        /// <summary>
        /// Encrypted-package segment size in bytes (4096). Each segment of the
        /// plaintext is encrypted independently with a per-segment IV derived
        /// from the key-data salt and the segment index.
        /// </summary>
        public const int SegmentSize = 4096;
    }
}
