namespace JetDatabaseWriter;

/// <summary>
/// Central container for project-wide constant values, grouped into nested
/// static classes by topic.
/// </summary>
internal static class Constants
{
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
    /// </summary>
    public static class SystemObjects
    {
        /// <summary>MSysObjects.Type value for a regular user table.</summary>
        public const int UserTableType = 1;

        /// <summary>MSysObjects.Type value for a linked Jet/Access table.</summary>
        public const int LinkedTableType = 4;

        /// <summary>MSysObjects.Type value for a linked ODBC table.</summary>
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
        public const int SectorSize = 4096;

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
}
