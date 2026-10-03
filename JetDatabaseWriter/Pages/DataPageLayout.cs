namespace JetDatabaseWriter.Pages;

using System;
using JetDatabaseWriter.Enums;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Per-format byte offsets within the data-page (page type 0x01) header that
/// precede the row-offset table. Selected by <see cref="For"/> at construction
/// time so call sites read e.g. <c>_dataPage.NumRows</c> instead of
/// inlining the <c>jet3 ? 8 : 12</c> ternary.
/// </summary>
/// <param name="TDefOff">The TDEF offset.</param>
/// <param name="NumRows">The number of rows.</param>
/// <param name="RowsStart">The rows start.</param>
internal readonly record struct DataPageLayout(int TDefOff, int NumRows, int RowsStart)
{
    /// <summary>Returns the data-page layout for <paramref name="format"/>.</summary>
    /// <param name="format">The format.</param>
    public static DataPageLayout For(DatabaseFormat format) => format != DatabaseFormat.Jet3Mdb
        ? new DataPageLayout(TDefOff: 4, NumRows: 12, RowsStart: 14)
        : new DataPageLayout(TDefOff: 4, NumRows: 8, RowsStart: 10);

    /// <summary>
    /// Returns the longest row an empty data page of <paramref name="pageSize"/>
    /// bytes holds: the page less its header and one 2-byte row-offset slot,
    /// 2,036 bytes on Jet3 and 4,080 on Jet4/ACE.
    /// </summary>
    /// <param name="pageSize">The page size in bytes.</param>
    public int MaxRowLength(int pageSize) => pageSize - this.RowsStart - 2;
}

/// <summary>
/// Per-format layout of an LVAL page: a data page (type <c>0x01</c>) whose
/// owner field at offset 4 holds the <c>"LVAL"</c> signature instead of a TDEF
/// page number. The row count and row-offset table sit where
/// <see cref="DataPage"/> puts them on any data page; the rest follows the
/// pages Access writes. On Jet4/ACE a full chained row starts at offset 20,
/// leaving 4 bytes of free space, and bytes 8-11 are unused (Access leaves
/// them zero; the writer stores its LVAL token there). On Jet3, Access 97
/// starts a full chained row at 12, right after the one-entry row-offset
/// table, packs a single-page row or the last chunk at the end of the page,
/// and stores no token anywhere, since offset 8 is the row count (measured on
/// test2V1997.mdb pages 37-49 and nwind.mdb).
/// </summary>
/// <param name="DataPage">The data-page header layout (row count and row-offset table).</param>
/// <param name="MinRowStart">The lowest row start of a one-row LVAL page; a row's payload capacity is the page size minus this.</param>
/// <param name="WritesToken">Whether bytes 8-11 are free for the LVAL token (Jet4/ACE only).</param>
/// <param name="PackRowsAtEnd">Whether every row is written at the end of its page, as Access 97 does, rather than at <paramref name="MinRowStart"/>.</param>
internal readonly record struct LvalPageLayout(DataPageLayout DataPage, int MinRowStart, bool WritesToken, bool PackRowsAtEnd)
{
    /// <summary>Returns the LVAL page layout for <paramref name="format"/>.</summary>
    /// <param name="format">The format.</param>
    public static LvalPageLayout For(DatabaseFormat format) => format != DatabaseFormat.Jet3Mdb
        ? new LvalPageLayout(DataPageLayout.For(format), MinRowStart: 20, WritesToken: true, PackRowsAtEnd: false)
        : new LvalPageLayout(DataPageLayout.For(format), MinRowStart: 12, WritesToken: false, PackRowsAtEnd: true);

    /// <summary>
    /// Returns the largest payload one LVAL row holds on a page of
    /// <paramref name="pageSize"/> bytes: 2036 on Jet3, 4076 on Jet4/ACE.
    /// </summary>
    /// <param name="pageSize">The page size in bytes.</param>
    public int SinglePagePayloadCapacity(int pageSize) => pageSize - this.MinRowStart;

    /// <summary>
    /// Returns the payload bytes one chained LVAL row holds after its 4-byte
    /// next-row pointer: 2032 on Jet3, Access 97's chunk size, and 4072 on Jet4/ACE.
    /// </summary>
    /// <param name="pageSize">The page size in bytes.</param>
    public int ChainedPagePayloadCapacity(int pageSize) => this.SinglePagePayloadCapacity(pageSize) - 4;

    /// <summary>
    /// Returns the free-space field (offset 2) of a one-row LVAL page whose row
    /// starts at <paramref name="rowStart"/>: the gap between the one-entry
    /// row-offset table and the row.
    /// </summary>
    /// <param name="rowStart">The row start offset.</param>
    public int FreeSpace(int rowStart) => rowStart - (this.DataPage.RowsStart + 2);
}

/// <summary>
/// Per-format byte offsets within a TDEF page's table-definition block, plus
/// the size of one real-index entry in the post-block skip region. Used by
/// every TDEF parse / rewrite call site. Jet4/ACE inserts a 4-byte field at
/// offset 12 and a 16-byte block after the AutoNumber counter, so every
/// header field after <c>tdef_len</c> sits at a different offset in Jet3
/// (mdbtools HACKING.md, Jackcess <c>JetFormat</c>).
/// </summary>
/// <param name="NumRows">Offset of the live-row count (uint32).</param>
/// <param name="AutoNumber">Offset of the AutoNumber counter (uint32): the last value handed out.</param>
/// <param name="TableType">Offset of the table-type byte (<c>0x4E</c> user, <c>0x53</c> system).</param>
/// <param name="MaxCols">Offset of <c>max_cols</c> (uint16).</param>
/// <param name="NumVarCols">Offset of <c>num_var_cols</c> (uint16).</param>
/// <param name="NumCols">Offset of <c>num_cols</c> (uint16).</param>
/// <param name="NumIdx">Offset of <c>num_idx</c>, the logical index count (int32).</param>
/// <param name="NumRealIdx">Offset of <c>num_real_idx</c>, the physical index count (int32).</param>
/// <param name="UsedPages">Offset of the owned-pages usage-map pointer (1-byte row + 3-byte page).</param>
/// <param name="FreePages">Offset of the free-space usage-map pointer (1-byte row + 3-byte page).</param>
/// <param name="BlockEnd">The block end.</param>
/// <param name="RealIdxEntrySz">The real index entry size.</param>
/// <param name="ComplexAutoNumber">
/// Offset of the complex AutoNumber (uint32): the last per-row complex
/// reference handed out to the table's Attachment / multi-value / version
/// history columns. ACE only (offset 28; Jackcess <c>JetFormat</c>
/// <c>OFFSET_NEXT_COMPLEX_AUTO_NUMBER</c>, mdbtools <c>ct_autonum</c>);
/// -1 on Jet3 and Jet4, which have no complex columns.
/// </param>
internal readonly record struct TDefHeaderLayout(
    int NumRows,
    int AutoNumber,
    int TableType,
    int MaxCols,
    int NumVarCols,
    int NumCols,
    int NumIdx,
    int NumRealIdx,
    int UsedPages,
    int FreePages,
    int BlockEnd,
    int RealIdxEntrySz,
    int ComplexAutoNumber)
{
    /// <summary>Returns the TDEF header layout for <paramref name="format"/>.</summary>
    /// <param name="format">The format.</param>
    public static TDefHeaderLayout For(DatabaseFormat format) => format != DatabaseFormat.Jet3Mdb
        ? new TDefHeaderLayout(
            NumRows: 16,
            AutoNumber: 20,
            TableType: 40,
            MaxCols: 41,
            NumVarCols: 43,
            NumCols: 45,
            NumIdx: 47,
            NumRealIdx: 51,
            UsedPages: 55,
            FreePages: 59,
            BlockEnd: 63,
            RealIdxEntrySz: 12,
            ComplexAutoNumber: format == DatabaseFormat.AceAccdb ? 28 : -1)
        : new TDefHeaderLayout(
            NumRows: 12,
            AutoNumber: 16,
            TableType: 20,
            MaxCols: 21,
            NumVarCols: 23,
            NumCols: 25,
            NumIdx: 27,
            NumRealIdx: 31,
            UsedPages: 35,
            FreePages: 39,
            BlockEnd: 43,
            RealIdxEntrySz: 8,
            ComplexAutoNumber: -1);

    /// <summary>Gets the offset of the owned-pages usage-map page number (3 bytes after the row byte).</summary>
    public int UsedPagesPage => this.UsedPages + 1;

    /// <summary>Gets the offset of the free-space usage-map page number (3 bytes after the row byte).</summary>
    public int FreePagesPage => this.FreePages + 1;
}

/// <summary>
/// Per-format byte offsets within a single column descriptor block, plus the
/// total descriptor size. Jet3 uses an 18-byte descriptor; Jet4/ACE uses 25
/// bytes with extra slots for the complex-column ID and calculated-column
/// flag.
/// </summary>
/// <param name="Size">The size in bytes.</param>
/// <param name="TypeOff">The type off.</param>
/// <param name="VarOff">The var off.</param>
/// <param name="FixedOff">The fixed off.</param>
/// <param name="SzOff">The size off.</param>
/// <param name="FlagsOff">The flags off.</param>
/// <param name="NumOff">The number of off.</param>
/// <param name="MiscOff">The misc off.</param>
internal readonly record struct ColumnDescriptorLayout(
    int Size,
    int TypeOff,
    int VarOff,
    int FixedOff,
    int SzOff,
    int FlagsOff,
    int NumOff,
    int MiscOff)
{
    /// <summary>Returns the column-descriptor layout for <paramref name="format"/>.</summary>
    /// <param name="format">The format.</param>
    public static ColumnDescriptorLayout For(DatabaseFormat format) => format != DatabaseFormat.Jet3Mdb
        ? new ColumnDescriptorLayout(
            Size: 25,
            TypeOff: 0, // col_type (1)
            VarOff: 7, // offset_V (2): 1+4+2
            FixedOff: 21, // offset_F (2): 1+4+2+2+2+2+2+1+1+4
            SzOff: 23, // col_len (2)
            FlagsOff: 15, // bitmask (1): 1+4+2+2+2+2+2
            NumOff: 5, // col_num (2)
            MiscOff: 11) // misc (4): 1+4+2+2+2 — carries ComplexID for complex columns
        : new ColumnDescriptorLayout(
            Size: 18,
            TypeOff: 0, // col_type (1)
            VarOff: 3, // offset_V (2): 1+2
            FixedOff: 14, // offset_F (2): 1+2+2+2+2+2+2+1
            SzOff: 16, // col_len (2)
            FlagsOff: 13, // bitmask (1)
            NumOff: 1, // col_num (2)
            MiscOff: 7); // misc (4) — Jet3 has no complex columns; included for layout symmetry
}

/// <summary>
/// Per-format byte sizes of the in-row trailer fields that vary between
/// Jet3 (1-byte fields) and Jet4/ACE (2-byte fields): the leading
/// <c>num_cols</c> count, each <c>var_table</c> entry, the trailing
/// <c>var_len</c> count, and the EOD pointer.
/// </summary>
/// <param name="NumCols">The number of cols.</param>
/// <param name="VarEntry">The var entry.</param>
/// <param name="Eod">The end-of-data marker size.</param>
/// <param name="VarLen">The var len.</param>
internal readonly record struct RowFieldSizes(int NumCols, int VarEntry, int Eod, int VarLen)
{
    /// <summary>Returns the row-trailer field sizes for <paramref name="format"/>.</summary>
    /// <param name="format">The format.</param>
    public static RowFieldSizes For(DatabaseFormat format) => format != DatabaseFormat.Jet3Mdb
        ? new RowFieldSizes(NumCols: 2, VarEntry: 2, Eod: 2, VarLen: 2)
        : new RowFieldSizes(NumCols: 1, VarEntry: 1, Eod: 1, VarLen: 1);

    /// <summary>Reads a <see cref="NumCols"/>-sized little-endian unsigned int (1 or 2 bytes) from <paramref name="page"/> at <paramref name="off"/>.</summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="off">The byte offset.</param>
    public int ReadNumCols(ReadOnlySpan<byte> page, int off) =>
        this.NumCols == 2 ? Ru16(page, off) : page[off];

    /// <summary>Reads a <see cref="VarEntry"/>-sized little-endian unsigned int (1 or 2 bytes) from <paramref name="page"/> at <paramref name="off"/>.</summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="off">The byte offset.</param>
    public int ReadVarEntry(ReadOnlySpan<byte> page, int off) =>
        this.VarEntry == 2 ? Ru16(page, off) : page[off];

    /// <summary>Reads a <see cref="VarLen"/>-sized little-endian unsigned int (1 or 2 bytes) from <paramref name="page"/> at <paramref name="off"/>.</summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="off">The byte offset.</param>
    public int ReadVarLen(ReadOnlySpan<byte> page, int off) =>
        this.VarLen == 2 ? Ru16(page, off) : page[off];

    /// <summary>Reads an <see cref="Eod"/>-sized little-endian unsigned int (1 or 2 bytes) from <paramref name="page"/> at <paramref name="off"/>.</summary>
    /// <param name="page">The page bytes.</param>
    /// <param name="off">The byte offset.</param>
    public int ReadEod(ReadOnlySpan<byte> page, int off) =>
        this.Eod == 2 ? Ru16(page, off) : page[off];
}
