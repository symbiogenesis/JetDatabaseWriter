namespace JetDatabaseWriter.Pages;

using System;
using System.Buffers.Binary;
using JetDatabaseWriter.Enums;

/// <summary>
/// Per-format byte offsets within the data-page (page type 0x01) header that
/// precede the row-offset table. Selected by <see cref="For"/> at construction
/// time so call sites read e.g. <c>_dataPage.NumRows</c> instead of
/// inlining the <c>jet3 ? 8 : 12</c> ternary.
/// </summary>
internal readonly record struct DataPageLayout(int TDefOff, int NumRows, int RowsStart)
{
    /// <summary>Returns the data-page layout for <paramref name="format"/>.</summary>
    public static DataPageLayout For(DatabaseFormat format) => format != DatabaseFormat.Jet3Mdb
        ? new DataPageLayout(TDefOff: 4, NumRows: 12, RowsStart: 14)
        : new DataPageLayout(TDefOff: 4, NumRows: 8, RowsStart: 10);
}

/// <summary>
/// Per-format byte offsets within a TDEF page's table-definition block, plus
/// the size of one real-index entry in the post-block skip region. Used by
/// every TDEF parse / rewrite call site.
/// </summary>
internal readonly record struct TDefHeaderLayout(int NumCols, int NumRealIdx, int BlockEnd, int RealIdxEntrySz)
{
    /// <summary>Returns the TDEF header layout for <paramref name="format"/>.</summary>
    public static TDefHeaderLayout For(DatabaseFormat format) => format != DatabaseFormat.Jet3Mdb
        ? new TDefHeaderLayout(NumCols: 45, NumRealIdx: 51, BlockEnd: 63, RealIdxEntrySz: 12)
        : new TDefHeaderLayout(NumCols: 25, NumRealIdx: 31, BlockEnd: 43, RealIdxEntrySz: 8);
}

/// <summary>
/// Per-format byte offsets within a single column descriptor block, plus the
/// total descriptor size. Jet3 uses an 18-byte descriptor; Jet4/ACE uses 25
/// bytes with extra slots for the complex-column ID and calculated-column
/// flag.
/// </summary>
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
internal readonly record struct RowFieldSizes(int NumCols, int VarEntry, int Eod, int VarLen)
{
    /// <summary>Returns the row-trailer field sizes for <paramref name="format"/>.</summary>
    public static RowFieldSizes For(DatabaseFormat format) => format != DatabaseFormat.Jet3Mdb
        ? new RowFieldSizes(NumCols: 2, VarEntry: 2, Eod: 2, VarLen: 2)
        : new RowFieldSizes(NumCols: 1, VarEntry: 1, Eod: 1, VarLen: 1);

    /// <summary>Reads a <see cref="NumCols"/>-sized little-endian unsigned int (1 or 2 bytes) from <paramref name="page"/> at <paramref name="off"/>.</summary>
    public int ReadNumCols(ReadOnlySpan<byte> page, int off) =>
        NumCols == 2 ? BinaryPrimitives.ReadUInt16LittleEndian(page.Slice(off, 2)) : page[off];

    /// <summary>Reads a <see cref="VarEntry"/>-sized little-endian unsigned int (1 or 2 bytes) from <paramref name="page"/> at <paramref name="off"/>.</summary>
    public int ReadVarEntry(ReadOnlySpan<byte> page, int off) =>
        VarEntry == 2 ? BinaryPrimitives.ReadUInt16LittleEndian(page.Slice(off, 2)) : page[off];

    /// <summary>Reads a <see cref="VarLen"/>-sized little-endian unsigned int (1 or 2 bytes) from <paramref name="page"/> at <paramref name="off"/>.</summary>
    public int ReadVarLen(ReadOnlySpan<byte> page, int off) =>
        VarLen == 2 ? BinaryPrimitives.ReadUInt16LittleEndian(page.Slice(off, 2)) : page[off];

    /// <summary>Reads an <see cref="Eod"/>-sized little-endian unsigned int (1 or 2 bytes) from <paramref name="page"/> at <paramref name="off"/>.</summary>
    public int ReadEod(ReadOnlySpan<byte> page, int off) =>
        Eod == 2 ? BinaryPrimitives.ReadUInt16LittleEndian(page.Slice(off, 2)) : page[off];
}
