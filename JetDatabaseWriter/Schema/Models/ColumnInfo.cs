namespace JetDatabaseWriter.Schema.Models;

using JetDatabaseWriter.Enums;
using static JetDatabaseWriter.Enums.ColumnType;

internal sealed class ColumnInfo
{
    public ColumnType Type { get; init; }

    /// <summary>
    /// Gets col_num: absolute column number (includes deleted cols).
    /// </summary>
    public int ColNum { get; init; }

    /// <summary>
    /// Gets offset_V: 0-based index in var_table.
    /// </summary>
    public int VarIdx { get; init; }

    /// <summary>
    /// Gets offset_F: byte offset within the fixed area.
    /// </summary>
    public int FixedOff { get; init; }

    /// <summary>
    /// Gets col_len (0 for MEMO/OLE/variable).
    /// </summary>
    public int Size { get; init; }

    public byte Flags { get; init; }

    /// <summary>
    /// Gets the byte at descriptor-relative offset 16 in the 25-byte
    /// ACE column descriptor (Jackcess <c>OFFSET_COLUMN_EXT_FLAGS</c>). Only
    /// populated for Jet4 / ACE files — the 18-byte Jet3 column descriptor has
    /// no equivalent slot, so this stays at <c>0</c>. The high two bits
    /// (<see cref="Constants.CalculatedColumn.ExtFlagMask"/>) mark Access 2010+
    /// calculated (expression) columns; the low bit (<c>0x01</c>) is
    /// Jackcess <c>COMPRESSED_UNICODE_EXT_FLAG_MASK</c>.
    /// </summary>
    public byte ExtraFlags { get; init; }

    /// <summary>
    /// Gets the logical JET type code stored in the calculated
    /// column's <c>ResultType</c> LvProp property. The descriptor
    /// <see cref="Type"/> still controls row storage layout, but the wrapped
    /// cached payload is encoded as this type when present.
    /// </summary>
    public ColumnType CalculatedResultType { get; init; }

    /// <summary>
    /// Gets a value indicating whether the column is an Access 2010+ calculated
    /// (expression) column — i.e. the <see cref="Constants.CalculatedColumn.ExtFlagMask"/>
    /// bits are set in <see cref="ExtraFlags"/>. Calculated columns store every
    /// value behind a 23-byte wrapper (see <c>CalculatedColumnUtil</c>) and
    /// surface their original column type via the <c>ResultType</c> property in
    /// <c>MSysObjects.LvProp</c>; the column-descriptor <c>col_type</c> byte
    /// already mirrors that result type for the columns Access produces.
    /// </summary>
    public bool IsCalculated => (this.ExtraFlags & Constants.CalculatedColumn.ExtFlagMask) == Constants.CalculatedColumn.ExtFlagMask;

    /// <summary>
    /// Gets a value indicating whether this text/memo column supports
    /// Jet4/ACE compressed-unicode encoding (<c>0xFF 0xFE</c> marker +
    /// 1 byte per Latin-1 character). True when the low bit of
    /// <see cref="ExtraFlags"/> is set
    /// (Jackcess <c>COMPRESSED_UNICODE_EXT_FLAG_MASK</c>).
    /// Always false for Jet3 columns (no ExtraFlags slot).
    /// </summary>
    public bool IsCompressedUnicode => (this.ExtraFlags & Constants.CompressedUnicodeExtFlagMask) != 0;

    public string Name { get; init; } = string.Empty;

    /// <summary>
    /// Gets the 4-byte value at descriptor-relative offset 11 (Jet4/ACE)
    /// of the TDEF column descriptor — the <c>misc</c> / <c>misc_ext</c> slot.
    /// For complex columns (<c>Attachment</c> / <c>Complex</c>) this carries
    /// the <c>ComplexID</c> that joins the parent column to its
    /// <c>MSysComplexColumns</c> row and (transitively) to the hidden flat child
    /// table. Zero for non-complex columns.
    /// See <see href="docs/design/complex-columns-format-notes.md" /> §2.1.
    /// </summary>
    public int Misc { get; init; }

    /// <summary>
    /// Gets the declared precision (total significant digits, 1..28)
    /// for a <c>Numeric</c> column. Persisted at descriptor-relative offset
    /// 11 (the first byte of <see cref="Misc"/> for Jet4 / ACE column
    /// descriptors). Zero for non-numeric columns.
    /// </summary>
    public byte NumericPrecision { get; init; }

    /// <summary>
    /// Gets the declared scale (decimal places, 0..28) for a
    /// <c>Numeric</c> column. Persisted at descriptor-relative offset 12
    /// (the second byte of <see cref="Misc"/>). The incremental fast paths
    /// use this value as the canonical index scale, rescaling every cell
    /// value via <see cref="System.MidpointRounding.ToEven"/> rounding
    /// before the encoder runs — matching Access semantics that every
    /// <c>Numeric</c> cell sorts at the column's declared scale.
    /// </summary>
    public byte NumericScale { get; init; }

    public ColumnInfo WithCalculatedResultType(ColumnType calculatedResultType) => new()
    {
        Type = this.Type,
        ColNum = this.ColNum,
        VarIdx = this.VarIdx,
        FixedOff = this.FixedOff,
        Size = this.Size,
        Flags = this.Flags,
        ExtraFlags = this.ExtraFlags,
        CalculatedResultType = calculatedResultType,
        Name = this.Name,
        Misc = this.Misc,
        NumericPrecision = this.NumericPrecision,
        NumericScale = this.NumericScale,
    };

    /// <summary>
    /// Gets a value indicating whether a column's data is stored in the fixed or variable
    /// area of the row. (representing the FLAG_FIXED bit (0x01) in the TDEF column descriptor)
    /// For most "inherently fixed" types (BOOL, LONG, DOUBLE, etc.) the bit is set,
    /// but Access system tables (e.g. complex-field flat tables) may store these
    /// types in the variable area with FLAG_FIXED cleared.
    /// Variable-length types (TEXT, BINARY, MEMO, OLE) are always variable.
    /// </summary>
    public bool IsFixed
    {
        get
        {
            // BOOL stores its value in the null mask, never in fixed area.
            if (this.Type == BooleanType && !this.IsCalculated)
            {
                return true;
            }

            // TEXT/BINARY/MEMO/OLE always live in the variable area.
            if (JetTypeInfo.IsAlwaysVariableLength(this.Type))
            {
                return false;
            }

            return (this.Flags & Constants.ColumnDescriptorFlags.Fixed) != 0;
        }
    }
}
