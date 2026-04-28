namespace JetDatabaseWriter.Internal.Models;

internal sealed class ColumnInfo
{
    public byte Type { get; set; }

    public int ColNum { get; set; } // col_num: absolute column number (includes deleted cols)

    public int VarIdx { get; set; } // offset_V: 0-based index in var_table

    public int FixedOff { get; set; } // offset_F: byte offset within the fixed area

    public int Size { get; set; } // col_len (0 for MEMO/OLE/variable)

    public byte Flags { get; set; }

    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the 4-byte value at descriptor-relative offset 11 (Jet4/ACE)
    /// of the TDEF column descriptor — the <c>misc</c> / <c>misc_ext</c> slot.
    /// For complex columns (<c>T_ATTACHMENT</c> / <c>T_COMPLEX</c>) this carries
    /// the <c>ComplexID</c> that joins the parent column to its
    /// <c>MSysComplexColumns</c> row and (transitively) to the hidden flat child
    /// table. Zero for non-complex columns.
    /// See <c>docs/design/complex-columns-format-notes.md</c> §2.1.
    /// </summary>
    public int Misc { get; set; }

    /// <summary>
    /// Gets or sets the declared precision (total significant digits, 1..28)
    /// for a <c>T_NUMERIC</c> column. Persisted at descriptor-relative offset
    /// 11 (the first byte of <see cref="Misc"/> for Jet4 / ACE column
    /// descriptors). Zero for non-numeric columns.
    /// </summary>
    public byte NumericPrecision { get; set; }

    /// <summary>
    /// Gets or sets the declared scale (decimal places, 0..28) for a
    /// <c>T_NUMERIC</c> column. Persisted at descriptor-relative offset 12
    /// (the second byte of <see cref="Misc"/>). The incremental fast paths
    /// use this value as the canonical index scale, rescaling every cell
    /// value via <see cref="System.MidpointRounding.ToEven"/> rounding
    /// before the encoder runs — matching Access semantics that every
    /// <c>T_NUMERIC</c> cell sorts at the column's declared scale.
    /// </summary>
    public byte NumericScale { get; set; }

    // The FLAG_FIXED bit (0x01) in the TDEF column descriptor determines whether
    // a column's data is stored in the fixed or variable area of the row.
    // For most "inherently fixed" types (BOOL, LONG, DOUBLE, etc.) the bit is set,
    // but Access system tables (e.g. complex-field flat tables) may store these
    // types in the variable area with FLAG_FIXED cleared.
    // Variable-length types (TEXT, BINARY, MEMO, OLE) are always variable.
    public bool IsFixed
    {
        get
        {
            switch (Type)
            {
                case 0x01: // T_BOOL
                    // BOOL stores its value in the null mask, never in fixed area.
                    return true;
                case 0x0A: // T_TEXT
                case 0x09: // T_BINARY
                case 0x0C: // T_MEMO
                case 0x0B: // T_OLE
                    return false;
                default:
                    return (Flags & 0x01) != 0; // FLAG_FIXED
            }
        }
    }
}
