namespace JetDatabaseWriter;

internal sealed class ColumnInfo
{
    public byte Type { get; set; }

    public int ColNum { get; set; } // col_num: absolute column number (includes deleted cols)

    public int VarIdx { get; set; } // offset_V: 0-based index in var_table

    public int FixedOff { get; set; } // offset_F: byte offset within the fixed area

    public int Size { get; set; } // col_len (0 for MEMO/OLE/variable)

    public byte Flags { get; set; }

    public string Name { get; set; } = string.Empty;

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
