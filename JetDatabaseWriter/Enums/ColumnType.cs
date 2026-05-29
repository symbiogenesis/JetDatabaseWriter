namespace JetDatabaseWriter.Enums;

using System.Diagnostics.CodeAnalysis;

/// <summary>
/// JET column-type discriminator codes as documented in the mdbtools
/// <see href="HACKING.md" /> reference. Stored in the <c>col_type</c> byte of each
/// TDEF column descriptor; also used as the runtime tag for every value
/// crack / encode path. Summaries retain the matching mdbtools
/// <c>MDB_*</c> identifiers while the members use idiomatic C# names.
/// </summary>
[SuppressMessage("Design", "CA1008:Enums should have zero value", Justification = "Zero is not a valid JET column-type discriminator; default is reserved for absence at internal boundaries.")]
[SuppressMessage("Design", "CA1027:Mark enums with FlagsAttribute", Justification = "JET column-type discriminator codes are mutually exclusive values, not bit flags.")]
[SuppressMessage("Design", "CA1028:Enum Storage should be Int32", Justification = "JET stores this discriminator as an on-disk byte and the enum models that binary contract.")]
public enum ColumnType : byte
{
    /// <summary>mdbtools <c>MDB_BOOL</c> (0x01): 1 bit — stored in the row null-mask, never in the fixed area.</summary>
    BooleanType = 0x01,

    /// <summary>mdbtools <c>MDB_BYTE</c> (0x02): 1-byte unsigned integer.</summary>
    ByteType = 0x02,

    /// <summary>mdbtools <c>MDB_INT</c> (0x03): 2-byte signed integer.</summary>
    IntegerType = 0x03,

    /// <summary>mdbtools <c>MDB_LONGINT</c> (0x04): 4-byte signed integer.</summary>
    LongIntegerType = 0x04,

    /// <summary>mdbtools <c>MDB_MONEY</c> (0x05): 8-byte int64 / 10000 fixed-point currency.</summary>
    MoneyType = 0x05,

    /// <summary>mdbtools <c>MDB_FLOAT</c> (0x06): 4-byte IEEE-754 single-precision float.</summary>
    FloatType = 0x06,

    /// <summary>mdbtools <c>MDB_DOUBLE</c> (0x07): 8-byte IEEE-754 double-precision float.</summary>
    DoubleType = 0x07,

    /// <summary>mdbtools <c>MDB_SDATETIME</c> (0x08): 8-byte OLE-Automation date.</summary>
    DateTimeType = 0x08,

    /// <summary>mdbtools <c>MDB_BINARY</c> (0x09): variable-length binary, ≤ 255 bytes inline.</summary>
    BinaryType = 0x09,

    /// <summary>mdbtools <c>MDB_TEXT</c> (0x0A): variable-length string (UCS-2 in Jet4/ACE, ANSI in Jet3).</summary>
    TextType = 0x0A,

    /// <summary>mdbtools <c>MDB_OLE</c> (0x0B): long-value (LVAL) OLE blob.</summary>
    OleType = 0x0B,

    /// <summary>mdbtools <c>MDB_MEMO</c> (0x0C): long-value (LVAL) text — stored inline when small.</summary>
    MemoType = 0x0C,

    /// <summary>mdbtools <c>MDB_REPID</c> (0x0F): 16-byte GUID (replication identifier).</summary>
    GuidType = 0x0F,

    /// <summary>mdbtools <c>MDB_NUMERIC</c> (0x10): 17-byte scaled decimal cell (sign + 16-byte magnitude; descriptor carries scale).</summary>
    NumericType = 0x10,

    /// <summary>Legacy/private attachment alias (0x11). Access-authored ACCDB complex columns use <see cref="ComplexType"/> and classify attachments via <c>MSysComplexColumns</c>.</summary>
    AttachmentType = 0x11,

    /// <summary>Access 2007+ complex parent column (0x12): attachment, multi-value, or version-history with hidden flat-table backing.</summary>
    ComplexType = 0x12,

    /// <summary>Access 2019+ extended Date/Time (0x14): 42-byte fixed string. No mdbtools symbol — post-dates mdbtools.</summary>
    DateTimeExtendedType = 0x14,
}
