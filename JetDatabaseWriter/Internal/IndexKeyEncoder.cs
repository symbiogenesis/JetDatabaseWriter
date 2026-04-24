namespace JetDatabaseWriter;

using System;
using System.Globalization;

/// <summary>
/// JET index sort-key encoder for fixed-width numeric and date/time column types
/// (W2 phase). Encodes a single column value into the per-entry byte sequence
/// described in <c>docs/design/index-and-relationship-format-notes.md</c> §4.3
/// (entry flag byte) and §5 (per-type sort-key encoding).
/// <para>
/// Supported column types in W2: <c>T_BYTE (0x02)</c>, <c>T_INT (0x03)</c>,
/// <c>T_LONG (0x04)</c>, <c>T_MONEY (0x05)</c>, <c>T_FLOAT (0x06)</c>,
/// <c>T_DOUBLE (0x07)</c>, <c>T_DATETIME (0x08)</c>. Text, MEMO, OLE, BINARY,
/// GUID, NUMERIC, complex, and DATETIMEEXT are deferred to later writer
/// phases (W7 for text; the rest have not been spec'd yet).
/// </para>
/// <para>
/// The encoded layout is one flag byte (0x7F asc / 0x80 desc for non-null,
/// 0x00 asc / 0xFF desc for null) followed by the encoded key bytes (omitted
/// for null entries). For ascending fixed-width keys the encoder writes the
/// value in big-endian order with the high bit of the most-significant byte
/// inverted (signed integers and floating-point), so a lexicographic sort
/// over the resulting bytes matches the natural numeric order. For descending,
/// every byte produced for the ascending form is one's-complemented, which is
/// the convention HACKING.md describes for descending-text indexes and which
/// preserves order for the numeric encodings as well.
/// </para>
/// <para>
/// <b>Validation status:</b> the per-type byte sequences below match the
/// conventional B-tree encoding documented in HACKING.md and used by Jackcess.
/// They have NOT been byte-compared against a real Access-authored index leaf
/// (no fixture in this repo carries one for these specific types, and the
/// writer pipeline that would let us synthesise one is W3+, which is what
/// uses this encoder). Round-trip via the in-repo reader still works because
/// the reader does not consult leaf pages today; Microsoft Access itself is
/// the only consumer that will exercise these bytes, and it must validate
/// after a Compact &amp; Repair (see §8 of the design doc).
/// </para>
/// </summary>
internal static class IndexKeyEncoder
{
    // Column type codes — duplicated here so this file does not need to
    // inherit from AccessBase (the constants there are private protected).
    private const byte T_BOOL = 0x01;
    private const byte T_BYTE = 0x02;
    private const byte T_INT = 0x03;
    private const byte T_LONG = 0x04;
    private const byte T_MONEY = 0x05;
    private const byte T_FLOAT = 0x06;
    private const byte T_DOUBLE = 0x07;
    private const byte T_DATETIME = 0x08;

    // Entry flag bytes — see §4.3.
    internal const byte FlagAscendingNonNull = 0x7F;
    internal const byte FlagDescendingNonNull = 0x80;
    internal const byte FlagAscendingNull = 0x00;
    internal const byte FlagDescendingNull = 0xFF;

    /// <summary>
    /// Returns the entry-flag + key-bytes block for a single column value.
    /// For null values the result is a single flag byte; for non-null values
    /// it is the flag byte followed by the encoded key bytes. The caller is
    /// responsible for concatenating per-column blocks (in column-map order)
    /// and appending the trailing 3-byte data page + 1-byte data row record
    /// pointer described in §4.3.
    /// </summary>
    /// <param name="columnType">JET column type code (e.g. <c>T_LONG = 0x04</c>).</param>
    /// <param name="value">Value to encode. <see langword="null"/> and
    /// <see cref="DBNull"/> are both treated as the SQL null marker.</param>
    /// <param name="ascending">Sort direction. <see langword="true"/> yields
    /// the ascending encoding; <see langword="false"/> ones-complements every
    /// byte of the ascending form.</param>
    /// <exception cref="NotSupportedException">The column type is outside the
    /// W2 supported set.</exception>
    /// <exception cref="ArgumentException">The value cannot be coerced to the
    /// .NET representation expected by <paramref name="columnType"/>.</exception>
    public static byte[] EncodeEntry(byte columnType, object? value, bool ascending = true)
    {
        bool isNull = value is null || value is DBNull;
        if (isNull)
        {
            return new[] { ascending ? FlagAscendingNull : FlagDescendingNull };
        }

        byte[] key = EncodeKey(columnType, value!);
        byte[] result = new byte[1 + key.Length];

        // Always emit the ascending flag here; if descending, the loop below
        // ones-complements the entire block (turning 0x7F → 0x80, etc.) per §5.
        result[0] = FlagAscendingNonNull;
        Buffer.BlockCopy(key, 0, result, 1, key.Length);

        if (!ascending)
        {
            // §5: descending = ones-complement of ascending encoding.
            for (int i = 0; i < result.Length; i++)
            {
                result[i] = unchecked((byte)~result[i]);
            }
        }

        return result;
    }

    private static byte[] EncodeKey(byte columnType, object value)
    {
        switch (columnType)
        {
            case T_BYTE:
                // Access "Byte" is unsigned 0..255 — no sign bit to flip.
                return new[] { ToByte(value) };

            case T_INT:
                return EncodeSignedBigEndian(ToInt16(value), 2);

            case T_LONG:
                return EncodeSignedBigEndian(ToInt32(value), 4);

            case T_MONEY:
                {
                    // Currency is stored as int64 = decimal × 10000.
                    decimal d = ToDecimal(value);
                    long scaled = decimal.ToInt64(decimal.Round(d * 10000m, 0, MidpointRounding.AwayFromZero));
                    return EncodeSignedBigEndian(scaled, 8);
                }

            case T_FLOAT:
                return EncodeIeeeBigEndian(BitConverter.GetBytes(ToSingle(value)));

            case T_DOUBLE:
                return EncodeIeeeBigEndian(BitConverter.GetBytes(ToDouble(value)));

            case T_DATETIME:
                {
                    DateTime dt = ToDateTime(value);
                    return EncodeIeeeBigEndian(BitConverter.GetBytes(dt.ToOADate()));
                }

            case T_BOOL:
                throw new NotSupportedException("BOOL columns are stored in the row null mask, not in index key bytes.");

            default:
                throw new NotSupportedException(
                    $"Index key encoding for column type 0x{columnType:X2} is not supported in this writer phase. " +
                    "Supported types: BYTE, INT, LONG, MONEY, FLOAT, DOUBLE, DATETIME.");
        }
    }

    /// <summary>
    /// Big-endian signed-integer encoding with the high bit of the most
    /// significant byte inverted, so two's-complement values sort correctly
    /// as unsigned bytes (negative values precede non-negative values).
    /// </summary>
    private static byte[] EncodeSignedBigEndian(long value, int byteCount)
    {
        byte[] result = new byte[byteCount];
        ulong u = unchecked((ulong)value);
        for (int i = byteCount - 1; i >= 0; i--)
        {
            result[i] = (byte)(u & 0xFF);
            u >>= 8;
        }

        result[0] ^= 0x80;
        return result;
    }

    /// <summary>
    /// IEEE-754 sort-key encoding: convert little-endian IEEE bytes to big-endian,
    /// then if the original sign bit was zero (non-negative) flip the sign bit;
    /// otherwise (negative) ones-complement every byte. Result sorts numerically.
    /// </summary>
    private static byte[] EncodeIeeeBigEndian(byte[] littleEndianIeee)
    {
        // Reverse to big-endian.
        byte[] be = new byte[littleEndianIeee.Length];
        for (int i = 0; i < littleEndianIeee.Length; i++)
        {
            be[i] = littleEndianIeee[littleEndianIeee.Length - 1 - i];
        }

        if ((be[0] & 0x80) == 0)
        {
            // Non-negative: flip the sign bit to push these above the encoded negatives.
            be[0] ^= 0x80;
        }
        else
        {
            // Negative: complement every byte so larger-magnitude negatives sort first.
            for (int i = 0; i < be.Length; i++)
            {
                be[i] = unchecked((byte)~be[i]);
            }
        }

        return be;
    }

    // ── Coercion helpers ────────────────────────────────────────────────
    // Mirror the loose typing AccessWriter accepts on row insert paths.

    private static byte ToByte(object value) => value switch
    {
        byte b => b,
        sbyte sb when sb >= 0 => (byte)sb,
        short s when s is >= 0 and <= 255 => (byte)s,
        int i when i is >= 0 and <= 255 => (byte)i,
        long l when l is >= 0 and <= 255 => (byte)l,
        _ => Convert.ToByte(value, CultureInfo.InvariantCulture),
    };

    private static short ToInt16(object value) => value switch
    {
        short s => s,
        byte b => b,
        sbyte sb => sb,
        int i => checked((short)i),
        long l => checked((short)l),
        _ => Convert.ToInt16(value, CultureInfo.InvariantCulture),
    };

    private static int ToInt32(object value) => value switch
    {
        int i => i,
        short s => s,
        byte b => b,
        sbyte sb => sb,
        uint u => checked((int)u),
        long l => checked((int)l),
        _ => Convert.ToInt32(value, CultureInfo.InvariantCulture),
    };

    private static decimal ToDecimal(object value) => value switch
    {
        decimal d => d,
        double db => (decimal)db,
        float f => (decimal)f,
        int i => i,
        long l => l,
        _ => Convert.ToDecimal(value, CultureInfo.InvariantCulture),
    };

    private static float ToSingle(object value) => value switch
    {
        float f => f,
        double d => (float)d,
        decimal m => (float)m,
        int i => i,
        long l => l,
        _ => Convert.ToSingle(value, CultureInfo.InvariantCulture),
    };

    private static double ToDouble(object value) => value switch
    {
        double d => d,
        float f => f,
        decimal m => (double)m,
        int i => i,
        long l => l,
        _ => Convert.ToDouble(value, CultureInfo.InvariantCulture),
    };

    private static DateTime ToDateTime(object value) => value switch
    {
        DateTime dt => dt,
        DateTimeOffset dto => dto.UtcDateTime,
        _ => Convert.ToDateTime(value, CultureInfo.InvariantCulture),
    };
}
