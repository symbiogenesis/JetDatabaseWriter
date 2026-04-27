namespace JetDatabaseWriter;

using System;
using System.Buffers.Binary;
using System.Globalization;
using System.Numerics;

/// <summary>
/// JET index sort-key encoder for fixed-width numeric and date/time column types
/// (W2 phase). Encodes a single column value into the per-entry byte sequence
/// described in <c>docs/design/index-and-relationship-format-notes.md</c> §4.3
/// (entry flag byte) and §5 (per-type sort-key encoding).
/// <para>
/// Supported column types in W2: <c>T_BYTE (0x02)</c>, <c>T_INT (0x03)</c>,
/// <c>T_LONG (0x04)</c>, <c>T_MONEY (0x05)</c>, <c>T_FLOAT (0x06)</c>,
/// <c>T_DOUBLE (0x07)</c>, <c>T_DATETIME (0x08)</c>. W7 added partial
/// support for <c>T_TEXT (0x0A)</c> using the "General Legacy"
/// encoding documented in HACKING.md (digits and ASCII letters only;
/// any character outside <c>0-9 / A-Z / a-z</c> throws
/// <see cref="NotSupportedException"/>, which the W5 maintenance loop
/// catches to leave the index leaf untouched). W12 adds <c>T_GUID (0x0F)</c>
/// using the Jackcess "general binary entry" wrapping (16-byte big-endian
/// payload packed into 9-byte length-suffixed segments; ascending leaves
/// data bytes intact and intermediate length bytes at <c>0x09</c> with the
/// final length byte at the actual segment length, descending bit-flips
/// every data byte and the FINAL length byte but leaves intermediate length
/// bytes at <c>0x09</c>). W19 extends the same general-binary-entry packing
/// to <c>T_BINARY (0x09)</c> for variable-length binary index keys. MEMO,
/// OLE, complex, and DATETIMEEXT are still deferred (no clean HACKING.md
/// spec for any of them).
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
    private const byte T_BINARY = 0x09;
    private const byte T_TEXT = 0x0A;
    private const byte T_MEMO = 0x0C;
    private const byte T_GUID = 0x0F;

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
            return [ascending ? FlagAscendingNull : FlagDescendingNull];
        }

        // GUID uses the Jackcess "general binary entry" wrapping where
        // intermediate length bytes (0x09) are NOT bit-flipped on descending —
        // only the data bytes and the FINAL length byte are flipped. This
        // differs from the simple "ones-complement the whole entry" rule used
        // by the fixed-width numeric / IEEE / text encodings, so emit it
        // directly here instead of routing through the post-loop bulk flip.
        if (columnType == T_GUID)
        {
            return EncodeGuidEntry(value!, ascending);
        }

        if (columnType == T_BINARY)
        {
            return EncodeBinaryEntry(value!, ascending);
        }

        // Text / Memo route through the dedicated General Legacy encoder which
        // emits a self-contained entry block (flag + inline + END_TEXT framing
        // + extra/unprintable/crazy streams + END_EXTRA_TEXT). The encoder
        // applies its own internal bit-flip for descending — the bulk flip
        // below is intentionally bypassed.
        if (columnType == T_TEXT || columnType == T_MEMO)
        {
            return GeneralLegacyTextIndexEncoder.Encode(ToText(value!), ascending);
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
                return [ToByte(value)];

            case T_INT:
                return EncodeSignedBigEndian(ToInt16(value), 2);

            case T_LONG:
                return EncodeSignedBigEndian(ToInt32(value), 4);

            case T_MONEY:
                {
                    // Currency is stored as int64 = decimal × 10000 (OLE Automation
                    // currency encoding); decimal.ToOACurrency applies the
                    // banker's-rounding policy specified by the OLE Automation
                    // type-conversion rules that VBA's CCur() also uses.
                    long scaled = decimal.ToOACurrency(ToDecimal(value));
                    return EncodeSignedBigEndian(scaled, 8);
                }

            case T_FLOAT:
                {
                    byte[] be = new byte[4];
                    BinaryPrimitives.WriteInt32BigEndian(be, BitConverter.SingleToInt32Bits(ToSingle(value)));
                    return TwiddleIeeeBigEndian(be);
                }

            case T_DOUBLE:
                {
                    byte[] be = new byte[8];
                    BinaryPrimitives.WriteInt64BigEndian(be, BitConverter.DoubleToInt64Bits(ToDouble(value)));
                    return TwiddleIeeeBigEndian(be);
                }

            case T_DATETIME:
                {
                    DateTime dt = ToDateTime(value);
                    byte[] be = new byte[8];
                    BinaryPrimitives.WriteInt64BigEndian(be, BitConverter.DoubleToInt64Bits(dt.ToOADate()));
                    return TwiddleIeeeBigEndian(be);
                }

            case T_BOOL:
                throw new NotSupportedException("BOOL columns are stored in the row null mask, not in index key bytes.");

            default:
                throw new NotSupportedException(
                    $"Index key encoding for column type 0x{columnType:X2} is not supported in this writer phase. " +
                    "Supported types: BYTE, INT, LONG, MONEY, FLOAT, DOUBLE, DATETIME, GUID, BINARY, TEXT, MEMO.");
        }
    }

    /// <summary>
    /// W12 — GUID sort-key encoding via the Jackcess "general binary entry"
    /// wrapping. The 16 raw GUID bytes are taken in <b>display</b> order
    /// (i.e. <c>byte 3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15</c>
    /// of the in-row storage layout) so lexicographic byte comparison matches
    /// the canonical hyphenated string ordering Access uses. The bytes are
    /// then packed into 9-byte segments, each containing 8 data bytes plus a
    /// trailing length byte: <c>0x09</c> for intermediate segments (signalling
    /// "more data follows") and the actual valid-byte count for the final
    /// segment (always <c>0x08</c> for 16-byte GUIDs).
    /// <para>
    /// Ascending: <c>7F | d0..d7 | 09 | d8..d15 | 08</c>.
    /// Descending: <c>80 | ~d0..~d7 | 09 | ~d8..~d15 | F7</c> — note the
    /// intermediate <c>0x09</c> is NOT flipped (per Jackcess
    /// <c>writeGeneralBinaryEntry</c>) but the final length byte is.
    /// </para>
    /// <para>
    /// The format-probe corpus does not contain a GUID-keyed index leaf;
    /// these byte sequences come from Jackcess <c>IndexData.writeGeneralBinaryEntry</c>
    /// and have not been independently verified against an Access-authored
    /// fixture. See <c>docs/design/index-and-relationship-format-notes.md</c> §8.
    /// </para>
    /// </summary>
    private static byte[] EncodeGuidEntry(object value, bool ascending)
    {
        Guid g = value switch
        {
            Guid guid => guid,
            string s => Guid.Parse(s),
            byte[] bytes when bytes.Length == 16 => new Guid(bytes),
            _ => throw new ArgumentException(
                $"Cannot coerce value of type {value.GetType().Name} to System.Guid for index key encoding.",
                nameof(value)),
        };

        // .NET Guid.ToByteArray() matches Jet GUID storage: the first three
        // groups are little-endian, the trailing 8 bytes are raw. Reorder to
        // display (big-endian) order so byte comparisons match canonical
        // string ordering.
        byte[] storage = g.ToByteArray();
        byte[] display =
        [
            storage[3], storage[2], storage[1], storage[0],
            storage[5], storage[4],
            storage[7], storage[6],
            storage[8], storage[9], storage[10], storage[11],
            storage[12], storage[13], storage[14], storage[15],
        ];

        return EncodeGeneralBinaryEntry(display, ascending);
    }

    /// <summary>
    /// W19 — Variable-length <c>T_BINARY (0x09)</c> sort-key encoding via the
    /// Jackcess general-binary-entry packing. Accepts <see cref="byte"/>[]
    /// (the canonical Access binary representation) and falls back to
    /// <see cref="Convert"/> coercion otherwise. Empty arrays are encoded as
    /// a single zero-length segment so two empty values compare equal and
    /// sort below any non-empty value.
    /// <para>
    /// <b>Validation gap:</b> the format-probe corpus does not contain a
    /// BINARY-keyed index leaf; these byte sequences come from Jackcess
    /// <c>IndexData.writeGeneralBinaryEntry</c> and have not been independently
    /// verified against an Access-authored fixture. See
    /// <c>docs/design/index-and-relationship-format-notes.md</c> §8.
    /// </para>
    /// </summary>
    private static byte[] EncodeBinaryEntry(object value, bool ascending)
    {
        byte[] data = value switch
        {
            byte[] b => b,
            ArraySegment<byte> seg => seg.ToArray(),
            ReadOnlyMemory<byte> rom => rom.ToArray(),
            Memory<byte> mem => mem.ToArray(),
            _ => throw new ArgumentException(
                $"Cannot coerce value of type {value.GetType().Name} to byte[] for T_BINARY index key encoding.",
                nameof(value)),
        };

        return EncodeGeneralBinaryEntry(data, ascending);
    }

    /// <summary>
    /// Shared general-binary-entry packer (Jackcess
    /// <c>IndexData.writeGeneralBinaryEntry</c>). Emits the entry-flag byte
    /// followed by ⌈len/8⌉ 9-byte segments (8 zero-padded data bytes + 1
    /// length byte): <c>0x09</c> for intermediate segments, the actual valid
    /// byte count for the final segment. For empty input one final segment
    /// of 8 zero bytes + length <c>0x00</c> is emitted so empty values are
    /// representable and byte-comparable. On descending the data bytes and
    /// the FINAL length byte are ones-complemented; intermediate length
    /// bytes (<c>0x09</c>) stay unflipped.
    /// </summary>
    private static byte[] EncodeGeneralBinaryEntry(ReadOnlySpan<byte> data, bool ascending)
    {
        // Always emit at least one segment so empty input round-trips.
        int segments = data.Length == 0 ? 1 : (data.Length + 7) / 8;
        byte[] result = new byte[1 + (segments * 9)];
        result[0] = ascending ? FlagAscendingNonNull : FlagDescendingNonNull;

        int read = 0;
        for (int s = 0; s < segments; s++)
        {
            int segStart = 1 + (s * 9);
            int chunkLen = Math.Min(data.Length - read, 8);
            if (chunkLen < 0)
            {
                chunkLen = 0;
            }

            for (int i = 0; i < chunkLen; i++)
            {
                byte b = data[read + i];
                result[segStart + i] = ascending ? b : unchecked((byte)~b);
            }

            // Zero-pad remaining bytes of the chunk; on descending the pad
            // bytes flip to 0xFF so the encoded form remains a pure ones-
            // complement of the ascending form (modulo the intermediate
            // length byte handled below).
            for (int i = chunkLen; i < 8; i++)
            {
                result[segStart + i] = ascending ? (byte)0x00 : (byte)0xFF;
            }

            read += chunkLen;
            bool isFinal = s == segments - 1;
            byte lenByte = isFinal ? (byte)chunkLen : (byte)0x09;

            // Intermediate length bytes (0x09) are NOT flipped on descending
            // — only data bytes and the FINAL length byte are.
            result[segStart + 8] = (ascending || !isFinal) ? lenByte : unchecked((byte)~lenByte);
        }

        return result;
    }

    /// <summary>
    /// W13 — Decimal (<c>T_NUMERIC = 0x10</c>) sort-key encoding via the Jackcess
    /// <c>FixedPointColumnDescriptor</c> / <c>LegacyFixedPointColumnDescriptor</c>
    /// layout. Produces the entry-flag byte (0x7F ascending non-null / 0x80
    /// descending non-null / 0x00 / 0xFF for null) followed by 17 bytes:
    /// 1 sign byte + 16-byte big-endian unsigned mantissa.
    /// <para>
    /// All values within a single index rebuild MUST be encoded with the same
    /// <paramref name="targetScale"/> to be byte-comparable. Callers should
    /// scan the snapshot to find the maximum natural scale present and pass it
    /// here. Values whose natural scale is less than <paramref name="targetScale"/>
    /// are multiplied by <c>10^(targetScale - naturalScale)</c> via
    /// <see cref="BigInteger"/> arithmetic; values whose mantissa exceeds the
    /// 16-byte (128-bit unsigned) field after scaling throw
    /// <see cref="NotSupportedException"/>, which the W5 maintenance loop
    /// catches to fall through to the stale-leaf path.
    /// </para>
    /// <para>
    /// Twiddling rules (per Jackcess <c>handleNegationAndOrder</c>) — the
    /// 17-byte payload is constructed with byte 0 = sign byte (0x80 negative,
    /// 0x00 positive) and bytes 1..16 = big-endian mantissa, then mutated:
    /// <list type="bullet">
    /// <item><description><b>Legacy</b> (Jet4 <c>.mdb</c>, V2000–V2003): if
    /// (<c>negative == ascending</c>) flip all 17 bytes; then set byte 0 to
    /// 0x00 (negative) or 0xFF (positive).</description></item>
    /// <item><description><b>New-style</b> (ACCDB, V2007+): set byte 0 to 0xFF;
    /// then if (<c>negative == ascending</c>) flip all 17 bytes.</description></item>
    /// </list>
    /// The flag-byte prefix is added unflipped (0x7F asc / 0x80 desc).
    /// </para>
    /// <para>
    /// <b>Validation gap:</b> the format-probe corpus does not contain a
    /// NUMERIC-keyed index leaf; these byte sequences come directly from
    /// Jackcess and have not been independently verified against an
    /// Access-authored fixture. See
    /// <c>docs/design/index-and-relationship-format-notes.md</c> §8.
    /// </para>
    /// </summary>
    public static byte[] EncodeNumericEntry(object? value, bool ascending, int targetScale, bool legacy)
    {
        bool isNull = value is null || value is DBNull;
        if (isNull)
        {
            return [ascending ? FlagAscendingNull : FlagDescendingNull];
        }

        decimal d = ToDecimal(value!);
        int[] bits = decimal.GetBits(d);
        int flags = bits[3];
        bool negative = (flags & unchecked((int)0x80000000)) != 0;
        int scale = (flags >> 16) & 0x7F;

        if (targetScale < 0 || targetScale > 28)
        {
            throw new ArgumentOutOfRangeException(nameof(targetScale), targetScale, "targetScale must be in [0, 28].");
        }

        if (targetScale < scale)
        {
            throw new ArgumentException(
                $"targetScale ({targetScale}) must be >= the value's natural scale ({scale}).",
                nameof(targetScale));
        }

        // Build BigInteger from the unsigned 96-bit mantissa.
        byte[] leMantissa = new byte[13]; // 12 data bytes + trailing zero ensures positive sign in BigInteger.
        BinaryPrimitives.WriteInt32LittleEndian(leMantissa.AsSpan(0, 4), bits[0]);
        BinaryPrimitives.WriteInt32LittleEndian(leMantissa.AsSpan(4, 4), bits[1]);
        BinaryPrimitives.WriteInt32LittleEndian(leMantissa.AsSpan(8, 4), bits[2]);
        BigInteger mag = new BigInteger(leMantissa);

        if (targetScale > scale)
        {
            mag *= BigInteger.Pow(10, targetScale - scale);
        }

        // Encode mag as big-endian 16-byte unsigned. BigInteger.ToByteArray returns
        // little-endian two's-complement; mag is non-negative here so we just need
        // to drop any trailing zero sign-byte before reversing.
        byte[] magLe = mag.ToByteArray();
        int magLen = magLe.Length;
        while (magLen > 0 && magLe[magLen - 1] == 0)
        {
            magLen--;
        }

        if (magLen > 16)
        {
            throw new NotSupportedException(
                $"Numeric index key mantissa requires {magLen} bytes after rescale to {targetScale} digits, " +
                "which exceeds the 16-byte (128-bit) NUMERIC field. Use a smaller target scale or a smaller value.");
        }

        byte[] valueBytes = new byte[17];
        valueBytes[0] = negative ? (byte)0x80 : (byte)0x00;
        for (int i = 0; i < magLen; i++)
        {
            valueBytes[1 + (16 - 1 - i)] = magLe[i];
        }

        // Apply Jackcess byte-twiddling rules (see XML doc above).
        if (legacy)
        {
            if (negative == ascending)
            {
                FlipBytes(valueBytes);
            }

            valueBytes[0] = negative ? (byte)0x00 : (byte)0xFF;
        }
        else
        {
            valueBytes[0] = 0xFF;
            if (negative == ascending)
            {
                FlipBytes(valueBytes);
            }
        }

        byte[] result = new byte[18];
        result[0] = ascending ? FlagAscendingNonNull : FlagDescendingNonNull;
        Buffer.BlockCopy(valueBytes, 0, result, 1, 17);
        return result;
    }

    /// <summary>
    /// Returns the maximum natural scale (decimal places) across the supplied
    /// values. Null and <see cref="DBNull"/> are skipped. Used by
    /// <see cref="EncodeNumericEntry"/> callers to compute a per-rebuild
    /// canonical <c>targetScale</c>.
    /// </summary>
    public static int ComputeMaxNumericScale(System.Collections.Generic.IEnumerable<object?> values)
    {
        Guard.NotNull(values, nameof(values));
        int max = 0;
        foreach (object? v in values)
        {
            if (v is null || v is DBNull)
            {
                continue;
            }

            decimal d = ToDecimal(v);
            int scale = (decimal.GetBits(d)[3] >> 16) & 0x7F;
            if (scale > max)
            {
                max = scale;
            }
        }

        return max;
    }

    private static void FlipBytes(byte[] arr)
    {
        for (int i = 0; i < arr.Length; i++)
        {
            arr[i] = unchecked((byte)~arr[i]);
        }
    }

    /// <summary>
    /// Big-endian signed-integer encoding with the high bit of the most
    /// significant byte inverted, so two's-complement values sort correctly
    /// as unsigned bytes (negative values precede non-negative values).
    /// </summary>
    private static byte[] EncodeSignedBigEndian(long value, int byteCount)
    {
        Span<byte> tmp = stackalloc byte[8];
        BinaryPrimitives.WriteInt64BigEndian(tmp, value);
        byte[] result = tmp.Slice(8 - byteCount, byteCount).ToArray();
        result[0] ^= 0x80;
        return result;
    }

    /// <summary>
    /// IEEE-754 sort-key twiddle (caller has already written the value in big-endian
    /// IEEE byte order): if the sign bit is zero (non-negative) flip the sign bit;
    /// otherwise (negative) ones-complement every byte. Result sorts numerically.
    /// </summary>
    private static byte[] TwiddleIeeeBigEndian(byte[] be)
    {
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

    private static string ToText(object value) => value switch
    {
        string s => s,
        char c => c.ToString(),
        _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty,
    };

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="columnType"/> is
    /// supported by <see cref="EncodeEntry(byte, object?, bool)"/> in the
    /// fixed-width single-call form used by the parent-seek RI enforcement
    /// path. Excludes <c>T_NUMERIC</c> (requires a per-batch canonical scale
    /// computed from the snapshot) and <c>T_BOOL</c> (BOOL is in the row
    /// null mask, never in the index key bytes).
    /// </summary>
    internal static bool IsColumnTypeSeekable(byte columnType) => columnType switch
    {
        T_BYTE or T_INT or T_LONG or T_MONEY or T_FLOAT or T_DOUBLE
            or T_DATETIME or T_BINARY or T_TEXT or T_MEMO or T_GUID => true,
        _ => false,
    };
}
