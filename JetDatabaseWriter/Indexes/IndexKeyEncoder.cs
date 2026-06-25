namespace JetDatabaseWriter.Indexes;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueEncoding;
using JetDatabaseWriter.ValueEncoding.Models;
using static JetDatabaseWriter.Constants.IndexEntryFlags;
using static JetDatabaseWriter.Enums.ColumnType;

/// <summary>
/// JET index sort-key encoder for fixed-width numeric and date/time column types.
/// Encodes a single column value into the per-entry byte sequence
/// described in <see href="docs/design/index-and-relationship-format-notes.md" /> §4.3
/// (entry flag byte) and §5 (per-type sort-key encoding).
/// <para>
/// Supported column types: <c>Byte (0x02)</c>, <c>Integer (0x03)</c>,
/// <c>LongInteger (0x04)</c>, <c>Money (0x05)</c>, <c>Float (0x06)</c>,
/// <c>Double (0x07)</c>, <c>DateTime (0x08)</c>, and <c>BigInt (0x13)</c>
/// using fixed-width sort keys; <c>Text (0x0A)</c> and <c>Memo (0x0C)</c>
/// using the General Legacy text encoder; and <c>Guid (0x0F)</c>,
/// <c>Binary (0x09)</c>, and <c>DateTimeExtended (0x14)</c> using the
/// Jackcess "general binary entry" wrapping. OLE, Attachment, and Complex
/// columns are intentionally unsupported because Access does not permit
/// indexes on them.
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
/// <b>Validation status:</b> fixed-width encodings follow the conventional
/// B-tree encoding documented in HACKING.md and used by Jackcess. Text/Memo
/// validation lives with <see cref="GeneralLegacyTextIndexEncoder"/>, including
/// long-row fixtures. Microsoft Access Compact &amp; Repair remains the final
/// compatibility oracle for writer-emitted index leaves (see §8 of the design doc).
/// </para>
/// </summary>
internal static class IndexKeyEncoder
{
    // Column type codes are imported via `using static JetDatabaseWriter.ColumnTypes;`.

    /// <summary>
    /// Returns the entry-flag + key-bytes block for a single column value.
    /// For null values the result is a single flag byte; for non-null values
    /// it is the flag byte followed by the encoded key bytes. The caller is
    /// responsible for concatenating per-column blocks (in column-map order)
    /// and appending the trailing 3-byte data page + 1-byte data row record
    /// pointer described in §4.3.
    /// </summary>
    /// <param name="columnType">JET column type code (e.g. <c>LongInteger = 0x04</c>).</param>
    /// <param name="value">Value to encode. <see langword="null"/> and
    /// <see cref="DBNull"/> are both treated as the SQL null marker.</param>
    /// <param name="ascending">Sort direction. <see langword="true"/> yields
    /// the ascending encoding; <see langword="false"/> ones-complements every
    /// byte of the ascending form.</param>
    /// <exception cref="NotSupportedException">The column type is outside the
    /// supported set.</exception>
    /// <exception cref="ArgumentException">The value cannot be coerced to the
    /// .NET representation expected by <paramref name="columnType"/>.</exception>
    public static byte[] EncodeEntry(ColumnType columnType, object? value, bool ascending = true)
    {
        bool isNull = value is null or DBNull;
        if (isNull)
        {
            return [ascending ? AscendingNull : DescendingNull];
        }

        // GUID uses the Jackcess "general binary entry" wrapping where
        // intermediate length bytes (0x09) are NOT bit-flipped on descending —
        // only the data bytes and the FINAL length byte are flipped. This
        // differs from the simple "ones-complement the whole entry" rule used
        // by the fixed-width numeric / IEEE / text encodings, so emit it
        // directly here instead of routing through the post-loop bulk flip.
        if (columnType == GuidType)
        {
            return EncodeGuidEntry(value!, ascending);
        }

        if (columnType == BinaryType)
        {
            return EncodeBinaryEntry(value!, ascending);
        }

        // Date/Time Extended (42-byte fixed) uses the same general-binary-entry
        // packing as GUID/BINARY — the raw in-row bytes are segmented into
        // 9-byte chunks with 0x09 intermediate length markers.
        if (columnType == DateTimeExtendedType)
        {
            return EncodeDateTimeExtEntry(value!, ascending);
        }

        // Text / Memo route through the dedicated General Legacy encoder which
        // emits a self-contained entry block (flag + inline + END_TEXT framing
        // + extra/unprintable/crazy streams + END_EXTRA_TEXT). The encoder
        // applies its own internal bit-flip for descending — the bulk flip
        // below is intentionally bypassed.
        if (columnType is TextType or MemoType)
        {
            return GeneralLegacyTextIndexEncoder.Encode(ToText(value!), ascending);
        }

        byte[] key = EncodeKey(columnType, value!);
        byte[] result = new byte[1 + key.Length];

        // Always emit the ascending flag here; if descending, the loop below
        // ones-complements the entire block (turning 0x7F → 0x80, etc.) per §5.
        result[0] = AscendingNonNull;
        key.AsSpan().CopyTo(result.AsSpan(1));

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

    private static byte[] EncodeKey(ColumnType columnType, object value)
    {
        switch (columnType)
        {
            case ByteType:
                // Access "Byte" is unsigned 0..255 — no sign bit to flip.
                return [ToByte(value)];

            case IntegerType:
            {
                byte[] r = new byte[2];
                BinaryPrimitives.WriteInt16BigEndian(r, ToInt16(value));
                r[0] ^= 0x80;
                return r;
            }

            case LongIntegerType:
            {
                byte[] r = new byte[4];
                BinaryPrimitives.WriteInt32BigEndian(r, ToInt32(value));
                r[0] ^= 0x80;
                return r;
            }

            case BigIntType:
            {
                byte[] r = new byte[8];
                BinaryPrimitives.WriteInt64BigEndian(r, ToInt64(value));
                r[0] ^= 0x80;
                return r;
            }

            case MoneyType:
            {
                long scaled = decimal.ToOACurrency(ToDecimal(value));
                byte[] r = new byte[8];
                BinaryPrimitives.WriteInt64BigEndian(r, scaled);
                r[0] ^= 0x80;
                return r;
            }

            case FloatType:
            {
                byte[] r = new byte[4];
                BinaryPrimitives.WriteInt32BigEndian(r, BitConverter.SingleToInt32Bits(ToSingle(value)));
                TwiddleIeeeBigEndianInPlace(r);
                return r;
            }

            case DoubleType:
            {
                byte[] r = new byte[8];
                BinaryPrimitives.WriteInt64BigEndian(r, BitConverter.DoubleToInt64Bits(ToDouble(value)));
                TwiddleIeeeBigEndianInPlace(r);
                return r;
            }

            case DateTimeType:
            {
                DateTime dt = ToDateTime(value);
                byte[] r = new byte[8];
                BinaryPrimitives.WriteInt64BigEndian(r, BitConverter.DoubleToInt64Bits(dt.ToOADate()));
                TwiddleIeeeBigEndianInPlace(r);
                return r;
            }

            case BooleanType:
                throw new NotSupportedException("BOOL columns are stored in the row null mask, not in index key bytes.");
            case BinaryType:
            case TextType:
            case OleType:
            case MemoType:
            case GuidType:
            case NumericType:
            case AttachmentType:
            case ComplexType:
            case DateTimeExtendedType:
                throw new NotSupportedException(
                    $"Index key encoding for column type {JetTypeInfo.GetTypeDisplayName(columnType)} is not supported. " +
                    "Supported types: BYTE, INT, LONG, BIGINT, MONEY, FLOAT, DOUBLE, DATETIME, DATETIMEEXT, GUID, BINARY, TEXT, MEMO.");
            default:
                throw new InvalidOperationException($"Index key encoding for column type {JetTypeInfo.GetTypeDisplayName(columnType)} is unknown.");
        }
    }

    /// <summary>
    /// GUID sort-key encoding via the Jackcess "general binary entry"
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
    /// fixture. See <see href="docs/design/index-and-relationship-format-notes.md" /> §8.
    /// </para>
    /// </summary>
    /// <param name="value">The value.</param>
    /// <param name="ascending">The ascending.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="value"/> cannot be coerced to a <see cref="GuidType"/>.</exception>
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
    /// Variable-length <c>Binary (0x09)</c> sort-key encoding via the
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
    /// <see href="docs/design/index-and-relationship-format-notes.md" /> §8.
    /// </para>
    /// </summary>
    /// <param name="value">The value.</param>
    /// <param name="ascending">The ascending.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="value"/> cannot be coerced to a byte array.</exception>
    private static byte[] EncodeBinaryEntry(object value, bool ascending)
    {
        byte[] data = value switch
        {
            byte[] b => b,
            ArraySegment<byte> seg => seg.ToArray(),
            ReadOnlyMemory<byte> rom => rom.ToArray(),
            Memory<byte> mem => mem.ToArray(),
            _ => throw new ArgumentException(
                $"Cannot coerce value of type {value.GetType().Name} to byte[] for Binary index key encoding.",
                nameof(value)),
        };

        return EncodeGeneralBinaryEntry(data, ascending);
    }

    /// <summary>
    /// Date/Time Extended (<c>DateTimeExtended = 0x14</c>) sort-key encoding.
    /// The 42-byte fixed in-row representation is packed via the same
    /// general-binary-entry segmenting used for GUID and BINARY: 6 segments
    /// of 9 bytes each (8 data + 1 length), yielding a 55-byte result
    /// (1 flag + 54 segment bytes). The final segment carries 2 valid bytes
    /// (42 mod 8 = 2) with a length trailer of <c>0x02</c>.
    /// </summary>
    /// <param name="value">The value.</param>
    /// <param name="ascending">The ascending.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="value"/> cannot be encoded as DateTime Extended.</exception>
    private static byte[] EncodeDateTimeExtEntry(object value, bool ascending)
    {
        byte[] data = value switch
        {
            byte[] b when b.Length == 42 => b,
            byte[] b => throw new ArgumentException(
                $"DateTimeExtended expects exactly 42 bytes but received {b.Length}.",
                nameof(value)),
            _ => EncodeDateTimeExtendedPayload(ToDateTime(value)),
        };

        return EncodeGeneralBinaryEntry(data, ascending);
    }

    private static byte[] EncodeDateTimeExtendedPayload(DateTime value)
    {
        byte[] data = new byte[42];
        JetTypeInfo.WriteDateTimeExtended(data, value);
        return data;
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
    /// <param name="data">The data bytes or values.</param>
    /// <param name="ascending">The ascending.</param>
    private static byte[] EncodeGeneralBinaryEntry(ReadOnlySpan<byte> data, bool ascending)
    {
        // Always emit at least one segment so empty input round-trips.
        int segments = data.Length == 0 ? 1 : (data.Length + 7) / 8;
        byte[] result = new byte[1 + (segments * 9)];
        result[0] = ascending ? AscendingNonNull : DescendingNonNull;

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
    /// Decimal (<c>Numeric = 0x10</c>) sort-key encoding via the Jackcess
    /// <c>FixedPointColumnDescriptor</c> / <c>LegacyFixedPointColumnDescriptor</c>
    /// layout. Produces the entry-flag byte (0x7F ascending non-null / 0x80
    /// descending non-null / 0x00 / 0xFF for null) followed by 17 bytes:
    /// 1 sign byte + 16-byte big-endian unsigned mantissa.
    /// <para>
    /// All values within a single index rebuild MUST be encoded with the same
    /// <paramref name="targetScale"/> to be byte-comparable. Callers should
    /// scan the snapshot to find the maximum natural scale present and pass it
    /// here. Values whose natural scale is less than <paramref name="targetScale"/>
    /// are multiplied by <c>10^(targetScale - naturalScale)</c>; values whose mantissa exceeds the
    /// 16-byte (128-bit unsigned) field after scaling throw
    /// <see cref="NotSupportedException"/>, which lets index maintenance use
    /// the conservative rebuild or snapshot fallback path.
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
    /// <see href="docs/design/index-and-relationship-format-notes.md" /> §8.
    /// </para>
    /// </summary>
    /// <param name="value">The value.</param>
    /// <param name="ascending">The ascending.</param>
    /// <param name="targetScale">The target scale.</param>
    /// <param name="legacy">The legacy.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="targetScale"/> is below the value's natural scale.</exception>
    /// <exception cref="NotSupportedException">Thrown when the rescaled mantissa exceeds the 16-byte NUMERIC field.</exception>
    public static byte[] EncodeNumericEntry(object? value, bool ascending, int targetScale, bool legacy)
    {
        bool isNull = value is null or DBNull;
        if (isNull)
        {
            return [ascending ? AscendingNull : DescendingNull];
        }

        decimal d = ToDecimal(value!);

        Guard.InRange(targetScale, 0, 28, nameof(targetScale));

        Span<byte> magnitudeBe = stackalloc byte[16];
        if (!NumericEncoder.TryEncodeFixedPointPayload(d, targetScale, magnitudeBe, out FixedPointPayload payload))
        {
            throw new NotSupportedException(
                $"Numeric index key mantissa requires {payload.MagnitudeByteCount} bytes after rescale to {targetScale} digits, " +
                "which exceeds the 16-byte (128-bit) NUMERIC field. Use a smaller target scale or a smaller value.");
        }

        byte[] valueBytes = new byte[17];
        valueBytes[0] = payload.Negative ? (byte)0x80 : (byte)0x00;
        magnitudeBe.CopyTo(valueBytes.AsSpan(1, 16));

        // Apply Jackcess byte-twiddling rules (see XML doc above).
        if (legacy)
        {
            if (payload.Negative == ascending)
            {
                FlipBytes(valueBytes);
            }

            valueBytes[0] = payload.Negative ? (byte)0x00 : (byte)0xFF;
        }
        else
        {
            valueBytes[0] = 0xFF;
            if (payload.Negative == ascending)
            {
                FlipBytes(valueBytes);
            }
        }

        byte[] result = new byte[18];
        result[0] = ascending ? AscendingNonNull : DescendingNonNull;
        Buffer.BlockCopy(valueBytes, 0, result, 1, 17);
        return result;
    }

    /// <summary>
    /// Decimal sort-key wrapper that mirrors Microsoft Access
    /// semantics for canonical scale: a <c>Numeric</c> column has a single
    /// declared scale that governs every cell, so the index encoder rescales
    /// each value to <paramref name="declaredScale"/> via
    /// <see cref="MidpointRounding.ToEven"/> rounding before delegating to
    /// <see cref="EncodeNumericEntry"/>. Removes the need for the per-rebuild
    /// snapshot pre-pass that previously computed
    /// <c>max(naturalScale)</c> across the table — the incremental fast paths
    /// now participate on numeric keys with no extra
    /// I/O. Null / <see cref="DBNull"/> emit the standard null flag byte.
    /// </summary>
    /// <param name="value">The value.</param>
    /// <param name="ascending">The ascending.</param>
    /// <param name="declaredScale">The declared scale.</param>
    /// <param name="legacy">The legacy.</param>
    public static byte[] EncodeNumericEntryAtDeclaredScale(object? value, bool ascending, byte declaredScale, bool legacy)
    {
        Guard.InRange(declaredScale, 0, 28, nameof(declaredScale));

        if (value is null or DBNull)
        {
            return EncodeNumericEntry(null, ascending, declaredScale, legacy);
        }

        // Mirror Access: cells are canonical at the column's declared scale.
        decimal d = decimal.Round(ToDecimal(value), declaredScale, MidpointRounding.ToEven);

        return EncodeNumericEntry(d, ascending, declaredScale, legacy);
    }

    /// <summary>
    /// Encodes a full composite index seek key from <paramref name="keyValues"/>,
    /// requiring exactly one value per index key column. Each value is encoded
    /// in column-map order via <see cref="EncodeEntry"/> (or
    /// <see cref="EncodeNumericEntryAtDeclaredScale"/> for <c>Numeric</c>
    /// columns) and the per-column blocks are concatenated.
    /// </summary>
    /// <param name="format">Database format; selects the legacy Jet4 vs. ACE numeric encoding.</param>
    /// <param name="tableName">Owning table name, used only in exception messages.</param>
    /// <param name="index">Index whose key columns drive the encoding order.</param>
    /// <param name="tableDef">Table definition supplying per-column type / scale metadata.</param>
    /// <param name="keyValues">Exactly one value per index key column.</param>
    /// <exception cref="ArgumentException">The value count does not equal the index key-column count.</exception>
    public static byte[] EncodeIndexSeekKey(DatabaseFormat format, string tableName, IndexMetadata index, TableDef tableDef, IReadOnlyList<object?> keyValues) =>
        EncodeIndexKey(format, tableName, index, tableDef, keyValues, requireFullKey: true, nameof(keyValues));

    /// <summary>
    /// Encodes a leading-column index key prefix from <paramref name="keyValues"/>,
    /// accepting between one and the index's key-column count values for range /
    /// prefix seeks. Encoding rules match <see cref="EncodeIndexSeekKey"/>.
    /// </summary>
    /// <param name="format">Database format; selects the legacy Jet4 vs. ACE numeric encoding.</param>
    /// <param name="tableName">Owning table name, used only in exception messages.</param>
    /// <param name="index">Index whose key columns drive the encoding order.</param>
    /// <param name="tableDef">Table definition supplying per-column type / scale metadata.</param>
    /// <param name="keyValues">One to N leading key-column values.</param>
    /// <param name="paramName">Originating caller parameter name, surfaced in argument-validation exceptions.</param>
    /// <exception cref="ArgumentException">The value count is zero or exceeds the index key-column count.</exception>
    public static byte[] EncodeIndexKeyPrefix(DatabaseFormat format, string tableName, IndexMetadata index, TableDef tableDef, IReadOnlyList<object?> keyValues, string paramName) =>
        EncodeIndexKey(format, tableName, index, tableDef, keyValues, requireFullKey: false, paramName);

    private static byte[] EncodeIndexKey(
        DatabaseFormat format,
        string tableName,
        IndexMetadata index,
        TableDef tableDef,
        IReadOnlyList<object?> keyValues,
        bool requireFullKey,
        string paramName)
    {
        Guard.NotNull(keyValues, paramName);

        if (requireFullKey && keyValues.Count != index.Columns.Count)
        {
            throw new ArgumentException(
                $"Index '{index.Name}' on table '{tableName}' expects {index.Columns.Count} key value(s), but {keyValues.Count} were supplied.",
                paramName);
        }

        if (!requireFullKey && (keyValues.Count == 0 || keyValues.Count > index.Columns.Count))
        {
            throw new ArgumentException(
                $"Index '{index.Name}' on table '{tableName}' expects between 1 and {index.Columns.Count} leading key value(s), but {keyValues.Count} were supplied.",
                paramName);
        }

        bool legacyNumeric = format == DatabaseFormat.Jet4Mdb;
        byte[][] perColumn = new byte[keyValues.Count][];
        int totalLength = 0;

        for (int i = 0; i < keyValues.Count; i++)
        {
            IndexColumnReference keyColumn = index.Columns[i];

            ColumnInfo? column = tableDef.Columns.Find(c => c.ColNum == keyColumn.ColumnNumber)
                ?? throw new InvalidDataException($"Index '{index.Name}' on table '{tableName}' references missing column number {keyColumn.ColumnNumber}.");

            object? value = keyValues[i];
            perColumn[i] = column.Type == NumericType
                ? EncodeNumericEntryAtDeclaredScale(value, keyColumn.IsAscending, column.NumericScale, legacyNumeric)
                : EncodeEntry(column.Type, value, keyColumn.IsAscending);
            totalLength += perColumn[i].Length;
        }

        byte[] composite = new byte[totalLength];
        int offset = 0;
        for (int i = 0; i < perColumn.Length; i++)
        {
            Buffer.BlockCopy(perColumn[i], 0, composite, offset, perColumn[i].Length);
            offset += perColumn[i].Length;
        }

        return composite;
    }

    /// <summary>
    /// Returns the maximum natural scale (decimal places) across the supplied
    /// values. Null and <see cref="DBNull"/> are skipped. Used by
    /// <see cref="EncodeNumericEntry"/> callers to compute a per-rebuild
    /// canonical <c>targetScale</c>.
    /// </summary>
    /// <param name="values">The values.</param>
    public static int ComputeMaxNumericScale(System.Collections.Generic.IEnumerable<object?> values)
    {
        Guard.NotNull(values, nameof(values));
        int max = 0;
        foreach (object? v in values)
        {
            if (v is null or DBNull)
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
    /// IEEE-754 sort-key twiddle (in-place): if the sign bit is zero (non-negative)
    /// flip the sign bit; otherwise (negative) ones-complement every byte.
    /// </summary>
    /// <param name="be">The big-endian byte sequence.</param>
    private static void TwiddleIeeeBigEndianInPlace(byte[] be)
    {
        if ((be[0] & 0x80) == 0)
        {
            be[0] ^= 0x80;
        }
        else
        {
            for (int i = 0; i < be.Length; i++)
            {
                be[i] = unchecked((byte)~be[i]);
            }
        }
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

    private static long ToInt64(object value) => value switch
    {
        long l => l,
        int i => i,
        short s => s,
        byte b => b,
        sbyte sb => sb,
        uint u => u,
        _ => Convert.ToInt64(value, CultureInfo.InvariantCulture),
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
    /// supported by the relationship-seek RI enforcement path once the
    /// resolver has captured any descriptor metadata the encoder needs.
    /// <c>Numeric</c> is seekable through the descriptor-scale wrapper;
    /// <c>Boolean</c> remains excluded because BOOL is stored in the row null
    /// mask, never in index key bytes.
    /// </summary>
    /// <param name="columnType">The column type.</param>
    internal static bool IsColumnTypeSeekable(ColumnType columnType) => columnType switch
    {
        ByteType or IntegerType or LongIntegerType or BigIntType or MoneyType or FloatType or DoubleType
            or DateTimeType or DateTimeExtendedType or BinaryType or TextType or MemoType or GuidType or NumericType => true,
        BooleanType or OleType or AttachmentType or ComplexType or _ => false,
    };
}
