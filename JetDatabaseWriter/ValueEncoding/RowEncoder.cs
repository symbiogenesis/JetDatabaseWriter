namespace JetDatabaseWriter.ValueEncoding;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueEncoding.Models;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Encodes in-memory value arrays into on-disk row byte layouts for a JET
/// data page.  Extracted from <see cref="AccessWriter"/>.
/// </summary>
/// <param name="db">The database page I/O and format context.</param>
internal sealed class RowEncoder(DatabaseFile db)
{
    internal static byte[]? EncodeOleValue(object value)
    {
        if (value is PreEncodedLongValue pre)
        {
            return pre.HeaderBytes;
        }

        byte[]? data = value as byte[];
        if (data == null)
        {
            string? stringValue = value as string;
            if (string.IsNullOrEmpty(stringValue))
            {
                return null;
            }

            data = Encoding.UTF8.GetBytes(stringValue);
        }

        if (data.Length > Constants.LongValue.MaxInlineOleBytes)
        {
            throw new JetLimitationException($"OLE value is {data.Length} bytes, which exceeds the inline limit of {Constants.LongValue.MaxInlineOleBytes} bytes.");
        }

        return LongValueStore.WrapInlineLongValue(data);
    }

    /// <summary>
    /// Encodes <paramref name="value"/> as the fixed-size payload of
    /// <paramref name="type"/>, which is the column's descriptor type for a stored
    /// column and its result type for a calculated one.
    /// </summary>
    /// <param name="type">The type to encode the value as.</param>
    /// <param name="column">The column, for its name and numeric precision and scale.</param>
    /// <param name="value">The value.</param>
    /// <param name="dest">The destination buffer.</param>
    /// <returns>The number of bytes written, or 0 when the type has no fixed payload.</returns>
    /// <exception cref="FormatException">A GUID column value is not a GUID.</exception>
    /// <exception cref="InvalidOperationException"><paramref name="type"/> is not a known column type.</exception>
    private static int TryEncodeFixedValue(ColumnType type, ColumnInfo column, object value, Span<byte> dest)
    {
        switch (type)
        {
            case ByteType:
                dest[0] = Convert.ToByte(value, CultureInfo.InvariantCulture);
                return 1;

            case IntegerType:
                BinaryPrimitives.WriteInt16LittleEndian(dest, Convert.ToInt16(value, CultureInfo.InvariantCulture));
                return 2;

            case LongIntegerType:
                BinaryPrimitives.WriteInt32LittleEndian(dest, Convert.ToInt32(value, CultureInfo.InvariantCulture));
                return 4;

            case FloatType:
                BinaryPrimitives.WriteInt32LittleEndian(
                    dest,
                    BitConverter.SingleToInt32Bits(Convert.ToSingle(value, CultureInfo.InvariantCulture)));
                return 4;

            case DoubleType:
                BinaryPrimitives.WriteInt64LittleEndian(
                    dest,
                    BitConverter.DoubleToInt64Bits(Convert.ToDouble(value, CultureInfo.InvariantCulture)));
                return 8;

            case DateTimeType:
                BinaryPrimitives.WriteInt64LittleEndian(
                    dest,
                    BitConverter.DoubleToInt64Bits(Convert.ToDateTime(value, CultureInfo.InvariantCulture).ToOADate()));
                return 8;

            case MoneyType:
                BinaryPrimitives.WriteInt64LittleEndian(
                    dest,
                    decimal.ToOACurrency(Convert.ToDecimal(value, CultureInfo.InvariantCulture)));
                return 8;

            case BigIntType:
                BinaryPrimitives.WriteInt64LittleEndian(dest, Convert.ToInt64(value, CultureInfo.InvariantCulture));
                return 8;

            case NumericType:
                EncodeNumericValue(column, Convert.ToDecimal(value, CultureInfo.InvariantCulture), dest);
                return 17;

            case DateTimeExtendedType:
                return EncodeDateTimeExtendedValue(column, value, dest);

            case ComplexType:
            case AttachmentType:
                int complexId = value is ComplexIdRef complexRef
                    ? complexRef.Id
                    : Convert.ToInt32(value, CultureInfo.InvariantCulture);
                BinaryPrimitives.WriteInt32LittleEndian(dest, complexId);
                return 4;

            case GuidType:
                Guid g = value is Guid guid
                        ? guid
                        : Guid.Parse(Convert.ToString(value, CultureInfo.InvariantCulture)
                            ?? throw new FormatException($"Column '{column.Name}' value cannot be converted to a GUID."));
                if (!g.TryWriteBytes(dest))
                {
                    return 0;
                }

                return 16;
            case BooleanType:
            case BinaryType:
            case TextType:
            case OleType:
            case MemoType:
                return 0;
            default:
                throw new InvalidOperationException($"Unknown column type: {JetTypeInfo.GetTypeDisplayName(type)}");
        }
    }

    private static int EncodeDateTimeExtendedValue(ColumnInfo column, object value, Span<byte> dest)
    {
        int required = JetTypeInfo.GetFixedSize(DateTimeExtendedType);
        if (value is byte[] payload)
        {
            if (payload.Length != required)
            {
                throw new ArgumentException(
                    $"Column '{column.Name}' Date/Time Extended payload must be exactly {required} bytes but received {payload.Length}.");
            }

            payload.CopyTo(dest);
            return required;
        }

        var dateTime = Convert.ToDateTime(value, CultureInfo.InvariantCulture);
        JetTypeInfo.WriteDateTimeExtended(dest, dateTime);
        return required;
    }

    private static void EncodeNumericValue(ColumnInfo column, decimal value, Span<byte> dest)
    {
        byte precision = column.NumericPrecision == 0 ? (byte)18 : column.NumericPrecision;
        byte declaredScale = column.NumericScale;
        decimal rounded = decimal.Round(value, declaredScale, MidpointRounding.ToEven);

        Span<byte> magnitudeBe = stackalloc byte[16];
        bool fits = NumericEncoder.TryEncodeFixedPointPayload(rounded, declaredScale, magnitudeBe, out FixedPointPayload payload);
        if (payload.DigitCount > precision)
        {
            throw new JetLimitationException(
                $"Numeric value '{value}' exceeds NUMERIC({precision},{declaredScale}) precision after rounding.");
        }

        if (!fits)
        {
            throw new JetLimitationException(
                $"Numeric value '{value}' requires {payload.MagnitudeByteCount} bytes, exceeding the 16-byte NUMERIC mantissa.");
        }

        dest[0] = payload.Negative ? (byte)0x80 : (byte)0x00;
        JetTypeInfo.FixNumericByteOrder(magnitudeBe);
        magnitudeBe.CopyTo(dest.Slice(1, 16));
    }

    private static byte[]? EncodeCalculatedFixedPayload(ColumnInfo column, ColumnType valueType, object value)
    {
        if (valueType == BooleanType)
        {
            return [Convert.ToBoolean(value, CultureInfo.InvariantCulture) ? (byte)0xFF : (byte)0x00];
        }

        if (valueType == NumericType)
        {
            return EncodeCalculatedNumericValue(Convert.ToDecimal(value, CultureInfo.InvariantCulture));
        }

        if (!JetTypeInfo.TryGetVariableSlotFixedPayloadSize(valueType, out int fixedSize))
        {
            return null;
        }

        byte[] payload = new byte[fixedSize];
        int written = TryEncodeFixedValue(valueType, column, value, payload);
        if (written <= 0)
        {
            return null;
        }

        if (written != payload.Length)
        {
            Array.Resize(ref payload, written);
        }

        return payload;
    }

    private static byte[]? EncodeCalculatedOleValue(object value)
    {
        if (value is PreEncodedLongValue preOle)
        {
            return preOle.HeaderBytes;
        }

        byte[]? data = value as byte[];
        if (data == null)
        {
            string? stringValue = value as string;
            if (string.IsNullOrEmpty(stringValue))
            {
                return null;
            }

            data = Encoding.UTF8.GetBytes(stringValue);
        }

        byte[] wrapped = CalculatedColumnUtil.Wrap(data);
        if (wrapped.Length > Constants.LongValue.MaxInlineOleBytes)
        {
            throw new JetLimitationException($"Calculated OLE value is {wrapped.Length} bytes after wrapping, which exceeds the inline limit of {Constants.LongValue.MaxInlineOleBytes} bytes.");
        }

        return LongValueStore.WrapInlineLongValue(wrapped);
    }

    private static byte[] EncodeCalculatedNumericValue(decimal value)
    {
        byte[] payload = new byte[16];
        BinaryPrimitives.WriteInt16LittleEndian(payload.AsSpan(0, 2), 14);

        Span<byte> mantissa = stackalloc byte[12];
        NumericEncoder.Decompose(value, mantissa, out bool negative, out int scale);
        payload[2] = (byte)scale;
        payload[3] = negative ? (byte)0x80 : (byte)0x00;

        mantissa.Slice(8, 4).CopyTo(payload.AsSpan(4, 4));
        mantissa[..4].CopyTo(payload.AsSpan(8, 4));
        mantissa.Slice(4, 4).CopyTo(payload.AsSpan(12, 4));
        return payload;
    }

    /// <summary>
    /// Returns the payload size limit of a calculated Text or Binary value: the
    /// descriptor size less the 23-byte wrapper. A descriptor too small to hold
    /// the wrapper (Access writes size 0 on a Text descriptor whose result type
    /// is something else) gives a Text result Access's 255-character limit.
    /// </summary>
    /// <param name="column">The calculated column.</param>
    /// <param name="valueType">The column's result type.</param>
    private static int GetCalculatedVariableSize(ColumnInfo column, ColumnType valueType)
    {
        if (column.Size > Constants.CalculatedColumn.ExtraDataLen)
        {
            return column.Size - Constants.CalculatedColumn.ExtraDataLen;
        }

        return valueType == TextType ? Constants.CalculatedColumn.MaxTextResultBytes : column.Size;
    }

    /// <summary>
    /// Serializes a typed value array into the binary row format understood
    /// by the JET engine (null mask, fixed area, variable-length trailers).
    /// A Jet3 row stores its EOD and variable-column offsets in one byte each
    /// and, past 256 bytes, carries a jump table for their high parts
    /// (<see cref="Jet3JumpTable"/>). Tables without variable columns still
    /// get the EOD, jump table and <c>var_len</c> trailer, which readers skip.
    /// </summary>
    /// <param name="tableDef">The table def.</param>
    /// <param name="values">The values.</param>
    /// <exception cref="JetLimitationException">
    /// A Jet3 row would have more than 255 columns or variable columns, or
    /// 255 variable columns with an EOD the jump table cannot encode.
    /// </exception>
    internal byte[] SerializeRow(TableDef tableDef, object[] values)
    {
        int numCols = 0;
        int maxFixedEnd = 0;
        int maxDefinedVarIdx = -1;
        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            ColumnInfo col = tableDef.Columns[i];
            numCols = Math.Max(numCols, col.ColNum + 1);
            if (col.IsFixed && col.Type != BooleanType)
            {
                maxFixedEnd = Math.Max(maxFixedEnd, col.FixedOff + JetTypeInfo.GetFixedSize(col.Type));
            }
            else if (!col.IsFixed)
            {
                maxDefinedVarIdx = Math.Max(maxDefinedVarIdx, col.VarIdx);
            }
        }

        int nullMaskLen = JetTypeInfo.GetNullMaskSizeBytes(numCols);
        int varLen = maxDefinedVarIdx + 1;
        bool jet3 = db.Format == DatabaseFormat.Jet3Mdb;
        if (jet3 && (numCols > Constants.TableDefinition.MaxJet3Columns || varLen > Constants.TableDefinition.MaxJet3Columns))
        {
            throw new JetLimitationException(
                $"The row has {numCols} columns, {varLen} of them variable-length; a Jet3 (Access 97) row holds at most {Constants.TableDefinition.MaxJet3Columns}.");
        }

        // Use ArrayPool for the fixed-area workspace to avoid per-row heap allocation.
        byte[] fixedArea = maxFixedEnd > 0 ? ArrayPool<byte>.Shared.Rent(maxFixedEnd) : [];
        if (maxFixedEnd > 0)
        {
            fixedArea.AsSpan(0, maxFixedEnd).Clear();
        }

        // Stack-allocate nullMask for typical table widths (up to 256 columns → 32 bytes).
        Span<byte> nullMask = nullMaskLen <= 32 ? stackalloc byte[nullMaskLen] : new byte[nullMaskLen];
        nullMask.Clear();

        int fixedAreaSize = 0;
        byte[][] varEntries = varLen > 0 ? new byte[varLen][] : [];
        int varPayloadSize = 0;

        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            ColumnInfo column = tableDef.Columns[i];
            object value = values[i] ?? DBNull.Value;

            if (column.Type == BooleanType && !column.IsCalculated)
            {
                if (value is not DBNull && Convert.ToBoolean(value, CultureInfo.InvariantCulture))
                {
                    JetTypeInfo.SetNullMaskBit(nullMask, column.ColNum, true);
                }

                continue;
            }

            if (value is DBNull)
            {
                if (column.IsFixed && (column.Type == AttachmentType || column.Type == ComplexType))
                {
                    fixedAreaSize = Math.Max(fixedAreaSize, column.FixedOff + JetTypeInfo.GetFixedSize(column.Type));
                }

                continue;
            }

            if (column.IsFixed)
            {
                if (!this.CanStoreFixedColumn(column))
                {
                    continue;
                }

                int fixedSize = JetTypeInfo.GetFixedSize(column.Type);
                if (fixedSize <= 0)
                {
                    continue;
                }

                int written = TryEncodeFixedValue(column.Type, column, value, fixedArea.AsSpan(column.FixedOff, fixedSize));
                if (written == 0)
                {
                    continue;
                }

                fixedAreaSize = Math.Max(fixedAreaSize, column.FixedOff + written);
                JetTypeInfo.SetNullMaskBit(nullMask, column.ColNum, true);
            }
            else
            {
                byte[]? variableValue = this.EncodeVariableValue(column, value);
                if (variableValue == null)
                {
                    continue;
                }

                varEntries[column.VarIdx] = variableValue;
                varPayloadSize += variableValue.Length;
                JetTypeInfo.SetNullMaskBit(nullMask, column.ColNum, true);
            }
        }

        int baseRowLength = db.RowFields.NumCols + fixedAreaSize + varPayloadSize + db.RowFields.Eod + (varLen * db.RowFields.VarEntry) + db.RowFields.VarLen + nullMaskLen;
        int jumpSize = jet3 ? Jet3JumpTable.CountForLength(baseRowLength) : 0;
        int rowLength = baseRowLength + jumpSize;

        byte[] row = new byte[rowLength];
        int pos = 0;

        WriteField(row, pos, db.RowFields.NumCols, numCols);
        pos += db.RowFields.NumCols;

        if (fixedAreaSize > 0)
        {
            Buffer.BlockCopy(fixedArea, 0, row, pos, fixedAreaSize);
            pos += fixedAreaSize;
        }

        // Return the pooled buffer now that we've copied its contents.
        if (maxFixedEnd > 0)
        {
            ArrayPool<byte>.Shared.Return(fixedArea);
        }

        int currentOffset = db.RowFields.NumCols + fixedAreaSize;

        // Stack-allocate variable offsets for typical tables (up to 128 var columns).
        Span<int> variableOffsets = varLen <= 128 ? stackalloc int[varLen] : new int[varLen];
        for (int varIndex = 0; varIndex < varLen; varIndex++)
        {
            variableOffsets[varIndex] = currentOffset;
            byte[]? payload = varEntries[varIndex];
            if (payload != null)
            {
                Buffer.BlockCopy(payload, 0, row, pos, payload.Length);
                pos += payload.Length;
                currentOffset += payload.Length;
            }
        }

        // Jet3 keeps only the low byte of the EOD and each offset; the jump
        // table below carries their high parts.
        WriteField(row, pos, db.RowFields.Eod, jet3 ? currentOffset & 0xFF : currentOffset);
        pos += db.RowFields.Eod;

        for (int varIndex = varLen - 1; varIndex >= 0; varIndex--)
        {
            WriteField(row, pos, db.RowFields.VarEntry, jet3 ? variableOffsets[varIndex] & 0xFF : variableOffsets[varIndex]);
            pos += db.RowFields.VarEntry;
        }

        if (jumpSize > 0)
        {
            Jet3JumpTable.Write(row.AsSpan(pos, jumpSize), variableOffsets, currentOffset);
            pos += jumpSize;
        }

        WriteField(row, pos, db.RowFields.VarLen, varLen);
        pos += db.RowFields.VarLen;
        nullMask.CopyTo(row.AsSpan(pos));

        return row;
    }

    private bool CanStoreFixedColumn(ColumnInfo column)
    {
        int size = JetTypeInfo.GetFixedSize(column.Type);
        return size >= 0 && column.FixedOff >= 0 && column.FixedOff + size < db.PageSizeBytes;
    }

    private byte[]? EncodeVariableValue(ColumnInfo column, object value)
    {
        if (column.IsCalculated)
        {
            return this.EncodeCalculatedValue(column, value);
        }

        if (JetTypeInfo.TryGetVariableSlotFixedPayloadSize(column.Type, out int fixedSize))
        {
            byte[] payload = new byte[fixedSize];
            int written = TryEncodeFixedValue(column.Type, column, value, payload);
            if (written <= 0)
            {
                return null;
            }

            if (written != payload.Length)
            {
                Array.Resize(ref payload, written);
            }

            return payload;
        }

        if (column.Type == TextType)
        {
            return this.EncodeTextValue(Convert.ToString(value, CultureInfo.InvariantCulture), column.Size, column.IsCompressedUnicode);
        }

        if (column.Type == BinaryType)
        {
            return this.EncodeBinaryValue(value, column.Size);
        }

        if (column.Type == MemoType)
        {
            return value is PreEncodedLongValue preMemo
                ? preMemo.HeaderBytes
                : this.EncodeMemoValue(Convert.ToString(value, CultureInfo.InvariantCulture), column.IsCompressedUnicode);
        }

        if (column.Type == OleType)
        {
            return EncodeOleValue(value);
        }

        if (column.Type == BooleanType)
        {
            return null;
        }

        throw new InvalidOperationException($"Unknown column type: {JetTypeInfo.GetTypeDisplayName(column.Type)}");
    }

    private byte[]? EncodeCalculatedValue(ColumnInfo column, object value)
    {
        // Access encodes the cached value by the column's ResultType, which can
        // differ from the descriptor type: a Memo result in a Text descriptor
        // still stores a long-value header in the row. The row slot stays where
        // the descriptor puts it.
        ColumnType valueType = JetTypeInfo.ResolveValueType(column);
        if (valueType == TextType)
        {
            return CalculatedColumnUtil.Wrap(
                this.EncodeTextValue(Convert.ToString(value, CultureInfo.InvariantCulture), GetCalculatedVariableSize(column, valueType), compress: false) ?? []);
        }

        if (valueType == BinaryType)
        {
            return CalculatedColumnUtil.Wrap(this.EncodeBinaryValue(value, GetCalculatedVariableSize(column, valueType)) ?? []);
        }

        if (valueType == MemoType)
        {
            return this.EncodeCalculatedMemoValue(value);
        }

        if (valueType == OleType)
        {
            return EncodeCalculatedOleValue(value);
        }

        if (valueType == BooleanType || valueType == NumericType || JetTypeInfo.TryGetVariableSlotFixedPayloadSize(valueType, out _))
        {
            byte[]? payload = EncodeCalculatedFixedPayload(column, valueType, value);
            return payload is null ? null : CalculatedColumnUtil.Wrap(payload);
        }

        throw new InvalidOperationException($"Unsupported column type: {JetTypeInfo.GetTypeDisplayName(valueType)}");
    }

    private byte[]? EncodeCalculatedMemoValue(object value)
    {
        if (value is PreEncodedLongValue preMemo)
        {
            return preMemo.HeaderBytes;
        }

        string? text = Convert.ToString(value, CultureInfo.InvariantCulture);
        if (text == null)
        {
            return null;
        }

        byte[] data = db.EncodeTextForFormat(text, compress: false);
        byte[] wrapped = CalculatedColumnUtil.Wrap(data);
        if (wrapped.Length > Constants.LongValue.MaxInlineMemoBytes)
        {
            throw new JetLimitationException($"Calculated MEMO value is {wrapped.Length} bytes after wrapping, which exceeds the inline limit of {Constants.LongValue.MaxInlineMemoBytes} bytes.");
        }

        return LongValueStore.WrapInlineLongValue(wrapped);
    }

    private byte[]? EncodeTextValue(string? value, int maxSize, bool compress)
    {
        if (value == null)
        {
            return null;
        }

        int limit = maxSize > 0 ? maxSize : int.MaxValue;
        byte[] bytes = db.EncodeTextForFormat(value, limit, compress);
        if (maxSize > 0 && bytes.Length > maxSize)
        {
            Array.Resize(ref bytes, maxSize);
        }

        return bytes;
    }

    private byte[]? EncodeBinaryValue(object value, int maxSize)
    {
        byte[]? bytes = value as byte[];
        if (bytes == null)
        {
            string? stringValue = Convert.ToString(value, CultureInfo.InvariantCulture);
            if (string.IsNullOrEmpty(stringValue))
            {
                return null;
            }

            bytes = db.AnsiEncoding.GetBytes(stringValue);
        }

        if (maxSize > 0 && bytes.Length > maxSize)
        {
            Array.Resize(ref bytes, maxSize);
        }

        return bytes;
    }

    private byte[]? EncodeMemoValue(string? value, bool compress)
    {
        if (value == null)
        {
            return null;
        }

        byte[] data = db.EncodeTextForFormat(value, compress);
        if (data.Length > Constants.LongValue.MaxInlineMemoBytes)
        {
            throw new JetLimitationException($"MEMO value is {data.Length} bytes, which exceeds the inline limit of {Constants.LongValue.MaxInlineMemoBytes} bytes.");
        }

        return LongValueStore.WrapInlineLongValue(data);
    }
}
