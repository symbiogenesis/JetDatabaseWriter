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
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueEncoding.Models;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Encodes in-memory value arrays into on-disk row byte layouts for a JET
/// data page.  Extracted from <see cref="AccessWriter"/>.
/// </summary>
/// <param name="writer">The writer.</param>
internal sealed class RowEncoder(AccessWriter writer)
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

    internal static void SetNullMaskBit(byte[] mask, int columnNumber, bool state)
        => SetNullMaskBit(mask.AsSpan(), columnNumber, state);

    internal static void SetNullMaskBit(Span<byte> mask, int columnNumber, bool state)
    {
        if (columnNumber < 0)
        {
            return;
        }

        int byteOffset = columnNumber / 8;
        int bitOffset = columnNumber % 8;
        if (byteOffset >= mask.Length)
        {
            return;
        }

        if (state)
        {
            mask[byteOffset] |= (byte)(1 << bitOffset);
        }
        else
        {
            mask[byteOffset] &= (byte)~(1 << bitOffset);
        }
    }

    private static int TryEncodeFixedValue(ColumnInfo column, object value, Span<byte> dest)
    {
        switch (column.Type)
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
                throw new InvalidOperationException($"Unknown column type: {JetTypeInfo.GetTypeDisplayName(column.Type)}");
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

    private static byte[]? EncodeCalculatedFixedPayload(ColumnInfo column, object value)
    {
        if (column.Type == BooleanType)
        {
            return [Convert.ToBoolean(value, CultureInfo.InvariantCulture) ? (byte)0xFF : (byte)0x00];
        }

        if (column.Type == NumericType)
        {
            return EncodeCalculatedNumericValue(Convert.ToDecimal(value, CultureInfo.InvariantCulture));
        }

        int fixedSize = JetTypeInfo.GetFixedSize(column.Type);
        if (fixedSize <= 0)
        {
            return null;
        }

        byte[] payload = new byte[fixedSize];
        int written = TryEncodeFixedValue(column, value, payload);
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

    private static int GetCalculatedVariableSize(ColumnInfo column)
        => column.Size > Constants.CalculatedColumn.ExtraDataLen
            ? column.Size - Constants.CalculatedColumn.ExtraDataLen
            : column.Size;

    /// <summary>
    /// Serializes a typed value array into the binary row format understood
    /// by the JET engine (null mask, fixed area, variable-length trailers).
    /// </summary>
    /// <param name="tableDef">The table def.</param>
    /// <param name="values">The values.</param>
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

        int nullMaskLen = (numCols + 7) / 8;
        int varLen = maxDefinedVarIdx + 1;

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
                    SetNullMaskBit(nullMask, column.ColNum, true);
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

                int written = TryEncodeFixedValue(column, value, fixedArea.AsSpan(column.FixedOff, fixedSize));
                if (written == 0)
                {
                    continue;
                }

                fixedAreaSize = Math.Max(fixedAreaSize, column.FixedOff + written);
                SetNullMaskBit(nullMask, column.ColNum, true);
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
                SetNullMaskBit(nullMask, column.ColNum, true);
            }
        }

        int baseRowLength = writer.RowFields.NumCols + fixedAreaSize + varPayloadSize + writer.RowFields.Eod + (varLen * writer.RowFields.VarEntry) + writer.RowFields.VarLen + nullMaskLen;

        int jumpSize = writer.Format != DatabaseFormat.Jet3Mdb ? 0 : baseRowLength / 256;
        int rowLength = baseRowLength + jumpSize;
        int finalJump = writer.Format != DatabaseFormat.Jet3Mdb ? 0 : rowLength / 256;
        if (finalJump != jumpSize)
        {
            jumpSize = finalJump;
            rowLength = baseRowLength + jumpSize;
        }

        byte[] row = new byte[rowLength];
        int pos = 0;

        WriteField(row, pos, writer.RowFields.NumCols, numCols);
        pos += writer.RowFields.NumCols;

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

        int currentOffset = writer.RowFields.NumCols + fixedAreaSize;

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

        WriteField(row, pos, writer.RowFields.Eod, currentOffset);
        pos += writer.RowFields.Eod;

        for (int varIndex = varLen - 1; varIndex >= 0; varIndex--)
        {
            WriteField(row, pos, writer.RowFields.VarEntry, variableOffsets[varIndex]);
            pos += writer.RowFields.VarEntry;
        }

        pos += jumpSize;

        WriteField(row, pos, writer.RowFields.VarLen, varLen);
        pos += writer.RowFields.VarLen;
        nullMask.CopyTo(row.AsSpan(pos));

        return row;
    }

    private bool CanStoreFixedColumn(ColumnInfo column)
    {
        int size = JetTypeInfo.GetFixedSize(column.Type);
        return size >= 0 && column.FixedOff >= 0 && column.FixedOff + size < writer.PageSizeBytes;
    }

    private byte[]? EncodeVariableValue(ColumnInfo column, object value)
    {
        if (column.IsCalculated)
        {
            return this.EncodeCalculatedValue(column, value);
        }

        switch (column.Type)
        {
            case TextType:
                return this.EncodeTextValue(Convert.ToString(value, CultureInfo.InvariantCulture), column.Size, column.IsCompressedUnicode);
            case BinaryType:
                return this.EncodeBinaryValue(value, column.Size);
            case MemoType:
                if (value is PreEncodedLongValue preMemo)
                {
                    return preMemo.HeaderBytes;
                }

                return this.EncodeMemoValue(Convert.ToString(value, CultureInfo.InvariantCulture), column.IsCompressedUnicode);
            case OleType:
                return EncodeOleValue(value);
            case ByteType:
            case IntegerType:
            case LongIntegerType:
            case FloatType:
            case DoubleType:
            case DateTimeType:
            case MoneyType:
            case BigIntType:
            case NumericType:
            case GuidType:
            case ComplexType:
            case AttachmentType:
            case DateTimeExtendedType:
                int fixedSize = column.Type is ComplexType or AttachmentType ? 4 : JetTypeInfo.GetFixedSize(column.Type);
                if (fixedSize <= 0)
                {
                    return null;
                }

                byte[] payload = new byte[fixedSize];
                int written = TryEncodeFixedValue(column, value, payload);
                if (written <= 0)
                {
                    return null;
                }

                if (written != payload.Length)
                {
                    Array.Resize(ref payload, written);
                }

                return payload;
            case BooleanType:
                return null;
            default:
                throw new InvalidOperationException($"Unknown column type: {JetTypeInfo.GetTypeDisplayName(column.Type)}");
        }
    }

    private byte[]? EncodeCalculatedValue(ColumnInfo column, object value)
    {
        switch (column.Type)
        {
            case TextType:
                return CalculatedColumnUtil.Wrap(
                    this.EncodeTextValue(Convert.ToString(value, CultureInfo.InvariantCulture), GetCalculatedVariableSize(column), compress: false) ?? []);
            case BinaryType:
                return CalculatedColumnUtil.Wrap(this.EncodeBinaryValue(value, GetCalculatedVariableSize(column)) ?? []);
            case MemoType:
                return this.EncodeCalculatedMemoValue(value);
            case OleType:
                return EncodeCalculatedOleValue(value);
            case BooleanType:
            case ByteType:
            case IntegerType:
            case LongIntegerType:
            case MoneyType:
            case FloatType:
            case DoubleType:
            case DateTimeType:
            case GuidType:
            case NumericType:
            case AttachmentType:
            case ComplexType:
            case BigIntType:
            case DateTimeExtendedType:
                byte[]? payload = EncodeCalculatedFixedPayload(column, value);
                return payload is null ? null : CalculatedColumnUtil.Wrap(payload);
            default:
                throw new InvalidOperationException($"Unsupported column type: {JetTypeInfo.GetTypeDisplayName(column.Type)}");
        }
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

        byte[] data = writer.EncodeTextForFormat(text, compress: false);
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
        byte[] bytes = writer.EncodeTextForFormat(value, limit, compress);
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

            bytes = writer.AnsiEncoding.GetBytes(stringValue);
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

        byte[] data = writer.EncodeTextForFormat(value, compress);
        if (data.Length > Constants.LongValue.MaxInlineMemoBytes)
        {
            throw new JetLimitationException($"MEMO value is {data.Length} bytes, which exceeds the inline limit of {Constants.LongValue.MaxInlineMemoBytes} bytes.");
        }

        return LongValueStore.WrapInlineLongValue(data);
    }
}
