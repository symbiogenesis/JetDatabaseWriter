namespace JetDatabaseWriter.ValueEncoding;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueEncoding.Models;
using static JetDatabaseWriter.AccessBase;
using static JetDatabaseWriter.Constants.ColumnTypes;

/// <summary>
/// Encodes in-memory value arrays into on-disk row byte layouts for a JET
/// data page.  Extracted from <see cref="AccessWriter"/>.
/// </summary>
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

        if (data.Length > AccessWriter.MaxInlineOleBytes)
        {
            throw new JetLimitationException($"OLE value is {data.Length} bytes, which exceeds the inline limit of {AccessWriter.MaxInlineOleBytes} bytes.");
        }

        return LongValueEncoder.WrapInlineLongValue(data);
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
            case T_BYTE:
                dest[0] = Convert.ToByte(value, CultureInfo.InvariantCulture);
                return 1;

            case T_INT:
                BinaryPrimitives.WriteInt16LittleEndian(dest, Convert.ToInt16(value, CultureInfo.InvariantCulture));
                return 2;

            case T_LONG:
                BinaryPrimitives.WriteInt32LittleEndian(dest, Convert.ToInt32(value, CultureInfo.InvariantCulture));
                return 4;

            case T_FLOAT:
                BinaryPrimitives.WriteInt32LittleEndian(
                    dest,
                    BitConverter.SingleToInt32Bits(Convert.ToSingle(value, CultureInfo.InvariantCulture)));
                return 4;

            case T_DOUBLE:
                BinaryPrimitives.WriteInt64LittleEndian(
                    dest,
                    BitConverter.DoubleToInt64Bits(Convert.ToDouble(value, CultureInfo.InvariantCulture)));
                return 8;

            case T_DATETIME:
                BinaryPrimitives.WriteInt64LittleEndian(
                    dest,
                    BitConverter.DoubleToInt64Bits(Convert.ToDateTime(value, CultureInfo.InvariantCulture).ToOADate()));
                return 8;

            case T_MONEY:
                BinaryPrimitives.WriteInt64LittleEndian(
                    dest,
                    decimal.ToOACurrency(Convert.ToDecimal(value, CultureInfo.InvariantCulture)));
                return 8;

            case T_NUMERIC:
                EncodeNumericValue(Convert.ToDecimal(value, CultureInfo.InvariantCulture), dest);
                return 17;

            case T_GUID:
                {
                    Guid g = value is Guid guid
                        ? guid
                        : Guid.Parse(Convert.ToString(value, CultureInfo.InvariantCulture)!);
                    if (!g.TryWriteBytes(dest))
                    {
                        return 0;
                    }

                    return 16;
                }

            default:
                return 0;
        }
    }

    private static void EncodeNumericValue(decimal value, Span<byte> dest)
    {
        Span<byte> mantissa = dest.Slice(4, 12);
        NumericEncoder.Decompose(value, mantissa, out bool negative, out int scale);

        dest[0] = NumericEncoder.ComputePrecision(mantissa);
        dest[1] = (byte)scale;
        dest[2] = negative ? (byte)1 : (byte)0;
        dest[3] = 0;
    }

    /// <summary>
    /// Serializes a typed value array into the binary row format understood
    /// by the JET engine (null mask, fixed area, variable-length trailers).
    /// </summary>
    internal byte[] SerializeRow(TableDef tableDef, object[] values)
    {
        int numCols = 0;
        int maxFixedEnd = 0;
        int maxDefinedVarIdx = -1;
        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            ColumnInfo col = tableDef.Columns[i];
            numCols = Math.Max(numCols, col.ColNum + 1);
            if (col.IsFixed && col.Type != T_BOOL)
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
        var varEntries = varLen > 0 ? new byte[varLen][] : [];
        int varPayloadSize = 0;

        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            ColumnInfo column = tableDef.Columns[i];
            object value = values[i] ?? DBNull.Value;

            if (column.Type == T_BOOL)
            {
                if (value is not DBNull && Convert.ToBoolean(value, CultureInfo.InvariantCulture))
                {
                    SetNullMaskBit(nullMask, column.ColNum, true);
                }

                continue;
            }

            if (value is DBNull)
            {
                if (column.IsFixed && (column.Type == T_ATTACHMENT || column.Type == T_COMPLEX))
                {
                    fixedAreaSize = Math.Max(fixedAreaSize, column.FixedOff + JetTypeInfo.GetFixedSize(column.Type));
                }

                continue;
            }

            if (column.IsFixed)
            {
                if (!CanStoreFixedColumn(column))
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
                byte[]? variableValue = EncodeVariableValue(column, value);
                if (variableValue == null)
                {
                    continue;
                }

                varEntries[column.VarIdx] = variableValue;
                varPayloadSize += variableValue.Length;
                SetNullMaskBit(nullMask, column.ColNum, true);
            }
        }

        int baseRowLength = writer._rowSz.NumCols + fixedAreaSize + varPayloadSize + writer._rowSz.Eod + (varLen * writer._rowSz.VarEntry) + writer._rowSz.VarLen + nullMaskLen;

        int jumpSize = writer._format != DatabaseFormat.Jet3Mdb ? 0 : baseRowLength / 256;
        int rowLength = baseRowLength + jumpSize;
        int finalJump = writer._format != DatabaseFormat.Jet3Mdb ? 0 : rowLength / 256;
        if (finalJump != jumpSize)
        {
            jumpSize = finalJump;
            rowLength = baseRowLength + jumpSize;
        }

        var row = new byte[rowLength];
        int pos = 0;

        WriteField(row, pos, writer._rowSz.NumCols, numCols);
        pos += writer._rowSz.NumCols;

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

        int currentOffset = writer._rowSz.NumCols + fixedAreaSize;

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

        WriteField(row, pos, writer._rowSz.Eod, currentOffset);
        pos += writer._rowSz.Eod;

        for (int varIndex = varLen - 1; varIndex >= 0; varIndex--)
        {
            WriteField(row, pos, writer._rowSz.VarEntry, variableOffsets[varIndex]);
            pos += writer._rowSz.VarEntry;
        }

        pos += jumpSize;

        WriteField(row, pos, writer._rowSz.VarLen, varLen);
        pos += writer._rowSz.VarLen;
        nullMask.CopyTo(row.AsSpan(pos));

        return row;
    }

    private bool CanStoreFixedColumn(ColumnInfo column)
    {
        int size = JetTypeInfo.GetFixedSize(column.Type);
        return size >= 0 && column.FixedOff >= 0 && column.FixedOff + size < writer._pgSz;
    }

    private byte[]? EncodeVariableValue(ColumnInfo column, object value)
    {
        switch (column.Type)
        {
            case T_TEXT:
                return EncodeTextValue(Convert.ToString(value, CultureInfo.InvariantCulture), column.Size);
            case T_BINARY:
                return EncodeBinaryValue(value, column.Size);
            case T_MEMO:
                if (value is PreEncodedLongValue preMemo)
                {
                    return preMemo.HeaderBytes;
                }

                return EncodeMemoValue(Convert.ToString(value, CultureInfo.InvariantCulture));
            case T_OLE:
                return EncodeOleValue(value);
            default:
                return null;
        }
    }

    private byte[]? EncodeTextValue(string? value, int maxSize)
    {
        if (value == null)
        {
            return null;
        }

        int limit = maxSize > 0 ? maxSize : int.MaxValue;
        byte[] bytes = writer._format != DatabaseFormat.Jet3Mdb ? EncodeJet4Text(value, limit) : writer.AnsiEncoding.GetBytes(value);
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

    private byte[]? EncodeMemoValue(string? value)
    {
        if (value == null)
        {
            return null;
        }

        byte[] data = writer._format != DatabaseFormat.Jet3Mdb ? EncodeJet4Text(value) : writer.AnsiEncoding.GetBytes(value);
        if (data.Length > AccessWriter.MaxInlineMemoBytes)
        {
            throw new JetLimitationException($"MEMO value is {data.Length} bytes, which exceeds the inline limit of {AccessWriter.MaxInlineMemoBytes} bytes.");
        }

        return LongValueEncoder.WrapInlineLongValue(data);
    }
}
