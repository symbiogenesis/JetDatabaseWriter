namespace JetDatabaseWriter.ValueDecoding;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding.Models;
using static JetDatabaseWriter.Enums.ColumnType;

internal sealed class RowDecodePlan
{
    private readonly List<ColumnInfo> columns;
    private readonly bool[]? wantedColumns;
    private readonly int[]? columnOrdinals;
    private readonly bool strictParsing;
    private readonly bool hasDeletedColumns;
    private readonly bool hasVarColumns;

    private RowDecodePlan(TableDef tableDef, bool[]? wantedColumns, int[]? columnOrdinals, bool strictParsing)
    {
        Guard.NotNull(tableDef, nameof(tableDef));

        this.columns = tableDef.Columns;
        this.wantedColumns = wantedColumns;
        this.columnOrdinals = columnOrdinals;
        this.strictParsing = strictParsing;
        this.hasDeletedColumns = tableDef.HasDeletedColumns;
        this.hasVarColumns = tableDef.HasVarColumns;
    }

    internal int ColumnCount => this.columns.Count;

    internal static RowDecodePlan CreateTyped(TableDef tableDef, bool[]? wantedColumns, bool strictParsing)
        => new(tableDef, wantedColumns, columnOrdinals: null, strictParsing);

    internal static RowDecodePlan CreateStrings(TableDef tableDef, bool strictParsing)
        => new(tableDef, wantedColumns: null, columnOrdinals: null, strictParsing);

    internal static RowDecodePlan CreatePartial(TableDef tableDef, int[] columnOrdinals)
    {
        Guard.NotNull(columnOrdinals, nameof(columnOrdinals));

        return new RowDecodePlan(tableDef, wantedColumns: null, columnOrdinals, strictParsing: true);
    }

    internal static ColumnSlice ResolveColumnSliceForDirectDecode(
        AccessBase source,
        byte[] page,
        int rowStart,
        int rowSize,
        RowLayout layout,
        ColumnInfo column)
        => ResolveColumnSlice(source.RowFields, page, rowStart, rowSize, layout, column);

    /// <summary>
    /// Parses the row-trailer metadata (numCols, null-mask position, var-table
    /// position and EOD pointer) for a row at <paramref name="rowStart"/>.
    /// Returns <see langword="false"/> when the row is too small or otherwise
    /// malformed; on success <paramref name="layout"/> is populated and can be
    /// passed to <see cref="ResolveColumnSlice"/> for any column.
    /// </summary>
    /// <param name="format">The database format (selects the Jet3 jump-byte rule).</param>
    /// <param name="rowFields">Row-trailer field sizes for the format.</param>
    /// <param name="page">Data page containing the row.</param>
    /// <param name="rowStart">Offset of the row within <paramref name="page"/>.</param>
    /// <param name="rowSize">Total size of the row in bytes.</param>
    /// <param name="hasVarColumns">When <see langword="false"/>, the var-length
    /// metadata is assumed to be omitted entirely (no varLen byte, no jump
    /// bytes, no var-offset table, no EOD marker) — which is how Jet lays out
    /// rows for tables with zero variable-length columns.</param>
    /// <param name="layout">Receives the parsed layout on success.</param>
    internal static bool TryParseRowLayout(
        DatabaseFormat format,
        in RowFieldSizes rowFields,
        ReadOnlySpan<byte> page,
        int rowStart,
        int rowSize,
        bool hasVarColumns,
        out RowLayout layout)
    {
        layout = default;
        if (rowSize < rowFields.NumCols)
        {
            return false;
        }

        int numCols = rowFields.ReadNumCols(page, rowStart);
        if (numCols == 0)
        {
            return false;
        }

        int nullMaskSz = (numCols + 7) / 8;
        int nullMaskPos = rowSize - nullMaskSz;
        if (nullMaskPos < rowFields.NumCols)
        {
            return false;
        }

        int varLen;
        int varTableStart;
        int eod;
        if (!hasVarColumns)
        {
            varLen = 0;
            varTableStart = nullMaskPos;
            eod = nullMaskPos;
        }
        else
        {
            int varLenPos = nullMaskPos - rowFields.VarLen;
            if (varLenPos < rowFields.NumCols)
            {
                return false;
            }

            varLen = rowFields.ReadVarLen(page, rowStart + varLenPos);
            int jumpSz = format != DatabaseFormat.Jet3Mdb ? 0 : (rowSize / 256);
            varTableStart = varLenPos - jumpSz - (varLen * rowFields.VarEntry);
            int eodPos = varTableStart - rowFields.Eod;
            if (eodPos < rowFields.NumCols)
            {
                return false;
            }

            eod = rowFields.ReadEod(page, rowStart + eodPos);
        }

        layout = new RowLayout(numCols, nullMaskPos, varLen, varTableStart, eod);
        return true;
    }

    /// <summary>
    /// Resolves the per-column data slice (or null/bool/empty marker) for
    /// <paramref name="col"/> within a row whose layout has been parsed by
    /// <see cref="TryParseRowLayout"/>.
    /// </summary>
    /// <param name="rowFields">Row-trailer field sizes for the format.</param>
    /// <param name="page">The page bytes.</param>
    /// <param name="rowStart">The row start.</param>
    /// <param name="rowSize">The row size.</param>
    /// <param name="layout">The layout.</param>
    /// <param name="col">The column descriptor.</param>
    internal static ColumnSlice ResolveColumnSlice(
        in RowFieldSizes rowFields,
        ReadOnlySpan<byte> page,
        int rowStart,
        int rowSize,
        in RowLayout layout,
        ColumnInfo col)
    {
        bool nullBit = false;
        if (col.ColNum < layout.NumCols)
        {
            int mByte = layout.NullMaskPos + (col.ColNum / 8);
            int mBit = col.ColNum % 8;
            if (mByte < rowSize)
            {
                nullBit = (page[rowStart + mByte] & (1 << mBit)) != 0;
            }
        }

        if (col.Type == BooleanType && !col.IsCalculated)
        {
            return new ColumnSlice(ColumnSliceKind.Bool, 0, 0, nullBit);
        }

        if (col.ColNum >= layout.NumCols || !nullBit)
        {
            return new ColumnSlice(ColumnSliceKind.Null, 0, 0, false);
        }

        if (col.IsFixed)
        {
            int start = rowFields.NumCols + col.FixedOff;
            int sz = col.IsCalculated ? col.Size : JetTypeInfo.GetFixedSize(col.Type);
            if (sz == 0 || start + sz > rowSize)
            {
                return new ColumnSlice(ColumnSliceKind.Empty, 0, 0, false);
            }

            return new ColumnSlice(col.IsCalculated ? ColumnSliceKind.Var : ColumnSliceKind.Fixed, start, sz, false);
        }

        if (col.VarIdx >= layout.VarLen)
        {
            return new ColumnSlice(ColumnSliceKind.Empty, 0, 0, false);
        }

        int entryPos = layout.VarTableStart + ((layout.VarLen - 1 - col.VarIdx) * rowFields.VarEntry);
        if (entryPos < 0 || entryPos + rowFields.VarEntry > rowSize)
        {
            return new ColumnSlice(ColumnSliceKind.Empty, 0, 0, false);
        }

        int varOff = rowFields.ReadVarEntry(page, rowStart + entryPos);

        int varEnd;
        if (col.VarIdx + 1 < layout.VarLen)
        {
            int nextEntry = layout.VarTableStart + ((layout.VarLen - 2 - col.VarIdx) * rowFields.VarEntry);
            varEnd = rowFields.ReadVarEntry(page, rowStart + nextEntry);
        }
        else
        {
            varEnd = layout.Eod;
        }

        int dataStart = varOff;
        int dataLen = varEnd - varOff;
        if (dataLen < 0 || dataStart < 0 || dataStart + dataLen > rowSize)
        {
            return new ColumnSlice(ColumnSliceKind.Empty, 0, 0, false);
        }

        return new ColumnSlice(ColumnSliceKind.Var, dataStart, dataLen, false);
    }

    internal bool TryDecodeDirect<T>(
        AccessReader source,
        byte[] page,
        int rowStart,
        int rowSize,
        DirectRowDecoder<T> directDecoder,
        T target)
        where T : class, new()
        => directDecoder(source, this, page, rowStart, rowSize, target);

    private static object DecodeLongVariableValue(
        byte[] page,
        int start,
        int length,
        ColumnInfo column,
        LongValueDecoder longValueDecoder,
        ref bool needsLongValue)
    {
        bool isOle = column.Type == OleType;
        if (length >= Constants.LongValue.HeaderSize
            && (page[start + 3] & Constants.LongValue.StorageModeMask) == Constants.LongValue.InlineStorageMode)
        {
            int valueLength = JetTypeInfo.ReadUInt24LittleEndian(page.AsSpan(start, 3));
            int valueStart = start + Constants.LongValue.HeaderSize;
            int inlineLength = Math.Min(valueLength, page.Length - valueStart);
            if (inlineLength <= 0)
            {
                return isOle ? Array.Empty<byte>() : string.Empty;
            }

            return isOle
                ? OleObjectDecoder.DecodeOleValueBytes(page, valueStart, inlineLength)
                : longValueDecoder.DecodeLongValue(page, valueStart, inlineLength, isOle: false);
        }

        needsLongValue = true;
        return new LongValueRef(start, length, isOle);
    }

    private static bool TryDecodeInlineColumnValue(
        AccessBase source,
        byte[] page,
        int start,
        ColumnInfo column,
        int length,
        out object? value)
    {
        value = null;
        if (length <= 0)
        {
            return false;
        }

        try
        {
            switch (column.Type)
            {
                case TextType:
                    value = source.DecodeTextForFormat(page, start, length);
                    return true;

                case BinaryType:
                    value = BinaryBuffer.CopySlice(page, start, length);
                    return true;

                case ByteType:
                case IntegerType:
                case LongIntegerType:
                case FloatType:
                case DoubleType:
                case DateTimeType:
                case MoneyType:
                case BigIntType:
                case GuidType:
                case NumericType:
                case DateTimeExtendedType:
                    int required = JetTypeInfo.GetFixedSize(column.Type);
                    if (length < required)
                    {
                        return false;
                    }

                    value = JetTypeInfo.ReadFixedTyped(page, start, column, column.Type == NumericType ? length : required, strictNumeric: true);
                    return value is not DBNull;
                case BooleanType:
                case OleType:
                case MemoType:
                case AttachmentType:
                case ComplexType:
                    return false;
                default:
                    throw new InvalidOperationException($"Unknown column type: {JetTypeInfo.GetTypeDisplayName(column.Type)}");
            }
        }
        catch (Exception ex) when (ex is JetLimitationException or ArgumentException or OverflowException)
        {
            // Corrupt row offsets surface as ArgumentException from the text/binary
            // slicers, and strict Numeric limits surface as JetLimitationException;
            // both mean "this inline value cannot be decoded". An out-of-range
            // *index* would indicate a real slicing bug, so IndexOutOfRangeException
            // is intentionally left to propagate rather than collapsing to false.
            return false;
        }
    }

    internal async ValueTask<string[]?> TryDecodeStringRowAsync(
        AccessBase source,
        byte[] page,
        int rowStart,
        int rowSize,
        LongValueDecoder longValueDecoder,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!this.TryParseLayout(source, page, rowStart, rowSize, out RowLayout layout))
        {
            return null;
        }

        string[] result = new string[this.columns.Count];
        for (int columnIndex = 0; columnIndex < this.columns.Count; columnIndex++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            ColumnInfo column = this.columns[columnIndex];
            ColumnSlice slice = ResolveColumnSlice(source.RowFields, page, rowStart, rowSize, layout, column);
            result[columnIndex] = await this.DecodeStringValueAsync(
                source,
                page,
                rowStart,
                slice,
                column,
                longValueDecoder,
                cancellationToken).ConfigureAwait(false);
        }

        return result;
    }

    internal bool TryDecodeTypedIntoBuffer(
        AccessBase source,
        byte[] page,
        int rowStart,
        int rowSize,
        LongValueDecoder longValueDecoder,
        object?[] buffer,
        out bool needsLongValue)
    {
        needsLongValue = false;
        if (!this.TryParseLayout(source, page, rowStart, rowSize, out RowLayout layout))
        {
            return false;
        }

        for (int columnIndex = 0; columnIndex < this.columns.Count; columnIndex++)
        {
            if (this.wantedColumns?[columnIndex] == false)
            {
                buffer[columnIndex] = null;
                continue;
            }

            ColumnInfo column = this.columns[columnIndex];
            ColumnSlice slice = ResolveColumnSlice(source.RowFields, page, rowStart, rowSize, layout, column);
            buffer[columnIndex] = this.DecodeTypedValue(source, page, rowStart, slice, column, longValueDecoder, ref needsLongValue);
        }

        return true;
    }

    internal bool TryDecodePartialColumns(AccessBase source, byte[] page, int rowStart, int rowSize, object?[] result)
    {
        if (this.columnOrdinals == null || result.Length < this.columnOrdinals.Length)
        {
            throw new InvalidOperationException("Partial row decoding requires a partial-column plan and a result buffer large enough for every ordinal.");
        }

        if (!this.TryParseLayout(source, page, rowStart, rowSize, out RowLayout layout))
        {
            return false;
        }

        for (int resultIndex = 0; resultIndex < this.columnOrdinals.Length; resultIndex++)
        {
            int columnOrdinal = this.columnOrdinals[resultIndex];
            if (columnOrdinal < 0 || columnOrdinal >= this.columns.Count)
            {
                return false;
            }

            ColumnInfo column = this.columns[columnOrdinal];
            ColumnSlice slice = ResolveColumnSlice(source.RowFields, page, rowStart, rowSize, layout, column);
            switch (slice.Kind)
            {
                case ColumnSliceKind.Bool:
                    result[resultIndex] = slice.BoolValue;
                    break;

                case ColumnSliceKind.Null:
                case ColumnSliceKind.Empty:
                    result[resultIndex] = null;
                    break;

                case ColumnSliceKind.Fixed:
                case ColumnSliceKind.Var:
                    if (column.IsCalculated
                        || !TryDecodeInlineColumnValue(source, page, rowStart + slice.DataStart, column, slice.DataLen, out object? value))
                    {
                        return false;
                    }

                    result[resultIndex] = value;
                    break;

                default:
                    return false;
            }
        }

        return true;
    }

    internal bool TryParseLayoutForDirectDecode(
        AccessBase source,
        byte[] page,
        int rowStart,
        int rowSize,
        out RowLayout layout)
        => this.TryParseLayout(source, page, rowStart, rowSize, out layout);

    private bool TryParseLayout(
        AccessBase source,
        byte[] page,
        int rowStart,
        int rowSize,
        out RowLayout layout)
    {
        layout = default;
        if (rowSize < source.RowColumnCountFieldSize)
        {
            return false;
        }

        int rawNumCols = source.ReadRowColumnCount(page, rowStart);
        if (rawNumCols == 0)
        {
            return false;
        }

        bool effectiveHasVarColumns = this.hasVarColumns || (this.hasDeletedColumns && rawNumCols > this.columns.Count);
        return TryParseRowLayout(source.Format, source.RowFields, page, rowStart, rowSize, effectiveHasVarColumns, out layout);
    }

    private async ValueTask<string> DecodeStringValueAsync(
        AccessBase source,
        byte[] page,
        int rowStart,
        ColumnSlice slice,
        ColumnInfo column,
        LongValueDecoder longValueDecoder,
        CancellationToken cancellationToken) => slice.Kind switch
        {
            ColumnSliceKind.Bool => slice.BoolValue ? "True" : "False",
            ColumnSliceKind.Null or ColumnSliceKind.Empty => string.Empty,
            ColumnSliceKind.Fixed => JetTypeInfo.ReadFixedString(page, rowStart + slice.DataStart, column, slice.DataLen, strictNumeric: true),
            ColumnSliceKind.Var => await this.DecodeStringVariableValueAsync(
                source,
                page,
                rowStart + slice.DataStart,
                slice.DataLen,
                column,
                longValueDecoder,
                cancellationToken).ConfigureAwait(false),
            _ => string.Empty,
        };

    private async ValueTask<string> DecodeStringVariableValueAsync(
        AccessBase source,
        byte[] page,
        int start,
        int length,
        ColumnInfo column,
        LongValueDecoder longValueDecoder,
        CancellationToken cancellationToken)
    {
        if (length <= 0)
        {
            return string.Empty;
        }

        if (column.IsCalculated)
        {
            return await this.DecodeCalculatedStringVariableValueAsync(
                source,
                page,
                start,
                length,
                column,
                longValueDecoder,
                cancellationToken).ConfigureAwait(false);
        }

        try
        {
            switch (column.Type)
            {
                case TextType:
                    return source.DecodeTextForFormat(page, start, length);

                case BinaryType:
                    return JetTypeInfo.ToHexStringNoSeparator(page.AsSpan(start, length));

                case MemoType:
                case OleType:
                    return await longValueDecoder.ReadLongValueAsync(page, start, length, column.Type == OleType, cancellationToken).ConfigureAwait(false);

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
                case DateTimeExtendedType:
                case ComplexType:
                case AttachmentType:
                    int required = column.Type is ComplexType or AttachmentType ? 4 : JetTypeInfo.GetFixedSize(column.Type);
                    return length >= required
                        ? JetTypeInfo.ReadFixedString(page, start, column, required, strictNumeric: true)
                        : string.Empty;

                case BooleanType:
                    return string.Empty;

                default:
                    throw new InvalidOperationException($"Column '{column.Name}' has unknown type {JetTypeInfo.GetTypeDisplayName(column.Type)}.");
            }
        }
        catch (JetLimitationException)
        {
            throw;
        }
        catch (ArgumentException)
        {
            return string.Empty;
        }
        catch (IndexOutOfRangeException)
        {
            return string.Empty;
        }
    }

    private async ValueTask<string> DecodeCalculatedStringVariableValueAsync(
        AccessBase source,
        byte[] page,
        int start,
        int length,
        ColumnInfo column,
        LongValueDecoder longValueDecoder,
        CancellationToken cancellationToken)
    {
        try
        {
            switch (column.Type)
            {
                case TextType:
                    byte[] textPayload = CalculatedColumnUtil.Unwrap(page.AsSpan(start, length));
                    return source.DecodeTextForFormat(textPayload, 0, textPayload.Length);
                case BinaryType:
                    return JetTypeInfo.ToHexStringNoSeparator(CalculatedColumnUtil.Unwrap(page.AsSpan(start, length)));
                case MemoType:
                {
                    byte[] raw = await longValueDecoder.ReadLongValueRawBytesAsync(page, start, length, cancellationToken).ConfigureAwait(false);
                    byte[] payload = CalculatedColumnUtil.Unwrap(raw);
                    return longValueDecoder.DecodeLongValue(payload, 0, payload.Length, isOle: false);
                }

                case OleType:
                {
                    byte[] raw = await longValueDecoder.ReadLongValueRawBytesAsync(page, start, length, cancellationToken).ConfigureAwait(false);
                    byte[] payload = CalculatedColumnUtil.Unwrap(raw);
                    return longValueDecoder.DecodeLongValue(payload, 0, payload.Length, isOle: true);
                }

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
                    return CalculatedColumnUtil.ReadPayloadString(
                        CalculatedColumnUtil.Unwrap(page.AsSpan(start, length)),
                        JetTypeInfo.ResolveValueType(column),
                        this.strictParsing);

                default:
                    throw new InvalidOperationException($"Calculated column of type {JetTypeInfo.GetTypeDisplayName(column.Type)} is unknown.");
            }
        }
        catch (JetLimitationException)
        {
            throw;
        }
        catch (ArgumentException)
        {
            return string.Empty;
        }
        catch (IndexOutOfRangeException)
        {
            return string.Empty;
        }
    }

    private object? DecodeTypedValue(
        AccessBase source,
        byte[] page,
        int rowStart,
        ColumnSlice slice,
        ColumnInfo column,
        LongValueDecoder longValueDecoder,
        ref bool needsLongValue) => slice.Kind switch
        {
            ColumnSliceKind.Bool => slice.BoolValue,
            ColumnSliceKind.Null or ColumnSliceKind.Empty => DBNull.Value,
            ColumnSliceKind.Fixed => JetTypeInfo.ReadFixedTyped(page, rowStart + slice.DataStart, column, slice.DataLen, this.strictParsing),
            ColumnSliceKind.Var => this.DecodeTypedVariableValue(source, page, rowStart + slice.DataStart, slice.DataLen, column, longValueDecoder, ref needsLongValue),
            _ => DBNull.Value,
        };

    private object? DecodeTypedVariableValue(
        AccessBase source,
        byte[] page,
        int start,
        int length,
        ColumnInfo column,
        LongValueDecoder longValueDecoder,
        ref bool needsLongValue)
    {
        if (length <= 0)
        {
            return TypedRowFallbackPolicy.EmptyVariableValue(column);
        }

        if (column.IsCalculated)
        {
            return this.DecodeCalculatedTypedVariableValue(source, page, start, length, column, ref needsLongValue);
        }

        try
        {
            switch (column.Type)
            {
                case TextType:
                    return source.DecodeTextForFormat(page, start, length);

                case BinaryType:
                    return BinaryBuffer.CopySlice(page, start, length);

                case MemoType:
                case OleType:
                    return DecodeLongVariableValue(page, start, length, column, longValueDecoder, ref needsLongValue);

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
                case DateTimeExtendedType:
                case ComplexType:
                case AttachmentType:
                    int required = column.Type is ComplexType or AttachmentType ? 4 : JetTypeInfo.GetFixedSize(column.Type);
                    return length >= required
                        ? JetTypeInfo.ReadFixedTyped(page, start, column, required, this.strictParsing)
                        : TypedRowFallbackPolicy.FixedVariableSlotTooShort(column, length, required, this.strictParsing);

                case BooleanType:
                    return DBNull.Value;

                default:
                    throw new InvalidOperationException($"Column '{column.Name}' has unknown type {JetTypeInfo.GetTypeDisplayName(column.Type)}.");
            }
        }
        catch (JetLimitationException)
        {
            throw;
        }
        catch (ArgumentException exception)
        {
            return TypedRowFallbackPolicy.MalformedVariableValue(column, exception, this.strictParsing);
        }
        catch (IndexOutOfRangeException exception)
        {
            return TypedRowFallbackPolicy.MalformedVariableValue(column, exception, this.strictParsing);
        }
        catch (OverflowException exception)
        {
            return TypedRowFallbackPolicy.MalformedVariableValue(column, exception, this.strictParsing);
        }
    }

    private object? DecodeCalculatedTypedVariableValue(
        AccessBase source,
        byte[] page,
        int start,
        int length,
        ColumnInfo column,
        ref bool needsLongValue)
    {
        try
        {
            switch (column.Type)
            {
                case TextType:
                    byte[] textPayload = CalculatedColumnUtil.Unwrap(page.AsSpan(start, length));
                    return source.DecodeTextForFormat(textPayload, 0, textPayload.Length);
                case BinaryType:
                    return CalculatedColumnUtil.Unwrap(page.AsSpan(start, length));
                case MemoType:
                case OleType:
                    needsLongValue = true;
                    return new CalculatedLongValueRef(start, length, column.Type == OleType);
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
                    return CalculatedColumnUtil.ReadPayloadTyped(
                        CalculatedColumnUtil.Unwrap(page.AsSpan(start, length)),
                        JetTypeInfo.ResolveValueType(column),
                        this.strictParsing);
                default:
                    throw new InvalidOperationException($"Calculated column of type {JetTypeInfo.GetTypeDisplayName(column.Type)} is unknown.");
            }
        }
        catch (JetLimitationException)
        {
            throw;
        }
        catch (ArgumentException exception)
        {
            return TypedRowFallbackPolicy.MalformedVariableValue(column, exception, this.strictParsing);
        }
        catch (IndexOutOfRangeException exception)
        {
            return TypedRowFallbackPolicy.MalformedVariableValue(column, exception, this.strictParsing);
        }
        catch (OverflowException exception)
        {
            return TypedRowFallbackPolicy.MalformedVariableValue(column, exception, this.strictParsing);
        }
    }
}
