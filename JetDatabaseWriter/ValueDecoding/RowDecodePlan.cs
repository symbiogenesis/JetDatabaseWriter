namespace JetDatabaseWriter.ValueDecoding;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Constants.ColumnTypes;

internal sealed class RowDecodePlan
{
    private readonly List<ColumnInfo> _columns;
    private readonly bool[]? _wantedColumns;
    private readonly int[]? _columnOrdinals;
    private readonly bool _strictParsing;
    private readonly bool _hasDeletedColumns;
    private readonly bool _hasVarColumns;

    private RowDecodePlan(TableDef tableDef, bool[]? wantedColumns, int[]? columnOrdinals, bool strictParsing)
    {
        Guard.NotNull(tableDef, nameof(tableDef));

        _columns = tableDef.Columns;
        _wantedColumns = wantedColumns;
        _columnOrdinals = columnOrdinals;
        _strictParsing = strictParsing;
        _hasDeletedColumns = tableDef.HasDeletedColumns;
        _hasVarColumns = tableDef.HasVarColumns;
    }

    internal int ColumnCount => _columns.Count;

    internal static RowDecodePlan CreateTyped(TableDef tableDef, bool[]? wantedColumns, bool strictParsing)
        => new(tableDef, wantedColumns, columnOrdinals: null, strictParsing);

    internal static RowDecodePlan CreatePartial(TableDef tableDef, int[] columnOrdinals)
    {
        Guard.NotNull(columnOrdinals, nameof(columnOrdinals));

        return new RowDecodePlan(tableDef, wantedColumns: null, columnOrdinals, strictParsing: true);
    }

    private static object DecodeLongVariableValue(
        AccessBase source,
        byte[] page,
        int start,
        int length,
        ColumnInfo column,
        LongValueDecoder longValueDecoder,
        ref bool needsLongValue)
    {
        bool isOle = column.Type == T_OLE;
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
                ? AccessReader.DecodeOleValueBytes(page, valueStart, inlineLength)
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
                case T_TEXT:
                    value = source.DecodeTextForFormat(page, start, length);
                    return true;

                case T_BINARY:
                    value = page.AsSpan(start, length).ToArray();
                    return true;

                case T_BYTE:
                case T_INT:
                case T_LONG:
                case T_FLOAT:
                case T_DOUBLE:
                case T_DATETIME:
                case T_MONEY:
                case T_GUID:
                case T_NUMERIC:
                    int required = JetTypeInfo.GetFixedSize(column.Type);
                    if (length < required)
                    {
                        return false;
                    }

                    value = JetTypeInfo.ReadFixedTyped(page, start, column, column.Type == T_NUMERIC ? length : required, strictNumeric: true);
                    return value is not DBNull;

                default:
                    return false;
            }
        }
        catch (JetLimitationException)
        {
            return false;
        }
        catch (ArgumentException)
        {
            return false;
        }
        catch (IndexOutOfRangeException)
        {
            return false;
        }
        catch (OverflowException)
        {
            return false;
        }
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
        if (!TryParseLayout(source, page, rowStart, rowSize, out var layout))
        {
            return false;
        }

        for (int columnIndex = 0; columnIndex < _columns.Count; columnIndex++)
        {
            if (_wantedColumns != null && !_wantedColumns[columnIndex])
            {
                buffer[columnIndex] = null;
                continue;
            }

            var column = _columns[columnIndex];
            var slice = source.ResolveColumnSliceForDecodePlan(page, rowStart, rowSize, layout, column);
            buffer[columnIndex] = DecodeTypedValue(source, page, rowStart, slice, column, longValueDecoder, ref needsLongValue);
        }

        return true;
    }

    internal bool TryDecodePartialColumns(AccessBase source, byte[] page, int rowStart, int rowSize, object?[] result)
    {
        if (_columnOrdinals == null || result.Length < _columnOrdinals.Length)
        {
            throw new InvalidOperationException("Partial row decoding requires a partial-column plan and a result buffer large enough for every ordinal.");
        }

        if (!TryParseLayout(source, page, rowStart, rowSize, out var layout))
        {
            return false;
        }

        for (int resultIndex = 0; resultIndex < _columnOrdinals.Length; resultIndex++)
        {
            int columnOrdinal = _columnOrdinals[resultIndex];
            if (columnOrdinal < 0 || columnOrdinal >= _columns.Count)
            {
                return false;
            }

            var column = _columns[columnOrdinal];
            var slice = source.ResolveColumnSliceForDecodePlan(page, rowStart, rowSize, layout, column);
            switch (slice.Kind)
            {
                case AccessBase.ColumnSliceKind.Bool:
                    result[resultIndex] = slice.BoolValue;
                    break;

                case AccessBase.ColumnSliceKind.Null:
                case AccessBase.ColumnSliceKind.Empty:
                    result[resultIndex] = null;
                    break;

                case AccessBase.ColumnSliceKind.Fixed:
                case AccessBase.ColumnSliceKind.Var:
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

    private bool TryParseLayout(
        AccessBase source,
        byte[] page,
        int rowStart,
        int rowSize,
        out AccessBase.RowLayout layout)
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

        bool effectiveHasVarColumns = _hasVarColumns || (_hasDeletedColumns && rawNumCols > _columns.Count);
        return source.TryParseRowLayoutForDecodePlan(page, rowStart, rowSize, effectiveHasVarColumns, out layout);
    }

    private object? DecodeTypedValue(
        AccessBase source,
        byte[] page,
        int rowStart,
        AccessBase.ColumnSlice slice,
        ColumnInfo column,
        LongValueDecoder longValueDecoder,
        ref bool needsLongValue)
    {
        switch (slice.Kind)
        {
            case AccessBase.ColumnSliceKind.Bool:
                return slice.BoolValue;

            case AccessBase.ColumnSliceKind.Null:
            case AccessBase.ColumnSliceKind.Empty:
                return DBNull.Value;

            case AccessBase.ColumnSliceKind.Fixed:
                return JetTypeInfo.ReadFixedTyped(page, rowStart + slice.DataStart, column, slice.DataLen, _strictParsing);

            case AccessBase.ColumnSliceKind.Var:
                return DecodeTypedVariableValue(source, page, rowStart + slice.DataStart, slice.DataLen, column, longValueDecoder, ref needsLongValue);

            default:
                return DBNull.Value;
        }
    }

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
            return DecodeCalculatedTypedVariableValue(source, page, start, length, column, ref needsLongValue);
        }

        try
        {
            switch (column.Type)
            {
                case T_TEXT:
                    return source.DecodeTextForFormat(page, start, length);

                case T_BINARY:
                    return page.AsSpan(start, length).ToArray();

                case T_MEMO:
                case T_OLE:
                    return DecodeLongVariableValue(source, page, start, length, column, longValueDecoder, ref needsLongValue);

                case T_BYTE:
                case T_INT:
                case T_LONG:
                case T_FLOAT:
                case T_DOUBLE:
                case T_DATETIME:
                case T_MONEY:
                case T_GUID:
                case T_COMPLEX:
                case T_ATTACHMENT:
                    int required = column.Type is T_COMPLEX or T_ATTACHMENT ? 4 : JetTypeInfo.GetFixedSize(column.Type);
                    return length >= required
                        ? JetTypeInfo.ReadFixedTyped(page, start, column, required, _strictParsing)
                        : TypedRowFallbackPolicy.FixedVariableSlotTooShort(column, length, required, _strictParsing);

                default:
                    return DBNull.Value;
            }
        }
        catch (JetLimitationException)
        {
            throw;
        }
        catch (ArgumentException exception)
        {
            return TypedRowFallbackPolicy.MalformedVariableValue(column, exception, _strictParsing);
        }
        catch (IndexOutOfRangeException exception)
        {
            return TypedRowFallbackPolicy.MalformedVariableValue(column, exception, _strictParsing);
        }
        catch (OverflowException exception)
        {
            return TypedRowFallbackPolicy.MalformedVariableValue(column, exception, _strictParsing);
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
                case T_TEXT:
                    byte[] textPayload = CalculatedColumnUtil.Unwrap(page.AsSpan(start, length));
                    return source.DecodeTextForFormat(textPayload, 0, textPayload.Length);
                case T_BINARY:
                    return CalculatedColumnUtil.Unwrap(page.AsSpan(start, length));
                case T_MEMO:
                case T_OLE:
                    needsLongValue = true;
                    return new CalculatedLongValueRef(start, length, column.Type == T_OLE);
                default:
                    return CalculatedColumnUtil.ReadPayloadTyped(
                        CalculatedColumnUtil.Unwrap(page.AsSpan(start, length)),
                        JetTypeInfo.ResolveValueType(column),
                        _strictParsing);
            }
        }
        catch (JetLimitationException)
        {
            throw;
        }
        catch (ArgumentException exception)
        {
            return TypedRowFallbackPolicy.MalformedVariableValue(column, exception, _strictParsing);
        }
        catch (IndexOutOfRangeException exception)
        {
            return TypedRowFallbackPolicy.MalformedVariableValue(column, exception, _strictParsing);
        }
        catch (OverflowException exception)
        {
            return TypedRowFallbackPolicy.MalformedVariableValue(column, exception, _strictParsing);
        }
    }

    internal readonly record struct LongValueRef(int Start, int Len, bool IsOle);

    internal readonly record struct CalculatedLongValueRef(int Start, int Len, bool IsOle);
}
