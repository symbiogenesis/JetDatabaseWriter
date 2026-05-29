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
    private readonly List<ColumnInfo> columns;
    private readonly bool[]? wantedColumns;
    private readonly int[]? columnOrdinals;
    private readonly bool strictParsing;
    private readonly bool hasDeletedColumns;
    private readonly bool hasVarColumns;

    private RowDecodePlan(TableDef tableDef, bool[]? wantedColumns, int[]? columnOrdinals, bool strictParsing)
    {
        Guard.NotNull(tableDef, nameof(tableDef));

        columns = tableDef.Columns;
        this.wantedColumns = wantedColumns;
        this.columnOrdinals = columnOrdinals;
        this.strictParsing = strictParsing;
        hasDeletedColumns = tableDef.HasDeletedColumns;
        hasVarColumns = tableDef.HasVarColumns;
    }

    internal int ColumnCount => columns.Count;

    internal static RowDecodePlan CreateTyped(TableDef tableDef, bool[]? wantedColumns, bool strictParsing)
        => new(tableDef, wantedColumns, columnOrdinals: null, strictParsing);

    internal static RowDecodePlan CreatePartial(TableDef tableDef, int[] columnOrdinals)
    {
        Guard.NotNull(columnOrdinals, nameof(columnOrdinals));

        return new RowDecodePlan(tableDef, wantedColumns: null, columnOrdinals, strictParsing: true);
    }

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
                case TextType:
                    value = source.DecodeTextForFormat(page, start, length);
                    return true;

                case BinaryType:
                    value = page.AsSpan(start, length).ToArray();
                    return true;

                case ByteType:
                case IntegerType:
                case LongIntegerType:
                case FloatType:
                case DoubleType:
                case DateTimeType:
                case MoneyType:
                case GuidType:
                case NumericType:
                    int required = JetTypeInfo.GetFixedSize(column.Type);
                    if (length < required)
                    {
                        return false;
                    }

                    value = JetTypeInfo.ReadFixedTyped(page, start, column, column.Type == NumericType ? length : required, strictNumeric: true);
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
        if (!TryParseLayout(source, page, rowStart, rowSize, out AccessBase.RowLayout layout))
        {
            return false;
        }

        for (int columnIndex = 0; columnIndex < columns.Count; columnIndex++)
        {
            if (wantedColumns?[columnIndex] == false)
            {
                buffer[columnIndex] = null;
                continue;
            }

            ColumnInfo column = columns[columnIndex];
            AccessBase.ColumnSlice slice = source.ResolveColumnSliceForDecodePlan(page, rowStart, rowSize, layout, column);
            buffer[columnIndex] = DecodeTypedValue(source, page, rowStart, slice, column, longValueDecoder, ref needsLongValue);
        }

        return true;
    }

    internal bool TryDecodePartialColumns(AccessBase source, byte[] page, int rowStart, int rowSize, object?[] result)
    {
        if (columnOrdinals == null || result.Length < columnOrdinals.Length)
        {
            throw new InvalidOperationException("Partial row decoding requires a partial-column plan and a result buffer large enough for every ordinal.");
        }

        if (!TryParseLayout(source, page, rowStart, rowSize, out AccessBase.RowLayout layout))
        {
            return false;
        }

        for (int resultIndex = 0; resultIndex < columnOrdinals.Length; resultIndex++)
        {
            int columnOrdinal = columnOrdinals[resultIndex];
            if (columnOrdinal < 0 || columnOrdinal >= columns.Count)
            {
                return false;
            }

            ColumnInfo column = columns[columnOrdinal];
            AccessBase.ColumnSlice slice = source.ResolveColumnSliceForDecodePlan(page, rowStart, rowSize, layout, column);
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

        bool effectiveHasVarColumns = hasVarColumns || (hasDeletedColumns && rawNumCols > columns.Count);
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
                return JetTypeInfo.ReadFixedTyped(page, rowStart + slice.DataStart, column, slice.DataLen, strictParsing);

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
                case TextType:
                    return source.DecodeTextForFormat(page, start, length);

                case BinaryType:
                    return page.AsSpan(start, length).ToArray();

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
                case GuidType:
                case ComplexType:
                case AttachmentType:
                    int required = column.Type is ComplexType or AttachmentType ? 4 : JetTypeInfo.GetFixedSize(column.Type);
                    return length >= required
                        ? JetTypeInfo.ReadFixedTyped(page, start, column, required, strictParsing)
                        : TypedRowFallbackPolicy.FixedVariableSlotTooShort(column, length, required, strictParsing);

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
            return TypedRowFallbackPolicy.MalformedVariableValue(column, exception, strictParsing);
        }
        catch (IndexOutOfRangeException exception)
        {
            return TypedRowFallbackPolicy.MalformedVariableValue(column, exception, strictParsing);
        }
        catch (OverflowException exception)
        {
            return TypedRowFallbackPolicy.MalformedVariableValue(column, exception, strictParsing);
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
                default:
                    return CalculatedColumnUtil.ReadPayloadTyped(
                        CalculatedColumnUtil.Unwrap(page.AsSpan(start, length)),
                        JetTypeInfo.ResolveValueType(column),
                        strictParsing);
            }
        }
        catch (JetLimitationException)
        {
            throw;
        }
        catch (ArgumentException exception)
        {
            return TypedRowFallbackPolicy.MalformedVariableValue(column, exception, strictParsing);
        }
        catch (IndexOutOfRangeException exception)
        {
            return TypedRowFallbackPolicy.MalformedVariableValue(column, exception, strictParsing);
        }
        catch (OverflowException exception)
        {
            return TypedRowFallbackPolicy.MalformedVariableValue(column, exception, strictParsing);
        }
    }

    internal readonly record struct LongValueRef(int Start, int Len, bool IsOle);

    internal readonly record struct CalculatedLongValueRef(int Start, int Len, bool IsOle);
}
