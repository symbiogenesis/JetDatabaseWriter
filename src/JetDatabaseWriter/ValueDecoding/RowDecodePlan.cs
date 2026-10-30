namespace JetDatabaseWriter.ValueDecoding;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding.Models;
using static JetDatabaseWriter.Enums.ColumnType;

internal sealed class RowDecodePlan
{
    private readonly IReadOnlyList<ColumnInfo> columns;
    private readonly bool[]? wantedColumns;
    private readonly int[]? columnOrdinals;
    private readonly bool strictParsing;
    private readonly bool hasDeletedColumns;
    private readonly bool hasVarColumns;
    private readonly bool hasBigBinaryColumns;

    private RowDecodePlan(TableDef tableDef, bool[]? wantedColumns, int[]? columnOrdinals, bool strictParsing, bool preserveStoredValues = false)
    {
        Guard.NotNull(tableDef, nameof(tableDef));

        this.columns = tableDef.Columns;
        this.wantedColumns = wantedColumns;
        this.columnOrdinals = columnOrdinals;
        this.strictParsing = strictParsing;
        this.hasDeletedColumns = tableDef.HasDeletedColumns;
        this.hasVarColumns = tableDef.HasVarColumns;
        foreach (ColumnInfo column in this.columns)
        {
            this.hasBigBinaryColumns |= column.Type == BigBinaryType;
        }

        if (columnOrdinals is not null)
        {
            foreach (int ordinal in columnOrdinals)
            {
                if (ordinal >= 0 && ordinal < this.columns.Count)
                {
                    ValidateColumnType(this.columns[ordinal], tableDef.TDefPageNumber);
                }
            }
        }
        else
        {
            for (int ordinal = 0; ordinal < this.columns.Count; ordinal++)
            {
                if (wantedColumns?[ordinal] != false)
                {
                    ValidateColumnType(this.columns[ordinal], tableDef.TDefPageNumber);
                }
            }
        }

        this.PreservesStoredValues = preserveStoredValues;
    }

    internal int ColumnCount => this.columns.Count;

    /// <summary>
    /// Gets a value indicating whether values retain their storage identity for
    /// a write-back. Dates retain their original OLE serial. A MEMO / OLE value
    /// whose stored bytes cannot be read decodes to an
    /// <see cref="UnreadableLongValue"/>, which the writer refuses to store,
    /// instead of a placeholder such as <c>"(memo)"</c>, an empty array or
    /// <see cref="DBNull"/>. OLE cells hold their stored bytes in every plan.
    /// </summary>
    internal bool PreservesStoredValues { get; }

    internal static RowDecodePlan CreateTyped(TableDef tableDef, bool[]? wantedColumns, bool strictParsing)
        => new(tableDef, wantedColumns, columnOrdinals: null, strictParsing);

    /// <summary>
    /// Creates a typed plan for the writer's row snapshots, whose values are
    /// re-inserted by updates, cascades and schema rewrites. See
    /// <see cref="PreservesStoredValues"/>.
    /// </summary>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="strictParsing">Whether malformed values throw instead of decoding to a fallback.</param>
    /// <param name="wantedColumns">The selected columns, or all columns when null.</param>
    internal static RowDecodePlan CreateTypedForWriteBack(TableDef tableDef, bool strictParsing, bool[]? wantedColumns = null)
        => new(tableDef, wantedColumns, columnOrdinals: null, strictParsing, preserveStoredValues: true);

    /// <summary>
    /// Creates a plan that decodes rows as strings. With
    /// <paramref name="wantedColumns"/>, only the selected columns are decoded and
    /// every other column is <see cref="string.Empty"/>, so a catalog scan never
    /// reads the long values of columns it does not use.
    /// </summary>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="strictParsing">Whether malformed values throw instead of decoding to a fallback.</param>
    /// <param name="wantedColumns">The columns to decode, by column index, or <see langword="null"/> for every column.</param>
    internal static RowDecodePlan CreateStrings(TableDef tableDef, bool strictParsing, bool[]? wantedColumns = null)
        => new(tableDef, wantedColumns, columnOrdinals: null, strictParsing);

    internal static RowDecodePlan CreatePartial(TableDef tableDef, int[] columnOrdinals)
    {
        Guard.NotNull(columnOrdinals, nameof(columnOrdinals));

        return new RowDecodePlan(tableDef, wantedColumns: null, columnOrdinals, strictParsing: true);
    }

    internal static ColumnSlice ResolveColumnSliceForDirectDecode(
        JetFormat source,
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
    /// passed to <see cref="ResolveColumnSlice"/> for any column. On Jet3 the
    /// one-byte EOD and offsets get their high part from the row's jump table
    /// (<see cref="Jet3JumpTable"/>), so <see cref="RowLayout.Eod"/> is the
    /// full row-relative offset.
    /// </summary>
    /// <param name="rowFields">Row-trailer field sizes for the format, which say whether rows keep a Jet3 jump table.</param>
    /// <param name="page">Data page containing the row.</param>
    /// <param name="rowStart">Offset of the row within <paramref name="page"/>.</param>
    /// <param name="rowSize">Total size of the row in bytes.</param>
    /// <param name="hasVarColumns">When <see langword="false"/>, the var-length
    /// metadata is not read (no varLen byte, no jump table, no var-offset
    /// table, no EOD marker): Access omits it for tables with zero
    /// variable-length columns, and the writer's unused trailer is skipped.</param>
    /// <param name="layout">Receives the parsed layout on success.</param>
    internal static bool TryParseRowLayout(
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

        int nullMaskSz = JetFormat.GetNullMaskSizeBytes(numCols);
        int nullMaskPos = rowSize - nullMaskSz;
        if (nullMaskPos < rowFields.NumCols)
        {
            return false;
        }

        if (!hasVarColumns)
        {
            layout = new RowLayout(numCols, nullMaskPos, VarLen: 0, VarTableStart: nullMaskPos, Eod: nullMaskPos);
            return true;
        }

        int varLenPos = nullMaskPos - rowFields.VarLen;
        if (varLenPos < rowFields.NumCols)
        {
            return false;
        }

        int varLen = rowFields.ReadVarLen(page, rowStart + varLenPos);
        int jumpEntries = rowFields.HasJumpTable ? Jet3JumpTable.EntryCount(rowSize) : 0;
        int varTableStart = varLenPos - jumpEntries - (varLen * rowFields.VarEntry);
        int eodPos = varTableStart - rowFields.Eod;
        if (eodPos < rowFields.NumCols)
        {
            return false;
        }

        // Every jump entry lies between the EOD byte and var_len, inside the row.
        int usableJumps = Jet3JumpTable.UsableEntryCount(jumpEntries, eodPos);
        int eod = rowFields.ReadEod(page, rowStart + eodPos);
        if (usableJumps != 0)
        {
            eod += Jet3JumpTable.HighPart(page, rowStart, varLenPos, usableJumps, varLen);
        }

        layout = new RowLayout(numCols, nullMaskPos, varLen, varTableStart, eod, varLenPos, usableJumps);
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
        bool nullBit = col.ColNum < layout.NumCols
            && JetFormat.IsNullMaskBitSet(
                page.Slice(rowStart + layout.NullMaskPos, rowSize - layout.NullMaskPos),
                col.ColNum);

        if (col.Type == BooleanType && !col.IsCalculated)
        {
            return new ColumnSlice(ColumnSliceKind.Bool, 0, 0, nullBit);
        }

        if (col.ColNum >= layout.NumCols || !nullBit)
        {
            return new ColumnSlice(ColumnSliceKind.Null, 0, 0, false);
        }

        if (col.Type == BigBinaryType && (!col.IsFixed || col.Size != JetTypeInfo.GetFixedSize(BigBinaryType)))
        {
            return new ColumnSlice(ColumnSliceKind.Empty, 0, 0, false);
        }

        if (col.IsFixed)
        {
            return ResolveFixedColumnSlice(rowFields, page, rowStart, layout, col);
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
        if (layout.JumpCount != 0)
        {
            varOff += Jet3JumpTable.HighPart(page, rowStart, layout.JumpTableEnd, layout.JumpCount, col.VarIdx);
        }

        int varEnd;
        if (col.VarIdx + 1 < layout.VarLen)
        {
            int nextEntry = layout.VarTableStart + ((layout.VarLen - 2 - col.VarIdx) * rowFields.VarEntry);
            varEnd = rowFields.ReadVarEntry(page, rowStart + nextEntry);
            if (layout.JumpCount != 0)
            {
                varEnd += Jet3JumpTable.HighPart(page, rowStart, layout.JumpTableEnd, layout.JumpCount, col.VarIdx + 1);
            }
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

    /// <summary>Resolves a fixed slot's physical bounds independently of its null bit.</summary>
    /// <param name="rowFields">The format's row field sizes.</param>
    /// <param name="page">The containing page.</param>
    /// <param name="rowStart">The row's start offset.</param>
    /// <param name="layout">The parsed row layout.</param>
    /// <param name="col">The fixed column descriptor.</param>
    internal static ColumnSlice ResolveFixedColumnSlice(in RowFieldSizes rowFields, ReadOnlySpan<byte> page, int rowStart, in RowLayout layout, ColumnInfo col)
    {
        int start = rowFields.NumCols + col.FixedOff;
        int sz = col.IsCalculated ? col.Size : JetTypeInfo.GetFixedStorageSize(col.Type, col.Size);
        int payloadEnd = Math.Min(layout.Eod, layout.NullMaskPos);
        if (layout.VarTableStart < layout.NullMaskPos)
        {
            payloadEnd = Math.Min(payloadEnd, layout.VarTableStart - rowFields.Eod);
        }

        if (layout.VarLen > 0)
        {
            int firstEntry = layout.VarTableStart + ((layout.VarLen - 1) * rowFields.VarEntry);
            int firstVariableStart = rowFields.ReadVarEntry(page, rowStart + firstEntry);
            if (layout.JumpCount != 0)
            {
                firstVariableStart += Jet3JumpTable.HighPart(page, rowStart, layout.JumpTableEnd, layout.JumpCount, 0);
            }

            payloadEnd = Math.Min(payloadEnd, firstVariableStart);
        }

        if (sz == 0 || start < rowFields.NumCols || start > payloadEnd || sz > payloadEnd - start)
        {
            return new ColumnSlice(ColumnSliceKind.Empty, 0, 0, false);
        }

        return new ColumnSlice(col.IsCalculated || col.Type is TextType or BinaryType or BigBinaryType ? ColumnSliceKind.Var : ColumnSliceKind.Fixed, start, sz, false);
    }

    private static void ValidateColumnType(ColumnInfo column, long? pageNumber)
    {
        ColumnType valueType = JetTypeInfo.ResolveValueType(column);
        if (JetTypeInfo.GetMetadata(valueType) is null)
        {
            throw new JetCorruptDataException(
                JetErrorCode.CorruptTableDefinition,
                $"Column '{column.Name}' declares unsupported value type {JetTypeInfo.GetTypeDisplayName(valueType)}.",
                new JetErrorInfo { ColumnName = column.Name, PageNumber = pageNumber });
        }
    }

    private static DateTime ReadDatePayload(ReadOnlySpan<byte> payload, ColumnType type)
        => type == DateTimeType
            ? DateTime.FromOADate(JetTypeInfo.ReadDoubleLittleEndian(payload[..8]))
            : JetTypeInfo.ReadDateTimeExtended(payload, 0, payload.Length);

    private static void ValidateComplexReference(byte[] page, int start, int length)
    {
        if (length != JetTypeInfo.GetFixedSize(ComplexType))
        {
            throw new JetCorruptDataException(JetErrorCode.MalformedValue, "A Complex reference slot must contain exactly four bytes.");
        }

        if (JetTypeInfo.Ri32(page, start) <= 0)
        {
            throw new JetCorruptDataException(JetErrorCode.MalformedValue, "An assigned Complex reference must be positive.");
        }
    }

    private static string FormatDatePayload(DateTime date, ColumnType type)
        => date.ToString(type == DateTimeType ? "yyyy-MM-dd HH:mm:ss" : "yyyy-MM-dd HH:mm:ss.fffffff", System.Globalization.CultureInfo.InvariantCulture);

    private object ReadTypedDatePayload(ReadOnlySpan<byte> payload, ColumnType type)
        => this.PreservesStoredValues && type == DateTimeType
            ? NativeDate.FromSerial(JetTypeInfo.ReadDoubleLittleEndian(payload[..8]))
            : ReadDatePayload(payload, type);

    /// <summary>Gets the name of the column at <paramref name="columnIndex"/>, for error messages.</summary>
    /// <param name="columnIndex">The column index.</param>
    internal string GetColumnName(int columnIndex) => this.columns[columnIndex].Name;

    internal object MalformedColumnValue(int columnIndex, Exception exception)
        => this.MalformedVariableValue(this.columns[columnIndex], exception);

    private object MalformedFixedDate(ColumnInfo column, Exception exception)
        => RowValueDecodePolicy.MalformedFixedDate(column, exception, this.strictParsing || this.PreservesStoredValues);

    internal bool TryReadDate(byte[] page, int start, ColumnInfo column, out DateTime value)
    {
        try
        {
            value = column.Type == DateTimeType ? JetTypeInfo.ReadDateTimeLE(page, start) : JetTypeInfo.ReadDateTimeExtendedAt(page, start);
            return true;
        }
        catch (Exception exception) when (RowValueDecodePolicy.IsMalformedValueException(exception))
        {
            _ = column.IsFixed ? this.MalformedFixedDate(column, exception) : this.MalformedVariableValue(column, exception);
            value = default;
            return false;
        }
    }

    private object DecodeFixedDate(byte[] page, int start, ColumnInfo column)
    {
        if (!this.TryReadDate(page, start, column, out DateTime value))
        {
            return DBNull.Value;
        }

        return this.PreservesStoredValues && column.Type == DateTimeType
            ? NativeDate.FromSerial(JetTypeInfo.ReadDoubleLittleEndian(page.AsSpan(start, 8)))
            : value;
    }

    private string DecodeFixedDateString(byte[] page, int start, ColumnInfo column)
    {
        object value = this.DecodeFixedDate(page, start, column);
        return value is DateTime date ? FormatDatePayload(date, column.Type) : string.Empty;
    }

    private object DecodeFixedComplexReference(byte[] page, int start, int length, ColumnInfo column)
    {
        try
        {
            ValidateComplexReference(page, start, length);
            return new ComplexIdRef(JetTypeInfo.Ri32(page, start));
        }
        catch (Exception exception) when (RowValueDecodePolicy.IsMalformedValueException(exception))
        {
            return RowValueDecodePolicy.MalformedFixedValue(column, exception, this.strictParsing || this.PreservesStoredValues);
        }
    }

    private string DecodeFixedComplexReferenceString(byte[] page, int start, int length, ColumnInfo column)
        => this.DecodeFixedComplexReference(page, start, length, column) is ComplexIdRef reference
            ? $"__CX:{reference.Id}__"
            : string.Empty;

    internal void ValidateColumnSlice(ColumnInfo column, ColumnSlice slice)
    {
        if (slice.Kind != ColumnSliceKind.Empty)
        {
            return;
        }

        if (column.IsFixed)
        {
            _ = RowValueDecodePolicy.MalformedFixedValue(
                column,
                new ArgumentException("The assigned fixed slot extends outside the row's fixed payload."),
                this.strictParsing || this.PreservesStoredValues);
            return;
        }

        ColumnType valueType = JetTypeInfo.ResolveValueType(column);
        if (JetTypeInfo.UsesLongValueStorage(column))
        {
            _ = LongValueReadPolicy.MissingValue(
                column.Name,
                new JetCorruptDataException(JetErrorCode.UnreadableLongValue, "The assigned long-value slot has missing or invalid row offsets."),
                this.strictParsing || this.PreservesStoredValues);
            return;
        }

        if (valueType is BooleanType or BinaryType or BigBinaryType or TextType
            || JetTypeInfo.TryGetVariableSlotFixedPayloadSize(valueType, out _))
        {
            _ = this.MalformedVariableValue(column, new ArgumentException("The assigned variable slot has missing or invalid row offsets."));
        }
    }

    internal bool ValidateDirectVariableSlot(ColumnInfo column, ColumnSlice slice)
        => slice.Kind != ColumnSliceKind.Var || RowValueDecodePolicy.HasFixedPayload(column, slice.DataLen, this.strictParsing);

    internal bool TryDecodeDirect<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(
        JetFormat source,
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
        if (LongValueDescriptor.TryRead(page.AsSpan(start, length), out LongValueDescriptor descriptor)
            && descriptor.IsInline)
        {
            int valueLength = descriptor.Length;
            int valueStart = start + Constants.LongValue.HeaderSize;
            longValueDecoder.ValidateStoredLength(valueLength);
            if (valueLength > length - Constants.LongValue.HeaderSize)
            {
                // Resolve the same fault policy as an external long value.
                needsLongValue = true;
                return new LongValueRef(start, length, isOle);
            }

            int inlineLength = valueLength;

            // An OLE value is its stored bytes; OleObjectValue unwraps them on request.
            return isOle
                ? BinaryBuffer.CopySlice(page, valueStart, inlineLength)
                : longValueDecoder.DecodeLongValue(page, valueStart, inlineLength, isOle: false);
        }

        needsLongValue = true;
        return new LongValueRef(start, length, isOle);
    }

    private static bool TryDecodeInlineColumnValue(
        JetFormat source,
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
            if (JetTypeInfo.TryGetVariableSlotFixedPayloadSize(column.Type, out int required))
            {
                if (column.Type is ComplexType || length < required)
                {
                    return false;
                }

                value = JetTypeInfo.ReadFixedTyped(page, start, column, column.Type == NumericType ? length : required, strictNumeric: true);
                return value is not DBNull;
            }

            if (column.Type == TextType)
            {
                value = column.IsFixed ? source.DecodeFixedText(page, start, length) : source.DecodeText(page, start, length, column.IsCompressedUnicode);
                return true;
            }

            if (column.Type is BinaryType or BigBinaryType)
            {
                value = BinaryBuffer.CopySlice(page, start, length);
                return true;
            }

            if (column.Type is BooleanType or OleType or MemoType)
            {
                return false;
            }

            throw new JetNotSupportedException(JetErrorCode.UnknownColumnType, $"Unknown column type: {JetTypeInfo.GetTypeDisplayName(column.Type)}");
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
        JetFormat source,
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

        RowFieldSizes rowFields = source.RowFields;
        string[] result = new string[this.columns.Count];
        for (int columnIndex = 0; columnIndex < this.columns.Count; columnIndex++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (this.wantedColumns?[columnIndex] == false)
            {
                result[columnIndex] = string.Empty;
                continue;
            }

            ColumnInfo column = this.columns[columnIndex];
            ColumnSlice slice = ResolveColumnSlice(rowFields, page, rowStart, rowSize, layout, column);
            this.ValidateColumnSlice(column, slice);
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
        JetFormat source,
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

        RowFieldSizes rowFields = source.RowFields;
        for (int columnIndex = 0; columnIndex < this.columns.Count; columnIndex++)
        {
            if (this.wantedColumns?[columnIndex] == false)
            {
                buffer[columnIndex] = null;
                continue;
            }

            ColumnInfo column = this.columns[columnIndex];
            ColumnSlice slice = ResolveColumnSlice(rowFields, page, rowStart, rowSize, layout, column);
            this.ValidateColumnSlice(column, slice);
            buffer[columnIndex] = this.DecodeTypedValue(source, page, rowStart, slice, column, longValueDecoder, ref needsLongValue);
        }

        return true;
    }

    internal bool TryDecodePartialColumns(JetFormat source, byte[] page, int rowStart, int rowSize, object?[] result)
    {
        if (this.columnOrdinals == null || result.Length < this.columnOrdinals.Length)
        {
            throw new JetOperationException(JetErrorCode.InvalidOperationState, "Partial row decoding requires a partial-column plan and a result buffer large enough for every ordinal.");
        }

        if (!this.TryParseLayout(source, page, rowStart, rowSize, out RowLayout layout))
        {
            return false;
        }

        RowFieldSizes rowFields = source.RowFields;
        for (int resultIndex = 0; resultIndex < this.columnOrdinals.Length; resultIndex++)
        {
            int columnOrdinal = this.columnOrdinals[resultIndex];
            if (columnOrdinal < 0 || columnOrdinal >= this.columns.Count)
            {
                return false;
            }

            ColumnInfo column = this.columns[columnOrdinal];
            ColumnSlice slice = ResolveColumnSlice(rowFields, page, rowStart, rowSize, layout, column);
            this.ValidateColumnSlice(column, slice);
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

    /// <summary>
    /// Returns whether the row's trailer parses under this plan. Every row
    /// decoder (typed, string and direct) skips exactly the rows for which
    /// this returns <see langword="false"/>, so a row count that uses it
    /// agrees with what the table reads return.
    /// </summary>
    /// <param name="source">The database the page belongs to.</param>
    /// <param name="page">The data page.</param>
    /// <param name="rowStart">The row start.</param>
    /// <param name="rowSize">The row size.</param>
    internal bool CanDecodeRow(JetFormat source, byte[] page, int rowStart, int rowSize)
        => this.TryParseLayout(source, page, rowStart, rowSize, out _);

    internal bool TryParseLayoutForDirectDecode(
        JetFormat source,
        byte[] page,
        int rowStart,
        int rowSize,
        out RowLayout layout)
        => this.TryParseLayout(source, page, rowStart, rowSize, out layout);

    private bool TryParseLayout(
        JetFormat source,
        byte[] page,
        int rowStart,
        int rowSize,
        out RowLayout layout)
    {
        layout = default;
        if (this.hasBigBinaryColumns && !source.SupportsBigBinary)
        {
            return false;
        }

        if (rowSize < source.RowFields.NumCols)
        {
            return false;
        }

        int rawNumCols = source.ReadRowColumnCount(page, rowStart);
        if (rawNumCols == 0)
        {
            return false;
        }

        bool effectiveHasVarColumns = this.hasVarColumns || (this.hasDeletedColumns && rawNumCols > this.columns.Count);
        return TryParseRowLayout(source.RowFields, page, rowStart, rowSize, effectiveHasVarColumns, out layout);
    }

    private async ValueTask<string> DecodeStringValueAsync(
        JetFormat source,
        byte[] page,
        int rowStart,
        ColumnSlice slice,
        ColumnInfo column,
        LongValueDecoder longValueDecoder,
        CancellationToken cancellationToken) => slice.Kind switch
        {
            ColumnSliceKind.Bool => slice.BoolValue ? "True" : "False",
            ColumnSliceKind.Empty when JetTypeInfo.UsesLongValueStorage(column) => null!,
            ColumnSliceKind.Null or ColumnSliceKind.Empty => string.Empty,
            ColumnSliceKind.Fixed when column.Type is DateTimeType or DateTimeExtendedType => this.DecodeFixedDateString(page, rowStart + slice.DataStart, column),
            ColumnSliceKind.Fixed when column.Type == ComplexType => this.DecodeFixedComplexReferenceString(page, rowStart + slice.DataStart, slice.DataLen, column),
            ColumnSliceKind.Fixed => JetTypeInfo.ReadFixedString(page, rowStart + slice.DataStart, column, slice.DataLen, this.strictParsing),
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
        JetFormat source,
        byte[] page,
        int start,
        int length,
        ColumnInfo column,
        LongValueDecoder longValueDecoder,
        CancellationToken cancellationToken)
    {
        if (length <= 0)
        {
            _ = this.DecodeEmptyVariableValue(column, length);
            return column.IsCalculated && JetTypeInfo.UsesLongValueStorage(column) ? null! : string.Empty;
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
            if (column.Type == ComplexType)
            {
                ValidateComplexReference(page, start, length);
            }

            if (column.Type is DateTimeType or DateTimeExtendedType)
            {
                return RowValueDecodePolicy.HasFixedPayload(column, length, this.strictParsing)
                    ? FormatDatePayload(ReadDatePayload(page.AsSpan(start, length), column.Type), column.Type)
                    : string.Empty;
            }

            if (JetTypeInfo.TryGetVariableSlotFixedPayloadSize(column.Type, out int required))
            {
                return RowValueDecodePolicy.HasFixedPayload(column, length, this.strictParsing)
                    ? JetTypeInfo.ReadFixedString(page, start, column, required, this.strictParsing)
                    : string.Empty;
            }

            if (column.Type == TextType)
            {
                return column.IsFixed ? source.DecodeFixedText(page, start, length) : source.DecodeText(page, start, length, column.IsCompressedUnicode);
            }

            if (column.Type is BinaryType or BigBinaryType)
            {
                return JetTypeInfo.ToHexStringNoSeparator(page.AsSpan(start, length));
            }

            if (column.Type is MemoType or OleType)
            {
                return await longValueDecoder.ReadLongValueAsync(page, start, length, column.Type == OleType, cancellationToken).ConfigureAwait(false);
            }

            if (column.Type == BooleanType)
            {
                return string.Empty;
            }

            throw new JetNotSupportedException(JetErrorCode.UnknownColumnType, $"Column '{column.Name}' has unknown type {JetTypeInfo.GetTypeDisplayName(column.Type)}.", new JetErrorInfo { ColumnName = column.Name });
        }
        catch (JetLimitationException)
        {
            throw;
        }
        catch (JetCorruptDataException exception) when (exception.ErrorCode != JetErrorCode.MalformedValue && JetTypeInfo.UsesLongValueStorage(column))
        {
            _ = LongValueReadPolicy.MissingValue(column.Name, exception, this.strictParsing);
            return null!;
        }
        catch (Exception exception) when (RowValueDecodePolicy.IsMalformedValueException(exception))
        {
            _ = RowValueDecodePolicy.MalformedVariableValue(column, exception, this.strictParsing);
            return string.Empty;
        }
    }

    private async ValueTask<string> DecodeCalculatedStringVariableValueAsync(
        JetFormat source,
        byte[] page,
        int start,
        int length,
        ColumnInfo column,
        LongValueDecoder longValueDecoder,
        CancellationToken cancellationToken)
    {
        try
        {
            if (!JetTypeInfo.UsesLongValueStorage(column))
            {
                RowValueDecodePolicy.ValidateCalculatedEnvelope(page.AsSpan(start, length));
                if (CalculatedColumnUtil.ReadError(page.AsSpan(start, length)) is { } error)
                {
                    return error.ToString();
                }
            }

            // Access stores the cached value by the column's ResultType, which can
            // differ from the descriptor type (a Memo result in a Text descriptor
            // keeps a long-value header in the row), so the codec follows it.
            ColumnType valueType = JetTypeInfo.ResolveValueType(column);
            if (valueType == TextType)
            {
                byte[] textPayload = RowValueDecodePolicy.UnwrapCalculatedPayload(page.AsSpan(start, length));
                return source.DecodeCalculatedText(textPayload, 0, textPayload.Length);
            }

            if (JetTypeInfo.UsesLongValueStorage(column))
            {
                byte[] raw = await longValueDecoder.ReadLongValueRawBytesAsync(page, start, length, cancellationToken).ConfigureAwait(false);
                byte[] payload = RowValueDecodePolicy.UnwrapCalculatedPayload(raw);
                if (CalculatedColumnUtil.ReadError(raw) is { } error)
                {
                    return error.ToString();
                }

                if (valueType == BinaryType)
                {
                    return JetTypeInfo.ToHexStringNoSeparator(payload);
                }

                return longValueDecoder.DecodeLongValue(payload, 0, payload.Length, valueType == OleType, compressedStorage: false);
            }

            if (valueType == BooleanType || valueType == NumericType || JetTypeInfo.TryGetVariableSlotFixedPayloadSize(valueType, out _))
            {
                byte[] payload = RowValueDecodePolicy.UnwrapCalculatedPayload(page.AsSpan(start, length));
                if (!RowValueDecodePolicy.HasCalculatedPayload(payload, valueType))
                {
                    return string.Empty;
                }

                if (valueType is DateTimeType or DateTimeExtendedType)
                {
                    return FormatDatePayload(ReadDatePayload(payload, valueType), valueType);
                }

                return CalculatedColumnUtil.ReadPayloadString(
                    payload,
                    valueType,
                    this.strictParsing || this.PreservesStoredValues);
            }

            throw new JetNotSupportedException(JetErrorCode.UnknownColumnType, $"Calculated column of type {JetTypeInfo.GetTypeDisplayName(column.Type)} is unknown.");
        }
        catch (JetLimitationException)
        {
            throw;
        }
        catch (JetCorruptDataException exception) when (exception.ErrorCode != JetErrorCode.MalformedValue && JetTypeInfo.UsesLongValueStorage(column))
        {
            _ = LongValueReadPolicy.MissingValue(column.Name, exception, this.strictParsing);
            return null!;
        }
        catch (Exception exception) when (RowValueDecodePolicy.IsMalformedValueException(exception))
        {
            _ = RowValueDecodePolicy.MalformedVariableValue(column, exception, this.strictParsing);
            return JetTypeInfo.UsesLongValueStorage(column) ? null! : string.Empty;
        }
    }

    private object? DecodeTypedValue(
        JetFormat source,
        byte[] page,
        int rowStart,
        ColumnSlice slice,
        ColumnInfo column,
        LongValueDecoder longValueDecoder,
        ref bool needsLongValue) => slice.Kind switch
        {
            ColumnSliceKind.Bool => BoxCache.Bool(slice.BoolValue),
            ColumnSliceKind.Empty when this.PreservesStoredValues && column.Type == BigBinaryType => throw new JetCorruptDataException(
                JetErrorCode.MalformedValue,
                $"Column '{column.Name}' has an unreadable BIGBINARY payload and cannot be written back.",
                new JetErrorInfo
                {
                    ColumnName = column.Name,
                    Reason = "The native fixed BIGBINARY slot is malformed or truncated.",
                }),
            ColumnSliceKind.Null or ColumnSliceKind.Empty => DBNull.Value,
            ColumnSliceKind.Fixed when column.Type is DateTimeType or DateTimeExtendedType => this.DecodeFixedDate(page, rowStart + slice.DataStart, column),
            ColumnSliceKind.Fixed when column.Type == ComplexType => this.DecodeFixedComplexReference(page, rowStart + slice.DataStart, slice.DataLen, column),
            ColumnSliceKind.Fixed => JetTypeInfo.ReadFixedTyped(page, rowStart + slice.DataStart, column, slice.DataLen, this.strictParsing),
            ColumnSliceKind.Var => this.DecodeTypedVariableValue(source, page, rowStart + slice.DataStart, slice.DataLen, column, longValueDecoder, ref needsLongValue),
            _ => DBNull.Value,
        };

    private object? DecodeTypedVariableValue(
        JetFormat source,
        byte[] page,
        int start,
        int length,
        ColumnInfo column,
        LongValueDecoder longValueDecoder,
        ref bool needsLongValue)
    {
        if (length <= 0)
        {
            return this.DecodeEmptyVariableValue(column, length);
        }

        if (column.IsCalculated)
        {
            return this.DecodeCalculatedTypedVariableValue(source, page, start, length, column, ref needsLongValue);
        }

        try
        {
            if (column.Type == ComplexType)
            {
                ValidateComplexReference(page, start, length);
            }

            if (column.Type is DateTimeType or DateTimeExtendedType)
            {
                return RowValueDecodePolicy.HasFixedPayload(column, length, this.strictParsing || this.PreservesStoredValues)
                    ? this.ReadTypedDatePayload(page.AsSpan(start, length), column.Type)
                    : DBNull.Value;
            }

            if (JetTypeInfo.TryGetVariableSlotFixedPayloadSize(column.Type, out int required))
            {
                return RowValueDecodePolicy.HasFixedPayload(column, length, this.strictParsing || this.PreservesStoredValues)
                    ? JetTypeInfo.ReadFixedTyped(page, start, column, required, this.strictParsing || this.PreservesStoredValues)
                    : DBNull.Value;
            }

            if (column.Type == TextType)
            {
                return column.IsFixed ? source.DecodeFixedText(page, start, length) : source.DecodeText(page, start, length, column.IsCompressedUnicode);
            }

            if (column.Type is BinaryType or BigBinaryType)
            {
                return BinaryBuffer.CopySlice(page, start, length);
            }

            if (column.Type is MemoType or OleType)
            {
                return DecodeLongVariableValue(page, start, length, column, longValueDecoder, ref needsLongValue);
            }

            if (column.Type == BooleanType)
            {
                return DBNull.Value;
            }

            throw new JetNotSupportedException(JetErrorCode.UnknownColumnType, $"Column '{column.Name}' has unknown type {JetTypeInfo.GetTypeDisplayName(column.Type)}.", new JetErrorInfo { ColumnName = column.Name });
        }
        catch (JetLimitationException)
        {
            throw;
        }
        catch (Exception exception) when (RowValueDecodePolicy.IsMalformedValueException(exception))
        {
            return this.MalformedVariableValue(column, exception);
        }
    }

    private object DecodeEmptyVariableValue(ColumnInfo column, int length)
    {
        if (column.IsCalculated)
        {
            return this.MalformedVariableValue(column, new ArgumentException("Calculated value envelope is truncated."));
        }

        _ = RowValueDecodePolicy.HasFixedPayload(column, length, this.strictParsing || this.PreservesStoredValues);
        return RowValueDecodePolicy.EmptyVariableValue(column);
    }

    private object MalformedVariableValue(ColumnInfo column, Exception exception)
    {
        // A write-back snapshot must not turn an unreadable MEMO / OLE value
        // into DBNull, which an update or schema rewrite would then store.
        if (this.PreservesStoredValues && JetTypeInfo.UsesLongValueStorage(column))
        {
            return new UnreadableLongValue(column.Name, exception.Message);
        }

        return RowValueDecodePolicy.MalformedVariableValue(column, exception, this.strictParsing || this.PreservesStoredValues);
    }

    private object? DecodeCalculatedTypedVariableValue(
        JetFormat source,
        byte[] page,
        int start,
        int length,
        ColumnInfo column,
        ref bool needsLongValue)
    {
        try
        {
            if (!JetTypeInfo.UsesLongValueStorage(column))
            {
                RowValueDecodePolicy.ValidateCalculatedEnvelope(page.AsSpan(start, length));
                if (CalculatedColumnUtil.ReadError(page.AsSpan(start, length)) is { } error)
                {
                    return error;
                }
            }

            // The cached value is encoded by the ResultType, not the descriptor
            // type; see DecodeCalculatedStringVariableValueAsync.
            ColumnType valueType = JetTypeInfo.ResolveValueType(column);
            if (valueType == TextType)
            {
                byte[] textPayload = RowValueDecodePolicy.UnwrapCalculatedPayload(page.AsSpan(start, length));
                string text = source.DecodeCalculatedText(textPayload, 0, textPayload.Length);
                return text.Length == 0 ? DBNull.Value : text;
            }

            if (JetTypeInfo.UsesLongValueStorage(column))
            {
                needsLongValue = true;
                return new CalculatedLongValueRef(start, length, valueType != MemoType);
            }

            if (valueType == BooleanType || valueType == NumericType || JetTypeInfo.TryGetVariableSlotFixedPayloadSize(valueType, out _))
            {
                byte[] payload = RowValueDecodePolicy.UnwrapCalculatedPayload(page.AsSpan(start, length));
                if (!RowValueDecodePolicy.HasCalculatedPayload(payload, valueType))
                {
                    return DBNull.Value;
                }

                if (valueType is DateTimeType or DateTimeExtendedType)
                {
                    return this.ReadTypedDatePayload(payload, valueType);
                }

                return CalculatedColumnUtil.ReadPayloadTyped(
                    payload,
                    valueType,
                    this.strictParsing || this.PreservesStoredValues);
            }

            throw new JetNotSupportedException(JetErrorCode.UnknownColumnType, $"Calculated column of type {JetTypeInfo.GetTypeDisplayName(column.Type)} is unknown.");
        }
        catch (JetLimitationException)
        {
            throw;
        }
        catch (Exception exception) when (RowValueDecodePolicy.IsMalformedValueException(exception))
        {
            return this.MalformedVariableValue(column, exception);
        }
    }
}
