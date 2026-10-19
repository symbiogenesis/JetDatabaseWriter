namespace JetDatabaseWriter.Maintenance;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;

/// <summary>Identifies physical field slots without value conversion, envelope interpretation or recalculation.</summary>
internal static class CompactRawFieldReader
{
    /// <summary>Validates one bounded stored row and visits its defined fields as borrowed raw slots.</summary>
    /// <param name="format">The verified source format.</param>
    /// <param name="definition">The source definition whose descriptor identities must remain unchanged.</param>
    /// <param name="location">The row's original header bookmark and resolved payload location.</param>
    /// <param name="row">The complete borrowed row payload, starting at offset zero.</param>
    /// <param name="visitor">Consumes each field before returning, without retaining borrowed memory.</param>
    /// <param name="cancellationToken">Cancels geometry checks and delivery between fields.</param>
    /// <param name="hydratedColumns">Optional column projections carrying bounded catalog ResultType metadata; all storage coordinates remain original.</param>
    /// <returns>A task completing after every field visitor has returned.</returns>
    /// <remarks>
    /// The existing native row slicer defines fixed widths and Jet3 jump-table geometry. All variable
    /// offsets are checked, including undefined and unassigned slots. Null slots retain their physical
    /// bytes. The complete source row remains necessary to preserve deleted fields, gaps and padding.
    /// This helper neither establishes the definition's ownership nor validates native value semantics.
    /// CPU is bounded by one physical page and the schema column limit; it allocates no field collection.
    /// </remarks>
    internal static async ValueTask ScanAsync(
        JetFormat format,
        TDefImage definition,
        RowLocation location,
        ReadOnlyMemory<byte> row,
        Func<CompactRawField, CancellationToken, ValueTask> visitor,
        CancellationToken cancellationToken,
        IReadOnlyList<ColumnInfo>? hydratedColumns = null)
    {
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(definition, nameof(definition));
        Guard.NotNull(visitor, nameof(visitor));
        cancellationToken.ThrowIfCancellationRequested();
        RowLayout layout = ValidateLayout(format, definition, location, row.Span, cancellationToken);
        foreach (ColumnInfo column in hydratedColumns ?? definition.Columns)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CompactRawField field = Resolve(format, column, location, row, layout);
            await visitor(field, cancellationToken).ConfigureAwait(false);
        }

        cancellationToken.ThrowIfCancellationRequested();
    }

    private static RowLayout ValidateLayout(JetFormat format, TDefImage definition, RowLocation location, ReadOnlySpan<byte> row, CancellationToken cancellationToken)
    {
        if (row.Length != location.RowSize || row.Length < format.RowFields.NumCols || row.Length > format.PageSize
            || location.RowStart < 0 || location.RowStart > format.PageSize - row.Length
            || definition.Columns.Count > Constants.TableDefinition.MaxColumns || definition.Header.ColumnCount != definition.Columns.Count)
        {
            throw Corrupt(location, "The raw field row or definition geometry is invalid.");
        }

        bool hasVariable = definition.Header.VariableColumnCount > 0;
        int previousColumn = -1;
        foreach (ColumnInfo column in definition.Columns)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (column.ColNum <= previousColumn || column.ColNum > ushort.MaxValue || column.FixedOff < 0 || column.Size < 0
                || (!column.IsFixed && (column.VarIdx < 0 || column.VarIdx >= Constants.TableDefinition.MaxColumns)))
            {
                throw Corrupt(location, "The source field descriptors have invalid or ambiguous coordinates.");
            }

            previousColumn = column.ColNum;
            hasVariable |= !column.IsFixed;
        }

        if (!RowDecodePlan.TryParseRowLayout(format.RowFields, row, 0, row.Length, hasVariable, out RowLayout layout)
            || layout.NumCols > Constants.TableDefinition.MaxColumns || layout.VarLen > Constants.TableDefinition.MaxColumns)
        {
            throw Corrupt(location, "The stored row trailer cannot be identified faithfully.");
        }

        if (hasVariable)
        {
            int payloadLimit = layout.VarTableStart - format.RowFields.Eod;
            if (layout.Eod < format.RowFields.NumCols || layout.Eod > payloadLimit)
            {
                throw Corrupt(location, "The stored row EOD extends outside its payload region.");
            }

            int previousJump = 0;
            for (int index = 0; index < layout.JumpCount; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int jump = row[layout.JumpTableEnd - 1 - index];
                if (jump < previousJump || jump > layout.VarLen)
                {
                    throw Corrupt(location, "The stored Jet3 jump table has invalid boundary indexes.");
                }

                previousJump = jump;
            }

            int previousOffset = format.RowFields.NumCols;
            for (int index = 0; index < layout.VarLen; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int offset = VariableOffset(format, row, layout, index);
                if (offset < previousOffset || offset > layout.Eod)
                {
                    throw Corrupt(location, "The stored variable offsets decrease or extend outside the payload region.");
                }

                previousOffset = offset;
            }
        }

        // Refuse damaged assigned or unassigned slots before delivering any field.
        foreach (ColumnInfo column in definition.Columns)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _ = ResolveSlice(format, column, location, row, layout);
        }

        return layout;
    }

    private static CompactRawField Resolve(JetFormat format, ColumnInfo column, RowLocation location, ReadOnlyMemory<byte> row, RowLayout layout)
    {
        ColumnSlice slice = ResolveSlice(format, column, location, row.Span, layout);
        if (column.ColNum >= layout.NumCols)
        {
            return new(column, location, CompactRawFieldKind.Absent, 0, default, false);
        }

        bool assigned = JetFormat.IsNullMaskBitSet(row.Span[layout.NullMaskPos..], column.ColNum);
        if (column.Type == ColumnType.BooleanType && !column.IsCalculated)
        {
            return new(column, location, CompactRawFieldKind.Boolean, 0, default, assigned);
        }

        return new(column, location, assigned ? CompactRawFieldKind.Stored : CompactRawFieldKind.Null, slice.DataStart, row.Slice(slice.DataStart, slice.DataLen), false);
    }

    private static ColumnSlice ResolveSlice(JetFormat format, ColumnInfo column, RowLocation location, ReadOnlySpan<byte> row, RowLayout layout)
    {
        if (column.ColNum >= layout.NumCols || (column.Type == ColumnType.BooleanType && !column.IsCalculated))
        {
            return default;
        }

        if (column.IsFixed)
        {
            ColumnSlice slice = RowDecodePlan.ResolveFixedColumnSlice(format.RowFields, row, 0, layout, column);
            if (slice.Kind == ColumnSliceKind.Empty)
            {
                throw Corrupt(location, $"The fixed slot for column '{column.Name}' extends outside the stored payload.");
            }

            return slice;
        }

        if (column.VarIdx >= layout.VarLen)
        {
            if (JetFormat.IsNullMaskBitSet(row[layout.NullMaskPos..], column.ColNum))
            {
                throw Corrupt(location, $"The assigned variable slot for column '{column.Name}' is missing.");
            }

            return default;
        }

        int start = VariableOffset(format, row, layout, column.VarIdx);
        int end = column.VarIdx + 1 == layout.VarLen ? layout.Eod : VariableOffset(format, row, layout, column.VarIdx + 1);
        return new(ColumnSliceKind.Var, start, end - start, false);
    }

    private static int VariableOffset(JetFormat format, ReadOnlySpan<byte> row, RowLayout layout, int index)
    {
        int entry = layout.VarTableStart + ((layout.VarLen - 1 - index) * format.RowFields.VarEntry);
        int offset = format.RowFields.ReadVarEntry(row, entry);
        return offset + Jet3JumpTable.HighPart(row, 0, layout.JumpTableEnd, layout.JumpCount, index);
    }

    private static JetCorruptDataException Corrupt(RowLocation location, string reason)
        => new(JetErrorCode.MalformedValue, reason, new JetErrorInfo { PageNumber = location.DataPageNumber, Reason = reason });
}
