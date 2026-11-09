namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tables;
using JetDatabaseWriter.ValueDecoding;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Extracts seekable predicates from LINQ expressions and selects optimal B-tree index queries on <see cref="AccessReader"/> and <see cref="AccessWriter"/>.
/// </summary>
internal static class JetIndexSeekPlanner
{
    /// <summary>
    /// Extracts index-seekable conditions from <paramref name="predicate"/> for <paramref name="entityType"/>.
    /// </summary>
    /// <param name="entityType">The target entity type.</param>
    /// <param name="predicate">The filter lambda expression.</param>
    /// <returns>A list of extracted seek conditions.</returns>
    internal static List<JetIndexSeekCondition> ExtractSeekConditions(IEntityType entityType, LambdaExpression predicate)
    {
        ArgumentNullException.ThrowIfNull(entityType);
        ArgumentNullException.ThrowIfNull(predicate);

        List<JetIndexSeekCondition> conditions = [];
        if (predicate.Parameters.Count != 1)
        {
            return conditions;
        }

        ParameterExpression entityParam = predicate.Parameters[0];
        ExtractConjunctions(entityType, entityParam, predicate.Body, conditions);
        return conditions;
    }

    /// <summary>
    /// Attempts to construct a B-tree index query on <paramref name="reader"/> when <paramref name="conditions"/> match a seekable index on <paramref name="resolved"/>.
    /// </summary>
    /// <param name="reader">The open <see cref="AccessReader"/>.</param>
    /// <param name="resolved">The resolved local table.</param>
    /// <param name="conditions">The extracted seek conditions.</param>
    /// <param name="parameters">The runtime query parameter values.</param>
    /// <param name="wantedColumns">Optional column projection mask.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>An index-backed row sequence, or <see langword="null"/> if a full table scan is required.</returns>
    internal static async ValueTask<IAsyncEnumerable<object[]>?> TryBuildIndexQueryAsync(
        AccessReader reader,
        ResolvedTable resolved,
        IReadOnlyList<JetIndexSeekCondition> conditions,
        IReadOnlyDictionary<string, object?> parameters,
        bool[]? wantedColumns,
        CancellationToken cancellationToken)
    {
        if (conditions.Count == 0 || !reader.Services.Indexes.CanSeek)
        {
            return null;
        }

        RowCriteria seekable = BuildSeekableCriteria(conditions, parameters, resolved.Definition);
        if (seekable.Count > 0)
        {
            IReadOnlyList<IndexMetadata> indexes = await reader.Services.Indexes.ListSeekableIndexesAsync(resolved, cancellationToken).ConfigureAwait(false);
            if (indexes.Count > 0)
            {
                string tableName = resolved.Entry.Name;
                JetFormat format = reader.Database.Format;
                IndexPlan? plan = IndexPlanner.TryPlan(
                    indexes,
                    seekable,
                    candidate => CanEncodePlan(format, resolved.Definition, tableName, candidate));
                if (plan is not null)
                {
                    return reader.Services.Indexes.ReadIndexRowsAsObjectsAsync(
                        tableName,
                        plan.Index.Name,
                        plan.Criteria,
                        wantedColumns,
                        cancellationToken);
                }
            }
        }

        foreach (JetIndexSeekCondition condition in conditions)
        {
            if (condition.Operator != JetIndexSeekOperator.In)
            {
                continue;
            }

            if (condition.ValueEvaluator(parameters) is not IReadOnlyList<object?[]> inKeyTuples)
            {
                continue;
            }

            if (inKeyTuples.Count == 0)
            {
                return YieldRowsAsync([]);
            }

            List<object[]>? multiKeyRows = await TryReadMultiKeyIndexRowsAsync(
                reader,
                resolved,
                [condition.Property],
                inKeyTuples,
                wantedColumns,
                cancellationToken).ConfigureAwait(false);
            if (multiKeyRows is not null)
            {
                return YieldRowsAsync(multiKeyRows);
            }
        }

        return null;
    }

    /// <summary>
    /// Attempts to construct an ordered full-index scan on <paramref name="reader"/> when <paramref name="orderedAscendingProperties"/>
    /// match the leading ascending columns of a B-tree index whose sort order matches CLR comparison.
    /// </summary>
    /// <param name="reader">The open <see cref="AccessReader"/>.</param>
    /// <param name="resolved">The resolved local table.</param>
    /// <param name="orderedAscendingProperties">The required ascending sort properties.</param>
    /// <param name="wantedColumns">Optional column projection mask.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>An index-ordered row sequence, or <see langword="null"/> if no suitable index exists.</returns>
    internal static async ValueTask<IAsyncEnumerable<object[]>?> TryBuildOrderedIndexQueryAsync(
        AccessReader reader,
        ResolvedTable resolved,
        IReadOnlyList<IProperty> orderedAscendingProperties,
        bool[]? wantedColumns,
        CancellationToken cancellationToken)
    {
        if (orderedAscendingProperties.Count == 0 || !reader.Services.Indexes.CanSeek)
        {
            return null;
        }

        IReadOnlyList<IndexMetadata> indexes = await reader.Services.Indexes.ListSeekableIndexesAsync(resolved, cancellationToken).ConfigureAwait(false);
        IndexMetadata? chosen = SelectOrderedScanIndex(reader.Database.Format, resolved.Definition, indexes, orderedAscendingProperties);
        if (chosen is null)
        {
            return null;
        }

        return reader.Services.Indexes.ReadIndexRowsAsObjectsAsync(
            resolved.Entry.Name,
            chosen.Name,
            IndexQueryCriteria.All,
            wantedColumns,
            cancellationToken);
    }

    /// <summary>
    /// Attempts to execute a B-tree index seek or ordered scan against <paramref name="writer"/>'s transaction-aware snapshot pages.
    /// </summary>
    /// <param name="writer">The active <see cref="AccessWriter"/>.</param>
    /// <param name="resolved">The resolved table.</param>
    /// <param name="conditions">Optional extracted seek conditions.</param>
    /// <param name="orderedAscendingProperties">Optional ascending sort properties when no seek conditions are present.</param>
    /// <param name="parameters">The runtime query parameter values.</param>
    /// <param name="wantedColumns">The column projection mask.</param>
    /// <param name="visitRow">Callback invoked for each decoded row along with whether the scan is ordered by <paramref name="orderedAscendingProperties"/>; returning <see langword="false"/> stops the scan.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns><see langword="true"/> if an index scan was executed; <see langword="false"/> if a full table scan is required.</returns>
    internal static async ValueTask<bool> TryReadFromActiveWriterIndexAsync(
        AccessWriter writer,
        ResolvedTable resolved,
        IReadOnlyList<JetIndexSeekCondition>? conditions,
        IReadOnlyList<IProperty>? orderedAscendingProperties,
        IReadOnlyDictionary<string, object?> parameters,
        bool[] wantedColumns,
        Func<object?[], bool, bool> visitRow,
        CancellationToken cancellationToken)
    {
        JetFormat format = writer.Database.Format;
        if (!format.SupportsIndexSeeks)
        {
            return false;
        }

        TableDef tableDef = resolved.Definition;
        string tableName = resolved.Entry.Name;
        IReadOnlyList<IndexMetadata> allIndexes = await writer.Services.Snapshots.ReadIndexMetadataSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);
        List<IndexMetadata> indexes = FilterSeekableIndexes(tableDef, allIndexes);
        if (indexes.Count == 0)
        {
            return false;
        }

        if (conditions is { Count: > 0 })
        {
            RowCriteria seekable = BuildSeekableCriteria(conditions, parameters, tableDef);
            if (seekable.Count > 0)
            {
                IndexPlan? plan = IndexPlanner.TryPlan(
                    indexes,
                    seekable,
                    candidate => CanEncodePlan(format, tableDef, tableName, candidate));
                if (plan?.Index is { FirstDp: > 0, Columns.Count: > 0 })
                {
                    await EnumerateActiveWriterIndexCriteriaAsync(
                        writer,
                        resolved,
                        plan.Index,
                        plan.Criteria,
                        wantedColumns,
                        row => visitRow(row, false),
                        cancellationToken).ConfigureAwait(false);
                    return true;
                }
            }

            foreach (JetIndexSeekCondition condition in conditions)
            {
                if (condition.Operator == JetIndexSeekOperator.In
                    && condition.ValueEvaluator(parameters) is IReadOnlyList<object?[]> inKeyTuples)
                {
                    if (inKeyTuples.Count == 0)
                    {
                        return true;
                    }

                    bool multiKeyUsed = await TryReadMultiKeyFromActiveWriterIndexAsync(
                        writer,
                        resolved,
                        indexes,
                        [condition.Property],
                        inKeyTuples,
                        wantedColumns,
                        row => visitRow(row, false),
                        cancellationToken).ConfigureAwait(false);
                    if (multiKeyUsed)
                    {
                        return true;
                    }
                }
            }
        }

        if (orderedAscendingProperties is { Count: > 0 })
        {
            IndexMetadata? orderedIndex = SelectOrderedScanIndex(format, tableDef, indexes, orderedAscendingProperties);
            if (orderedIndex is not null)
            {
                await EnumerateActiveWriterIndexCriteriaAsync(
                    writer,
                    resolved,
                    orderedIndex,
                    IndexQueryCriteria.All,
                    wantedColumns,
                    row => visitRow(row, true),
                    cancellationToken).ConfigureAwait(false);
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Attempts to read rows matching any of <paramref name="keyTuples"/> from <paramref name="writer"/> using a B-tree index on <paramref name="resolved"/>.
    /// </summary>
    /// <param name="writer">The active <see cref="AccessWriter"/>.</param>
    /// <param name="resolved">The resolved table.</param>
    /// <param name="indexes">Optional preloaded seekable indexes.</param>
    /// <param name="keyProperties">The key properties corresponding to each tuple element.</param>
    /// <param name="keyTuples">The distinct model key tuples to seek.</param>
    /// <param name="wantedColumns">The column projection mask.</param>
    /// <param name="visitRow">Callback invoked for each decoded row; returning <see langword="false"/> stops the scan.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns><see langword="true"/> if a covering seekable index was used; otherwise, <see langword="false"/>.</returns>
    internal static async ValueTask<bool> TryReadMultiKeyFromActiveWriterIndexAsync(
        AccessWriter writer,
        ResolvedTable resolved,
        IReadOnlyList<IndexMetadata>? indexes,
        IReadOnlyList<IProperty> keyProperties,
        IReadOnlyList<object?[]> keyTuples,
        bool[] wantedColumns,
        Func<object?[], bool> visitRow,
        CancellationToken cancellationToken)
    {
        JetFormat format = writer.Database.Format;
        if (keyProperties.Count == 0 || keyTuples.Count == 0 || !format.SupportsIndexSeeks)
        {
            return false;
        }

        TableDef tableDef = resolved.Definition;
        if (!TryPrepareMultiKeyTuples(tableDef, keyProperties, keyTuples, out string[]? keyColumnNames, out List<object?[]>? providerTuples))
        {
            return false;
        }

        string tableName = resolved.Entry.Name;
        if (indexes is null)
        {
            IReadOnlyList<IndexMetadata> allIndexes = await writer.Services.Snapshots.ReadIndexMetadataSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);
            indexes = FilterSeekableIndexes(tableDef, allIndexes);
        }

        IndexMetadata? chosenIndex = SelectPrefixMatchingIndex(indexes, keyColumnNames!);
        if (chosenIndex is null)
        {
            return false;
        }

        if (!CanEncodeMultiKeyTuples(format, tableDef, tableName, chosenIndex, providerTuples!))
        {
            return false;
        }

        bool isExact = chosenIndex.Columns.Count == keyColumnNames!.Length;
        foreach (object?[] tuple in providerTuples!)
        {
            IndexQueryCriteria criteria = isExact
                ? IndexQueryCriteria.Exact(tuple)
                : IndexQueryCriteria.KeyPrefix(tuple);
            bool keepGoing = await EnumerateActiveWriterIndexCriteriaAsync(
                writer,
                resolved,
                chosenIndex,
                criteria,
                wantedColumns,
                visitRow,
                cancellationToken).ConfigureAwait(false);
            if (!keepGoing)
            {
                break;
            }
        }

        return true;
    }

    /// <summary>
    /// Attempts to read rows matching any of <paramref name="keyTuples"/> using a B-tree index on <paramref name="resolved"/>.
    /// </summary>
    /// <param name="reader">The open <see cref="AccessReader"/>.</param>
    /// <param name="resolved">The resolved local table.</param>
    /// <param name="keyProperties">The key properties corresponding to each tuple element.</param>
    /// <param name="keyTuples">The distinct provider-converted key tuples to seek.</param>
    /// <param name="wantedColumns">Optional column projection mask.</param>
    /// <param name="cancellationToken">A token to observe while waiting for the task to complete.</param>
    /// <returns>The matched rows, or <see langword="null"/> if no covering seekable index is available.</returns>
    internal static async ValueTask<List<object[]>?> TryReadMultiKeyIndexRowsAsync(
        AccessReader reader,
        ResolvedTable resolved,
        IReadOnlyList<IProperty> keyProperties,
        IReadOnlyList<object?[]> keyTuples,
        bool[]? wantedColumns,
        CancellationToken cancellationToken)
    {
        if (keyProperties.Count == 0 || keyTuples.Count == 0 || !reader.Services.Indexes.CanSeek)
        {
            return null;
        }

        TableDef tableDef = resolved.Definition;
        if (!TryPrepareMultiKeyTuples(tableDef, keyProperties, keyTuples, out string[]? keyColumnNames, out List<object?[]>? providerTuples))
        {
            return null;
        }

        IReadOnlyList<IndexMetadata> indexes = await reader.Services.Indexes.ListSeekableIndexesAsync(resolved, cancellationToken).ConfigureAwait(false);
        IndexMetadata? chosenIndex = SelectPrefixMatchingIndex(indexes, keyColumnNames!);
        if (chosenIndex is null)
        {
            return null;
        }

        string tableName = resolved.Entry.Name;
        if (!await reader.Services.Indexes.CanSeekJoinKeysAsync(tableName, chosenIndex, providerTuples!, cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        bool isExact = chosenIndex.Columns.Count == keyColumnNames!.Length;
        List<object[]> results = [];
        foreach (object?[] tuple in providerTuples!)
        {
            IndexQueryCriteria criteria = isExact
                ? IndexQueryCriteria.Exact(tuple)
                : IndexQueryCriteria.KeyPrefix(tuple);
            await foreach (object[] row in reader.Services.Indexes.ReadIndexRowsAsObjectsAsync(
                tableName,
                chosenIndex.Name,
                criteria,
                wantedColumns,
                cancellationToken).ConfigureAwait(false))
            {
                results.Add(row);
            }
        }

        return results;
    }

    private static RowCriteria BuildSeekableCriteria(
        IReadOnlyList<JetIndexSeekCondition> conditions,
        IReadOnlyDictionary<string, object?> parameters,
        TableDef tableDef)
    {
        RowCriteria seekable = [];
        foreach (JetIndexSeekCondition condition in conditions)
        {
            if (condition.Operator == JetIndexSeekOperator.In)
            {
                continue;
            }

            object? evaluated = condition.ValueEvaluator(parameters);
            if (evaluated is null or DBNull)
            {
                continue;
            }

            ColumnInfo? column = tableDef.FindColumn(condition.ColumnName);
            if (column is null)
            {
                continue;
            }

            ColumnPredicateOperator rawOp = MapToColumnPredicateOperator(condition.Operator);
            Type storageType = JetEntityTypeMetadata.GetStorageClrType(condition.Property);
            if (IndexSeekFilter.TryRewriteSeekOperator(column, storageType, rawOp, evaluated, out ColumnPredicateOperator rewrittenOp))
            {
                seekable.Add(CreateColumnPredicate(condition.ColumnName, rewrittenOp, evaluated));
            }
        }

        return seekable;
    }

    private static List<IndexMetadata> FilterSeekableIndexes(TableDef tableDef, IReadOnlyList<IndexMetadata> indexes)
    {
        List<IndexMetadata> result = [];
        foreach (IndexMetadata index in indexes)
        {
            bool supported = true;
            foreach (IndexColumnReference key in index.Columns)
            {
                ColumnInfo? column = tableDef.Columns.FirstOrDefault(c => c.ColNum == key.ColumnNumber);
                if (column is null || (column.Type is TextType or MemoType && !column.TextSortOrder.IsSupported))
                {
                    supported = false;
                    break;
                }
            }

            if (supported)
            {
                result.Add(index);
            }
        }

        return result;
    }

    private static IndexMetadata? SelectOrderedScanIndex(
        JetFormat format,
        TableDef tableDef,
        IReadOnlyList<IndexMetadata> indexes,
        IReadOnlyList<IProperty> orderedAscendingProperties)
    {
        if (orderedAscendingProperties.Count == 0)
        {
            return null;
        }

        string[] targetColumns = new string[orderedAscendingProperties.Count];
        for (int i = 0; i < orderedAscendingProperties.Count; i++)
        {
            IProperty prop = orderedAscendingProperties[i];
            if (prop.GetValueConverter() is not null || prop.FindTypeMapping()?.Converter is not null)
            {
                return null;
            }

            Type unwrapped = Nullable.GetUnderlyingType(prop.ClrType) ?? prop.ClrType;
            if (unwrapped.IsEnum)
            {
                unwrapped = Enum.GetUnderlyingType(unwrapped);
            }

            if (unwrapped != typeof(byte)
                && unwrapped != typeof(short)
                && unwrapped != typeof(int)
                && unwrapped != typeof(long)
                && unwrapped != typeof(float)
                && unwrapped != typeof(double)
                && unwrapped != typeof(decimal)
                && unwrapped != typeof(DateTime)
                && unwrapped != typeof(DateOnly)
                && unwrapped != typeof(TimeOnly))
            {
                return null;
            }

            string colName = JetEntityTypeMetadata.GetColumnName(prop);
            ColumnInfo? colInfo = tableDef.FindColumn(colName);
            if (colInfo?.IsCalculated != false)
            {
                return null;
            }

            if (format.LegacyNumericIndexKeys && colInfo.Type == NumericType)
            {
                return null;
            }

            targetColumns[i] = colName;
        }

        List<IndexMetadata> candidates = [];
        foreach (IndexMetadata index in indexes)
        {
            if (index.FirstDp <= 0
                || index.IgnoreNulls
                || index.Columns.Count < targetColumns.Length)
            {
                continue;
            }

            bool matches = true;
            for (int i = 0; i < targetColumns.Length; i++)
            {
                if (!index.Columns[i].IsAscending
                    || !string.Equals(index.Columns[i].Name, targetColumns[i], StringComparison.OrdinalIgnoreCase))
                {
                    matches = false;
                    break;
                }
            }

            if (matches)
            {
                candidates.Add(index);
            }
        }

        return candidates
            .OrderByDescending(static idx => idx.Kind == IndexKind.PrimaryKey)
            .ThenByDescending(static idx => idx.EnforcesUniqueness)
            .ThenBy(static idx => idx.Columns.Count)
            .FirstOrDefault();
    }

    private static bool TryPrepareMultiKeyTuples(
        TableDef tableDef,
        IReadOnlyList<IProperty> keyProperties,
        IReadOnlyList<object?[]> keyTuples,
        out string[]? keyColumnNames,
        out List<object?[]>? providerTuples)
    {
        keyColumnNames = new string[keyProperties.Count];
        var keyColumns = new ColumnInfo[keyProperties.Count];
        var keyStorageTypes = new Type[keyProperties.Count];

        for (int i = 0; i < keyProperties.Count; i++)
        {
            string colName = JetEntityTypeMetadata.GetColumnName(keyProperties[i]);
            ColumnInfo? col = tableDef.FindColumn(colName);
            if (col is null)
            {
                providerTuples = null;
                return false;
            }

            keyColumnNames[i] = colName;
            keyColumns[i] = col;
            keyStorageTypes[i] = JetEntityTypeMetadata.GetStorageClrType(keyProperties[i]);
        }

        providerTuples = [];
        providerTuples.Capacity = keyTuples.Count;
        foreach (object?[] rawTuple in keyTuples)
        {
            if (rawTuple.Length != keyProperties.Count)
            {
                return false;
            }

            object?[] convertedTuple = new object?[keyProperties.Count];
            for (int i = 0; i < keyProperties.Count; i++)
            {
                object? providerVal = JetEntityTypeMetadata.ConvertToProvider(keyProperties[i], rawTuple[i]);
                if (providerVal is null or DBNull
                    || !IndexSeekFilter.IsSeekable(keyColumns[i], keyStorageTypes[i], ColumnPredicateOperator.Equal, providerVal))
                {
                    return false;
                }

                convertedTuple[i] = providerVal;
            }

            providerTuples.Add(convertedTuple);
        }

        return true;
    }

    private static bool CanEncodeMultiKeyTuples(
        JetFormat format,
        TableDef tableDef,
        string tableName,
        IndexMetadata index,
        List<object?[]> providerTuples)
    {
        foreach (object?[] key in providerTuples)
        {
            for (int ordinal = 0; ordinal < key.Length && ordinal < index.Columns.Count; ordinal++)
            {
                ColumnInfo? column = tableDef.Columns.FirstOrDefault(value => string.Equals(value.Name, index.Columns[ordinal].Name, StringComparison.OrdinalIgnoreCase));
                if (column?.Type is TextType or MemoType
                    && TextCollationFamily(column.TextSortOrder) != TextCollationFamily(format.DefaultTextSortOrder))
                {
                    return false;
                }
            }

            if (!CanEncodePlan(format, tableDef, tableName, new IndexPlan(index, IndexQueryCriteria.KeyPrefix(key), key.Length)))
            {
                return false;
            }
        }

        return true;
    }

    private static int TextCollationFamily(JetDatabaseWriter.Indexes.Collation.TextSortOrder order)
    {
        if (order.Value == 0 || (order.HasVersion && order.Version == 0))
        {
            return 0;
        }

        return order.HasVersion ? 1 : 2;
    }

    private static async ValueTask<bool> EnumerateActiveWriterIndexCriteriaAsync(
        AccessWriter writer,
        ResolvedTable resolved,
        IndexMetadata index,
        IndexQueryCriteria criteria,
        bool[] wantedColumns,
        Func<object?[], bool> visitRow,
        CancellationToken cancellationToken)
    {
        JetFormat format = writer.Database.Format;
        TableDef td = resolved.Definition;
        string tableName = resolved.Entry.Name;
        ReaderPageCache snapshotPages = writer.Services.SnapshotPages;

        using var cursor = new IndexCursor(
            format.IndexPage,
            snapshotPages.ReadPageAsync,
            format.PageSize);
        var rowDecoder = new RowDecoder(
            format,
            writer.Database.OwnedPages,
            snapshotPages,
            new LongValueDecoder(format, snapshotPages, 0xFFFFFF),
            strictParsing: true,
            new RowReadAccounting());
        var decodePlan = RowDecodePlan.CreateTyped(td, wantedColumns, strictParsing: true);
        bool needsHyperlinkPass = td.HasHyperlinkColumns
            && TableReader.HasWantedHyperlinkColumn(td.ClrTypes, wantedColumns);

        await foreach ((long dataPage, int rowIndex) in cursor.EnumerateRowLocationsForCriteriaAsync(
            format,
            tableName,
            index,
            td,
            criteria,
            cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            object?[]? row = await MaterializeActiveWriterSeekRowAsync(
                format,
                snapshotPages,
                rowDecoder,
                resolved.Entry.TDefPage,
                td,
                dataPage,
                rowIndex,
                decodePlan,
                needsHyperlinkPass,
                cancellationToken).ConfigureAwait(false);
            if (row is null)
            {
                continue;
            }

            if (!visitRow(row))
            {
                return false;
            }
        }

        return true;
    }

    private static async ValueTask<object?[]?> MaterializeActiveWriterSeekRowAsync(
        JetFormat format,
        ReaderPageCache pages,
        RowDecoder rows,
        long expectedTDefPage,
        TableDef td,
        long dataPage,
        int rowIndex,
        RowDecodePlan decodePlan,
        bool needsHyperlinkPass,
        CancellationToken cancellationToken)
    {
        using PageLease lease = await pages.ReadPageAsync(dataPage, cancellationToken).ConfigureAwait(false);
        byte[] page = lease.Buffer;
        if (page[0] != Constants.PageTypes.Data || Ri32(page, format.DataPage.TDefOff) != expectedTDefPage)
        {
            _ = rows.AdmitRow(readable: false, dataPage, new RowBound(rowIndex, 0, 0));
            return null;
        }

        if (!TryFindRowEntry(format, lease, rowIndex, out RowBound rowBound))
        {
            _ = rows.AdmitRow(readable: false, dataPage, new RowBound(rowIndex, 0, 0));
            return null;
        }

        using OverflowRowTarget? resolved = rowBound.IsOverflowPointer
            ? await rows.ResolveOverflowAsync(page, rowBound, cancellationToken).ConfigureAwait(false)
            : null;
        if (rowBound.IsOverflowPointer)
        {
            if (resolved is not { } target)
            {
                _ = rows.AdmitRow(readable: false, dataPage, rowBound);
                return null;
            }

            (page, rowBound) = (target.Page, target.Bound);
        }

        if (rowBound.RowSize < format.RowFields.NumCols)
        {
            _ = rows.AdmitRow(readable: false, dataPage, rowBound);
            return null;
        }

        object?[]? row = await rows.CrackRowTypedAsync(page, rowBound.RowStart, rowBound.RowSize, decodePlan, cancellationToken).ConfigureAwait(false);
        if (row is null)
        {
            _ = rows.AdmitRow(readable: false, dataPage, rowBound);
            return null;
        }

        if (needsHyperlinkPass)
        {
            TableReader.WrapHyperlinkColumns(row, td.ClrTypes);
        }

        return row;
    }

    private static bool TryFindRowEntry(JetFormat format, PageLease lease, int rowIndex, out RowBound rowBound)
    {
        foreach (RowBound candidate in lease.GetRowDirectory(format))
        {
            if (candidate.RowIndex == rowIndex)
            {
                rowBound = candidate;
                return true;
            }
        }

        rowBound = default;
        return false;
    }

    private static ColumnPredicate CreateColumnPredicate(string columnName, ColumnPredicateOperator op, object value)
    {
        if (op == ColumnPredicateOperator.GreaterThan)
        {
            return ColumnPredicate.GreaterThan(columnName, value);
        }

        if (op == ColumnPredicateOperator.GreaterThanOrEqual)
        {
            return ColumnPredicate.GreaterThanOrEqual(columnName, value);
        }

        if (op == ColumnPredicateOperator.LessThan)
        {
            return ColumnPredicate.LessThan(columnName, value);
        }

        if (op == ColumnPredicateOperator.LessThanOrEqual)
        {
            return ColumnPredicate.LessThanOrEqual(columnName, value);
        }

        return ColumnPredicate.EqualTo(columnName, value);
    }

    private static bool CanEncodePlan(JetFormat format, TableDef definition, string tableName, IndexPlan plan)
        => (plan.Criteria.Values is null || IndexKeyEncoder.CanEncodeIndexKeyPrefix(format, tableName, plan.Index, definition, plan.Criteria.Values))
            && (plan.Criteria.Lower is null || IndexKeyEncoder.CanEncodeIndexKeyPrefix(format, tableName, plan.Index, definition, plan.Criteria.Lower.Values))
            && (plan.Criteria.Upper is null || IndexKeyEncoder.CanEncodeIndexKeyPrefix(format, tableName, plan.Index, definition, plan.Criteria.Upper.Values));

    private static IndexMetadata? SelectPrefixMatchingIndex(IReadOnlyList<IndexMetadata> indexes, string[] keyColumnNames)
    {
        List<IndexMetadata> candidates = [];
        foreach (IndexMetadata index in indexes)
        {
            if (index.FirstDp <= 0 || index.Columns.Count < keyColumnNames.Length)
            {
                continue;
            }

            bool matchesPrefix = true;
            for (int i = 0; i < keyColumnNames.Length; i++)
            {
                if (!string.Equals(index.Columns[i].Name, keyColumnNames[i], StringComparison.OrdinalIgnoreCase))
                {
                    matchesPrefix = false;
                    break;
                }
            }

            if (matchesPrefix)
            {
                candidates.Add(index);
            }
        }

        return candidates
            .OrderByDescending(idx => idx.Columns.Count == keyColumnNames.Length)
            .ThenByDescending(static idx => idx.Kind == IndexKind.PrimaryKey)
            .ThenByDescending(static idx => idx.EnforcesUniqueness)
            .ThenBy(static idx => idx.Columns.Count)
            .FirstOrDefault();
    }

    private static ColumnPredicateOperator MapToColumnPredicateOperator(JetIndexSeekOperator op)
        => op switch
        {
            JetIndexSeekOperator.Equal => ColumnPredicateOperator.Equal,
            JetIndexSeekOperator.GreaterThan => ColumnPredicateOperator.GreaterThan,
            JetIndexSeekOperator.GreaterThanOrEqual => ColumnPredicateOperator.GreaterThanOrEqual,
            JetIndexSeekOperator.LessThan => ColumnPredicateOperator.LessThan,
            JetIndexSeekOperator.LessThanOrEqual => ColumnPredicateOperator.LessThanOrEqual,
            JetIndexSeekOperator.In => ColumnPredicateOperator.Equal,
            _ => ColumnPredicateOperator.Equal,
        };

    private static void ExtractConjunctions(
        IEntityType entityType,
        ParameterExpression entityParam,
        Expression expression,
        List<JetIndexSeekCondition> conditions)
    {
        Expression unwrapped = StripQuotesAndConverts(expression);
        if (unwrapped is BinaryExpression { NodeType: ExpressionType.AndAlso } andAlso)
        {
            ExtractConjunctions(entityType, entityParam, andAlso.Left, conditions);
            ExtractConjunctions(entityType, entityParam, andAlso.Right, conditions);
            return;
        }

        if (unwrapped is BinaryExpression { NodeType: ExpressionType.OrElse } orElse
            && TryExtractOrElseInCondition(entityType, entityParam, orElse, out JetIndexSeekCondition? orInCondition))
        {
            conditions.Add(orInCondition!);
            return;
        }

        if (unwrapped is MethodCallExpression methodCall
            && TryExtractContainsCondition(entityType, entityParam, methodCall, out JetIndexSeekCondition? inCondition))
        {
            conditions.Add(inCondition!);
            return;
        }

        if (unwrapped is UnaryExpression { NodeType: ExpressionType.Not } notUnary
            && TryExtractPropertyAccess(entityType, entityParam, notUnary.Operand, out IProperty? negatedBoolProp)
            && (negatedBoolProp!.ClrType == typeof(bool) || negatedBoolProp.ClrType == typeof(bool?)))
        {
            object? convertedFalse = JetEntityTypeMetadata.ConvertToProvider(negatedBoolProp, false);
            conditions.Add(new JetIndexSeekCondition(
                negatedBoolProp,
                JetEntityTypeMetadata.GetColumnName(negatedBoolProp),
                JetIndexSeekOperator.Equal,
                _ => convertedFalse));
            return;
        }

        if (TryExtractPropertyAccess(entityType, entityParam, unwrapped, out IProperty? bareBoolProp)
            && (bareBoolProp!.ClrType == typeof(bool) || bareBoolProp.ClrType == typeof(bool?)))
        {
            object? convertedTrue = JetEntityTypeMetadata.ConvertToProvider(bareBoolProp, true);
            conditions.Add(new JetIndexSeekCondition(
                bareBoolProp,
                JetEntityTypeMetadata.GetColumnName(bareBoolProp),
                JetIndexSeekOperator.Equal,
                _ => convertedTrue));
            return;
        }

        if (unwrapped is not BinaryExpression binary)
        {
            return;
        }

        if (TryMapOperator(binary.NodeType, reversed: false, out JetIndexSeekOperator op)
            && TryExtractPropertyAccess(entityType, entityParam, binary.Left, out IProperty? leftProperty)
            && IsOperatorSupportedForProperty(leftProperty!, op)
            && !ReferencesParameter(binary.Right, entityParam)
            && TryBuildValueEvaluator(leftProperty!, binary.Right, out Func<IReadOnlyDictionary<string, object?>, object?>? rightEval))
        {
            conditions.Add(new JetIndexSeekCondition(
                leftProperty!,
                JetEntityTypeMetadata.GetColumnName(leftProperty!),
                op,
                rightEval!));
            return;
        }

        if (TryMapOperator(binary.NodeType, reversed: true, out JetIndexSeekOperator reversedOp)
            && TryExtractPropertyAccess(entityType, entityParam, binary.Right, out IProperty? rightProperty)
            && IsOperatorSupportedForProperty(rightProperty!, reversedOp)
            && !ReferencesParameter(binary.Left, entityParam)
            && TryBuildValueEvaluator(rightProperty!, binary.Left, out Func<IReadOnlyDictionary<string, object?>, object?>? leftEval))
        {
            conditions.Add(new JetIndexSeekCondition(
                rightProperty!,
                JetEntityTypeMetadata.GetColumnName(rightProperty!),
                reversedOp,
                leftEval!));
        }
    }

    private static bool TryExtractOrElseInCondition(
        IEntityType entityType,
        ParameterExpression entityParam,
        BinaryExpression orElse,
        out JetIndexSeekCondition? condition)
    {
        condition = null;
        List<Expression> leaves = [];
        CollectOrElseLeaves(orElse, leaves);
        if (leaves.Count < 2)
        {
            return false;
        }

        IProperty? commonProperty = null;
        List<Func<IReadOnlyDictionary<string, object?>, object?>> valueEvaluators = [];
        valueEvaluators.Capacity = leaves.Count;

        foreach (Expression leaf in leaves)
        {
            if (leaf is not BinaryExpression { NodeType: ExpressionType.Equal } eq)
            {
                return false;
            }

            Expression valueExpr;
            if (TryExtractPropertyAccess(entityType, entityParam, eq.Left, out IProperty? prop)
                && !ReferencesParameter(eq.Right, entityParam))
            {
                valueExpr = eq.Right;
            }
            else if (TryExtractPropertyAccess(entityType, entityParam, eq.Right, out prop)
                && !ReferencesParameter(eq.Left, entityParam))
            {
                valueExpr = eq.Left;
            }
            else
            {
                return false;
            }

            if (commonProperty is null)
            {
                commonProperty = prop;
            }
            else if (!ReferenceEquals(commonProperty, prop))
            {
                return false;
            }

            if (!TryBuildRawModelValueEvaluator(valueExpr, out Func<IReadOnlyDictionary<string, object?>, object?>? eval))
            {
                return false;
            }

            valueEvaluators.Add(eval!);
        }

        if (commonProperty is null)
        {
            return false;
        }

        Func<IReadOnlyDictionary<string, object?>, object?>[] evalArray = [.. valueEvaluators];
        condition = new JetIndexSeekCondition(
            commonProperty,
            JetEntityTypeMetadata.GetColumnName(commonProperty),
            JetIndexSeekOperator.In,
            parameters =>
            {
                List<object?[]> tuples = [];
                tuples.Capacity = evalArray.Length;
                HashSet<object> seen = [];
                for (int i = 0; i < evalArray.Length; i++)
                {
                    object? val = evalArray[i](parameters);
                    if (val is not (null or DBNull) && seen.Add(val))
                    {
                        tuples.Add([val]);
                    }
                }

                return tuples;
            });
        return true;
    }

    private static void CollectOrElseLeaves(Expression expression, List<Expression> leaves)
    {
        Expression unwrapped = StripQuotesAndConverts(expression);
        if (unwrapped is BinaryExpression { NodeType: ExpressionType.OrElse } orElse)
        {
            CollectOrElseLeaves(orElse.Left, leaves);
            CollectOrElseLeaves(orElse.Right, leaves);
            return;
        }

        leaves.Add(unwrapped);
    }

    private static bool TryBuildRawModelValueEvaluator(
        Expression valueExpression,
        out Func<IReadOnlyDictionary<string, object?>, object?>? evaluator)
    {
        Expression unwrapped = StripQuotesAndConverts(valueExpression);
        if (unwrapped is ConstantExpression constantExpr)
        {
            object? val = constantExpr.Value;
            evaluator = _ => val;
            return true;
        }

        if (unwrapped is QueryParameterExpression queryParam)
        {
            string paramName = queryParam.Name;
            evaluator = parameters => parameters.GetValueOrDefault(paramName);
            return true;
        }

        ParameterExpression dictParam = Expression.Parameter(typeof(IReadOnlyDictionary<string, object?>), "parameters");
        ParameterRewritingVisitor rewriter = new(dictParam);
        Expression rewritten = rewriter.Visit(valueExpression);
        if (!rewriter.IsSupported || rewritten.Type.IsByRefLike)
        {
            evaluator = null;
            return false;
        }

        Expression boxed = Expression.Convert(rewritten, typeof(object));
        evaluator = Expression.Lambda<Func<IReadOnlyDictionary<string, object?>, object?>>(boxed, dictParam).Compile();
        return true;
    }

    private static bool IsOperatorSupportedForProperty(IProperty property, JetIndexSeekOperator seekOperator)
    {
        if (seekOperator == JetIndexSeekOperator.Equal)
        {
            return true;
        }

        return property.GetValueConverter() is null && property.FindTypeMapping()?.Converter is null;
    }

    private static bool TryMapOperator(ExpressionType nodeType, bool reversed, out JetIndexSeekOperator seekOperator)
    {
        if (nodeType == ExpressionType.Equal)
        {
            seekOperator = JetIndexSeekOperator.Equal;
            return true;
        }

        if (nodeType == ExpressionType.GreaterThan)
        {
            seekOperator = reversed ? JetIndexSeekOperator.LessThan : JetIndexSeekOperator.GreaterThan;
            return true;
        }

        if (nodeType == ExpressionType.GreaterThanOrEqual)
        {
            seekOperator = reversed ? JetIndexSeekOperator.LessThanOrEqual : JetIndexSeekOperator.GreaterThanOrEqual;
            return true;
        }

        if (nodeType == ExpressionType.LessThan)
        {
            seekOperator = reversed ? JetIndexSeekOperator.GreaterThan : JetIndexSeekOperator.LessThan;
            return true;
        }

        if (nodeType == ExpressionType.LessThanOrEqual)
        {
            seekOperator = reversed ? JetIndexSeekOperator.GreaterThanOrEqual : JetIndexSeekOperator.LessThanOrEqual;
            return true;
        }

        seekOperator = default;
        return false;
    }

    internal static bool TryExtractPropertyAccess(
        IEntityType entityType,
        ParameterExpression entityParam,
        Expression candidate,
        out IProperty? property)
    {
        Expression current = StripQuotesAndConverts(candidate);
        if (current is MemberExpression memberExpr)
        {
            List<MemberInfo> chain = [];
            Expression? cursor = memberExpr;
            while (cursor is MemberExpression step)
            {
                chain.Add(step.Member);
                cursor = step.Expression is not null ? StripQuotesAndConverts(step.Expression) : null;
            }

            if (cursor == entityParam && chain.Count > 0)
            {
                chain.Reverse();
                ITypeBase currentType = entityType;
                for (int i = 0; i < chain.Count - 1; i++)
                {
                    IComplexProperty? complexProp = currentType.FindComplexProperty(chain[i]);
                    if (complexProp is null)
                    {
                        property = null;
                        return false;
                    }

                    currentType = complexProp.ComplexType;
                }

                property = currentType.FindProperty(chain[^1]);
                return property is not null;
            }
        }

        if (current is MethodCallExpression { Method.Name: nameof(EF.Property) } callExpr
            && callExpr.Method.DeclaringType == typeof(EF)
            && callExpr.Arguments.Count == 2
            && StripQuotesAndConverts(callExpr.Arguments[0]) == entityParam
            && StripQuotesAndConverts(callExpr.Arguments[1]) is ConstantExpression { Value: string propName })
        {
            property = entityType.FindProperty(propName);
            return property is not null;
        }

        property = null;
        return false;
    }

    private static bool TryBuildValueEvaluator(
        IProperty property,
        Expression valueExpression,
        out Func<IReadOnlyDictionary<string, object?>, object?>? evaluator)
    {
        Expression unwrapped = StripQuotesAndConverts(valueExpression);
        if (unwrapped is ConstantExpression constantExpr)
        {
            object? converted = JetEntityTypeMetadata.ConvertToProvider(property, constantExpr.Value);
            evaluator = _ => converted;
            return true;
        }

        if (unwrapped is QueryParameterExpression queryParam)
        {
            string paramName = queryParam.Name;
            evaluator = parameters =>
                parameters.TryGetValue(paramName, out object? raw)
                    ? JetEntityTypeMetadata.ConvertToProvider(property, raw)
                    : null;
            return true;
        }

        ParameterExpression dictParam = Expression.Parameter(typeof(IReadOnlyDictionary<string, object?>), "parameters");
        ParameterRewritingVisitor rewriter = new(dictParam);
        Expression rewritten = rewriter.Visit(valueExpression);
        if (!rewriter.IsSupported)
        {
            evaluator = null;
            return false;
        }

        Expression boxed = Expression.Convert(rewritten, typeof(object));
        Func<IReadOnlyDictionary<string, object?>, object?> compiled =
            Expression.Lambda<Func<IReadOnlyDictionary<string, object?>, object?>>(boxed, dictParam).Compile();
        evaluator = parameters => JetEntityTypeMetadata.ConvertToProvider(property, compiled(parameters));
        return true;
    }

    private static async IAsyncEnumerable<object[]> YieldRowsAsync(List<object[]> rows)
    {
        await Task.CompletedTask.ConfigureAwait(false);
        foreach (object[] row in rows)
        {
            yield return row;
        }
    }

    private static bool TryExtractContainsCondition(
        IEntityType entityType,
        ParameterExpression entityParam,
        MethodCallExpression methodCall,
        out JetIndexSeekCondition? condition)
    {
        condition = null;
        if (!string.Equals(methodCall.Method.Name, nameof(Enumerable.Contains), StringComparison.Ordinal))
        {
            return false;
        }

        Expression? collectionExpr = null;
        Expression? itemExpr = null;
        if (methodCall.Object is not null && methodCall.Arguments.Count == 1)
        {
            collectionExpr = methodCall.Object;
            itemExpr = methodCall.Arguments[0];
        }
        else if (methodCall.Object is null && methodCall.Arguments.Count is 2 or 3)
        {
            if (methodCall.Arguments.Count == 3 && methodCall.Arguments[2] is not ConstantExpression { Value: null })
            {
                return false;
            }

            collectionExpr = methodCall.Arguments[0];
            itemExpr = methodCall.Arguments[1];
        }

        if (collectionExpr is null || itemExpr is null)
        {
            return false;
        }

        collectionExpr = UnwrapSpanConversion(collectionExpr);
        if (collectionExpr.Type.IsByRefLike
            || !typeof(System.Collections.IEnumerable).IsAssignableFrom(collectionExpr.Type)
            || collectionExpr.Type == typeof(string)
            || ReferencesParameter(collectionExpr, entityParam)
            || !TryExtractPropertyAccess(entityType, entityParam, itemExpr, out IProperty? property))
        {
            return false;
        }

        if (!TryBuildInValuesEvaluator(collectionExpr, out Func<IReadOnlyDictionary<string, object?>, object?>? inEvaluator))
        {
            return false;
        }

        condition = new JetIndexSeekCondition(
            property!,
            JetEntityTypeMetadata.GetColumnName(property!),
            JetIndexSeekOperator.In,
            inEvaluator!);
        return true;
    }

    private static Expression UnwrapSpanConversion(Expression expression)
    {
        Expression current = StripQuotesAndConverts(expression);
        while (current.Type.IsByRefLike)
        {
            if (current is UnaryExpression { Operand: not null } unary)
            {
                current = StripQuotesAndConverts(unary.Operand);
                continue;
            }

            if (current is MethodCallExpression { Arguments.Count: >= 1 } call
                && call.Method.Name is "op_Implicit" or nameof(MemoryExtensions.AsSpan))
            {
                current = StripQuotesAndConverts(call.Arguments[0]);
                continue;
            }

            if (current is NewExpression { Arguments.Count: >= 1 } newExpr)
            {
                current = StripQuotesAndConverts(newExpr.Arguments[0]);
                continue;
            }

            break;
        }

        return current;
    }

    private static bool TryBuildInValuesEvaluator(
        Expression collectionExpression,
        out Func<IReadOnlyDictionary<string, object?>, object?>? evaluator)
    {
        if (collectionExpression is ConstantExpression constantExpr)
        {
            IReadOnlyList<object?[]>? staticTuples = ConvertEnumerableToDistinctKeyTuples(constantExpr.Value);
            evaluator = _ => staticTuples;
            return true;
        }

        if (collectionExpression is QueryParameterExpression queryParam)
        {
            string paramName = queryParam.Name;
            evaluator = parameters =>
                parameters.TryGetValue(paramName, out object? raw)
                    ? ConvertEnumerableToDistinctKeyTuples(raw)
                    : null;
            return true;
        }

        ParameterExpression dictParam = Expression.Parameter(typeof(IReadOnlyDictionary<string, object?>), "parameters");
        ParameterRewritingVisitor rewriter = new(dictParam);
        Expression rewritten = rewriter.Visit(collectionExpression);
        if (!rewriter.IsSupported || rewritten.Type.IsByRefLike)
        {
            evaluator = null;
            return false;
        }

        Expression boxed = Expression.Convert(rewritten, typeof(object));
        Func<IReadOnlyDictionary<string, object?>, object?> compiled =
            Expression.Lambda<Func<IReadOnlyDictionary<string, object?>, object?>>(boxed, dictParam).Compile();
        evaluator = parameters => ConvertEnumerableToDistinctKeyTuples(compiled(parameters));
        return true;
    }

    private static List<object?[]>? ConvertEnumerableToDistinctKeyTuples(object? rawEnumerable)
    {
        if (rawEnumerable is not System.Collections.IEnumerable enumerable || rawEnumerable is string)
        {
            return null;
        }

        List<object?[]> tuples = [];
        HashSet<object> seen = [];
        foreach (object? item in enumerable)
        {
            if (item is null or DBNull)
            {
                continue;
            }

            if (seen.Add(item))
            {
                tuples.Add([item]);
            }
        }

        return tuples;
    }

    private static Expression StripQuotesAndConverts(Expression expression)
    {
        Expression current = expression;
        while (current is UnaryExpression unary
               && unary.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked or ExpressionType.Quote)
        {
            current = unary.Operand;
        }

        return current;
    }

    private static bool ReferencesParameter(Expression expression, ParameterExpression parameter)
    {
        ParameterReferenceDetector detector = new(parameter);
        _ = detector.Visit(expression);
        return detector.Found;
    }

    private sealed class ParameterReferenceDetector(ParameterExpression target) : ExpressionVisitor
    {
        public bool Found { get; private set; }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == target)
            {
                this.Found = true;
            }

            return base.VisitParameter(node);
        }
    }

    private sealed class ParameterRewritingVisitor(ParameterExpression dictionaryParameter) : ExpressionVisitor
    {
        private static readonly MethodInfo GetValueOrDefaultMethod =
            typeof(ParameterRewritingVisitor).GetMethod(
                nameof(GetParameterValue),
                BindingFlags.NonPublic | BindingFlags.Static)!;

        public bool IsSupported { get; private set; } = true;

        protected override Expression VisitExtension(Expression node)
        {
            if (node is QueryParameterExpression queryParam)
            {
                MethodInfo typedMethod = GetValueOrDefaultMethod.MakeGenericMethod(queryParam.Type);
                return Expression.Call(
                    typedMethod,
                    dictionaryParameter,
                    Expression.Constant(queryParam.Name));
            }

            this.IsSupported = false;
            return node;
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node != dictionaryParameter)
            {
                this.IsSupported = false;
            }

            return base.VisitParameter(node);
        }

        private static T? GetParameterValue<T>(IReadOnlyDictionary<string, object?> parameters, string name)
            => parameters.TryGetValue(name, out object? val) && val is T typed ? typed : default;
    }
}
