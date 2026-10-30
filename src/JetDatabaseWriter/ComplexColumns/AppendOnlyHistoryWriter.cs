namespace JetDatabaseWriter.ComplexColumns;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tables;

/// <summary>Stages native version snapshots as part of the parent row's atomic statement.</summary>
/// <param name="format">The database format.</param>
/// <param name="catalog">The writer's table catalog.</param>
/// <param name="complexColumns">Validates complex descriptors and their flat tables.</param>
/// <param name="snapshots">Resolves hidden flat tables.</param>
/// <param name="seeds">Validates existing parent references.</param>
/// <param name="indexes">Validates history flat-table index support.</param>
/// <param name="constraints">Assigns native flat-table AutoNumbers.</param>
/// <param name="uniqueIndexes">Checks the generated keys.</param>
internal sealed class AppendOnlyHistoryWriter(
    JetFormat format,
    TableCatalog catalog,
    ComplexColumnReader complexColumns,
    TableSnapshotReader snapshots,
    ComplexReferenceSeedReader seeds,
    IndexMaintainer indexes,
    ConstraintRegistry constraints,
    UniqueIndexChecker uniqueIndexes)
{
    /// <summary>Adds prepared history inserts to the statement's shared table change sets.</summary>
    /// <param name="plan">The statement's accumulated changes.</param>
    /// <param name="batches">The prepared history rows.</param>
    internal static void AddToPlan(MutationPlan plan, IEnumerable<HistoryBatch> batches)
    {
        foreach (HistoryBatch batch in batches)
        {
            ResolvedTable table = batch.Table;
            MutationPlan.TableChanges changes = plan.ForTable(table.Entry.TDefPage, table.Definition, table.Entry.Name);
            foreach (object[] version in batch.Rows)
            {
                changes.Insert(version);
            }

            changes.RaiseAutoNumbers = true;
        }
    }

    /// <summary>Prepares history rows before any parent or cascaded row is changed.</summary>
    /// <param name="tableName">The parent table.</param>
    /// <param name="tableDef">Its definition.</param>
    /// <param name="newRows">The final parent values, including complex references.</param>
    /// <param name="assignedColumns">The assigned update columns; null for an insert.</param>
    /// <param name="insertAssignments">The columns explicitly supplied by each inserted row.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The staged flat-table insert batches.</returns>
    /// <exception cref="JetCorruptDataException">The append-only column has no unambiguous native history binding.</exception>
    internal async ValueTask<List<HistoryBatch>> PrepareAsync(
        string tableName,
        TableDef tableDef,
        IReadOnlyList<object[]> newRows,
        ICollection<int>? assignedColumns,
        IReadOnlyList<ICollection<int>>? insertAssignments,
        CancellationToken cancellationToken)
    {
        var batches = new List<HistoryBatch>();
        if (!format.SupportsComplexColumns || newRows.Count == 0 || !tableDef.Columns.Any(column => column.Type == ColumnType.MemoType))
        {
            return batches;
        }

        long parentPage = tableDef.TDefPageNumber ?? throw InvalidHistory(tableName, string.Empty);
        TableSchema? schema = await catalog.GetSchemaAsync(parentPage, cancellationToken: cancellationToken).ConfigureAwait(false);
        ColumnPropertyBlock? properties = schema?.Properties;
        var appendColumns = new List<int>();
        for (int index = 0; index < tableDef.Columns.Count; index++)
        {
            ColumnInfo column = tableDef.Columns[index];
            if (column.Type == ColumnType.MemoType
                && assignedColumns?.Contains(index) is not false
                && properties?.FindPropertyTarget(column.Name, "AppendOnly")?.GetBooleanValue("AppendOnly") == true)
            {
                appendColumns.Add(index);
            }
        }

        if (appendColumns.Count == 0)
        {
            return batches;
        }

        IReadOnlyList<ComplexColumnInfo> descriptors = await complexColumns.GetComplexColumnsAsync(tableName, cancellationToken).ConfigureAwait(false);
        _ = await seeds.ReadSeedAsync(parentPage, tableDef, cancellationToken).ConfigureAwait(false);
        DateTime modified = DateTimeOffset.Now.DateTime;
        foreach (int memoIndex in appendColumns)
        {
            string memoName = tableDef.Columns[memoIndex].Name;
            ResolvedTable? flat = null;
            ComplexColumnInfo? history = null;
            foreach (ComplexColumnInfo descriptor in descriptors)
            {
                if (descriptor.Kind != ComplexColumnKind.VersionHistory)
                {
                    continue;
                }

                ResolvedTable? candidate = await snapshots.ResolveTableAsync(descriptor.FlatTableName, cancellationToken).ConfigureAwait(false);
                if (candidate?.Definition.FindColumn(memoName)?.Type == ColumnType.MemoType)
                {
                    if (flat is not null)
                    {
                        throw InvalidHistory(tableName, memoName);
                    }

                    flat = candidate;
                    history = descriptor;
                }
            }

            if (flat is null || history is null)
            {
                throw InvalidHistory(tableName, memoName);
            }

            TableDef flatDef = flat.Definition;
            int referenceIndex = tableDef.FindColumnIndex(history.ColumnName);
            int foreignKeyIndex = flatDef.FindColumnIndex(column => column.Type == ColumnType.LongIntegerType && !column.IsAutoNumber && column.Name.StartsWith('_'));
            int valueIndex = flatDef.FindColumnIndex(memoName);
            int modifiedIndex = flatDef.FindColumnIndex(column => column.Type == ColumnType.DateTimeType);
            if (referenceIndex < 0 || foreignKeyIndex < 0 || modifiedIndex < 0
                || flat.Entry.TDefPage != CatalogValueReader.TdefPageFromId(history.FlatTableId))
            {
                throw InvalidHistory(tableName, memoName);
            }

            await indexes.ThrowIfIndexesUnmaintainableAsync(flat.Entry.TDefPage, flatDef, flat.Entry.Name, cancellationToken).ConfigureAwait(false);
            var versions = new List<object[]>(newRows.Count);
            for (int rowIndex = 0; rowIndex < newRows.Count; rowIndex++)
            {
                object[] parent = newRows[rowIndex];
                object value = parent[memoIndex];
                if (assignedColumns is null && value is DBNull && insertAssignments?[rowIndex].Contains(memoIndex) != true)
                {
                    continue;
                }

                int reference = parent[referenceIndex] is ComplexIdRef token
                    ? token.Id
                    : Convert.ToInt32(parent[referenceIndex], CultureInfo.InvariantCulture);
                if (reference <= 0)
                {
                    throw InvalidHistory(tableName, memoName);
                }

                object[] version = new object[flatDef.Columns.Count];
                Array.Fill(version, DBNull.Value);
                version[foreignKeyIndex] = reference;

                // ACE records explicit empty and Null assignments as U+0000.
                string? text = value is DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
                version[valueIndex] = string.IsNullOrEmpty(text) ? "\0" : text;
                version[modifiedIndex] = modified;
                _ = await constraints.ApplyAsync(flat.Entry.Name, flatDef, version, cancellationToken).ConfigureAwait(false);
                versions.Add(version);
            }

            if (versions.Count > 0)
            {
                batches.Add(new HistoryBatch(flat, versions));
            }
        }

        return batches;
    }

    /// <summary>Checks each shared flat table across every Memo and cascade in the statement.</summary>
    /// <param name="batches">All staged statement batches.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    internal async ValueTask ValidateAsync(IEnumerable<HistoryBatch> batches, CancellationToken cancellationToken)
    {
        foreach (IGrouping<long, HistoryBatch> group in batches.GroupBy(batch => batch.Table.Entry.TDefPage))
        {
            ResolvedTable table = group.First().Table;
            List<object[]> versions = [.. group.SelectMany(batch => batch.Rows)];
            await uniqueIndexes.CheckUniqueIndexesPreInsertAsync(table.Entry.TDefPage, table.Definition, table.Entry.Name, versions, cancellationToken).ConfigureAwait(false);
        }
    }

    private static JetCorruptDataException InvalidHistory(string tableName, string memoName)
        => new(JetErrorCode.CorruptComplexColumn, $"Append-only Memo '{tableName}.{memoName}' has no unambiguous native version-history table.", new JetErrorInfo { TableName = tableName, ColumnName = memoName });

    /// <summary>A flat-table batch owned by the parent statement journal.</summary>
    /// <param name="Table">The hidden history table.</param>
    /// <param name="Rows">The generated version rows.</param>
    internal sealed record HistoryBatch(ResolvedTable Table, List<object[]> Rows);
}
