namespace JetDatabaseWriter.Tables;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Index reads behind <see cref="Interfaces.IAccessReader"/>: lists a table's
/// logical indexes, seeks and range-scans rows through an index B-tree, and
/// serves predicate filters through an inferred index when one covers the
/// predicate (falling back to a table scan otherwise). Each operation enters the
/// reader's operation gate so disposal waits for it.
/// </summary>
/// <param name="format">The file's format profile: the TDEF, index and data-page layouts and the index key encoding.</param>
/// <param name="tableDefs">Reads each table's TDEF bytes, which hold its index definitions.</param>
/// <param name="pages">The reader's page cache, which index and data pages are read through.</param>
/// <param name="rows">Decodes the rows an index points at.</param>
/// <param name="catalog">Resolves tables by name.</param>
/// <param name="complexColumns">Builds the complex-column cells (every attachment or multi-value item of a row) that replace each row's complex reference.</param>
/// <param name="tables">Scans the table when no index covers a predicate.</param>
/// <param name="operations">The reader's operation gate.</param>
internal sealed class IndexRowReader(
    JetFormat format,
    TableDefReader tableDefs,
    ReaderPageCache pages,
    RowDecoder rows,
    CatalogReader catalog,
    ComplexColumnReader complexColumns,
    TableReader tables,
    AsyncReentrantOperationGate operations)
{
    private static bool CanEncodePlan(JetFormat format, TableDef definition, string tableName, IndexPlan plan)
    {
        try
        {
            if (plan.Criteria.Values is not null)
            {
                _ = IndexKeyEncoder.EncodeIndexKeyPrefix(format, tableName, plan.Index, definition, plan.Criteria.Values, nameof(plan));
            }

            if (plan.Criteria.Lower is not null)
            {
                _ = IndexKeyEncoder.EncodeIndexKeyPrefix(format, tableName, plan.Index, definition, plan.Criteria.Lower.Values, nameof(plan));
            }

            if (plan.Criteria.Upper is not null)
            {
                _ = IndexKeyEncoder.EncodeIndexKeyPrefix(format, tableName, plan.Index, definition, plan.Criteria.Upper.Values, nameof(plan));
            }

            return true;
        }
        catch (Exception ex) when (ex is NotSupportedException or ArgumentException or OverflowException)
        {
            return false;
        }
    }

    private static int TextCollationFamily(JetDatabaseWriter.Indexes.Collation.TextSortOrder order)
    {
        if (order.Value == 0 || (order.HasVersion && order.Version == 0))
        {
            return 0;
        }

        return order.HasVersion ? 1 : 2;
    }

    /// <summary>Gets a value indicating whether the database format uses index seeks (<see cref="JetFormat.SupportsIndexSeeks"/>).</summary>
    internal bool CanSeek => format.SupportsIndexSeeks;

    /// <summary>Gets the database collation used to compare Include join text.</summary>
    internal JetDatabaseWriter.Indexes.Collation.TextSortOrder DefaultTextSortOrder => format.DefaultTextSortOrder;

    /// <summary>
    /// Returns metadata for every logical index defined on <paramref name="tableName"/>,
    /// parsed from the table's TDEF page chain.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<IReadOnlyList<IndexMetadata>> ListIndexesAsync(string tableName, CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        return resolved == null ? [] : await this.ReadIndexesAsync(resolved, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Seeks rows through the named index using an exact key tuple and returns matching rows
    /// as typed object arrays in index order.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="indexName">Index name (case-insensitive).</param>
    /// <param name="keyValues">Exact key tuple, one value per indexed column.</param>
    /// <param name="cancellationToken">A token used to cancel asynchronous enumeration.</param>
    internal async IAsyncEnumerable<object[]> SeekRowsAsync(
        string tableName,
        string indexName,
        IReadOnlyList<object?> keyValues,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (object[] row in this.ReadIndexRowsAsObjectsAsync(
                tableName,
                indexName,
                IndexQueryCriteria.Exact(keyValues),
                cancellationToken).ConfigureAwait(false))
        {
            yield return row;
        }
    }

    internal IAsyncEnumerable<object[]> ReadIndexRowsAsObjectsAsync(
        string tableName,
        string indexName,
        IndexQueryCriteria criteria,
        CancellationToken cancellationToken = default) =>
        this.EnumerateIndexRowsAsync<object[]>(
            tableName,
            indexName,
            criteria,
            static _ => (static row => (object[])row, null),
            cancellationToken);

    internal IAsyncEnumerable<T> ReadIndexRowsAsync<T>(
        string tableName,
        string indexName,
        IndexQueryCriteria criteria,
        CancellationToken cancellationToken = default)
        where T : class, new() =>
        this.EnumerateIndexRowsAsync(
            tableName,
            indexName,
            criteria,
            static td =>
            {
                string[] headers = new string[td.Columns.Count];
                for (int i = 0; i < td.Columns.Count; i++)
                {
                    headers[i] = td.Columns[i].Name;
                }

                // Decode and load only the columns T binds; complex columns
                // it leaves out are never read from their flat tables.
                Func<object?[], T> factory = RowMapper<T>.Build(td);
                bool[] wantedColumns = RowMapper<T>.GetBoundColumnMask(headers);

                return (factory, wantedColumns);
            },
            cancellationToken);

    /// <summary>
    /// Returns the rows of <paramref name="tableName"/> that satisfy
    /// <paramref name="predicate"/>, read through an inferred index when one
    /// covers the predicate's pushable conditions and through a table scan otherwise.
    /// </summary>
    /// <typeparam name="T">The mapped row type.</typeparam>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="predicate">A row filter expression; drives index inference and the client-side filter.</param>
    /// <param name="progress">Optional progress reporter — receives the matched-row count.</param>
    /// <param name="cancellationToken">A token used to cancel asynchronous enumeration.</param>
    internal IAsyncEnumerable<T> Rows<T>(
        string tableName,
        Expression<Func<T, bool>> predicate,
        IProgress<long>? progress,
        CancellationToken cancellationToken)
        where T : class, new()
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(predicate, nameof(predicate));

        Func<T, bool> compiled = predicate.Compile();
        RowCriteria pushable = IndexPredicateTranslator.ExtractPushableCriteria(predicate);
        return this.RowsInferredAsync(tableName, compiled, pushable, progress, cancellationToken);
    }

    /// <summary>Lists indexes whose text columns have a supported Access sort order.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    internal async ValueTask<IReadOnlyList<IndexMetadata>> ListSeekableIndexesAsync(string tableName, CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        await tables.ThrowIfLinkedExecutionUnavailableAsync(tableName, cancellationToken).ConfigureAwait(false);
        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved is null)
        {
            return [];
        }

        IReadOnlyList<IndexMetadata> indexes = await this.ReadIndexesAsync(resolved, cancellationToken).ConfigureAwait(false);
        var result = new List<IndexMetadata>();
        foreach (IndexMetadata index in indexes)
        {
            bool supported = true;
            foreach (IndexColumnReference key in index.Columns)
            {
                ColumnInfo? column = resolved.Definition.Columns.FirstOrDefault(c => c.ColNum == key.ColumnNumber);
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

    /// <summary>Checks join collation and every key before Include chooses an index seek.</summary>
    /// <param name="tableName">The related table.</param>
    /// <param name="index">The candidate index.</param>
    /// <param name="keys">The join keys.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    internal async ValueTask<bool> CanSeekJoinKeysAsync(string tableName, IndexMetadata index, IEnumerable<object?[]> keys, CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        await tables.ThrowIfLinkedExecutionUnavailableAsync(tableName, cancellationToken).ConfigureAwait(false);
        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved is null)
        {
            return false;
        }

        foreach (object?[] key in keys)
        {
            for (int ordinal = 0; ordinal < key.Length && ordinal < index.Columns.Count; ordinal++)
            {
                ColumnInfo? column = resolved.Definition.Columns.FirstOrDefault(value => string.Equals(value.Name, index.Columns[ordinal].Name, StringComparison.OrdinalIgnoreCase));
                if (column?.Type is TextType or MemoType
                    && TextCollationFamily(column.TextSortOrder) != TextCollationFamily(format.DefaultTextSortOrder))
                {
                    // Include matches by database collation. A differently collated
                    // index cannot safely narrow its candidate rows.
                    return false;
                }
            }

            if (!CanEncodePlan(format, resolved.Definition, tableName, new IndexPlan(index, IndexQueryCriteria.KeyPrefix(key), key.Length)))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Drives <see cref="Rows{T}(string, Expression{Func{T, bool}}, IProgress{long}?, CancellationToken)"/>:
    /// plans an index seek from the pushable predicate, streams from the index when
    /// one is usable (or scans otherwise), and applies the fully compiled predicate
    /// to every candidate row. The seek only ever narrows the candidate set, so the
    /// compiled filter guarantees the result is exactly the predicate's matches.
    /// </summary>
    /// <typeparam name="T">The mapped row type.</typeparam>
    /// <param name="tableName">The table to read.</param>
    /// <param name="predicate">The compiled row filter applied to every candidate.</param>
    /// <param name="pushable">The index-seekable necessary conditions extracted from the predicate.</param>
    /// <param name="progress">Optional matched-row-count progress sink.</param>
    /// <param name="cancellationToken">A token used to cancel enumeration.</param>
    private async IAsyncEnumerable<T> RowsInferredAsync<T>(
        string tableName,
        Func<T, bool> predicate,
        RowCriteria pushable,
        IProgress<long>? progress,
        [EnumeratorCancellation] CancellationToken cancellationToken)
        where T : class, new()
    {
        IndexPlan? plan = await this.TryPlanIndexReadAsync(tableName, typeof(T), pushable, cancellationToken).ConfigureAwait(false);

        IAsyncEnumerable<T> candidates = plan is not null
            ? this.ReadIndexRowsAsync<T>(tableName, plan.Index.Name, plan.Criteria, cancellationToken)
            : tables.Rows<T>(tableName, progress: null, cancellationToken);

        long produced = 0;
        await foreach (T item in candidates.ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (predicate(item))
            {
                produced++;
                progress?.Report(produced);
                yield return item;
            }
        }
    }

    /// <summary>
    /// Picks the index that best satisfies <paramref name="pushable"/>, or returns
    /// <see langword="null"/> when a full scan is required (no pushable conditions,
    /// a Jet3 database, a linked or missing table, no comparison a seek answers
    /// exactly, or no covering index).
    /// </summary>
    /// <param name="tableName">The table to read.</param>
    /// <param name="rowType">The type the rows map to, whose properties the conditions compare.</param>
    /// <param name="pushable">The index-seekable necessary conditions.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The chosen index plan, or <see langword="null"/> to scan.</returns>
    private async ValueTask<IndexPlan?> TryPlanIndexReadAsync(
        string tableName,
        Type rowType,
        RowCriteria pushable,
        CancellationToken cancellationToken)
    {
        // Every supported format can use its own index page layout.
        if (pushable.Count == 0 || !this.CanSeek)
        {
            return null;
        }

        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        await tables.ThrowIfLinkedExecutionUnavailableAsync(tableName, cancellationToken).ConfigureAwait(false);
        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            return null;
        }

        // A seek compares the operand with the stored keys, the residual filter with the
        // mapped property; keep only the comparisons on which the two agree.
        RowCriteria seekable = IndexSeekFilter.SelectSeekable(pushable, rowType, resolved.Definition);
        if (seekable.Count == 0)
        {
            return null;
        }

        IReadOnlyList<IndexMetadata> indexes = await this.ReadIndexesAsync(resolved, cancellationToken).ConfigureAwait(false);
        return IndexPlanner.TryPlan(indexes, seekable, plan => CanEncodePlan(format, resolved.Definition, tableName, plan));
    }

    private async ValueTask<IReadOnlyList<IndexMetadata>> ReadIndexesAsync(ResolvedTable resolved, CancellationToken cancellationToken)
    {
        byte[]? td = await tableDefs.ReadTDefBytesAsync(resolved.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        if (td == null || td.Length < format.TDef.BlockEnd)
        {
            return [];
        }

        return IndexCatalogReader.ReadMetadata(format, td, resolved.Definition.Columns);
    }

    private async IAsyncEnumerable<TRow> EnumerateIndexRowsAsync<TRow>(
        string tableName,
        string indexName,
        IndexQueryCriteria criteria,
        Func<TableDef, (Func<object?[], TRow> Factory, bool[]? WantedColumns)> createProjection,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(indexName, nameof(indexName));
        Guard.NotNull(criteria, nameof(criteria));
        Guard.NotNull(createProjection, nameof(createProjection));
        cancellationToken.ThrowIfCancellationRequested();

        await tables.ThrowIfLinkedExecutionUnavailableAsync(tableName, cancellationToken).ConfigureAwait(false);
        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            yield break;
        }

        if (!this.CanSeek)
        {
            throw new NotSupportedException("Index seeks are currently supported for Jet4/ACE databases only.");
        }

        CatalogEntry entry = resolved.Entry;
        TableDef td = resolved.Definition;
        byte[]? tdefBytes = await tableDefs.ReadTDefBytesAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        if (tdefBytes == null || tdefBytes.Length < format.TDef.BlockEnd)
        {
            yield break;
        }

        List<IndexMetadata> indexes = IndexCatalogReader.ReadMetadata(format, tdefBytes, td.Columns);

        IndexMetadata? index = indexes.Find(i => string.Equals(i.Name, indexName, StringComparison.OrdinalIgnoreCase))
            ?? throw new JetObjectNotFoundException(JetErrorCode.IndexNotFound, $"Index '{indexName}' was not found on table '{tableName}'.", nameof(indexName), errorInfo: new JetErrorInfo { TableName = tableName, IndexName = indexName });

        if (index.FirstDp <= 0 || index.Columns.Count == 0)
        {
            yield break;
        }

        var cursor = new IndexCursor(
            format.IndexPage,
            pages.ReadPageAsync,
            format.PageSize);
        IAsyncEnumerable<(long DataPage, int RowIndex)> hits = cursor.EnumerateRowLocationsForCriteriaAsync(format, tableName, index, td, criteria, cancellationToken);

        (Func<object?[], TRow> factory, bool[]? wantedColumns) = createProjection(td);

        bool needsComplexPass = td.HasComplexColumns
            && (wantedColumns == null || TableReader.HasWantedColumnOfType(td.Columns, wantedColumns, ComplexType, AttachmentType));
        bool needsHyperlinkPass = td.HasHyperlinkColumns
            && (wantedColumns == null || TableReader.HasWantedHyperlinkColumn(td.ClrTypes, wantedColumns));
        Dictionary<int, Dictionary<int, byte[]>>? complexData = needsComplexPass
            ? await complexColumns.BuildColumnDataAsync(tableName, td.Columns, wantedColumns, cancellationToken).ConfigureAwait(false)
            : null;
        var decodePlan = RowDecodePlan.CreateTyped(td, wantedColumns, rows.StrictParsing);

        await foreach ((long dataPage, int rowIndex) in hits.ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            object?[]? row = await this.MaterializeSeekRowAsync(
                entry.TDefPage,
                td,
                dataPage,
                rowIndex,
                decodePlan,
                complexData,
                needsComplexPass,
                needsHyperlinkPass,
                cancellationToken).ConfigureAwait(false);
            if (row == null)
            {
                continue;
            }

            yield return factory(row);
        }
    }

    private async ValueTask<object?[]?> MaterializeSeekRowAsync(
        long expectedTDefPage,
        TableDef td,
        long dataPage,
        int rowIndex,
        RowDecodePlan decodePlan,
        Dictionary<int, Dictionary<int, byte[]>>? complexData,
        bool needsComplexPass,
        bool needsHyperlinkPass,
        CancellationToken cancellationToken)
    {
        byte[] page = await pages.ReadPageAsync(dataPage, cancellationToken).ConfigureAwait(false);
        if (page[0] != Constants.PageTypes.Data || Ri32(page, format.DataPage.TDefOff) != expectedTDefPage)
        {
            return null;
        }

        if (!this.TryFindRowEntry(page, dataPage, rowIndex, out RowBound rowBound))
        {
            return null;
        }

        // Index entries name an overflow row's header slot; read the row from
        // the slot the header points at.
        if (rowBound.IsOverflowPointer)
        {
            if (await rows.ResolveOverflowAsync(page, rowBound, cancellationToken).ConfigureAwait(false) is not { } target)
            {
                return null;
            }

            (page, rowBound) = (target.Page, target.Bound);
        }

        if (rowBound.RowSize < format.RowFields.NumCols)
        {
            return null;
        }

        object?[]? row = await rows.CrackRowTypedAsync(page, rowBound.RowStart, rowBound.RowSize, decodePlan, cancellationToken).ConfigureAwait(false);
        if (row == null)
        {
            return null;
        }

        if (needsComplexPass)
        {
            ComplexColumnReader.ResolveColumns(row, td.Columns, complexData);
        }

        if (needsHyperlinkPass)
        {
            TableReader.WrapHyperlinkColumns(row, td.ClrTypes);
        }

        return row;
    }

    private bool TryFindRowEntry(byte[] page, long pageNumber, int rowIndex, out RowBound rowBound)
    {
        foreach (RowBound candidate in pages.GetRowDirectory(pageNumber, page))
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
}
