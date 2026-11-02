namespace JetDatabaseWriter.Tables;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Queries;
using JetDatabaseWriter.Relationships;
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
/// <param name="linked">Delegates index reads to approved Access sources.</param>
/// <param name="operations">The reader's operation gate.</param>
internal sealed class IndexRowReader(
    JetFormat format,
    TableDefReader tableDefs,
    ReaderPageCache pages,
    RowDecoder rows,
    CatalogReader catalog,
    ComplexColumnReader complexColumns,
    TableReader tables,
    LinkedTableReader linked,
    AsyncReentrantOperationGate operations)
{
    private const int IndexCacheCapacity = 256;
    private readonly Dictionary<long, (TDefImage Image, IReadOnlyList<IndexMetadata> Indexes)> indexMetadata = [];

    private static bool CanEncodePlan(JetFormat format, TableDef definition, string tableName, IndexPlan plan)
        => (plan.Criteria.Values is null || IndexKeyEncoder.CanEncodeIndexKeyPrefix(format, tableName, plan.Index, definition, plan.Criteria.Values))
            && (plan.Criteria.Lower is null || IndexKeyEncoder.CanEncodeIndexKeyPrefix(format, tableName, plan.Index, definition, plan.Criteria.Lower.Values))
            && (plan.Criteria.Upper is null || IndexKeyEncoder.CanEncodeIndexKeyPrefix(format, tableName, plan.Index, definition, plan.Criteria.Upper.Values));

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

        LinkedTableKind? linkKind = await linked.GetTableKindAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (linkKind == LinkedTableKind.Text)
        {
            return [];
        }

        if (linkKind == LinkedTableKind.Access)
        {
            return await linked.TryListIndexesAsync(tableName, cancellationToken).ConfigureAwait(false) ?? [];
        }

        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved is null)
        {
            await tables.RequireTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            return [];
        }

        return await this.ReadIndexesAsync(resolved, cancellationToken).ConfigureAwait(false);
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

    internal IAsyncEnumerable<T> ReadIndexRowsAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(
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
    internal IAsyncEnumerable<T> Rows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(
        string tableName,
        Expression<Func<T, bool>> predicate,
        IProgress<long>? progress,
        CancellationToken cancellationToken)
        where T : class, new()
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(predicate, nameof(predicate));

        Func<T, bool> compiled = QueryLambdaCompiler.Shared.Compile(predicate);
        RowCriteria pushable = IndexPredicateTranslator.ExtractPushableCriteria(predicate);
        return this.RowsInferredAsync(tableName, compiled, pushable, progress, cancellationToken);
    }

    /// <summary>Lists indexes whose text columns have a supported Access sort order.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    internal async ValueTask<IReadOnlyList<IndexMetadata>> ListSeekableIndexesAsync(string tableName, CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        ResolvedTable? resolved = await tables.ResolveLocalTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved is null)
        {
            await tables.RequireTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            return [];
        }

        return await this.ListSeekableIndexesAsync(resolved, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Lists seekable indexes against the same acquired schema used by query admission.</summary>
    /// <param name="resolved">The independently resolved local table and its native column types.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    internal async ValueTask<IReadOnlyList<IndexMetadata>> ListSeekableIndexesAsync(ResolvedTable resolved, CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
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
        ResolvedTable? resolved = await tables.ResolveLocalTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved is null)
        {
            await tables.RequireTableAsync(tableName, cancellationToken).ConfigureAwait(false);
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
    private async IAsyncEnumerable<T> RowsInferredAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(
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
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type rowType,
        RowCriteria pushable,
        CancellationToken cancellationToken)
    {
        // Every supported format can use its own index page layout.
        if (pushable.Count == 0 || !this.CanSeek)
        {
            return null;
        }

        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        ResolvedTable? resolved = await tables.ResolveLocalTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            await tables.RequireTableAsync(tableName, cancellationToken).ConfigureAwait(false);
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
        TDefImage? image = await tableDefs.ReadImageAsync(resolved.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        if (image is null)
        {
            return [];
        }

        lock (this.indexMetadata)
        {
            if (this.indexMetadata.TryGetValue(resolved.Entry.TDefPage, out (TDefImage Image, IReadOnlyList<IndexMetadata> Indexes) cached) && ReferenceEquals(cached.Image, image))
            {
                return cached.Indexes;
            }
        }

        List<IndexMetadata> parsed = IndexCatalogReader.ReadMetadata(format, image.CopyBytes(), image.Columns);
        for (int i = 0; i < parsed.Count; i++)
        {
            parsed[i] = parsed[i] with { Columns = Array.AsReadOnly(parsed[i].Columns.ToArray()) };
        }

        IReadOnlyList<IndexMetadata> indexes = Array.AsReadOnly(parsed.ToArray());
        lock (this.indexMetadata)
        {
            if (this.indexMetadata.Count == IndexCacheCapacity && !this.indexMetadata.ContainsKey(resolved.Entry.TDefPage))
            {
                this.indexMetadata.Clear();
            }

            this.indexMetadata[resolved.Entry.TDefPage] = (image, indexes);
        }

        return indexes;
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

        if (await linked.IsLinkedTableAsync(tableName, cancellationToken).ConfigureAwait(false))
        {
            await foreach (TRow row in linked.ReadAccessIndexAsync(tableName, (source, name) => source.EnumerateIndexRowsAsync(name, indexName, criteria, createProjection, cancellationToken), cancellationToken).ConfigureAwait(false))
            {
                yield return row;
                operation.Resume();
            }

            yield break;
        }

        ResolvedTable? resolved = await tables.ResolveLocalTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            await tables.RequireTableAsync(tableName, cancellationToken).ConfigureAwait(false);
            yield break;
        }

        if (!this.CanSeek)
        {
            throw new JetNotSupportedException(JetErrorCode.FeatureNotSupported, "Index seeks are currently supported for Jet4/ACE databases only.", new JetErrorInfo { TableName = tableName, IndexName = indexName });
        }

        CatalogEntry entry = resolved.Entry;
        TableDef td = resolved.Definition;
        IReadOnlyList<IndexMetadata> indexes = await this.ReadIndexesAsync(resolved, cancellationToken).ConfigureAwait(false);

        IndexMetadata? index = indexes.FirstOrDefault(i => string.Equals(i.Name, indexName, StringComparison.OrdinalIgnoreCase))
            ?? throw new JetObjectNotFoundException(JetErrorCode.IndexNotFound, $"Index '{indexName}' was not found on table '{tableName}'.", nameof(indexName), errorInfo: new JetErrorInfo { TableName = tableName, IndexName = indexName });

        if (index.FirstDp <= 0 || index.Columns.Count == 0)
        {
            yield break;
        }

        using var cursor = new IndexCursor(
            format.IndexPage,
            pages.ReadPageAsync,
            format.PageSize);
        IAsyncEnumerable<(long DataPage, int RowIndex)> hits = cursor.EnumerateRowLocationsForCriteriaAsync(format, tableName, index, td, criteria, cancellationToken);

        (Func<object?[], TRow> factory, bool[]? wantedColumns) = createProjection(td);

        bool needsComplexPass = td.HasComplexColumns
            && (wantedColumns == null || TableReader.HasWantedColumnOfType(td.Columns, wantedColumns, ComplexType));
        bool needsHyperlinkPass = td.HasHyperlinkColumns
            && (wantedColumns == null || TableReader.HasWantedHyperlinkColumn(td.ClrTypes, wantedColumns));
        ComplexColumnReader.RowResolver? complexResolver = needsComplexPass
            ? await complexColumns.CreateRowResolverAsync(tableName, td.Columns, wantedColumns, cancellationToken).ConfigureAwait(false)
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
                complexResolver,
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
        ComplexColumnReader.RowResolver? complexResolver,
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

        if (!this.TryFindRowEntry(lease, rowIndex, out RowBound rowBound))
        {
            _ = rows.AdmitRow(readable: false, dataPage, new RowBound(rowIndex, 0, 0));
            return null;
        }

        // Index entries name an overflow row's header slot; read the row from
        // the slot the header points at.
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
        if (row == null)
        {
            _ = rows.AdmitRow(readable: false, dataPage, rowBound);
            return null;
        }

        if (complexResolver is not null)
        {
            await complexResolver.ResolveAsync(row, cancellationToken).ConfigureAwait(false);
        }

        if (needsHyperlinkPass)
        {
            TableReader.WrapHyperlinkColumns(row, td.ClrTypes);
        }

        return row;
    }

    private bool TryFindRowEntry(PageLease lease, int rowIndex, out RowBound rowBound)
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
}
