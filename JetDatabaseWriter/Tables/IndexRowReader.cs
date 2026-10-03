namespace JetDatabaseWriter.Tables;

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
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
/// <param name="db">The database page I/O and format context.</param>
/// <param name="pages">The reader's page cache, which index and data pages are read through.</param>
/// <param name="rows">Decodes the rows an index points at.</param>
/// <param name="catalog">Resolves tables by name.</param>
/// <param name="complexColumns">Loads attachment payloads for complex columns.</param>
/// <param name="tables">Scans the table when no index covers a predicate.</param>
/// <param name="operations">The reader's operation gate.</param>
internal sealed class IndexRowReader(
    DatabaseFile db,
    ReaderPageCache pages,
    RowDecoder rows,
    CatalogReader catalog,
    ComplexColumnReader complexColumns,
    TableReader tables,
    AsyncReentrantOperationGate operations)
{
    /// <summary>Gets a value indicating whether the database format supports index seeks (Jet4 / ACE only).</summary>
    internal bool CanSeek => db.Format != DatabaseFormat.Jet3Mdb;

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
        if (resolved == null)
        {
            return [];
        }

        byte[]? td = await db.ReadTDefBytesAsync(resolved.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        if (td == null || td.Length < db.TDef.BlockEnd)
        {
            return [];
        }

        return IndexCatalogReader.ReadMetadata(db, td, resolved.Definition.Columns);
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

                Func<object?[], T> factory = RowMapper<T>.Build(headers, td.ClrTypes);
                bool[]? wantedColumns = td.HasComplexColumns
                    ? null
                    : RowMapper<T>.GetBoundColumnMask(headers);

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
        IndexPlan? plan = await this.TryPlanIndexReadAsync(tableName, pushable, cancellationToken).ConfigureAwait(false);

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
    /// a Jet3 database, a linked or missing table, or no covering index).
    /// </summary>
    /// <param name="tableName">The table to read.</param>
    /// <param name="pushable">The index-seekable necessary conditions.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The chosen index plan, or <see langword="null"/> to scan.</returns>
    private async ValueTask<IndexPlan?> TryPlanIndexReadAsync(
        string tableName,
        RowCriteria pushable,
        CancellationToken cancellationToken)
    {
        // Index seeks are Jet4/ACE-only; everything else falls back to a scan.
        if (pushable.Count == 0 || !this.CanSeek)
        {
            return null;
        }

        IReadOnlyList<IndexMetadata> indexes = await this.ListIndexesAsync(tableName, cancellationToken).ConfigureAwait(false);
        return IndexPlanner.TryPlan(indexes, pushable);
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
        byte[]? tdefBytes = await db.ReadTDefBytesAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        if (tdefBytes == null || tdefBytes.Length < db.TDef.BlockEnd)
        {
            yield break;
        }

        List<IndexMetadata> indexes = IndexCatalogReader.ReadMetadata(db, tdefBytes, td.Columns);

        IndexMetadata? index = indexes.Find(i => string.Equals(i.Name, indexName, StringComparison.OrdinalIgnoreCase))
            ?? throw new ArgumentException($"Index '{indexName}' was not found on table '{tableName}'.", nameof(indexName));

        if (index.FirstDp <= 0 || index.Columns.Count == 0)
        {
            yield break;
        }

        var cursor = new IndexCursor(
            pages.ReadPageAsync,
            db.PageSizeBytes);
        List<(long DataPage, int RowIndex)> hits = await cursor.FindRowLocationsForCriteriaAsync(
            db.Format,
            tableName,
            index,
            td,
            criteria,
            cancellationToken).ConfigureAwait(false);

        (Func<object?[], TRow> factory, bool[]? wantedColumns) = createProjection(td);

        bool needsComplexPass = td.HasComplexColumns
            && (wantedColumns == null || TableReader.HasWantedColumnOfType(td.Columns, wantedColumns, ComplexType, AttachmentType));
        bool needsHyperlinkPass = td.HasHyperlinkColumns
            && (wantedColumns == null || TableReader.HasWantedHyperlinkColumn(td.ClrTypes, wantedColumns));
        Dictionary<int, Dictionary<int, byte[]>>? complexData = needsComplexPass
            ? await complexColumns.BuildColumnDataAsync(tableName, td.Columns, cancellationToken).ConfigureAwait(false)
            : null;
        var decodePlan = RowDecodePlan.CreateTyped(td, wantedColumns, rows.StrictParsing);

        foreach ((long dataPage, int rowIndex) in hits)
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
        if (page[0] != Constants.PageTypes.Data || Ri32(page, db.DataPage.TDefOff) != expectedTDefPage)
        {
            return null;
        }

        if (!this.TryFindLiveRowBound(page, dataPage, rowIndex, out RowBound rowBound) || rowBound.RowSize < db.RowFields.NumCols)
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

    private bool TryFindLiveRowBound(byte[] page, long pageNumber, int rowIndex, out RowBound rowBound)
    {
        foreach (RowBound candidate in pages.GetLiveRowBounds(pageNumber, page))
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
