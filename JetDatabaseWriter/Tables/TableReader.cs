namespace JetDatabaseWriter.Tables;

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Table-data reads behind <see cref="Interfaces.IAccessReader"/>: streams rows
/// as typed arrays, strings, or mapped objects, materializes
/// <see cref="DataTable"/>s, and counts live rows. Scans walk a table's owned
/// data pages through the page cache (with read-ahead when it pays off),
/// resolve complex columns and Hyperlink values, and fall back to
/// <see cref="LinkedTableReader"/> for names that are linked tables. Each
/// operation enters the reader's operation gate so disposal waits for it.
/// </summary>
/// <param name="db">The database page I/O and format context.</param>
/// <param name="pages">The reader's page cache.</param>
/// <param name="rows">Decodes rows from cached pages.</param>
/// <param name="catalog">Resolves user and system tables by name.</param>
/// <param name="complexColumns">Builds the complex-column cells (every attachment or multi-value item of a row) that replace each row's complex reference.</param>
/// <param name="linked">Reads names that resolve to linked tables.</param>
/// <param name="operations">The reader's operation gate.</param>
/// <param name="options">The reader options; supply the read-ahead mode (<see cref="AccessReaderOptions.PageReadOptimizationMode"/>).</param>
internal sealed class TableReader(
    DatabaseFile db,
    ReaderPageCache pages,
    RowDecoder rows,
    CatalogReader catalog,
    ComplexColumnReader complexColumns,
    LinkedTableReader linked,
    AsyncReentrantOperationGate operations,
    AccessReaderOptions options)
{
    private const int MinimumAutoTableScanReadAheadPages = 3;

    /// <summary>
    /// Returns <see langword="true"/> when any column flagged in
    /// <paramref name="wantedColumns"/> has type <paramref name="type1"/> or
    /// <paramref name="type2"/>.
    /// </summary>
    /// <param name="columns">The columns.</param>
    /// <param name="wantedColumns">Optional bitmap selecting columns to decode.</param>
    /// <param name="type1">The type1.</param>
    /// <param name="type2">The type2.</param>
    internal static bool HasWantedColumnOfType(List<ColumnInfo> columns, bool[] wantedColumns, ColumnType type1, ColumnType type2)
    {
        int limit = Math.Min(columns.Count, wantedColumns.Length);
        for (int i = 0; i < limit; i++)
        {
            if (wantedColumns[i] && (columns[i].Type == type1 || columns[i].Type == type2))
            {
                return true;
            }
        }

        return false;
    }

    internal static bool HasWantedHyperlinkColumn(Type[] clrTypes, bool[] wantedColumns)
    {
        int limit = Math.Min(clrTypes.Length, wantedColumns.Length);
        for (int i = 0; i < limit; i++)
        {
            if (wantedColumns[i] && clrTypes[i] == typeof(Hyperlink))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Wraps text payloads of Hyperlink-flagged columns in a typed row into
    /// <see cref="Hyperlink"/> instances, mirroring the projection
    /// <see cref="ResolveClrType"/> exposes via the public API.
    /// Non-string slots (e.g. <see cref="DBNull.Value"/>) are left untouched;
    /// strings that <see cref="Hyperlink.Parse"/> rejects collapse to
    /// <see cref="DBNull.Value"/>.
    /// </summary>
    /// <param name="typedRow">The decoded row.</param>
    /// <param name="clrTypes">The table's per-column CLR types.</param>
    internal static void WrapHyperlinkColumns(object?[] typedRow, Type[] clrTypes)
    {
        int limit = Math.Min(clrTypes.Length, typedRow.Length);
        for (int i = 0; i < limit; i++)
        {
            if (clrTypes[i] != typeof(Hyperlink))
            {
                continue;
            }

            if (typedRow[i] is string s)
            {
                typedRow[i] = (object?)Hyperlink.Parse(s) ?? DBNull.Value;
            }
        }
    }

    /// <summary>
    /// Asynchronously returns up to <paramref name="maxRows"/> rows (as strings)
    /// from the first user table.
    /// </summary>
    /// <param name="maxRows">Maximum number of rows to read, or <see langword="null"/> for unlimited.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<DataTable> ReadFirstTableAsStringsAsync(uint? maxRows, CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        cancellationToken.ThrowIfCancellationRequested();

        List<CatalogEntry> tables = await catalog.GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
        if (tables.Count == 0)
        {
            return new DataTable();
        }

        // Same read as naming the table, so calculated columns get their
        // persisted result types from the catalog before decoding.
        return await this.ReadTableAsStringsAsync(tables[0].Name, maxRows, progress: null, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Counts the rows of a table by scanning its data pages: the live rows,
    /// overflow rows read through their pointer included, whose layout decodes,
    /// which are exactly the rows the table-read APIs return.
    /// </summary>
    /// <param name="tableName">Name of the table to count rows for (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<long> GetRealRowCountAsync(string tableName, CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            long? linkedCount = await linked.TryGetRowCountAsync(tableName, cancellationToken).ConfigureAwait(false);
            return linkedCount ?? 0;
        }

        long count = 0;
        TableDef td = resolved.Definition;
        IReadOnlyList<long> pageNumbers = await db.GetOwnedDataPagesAsync(resolved.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        var decodePlan = RowDecodePlan.CreateTyped(td, wantedColumns: null, rows.StrictParsing);

        await foreach (TableScanPage scanPage in this.EnumerateTableScanPagesAsync(td, pageNumbers, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (RowBound slot in pages.GetRowDirectory(scanPage.PageNumber, scanPage.Page))
            {
                byte[] rowPage = scanPage.Page;
                RowBound rb = slot;
                if (slot.IsOverflowPointer)
                {
                    if (await rows.ResolveOverflowAsync(scanPage.Page, slot, cancellationToken).ConfigureAwait(false) is not { } target)
                    {
                        continue;
                    }

                    (rowPage, rb) = (target.Page, target.Bound);
                }

                if (rb.RowSize >= db.RowFields.NumCols
                    && decodePlan.CanDecodeRow(db, rowPage, rb.RowStart, rb.RowSize))
                {
                    count++;
                }
            }
        }

        return count;
    }

    /// <summary>Streams a table's rows as typed object arrays.</summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="progress">Optional row-count progress sink.</param>
    /// <param name="cancellationToken">A token used to cancel enumeration.</param>
    internal async IAsyncEnumerable<object[]> Rows(
        string tableName,
        IProgress<long>? progress,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            await foreach (object[] row in linked.EnumerateRowsAsync(tableName, progress, cancellationToken).ConfigureAwait(false))
            {
                yield return row;
            }

            yield break;
        }

        CatalogEntry entry = resolved.Entry;
        TableDef td = resolved.Definition;
        await foreach (object?[] row in this.EnumerateTypedRowsAsync(tableName, entry, td, wantedColumns: null, progress, cancellationToken).ConfigureAwait(false))
        {
            yield return (object[])row;
        }
    }

    /// <summary>Streams a table's rows mapped to <typeparamref name="T"/>.</summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public settable properties map to columns by name, or by <c>[Column("...")]</c> when set; <c>[NotMapped]</c> properties are skipped.</typeparam>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="progress">Optional row-count progress sink.</param>
    /// <param name="cancellationToken">A token used to cancel enumeration.</param>
    internal async IAsyncEnumerable<T> Rows<T>(
        string tableName,
        IProgress<long>? progress,
        [EnumeratorCancellation] CancellationToken cancellationToken)
        where T : class, new()
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            await foreach (T? row in linked.EnumerateRowsAsync<T>(tableName, progress, cancellationToken).ConfigureAwait(false))
            {
                yield return row;
            }

            yield break;
        }

        await foreach (T item in this.EnumerateMappedRowsAsync<T>(tableName, resolved, progress, cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }
    }

    /// <summary>Streams a table's rows as strings.</summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="progress">Optional row-count progress sink.</param>
    /// <param name="cancellationToken">A token used to cancel enumeration.</param>
    internal async IAsyncEnumerable<string[]> RowsAsStrings(
        string tableName,
        IProgress<long>? progress,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            await foreach (string[] row in linked.EnumerateRowsAsStringsAsync(tableName, progress, cancellationToken).ConfigureAwait(false))
            {
                yield return row;
            }

            yield break;
        }

        CatalogEntry entry = resolved.Entry;
        TableDef td = resolved.Definition;
        long rowCount = 0;
        Dictionary<int, Dictionary<int, byte[]>>? complexData = td.HasComplexColumns
            ? await complexColumns.BuildColumnDataAsync(tableName, td.Columns, cancellationToken).ConfigureAwait(false)
            : null;
        IReadOnlyList<long> pageNumbers = await db.GetOwnedDataPagesAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        var decodePlan = RowDecodePlan.CreateStrings(td, rows.StrictParsing);

        await foreach (TableScanPage scanPage in this.EnumerateTableScanPagesAsync(td, pageNumbers, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            await foreach (string[] row in rows.EnumerateRowsAsync(scanPage.PageNumber, scanPage.Page, decodePlan, cancellationToken).ConfigureAwait(false))
            {
                if (td.HasComplexColumns)
                {
                    ComplexColumnReader.ResolveStringColumns(row, td.Columns, complexData);
                }

                yield return row;
                rowCount++;
            }

            progress?.Report(rowCount);
        }
    }

    /// <summary>
    /// Reads the entire table into a DataTable with properly typed columns.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive). If null or empty, reads the first table.</param>
    /// <param name="maxRows">Maximum number of rows to read, or <see langword="null"/> for unlimited.</param>
    /// <param name="progress">Optional progress reporter - receives row count after each page.</param>
    /// <param name="cancellationToken">Token used to cancel the asynchronous operation.</param>
    internal ValueTask<DataTable> ReadTableAsync(string? tableName, uint? maxRows, IProgress<long>? progress, CancellationToken cancellationToken)
        => this.ReadDataTableCoreAsync(tableName, maxRows, progress, cancellationToken);

    /// <summary>Reads up to <paramref name="maxRows"/> rows mapped to <typeparamref name="T"/>.</summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public settable properties map to columns by name, or by <c>[Column("...")]</c> when set; <c>[NotMapped]</c> properties are skipped.</typeparam>
    /// <param name="tableName">The table name.</param>
    /// <param name="maxRows">The max rows.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<IReadOnlyList<T>> ReadTableAsync<T>(string tableName, uint? maxRows, CancellationToken cancellationToken)
        where T : class, new()
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            IReadOnlyList<T>? linkedRows = await linked.TryReadTableAsync<T>(tableName, maxRows, cancellationToken).ConfigureAwait(false);
            return linkedRows ?? [];
        }

        // Materialize through the same scan Rows<T> streams from, so both
        // APIs pick the same decoder, projection and complex-column pass.
        var items = new List<T>();
        if (IsRowLimitReached(0, maxRows))
        {
            return items;
        }

        await foreach (T item in this.EnumerateMappedRowsAsync<T>(tableName, resolved, progress: null, cancellationToken).ConfigureAwait(false))
        {
            items.Add(item);
            if (IsRowLimitReached(items.Count, maxRows))
            {
                break;
            }
        }

        return items;
    }

    /// <summary>
    /// Reads up to <paramref name="maxRows"/> rows as a string-typed <see cref="DataTable"/>.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="maxRows">Maximum number of rows to read, or <c>null</c> for unlimited.</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <param name="cancellationToken">Token used to cancel the asynchronous operation.</param>
    internal async ValueTask<DataTable> ReadTableAsStringsAsync(string tableName, uint? maxRows, IProgress<long>? progress, CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        cancellationToken.ThrowIfCancellationRequested();

        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            DataTable? linkedTable = await linked.TryReadTableAsStringsAsync(tableName, maxRows, progress, cancellationToken).ConfigureAwait(false);

#pragma warning disable CA2000 // CA2000: ownership is transferred to the caller through the returned DataTable.
            return linkedTable ?? new DataTable(tableName);
#pragma warning restore CA2000 // CA2000: ownership is transferred to the caller through the returned DataTable.
        }

        CatalogEntry entry = resolved.Entry;
        TableDef td = resolved.Definition;
        DataTable? dt = null;
        try
        {
            dt = new DataTable(tableName);
            foreach (ColumnInfo col in td.Columns)
            {
                _ = dt.Columns.Add(col.Name, typeof(string));
            }

            if (IsRowLimitReached(0, maxRows))
            {
                DataTable empty = dt;
                dt = null;
                return empty;
            }

            Dictionary<int, Dictionary<int, byte[]>>? complexData = td.HasComplexColumns
                ? await complexColumns.BuildColumnDataAsync(tableName, td.Columns, cancellationToken).ConfigureAwait(false)
                : null;
            IReadOnlyList<long> pageNumbers = await db.GetOwnedDataPagesAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
            var decodePlan = RowDecodePlan.CreateStrings(td, rows.StrictParsing);

            await foreach (TableScanPage scanPage in this.EnumerateTableScanPagesAsync(td, pageNumbers, cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                await foreach (string[] row in rows.EnumerateRowsAsync(scanPage.PageNumber, scanPage.Page, decodePlan, cancellationToken).ConfigureAwait(false))
                {
                    if (td.HasComplexColumns)
                    {
                        ComplexColumnReader.ResolveStringColumns(row, td.Columns, complexData);
                    }

                    _ = dt.Rows.Add(row);
                    if (IsRowLimitReached(dt.Rows.Count, maxRows))
                    {
                        progress?.Report(dt.Rows.Count);
                        DataTable result = dt;
                        dt = null;
                        return result;
                    }
                }

                progress?.Report(dt.Rows.Count);
            }

            DataTable final = dt;
            dt = null;
            return final;
        }
        finally
        {
            dt?.Dispose();
        }
    }

    /// <summary>
    /// Reads all user tables into a dictionary of DataTables with properly typed columns.
    /// </summary>
    /// <param name="progress">Optional progress reporter for table read operations.</param>
    /// <param name="cancellationToken">Token used to cancel the asynchronous operation.</param>
    internal async ValueTask<IReadOnlyDictionary<string, DataTable>> ReadAllTablesAsync(IProgress<TableProgress>? progress, CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        cancellationToken.ThrowIfCancellationRequested();

        var result = new Dictionary<string, DataTable>(StringComparer.OrdinalIgnoreCase);
        List<CatalogEntry> tables = await catalog.GetUserTablesAsync(cancellationToken).ConfigureAwait(false);

        for (int i = 0; i < tables.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CatalogEntry table = tables[i];
            progress?.Report(new TableProgress { TableName = table.Name, TableIndex = i, TableCount = tables.Count });
            result[table.Name] = await this.ReadTableAsync(table.Name, maxRows: null, progress: null, cancellationToken).ConfigureAwait(false);
        }

        return result;
    }

    private static void EndDataTableLoad(DataTable table, ref bool dataLoadStarted)
    {
        if (!dataLoadStarted)
        {
            return;
        }

        dataLoadStarted = false;
        table.EndLoadData();
    }

    /// <summary>
    /// Returns whether <paramref name="rowCount"/> rows already satisfy the
    /// caller's <paramref name="maxRows"/> limit. Every bounded read checks it
    /// before scanning (so a limit of 0 returns no rows) and after each row.
    /// </summary>
    /// <param name="rowCount">The rows collected so far.</param>
    /// <param name="maxRows">The caller's row limit, or <see langword="null"/> for unlimited.</param>
    private static bool IsRowLimitReached(long rowCount, uint? maxRows)
        => maxRows.HasValue && rowCount >= maxRows.Value;

    private static int ResolveDataTableMinimumCapacity(long rowCount, uint? maxRows)
    {
        long capacity = rowCount;
        if (maxRows.HasValue)
        {
            long limit = maxRows.Value;
            capacity = capacity > 0 ? Math.Min(capacity, limit) : limit;
        }

        return capacity is > 0 and <= int.MaxValue ? (int)capacity : 0;
    }

    /// <summary>
    /// Returns <see langword="true"/> when the table has a MEMO, OLE, complex
    /// or attachment column, whose scans <see cref="ShouldReadAheadTablePages"/>
    /// leaves sequential.
    /// </summary>
    /// <param name="tableDef">The table definition.</param>
    private static bool HasLongValueOrComplexColumns(TableDef tableDef)
    {
        foreach (ColumnInfo column in tableDef.Columns)
        {
            if (column.Type is MemoType or OleType or ComplexType or AttachmentType)
            {
                return true;
            }
        }

        return false;
    }

    private static async ValueTask ObserveAbandonedTableScanReadAsync(Task<TableScanPage> task)
    {
        if (task.IsCompleted)
        {
            _ = task.Exception;
            return;
        }

        await task.ContinueWith(
            static completed => _ = completed.Exception,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default).ConfigureAwait(false);
    }

    private async ValueTask<DataTable> ReadDataTableCoreAsync(
        string? tableName,
        uint? maxRows,
        IProgress<long>? progress,
        CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        cancellationToken.ThrowIfCancellationRequested();

        if (string.IsNullOrEmpty(tableName))
        {
            List<CatalogEntry> tables = await catalog.GetUserTablesAsync(cancellationToken).ConfigureAwait(false);
            if (tables.Count == 0)
            {
                return new DataTable();
            }

            tableName = tables[0].Name;
        }

        ResolvedTable? resolved = await catalog.ResolveTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (resolved == null)
        {
            DataTable? linkedTable = await linked.TryReadDataTableAsync(tableName, maxRows, progress, cancellationToken).ConfigureAwait(false);

#pragma warning disable CA2000 // CA2000: ownership is transferred to the caller through the returned DataTable.
            return linkedTable ?? new DataTable(tableName);
#pragma warning restore CA2000 // CA2000: ownership is transferred to the caller through the returned DataTable.
        }

        CatalogEntry entry = resolved.Entry;
        TableDef td = resolved.Definition;
        DataTable? dt = null;
        bool dataLoadStarted = false;
        try
        {
            dt = new DataTable(tableName);
            foreach (ColumnInfo col in td.Columns)
            {
                _ = dt.Columns.Add(col.Name, ResolveClrType(col));
            }

            if (IsRowLimitReached(0, maxRows))
            {
                DataTable empty = dt;
                dt = null;
                return empty;
            }

            Dictionary<int, Dictionary<int, byte[]>>? complexData = td.HasComplexColumns
                ? await complexColumns.BuildColumnDataAsync(tableName, td.Columns, cancellationToken).ConfigureAwait(false)
                : null;
            IReadOnlyList<long> pageNumbers = await db.GetOwnedDataPagesAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);

            int minimumCapacity = ResolveDataTableMinimumCapacity(td.RowCount, maxRows);
            if (minimumCapacity > 0)
            {
                dt.MinimumCapacity = minimumCapacity;
            }

            dt.BeginLoadData();
            dataLoadStarted = true;

            // Rent a single object?[] from the shared pool and
            // reuse it across every row. The DataRow ingestion below
            // copies values out via the per-cell setter, so the buffer is
            // never retained by the table.
            int colCount = td.Columns.Count;
            long loadedRows = 0;
            var decodePlan = RowDecodePlan.CreateTyped(td, wantedColumns: null, rows.StrictParsing);
            object?[] rowBuffer = ArrayPool<object?>.Shared.Rent(colCount);
            try
            {
                await foreach (TableScanPage scanPage in this.EnumerateTableScanPagesAsync(td, pageNumbers, cancellationToken).ConfigureAwait(false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    foreach (RowBound slot in pages.GetRowDirectory(scanPage.PageNumber, scanPage.Page))
                    {
                        byte[] rowPage = scanPage.Page;
                        RowBound rb = slot;
                        if (slot.IsOverflowPointer)
                        {
                            if (await rows.ResolveOverflowAsync(scanPage.Page, slot, cancellationToken).ConfigureAwait(false) is not { } target)
                            {
                                continue;
                            }

                            (rowPage, rb) = (target.Page, target.Bound);
                        }

                        if (rb.RowSize < db.RowFields.NumCols)
                        {
                            continue;
                        }

                        bool ok = await rows.CrackRowTypedIntoBufferAsync(rowPage, rb.RowStart, rb.RowSize, decodePlan, rowBuffer, cancellationToken).ConfigureAwait(false);
                        if (!ok)
                        {
                            continue;
                        }

                        if (td.HasComplexColumns)
                        {
                            ComplexColumnReader.ResolveColumns(rowBuffer, td.Columns, complexData);
                        }

                        if (td.HasHyperlinkColumns)
                        {
                            WrapHyperlinkColumns(rowBuffer, td.ClrTypes);
                        }

                        DataRow newRow = dt.NewRow();
                        for (int i = 0; i < colCount; i++)
                        {
                            newRow[i] = rowBuffer[i] ?? DBNull.Value;
                        }

                        dt.Rows.Add(newRow);
                        loadedRows++;
                        if (IsRowLimitReached(loadedRows, maxRows))
                        {
                            progress?.Report(loadedRows);
                            EndDataTableLoad(dt, ref dataLoadStarted);
                            DataTable result = dt;
                            dt = null;
                            return result;
                        }
                    }

                    progress?.Report(loadedRows);
                }
            }
            finally
            {
                ArrayPool<object?>.Shared.Return(rowBuffer, clearArray: true);
            }

            EndDataTableLoad(dt, ref dataLoadStarted);
            DataTable final = dt;
            dt = null;
            return final;
        }
        finally
        {
            if (dt != null && dataLoadStarted)
            {
                EndDataTableLoad(dt, ref dataLoadStarted);
            }

            dt?.Dispose();
        }
    }

    /// <summary>
    /// The mapped-row scan behind <see cref="Rows{T}(string, IProgress{long}?, CancellationToken)"/>
    /// and <see cref="ReadTableAsync{T}(string, uint?, CancellationToken)"/> for a
    /// native table: uses a compiled direct page-to-<typeparamref name="T"/>
    /// decoder when every bound column supports it, and otherwise decodes rows
    /// into a pooled buffer (resolving complex columns and Hyperlinks) and maps them.
    /// </summary>
    /// <typeparam name="T">The mapped row type.</typeparam>
    /// <param name="tableName">The table name, used to load complex-column data.</param>
    /// <param name="resolved">The resolved table.</param>
    /// <param name="progress">Optional row-count progress sink.</param>
    /// <param name="cancellationToken">A token used to cancel enumeration.</param>
    private IAsyncEnumerable<T> EnumerateMappedRowsAsync<T>(
        string tableName,
        ResolvedTable resolved,
        IProgress<long>? progress,
        CancellationToken cancellationToken)
        where T : class, new()
    {
        CatalogEntry entry = resolved.Entry;
        TableDef td = resolved.Definition;

        // Bind the compiled mapper directly against the per-table column
        // headers + ClrTypes; avoids a GetColumnMetadataAsync round-trip.
        string[] headers = new string[td.Columns.Count];
        for (int i = 0; i < td.Columns.Count; i++)
        {
            headers[i] = td.Columns[i].Name;
        }

        // Try to compile a direct page → T decoder that skips the per-row
        // object?[] buffer and primitive boxing entirely. The builder returns
        // null when any bound column requires the slow path (Memo/Ole
        // LVAL chain, Complex/Attachment, Hyperlink prop).
        DirectRowDecoder<T>? directDecoder = td.HasComplexColumns
            ? null
            : DirectRowDecoderBuilder.TryBuild<T>(headers, td.Columns, td.ClrTypes);

        if (directDecoder != null)
        {
            return this.EnumerateDirectRowsAsync(entry, td, directDecoder, progress, cancellationToken);
        }

        Func<object?[], T> factory = RowMapper<T>.Build(headers, td.ClrTypes);

        // Skip per-row decode of columns the mapper never reads. For wide
        // tables and narrow DTOs this can eliminate the bulk of the per-row
        // decode + boxing cost. Tables with complex/attachment columns still
        // decode every column. Complex resolution itself only needs each
        // complex column's own reference, so this could be relaxed.
        bool[]? wantedColumns = td.HasComplexColumns
            ? null
            : RowMapper<T>.GetBoundColumnMask(headers);

        return this.EnumerateMappedRowsPooledAsync(tableName, entry, td, wantedColumns, factory, progress, cancellationToken);
    }

    /// <summary>
    /// Fallback path for <see cref="Rows{T}(string, IProgress{long}?, CancellationToken)"/>:
    /// walks every owned data page for <paramref name="entry"/>, decodes each
    /// row into a single <see cref="ArrayPool{T}.Shared"/>-rented buffer,
    /// applies the mapper, and yields the produced <typeparamref name="T"/>.
    /// The buffer is reused across every row and returned to the pool on
    /// completion (or exception); the mapper consumes values out of the
    /// buffer before the next iteration overwrites it, so no caller ever
    /// observes the pooled array.
    /// </summary>
    /// <typeparam name="T">The mapped row type yielded by the enumerator.</typeparam>
    /// <param name="tableName">The table to stream.</param>
    /// <param name="entry">Catalog entry for the table.</param>
    /// <param name="td">Parsed table definition.</param>
    /// <param name="wantedColumns">Optional bitmap selecting columns to decode.</param>
    /// <param name="factory">Delegate that maps decoded row values to <typeparamref name="T"/>.</param>
    /// <param name="progress">Optional row-count progress sink.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async IAsyncEnumerable<T> EnumerateMappedRowsPooledAsync<T>(
        string tableName,
        CatalogEntry entry,
        TableDef td,
        bool[]? wantedColumns,
        Func<object?[], T> factory,
        IProgress<long>? progress,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        long rowCount = 0;

        bool needsComplexPass = td.HasComplexColumns
            && (wantedColumns == null || HasWantedColumnOfType(td.Columns, wantedColumns, ComplexType, AttachmentType));
        bool needsHyperlinkPass = td.HasHyperlinkColumns
            && (wantedColumns == null || HasWantedHyperlinkColumn(td.ClrTypes, wantedColumns));

        Dictionary<int, Dictionary<int, byte[]>>? complexData = needsComplexPass
            ? await complexColumns.BuildColumnDataAsync(tableName, td.Columns, cancellationToken).ConfigureAwait(false)
            : null;
        IReadOnlyList<long> pageNumbers = await db.GetOwnedDataPagesAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        var decodePlan = RowDecodePlan.CreateTyped(td, wantedColumns, rows.StrictParsing);

        int colCount = td.Columns.Count;
        object?[] rowBuffer = ArrayPool<object?>.Shared.Rent(colCount);
        try
        {
            await foreach (TableScanPage scanPage in this.EnumerateTableScanPagesAsync(td, pageNumbers, cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                foreach (RowBound slot in pages.GetRowDirectory(scanPage.PageNumber, scanPage.Page))
                {
                    byte[] rowPage = scanPage.Page;
                    RowBound rb = slot;
                    if (slot.IsOverflowPointer)
                    {
                        if (await rows.ResolveOverflowAsync(scanPage.Page, slot, cancellationToken).ConfigureAwait(false) is not { } target)
                        {
                            continue;
                        }

                        (rowPage, rb) = (target.Page, target.Bound);
                    }

                    if (rb.RowSize < db.RowFields.NumCols)
                    {
                        continue;
                    }

                    bool ok = await rows.CrackRowTypedIntoBufferAsync(rowPage, rb.RowStart, rb.RowSize, decodePlan, rowBuffer, cancellationToken).ConfigureAwait(false);
                    if (!ok)
                    {
                        continue;
                    }

                    if (needsComplexPass)
                    {
                        ComplexColumnReader.ResolveColumns(rowBuffer, td.Columns, complexData);
                    }

                    if (needsHyperlinkPass)
                    {
                        WrapHyperlinkColumns(rowBuffer, td.ClrTypes);
                    }

                    yield return factory(rowBuffer);
                    rowCount++;
                }

                progress?.Report(rowCount);
            }
        }
        finally
        {
            ArrayPool<object?>.Shared.Return(rowBuffer, clearArray: true);
        }
    }

    /// <summary>
    /// Shared typed-row enumerator used by <see cref="Rows(string, IProgress{long}?, CancellationToken)"/>.
    /// Walks every owned data page for <paramref name="entry"/>, emitting per-row
    /// <c>object?[]</c> buffers with complex-attachment and Hyperlink
    /// post-processing applied (gated by the per-table flags). Centralising
    /// the page scan here keeps the entry point on a single iterator
    /// (one C# async state machine instead of two).
    /// When <paramref name="wantedColumns"/> is non-<see langword="null"/>, only the
    /// flagged column indices are decoded and the complex-attachment / Hyperlink
    /// post-processing passes are skipped when no wanted column is affected by them.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="entry">Catalog entry for the table.</param>
    /// <param name="td">Parsed table definition.</param>
    /// <param name="wantedColumns">Optional bitmap selecting columns to decode.</param>
    /// <param name="progress">Optional row-count progress sink.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async IAsyncEnumerable<object?[]> EnumerateTypedRowsAsync(
        string tableName,
        CatalogEntry entry,
        TableDef td,
        bool[]? wantedColumns,
        IProgress<long>? progress,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        long rowCount = 0;

        // Decide which post-processing passes are needed up front. When a
        // projection mask is supplied, skip a pass entirely if no wanted
        // column requires it; otherwise run with the table-wide flag.
        bool needsComplexPass = td.HasComplexColumns
            && (wantedColumns == null || HasWantedColumnOfType(td.Columns, wantedColumns, ComplexType, AttachmentType));
        bool needsHyperlinkPass = td.HasHyperlinkColumns
            && (wantedColumns == null || HasWantedHyperlinkColumn(td.ClrTypes, wantedColumns));

        Dictionary<int, Dictionary<int, byte[]>>? complexData = needsComplexPass
            ? await complexColumns.BuildColumnDataAsync(tableName, td.Columns, cancellationToken).ConfigureAwait(false)
            : null;
        IReadOnlyList<long> pageNumbers = await db.GetOwnedDataPagesAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        var decodePlan = RowDecodePlan.CreateTyped(td, wantedColumns, rows.StrictParsing);

        await foreach (TableScanPage scanPage in this.EnumerateTableScanPagesAsync(td, pageNumbers, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (RowBound slot in pages.GetRowDirectory(scanPage.PageNumber, scanPage.Page))
            {
                byte[] rowPage = scanPage.Page;
                RowBound rb = slot;
                if (slot.IsOverflowPointer)
                {
                    if (await rows.ResolveOverflowAsync(scanPage.Page, slot, cancellationToken).ConfigureAwait(false) is not { } target)
                    {
                        continue;
                    }

                    (rowPage, rb) = (target.Page, target.Bound);
                }

                if (rb.RowSize < db.RowFields.NumCols)
                {
                    continue;
                }

                object?[]? row = await rows.CrackRowTypedAsync(rowPage, rb.RowStart, rb.RowSize, decodePlan, cancellationToken).ConfigureAwait(false);
                if (row == null)
                {
                    continue;
                }

                if (needsComplexPass)
                {
                    ComplexColumnReader.ResolveColumns(row, td.Columns, complexData);
                }

                if (needsHyperlinkPass)
                {
                    WrapHyperlinkColumns(row, td.ClrTypes);
                }

                yield return row;
                rowCount++;
            }

            progress?.Report(rowCount);
        }
    }

    /// <summary>
    /// Direct-decoder fast-path enumerator: walks every owned data page for
    /// <paramref name="entry"/> and invokes the compiled
    /// <paramref name="directDecoder"/> against each live row, allocating a
    /// fresh <typeparamref name="T"/> per row but no <c>object?[]</c> buffer.
    /// Used by <see cref="Rows{T}(string, IProgress{long}?, CancellationToken)"/>
    /// when every bound column is directly decodable; otherwise the
    /// projection-aware fallback path runs.
    /// </summary>
    /// <typeparam name="T">The row type decoded directly from page bytes.</typeparam>
    /// <param name="entry">Catalog entry for the table.</param>
    /// <param name="td">Parsed table definition.</param>
    /// <param name="directDecoder">Compiled direct-row decoder.</param>
    /// <param name="progress">Optional row-count progress sink.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async IAsyncEnumerable<T> EnumerateDirectRowsAsync<T>(
        CatalogEntry entry,
        TableDef td,
        DirectRowDecoder<T> directDecoder,
        IProgress<long>? progress,
        [EnumeratorCancellation] CancellationToken cancellationToken)
        where T : class, new()
    {
        long rowCount = 0;
        IReadOnlyList<long> pageNumbers = await db.GetOwnedDataPagesAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        var decodePlan = RowDecodePlan.CreateTyped(td, wantedColumns: null, rows.StrictParsing);

        await foreach (TableScanPage scanPage in this.EnumerateTableScanPagesAsync(td, pageNumbers, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach (RowBound slot in pages.GetRowDirectory(scanPage.PageNumber, scanPage.Page))
            {
                byte[] rowPage = scanPage.Page;
                RowBound rb = slot;
                if (slot.IsOverflowPointer)
                {
                    if (await rows.ResolveOverflowAsync(scanPage.Page, slot, cancellationToken).ConfigureAwait(false) is not { } overflow)
                    {
                        continue;
                    }

                    (rowPage, rb) = (overflow.Page, overflow.Bound);
                }

                if (rb.RowSize < db.RowFields.NumCols)
                {
                    continue;
                }

                T target = new();
                if (!decodePlan.TryDecodeDirect(db, rowPage, rb.RowStart, rb.RowSize, directDecoder, target))
                {
                    continue;
                }

                yield return target;
                rowCount++;
            }

            progress?.Report(rowCount);
        }
    }

    private async IAsyncEnumerable<TableScanPage> EnumerateTableScanPagesAsync(
        TableDef tableDef,
        IReadOnlyList<long> pageNumbers,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (!this.ShouldReadAheadTablePages(tableDef, pageNumbers))
        {
            foreach (long pageNumber in pageNumbers)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return await this.ReadTableScanPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            }

            yield break;
        }

        Task<TableScanPage>? nextPageTask = null;
        try
        {
            int pageIndex = 0;
            if (options.PageReadOptimizationMode == PageReadOptimizationMode.Auto)
            {
                yield return await this.ReadTableScanPageAsync(pageNumbers[pageIndex], cancellationToken).ConfigureAwait(false);
                pageIndex++;
            }

            for (; pageIndex < pageNumbers.Count; pageIndex++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                Task<TableScanPage> currentPageTask = nextPageTask
                    ?? this.ReadTableScanPageAsync(pageNumbers[pageIndex], cancellationToken).AsTask();
                nextPageTask = pageIndex + 1 < pageNumbers.Count
                    ? this.ReadTableScanPageAsync(pageNumbers[pageIndex + 1], cancellationToken).AsTask()
                    : null;

                yield return await currentPageTask.ConfigureAwait(false);
            }
        }
        finally
        {
            if (nextPageTask is not null)
            {
                await ObserveAbandonedTableScanReadAsync(nextPageTask).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Determines whether a table scan reads its next data page while the
    /// caller decodes the current one. Auto mode needs a file-backed stream
    /// and at least <see cref="MinimumAutoTableScanReadAheadPages"/> pages, and
    /// yields the first page before prefetch begins to preserve first-row
    /// latency; Enabled needs two pages; Disabled never reads ahead. Reads
    /// through an attached transaction journal stay sequential.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Any page-cache size qualifies, including a disabled cache. Caches under
    /// three pages used to be excluded because the cache returned evicted
    /// buffers to the shared pool while a scan still read them; it now leaves
    /// them to the GC, so the prefetch cannot overwrite the page being decoded.
    /// The two reads in flight can decrypt pages on two threads at once, which
    /// <see cref="Encryption.Models.PageDecryptionKeys"/> serializes for AES.
    /// </para>
    /// <para>
    /// Tables with MEMO, OLE, complex or attachment columns stay sequential.
    /// That is no longer a safety rule, since a long-value read that evicts the
    /// scan's data page cannot overwrite it either, but a measured trade-off:
    /// on a warm 5,000-row MEMO table and a 2,000-row OLE table, read-ahead
    /// changed scan time by less than the run-to-run noise at cache sizes 0, 2
    /// and 256, because each row's long-value page reads dwarf the one data
    /// page the prefetch overlaps. Complex columns were not measured; their
    /// exclusion is kept from before.
    /// </para>
    /// <para>
    /// When the scan runs on a thread-pool thread, a path-opened reader reads
    /// pages on that thread (<see cref="DatabaseFile.ReadsInlineOnThreadPool"/>),
    /// so the prefetch completes before the current page is yielded and no
    /// longer overlaps decode. That still measured faster in each of four
    /// interleaved runs: a warm scan of the 25,000-row numeric table took
    /// 9.5-9.9 ms with inline reads in three of them, against 12.9-13.8 ms with
    /// every read, prefetch included, handed to another pool thread. Other
    /// callers keep the overlap.
    /// </para>
    /// </remarks>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="pageNumbers">The list of page numbers for the table.</param>
    /// <returns><c>true</c> if table pages should be read ahead; otherwise, <c>false</c>.</returns>
    internal bool ShouldReadAheadTablePages(TableDef tableDef, IReadOnlyList<long> pageNumbers) =>
        db.ActiveJournal is null
            && !HasLongValueOrComplexColumns(tableDef)
            && this.HasEligibleTableScanReadAheadPageCount(pageNumbers);

    private bool HasEligibleTableScanReadAheadPageCount(IReadOnlyList<long> pageNumbers) =>
        options.PageReadOptimizationMode switch
        {
            PageReadOptimizationMode.Auto => db.DatabaseStream is FileStream && pageNumbers.Count >= MinimumAutoTableScanReadAheadPages,
            PageReadOptimizationMode.Disabled => false,
            PageReadOptimizationMode.Enabled => pageNumbers.Count > 1,
            _ => false,
        };

    private async ValueTask<TableScanPage> ReadTableScanPageAsync(long pageNumber, CancellationToken cancellationToken)
    {
        byte[] page = await pages.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        return new TableScanPage(pageNumber, page);
    }

    private readonly record struct TableScanPage(long PageNumber, byte[] Page);
}
