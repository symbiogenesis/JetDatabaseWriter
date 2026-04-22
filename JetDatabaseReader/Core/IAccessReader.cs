namespace JetDatabaseReader;

using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Interface for reading Microsoft Access JET databases (.mdb / .accdb).
/// Provides methods for listing tables, reading data, and streaming large datasets.
/// </summary>
public interface IAccessReader : IAccessBase
{
    /// <summary>Gets a value indicating whether GetUserTables logs verbose hex dumps for debugging. Default: false.</summary>
    bool DiagnosticsEnabled { get; }

    /// <summary>Gets the maximum number of pages to keep in cache. 0 = unlimited, -1 = disabled. Default: 256 (1 MB for 4K pages).</summary>
    int PageCacheSize { get; }

    /// <summary>Gets a value indicating whether parallel processing is used for reading multiple pages. Can improve performance for large tables. Default: false.</summary>
    bool ParallelPageReadsEnabled { get; }

    /// <summary>Gets diagnostic output populated after each call to <see cref="ListTablesAsync"/>.</summary>
    string LastDiagnostics { get; }

    /// <summary>
    /// Asynchronously returns up to <paramref name="maxRows"/> rows (as strings)
    /// from the first user table.
    /// </summary>
    /// <param name="maxRows">Maximum number of rows to read, or <see langword="null"/> for unlimited. Use with large tables to avoid long reads or out-of-memory errors.</param>
    /// <param name="cancellationToken">A token used to cancel the asynchronous operation.</param>
    /// <returns>A <see cref="DataTable"/> with string-typed columns for the first user table, or an empty DataTable if no tables exist.</returns>
    ValueTask<DataTable> ReadFirstTableAsync(uint? maxRows = null, CancellationToken cancellationToken = default);

    /// <summary>Returns the names of all user tables in the database asynchronously.</summary>
    /// <returns>A <see cref="ValueTask{TResult}"/> representing the asynchronous operation.</returns>
    ValueTask<List<string>> ListTablesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns metadata about linked tables (Access-linked type 4 and ODBC-linked type 6)
    /// found in the database catalog asynchronously.
    /// </summary>
    /// <returns>A list of <see cref="LinkedTableInfo"/> with metadata for each linked table.</returns>
    ValueTask<List<LinkedTableInfo>> ListLinkedTablesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns name, stored row-count, and column-count for every user table asynchronously.
    /// Calling this instead of <see cref="ListTablesAsync"/> avoids a duplicate catalog scan.
    /// </summary>
    /// <returns>A list of <see cref="TableStat"/> with metadata for each user table.</returns>
    ValueTask<List<TableStat>> GetTableStatsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns table metadata as a DataTable with columns: TableName, RowCount, ColumnCount asynchronously.
    /// Ideal for binding to data grids or exporting to CSV/Excel.
    /// </summary>
    /// <returns>A <see cref="DataTable"/> containing table metadata.</returns>
    ValueTask<DataTable> GetTablesAsDataTableAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Scans all data pages to count live (non-deleted, non-overflow) rows for the specified table asynchronously.
    /// This is slower than reading the TDEF RowCount (which may be stale), but always accurate.
    /// Use this after many deletes/imports when `Compact and Repair` hasn't been run.
    /// </summary>
    /// <param name="tableName">Name of the table to count rows for (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the asynchronous operation.</param>
    /// <returns>Number of live rows in the specified table.</returns>
    ValueTask<long> GetRealRowCountAsync(string tableName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads up to <paramref name="maxRows"/> rows mapped to <typeparamref name="T"/> asynchronously.
    /// </summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public settable properties match column names.</typeparam>
    /// <returns>A <see cref="ValueTask{TResult}"/> representing the asynchronous operation.</returns>
    ValueTask<List<T>> ReadTableAsync<T>(string tableName, uint? maxRows = null, CancellationToken cancellationToken = default)
        where T : class, new();

    /// <summary>
    /// Reads up to <paramref name="maxRows"/> rows as a string-typed <see cref="DataTable"/> asynchronously.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="maxRows">Maximum number of rows to read, or <c>null</c> for unlimited.</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <param name="cancellationToken">Token used to cancel the asynchronous operation.</param>
    /// <returns>A <see cref="DataTable"/> with all columns typed as <see cref="string"/>.</returns>
    ValueTask<DataTable> ReadTableAsStringsAsync(string tableName, uint? maxRows = null, IProgress<int>? progress = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously yields rows from <paramref name="tableName"/> as properly typed object arrays without collecting them all in memory.
    /// Each element in the array is the native CLR type (int, DateTime, decimal, etc.).
    /// Ideal for large tables — use foreach to process one row at a time.
    /// This is the recommended method for streaming data.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <param name="cancellationToken">A token used to cancel asynchronous enumeration.</param>
    /// <returns>An enumerable of object arrays, each representing a row with typed values.</returns>
    IAsyncEnumerable<object[]> StreamRowsAsync(string tableName, IProgress<int>? progress = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously yields rows from <paramref name="tableName"/> mapped to instances of <typeparamref name="T"/>.
    /// </summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public settable properties match column names.</typeparam>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="progress">Optional progress reporter - receives row count after each page.</param>
    /// <param name="cancellationToken">A token used to cancel asynchronous enumeration.</param>
    /// <returns>An enumerable of <typeparamref name="T"/> instances, each representing a row.</returns>
    IAsyncEnumerable<T> StreamRowsAsync<T>(string tableName, IProgress<int>? progress = null, CancellationToken cancellationToken = default)
        where T : class, new();

    /// <summary>
    /// Asynchronously yields rows from <paramref name="tableName"/> as string arrays
    /// without collecting them all in memory.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="progress">Optional progress reporter - receives row count after each page.</param>
    /// <param name="cancellationToken">A token used to cancel asynchronous enumeration.</param>
    /// <returns>An enumerable of string arrays, each representing a row.</returns>
    IAsyncEnumerable<string[]> StreamRowsAsStringsAsync(string tableName, IProgress<int>? progress = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns rich metadata for all columns in the specified table asynchronously.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the asynchronous operation.</param>
    /// <returns>A list of <see cref="ColumnMetadata"/> objects describing each column in the table.</returns>
    ValueTask<List<ColumnMetadata>> GetColumnMetadataAsync(string tableName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the entire table into a DataTable with properly typed columns asynchronously.
    /// Each column uses its native CLR type (int, DateTime, decimal, etc.).
    /// </summary>
    /// <returns>A <see cref="DataTable"/> containing the table's data with properly typed columns. Returns an empty DataTable if the table is not found.</returns>
    ValueTask<DataTable> ReadDataTableAsync(string? tableName = null, uint? maxRows = null, IProgress<int>? progress = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns statistical information about the database asynchronously.
    /// </summary>
    /// <returns>A <see cref="ValueTask{TResult}"/> representing the asynchronous operation.</returns>
    ValueTask<DatabaseStatistics> GetStatisticsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads all tables into a dictionary of DataTables with properly typed columns asynchronously.
    /// Each table's columns use their native CLR types (int, DateTime, decimal, etc.).
    /// </summary>
    /// <returns>A <see cref="ValueTask{TResult}"/> representing the asynchronous operation.</returns>
    ValueTask<Dictionary<string, DataTable>> ReadAllTablesAsync(IProgress<string>? progress = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads all tables into a dictionary of DataTables with all columns typed as strings asynchronously.
    /// Use this for compatibility scenarios.
    /// </summary>
    /// <returns>A <see cref="ValueTask{TResult}"/> representing the asynchronous operation.</returns>
    ValueTask<Dictionary<string, DataTable>> ReadAllTablesAsStringsAsync(IProgress<string>? progress = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a fluent query interface for the specified table.
    /// Supports both typed and string row access:
    /// <list type="bullet">
    ///   <item>Typed chain:  <c>Where(obj => ...)</c>          → <c>ExecuteAsync()</c>          / <c>FirstOrDefaultAsync()</c>          / <c>CountAsync()</c></item>
    ///   <item>String chain: <c>WhereAsStrings(str => ...)</c> → <c>ExecuteAsStringsAsync()</c> / <c>FirstOrDefaultAsStringsAsync()</c> / <c>CountAsStringsAsync()</c></item>
    /// </list>
    /// </summary>
    /// <returns></returns>
    TableQuery Query(string tableName);
}
