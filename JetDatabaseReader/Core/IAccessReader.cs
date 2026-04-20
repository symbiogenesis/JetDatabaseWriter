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

    /// <summary>Gets the diagnostic output populated after each call to <see cref="ListTables"/>.</summary>
    string LastDiagnostics { get; }

    /// <summary>
    /// Returns the column headers and up to <paramref name="maxRows"/> rows
    /// from the first user table, plus the table name and total table count.
    /// </summary>
    /// <returns></returns>
    FirstTableResult ReadFirstTable(int maxRows = 100);

    /// <summary>Returns the names of all user tables in the database.</summary>
    /// <returns></returns>
    List<string> ListTables();

    /// <summary>
    /// Returns metadata about linked tables (Access-linked type 4 and ODBC-linked type 6)
    /// found in the database catalog. These tables reference external data sources.
    /// </summary>
    /// <returns>A list of linked table metadata, or an empty list if none exist.</returns>
    List<LinkedTableInfo> ListLinkedTables();

    /// <summary>
    /// Returns name, stored row-count, and column-count for every user table.
    /// Calling this instead of <see cref="ListTables"/> avoids a duplicate catalog scan.
    /// </summary>
    /// <returns></returns>
    List<TableStat> GetTableStats();

    /// <summary>
    /// Returns table metadata as a DataTable with columns: TableName, RowCount, ColumnCount.
    /// Ideal for binding to data grids or exporting to CSV/Excel.
    /// </summary>
    /// <returns></returns>
    DataTable GetTablesAsDataTable();

    /// <summary>
    /// Scans all data pages to count live (non-deleted, non-overflow) rows for the specified table.
    /// This is slower than reading the TDEF RowCount (which may be stale), but always accurate.
    /// Use this after many deletes/imports when Compact &amp; Repair hasn't been run.
    /// </summary>
    /// <returns></returns>
    long GetRealRowCount(string tableName);

    /// <summary>
    /// Reads up to <paramref name="maxRows"/> rows from the table named
    /// <paramref name="tableName"/> (case-insensitive) with native CLR types.
    /// Rows are in <see cref="TableResult.Rows"/>.
    /// Use <see cref="ReadTableAsStrings"/> when raw string values are needed.
    /// </summary>
    /// <returns></returns>
    TableResult ReadTable(string tableName, int maxRows);

    /// <summary>
    /// Reads up to <paramref name="maxRows"/> rows from <paramref name="tableName"/>
    /// and maps each row to an instance of <typeparamref name="T"/>.
    /// Property names are matched to column headers (case-insensitive).
    /// </summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public settable properties match column names.</typeparam>
    /// <returns></returns>
    List<T> ReadTable<T>(string tableName, int maxRows)
        where T : class, new();

    /// <summary>
    /// Reads up to <paramref name="maxRows"/> rows from the table named
    /// <paramref name="tableName"/> (case-insensitive) with all values as strings.
    /// Rows are in <see cref="StringTableResult.Rows"/>.
    /// </summary>
    /// <returns></returns>
    StringTableResult ReadTableAsStrings(string tableName, int maxRows);

    /// <summary>
    /// Async overload of <see cref="ReadTable(string, int)"/>.
    /// Reads up to <paramref name="maxRows"/> rows with native CLR types asynchronously.
    /// </summary>
    /// <returns>A <see cref="ValueTask{TResult}"/> representing the asynchronous operation.</returns>
    ValueTask<TableResult> ReadTableAsync(string tableName, int maxRows, CancellationToken cancellationToken = default);

    /// <summary>
    /// Async overload of <see cref="ReadTable{T}(string, int)"/>.
    /// Reads up to <paramref name="maxRows"/> rows mapped to <typeparamref name="T"/> asynchronously.
    /// </summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public settable properties match column names.</typeparam>
    /// <returns>A <see cref="ValueTask{TResult}"/> representing the asynchronous operation.</returns>
    ValueTask<List<T>> ReadTableAsync<T>(string tableName, int maxRows, CancellationToken cancellationToken = default)
        where T : class, new();

    /// <summary>
    /// Async overload of <see cref="ReadTableAsStrings(string, int)"/>.
    /// Reads up to <paramref name="maxRows"/> rows as strings asynchronously.
    /// </summary>
    /// <returns>A <see cref="ValueTask{TResult}"/> representing the asynchronous operation.</returns>
    ValueTask<StringTableResult> ReadTableAsStringsAsync(string tableName, int maxRows, CancellationToken cancellationToken = default);

    /// <summary>
    /// Yields rows from <paramref name="tableName"/> as properly typed object arrays without collecting them all in memory.
    /// Each element in the array is the native CLR type (int, DateTime, decimal, etc.).
    /// Ideal for large tables — use foreach to process one row at a time.
    /// This is the recommended method for streaming data.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <returns></returns>
    IEnumerable<object[]> StreamRows(string tableName, IProgress<int>? progress = null);

    /// <summary>
    /// Yields rows from <paramref name="tableName"/> mapped to instances of <typeparamref name="T"/>.
    /// Property names are matched to column headers (case-insensitive). Unmatched properties keep their default value.
    /// </summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public settable properties match column names.</typeparam>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <returns></returns>
    IEnumerable<T> StreamRows<T>(string tableName, IProgress<int>? progress = null)
        where T : class, new();

    /// <summary>
    /// Yields rows from <paramref name="tableName"/> as string arrays without collecting them all in memory.
    /// Use this for compatibility scenarios or when you need raw string data.
    /// For most use cases, prefer <see cref="StreamRows"/> which returns properly typed data.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <returns></returns>
    IEnumerable<string[]> StreamRowsAsStrings(string tableName, IProgress<int>? progress = null);

    /// <summary>
    /// Reads the entire table into a DataTable with all columns typed as strings.
    /// Use this for compatibility scenarios or when you need raw string data.
    /// For most use cases, prefer <see cref="ReadTable(string, IProgress{int})"/> which returns properly typed columns.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive). If null or empty, reads the first table.</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <returns></returns>
    DataTable? ReadTableAsStringDataTable(string? tableName = null, IProgress<int>? progress = null);

    /// <summary>
    /// Returns rich metadata for all columns in the specified table.
    /// </summary>
    /// <returns></returns>
    List<ColumnMetadata> GetColumnMetadata(string tableName);

    /// <summary>
    /// Returns statistical information about the database.
    /// </summary>
    /// <returns></returns>
    DatabaseStatistics GetStatistics();

    /// <summary>
    /// Reads all tables into a dictionary of DataTables with properly typed columns.
    /// Each table's columns use their native CLR types (int, DateTime, decimal, etc.).
    /// This is the recommended method for bulk reading.
    /// </summary>
    /// <returns></returns>
    Dictionary<string, DataTable> ReadAllTables(IProgress<string>? progress = null);

    /// <summary>
    /// Reads all tables into a dictionary of DataTables with all columns typed as strings.
    /// Use this for compatibility scenarios.
    /// </summary>
    /// <returns></returns>
    Dictionary<string, DataTable> ReadAllTablesAsStrings(IProgress<string>? progress = null);

    /// <summary>
    /// Reads the entire table into a DataTable with properly typed columns.
    /// Each column uses its native CLR type (int, DateTime, decimal, etc.).
    /// This is the recommended method for reading table data.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive). If null or empty, reads the first table.</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <returns></returns>
    DataTable? ReadTable(string? tableName = null, IProgress<int>? progress = null);

    // ── Async Methods ──────────────────────────────────────────────────

    /// <summary>Returns the names of all user tables in the database asynchronously.</summary>
    /// <returns>A <see cref="ValueTask{TResult}"/> representing the asynchronous operation.</returns>
    ValueTask<List<string>> ListTablesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the entire table into a DataTable with properly typed columns asynchronously.
    /// Each column uses its native CLR type (int, DateTime, decimal, etc.).
    /// </summary>
    /// <returns>A <see cref="ValueTask{TResult}"/> representing the asynchronous operation.</returns>
    ValueTask<DataTable?> ReadTableAsync(string? tableName = null, IProgress<int>? progress = null, CancellationToken cancellationToken = default);

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
    ///   <item>Typed chain:  <c>Where(obj => ...)</c>          → <c>Execute()</c>          / <c>FirstOrDefault()</c>          / <c>Count()</c></item>
    ///   <item>String chain: <c>WhereAsStrings(str => ...)</c> → <c>ExecuteAsStrings()</c> / <c>FirstOrDefaultAsStrings()</c> / <c>CountAsStrings()</c></item>
    /// </list>
    /// </summary>
    /// <returns></returns>
    TableQuery Query(string tableName);
}
