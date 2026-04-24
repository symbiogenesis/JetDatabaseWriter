namespace JetDatabaseWriter;

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
    ValueTask<DataTable> ReadTableAsStringsAsync(string tableName, uint? maxRows = null, IProgress<long>? progress = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns rich metadata for all columns in the specified table asynchronously.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the asynchronous operation.</param>
    /// <returns>A list of <see cref="ColumnMetadata"/> objects describing each column in the table.</returns>
    ValueTask<List<ColumnMetadata>> GetColumnMetadataAsync(string tableName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns metadata for every logical index defined on <paramref name="tableName"/>,
    /// parsed from the table's TDEF page chain.
    /// </summary>
    /// <remarks>
    /// Only schema metadata is surfaced — the index B-tree leaf pages are not traversed.
    /// Multiple logical indexes may share the same physical (real) index; consult
    /// <see cref="IndexMetadata.RealIndexNumber"/> to detect that sharing. Returns an
    /// empty list when the table has no indexes or cannot be resolved.
    /// </remarks>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the asynchronous operation.</param>
    /// <returns>A read-only list of <see cref="IndexMetadata"/> entries in TDEF order.</returns>
    ValueTask<IReadOnlyList<IndexMetadata>> ListIndexesAsync(string tableName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns metadata for every Access 2007+ "complex" column (Attachment,
    /// Multi-value, Version-history) declared on <paramref name="tableName"/>.
    /// </summary>
    /// <remarks>
    /// Joins the parent TDEF column descriptors with <c>MSysComplexColumns</c> to
    /// expose the per-column <c>ComplexID</c>, the hidden flat child-table name and
    /// page, and the column subtype. Returns an empty list for tables that contain
    /// no complex columns or for older Jet3 / Jet4 databases.
    /// </remarks>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the asynchronous operation.</param>
    /// <returns>A read-only list of <see cref="ComplexColumnInfo"/> entries, one per complex column on the table.</returns>
    ValueTask<IReadOnlyList<ComplexColumnInfo>> GetComplexColumnsAsync(string tableName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the entire table into a DataTable with properly typed columns asynchronously.
    /// Each column uses its native CLR type (int, DateTime, decimal, etc.).
    /// </summary>
    /// <returns>A <see cref="DataTable"/> containing the table's data with properly typed columns. Returns an empty DataTable if the table is not found.</returns>
    ValueTask<DataTable> ReadDataTableAsync(string? tableName = null, uint? maxRows = null, IProgress<long>? progress = null, CancellationToken cancellationToken = default);

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
    ValueTask<Dictionary<string, DataTable>> ReadAllTablesAsync(IProgress<TableProgress>? progress = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads all tables into a dictionary of DataTables with all columns typed as strings asynchronously.
    /// Use this for compatibility scenarios.
    /// </summary>
    /// <returns>A <see cref="ValueTask{TResult}"/> representing the asynchronous operation.</returns>
    ValueTask<Dictionary<string, DataTable>> ReadAllTablesAsStringsAsync(IProgress<TableProgress>? progress = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the rows of <paramref name="tableName"/> as a lazily-streamed
    /// <see cref="IAsyncEnumerable{T}"/> of typed object arrays. Compose with the standard
    /// async LINQ operators (<c>Where</c>, <c>Take</c>, <c>Select</c>, <c>ToListAsync</c>,
    /// <c>FirstOrDefaultAsync</c>, <c>CountAsync</c>, …) — no terminal <c>Execute</c> required.
    /// Ideal for large tables — use <c>await foreach</c> to process one row at a time.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <param name="cancellationToken">A token used to cancel asynchronous enumeration.</param>
    /// <returns>An async sequence of object arrays, each representing a row with typed values.</returns>
    IAsyncEnumerable<object[]> Rows(string tableName, IProgress<long>? progress = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the rows of <paramref name="tableName"/> mapped to instances of <typeparamref name="T"/>
    /// as a lazily-streamed <see cref="IAsyncEnumerable{T}"/>.
    /// Compose with the standard async LINQ operators.
    /// </summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public settable properties match column names.</typeparam>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <param name="cancellationToken">A token used to cancel asynchronous enumeration.</param>
    /// <returns>An async sequence of <typeparamref name="T"/> instances.</returns>
    IAsyncEnumerable<T> Rows<T>(string tableName, IProgress<long>? progress = null, CancellationToken cancellationToken = default)
        where T : class, new();

    /// <summary>
    /// Returns the rows of <paramref name="tableName"/> as a lazily-streamed
    /// <see cref="IAsyncEnumerable{T}"/> of string arrays.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <param name="cancellationToken">A token used to cancel asynchronous enumeration.</param>
    /// <returns>An async sequence of string arrays.</returns>
    IAsyncEnumerable<string[]> RowsAsStrings(string tableName, IProgress<long>? progress = null, CancellationToken cancellationToken = default);
}
