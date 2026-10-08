namespace JetDatabaseWriter.Interfaces;

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;

/// <summary>
/// Interface for reading Microsoft Access JET databases (.mdb / .accdb).
/// Provides methods for listing tables, reading data, and streaming large datasets.
/// </summary>
/// <remarks>
/// Every typed read (the row streams, the <see cref="DataTable"/> and POCO reads, index
/// seeks and LINQ queries) returns an OLE Object value as its stored bytes, as DAO, ADO
/// and Jackcess do: an object Access inserted keeps Access's OLE header and the OLE
/// object stream around the file or document. <see cref="OleObjectValue"/> unwraps them
/// on request (<see cref="OleObjectValue.GetContent(byte[])"/>). The string reads render
/// an OLE value as a <c>data:</c> URI of the stored bytes, whose media type comes from a
/// file signature at the first byte, or is <c>application/octet-stream</c>.
/// </remarks>
public interface IAccessReader : IAccessBase
{
    /// <summary>Gets a value indicating whether GetUserTables logs verbose hex dumps for debugging. Default: false.</summary>
    public bool DiagnosticsEnabled { get; }

    /// <summary>Gets the maximum number of pages to keep in cache. Positive values enable caching; 0 or negative disables it. Default: 256 (1 MB for 4K pages).</summary>
    public int PageCacheSize { get; }

    /// <summary>Gets the page-I/O optimization mode used by this reader.</summary>
    public PageReadOptimizationMode PageReadOptimizationMode { get; }

    /// <summary>Gets diagnostic output populated after each call to <see cref="ListTablesAsync"/>.</summary>
    public string LastDiagnostics { get; }

    /// <summary>
    /// Asynchronously returns up to <paramref name="maxRows"/> rows (as strings)
    /// from the first user table.
    /// </summary>
    /// <param name="maxRows">Maximum number of rows to read, or <see langword="null"/> for unlimited. Use with large tables to avoid long reads or out-of-memory errors.</param>
    /// <param name="cancellationToken">A token used to cancel the asynchronous operation.</param>
    /// <returns>A <see cref="DataTable"/> with string-typed columns for the first user table, or an empty DataTable if no tables exist.</returns>
    public ValueTask<DataTable> ReadFirstTableAsStringsAsync(uint? maxRows = null, CancellationToken cancellationToken = default);

    /// <summary>Returns the names of all user tables in the database asynchronously.</summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A read-only snapshot of user table names.</returns>
    public ValueTask<IReadOnlyList<string>> ListTablesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns metadata about linked tables (Access-file, text, and ODBC)
    /// found in the database catalog asynchronously.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A read-only snapshot of <see cref="LinkedTableInfo"/> entries.</returns>
    public ValueTask<IReadOnlyList<LinkedTableInfo>> ListLinkedTablesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns name, stored row-count, and column-count for every user table asynchronously.
    /// Calling this instead of <see cref="ListTablesAsync"/> avoids a duplicate catalog scan.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A read-only snapshot of <see cref="TableStat"/> entries.</returns>
    public ValueTask<IReadOnlyList<TableStat>> GetTableStatsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns table metadata as a DataTable with columns: TableName, RowCount, ColumnCount asynchronously.
    /// Ideal for binding to data grids or exporting to CSV/Excel.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="DataTable"/> containing table metadata.</returns>
    public ValueTask<DataTable> GetTablesAsDataTableAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Scans all data pages to count the live rows of the specified table asynchronously, overflow rows (rows Access moved to another page) included.
    /// Rows whose layout is too damaged to decode are not counted, so the result matches the number of rows
    /// <see cref="Rows(string, IProgress{long}?, CancellationToken)"/> and the other table reads return.
    /// This is slower than reading the TDEF RowCount (which may be stale), but always accurate.
    /// Use this after many deletes/imports when `Compact and Repair` hasn't been run.
    /// </summary>
    /// <param name="tableName">Name of the table to count rows for (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the asynchronous operation.</param>
    /// <returns>Number of live rows in the specified table.</returns>
    public ValueTask<long> GetRealRowCountAsync(string tableName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the entire table into a DataTable with properly typed columns asynchronously.
    /// Each column uses its native CLR type (int, DateTime, decimal, etc.).
    /// Prefer <see cref="Rows(string, IProgress{long}?, CancellationToken)"/> or
    /// <see cref="Rows{T}(string, IProgress{long}?, CancellationToken)"/> for bulk processing
    /// when a fully materialized <see cref="DataTable"/> is not required.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive). If null or empty, reads the first table.</param>
    /// <param name="maxRows">Maximum number of rows to read, or <see langword="null"/> for unlimited.</param>
    /// <param name="progress">Optional row-count progress sink.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="DataTable"/> containing the table's data with properly typed columns. Returns an empty DataTable if the table is not found.</returns>
    public ValueTask<DataTable> ReadTableAsync(string? tableName = null, uint? maxRows = null, IProgress<long>? progress = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads up to <paramref name="maxRows"/> rows mapped to <typeparamref name="T"/> asynchronously.
    /// </summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public settable properties map to columns by name, or by <c>[Column("...")]</c> when set; <c>[NotMapped]</c> properties are skipped.</typeparam>
    /// <param name="tableName">The table name.</param>
    /// <param name="maxRows">The max rows.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A read-only snapshot of mapped rows.</returns>
    public ValueTask<IReadOnlyList<T>> ReadTableAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(string tableName, uint? maxRows = null, CancellationToken cancellationToken = default)
        where T : class, new();

    /// <summary>
    /// Reads up to <paramref name="maxRows"/> rows as a string-typed <see cref="DataTable"/> asynchronously.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="maxRows">Maximum number of rows to read, or <c>null</c> for unlimited.</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <param name="cancellationToken">Token used to cancel the asynchronous operation.</param>
    /// <returns>A <see cref="DataTable"/> with all columns typed as <see cref="string"/>.</returns>
    public ValueTask<DataTable> ReadTableAsStringsAsync(string tableName, uint? maxRows = null, IProgress<long>? progress = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns rich metadata for all columns in the specified table asynchronously.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the asynchronous operation.</param>
    /// <returns>A read-only snapshot of <see cref="ColumnMetadata"/> entries describing each column in the table.</returns>
    public ValueTask<IReadOnlyList<ColumnMetadata>> GetColumnMetadataAsync(string tableName, CancellationToken cancellationToken = default);

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
    public ValueTask<IReadOnlyList<IndexMetadata>> ListIndexesAsync(string tableName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns metadata for every foreign-key relationship declared in the database's
    /// <c>MSysRelationships</c> catalog.
    /// </summary>
    /// <remarks>
    /// Each entry links a child (<see cref="RelationshipMetadata.ForeignTable"/>) to a
    /// parent (<see cref="RelationshipMetadata.PrimaryTable"/>), with composite keys listed
    /// in matching column order. Returns an empty list for databases without the
    /// <c>MSysRelationships</c> table. Access-authored Jet3 files can contain relationships.
    /// </remarks>
    /// <param name="cancellationToken">A token used to cancel the asynchronous operation.</param>
    /// <returns>A read-only list of <see cref="RelationshipMetadata"/> entries.</returns>
    public ValueTask<IReadOnlyList<RelationshipMetadata>> ListRelationshipsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Starts an explicit index-backed read query over <paramref name="indexName"/>
    /// and returns matching rows as typed object arrays.
    /// </summary>
    /// <remarks>
    /// Predicates added through the returned query are evaluated by walking the
    /// named Access index. Standard LINQ operators composed over
    /// <see cref="Rows(string, IProgress{long}?, CancellationToken)"/> remain
    /// client-side table scans unless enumeration short-circuits.
    /// </remarks>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="indexName">Index name (case-insensitive).</param>
    /// <returns>A fluent index-query builder.</returns>
    public IAccessIndexQuery<object[]> FromIndex(string tableName, string indexName);

    /// <summary>
    /// Starts an explicit index-backed read query over <paramref name="indexName"/>
    /// and maps matching rows to <typeparamref name="T"/>.
    /// </summary>
    /// <remarks>
    /// Predicates added through the returned query are evaluated by walking the
    /// named Access index. Standard LINQ operators composed over
    /// <see cref="Rows{T}(string, IProgress{long}?, CancellationToken)"/> remain
    /// client-side table scans unless enumeration short-circuits.
    /// </remarks>
    /// <typeparam name="T">A class with a parameterless constructor whose public settable properties map to columns by name, or by <c>[Column("...")]</c> when set; <c>[NotMapped]</c> properties are skipped.</typeparam>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="indexName">Index name (case-insensitive).</param>
    /// <returns>A fluent index-query builder.</returns>
    public IAccessIndexQuery<T> FromIndex<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(string tableName, string indexName)
        where T : class, new();

    /// <summary>
    /// Starts an <see cref="System.Linq.IQueryable{T}"/> entity query over
    /// <paramref name="tableName"/>. Supported operators (<c>Where</c>, <c>OrderBy</c>/
    /// <c>ThenBy</c>, <c>Skip</c>/<c>Take</c>, and the <c>Include</c> extension) translate
    /// to reader operations; <c>Where</c> drives index inference and <c>Include</c> eager-loads
    /// an inferred relationship. Use the async terminal extensions (<c>ToListAsync</c>, …) or
    /// <c>await foreach</c> to execute. <c>Include</c>, <c>ThenInclude</c> and the async
    /// terminals are extension methods in <see cref="JetDatabaseWriter.Linq.AccessQueryExtensions"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The results are async-only: enumerating the query synchronously (<c>foreach</c>,
    /// <c>ToList()</c>) or calling a synchronous LINQ terminal on it (<c>Count()</c>,
    /// <c>First()</c>, <c>Sum(...)</c>, …) throws <see cref="System.NotSupportedException"/>
    /// naming <c>ToListAsync</c>, and the matching async terminal when there is one, before
    /// anything is read.
    /// </para>
    /// <para>
    /// The operators above those (a <c>Select</c> projection and what follows it, or an
    /// ordering with a comparer) run in memory over the rows as the read streams them, with
    /// LINQ to Objects' results. <c>GroupBy</c>, <c>SelectMany</c>, the joins, <c>Zip</c>,
    /// <c>Chunk</c>, the <c>DistinctBy</c>-style set operators and the overloads whose lambdas
    /// take an element index throw <see cref="System.NotSupportedException"/> naming the
    /// operator and <c>AsAsyncEnumerable()</c>, which hands the rows to the async LINQ
    /// operators instead.
    /// </para>
    /// <para>
    /// <c>Include</c> / <c>ThenInclude</c> resolve the related table from the navigation's
    /// target type by name, ignoring case and non-alphanumeric separators (so an
    /// <c>OrderLine</c> type binds to an <c>Order_Line</c> table). Annotate the type with
    /// <c>[Table("ActualName")]</c>
    /// (<see cref="System.ComponentModel.DataAnnotations.Schema.TableAttribute"/>) to bind a
    /// POCO whose name does not match its table. The root <paramref name="tableName"/> is
    /// always passed explicitly, so the root type needs no attribute. Properties bind to
    /// columns by the same rule everywhere: <c>[Column("Last Name")]</c>
    /// (<see cref="System.ComponentModel.DataAnnotations.Schema.ColumnAttribute"/>) maps a
    /// property to a differently named column for materialization, <c>Where</c> index
    /// inference, index-ordered <c>OrderBy</c>, and <c>Include</c> join keys, and
    /// <c>[NotMapped]</c> excludes a property.
    /// </para>
    /// </remarks>
    /// <typeparam name="T">A class with a parameterless constructor whose public settable properties map to columns by name, or by <c>[Column("...")]</c> when set; <c>[NotMapped]</c> properties are skipped.</typeparam>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <returns>A composable query; enumerate with the async terminal extensions or <c>AsAsyncEnumerable()</c>.</returns>
    [RequiresUnreferencedCode("LINQ queries and Include discover entity types and members at runtime. Use typed row readers for trimmed applications.")]
    [RequiresDynamicCode("LINQ queries and Include construct generic types at runtime. Use typed row readers for NativeAOT applications.")]
    public IQueryable<T> Query<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(string tableName)
        where T : class, new();

    /// <summary>
    /// Seeks rows through the named index using an exact key tuple and returns matching rows
    /// as typed object arrays in index order.
    /// </summary>
    /// <remarks>
    /// This is an exact-match seek only; range scans remain a separate concern. The number
    /// and order of <paramref name="keyValues"/> must match the columns returned for the
    /// index by <see cref="ListIndexesAsync"/>.
    /// </remarks>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="indexName">Index name (case-insensitive).</param>
    /// <param name="keyValues">Exact key tuple, one value per indexed column.</param>
    /// <param name="cancellationToken">A token used to cancel asynchronous enumeration.</param>
    /// <returns>An async sequence of typed object arrays whose key equals <paramref name="keyValues"/>.</returns>
    public IAsyncEnumerable<object[]> SeekRowsAsync(
        string tableName,
        string indexName,
        IReadOnlyList<object?> keyValues,
        CancellationToken cancellationToken = default);

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
    public ValueTask<IReadOnlyList<ComplexColumnInfo>> GetComplexColumnsAsync(string tableName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns every attachment row stored in the hidden flat child table backing
    /// the Access 2007+ Attachment column <paramref name="columnName"/> on
    /// <paramref name="tableName"/>. Each result is decoded per
    /// <see href="docs/design/complex-columns-format-notes.md" /> §3 — wrapper stripped
    /// and (when present) deflate decompression applied to <c>FileData</c>.
    /// </summary>
    /// <remarks>
    /// Row reads return the same records per parent row: the column's cell holds them as a
    /// <see cref="ComplexCellValue"/> cell, decoded by <see cref="ComplexCellValue.ReadAttachments(byte[])"/>.
    /// </remarks>
    /// <param name="tableName">Parent table name (case-insensitive).</param>
    /// <param name="columnName">Attachment column name (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the asynchronous operation.</param>
    /// <returns>One <see cref="AttachmentRecord"/> per flat-table row. Empty when the column is unknown, has no rows, or is not an Attachment column.</returns>
    public ValueTask<IReadOnlyList<AttachmentRecord>> GetAttachmentsAsync(string tableName, string columnName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns every value stored in the hidden flat child table backing the
    /// Access 2007+ Multi-Value column <paramref name="columnName"/> on
    /// <paramref name="tableName"/>, paired with the parent row's per-row
    /// complex reference value so callers can group items by parent. For a
    /// Version-history column, each item is one version of the append-only Memo
    /// column, with the time Access recorded it in <see cref="MultiValueItem.Modified"/>,
    /// in the flat table's order.
    /// </summary>
    /// <remarks>
    /// Row reads return the same items per parent row: the column's cell holds them as a
    /// <see cref="ComplexCellValue"/> cell, decoded by <see cref="ComplexCellValue.ReadMultiValueItems(byte[])"/>.
    /// </remarks>
    /// <param name="tableName">Parent table name (case-insensitive).</param>
    /// <param name="columnName">Multi-Value or Version-history column name (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the asynchronous operation.</param>
    /// <returns>One <see cref="MultiValueItem"/> per flat-table row. Empty when the column is unknown or has no rows.</returns>
    public ValueTask<IReadOnlyList<MultiValueItem>> GetMultiValueItemsAsync(string tableName, string columnName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns statistical information about the database asynchronously.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> representing the asynchronous operation.</returns>
    public ValueTask<DatabaseStatistics> GetStatisticsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads all tables into a dictionary of DataTables with properly typed columns asynchronously.
    /// Each table's columns use their native CLR types (int, DateTime, decimal, etc.).
    /// This fully materializes every user table; prefer table-by-table streaming for large databases
    /// unless callers specifically need <see cref="DataTable"/> instances.
    /// </summary>
    /// <param name="progress">Optional row-count progress sink.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A read-only dictionary mapping table names to their corresponding <see cref="DataTable"/> snapshots.</returns>
    public ValueTask<IReadOnlyDictionary<string, DataTable>> ReadAllTablesAsync(IProgress<TableProgress>? progress = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the rows of <paramref name="tableName"/> as a lazily-streamed
    /// <see cref="IAsyncEnumerable{T}"/> of typed object arrays. Compose with the standard
    /// async LINQ operators (<c>Where</c>, <c>Take</c>, <c>Select</c>, <c>ToListAsync</c>,
    /// <c>FirstOrDefaultAsync</c>, <c>CountAsync</c>, …) — no terminal <c>Execute</c> required.
    /// LINQ filtering and projection run client-side and require a table scan unless enumeration
    /// short-circuits; use <see cref="FromIndex(string, string)"/> for explicit index-backed reads.
    /// Ideal for large tables — use <c>await foreach</c> to process one row at a time.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <param name="cancellationToken">A token used to cancel asynchronous enumeration.</param>
    /// <returns>An async sequence of object arrays, each representing a row with typed values.</returns>
    public IAsyncEnumerable<object[]> Rows(string tableName, IProgress<long>? progress = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the rows of <paramref name="tableName"/> mapped to instances of <typeparamref name="T"/>
    /// as a lazily-streamed <see cref="IAsyncEnumerable{T}"/>.
    /// Compose with the standard async LINQ operators. LINQ filtering and projection run client-side
    /// and require a table scan unless enumeration short-circuits; use
    /// <see cref="FromIndex{T}(string, string)"/> for explicit index-backed reads.
    /// </summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public settable properties map to columns by name, or by <c>[Column("...")]</c> when set; <c>[NotMapped]</c> properties are skipped.</typeparam>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <param name="cancellationToken">A token used to cancel asynchronous enumeration.</param>
    /// <returns>An async sequence of <typeparamref name="T"/> instances.</returns>
    public IAsyncEnumerable<T> Rows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(string tableName, IProgress<long>? progress = null, CancellationToken cancellationToken = default)
        where T : class, new();

    /// <summary>
    /// Returns the rows of <paramref name="tableName"/> that satisfy
    /// <paramref name="predicate"/>, mapped to instances of <typeparamref name="T"/>
    /// and lazily streamed.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The reader infers an index from the predicate automatically: when a usable
    /// index covers a leading-key equality (optionally terminated by one range)
    /// the read is index-backed; otherwise it falls back to a table scan. The
    /// outcome is identical either way — inference is a pure optimization — so the
    /// only observable difference is performance and row ordering. Use
    /// <see cref="FromIndex{T}(string, string)"/> to force a specific index or to
    /// guarantee index-ordered results.
    /// </para>
    /// <para>
    /// Only conjuncts combined with <c>&amp;&amp;</c> over direct column members
    /// (<c>o.Column == value</c>, <c>o.Column &gt; value</c>, …) drive index
    /// inference; any other shape is evaluated client-side. <paramref name="progress"/>
    /// reports the count of matched rows yielded so far.
    /// </para>
    /// </remarks>
    /// <typeparam name="T">A class with a parameterless constructor whose public settable properties map to columns by name, or by <c>[Column("...")]</c> when set; <c>[NotMapped]</c> properties are skipped.</typeparam>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="predicate">A row filter expression; drives index inference and the client-side filter.</param>
    /// <param name="progress">Optional progress reporter — receives the matched-row count.</param>
    /// <param name="cancellationToken">A token used to cancel asynchronous enumeration.</param>
    /// <returns>An async sequence of matching <typeparamref name="T"/> instances.</returns>
    public IAsyncEnumerable<T> Rows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(
        string tableName,
        Expression<Func<T, bool>> predicate,
        IProgress<long>? progress = null,
        CancellationToken cancellationToken = default)
        where T : class, new();

    /// <summary>
    /// Returns the rows of <paramref name="tableName"/> as a lazily-streamed
    /// <see cref="IAsyncEnumerable{T}"/> of string arrays.
    /// LINQ filtering and projection run client-side and require a table scan unless enumeration
    /// short-circuits; use <see cref="FromIndex(string, string)"/> for explicit index-backed reads.
    /// </summary>
    /// <param name="tableName">Table name (case-insensitive).</param>
    /// <param name="progress">Optional progress reporter — receives row count after each page.</param>
    /// <param name="cancellationToken">A token used to cancel asynchronous enumeration.</param>
    /// <returns>An async sequence of string arrays.</returns>
    public IAsyncEnumerable<string[]> RowsAsStrings(string tableName, IProgress<long>? progress = null, CancellationToken cancellationToken = default);

    /// <summary>Reads a table's persisted validation expression and message.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The table rule, or null when no rule is stored.</returns>
    public ValueTask<TableValidationRule?> GetTableValidationRuleAsync(string tableName, CancellationToken cancellationToken = default);
}
