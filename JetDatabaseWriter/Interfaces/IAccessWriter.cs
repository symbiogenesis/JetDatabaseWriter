namespace JetDatabaseWriter.Interfaces;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;

/// <summary>
/// Data Manipulation Language (DML) operations for Microsoft Access JET databases.
/// <para>
/// DML operations read and write user row data — inserting, updating, and deleting rows,
/// as well as managing complex-column child records (attachments, multi-value items).
/// They do not alter the database schema; for table, column, linked-table, and
/// relationship management see <see cref="IAccessSchema"/>.
/// </para>
/// <para>
/// This interface and <see cref="IAccessSchema"/> are independent peers — both extend
/// <see cref="IAccessBase"/> but neither inherits the other. The concrete
/// <c>AccessWriter</c> implements both, so consumers can depend on whichever
/// slice they need: <see cref="IAccessWriter"/> for row-level CRUD,
/// <see cref="IAccessSchema"/> for schema management, or the concrete type for both.
/// </para>
/// <para>
/// Implementations are <em>not</em> thread-safe; callers must serialize DML calls
/// against the same <see cref="IAccessBase"/> instance. DML and DDL operations
/// may be freely interleaved on the same instance but must not overlap concurrently.
/// </para>
/// </summary>
/// <remarks>
/// The interface layout is:
/// <code>
/// IAccessBase       (format metadata, IAsyncDisposable)
///   ├─ IAccessSchema  (DDL: CreateTable, DropTable, AddColumn, …)
///   └─ IAccessWriter  (DML: InsertRow, UpdateRows, DeleteRows, …)
/// </code>
/// Both are implemented by <c>AccessWriter</c>.
/// </remarks>
public interface IAccessWriter : IAccessBase
{
    /// <summary>
    /// Asynchronously inserts a single row into the specified table.
    /// Values must be in the same order as the table's columns.
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="values">Column values in table-column order. <see langword="null"/> and <see cref="System.DBNull.Value"/> both represent database null.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public ValueTask InsertRowAsync(string tableName, object?[] values, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously inserts a single row by mapping a POCO's properties to the table's columns.
    /// </summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public readable properties match column names.</typeparam>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="item">The object whose properties supply the column values.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public ValueTask InsertRowAsync<T>(string tableName, T item, CancellationToken cancellationToken = default)
        where T : class, new();

    /// <summary>
    /// Asynchronously inserts multiple rows into the specified table in a single operation.
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="rows">Collection of rows, each containing column values in table-column order. <see langword="null"/> and <see cref="System.DBNull.Value"/> both represent database null.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that yields the number of rows inserted.</returns>
    public ValueTask<int> InsertRowsAsync(string tableName, IEnumerable<object?[]> rows, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously inserts multiple rows by mapping each POCO's properties to the table's columns.
    /// </summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public readable properties match column names.</typeparam>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="items">Collection of objects whose properties supply the column values.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that yields the number of rows inserted.</returns>
    public ValueTask<int> InsertRowsAsync<T>(string tableName, IEnumerable<T> items, CancellationToken cancellationToken = default)
        where T : class, new();

    /// <summary>
    /// Asynchronously inserts a single row described by named columns. Column identity
    /// is by name rather than position, removing the silent-corruption risk of a
    /// positional <c>object?[]</c> whose order drifts from the schema. Columns not named
    /// are left to the engine's default (an AutoNumber column generates its next value;
    /// any other omitted column stores database null).
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="row">The named-column values to insert.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="System.ArgumentException">Thrown when <paramref name="row"/> names a column that does not exist on the table.</exception>
    public ValueTask InsertRowAsync(string tableName, RowValues row, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously inserts multiple named-column rows in a single operation. See
    /// <see cref="InsertRowAsync(string, RowValues, CancellationToken)"/> for column-resolution semantics.
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="rows">Collection of named-column rows to insert.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that yields the number of rows inserted.</returns>
    /// <exception cref="System.ArgumentException">Thrown when any row names a column that does not exist on the table.</exception>
    public ValueTask<int> InsertRowsAsync(string tableName, IEnumerable<RowValues> rows, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously updates rows that satisfy a multi-column filter. The
    /// <see cref="RowCriteria"/> combines one or more column predicates with logical
    /// AND, so filters such as <c>WHERE a = 1 AND b &gt; 2</c>, set membership, and
    /// ranges are expressible directly.
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="criteria">The row filter. An empty criteria matches every row.</param>
    /// <param name="updatedValues">The named columns to assign on each matching row.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that yields the number of rows updated.</returns>
    /// <exception cref="System.ArgumentException">Thrown when the criteria or updated values name a column that does not exist on the table.</exception>
    public ValueTask<int> UpdateRowsAsync(string tableName, RowCriteria criteria, RowValues updatedValues, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously updates rows in the specified table where the predicate column matches the given value.
    /// </summary>
    /// <remarks>
    /// This is a single-column convenience over
    /// <see cref="UpdateRowsAsync(string, RowCriteria, RowValues, CancellationToken)"/>. Use the
    /// <see cref="RowCriteria"/> overload for multi-column and range filters.
    /// </remarks>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="predicateColumn">Column name to filter on.</param>
    /// <param name="predicateValue">Value to match in the predicate column, or <see langword="null"/> for IS NULL matching.</param>
    /// <param name="updatedValues">Dictionary of column-name -> new-value pairs to apply. <see langword="null"/> and <see cref="System.DBNull.Value"/> both clear the column to database null.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that yields the number of rows updated.</returns>
    public ValueTask<int> UpdateRowsAsync(string tableName, string predicateColumn, object? predicateValue, IReadOnlyDictionary<string, object?> updatedValues, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously deletes rows that satisfy a multi-column filter. The
    /// <see cref="RowCriteria"/> combines one or more column predicates with logical
    /// AND, so filters such as <c>WHERE a = 1 AND b &gt; 2</c>, set membership, and
    /// ranges are expressible directly.
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="criteria">The row filter. An empty criteria matches (and deletes) every row.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that yields the number of rows deleted.</returns>
    /// <exception cref="System.ArgumentException">Thrown when the criteria names a column that does not exist on the table.</exception>
    public ValueTask<int> DeleteRowsAsync(string tableName, RowCriteria criteria, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously deletes rows from the specified table where the predicate column matches the given value.
    /// </summary>
    /// <remarks>
    /// This is a single-column convenience over
    /// <see cref="DeleteRowsAsync(string, RowCriteria, CancellationToken)"/>. Use the
    /// <see cref="RowCriteria"/> overload for multi-column and range filters.
    /// </remarks>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="predicateColumn">Column name to filter on.</param>
    /// <param name="predicateValue">Value to match in the predicate column, or <see langword="null"/> for IS NULL matching.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that yields the number of rows deleted.</returns>
    public ValueTask<int> DeleteRowsAsync(string tableName, string predicateColumn, object? predicateValue, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously appends one file to a parent row's Access 2007+ Attachment
    /// column. Locates the parent row by composite primary-key tuple, lazily
    /// allocates a per-row complex reference value, patches the parent row's
    /// complex-column slot with that ID, and inserts a row into the hidden flat
    /// child table carrying the wrapper-encoded payload (per
    /// <see href="docs/design/complex-columns-format-notes.md" /> §3).
    /// </summary>
    /// <param name="tableName">Parent table name (case-insensitive).</param>
    /// <param name="columnName">Name of the Attachment column on <paramref name="tableName"/>.</param>
    /// <param name="parentRowKey">
    /// Column-name -> value pairs identifying exactly one live row in
    /// <paramref name="tableName"/>. The dictionary may name any subset of columns
    /// sufficient to uniquely match a row; matching is case-insensitive on both
    /// column name and string value.
    /// </param>
    /// <param name="attachment">The file payload to attach.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="System.NotSupportedException">
    /// Thrown when the database is not ACE (.accdb) or when the named column is
    /// not a declared Attachment column.
    /// </exception>
    /// <exception cref="System.InvalidOperationException">
    /// Thrown when no row, or more than one row, matches <paramref name="parentRowKey"/>.
    /// </exception>
    public ValueTask AddAttachmentAsync(string tableName, string columnName, IReadOnlyDictionary<string, object?> parentRowKey, AttachmentInput attachment, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously appends one value to a parent row's Access 2007+ Multi-Value
    /// column. Locates the parent row by composite primary-key tuple, lazily
    /// allocates a per-row complex reference value, patches the parent row's
    /// complex-column slot with that ID, and inserts a row into the hidden flat
    /// child table whose <c>value</c> column carries <paramref name="value"/>.
    /// </summary>
    /// <param name="tableName">Parent table name (case-insensitive).</param>
    /// <param name="columnName">Name of the Multi-Value column on <paramref name="tableName"/>.</param>
    /// <param name="parentRowKey">Column-name -> value pairs identifying exactly one live row.</param>
    /// <param name="value">Element value, assignment-compatible with the column's <c>MultiValueElementType</c>. <see langword="null"/> and <see cref="System.DBNull.Value"/> both represent database null.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="System.NotSupportedException">
    /// Thrown when the database is not ACE (.accdb) or when the named column is
    /// not a declared Multi-Value column.
    /// </exception>
    /// <exception cref="System.InvalidOperationException">
    /// Thrown when no row, or more than one row, matches <paramref name="parentRowKey"/>.
    /// </exception>
    public ValueTask AddMultiValueItemAsync(string tableName, string columnName, IReadOnlyDictionary<string, object?> parentRowKey, object? value, CancellationToken cancellationToken = default);
}
