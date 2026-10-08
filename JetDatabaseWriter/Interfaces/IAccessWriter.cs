namespace JetDatabaseWriter.Interfaces;

using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
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
/// A Jet3 (Access 97) database stores text in its ANSI code page. An insert or
/// update whose Text or Memo value holds a character outside that code page throws
/// <see cref="Exceptions.JetLimitationException"/> before anything is written, where
/// .NET would otherwise store its closest match or <c>?</c>.
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
    /// <param name="values">Column values in table-column order. Every value is stored as given: <see langword="null"/> and <see cref="System.DBNull.Value"/> store database null, as an explicit Null does in an Access SQL INSERT, even when the column has a default value, and a NOT NULL column rejects them. Pass <see cref="DbDefault.Value"/> to store the column's default value instead, or database null when it has none. An AutoNumber column generates its next value, a complex (Attachment / multi-value) column gets the row's per-row complex reference, and a calculated column is computed, for any of the three.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="System.InvalidOperationException">Thrown when a NOT NULL column is null after defaults and AutoNumber values are applied, for example when the row supplies null for it.</exception>
    public ValueTask InsertRowAsync(string tableName, object?[] values, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously inserts a single row by mapping a POCO's properties to the table's columns.
    /// </summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public settable properties map to columns by name, or by <c>[Column("...")]</c> when set; <c>[NotMapped]</c> properties are skipped. A column no readable property maps to is left out of the insert: it gets its next AutoNumber, its per-row complex reference, its computed value or its default value, or database null when it has none of them. A mapped property whose value is <see langword="null"/> stores database null, even when the column has a default value. A non-nullable numeric property mapped to an AutoNumber column, such as <c>int Id</c>, counts as not supplied while it holds 0, its CLR default, so the column generates its next value, as Entity Framework treats a generated key; any other value is stored as given, and a nullable property generates for <see langword="null"/>. To store 0 itself, insert the row as <see cref="RowValues"/> or <c>object[]</c>.</typeparam>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="item">The object whose properties supply the column values.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="System.InvalidOperationException">Thrown when a NOT NULL column is null after defaults and AutoNumber values are applied, for example when the row supplies null for it.</exception>
    public ValueTask InsertRowAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(string tableName, T item, CancellationToken cancellationToken = default)
        where T : class, new();

    /// <summary>
    /// Asynchronously inserts multiple rows into the specified table in a single operation.
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="rows">Collection of rows, each containing column values in table-column order. Every value is stored as given: <see langword="null"/> and <see cref="System.DBNull.Value"/> store database null, as an explicit Null does in an Access SQL INSERT, even when the column has a default value, and a NOT NULL column rejects them. Pass <see cref="DbDefault.Value"/> to store the column's default value instead, or database null when it has none. An AutoNumber column generates its next value, a complex (Attachment / multi-value) column gets the row's per-row complex reference, and a calculated column is computed, for any of the three.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that yields the number of rows inserted.</returns>
    /// <exception cref="System.InvalidOperationException">Thrown when a NOT NULL column is null after defaults and AutoNumber values are applied, for example when the row supplies null for it.</exception>
    public ValueTask<int> InsertRowsAsync(string tableName, IEnumerable<object?[]> rows, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously inserts multiple rows by mapping each POCO's properties to the table's columns.
    /// </summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public settable properties map to columns by name, or by <c>[Column("...")]</c> when set; <c>[NotMapped]</c> properties are skipped. A column no readable property maps to is left out of the insert: it gets its next AutoNumber, its per-row complex reference, its computed value or its default value, or database null when it has none of them. A mapped property whose value is <see langword="null"/> stores database null, even when the column has a default value. A non-nullable numeric property mapped to an AutoNumber column, such as <c>int Id</c>, counts as not supplied while it holds 0, its CLR default, so the column generates its next value, as Entity Framework treats a generated key; any other value is stored as given, and a nullable property generates for <see langword="null"/>. To store 0 itself, insert the row as <see cref="RowValues"/> or <c>object[]</c>.</typeparam>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="items">Collection of objects whose properties supply the column values.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that yields the number of rows inserted.</returns>
    /// <exception cref="System.InvalidOperationException">Thrown when a NOT NULL column is null after defaults and AutoNumber values are applied, for example when the row supplies null for it.</exception>
    public ValueTask<int> InsertRowsAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(string tableName, IEnumerable<T> items, CancellationToken cancellationToken = default)
        where T : class, new();

    /// <summary>
    /// Asynchronously inserts a single row described by named columns. Column identity
    /// is by name rather than position, removing the silent-corruption risk of a
    /// positional <c>object?[]</c> whose order drifts from the schema. Columns not named
    /// are left out of the insert, as columns missing from an Access SQL INSERT column
    /// list are: an AutoNumber column generates its next value, a complex column gets the
    /// row's per-row complex reference, a calculated column is computed, and any other
    /// column stores its default value, or database null when it has none. A named column
    /// set to <see langword="null"/> or <see cref="System.DBNull.Value"/> stores database
    /// null even when it has a default value; set it to <see cref="DbDefault.Value"/> to
    /// store the default.
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="row">The named-column values to insert.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="System.ArgumentException">Thrown when <paramref name="row"/> names a column that does not exist on the table.</exception>
    /// <exception cref="System.InvalidOperationException">Thrown when a NOT NULL column is null after defaults and AutoNumber values are applied, for example when the row supplies null for it.</exception>
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
    /// <exception cref="System.InvalidOperationException">Thrown when a NOT NULL column is null after defaults and AutoNumber values are applied, for example when the row supplies null for it.</exception>
    public ValueTask<int> InsertRowsAsync(string tableName, IEnumerable<RowValues> rows, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously updates rows that satisfy a multi-column filter. The
    /// <see cref="RowCriteria"/> combines one or more column predicates with logical
    /// AND, so filters such as <c>WHERE a = 1 AND b &gt; 2</c>, set membership, and
    /// ranges are expressible directly.
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="criteria">The row filter. An empty criteria matches every row.</param>
    /// <param name="updatedValues">The named columns to assign on each matching row. <see langword="null"/> and <see cref="System.DBNull.Value"/> set a column to database null; <see cref="DbDefault.Value"/> is rejected, because an update never applies a default.</param>
    /// <param name="cancellationToken">A token used to cancel the operation. It is honoured through every read and check, up to the first page write; from then on the update ignores it and runs to completion, cascades, index maintenance and the AutoNumber high-water value included, and returns its count, so a cancel never loses a row or leaves the indexes out of step with the rows. With <see cref="AccessWriterOptions.UseTransactionalWrites"/> and no explicit transaction active, the call's own commit checks the token again before it writes to the file, so a cancel that arrives before the commit starts writing throws and leaves the file unchanged; a call inside an explicit transaction behaves as described above, and the transaction's <see cref="JetTransaction.CommitAsync"/> honours its own token only until its first page write.</param>
    /// <returns>A task that yields the number of rows updated.</returns>
    /// <exception cref="System.ArgumentException">Thrown when the criteria or updated values name a column that does not exist on the table, when an updated value is <see cref="DbDefault.Value"/>, when the updated values name an Attachment or multi-value column (whose items change only through <see cref="AddAttachmentAsync"/> and <see cref="AddMultiValueItemAsync"/>), or when a column's validation rule rejects an assigned value.</exception>
    /// <exception cref="System.InvalidOperationException">Thrown when an assigned NOT NULL or AutoNumber column is set to null, when the update would duplicate a key in a unique index of the table, when the update changes a foreign key of an enforced relationship to a value no row of the related table has, when a relationship the update must check names a table or key column that cannot be found, or when it changes a key that other rows reference and the relationship does not cascade updates. As in Access, a foreign key the update leaves unchanged is not checked. These checks cover every relationship whose key the update changes and run before any row is written, so a rejected update changes nothing, even when another relationship would cascade the change. A cascaded key that would duplicate a key in a unique index of a child table, which a child row left without a parent row can cause, is the exception: it is refused only after the child rows are rewritten, so run updates over such data with <see cref="AccessWriterOptions.UseTransactionalWrites"/> or in a transaction you roll back on failure.</exception>
    /// <exception cref="System.IO.InvalidDataException">Thrown, before any row changes, when a matching row holds a MEMO or OLE value in a column not being assigned whose stored data cannot be read; rewriting the row would lose that value. Assigning a new value to that column is allowed. A cascade update checks every dependent row it would rewrite the same way, in every child table, before any row is written.</exception>
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
    /// <param name="updatedValues">Dictionary of column-name -> new-value pairs to apply. <see langword="null"/> and <see cref="System.DBNull.Value"/> both clear the column to database null. <see cref="DbDefault.Value"/> is rejected, because an update never applies a default.</param>
    /// <param name="cancellationToken">A token used to cancel the operation. It is honoured through every read and check, up to the first page write; from then on the update ignores it and runs to completion, cascades, index maintenance and the AutoNumber high-water value included, and returns its count, so a cancel never loses a row or leaves the indexes out of step with the rows. With <see cref="AccessWriterOptions.UseTransactionalWrites"/> and no explicit transaction active, the call's own commit checks the token again before it writes to the file, so a cancel that arrives before the commit starts writing throws and leaves the file unchanged; a call inside an explicit transaction behaves as described above, and the transaction's <see cref="JetTransaction.CommitAsync"/> honours its own token only until its first page write.</param>
    /// <returns>A task that yields the number of rows updated.</returns>
    /// <exception cref="System.ArgumentException">Thrown when an updated value is <see cref="DbDefault.Value"/>, when the updated values name an Attachment or multi-value column (whose items change only through <see cref="AddAttachmentAsync"/> and <see cref="AddMultiValueItemAsync"/>), or when a column's validation rule rejects an assigned value.</exception>
    /// <exception cref="System.InvalidOperationException">Thrown when an assigned NOT NULL or AutoNumber column is set to null, when the update would duplicate a key in a unique index of the table, when the update changes a foreign key of an enforced relationship to a value no row of the related table has, when a relationship the update must check names a table or key column that cannot be found, or when it changes a key that other rows reference and the relationship does not cascade updates. As in Access, a foreign key the update leaves unchanged is not checked. These checks cover every relationship whose key the update changes and run before any row is written, so a rejected update changes nothing, even when another relationship would cascade the change. A cascaded key that would duplicate a key in a unique index of a child table, which a child row left without a parent row can cause, is the exception: it is refused only after the child rows are rewritten, so run updates over such data with <see cref="AccessWriterOptions.UseTransactionalWrites"/> or in a transaction you roll back on failure.</exception>
    /// <exception cref="System.IO.InvalidDataException">Thrown, before any row changes, when a matching row holds a MEMO or OLE value in a column not being assigned whose stored data cannot be read; rewriting the row would lose that value. Assigning a new value to that column is allowed. A cascade update checks every dependent row it would rewrite the same way, in every child table, before any row is written.</exception>
    public ValueTask<int> UpdateRowsAsync(string tableName, string predicateColumn, object? predicateValue, IReadOnlyDictionary<string, object?> updatedValues, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously deletes rows that satisfy a multi-column filter. The
    /// <see cref="RowCriteria"/> combines one or more column predicates with logical
    /// AND, so filters such as <c>WHERE a = 1 AND b &gt; 2</c>, set membership, and
    /// ranges are expressible directly.
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="criteria">The row filter. An empty criteria matches (and deletes) every row.</param>
    /// <param name="cancellationToken">A token used to cancel the operation. It is honoured through every read and check, up to the first page write; from then on the delete ignores it and runs to completion, cascades, attachment and multi-value rows, row counts and index maintenance included, and returns its count, so a cancel leaves the rows, the row counts and the indexes exactly as a completed delete does. With <see cref="AccessWriterOptions.UseTransactionalWrites"/> and no explicit transaction active, the call's own commit checks the token again before it writes to the file, so a cancel that arrives before the commit starts writing throws and leaves the file unchanged; a call inside an explicit transaction behaves as described above, and the transaction's <see cref="JetTransaction.CommitAsync"/> honours its own token only until its first page write.</param>
    /// <returns>A task that yields the number of rows deleted.</returns>
    /// <exception cref="System.ArgumentException">Thrown when the criteria names a column that does not exist on the table.</exception>
    /// <exception cref="System.InvalidOperationException">Thrown when other rows reference a deleted row's key through an enforced relationship that does not cascade deletes, when a relationship the delete must check names a table or key column that cannot be found, or when cascading deletes nest more than 64 levels deep. These checks cover every table the delete cascades into and run before any row is deleted, so a rejected delete changes nothing.</exception>
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
    /// <param name="cancellationToken">A token used to cancel the operation. It is honoured through every read and check, up to the first page write; from then on the delete ignores it and runs to completion, cascades, attachment and multi-value rows, row counts and index maintenance included, and returns its count, so a cancel leaves the rows, the row counts and the indexes exactly as a completed delete does. With <see cref="AccessWriterOptions.UseTransactionalWrites"/> and no explicit transaction active, the call's own commit checks the token again before it writes to the file, so a cancel that arrives before the commit starts writing throws and leaves the file unchanged; a call inside an explicit transaction behaves as described above, and the transaction's <see cref="JetTransaction.CommitAsync"/> honours its own token only until its first page write.</param>
    /// <returns>A task that yields the number of rows deleted.</returns>
    /// <exception cref="System.InvalidOperationException">Thrown when other rows reference a deleted row's key through an enforced relationship that does not cascade deletes, when a relationship the delete must check names a table or key column that cannot be found, or when cascading deletes nest more than 64 levels deep. These checks cover every table the delete cascades into and run before any row is deleted, so a rejected delete changes nothing.</exception>
    public ValueTask<int> DeleteRowsAsync(string tableName, string predicateColumn, object? predicateValue, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously appends one file to a parent row's Access 2007+ Attachment
    /// column. Locates the parent row by the supplied key columns and inserts a
    /// row into the hidden flat child table carrying the wrapper-encoded payload
    /// (per <see href="docs/design/complex-columns-format-notes.md" /> §3),
    /// joined to the parent through the row's per-row complex reference, which
    /// every inserted row gets from the table's complex AutoNumber. The existing
    /// reference must be positive and covered by the persisted counter; malformed
    /// references are refused before any child row is written.
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
    /// column. Locates the parent row by the supplied key columns and inserts a
    /// row into the hidden flat child table whose <c>value</c> column carries
    /// <paramref name="value"/>, joined to the parent through the row's per-row
    /// complex reference, which every inserted row gets from the table's complex
    /// AutoNumber. The existing reference must be positive and covered by the
    /// persisted counter; malformed references are refused before any child row
    /// is written.
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
