namespace JetDatabaseWriter;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Interface for writing to Microsoft Access JET databases (.mdb / .accdb).
/// Provides methods for creating tables, inserting, updating, and deleting rows.
/// </summary>
public interface IAccessWriter : IAccessBase
{
    /// <summary>
    /// Asynchronously creates a new table with the specified columns.
    /// Throws if a table with the same name already exists.
    /// </summary>
    /// <param name="tableName">Name of the table to create.</param>
    /// <param name="columns">Column definitions for the new table.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask CreateTableAsync(string tableName, IReadOnlyList<ColumnDefinition> columns, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously creates a new table with the specified columns and the specified
    /// single-column, non-unique, ascending logical indexes. Throws if a table with the
    /// same name already exists.
    /// </summary>
    /// <param name="tableName">Name of the table to create.</param>
    /// <param name="columns">Column definitions for the new table.</param>
    /// <param name="indexes">
    /// Logical-index schema entries to write into the new table's TDEF page chain.
    /// See <see cref="IndexDefinition"/> for the constraints in force today (single
    /// column, non-unique, ascending, Jet4/ACE only). One empty B-tree leaf page is
    /// allocated per index at table-creation time, but the leaf is not maintained
    /// by subsequent insert / update / delete calls and goes stale until Microsoft
    /// Access rebuilds it on the next Compact &amp; Repair pass.
    /// </param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask CreateTableAsync(string tableName, IReadOnlyList<ColumnDefinition> columns, IReadOnlyList<IndexDefinition> indexes, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously drops (deletes) the specified table and all of its data.
    /// Throws if the table does not exist.
    /// </summary>
    /// <param name="tableName">Name of the table to drop.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask DropTableAsync(string tableName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously appends a new column to an existing table. Existing rows receive
    /// <see cref="System.DBNull.Value"/> for the new column. Implemented by copying the
    /// table to a new schema and renaming the result back to <paramref name="tableName"/>.
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="column">The new column definition. Its name must not already exist on the table.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask AddColumnAsync(string tableName, ColumnDefinition column, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously drops the named column from an existing table. The column's data is
    /// permanently lost. Implemented by copying the remaining columns to a new schema and
    /// renaming the result back to <paramref name="tableName"/>. The table must retain at
    /// least one column after the drop.
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="columnName">The column to drop (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask DropColumnAsync(string tableName, string columnName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously renames a column on an existing table. Row data is preserved.
    /// Implemented by copying the table to a new schema and renaming the result back to
    /// <paramref name="tableName"/>.
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="oldColumnName">The current column name (case-insensitive).</param>
    /// <param name="newColumnName">The new column name. Must not already exist on the table.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask RenameColumnAsync(string tableName, string oldColumnName, string newColumnName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously inserts a single row into the specified table.
    /// Values must be in the same order as the table's columns.
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="values">Column values in table-column order.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask InsertRowAsync(string tableName, object[] values, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously inserts a single row by mapping a POCO's properties to the table's columns.
    /// </summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public readable properties match column names.</typeparam>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="item">The object whose properties supply the column values.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask InsertRowAsync<T>(string tableName, T item, CancellationToken cancellationToken = default)
        where T : class, new();

    /// <summary>
    /// Asynchronously inserts multiple rows into the specified table in a single operation.
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="rows">Collection of rows, each containing column values in table-column order.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that yields the number of rows inserted.</returns>
    ValueTask<int> InsertRowsAsync(string tableName, IEnumerable<object[]> rows, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously inserts multiple rows by mapping each POCO's properties to the table's columns.
    /// </summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public readable properties match column names.</typeparam>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="items">Collection of objects whose properties supply the column values.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that yields the number of rows inserted.</returns>
    ValueTask<int> InsertRowsAsync<T>(string tableName, IEnumerable<T> items, CancellationToken cancellationToken = default)
        where T : class, new();

    /// <summary>
    /// Asynchronously updates rows in the specified table where the predicate column matches the given value.
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="predicateColumn">Column name to filter on.</param>
    /// <param name="predicateValue">Value to match in the predicate column, or <see langword="null"/> for IS NULL matching.</param>
    /// <param name="updatedValues">Dictionary of column-name -> new-value pairs to apply.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that yields the number of rows updated.</returns>
    ValueTask<int> UpdateRowsAsync(string tableName, string predicateColumn, object? predicateValue, IReadOnlyDictionary<string, object> updatedValues, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously deletes rows from the specified table where the predicate column matches the given value.
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="predicateColumn">Column name to filter on.</param>
    /// <param name="predicateValue">Value to match in the predicate column, or <see langword="null"/> for IS NULL matching.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that yields the number of rows deleted.</returns>
    ValueTask<int> DeleteRowsAsync(string tableName, string predicateColumn, object? predicateValue, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously creates a linked-table entry (MSysObjects type 4) that references a
    /// table in another Access database. The entry is metadata only — no rows are stored
    /// locally; readers follow <paramref name="sourceDatabasePath"/> /
    /// <paramref name="foreignTableName"/> to retrieve data on demand.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database.</param>
    /// <param name="sourceDatabasePath">Path to the source Access database file (.mdb / .accdb).</param>
    /// <param name="foreignTableName">The name of the table in the source database.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask CreateLinkedTableAsync(string linkedTableName, string sourceDatabasePath, string foreignTableName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously creates a linked-ODBC table entry (MSysObjects type 6) that references
    /// a table accessible via an ODBC connection. The entry is metadata only — no rows are
    /// stored locally. The connection string must use the Access ODBC link format and is
    /// expected to begin with the literal prefix <c>"ODBC;"</c>.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database.</param>
    /// <param name="connectionString">ODBC connection string (e.g. <c>"ODBC;DSN=Sales;UID=app;..."</c> or <c>"ODBC;DRIVER={SQL Server};SERVER=...;..."</c>). The <c>"ODBC;"</c> prefix is added automatically when omitted.</param>
    /// <param name="foreignTableName">The name of the table at the ODBC source.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask CreateLinkedOdbcTableAsync(string linkedTableName, string connectionString, string foreignTableName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously creates a foreign-key relationship between two existing user tables
    /// by appending one row per FK column to the <c>MSysRelationships</c> system table.
    /// The relationship is visible in the Microsoft Access Relationships designer; runtime
    /// enforcement of referential integrity is performed by Microsoft Access (after
    /// Compact &amp; Repair regenerates the per-TDEF FK index entries), not by this library.
    /// </summary>
    /// <param name="relationship">The relationship to create. Both referenced tables and
    /// every named column must already exist; <see cref="RelationshipDefinition.Name"/>
    /// must not duplicate any existing relationship.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="System.NotSupportedException">
    /// Thrown when the database does not contain a <c>MSysRelationships</c> table —
    /// e.g. databases freshly created by <c>AccessWriter.CreateDatabaseAsync</c> do not
    /// include this catalog table. Open an Access-authored fixture or copy one before
    /// declaring relationships.
    /// </exception>
    /// <exception cref="System.InvalidOperationException">
    /// Thrown when a referenced table does not exist or when a relationship with the
    /// same name already exists in the database.
    /// </exception>
    /// <exception cref="System.ArgumentException">
    /// Thrown when a referenced column does not exist on its table.
    /// </exception>
    ValueTask CreateRelationshipAsync(RelationshipDefinition relationship, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously deletes a foreign-key relationship previously created with
    /// <see cref="CreateRelationshipAsync(RelationshipDefinition, CancellationToken)"/>.
    /// Removes every row in <c>MSysRelationships</c> whose <c>szRelationship</c> matches
    /// <paramref name="relationshipName"/> (case-insensitive) and, on Jet4 / ACE
    /// (<c>.accdb</c>) databases, removes the corresponding per-TDEF foreign-key
    /// logical-index entries on both the PK-side and FK-side TDEFs so the next
    /// reader observes the relationship gone immediately (without waiting for a
    /// Microsoft Access Compact &amp; Repair pass).
    /// </summary>
    /// <param name="relationshipName">Case-insensitive relationship name as supplied to
    /// <see cref="CreateRelationshipAsync(RelationshipDefinition, CancellationToken)"/>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="System.NotSupportedException">
    /// Thrown when the database does not contain a <c>MSysRelationships</c> table.
    /// </exception>
    /// <exception cref="System.InvalidOperationException">
    /// Thrown when no relationship named <paramref name="relationshipName"/> exists.
    /// </exception>
    /// <remarks>
    /// W14 limitations: the orphaned real-index slot whose backing leaf was created
    /// for the FK is left in place on the TDEF; Microsoft Access reclaims the
    /// disconnected slot during Compact &amp; Repair. The library does not roll back
    /// runtime cascade-update / cascade-delete enforcement that ran inside the
    /// same call before the drop.
    /// </remarks>
    ValueTask DropRelationshipAsync(string relationshipName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously renames a foreign-key relationship previously created with
    /// <see cref="CreateRelationshipAsync(RelationshipDefinition, CancellationToken)"/>.
    /// Updates the <c>szRelationship</c> column of every matching row in
    /// <c>MSysRelationships</c> (case-insensitive lookup on
    /// <paramref name="oldName"/>). The per-TDEF foreign-key logical-index name
    /// cookies are left at their original value; Microsoft Access reads the
    /// canonical name from the catalog and regenerates the cookies on the next
    /// Compact &amp; Repair pass.
    /// </summary>
    /// <param name="oldName">Case-insensitive existing relationship name.</param>
    /// <param name="newName">New relationship name. Must not match any existing
    /// relationship (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="System.NotSupportedException">
    /// Thrown when the database does not contain a <c>MSysRelationships</c> table.
    /// </exception>
    /// <exception cref="System.InvalidOperationException">
    /// Thrown when no relationship named <paramref name="oldName"/> exists,
    /// or when a relationship named <paramref name="newName"/> already exists.
    /// </exception>
    /// <exception cref="System.ArgumentException">
    /// Thrown when <paramref name="newName"/> is null or empty.
    /// </exception>
    ValueTask RenameRelationshipAsync(string oldName, string newName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously appends one file to a parent row's Access 2007+ Attachment
    /// column. Locates the parent row by composite primary-key tuple, lazily
    /// allocates a per-row <c>ConceptualTableID</c>, patches the parent row's
    /// complex-column slot with that ID, and inserts a row into the hidden flat
    /// child table carrying the wrapper-encoded payload (per
    /// <c>docs/design/complex-columns-format-notes.md</c> §3).
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
    ValueTask AddAttachmentAsync(string tableName, string columnName, IReadOnlyDictionary<string, object> parentRowKey, AttachmentInput attachment, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously appends one value to a parent row's Access 2007+ Multi-Value
    /// column. Locates the parent row by composite primary-key tuple, lazily
    /// allocates a per-row <c>ConceptualTableID</c>, patches the parent row's
    /// complex-column slot with that ID, and inserts a row into the hidden flat
    /// child table whose <c>value</c> column carries <paramref name="value"/>.
    /// </summary>
    /// <param name="tableName">Parent table name (case-insensitive).</param>
    /// <param name="columnName">Name of the Multi-Value column on <paramref name="tableName"/>.</param>
    /// <param name="parentRowKey">Column-name -> value pairs identifying exactly one live row.</param>
    /// <param name="value">Element value, assignment-compatible with the column's <c>MultiValueElementType</c>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="System.NotSupportedException">
    /// Thrown when the database is not ACE (.accdb) or when the named column is
    /// not a declared Multi-Value column.
    /// </exception>
    /// <exception cref="System.InvalidOperationException">
    /// Thrown when no row, or more than one row, matches <paramref name="parentRowKey"/>.
    /// </exception>
    ValueTask AddMultiValueItemAsync(string tableName, string columnName, IReadOnlyDictionary<string, object> parentRowKey, object value, CancellationToken cancellationToken = default);
}
