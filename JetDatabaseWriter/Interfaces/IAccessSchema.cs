namespace JetDatabaseWriter.Interfaces;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;

/// <summary>
/// Data Definition Language (DDL) operations for Microsoft Access JET databases.
/// <para>
/// DDL operations modify the database schema — creating or dropping tables, adding or
/// removing columns, managing linked-table references, and declaring foreign-key
/// relationships. They do not touch user row data directly (although some operations
/// like <see cref="DropTableAsync"/> implicitly destroy all rows in the target table,
/// and schema-evolution methods like <see cref="AddColumnAsync"/> perform an internal
/// copy-and-swap that preserves existing rows).
/// </para>
/// <para>
/// Implementations are <em>not</em> thread-safe; callers must serialize DDL calls
/// against the same <see cref="IAccessBase"/> instance. DDL and DML operations
/// (see <see cref="IAccessWriter"/>) may be freely interleaved on the same instance
/// but must not overlap concurrently.
/// </para>
/// </summary>
/// <remarks>
/// This interface is the DDL half of the writer surface. The DML half is
/// <see cref="IAccessWriter"/>. Both are implemented by <c>AccessWriter</c>, which
/// also exposes <see cref="IAccessBase"/> for format metadata. Consumers that only
/// need row-level CRUD should depend on <see cref="IAccessWriter"/>; consumers that
/// only need schema management should depend on <see cref="IAccessSchema"/>.
/// </remarks>
public interface IAccessSchema : IAccessBase
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
    /// Limitations: the orphaned real-index slot whose backing leaf was created
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
}
