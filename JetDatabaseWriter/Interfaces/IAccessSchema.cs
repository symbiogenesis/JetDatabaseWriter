namespace JetDatabaseWriter.Interfaces;

using System;
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
/// A Jet3 (Access 97) database stores names in its ANSI code page, so on a Jet3
/// file every new name, and a linked table's foreign name, path and connect string,
/// must also be in that code page. A character outside it throws
/// <see cref="ArgumentException"/> before anything is written, where .NET would
/// otherwise store its closest match or <c>?</c>.
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
    /// <summary>Sets or removes a table validation rule after checking every existing row.</summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="rule">The new rule, or null to remove it and its validation message.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The asynchronous operation.</returns>
    public ValueTask SetTableValidationRuleAsync(string tableName, TableValidationRule? rule, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously creates a new table with the specified columns.
    /// Throws if a table with the same name already exists.
    /// </summary>
    /// <param name="tableName">Name of the table to create. It must follow the Access naming rules: 1 to 64 characters, not only white space, no leading space, and none of <c>. ! ` [ ]</c> or a control character.</param>
    /// <param name="columns">Column definitions for the new table. Each column name follows the same rules, and no two may be equal ignoring case.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="tableName"/> is empty (<see cref="ArgumentNullException"/> when it is <see langword="null"/>) or breaks the naming rules; or, with <see cref="ArgumentException.ParamName"/> <c>columns</c>, when a column is <see langword="null"/>, has no name, has a name that breaks the rules or that another column has ignoring case, has flags that conflict (<see cref="ColumnDefinition.IsCurrency"/> on a type other than <see cref="decimal"/> or on an Attachment or multi-value column, <see cref="ColumnDefinition.IsDateTimeExtended"/> on a type other than <see cref="DateTime"/>, both <see cref="ColumnDefinition.IsAttachment"/> and <see cref="ColumnDefinition.IsMultiValue"/>, or <see cref="ColumnDefinition.IsHyperlink"/> on a column that is not a Memo), is calculated with no <see cref="ColumnDefinition.CalculationExpression"/> or with one that is not valid Access expression syntax (for example Excel's postfix <c>%</c>), or declares a default it cannot have, such as a <see cref="ColumnDefinition.DefaultValue"/> or <see cref="ColumnDefinition.DefaultValueExpression"/> on an AutoNumber, calculated, Attachment or multi-value column, or a numeric <see cref="ColumnDefinition.DefaultValue"/> the column's type cannot hold, such as 1e39 on a Single column (see <see cref="ColumnDefinition.DefaultValue"/>); <see cref="ArgumentOutOfRangeException"/>, also with <see cref="ArgumentException.ParamName"/> <c>columns</c>, when the <see cref="ColumnDefinition.NumericPrecision"/> of a decimal column, or of a multi-value column's decimal items, is not 1-28 or its <see cref="ColumnDefinition.NumericScale"/> is above it. The names are checked first.</exception>
    /// <exception cref="NotSupportedException">Thrown when the format cannot hold a column: a calculated, Large Number, Date/Time Extended, Attachment or multi-value column on a Jet3 or Jet4 <c>.mdb</c>, a calculated column that is also AutoNumber, Attachment, multi-value or Hyperlink, or a decimal column on a Jet3 <c>.mdb</c>, which is created as Currency, whose <see cref="ColumnDefinition.NumericScale"/> is above 4 or whose <see cref="ColumnDefinition.NumericPrecision"/> leaves more than 14 digits before the decimal point (see <see cref="ColumnDefinition.NumericPrecision"/>). These checks run before the calculated expressions are parsed. Also thrown before anything is written for a byte or long AutoNumber declaration, or a CLR default with no supported Access literal.</exception>
    /// <exception cref="InvalidOperationException">Thrown when a table named <paramref name="tableName"/> already exists. Every argument check above runs first.</exception>
    /// <exception cref="Exceptions.JetLimitationException">Thrown, before anything is written, when a table would have more than 255 columns, in any database format.</exception>
    public ValueTask CreateTableAsync(string tableName, IReadOnlyList<ColumnDefinition> columns, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously creates a new table with the specified columns and the specified
    /// logical indexes on Jet3, Jet4 or ACE. Throws if a table with the
    /// same name already exists.
    /// </summary>
    /// <param name="tableName">Name of the table to create. It must follow the Access naming rules: 1 to 64 characters, not only white space, no leading space, and none of <c>. ! ` [ ]</c> or a control character.</param>
    /// <param name="columns">Column definitions for the new table. Each column name follows the same rules, and no two may be equal ignoring case.</param>
    /// <param name="indexes">
    /// Logical-index schema entries to write into the new table's TDEF page chain.
    /// Each index may contain up to ten columns, enforce uniqueness or a primary
    /// key, and select descending columns. See <see cref="IndexDefinition"/> for
    /// the enforced constraints. Index leaves are emitted at
    /// table-creation time and maintained by supported writer insert / update /
    /// delete paths. Each index name follows the same naming rules.
    /// </param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="tableName"/> is empty (<see cref="ArgumentNullException"/> when it is <see langword="null"/>) or breaks the naming rules; with <see cref="ArgumentException.ParamName"/> <c>indexes</c>, when an index is <see langword="null"/>, has no name or has a name that breaks the rules; or, with <see cref="ArgumentException.ParamName"/> <c>columns</c>, when a column is <see langword="null"/>, has no name, has a name that breaks the rules or that another column has ignoring case, has flags that conflict (<see cref="ColumnDefinition.IsCurrency"/> on a type other than <see cref="decimal"/> or on an Attachment or multi-value column, <see cref="ColumnDefinition.IsDateTimeExtended"/> on a type other than <see cref="DateTime"/>, both <see cref="ColumnDefinition.IsAttachment"/> and <see cref="ColumnDefinition.IsMultiValue"/>, or <see cref="ColumnDefinition.IsHyperlink"/> on a column that is not a Memo), is calculated with no <see cref="ColumnDefinition.CalculationExpression"/> or with one that is not valid Access expression syntax (for example Excel's postfix <c>%</c>), or declares a default it cannot have, such as a <see cref="ColumnDefinition.DefaultValue"/> or <see cref="ColumnDefinition.DefaultValueExpression"/> on an AutoNumber, calculated, Attachment or multi-value column, or a numeric <see cref="ColumnDefinition.DefaultValue"/> the column's type cannot hold, such as 1e39 on a Single column (see <see cref="ColumnDefinition.DefaultValue"/>); <see cref="ArgumentOutOfRangeException"/>, also with <see cref="ArgumentException.ParamName"/> <c>columns</c>, when the <see cref="ColumnDefinition.NumericPrecision"/> of a decimal column, or of a multi-value column's decimal items, is not 1-28 or its <see cref="ColumnDefinition.NumericScale"/> is above it. The names are checked first.</exception>
    /// <exception cref="NotSupportedException">Thrown when the format cannot hold a column: a calculated, Large Number, Date/Time Extended, Attachment or multi-value column on a Jet3 or Jet4 <c>.mdb</c>, a calculated column that is also AutoNumber, Attachment, multi-value or Hyperlink, or a decimal column on a Jet3 <c>.mdb</c>, which is created as Currency, whose <see cref="ColumnDefinition.NumericScale"/> is above 4 or whose <see cref="ColumnDefinition.NumericPrecision"/> leaves more than 14 digits before the decimal point (see <see cref="ColumnDefinition.NumericPrecision"/>). These checks run before the calculated expressions are parsed. Also thrown before anything is written for a byte or long AutoNumber declaration, or a CLR default with no supported Access literal.</exception>
    /// <exception cref="InvalidOperationException">Thrown when a table named <paramref name="tableName"/> already exists. Every argument check above runs first.</exception>
    /// <exception cref="Exceptions.JetLimitationException">Thrown, before anything is written, when a table would have more than 255 columns, in any database format.</exception>
    public ValueTask CreateTableAsync(string tableName, IReadOnlyList<ColumnDefinition> columns, IReadOnlyList<IndexDefinition> indexes, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously drops (deletes) the specified table and all of its data.
    /// An enforced relationship to another table blocks the drop. Self relationships
    /// and relationships without integrity enforcement are removed with the table.
    /// </summary>
    /// <param name="tableName">Name of the table to drop (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the table does not exist, or, before anything is written, when an enforced relationship to another table names it; the message lists the relationships.</exception>
    public ValueTask DropTableAsync(string tableName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously appends a new column to an existing table. Existing rows receive
    /// <see cref="DBNull.Value"/> for the new column. Implemented by copying the
    /// table to a new schema and renaming the result back to <paramref name="tableName"/>.
    /// The existing columns keep their data, their type and every persisted property, and the
    /// table keeps its own, as described for <see cref="RenameColumnAsync"/>; the new column gets the
    /// properties <paramref name="column"/> declares.
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="column">The new column definition. Its name must not already exist on the table, and must follow the Access naming rules: 1 to 64 characters, not only white space, no leading space, and none of <c>. ! ` [ ]</c> or a control character.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="NotSupportedException">Thrown, before the table is read, when the format cannot hold <paramref name="column"/>: a calculated, Large Number, Date/Time Extended, Attachment or multi-value column on a Jet3 or Jet4 <c>.mdb</c>, a calculated column that is also AutoNumber, Attachment, multi-value or Hyperlink, or a decimal column on a Jet3 <c>.mdb</c>, which is created as Currency, whose <see cref="ColumnDefinition.NumericScale"/> is above 4 or whose <see cref="ColumnDefinition.NumericPrecision"/> leaves more than 14 digits before the decimal point (see <see cref="ColumnDefinition.NumericPrecision"/>). Also thrown before anything is written for a byte or long AutoNumber declaration, or a CLR default with no supported Access literal.</exception>
    /// <exception cref="ArgumentException">Thrown, with <see cref="ArgumentException.ParamName"/> <c>column</c> and before the table is read, when the column has no name or a name that breaks the naming rules (checked first), when its flags conflict (<see cref="ColumnDefinition.IsCurrency"/> on a type other than <see cref="decimal"/> or on an Attachment or multi-value column, <see cref="ColumnDefinition.IsDateTimeExtended"/> on a type other than <see cref="DateTime"/>, both <see cref="ColumnDefinition.IsAttachment"/> and <see cref="ColumnDefinition.IsMultiValue"/>, or <see cref="ColumnDefinition.IsHyperlink"/> on a column that is not a Memo), when a calculated column has no <see cref="ColumnDefinition.CalculationExpression"/> or one that is not valid Access expression syntax (for example Excel's postfix <c>%</c>), or when the column declares a default it cannot have, such as a <see cref="ColumnDefinition.DefaultValue"/> or <see cref="ColumnDefinition.DefaultValueExpression"/> on an AutoNumber, calculated, Attachment or multi-value column, or a numeric <see cref="ColumnDefinition.DefaultValue"/> the column's type cannot hold, such as 1e39 on a Single column (see <see cref="ColumnDefinition.DefaultValue"/>); <see cref="ArgumentOutOfRangeException"/>, also with <see cref="ArgumentException.ParamName"/> <c>column</c> and before the table is read, when the <see cref="ColumnDefinition.NumericPrecision"/> of a decimal column, or of a multi-value column's decimal items, is not 1-28 or its <see cref="ColumnDefinition.NumericScale"/> is above it.</exception>
    /// <exception cref="System.IO.InvalidDataException">Thrown, before the table changes, when a row holds a MEMO or OLE value whose stored data cannot be read; copying the row would lose that value.</exception>
    /// <exception cref="Exceptions.JetLimitationException">Thrown, before the table changes, when a table would have more than 255 columns, in any database format.</exception>
    public ValueTask AddColumnAsync(string tableName, ColumnDefinition column, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously drops the named column from an existing table. The column's data is
    /// permanently lost. Implemented by copying the remaining columns to a new schema and
    /// renaming the result back to <paramref name="tableName"/>. The table must retain at
    /// least one column after the drop, the column must not be a key column of a
    /// foreign-key relationship (drop the relationship first, as Microsoft Access requires),
    /// and no other column's calculated expression, validation rule or default value
    /// expression may name it (change or drop that column first). A mention inside a string
    /// literal, or in the dropped column's own rule, does not count. The remaining columns
    /// keep their data, their type and every persisted property, and the table keeps its own,
    /// as described for <see cref="RenameColumnAsync"/>; the dropped column's properties go with it.
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="columnName">The column to drop (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="System.IO.InvalidDataException">Thrown, before the table changes, when a row holds a MEMO or OLE value in a column being kept whose stored data cannot be read; copying the row would lose that value. An unreadable value in the dropped column does not block the drop.</exception>
    /// <exception cref="InvalidOperationException">Thrown, before the table changes, when the column is the table's last column, a relationship key column, or named by another column's calculated expression, validation rule or default value expression; the message names that column and its expression.</exception>
    public ValueTask DropColumnAsync(string tableName, string columnName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously renames a column on an existing table. Implemented by copying the
    /// table to a new schema and renaming the result back to <paramref name="tableName"/>.
    /// Row data is preserved, and so are each column's type, size, precision and scale,
    /// Unicode compression and calculated expression, and every persisted
    /// <c>MSysObjects.LvProp</c> property of the table and its columns as stored, including
    /// those the writer does not model, such as a column's <c>Caption</c>, <c>Format</c> and
    /// <c>AllowZeroLength</c> and the table-level <c>Filter</c>, <c>OrderBy</c> and
    /// <c>ValidationRule</c>. The renamed column's properties move to its new name, and the
    /// table-level <c>NameMap</c> is dropped. When the column is a
    /// key column of a foreign-key relationship, the relationship's <c>MSysRelationships</c>
    /// rows are updated to the new name. Every calculated expression, validation rule and
    /// default value expression in the table that names the column is rewritten:
    /// <c>[Old]</c> and a bare <c>Old</c> become <c>[New]</c>, so the expression keeps
    /// evaluating. A reference qualified by the table's own name keeps the qualifier
    /// (<c>[T].[Old]</c> becomes <c>[T].[New]</c> and <c>T.Old</c> becomes
    /// <c>T.[New]</c>); the expression engine does not evaluate table-qualified references
    /// yet, before or after the rename. The rest of each expression, including text inside
    /// string literals, is kept as it was. The table-level <c>Filter</c>, <c>OrderBy</c> and
    /// <c>ValidationRule</c> follows the renamed column; dropping a field it names is refused.
    /// A rename may change only the letter case of the name, and is then handled like any other
    /// rename. A rename to the name the column already has, spelled as stored, changes nothing
    /// and does not rewrite the table. Both follow what Microsoft Access is believed to do,
    /// which has not been checked against Access.
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="oldColumnName">The current column name (case-insensitive).</param>
    /// <param name="newColumnName">The new column name. Must not be the name of another column of the table, compared ignoring case, and must follow the Access naming rules: 1 to 64 characters, not only white space, no leading space, and none of <c>. ! ` [ ]</c> or a control character. The current name is only looked up, so a column whose name breaks the rules can be renamed to one that follows them.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">Thrown, before the table is read, when <paramref name="newColumnName"/> is empty (<see cref="ArgumentNullException"/> when it is <see langword="null"/>) or breaks the naming rules, or, before the table changes, when the new name would push an expression that names the column past the expression engine's length limit.</exception>
    /// <exception cref="InvalidOperationException">Thrown, before the table changes, when the table does not exist or another column of the table has the new name, compared ignoring case.</exception>
    /// <exception cref="System.IO.InvalidDataException">Thrown, before the table changes, when a row holds a MEMO or OLE value whose stored data cannot be read; copying the row would lose that value.</exception>
    public ValueTask RenameColumnAsync(string tableName, string oldColumnName, string newColumnName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously creates a linked-table entry (MSysObjects type 6) that references a
    /// table in another Access database. The entry is metadata only — no rows are stored
    /// locally; readers follow <paramref name="sourceDatabasePath"/> /
    /// <paramref name="foreignTableName"/> to retrieve data on demand.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database. It must follow the Access naming rules: 1 to 64 characters, not only white space, no leading space, and none of <c>. ! ` [ ]</c> or a control character. The foreign name is not checked against these rules.</param>
    /// <param name="sourceDatabasePath">Path to the source Access database file (.mdb / .accdb).</param>
    /// <param name="foreignTableName">The name of the table in the source database.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">Thrown, before anything is written, when <paramref name="linkedTableName"/> is empty (<see cref="ArgumentNullException"/> when it is <see langword="null"/>) or breaks the naming rules.</exception>
    public ValueTask CreateLinkedTableAsync(string linkedTableName, string sourceDatabasePath, string foreignTableName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously creates a linked-text/CSV table entry (MSysObjects type 6)
    /// that references a text or CSV file in a directory. No rows are stored
    /// locally; managed readers parse supported delimited text sources on demand
    /// through the linked-source path policy and expose fields as strings.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database. It must follow the Access naming rules: 1 to 64 characters, not only white space, no leading space, and none of <c>. ! ` [ ]</c> or a control character. The foreign name is not checked against these rules.</param>
    /// <param name="sourceDirectoryPath">Path to the directory containing the text/CSV source file.</param>
    /// <param name="foreignFileName">The filename of the text/CSV source, e.g. <c>"data.csv"</c>.</param>
    /// <param name="connectString">The text-driver connect string, e.g. <c>"Text;HDR=YES;FMT=Delimited"</c>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">Thrown, before anything is written, when <paramref name="linkedTableName"/> is empty (<see cref="ArgumentNullException"/> when it is <see langword="null"/>) or breaks the naming rules.</exception>
    public ValueTask CreateLinkedTextTableAsync(string linkedTableName, string sourceDirectoryPath, string foreignFileName, string connectString, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously creates a linked-ODBC table entry (MSysObjects type 4) that references
    /// a table accessible via an ODBC connection. The entry is metadata only — no rows are
    /// stored locally. Managed readers expose the catalog metadata but do not open the ODBC
    /// source. Because this overload receives no source columns, it writes a real
    /// table-level <c>MSysObjects.LvProp</c> property block but cannot cache the remote
    /// column schema. Use the source-column overload for generated column-level
    /// metadata, or the <c>cachedSchemaLvProp</c> overload when byte-for-byte
    /// Access/DAO-authored metadata is required. The connection string must use the
    /// Access ODBC link format and is expected to begin with the literal prefix
    /// <c>"ODBC;"</c>.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database. It must follow the Access naming rules: 1 to 64 characters, not only white space, no leading space, and none of <c>. ! ` [ ]</c> or a control character. The foreign name is not checked against these rules.</param>
    /// <param name="connectionString">ODBC connection string (e.g. <c>"ODBC;DSN=Sales;UID=app;..."</c> or <c>"ODBC;DRIVER={SQL Server};SERVER=...;..."</c>). The <c>"ODBC;"</c> prefix is added automatically when omitted.</param>
    /// <param name="foreignTableName">The name of the table at the ODBC source.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">Thrown, before anything is written, when <paramref name="linkedTableName"/> is empty (<see cref="ArgumentNullException"/> when it is <see langword="null"/>) or breaks the naming rules.</exception>
    public ValueTask CreateLinkedOdbcTableAsync(string linkedTableName, string connectionString, string foreignTableName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously creates a linked-ODBC table entry (MSysObjects type 4) and
    /// generates a cached-schema <c>MSysObjects.LvProp</c> property block from
    /// the supplied remote column definitions.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database. It must follow the Access naming rules: 1 to 64 characters, not only white space, no leading space, and none of <c>. ! ` [ ]</c> or a control character. The foreign name is not checked against these rules.</param>
    /// <param name="connectionString">ODBC connection string. The <c>"ODBC;"</c> prefix is added automatically when omitted.</param>
    /// <param name="foreignTableName">The name of the table at the ODBC source.</param>
    /// <param name="sourceColumns">Column definitions for the remote source table.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">Thrown, before anything is written, when <paramref name="linkedTableName"/> is empty (<see cref="ArgumentNullException"/> when it is <see langword="null"/>) or breaks the naming rules.</exception>
    public ValueTask CreateLinkedOdbcTableAsync(
        string linkedTableName,
        string connectionString,
        string foreignTableName,
        IReadOnlyList<ColumnDefinition> sourceColumns,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously creates a linked-ODBC table entry (MSysObjects type 4) using
    /// a caller-supplied Access/DAO cached-schema payload for <c>MSysObjects.LvProp</c>.
    /// The payload must come from an Access-compatible ODBC link to the same source
    /// schema; the writer validates that it is a non-empty <c>MR2\0</c> / <c>KKD\0</c>
    /// property block and stores it verbatim.
    /// </summary>
    /// <param name="linkedTableName">The name of the linked table as it appears in this database. It must follow the Access naming rules: 1 to 64 characters, not only white space, no leading space, and none of <c>. ! ` [ ]</c> or a control character. The foreign name is not checked against these rules.</param>
    /// <param name="connectionString">ODBC connection string. The <c>"ODBC;"</c> prefix is added automatically when omitted.</param>
    /// <param name="foreignTableName">The name of the table at the ODBC source.</param>
    /// <param name="cachedSchemaLvProp">Access/DAO-authored cached linked-schema payload for <c>MSysObjects.LvProp</c>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="ArgumentException">Thrown, before anything is written, when <paramref name="linkedTableName"/> is empty (<see cref="ArgumentNullException"/> when it is <see langword="null"/>) or breaks the naming rules.</exception>
    public ValueTask CreateLinkedOdbcTableAsync(
        string linkedTableName,
        string connectionString,
        string foreignTableName,
        ReadOnlyMemory<byte> cachedSchemaLvProp,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously creates a foreign-key relationship between two existing user tables
    /// by appending one row per FK column to the <c>MSysRelationships</c> system table.
    /// The relationship is visible in the Microsoft Access Relationships designer.
    /// The writer also enforces the declared referential-integrity rules on supported
    /// row mutations, and Microsoft Access can compact/rebuild the persisted metadata.
    /// </summary>
    /// <param name="relationship">The relationship to create. Both referenced tables and
    /// every named column must already exist; <see cref="RelationshipDefinition.Name"/>
    /// must not duplicate any existing relationship, and must follow the Access naming
    /// rules: 1 to 64 characters, not only white space, no leading space, and none of
    /// <c>. ! ` [ ]</c> or a control character.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="NotSupportedException">
    /// Thrown when the database does not contain a <c>MSysRelationships</c> table.
    /// ACCDB databases created by <c>AccessWriter.CreateDatabaseAsync</c> include this table.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when a referenced table does not exist or when a relationship with the
    /// same name already exists in the database, or when an enforced relationship would
    /// include an existing non-null foreign key with no matching parent.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when a referenced column does not exist on its table, or, before anything
    /// is read and with <see cref="ArgumentException.ParamName"/> <c>relationship.Name</c>,
    /// when the name is empty (<see cref="ArgumentNullException"/> when it is
    /// <see langword="null"/>) or breaks the naming rules.
    /// </exception>
    public ValueTask CreateRelationshipAsync(RelationshipDefinition relationship, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously deletes a foreign-key relationship previously created with
    /// <see cref="CreateRelationshipAsync(RelationshipDefinition, CancellationToken)"/>.
    /// Removes every row in <c>MSysRelationships</c> whose <c>szRelationship</c> matches
    /// <paramref name="relationshipName"/> (case-insensitive) and removes the
    /// corresponding per-TDEF foreign-key logical-index entries on both the
    /// PK-side and FK-side TDEFs, on every format, so the next
    /// reader observes the relationship gone immediately (without waiting for a
    /// Microsoft Access Compact &amp; Repair pass).
    /// </summary>
    /// <param name="relationshipName">Case-insensitive relationship name as supplied to
    /// <see cref="CreateRelationshipAsync(RelationshipDefinition, CancellationToken)"/>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="NotSupportedException">
    /// Thrown when the database does not contain a <c>MSysRelationships</c> table.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when no relationship named <paramref name="relationshipName"/> exists.
    /// </exception>
    /// <remarks>
    /// Limitations: trailing FK real-index slots are reclaimed when doing so does
    /// not require renumbering other TDEF references; non-trailing orphaned slots
    /// are left for Microsoft Access Compact &amp; Repair. The library does not roll
    /// back runtime cascade-update / cascade-delete enforcement that ran inside
    /// the same call before the drop.
    /// </remarks>
    public ValueTask DropRelationshipAsync(string relationshipName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously renames a foreign-key relationship previously created with
    /// <see cref="CreateRelationshipAsync(RelationshipDefinition, CancellationToken)"/>.
    /// Updates the <c>szRelationship</c> column of every matching row in
    /// <c>MSysRelationships</c> (case-insensitive lookup on
    /// <paramref name="oldName"/>). The matching per-TDEF foreign-key
    /// logical-index name cookies are rewritten through the logical TDEF-chain
    /// writer so reopened readers see the new name immediately.
    /// </summary>
    /// <param name="oldName">Case-insensitive existing relationship name.</param>
    /// <param name="newName">New relationship name. Must not match any existing
    /// relationship (case-insensitive), and must follow the Access naming rules: 1 to 64
    /// characters, not only white space, no leading space, and none of <c>. ! ` [ ]</c>
    /// or a control character.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="NotSupportedException">
    /// Thrown when the database does not contain a <c>MSysRelationships</c> table.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when no relationship named <paramref name="oldName"/> exists,
    /// or when a relationship named <paramref name="newName"/> already exists.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown, before anything is read, when <paramref name="oldName"/> or
    /// <paramref name="newName"/> is empty (<see cref="ArgumentNullException"/> when it is
    /// <see langword="null"/>), or when <paramref name="newName"/> breaks the naming rules.
    /// </exception>
    public ValueTask RenameRelationshipAsync(string oldName, string newName, CancellationToken cancellationToken = default);
}
