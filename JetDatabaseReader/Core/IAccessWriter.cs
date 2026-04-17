namespace JetDatabaseReader
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Interface for writing to Microsoft Access JET databases (.mdb / .accdb).
    /// Provides methods for creating tables, inserting, updating, and deleting rows.
    /// </summary>
    public interface IAccessWriter : IDisposable
    {
        /// <summary>
        /// Creates a new table with the specified columns.
        /// Throws if a table with the same name already exists.
        /// </summary>
        /// <param name="tableName">Name of the table to create.</param>
        /// <param name="columns">Column definitions for the new table.</param>
        void CreateTable(string tableName, IReadOnlyList<ColumnDefinition> columns);

        /// <summary>
        /// Drops (deletes) the specified table and all of its data.
        /// Throws if the table does not exist.
        /// </summary>
        /// <param name="tableName">Name of the table to drop.</param>
        void DropTable(string tableName);

        /// <summary>
        /// Inserts a single row into the specified table.
        /// Values must be in the same order as the table's columns.
        /// </summary>
        /// <param name="tableName">Target table name (case-insensitive).</param>
        /// <param name="values">Column values in table-column order.</param>
        void InsertRow(string tableName, object[] values);

        /// <summary>
        /// Inserts multiple rows into the specified table in a single operation.
        /// </summary>
        /// <param name="tableName">Target table name (case-insensitive).</param>
        /// <param name="rows">Collection of rows, each containing column values in table-column order.</param>
        /// <returns>The number of rows inserted.</returns>
        int InsertRows(string tableName, IEnumerable<object[]> rows);

        /// <summary>
        /// Updates rows in the specified table where the predicate column matches the given value.
        /// </summary>
        /// <param name="tableName">Target table name (case-insensitive).</param>
        /// <param name="predicateColumn">Column name to filter on.</param>
        /// <param name="predicateValue">Value to match in the predicate column.</param>
        /// <param name="updatedValues">Dictionary of column-name → new-value pairs to apply.</param>
        /// <returns>The number of rows updated.</returns>
        int UpdateRows(string tableName, string predicateColumn, object predicateValue, IDictionary<string, object> updatedValues);

        /// <summary>
        /// Deletes rows from the specified table where the predicate column matches the given value.
        /// </summary>
        /// <param name="tableName">Target table name (case-insensitive).</param>
        /// <param name="predicateColumn">Column name to filter on.</param>
        /// <param name="predicateValue">Value to match in the predicate column.</param>
        /// <returns>The number of rows deleted.</returns>
        int DeleteRows(string tableName, string predicateColumn, object predicateValue);
    }
}
