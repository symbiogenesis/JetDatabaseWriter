namespace JetDatabaseReader;

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
    /// Inserts a single row by mapping a POCO's properties to the table's columns (case-insensitive name match).
    /// </summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public readable properties match column names.</typeparam>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="item">The object whose properties supply the column values.</param>
    void InsertRow<T>(string tableName, T item)
        where T : class, new();

    /// <summary>
    /// Inserts multiple rows into the specified table in a single operation.
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="rows">Collection of rows, each containing column values in table-column order.</param>
    /// <returns>The number of rows inserted.</returns>
    int InsertRows(string tableName, IEnumerable<object[]> rows);

    /// <summary>
    /// Inserts multiple rows by mapping each POCO's properties to the table's columns (case-insensitive name match).
    /// </summary>
    /// <typeparam name="T">A class with a parameterless constructor whose public readable properties match column names.</typeparam>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="items">Collection of objects whose properties supply the column values.</param>
    /// <returns>The number of rows inserted.</returns>
    int InsertRows<T>(string tableName, IEnumerable<T> items)
        where T : class, new();

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
    /// Asynchronously drops (deletes) the specified table and all of its data.
    /// Throws if the table does not exist.
    /// </summary>
    /// <param name="tableName">Name of the table to drop.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    ValueTask DropTableAsync(string tableName, CancellationToken cancellationToken = default);

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
    /// <param name="predicateValue">Value to match in the predicate column.</param>
    /// <param name="updatedValues">Dictionary of column-name → new-value pairs to apply.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that yields the number of rows updated.</returns>
    ValueTask<int> UpdateRowsAsync(string tableName, string predicateColumn, object predicateValue, IDictionary<string, object> updatedValues, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously deletes rows from the specified table where the predicate column matches the given value.
    /// </summary>
    /// <param name="tableName">Target table name (case-insensitive).</param>
    /// <param name="predicateColumn">Column name to filter on.</param>
    /// <param name="predicateValue">Value to match in the predicate column.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that yields the number of rows deleted.</returns>
    ValueTask<int> DeleteRowsAsync(string tableName, string predicateColumn, object predicateValue, CancellationToken cancellationToken = default);
}
