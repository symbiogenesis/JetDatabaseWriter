namespace JetDatabaseReader;

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;

/// <summary>
/// Result of a typed table read (<see cref="IAccessReader.ReadTableAsync(string, int, System.Threading.CancellationToken)"/>).
/// Column values are returned as native CLR types (int, DateTime, decimal, etc.) in <see cref="Rows"/>.
/// Use <see cref="StringTableResult"/> when raw string values are needed.
/// </summary>
public class TableResult
{
    private static readonly IReadOnlyList<string> EmptyHeaders = new ReadOnlyCollection<string>([]);
    private static readonly IReadOnlyList<IReadOnlyList<object?>> EmptyRows = new ReadOnlyCollection<IReadOnlyList<object?>>([]);
    private static readonly IReadOnlyList<TableColumn> EmptySchema = new ReadOnlyCollection<TableColumn>([]);

    private IReadOnlyList<string> _headers = EmptyHeaders;
    private IReadOnlyList<IReadOnlyList<object?>> _rows = EmptyRows;
    private IReadOnlyList<TableColumn> _schema = EmptySchema;

    /// <summary>Gets the ordered list of column names.</summary>
    public IReadOnlyList<string> Headers
    {
        get => _headers;
        init => _headers = FreezeHeaders(value);
    }

    /// <summary>Gets up to <c>maxRows</c> rows with native CLR types (int, DateTime, decimal, etc.).</summary>
    public IReadOnlyList<IReadOnlyList<object?>> Rows
    {
        get => _rows;
        init => _rows = FreezeRows(value);
    }

    /// <summary>Gets the per-column schema information in the same order as <see cref="Headers"/>.</summary>
    public IReadOnlyList<TableColumn> Schema
    {
        get => _schema;
        init => _schema = FreezeSchema(value);
    }

    /// <summary>Gets or initializes the name of the table this result was read from.</summary>
    public string TableName { get; init; } = string.Empty;

    /// <summary>Gets the total number of rows in the result.</summary>
    public int RowCount => Rows.Count;

    /// <summary>
    /// Converts this result to a <see cref="DataTable"/> with properly typed columns.
    /// Column types are taken from <see cref="Schema"/>; falls back to <see cref="object"/> when schema is unavailable.
    /// </summary>
    /// <returns></returns>
    public DataTable ToDataTable()
    {
        var dt = new DataTable(TableName);

        for (int i = 0; i < Headers.Count; i++)
        {
            Type colType = (i < Schema.Count && Schema[i].Type != null)
                ? Schema[i].Type
                : typeof(object);
            _ = dt.Columns.Add(Headers[i], colType);
        }

        foreach (IReadOnlyList<object?> row in Rows)
        {
            DataRow dr = dt.NewRow();
            for (int i = 0; i < row.Count && i < dt.Columns.Count; i++)
            {
                dr[i] = row[i] ?? DBNull.Value;
            }

            dt.Rows.Add(dr);
        }

        return dt;
    }

    private static IReadOnlyList<string> FreezeHeaders(IEnumerable<string>? headers)
    {
        if (headers == null)
        {
            return EmptyHeaders;
        }

        var copy = new List<string>();
        foreach (string header in headers)
        {
            copy.Add(header ?? string.Empty);
        }

        return copy.Count == 0 ? EmptyHeaders : new ReadOnlyCollection<string>(copy);
    }

    private static IReadOnlyList<IReadOnlyList<object?>> FreezeRows(IEnumerable<IEnumerable<object?>>? rows)
    {
        if (rows == null)
        {
            return EmptyRows;
        }

        var rowCopies = new List<IReadOnlyList<object?>>();
        foreach (IEnumerable<object?>? row in rows)
        {
            if (row == null)
            {
                rowCopies.Add(Array.Empty<object?>());
                continue;
            }

            var values = new List<object?>();
            foreach (object? value in row)
            {
                values.Add(value);
            }

            rowCopies.Add(values.Count == 0
                ? Array.Empty<object?>()
                : new ReadOnlyCollection<object?>(values));
        }

        return rowCopies.Count == 0 ? EmptyRows : new ReadOnlyCollection<IReadOnlyList<object?>>(rowCopies);
    }

    private static IReadOnlyList<TableColumn> FreezeSchema(IEnumerable<TableColumn>? schema)
    {
        if (schema == null)
        {
            return EmptySchema;
        }

        var copy = new List<TableColumn>();
        foreach (TableColumn? column in schema)
        {
            if (column == null)
            {
                continue;
            }

            copy.Add(new TableColumn
            {
                Name = column.Name,
                Type = column.Type,
                Size = column.Size,
            });
        }

        return copy.Count == 0 ? EmptySchema : new ReadOnlyCollection<TableColumn>(copy);
    }
}
