namespace JetDatabaseReader;

using System.Collections.Generic;
using System.Data;

/// <summary>
/// Result of a string-mode table read (<see cref="IAccessReader.ReadTableAsStringsAsync"/>).
/// All column values are returned as strings in <see cref="Rows"/>.
/// Use <see cref="TableResult"/> when native CLR types are preferred.
/// </summary>
public class StringTableResult
{
    /// <summary>Gets or sets the ordered list of column names.</summary>
    public List<string> Headers { get; set; } = [];

    /// <summary>Gets or sets up to <c>maxRows</c> rows, each row a list of string values (one per column).</summary>
    public List<List<string>> Rows { get; set; } = [];

    /// <summary>Gets or sets the per-column schema information in the same order as <see cref="Headers"/>.</summary>
    public List<TableColumn> Schema { get; set; } = [];

    /// <summary>Gets or sets the name of the table this result was read from.</summary>
    public string TableName { get; set; } = string.Empty;

    /// <summary>Gets the total number of rows in the result.</summary>
    public int RowCount => Rows?.Count ?? 0;

    /// <summary>
    /// Converts this result to a <see cref="DataTable"/> with all columns typed as <see cref="string"/>.
    /// </summary>
    /// <returns></returns>
    public DataTable ToDataTable()
    {
        var dt = new DataTable(TableName);

        if (Headers == null)
        {
            return dt;
        }

        foreach (string header in Headers)
        {
            _ = dt.Columns.Add(header, typeof(string));
        }

        if (Rows == null)
        {
            return dt;
        }

        foreach (List<string> row in Rows)
        {
            _ = dt.Rows.Add(row.ToArray());
        }

        return dt;
    }
}
