namespace JetDatabaseReader;

/// <summary>
/// Metadata about a linked table entry in the database catalog.
/// </summary>
public sealed record LinkedTableInfo
{
    /// <summary>Gets or sets the table name as it appears in this database.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Gets or sets the name of the table in the source database.</summary>
    public string ForeignName { get; set; } = string.Empty;

    /// <summary>Gets or sets the path to the source database file (for Access-linked tables).</summary>
    public string? SourceDatabasePath { get; set; }

    /// <summary>Gets or sets the ODBC connection string (for ODBC-linked tables).</summary>
    public string? ConnectionString { get; set; }

    /// <summary>Gets or sets a value indicating whether this is an ODBC-linked table (type 6) vs Access-linked (type 4).</summary>
    public bool IsOdbc { get; set; }
}
