namespace JetDatabaseWriter.Enums;

/// <summary>
/// Classification of a linked-table catalog entry.
/// </summary>
public enum LinkedTableKind
{
    /// <summary>An Access-file link to a table in another .mdb or .accdb database.</summary>
    Access = 0,

    /// <summary>An ODBC link to an external table.</summary>
    Odbc = 1,

    /// <summary>A text-driver link to a text or CSV file.</summary>
    Text = 2,
}
