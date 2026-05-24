namespace JetDatabaseWriter.Models;

using JetDatabaseWriter.Enums;

/// <summary>
/// Metadata about a linked table entry in the database catalog.
/// </summary>
public sealed record LinkedTableInfo
{
    /// <summary>Gets or sets the table name as it appears in this database.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Gets or sets the kind of linked-table entry.</summary>
    public LinkedTableKind Kind { get; set; }

    /// <summary>Gets or sets the source object name, such as a remote table name or text filename.</summary>
    public string SourceObjectName { get; set; } = string.Empty;

    /// <summary>Gets or sets the source path, such as an Access database file or text source directory.</summary>
    public string? SourcePath { get; set; }

    /// <summary>Gets or sets the link connect string, when present.</summary>
    public string? ConnectString { get; set; }
}
