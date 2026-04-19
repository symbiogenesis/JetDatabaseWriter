namespace JetDatabaseReader;

/// <summary>
/// Result returned by <see cref="IAccessReader.ReadFirstTable"/>.
/// Extends <see cref="StringTableResult"/> with the total number of user tables
/// found in the database.
/// </summary>
public sealed class FirstTableResult : StringTableResult
{
    /// <summary>Gets or sets the total number of user tables found in the database.</summary>
    public int TableCount { get; set; }
}
