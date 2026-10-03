namespace JetDatabaseWriter.Relationships;

/// <summary>
/// The statement a foreign-key parent check runs for, which names the
/// statement and the key's source in the violation message.
/// </summary>
internal enum FkCheckKind
{
    /// <summary>The foreign key of a row being inserted.</summary>
    Insert = 0,

    /// <summary>A foreign key an update changes.</summary>
    Update = 1,
}
