namespace JetDatabaseWriter.Maintenance;

/// <summary>Hard limits for the physical, catalog, definition and map inventory phase.</summary>
internal sealed class CompactTraversalBudget
{
    /// <summary>Gets the maximum physical file size in pages, checked before traversal.</summary>
    public long MaxPages { get; init; } = 1_048_576;

    /// <summary>Gets the maximum live catalog object count.</summary>
    public int MaxObjects { get; init; } = 65_536;

    /// <summary>Gets the maximum emitted known-reference count.</summary>
    public long MaxReferences { get; init; } = 4_194_304;

    /// <summary>Gets the maximum uncertainty diagnostic count.</summary>
    public int MaxDiagnostics { get; init; } = 1_024;

    /// <summary>Gets the aggregate logical table-definition byte limit.</summary>
    public int MaxSchemaBytes { get; init; } = 64 * 1024 * 1024;
}
