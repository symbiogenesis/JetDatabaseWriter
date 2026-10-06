namespace JetDatabaseWriter.Catalog.Models;

using JetDatabaseWriter.Schema.Models;

/// <summary>A catalog identity and its resolved schema, with an immutable column layout.</summary>
/// <param name="Entry">The catalog identity.</param>
/// <param name="Schema">The resolved structural image and properties.</param>
internal sealed record ResolvedTable(CatalogEntry Entry, TableSchema Schema)
{
    /// <summary>Gets the immutable column layout.</summary>
    internal TableDef Definition { get; } = Schema.CreateDefinition();
}
