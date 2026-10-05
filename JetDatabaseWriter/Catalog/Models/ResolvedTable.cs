namespace JetDatabaseWriter.Catalog.Models;

using JetDatabaseWriter.Schema.Models;

/// <summary>A catalog identity and its resolved schema, with an independently owned layout.</summary>
/// <param name="Entry">The catalog identity.</param>
/// <param name="Schema">The resolved structural image and properties.</param>
/// <param name="Counters">The current writer counters, when supplied.</param>
internal sealed record ResolvedTable(CatalogEntry Entry, TableSchema Schema, TableCounters? Counters = null)
{
    /// <summary>Gets the caller's layout, including a current writer counter snapshot.</summary>
    internal TableDef Definition { get; } = CreateDefinition(Schema, Counters);

    private static TableDef CreateDefinition(TableSchema schema, TableCounters? counters)
    {
        TableDef definition = schema.CreateDefinition();
        if (counters is { } current)
        {
            definition.RowCount = current.RowCount;
        }

        return definition;
    }
}
