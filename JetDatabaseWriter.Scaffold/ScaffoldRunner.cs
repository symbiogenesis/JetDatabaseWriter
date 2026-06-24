namespace JetDatabaseWriter.Scaffold;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;

/// <summary>
/// Orchestrates reading table schemas from a JET database and emitting C# entity-model source files.
/// Extracted from the CLI entry point for testability.
/// </summary>
/// <param name="reader">The reader.</param>
/// <param name="output">The output.</param>
/// <param name="error">The error.</param>
internal sealed class ScaffoldRunner(IAccessReader reader, TextWriter output, TextWriter error)
{
    /// <summary>
    /// Generates C# entity files for all user tables visible through the configured reader.
    /// </summary>
    /// <param name="outputDir">Directory to write generated .cs files into.</param>
    /// <param name="ns">Namespace for generated classes.</param>
    /// <param name="useRecords">Whether to emit C# records instead of classes.</param>
    /// <param name="nullable">Whether to emit nullable reference type annotations.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of models generated, or -1 on failure.</returns>
    public async Task<int> RunAsync(
        string outputDir,
        string ns,
        bool useRecords,
        bool nullable,
        CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(outputDir);

        IReadOnlyList<string> tables = await reader.ListTablesAsync(cancellationToken);
        if (tables.Count == 0)
        {
            await output.WriteLineAsync("No user tables found in the database.");
            return 0;
        }

        await output.WriteLineAsync($"Found {tables.Count} table(s). Generating models into: {outputDir}");

        IReadOnlyList<RelationshipMetadata> relationships;
        try
        {
            relationships = await reader.ListRelationshipsAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is InvalidOperationException or IOException)
        {
            await error.WriteLineAsync($"  Warning: relationships unavailable; navigation properties skipped: {ex.Message}");
            relationships = [];
        }

        Dictionary<string, List<ScaffoldNavigation>> navigationsByTable = NavigationResolver.Resolve(tables, relationships);

        int generated = 0;
        foreach (string table in tables)
        {
            IReadOnlyList<ColumnMetadata> columns;
            try
            {
                columns = await reader.GetColumnMetadataAsync(table, cancellationToken);
            }
            catch (Exception ex) when (ex is InvalidOperationException or IOException)
            {
                await error.WriteLineAsync($"  Warning: skipping table '{table}': {ex.Message}");
                continue;
            }

            string className = NameCleaner.ToClassName(table);
            string filePath = Path.Combine(outputDir, $"{className}.cs");

            IReadOnlyList<ScaffoldNavigation> navigations = navigationsByTable.TryGetValue(table, out List<ScaffoldNavigation>? navs)
                ? navs
                : [];

            await File.WriteAllTextAsync(filePath, EntityEmitter.Emit(className, columns, navigations, ns, useRecords, nullable), cancellationToken);
            await output.WriteLineAsync($"  {table} -> {className}.cs ({columns.Count} columns)");
            generated++;
        }

        await output.WriteLineAsync($"Done. {generated} model(s) generated.");
        return generated;
    }
}
