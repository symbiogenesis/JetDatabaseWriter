namespace JetDatabaseWriter.Scaffold;

using System;
using System.Collections.Generic;
using System.CommandLine;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// CLI tool that reads the schema of every user table in a JET database
/// and emits one C# entity-model source file per table.
/// </summary>
internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        var databaseArgument = new Argument<FileInfo?>("database")
        {
            Arity = ArgumentArity.ZeroOrOne,
            Description = "Path to the .mdb or .accdb file",
        };

        var databaseOption = new Option<FileInfo?>("--database", "-d")
        {
            Description = "Path to the .mdb or .accdb file (alternative to positional argument)",
        };

        var outputOption = new Option<DirectoryInfo?>("--output", "-o")
        {
            Description = "Output directory (default: ./Models)",
        };

        var namespaceOption = new Option<string?>("--namespace", "-n")
        {
            Description = "Namespace for generated classes (default: GeneratedModels)",
        };

        var passwordOption = new Option<string?>("--password", "-p")
        {
            Description = "Database password (for encrypted files)",
        };

        var recordsOption = new Option<bool>("--records")
        {
            Description = "Emit C# records instead of classes",
        };

        var nullableOption = new Option<bool>("--nullable")
        {
            Description = "Emit nullable reference types (adds #nullable enable)",
            DefaultValueFactory = _ => true,
        };

        var rootCommand = new RootCommand("JetDatabaseWriter.Scaffold — Generate C# entity models from Access databases")
        {
            databaseArgument,
            databaseOption,
            outputOption,
            namespaceOption,
            passwordOption,
            recordsOption,
            nullableOption,
        };

        rootCommand.SetAction(async (parseResult, cancellationToken) =>
        {
            var dbFile = parseResult.GetValue(databaseOption) ?? parseResult.GetValue(databaseArgument);

            if (dbFile is null)
            {
                await Console.Error.WriteLineAsync("Error: database path is required. Use --database or pass a positional argument.");
                return 1;
            }

            if (!dbFile.Exists)
            {
                await Console.Error.WriteLineAsync($"Error: database file not found: {dbFile.FullName}");
                return 1;
            }

            string outputDir = parseResult.GetValue(outputOption)?.FullName
                                ?? Path.Combine(Directory.GetCurrentDirectory(), "Models");
            string ns = parseResult.GetValue(namespaceOption) ?? "GeneratedModels";
            bool useRecords = parseResult.GetValue(recordsOption);
            bool nullable = parseResult.GetValue(nullableOption);
            string? password = parseResult.GetValue(passwordOption);

            Directory.CreateDirectory(outputDir);

            var options = new AccessReaderOptions
            {
                UseLockFile = false,
                FileShare = FileShare.ReadWrite,
            };

            if (!string.IsNullOrEmpty(password))
            {
                options = new AccessReaderOptions(password)
                {
                    UseLockFile = false,
                    FileShare = FileShare.ReadWrite,
                };
            }

            await using var reader = await AccessReader.OpenAsync(dbFile.FullName, options, cancellationToken);

            List<string> tables = await reader.ListTablesAsync(cancellationToken);
            if (tables.Count == 0)
            {
                Console.WriteLine("No user tables found in the database.");
                return 0;
            }

            Console.WriteLine($"Found {tables.Count} table(s). Generating models into: {outputDir}");

            int generated = 0;
            foreach (string table in tables)
            {
                List<ColumnMetadata> columns;
                try
                {
                    columns = await reader.GetColumnMetadataAsync(table, cancellationToken);
                }
                catch (InvalidOperationException ex)
                {
                    await Console.Error.WriteLineAsync($"  Warning: skipping table '{table}': {ex.Message}");
                    continue;
                }
                catch (IOException ex)
                {
                    await Console.Error.WriteLineAsync($"  Warning: skipping table '{table}': {ex.Message}");
                    continue;
                }

                string className = NameCleaner.ToClassName(table);
                string source = EntityEmitter.Emit(className, columns, ns, useRecords, nullable);
                string filePath = Path.Combine(outputDir, $"{className}.cs");

                await File.WriteAllTextAsync(filePath, source, cancellationToken);
                Console.WriteLine($"  {table} -> {className}.cs ({columns.Count} columns)");
                generated++;
            }

            Console.WriteLine($"Done. {generated} model(s) generated.");
            return 0;
        });

        ParseResult parseResult = rootCommand.Parse(args);
        return await parseResult.InvokeAsync(cancellationToken: CancellationToken.None);
    }
}
