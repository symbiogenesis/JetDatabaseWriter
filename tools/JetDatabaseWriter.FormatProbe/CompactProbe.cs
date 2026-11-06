namespace JetDatabaseWriter.FormatProbe;

using System.CommandLine;
using System.Text.Json;
using JetDatabaseWriter;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;

/// <summary>Executes Compact &amp; Repair from the FormatProbe CLI.</summary>
internal static class CompactProbe
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    internal static async Task<int> RunCliAsync(string[] args)
    {
        var input = new Option<string>("--input") { Required = true, Description = "Absolute source MDB/ACCDB path." };
        var output = new Option<string?>("--output") { Description = "Absolute destination MDB/ACCDB path (mutually exclusive with --in-place)." };
        var inPlace = new Option<bool>("--in-place") { Description = "Compact and atomically replace --input in place after complete validation." };
        var report = new Option<string?>("--report") { Description = "Optional absolute path for a new JSON compact report." };
        var allowPartialRecovery = new Option<bool>("--allow-partial-recovery") { Description = "Allow writing a validated salvage copy to --output when partial recovery diagnostics are present." };
        var maxPages = new Option<long>("--max-pages") { DefaultValueFactory = _ => 524_288L, Description = "Maximum physical pages." };
        var maxTables = new Option<int>("--max-tables") { DefaultValueFactory = _ => 4_096, Description = "Maximum local tables." };
        var maxWorkingBytes = new Option<long>("--max-working-bytes") { DefaultValueFactory = _ => 512L * 1024 * 1024, Description = "Maximum working memory admission in bytes." };
        var passwordEnvironment = new Option<string?>("--password-environment") { Description = "Name of an environment variable containing the database password." };

        var command = new RootCommand("Compact & Repair a Jet3/Jet4 (.mdb) or ACE (.accdb) database with independent reopened validation.")
        {
            input,
            output,
            inPlace,
            report,
            allowPartialRecovery,
            maxPages,
            maxTables,
            maxWorkingBytes,
            passwordEnvironment,
        };

        command.SetAction(async (parse, cancellationToken) =>
        {
            string sourcePath = parse.GetRequiredValue(input);
            string? destinationPath = parse.GetValue(output);
            bool replaceInPlace = parse.GetValue(inPlace);
            string? reportPath = parse.GetValue(report);
            bool partialRecovery = parse.GetValue(allowPartialRecovery);
            long pageLimit = parse.GetValue(maxPages);
            int tableLimit = parse.GetValue(maxTables);
            long workingLimit = parse.GetValue(maxWorkingBytes);
            string? passwordVariable = parse.GetValue(passwordEnvironment);

            if (!Path.IsPathFullyQualified(sourcePath)
                || (destinationPath is not null && !Path.IsPathFullyQualified(destinationPath))
                || (reportPath is not null && !Path.IsPathFullyQualified(reportPath))
                || (replaceInPlace == (destinationPath is not null))
                || (replaceInPlace && partialRecovery)
                || pageLimit <= 0
                || tableLimit <= 0
                || workingLimit <= 0)
            {
                await Console.Error.WriteLineAsync(
                    "Specify --input as an absolute path and exactly one of --output <absolute path> or --in-place. --allow-partial-recovery cannot be combined with --in-place.",
                    CancellationToken.None).ConfigureAwait(false);
                return 2;
            }

            string? password = passwordVariable is null ? null : Environment.GetEnvironmentVariable(passwordVariable);
            if (passwordVariable is not null && password is null)
            {
                await Console.Error.WriteLineAsync("The requested password environment variable is absent.", CancellationToken.None).ConfigureAwait(false);
                return 2;
            }

            var options = new AccessCompactOptions(password)
            {
                AllowPartialRecovery = partialRecovery,
                MaxPages = pageLimit,
                MaxTables = tableLimit,
                MaxWorkingBytes = workingLimit,
            };

            try
            {
                AccessCompactReport result = replaceInPlace
                    ? await AccessDatabaseCompact.CompactInPlaceAsync(sourcePath, options, cancellationToken).ConfigureAwait(false)
                    : await AccessDatabaseCompact.CompactToAsync(sourcePath, destinationPath!, options, cancellationToken).ConfigureAwait(false);

                if (reportPath is not null)
                {
                    await using var reportStream = new FileStream(reportPath, FileMode.CreateNew, FileAccess.Write, FileShare.Read, 4096, FileOptions.Asynchronous);
                    await JsonSerializer.SerializeAsync(reportStream, result, JsonOptions, cancellationToken).ConfigureAwait(false);
                    await reportStream.FlushAsync(cancellationToken).ConfigureAwait(false);
                }

                Console.WriteLine(
                    $"Compacted {sourcePath}: {result.SourcePages} -> {result.DestinationPages} pages (reclaimed {result.ReclaimedPages} pages, {result.RebuiltTableCount} tables, {result.ActiveRowCount} active rows, complete={result.IsComplete}).");
                return 0;
            }
            catch (OperationCanceledException)
            {
                await Console.Error.WriteLineAsync("Compact cancelled.", CancellationToken.None).ConfigureAwait(false);
                return 3;
            }
            catch (Exception error)
            {
                string code = error is IJetException structured ? structured.ErrorCode.ToString() : error.GetType().Name;
                await Console.Error.WriteLineAsync($"Compact failed ({code}): {error.Message}", CancellationToken.None).ConfigureAwait(false);
                return 1;
            }
        });

        return await command.Parse(args).InvokeAsync(cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }
}
