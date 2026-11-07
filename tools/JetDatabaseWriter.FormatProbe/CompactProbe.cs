namespace JetDatabaseWriter.FormatProbe;

using System.CommandLine;
using System.Text.Json;
using JetDatabaseWriter;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;

/// <summary>Executes Compact &amp; Repair or dry-run inspection from the FormatProbe CLI.</summary>
internal static class CompactProbe
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    internal static async Task<int> RunCliAsync(string[] args)
    {
        var input = new Option<string>("--input") { Required = true, Description = "Absolute source MDB/ACCDB path." };
        var output = new Option<string?>("--output") { Description = "Absolute destination MDB/ACCDB path (mutually exclusive with --in-place and --verify-only)." };
        var inPlace = new Option<bool>("--in-place") { Description = "Compact and atomically replace --input in place after complete validation." };
        var verifyOnly = new Option<bool>("--verify-only") { Description = "Run read-only dry-run health, index, counter, and reclamation inspection without writing output." };
        var report = new Option<string?>("--report") { Description = "Optional absolute path for a new JSON compact report." };
        var quarantine = new Option<string?>("--quarantine") { Description = "Optional absolute path for an NDJSON quarantine sidecar file." };
        var allowPartialRecovery = new Option<bool>("--allow-partial-recovery") { Description = "Allow writing a validated salvage copy to --output when partial recovery diagnostics are present." };
        var maxPages = new Option<long>("--max-pages") { DefaultValueFactory = _ => 524_288L, Description = "Maximum physical pages." };
        var maxTables = new Option<int>("--max-tables") { DefaultValueFactory = _ => 4_096, Description = "Maximum local tables." };
        var maxWorkingBytes = new Option<long>("--max-working-bytes") { DefaultValueFactory = _ => 512L * 1024 * 1024, Description = "Maximum working memory admission in bytes." };
        var passwordEnvironment = new Option<string?>("--password-environment") { Description = "Name of an environment variable containing the database password." };

        var command = new RootCommand("Compact & Repair or inspect a Jet3/Jet4 (.mdb) or ACE (.accdb) database with independent reopened validation.")
        {
            input,
            output,
            inPlace,
            verifyOnly,
            report,
            quarantine,
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
            bool inspectOnly = parse.GetValue(verifyOnly);
            string? reportPath = parse.GetValue(report);
            string? quarantinePath = parse.GetValue(quarantine);
            bool partialRecovery = parse.GetValue(allowPartialRecovery);
            long pageLimit = parse.GetValue(maxPages);
            int tableLimit = parse.GetValue(maxTables);
            long workingLimit = parse.GetValue(maxWorkingBytes);
            string? passwordVariable = parse.GetValue(passwordEnvironment);

            int modeCount = (destinationPath is not null ? 1 : 0) + (replaceInPlace ? 1 : 0) + (inspectOnly ? 1 : 0);
            if (!Path.IsPathFullyQualified(sourcePath)
                || (destinationPath is not null && !Path.IsPathFullyQualified(destinationPath))
                || (reportPath is not null && !Path.IsPathFullyQualified(reportPath))
                || (quarantinePath is not null && !Path.IsPathFullyQualified(quarantinePath))
                || modeCount != 1
                || (replaceInPlace && partialRecovery)
                || pageLimit <= 0
                || tableLimit <= 0
                || workingLimit <= 0)
            {
                await Console.Error.WriteLineAsync(
                    "Specify --input as an absolute path and exactly one of --output <absolute path>, --in-place, or --verify-only. --allow-partial-recovery cannot be combined with --in-place.",
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
                QuarantinePath = quarantinePath,
                MaxPages = pageLimit,
                MaxTables = tableLimit,
                MaxWorkingBytes = workingLimit,
            };

            try
            {
                AccessCompactReport result;
                if (inspectOnly)
                {
                    result = await AccessDatabaseCompact.InspectAsync(sourcePath, options, cancellationToken).ConfigureAwait(false);
                }
                else if (replaceInPlace)
                {
                    result = await AccessDatabaseCompact.CompactInPlaceAsync(sourcePath, options, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    result = await AccessDatabaseCompact.CompactToAsync(sourcePath, destinationPath!, options, cancellationToken).ConfigureAwait(false);
                }

                if (reportPath is not null)
                {
                    await using var reportStream = new FileStream(reportPath, FileMode.CreateNew, FileAccess.Write, FileShare.Read, 4096, FileOptions.Asynchronous);
                    await JsonSerializer.SerializeAsync(reportStream, result, JsonOptions, cancellationToken).ConfigureAwait(false);
                    await reportStream.FlushAsync(cancellationToken).ConfigureAwait(false);
                }

                string verb = inspectOnly ? "Inspected" : "Compacted";
                Console.WriteLine(
                    $"{verb} {sourcePath}: {result.SourcePages} -> {result.DestinationPages} pages (reclaimed {result.ReclaimedPages} pages, packed {result.PackedInteriorPages} interior pages, {result.RebuiltTableCount} tables, {result.ActiveRowCount} active rows, regeneratedIndexes={result.RegeneratedIndexCount}, salvagedRows={result.SalvagedRowCount}, repairedCounters={result.RepairedCounterTableCount}, complete={result.IsComplete}).");
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
