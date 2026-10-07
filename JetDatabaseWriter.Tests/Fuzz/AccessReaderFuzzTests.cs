namespace JetDatabaseWriter.Tests.Fuzz;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Fuzz test for AccessReader. This test awaits one process-isolated fuzz input selected by the hosted driver.
/// It is NOT required for full code coverage and should be run as an explicit <c>Category=Fuzz</c> test because it performs a complete mutation iteration.
/// For full coverage, prefer targeted unit tests that systematically exercise each feature and branch.
/// </summary>
/// <param name="output">The output.</param>
public class AccessReaderFuzzTests(ITestOutputHelper output)
{
    [Trait("Category", "Fuzz")]
    [Fact(Explicit = true)]
    public async Task FuzzAccessReader()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var inputStream = new MemoryStream(await FuzzInput.ReadAsync(ct), writable: false);
        await RunIterationAsync(inputStream);

        async Task RunIterationAsync(Stream stream)
        {
            output.WriteLine($"--- Fuzzing iteration started at {DateTime.UtcNow:O} ---");
            byte[]? fuzzedBytes = null;
            try
            {
                // Read fuzzed input for logging and saving on crash
                fuzzedBytes = new byte[stream.Length];
                await stream.ReadExactlyAsync(fuzzedBytes);
                stream.Position = 0;

                var random = FuzzRandom.Create(fuzzedBytes);
                output.WriteLine($"[Fuzzing] FuzzRandom bytes: {fuzzedBytes.Length}");

                // The hosted driver mutates real database fixtures before this process starts.
                await using var processedStream = new MemoryStream(fuzzedBytes, writable: false);
                var options = new AccessReaderOptions();
                await using AccessReader reader = await AccessReader.OpenAsync(processedStream, options, cancellationToken: ct);

                LogReaderState(output, reader);
                await ReadDiscoveredTablesAsync(output, reader, random, ct);
            }
            catch (IOException ex)
            {
                LogExpectedIterationException(output, ex);
            }
            catch (UnauthorizedAccessException ex)
            {
                LogExpectedIterationException(output, ex);
            }
            catch (InvalidDataException ex)
            {
                LogExpectedIterationException(output, ex);
            }
            catch (InvalidOperationException ex)
            {
                LogExpectedIterationException(output, ex);
            }
            catch (NotSupportedException ex)
            {
                LogExpectedIterationException(output, ex);
            }
            catch (ArgumentException ex)
            {
                LogExpectedIterationException(output, ex);
            }
            catch (FormatException ex)
            {
                LogExpectedIterationException(output, ex);
            }
            catch (JetLimitationException ex)
            {
                LogExpectedIterationException(output, ex);
            }

            output.WriteLine($"""
                --- Fuzzing iteration completed at {DateTime.UtcNow:O} ---

                """);
        }
    }

    private static void LogReaderState(ITestOutputHelper output, AccessReader reader)
    {
        output.WriteLine($"CodePage: {reader.CodePage}");
        output.WriteLine($"DatabaseFormat: {reader.DatabaseFormat}");
        output.WriteLine($"PageReadOptimizationMode: {reader.PageReadOptimizationMode}");
        output.WriteLine($"PageCacheSize: {reader.PageCacheSize}");
        output.WriteLine($"PageSize: {reader.PageSize}");
        output.WriteLine($"DiagnosticsEnabled: {reader.DiagnosticsEnabled}");
        output.WriteLine($"LastDiagnostics: {reader.LastDiagnostics}");
    }

    private static async Task ReadDiscoveredTablesAsync(ITestOutputHelper output, AccessReader reader, FuzzRandom random, CancellationToken cancellationToken)
    {
        DataTable tables = await reader.GetTablesAsDataTableAsync(cancellationToken);
        foreach (DataRow row in tables.Rows)
        {
            string? tableName = row["TableName"] as string;
            if (string.IsNullOrEmpty(tableName))
            {
                continue;
            }

            output.WriteLine($"Reading table: {tableName}");
            await RunExpectedReaderOperationAsync(output, $"reading table {tableName}", async () =>
            {
                int maxRows = random.Next(1, 11);
                int count = 0;
                await foreach (object[] dataRow in reader.Rows(tableName, cancellationToken: cancellationToken))
                {
                    _ = dataRow;
                    count++;
                    if (count > maxRows)
                    {
                        break;
                    }
                }
            });

            await RunExpectedReaderOperationAsync(output, $"reading schema for {tableName}", async () =>
            {
                IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync(tableName, cancellationToken);
                output.WriteLine($"Schema columns: {columns.Count}");
            });

            await RunExpectedReaderOperationAsync(output, $"reading indexes for {tableName}", async () =>
            {
                IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(tableName, cancellationToken);
                output.WriteLine($"Index count: {indexes.Count}");
            });
        }
    }

    private static async Task RunExpectedReaderOperationAsync(ITestOutputHelper output, string operation, Func<Task> action)
    {
        try
        {
            await action().ConfigureAwait(false);
        }
        catch (IOException ex)
        {
            LogExpectedException(output, operation, ex);
        }
        catch (UnauthorizedAccessException ex)
        {
            LogExpectedException(output, operation, ex);
        }
        catch (InvalidDataException ex)
        {
            LogExpectedException(output, operation, ex);
        }
        catch (InvalidOperationException ex)
        {
            LogExpectedException(output, operation, ex);
        }
        catch (NotSupportedException ex)
        {
            LogExpectedException(output, operation, ex);
        }
        catch (ArgumentException ex)
        {
            LogExpectedException(output, operation, ex);
        }
        catch (FormatException ex)
        {
            LogExpectedException(output, operation, ex);
        }
        catch (JetLimitationException ex)
        {
            LogExpectedException(output, operation, ex);
        }
    }

    private static void LogExpectedException(ITestOutputHelper output, string operation, Exception ex) =>
        output.WriteLine($"""
            [Fuzzing] Expected exception while {operation}: {ex.GetType().Name}
            {ex}
            """);

    private static void LogExpectedIterationException(ITestOutputHelper output, Exception ex) =>
        output.WriteLine($"""
            [Fuzzing] Expected exception during fuzzing iteration: {ex.GetType().Name}
            {ex}
            """);
}
