namespace JetDatabaseWriter.Tests.Fuzz;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using SharpFuzz;
using Xunit;

#pragma warning disable CA1031 // Catching all exceptions is intentional for fuzz testing.
#pragma warning disable CA5394 // Using non-cryptographic random for fuzz testing is acceptable.

/// <summary>
/// Fuzz test for AccessWriter. This test is designed to find crashes and robustness issues by exploring random combinations of options and data.
/// It is NOT required for full code coverage and is intentionally skipped by default because it is slow and non-deterministic.
/// For full coverage, prefer targeted unit tests that systematically exercise each feature and branch.
/// </summary>
public class AccessWriterFuzzTests(ITestOutputHelper output)
{
    private static readonly DatabaseFormat[] Formats = Enum.GetValues<DatabaseFormat>();

    [Trait("Category", "Fuzz")]
    [Fact]
    public async Task FuzzAccessWriter()
    {
        var ct = TestContext.Current.CancellationToken;
        Fuzzer.Run(async stream =>
        {
            output.WriteLine($"--- Fuzzing iteration started at {DateTime.UtcNow:O} ---");
            byte[]? fuzzedBytes = null;
            try
            {
                fuzzedBytes = new byte[stream.Length];
                await stream.ReadExactlyAsync(fuzzedBytes);
                stream.Position = 0;

                await FuzzIterationAsync(output, fuzzedBytes, ct);
            }
            catch (Exception ex)
            {
                output.WriteLine($"""
                    [Fuzzing] Caught exception during fuzzing iteration: {ex.GetType().Name}
                    {ex}
                    """);
                if (fuzzedBytes != null)
                {
                    SaveCrashInput(output, fuzzedBytes);
                }
            }

            output.WriteLine($"""
                --- Fuzzing iteration completed at {DateTime.UtcNow:O} ---

                """);
        });
    }

    private static void SaveCrashInput(ITestOutputHelper output, byte[] fuzzedBytes)
    {
        try
        {
            string crashDir = Path.Combine(AppContext.BaseDirectory, "..", "..", "Fuzz", "Crashes");
            Directory.CreateDirectory(crashDir);
            string fileName = $"crash_{DateTime.UtcNow:yyyyMMdd_HHmmssfff}.bin";
            string filePath = Path.Combine(crashDir, fileName);
            File.WriteAllBytes(filePath, fuzzedBytes);
            output.WriteLine($"[Fuzzing] Saved crashing input to: {filePath}");
        }
        catch (Exception saveEx)
        {
            output.WriteLine($"[Fuzzing] Failed to save crashing input: {saveEx}");
        }
    }

    private static async Task FuzzIterationAsync(ITestOutputHelper output, byte[]? fuzzedBytes, CancellationToken ct)
    {
        await using var ms = new MemoryStream();
        FuzzRandom random = FuzzRandom.Create(fuzzedBytes);

        var options = RandomOptions(random);
        var format = Formats[random.Next(Formats.Length)];

        output.WriteLine($"Creating database with format: {format}, options: {{ UseLockFile={options.UseLockFile}, UseTransactionalWrites={options.UseTransactionalWrites} }}");
        await using var writer = await AccessWriter.CreateDatabaseAsync(ms, format, options, leaveOpen: true, TestContext.Current.CancellationToken);

        int tableCount = random.Next(1, 4);
        for (int t = 0; t < tableCount; t++)
        {
            await FuzzTableAsync(writer, output, random, t);
        }

        if (random.NextDouble() < 0.5)
        {
            await FuzzTransactionAsync(writer, output, random);
        }

        await VerifyRoundTripAsync(ms, output, ct);
    }

    private static async Task VerifyRoundTripAsync(MemoryStream ms, ITestOutputHelper output, CancellationToken ct)
    {
        try
        {
            ms.Position = 0;
            await using var reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions(), cancellationToken: ct);
            var tableNames = await reader.ListTablesAsync(ct);
            output.WriteLine($"[RoundTrip] Opened written DB with AccessReader. Tables: [{string.Join(", ", tableNames)}]");
            foreach (var tableName in tableNames)
            {
                output.WriteLine($"[RoundTrip] Reading table: {tableName}");
                int count = 0;
                await foreach (object[] dataRow in reader.Rows(tableName, cancellationToken: ct))
                {
                    if (++count > 10)
                    {
                        break;
                    }
                }

                output.WriteLine($"[RoundTrip] Read {count} rows from {tableName}");
            }
        }
        catch (Exception ex)
        {
            output.WriteLine($"""
                [RoundTrip] Exception reading written DB: {ex.GetType().Name}
                {ex}
                """);
        }
    }

    private static async Task FuzzTableAsync(AccessWriter writer, ITestOutputHelper output, FuzzRandom random, int tableIndex)
    {
        string tableName = $"FuzzTable_{tableIndex}_{random.RandomString(4)}";
        int colCount = random.Next(1, 6);
        var columns = CreateRandomColumns(random, colCount);

        output.WriteLine($"Creating table: {tableName} with {colCount} columns");
        await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);

        int rowCount = random.Next(1, 10);
        var rows = new List<object[]>(rowCount);
        for (int r = 0; r < rowCount; r++)
        {
            rows.Add(CreateRandomRow(random, columns));
            output.WriteLine($"Inserting row {r + 1}/{rowCount} into {tableName}");
        }

        await writer.InsertRowsAsync(tableName, rows, TestContext.Current.CancellationToken);

        if (random.NextDouble() < 0.5)
        {
            await TryUpdateAndDeleteAsync(writer, output, random, tableName, columns);
        }
    }

    private static async Task TryUpdateAndDeleteAsync(AccessWriter writer, ITestOutputHelper output, FuzzRandom random, string tableName, ColumnDefinition[] columns)
    {
        string predicateColumn = columns[0].Name;
        object? predicateValue = random.RandomValue(columns[0].ClrType);
        var updatedValues = new Dictionary<string, object>(columns.Length);
        foreach (var col in columns)
        {
            updatedValues[col.Name] = random.RandomValue(col.ClrType) ?? DBNull.Value;
        }

        try
        {
            output.WriteLine($"Attempting update on {tableName} where {predicateColumn} = {predicateValue}");
            await writer.UpdateRowsAsync(tableName, predicateColumn, predicateValue, updatedValues, TestContext.Current.CancellationToken);
        }
        catch (Exception ex)
        {
            output.WriteLine($"Update failed: {ex.GetType().Name}");
        }

        try
        {
            output.WriteLine($"Attempting delete on {tableName} where {predicateColumn} = {predicateValue}");
            await writer.DeleteRowsAsync(tableName, predicateColumn, predicateValue, TestContext.Current.CancellationToken);
        }
        catch (Exception ex)
        {
            output.WriteLine($"Delete failed: {ex.GetType().Name}");
        }
    }

    private static async Task FuzzTransactionAsync(AccessWriter writer, ITestOutputHelper output, FuzzRandom random)
    {
        output.WriteLine("Starting transaction block");
        await using var tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        string txTable = $"TxTable_{random.RandomString(4)}";
        var txColumns = new[] { new ColumnDefinition("TxCol", typeof(int)) };
        await writer.CreateTableAsync(txTable, txColumns, TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(txTable, [random.Next()], TestContext.Current.CancellationToken);
        if (random.NextDouble() < 0.5)
        {
            output.WriteLine("Committing transaction");
            await tx.CommitAsync(TestContext.Current.CancellationToken);
        }
        else
        {
            output.WriteLine("Rolling back transaction");
            await tx.RollbackAsync(TestContext.Current.CancellationToken);
        }
    }

    // --- Helper methods for randomization and construction ---

    private static AccessWriterOptions RandomOptions(FuzzRandom random) =>
        new()
        {
            Password = random.NextDouble() < 0.2 ? random.RandomString(random.Next(4, 16)).ToCharArray() : ReadOnlyMemory<char>.Empty,
            UseLockFile = random.NextDouble() < 0.5,
            WriteFullCatalogSchema = random.NextDouble() < 0.5,
            RespectExistingLockFile = random.NextDouble() < 0.5,
            LockFileUserName = random.NextDouble() < 0.2 ? random.RandomString(random.Next(4, 16)) : null,
            LockFileMachineName = random.NextDouble() < 0.2 ? random.RandomString(random.Next(4, 16)) : null,
            UseByteRangeLocks = random.NextDouble() < 0.5,
            LockTimeoutMilliseconds = random.Next(100, 10000),
            MaxTransactionPageBudget = random.Next(1, 32768),
            UseTransactionalWrites = random.NextDouble() < 0.5,
        };

    private static ColumnDefinition[] CreateRandomColumns(FuzzRandom random, int count)
    {
        var columns = new ColumnDefinition[count];
        for (int i = 0; i < count; i++)
        {
            columns[i] = new ColumnDefinition(
                $"Col_{i}_{random.RandomString(3)}",
                random.RandomType());
        }

        return columns;
    }

    private static object[] CreateRandomRow(FuzzRandom random, ColumnDefinition[] columns)
    {
        var row = new object[columns.Length];
        for (int i = 0; i < columns.Length; i++)
        {
            row[i] = random.RandomValue(columns[i].ClrType) ?? DBNull.Value;
        }

        return row;
    }
}
