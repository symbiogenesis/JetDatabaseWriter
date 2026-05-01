namespace JetDatabaseWriter.Tests.Fuzz;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
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
    [Trait("Category", "Fuzz")]
    [Fact]
    public async Task FuzzAccessWriter()
    {
        Fuzzer.Run(async stream =>
        {
            output.WriteLine($"--- Fuzzing iteration started at {DateTime.UtcNow:O} ---");
            byte[]? fuzzedBytes = null;
            try
            {
                // Read fuzzed input for logging and saving on crash
                fuzzedBytes = new byte[stream.Length];
                await stream.ReadExactlyAsync(fuzzedBytes);
                stream.Position = 0;

                await FuzzIterationAsync(output, fuzzedBytes);
            }
            catch (Exception ex)
            {
                output.WriteLine($"[Fuzzing] Caught exception during fuzzing iteration: {ex.GetType().Name}\n{ex}");
                if (fuzzedBytes != null)
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
            }

            output.WriteLine($"--- Fuzzing iteration completed at {DateTime.UtcNow:O} ---\n");
        });
    }

    private static async Task FuzzIterationAsync(ITestOutputHelper output, byte[]? fuzzedBytes)
    {
        // Use fuzzedBytes to drive randomization if available, else fallback to Random
        await using var ms = new MemoryStream();
        FuzzRandom random = FuzzRandom.Create(fuzzedBytes);

        var options = RandomOptions(random);
        var format = RandomFormat(random);

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

        // Try to read back the database with AccessReader for round-trip fuzzing
        try
        {
            ms.Position = 0;
            var readerOptions = new AccessReaderOptions();
            await using var reader = await AccessReader.OpenAsync(ms, readerOptions);
            var tableNames = await reader.ListTablesAsync();
            output.WriteLine($"[RoundTrip] Opened written DB with AccessReader. Tables: [{string.Join(", ", tableNames)}]");
            foreach (var tableName in tableNames)
            {
                output.WriteLine($"[RoundTrip] Reading table: {tableName}");
                int count = 0;
                await foreach (object[] dataRow in reader.Rows(tableName))
                {
                    count++;
                    if (count > 10)
                    {
                        break;
                    }
                }

                output.WriteLine($"[RoundTrip] Read {count} rows from {tableName}");
            }
        }
        catch (Exception ex)
        {
            output.WriteLine($"[RoundTrip] Exception reading written DB: {ex.GetType().Name}\n{ex}");
        }
    }

    private static async Task FuzzTableAsync(AccessWriter writer, ITestOutputHelper output, Random random, int tableIndex)
    {
        string tableName = $"FuzzTable_{tableIndex}_{RandomString(random, 4)}";
        int colCount = random.Next(1, 6);
        var columns = CreateRandomColumns(random, colCount);

        output.WriteLine($"Creating table: {tableName} with {colCount} columns");
        await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);

        int rowCount = random.Next(1, 10);
        for (int r = 0; r < rowCount; r++)
        {
            var row = CreateRandomRow(random, columns);
            output.WriteLine($"Inserting row {r + 1}/{rowCount} into {tableName}");
            await writer.InsertRowAsync(tableName, row, TestContext.Current.CancellationToken);
        }

        if (rowCount > 0 && random.NextDouble() < 0.5)
        {
            await TryUpdateAndDeleteAsync(writer, output, random, tableName, columns);
        }
    }

    private static async Task TryUpdateAndDeleteAsync(AccessWriter writer, ITestOutputHelper output, Random random, string tableName, ColumnDefinition[] columns)
    {
        string predicateColumn = columns[0].Name;
        object? predicateValue = RandomValue(random, columns[0].ClrType);
        var updatedValues = new System.Collections.Generic.Dictionary<string, object>();
        foreach (var col in columns)
        {
            updatedValues[col.Name] = RandomValue(random, col.ClrType) ?? DBNull.Value;
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

    private static async Task FuzzTransactionAsync(AccessWriter writer, ITestOutputHelper output, Random random)
    {
        output.WriteLine("Starting transaction block");
        await using var tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        string txTable = $"TxTable_{RandomString(random, 4)}";
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

    private static Random CreateRandom(string seed) =>
        new(seed.GetHashCode(StringComparison.Ordinal));

    private static AccessWriterOptions RandomOptions(Random random) =>
        new()
        {
            Password = random.NextDouble() < 0.2 ? RandomString(random, random.Next(4, 16)).ToCharArray() : ReadOnlyMemory<char>.Empty,
            UseLockFile = random.NextDouble() < 0.5,
            WriteFullCatalogSchema = random.NextDouble() < 0.5,
            RespectExistingLockFile = random.NextDouble() < 0.5,
            LockFileUserName = random.NextDouble() < 0.2 ? RandomString(random, random.Next(4, 16)) : null,
            LockFileMachineName = random.NextDouble() < 0.2 ? RandomString(random, random.Next(4, 16)) : null,
            UseByteRangeLocks = random.NextDouble() < 0.5,
            LockTimeoutMilliseconds = random.Next(100, 10000),
            MaxTransactionPageBudget = random.Next(1, 32768),
            UseTransactionalWrites = random.NextDouble() < 0.5,
        };

    private static DatabaseFormat RandomFormat(Random random)
    {
        var formats = Enum.GetValues<DatabaseFormat>();
        var formatObj = formats.GetValue(random.Next(formats.Length));
        return formatObj is DatabaseFormat df ? df : DatabaseFormat.Jet3Mdb;
    }

    private static ColumnDefinition[] CreateRandomColumns(Random random, int count)
    {
        var columns = new ColumnDefinition[count];
        for (int i = 0; i < count; i++)
        {
            string colName = $"Col_{i}_{RandomString(random, 3)}";
            var type = RandomType(random);
            columns[i] = new ColumnDefinition(colName, type);
        }

        return columns;
    }

    private static object[] CreateRandomRow(Random random, ColumnDefinition[] columns)
    {
        var row = new object[columns.Length];
        for (int i = 0; i < columns.Length; i++)
        {
            row[i] = RandomValue(random, columns[i].ClrType) ?? DBNull.Value;
        }

        return row;
    }

    private static string RandomString(Random random, int length)
    {
        const string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        var str = new char[length];
        for (int i = 0; i < length; i++)
        {
            str[i] = chars[random.Next(chars.Length)];
        }

        return new string(str);
    }

    private static Type RandomType(Random random)
    {
        Type[] types =
        [
            typeof(int), typeof(long), typeof(short), typeof(byte), typeof(bool),
            typeof(string), typeof(DateTime), typeof(double), typeof(float), typeof(byte[])
        ];

        return types[random.Next(types.Length)];
    }

    private static object? RandomValue(Random random, Type type)
    {
        return type switch
        {
            var t when t == typeof(int) => random.Next(),
            var t when t == typeof(long) => (long)random.Next() << 32 | (long)random.Next(),
            var t when t == typeof(short) => (short)random.Next(short.MinValue, short.MaxValue),
            var t when t == typeof(byte) => (byte)random.Next(byte.MinValue, byte.MaxValue),
            var t when t == typeof(bool) => random.NextDouble() < 0.5,
            var t when t == typeof(string) => RandomString(random, random.Next(0, 20)),
            var t when t == typeof(DateTime) => DateTime.UtcNow.AddDays(random.Next(-10000, 10000)),
            var t when t == typeof(double) => random.NextDouble() * random.Next(),
            var t when t == typeof(float) => (float)(random.NextDouble() * random.Next()),
            var t when t == typeof(byte[]) => RandomBytes(random),
            _ => null,
        };
    }

    private static byte[] RandomBytes(Random random)
    {
        var len = random.Next(0, 32);
        var arr = new byte[len];
        random.NextBytes(arr);
        return arr;
    }
}
