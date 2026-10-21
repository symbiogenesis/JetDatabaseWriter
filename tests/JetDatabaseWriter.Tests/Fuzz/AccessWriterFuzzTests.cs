namespace JetDatabaseWriter.Tests.Fuzz;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Fuzz test for AccessWriter. One process-isolated input drives bounded database mutations.
/// Run intentionally on hosted CI; ordinary tests verify the refusal and output oracles.
/// </summary>
/// <param name="output">The output.</param>
public class AccessWriterFuzzTests(ITestOutputHelper output)
{
    private static readonly DatabaseFormat[] Formats = Enum.GetValues<DatabaseFormat>();

    [Trait("Category", "Fuzz")]
    [Fact(Explicit = true)]
    public async Task FuzzAccessWriter()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] input = await FuzzInput.ReadAsync(ct);
        await using var stream = new MemoryStream();
        var random = FuzzRandom.Create(input);
        AccessWriterOptions options = RandomOptions(random);
        DatabaseFormat format = Formats[random.Next(Formats.Length)];

        output.WriteLine($"Creating {format}; password protection: {!options.Password.IsEmpty}");
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, options, leaveOpen: true, ct))
        {
            if (!options.Password.IsEmpty)
            {
                output.WriteLine("[FuzzCoverage] encryption.write");
            }

            int tableCount = random.Next(1, 4);
            for (int t = 0; t < tableCount; t++)
            {
                await FuzzTableAsync(writer, stream, output, random, t, ct);
            }

            await FuzzTransactionAsync(writer, stream, output, random, ct);
        }

        // This verifies library-authored output: any failure, including bad passwords or corrupt data, is a bug.
        await WriterFuzzSafetyOracle.VerifyRoundTripAsync(stream, options.Password, ct);
        output.WriteLine("[FuzzCoverage] writer.roundtrip");
    }

    private static async Task FuzzTableAsync(AccessWriter writer, MemoryStream stream, ITestOutputHelper output, FuzzRandom random, int tableIndex, CancellationToken ct)
    {
        string tableName = $"FuzzTable_{tableIndex}_{random.RandomString(4)}";
        ColumnDefinition[] columns = CreateRandomColumns(random, random.Next(1, 6));
        if (!await ObserveAsync(stream, output, "schema.write", async () => await writer.CreateTableAsync(tableName, columns, ct)))
        {
            return;
        }

        // Ensure later mutations encounter stored rows as well as generated refusals.
        await ObserveAsync(stream, output, "writer.insert", async () => await writer.InsertRowAsync(tableName, CreateRandomRow(random, columns, 1, allowNull: false), ct));
        int rowCount = random.Next(1, 10);
        var rows = new List<object[]>(rowCount);
        for (int r = 0; r < rowCount; r++)
        {
            int id = random.NextDouble() < 0.25 ? 1 : r + 2;
            rows.Add(CreateRandomRow(random, columns, id, allowNull: true));
        }

        await ObserveAsync(stream, output, "writer.insert", async () => _ = await writer.InsertRowsAsync(tableName, rows, ct));
        await FuzzUpdateAndDeleteAsync(writer, stream, output, random, tableName, columns, ct);
        await FuzzSchemaAsync(writer, stream, output, random, tableName, columns, ct);
    }

    private static async Task FuzzUpdateAndDeleteAsync(AccessWriter writer, MemoryStream stream, ITestOutputHelper output, FuzzRandom random, string tableName, ColumnDefinition[] columns, CancellationToken ct)
    {
        var updatedValues = new Dictionary<string, object?>(columns.Length);
        foreach (ColumnDefinition column in columns)
        {
            updatedValues[column.Name] = random.NextDouble() < 0.25 ? DBNull.Value : random.RandomValue(column.ClrType);
        }

        object predicate = random.NextDouble() < 0.5 ? 1 : random.Next();
        await ObserveAsync(stream, output, "writer.update", async () =>
            _ = await writer.UpdateRowsAsync(tableName, columns[0].Name, predicate, updatedValues, ct));
        string deleteTable = random.NextDouble() < 0.25 ? $"Missing_{tableName}" : tableName;
        await ObserveAsync(stream, output, "writer.delete", async () =>
            _ = await writer.DeleteRowsAsync(deleteTable, columns[0].Name, predicate, ct));
    }

    private static async Task FuzzSchemaAsync(AccessWriter writer, MemoryStream stream, ITestOutputHelper output, FuzzRandom random, string tableName, ColumnDefinition[] columns, CancellationToken ct)
    {
        string extraColumn = random.NextDouble() < 0.25 ? columns[0].Name : "Extra";
        await ObserveAsync(stream, output, "schema.write", async () =>
            await writer.AddColumnAsync(tableName, new ColumnDefinition(extraColumn, random.RandomType()), ct));

        string renamed = random.NextDouble() < 0.25 ? "Invalid.Name" : "Renamed";
        await ObserveAsync(stream, output, "schema.rewrite", async () =>
            await writer.RenameColumnAsync(tableName, columns[0].Name, renamed, ct));

        // Dropping a missing column and dropping the sole column are documented schema refusals.
        string dropped = random.NextDouble() < 0.5 ? columns[^1].Name : "Missing";
        await ObserveAsync(stream, output, "schema.rewrite", async () => await writer.DropColumnAsync(tableName, dropped, ct));
        if (random.NextDouble() < 0.25)
        {
            await ObserveAsync(stream, output, "schema.write", async () => await writer.DropTableAsync(tableName, ct));
        }
    }

    private static async Task FuzzTransactionAsync(AccessWriter writer, MemoryStream stream, ITestOutputHelper output, FuzzRandom random, CancellationToken ct)
    {
        byte[] before = stream.ToArray();
        await using JetTransaction transaction = await writer.BeginTransactionAsync(ct);
        string tableName = $"TxTable_{random.RandomString(4)}";
        ColumnDefinition[] columns = [new("TxCol", typeof(int)) { IsPrimaryKey = true }];
        if (await ObserveAsync(stream, output, "schema.write", async () => await writer.CreateTableAsync(tableName, columns, ct)))
        {
            await ObserveAsync(stream, output, "writer.insert", async () => await writer.InsertRowAsync(tableName, [random.Next()], ct));
        }

        if (random.NextDouble() < 0.5)
        {
            await ObserveAsync(stream, output, "transaction.commit", async () => await transaction.CommitAsync(ct));
        }
        else
        {
            await transaction.RollbackAsync(ct);
            WriterFuzzSafetyOracle.AssertUnchanged(stream, before);
            output.WriteLine("[FuzzCoverage] transaction.rollback");
        }
    }

    private static async Task<bool> ObserveAsync(MemoryStream stream, ITestOutputHelper output, string operation, Func<Task> mutation)
    {
        output.WriteLine($"[FuzzCoverage] {operation}");
        Exception? rejection = await WriterFuzzSafetyOracle.ExecuteAsync(stream, mutation);
        if (rejection is not null)
        {
            output.WriteLine($"[WriterRefusal] {operation}: {rejection.GetType().Name}: {rejection.Message}");
            output.WriteLine($"[FuzzCoverage] {operation}.refused");
        }
        else
        {
            output.WriteLine($"[FuzzCoverage] {operation}.succeeded");
        }

        return rejection is null;
    }

    private static AccessWriterOptions RandomOptions(FuzzRandom random) => new()
    {
        Password = random.NextDouble() < 0.25 ? random.RandomString(random.Next(4, 16)).ToCharArray() : ReadOnlyMemory<char>.Empty,
        UseLockFile = false,
        UseByteRangeLocks = false,
        PageCacheSize = random.Next(0, 16),
        MaxTransactionPageBudget = random.NextDouble() < 0.25 ? random.Next(1, 8) : 128,
        UseTransactionalWrites = true,
    };

    private static ColumnDefinition[] CreateRandomColumns(FuzzRandom random, int count)
    {
        var columns = new ColumnDefinition[count];
        columns[0] = new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true };
        for (int i = 1; i < count; i++)
        {
            columns[i] = new ColumnDefinition($"Col_{i}_{random.RandomString(3)}", random.RandomType()) { IsNullable = random.NextDouble() < 0.5 };
        }

        return columns;
    }

    private static object[] CreateRandomRow(FuzzRandom random, ColumnDefinition[] columns, int id, bool allowNull)
    {
        object[] row = new object[columns.Length];
        row[0] = id;
        for (int i = 1; i < columns.Length; i++)
        {
            row[i] = allowNull && random.NextDouble() < 0.25 ? DBNull.Value : random.RandomValue(columns[i].ClrType) ?? DBNull.Value;
        }

        return row;
    }
}
