namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.ComplexColumns;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// A rolled-back transaction must leave the writer as it was when the
/// transaction began, not only discard the journaled pages. The writer caches
/// the user-table catalog, an insert-page hint, the set of usage maps it may
/// extend, and the per-table constraint registry; every one of them can change
/// inside a transaction, so each must be restored or invalidated on rollback.
/// </summary>
public sealed class TransactionRollbackStateTests
{
    public static TheoryData<DatabaseFormat, int, int> InsertRollbackCases => new()
    {
        { DatabaseFormat.Jet3Mdb, 0, 1 },
        { DatabaseFormat.Jet4Mdb, 0, 1 },
        { DatabaseFormat.AceAccdb, 0, 1 },
        { DatabaseFormat.Jet3Mdb, 3, 300 },
        { DatabaseFormat.Jet4Mdb, 3, 300 },
        { DatabaseFormat.AceAccdb, 3, 300 },
    };

    public static TheoryData<DatabaseFormat, string> SchemaChangeCases => new()
    {
        { DatabaseFormat.Jet4Mdb, "AddColumn" },
        { DatabaseFormat.Jet4Mdb, "DropColumn" },
        { DatabaseFormat.Jet4Mdb, "RenameColumn" },
        { DatabaseFormat.AceAccdb, "AddColumn" },
        { DatabaseFormat.AceAccdb, "DropColumn" },
        { DatabaseFormat.AceAccdb, "RenameColumn" },
    };

    /// <summary>
    /// Inserting after a rolled-back insert used to throw
    /// <see cref="EndOfStreamException"/>: the insert-page hint still pointed
    /// at a data page the transaction had appended, which rollback discarded.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="seedRows">Rows committed before the transaction.</param>
    /// <param name="transactionRows">Rows inserted inside the rolled-back transaction.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(InsertRollbackCases))]
    public async Task InsertRow_AfterRolledBackInsert_Succeeds(DatabaseFormat format, int seedRows, int transactionRows)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(stream, format))
        {
            await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);
            for (int id = 1; id <= seedRows; id++)
            {
                await writer.InsertRowAsync("Items", [id, "Seed" + id], TestContext.Current.CancellationToken);
            }

            JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            List<object?[]> pending = [.. Enumerable.Range(1000, transactionRows).Select(id => new object?[] { id, new string('x', 100) })];
            _ = await writer.InsertRowsAsync("Items", pending, TestContext.Current.CancellationToken);
            await tx.RollbackAsync(TestContext.Current.CancellationToken);

            await writer.InsertRowAsync("Items", [seedRows + 1, "After"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [seedRows + 2, "After"], TestContext.Current.CancellationToken);
        }

        int[] expected = [.. Enumerable.Range(1, seedRows + 2)];
        await using AccessReader reader = await OpenReaderAsync(stream);
        Assert.Equal(expected, await ReadIdsAsync(reader, "Items"));
        Assert.Equal(expected.Length, await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken));
        Assert.Equal(expected.Length, await GetStatsRowCountAsync(reader, "Items"));
    }

    /// <summary>
    /// A table created inside a rolled-back transaction is gone afterwards. The
    /// writer's cached catalog used to keep it, so inserting into it threw
    /// <see cref="EndOfStreamException"/> and re-creating it reported that it
    /// already existed.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateTable_RolledBack_TableIsGoneAndCanBeRecreated(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(stream, format))
        {
            await writer.CreateTableAsync("Seed", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);

            JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await writer.CreateTableAsync("X", ItemsSchema(), TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("X", [1, "In transaction"], TestContext.Current.CancellationToken);
            await tx.RollbackAsync(TestContext.Current.CancellationToken);

            JetOperationException missing = await Assert.ThrowsAsync<JetOperationException>(async () =>
                await writer.InsertRowAsync("X", [2, "After rollback"], TestContext.Current.CancellationToken));
            Assert.Contains("'X'", missing.Message, StringComparison.Ordinal);

            await writer.CreateTableAsync("X", [new ColumnDefinition("Code", typeof(int))], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("X", [7], TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Single(tables, name => string.Equals(name, "X", StringComparison.OrdinalIgnoreCase));

        DataTable x = await reader.ReadTableAsync("X", cancellationToken: TestContext.Current.CancellationToken);
        DataColumn column = Assert.Single(x.Columns.Cast<DataColumn>());
        Assert.Equal("Code", column.ColumnName);
        DataRow row = Assert.Single(x.Rows.Cast<DataRow>());
        Assert.Equal(7, row["Code"]);
    }

    /// <summary>
    /// A table dropped inside a rolled-back transaction is still there
    /// afterwards, with its rows and its in-code rules. The constraint registry
    /// used to have forgotten the rules, so its DefaultValue stopped applying.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DropTable_RolledBack_TableAndRulesRemain(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(stream, format))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new("Id", typeof(int)),
                    new("Score", typeof(int)) { DefaultValue = 7 },
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [1, 5], TestContext.Current.CancellationToken);

            JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await writer.DropTableAsync("T", TestContext.Current.CancellationToken);
            await tx.RollbackAsync(TestContext.Current.CancellationToken);

            await writer.InsertRowAsync("T", [2, DbDefault.Value], TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable table = await reader.ReadTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(
            ["1|5", "2|7"],
            table.Rows.Cast<DataRow>().Select(r => $"{r["Id"]}|{r["Score"]}").Order(StringComparer.Ordinal));
    }

    /// <summary>
    /// Rolling back an AddColumn, DropColumn or RenameColumn used to leave the
    /// constraint registry describing the rewritten table. When the column
    /// count no longer matched the restored table definition, every check on
    /// the table was skipped, so NULL reached the AutoNumber and Required columns.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="schemaChange">The schema change made inside the rolled-back transaction.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(SchemaChangeCases))]
    public async Task Constraints_AfterRolledBackSchemaChange_StillApply(DatabaseFormat format, string schemaChange)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(stream, format))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new("Id", typeof(int)) { IsAutoIncrement = true },
                    new("Name", typeof(string), maxLength: 50) { IsNullable = false },
                    new("Other", typeof(int)),
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [DBNull.Value, "a", 1], TestContext.Current.CancellationToken);

            JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await ChangeSchemaAsync(writer, schemaChange, "Other");
            await tx.RollbackAsync(TestContext.Current.CancellationToken);

            _ = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                await writer.InsertRowAsync("T", [DBNull.Value, DBNull.Value, 2], TestContext.Current.CancellationToken));

            await writer.InsertRowAsync("T", [DBNull.Value, "b", 3], TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable table = await reader.ReadTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(["Id", "Name", "Other"], table.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
        Assert.Equal(
            ["1|a|1", "2|b|3"],
            table.Rows.Cast<DataRow>().Select(r => $"{r["Id"]}|{r["Name"]}|{r["Other"]}").Order(StringComparer.Ordinal));
    }

    /// <summary>
    /// A rolled-back DropColumn or RenameColumn must bring back the column's
    /// in-code rules. They exist only in the writer's constraint registry, so
    /// they used to be lost (dropped) or kept under the wrong name (renamed),
    /// and a later committed schema change then stopped carrying them forward.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="schemaChange">The schema change made inside the rolled-back transaction.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(SchemaChangeCases))]
    public async Task ClrRules_AfterRolledBackSchemaChange_AreRestored(DatabaseFormat format, string schemaChange)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(stream, format))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new("Id", typeof(int)),
                    new("Score", typeof(int)) { DefaultValue = 7, ValidationRule = value => value is int score && score >= 0 },
                    new("Other", typeof(int)),
                ],
                TestContext.Current.CancellationToken);

            JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await ChangeSchemaAsync(writer, schemaChange, "Score");
            await tx.RollbackAsync(TestContext.Current.CancellationToken);

            // A committed rewrite carries the rules forward by column name.
            await writer.AddColumnAsync("T", new ColumnDefinition("Late", typeof(int)), TestContext.Current.CancellationToken);

            await writer.InsertRowAsync("T", [1, DbDefault.Value, 5, DBNull.Value], TestContext.Current.CancellationToken);
            _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () =>
                await writer.InsertRowAsync("T", [2, -1, 5, DBNull.Value], TestContext.Current.CancellationToken));
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable table = await reader.ReadTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        DataRow row = Assert.Single(table.Rows.Cast<DataRow>());
        Assert.Equal(1, row["Id"]);
        Assert.Equal(7, row["Score"]);
    }

    /// <summary>
    /// AutoNumber values a rolled-back transaction consumed are handed out
    /// again: rollback discards the TDEF counter the transaction advanced, so
    /// the writer's in-memory counter goes back with it.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task AutoNumber_AfterRolledBackInserts_ContinuesFromCommittedRows(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(stream, format))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new("Id", typeof(int)) { IsAutoIncrement = true },
                    new("Name", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [DBNull.Value, "a"], TestContext.Current.CancellationToken);

            JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            _ = await writer.InsertRowsAsync(
                "T",
                [[DBNull.Value, "x"], [DBNull.Value, "y"], [DBNull.Value, "z"]],
                TestContext.Current.CancellationToken);
            await tx.RollbackAsync(TestContext.Current.CancellationToken);

            await writer.InsertRowAsync("T", [DBNull.Value, "b"], TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable table = await reader.ReadTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(
            ["1|a", "2|b"],
            table.Rows.Cast<DataRow>().Select(r => $"{r["Id"]}|{r["Name"]}").Order(StringComparer.Ordinal));
    }

    /// <summary>
    /// Inserts assign each row a per-row complex reference from the table's
    /// complex AutoNumber, which the registry caches for the session. A
    /// rolled-back insert discards the TDEF counter it raised, so the
    /// registry's counter must rewind with it and hand the references out
    /// again. ACCDB only: Jet3 and Jet4 cannot declare complex columns.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task ComplexReference_AfterRolledBackInsert_IsReused()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(stream, DatabaseFormat.AceAccdb))
        {
            await writer.CreateTableAsync(
                "Docs",
                [new("Id", typeof(int)), new("Files", typeof(byte[])) { IsAttachment = true }],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Docs", [1, DBNull.Value], TestContext.Current.CancellationToken);

            JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            _ = await writer.InsertRowsAsync("Docs", [[2, DBNull.Value], [3, DBNull.Value]], TestContext.Current.CancellationToken);
            await tx.RollbackAsync(TestContext.Current.CancellationToken);

            await writer.InsertRowAsync("Docs", [4, DBNull.Value], TestContext.Current.CancellationToken);
        }

        ComplexColumnTestSupport.RawTable docs = await ComplexColumnTestSupport.ReadRawTableAsync(stream, "Docs");
        Assert.Equal(
            ["1|1", "4|2"],
            docs.Rows.Select(r => $"{r[0]}|{ComplexColumnTestSupport.Slot(docs, r, "Files")}").Order(StringComparer.Ordinal));
        Assert.Equal(2, docs.ComplexAutoNumber);
    }

    /// <summary>
    /// With <see cref="AccessWriterOptions.UseTransactionalWrites"/>, a call
    /// that fails is rolled back by its implicit transaction. That rollback
    /// must reset the writer too: a batch that appended data pages before
    /// failing used to leave the insert hint past the end of the file.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task InsertRow_AfterFailedAutoCommitBatch_Succeeds(DatabaseFormat format)
    {
        await using var stream = new WriteFaultStream();
        await using (AccessWriter writer = await CreateWriterAsync(stream, format))
        {
            await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [1, "Seed"], TestContext.Current.CancellationToken);
        }

        var transactionalOptions = new AccessWriterOptions
        {
            UseLockFile = false,
            UseByteRangeLocks = false,
            UseTransactionalWrites = true,
            MaxTransactionPageBudget = 8,
        };

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, transactionalOptions, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            stream.FailOnWrite(1);
            List<object?[]> tooManyPages = [.. Enumerable.Range(1000, 400).Select(id => new object?[] { id, new string('x', 100) })];
            _ = await Assert.ThrowsAsync<IOException>(async () =>
                await writer.InsertRowsAsync("Items", tooManyPages, TestContext.Current.CancellationToken));

            await writer.InsertRowAsync("Items", [2, "After"], TestContext.Current.CancellationToken);
        }

        int[] expected = [1, 2];
        await using AccessReader reader = await OpenReaderAsync(stream);
        Assert.Equal(expected, await ReadIdsAsync(reader, "Items"));
        Assert.Equal(expected.Length, await GetStatsRowCountAsync(reader, "Items"));
    }

    /// <summary>
    /// A commit that fails before it writes any page is a rollback: the
    /// transaction reports <see cref="JetTransaction.IsRolledBack"/> and the
    /// writer is reset. A commit-lock timeout used to escape the commit before
    /// the transaction was marked, leaving it neither committed nor rolled back
    /// and the writer's insert hint pointing past the end of the file.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task Commit_WhenCommitLockTimesOut_RollsBackAndResetsWriter()
    {
        if (!OperatingSystem.IsWindows())
        {
            // Same-process byte-range lock contention needs Windows; POSIX
            // record locks are process-scoped.
            return;
        }

        string path = Path.Combine(Path.GetTempPath(), $"TransactionRollbackStateTests_{Guid.NewGuid():N}.accdb");
        var options = new AccessWriterOptions
        {
            UseLockFile = false,
            UseByteRangeLocks = true,
            LockTimeoutMilliseconds = 100,
        };

        try
        {
            // Explicit sharing permits the competing test handle; path writers are exclusive.
            await using (var stream = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite))
            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, options, leaveOpen: true, TestContext.Current.CancellationToken))
            {
                await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);

                JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Items", [1, "In transaction"], TestContext.Current.CancellationToken);

                await using (var holder = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    holder.Lock(0xFFFFFFFCL, 1);
                    JetLockException error = await Assert.ThrowsAsync<JetLockException>(async () =>
                        await tx.CommitAsync(TestContext.Current.CancellationToken));
                    Assert.Equal(JetErrorCode.LockTimeout, error.ErrorCode);
                    holder.Unlock(0xFFFFFFFCL, 1);
                }

                Assert.True(tx.IsRolledBack);
                Assert.False(tx.IsCommitted);

                await writer.InsertRowAsync("Items", [2, "After"], TestContext.Current.CancellationToken);
            }

            int[] expected = [2];
            await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
            Assert.Equal(expected, await ReadIdsAsync(reader, "Items"));
        }
        finally
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
                // Best-effort cleanup.
            }
        }
    }

    private static List<ColumnDefinition> ItemsSchema() =>
    [
        new("Id", typeof(int)),
        new("Label", typeof(string), maxLength: 100),
    ];

    private static ValueTask<AccessWriter> CreateWriterAsync(MemoryStream stream, DatabaseFormat format) =>
        AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }

    private static async ValueTask ChangeSchemaAsync(AccessWriter writer, string schemaChange, string columnName)
    {
        switch (schemaChange)
        {
            case "AddColumn":
                await writer.AddColumnAsync("T", new ColumnDefinition("Extra", typeof(int)), TestContext.Current.CancellationToken);
                break;
            case "DropColumn":
                await writer.DropColumnAsync("T", columnName, TestContext.Current.CancellationToken);
                break;
            case "RenameColumn":
                await writer.RenameColumnAsync("T", columnName, columnName + "Renamed", TestContext.Current.CancellationToken);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(schemaChange), schemaChange, "Unknown schema change.");
        }
    }

    private static async ValueTask<int[]> ReadIdsAsync(AccessReader reader, string tableName)
    {
        var ids = new List<int>();
        await foreach (object[] row in reader.Rows(tableName, cancellationToken: TestContext.Current.CancellationToken))
        {
            ids.Add((int)row[0]);
        }

        ids.Sort();
        return [.. ids];
    }

    private static async ValueTask<long> GetStatsRowCountAsync(AccessReader reader, string tableName)
    {
        IReadOnlyList<TableStat> stats = await reader.GetTableStatsAsync(TestContext.Current.CancellationToken);
        TableStat stat = stats.Single(s => string.Equals(s.Name, tableName, StringComparison.OrdinalIgnoreCase));
        return stat.RowCount;
    }
}
