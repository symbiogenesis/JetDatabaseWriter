namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Round-trip tests for pre-write unique-index enforcement: the duplicate
/// check runs BEFORE the row is encoded and written, so the row never hits
/// disk and the caller-facing error message reflects that.
/// </summary>
public sealed class IndexPreWriteUniqueEnforcementTests
{
    private static readonly int[] ExpectedIds123 = [1, 2, 3];
    private static readonly string[] CompositeAB = ["A", "B"];
    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    [Fact]
    public async Task SingleInsert_DuplicateAgainstExisting_ThrowsAndLeavesTableUnchanged()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using AccessWriter writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            "T",
            [new ColumnDefinition("Id", typeof(int))],
            [new IndexDefinition("UQ_Id", "Id") { IsUnique = true }],
            this.ct);

        await writer.InsertRowAsync("T", [1], this.ct);
        await writer.InsertRowAsync("T", [2], this.ct);

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("T", [1], this.ct));

        // Error message must indicate the conflict was caught BEFORE the
        // row hit disk: it should contain "before any row was written".
        Assert.Contains("before any row was written", ex.Message, StringComparison.Ordinal);

        // Table should still contain exactly the two rows successfully inserted.
        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync("T", cancellationToken: this.ct);
        Assert.NotNull(dt);
        Assert.Equal(2, dt.Rows.Count);
    }

    [Fact]
    public async Task SingleInsert_MemoDuplicateAgainstExisting_ThrowsBeforeWrite()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using AccessWriter writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            "T",
            [new ColumnDefinition("Body", typeof(string))],
            [new IndexDefinition("UQ_Body", "Body") { IsUnique = true }],
            this.ct);

        await writer.InsertRowAsync("T", ["duplicate memo key"], this.ct);

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("T", ["duplicate memo key"], this.ct));

        Assert.Contains("before any row was written", ex.Message, StringComparison.Ordinal);

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync("T", cancellationToken: this.ct);
        Assert.NotNull(dt);
        Assert.Single(dt.Rows);
    }

    [Fact]
    public async Task SingleInsert_DuplicateAgainstExisting_DoesNotConsumeAutoIncrement()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using AccessWriter writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            "T",
            [
                new ColumnDefinition("Id", typeof(int)) { IsAutoIncrement = true },
                new ColumnDefinition("Tag", typeof(int)),
            ],
            [new IndexDefinition("UQ_Tag", "Tag") { IsUnique = true }],
            this.ct);

        await writer.InsertRowAsync("T", [DBNull.Value, 100], this.ct); // Id=1
        await writer.InsertRowAsync("T", [DBNull.Value, 200], this.ct); // Id=2

        // Duplicate Tag=100 → must throw before consuming Id=3.
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("T", [DBNull.Value, 100], this.ct));

        // Next successful insert should use Id=3, not Id=4.
        await writer.InsertRowAsync("T", [DBNull.Value, 300], this.ct);

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync("T", cancellationToken: this.ct);
        Assert.NotNull(dt);
        var ids = dt.Rows.Cast<DataRow>().Select(r => (int)r["Id"]).Order().ToArray();
        Assert.Equal(ExpectedIds123, ids);
    }

    [Fact]
    public async Task BatchInsert_IntraBatchDuplicate_ThrowsAndPersistsNoRows()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("UQ_Id", "Id") { IsUnique = true }],
                this.ct);

            var batch = new[]
            {
                new object[] { 1 },
                [2],
                [3],
                [2], // intra-batch duplicate
                [4],
            };

            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.InsertRowsAsync("T", batch, this.ct));
            Assert.Contains("before any row was written", ex.Message, StringComparison.Ordinal);
        }

        // Re-open and confirm the batch was fully rolled back.
        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync("T", cancellationToken: this.ct);
        Assert.NotNull(dt);
        Assert.Empty(dt.Rows);
    }

    [Fact]
    public async Task UpdateRows_CreatesDuplicate_ThrowsAndLeavesTableUnchanged()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Code", typeof(int)),
                ],
                [new IndexDefinition("UQ_Code", "Code") { IsUnique = true }],
                this.ct);

            await writer.InsertRowsAsync(
                "T",
                [
                    [1, 100],
                    [2, 200],
                    [3, 300],
                ],
                this.ct);

            // Try to update Id=2 so its Code collides with Id=1's Code.
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.UpdateRowsAsync(
                    "T",
                    "Id",
                    2,
                    new Dictionary<string, object?> { ["Code"] = 100 },
                    this.ct));
        }

        // Reopen and confirm the original Code value survived.
        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync("T", cancellationToken: this.ct);
        Assert.NotNull(dt);
        var codeById = dt.Rows.Cast<DataRow>().ToDictionary(r => (int)r["Id"], r => (int)r["Code"]);
        Assert.Equal(100, codeById[1]);
        Assert.Equal(200, codeById[2]);
        Assert.Equal(300, codeById[3]);
    }

    [Fact]
    public async Task MultiColumnUniqueIndex_DuplicateComposite_Throws()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using AccessWriter writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            "T",
            [
                new ColumnDefinition("A", typeof(int)),
                new ColumnDefinition("B", typeof(int)),
            ],
            [new IndexDefinition("UQ_AB", CompositeAB) { IsUnique = true }],
            this.ct);

        await writer.InsertRowAsync("T", [1, 10], this.ct);
        await writer.InsertRowAsync("T", [1, 20], this.ct); // different B → ok
        await writer.InsertRowAsync("T", [2, 10], this.ct); // different A → ok

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("T", [1, 10], this.ct));
    }

    [Fact]
    public async Task PrimaryKey_DuplicateInsert_ThrowsBeforeWrite()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using AccessWriter writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            "T",
            [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }],
            this.ct);

        await writer.InsertRowAsync("T", [1], this.ct);

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("T", [1], this.ct));
        Assert.Contains("before any row was written", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NonUniqueIndex_DuplicateInsert_IsAllowed()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                this.ct);

            // Same Id three times — non-unique → must succeed.
            await writer.InsertRowsAsync(
                "T",
                [
                    [1],
                    [1],
                    [1],
                ],
                this.ct);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync("T", cancellationToken: this.ct);
        Assert.NotNull(dt);
        Assert.Equal(3, dt.Rows.Count);
    }

    // ── helpers (mirrors IndexBulkInsertStressTests / IndexWriterAdvancedTests) ───

    private static async ValueTask<MemoryStream> CreateFreshAccdbStreamAsync()
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        return ms;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }
}
