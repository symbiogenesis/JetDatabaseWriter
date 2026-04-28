namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Round-trip tests for pre-write unique-index enforcement: the duplicate
/// check runs BEFORE the row is encoded and written, so the row never hits
/// disk and the caller-facing error message reflects that.
/// </summary>
public sealed class PreWriteUniqueEnforcementTests
{
    private static readonly int[] ExpectedIds123 = [1, 2, 3];
    private static readonly string[] CompositeAB = ["A", "B"];

    [Fact]
    public async Task SingleInsert_DuplicateAgainstExisting_ThrowsAndLeavesTableUnchanged()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using var writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            "T",
            [new ColumnDefinition("Id", typeof(int))],
            [new IndexDefinition("UQ_Id", "Id") { IsUnique = true }],
            ct);

        await writer.InsertRowAsync("T", [1], ct);
        await writer.InsertRowAsync("T", [2], ct);

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("T", [1], ct));

        // Error message must indicate the conflict was caught BEFORE the
        // row hit disk: it should contain "before any row was written".
        Assert.Contains("before any row was written", ex.Message, StringComparison.Ordinal);

        // Table should still contain exactly the two rows successfully inserted.
        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(2, dt!.Rows.Count);
    }

    [Fact]
    public async Task SingleInsert_DuplicateAgainstExisting_DoesNotConsumeAutoIncrement()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using var writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            "T",
            [
                new ColumnDefinition("Id", typeof(int)) { IsAutoIncrement = true },
                new ColumnDefinition("Tag", typeof(int)),
            ],
            [new IndexDefinition("UQ_Tag", "Tag") { IsUnique = true }],
            ct);

        await writer.InsertRowAsync("T", [DBNull.Value, 100], ct); // Id=1
        await writer.InsertRowAsync("T", [DBNull.Value, 200], ct); // Id=2

        // Duplicate Tag=100 → must throw before consuming Id=3.
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("T", [DBNull.Value, 100], ct));

        // Next successful insert should use Id=3, not Id=4.
        await writer.InsertRowAsync("T", [DBNull.Value, 300], ct);

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        var ids = dt!.Rows.Cast<DataRow>().Select(r => (int)r["Id"]).OrderBy(x => x).ToArray();
        Assert.Equal(ExpectedIds123, ids);
    }

    [Fact]
    public async Task BatchInsert_IntraBatchDuplicate_ThrowsAndPersistsNoRows()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("UQ_Id", "Id") { IsUnique = true }],
                ct);

            var batch = new[]
            {
                new object[] { 1 },
                [2],
                [3],
                [2], // intra-batch duplicate
                [4],
            };

            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.InsertRowsAsync("T", batch, ct));
            Assert.Contains("before any row was written", ex.Message, StringComparison.Ordinal);
        }

        // Re-open and confirm the batch was fully rolled back.
        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Empty(dt!.Rows);
    }

    [Fact]
    public async Task UpdateRows_CreatesDuplicate_ThrowsAndLeavesTableUnchanged()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Code", typeof(int)),
                ],
                [new IndexDefinition("UQ_Code", "Code") { IsUnique = true }],
                ct);

            await writer.InsertRowsAsync(
                "T",
                [
                    [1, 100],
                    [2, 200],
                    [3, 300],
                ],
                ct);

            // Try to update Id=2 so its Code collides with Id=1's Code.
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.UpdateRowsAsync(
                    "T",
                    "Id",
                    2,
                    new Dictionary<string, object> { ["Code"] = 100 },
                    ct));
        }

        // Reopen and confirm the original Code value survived.
        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        var codeById = dt!.Rows.Cast<DataRow>().ToDictionary(r => (int)r["Id"], r => (int)r["Code"]);
        Assert.Equal(100, codeById[1]);
        Assert.Equal(200, codeById[2]);
        Assert.Equal(300, codeById[3]);
    }

    [Fact]
    public async Task MultiColumnUniqueIndex_DuplicateComposite_Throws()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using var writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            "T",
            [
                new ColumnDefinition("A", typeof(int)),
                new ColumnDefinition("B", typeof(int)),
            ],
            [new IndexDefinition("UQ_AB", CompositeAB) { IsUnique = true }],
            ct);

        await writer.InsertRowAsync("T", [1, 10], ct);
        await writer.InsertRowAsync("T", [1, 20], ct); // different B → ok
        await writer.InsertRowAsync("T", [2, 10], ct); // different A → ok

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("T", [1, 10], ct));
    }

    [Fact]
    public async Task PrimaryKey_DuplicateInsert_ThrowsBeforeWrite()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using var writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            "T",
            [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }],
            ct);

        await writer.InsertRowAsync("T", [1], ct);

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("T", [1], ct));
        Assert.Contains("before any row was written", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NonUniqueIndex_DuplicateInsert_IsAllowed()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            // Same Id three times — non-unique → must succeed.
            await writer.InsertRowsAsync(
                "T",
                [
                    [1],
                    [1],
                    [1],
                ],
                ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(3, dt!.Rows.Count);
    }

    // ── helpers (mirrors BigIndexStressTests / IndexWriterAdvancedTests) ───

    private static async ValueTask<MemoryStream> CreateFreshAccdbStreamAsync()
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
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
