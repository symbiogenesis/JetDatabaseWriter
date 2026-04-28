namespace JetDatabaseWriter.Tests.Core;

using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Round-trip tests for the single-leaf incremental incremental B-tree maintenance
/// fast path. The fast path is engaged on insert/update/delete when the
/// index B-tree fits on a single leaf page; otherwise the writer falls back
/// to the bulk <c>MaintainIndexesAsync</c> rebuild. Tests cover:
/// <list type="bullet">
///   <item>Insert hits fast path → exactly ONE new leaf page is appended per
///   call (vs. one-leaf-per-rebuild for the bulk path).</item>
///   <item>Delete hits fast path → ONE new leaf is appended, with the deleted
///   row's pointer absent from the new leaf.</item>
///   <item>Read-back of inserted/deleted rows is correct.</item>
///   <item>Index leaf entry count tracks the table row count after each
///   incremental mutation.</item>
/// </list>
/// </summary>
public sealed class IncrementalIndexMaintenanceTests
{
    private const int PageSize = 4096; // ACE
    private const int LeafBitmaskOffset = 0x1B;
    private const int LeafFirstEntryOffset = 0x1E0;

    [Fact]
    public async Task SingleInsert_AppendsExactlyOneNewLeafPage()
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
        }

        int leafCountBefore = CountLeafPages(stream.ToArray());

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.InsertRowAsync("T", [42], ct);
        }

        int leafCountAfter = CountLeafPages(stream.ToArray());

        // Fast path appends one leaf per InsertRowAsync call. The bulk path
        // would also append one leaf for a single-leaf tree, but the win is
        // observable on update/delete and on text/numeric/etc. workloads.
        // Here we just confirm the fast path yields a valid file.
        Assert.Equal(leafCountBefore + 1, leafCountAfter);

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Single(dt!.Rows);
        Assert.Equal(42, dt.Rows[0]["Id"]);
    }

    [Fact]
    public async Task RepeatedSingleInserts_EachAppendsOneLeaf_AllRowsReadable()
    {
        // Demonstrates the fast path advantage: 5 sequential single-row
        // inserts append exactly 5 new leaf pages (one per call), without
        // re-scanning the whole table for each.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);
        }

        int leafCountBefore = CountLeafPages(stream.ToArray());

        await using (var writer = await OpenWriterAsync(stream))
        {
            for (int i = 1; i <= 5; i++)
            {
                await writer.InsertRowAsync("T", [i * 10], ct);
            }
        }

        int leafCountAfter = CountLeafPages(stream.ToArray());
        Assert.Equal(leafCountBefore + 5, leafCountAfter);

        // Latest leaf must hold all 5 entries.
        Assert.Equal(5, GetLatestLeafEntryCount(stream.ToArray()));

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(5, dt!.Rows.Count);
    }

    [Fact]
    public async Task SingleDelete_AppendsOneLeaf_WithReducedEntryCount()
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
            await writer.InsertRowsAsync(
                "T",
                [
                    [1],
                    [2],
                    [3],
                    [4],
                ],
                ct);
        }

        int leafCountBefore = CountLeafPages(stream.ToArray());

        await using (var writer = await OpenWriterAsync(stream))
        {
            int deleted = await writer.DeleteRowsAsync("T", "Id", 2, ct);
            Assert.Equal(1, deleted);
        }

        int leafCountAfter = CountLeafPages(stream.ToArray());
        Assert.Equal(leafCountBefore + 1, leafCountAfter);
        Assert.Equal(3, GetLatestLeafEntryCount(stream.ToArray()));

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(3, dt!.Rows.Count);
    }

    [Fact]
    public async Task UpdateRow_FastPath_RowReadableWithNewValue()
    {
        // Update is delete+insert on the same call; the fast path receives
        // both in a single hint and emits one new leaf per index.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Score", typeof(int)),
                ],
                [new IndexDefinition("IX_Score", "Score")],
                ct);
            await writer.InsertRowsAsync(
                "T",
                [
                    [1, 10],
                    [2, 20],
                    [3, 30],
                ],
                ct);

            int updated = await writer.UpdateRowsAsync(
                "T",
                "Id",
                2,
                new Dictionary<string, object> { ["Score"] = 99 },
                ct);
            Assert.Equal(1, updated);
        }

        Assert.Equal(3, GetLatestLeafEntryCount(stream.ToArray()));

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(3, dt!.Rows.Count);
        bool foundUpdated = false;
        foreach (DataRow row in dt.Rows)
        {
            if ((int)row["Id"] == 2)
            {
                Assert.Equal(99, row["Score"]);
                foundUpdated = true;
            }
        }

        Assert.True(foundUpdated);
    }

    [Fact]
    public async Task FastPath_FallsBackToBulk_WhenLeafOverflows()
    {
        // Small page-fitting table → fast path. Then push enough rows in a
        // single batch to spill the leaf → bulk path takes over (multi-page
        // tree). The end result must still be correct.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            // ~9 bytes per int entry; ~3616 byte payload area / 9 ≈ 400 entries
            // per leaf. Insert 800 to force a multi-leaf bulk rebuild.
            var rows = new object[800][];
            for (int i = 0; i < 800; i++)
            {
                rows[i] = [i + 1];
            }

            await writer.InsertRowsAsync("T", rows, ct);

            // Now insert one more row — fast path won't fit in the single
            // leaf (because the tree is already multi-level), so the bulk
            // path runs. Must succeed without corrupting the file.
            await writer.InsertRowAsync("T", [99999], ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(801, dt!.Rows.Count);
    }

    [Fact]
    public async Task FastPath_TextIndex_InsertReadableAfterIncrementalMaintenance()
    {
        // Text indexes are supported by the General Legacy encoder, so
        // single-row inserts should hit the fast path.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Code", typeof(string), maxLength: 32)],
                [new IndexDefinition("IX_Code", "Code")],
                ct);
            await writer.InsertRowAsync("T", ["alpha"], ct);
            await writer.InsertRowAsync("T", ["beta"], ct);
            await writer.InsertRowAsync("T", ["gamma"], ct);
        }

        Assert.Equal(3, GetLatestLeafEntryCount(stream.ToArray()));

        await using var reader = await OpenReaderAsync(stream);
        DataTable? dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(3, dt!.Rows.Count);
    }

    [Fact]
    public async Task FastPath_UniqueIndex_PreCheckStillFires()
    {
        // The pre-write unique check pre-write unique-index check must still reject duplicates
        // even when the post-mutation index maintenance is incremental.
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

        await Assert.ThrowsAsync<System.InvalidOperationException>(async () =>
            await writer.InsertRowAsync("T", [1], ct));
    }

    private static int CountLeafEntries(byte[] fileBytes, int leafOffset)
    {
        int count = 1;
        for (int i = LeafBitmaskOffset; i < LeafFirstEntryOffset; i++)
        {
            byte b = fileBytes[leafOffset + i];
            for (int bit = 0; bit < 8; bit++)
            {
                if ((b & (1 << bit)) != 0)
                {
                    count++;
                }
            }
        }

        return count;
    }

    private static int CountLeafPages(byte[] fileBytes)
    {
        int n = 0;
        for (int p = 0; p < fileBytes.Length / PageSize; p++)
        {
            int o = p * PageSize;
            if (fileBytes[o] == 0x04 && fileBytes[o + 1] == 0x01)
            {
                n++;
            }
        }

        return n;
    }

    private static int GetLatestLeafEntryCount(byte[] fileBytes)
    {
        int latest = -1;
        for (int p = 0; p < fileBytes.Length / PageSize; p++)
        {
            int o = p * PageSize;
            if (fileBytes[o] == 0x04 && fileBytes[o + 1] == 0x01)
            {
                latest = p;
            }
        }

        Assert.True(latest >= 0, "Expected at least one index leaf page in the file.");
        return CountLeafEntries(fileBytes, latest * PageSize);
    }

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
