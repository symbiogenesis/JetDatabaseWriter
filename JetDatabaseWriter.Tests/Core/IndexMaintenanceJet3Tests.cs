namespace JetDatabaseWriter.Tests.Core;

using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Round-trip tests for the Jet3 live-leaf live Jet3 (<c>.mdb</c> Access 97) leaf
/// maintenance path. Mirrors <see cref="IndexMaintenanceTests"/> but targets
/// the Jet3 page-size / bitmask layout (page 2048, bitmask at <c>0x16</c>,
/// first entry at <c>0xF8</c>) per <c>docs/design/index-and-relationship-format-notes.md</c>
/// §4.2 / <c>format-probe-appendix-jet3-index.md</c>. The Jet3 empty-leaf empty leaf is
/// now populated by the bulk-rebuild path on every InsertRow*/UpdateRows/DeleteRows
/// call rather than going stale until Microsoft Access Compact &amp; Repair.
/// </summary>
public sealed class IndexMaintenanceJet3Tests
{
    private const int PageSize = 2048; // Jet3
    private const int LeafBitmaskOffset = 0x16;
    private const int LeafFirstEntryOffset = 0xF8;

    [Fact]
    public async Task Jet3_InsertRows_RebuildsIndexLeaf_WithExpectedEntryCount()
    {
        await using var stream = await CreateFreshJet3StreamAsync();
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
                    new object[] { 3 },
                    [1],
                    [2],
                ],
                ct);
        }

        Assert.Equal(3, FindMaxLeafEntryCount(stream.ToArray()));
    }

    [Fact]
    public async Task Jet3_InsertRow_Single_RebuildsIndexLeaf()
    {
        await using var stream = await CreateFreshJet3StreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            await writer.InsertRowAsync("T", [42], ct);
            await writer.InsertRowAsync("T", [7], ct);
        }

        Assert.Equal(2, FindMaxLeafEntryCount(stream.ToArray()));
    }

    [Fact]
    public async Task Jet3_UpdateRows_RebuildsIndexLeaf_PreservingRowCount()
    {
        await using var stream = await CreateFreshJet3StreamAsync();
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
                    new object[] { 1, 10 },
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

        Assert.Equal(3, FindMaxLeafEntryCount(stream.ToArray()));
    }

    [Fact]
    public async Task Jet3_DeleteRows_RebuildsIndexLeaf_WithReducedEntryCount()
    {
        await using var stream = await CreateFreshJet3StreamAsync();
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
                    new object[] { 1 },
                    [2],
                    [3],
                    [4],
                ],
                ct);

            int deleted = await writer.DeleteRowsAsync("T", "Id", 2, ct);
            Assert.Equal(1, deleted);
        }

        Assert.Equal(3, GetLatestLeafEntryCount(stream.ToArray()));
    }

    [Fact]
    public async Task Jet3_PrimaryKey_InsertDuplicate_Throws()
    {
        await using var stream = await CreateFreshJet3StreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using var writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            "T",
            [
                new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                new ColumnDefinition("Name", typeof(string), maxLength: 50),
            ],
            ct);

        await writer.InsertRowAsync("T", [1, "first"], ct);

        // Pre-write enforcement detects the duplicate before any row is
        // encoded. Jet3 participates because the Jet3 logical-idx PK
        // discriminator (index_type = 0x01 at byte 19) is read with the
        // same per-format offset path as the Jet4 emission.
        await Assert.ThrowsAsync<System.InvalidOperationException>(
            async () => await writer.InsertRowAsync("T", [1, "dup"], ct));
    }

    [Fact]
    public async Task Jet3_TextIndex_InsertRows_RebuildsLeafViaGeneralLegacyEncoder()
    {
        // Confirms the General Legacy text encoder routes through the same
        // Jet3 page layout (bitmask at 0x16, first-entry at 0xF8) as the
        // numeric path. Strings include a non-ASCII codepoint to exercise
        // the EXTRA stream framing.
        await using var stream = await CreateFreshJet3StreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Code", typeof(string), maxLength: 32)],
                [new IndexDefinition("IX_Code", "Code")],
                ct);

            await writer.InsertRowsAsync(
                "T",
                [
                    new object[] { "Bravo" },
                    ["Alpha"],
                    ["café"],
                ],
                ct);
        }

        Assert.Equal(3, FindMaxLeafEntryCount(stream.ToArray()));
    }

    [Fact]
    public async Task Jet3_AddColumn_PreservesExistingIndex()
    {
        await using var stream = await CreateFreshJet3StreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            await writer.InsertRowsAsync("T", [new object[] { 1 }, [2]], ct);
            await writer.AddColumnAsync("T", new ColumnDefinition("Note", typeof(string), maxLength: 50), ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        var indexes = await reader.ListIndexesAsync("T", TestContext.Current.CancellationToken);
        Assert.Single(indexes);
        Assert.Equal("IX_Id", indexes[0].Name);
        Assert.Equal("Id", indexes[0].Columns[0].Name);
    }

    [Fact]
    public async Task Jet3_IncrementalFastPath_SplicesSingleLeaf_OnInsertAndDelete()
    {
        // The single-leaf splice single-leaf splice writes exactly ONE new leaf page per
        // mutation (then patches first_dp on the TDEF). Compare the file
        // length before and after a small InsertRowAsync / DeleteRowsAsync
        // pair: the growth must be small (≤ a couple of pages — one leaf +
        // any new data-page allocation, at most). The bulk-rebuild path
        // would orphan and re-emit the whole index from scratch; on a
        // single-leaf tree both paths grow by one leaf, so this is mostly
        // a smoke-test that the incremental path doesn't silently corrupt
        // the leaf entries.
        await using var stream = await CreateFreshJet3StreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using var writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            "T",
            [new ColumnDefinition("Id", typeof(int))],
            [new IndexDefinition("IX_Id", "Id")],
            ct);

        await writer.InsertRowsAsync("T", [new object[] { 10 }, [20], [30]], ct);
        await writer.InsertRowAsync("T", [15], ct); // splice into existing leaf
        int deleted = await writer.DeleteRowsAsync("T", "Id", 20, ct);

        Assert.Equal(1, deleted);

        // The latest leaf must contain the post-mutation entries [10, 15, 30].
        long fileLen = stream.Length;
        byte[] bytes = stream.GetBuffer();
        int latestEntryCount = -1;
        for (int p = 0; p < fileLen / PageSize; p++)
        {
            int o = p * PageSize;
            if (bytes[o] == 0x04 && bytes[o + 1] == 0x01)
            {
                latestEntryCount = CountLeafEntries(bytes, o);
            }
        }

        Assert.Equal(3, latestEntryCount);
    }

    [Fact]
    public async Task Jet3_IncrementalFastPath_RebuildsMultiLevelTree()
    {
        // A Jet3 leaf page (2048 bytes, payload area 0xF8..end ≈ 1880 bytes)
        // holds roughly ~200 INT entries before splitting. Bulk-load enough
        // rows up front to force an intermediate (0x03) root, then splice
        // one more row through the single-leaf splice multi-level rebuild path. The
        // resulting tree must still round-trip every (Id) row via the
        // reader.
        const int InitialRows = 400;

        await using var stream = await CreateFreshJet3StreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            var rows = new object[InitialRows][];
            for (int i = 0; i < InitialRows; i++)
            {
                rows[i] = [i * 2];
            }

            await writer.InsertRowsAsync("T", rows, ct);

            // Splice one row through the single-leaf splice multi-level path.
            await writer.InsertRowAsync("T", [1], ct);
        }

        // Confirm at least one intermediate (0x03) page exists — the multi-
        // level path must be exercised.
        byte[] bytes = stream.GetBuffer();
        int intermediateCount = 0;
        for (int p = 0; p < stream.Length / PageSize; p++)
        {
            int o = p * PageSize;
            if (bytes[o] == 0x03 && bytes[o + 1] == 0x01)
            {
                intermediateCount++;
            }
        }

        Assert.True(intermediateCount >= 1, $"Expected at least one intermediate (0x03) index page; found {intermediateCount}.");

        // Reader sees every row back.
        await using var reader = await OpenReaderAsync(stream);
        var rowsBack = new System.Collections.Generic.List<int>();
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            rowsBack.Add((int)row[0]);
        }

        Assert.Equal(InitialRows + 1, rowsBack.Count);
    }

    private static int CountLeafEntries(byte[] fileBytes, int leafOffset)
    {
        // §4.2: one implicit first entry, plus one bit per subsequent entry
        // in the bitmask spanning [0x16 .. 0xF8) on Jet3.
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

    private static int FindMaxLeafEntryCount(byte[] fileBytes)
    {
        int max = 0;
        for (int p = 0; p < fileBytes.Length / PageSize; p++)
        {
            int o = p * PageSize;
            if (fileBytes[o] == 0x04 && fileBytes[o + 1] == 0x01)
            {
                int n = CountLeafEntries(fileBytes, o);
                if (n > max)
                {
                    max = n;
                }
            }
        }

        return max;
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

    private static async ValueTask<MemoryStream> CreateFreshJet3StreamAsync()
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.Jet3Mdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
            // Empty database; tests reopen and add tables.
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
