namespace JetDatabaseWriter.Tests;

using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// W5 round-trip tests for the index B-tree maintenance hooks added to
/// <see cref="IAccessWriter.InsertRowAsync(string, object[], System.Threading.CancellationToken)"/>,
/// <see cref="IAccessWriter.InsertRowsAsync(string, System.Collections.Generic.IEnumerable{object[]}, System.Threading.CancellationToken)"/>,
/// <see cref="IAccessWriter.UpdateRowsAsync"/>,
/// <see cref="IAccessWriter.DeleteRowsAsync"/>, and the copy-and-swap path used by
/// <see cref="IAccessWriter.AddColumnAsync"/> /
/// <see cref="IAccessWriter.DropColumnAsync"/> /
/// <see cref="IAccessWriter.RenameColumnAsync"/>.
/// <para>
/// The library does not expose an internal API to walk an index B-tree, so
/// these tests verify maintenance by scanning the on-disk byte stream for
/// leaf pages (<c>page_type = 0x04</c>) and counting their entry-start
/// bitmask bits (one implicit first entry plus one bit per subsequent entry,
/// see <c>docs/design/index-and-relationship-format-notes.md</c> §4.2).
/// Old leaf pages are orphaned by maintenance, so we check the highest-page
/// leaf (always the most recent rebuild) when the row count is expected to
/// shrink, and the highest entry count across all leafs otherwise.
/// </para>
/// </summary>
public sealed class IndexMaintenanceTests
{
    private const int PageSize = 4096; // ACE
    private const int LeafBitmaskOffset = 0x1B;
    private const int LeafFirstEntryOffset = 0x1E0;

    [Fact]
    public async Task InsertRows_RebuildsIndexLeaf_WithExpectedEntryCount()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                new[] { new ColumnDefinition("Id", typeof(int)) },
                new[] { new IndexDefinition("IX_Id", "Id") },
                ct);

            await writer.InsertRowsAsync(
                "T",
                new[]
                {
                    new object[] { 3 },
                    new object[] { 1 },
                    new object[] { 2 },
                },
                ct);
        }

        // The most-recent rebuild produced a leaf with 3 entries; orphaned
        // earlier leafs still show 1 (implicit empty), so MAX is the right
        // signal for the post-grow state.
        Assert.Equal(3, FindMaxLeafEntryCount(stream.ToArray()));
    }

    [Fact]
    public async Task InsertRow_Single_RebuildsIndexLeaf()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                new[] { new ColumnDefinition("Id", typeof(int)) },
                new[] { new IndexDefinition("IX_Id", "Id") },
                ct);

            await writer.InsertRowAsync("T", new object[] { 42 }, ct);
            await writer.InsertRowAsync("T", new object[] { 7 }, ct);
        }

        Assert.Equal(2, FindMaxLeafEntryCount(stream.ToArray()));
    }

    [Fact]
    public async Task UpdateRows_RebuildsIndexLeaf_PreservingRowCount()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                new[]
                {
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Score", typeof(int)),
                },
                new[] { new IndexDefinition("IX_Score", "Score") },
                ct);

            await writer.InsertRowsAsync(
                "T",
                new[]
                {
                    new object[] { 1, 10 },
                    new object[] { 2, 20 },
                    new object[] { 3, 30 },
                },
                ct);

            // Update the indexed column on one row → triggers index maintenance.
            int updated = await writer.UpdateRowsAsync(
                "T",
                "Id",
                2,
                new Dictionary<string, object> { ["Score"] = 99 },
                ct);

            Assert.Equal(1, updated);
        }

        // Row count unchanged; the rebuilt leaf still has 3 entries.
        Assert.Equal(3, FindMaxLeafEntryCount(stream.ToArray()));
    }

    [Fact]
    public async Task DeleteRows_RebuildsIndexLeaf_WithReducedEntryCount()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                new[] { new ColumnDefinition("Id", typeof(int)) },
                new[] { new IndexDefinition("IX_Id", "Id") },
                ct);

            await writer.InsertRowsAsync(
                "T",
                new[]
                {
                    new object[] { 1 },
                    new object[] { 2 },
                    new object[] { 3 },
                    new object[] { 4 },
                },
                ct);

            int deleted = await writer.DeleteRowsAsync("T", "Id", 2, ct);
            Assert.Equal(1, deleted);
        }

        // The latest leaf (highest page number) is the post-delete rebuild
        // and must report the reduced row count of 3.
        Assert.Equal(3, GetLatestLeafEntryCount(stream.ToArray()));
    }

    [Fact]
    public async Task AddColumn_PreservesExistingIndex()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                new[] { new ColumnDefinition("Id", typeof(int)) },
                new[] { new IndexDefinition("IX_Id", "Id") },
                ct);

            await writer.InsertRowsAsync("T", new[] { new object[] { 1 }, new object[] { 2 } }, ct);
            await writer.AddColumnAsync("T", new ColumnDefinition("Note", typeof(string), maxLength: 50), ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        var indexes = await reader.ListIndexesAsync("T", TestContext.Current.CancellationToken);
        Assert.Single(indexes);
        Assert.Equal("IX_Id", indexes[0].Name);
        Assert.Equal("Id", indexes[0].Columns[0].Name);
    }

    [Fact]
    public async Task RenameColumn_RemapsIndexColumnReference()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                new[] { new ColumnDefinition("Id", typeof(int)) },
                new[] { new IndexDefinition("IX_Id", "Id") },
                ct);

            await writer.InsertRowsAsync("T", new[] { new object[] { 1 }, new object[] { 2 } }, ct);
            await writer.RenameColumnAsync("T", "Id", "Identifier", ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        var indexes = await reader.ListIndexesAsync("T", TestContext.Current.CancellationToken);
        Assert.Single(indexes);
        Assert.Equal("IX_Id", indexes[0].Name);
        Assert.Equal("Identifier", indexes[0].Columns[0].Name);
    }

    [Fact]
    public async Task DropColumn_DropsIndexReferencingDroppedColumn()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                new[]
                {
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Score", typeof(int)),
                },
                new[]
                {
                    new IndexDefinition("IX_Id", "Id"),
                    new IndexDefinition("IX_Score", "Score"),
                },
                ct);

            await writer.InsertRowsAsync("T", new[] { new object[] { 1, 10 } }, ct);
            await writer.DropColumnAsync("T", "Score", ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        var indexes = await reader.ListIndexesAsync("T", TestContext.Current.CancellationToken);
        Assert.Single(indexes);
        Assert.Equal("IX_Id", indexes[0].Name);
    }

    [Fact]
    public async Task InsertRows_TextIndex_GeneralLegacyKeys_RebuildsLeaf()
    {
        // W7: text indexes whose values are limited to digits + ASCII letters
        // are now maintained on insert via the General Legacy sort-key encoder.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                new[] { new ColumnDefinition("Code", typeof(string), maxLength: 32) },
                new[] { new IndexDefinition("IX_Code", "Code") },
                ct);

            await writer.InsertRowsAsync(
                "T",
                new[]
                {
                    new object[] { "B" },
                    new object[] { "A" },
                    new object[] { "C" },
                },
                ct);
        }

        Assert.Equal(3, FindMaxLeafEntryCount(stream.ToArray()));
    }

    [Fact]
    public async Task InsertRows_TextIndex_UnsupportedCharacter_LeavesLeafStale()
    {
        // W7 fail-closed: any character outside the General Legacy range
        // (here a space) makes the encoder throw NotSupportedException, and
        // the W5 maintenance loop swallows it to leave the original empty
        // placeholder leaf in place. We verify the inserts succeed and the
        // visible leafs all show the implicit single-entry count.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                new[] { new ColumnDefinition("Code", typeof(string), maxLength: 32) },
                new[] { new IndexDefinition("IX_Code", "Code") },
                ct);

            await writer.InsertRowsAsync(
                "T",
                new[]
                {
                    new object[] { "Hello world" },
                    new object[] { "Foo Bar" },
                },
                ct);
        }

        // Maintenance was skipped, so no leaf has more than the implicit first
        // entry — the placeholder remains, hence MAX == 1.
        Assert.Equal(1, FindMaxLeafEntryCount(stream.ToArray()));
    }

    private static int CountLeafEntries(byte[] fileBytes, int leafOffset)
    {
        // §4.2: one implicit first entry, plus one bit per subsequent entry
        // in the bitmask spanning [0x1B .. 0x1E0).
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
        // The most-recently-written leaf is the one with the highest page number
        // — maintenance always appends new index pages to the end of the file.
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
