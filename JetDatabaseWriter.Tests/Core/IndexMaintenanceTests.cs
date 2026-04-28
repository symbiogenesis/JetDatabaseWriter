namespace JetDatabaseWriter.Tests;

using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Round-trip tests for the index B-tree maintenance hooks on
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
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            await writer.InsertRowAsync("T", [42], ct);
            await writer.InsertRowAsync("T", [7], ct);
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
    public async Task RenameColumn_RemapsIndexColumnReference()
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

            await writer.InsertRowsAsync("T", [new object[] { 1 }, [2]], ct);
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
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Score", typeof(int)),
                ],
                [
                    new IndexDefinition("IX_Id", "Id"),
                    new IndexDefinition("IX_Score", "Score"),
                ],
                ct);

            await writer.InsertRowsAsync("T", [new object[] { 1, 10 }], ct);
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
        // Text indexes whose values are limited to digits + ASCII letters
        // are maintained on insert via the General Legacy sort-key encoder.
        await using var stream = await CreateFreshAccdbStreamAsync();
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
                    new object[] { "B" },
                    ["A"],
                    ["C"],
                ],
                ct);
        }

        Assert.Equal(3, FindMaxLeafEntryCount(stream.ToArray()));
    }

    [Fact]
    public async Task InsertRows_TextIndex_UnicodeAndPunctuation_RebuildsLeaf()
    {
        // The full Jackcess General Legacy port supports the entire BMP
        // (spaces, punctuation, accented characters). Strings that previously
        // fell through to the stale-leaf path now participate in the index maintenance
        // bulk B-tree rebuild, so the emitted leaf reflects all inserted rows.
        await using var stream = await CreateFreshAccdbStreamAsync();
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
                    new object[] { "Hello world" },
                    ["Foo Bar"],
                    ["café"],
                ],
                ct);
        }

        Assert.Equal(3, FindMaxLeafEntryCount(stream.ToArray()));
    }

    [Fact]
    public async Task InsertRows_MemoIndex_RebuildsLeafViaSameEncoder()
    {
        // MEMO columns route through the same General Legacy encoder as TEXT
        // (T_TEXT = 0x0A, T_MEMO = 0x0C both supported by IndexKeyEncoder).
        // Round-trip a memo-keyed index and confirm the bulk rebuild populated
        // the leaf instead of leaving the leaf-page emission placeholder in place.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Body", typeof(string))], // maxLength=0 → MEMO
                [new IndexDefinition("IX_Body", "Body")],
                ct);

            await writer.InsertRowsAsync(
                "T",
                [
                    new object[] { "the quick brown fox" },
                    ["jumps over the lazy dog"],
                    ["pack my box with five dozen liquor jugs"],
                ],
                ct);
        }

        Assert.Equal(3, FindMaxLeafEntryCount(stream.ToArray()));
    }

    [Fact]
    public async Task InsertRows_LargeBatch_GrowsToMultiLevelTree_AndStaysEnumerable()
    {
        // Forces a multi-level B-tree by inserting more entries than fit on a
        // single 4 KB leaf (~400 int entries). Verifies that the resulting
        // tree (intermediate root over a chain of leaves) round-trips
        // correctly through the reader.
        const int RowCount = 700;
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            var rows = new List<object[]>(RowCount);
            for (int i = 0; i < RowCount; i++)
            {
                rows.Add([i]);
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        Assert.Equal(0x03, FindLatestRootPageType(stream.ToArray()));

        await using var reader = await OpenReaderAsync(stream);
        var indexes = await reader.ListIndexesAsync("T", ct);
        Assert.Single(indexes);
        Assert.Equal("IX_Id", indexes[0].Name);

        // Rows still readable via table scan (the reader does not consume the
        // index, but the rows-on-disk count is the index's truth source).
        var rowsRead = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.Equal(RowCount, rowsRead.Rows.Count);
    }

    [Fact]
    public async Task InsertRow_AfterMultiLevelTreeExists_RebuildsViaIncrementalPath()
    {
        // Build a multi-level tree, then add a single row. The incremental
        // path must descend into the tree, walk the leaf chain, splice in
        // the new entry, and emit a fresh root. The reader's row count must
        // include the late insert.
        const int InitialRows = 700;
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            var rows = new List<object[]>(InitialRows);
            for (int i = 0; i < InitialRows; i++)
            {
                rows.Add([i]);
            }

            await writer.InsertRowsAsync("T", rows, ct);

            // Late single insert against the now-multi-level tree.
            await writer.InsertRowAsync("T", [InitialRows], ct);
        }

        Assert.Equal(0x03, FindLatestRootPageType(stream.ToArray()));

        await using var reader = await OpenReaderAsync(stream);
        var rowsRead = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.Equal(InitialRows + 1, rowsRead.Rows.Count);
    }

    [Fact]
    public async Task DeleteRows_AfterMultiLevelTreeExists_ShrinksTreeAndStaysConsistent()
    {
        const int InitialRows = 700;
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            var rows = new List<object[]>(InitialRows);
            for (int i = 0; i < InitialRows; i++)
            {
                rows.Add([i]);
            }

            await writer.InsertRowsAsync("T", rows, ct);

            int deleted = await writer.DeleteRowsAsync("T", "Id", 42, ct);
            Assert.Equal(1, deleted);
        }

        await using var reader = await OpenReaderAsync(stream);
        var rowsRead = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.Equal(InitialRows - 1, rowsRead.Rows.Count);
    }

    /// <summary>
    /// Returns the page type byte (<c>0x03</c> for intermediate,
    /// <c>0x04</c> for leaf) of the highest-page-numbered index page in the
    /// file. The most recently maintained index always patches
    /// <c>first_dp</c> to a freshly-allocated page at the end of the file,
    /// so the highest-numbered index page is the current root. Returns -1
    /// when no index page is found.
    /// </summary>
    private static int FindLatestRootPageType(byte[] fileBytes)
    {
        int latest = -1;
        for (int p = 0; p < fileBytes.Length / PageSize; p++)
        {
            int o = p * PageSize;
            byte t = fileBytes[o];
            if ((t == 0x03 || t == 0x04) && fileBytes[o + 1] == 0x01)
            {
                latest = p;
            }
        }

        return latest < 0 ? -1 : fileBytes[latest * PageSize];
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
