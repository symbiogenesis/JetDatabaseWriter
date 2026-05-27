namespace JetDatabaseWriter.Tests.Indexes;

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using Xunit;

/// <summary>
/// Unit tests for the shared index page codec and read-only cursor.
/// </summary>
public sealed class IndexCursorTests
{
    private const long ParentTdefPage = 7;
    private const long FirstPageNumber = 50;

    private readonly CancellationToken cancellationToken = TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void PageCodec_DecodeLeafEntries_RoundTripsBuilderOutput(DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        var layout = IndexLeafPageBuilder.GetLayout(format);
        var entries = BuildIntEntries(8);

        byte[] page = IndexLeafPageBuilder.BuildLeafPage(
            layout,
            pageSize,
            ParentTdefPage,
            entries,
            prevPage: 0,
            nextPage: 0,
            tailPage: 0,
            enablePrefixCompression: true);

        var decoded = IndexPageCodec.DecodeLeafEntries(layout, page, pageSize);

        AssertEntriesEqual(entries, decoded);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task ContainsKeyAsync_MultiLevelTree_FindsExistingKey(DatabaseFormat format)
    {
        var tree = BuildTree(format, BuildIntEntries(900));
        var cursor = CreateCursor(tree);

        byte[] existingKey = IndexKeyEncoder.EncodeEntry(0x04, 750, ascending: true);
        byte[] missingKey = IndexKeyEncoder.EncodeEntry(0x04, 5000, ascending: true);

        Assert.True(await cursor.ContainsKeyAsync(tree.RootPageNumber, existingKey, cancellationToken));
        Assert.False(await cursor.ContainsKeyAsync(tree.RootPageNumber, missingKey, cancellationToken));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task FindRowLocationsAsync_DuplicateKeySpanningLeaves_ReturnsEveryMatch(DatabaseFormat format)
    {
        byte[] duplicateKey = IndexKeyEncoder.EncodeEntry(0x04, 42, ascending: true);
        var entries = BuildDuplicateEntries(duplicateKey, 500);
        var tree = BuildTree(format, entries);
        var cursor = CreateCursor(tree);

        var matches = await cursor.FindRowLocationsAsync(
            tree.RootPageNumber,
            duplicateKey,
            cancellationToken);

        HashSet<(long DataPage, int RowIndex)> expected = entries
            .Select(entry => (entry.DataPage, RowIndex: (int)entry.DataRow))
            .ToHashSet();

        Assert.Equal(expected.Count, matches.Count);
        foreach ((long dataPage, int rowIndex) in matches)
        {
            Assert.Contains((dataPage, rowIndex), expected);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task ContainsKeyAsync_StaleIntermediateSummary_FollowsTailPage(DatabaseFormat format)
    {
        var tree = BuildTree(format, BuildIntEntries(900));
        long tailPageNumber = FindTailLeafPage(tree);
        byte[] appendedKey = IndexKeyEncoder.EncodeEntry(0x04, 5000, ascending: true);
        var appendedEntry = new IndexEntry(appendedKey, DataPage: 999, DataRow: 7);

        byte[] tailPage = tree.Pages[tailPageNumber];
        var tailEntries = IndexPageCodec.DecodeLeafEntries(tree.Layout, tailPage, tree.PageSize);
        tailEntries.Add(appendedEntry);

        var (previousPage, nextPage, tailHeaderPage) = IndexPageCodec.ReadSiblingPointers(tree.Layout, tailPage);
        tree.Pages[tailPageNumber] = IndexLeafPageBuilder.BuildLeafPage(
            tree.Layout,
            tree.PageSize,
            ParentTdefPage,
            tailEntries,
            previousPage,
            nextPage,
            tailHeaderPage,
            enablePrefixCompression: true);

        var cursor = CreateCursor(tree);

        Assert.True(await cursor.ContainsKeyAsync(tree.RootPageNumber, appendedKey, cancellationToken));
        var matches = await cursor.FindRowLocationsAsync(
            tree.RootPageNumber,
            appendedKey,
            cancellationToken);
        Assert.Equal([(999, 7)], matches);
    }

    private static IndexCursor CreateCursor(TreeFixture tree)
        => new(
            tree.Layout,
            (pageNumber, token) => ReadPageAsync(tree.Pages, pageNumber, token),
            tree.PageSize);

    private static ValueTask<byte[]> ReadPageAsync(
        Dictionary<long, byte[]> pages,
        long pageNumber,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new ValueTask<byte[]>(pages[pageNumber]);
    }

    private static TreeFixture BuildTree(DatabaseFormat format, IReadOnlyList<IndexEntry> entries)
    {
        int pageSize = PageSizeOf(format);
        var layout = IndexLeafPageBuilder.GetLayout(format);
        var build = IndexBTreeBuilder.Build(
            layout,
            pageSize,
            ParentTdefPage,
            entries,
            FirstPageNumber);

        var pages = new Dictionary<long, byte[]>(build.Pages.Count);
        for (int pageIndex = 0; pageIndex < build.Pages.Count; pageIndex++)
        {
            pages.Add(build.FirstPageNumber + pageIndex, build.Pages[pageIndex]);
        }

        return new TreeFixture(layout, pageSize, build.RootPageNumber, pages);
    }

    private static List<IndexEntry> BuildIntEntries(int count)
    {
        var entries = new List<IndexEntry>(count);
        for (int entryNumber = 0; entryNumber < count; entryNumber++)
        {
            entries.Add(new IndexEntry(
                IndexKeyEncoder.EncodeEntry(0x04, entryNumber, ascending: true),
                DataPage: 100 + (entryNumber / 200),
                DataRow: (byte)(entryNumber % 200)));
        }

        return entries;
    }

    private static List<IndexEntry> BuildDuplicateEntries(byte[] key, int count)
    {
        var entries = new List<IndexEntry>(count);
        for (int entryNumber = 0; entryNumber < count; entryNumber++)
        {
            entries.Add(new IndexEntry(
                key,
                DataPage: 200 + (entryNumber / 200),
                DataRow: (byte)(entryNumber % 200)));
        }

        return entries;
    }

    private static long FindTailLeafPage(TreeFixture tree)
    {
        foreach (var page in tree.Pages)
        {
            if (!IndexPageCodec.IsLeaf(page.Value))
            {
                continue;
            }

            long nextPage = IndexPageCodec.ReadNextPage(tree.Layout, page.Value);
            if (nextPage == 0)
            {
                return page.Key;
            }
        }

        Assert.Fail("Expected a tail leaf page.");
        return 0;
    }

    private static void AssertEntriesEqual(List<IndexEntry> expected, List<IndexEntry> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (int entryIndex = 0; entryIndex < expected.Count; entryIndex++)
        {
            Assert.Equal(expected[entryIndex].Key, actual[entryIndex].Key);
            Assert.Equal(expected[entryIndex].DataPage, actual[entryIndex].DataPage);
            Assert.Equal(expected[entryIndex].DataRow, actual[entryIndex].DataRow);
        }
    }

    private static int PageSizeOf(DatabaseFormat format)
        => format == DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet3 : Constants.PageSizes.Jet4;

    private sealed record TreeFixture(
        IndexLeafPageBuilder.LeafPageLayout Layout,
        int PageSize,
        long RootPageNumber,
        Dictionary<long, byte[]> Pages);
}
