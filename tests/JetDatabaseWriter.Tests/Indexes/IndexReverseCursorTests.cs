namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using Xunit;

public sealed class IndexReverseCursorTests
{
    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task Reverse_Stream_Crosses_Leaves_And_Unlisted_Empty_Tail(DatabaseFormat format)
    {
        var profile = JetFormat.ForNewDatabase(format);
        var entries = Enumerable.Range(0, 900).Select(Entry).ToList();
        IndexBTreeBuildResult tree = IndexBTreeBuilder.Build(profile.IndexPage, profile.PageSize, 7, entries, 50);
        var pages = tree.Pages.Select((page, ordinal) => (Number: tree.FirstPageNumber + ordinal, Page: page))
            .ToDictionary(static page => page.Number, static page => page.Page);
        long tail = pages.Single(page => IndexPageCodec.IsLeaf(page.Value) && IndexPageCodec.ReadNextPage(profile.IndexPage, page.Value) == 0).Key;
        long extra = pages.Keys.Max() + 1;
        pages[extra] = IndexPageCodec.BuildLeafPage(profile.IndexPage, profile.PageSize, 7, [Entry(900)], tail, extra + 1, 0, enablePrefixCompression: true);
        pages[extra + 1] = IndexPageCodec.BuildLeafPage(profile.IndexPage, profile.PageSize, 7, [], extra, 0, 0, enablePrefixCompression: true);
        IndexPageCodec.WriteNextPage(profile.IndexPage, pages[tail], extra);
        using IndexCursor cursor = CreateCursor(profile, pages);
        List<(long DataPage, int RowIndex)> actual = [];

        await foreach ((long DataPage, int RowIndex) location in cursor.EnumerateRowLocationsForCriteriaAsync(
            profile,
            "Items",
            new IndexMetadata { FirstDp = checked((int)tree.RootPageNumber) },
            new TableDef(),
            IndexQueryCriteria.AllReverse,
            TestContext.Current.CancellationToken))
        {
            actual.Add(location);
        }

        Assert.Equal(Enumerable.Range(0, 901).Reverse().Select(static value => (100L + (value / 200), value % 200)), actual);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task Reverse_Stream_Rejects_Backward_Cycle_Before_Revisiting_Leaf(DatabaseFormat format)
    {
        var profile = JetFormat.ForNewDatabase(format);
        var pages = new Dictionary<long, byte[]>()
        {
            [50] = IndexPageCodec.BuildLeafPage(profile.IndexPage, profile.PageSize, 7, [Entry(1)], 51, 51, 0, enablePrefixCompression: true),
            [51] = IndexPageCodec.BuildLeafPage(profile.IndexPage, profile.PageSize, 7, [Entry(2)], 50, 0, 0, enablePrefixCompression: true),
        };
        using IndexCursor cursor = CreateCursor(profile, pages);
        List<int> emitted = [];

        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
        {
            await foreach ((long _, int rowIndex) in cursor.EnumerateRowLocationsForCriteriaAsync(
                profile,
                "Items",
                new IndexMetadata { FirstDp = 50 },
                new TableDef(),
                IndexQueryCriteria.AllReverse,
                TestContext.Current.CancellationToken))
            {
                emitted.Add(rowIndex);
            }
        });

        Assert.Equal(JetErrorCode.CorruptIndex, error.ErrorCode);
        Assert.Equal(51, error.ErrorInfo.PageNumber);
        Assert.Equal([2, 1], emitted);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task Reverse_Stream_Observes_Cancellation_Between_Pages(DatabaseFormat format)
    {
        var profile = JetFormat.ForNewDatabase(format);
        var pages = new Dictionary<long, byte[]>()
        {
            [50] = IndexPageCodec.BuildLeafPage(profile.IndexPage, profile.PageSize, 7, [Entry(1)], 0, 51, 0, enablePrefixCompression: true),
            [51] = IndexPageCodec.BuildLeafPage(profile.IndexPage, profile.PageSize, 7, [Entry(2)], 50, 0, 0, enablePrefixCompression: true),
        };
        using CancellationTokenSource cancellation = new();
        int reads = 0;
        using IndexCursor cursor = new(
            profile.IndexPage,
            async (number, _) =>
            {
                if (++reads == 2)
                {
                    await cancellation.CancelAsync();
                }

                return PageLease.Own(pages[number], profile.PageSize, static _ => { });
            },
            profile.PageSize);

        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach ((long _, int _) in cursor.EnumerateRowLocationsForCriteriaAsync(
                profile, "Items", new IndexMetadata { FirstDp = 50 }, new TableDef(), IndexQueryCriteria.AllReverse, cancellation.Token))
            {
                Assert.Fail("The cancelled reverse scan emitted a row.");
            }
        });

        Assert.Equal(2, reads);
    }

    private static IndexEntry Entry(int value)
        => new(IndexKeyEncoder.EncodeEntry(ColumnType.LongIntegerType, value, ascending: true), 100 + (value / 200), checked((byte)(value % 200)));

    private static IndexCursor CreateCursor(JetFormat profile, Dictionary<long, byte[]> pages)
        => new(
            profile.IndexPage,
            (number, token) =>
            {
                token.ThrowIfCancellationRequested();
                return new ValueTask<PageLease>(PageLease.Own(pages[number], profile.PageSize, static _ => { }));
            },
            profile.PageSize);
}
