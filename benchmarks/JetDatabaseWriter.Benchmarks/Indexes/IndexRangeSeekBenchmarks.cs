namespace JetDatabaseWriter.Benchmarks.Indexes;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using static JetDatabaseWriter.Enums.ColumnType;

[MemoryDiagnoser]
public class IndexRangeSeekBenchmarks
{
    private const long ParentTdefPage = 7;
    private const long FirstPageNumber = 50;
    private const int EntryCount = 10_000;

    private TreeFixture singleColumnTree = null!;
    private TreeFixture compositeTree = null!;
    private IndexCursor singleColumnCursor = null!;
    private IndexCursor compositeCursor = null!;
    private EncodedIndexRange boundedRange;
    private EncodedIndexRange requiredPrefixRange;

    [GlobalSetup]
    public void Setup()
    {
        this.singleColumnTree = BuildTree(BuildSingleColumnEntries());
        this.compositeTree = BuildTree(BuildCompositeEntries());
        this.singleColumnCursor = CreateCursor(this.singleColumnTree);
        this.compositeCursor = CreateCursor(this.compositeTree);
        byte[] rangeLowerKey = EncodeIntKey(4_000);
        byte[] rangeUpperKey = EncodeIntKey(4_250);
        byte[] requiredPrefix = EncodeIntKey(42);
        this.boundedRange = new EncodedIndexRange(
            new EncodedIndexBound(rangeLowerKey, Inclusive: true, IsPrefix: false),
            new EncodedIndexBound(rangeUpperKey, Inclusive: false, IsPrefix: false));
        this.requiredPrefixRange = new EncodedIndexRange(
            new EncodedIndexBound(requiredPrefix, Inclusive: true, IsPrefix: false),
            EncodedIndexBound.None,
            requiredPrefix);
    }

    [Benchmark]
    public async Task<int> BoundedRange()
    {
        List<(long DataPage, int RowIndex)> matches = await this.singleColumnCursor.FindRowLocationsInRangeAsync(
            this.singleColumnTree.RootPageNumber,
            this.boundedRange,
            CancellationToken.None).ConfigureAwait(false);

        return matches.Count;
    }

    [Benchmark]
    public async Task<int> RequiredPrefix()
    {
        List<(long DataPage, int RowIndex)> matches = await this.compositeCursor.FindRowLocationsInRangeAsync(
            this.compositeTree.RootPageNumber,
            this.requiredPrefixRange,
            CancellationToken.None).ConfigureAwait(false);

        return matches.Count;
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

    private static TreeFixture BuildTree(IReadOnlyList<IndexEntry> entries)
    {
        IndexPageLayout layout = IndexPageLayout.Jet4;
        IndexBTreeBuildResult build = IndexBTreeBuilder.Build(
            layout,
            Constants.PageSizes.Jet4,
            ParentTdefPage,
            entries,
            FirstPageNumber);

        var pages = new Dictionary<long, byte[]>(build.Pages.Count);
        for (int pageIndex = 0; pageIndex < build.Pages.Count; pageIndex++)
        {
            pages.Add(build.FirstPageNumber + pageIndex, build.Pages[pageIndex]);
        }

        return new TreeFixture(layout, Constants.PageSizes.Jet4, build.RootPageNumber, pages);
    }

    private static List<IndexEntry> BuildSingleColumnEntries()
    {
        var entries = new List<IndexEntry>(EntryCount);
        for (int value = 0; value < EntryCount; value++)
        {
            entries.Add(new IndexEntry(
                EncodeIntKey(value),
                DataPage: 100 + (value / 200),
                DataRow: (byte)(value % 200)));
        }

        return entries;
    }

    private static List<IndexEntry> BuildCompositeEntries()
    {
        var entries = new List<IndexEntry>(EntryCount);
        for (int tenant = 0; tenant < 100; tenant++)
        {
            for (int value = 0; value < 100; value++)
            {
                entries.Add(new IndexEntry(
                    EncodeCompositeIntKey(tenant, value),
                    DataPage: 1_000 + tenant,
                    DataRow: (byte)value));
            }
        }

        return entries;
    }

    private static byte[] EncodeCompositeIntKey(int first, int second)
    {
        byte[] firstKey = EncodeIntKey(first);
        byte[] secondKey = EncodeIntKey(second);
        byte[] composite = new byte[firstKey.Length + secondKey.Length];
        Buffer.BlockCopy(firstKey, 0, composite, 0, firstKey.Length);
        Buffer.BlockCopy(secondKey, 0, composite, firstKey.Length, secondKey.Length);
        return composite;
    }

    private static byte[] EncodeIntKey(int value) =>
        IndexKeyEncoder.EncodeEntry(LongIntegerType, value, ascending: true);

    private sealed record TreeFixture(
        IndexPageLayout Layout,
        int PageSize,
        long RootPageNumber,
        Dictionary<long, byte[]> Pages);
}
