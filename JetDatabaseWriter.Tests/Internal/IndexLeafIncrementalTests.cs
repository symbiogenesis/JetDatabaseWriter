namespace JetDatabaseWriter.Tests.Internal;

using System.Collections.Generic;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Internal.Builders;
using JetDatabaseWriter.Internal.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Unit tests for <see cref="IndexLeafIncremental"/> — the single-leaf incremental
/// in-place leaf splice helper. Decode → splice → re-emit must round-trip
/// back to entries that match the original input.
/// Tests run against both Jet3 and Jet4/ACE layouts via <c>[Theory]</c> parameters.
/// </summary>
public sealed class IndexLeafIncrementalTests
{
    private const long ParentTdef = 7;

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void DecodeEntries_RoundTripsThreeIntKeys(DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        var entries = new List<IndexEntry>
        {
            new(IndexKeyEncoder.EncodeEntry(0x04, 1, ascending: true), 100, 0),
            new(IndexKeyEncoder.EncodeEntry(0x04, 2, ascending: true), 100, 1),
            new(IndexKeyEncoder.EncodeEntry(0x04, 3, ascending: true), 100, 2),
        };

        byte[] page = IndexLeafPageBuilder.BuildLeafPage(
            layout, pageSize, ParentTdef, entries, prevPage: 0, nextPage: 0, tailPage: 0, enablePrefixCompression: true);

        List<IndexEntry> decoded = IndexLeafIncremental.DecodeEntries(layout, page, pageSize);

        Assert.Equal(3, decoded.Count);
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(entries[i].Key, decoded[i].Key);
            Assert.Equal(entries[i].DataPage, decoded[i].DataPage);
            Assert.Equal(entries[i].DataRow, decoded[i].DataRow);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void DecodeEntries_EmptyLeaf_ReturnsEmptyList(DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        byte[] page = IndexLeafPageBuilder.BuildLeafPage(layout, pageSize, ParentTdef, [], 0, 0, 0, enablePrefixCompression: false);
        List<IndexEntry> decoded = IndexLeafIncremental.DecodeEntries(layout, page, pageSize);
        Assert.Empty(decoded);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void IsSingleRootLeaf_TrueForFreshLeaf(DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        byte[] page = IndexLeafPageBuilder.BuildLeafPage(layout, pageSize, ParentTdef, [], 0, 0, 0, enablePrefixCompression: false);
        Assert.True(IndexLeafIncremental.IsSingleRootLeaf(page));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void IsSingleRootLeaf_FalseWhenSiblingPointersSet(DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        byte[] page = IndexLeafPageBuilder.BuildLeafPage(
            layout, pageSize, ParentTdef, [], prevPage: 0, nextPage: 99, tailPage: 0, enablePrefixCompression: false);
        Assert.False(IndexLeafIncremental.IsSingleRootLeaf(page));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void IsSingleRootLeaf_FalseForIntermediatePage(DatabaseFormat format)
    {
        byte[] page = new byte[PageSizeOf(format)];
        page[0] = 0x03; // intermediate
        page[1] = 0x01;
        Assert.False(IndexLeafIncremental.IsSingleRootLeaf(page));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void Splice_InsertSortsByKey(DatabaseFormat format)
    {
        _ = format; // Splice is format-agnostic; parameter present for consistency.
        var existing = new List<IndexEntry>
        {
            new(IndexKeyEncoder.EncodeEntry(0x04, 1, true), 100, 0),
            new(IndexKeyEncoder.EncodeEntry(0x04, 3, true), 100, 1),
        };
        var adds = new List<IndexEntry>
        {
            new(IndexKeyEncoder.EncodeEntry(0x04, 2, true), 100, 2),
        };

        List<IndexEntry>? spliced = IndexLeafIncremental.Splice(existing, adds, []);
        Assert.NotNull(spliced);
        Assert.Equal(3, spliced!.Count);
        Assert.Equal(0, spliced[0].DataRow);
        Assert.Equal(2, spliced[1].DataRow); // key=2 sorts between 1 and 3
        Assert.Equal(1, spliced[2].DataRow);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void Splice_RemoveByPageRowPointer(DatabaseFormat format)
    {
        _ = format; // Splice is format-agnostic; parameter present for consistency.
        var existing = new List<IndexEntry>
        {
            new(IndexKeyEncoder.EncodeEntry(0x04, 1, true), 100, 0),
            new(IndexKeyEncoder.EncodeEntry(0x04, 2, true), 100, 1),
            new(IndexKeyEncoder.EncodeEntry(0x04, 3, true), 100, 2),
        };

        List<IndexEntry>? spliced = IndexLeafIncremental.Splice(
            existing, [], [(100, 1)]);

        Assert.NotNull(spliced);
        Assert.Equal(2, spliced!.Count);
        Assert.Equal(0, spliced[0].DataRow);
        Assert.Equal(2, spliced[1].DataRow);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void Splice_ReturnsNullWhenRemoveTargetMissing(DatabaseFormat format)
    {
        _ = format; // Splice is format-agnostic; parameter present for consistency.
        var existing = new List<IndexEntry>
        {
            new(IndexKeyEncoder.EncodeEntry(0x04, 1, true), 100, 0),
        };

        List<IndexEntry>? spliced = IndexLeafIncremental.Splice(
            existing, [], [(999, 99)]); // not present

        Assert.Null(spliced);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void Splice_CombinedInsertAndDelete(DatabaseFormat format)
    {
        _ = format; // Splice is format-agnostic; parameter present for consistency.
        var existing = new List<IndexEntry>
        {
            new(IndexKeyEncoder.EncodeEntry(0x04, 1, true), 100, 0),
            new(IndexKeyEncoder.EncodeEntry(0x04, 5, true), 100, 1),
        };
        var adds = new List<IndexEntry>
        {
            new(IndexKeyEncoder.EncodeEntry(0x04, 3, true), 100, 2),
        };

        List<IndexEntry>? spliced = IndexLeafIncremental.Splice(
            existing, adds, [(100, 0)]);

        Assert.NotNull(spliced);
        Assert.Equal(2, spliced!.Count);
        Assert.Equal(2, spliced[0].DataRow); // new key=3
        Assert.Equal(1, spliced[1].DataRow); // existing key=5
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void TryRebuildLeaf_RoundTripsThroughSeekableEncoding(DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        var entries = new List<IndexEntry>
        {
            new(IndexKeyEncoder.EncodeEntry(0x04, 1, true), 200, 0),
            new(IndexKeyEncoder.EncodeEntry(0x04, 2, true), 200, 1),
        };

        byte[]? page = IndexLeafIncremental.TryRebuildLeaf(layout, pageSize, ParentTdef, entries);
        Assert.NotNull(page);
        Assert.Equal(0x04, page![0]);
        Assert.True(IndexLeafIncremental.IsSingleRootLeaf(page));

        List<IndexEntry> decoded = IndexLeafIncremental.DecodeEntries(layout, page, pageSize);
        Assert.Equal(2, decoded.Count);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void TryRebuildLeaf_ReturnsNullOnOverflow(DatabaseFormat format)
    {
        // Build an entry list that vastly exceeds the leaf payload capacity for
        // either format. Each int entry is ~9 bytes; 1000 entries blow past the
        // ~3616-byte (Jet4) or ~1800-byte (Jet3) payload areas.
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        var entries = new List<IndexEntry>(1000);
        for (int i = 0; i < 1000; i++)
        {
            entries.Add(new IndexEntry(
                IndexKeyEncoder.EncodeEntry(0x04, i, true), 100, 0));
        }

        byte[]? page = IndexLeafIncremental.TryRebuildLeaf(layout, pageSize, ParentTdef, entries);
        Assert.Null(page);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void DecodeSpliceRebuild_PrefixCompressedRoundTrip(DatabaseFormat format)
    {
        // Build a leaf with strongly-shared prefixes so prefix compression
        // kicks in, then splice-add a new entry and verify the canonical
        // bytes survive the compress→decompress round trip.
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        var entries = new List<IndexEntry>
        {
            new(IndexKeyEncoder.EncodeEntry(0x04, 1000, true), 100, 0),
            new(IndexKeyEncoder.EncodeEntry(0x04, 1001, true), 100, 1),
            new(IndexKeyEncoder.EncodeEntry(0x04, 1002, true), 100, 2),
        };

        byte[] page = IndexLeafPageBuilder.BuildLeafPage(
            layout, pageSize, ParentTdef, entries, 0, 0, 0, enablePrefixCompression: true);

        List<IndexEntry> decoded = IndexLeafIncremental.DecodeEntries(layout, page, pageSize);
        Assert.Equal(3, decoded.Count);

        var adds = new List<IndexEntry>
        {
            new(IndexKeyEncoder.EncodeEntry(0x04, 1003, true), 100, 3),
        };

        List<IndexEntry>? spliced = IndexLeafIncremental.Splice(decoded, adds, []);
        Assert.NotNull(spliced);

        byte[]? newPage = IndexLeafIncremental.TryRebuildLeaf(layout, pageSize, ParentTdef, spliced!);
        Assert.NotNull(newPage);

        List<IndexEntry> reDecoded = IndexLeafIncremental.DecodeEntries(layout, newPage!, pageSize);
        Assert.Equal(4, reDecoded.Count);
        for (int i = 0; i < 4; i++)
        {
            Assert.Equal(spliced[i].Key, reDecoded[i].Key);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void IsIntermediate_TrueOnly_For_PageType_03(DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        byte[] inter = new byte[pageSize];
        inter[0] = 0x03;
        Assert.True(IndexLeafIncremental.IsIntermediate(inter));

        byte[] leaf = IndexLeafPageBuilder.BuildLeafPage(layout, pageSize, ParentTdef, [], 0, 0, 0, enablePrefixCompression: false);
        Assert.False(IndexLeafIncremental.IsIntermediate(leaf));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void ReadNextLeafPage_ReturnsHeaderField(DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        byte[] page = IndexLeafPageBuilder.BuildLeafPage(
            layout, pageSize, ParentTdef, [], prevPage: 0, nextPage: 12345, tailPage: 0, enablePrefixCompression: false);
        Assert.Equal(12345, IndexLeafIncremental.ReadNextLeafPage(page));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void ReadFirstChildPointer_RecoversChildPageFromMultiLevelTree(DatabaseFormat format)
    {
        // Build a multi-level tree by feeding IndexBTreeBuilder enough int
        // entries to overflow a single leaf, then walk into the root and
        // confirm the first intermediate entry's child pointer matches the
        // first leaf's allocated page number (sequential allocation by the
        // builder starting at FirstPageNumber).
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        var entries = new List<IndexEntry>(800);
        for (int i = 0; i < 800; i++)
        {
            entries.Add(new IndexEntry(
                IndexKeyEncoder.EncodeEntry(0x04, i, true), 100 + (i / 10), (byte)(i % 10)));
        }

        IndexBTreeBuilder.BuildResult build = IndexBTreeBuilder.Build(layout, pageSize, ParentTdef, entries, firstPageNumber: 50);
        Assert.True(build.RootPageNumber > build.FirstPageNumber, "Expected the multi-level tree to root at an intermediate page above its leaves.");

        int rootIdx = (int)(build.RootPageNumber - build.FirstPageNumber);
        byte[] root = build.Pages[rootIdx];
        Assert.True(IndexLeafIncremental.IsIntermediate(root));

        long firstChild = IndexLeafIncremental.ReadFirstChildPointer(layout, root, pageSize);

        // The first intermediate entry summarises the first leaf page, which
        // is the page allocated at FirstPageNumber.
        Assert.Equal(build.FirstPageNumber, firstChild);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void ReadFirstChildPointer_ReturnsZero_OnNonIntermediatePage(DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        byte[] leaf = IndexLeafPageBuilder.BuildLeafPage(layout, pageSize, ParentTdef, [], 0, 0, 0, enablePrefixCompression: false);
        Assert.Equal(0, IndexLeafIncremental.ReadFirstChildPointer(layout, leaf, pageSize));
    }

    private static int PageSizeOf(DatabaseFormat fmt) =>
        fmt == DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet3 : Constants.PageSizes.Jet4;
}
