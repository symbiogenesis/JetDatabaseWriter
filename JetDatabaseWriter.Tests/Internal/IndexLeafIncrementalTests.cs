namespace JetDatabaseWriter.Tests.Internal;

using System.Collections.Generic;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Internal.Builders;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Unit tests for <see cref="IndexLeafIncremental"/> — the single-leaf incremental
/// in-place leaf splice helper. Decode → splice → re-emit must round-trip
/// back to entries that match the original input.
/// </summary>
public sealed class IndexLeafIncrementalTests
{
    private const int PageSize = 4096;
    private const long ParentTdef = 7;

    [Fact]
    public void DecodeEntries_RoundTripsThreeIntKeys()
    {
        var entries = new List<IndexLeafPageBuilder.LeafEntry>
        {
            new(IndexKeyEncoder.EncodeEntry(0x04, 1, ascending: true), 100, 0),
            new(IndexKeyEncoder.EncodeEntry(0x04, 2, ascending: true), 100, 1),
            new(IndexKeyEncoder.EncodeEntry(0x04, 3, ascending: true), 100, 2),
        };

        byte[] page = IndexLeafPageBuilder.BuildJet4LeafPage(
            PageSize, ParentTdef, entries, prevPage: 0, nextPage: 0, tailPage: 0, enablePrefixCompression: true);

        List<IndexLeafIncremental.DecodedEntry> decoded = IndexLeafIncremental.DecodeEntries(page, PageSize);

        Assert.Equal(3, decoded.Count);
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(entries[i].EncodedKey, decoded[i].Key);
            Assert.Equal(entries[i].DataPage, decoded[i].DataPage);
            Assert.Equal(entries[i].DataRow, decoded[i].DataRow);
        }
    }

    [Fact]
    public void DecodeEntries_EmptyLeaf_ReturnsEmptyList()
    {
        byte[] page = IndexLeafPageBuilder.BuildJet4LeafPage(PageSize, ParentTdef, []);
        List<IndexLeafIncremental.DecodedEntry> decoded = IndexLeafIncremental.DecodeEntries(page, PageSize);
        Assert.Empty(decoded);
    }

    [Fact]
    public void IsSingleRootLeaf_TrueForFreshLeaf()
    {
        byte[] page = IndexLeafPageBuilder.BuildJet4LeafPage(PageSize, ParentTdef, []);
        Assert.True(IndexLeafIncremental.IsSingleRootLeaf(page));
    }

    [Fact]
    public void IsSingleRootLeaf_FalseWhenSiblingPointersSet()
    {
        byte[] page = IndexLeafPageBuilder.BuildJet4LeafPage(
            PageSize, ParentTdef, [], prevPage: 0, nextPage: 99, tailPage: 0);
        Assert.False(IndexLeafIncremental.IsSingleRootLeaf(page));
    }

    [Fact]
    public void IsSingleRootLeaf_FalseForIntermediatePage()
    {
        byte[] page = new byte[PageSize];
        page[0] = 0x03; // intermediate
        page[1] = 0x01;
        Assert.False(IndexLeafIncremental.IsSingleRootLeaf(page));
    }

    [Fact]
    public void Splice_InsertSortsByKey()
    {
        var existing = new List<IndexLeafIncremental.DecodedEntry>
        {
            new(IndexKeyEncoder.EncodeEntry(0x04, 1, true), 100, 0),
            new(IndexKeyEncoder.EncodeEntry(0x04, 3, true), 100, 1),
        };
        var adds = new List<(byte[], long, byte)>
        {
            (IndexKeyEncoder.EncodeEntry(0x04, 2, true), 100, 2),
        };

        List<IndexLeafPageBuilder.LeafEntry>? spliced = IndexLeafIncremental.Splice(existing, adds, []);
        Assert.NotNull(spliced);
        Assert.Equal(3, spliced!.Count);
        Assert.Equal(0, spliced[0].DataRow);
        Assert.Equal(2, spliced[1].DataRow); // key=2 sorts between 1 and 3
        Assert.Equal(1, spliced[2].DataRow);
    }

    [Fact]
    public void Splice_RemoveByPageRowPointer()
    {
        var existing = new List<IndexLeafIncremental.DecodedEntry>
        {
            new(IndexKeyEncoder.EncodeEntry(0x04, 1, true), 100, 0),
            new(IndexKeyEncoder.EncodeEntry(0x04, 2, true), 100, 1),
            new(IndexKeyEncoder.EncodeEntry(0x04, 3, true), 100, 2),
        };

        List<IndexLeafPageBuilder.LeafEntry>? spliced = IndexLeafIncremental.Splice(
            existing, [], [(100, 1)]);

        Assert.NotNull(spliced);
        Assert.Equal(2, spliced!.Count);
        Assert.Equal(0, spliced[0].DataRow);
        Assert.Equal(2, spliced[1].DataRow);
    }

    [Fact]
    public void Splice_ReturnsNullWhenRemoveTargetMissing()
    {
        var existing = new List<IndexLeafIncremental.DecodedEntry>
        {
            new(IndexKeyEncoder.EncodeEntry(0x04, 1, true), 100, 0),
        };

        List<IndexLeafPageBuilder.LeafEntry>? spliced = IndexLeafIncremental.Splice(
            existing, [], [(999, 99)]); // not present

        Assert.Null(spliced);
    }

    [Fact]
    public void Splice_CombinedInsertAndDelete()
    {
        var existing = new List<IndexLeafIncremental.DecodedEntry>
        {
            new(IndexKeyEncoder.EncodeEntry(0x04, 1, true), 100, 0),
            new(IndexKeyEncoder.EncodeEntry(0x04, 5, true), 100, 1),
        };
        var adds = new List<(byte[], long, byte)>
        {
            (IndexKeyEncoder.EncodeEntry(0x04, 3, true), 100, 2),
        };

        List<IndexLeafPageBuilder.LeafEntry>? spliced = IndexLeafIncremental.Splice(
            existing, adds, [(100, 0)]);

        Assert.NotNull(spliced);
        Assert.Equal(2, spliced!.Count);
        Assert.Equal(2, spliced[0].DataRow); // new key=3
        Assert.Equal(1, spliced[1].DataRow); // existing key=5
    }

    [Fact]
    public void TryRebuildLeaf_RoundTripsThroughSeekableEncoding()
    {
        var entries = new List<IndexLeafPageBuilder.LeafEntry>
        {
            new(IndexKeyEncoder.EncodeEntry(0x04, 1, true), 200, 0),
            new(IndexKeyEncoder.EncodeEntry(0x04, 2, true), 200, 1),
        };

        byte[]? page = IndexLeafIncremental.TryRebuildLeaf(PageSize, ParentTdef, entries);
        Assert.NotNull(page);
        Assert.Equal(0x04, page![0]);
        Assert.True(IndexLeafIncremental.IsSingleRootLeaf(page));

        List<IndexLeafIncremental.DecodedEntry> decoded = IndexLeafIncremental.DecodeEntries(page, PageSize);
        Assert.Equal(2, decoded.Count);
    }

    [Fact]
    public void TryRebuildLeaf_ReturnsNullOnOverflow()
    {
        // Build an entry list that vastly exceeds the leaf payload capacity.
        // Each int entry is ~9 bytes; 1000 entries blow past the ~3616-byte
        // payload area on a 4 KB page.
        var entries = new List<IndexLeafPageBuilder.LeafEntry>(1000);
        for (int i = 0; i < 1000; i++)
        {
            entries.Add(new IndexLeafPageBuilder.LeafEntry(
                IndexKeyEncoder.EncodeEntry(0x04, i, true), 100, 0));
        }

        byte[]? page = IndexLeafIncremental.TryRebuildLeaf(PageSize, ParentTdef, entries);
        Assert.Null(page);
    }

    [Fact]
    public void DecodeSpliceRebuild_PrefixCompressedRoundTrip()
    {
        // Build a leaf with strongly-shared prefixes so prefix compression
        // kicks in, then splice-add a new entry and verify the canonical
        // bytes survive the compress→decompress round trip.
        var entries = new List<IndexLeafPageBuilder.LeafEntry>
        {
            new(IndexKeyEncoder.EncodeEntry(0x04, 1000, true), 100, 0),
            new(IndexKeyEncoder.EncodeEntry(0x04, 1001, true), 100, 1),
            new(IndexKeyEncoder.EncodeEntry(0x04, 1002, true), 100, 2),
        };

        byte[] page = IndexLeafPageBuilder.BuildJet4LeafPage(
            PageSize, ParentTdef, entries, 0, 0, 0, enablePrefixCompression: true);

        List<IndexLeafIncremental.DecodedEntry> decoded = IndexLeafIncremental.DecodeEntries(page, PageSize);
        Assert.Equal(3, decoded.Count);

        var adds = new List<(byte[], long, byte)>
        {
            (IndexKeyEncoder.EncodeEntry(0x04, 1003, true), 100, 3),
        };

        List<IndexLeafPageBuilder.LeafEntry>? spliced = IndexLeafIncremental.Splice(decoded, adds, []);
        Assert.NotNull(spliced);

        byte[]? newPage = IndexLeafIncremental.TryRebuildLeaf(PageSize, ParentTdef, spliced!);
        Assert.NotNull(newPage);

        List<IndexLeafIncremental.DecodedEntry> reDecoded = IndexLeafIncremental.DecodeEntries(newPage!, PageSize);
        Assert.Equal(4, reDecoded.Count);
        for (int i = 0; i < 4; i++)
        {
            Assert.Equal(spliced[i].EncodedKey, reDecoded[i].Key);
        }
    }

    [Fact]
    public void IsIntermediate_TrueOnly_For_PageType_03()
    {
        byte[] inter = new byte[PageSize];
        inter[0] = 0x03;
        Assert.True(IndexLeafIncremental.IsIntermediate(inter));

        byte[] leaf = IndexLeafPageBuilder.BuildJet4LeafPage(PageSize, ParentTdef, []);
        Assert.False(IndexLeafIncremental.IsIntermediate(leaf));
    }

    [Fact]
    public void ReadNextLeafPage_ReturnsHeaderField()
    {
        byte[] page = IndexLeafPageBuilder.BuildJet4LeafPage(
            PageSize, ParentTdef, [], prevPage: 0, nextPage: 12345, tailPage: 0);
        Assert.Equal(12345, IndexLeafIncremental.ReadNextLeafPage(page));
    }

    [Fact]
    public void ReadFirstChildPointer_RecoversChildPageFromMultiLevelTree()
    {
        // Build a multi-level tree by feeding IndexBTreeBuilder enough int
        // entries to overflow a single leaf, then walk into the root and
        // confirm the first intermediate entry's child pointer matches the
        // first leaf's allocated page number (sequential allocation by the
        // builder starting at FirstPageNumber).
        var entries = new List<IndexLeafPageBuilder.LeafEntry>(800);
        for (int i = 0; i < 800; i++)
        {
            entries.Add(new IndexLeafPageBuilder.LeafEntry(
                IndexKeyEncoder.EncodeEntry(0x04, i, true), 100 + (i / 10), (byte)(i % 10)));
        }

        IndexBTreeBuilder.BuildResult build = IndexBTreeBuilder.Build(PageSize, ParentTdef, entries, firstPageNumber: 50);
        Assert.True(build.RootPageNumber > build.FirstPageNumber, "Expected the multi-level tree to root at an intermediate page above its leaves.");

        int rootIdx = (int)(build.RootPageNumber - build.FirstPageNumber);
        byte[] root = build.Pages[rootIdx];
        Assert.True(IndexLeafIncremental.IsIntermediate(root));

        long firstChild = IndexLeafIncremental.ReadFirstChildPointer(root, PageSize);

        // The first intermediate entry summarises the first leaf page, which
        // is the page allocated at FirstPageNumber.
        Assert.Equal(build.FirstPageNumber, firstChild);
    }

    [Fact]
    public void ReadFirstChildPointer_ReturnsZero_OnNonIntermediatePage()
    {
        byte[] leaf = IndexLeafPageBuilder.BuildJet4LeafPage(PageSize, ParentTdef, []);
        Assert.Equal(0, IndexLeafIncremental.ReadFirstChildPointer(leaf, PageSize));
    }
}
