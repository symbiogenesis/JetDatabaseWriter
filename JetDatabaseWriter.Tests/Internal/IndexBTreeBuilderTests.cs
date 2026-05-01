namespace JetDatabaseWriter.Tests.Internal;

using System.Collections.Generic;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Internal.Builders;
using JetDatabaseWriter.Internal.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention.

/// <summary>
/// Unit tests for <see cref="IndexBTreeBuilder"/>. Verifies B-tree leaf
/// splitting, sibling chains, and intermediate (<c>0x03</c>) page emission.
/// Tests run against both Jet3 and Jet4/ACE layouts via <c>[Theory]</c> parameters.
/// Page format references in
/// <c>docs/design/index-and-relationship-format-notes.md</c> §4.1–4.3.
/// </summary>
public sealed class IndexBTreeBuilderTests
{
    private const long ParentTdef = 100;
    private const long FirstPage = 50;

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void Empty_ProducesSingleEmptyLeaf_RootIsThatLeaf(DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);

        IndexBTreeBuilder.BuildResult r = IndexBTreeBuilder.Build(layout, pageSize, ParentTdef, [], FirstPage);

        Assert.Single(r.Pages);
        Assert.Equal(FirstPage, r.RootPageNumber);
        Assert.Equal(0x04, r.Pages[0][0]);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void SmallEntrySet_FitsInOneLeaf_NoIntermediate(DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        var entries = new List<IndexEntry>();
        for (int i = 0; i < 10; i++)
        {
            byte[] key = IndexKeyEncoder.EncodeEntry(0x04, i, ascending: true);
            entries.Add(new IndexEntry(key, 1, (byte)i));
        }

        IndexBTreeBuilder.BuildResult r = IndexBTreeBuilder.Build(layout, pageSize, ParentTdef, entries, FirstPage);

        Assert.Single(r.Pages);
        Assert.Equal(FirstPage, r.RootPageNumber);
        Assert.Equal(0x04, r.Pages[0][0]);

        // No siblings.
        Assert.Equal(0, ReadI32(r.Pages[0], 8));   // prev_page
        Assert.Equal(0, ReadI32(r.Pages[0], 12));  // next_page
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void OverflowsOneLeaf_SplitsAndAddsIntermediateRoot(DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);

        // Keys are 200 bytes; big[0]=i>>8 (always 0 for i<256), big[1]=i.
        // All keys share big[0]=0 → 1-byte within-leaf prefix.
        // First leaf entry: 204 bytes; subsequent (1-byte prefix): 203 bytes.
        var entries = new List<IndexEntry>();
        for (int i = 0; i < 40; i++)
        {
            byte[] big = new byte[200];
            big[0] = (byte)(i >> 8);
            big[1] = (byte)i;
            entries.Add(new IndexEntry(big, 1, (byte)i));
        }

        IndexBTreeBuilder.BuildResult r = IndexBTreeBuilder.Build(layout, pageSize, ParentTdef, entries, FirstPage);

        // Compute expected leaf count from layout capacity.
        int payloadArea = pageSize - layout.FirstEntryOffset;
        int leafCapacity = 1 + ((payloadArea - 204) / 203);
        int expectedLeaves = (entries.Count + leafCapacity - 1) / leafCapacity;
        int expectedTotal = expectedLeaves + 1;

        Assert.Equal(expectedTotal, r.Pages.Count);

        for (int i = 0; i < expectedLeaves; i++)
        {
            Assert.Equal(0x04, r.Pages[i][0]);
        }

        Assert.Equal(0x03, r.Pages[expectedLeaves][0]);
        Assert.Equal(FirstPage + expectedLeaves, r.RootPageNumber);

        // Sibling chain: first leaf prev=0, last leaf next=0.
        Assert.Equal(0, ReadI32(r.Pages[0], 8));
        Assert.Equal(FirstPage + 1, ReadI32(r.Pages[0], 12));
        Assert.Equal(FirstPage + expectedLeaves - 2, ReadI32(r.Pages[expectedLeaves - 1], 8));
        Assert.Equal(0, ReadI32(r.Pages[expectedLeaves - 1], 12));

        // parent_page on every page.
        for (int i = 0; i < expectedTotal; i++)
        {
            Assert.Equal(ParentTdef, ReadI32(r.Pages[i], 4));
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void IntermediateRoot_EntriesPointToChildPagesInOrder(DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);

        var entries = new List<IndexEntry>();
        for (int i = 0; i < 40; i++)
        {
            byte[] big = new byte[200];
            big[0] = (byte)(i >> 8);
            big[1] = (byte)i;
            entries.Add(new IndexEntry(big, 1, (byte)i));
        }

        IndexBTreeBuilder.BuildResult r = IndexBTreeBuilder.Build(layout, pageSize, ParentTdef, entries, FirstPage);

        int rootIdx = (int)(r.RootPageNumber - r.FirstPageNumber);
        byte[] intermediate = r.Pages[rootIdx];
        Assert.Equal(0x03, intermediate[0]);

        // Intermediate entries summarise the last leaf entry of each child.
        // All boundary keys share big[0]=0 → prefix compression hoists 1 byte.
        int prefLen = ReadU16(intermediate, 20);
        Assert.Equal(1, prefLen);

        // Entry 0 (full): key (200) + 3-byte BE data_page + 1-byte data_row + 4-byte child_page.
        const int entry0KeyLen = 200;
        int firstChildOffset = layout.FirstEntryOffset + entry0KeyLen + 4;
        Assert.Equal(r.FirstPageNumber + 0, ReadI32(intermediate, firstChildOffset));

        // Entry 1 (compressed): key (200 - prefLen=1) + 4 + 4.
        int entry0Stride = entry0KeyLen + 4 + 4;
        int compressedKeyLen = 200 - prefLen;
        int entry1Start = layout.FirstEntryOffset + entry0Stride;
        int secondChildOffset = entry1Start + compressedKeyLen + 4;
        Assert.Equal(r.FirstPageNumber + 1, ReadI32(intermediate, secondChildOffset));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void Build_LeavesEmitPrefixCompression_WhenEntriesShareLeadingBytes(DatabaseFormat format)
    {
        // Encoded T_LONG keys all share the leading 0x7F flag + 0x80 sign-flipped
        // high bytes (values 1..3 → bytes [0x7F 0x80 0x00 0x00 0x01..0x03]).
        // Common byte prefix is 4 bytes.
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);

        var entries = new List<IndexEntry>();
        for (int i = 1; i <= 3; i++)
        {
            byte[] key = IndexKeyEncoder.EncodeEntry(0x04, i, ascending: true);
            entries.Add(new IndexEntry(key, 1, (byte)i));
        }

        IndexBTreeBuilder.BuildResult r = IndexBTreeBuilder.Build(layout, pageSize, ParentTdef, entries, FirstPage);

        Assert.Single(r.Pages);
        byte[] leaf = r.Pages[0];
        Assert.Equal(0x04, leaf[0]);

        // pref_len at offset 20 should be 4 (the shared 0x7F 0x80 0x00 0x00 prefix).
        Assert.Equal(4, ReadU16(leaf, 20));

        // First entry: full 5-byte key + 4-byte rowptr at firstEntryOffset (stride 9).
        // Entry 1 at offset 9, entry 2 at offset 14 — both compressed to 1 + 4 bytes.
        // Bitmask: byte 1 carries bits 1 (0x02) and 6 (0x40) → 0x42.
        Assert.Equal(0x42, leaf[layout.BitmaskOffset + 1]);

        // Verify entry[1] starts at firstEntryOffset + 9 and contains the suffix byte 0x02 (i=2).
        Assert.Equal(0x02, leaf[layout.FirstEntryOffset + 9]);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void Build_NoSharedPrefix_LeavesPrefLenAtZero(DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);

        // Two entries that diverge at byte 0 → no shared prefix.
        var k1 = new byte[] { 0x10, 0x20 };
        var k2 = new byte[] { 0x30, 0x40 };
        List<IndexEntry> entries =
        [
            new(k1, 1, 0),
            new(k2, 1, 1),
        ];

        IndexBTreeBuilder.BuildResult r = IndexBTreeBuilder.Build(layout, pageSize, ParentTdef, entries, FirstPage);

        Assert.Single(r.Pages);
        Assert.Equal(0, ReadU16(r.Pages[0], 20));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void ManyLeaves_ProducesMultiLevelTree(DatabaseFormat format)
    {
        // 200-byte leaf entries with keys big=[0, (i>>8)&0xFF, i&0xFF, 0...].
        // 3600 entries guarantee a multi-level tree on any supported page size.
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        const int totalEntries = 3600;

        var entries = new List<IndexEntry>(totalEntries);
        for (int i = 0; i < totalEntries; i++)
        {
            byte[] big = new byte[200];
            big[0] = (byte)((i >> 16) & 0xFF);
            big[1] = (byte)((i >> 8) & 0xFF);
            big[2] = (byte)(i & 0xFF);
            entries.Add(new IndexEntry(big, 1, 0));
        }

        IndexBTreeBuilder.BuildResult r = IndexBTreeBuilder.Build(layout, pageSize, ParentTdef, entries, FirstPage);

        // All leaf pages come first, then all intermediate pages (incl. root).
        int leafCount = 0;
        while (leafCount < r.Pages.Count && r.Pages[leafCount][0] == 0x04)
        {
            leafCount++;
        }

        int interCount = r.Pages.Count - leafCount;

        Assert.True(leafCount > 10, $"Expected many leaves, got {leafCount}");
        Assert.True(interCount > 1, $"Expected multiple intermediate pages, got {interCount}");

        for (int i = 0; i < leafCount; i++)
        {
            Assert.Equal(0x04, r.Pages[i][0]);
        }

        for (int i = leafCount; i < r.Pages.Count; i++)
        {
            Assert.Equal(0x03, r.Pages[i][0]);
        }

        // Root is the final page in the build output.
        Assert.Equal(0x03, r.Pages[r.Pages.Count - 1][0]);
        Assert.Equal(FirstPage + r.Pages.Count - 1, r.RootPageNumber);
    }

    private static int PageSizeOf(DatabaseFormat fmt) =>
        fmt == DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet3 : Constants.PageSizes.Jet4;

    private static int ReadU16(byte[] b, int o) => b[o] | (b[o + 1] << 8);

    private static int ReadI32(byte[] b, int o) =>
        b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24);
}
