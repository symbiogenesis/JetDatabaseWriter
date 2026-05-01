namespace JetDatabaseWriter.Tests.Internal;

using System.Collections.Generic;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Internal.Builders;
using JetDatabaseWriter.Internal.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention.

/// <summary>
/// Unit tests for <see cref="IndexBTreeBuilder"/>. Verifies B-tree leaf
/// splitting, sibling chains, and intermediate (<c>0x03</c>) page emission.
/// Page format references in
/// <c>docs/design/index-and-relationship-format-notes.md</c> §4.1–4.3.
/// </summary>
public sealed class IndexBTreeBuilderTests
{
    private const long ParentTdef = 100;
    private const long FirstPage = 50;

    [Fact]
    public void Empty_ProducesSingleEmptyLeaf_RootIsThatLeaf()
    {
        IndexBTreeBuilder.BuildResult r = IndexBTreeBuilder.Build(
            Constants.PageSizes.Jet4, ParentTdef, new List<IndexEntry>(), FirstPage);

        Assert.Single(r.Pages);
        Assert.Equal(FirstPage, r.RootPageNumber);
        Assert.Equal(0x04, r.Pages[0][0]);
    }

    [Fact]
    public void SmallEntrySet_FitsInOneLeaf_NoIntermediate()
    {
        var entries = new List<IndexEntry>();
        for (int i = 0; i < 10; i++)
        {
            byte[] key = IndexKeyEncoder.EncodeEntry(0x04, i, ascending: true);
            entries.Add(new IndexEntry(key, 1, (byte)i));
        }

        IndexBTreeBuilder.BuildResult r = IndexBTreeBuilder.Build(Constants.PageSizes.Jet4, ParentTdef, entries, FirstPage);

        Assert.Single(r.Pages);
        Assert.Equal(FirstPage, r.RootPageNumber);
        Assert.Equal(0x04, r.Pages[0][0]);

        // No siblings.
        Assert.Equal(0, ReadI32(r.Pages[0], 8));   // prev_page
        Assert.Equal(0, ReadI32(r.Pages[0], 12));  // next_page
    }

    [Fact]
    public void OverflowsOneLeaf_SplitsAndAddsIntermediateRoot()
    {
        // Force 2 leaves: each entry is ~200 bytes, area is 3616 bytes ⇒ ~18 entries fit.
        // 40 entries → 3 leaves (18 + 18 + 4) → 1 intermediate root.
        var entries = new List<IndexEntry>();
        for (int i = 0; i < 40; i++)
        {
            // Embed i so each key is unique and ordered.
            byte[] big = new byte[200];
            big[0] = (byte)(i >> 8);
            big[1] = (byte)i;
            entries.Add(new IndexEntry(big, 1, (byte)i));
        }

        IndexBTreeBuilder.BuildResult r = IndexBTreeBuilder.Build(Constants.PageSizes.Jet4, ParentTdef, entries, FirstPage);

        // 3 leaves + 1 intermediate root = 4 pages.
        Assert.Equal(4, r.Pages.Count);

        // Leaves are pages [0..2], intermediate is page [3].
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(0x04, r.Pages[i][0]);
        }

        Assert.Equal(0x03, r.Pages[3][0]);
        Assert.Equal(FirstPage + 3, r.RootPageNumber);

        // Sibling chain on leaves.
        Assert.Equal(0, ReadI32(r.Pages[0], 8));                 // leaf 0 prev = 0
        Assert.Equal(FirstPage + 1, ReadI32(r.Pages[0], 12));    // leaf 0 next
        Assert.Equal(FirstPage + 0, ReadI32(r.Pages[1], 8));     // leaf 1 prev
        Assert.Equal(FirstPage + 2, ReadI32(r.Pages[1], 12));    // leaf 1 next
        Assert.Equal(FirstPage + 1, ReadI32(r.Pages[2], 8));     // leaf 2 prev
        Assert.Equal(0, ReadI32(r.Pages[2], 12));                // leaf 2 next = 0

        // parent_page on every page.
        for (int i = 0; i < 4; i++)
        {
            Assert.Equal(ParentTdef, ReadI32(r.Pages[i], 4));
        }
    }

    [Fact]
    public void IntermediateRoot_EntriesPointToChildPagesInOrder()
    {
        var entries = new List<IndexEntry>();
        for (int i = 0; i < 40; i++)
        {
            byte[] big = new byte[200];
            big[0] = (byte)(i >> 8);
            big[1] = (byte)i;
            entries.Add(new IndexEntry(big, 1, (byte)i));
        }

        IndexBTreeBuilder.BuildResult r = IndexBTreeBuilder.Build(Constants.PageSizes.Jet4, ParentTdef, entries, FirstPage);

        byte[] intermediate = r.Pages[3];

        // Intermediate entries summarise the last leaf entry of each child:
        // i = 16 (leaf 0), i = 33 (leaf 1), i = 39 (leaf 2). Bytes [0] and
        // [2..199] are all zero across the three summaries; only byte [1]
        // differs. Prefix compression hoists the leading 0x00 (1 byte) into
        // pref_len, so subsequent entries strip 1 byte from the front.
        int prefLen = ReadU16(intermediate, 20);
        Assert.Equal(1, prefLen);

        // Entry 0 (full): key (200) + 3-byte BE data_page + 1-byte data_row + 4-byte child_page.
        int entry0KeyLen = 200;
        int entry0Stride = entry0KeyLen + 4 + 4;
        int firstChildOffset = Constants.IndexLeafPage.Jet4FirstEntryOffset + entry0KeyLen + 4;
        Assert.Equal(FirstPage + 0, ReadI32(intermediate, firstChildOffset));

        // Entry 1 (compressed): key (200 - prefLen) + 4 + 4.
        int compressedKeyLen = 200 - prefLen;
        int compressedStride = compressedKeyLen + 4 + 4;
        int entry1Start = Constants.IndexLeafPage.Jet4FirstEntryOffset + entry0Stride;
        int secondChildOffset = entry1Start + compressedKeyLen + 4;
        Assert.Equal(FirstPage + 1, ReadI32(intermediate, secondChildOffset));

        int entry2Start = entry1Start + compressedStride;
        int thirdChildOffset = entry2Start + compressedKeyLen + 4;
        Assert.Equal(FirstPage + 2, ReadI32(intermediate, thirdChildOffset));
    }

    [Fact]
    public void Build_LeavesEmitPrefixCompression_WhenEntriesShareLeadingBytes()
    {
        // Encoded T_LONG keys all share the leading 0x7F flag + 0x80 sign-flipped
        // high bytes (values 1..3 → bytes [0x7F 0x80 0x00 0x00 0x01..0x03]).
        // Common byte prefix is 4 bytes.
        var entries = new List<IndexEntry>();
        for (int i = 1; i <= 3; i++)
        {
            byte[] key = IndexKeyEncoder.EncodeEntry(0x04, i, ascending: true);
            entries.Add(new IndexEntry(key, 1, (byte)i));
        }

        IndexBTreeBuilder.BuildResult r = IndexBTreeBuilder.Build(Constants.PageSizes.Jet4, ParentTdef, entries, FirstPage);

        Assert.Single(r.Pages);
        byte[] leaf = r.Pages[0];
        Assert.Equal(0x04, leaf[0]);

        // pref_len at offset 20 should be 4 (the shared 0x7F 0x80 0x00 0x00 prefix).
        Assert.Equal(4, ReadU16(leaf, 20));

        // First entry: full 5-byte key + 4-byte rowptr at 0x1E0 (stride 9).
        // Entry 1 at offset 9, entry 2 at offset 14 — both compressed to 1 + 4 bytes.
        // Bitmask: byte 1 carries bits 1 (0x02) and 6 (0x40) → 0x42.
        const int Jet4BitmaskOffset = Constants.IndexLeafPage.Jet4BitmaskOffset;
        Assert.Equal(0x42, leaf[Jet4BitmaskOffset + 1]);

        // Verify entry[1] starts at 0x1E0 + 9 and contains the suffix byte 0x02 (i=2).
        Assert.Equal(0x02, leaf[Constants.IndexLeafPage.Jet4FirstEntryOffset + 9]);
    }

    [Fact]
    public void Build_NoSharedPrefix_LeavesPrefLenAtZero()
    {
        // Two entries that diverge at byte 0 → no shared prefix.
        var k1 = new byte[] { 0x10, 0x20 };
        var k2 = new byte[] { 0x30, 0x40 };
        List<IndexEntry> entries =
        [
            new(k1, 1, 0),
            new(k2, 1, 1),
        ];

        IndexBTreeBuilder.BuildResult r = IndexBTreeBuilder.Build(Constants.PageSizes.Jet4, ParentTdef, entries, FirstPage);

        Assert.Single(r.Pages);
        Assert.Equal(0, ReadU16(r.Pages[0], 20));
    }

    [Fact]
    public void ManyLeaves_ProducesMultiLevelTree()
    {
        // 200-byte leaf entries → leaf entry = 204 bytes; entry area = 3616 bytes
        // → 17 entries per leaf. 3600 entries → ⌈3600/17⌉ = 212 leaves.
        // Intermediate entry = 208 bytes → 17 per intermediate
        // → ⌈212/17⌉ = 13 level-1 intermediates → 1 root.
        // Total pages = 212 + 13 + 1 = 226.
        const int totalEntries = 3600;
        const int expectedLeaves = 212;
        const int expectedLevel1 = 13;
        const int expectedTotal = expectedLeaves + expectedLevel1 + 1;

        var entries = new List<IndexEntry>(totalEntries);
        for (int i = 0; i < totalEntries; i++)
        {
            byte[] big = new byte[200];
            big[0] = (byte)((i >> 16) & 0xFF);
            big[1] = (byte)((i >> 8) & 0xFF);
            big[2] = (byte)(i & 0xFF);
            entries.Add(new IndexEntry(big, 1, 0));
        }

        IndexBTreeBuilder.BuildResult r = IndexBTreeBuilder.Build(Constants.PageSizes.Jet4, ParentTdef, entries, FirstPage);

        Assert.Equal(expectedTotal, r.Pages.Count);

        for (int i = 0; i < expectedLeaves; i++)
        {
            Assert.Equal(0x04, r.Pages[i][0]);
        }

        for (int i = expectedLeaves; i < expectedLeaves + expectedLevel1; i++)
        {
            Assert.Equal(0x03, r.Pages[i][0]);
        }

        // Final page is the root intermediate.
        Assert.Equal(0x03, r.Pages[expectedTotal - 1][0]);
        Assert.Equal(FirstPage + expectedTotal - 1, r.RootPageNumber);
    }

    private static int ReadU16(byte[] b, int o) => b[o] | (b[o + 1] << 8);

    private static int ReadI32(byte[] b, int o) =>
        b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24);
}
