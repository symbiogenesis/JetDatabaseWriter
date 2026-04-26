namespace JetDatabaseWriter.Tests;

using System.Collections.Generic;
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
    private const int PageSize = 4096;
    private const int FirstEntryOffset = 0x1E0;

    private const long ParentTdef = 100;
    private const long FirstPage = 50;

    [Fact]
    public void Empty_ProducesSingleEmptyLeaf_RootIsThatLeaf()
    {
        IndexBTreeBuilder.BuildResult r = IndexBTreeBuilder.Build(
            PageSize, ParentTdef, new List<IndexLeafPageBuilder.LeafEntry>(), FirstPage);

        Assert.Single(r.Pages);
        Assert.Equal(FirstPage, r.RootPageNumber);
        Assert.Equal(0x04, r.Pages[0][0]);
    }

    [Fact]
    public void SmallEntrySet_FitsInOneLeaf_NoIntermediate()
    {
        var entries = new List<IndexLeafPageBuilder.LeafEntry>();
        for (int i = 0; i < 10; i++)
        {
            byte[] key = IndexKeyEncoder.EncodeEntry(0x04, i, ascending: true);
            entries.Add(new IndexLeafPageBuilder.LeafEntry(key, dataPage: 1, dataRow: (byte)i));
        }

        IndexBTreeBuilder.BuildResult r = IndexBTreeBuilder.Build(PageSize, ParentTdef, entries, FirstPage);

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
        var entries = new List<IndexLeafPageBuilder.LeafEntry>();
        for (int i = 0; i < 40; i++)
        {
            // Embed i so each key is unique and ordered.
            byte[] big = new byte[200];
            big[0] = (byte)(i >> 8);
            big[1] = (byte)i;
            entries.Add(new IndexLeafPageBuilder.LeafEntry(big, dataPage: 1, dataRow: (byte)i));
        }

        IndexBTreeBuilder.BuildResult r = IndexBTreeBuilder.Build(PageSize, ParentTdef, entries, FirstPage);

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
        var entries = new List<IndexLeafPageBuilder.LeafEntry>();
        for (int i = 0; i < 40; i++)
        {
            byte[] big = new byte[200];
            big[0] = (byte)(i >> 8);
            big[1] = (byte)i;
            entries.Add(new IndexLeafPageBuilder.LeafEntry(big, dataPage: 1, dataRow: (byte)i));
        }

        IndexBTreeBuilder.BuildResult r = IndexBTreeBuilder.Build(PageSize, ParentTdef, entries, FirstPage);

        byte[] intermediate = r.Pages[3];

        // First intermediate entry begins at 0x1E0 and consists of:
        // 200-byte key + 3-byte BE data_page + 1-byte data_row + 4-byte child_page.
        // Child page (4-byte LE) at offset 0x1E0 + 200 + 4.
        int firstChildOffset = FirstEntryOffset + 200 + 4;
        Assert.Equal(FirstPage + 0, ReadI32(intermediate, firstChildOffset));

        // Second entry starts immediately after (entry stride = 200 + 8 = 208).
        int secondChildOffset = firstChildOffset + 208;
        Assert.Equal(FirstPage + 1, ReadI32(intermediate, secondChildOffset));

        // Third entry.
        int thirdChildOffset = secondChildOffset + 208;
        Assert.Equal(FirstPage + 2, ReadI32(intermediate, thirdChildOffset));
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

        var entries = new List<IndexLeafPageBuilder.LeafEntry>(totalEntries);
        for (int i = 0; i < totalEntries; i++)
        {
            byte[] big = new byte[200];
            big[0] = (byte)((i >> 16) & 0xFF);
            big[1] = (byte)((i >> 8) & 0xFF);
            big[2] = (byte)(i & 0xFF);
            entries.Add(new IndexLeafPageBuilder.LeafEntry(big, dataPage: 1, dataRow: 0));
        }

        IndexBTreeBuilder.BuildResult r = IndexBTreeBuilder.Build(PageSize, ParentTdef, entries, FirstPage);

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

    private static int ReadI32(byte[] b, int o) =>
        b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24);
}
