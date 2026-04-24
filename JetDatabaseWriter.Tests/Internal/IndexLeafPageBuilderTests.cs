namespace JetDatabaseWriter.Tests;

using System;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention.

/// <summary>
/// Unit tests for <see cref="IndexLeafPageBuilder"/> (W3). The Jet4 / ACE
/// leaf-page layout is described in
/// <c>docs/design/index-and-relationship-format-notes.md</c> §4.1 (page header),
/// §4.2 (entry-start bitmask), and §4.3 (per-entry record).
/// </summary>
public sealed class IndexLeafPageBuilderTests
{
    private const int PageSize = 4096;

    [Fact]
    public void EmptyPage_HasCorrectHeaderAndFreeSpace()
    {
        byte[] page = IndexLeafPageBuilder.BuildJet4LeafPage(PageSize, parentTdefPage: 42, Array.Empty<IndexLeafPageBuilder.LeafEntry>());

        Assert.Equal(PageSize, page.Length);
        Assert.Equal(0x04, page[0]);                                // page_type
        Assert.Equal(0x01, page[1]);                                // unknown
        Assert.Equal(PageSize - 0x1E0, ReadU16(page, 2));           // free_space (entire payload area)
        Assert.Equal(42, ReadI32(page, 4));                         // parent_page (TDEF)
        Assert.Equal(0, ReadI32(page, 8));                          // prev_page
        Assert.Equal(0, ReadI32(page, 12));                         // next_page
        Assert.Equal(0, ReadI32(page, 16));                         // tail_page
        Assert.Equal(0, ReadU16(page, 20));                         // pref_len

        // Bitmask (0x1B .. 0x1DF) is all zero.
        for (int i = 0x1B; i < 0x1E0; i++)
        {
            Assert.Equal(0, page[i]);
        }
    }

    [Fact]
    public void SingleEntry_WritesKeyAndRowPointer_NoBitmaskBitSet()
    {
        // §4.3: first entry is implicit (no bit in §4.2 bitmask).
        byte[] key = IndexKeyEncoder.EncodeEntry(0x04, 7, ascending: true); // T_LONG=7 → 5 bytes
        var entries = new[] { new IndexLeafPageBuilder.LeafEntry(key, dataPage: 0x123456, dataRow: 9) };

        byte[] page = IndexLeafPageBuilder.BuildJet4LeafPage(PageSize, parentTdefPage: 100, entries);

        // Key bytes copied at first-entry offset.
        for (int i = 0; i < key.Length; i++)
        {
            Assert.Equal(key[i], page[0x1E0 + i]);
        }

        // Row pointer: 3-byte BE data page, 1-byte row.
        int rp = 0x1E0 + key.Length;
        Assert.Equal(0x12, page[rp + 0]);
        Assert.Equal(0x34, page[rp + 1]);
        Assert.Equal(0x56, page[rp + 2]);
        Assert.Equal(9, page[rp + 3]);

        // Bitmask still zero (first entry is implicit).
        for (int i = 0x1B; i < 0x1E0; i++)
        {
            Assert.Equal(0, page[i]);
        }

        // Free space accounts for header + 1 entry.
        int entryLen = key.Length + 4;
        Assert.Equal(PageSize - 0x1E0 - entryLen, ReadU16(page, 2));
    }

    [Fact]
    public void MultipleEntries_SetsBitmaskBitForEachAfterFirst()
    {
        byte[] k1 = IndexKeyEncoder.EncodeEntry(0x04, 1, ascending: true);
        byte[] k2 = IndexKeyEncoder.EncodeEntry(0x04, 2, ascending: true);
        byte[] k3 = IndexKeyEncoder.EncodeEntry(0x04, 3, ascending: true);
        var entries = new[]
        {
            new IndexLeafPageBuilder.LeafEntry(k1, 1, 0),
            new IndexLeafPageBuilder.LeafEntry(k2, 1, 1),
            new IndexLeafPageBuilder.LeafEntry(k3, 1, 2),
        };

        byte[] page = IndexLeafPageBuilder.BuildJet4LeafPage(PageSize, parentTdefPage: 100, entries);

        // Each entry occupies key.Length + 4 = 9 bytes.
        // Entry 1 starts at 0x1E0 + 9; entry 2 at 0x1E0 + 18.
        // Bit at offset 9 → byte 1, bit 1; offset 18 → byte 2, bit 2.
        Assert.Equal(0b0000_0010, page[0x1B + 1]);
        Assert.Equal(0b0000_0100, page[0x1B + 2]);

        // Other bitmask bytes remain zero.
        Assert.Equal(0, page[0x1B + 0]);
        Assert.Equal(0, page[0x1B + 3]);
    }

    [Fact]
    public void EntriesExceedingPage_Throws()
    {
        byte[] bigKey = new byte[200];
        var entry = new IndexLeafPageBuilder.LeafEntry(bigKey, 1, 0);

        // 4096 - 0x1E0 = 3616 bytes payload area. 200+4 = 204 per entry → ~17 fit.
        var entries = new IndexLeafPageBuilder.LeafEntry[20];
        for (int i = 0; i < entries.Length; i++)
        {
            entries[i] = entry;
        }

        Assert.Throws<ArgumentOutOfRangeException>(() => IndexLeafPageBuilder.BuildJet4LeafPage(PageSize, 100, entries));
    }

    [Fact]
    public void DataPageOverflow24Bit_Throws()
    {
        byte[] key = IndexKeyEncoder.EncodeEntry(0x04, 1, ascending: true);
        var entries = new[] { new IndexLeafPageBuilder.LeafEntry(key, 0x1_000_000L, 0) };

        Assert.Throws<ArgumentOutOfRangeException>(() => IndexLeafPageBuilder.BuildJet4LeafPage(PageSize, 100, entries));
    }

    private static int ReadU16(byte[] b, int o) => b[o] | (b[o + 1] << 8);

    private static int ReadI32(byte[] b, int o) =>
        b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24);
}
