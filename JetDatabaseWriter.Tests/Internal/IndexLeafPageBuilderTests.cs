namespace JetDatabaseWriter.Tests.Internal;

using System;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Internal.Builders;
using JetDatabaseWriter.Internal.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention.

/// <summary>
/// Unit tests for <see cref="IndexLeafPageBuilder"/>. Both Jet4 / ACE and Jet3
/// leaf-page layouts are covered via <c>[Theory]</c> parameters; the format-specific
/// offsets (bitmask position, first-entry offset, page size) are derived from the
/// <see cref="IndexLeafPageBuilder.LeafPageLayout"/> descriptor.
/// Layout is described in <c>docs/design/index-and-relationship-format-notes.md</c>
/// §4.1 (page header), §4.2 (entry-start bitmask), and §4.3 (per-entry record).
/// </summary>
public sealed class IndexLeafPageBuilderTests
{
    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void EmptyPage_HasCorrectHeaderAndFreeSpace(DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        byte[] page = IndexLeafPageBuilder.BuildLeafPage(layout, pageSize, parentTdefPage: 42, [], 0, 0, 0, enablePrefixCompression: false);

        Assert.Equal(pageSize, page.Length);
        Assert.Equal(0x04, page[0]);                                          // page_type
        Assert.Equal(0x01, page[1]);                                          // unknown
        Assert.Equal(pageSize - layout.FirstEntryOffset, ReadU16(page, 2));   // free_space (entire payload area)
        Assert.Equal(42, ReadI32(page, 4));                                   // parent_page (TDEF)
        Assert.Equal(0, ReadI32(page, 8));                                    // prev_page
        Assert.Equal(0, ReadI32(page, 12));                                   // next_page
        Assert.Equal(0, ReadI32(page, 16));                                   // tail_page
        Assert.Equal(0, ReadU16(page, 20));                                   // pref_len

        // Bitmask (layout.BitmaskOffset .. layout.FirstEntryOffset-1) is all zero.
        for (int i = layout.BitmaskOffset; i < layout.FirstEntryOffset; i++)
        {
            Assert.Equal(0, page[i]);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void SingleEntry_WritesKeyAndRowPointer_NoBitmaskBitSet(DatabaseFormat format)
    {
        // §4.3: first entry is implicit (no bit in §4.2 bitmask).
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        byte[] key = IndexKeyEncoder.EncodeEntry(0x04, 7, ascending: true); // T_LONG=7 → 5 bytes
        var entries = new[] { new IndexEntry(key, 0x123456, 9) };

        byte[] page = IndexLeafPageBuilder.BuildLeafPage(layout, pageSize, parentTdefPage: 100, entries, 0, 0, 0, enablePrefixCompression: false);

        // Key bytes copied at first-entry offset.
        for (int i = 0; i < key.Length; i++)
        {
            Assert.Equal(key[i], page[layout.FirstEntryOffset + i]);
        }

        // Row pointer: 3-byte BE data page, 1-byte row.
        int rp = layout.FirstEntryOffset + key.Length;
        Assert.Equal(0x12, page[rp + 0]);
        Assert.Equal(0x34, page[rp + 1]);
        Assert.Equal(0x56, page[rp + 2]);
        Assert.Equal(9, page[rp + 3]);

        // Bitmask still zero (first entry is implicit).
        for (int i = layout.BitmaskOffset; i < layout.FirstEntryOffset; i++)
        {
            Assert.Equal(0, page[i]);
        }

        // Free space accounts for header + 1 entry.
        int entryLen = key.Length + 4;
        Assert.Equal(pageSize - layout.FirstEntryOffset - entryLen, ReadU16(page, 2));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void MultipleEntries_SetsBitmaskBitForEachAfterFirst(DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        byte[] k1 = IndexKeyEncoder.EncodeEntry(0x04, 1, ascending: true);
        byte[] k2 = IndexKeyEncoder.EncodeEntry(0x04, 2, ascending: true);
        byte[] k3 = IndexKeyEncoder.EncodeEntry(0x04, 3, ascending: true);
        var entries = new[]
        {
            new IndexEntry(k1, 1, 0),
            new IndexEntry(k2, 1, 1),
            new IndexEntry(k3, 1, 2),
        };

        byte[] page = IndexLeafPageBuilder.BuildLeafPage(layout, pageSize, parentTdefPage: 100, entries, 0, 0, 0, enablePrefixCompression: false);

        // Each entry occupies key.Length + 4 = 9 bytes.
        // Entry 1 starts at firstEntryOffset + 9; entry 2 at firstEntryOffset + 18.
        // Bit at offset 9 → byte 1, bit 1; offset 18 → byte 2, bit 2.
        Assert.Equal(0b0000_0010, page[layout.BitmaskOffset + 1]);
        Assert.Equal(0b0000_0100, page[layout.BitmaskOffset + 2]);

        // Other bitmask bytes remain zero.
        Assert.Equal(0, page[layout.BitmaskOffset + 0]);
        Assert.Equal(0, page[layout.BitmaskOffset + 3]);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void EntriesExceedingPage_Throws(DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        byte[] bigKey = new byte[200];
        var entry = new IndexEntry(bigKey, 1, 0);

        // 200+4 = 204 bytes per entry; enough copies will overflow any page size.
        int overflowCount = (pageSize / (200 + 4)) + 2;
        var entries = new IndexEntry[overflowCount];
        for (int i = 0; i < entries.Length; i++)
        {
            entries[i] = entry;
        }

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            IndexLeafPageBuilder.BuildLeafPage(layout, pageSize, 100, entries, 0, 0, 0, enablePrefixCompression: false));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void DataPageOverflow24Bit_Throws(DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        byte[] key = IndexKeyEncoder.EncodeEntry(0x04, 1, ascending: true);
        var entries = new[] { new IndexEntry(key, 0x1_000_000L, 0) };

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            IndexLeafPageBuilder.BuildLeafPage(layout, pageSize, 100, entries, 0, 0, 0, enablePrefixCompression: false));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void PrefixCompressionDisabled_PrefLenIsZero(DatabaseFormat format)
    {
        // Default (no enablePrefixCompression) keeps pref_len = 0 even when
        // entries share leading bytes — preserves the leaf-page emission byte layout
        // for callers that haven't opted in.
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        byte[] k1 = IndexKeyEncoder.EncodeEntry(0x04, 1, ascending: true);
        byte[] k2 = IndexKeyEncoder.EncodeEntry(0x04, 2, ascending: true);
        var entries = new[]
        {
            new IndexEntry(k1, 1, 0),
            new IndexEntry(k2, 1, 1),
        };

        byte[] page = IndexLeafPageBuilder.BuildLeafPage(layout, pageSize, parentTdefPage: 100, entries, 0, 0, 0, enablePrefixCompression: false);

        Assert.Equal(0, ReadU16(page, 20));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void PrefixCompressionEnabled_HoistsSharedPrefix_AndStripsItFromTrailingEntries(DatabaseFormat format)
    {
        // T_LONG ascending encoding for 1, 2, 3 produces 5-byte keys whose
        // first 4 bytes are identical (0x7F flag + 0x80 sign-flipped MSB +
        // two 0x00 bytes). Compressed entries beyond the first carry only the
        // single trailing differing byte.
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        byte[] k1 = IndexKeyEncoder.EncodeEntry(0x04, 1, ascending: true);
        byte[] k2 = IndexKeyEncoder.EncodeEntry(0x04, 2, ascending: true);
        byte[] k3 = IndexKeyEncoder.EncodeEntry(0x04, 3, ascending: true);
        IndexEntry[] entries =
        [
            new(k1, 1, 0),
            new(k2, 1, 1),
            new(k3, 1, 2),
        ];

        byte[] page = IndexLeafPageBuilder.BuildLeafPage(
            layout,
            pageSize,
            parentTdefPage: 100,
            entries,
            prevPage: 0,
            nextPage: 0,
            tailPage: 0,
            enablePrefixCompression: true);

        Assert.Equal(4, ReadU16(page, 20));

        // First entry is whole: 5-byte key + 4-byte rowptr at firstEntryOffset.
        for (int i = 0; i < 5; i++)
        {
            Assert.Equal(k1[i], page[layout.FirstEntryOffset + i]);
        }

        // Entry 1 starts at firstEntryOffset + 9 (5 + 4) and consists of the single
        // suffix byte (0x02) + 4-byte rowptr.
        int entry1Start = layout.FirstEntryOffset + 9;
        Assert.Equal(0x02, page[entry1Start]);

        // Entry 2 starts at entry1Start + 5 (1 byte key + 4 byte rowptr).
        int entry2Start = entry1Start + 5;
        Assert.Equal(0x03, page[entry2Start]);

        // Bitmask: bit at offset 9 (entry 1) and offset 14 (entry 2).
        // Byte 1, bits 1 and 6 → 0b0100_0010.
        Assert.Equal(0b0100_0010, page[layout.BitmaskOffset + 1]);

        // Free space reflects the tighter packing: header consumed +
        // (5+4) + (1+4) + (1+4) = 19 bytes from the entry area.
        Assert.Equal(pageSize - layout.FirstEntryOffset - 19, ReadU16(page, 2));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void PrefixCompressionEnabled_SingleEntry_LeavesPrefLenAtZero(DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        byte[] key = IndexKeyEncoder.EncodeEntry(0x04, 7, ascending: true);
        var entries = new[] { new IndexEntry(key, 1, 0) };

        byte[] page = IndexLeafPageBuilder.BuildLeafPage(layout, pageSize, 100, entries, 0, 0, 0, enablePrefixCompression: true);

        Assert.Equal(0, ReadU16(page, 20));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void PrefixCompressionEnabled_NoCommonPrefix_LeavesPrefLenAtZero(DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(format);
        byte[] k1 = [0x10, 0x20];
        byte[] k2 = [0x30, 0x40];
        IndexEntry[] entries =
        [
            new(k1, 1, 0),
            new(k2, 1, 1),
        ];

        byte[] page = IndexLeafPageBuilder.BuildLeafPage(layout, pageSize, 100, entries, 0, 0, 0, enablePrefixCompression: true);

        Assert.Equal(0, ReadU16(page, 20));
    }

    private static int PageSizeOf(DatabaseFormat fmt) =>
        fmt == DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet3 : Constants.PageSizes.Jet4;

    private static int ReadU16(byte[] b, int o) => b[o] | (b[o + 1] << 8);

    private static int ReadI32(byte[] b, int o) =>
        b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24);
}
