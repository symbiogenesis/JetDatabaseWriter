namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Buffers.Binary;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Intermediate-page descent preserves compressed unsigned keys and exact child identities.</summary>
public sealed class IndexChildSelectionKernelTests
{
    [Fact]
    public void SearchAdmission_RefusesNativeWidthsOutsideTheManagedPage()
    {
        Assert.True(RecoveryKernel.IsIndexChildSearch(4096, 27, 480, 600, 16));
        Assert.False(RecoveryKernel.IsIndexChildSearch(uint.MaxValue, 27, 480, 600, 16));
        Assert.False(RecoveryKernel.IsIndexChildSearch(4096, uint.MaxValue, 480, 600, 16));
        Assert.False(RecoveryKernel.IsIndexChildSearch(4096, 27, uint.MaxValue, 600, 16));
        Assert.False(RecoveryKernel.IsIndexChildSearch(4096, 27, 480, uint.MaxValue, 16));
        Assert.False(RecoveryKernel.IsIndexChildSearch(4096, 27, 480, 600, uint.MaxValue));
        Assert.False(RecoveryKernel.IsIndexChildSearch(4096, 480, 480, 600, 16));
        Assert.False(RecoveryKernel.IsIndexChildSearch(4096, 27, 600, 600, 16));
    }

    [Fact]
    public void SearchStep_ConsumesExtremeComparisonSignsAndSeparatesContinuationFromChildIdentity()
    {
        IndexPageLayout layout = IndexPageLayout.Jet4;
        byte[] page = IndexPageCodec.BuildIntermediatePage(layout, 4096, 7, [new(new([1], 1, 0), 0xFFFFFFFF), new(new([2], 1, 1), 0)], 0, 0, 0);
        uint first = (uint)layout.FirstEntryOffset;
        uint payloadEnd = 4096U - BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(2));
        uint end = RecoveryKernel.ReadIndexChildEntry(page, (uint)layout.BitmaskOffset, first, payloadEnd, first, 0, true);
        Assert.Equal(first + 9, end);
        Assert.Equal(uint.MaxValue, RecoveryKernel.AdvanceIndexChildSearch(page, first, payloadEnd, first, end, 0, true, int.MinValue));
        Assert.Equal(uint.MaxValue, RecoveryKernel.AdvanceIndexChildSearch(page, first, payloadEnd, first, end, 0, true, 0));
        Assert.Equal((1UL << 32) + end, RecoveryKernel.AdvanceIndexChildSearch(page, first, payloadEnd, first, end, 0, true, int.MaxValue));
        uint lastEnd = RecoveryKernel.ReadIndexChildEntry(page, (uint)layout.BitmaskOffset, first, payloadEnd, end, 0, false);
        Assert.Equal(payloadEnd, lastEnd);
        Assert.Equal(0UL, RecoveryKernel.AdvanceIndexChildSearch(page, first, payloadEnd, end, lastEnd, 0, false, 0));
        Assert.Equal(ulong.MaxValue, RecoveryKernel.AdvanceIndexChildSearch(page, first, payloadEnd, end, lastEnd, 0, false, 1));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void SelectChildPage_MatchesUncompressedSummaryOracle(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        DecodedIntermediateEntry[] entries =
        [
            new(new([127, 128, 0], 1, 0), 0),
            new(new([127, 128, 0], 1, 1), 0x80000000),
            new(new([127, 128, 1], 1, 2), 0xFFFFFFFE),
            new(new([127, 128, 127], 1, 3), 0xFFFFFFFF),
            new(new([127, 128, 128], 1, 4), 0x1020304),
            new(new([127, 128, 255], 1, 5), 19),
        ];
        foreach (int prefixLimit in new[] { 0, 1, 2, 255 })
        {
            byte[] page = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 7, entries, 0, 0, 0, prefixLimit);
            for (int value = 0; value <= byte.MaxValue; value++)
            {
                byte[] search = [127, 128, (byte)value];
                long? expected = null;
                foreach (DecodedIntermediateEntry entry in entries)
                {
                    if (entry.Entry.Key.AsSpan().SequenceCompareTo(search) >= 0)
                    {
                        expected = entry.ChildPage;
                        break;
                    }
                }

                Assert.Equal(expected, IndexPageCodec.SelectChildPage(format.IndexPage, page, format.PageSize, search));
            }

            Assert.Equal(0, IndexPageCodec.SelectChildPage(format.IndexPage, page, format.PageSize, []));
            Assert.Equal(0, IndexPageCodec.SelectChildPage(format.IndexPage, page, format.PageSize, [127]));
            Assert.Null(IndexPageCodec.SelectChildPage(format.IndexPage, page, format.PageSize, [128]));
            Assert.Null(IndexPageCodec.SelectChildPage(format.IndexPage, page, format.PageSize, [127, 128, 255, 0]));
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void SelectChildPage_PreservesLongSharedPrefixesAndUnequalSearchLengths(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        foreach (int width in new[] { 15, 16, 17, 31, 32, 33, 128, 256 })
        {
            byte[] prefix = new byte[width];
            Array.Fill(prefix, (byte)128);
            DecodedIntermediateEntry[] entries =
            [
                new(new([.. prefix, 0], 1, 0), 100),
                new(new([.. prefix, 0, 0], 1, 1), 101),
                new(new([.. prefix, 127], 1, 2), 102),
                new(new([.. prefix, 128], 1, 3), 103),
                new(new([.. prefix, 255], 1, 4), 104),
            ];
            byte[] page = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 7, entries, 0, 0, 0);
            byte[][] searches =
            [
                prefix[..^1],
                prefix,
                [.. prefix, 0],
                [.. prefix, 0, 0],
                [.. prefix, 0, 0, 0],
                [.. prefix, 126],
                [.. prefix, 127],
                [.. prefix, 128],
                [.. prefix, 254],
                [.. prefix, 255],
                [.. prefix, 255, 0],
            ];
            foreach (byte[] search in searches)
            {
                long? expected = null;
                foreach (DecodedIntermediateEntry entry in entries)
                {
                    if (entry.Entry.Key.AsSpan().SequenceCompareTo(search) >= 0)
                    {
                        expected = entry.ChildPage;
                        break;
                    }
                }

                Assert.Equal(expected, IndexPageCodec.SelectChildPage(format.IndexPage, page, format.PageSize, search));
            }
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void SelectChildPage_CompressedBookmarkBytesNeverParticipateInKeyComparison(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        IndexPageLayout layout = format.IndexPage;
        byte[] page = new byte[format.PageSize];
        page[0] = 3;
        page[layout.PrefLenOffset] = 9;
        byte[] first = [127, 128, 0, 0, 1, 2, 0xFF, 0xFF, 0xFF, 0xFE];
        first.CopyTo(page, layout.FirstEntryOffset);
        page[layout.FirstEntryOffset + first.Length] = 0xFF;
        page[layout.BitmaskOffset + 1] = 12;
        BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(2), checked((ushort)(format.PageSize - layout.FirstEntryOffset - 11)));

        Assert.Equal(0xFFFFFFFEL, IndexPageCodec.SelectChildPage(layout, page, format.PageSize, [127, 128]));
        Assert.Null(IndexPageCodec.SelectChildPage(layout, page, format.PageSize, [127, 128, 0]));
        Assert.Equal(0xFFFFFFFFL, IndexPageCodec.ReadLastChildPointer(layout, page, format.PageSize));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void SelectChildPage_RefusesTruncatedTrailerAndImpossibleFirstPrefix(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        IndexPageLayout layout = format.IndexPage;
        byte[] page = IndexPageCodec.BuildIntermediatePage(layout, format.PageSize, 7, [new(new([1], 1, 0), 42)], 0, 0, 0);
        page[layout.PrefLenOffset] = 10;
        Assert.Null(IndexPageCodec.SelectChildPage(layout, page, format.PageSize, []));
        page[layout.PrefLenOffset] = 0;
        BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(2), checked((ushort)(format.PageSize - layout.FirstEntryOffset - 7)));
        Assert.Null(IndexPageCodec.SelectChildPage(layout, page, format.PageSize, []));
    }

    [Fact]
    public void SelectChildPage_DoesNotAllocateDuringDescent()
    {
        IndexPageLayout layout = IndexPageLayout.Jet4;
        byte[] page = IndexPageCodec.BuildIntermediatePage(layout, 4096, 7, [new(new([1], 1, 0), 41), new(new([2], 1, 1), 42)], 0, 0, 0);
        byte[] search = [2];
        for (int iteration = 0; iteration < 100; iteration++)
        {
            _ = IndexPageCodec.SelectChildPage(layout, page, 4096, search);
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        long total = 0;
        for (int iteration = 0; iteration < 100; iteration++)
        {
            total += IndexPageCodec.SelectChildPage(layout, page, 4096, search) ?? 0;
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(4200, total);
        Assert.Equal(0, allocated);
    }
}
