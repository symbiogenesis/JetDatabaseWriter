namespace JetDatabaseWriter.Tests.Indexes;

using System;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Schema;
using Xunit;

/// <summary>Entry boundary scans retain exact unsigned bitmap and sentinel semantics.</summary>
public sealed class IndexEntryBoundaryKernelTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void NextEntryStart_MatchesIndependentBitmapOracle(DatabaseFormat databaseFormat)
    {
        var format = JetFormat.ForNewDatabase(databaseFormat);
        IndexPageLayout layout = format.IndexPage;
        byte[] page = new byte[format.PageSize];
        int first = layout.FirstEntryOffset;
        const int payloadLength = 24;
        for (int value = 0; value <= byte.MaxValue; value++)
        {
            page[layout.BitmaskOffset] = (byte)value;
            page[layout.BitmaskOffset + 1] = (byte)(byte.MaxValue - value);
            page[layout.BitmaskOffset + 2] = (byte)value;
            page[layout.BitmaskOffset + 3] = byte.MaxValue;
            for (int current = 0; current <= payloadLength; current++)
            {
                int expected = -1;
                for (int candidate = current + 1; candidate < payloadLength; candidate++)
                {
                    if ((page[layout.BitmaskOffset + (candidate / 8)] & (1 << (candidate % 8))) != 0)
                    {
                        expected = first + candidate;
                        break;
                    }
                }

                Assert.Equal(expected, IndexPageCodec.NextEntryStart(layout, page, first + payloadLength, first + current));
            }
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void StrictLeaf_RefusesEveryBitOutsideTheAdmittedBoundary(DatabaseFormat databaseFormat)
    {
        var format = JetFormat.ForNewDatabase(databaseFormat);
        IndexPageLayout layout = format.IndexPage;
        byte[] baseline = IndexPageCodec.BuildLeafPage(layout, format.PageSize, 2, [new([1, 2, 3, 4], 12, 0)], enablePrefixCompression: false);
        int payloadLength = format.PageSize - JetTypeInfo.Ru16(baseline, 2) - layout.FirstEntryOffset;
        Assert.Equal(8, payloadLength);
        Assert.True(IndexPageCodec.TryDecodeLeafEntries(layout, baseline, format.PageSize, out _));
        int bitmapBits = (layout.FirstEntryOffset - layout.BitmaskOffset) * 8;
        for (int bit = 0; bit < bitmapBits; bit++)
        {
            if (bit > 0 && bit < payloadLength)
            {
                continue;
            }

            byte[] page = (byte[])baseline.Clone();
            page[layout.BitmaskOffset + (bit / 8)] ^= (byte)(1 << (bit % 8));
            Assert.False(IndexPageCodec.TryDecodeLeafEntries(layout, page, format.PageSize, out _));
        }
    }

    [Fact]
    public void NextEntryStart_StopsAtThePhysicalBitmapAndExcludesTheSentinel()
    {
        var layout = new IndexPageLayout(8, 16, 8, 8, 8, 8);
        byte[] page = new byte[128];
        Array.Fill(page, byte.MaxValue, layout.FirstEntryOffset, page.Length - layout.FirstEntryOffset);
        page[15] = 128;
        Assert.Equal(79, IndexPageCodec.NextEntryStart(layout, page, page.Length, 78));
        Assert.Equal(-1, IndexPageCodec.NextEntryStart(layout, page, page.Length, 79));
        Assert.Equal(-1, IndexPageCodec.NextEntryStart(layout, page, 79, 78));
    }

    [Fact]
    public void NextEntryStart_RefusesMalformedLayoutAndPayloadBeforeReading()
    {
        byte[] page = new byte[64];
        Assert.Equal(-1, IndexPageCodec.NextEntryStart(new(-1, 16, 8, 8, 8, 8), page, 24, 16));
        Assert.Equal(-1, IndexPageCodec.NextEntryStart(new(8, -1, 8, 8, 8, 8), page, 24, 16));
        Assert.Equal(-1, IndexPageCodec.NextEntryStart(new(8, 16, 8, 8, 8, 8), page, int.MaxValue, 16));
        Assert.Equal(-1, IndexPageCodec.NextEntryStart(new(8, 16, 8, 8, 8, 8), page, 24, int.MaxValue));
    }
}
