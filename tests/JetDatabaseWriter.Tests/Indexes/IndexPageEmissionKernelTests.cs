namespace JetDatabaseWriter.Tests.Indexes;

using System;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Independent byte vectors for the executing index emission boundary.</summary>
public sealed class IndexPageEmissionKernelTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public void Emission_PreservesIndependentHeaderPayloadAndBoundaryBytes(DatabaseFormat databaseFormat, bool intermediate)
    {
        var format = JetFormat.ForNewDatabase(databaseFormat);
        IndexPageLayout layout = format.IndexPage;
        IndexEntry[] entries = [new([0x11, 0x22, 0x33], 0xFFFFFF, 255), new([0x11, 0x22, 0x44], 0, 0)];
        byte[] page = intermediate
            ? IndexPageCodec.BuildIntermediatePage(layout, format.PageSize, 0xFEDCBA98, [new(entries[0], uint.MaxValue), new(entries[1], 0)], 0x12345678, 0x89ABCDEF, 0x76543210, maxPrefixLength: 2)
            : IndexPageCodec.BuildLeafPage(layout, format.PageSize, 0xFEDCBA98, entries, 0x12345678, 0x89ABCDEF, 0x76543210, true, maxPrefixLength: 2);

        byte[] payload = intermediate
            ? [0x11, 0x22, 0x33, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x44, 0, 0, 0, 0, 0, 0, 0, 0]
            : [0x11, 0x22, 0x33, 0xFF, 0xFF, 0xFF, 0xFF, 0x44, 0, 0, 0, 0];
        byte[] expected = new byte[format.PageSize];
        expected[0] = intermediate ? (byte)3 : (byte)4;
        expected[1] = 1;
        int free = format.PageSize - layout.FirstEntryOffset - payload.Length;
        expected[2] = (byte)(free & 255);
        expected[3] = (byte)(free >> 8);
        PutWord(expected, 4, [0x98, 0xBA, 0xDC, 0xFE]);
        PutWord(expected, layout.PrevPageOffset, [0x78, 0x56, 0x34, 0x12]);
        PutWord(expected, layout.NextPageOffset, [0xEF, 0xCD, 0xAB, 0x89]);
        PutWord(expected, layout.TailPageOffset, [0x10, 0x32, 0x54, 0x76]);
        expected[layout.PrefLenOffset] = 2;
        int secondStart = intermediate ? 11 : 7;
        expected[layout.BitmaskOffset + (secondStart / 8)] |= (byte)(1 << (secondStart % 8));
        expected[layout.BitmaskOffset + (payload.Length / 8)] |= (byte)(1 << (payload.Length % 8));
        payload.CopyTo(expected, layout.FirstEntryOffset);

        Assert.Equal(expected, page);
        Assert.Equal([0x11, 0x22, 0x33], entries[0].Key);
        Assert.Equal([0x11, 0x22, 0x44], entries[1].Key);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void Emission_EmptyKeysAndFullPayloadKeepImplicitFirstAndExactSentinel(DatabaseFormat databaseFormat)
    {
        var format = JetFormat.ForNewDatabase(databaseFormat);
        IndexPageLayout layout = format.IndexPage;
        int keyLength = format.PageSize - layout.FirstEntryOffset - 4;
        byte[] key = new byte[keyLength];
        Array.Fill(key, (byte)0xA5);
        byte[] page = IndexPageCodec.BuildLeafPage(layout, format.PageSize, 1, [new(key, 0x123456, 7)], false);
        Assert.Equal(0, page[2]);
        Assert.Equal(0, page[3]);
        byte[] expectedBitmap = new byte[layout.FirstEntryOffset - layout.BitmaskOffset];
        int payloadLength = format.PageSize - layout.FirstEntryOffset;
        if (payloadLength / 8 < expectedBitmap.Length)
        {
            expectedBitmap[payloadLength / 8] = (byte)(1 << (payloadLength % 8));
        }

        Assert.Equal(expectedBitmap, page.AsSpan(layout.BitmaskOffset, expectedBitmap.Length).ToArray());
        Assert.Equal([0x12, 0x34, 0x56, 7], page.AsSpan(format.PageSize - 4).ToArray());

        byte[] emptyKeyPage = IndexPageCodec.BuildLeafPage(layout, format.PageSize, 1, [new([], 1, 2), new([], 3, 4)], true);
        Assert.Equal([0, 0, 1, 2, 0, 0, 3, 4], emptyKeyPage.AsSpan(layout.FirstEntryOffset, 8).ToArray());
        Assert.Equal(0x10, emptyKeyPage[layout.BitmaskOffset]);
        Assert.Equal(1, emptyKeyPage[layout.BitmaskOffset + 1]);
    }

    [Fact]
    public void Initialize_RefusesInvalidRangesAndPrefixBeforeChangingBytes()
    {
        byte[] page = new byte[64];
        Array.Fill(page, (byte)0xA5);
        byte[] before = (byte[])page.Clone();
        Assert.False(IndexMutationKernel.InitializePage(page, false, 1, 2, 3, 4, uint.MaxValue, 12, 16, 20, 0, true));
        Assert.Equal(before, page);
        Assert.False(IndexMutationKernel.InitializePage(page, false, 1, 2, 3, 4, 8, 12, 16, 20, 256, true));
        Assert.Equal(before, page);
        Assert.False(IndexMutationKernel.InitializePage(page, false, 1, 2, 3, 4, 8, 12, 16, 20, 65536, false));
        Assert.Equal(before, page);
    }

    [Fact]
    public void Admission_FullWidthInputsAndBoundaryExhaustionRefuseWithoutOverflow()
    {
        Assert.Equal(0U, IndexMutationKernel.AdvanceEntry(uint.MaxValue, 0, 1, uint.MaxValue, uint.MaxValue, 0, false, true));
        Assert.Equal(0U, IndexMutationKernel.AdvanceEntry(64, 15, 16, 24, 0, 0, false, false));
        Assert.Equal(0U, IndexMutationKernel.AdvanceEntry(64, 8, 16, 16, 1, 2, false, false));
        Assert.Equal(64U, IndexMutationKernel.AdvanceEntry(64, 8, 16, 16, 44, 0, true, false));
    }

    [Fact]
    public void Prefix_MaximalMatchAndAliasedInputsPreserveKeys()
    {
        byte[] first = [1, 2, 3, 4];
        byte[] second = [1, 2, 9];
        Assert.Equal(2U, IndexMutationKernel.CommonPrefix(first, second, 4));
        Assert.Equal(1U, IndexMutationKernel.CommonPrefix(first, second, 1));
        Assert.Equal(4U, IndexMutationKernel.CommonPrefix(first, first, 4));
        Assert.Equal(0U, IndexMutationKernel.CommonPrefix(first, [], 4));
        Assert.Equal([1, 2, 3, 4], first);
        Assert.Equal([1, 2, 9], second);
    }

    [Fact]
    public void Finish_FullPayloadWithUnrepresentableSentinelPreservesOtherBytes()
    {
        byte[] page = new byte[64];
        Array.Fill(page, (byte)0xA5);
        byte[] expected = (byte[])page.Clone();
        expected[2] = 0;
        expected[3] = 0;
        IndexMutationKernel.FinishPage(page, 15, 16, 64, true);
        Assert.Equal(expected, page);
    }

    private static void PutWord(byte[] destination, int offset, byte[] bytes) => bytes.CopyTo(destination, offset);
}
