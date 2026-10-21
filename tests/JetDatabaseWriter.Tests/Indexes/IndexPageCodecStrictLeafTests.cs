namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Schema;
using Xunit;

/// <summary>Strict leaf decoding distinguishes a complete payload from a safely decoded prefix.</summary>
public sealed class IndexPageCodecStrictLeafTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public void CompleteLeaf_RoundTripsCompressedAndUncompressedEntries(DatabaseFormat databaseFormat, bool compressed)
    {
        var format = JetFormat.ForNewDatabase(databaseFormat);
        IndexEntry[] expected = [new([1, 2, 3], 12, 0), new([1, 2, 4], 12, 1), new([1, 2, 5], 13, 0)];
        byte[] page = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, expected, compressed);
        Assert.True(IndexPageCodec.TryDecodeLeafEntries(format.IndexPage, page, format.PageSize, out List<IndexEntry> actual));
        Assert.Equal(expected.Length, actual.Count);
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i].Key, actual[i].Key);
            Assert.Equal(expected[i].DataPage, actual[i].DataPage);
            Assert.Equal(expected[i].DataRow, actual[i].DataRow);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void EmptyLeaf_IsCompleteWithoutAnEndSentinel(DatabaseFormat databaseFormat)
    {
        var format = JetFormat.ForNewDatabase(databaseFormat);
        byte[] page = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [], enablePrefixCompression: true);
        Assert.True(IndexPageCodec.TryDecodeLeafEntries(format.IndexPage, page, format.PageSize, out List<IndexEntry> actual));
        Assert.Empty(actual);

        page[format.IndexPage.BitmaskOffset] = 1;
        Assert.False(IndexPageCodec.TryDecodeLeafEntries(format.IndexPage, page, format.PageSize, out _));
        page[format.IndexPage.BitmaskOffset] = 0;
        page[format.IndexPage.PrefLenOffset] = 1;
        Assert.False(IndexPageCodec.TryDecodeLeafEntries(format.IndexPage, page, format.PageSize, out _));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void CompressedRowPointerSuffix_ReconstructsTheEntireTrailer(DatabaseFormat databaseFormat)
    {
        var format = JetFormat.ForNewDatabase(databaseFormat);
        IndexEntry[] expected = [new([1], 12, 0), new([1], 12, 1)];
        byte[] page = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, expected, enablePrefixCompression: false);
        Array.Clear(page, format.IndexPage.BitmaskOffset, format.IndexPage.FirstEntryOffset - format.IndexPage.BitmaskOffset);
        page[format.IndexPage.PrefLenOffset] = 4;
        page[format.IndexPage.FirstEntryOffset + 5] = 1;
        page[format.IndexPage.BitmaskOffset] = 0b0110_0000;
        JetTypeInfo.Wu16(page, 2, format.PageSize - format.IndexPage.FirstEntryOffset - 6);

        Assert.True(IndexPageCodec.TryDecodeLeafEntries(format.IndexPage, page, format.PageSize, out List<IndexEntry> actual));
        Assert.Equal(expected.Length, actual.Count);
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i].Key, actual[i].Key);
            Assert.Equal(expected[i].DataPage, actual[i].DataPage);
            Assert.Equal(expected[i].DataRow, actual[i].DataRow);
        }

        Assert.Equal(expected.Length, IndexPageCodec.DecodeLeafEntries(format.IndexPage, page, format.PageSize).Count);
    }

    [Theory]
    [InlineData("FixedText/NativeFixedTextJet3Cp1252.mdb")]
    [InlineData("FixedText/NativeFixedTextJet3Cp932.mdb")]
    [InlineData("NorthwindTraders.accdb")]
    public async Task NativeCatalogLeaves_HaveCompletePayloadsIncludingEmptyLeaves(string fixture)
    {
        byte[] image = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", fixture), TestContext.Current.CancellationToken);
        var format = JetFormat.FromHeader(image);
        int count = 0;
        for (int offset = format.PageSize; offset + format.PageSize <= image.Length; offset += format.PageSize)
        {
            byte[] page = image.AsSpan(offset, format.PageSize).ToArray();
            if (!IndexPageCodec.IsLeaf(page) || JetTypeInfo.Ri32(page, 4) != 2)
            {
                continue;
            }

            Assert.True(IndexPageCodec.TryDecodeLeafEntries(format.IndexPage, page, format.PageSize, out _), $"Incomplete native catalog leaf at page {offset / format.PageSize}.");
            count++;
        }

        Assert.True(count > 0);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void GenericDecoder_RetainsEntriesWithoutStrictSentinelValidation(DatabaseFormat databaseFormat)
    {
        var format = JetFormat.ForNewDatabase(databaseFormat);
        IndexEntry[] expected = [new([1, 2, 3], 12, 0), new([1, 2, 4], 12, 1)];
        byte[] page = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, expected, enablePrefixCompression: true);
        int payloadLength = format.PageSize - JetTypeInfo.Ru16(page, 2) - format.IndexPage.FirstEntryOffset;
        page[format.IndexPage.BitmaskOffset + (payloadLength / 8)] &= unchecked((byte)~(1 << (payloadLength % 8)));
        Assert.False(IndexPageCodec.TryDecodeLeafEntries(format.IndexPage, page, format.PageSize, out _));
        Assert.Equal(expected.Length, IndexPageCodec.DecodeLeafEntries(format.IndexPage, page, format.PageSize).Count);
    }
}
