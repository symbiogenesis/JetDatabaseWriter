namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Access-authored Jet3 prefixes can include row-pointer bytes.</summary>
public sealed class Jet3CompressedIndexFixtureTests
{
    [Fact]
    public void LongSharedPrefix_IsCappedBeforePayloadEmission()
    {
        byte[] key = Enumerable.Repeat((byte)0x7F, 300).ToArray();
        IndexEntry[] entries = [new(key, 1, 0), new(key, 1, 1)];
        byte[] page = IndexPageCodec.BuildLeafPage(IndexPageLayout.Jet3, 2048, 1, entries, 0, 0, 0, enablePrefixCompression: true);
        Assert.Equal(255, IndexPageCodec.ReadPrefixLength(IndexPageLayout.Jet3, page));
        Assert.Equal(0, page[21]);
        Assert.Equal(entries, IndexPageCodec.DecodeLeafEntries(IndexPageLayout.Jet3, page, 2048), new EntryComparer());
    }

    [Fact]
    public async Task NorthwindShipVia_CompressedPointerDecodesAndSeeks()
    {
        byte[] database = await File.ReadAllBytesAsync(TestDatabases.MdbtoolsNwind, TestContext.Current.CancellationToken);
        byte[] page = database.AsSpan(593 * 2048, 2048).ToArray();
        IndexPageLayout layout = IndexPageLayout.Jet3;
        byte[] key = [0x7F, 0x80, 0x00, 0x00, 0x01];
        Assert.Equal(6, IndexPageCodec.ReadPrefixLength(layout, page));
        List<IndexEntry> entries = IndexPageCodec.DecodeLeafEntries(layout, page, 2048);
        Assert.Equal(249, entries.Count);
        Assert.All(entries, entry => Assert.Equal(key, entry.Key));
        Assert.Equal(399, entries[0].DataPage);
        Assert.Equal(0, entries[0].DataRow);
        Assert.Equal(1, entries[1].DataRow);
        Assert.True(IndexPageCodec.ContainsKeyInLeafPage(layout, page, 2048, key).Found);
        var matches = new List<(long DataPage, int RowIndex)>();
        IndexPageCodec.CollectMatchingLeafEntries(layout, page, 2048, key, matches);
        Assert.Equal(entries.Select(entry => (entry.DataPage, (int)entry.DataRow)), matches);
        matches.Clear();
        var range = new EncodedIndexRange(new(key, true, false), new(key, true, false), key);
        IndexPageCodec.CollectRangeLeafEntries(layout, page, 2048, range, matches);
        Assert.Equal(entries.Select(entry => (entry.DataPage, (int)entry.DataRow)), matches);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public void IntermediatePrefix_ReconstructsRowAndChildPointers(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        IndexPageLayout layout = format.IndexPage;
        byte[] page = new byte[format.PageSize];
        page[0] = 3;
        page[1] = 1;
        page[layout.PrefLenOffset] = 12;
        if (kind == DatabaseFormat.Jet3Mdb)
        {
            page[21] = 1;
        }

        byte[] firstEntry = [0x7F, 0x80, 0, 0, 1, 0, 1, 0x8F, 2, 0, 0, 1, 2];
        firstEntry.CopyTo(page, layout.FirstEntryOffset);
        page[layout.FirstEntryOffset + 13] = 3;
        page[layout.BitmaskOffset + 1] = 0x60;
        BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(2, 2), (ushort)(format.PageSize - layout.FirstEntryOffset - 14));
        byte[] key = firstEntry.AsSpan(0, 5).ToArray();
        List<DecodedIntermediateEntry> entries = IndexPageCodec.DecodeIntermediateEntries(layout, page, format.PageSize);
        Assert.Equal(2, entries.Count);
        Assert.All(entries, entry => Assert.Equal(key, entry.Entry.Key));
        Assert.All(entries, entry => Assert.Equal(399, entry.Entry.DataPage));
        Assert.All(entries, entry => Assert.Equal(2, entry.Entry.DataRow));
        Assert.Equal(258, entries[0].ChildPage);
        Assert.Equal(259, entries[1].ChildPage);
        Assert.Equal(258, IndexPageCodec.SelectChildPage(layout, page, format.PageSize, key));
        Assert.Equal(258, IndexPageCodec.ReadFirstChildPointer(layout, page, format.PageSize));
        Assert.Equal(259, IndexPageCodec.ReadLastChildPointer(layout, page, format.PageSize));
    }

    private sealed class EntryComparer : IEqualityComparer<IndexEntry>
    {
        public bool Equals(IndexEntry x, IndexEntry y)
            => x.DataPage == y.DataPage && x.DataRow == y.DataRow && x.Key.SequenceEqual(y.Key);

        public int GetHashCode(IndexEntry obj) => obj.DataRow;
    }
}
