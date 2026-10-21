namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.Linq;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using Xunit;

/// <summary>Unsigned lexicographic ordering of encoded index keys.</summary>
public sealed class IndexKeyComparisonTests
{
    [Theory]
    [InlineData(new byte[] { }, new byte[] { }, 0)]
    [InlineData(new byte[] { }, new byte[] { 0 }, -1)]
    [InlineData(new byte[] { 0 }, new byte[] { }, 1)]
    [InlineData(new byte[] { 1, 2 }, new byte[] { 1, 2 }, 0)]
    [InlineData(new byte[] { 1, 2 }, new byte[] { 1, 2, 0 }, -1)]
    [InlineData(new byte[] { 1, 2, 0 }, new byte[] { 1, 2 }, 1)]
    [InlineData(new byte[] { 127 }, new byte[] { 128 }, -1)]
    [InlineData(new byte[] { 255 }, new byte[] { 0 }, 1)]
    public void CompareKeyBytes_UsesUnsignedLexicographicOrder(byte[] left, byte[] right, int expected)
        => Assert.Equal(expected, Math.Sign(IndexPageCodec.CompareKeyBytes(left, right)));

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void PageSearch_PreservesCanonicalKeyOrderingAndPrefixes(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        byte[][] keys =
        [
            [0x7F, 0x80],
            [0x7F, 0x80, 0],
            [0x7F, 0x80, 0x7F],
            [0x7F, 0x80, 0x80],
            [0x7F, 0x80, 0xFF],
            [0x7F, 0x80, 0xFF, 0],
        ];
        IndexEntry[] entries = keys.Select((key, index) => new IndexEntry(key, 100 + index, checked((byte)index))).ToArray();
        DecodedIntermediateEntry[] children = entries.Select((entry, index) => new DecodedIntermediateEntry(entry, 200 + index)).ToArray();
        byte[][] searches =
        [
            [],
            [0x7F],
            [0x7F, 0x7F],
            [0x7F, 0x80],
            [0x7F, 0x80, 0],
            [0x7F, 0x80, 0, 0],
            [0x7F, 0x80, 0x7F],
            [0x7F, 0x80, 0x80],
            [0x7F, 0x80, 0xFF],
            [0x7F, 0x80, 0xFF, 0],
            [0x7F, 0x80, 0xFF, 1],
            [0x80],
        ];

        foreach (bool compressed in new[] { false, true })
        {
            byte[] leaf = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 7, entries, enablePrefixCompression: compressed);
            byte[] intermediate = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 7, children, 0, 0, 0, maxPrefixLength: compressed ? null : 0);
            foreach (byte[] search in searches)
            {
                var exactMatches = new List<(long DataPage, int RowIndex)>();
                _ = IndexPageCodec.CollectMatchingLeafEntries(format.IndexPage, leaf, format.PageSize, search, exactMatches);
                Assert.Equal(entries.Where(entry => entry.Key.AsSpan().SequenceEqual(search)).Select(entry => (entry.DataPage, (int)entry.DataRow)), exactMatches);
                Assert.Equal(exactMatches.Count > 0, IndexPageCodec.ContainsKeyInLeafPage(format.IndexPage, leaf, format.PageSize, search).Found);

                var prefixMatches = new List<(long DataPage, int RowIndex)>();
                var range = new EncodedIndexRange(new(search, true, true), new(search, true, true), search);
                _ = IndexPageCodec.CollectRangeLeafEntries(format.IndexPage, leaf, format.PageSize, range, prefixMatches);
                Assert.Equal(entries.Where(entry => entry.Key.AsSpan().StartsWith(search)).Select(entry => (entry.DataPage, (int)entry.DataRow)), prefixMatches);

                long? expectedChild = children.Where(child => child.Entry.Key.AsSpan().SequenceCompareTo(search) >= 0).Select(child => (long?)child.ChildPage).FirstOrDefault();
                Assert.Equal(expectedChild, IndexPageCodec.SelectChildPage(format.IndexPage, intermediate, format.PageSize, search));
            }
        }
    }
}
