namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Bitmap enumeration retains every marked page, order and strict boundary check.</summary>
public sealed class UsageMapBitmapEnumerationTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task EveryBytePatternAndSparseEnds_PreserveMembersAndBoundaryFailures(DatabaseFormat kind, bool reference)
    {
        var format = JetFormat.ForNewDatabase(kind);
        byte[] map = new byte[format.PageSize];
        byte[] bitmap = reference ? new byte[format.PageSize] : map;
        int start = 32;
        int size = reference ? 13 : format.PageSize - start;
        var row = new RowBound(0, start, size);
        map[start] = reference ? Constants.UsageMap.ReferenceMapType : Constants.UsageMap.InlineMapType;
        Wi32(map, start + (reference ? 9 : 1), reference ? 7 : 16);
        bitmap[0] = reference ? Constants.PageTypes.UsageMap : (byte)0;
        int bitmapOffset = reference ? Constants.UsageMap.ReferenceMapBitmapOffset : start + Constants.UsageMap.InlineBitmapOffset;
        int capacity = (format.PageSize - bitmapOffset) * 8;
        long firstPage = reference ? 2L * UsageMap.PagesPerReferenceMapPage(format.PageSize) : 16;
        int reads = 0;
        int returns = 0;
        for (int pattern = 0; pattern < 3; pattern++)
        {
            Array.Clear(bitmap, bitmapOffset, bitmap.Length - bitmapOffset);
            var expected = new List<long>();
            for (int bit = 0; bit < capacity; bit++)
            {
                bool marked = pattern switch
                {
                    0 => (((bit / 8) % 256) & (1 << (bit % 8))) != 0,
                    1 => bit == 0 || bit == 7 || bit == 8 || bit == capacity - 1,
                    _ => false,
                };
                if (marked)
                {
                    bitmap[bitmapOffset + (bit / 8)] |= (byte)(1 << (bit % 8));
                    expected.Add(firstPage + bit);
                }
            }

            var actual = new List<long>();
            Assert.True(await EnumerateAsync(0, firstPage + capacity, true, actual));
            Assert.Equal(expected, actual);

            long minimum = firstPage + 9;
            long total = firstPage + capacity - 9;
            actual.Clear();
            Assert.True(await EnumerateAsync(minimum, total, false, actual));
            Assert.Equal(expected.FindAll(page => page >= minimum && page < total), actual);
            actual.Clear();
            bool withinBounds = expected.TrueForAll(page => page >= minimum && page < total);
            Assert.Equal(withinBounds, await EnumerateAsync(minimum, total, true, actual));
        }

        Assert.Equal(reference ? 9 : 0, reads);
        Assert.Equal(reads, returns);

        ValueTask<bool> EnumerateAsync(long minimum, long total, bool strict, List<long> pages)
            => UsageMap.TryEnumeratePagesAsync(map, row, format.PageSize, total, minimum, strict, ReadAsync, Return, pages, TestContext.Current.CancellationToken);

        ValueTask<byte[]> ReadAsync(long page, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Assert.Equal(7, page);
            reads++;
            return new ValueTask<byte[]>(bitmap);
        }

        void Return(byte[] page)
        {
            Assert.Same(bitmap, page);
            returns++;
        }
    }
}
