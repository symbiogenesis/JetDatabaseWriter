namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using Xunit;

/// <summary>Checks cached allocation state and pending blank reservations.</summary>
public sealed class PageAllocatorStateTests
{
    /// <summary>An empty free set is remembered; a reservation's final image replaces its zeroes.</summary>
    [Fact]
    public async Task Reservations_ReuseFreeSet_AndWriteFinalImage()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        await using var stream = new MemoryStream();
        stream.SetLength(format.PageSize * 3);
        byte[] map = new byte[format.PageSize];
        map[0] = Constants.PageTypes.Data;
        map[1] = 1;
        BinaryPrimitives.WriteUInt16LittleEndian(map.AsSpan(format.DataPage.NumRows, 2), 1);
        BinaryPrimitives.WriteUInt16LittleEndian(map.AsSpan(format.DataPage.RowsStart, 2), checked((ushort)(format.PageSize - Constants.UsageMap.RowSize)));
        stream.Position = format.PageSize;
        stream.Write(map);
#pragma warning disable CA2000 // The awaited pager owns its codec.
        await using var pager = new Pager(stream, format.PageSize, new NoPageCodec(), true, typeof(AccessWriter), 0);
#pragma warning restore CA2000
        var allocator = new PageAllocator(format, pager, new AccessWriterOptions());
        await using (pager.BeginWriteScope())
        {
            long first = await allocator.ReserveContiguousPagesAsync(1, TestContext.Current.CancellationToken);
            long reads = pager.Statistics.StoreReads;
            long second = await allocator.ReserveContiguousPagesAsync(1, TestContext.Current.CancellationToken);
            Assert.Equal(first + 1, second);
            Assert.Equal(reads, pager.Statistics.StoreReads);
            byte[] final = new byte[format.PageSize];
            final[0] = 2;
            await pager.WritePageAsync(first, final, TestContext.Current.CancellationToken);
            Assert.Equal(format.PageSize * 3, stream.Length);
        }

        Assert.Equal(2, stream.ToArray()[format.PageSize * 3]);
        pager.InvalidateAll();
        long readsBeforeInvalidation = pager.Statistics.StoreReads;
        await allocator.ReserveContiguousPagesAsync(1, TestContext.Current.CancellationToken);
        Assert.True(pager.Statistics.StoreReads > readsBeforeInvalidation);
    }
}
