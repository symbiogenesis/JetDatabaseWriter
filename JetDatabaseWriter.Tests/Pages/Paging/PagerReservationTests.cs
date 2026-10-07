namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Checks logical zero reservations independently of physical writes.</summary>
public sealed class PagerReservationTests
{
    /// <summary>Reserved zero images survive cache eviction and dirty spills without physical writes.</summary>
    /// <param name="cacheSize">The retained frame count.</param>
    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    public async Task Reservations_AcrossSpill_WriteOnlyInitializedImages(int cacheSize)
    {
        await using var stream = new MemoryStream();
        stream.SetLength(16);
        await using var trace = new PageTraceStream(stream, 16);
#pragma warning disable CA2000 // The awaited pager owns the codec.
        await using var pager = new Pager(trace, 16, new NoPageCodec(), true, typeof(AccessWriter), cacheSize);
#pragma warning restore CA2000
        await using (pager.BeginWriteScope())
        {
            for (int index = 0; index < 70; index++)
            {
                Assert.Equal(index + 1, await pager.ReserveZeroedPageAsync(TestContext.Current.CancellationToken));
            }

            Assert.Equal(16, stream.Length);
            for (int index = 1; index <= 70; index++)
            {
                byte[] reserved = await pager.ReadUncachedPageAsync(index, TestContext.Current.CancellationToken);
                Assert.Equal(new byte[16], reserved.AsSpan(0, 16).ToArray());
                PageBuffers.Return(reserved);
            }

            for (int index = 1; index <= 70; index++)
            {
                byte[] image = new byte[16];
                image[0] = 37;
                await pager.WritePageAsync(index, image, TestContext.Current.CancellationToken);
                if (index == 64)
                {
                    Assert.Equal(71, pager.PageCount);
                    Assert.Equal(65 * 16, stream.Length);
                    Assert.Equal(new byte[16], await pager.ReadPageCopyAsync(70, TestContext.Current.CancellationToken));
                }
            }
        }

        Assert.Equal(71, pager.PageCount);
        Assert.Equal(71 * 16, stream.Length);
        Assert.Equal(70 * 16, trace.BytesWritten);
        Assert.Equal(70, trace.WriteCounts(16).Count);
        Assert.All(trace.WriteCounts(16).Values, count => Assert.Equal(1, count));
    }

    /// <summary>Rollback discards reserved logical pages and their readable zero images.</summary>
    [Fact]
    public async Task JournalRollback_DiscardsReservations()
    {
        await using var stream = new MemoryStream();
        stream.SetLength(16);
#pragma warning disable CA2000 // The awaited pager owns the codec.
        await using var pager = new Pager(stream, 16, new NoPageCodec(), true, typeof(AccessWriter));
#pragma warning restore CA2000
        using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(TestContext.Current.CancellationToken))
        {
            gate.Attach(new PagerTransaction(16, 16, 4));
        }

        Assert.Equal(1, await pager.ReserveZeroedPageAsync(TestContext.Current.CancellationToken));
        Assert.Equal(new byte[16], await pager.ReadPageCopyAsync(1, TestContext.Current.CancellationToken));
        using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(TestContext.Current.CancellationToken))
        {
            gate.Detach();
        }

        Assert.Equal(1, pager.PageCount);
        await Assert.ThrowsAsync<EndOfStreamException>(() => pager.ReadPageCopyAsync(1, TestContext.Current.CancellationToken).AsTask());
    }

    /// <summary>An abandoned reservation is released as reusable freed pages, with no stale payload on reuse.</summary>
    [Fact]
    public async Task AbandonedRun_ReleaseAndReuse_ReturnsZeroImages()
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
#pragma warning disable CA2000 // The awaited pager owns the codec.
        await using var pager = new Pager(stream, format.PageSize, new NoPageCodec(), true, typeof(AccessWriter), 0);
#pragma warning restore CA2000
        var allocator = new PageAllocator(format, pager, new AccessWriterOptions());
        await using (pager.BeginWriteScope())
        {
            long first = await allocator.ReserveContiguousPagesAsync(70, TestContext.Current.CancellationToken);
            await allocator.ReleaseReservedPagesAsync(first, 70);
            Assert.True(await allocator.IsPageFreeAsync(first, TestContext.Current.CancellationToken));
            Assert.True(await allocator.IsPageFreeAsync(first + 69, TestContext.Current.CancellationToken));
            Assert.Equal(first, await allocator.ReserveContiguousPagesAsync(70, TestContext.Current.CancellationToken));
            Assert.Equal(new byte[format.PageSize], await pager.ReadPageCopyAsync(first, TestContext.Current.CancellationToken));
            await allocator.ReleaseReservedPagesAsync(first, 70);
        }
    }

    /// <summary>A failed budget reservation consumes no logical page number.</summary>
    [Fact]
    public async Task FailedReservation_DoesNotAdvanceJournalEnd()
    {
        await using var stream = new MemoryStream();
        stream.SetLength(16);
#pragma warning disable CA2000 // The awaited pager owns the codec.
        await using var pager = new Pager(stream, 16, new NoPageCodec(), true, typeof(AccessWriter));
#pragma warning restore CA2000
        using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(TestContext.Current.CancellationToken))
        {
            gate.Attach(new PagerTransaction(16, 16, 1));
        }

        Assert.Equal(1, await pager.ReserveZeroedPageAsync(TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<JetLimitationException>(() => pager.ReserveZeroedPageAsync(TestContext.Current.CancellationToken).AsTask());
        Assert.Equal(2, pager.PageCount);
        Assert.Equal(16, stream.Length);
        using Pager.JournalGate detached = await pager.EnterJournalGateAsync(TestContext.Current.CancellationToken);
        detached.Detach();
    }
}
