namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.ValueDecoding;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

public sealed class LongValueMaterializationAllocationTests
{
    [Theory]
    [InlineData(false, 0)]
    [InlineData(false, 1)]
    [InlineData(false, 2)]
    [InlineData(false, 3)]
    [InlineData(false, 4)]
    [InlineData(true, 0)]
    [InlineData(true, 1)]
    [InlineData(true, 2)]
    [InlineData(true, 3)]
    [InlineData(true, 4)]
    public async Task UnreadableFirstRow_RefusesBeforeAllocatingClaimedPayload(bool chained, int damage)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        byte[] physicalPage = new byte[format.PageSize];
        physicalPage[0] = damage == 2 ? Constants.PageTypes.TableDefinition : Constants.PageTypes.Data;
        "LVAL"u8.CopyTo(physicalPage.AsSpan(4));
        Wu16(physicalPage, format.DataPage.NumRows, 1);
        Wu16(physicalPage, format.DataPage.RowsStart, format.PageSize - 3);
        var source = new SynchronousPageSource(physicalPage);
        using var pages = new ReaderPageCache(format, source, capacity: 0);
        var decoder = new LongValueDecoder(format, pages);
        uint pointer = damage switch
        {
            0 => 0,
            1 => LongValueStore.MakeRowPointer(2, 0),
            3 => LongValueStore.MakeRowPointer(1, 1),
            _ => LongValueStore.MakeRowPointer(1, 0),
        };
        byte[] descriptor = (chained
            ? LongValueDescriptor.Chained(0xFFFFFF, pointer, 0)
            : LongValueDescriptor.SinglePage(0xFFFFFF, pointer, 0)).ToHeaderBytes();

        // Every source operation completes synchronously. Measure only the call
        // returning its completed refusal, before any await can change threads.
        long before = GC.GetAllocatedBytesForCurrentThread();
        ValueTask<byte[]> read = decoder.ReadLongValueBytesExactAsync(descriptor, 0, descriptor.Length, TestContext.Current.CancellationToken);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(read.IsCompleted);
        await Assert.ThrowsAsync<InvalidDataException>(async () => await read);
        Assert.Equal(damage is 0 or 1 ? 0 : 1, source.ReadCount);
        Assert.InRange(allocated, 0, 1024 * 1024);
    }

    private sealed class SynchronousPageSource(byte[] physicalPage) : IPageSource
    {
        public int PageSize => physicalPage.Length;

        public long PageCount => 2;

        public bool IsDisposed => false;

        internal int ReadCount { get; private set; }

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Assert.Equal(1, pageNumber);
            this.ReadCount++;
            byte[] buffer = ArrayPool<byte>.Shared.Rent(this.PageSize);
            physicalPage.CopyTo(buffer.AsSpan());
            return new ValueTask<byte[]>(buffer);
        }

        public void ThrowIfDisposed()
        {
        }

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken)
            => cancellationToken.ThrowIfCancellationRequested();
    }
}
