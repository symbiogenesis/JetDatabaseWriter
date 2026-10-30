namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using System.Buffers;
using System.Buffers.Binary;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Pages.Paging;
using Xunit;

/// <summary>Page ownership survives eviction until the last reader releases its lease.</summary>
public sealed class PageLeaseTests
{
    [Fact]
    public void EvictionAndRepeatedDisposal_ReturnBufferExactlyOnceAfterLastLease()
    {
        byte[] bytes = ArrayPool<byte>.Shared.Rent(4096);
        bytes[0] = 73;
        int returned = 0;
        var frame = new PageFrame(bytes, 4096, buffer =>
        {
            Assert.Same(bytes, buffer);
            returned++;
            ArrayPool<byte>.Shared.Return(buffer);
        });
        using PageLease first = frame.Acquire();
        using PageLease second = frame.Acquire();
        frame.Release();
        Assert.Equal(0, returned);
        Assert.Equal(73, first.Memory.Span[0]);
        first.Dispose();
        first.Dispose();
        Assert.Equal(0, returned);
        Assert.Equal(73, second.Memory.Span[0]);
        second.Dispose();
        second.Dispose();
        Assert.Equal(1, returned);
        Assert.Throws<ObjectDisposedException>(() => second.Memory);
    }

    [Fact]
    public void ReleasedFrame_RefusesAcquisitionWithoutReturningBufferAgain()
    {
        byte[] bytes = ArrayPool<byte>.Shared.Rent(4096);
        int returned = 0;
        var frame = new PageFrame(bytes, 4096, buffer =>
        {
            returned++;
            ArrayPool<byte>.Shared.Return(buffer);
        });
        frame.Release();
        Assert.Throws<ObjectDisposedException>(() => frame.Acquire());
        Assert.Equal(1, returned);
    }

    [Fact]
    public void SharedImage_RetainsItsParsedDirectoryAfterCacheOwnershipEnds()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        byte[] bytes = ArrayPool<byte>.Shared.Rent(format.PageSize);
        Array.Clear(bytes, 0, format.PageSize);
        bytes[0] = Constants.PageTypes.Data;
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(format.DataPage.NumRows), 1);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(format.DataPage.RowsStart), checked((ushort)(format.PageSize - 4)));
        var frame = new PageFrame(bytes, format.PageSize);
        using PageLease first = frame.Acquire();
        using PageLease second = frame.Acquire();
        frame.Release();
        Assert.Single(first.GetRowDirectory(format));
        Assert.Same(first.GetRowDirectory(format), second.GetRowDirectory(format));
        first.Dispose();
        Assert.Equal(4, Assert.Single(second.GetRowDirectory(format)).RowSize);
    }
}
