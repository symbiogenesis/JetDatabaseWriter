namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Checks physical-store capabilities, ownership and concurrent I/O.</summary>
public sealed class PageStoreTests
{
    /// <summary>Memory stores expose in-place pages without file durability.</summary>
    [Fact]
    public async Task Memory_Capabilities_AndOwnership()
    {
        await using var stream = new MemoryStream();
        await using (var store = new MemoryPageStore(stream, leaveOpen: true))
        {
            Assert.Equal(new StoreCapabilities(false, false, false, false, false, true), store.Capabilities);
            await store.WriteAsync(0, new byte[] { 1, 2, 3 }, TestContext.Current.CancellationToken);
            byte[] page = new byte[3];
            await store.ReadAsync(0, page, false, TestContext.Current.CancellationToken);
            Assert.Equal(new byte[] { 1, 2, 3 }, page);
            await store.SetLengthAsync(2, TestContext.Current.CancellationToken);
            Assert.Equal(2, store.Length);
        }

        Assert.True(stream.CanRead);
    }

    /// <summary>A short read releases the seek gate so the store remains usable.</summary>
    /// <param name="inline">Whether to use synchronous reads.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ShortRead_DoesNotLeakGate(bool inline)
    {
        await using var stream = new MemoryStream();
        await using var store = new MemoryPageStore(stream, leaveOpen: true);
        _ = await Assert.ThrowsAsync<EndOfStreamException>(async () => await store.ReadAsync(0, new byte[16], inline, TestContext.Current.CancellationToken));
        await store.WriteAsync(0, new byte[16], TestContext.Current.CancellationToken);
        await store.ReadAsync(0, new byte[16], inline, TestContext.Current.CancellationToken);
    }

    /// <summary>Both read paths fill the requested buffer when a stream returns partial reads.</summary>
    /// <param name="inline">Whether to use synchronous reads.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PartialReads_FillRequestedSlice(bool inline)
    {
        await using var stream = new ChunkedReadStream([1, 2, 3, 4, 5, 6, 7]);
        await using var store = new StreamPageStore(stream, leaveOpen: true);
        byte[] page = [99, 99, 99, 99, 99, 99, 99];
        await store.ReadAsync(1, page.AsMemory(1, 5), inline, TestContext.Current.CancellationToken);
        Assert.Equal(new byte[] { 99, 2, 3, 4, 5, 6, 99 }, page);
        Assert.Equal(6, stream.Position);
    }

    /// <summary>Seek-based operations cannot corrupt each other's stream position.</summary>
    [Fact]
    public async Task ConcurrentReads_UseTheirOwnOffsets()
    {
        await using var stream = new MemoryStream([1, 2, 3, 4]);
        await using var store = new MemoryPageStore(stream, leaveOpen: true);
        byte[] first = new byte[2];
        byte[] second = new byte[2];
        Task one = store.ReadAsync(0, first, false, TestContext.Current.CancellationToken).AsTask();
        Task two = store.ReadAsync(2, second, false, TestContext.Current.CancellationToken).AsTask();
        await Task.WhenAll(one, two);
        Assert.Equal(new byte[] { 1, 2 }, first);
        Assert.Equal(new byte[] { 3, 4 }, second);
    }

    /// <summary>File stores advertise durability and preserve position on positional reads.</summary>
    [Fact]
    public async Task File_PositionalReads_AndDurableFlush()
    {
        string path = Path.Combine(Path.GetTempPath(), $"PageStore_{Guid.NewGuid():N}.bin");
        try
        {
            await using var stream = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite);
            await stream.WriteAsync(new byte[] { 1, 2, 3, 4 }, TestContext.Current.CancellationToken);
            await using var store = new StreamPageStore(stream, leaveOpen: true);
            store.EnablePositionalReads();
            Assert.True(store.Capabilities.IsFileBacked);
            Assert.True(store.Capabilities.DurableFlush);
            Assert.False(store.Capabilities.AtomicCommit);
            Assert.Equal(!LibraryTarget.IsNetStandard, store.Capabilities.PositionalReads);
            stream.Position = 3;
            byte[] page = new byte[2];
            await store.ReadAsync(0, page, false, TestContext.Current.CancellationToken);
            Assert.Equal(new byte[] { 1, 2 }, page);
            if (!LibraryTarget.IsNetStandard)
            {
                Assert.Equal(3, stream.Position);
            }

            await store.FlushAsync(true, TestContext.Current.CancellationToken);
        }
        finally
        {
            File.Delete(path);
        }
    }

    /// <summary>Construction cleanup releases synchronization without taking stream ownership.</summary>
    [Fact]
    public async Task ConstructionCleanup_LeavesCallerStreamOpen()
    {
        await using var stream = new MemoryStream();
        await using var store = new MemoryPageStore(stream, leaveOpen: true);
        store.DisposeManagedResources();
        store.DisposeManagedResources();
        Assert.True(stream.CanRead);
    }

    private sealed class ChunkedReadStream(byte[] buffer) : MemoryStream(buffer)
    {
        public override int Read(Span<byte> buffer) => base.Read(buffer[..Math.Min(buffer.Length, 2)]);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => base.ReadAsync(buffer[..Math.Min(buffer.Length, 2)], cancellationToken);
    }
}
