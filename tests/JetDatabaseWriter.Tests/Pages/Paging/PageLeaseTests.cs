namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Threading;
using System.Threading.Tasks;
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
    public async Task ConcurrentDisposalOfOneLease_PreservesSurvivorAndReleasesExactlyOnce()
    {
        byte[] bytes = [73, 19];
        int returned = 0;
        int released = 0;
        var frame = new PageFrame(bytes, bytes.Length, buffer =>
        {
            Assert.Same(bytes, buffer);
            Interlocked.Increment(ref returned);
        });
        using PageLease survivor = frame.Acquire();
        using PageLease disposed = frame.Acquire(() => Interlocked.Increment(ref released));
        frame.Release();
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var workers = new Task[32];
        for (int index = 0; index < workers.Length; index++)
        {
            workers[index] = DisposeAsync();
        }

        var allWorkers = Task.WhenAll(workers);
        start.SetResult();

        // Retain worker failures and finish cleanup even after test cancellation.
        await Assert.MultipleAsync(
            async () => await allWorkers.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken),
            async () => await allWorkers.WaitAsync(TimeSpan.FromSeconds(30), CancellationToken.None));
        Assert.Equal(1, released);
        Assert.Equal(0, returned);
        Assert.Equal(bytes, survivor.Memory.ToArray());
        Assert.Throws<ObjectDisposedException>(() => disposed.Memory);
        survivor.Dispose();
        survivor.Dispose();
        Assert.Equal(1, returned);
        Assert.Throws<ObjectDisposedException>(() => frame.Acquire());

        async Task DisposeAsync()
        {
            await start.Task.WaitAsync(TestContext.Current.CancellationToken);
            disposed.Dispose();
        }
    }

    [Fact]
    public async Task ConcurrentDistinctLeases_RetainProducerUntilItsFinalRelease()
    {
        byte[] bytes = [73, 19];
        int returned = 0;
        int released = 0;
        int acquired = 0;
        var frame = new PageFrame(bytes, bytes.Length, buffer =>
        {
            Assert.Same(bytes, buffer);
            Interlocked.Increment(ref returned);
        });
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allAcquired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var workers = new Task[32];
        for (int index = 0; index < workers.Length; index++)
        {
            workers[index] = AcquireAndReleaseAsync();
        }

        var allWorkers = Task.WhenAll(workers);
        bool producerReleased = false;
        try
        {
            start.SetResult();
            await Assert.MultipleAsync(
                async () =>
                {
                    try
                    {
                        await allAcquired.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
                        Assert.Equal(0, returned);
                    }
                    finally
                    {
                        release.TrySetResult();
                    }
                },
                async () => await allWorkers.WaitAsync(TimeSpan.FromSeconds(30), CancellationToken.None));
            Assert.Equal(workers.Length, released);
            Assert.Equal(0, returned);
            using PageLease last = frame.Acquire(() => Interlocked.Increment(ref released));
            Assert.Equal(bytes, last.Memory.ToArray());
            producerReleased = true;
            frame.Release();
            Assert.Equal(0, returned);
            last.Dispose();
            Assert.Equal(workers.Length + 1, released);
            Assert.Equal(1, returned);
            Assert.Throws<ObjectDisposedException>(() => frame.Acquire());
        }
        finally
        {
            if (!producerReleased)
            {
                frame.Release();
            }
        }

        async Task AcquireAndReleaseAsync()
        {
            await start.Task.WaitAsync(TestContext.Current.CancellationToken);
            using PageLease lease = frame.Acquire(() => Interlocked.Increment(ref released));
            if (Interlocked.Increment(ref acquired) == workers.Length)
            {
                allAcquired.SetResult();
            }

            await release.Task.WaitAsync(TestContext.Current.CancellationToken);
            Assert.Equal(bytes, lease.Memory.ToArray());
        }
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
