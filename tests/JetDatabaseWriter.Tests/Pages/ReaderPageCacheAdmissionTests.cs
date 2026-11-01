namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Concurrent cache misses share a load without sharing caller cancellation.</summary>
public sealed class ReaderPageCacheAdmissionTests
{
    [Theory]
    [InlineData(0U, 0U, false, 0U)]
    [InlineData(0U, 1U, true, 1U)]
    [InlineData(1U, 1U, false, 1U)]
    [InlineData(uint.MaxValue - 1, uint.MaxValue, true, uint.MaxValue)]
    [InlineData(uint.MaxValue, uint.MaxValue, false, uint.MaxValue)]
    public void GeneratedReservation_PreservesNativeUnsignedBoundaries(uint current, uint limit, bool expectedAdmission, uint expectedNext)
    {
        DataPageKernel.TryReserveCount(current, limit, out bool admitted, out uint next);
        Assert.Equal(expectedAdmission, admitted);
        Assert.Equal(expectedNext, next);
    }

    [Theory]
    [InlineData(0, true)]
    [InlineData(-1, true)]
    [InlineData(0, false)]
    [InlineData(-1, false)]
    public async Task NonpositiveResourceOptions_RefuseOpen(int limit, bool pages)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.True(stream.Length > 0);
        }

        AccessReaderOptions options = pages
            ? new AccessReaderOptions { MaxActivePageLeases = limit }
            : new AccessReaderOptions { MaxIncludeMaterializedRowsPerOperation = limit };
        stream.Position = 0;
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await AccessReader.OpenAsync(stream, options, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken));
        Assert.True(stream.CanRead);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public async Task ConcurrentSamePageMisses_ShareOneFrameAndOneSourceRead(int capacity)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new ControlledPageSource(format.PageSize);
        using var cache = new ReaderPageCache(format, source, capacity);
        Task<PageLease> first = cache.ReadPageAsync(1, TestContext.Current.CancellationToken).AsTask();
        Task<PageLease> second = cache.ReadPageAsync(1, TestContext.Current.CancellationToken).AsTask();
        source.Complete();
        using PageLease firstLease = await first;
        using PageLease secondLease = await second;
        Assert.Equal(1, source.ReadCount);
        Assert.Same(firstLease.Buffer, secondLease.Buffer);
        Assert.Equal(2, cache.OutstandingLeases);
        firstLease.Dispose();
        secondLease.Dispose();
        Assert.Equal(0, cache.OutstandingLeases);
    }

    [Fact]
    public async Task CancelledProducer_DoesNotCancelAnotherCallerOrRetainItsAdmission()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new ControlledPageSource(format.PageSize);
        using var cache = new ReaderPageCache(format, source, capacity: 1);
        using var cancelled = new CancellationTokenSource();
        Task<PageLease> first = cache.ReadPageAsync(1, cancelled.Token).AsTask();
        Task<PageLease> second = cache.ReadPageAsync(1, TestContext.Current.CancellationToken).AsTask();
        await cancelled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await first);
        source.Complete();
        using PageLease lease = await second;
        Assert.Equal(2, source.ReadCount);
        Assert.Equal(1, cache.OutstandingLeases);
        lease.Dispose();
        Assert.Equal(0, cache.OutstandingLeases);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public async Task PendingAndLiveLeases_ShareImmediateAdmissionAndReleaseIt(int capacity)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new ControlledPageSource(format.PageSize);
        using var cache = new ReaderPageCache(format, source, capacity, maxActiveLeases: 1);
        Task<PageLease> pending = cache.ReadPageAsync(1, TestContext.Current.CancellationToken).AsTask();
        JetLimitationException pendingFailure = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await cache.ReadPageAsync(2, TestContext.Current.CancellationToken));
        Assert.Equal(JetErrorCode.PageLeaseBudgetExceeded, pendingFailure.ErrorCode);
        Assert.Equal(1, source.ReadCount);
        source.Complete();
        using PageLease lease = await pending;
        JetLimitationException liveFailure = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await cache.ReadPageAsync(1, TestContext.Current.CancellationToken));
        Assert.Equal(JetErrorCode.PageLeaseBudgetExceeded, liveFailure.ErrorCode);
        Assert.Equal(1, cache.OutstandingLeases);
        lease.Dispose();
        using PageLease retry = await cache.ReadPageAsync(2, TestContext.Current.CancellationToken);
        Assert.Equal(1, cache.OutstandingLeases);
        retry.Dispose();
        Assert.Equal(0, cache.OutstandingLeases);
    }

    [Fact]
    public async Task DisposalDuringLoad_RefusesPublicationAndReleasesAdmission()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new ControlledPageSource(format.PageSize);
        using var cache = new ReaderPageCache(format, source, capacity: 1);
        Task<PageLease> pending = cache.ReadPageAsync(1, TestContext.Current.CancellationToken).AsTask();
        cache.Dispose();
        source.Complete();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await pending);
        Assert.Equal(0, cache.OutstandingLeases);
    }

    private sealed class ControlledPageSource(int size) : IPageSource
    {
        private readonly TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int reads;

        public int PageSize => size;

        public long PageCount => 4;

        public bool IsDisposed => false;

        internal int ReadCount => Volatile.Read(ref this.reads);

        internal void Complete() => this.completed.TrySetResult();

        public async ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            Assert.InRange(pageNumber, 1, 3);
            Interlocked.Increment(ref this.reads);
            await this.completed.Task.WaitAsync(cancellationToken);
            return ArrayPool<byte>.Shared.Rent(size);
        }

        public void ThrowIfDisposed()
        {
        }

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken)
            => cancellationToken.ThrowIfCancellationRequested();
    }
}
