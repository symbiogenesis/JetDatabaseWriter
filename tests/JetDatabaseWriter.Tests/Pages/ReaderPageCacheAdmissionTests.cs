namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Concurrent cache misses, aggregate frame/lease/directory budgets, and public read surface accounting.</summary>
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
    [InlineData(0, 0)]
    [InlineData(-1, 0)]
    [InlineData(0, 1)]
    [InlineData(-1, 1)]
    [InlineData(-1, 2)]
    [InlineData(0, 3)]
    [InlineData(-1, 3)]
    public async Task InvalidResourceOptions_RefuseOpen(int limit, int kind)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, leaveOpen: true, cancellationToken: ct))
        {
            Assert.True(stream.Length > 0);
        }

        AccessReaderOptions readerOptions = kind switch
        {
            0 => new AccessReaderOptions { MaxActivePageLeases = limit },
            1 => new AccessReaderOptions { MaxIncludeMaterializedRowsPerOperation = limit },
            2 => new AccessReaderOptions { MaxLivePageFrames = limit },
            _ => new AccessReaderOptions { MaxCachedRowDirectoryEntries = limit },
        };
        stream.Position = 0;
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await AccessReader.OpenAsync(stream, readerOptions, leaveOpen: true, cancellationToken: ct));
        Assert.True(stream.CanRead);

        if (kind is 0 or 2 or 3)
        {
            AccessWriterOptions writerOptions = kind switch
            {
                0 => new AccessWriterOptions { MaxActivePageLeases = limit },
                2 => new AccessWriterOptions { MaxLivePageFrames = limit },
                _ => new AccessWriterOptions { MaxCachedRowDirectoryEntries = limit },
            };
            stream.Position = 0;
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
                await AccessWriter.OpenAsync(stream, writerOptions, leaveOpen: true, cancellationToken: ct));
        }
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
        Assert.Equal(1, cache.LiveFrames);
        firstLease.Dispose();
        secondLease.Dispose();
        Assert.Equal(0, cache.OutstandingLeases);
        Assert.Equal(capacity, cache.RetainedFrames);
        Assert.Equal(0, cache.EvictedPinnedFrames);
        Assert.Equal(capacity, cache.LiveFrames);
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
        Assert.Equal(0, cache.Statistics.ActiveLoadGates);
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
        Assert.Equal(0, cache.LiveFrames);
    }

    [Fact]
    public async Task MaxLivePageFrames_EvictsUnpinnedFramesAndRefusesWhenAllPinned()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new ControlledPageSource(format.PageSize);
        source.Complete();
        using var cache = new ReaderPageCache(format, source, capacity: 2, maxActiveLeases: 4, maxLiveFrames: 2);

        using (PageLease first = await cache.ReadPageAsync(1, ct))
        {
            Assert.Equal(1, cache.RetainedFrames);
            Assert.Equal(0, cache.EvictedPinnedFrames);
            Assert.Equal(1, cache.LiveFrames);
        }

        using PageLease pinnedSecond = await cache.ReadPageAsync(2, ct);
        Assert.Equal(2, cache.RetainedFrames);
        Assert.Equal(0, cache.EvictedPinnedFrames);
        Assert.Equal(2, cache.LiveFrames);

        using PageLease pinnedThird = await cache.ReadPageAsync(3, ct);
        Assert.Equal(2, cache.RetainedFrames);
        Assert.Equal(0, cache.EvictedPinnedFrames);
        Assert.Equal(2, cache.LiveFrames);
        Assert.Equal(2, cache.OutstandingLeases);

        JetLimitationException exhausted = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await cache.ReadPageAsync(1, ct));
        Assert.Equal(JetErrorCode.PageLeaseBudgetExceeded, exhausted.ErrorCode);
        Assert.Equal(2, cache.LiveFrames);
        Assert.Equal(2, cache.OutstandingLeases);

        pinnedSecond.Dispose();
        Assert.Equal(1, cache.OutstandingLeases);

        using PageLease reloadedFirst = await cache.ReadPageAsync(1, ct);
        Assert.Equal(2, cache.RetainedFrames);
        Assert.Equal(0, cache.EvictedPinnedFrames);
        Assert.Equal(2, cache.LiveFrames);

        reloadedFirst.Dispose();
        pinnedThird.Dispose();
        Assert.Equal(0, cache.OutstandingLeases);
        Assert.Equal(0, cache.EvictedPinnedFrames);
        Assert.Equal(2, cache.LiveFrames);
    }

    [Fact]
    public async Task EvictedPinnedFrames_CountAgainstMaxLiveFramesUntilDisposed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new ControlledPageSource(format.PageSize);
        source.Complete();
        using var cache = new ReaderPageCache(format, source, capacity: 1, maxActiveLeases: 4, maxLiveFrames: 2);

        using PageLease first = await cache.ReadPageAsync(1, ct);
        Assert.Equal(1, cache.RetainedFrames);
        Assert.Equal(0, cache.EvictedPinnedFrames);
        Assert.Equal(1, cache.LiveFrames);

        using PageLease second = await cache.ReadPageAsync(2, ct);
        Assert.Equal(1, cache.RetainedFrames);
        Assert.Equal(1, cache.EvictedPinnedFrames);
        Assert.Equal(2, cache.LiveFrames);

        JetLimitationException exhausted = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await cache.ReadPageAsync(3, ct));
        Assert.Equal(JetErrorCode.PageLeaseBudgetExceeded, exhausted.ErrorCode);

        first.Dispose();
        Assert.Equal(1, cache.RetainedFrames);
        Assert.Equal(0, cache.EvictedPinnedFrames);
        Assert.Equal(1, cache.LiveFrames);

        using PageLease third = await cache.ReadPageAsync(3, ct);
        Assert.Equal(1, cache.RetainedFrames);
        Assert.Equal(1, cache.EvictedPinnedFrames);
        Assert.Equal(2, cache.LiveFrames);

        second.Dispose();
        third.Dispose();
        Assert.Equal(1, cache.RetainedFrames);
        Assert.Equal(0, cache.EvictedPinnedFrames);
        Assert.Equal(1, cache.LiveFrames);
        Assert.Equal(0, cache.OutstandingLeases);
    }

    [Fact]
    public async Task MaxCachedRowDirectoryEntries_BoundsRetainedDirectoriesAndReleasesOnEviction()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new ControlledPageSource(format.PageSize) { RowCountPerPage = 3 };
        source.Complete();
        using var cache = new ReaderPageCache(format, source, capacity: 2, maxActiveLeases: 4, maxLiveFrames: 4, maxCachedRowDirectoryEntries: 4);

        using (PageLease first = await cache.ReadPageAsync(1, ct))
        {
            RowBound[] dir1 = first.GetRowDirectory(format);
            Assert.Equal(3, dir1.Length);
            Assert.Same(dir1, first.GetRowDirectory(format));
            Assert.Equal(3, cache.CachedRowDirectoryEntries);
        }

        using (PageLease second = await cache.ReadPageAsync(2, ct))
        {
            RowBound[] dir2 = second.GetRowDirectory(format);
            Assert.Equal(3, dir2.Length);
            Assert.NotSame(dir2, second.GetRowDirectory(format));
            Assert.Equal(3, cache.CachedRowDirectoryEntries);
        }

        cache.Dispose();
        Assert.Equal(0, cache.CachedRowDirectoryEntries);
        Assert.Equal(0, cache.LiveFrames);
    }

    [Fact]
    public async Task PublicReadSurfaces_ReleaseLeasesAndPinnedFrames_AcrossConcurrentCancelledAndAbandonedReads()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, leaveOpen: true, cancellationToken: ct))
        {
            await writer.CreateTableAsync(
                "Items",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Code", typeof(string), 40),
                    new ColumnDefinition("Body", typeof(string), 2000),
                ],
                [new IndexDefinition("IX_Items_Code", "Code")],
                ct);

            var rows = new List<object[]>(48);
            string largeMemo = new('M', 2400);
            for (int i = 1; i <= 48; i++)
            {
                rows.Add([i, FormattableString.Invariant($"code-{i:D3}"), largeMemo]);
            }

            await writer.InsertRowsAsync("Items", rows, ct);
        }

        stream.Position = 0;
        var options = new AccessReaderOptions
        {
            UseLockFile = false,
            PageCacheSize = 4,
            MaxActivePageLeases = 16,
            MaxLivePageFrames = 20,
            MaxCachedRowDirectoryEntries = 64,
        };
        await using AccessReader reader = await AccessReader.OpenAsync(stream, options, leaveOpen: true, cancellationToken: ct);
        ReaderPageCache cache = reader.Services.PageCache;

        Task<DataTable>[] concurrentScans =
        [
            reader.ReadTableAsync("Items", cancellationToken: ct).AsTask(),
            reader.ReadTableAsync("Items", cancellationToken: ct).AsTask(),
            reader.ReadTableAsync("Items", cancellationToken: ct).AsTask(),
        ];
        DataTable[] tables = await Task.WhenAll(concurrentScans);
        foreach (DataTable table in tables)
        {
            Assert.Equal(48, table.Rows.Count);
            table.Dispose();
        }

        await foreach (object[] row in reader.Rows("Items", cancellationToken: ct))
        {
            Assert.Equal(1, row[0]);
            break;
        }

        using (var cts = new CancellationTokenSource())
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                await foreach (object[] row in reader.Rows("Items", cancellationToken: cts.Token))
                {
                    Assert.NotNull(row);
                    await cts.CancelAsync();
                }
            });
        }

        await foreach (object[] row in reader.SeekRowsAsync("Items", "IX_Items_Code", ["code-010"], ct))
        {
            Assert.Equal(10, row[0]);
            break;
        }

        PageCacheStatistics stats = cache.Statistics;
        Assert.Equal(0, stats.OutstandingLeases);
        Assert.Equal(0, stats.EvictedPinnedFrames);
        Assert.Equal(0, stats.ActiveLoadGates);
        Assert.Equal(0, stats.InFlightStoreReads);
        Assert.Equal(0, reader.Database.Pages.OutstandingLeases);
        Assert.Equal(0, reader.Database.Pages.InFlightReads);
        Assert.Equal(stats.RetainedFrames, stats.LiveFrames);
        Assert.InRange(stats.LiveFrames, 0, options.PageCacheSize);
        Assert.InRange(stats.CachedRowDirectoryEntries, 0, options.MaxCachedRowDirectoryEntries);
    }

    private sealed class ControlledPageSource(int size) : IPageSource
    {
        private readonly TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int reads;

        public int PageSize => size;

        public long PageCount => 4;

        public bool IsDisposed => false;

        internal int RowCountPerPage { get; init; }

        internal int ReadCount => Volatile.Read(ref this.reads);

        internal void Complete() => this.completed.TrySetResult();

        public async ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            Assert.InRange(pageNumber, 1, 3);
            Interlocked.Increment(ref this.reads);
            await this.completed.Task.WaitAsync(cancellationToken);
            byte[] page = ArrayPool<byte>.Shared.Rent(size);
            Array.Clear(page, 0, size);
            if (this.RowCountPerPage > 0)
            {
                page[0] = Constants.PageTypes.Data;
                page[1] = 1;
                BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(12, 2), (ushort)this.RowCountPerPage);
                int rowEnd = size;
                for (int slot = 0; slot < this.RowCountPerPage; slot++)
                {
                    rowEnd -= 8;
                    BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(14 + (slot * 2), 2), (ushort)rowEnd);
                }
            }

            return page;
        }

        public void ThrowIfDisposed()
        {
        }

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken)
            => cancellationToken.ThrowIfCancellationRequested();
    }
}
