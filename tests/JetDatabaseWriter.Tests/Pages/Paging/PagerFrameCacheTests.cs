namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using Xunit;

/// <summary>Checks owned reads, write-through coherence, invalidation, and aggregate frame/lease accounting.</summary>
public sealed class PagerFrameCacheTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Readers share immutable images while writes, reservations and invalidation replace cache ownership.</summary>
    [Fact]
    public async Task Leases_ShareHitsAndRetainOldImagesAcrossMutationAndDisposal()
    {
        await using var stream = new MemoryStream(new byte[48], writable: true);
#pragma warning disable CA2000 // The awaited pager owns and disposes the page codec.
        await using var pager = new Pager(stream, 16, new NoPageCodec(), leaveOpen: true, typeof(AccessWriter), cacheSize: 1);
#pragma warning restore CA2000
        using PageLease original = await pager.ReadPageLeaseAsync(1, Ct);
        using PageLease shared = await pager.ReadPageLeaseAsync(1, Ct);
        Assert.Same(original.Buffer, shared.Buffer);
        byte[] update = new byte[16];
        update[0] = 37;
        await pager.WritePageAsync(1, update, Ct);
        update[0] = 99;
        using PageLease changed = await pager.ReadPageLeaseAsync(1, Ct);
        Assert.Equal(0, original.Memory.Span[0]);
        Assert.Equal(37, changed.Memory.Span[0]);
        Assert.NotSame(original.Buffer, changed.Buffer);
        Assert.Equal(1, await pager.ReserveZeroedPageAsync(Ct, 1));
        using PageLease reserved = await pager.ReadPageLeaseAsync(1, Ct);
        Assert.Equal(0, reserved.Memory.Span[0]);
        Assert.Equal(37, changed.Memory.Span[0]);
        pager.InvalidateAll();
        await pager.DisposeAsync();
        Assert.Equal(0, original.Memory.Span[0]);
        Assert.Equal(37, changed.Memory.Span[0]);
    }

    /// <summary>Mutating a returned page never mutates the cache; writes replace it.</summary>
    [Fact]
    public async Task Cache_OwnedCopies_WriteThrough_AndTruncate()
    {
        await using var stream = new MemoryStream(new byte[48], writable: true);
#pragma warning disable CA2000 // The awaited pager owns and disposes the page codec.
        await using var pager = new Pager(stream, 16, new NoPageCodec(), leaveOpen: true, typeof(AccessWriter), cacheSize: 2);
#pragma warning restore CA2000
        byte[] first = await pager.ReadPageAsync(1, Ct);
        first[0] = 91;
        PageBuffers.Return(first);
        byte[] second = await pager.ReadPageAsync(1, Ct);
        Assert.Equal(0, second[0]);
        PageBuffers.Return(second);
        Assert.Equal(1, pager.Statistics.StoreReads);
        byte[] replacement = new byte[16];
        replacement[0] = 37;
        await pager.WritePageAsync(1, replacement, Ct);
        replacement[0] = 99;
        byte[] third = await pager.ReadPageAsync(1, Ct);
        Assert.Equal(37, third[0]);
        PageBuffers.Return(third);
        await pager.TruncateAsync(1, Ct);
        Assert.Equal(1, pager.PageCount);
        Assert.Equal(0, pager.Statistics.CachedFrames);
    }

    /// <summary>Capacity zero always reads the store.</summary>
    [Fact]
    public async Task ZeroCapacity_DoesNotCache()
    {
        await using var stream = new MemoryStream(new byte[32]);
#pragma warning disable CA2000 // The awaited pager owns and disposes the page codec.
        await using var pager = new Pager(stream, 16, new NoPageCodec(), leaveOpen: true, typeof(AccessWriter), cacheSize: 0);
#pragma warning restore CA2000
        PageBuffers.Return(await pager.ReadPageAsync(1, Ct));
        PageBuffers.Return(await pager.ReadPageAsync(1, Ct));
        Assert.Equal(2, pager.Statistics.StoreReads);
    }

    /// <summary>A delayed old read cannot refill the cache after invalidation.</summary>
    [Fact]
    public async Task DelayedLoad_AfterInvalidation_IsNotRetained()
    {
#pragma warning disable CA2000 // The awaited pager owns this store and codec and disposes them on every exit.
        var store = new ControlledStore(32) { HoldRead = true };
        await using var pager = new Pager(store, 16, new NoPageCodec(), typeof(AccessWriter), cacheSize: 2);
#pragma warning restore CA2000
        Task<byte[]> first = pager.ReadPageAsync(1, Ct).AsTask();
        await store.ReadCaptured.Task;
        pager.InvalidateAll();
        await store.WriteAsync(16, "%"u8.ToArray(), TestContext.Current.CancellationToken);
        store.ReleaseRead.SetResult();
        PageBuffers.Return(await first);
        byte[] second = await pager.ReadPageAsync(1, Ct);
        Assert.Equal(37, second[0]);
        PageBuffers.Return(second);
        Assert.Equal(2, store.Reads);
    }

    /// <summary>An invalidated in-flight lease load remains readable without retaining the stale image.</summary>
    [Fact]
    public async Task DelayedLeaseLoad_AfterInvalidation_IsNotRetainedAndSurvivesDisposal()
    {
#pragma warning disable CA2000 // The awaited pager owns this store and codec and disposes them on every exit.
        var store = new ControlledStore(32) { HoldRead = true };
        await using var pager = new Pager(store, 16, new NoPageCodec(), typeof(AccessWriter), cacheSize: 2);
#pragma warning restore CA2000
        byte[] original = new byte[16];
        Array.Fill(original, (byte)17);
        await store.WriteAsync(16, original, Ct);
        Task<PageLease> pending = pager.ReadPageLeaseAsync(1, Ct).AsTask();
        await store.ReadCaptured.Task;
        pager.InvalidateAll();
        byte[] replacement = new byte[16];
        Array.Fill(replacement, (byte)37);
        await store.WriteAsync(16, replacement, Ct);
        store.ReleaseRead.SetResult();

        using PageLease stale = await pending;
        Assert.Equal(original, stale.Memory.ToArray());
        Assert.Equal(0, pager.Statistics.CachedFrames);
        Assert.Equal(1, pager.Statistics.StoreReads);

        using PageLease current = await pager.ReadPageLeaseAsync(1, Ct);
        Assert.Equal(replacement, current.Memory.ToArray());
        Assert.NotSame(stale.Buffer, current.Buffer);
        Assert.Equal(original, stale.Memory.ToArray());
        Assert.Equal(1, pager.Statistics.CachedFrames);
        Assert.Equal(2, pager.Statistics.StoreReads);
        using PageLease shared = await pager.ReadPageLeaseAsync(1, Ct);
        Assert.Same(current.Buffer, shared.Buffer);
        Assert.Equal(2, pager.Statistics.StoreReads);

        await pager.DisposeAsync();
        Assert.Equal(original, stale.Memory.ToArray());
        Assert.Equal(replacement, current.Memory.ToArray());
        Assert.Equal(replacement, shared.Memory.ToArray());
    }

    /// <summary>Concurrent misses share one load and return independently owned buffers.</summary>
    [Fact]
    public async Task ConcurrentReads_AreSingleFlight()
    {
#pragma warning disable CA2000 // The awaited pager owns this store and codec and disposes them on every exit.
        var store = new ControlledStore(32) { HoldRead = true };
        await using var pager = new Pager(store, 16, new NoPageCodec(), typeof(AccessWriter), cacheSize: 2);
#pragma warning restore CA2000
        Task<byte[]> first = pager.ReadPageAsync(1, Ct).AsTask();
        await store.ReadCaptured.Task;
        Task<byte[]> second = pager.ReadPageAsync(1, Ct).AsTask();
        store.ReleaseRead.SetResult();
        byte[] firstPage = await first;
        byte[] secondPage = await second;
        Assert.NotSame(firstPage, secondPage);
        Assert.Equal(1, store.Reads);
        PageBuffers.Return(firstPage);
        PageBuffers.Return(secondPage);
    }

    /// <summary>Concurrent appends reserve distinct page numbers before writing.</summary>
    [Fact]
    public async Task ConcurrentAppends_DoNotOverwriteEachOther()
    {
#pragma warning disable CA2000 // The awaited pager owns this store and codec and disposes them on every exit.
        var store = new ControlledStore(16) { HoldWrite = true };
        await using var pager = new Pager(store, 16, new NoPageCodec(), typeof(AccessWriter), cacheSize: 2);
#pragma warning restore CA2000
        byte[] firstPage = new byte[16];
        byte[] secondPage = new byte[16];
        firstPage[0] = 17;
        secondPage[0] = 29;
        Task<long> first = pager.AppendPageAsync(firstPage, Ct).AsTask();
        await store.WriteCaptured.Task;
        Task<long> second = pager.AppendPageAsync(secondPage, Ct).AsTask();
        Assert.False(second.IsCompleted);
        store.ReleaseWrite.SetResult();
        Assert.Equal(1, await first);
        Assert.Equal(2, await second);
        Assert.Equal(3, pager.PageCount);
        Assert.Equal(firstPage, await pager.ReadPageCopyAsync(1, Ct));
        Assert.Equal(secondPage, await pager.ReadPageCopyAsync(2, Ct));
    }

    /// <summary>Journal attachment waits for physical appends before capturing their length.</summary>
    [Fact]
    public async Task JournalGate_WaitsForPhysicalAppend()
    {
#pragma warning disable CA2000 // The awaited pager owns this store and codec and disposes them on every exit.
        var store = new ControlledStore(16) { HoldWrite = true };
        await using var pager = new Pager(store, 16, new NoPageCodec(), typeof(AccessWriter), cacheSize: 2);
#pragma warning restore CA2000
        Task<long> append = pager.AppendPageAsync(new byte[16], Ct).AsTask();
        await store.WriteCaptured.Task;
        Task<Pager.JournalGate> entering = pager.EnterJournalGateAsync(TestContext.Current.CancellationToken).AsTask();
        Assert.False(entering.IsCompleted);
        store.ReleaseWrite.SetResult();
        Assert.Equal(1, await append);
        using Pager.JournalGate gate = await entering;
        Assert.Equal(32, gate.PhysicalLengthBytes);
    }

    /// <summary>CLOCK eviction bounds frames; NoCache scans never displace them.</summary>
    [Fact]
    public async Task Clock_BoundsFrames_AndNoCachePreservesHotPages()
    {
#pragma warning disable CA2000 // The awaited pager owns this store and codec and disposes them on every exit.
        var store = new ControlledStore(80);
        await using var pager = new Pager(store, 16, new NoPageCodec(), typeof(AccessWriter), cacheSize: 2);
#pragma warning restore CA2000
        PageBuffers.Return(await pager.ReadPageAsync(1, Ct));
        PageBuffers.Return(await pager.ReadPageAsync(2, Ct));
        PageBuffers.Return(await pager.ReadPageAsync(3, PageReadHint.NoCache, Ct));
        PageBuffers.Return(await pager.ReadPageAsync(1, Ct));
        Assert.Equal(3, store.Reads);
        PageBuffers.Return(await pager.ReadPageAsync(4, Ct));
        Assert.Equal(2, pager.Statistics.CachedFrames);
        Assert.Equal(1, pager.Statistics.Evictions);
    }

    /// <summary>Readers cannot retain an old page while a truncate is pending.</summary>
    [Fact]
    public async Task DelayedTruncate_BlocksCacheFill_AndDropsFrames()
    {
#pragma warning disable CA2000 // The awaited pager owns this store and codec and disposes them on every exit.
        var store = new ControlledStore(48) { HoldResize = true };
        await using var pager = new Pager(store, 16, new NoPageCodec(), typeof(AccessWriter), cacheSize: 2);
#pragma warning restore CA2000
        PageBuffers.Return(await pager.ReadPageAsync(2, Ct));
        Task truncate = pager.TruncateAsync(1, Ct).AsTask();
        await store.ResizeCaptured.Task;
        Task<byte[]> read = pager.ReadPageAsync(2, Ct).AsTask();
        Assert.False(read.IsCompleted);
        store.ReleaseResize.SetResult();
        await truncate;
        _ = await Assert.ThrowsAsync<EndOfStreamException>(async () => await read);
        Assert.Equal(0, pager.Statistics.CachedFrames);
    }

    /// <summary>A failed physical write evicts prior frames before the next read.</summary>
    [Fact]
    public async Task FailedWrite_DiscardsFrames()
    {
#pragma warning disable CA2000 // The awaited pager owns this store and codec and disposes them on every exit.
        var store = new ControlledStore(32);
        await using var pager = new Pager(store, 16, new NoPageCodec(), typeof(AccessWriter), cacheSize: 2);
#pragma warning restore CA2000
        PageBuffers.Return(await pager.ReadPageAsync(1, Ct));
        store.FailWrite = true;
        _ = await Assert.ThrowsAsync<IOException>(async () => await pager.WritePageAsync(1, new byte[16], Ct));
        Assert.Equal(0, pager.Statistics.CachedFrames);
        PageBuffers.Return(await pager.ReadPageAsync(1, Ct));
        Assert.Equal(2, store.Reads);
    }

    /// <summary>Encrypted pages are decoded once while resident and writes preserve their input.</summary>
    /// <param name="scheme">The page-encryption scheme.</param>
    [Theory]
    [InlineData("rc4")]
    [InlineData("jet3")]
    public async Task EncryptedFrame_DecodesOnce_AndWriteUsesScratch(string scheme)
    {
#pragma warning disable CA2000 // Cipher ownership flows through CountingCodec into the awaited pager; the pager also owns the store.
        Jet4Rc4PageCodec cipher = scheme switch
        {
            "rc4" => new Jet4Rc4PageCodec(0x12345678),
            _ => new Jet4Rc4PageCodec(0xA7E0C0FE),
        };
        var codec = new CountingCodec(cipher);
        var store = new ControlledStore(32);
        await using var pager = new Pager(store, 16, codec, typeof(AccessWriter), cacheSize: 2);
#pragma warning restore CA2000
        byte[] plaintext = new byte[16];
        plaintext[0] = 73;
        byte[] ciphertext = (byte[])plaintext.Clone();
        cipher.Encode(ciphertext, 0, 1, 16);
        await store.WriteAsync(16, ciphertext, TestContext.Current.CancellationToken);
        Assert.Equal(plaintext, await pager.ReadPageCopyAsync(1, Ct));
        Assert.Equal(plaintext, await pager.ReadPageCopyAsync(1, Ct));
        Assert.Equal(1, codec.Decodes);
        plaintext[0] = 91;
        await pager.WritePageAsync(1, plaintext, Ct);
        Assert.Equal(91, plaintext[0]);
        Assert.Equal(plaintext, await pager.ReadPageCopyAsync(1, Ct));
        Assert.Equal(1, codec.Decodes);
    }

    /// <summary>Pager enforces MaxLiveFrames by evicting unpinned CLOCK frames and refusing when all live frames are pinned.</summary>
    [Fact]
    public async Task MaxLiveFrames_EvictsUnpinnedClockFrames_AndRefusesWhenAllPinned()
    {
#pragma warning disable CA2000 // The awaited pager owns this store and codec and disposes them on every exit.
        var store = new ControlledStore(80);
        await using var pager = new Pager(store, 16, new NoPageCodec(), typeof(AccessWriter), cacheSize: 2, maxActiveLeases: 4, maxLiveFrames: 2);
#pragma warning restore CA2000
        using (PageLease first = await pager.ReadPageLeaseAsync(1, Ct))
        {
            Assert.Equal(1, pager.Statistics.CachedFrames);
            Assert.Equal(0, pager.Statistics.EvictedPinnedFrames);
            Assert.Equal(1, pager.Statistics.LiveFrames);
        }

        using PageLease pinnedSecond = await pager.ReadPageLeaseAsync(2, Ct);
        Assert.Equal(2, pager.Statistics.CachedFrames);
        Assert.Equal(2, pager.Statistics.LiveFrames);

        using PageLease pinnedThird = await pager.ReadPageLeaseAsync(3, Ct);
        Assert.Equal(2, pager.Statistics.CachedFrames);
        Assert.Equal(0, pager.Statistics.EvictedPinnedFrames);
        Assert.Equal(2, pager.Statistics.LiveFrames);
        Assert.Equal(2, pager.Statistics.OutstandingLeases);

        JetLimitationException exhausted = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await pager.ReadPageLeaseAsync(1, Ct));
        Assert.Equal(JetErrorCode.PageLeaseBudgetExceeded, exhausted.ErrorCode);

        byte[] update = new byte[16];
        update[0] = 42;
        await pager.WritePageAsync(2, update, Ct);
        Assert.Equal(1, pager.Statistics.CachedFrames);
        Assert.Equal(1, pager.Statistics.EvictedPinnedFrames);
        Assert.Equal(2, pager.Statistics.LiveFrames);

        JetLimitationException stillPinned = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await pager.ReadPageLeaseAsync(2, Ct));
        Assert.Equal(JetErrorCode.PageLeaseBudgetExceeded, stillPinned.ErrorCode);

        pinnedSecond.Dispose();
        Assert.Equal(1, pager.Statistics.CachedFrames);
        Assert.Equal(0, pager.Statistics.EvictedPinnedFrames);
        Assert.Equal(1, pager.Statistics.LiveFrames);

        using PageLease updatedSecond = await pager.ReadPageLeaseAsync(2, Ct);
        Assert.Equal(42, updatedSecond.Memory.Span[0]);
        Assert.Equal(2, pager.Statistics.CachedFrames);
        Assert.Equal(2, pager.Statistics.LiveFrames);

        updatedSecond.Dispose();
        pinnedThird.Dispose();
        Assert.Equal(0, pager.Statistics.OutstandingLeases);
        Assert.Equal(0, pager.Statistics.EvictedPinnedFrames);
    }

    /// <summary>Direct read-only PageFile tracks in-flight reads, leases, and live frames within configured budgets.</summary>
    [Fact]
    public async Task DirectPageFile_TracksInFlightReadsLeasesAndLiveFrames()
    {
#pragma warning disable CA2000 // The awaited PageFile owns this store and codec and disposes them on every exit.
        var store = new ControlledStore(48) { HoldRead = true };
        await using var pageFile = new PageFile(store, 16, new NoPageCodec(), typeof(AccessReader), maxActiveLeases: 1, maxLiveFrames: 1, maxCachedRowDirectoryEntries: 4);
#pragma warning restore CA2000
        Task<byte[]> inFlight = pageFile.ReadPageAsync(1, Ct).AsTask();
        await store.ReadCaptured.Task;
        Assert.Equal(1, pageFile.InFlightReads);

        JetLimitationException inFlightLimit = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await pageFile.ReadPageAsync(2, Ct));
        Assert.Equal(JetErrorCode.PageLeaseBudgetExceeded, inFlightLimit.ErrorCode);

        store.ReleaseRead.SetResult();
        PageBuffers.Return(await inFlight);
        Assert.Equal(0, pageFile.InFlightReads);

        using PageLease lease = await pageFile.ReadPageLeaseAsync(1, Ct);
        Assert.Equal(1, pageFile.OutstandingLeases);
        Assert.Equal(1, pageFile.LiveFrames);

        JetLimitationException leaseLimit = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await pageFile.ReadPageLeaseAsync(2, Ct));
        Assert.Equal(JetErrorCode.PageLeaseBudgetExceeded, leaseLimit.ErrorCode);

        lease.Dispose();
        Assert.Equal(0, pageFile.OutstandingLeases);
        Assert.Equal(0, pageFile.LiveFrames);
    }

    /// <summary>Writer nested and outer transaction rollbacks invalidate uncommitted frames and release all leases across abandoned reads.</summary>
    [Fact]
    public async Task WriterRollbackAndAbandonedReads_ReleaseLeasesAndInvalidateUncommittedFrames()
    {
        await using var stream = new MemoryStream();
        var options = new AccessWriterOptions
        {
            UseLockFile = false,
            PageCacheSize = 4,
            MaxActivePageLeases = 16,
            MaxLivePageFrames = 20,
            MaxCachedRowDirectoryEntries = 64,
        };
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, options, leaveOpen: true, cancellationToken: Ct);
        await writer.CreateTableAsync(
            "Ledger",
            [
                new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                new ColumnDefinition("Tag", typeof(string), 32),
                new ColumnDefinition("Notes", typeof(string), 2000),
            ],
            [new IndexDefinition("IX_Ledger_Tag", "Tag")],
            Ct);

        string memo = new('N', 2200);
        await writer.InsertRowsAsync(
            "Ledger",
            [
                [1, "alpha", memo],
                [2, "beta", memo],
            ],
            Ct);

        CatalogEntry? entry = await writer.Services.Catalog.GetCatalogEntryAsync("Ledger", Ct);
        Assert.NotNull(entry);

        await using (JetTransaction outerTx = await writer.BeginTransactionAsync(Ct))
        {
            await writer.InsertRowAsync("Ledger", [3, "gamma", memo], Ct);
            await using (JetTransaction childTx = await writer.BeginTransactionAsync(Ct))
            {
                await writer.InsertRowAsync("Ledger", [4, "delta", memo], Ct);

                int visited = 0;
                await writer.Services.Snapshots.ForEachDecodedRowAsync(
                    entry.TDefPage,
                    (row, _) =>
                    {
                        Assert.NotNull(row.Values);
                        visited++;
                        return ValueTask.FromResult(false);
                    },
                    Ct);
                Assert.Equal(1, visited);

                await childTx.RollbackAsync(Ct);
            }

            Assert.Equal(3, (await writer.Services.Snapshots.ReadRowsAsync(entry.TDefPage, Ct)).Count);
            await outerTx.RollbackAsync(Ct);
        }

        Assert.Equal(2, (await writer.Services.Snapshots.ReadRowsAsync(entry.TDefPage, Ct)).Count);

        PagerStatistics stats = writer.Pager.Statistics;
        ReaderPageCache snapshotCache = writer.Services.SnapshotPages;
        Assert.Equal(0, stats.OutstandingLeases);
        Assert.Equal(0, stats.EvictedPinnedFrames);
        Assert.Equal(0, stats.InFlightStoreReads);
        Assert.Equal(0, stats.DirtyBufferedPages);
        Assert.Equal(0, snapshotCache.OutstandingLeases);
        Assert.Equal(0, snapshotCache.EvictedPinnedFrames);
        Assert.Equal(0, snapshotCache.Statistics.ActiveLoadGates);
        Assert.InRange(stats.LiveFrames, 0, options.MaxLivePageFrames);
        Assert.InRange(stats.CachedRowDirectoryEntries, 0, options.MaxCachedRowDirectoryEntries);
    }

    private sealed class CountingCodec(IPageCodec inner) : IPageCodec
    {
        /// <inheritdoc/>
        public bool HasEncryption => inner.HasEncryption;

        internal int Decodes { get; private set; }

        /// <inheritdoc/>
        public void Decode(byte[] data, int offset, long pageNumber, int pageSize)
        {
            this.Decodes++;
            inner.Decode(data, offset, pageNumber, pageSize);
        }

        /// <inheritdoc/>
        public void Encode(byte[] data, int offset, long pageNumber, int pageSize) => inner.Encode(data, offset, pageNumber, pageSize);

        /// <inheritdoc/>
        public void Dispose() => inner.Dispose();
    }

    private sealed class ControlledStore : IPageStore
    {
        private readonly MemoryStream image = new();
        private readonly MemoryPageStore inner;

        internal ControlledStore(int length)
        {
            this.image.SetLength(length);
            this.inner = new MemoryPageStore(this.image, leaveOpen: false);
        }

        /// <inheritdoc/>
        public StoreCapabilities Capabilities => this.inner.Capabilities;

        /// <inheritdoc/>
        public long Length => this.inner.Length;

        internal bool HoldRead { get; set; }

        internal bool HoldWrite { get; set; }

        internal bool HoldResize { get; set; }

        internal bool FailWrite { get; set; }

        internal TaskCompletionSource ResizeCaptured { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource ReleaseResize { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal int Reads { get; private set; }

        internal TaskCompletionSource ReadCaptured { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource ReleaseRead { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource WriteCaptured { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource ReleaseWrite { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        public async ValueTask ReadAsync(long offset, Memory<byte> buffer, bool inline, CancellationToken cancellationToken)
        {
            this.Reads++;
            await this.inner.ReadAsync(offset, buffer, inline, cancellationToken);
            if (this.HoldRead)
            {
                this.HoldRead = false;
                this.ReadCaptured.SetResult();
                await this.ReleaseRead.Task.WaitAsync(cancellationToken);
            }
        }

        /// <inheritdoc/>
        public async ValueTask WriteAsync(long offset, ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
        {
            if (this.FailWrite)
            {
                throw new IOException("Injected store write failure.");
            }

            if (this.HoldWrite)
            {
                this.HoldWrite = false;
                this.WriteCaptured.SetResult();
                await this.ReleaseWrite.Task.WaitAsync(cancellationToken);
            }

            await this.inner.WriteAsync(offset, buffer, cancellationToken);
        }

        /// <inheritdoc/>
        public async ValueTask SetLengthAsync(long length, CancellationToken cancellationToken)
        {
            if (this.HoldResize)
            {
                this.HoldResize = false;
                this.ResizeCaptured.SetResult();
                await this.ReleaseResize.Task.WaitAsync(cancellationToken);
            }

            await this.inner.SetLengthAsync(length, cancellationToken);
        }

        /// <inheritdoc/>
        public ValueTask FlushAsync(bool toDisk, CancellationToken cancellationToken) => this.inner.FlushAsync(toDisk, cancellationToken);

        /// <inheritdoc/>
        public ValueTask DisposeAsync() => this.inner.DisposeAsync();

        /// <inheritdoc/>
        public void DisposeManagedResources() => this.inner.DisposeManagedResources();
    }
}
