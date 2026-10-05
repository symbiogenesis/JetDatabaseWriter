namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Pages.Paging;
using Xunit;

/// <summary>Checks deferred writes and owned pending images.</summary>
public sealed class WriteBackTests
{
    /// <summary>Repeated writes coalesce and pending appends are readable.</summary>
    [Fact]
    public async Task Scope_CoalescesWrites_AndExposesAppends()
    {
        await using var stream = new MemoryStream();
        stream.SetLength(32);
#pragma warning disable CA2000 // The awaited pager owns the codec.
        await using var pager = new Pager(stream, 16, new NoPageCodec(), leaveOpen: true, typeof(AccessWriter), cacheSize: 0);
#pragma warning restore CA2000
        await using (WriteScope scope = pager.BeginWriteScope())
        {
            byte[] page = new byte[16];
            page[0] = 17;
            await pager.WritePageAsync(1, page, TestContext.Current.CancellationToken);
            page[0] = 29;
            await pager.WritePageAsync(1, page, TestContext.Current.CancellationToken);
            Assert.Equal(0, stream.ToArray()[16]);
            Assert.Equal(2, await pager.AppendPageAsync(page, TestContext.Current.CancellationToken));
            Assert.Equal(3, pager.PageCount);
            byte[] pending = await pager.ReadPageAsync(2, TestContext.Current.CancellationToken);
            Assert.Equal(29, pending[0]);
            PageBuffers.Return(pending);
        }

        Assert.Equal(29, stream.ToArray()[16]);
        Assert.Equal(48, stream.Length);
    }

    /// <summary>Nested scopes flush once and observers receive sorted distinct writes.</summary>
    [Fact]
    public async Task NestedScope_WritesDistinctPagesAscending_AndFlushesOnce()
    {
        await using var stream = new CountingStream();
        stream.SetLength(64);
#pragma warning disable CA2000 // The awaited pager owns the codec.
        await using var pager = new Pager(stream, 16, new NoPageCodec(), true, typeof(AccessWriter));
#pragma warning restore CA2000
        var observer = new Observer();
        pager.AddWriteObserver(observer);
        await using (WriteScope outer = pager.BeginWriteScope())
        {
            await pager.WritePageAsync(3, new byte[16], TestContext.Current.CancellationToken);
            await using (WriteScope inner = pager.BeginWriteScope())
            {
                await pager.WritePageAsync(1, new byte[16], TestContext.Current.CancellationToken);
                await pager.WritePageAsync(3, new byte[16], TestContext.Current.CancellationToken);
            }

            Assert.Equal(0, stream.Writes);
        }

        Assert.Equal(2, stream.Writes);
        Assert.Equal([16L, 48L], stream.Offsets);
        Assert.Equal(1, stream.Flushes);
        Assert.Equal([3L, 1L, 3L], observer.Pages);
    }

    /// <summary>A failed replay drops buffered writes before the next scope.</summary>
    [Fact]
    public async Task FailedDrain_DoesNotReplayStalePages()
    {
        await using var stream = new CountingStream { FailWrite = true };
        stream.SetLength(32);
#pragma warning disable CA2000 // The awaited pager owns the codec.
        await using var pager = new Pager(stream, 16, new NoPageCodec(), true, typeof(AccessWriter));
#pragma warning restore CA2000
        WriteScope scope = pager.BeginWriteScope();
        await pager.WritePageAsync(1, new byte[16], TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<IOException>(() => scope.DisposeAsync().AsTask());
        stream.FailWrite = false;
        await using (pager.BeginWriteScope())
        {
        }

        Assert.Equal(0, stream.Writes);
    }

    /// <summary>Transactions notify logical changes before commit, including appended images.</summary>
    [Fact]
    public async Task Transaction_ObserverSeesBeforeImages_AndAppends()
    {
        await using var stream = new MemoryStream();
        stream.SetLength(32);
#pragma warning disable CA2000 // The awaited pager owns the codec.
        await using var pager = new Pager(stream, 16, new NoPageCodec(), true, typeof(AccessWriter));
#pragma warning restore CA2000
        using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(TestContext.Current.CancellationToken))
        {
            gate.Attach(new PagerTransaction(32, 16, 4));
        }

        var observer = new Observer();
        pager.AddWriteObserver(observer);
        byte[] page = new byte[16];
        page[0] = 17;
        await pager.WritePageAsync(1, page, TestContext.Current.CancellationToken);
        page[0] = 29;
        await pager.WritePageAsync(1, page, TestContext.Current.CancellationToken);
        await pager.AppendPageAsync(page, TestContext.Current.CancellationToken);
        Assert.Equal([1L, 1L, 2L], observer.Pages);
        Assert.Equal(new byte?[] { 0, 17, null }, observer.Before);
        Assert.Equal(32, stream.Length);
        using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(TestContext.Current.CancellationToken))
        {
            gate.Detach();
        }

        Assert.Equal(0, stream.ToArray()[16]);
    }

    /// <summary>Spills retain plaintext visibility and flush once for encrypted stores too.</summary>
    /// <param name="encrypted">Whether the physical pages use AES.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Spill_PreservesPlaintext_AndFlushesOnce(bool encrypted)
    {
        await using var stream = new CountingStream();
        stream.SetLength(16);
#pragma warning disable CA2000 // The awaited pager owns the selected codec.
        await using var pager = new Pager(stream, 16, encrypted ? new AesEcbPageCodec(new byte[16]) : new NoPageCodec(), true, typeof(AccessWriter), 0);
#pragma warning restore CA2000
        byte[] page = Enumerable.Repeat((byte)37, 16).ToArray();
        await using (pager.BeginWriteScope())
        {
            for (int number = 1; number <= 65; number++)
            {
                Assert.Equal(number, await pager.AppendPageAsync(page, TestContext.Current.CancellationToken));
            }

            Assert.Equal(64, stream.Writes);
            Assert.Equal(0, stream.Flushes);
            byte[] pending = await pager.ReadPageAsync(65, PageReadHint.NoCache, TestContext.Current.CancellationToken);
            Assert.Equal(page, pending.AsSpan(0, 16).ToArray());
            PageBuffers.Return(pending);
        }

        Assert.Equal(65, stream.Writes);
        Assert.Equal(1, stream.Flushes);
        Assert.Equal(Enumerable.Range(1, 65).Select(number => number * 16L), stream.Offsets);
        pager.InvalidateAll();
        byte[] stored = await pager.ReadPageAsync(1, TestContext.Current.CancellationToken);
        Assert.Equal(page, stored.AsSpan(0, 16).ToArray());
        PageBuffers.Return(stored);
        if (encrypted)
        {
            Assert.NotEqual(page, stream.ToArray().AsSpan(16, 16).ToArray());
        }
    }

    /// <summary>Disposal does not retry the flush of a failed transaction replay.</summary>
    [Fact]
    public async Task FailedTransactionFlush_PendingFlushDoesNotRetry()
    {
        await using var stream = new CountingStream { FailFlush = true };
        stream.SetLength(32);
#pragma warning disable CA2000 // The awaited pager owns the codec.
        await using var pager = new Pager(stream, 16, new NoPageCodec(), true, typeof(AccessWriter));
#pragma warning restore CA2000
        var transaction = new PagerTransaction(32, 16, 4);
        transaction.Write(1, new byte[16]);
        AggregateException failure = await Assert.ThrowsAsync<AggregateException>(() => pager.CommitAsync(transaction, () => { }, TestContext.Current.CancellationToken).AsTask());
        Assert.Collection(failure.InnerExceptions, error => Assert.IsType<IOException>(error), error => Assert.IsType<IOException>(error));
        Assert.True(pager.IsFaulted);
        await pager.FlushPendingWritesAsync();
        Assert.Equal(2, stream.Flushes);
    }

    /// <summary>A successful statement retains frames; discarding a statement invalidates observers.</summary>
    [Fact]
    public async Task StatementCommit_PreservesFrames_RollbackInvalidates()
    {
        await using var stream = new MemoryStream(new byte[32], writable: true);
#pragma warning disable CA2000 // The awaited pager owns and disposes the codec.
        await using var pager = new Pager(stream, 16, new NoPageCodec(), true, typeof(AccessWriter));
#pragma warning restore CA2000
        var observer = new Observer();
        pager.AddWriteObserver(observer);
        byte[] page = await pager.ReadPageAsync(1, TestContext.Current.CancellationToken);
        PageBuffers.Return(page);
        var transaction = new PagerTransaction(32, 16, 4);
        using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(TestContext.Current.CancellationToken))
        {
            gate.Attach(transaction, preserveFrames: true);
        }

        page = new byte[16];
        page[0] = 17;
        await pager.WritePageAsync(1, page, TestContext.Current.CancellationToken);
        using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(TestContext.Current.CancellationToken))
        {
            gate.Detach(invalidate: false);
        }

        await pager.CommitAsync(transaction, () => { }, TestContext.Current.CancellationToken, durable: false);
        long reads = pager.Statistics.StoreReads;
        byte[] committed = await pager.ReadPageAsync(1, TestContext.Current.CancellationToken);
        Assert.Equal(17, committed[0]);
        PageBuffers.Return(committed);
        Assert.Equal(reads, pager.Statistics.StoreReads);
        Assert.Equal(0, observer.Invalidations);
        using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(TestContext.Current.CancellationToken))
        {
            gate.Attach(new PagerTransaction(32, 16, 4), preserveFrames: true);
        }

        page[0] = 29;
        await pager.WritePageAsync(1, page, TestContext.Current.CancellationToken);
        using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(TestContext.Current.CancellationToken))
        {
            gate.Detach();
        }

        Assert.Equal(1, observer.Invalidations);
        byte[] rolledBack = await pager.ReadPageAsync(1, TestContext.Current.CancellationToken);
        Assert.Equal(17, rolledBack[0]);
        PageBuffers.Return(rolledBack);
    }

    private sealed class Observer : IPageWriteObserver
    {
        internal int Invalidations { get; private set; }

        internal List<long> Pages { get; } = [];

        internal List<byte?> Before { get; } = [];

        public void OnPageWritten(long pageNumber, ReadOnlySpan<byte> before, ReadOnlySpan<byte> after)
        {
            this.Pages.Add(pageNumber);
            this.Before.Add(before.IsEmpty ? null : before[0]);
        }

        public void OnInvalidateAll() => this.Invalidations++;
    }

    private sealed class CountingStream : MemoryStream
    {
        internal List<long> Offsets { get; } = [];

        internal int Writes { get; private set; }

        internal int Flushes { get; private set; }

        internal bool FailWrite { get; set; }

        internal bool FailFlush { get; set; }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (this.FailWrite)
            {
                throw new IOException("Injected write failure.");
            }

            this.Offsets.Add(this.Position);
            this.Writes++;
            return base.WriteAsync(buffer, cancellationToken);
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            this.Flushes++;
            return this.FailFlush ? Task.FromException(new IOException("Injected flush failure.")) : base.FlushAsync(cancellationToken);
        }
    }
}
