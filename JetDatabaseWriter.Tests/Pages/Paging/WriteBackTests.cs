namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using System.Collections.Generic;
using System.IO;
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
        await using (var scope = pager.BeginWriteScope())
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
        await using (var outer = pager.BeginWriteScope())
        {
            await pager.WritePageAsync(3, new byte[16], TestContext.Current.CancellationToken);
            await using (var inner = pager.BeginWriteScope())
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
    private sealed class Observer : IPageWriteObserver
    {
        internal List<long> Pages { get; } = [];

        internal List<byte?> Before { get; } = [];

        public void OnPageWritten(long pageNumber, ReadOnlySpan<byte> before, ReadOnlySpan<byte> after)
        {
            this.Pages.Add(pageNumber);
            this.Before.Add(before.IsEmpty ? null : before[0]);
        }

        public void OnInvalidateAll()
        {
        }
    }

    private sealed class CountingStream : MemoryStream
    {
        internal List<long> Offsets { get; } = [];

        internal int Writes { get; private set; }

        internal int Flushes { get; private set; }

        internal bool FailWrite { get; set; }

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
            return base.FlushAsync(cancellationToken);
        }
    }
}
