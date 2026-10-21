namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Pages.Paging;
using Xunit;

/// <summary>Checks pager-owned transaction replay and cancellation.</summary>
public sealed class PagerTransactionTests
{
    /// <summary>Replay checks cancellation before preparation and ignores it after preparation.</summary>
    [Fact]
    public async Task Commit_HonorsCancellationOnlyBeforeFirstWrite()
    {
        await using var stream = new MemoryStream();
        stream.SetLength(32);
#pragma warning disable CA2000 // The awaited pager owns the codec.
        await using var pager = new Pager(stream, 16, new NoPageCodec(), true, typeof(AccessWriter));
#pragma warning restore CA2000
        var transaction = new PagerTransaction(32, 16, 4);
        byte[] page = new byte[16];
        page[0] = 37;
        transaction.Write(1, page);
        using var source = new CancellationTokenSource();
        await source.CancelAsync();
        bool prepared = false;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pager.CommitAsync(transaction, () => prepared = true, source.Token).AsTask());
        Assert.False(prepared);
        Assert.Equal(0, stream.ToArray()[16]);
        using var duringReplay = new CancellationTokenSource();
        await pager.CommitAsync(transaction, duringReplay.Cancel, duringReplay.Token);
        Assert.Equal(37, stream.ToArray()[16]);
    }
}
