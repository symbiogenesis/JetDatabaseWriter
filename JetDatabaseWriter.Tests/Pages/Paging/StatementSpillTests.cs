namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Checks bounded private statement frames and restoration after spill.</summary>
public sealed class StatementSpillTests
{
    /// <summary>A file-backed log crosses its threshold using only raw ciphertext images.</summary>
    [Fact]
    public async Task FileUndoLog_RetainsCiphertextAcrossSpill()
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-spill-test-" + Guid.NewGuid().ToString("N") + ".tmp");
        await using var stream = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.Asynchronous | FileOptions.DeleteOnClose);
        byte[] ciphertext = new byte[16 * 80];
        Array.Fill(ciphertext, (byte)0xA5);
        using var codec = new Jet4Rc4PageCodec(0x12345678);
        for (int page = 1; page < 80; page++)
        {
            codec.Encode(ciphertext, page * 16, page, 16);
        }

        await stream.WriteAsync(ciphertext, TestContext.Current.CancellationToken);
        await using var store = new StreamPageStore(stream, true);
        await using var log = new StatementUndoLog(ciphertext.Length, true, 64);
        for (int page = 0; page < 80; page++)
        {
            await log.CaptureAsync(store, page * 16, 16, TestContext.Current.CancellationToken);
        }

        Assert.True(log.IsFileSpilled);
        byte[] plaintext = new byte[16];
        Array.Fill(plaintext, (byte)0xA5);
        Assert.NotEqual(plaintext, await log.ReadRawImageAsync(16));
        for (int page = 0; page < 80; page++)
        {
            Assert.Equal(ciphertext.AsSpan(page * 16, 16).ToArray(), await log.ReadRawImageAsync(page * 16));
        }

        await store.WriteAsync(0, new byte[16], CancellationToken.None);
        await log.RestoreAsync(store, false);
        stream.Position = 0;
        byte[] restored = new byte[ciphertext.Length];
        await stream.ReadExactlyAsync(restored, TestContext.Current.CancellationToken);
        Assert.Equal(ciphertext, restored);
    }

    /// <summary>Cancellation while preparing a later spill restores all previous early writes.</summary>
    [Fact]
    public async Task CancellationAfterSpill_RestoresOriginal()
    {
        await using var stream = new WriteFaultStream();
        stream.SetLength(16 * 160);
#pragma warning disable CA2000 // The awaited pager owns its codec.
        await using var pager = new Pager(stream, 16, new NoPageCodec(), true, typeof(AccessWriter), 0);
#pragma warning restore CA2000
        var transaction = new PagerTransaction(stream.Length, 16, int.MaxValue, statement: true);
        using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(CancellationToken.None))
        {
            gate.Attach(transaction);
        }

        byte[] page = new byte[16];
        Array.Fill(page, (byte)37);
        for (int index = 0; index < 64; index++)
        {
            await pager.WritePageAsync(index, page, TestContext.Current.CancellationToken);
        }

        using var source = new CancellationTokenSource();
        stream.CancelAfterReads(1, source);
        for (int index = 64; index < 127; index++)
        {
            await pager.WritePageAsync(index, page, TestContext.Current.CancellationToken);
        }

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pager.WritePageAsync(127, page, source.Token).AsTask());
        using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(CancellationToken.None))
        {
            gate.Detach();
        }

        await pager.RollbackStatementAsync(transaction);
        Assert.Equal(new byte[16 * 160], stream.ToArray());
        Assert.False(pager.IsFaulted);
    }

    /// <summary>Every torn spill and final write restores a repeatedly modified page and appended tail.</summary>
    [Fact]
    public async Task TornWriteSweep_RestoresOriginalAndAllowsRetry()
    {
        for (int fault = 1; fault <= 141; fault++)
        {
            await using var stream = new WriteFaultStream();
            stream.SetLength(16 * 80);
#pragma warning disable CA2000 // The awaited pager owns its codec.
            await using var pager = new Pager(stream, 16, new NoPageCodec(), true, typeof(AccessWriter), 0);
#pragma warning restore CA2000
            var transaction = new PagerTransaction(stream.Length, 16, int.MaxValue, statement: true);
            using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(CancellationToken.None))
            {
                gate.Attach(transaction);
            }

            stream.FailDuringWrite(fault);
            byte[] page = new byte[16];
            Array.Fill(page, (byte)37);
            try
            {
                for (int index = 0; index < 140; index++)
                {
                    await pager.WritePageAsync(index, page, TestContext.Current.CancellationToken);
                    if (index == 64)
                    {
                        byte[] rewritten = new byte[16];
                        Array.Fill(rewritten, (byte)82);
                        await pager.WritePageAsync(1, rewritten, TestContext.Current.CancellationToken);
                    }
                }

                using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(CancellationToken.None))
                {
                    gate.Detach(invalidate: false);
                }

                await pager.CommitAsync(transaction, () => { }, CancellationToken.None);
                Assert.Fail("The selected physical write must fail.");
            }
            catch (IOException)
            {
                using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(CancellationToken.None))
                {
                    gate.Detach();
                }

                await pager.RollbackStatementAsync(transaction);
            }

            Assert.Equal(new byte[16 * 80], stream.ToArray());
            Assert.False(pager.IsFaulted);
            await pager.WritePageAsync(1, page, TestContext.Current.CancellationToken);
            Assert.Equal(37, stream.ToArray()[16]);
        }
    }

    /// <summary>A page changed again after spill still restores its original bytes.</summary>
    [Fact]
    public async Task Rollback_RestoresFirstImageAfterRepeatedSpills()
    {
        await using var stream = new MemoryStream(new byte[16 * 160]);
#pragma warning disable CA2000 // The awaited pager owns its codec.
        await using var pager = new Pager(stream, 16, new NoPageCodec(), true, typeof(AccessWriter), 0);
#pragma warning restore CA2000
        var transaction = new PagerTransaction(stream.Length, 16, int.MaxValue, statement: true);
        using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(CancellationToken.None))
        {
            gate.Attach(transaction);
        }

        byte[] page = new byte[16];
        Array.Fill(page, (byte)37);
        for (int index = 0; index < 140; index++)
        {
            await pager.WritePageAsync(index, page, TestContext.Current.CancellationToken);
            if (index == 64)
            {
                byte[] rewritten = new byte[16];
                Array.Fill(rewritten, (byte)82);
                await pager.WritePageAsync(1, rewritten, TestContext.Current.CancellationToken);
            }

            Assert.InRange(transaction.Count, 0, 63);
        }

        Array.Fill(page, (byte)82);
        await pager.WritePageAsync(1, page, TestContext.Current.CancellationToken);
        Assert.Equal(82, stream.ToArray()[16]);
        using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(CancellationToken.None))
        {
            gate.Detach();
        }

        await pager.RollbackStatementAsync(transaction);
        Assert.Equal(new byte[16 * 160], stream.ToArray());
    }
}
