namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Pages.Paging;
using Xunit;

/// <summary>Checks recovery evidence at commit decision and restoration failure boundaries.</summary>
public sealed class PagerRecoveryFailureTests
{
    private const int PageSize = 4096;

    /// <summary>A failed commit decision preparation preserves written bytes and faults the writer.</summary>
    [Fact]
    public async Task CommitDecisionFailure_PreservesRecoveryEvidence()
    {
        string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".mdb");
        byte[] original = new byte[PageSize * 2];
        await File.WriteAllBytesAsync(path, original, TestContext.Current.CancellationToken);
        try
        {
            await using (var stream = new RecoveryFaultStream(path) { FailDecisionFlush = true })
            {
#pragma warning disable CA2000 // The awaited pager owns and disposes the codec.
                await using var pager = new Pager(stream, PageSize, new NoPageCodec(), true, typeof(AccessWriter));
#pragma warning restore CA2000
                var transaction = new PagerTransaction(original.Length, PageSize, 8);
                byte[] after = new byte[PageSize];
                Array.Fill(after, (byte)7);
                transaction.Write(1, after);
                await Assert.ThrowsAsync<IOException>(() => pager.CommitAsync(transaction, () => { }, TestContext.Current.CancellationToken).AsTask());
                Assert.True(pager.IsFaulted);
                Assert.Equal(1, stream.PhysicalWrites);
                Assert.True(File.Exists(path + ".jdw-journal"));
                stream.Position = PageSize;
                Assert.Equal(7, stream.ReadByte());
            }

            await using (var recovery = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                PersistentRollbackJournal.Recover(recovery);
            }

            Assert.Equal(original, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
        }
    }

    /// <summary>Restoration does not append recovery intents after a failed physical write.</summary>
    [Fact]
    public async Task FailedWrite_RestorationDoesNotAppendJournalRecords()
    {
        string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".mdb");
        byte[] original = new byte[PageSize * 2];
        await File.WriteAllBytesAsync(path, original, TestContext.Current.CancellationToken);
        try
        {
            await using (var stream = new RecoveryFaultStream(path) { FailFirstWrite = true })
            {
#pragma warning disable CA2000 // The awaited pager owns and disposes the codec.
                await using var pager = new Pager(stream, PageSize, new NoPageCodec(), true, typeof(AccessWriter));
#pragma warning restore CA2000
                var transaction = new PagerTransaction(original.Length, PageSize, 8);
                byte[] after = new byte[PageSize];
                Array.Fill(after, (byte)9);
                transaction.Write(1, after);
                await Assert.ThrowsAsync<IOException>(() => pager.CommitAsync(transaction, () => { }, TestContext.Current.CancellationToken).AsTask());
                Assert.False(pager.IsFaulted);
                Assert.Equal(stream.FailedWriteJournalLength, stream.RestorationJournalLength);
                Assert.False(File.Exists(path + ".jdw-journal"));
            }

            Assert.Equal(original, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
        }
    }

    private sealed class RecoveryFaultStream(string path) : FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None)
    {
        private int durableFlushes;

        internal bool FailDecisionFlush { get; init; }

        internal bool FailFirstWrite { get; init; }

        internal int PhysicalWrites { get; private set; }

        internal long FailedWriteJournalLength { get; private set; }

        internal long RestorationJournalLength { get; private set; }

        public override void Flush(bool flushToDisk)
        {
            if (flushToDisk && ++this.durableFlushes == 2 && this.FailDecisionFlush)
            {
                throw new IOException("Injected commit decision preparation failure.");
            }

            base.Flush(flushToDisk);
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            this.PhysicalWrites++;
            if (this.FailFirstWrite && this.PhysicalWrites == 1)
            {
                this.FailedWriteJournalLength = new FileInfo(this.Name + ".jdw-journal").Length;
                throw new IOException("Injected physical write failure.");
            }

            if (this.FailFirstWrite)
            {
                this.RestorationJournalLength = new FileInfo(this.Name + ".jdw-journal").Length;
            }

            return base.WriteAsync(buffer, cancellationToken);
        }
    }
}
