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

/// <summary>Checks live file rollback against its original raw recovery snapshot.</summary>
public sealed class FileRollbackTests
{
    private const int PageSize = 2048;

    /// <summary>Commit failure restores raw changed pages without rewriting unrelated pages.</summary>
    /// <param name="unrelatedWrite">Whether unrelated raw content changes during the failure.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FailedWrite_RestoresRawPagesAndRemovesAppendedTail(bool unrelatedWrite)
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-file-rollback-" + Guid.NewGuid().ToString("N") + ".mdb");
        byte[] original = EncryptedPages(5);
        await File.WriteAllBytesAsync(path, original, TestContext.Current.CancellationToken);
        try
        {
            await using (var stream = new TornWriteStream(path, unrelatedWrite))
            {
#pragma warning disable CA2000 // The awaited pager owns and disposes the codec.
                await using var pager = new Pager(stream, PageSize, new Jet4Rc4PageCodec(0x12345678), true, typeof(AccessWriter), 0);
#pragma warning restore CA2000
                var transaction = new PagerTransaction(original.Length, PageSize, 8);
                byte[] after = new byte[PageSize];
                Array.Fill(after, (byte)101);
                transaction.Write(1, after);
                transaction.Write(3, after);
                Assert.Equal(5, transaction.Append(after));

                await Assert.ThrowsAsync<IOException>(() => pager.CommitAsync(transaction, () => { }, TestContext.Current.CancellationToken).AsTask());

                Assert.False(pager.IsFaulted);
                Assert.Equal(3, stream.AttemptedWrites);
                Assert.Equal([PageSize, PageSize * 3], stream.RestoredOffsets.Order());
                Assert.Equal(original.Length, stream.Length);
                Assert.False(File.Exists(path + ".jdw-journal"));
            }

            if (unrelatedWrite)
            {
                original[PageSize * 2] = 201;
            }

            Assert.Equal(original, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
        }
    }

    /// <summary>Repeated encrypted statement spills share the durable original image.</summary>
    [Fact]
    public async Task StatementSpills_UseRecoverySnapshotAndRestoreFirstRawImage()
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-file-rollback-" + Guid.NewGuid().ToString("N") + ".mdb");
        byte[] original = EncryptedPages(80);
        await File.WriteAllBytesAsync(path, original, TestContext.Current.CancellationToken);
        try
        {
            await using (var stream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
#pragma warning disable CA2000 // The awaited pager owns and disposes the codec.
                await using var pager = new Pager(stream, PageSize, new Jet4Rc4PageCodec(0x12345678), true, typeof(AccessWriter), 0);
#pragma warning restore CA2000
                var transaction = new PagerTransaction(original.Length, PageSize, int.MaxValue, statement: true);
                using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(CancellationToken.None))
                {
                    gate.Attach(transaction);
                }

                try
                {
                    byte[] after = new byte[PageSize];
                    Array.Fill(after, (byte)101);
                    for (int page = 0; page < 140; page++)
                    {
                        await pager.WritePageAsync(page, after, TestContext.Current.CancellationToken);
                        if (page == 64)
                        {
                            after[0] = 102;
                            await pager.WritePageAsync(1, after, TestContext.Current.CancellationToken);
                        }

                        Assert.InRange(transaction.Count, 0, 63);
                    }

                    Assert.True(stream.Length > original.Length);
                    Assert.NotNull(transaction.Rollback);
                    Assert.True(File.Exists(path + ".jdw-journal"));
                }
                finally
                {
                    using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(CancellationToken.None))
                    {
                        gate.Detach();
                    }

                    await pager.RollbackStatementAsync(transaction);
                }

                Assert.False(pager.IsFaulted);
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

    /// <summary>A truncated untouched tail is restored along with physically spilled pages.</summary>
    [Fact]
    public async Task StatementRollback_RestoresTruncatedUntouchedTail()
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-file-rollback-" + Guid.NewGuid().ToString("N") + ".mdb");
        byte[] original = EncryptedPages(80);
        await File.WriteAllBytesAsync(path, original, TestContext.Current.CancellationToken);
        try
        {
            await using (var stream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
#pragma warning disable CA2000 // The awaited pager owns and disposes the codec.
                await using var pager = new Pager(stream, PageSize, new Jet4Rc4PageCodec(0x12345678), true, typeof(AccessWriter), 0);
#pragma warning restore CA2000
                var transaction = new PagerTransaction(original.Length, PageSize, int.MaxValue, statement: true);
                using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(CancellationToken.None))
                {
                    gate.Attach(transaction);
                }

                try
                {
                    byte[] after = new byte[PageSize];
                    Array.Fill(after, (byte)101);
                    for (int page = 0; page < 64; page++)
                    {
                        await pager.WritePageAsync(page, after, TestContext.Current.CancellationToken);
                    }

                    Assert.NotNull(transaction.Rollback);
                    Assert.True(File.Exists(path + ".jdw-journal"));
                    await pager.TruncateAsync(40, TestContext.Current.CancellationToken);
                }
                finally
                {
                    using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(CancellationToken.None))
                    {
                        gate.Detach();
                    }

                    await pager.RollbackStatementAsync(transaction);
                }

                Assert.False(pager.IsFaulted);
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

    private static byte[] EncryptedPages(int count)
    {
        byte[] bytes = new byte[PageSize * count];
        using var codec = new Jet4Rc4PageCodec(0x12345678);
        for (int page = 0; page < count; page++)
        {
            bytes.AsSpan(page * PageSize, PageSize).Fill(checked((byte)(page + 1)));
            codec.Encode(bytes, page * PageSize, page, PageSize);
        }

        return bytes;
    }

    private sealed class TornWriteStream(string path, bool unrelatedWrite) : FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None)
    {
        private bool failed;

        internal int AttemptedWrites { get; private set; }

        internal List<long> RestoredOffsets { get; } = [];

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (this.failed)
            {
                this.RestoredOffsets.Add(this.Position);
            }
            else if (++this.AttemptedWrites == 3)
            {
                await base.WriteAsync(buffer[..7], cancellationToken);
                if (unrelatedWrite)
                {
                    long position = this.Position;
                    this.Position = PageSize * 2;
                    await base.WriteAsync(new ReadOnlyMemory<byte>([201]), cancellationToken);
                    this.Position = position;
                }

                this.failed = true;
                throw new IOException("Injected torn append failure.");
            }

            await base.WriteAsync(buffer, cancellationToken);
        }
    }
}
