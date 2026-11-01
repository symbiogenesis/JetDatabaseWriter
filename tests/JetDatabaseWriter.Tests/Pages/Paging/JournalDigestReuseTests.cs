namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Checks that repeated point transactions do not reread unchanged database payloads.</summary>
public sealed class JournalDigestReuseTests
{
    /// <summary>Compiled digest replacement preserves unrelated slots and refuses out-of-range positions unchanged.</summary>
    /// <param name="index">The requested digest slot.</param>
    [Theory]
    [InlineData(0u)]
    [InlineData(1u)]
    [InlineData(2u)]
    [InlineData(uint.MaxValue)]
    public void DigestReplacement_PreservesIndependentSlots(uint index)
    {
        byte[] ledger = new byte[64];
        Array.Fill(ledger, (byte)17);
        byte[] digest = new byte[32];
        Array.Fill(digest, (byte)93);
        byte[] expected = (byte[])ledger.Clone();
        if (index < 2)
        {
            digest.CopyTo(expected, checked((int)index) * 32);
        }

        Assert.Equal(index < 2, PageDigestKernel.ReplaceDigest(ledger, index, digest));
        Assert.Equal(expected, ledger);
    }

    /// <summary>A second transaction reuses unchanged raw page digests under the same exclusive owner.</summary>
    /// <param name="pageSize">The physical page size.</param>
    [Theory]
    [InlineData(2048)]
    [InlineData(4096)]
    public async Task PointTransactions_ReadOnlyChangedPayloadAfterFirstPreparation(int pageSize)
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-digest-" + Guid.NewGuid().ToString("N") + ".mdb");
        try
        {
            await File.WriteAllBytesAsync(path, new byte[pageSize * 64], TestContext.Current.CancellationToken);
            await using var database = new CountingFileStream(path);
            await using var store = new StreamPageStore(database, leaveOpen: true);
            await WritePageAsync(store, pageSize, 1);
            database.BytesRead = 0;

            await WritePageAsync(store, pageSize, 2);

            Assert.InRange(database.BytesRead, pageSize, pageSize * 3L);
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
            File.Delete(path + ".jdw-journal.preparing");
        }
    }

    /// <summary>Tracked append, overwrite, truncation and rollback retain exact independent image commitments.</summary>
    /// <param name="pageSize">The physical page size.</param>
    [Theory]
    [InlineData(2048)]
    [InlineData(4096)]
    public async Task DigestLedger_TracksPhysicalChangesAndRollback(int pageSize)
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-digest-" + Guid.NewGuid().ToString("N") + ".mdb");
        try
        {
            await File.WriteAllBytesAsync(path, new byte[pageSize * 3], TestContext.Current.CancellationToken);
            await using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            await using (var store = new StreamPageStore(database, leaveOpen: true))
            {
                await WritePageAsync(store, pageSize, 17);
                byte[] page = new byte[pageSize];
                Array.Fill(page, (byte)93);
                await store.WriteAsync(pageSize * 5L, page, TestContext.Current.CancellationToken);
                await store.SetLengthAsync(pageSize * 4L, TestContext.Current.CancellationToken);
                await store.FlushAsync(false, TestContext.Current.CancellationToken);
                Assert.Equal(await IndependentHashAsync(database, pageSize), store.GetPageDigests(pageSize).DatabaseHash());

                await using (var rollback = new PhysicalRollback(store, store.Length, pageSize, 64))
                {
                    rollback.BeginWrite();
                    await store.WriteAsync(pageSize, page, TestContext.Current.CancellationToken);
                    await store.SetLengthAsync(pageSize * 2L, TestContext.Current.CancellationToken);
                    await rollback.RestoreAsync(durable: true);
                }

                Assert.Equal(await IndependentHashAsync(database, pageSize), store.GetPageDigests(pageSize).DatabaseHash());
            }

            await using var reopened = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            PersistentRollbackJournal.Recover(reopened);
            Assert.False(File.Exists(path + ".jdw-journal"));
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
            File.Delete(path + ".jdw-journal.preparing");
        }
    }

    /// <summary>A torn write invalidates reuse before rollback and cannot supply a stale commit digest.</summary>
    [Fact]
    public async Task FailedPhysicalWrite_InvalidatesDigestReuse()
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-digest-" + Guid.NewGuid().ToString("N") + ".mdb");
        try
        {
            await File.WriteAllBytesAsync(path, new byte[4096 * 8], TestContext.Current.CancellationToken);
            await using var database = new CountingFileStream(path);
            await using var store = new StreamPageStore(database, leaveOpen: true);
            await WritePageAsync(store, 4096, 17);
            FilePageDigests ledger = store.GetPageDigests(4096);
            await using (var rollback = new PhysicalRollback(store, store.Length, 4096, 64))
            {
                rollback.BeginWrite();
                database.FailNextWrite = true;
                _ = await Assert.ThrowsAsync<IOException>(async () => await store.WriteAsync(4096, new byte[4096], TestContext.Current.CancellationToken));
                Assert.False(ledger.IsValid);
                await rollback.RestoreAsync(durable: true);
            }

            Assert.Equal(await IndependentHashAsync(database, 4096), store.GetPageDigests(4096).DatabaseHash());
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
            File.Delete(path + ".jdw-journal.preparing");
        }
    }

    /// <summary>Length extension cannot assume the operating system supplies zero-filled bytes.</summary>
    /// <param name="sparseWrite">Whether a sparse write exposes the gap instead of a resize.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LengthExtension_AcquiresActualExposedBytes(bool sparseWrite)
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-digest-" + Guid.NewGuid().ToString("N") + ".mdb");
        try
        {
            await File.WriteAllBytesAsync(path, new byte[4096 * 3], TestContext.Current.CancellationToken);
            await using var database = new CountingFileStream(path) { FillExtensions = true };
            await using var store = new StreamPageStore(database, leaveOpen: true);
            FilePageDigests original = store.GetPageDigests(4096);

            if (sparseWrite)
            {
                await store.WriteAsync(4096 * 5L, new byte[4096], TestContext.Current.CancellationToken);
            }
            else
            {
                await store.SetLengthAsync(4096 * 5L, TestContext.Current.CancellationToken);
            }

            Assert.False(original.IsValid);
            Assert.Equal(await IndependentHashAsync(database, 4096), store.GetPageDigests(4096).DatabaseHash());
        }
        finally
        {
            File.Delete(path);
        }
    }

    /// <summary>The store cannot reuse a digest ledger framed with a different page size.</summary>
    [Fact]
    public async Task DigestReuse_BindsPhysicalPageSize()
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-digest-" + Guid.NewGuid().ToString("N") + ".mdb");
        try
        {
            await File.WriteAllBytesAsync(path, new byte[8192], TestContext.Current.CancellationToken);
            await using var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            await using var store = new StreamPageStore(database, leaveOpen: true);
            FilePageDigests original = store.GetPageDigests(2048);
            FilePageDigests replacement = store.GetPageDigests(4096);

            Assert.False(original.IsValid);
            Assert.NotSame(original, replacement);
            Assert.Equal(await IndependentHashAsync(database, 4096), replacement.DatabaseHash());
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static async Task<byte[]> IndependentHashAsync(FileStream database, int pageSize)
    {
        byte[] bytes = new byte[checked((int)database.Length)];
        database.Position = 0;
        await database.ReadExactlyAsync(bytes, TestContext.Current.CancellationToken);
        byte[] framed = new byte[16 + ((bytes.Length + pageSize - 1) / pageSize * 32)];
        BinaryPrimitives.WriteInt64LittleEndian(framed, bytes.Length);
        BinaryPrimitives.WriteInt32LittleEndian(framed.AsSpan(8), pageSize);
        for (int offset = 0, cursor = 16; offset < bytes.Length; offset += pageSize, cursor += 32)
        {
            SHA256.HashData(bytes.AsSpan(offset, Math.Min(pageSize, bytes.Length - offset))).CopyTo(framed, cursor);
        }

        return SHA256.HashData(framed);
    }

    private static async Task WritePageAsync(StreamPageStore store, int pageSize, byte value)
    {
        await using var rollback = new PhysicalRollback(store, store.Length, pageSize, 64);
        rollback.BeginWrite();
        byte[] page = new byte[pageSize];
        Array.Fill(page, value);
        await store.WriteAsync(pageSize, page, TestContext.Current.CancellationToken);
        await rollback.CommitAsync(durable: true);
    }

    private sealed class CountingFileStream(string path) : FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None)
    {
        internal long BytesRead { get; set; }

        internal bool FailNextWrite { get; set; }

        internal bool FillExtensions { get; init; }

        public override void SetLength(long value)
        {
            long original = this.Length;
            long position = this.Position;
            base.SetLength(value);
            if (this.FillExtensions && value > original)
            {
                byte[] exposed = new byte[checked((int)(value - original))];
                Array.Fill(exposed, (byte)93);
                this.Position = original;
                this.Write(exposed, 0, exposed.Length);
                this.Position = position;
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int read = base.Read(buffer, offset, count);
            this.BytesRead += read;
            return read;
        }

        public override int Read(Span<byte> buffer)
        {
            int read = base.Read(buffer);
            this.BytesRead += read;
            return read;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            int read = await base.ReadAsync(buffer, cancellationToken);
            this.BytesRead += read;
            return read;
        }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (this.FillExtensions && this.Position > this.Length)
            {
                this.SetLength(this.Position);
            }

            if (this.FailNextWrite)
            {
                this.FailNextWrite = false;
                await base.WriteAsync(buffer[..17], cancellationToken);
                throw new IOException("Injected torn raw write.");
            }

            await base.WriteAsync(buffer, cancellationToken);
        }
    }
}
