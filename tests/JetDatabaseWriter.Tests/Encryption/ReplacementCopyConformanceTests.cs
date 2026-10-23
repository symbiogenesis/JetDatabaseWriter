namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Exceptions;
using Xunit;

/// <summary>Drives the production copy loop through bounded caller-owned file reads.</summary>
public sealed class ReplacementCopyConformanceTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Every positive short read advances the actual bounded copy without changing the source position.</summary>
    /// <param name="length">The complete source length.</param>
    /// <param name="maximumRead">The largest successful read returned by the source.</param>
    [Theory]
    [InlineData(0, 1)]
    [InlineData(65537, 1)]
    [InlineData(65537, 997)]
    [InlineData(131089, 65536)]
    public async Task Begin_PositiveShortReads_CopyExactImageAndRestoreSourcePosition(int length, int maximumRead)
    {
        using var directory = new CopyDirectory();
        byte[] original = Image(length);
        await File.WriteAllBytesAsync(directory.Path, original, Ct);
        await using var source = new ObservedSource(directory.Path, maximumRead);
        source.Position = 17;
        using (var owner = EncryptionReplacementIntent.Begin(directory.Path, source, cancellationToken: Ct))
        {
            Assert.False(owner.IsPrepared);
            Assert.Equal(17, source.Position);
            Assert.Equal(length, source.BytesRead);
            Assert.InRange(source.MaximumRequested, 0, 65536);
            Assert.Equal(length != 0, source.ReadCalls != 0);
            Assert.Equal(maximumRead < Math.Min(length, 65536), source.ShortReads != 0);
            Assert.Equal(original, await File.ReadAllBytesAsync(owner.OldPath, Ct));
        }

        Assert.Equal(17, source.PositionAtDisposal);
        Assert.Equal(directory.Path, Assert.Single(Directory.GetFiles(directory.DirectoryPath)));
        Assert.Equal(original, await File.ReadAllBytesAsync(directory.Path, Ct));
    }

    /// <summary>A zero read, failed read or cancellation refuses preparation and cleans only its unprepared copies.</summary>
    /// <param name="failure">The source operation that stops progress.</param>
    /// <param name="stopAfter">The number of exact source bytes returned before interruption.</param>
    [Theory]
    [InlineData("eof", 0)]
    [InlineData("eof", 1)]
    [InlineData("eof", 65536)]
    [InlineData("read", 0)]
    [InlineData("read", 1)]
    [InlineData("read", 65536)]
    [InlineData("cancel", 1)]
    [InlineData("cancel", 65536)]
    public async Task Begin_InterruptedReads_RefuseWithoutPublishingAndRestoreSourcePosition(string failure, int stopAfter)
    {
        using var directory = new CopyDirectory();
        byte[] original = Image(131089);
        await File.WriteAllBytesAsync(directory.Path, original, Ct);
        string unrelated = directory.Path + ".jdw-replace-unrelated.old";
        byte[] unrelatedBytes = [201, 202, 203];
        await File.WriteAllBytesAsync(unrelated, unrelatedBytes, Ct);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await using var source = new ObservedSource(directory.Path, 997, stopAfter, failure, cancellation);
        source.Position = 23;
        void Begin()
        {
            using var owner = EncryptionReplacementIntent.Begin(directory.Path, source, cancellationToken: cancellation.Token);
        }

        Exception error = failure switch
        {
            "eof" => Assert.Throws<JetCorruptDataException>(Begin),
            "read" => Assert.Throws<IOException>(Begin),
            _ => Assert.Throws<OperationCanceledException>(Begin),
        };
        if (error is JetCorruptDataException corrupt)
        {
            Assert.Equal(JetErrorCode.CorruptRecoveryJournal, corrupt.ErrorCode);
        }

        if (error is OperationCanceledException canceled)
        {
            Assert.Equal(cancellation.Token, canceled.CancellationToken);
        }

        Assert.Equal(stopAfter, source.BytesRead);
        Assert.Equal(23, source.PositionAtDisposal);
        Assert.InRange(source.MaximumRequested, 1, 65536);
        Assert.Equal(2, Directory.GetFiles(directory.DirectoryPath).Length);
        Assert.Equal(original, await File.ReadAllBytesAsync(directory.Path, Ct));
        Assert.Equal(unrelatedBytes, await File.ReadAllBytesAsync(unrelated, Ct));
    }

    private static byte[] Image(int length)
    {
        byte[] bytes = new byte[length];
        for (int index = 0; index < bytes.Length; index++)
        {
            bytes[index] = (byte)((index + (index / 65536)) % 251);
        }

        return bytes;
    }

    private sealed class ObservedSource(string path, int maximumRead, int stopAfter = int.MaxValue, string failure = "none", CancellationTokenSource? cancellation = null)
        : FileStream(path, FileMode.Open, FileAccess.Read, FileShare.None)
    {
        private bool disposed;

        internal int BytesRead { get; private set; }

        internal int ReadCalls { get; private set; }

        internal int ShortReads { get; private set; }

        internal int MaximumRequested { get; private set; }

        internal long PositionAtDisposal { get; private set; }

        public override int Read(byte[] buffer, int offset, int count)
        {
            this.ReadCalls++;
            this.MaximumRequested = Math.Max(this.MaximumRequested, count);
            if (this.BytesRead == stopAfter)
            {
                if (failure == "read")
                {
                    throw new IOException("Injected replacement source read failure.");
                }

                return 0;
            }

            int read = base.Read(buffer, offset, Math.Min(count, Math.Min(maximumRead, stopAfter - this.BytesRead)));
            this.BytesRead += read;
            this.ShortReads += read < count ? 1 : 0;
            if (failure == "cancel" && this.BytesRead == stopAfter)
            {
                cancellation!.Cancel();
            }

            return read;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing && !this.disposed)
            {
                this.PositionAtDisposal = this.Position;
                this.disposed = true;
            }

            base.Dispose(disposing);
        }
    }

    private sealed class CopyDirectory : IDisposable
    {
        internal CopyDirectory()
        {
            this.DirectoryPath = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "jdw-replacement-copy-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(this.DirectoryPath);
            this.Path = System.IO.Path.Combine(this.DirectoryPath, "database.accdb");
        }

        internal string DirectoryPath { get; }

        internal string Path { get; }

        public void Dispose() => Directory.Delete(this.DirectoryPath, recursive: true);
    }
}
