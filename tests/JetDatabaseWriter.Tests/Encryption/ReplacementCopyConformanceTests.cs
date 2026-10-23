namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.IO;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Exceptions;
using Xunit;

/// <summary>Drives the production copy loop and recovery of its retained Prepared staging images.</summary>
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

    /// <summary>The publication copier handles positive short reads, and recovery republishes its exact staged image.</summary>
    /// <param name="length">The selected original image length.</param>
    /// <param name="maximumRead">The largest successful read returned by the source.</param>
    [Theory]
    [InlineData(0, 1)]
    [InlineData(1, 1)]
    [InlineData(65537, 1)]
    [InlineData(131089, 997)]
    [InlineData(131089, 65536)]
    public async Task PreparedCopy_PositiveShortReads_RecoverMissingDestination(int length, int maximumRead)
    {
        using var directory = new CopyDirectory();
        byte[] original = Image(length);
        (string old, string next) = await PrepareMissingDestinationAsync(directory.Path, original);
        string publication = next.Replace(".new.tmp", ".publish.tmp", StringComparison.Ordinal);
        await using (var source = new ObservedSource(old, maximumRead))
        {
            source.Position = 17;
            await using (var output = new FileStream(publication, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
                CopyForPublication(source, output);
            }

            Assert.Equal(17, source.Position);
            Assert.Equal(length, source.BytesRead);
            Assert.InRange(source.MaximumRequested, 0, 65536);
            Assert.Equal(maximumRead < Math.Min(length, 65536), source.ShortReads != 0);
        }

        Assert.Equal(original, await File.ReadAllBytesAsync(publication, Ct));
        Assert.False(File.Exists(directory.Path));
        int publications = 0;
        Assert.True(EncryptionReplacementIntent.Recover(directory.Path, phase => publications += phase == "publication-ready" ? 1 : 0, Ct));
        Assert.Equal(1, publications);
        Assert.Equal(original, await File.ReadAllBytesAsync(directory.Path, Ct));
        Assert.Equal(directory.Path, Assert.Single(Directory.GetFiles(directory.DirectoryPath)));
        Assert.False(EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
    }

    /// <summary>Faults in the shared copier leave an exact prefix that real Prepared recovery can validate and retry.</summary>
    /// <remarks>Faults target the helper directly; recovery's own I/O exception ordering and physical crash images remain separate obligations.</remarks>
    /// <param name="failure">The copier operation to interrupt.</param>
    /// <param name="stopAfter">The surviving prefix length, or complete length for a flush failure.</param>
    [Theory]
    [InlineData("eof", 0)]
    [InlineData("eof", 1)]
    [InlineData("eof", 65536)]
    [InlineData("read", 0)]
    [InlineData("read", 1)]
    [InlineData("read", 65536)]
    [InlineData("write", 0)]
    [InlineData("write", 1)]
    [InlineData("write", 65535)]
    [InlineData("write", 65536)]
    [InlineData("write", 65537)]
    [InlineData("write", 131088)]
    [InlineData("flush", 131089)]
    [InlineData("flush-persisted", 131089)]
    public async Task PreparedCopy_InterruptedIo_RetainsSelectedEvidenceAcrossRecoveryRetries(string failure, int stopAfter)
    {
        using var directory = new CopyDirectory();
        byte[] original = Image(131089);
        (string old, string next) = await PrepareMissingDestinationAsync(directory.Path, original);
        string publication = next.Replace(".new.tmp", ".publish.tmp", StringComparison.Ordinal);
        byte[] intent = await File.ReadAllBytesAsync(EncryptionReplacementIntent.IntentPath(directory.Path), Ct);
        byte[] replacement = await File.ReadAllBytesAsync(next, Ct);
        for (int attempt = 0; attempt < 2; attempt++)
        {
            File.Delete(publication);
            int readLimit = failure is "eof" or "read" ? stopAfter : int.MaxValue;
            await using (var source = new ObservedSource(old, 65536, readLimit, failure))
            {
                source.Position = 23;
                await using (var output = new InterruptedPublication(publication, failure, stopAfter))
                {
                    if (failure == "eof")
                    {
                        JetCorruptDataException error = Assert.Throws<JetCorruptDataException>(() => CopyForPublication(source, output));
                        Assert.Equal(JetErrorCode.CorruptRecoveryJournal, error.ErrorCode);
                    }
                    else
                    {
                        Assert.Throws<IOException>(() => CopyForPublication(source, output));
                    }

                    Assert.Equal(stopAfter, output.BytesWritten);
                    Assert.Equal(failure is "flush" or "flush-persisted", output.FlushAttempted);
                    Assert.Equal(failure == "flush-persisted", output.FlushSucceeded);
                }

                Assert.Equal(23, source.Position);
            }

            // Disposal may persist buffered bytes. This checks an exception survivor, not a power-loss image.
            Assert.Equal(original.AsSpan(0, stopAfter).ToArray(), await File.ReadAllBytesAsync(publication, Ct));
            Assert.False(File.Exists(directory.Path));
            Assert.Equal(intent, await File.ReadAllBytesAsync(EncryptionReplacementIntent.IntentPath(directory.Path), Ct));
            Assert.Equal(original, await File.ReadAllBytesAsync(old, Ct));
            Assert.Equal(replacement, await File.ReadAllBytesAsync(next, Ct));

            // Real recovery admits the helper's survivor, recreates the full copy, then retains it at the observed boundary.
            Assert.Throws<IOException>(() => EncryptionReplacementIntent.Recover(
                directory.Path,
                phase =>
                {
                    if (phase == "publication-ready")
                    {
                        throw new IOException("Interrupted Prepared recovery after exact copy and flush.");
                    }
                },
                Ct));
            Assert.False(File.Exists(directory.Path));
            Assert.Equal(original, await File.ReadAllBytesAsync(publication, Ct));
            Assert.Equal(intent, await File.ReadAllBytesAsync(EncryptionReplacementIntent.IntentPath(directory.Path), Ct));
            Assert.Equal(original, await File.ReadAllBytesAsync(old, Ct));
            Assert.Equal(replacement, await File.ReadAllBytesAsync(next, Ct));
        }

        Assert.True(EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
        Assert.Equal(original, await File.ReadAllBytesAsync(directory.Path, Ct));
        Assert.Equal(directory.Path, Assert.Single(Directory.GetFiles(directory.DirectoryPath)));
        Assert.False(EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
    }

    /// <summary>A conflicting byte in a partial copier write refuses before recovery authority acquisition or mutation.</summary>
    /// <param name="stopAfter">The prefix length at the partial-write exception.</param>
    [Theory]
    [InlineData(1)]
    [InlineData(65537)]
    [InlineData(131088)]
    public async Task PreparedCopy_ConflictingPartialWrite_RefusesRepeatedlyWithoutDeletingEvidence(int stopAfter)
    {
        using var directory = new CopyDirectory();
        byte[] original = Image(131089);
        (string old, string next) = await PrepareMissingDestinationAsync(directory.Path, original);
        string publication = next.Replace(".new.tmp", ".publish.tmp", StringComparison.Ordinal);
        byte[] intent = await File.ReadAllBytesAsync(EncryptionReplacementIntent.IntentPath(directory.Path), Ct);
        byte[] replacement = await File.ReadAllBytesAsync(next, Ct);
        await using (var source = new FileStream(old, FileMode.Open, FileAccess.Read, FileShare.None))
        await using (var output = new InterruptedPublication(publication, "write", stopAfter))
        {
            Assert.Throws<IOException>(() => CopyForPublication(source, output));
        }

        byte[] conflict = await File.ReadAllBytesAsync(publication, Ct);
        Assert.Equal(stopAfter, conflict.Length);
        conflict[^1] = 252; // Neither retained image contains this value.
        await File.WriteAllBytesAsync(publication, conflict, Ct);
        for (int attempt = 0; attempt < 2; attempt++)
        {
            int validated = 0;
            JetCorruptDataException error = Assert.Throws<JetCorruptDataException>(() => EncryptionReplacementIntent.Recover(directory.Path, phase => validated += phase == "validated" ? 1 : 0, Ct));
            Assert.Equal(JetErrorCode.CorruptRecoveryJournal, error.ErrorCode);
            Assert.Equal(0, validated);
            Assert.False(File.Exists(directory.Path));
            Assert.Equal(4, Directory.GetFiles(directory.DirectoryPath).Length);
            Assert.Equal(conflict, await File.ReadAllBytesAsync(publication, Ct));
            Assert.Equal(intent, await File.ReadAllBytesAsync(EncryptionReplacementIntent.IntentPath(directory.Path), Ct));
            Assert.Equal(original, await File.ReadAllBytesAsync(old, Ct));
            Assert.Equal(replacement, await File.ReadAllBytesAsync(next, Ct));
        }

        await File.WriteAllBytesAsync(publication, original.AsMemory(0, stopAfter), Ct);
        Assert.True(EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
        Assert.Equal(original, await File.ReadAllBytesAsync(directory.Path, Ct));
        Assert.Equal(directory.Path, Assert.Single(Directory.GetFiles(directory.DirectoryPath)));
    }

    private static async Task<(string Old, string Next)> PrepareMissingDestinationAsync(string path, byte[] original)
    {
        await File.WriteAllBytesAsync(path, original, Ct);
        using var owner = EncryptionReplacementIntent.Begin(
            path,
            boundary: phase =>
            {
                if (phase == "renamed")
                {
                    throw new IOException("Interrupted before the replacement commit decision.");
                }
            },
            cancellationToken: Ct);
        await using (FileStream next = owner.CreateReplacement())
        {
            await next.WriteAsync(new byte[] { 253, 254, 255 }, Ct);
        }

        Assert.Throws<IOException>(() => owner.Commit(Ct));
        Assert.True(owner.IsPrepared);
        string oldPath = owner.OldPath;
        string nextPath = owner.NewPath;
        owner.Dispose();
        File.Delete(path);
        return (oldPath, nextPath);
    }

    private static void CopyForPublication(FileStream source, FileStream output)
    {
        // Exercise the exact helper called by Publish without adding a production file-creation seam.
        MethodInfo copy = typeof(EncryptionReplacementIntent).GetMethod("Copy", BindingFlags.NonPublic | BindingFlags.Static)!;
        try
        {
            _ = copy.Invoke(null, [source, output, Ct]);
        }
        catch (TargetInvocationException error) when (error.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(error.InnerException).Throw();
        }
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

    private sealed class InterruptedPublication(string path, string failure, int stopAfter)
        : FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None)
    {
        internal int BytesWritten { get; private set; }

        internal bool FlushAttempted { get; private set; }

        internal bool FlushSucceeded { get; private set; }

        public override void Write(byte[] buffer, int offset, int count)
        {
            int written = failure == "write" ? Math.Min(count, stopAfter - this.BytesWritten) : count;
            base.Write(buffer, offset, written);
            this.BytesWritten += written;
            if (written < count)
            {
                throw new IOException("Injected partial replacement publication write.");
            }
        }

        public override void Flush(bool flushToDisk)
        {
            this.FlushAttempted = true;
            if (failure == "flush")
            {
                throw new IOException("Injected replacement publication flush failure before its persistence request.");
            }

            base.Flush(flushToDisk);
            this.FlushSucceeded = true;
            if (failure == "flush-persisted")
            {
                throw new IOException("Injected replacement publication flush failure after its persistence request succeeded.");
            }
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
