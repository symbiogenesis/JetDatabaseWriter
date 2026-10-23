namespace JetDatabaseWriter.Tests.Transactions;

using System;
using System.IO;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Transactions;
using Xunit;

/// <summary>Checks exact generation ownership independently of journal recovery.</summary>
public sealed class RecoverableWriterLockTests
{
    /// <summary>Failed acquisition keeps its prepared ownership proof when lock deletion fails.</summary>
    [Fact]
    public void Acquire_CleanupBlocked_PreservesPreparedReceiptUntilRetry()
    {
        using var directory = new LockDirectory();
        IOException error = Assert.Throws<IOException>(() => RecoverableWriterLock.Acquire(
            directory.Path,
            "host",
            "user",
            boundary =>
            {
                if (boundary == "prepared")
                {
                    throw new IOException("Prepared acquisition failed.");
                }

                Assert.Equal("lock-cleanup", boundary);
                throw new IOException("Lock cleanup refused.");
            }));
        Assert.Equal("Prepared acquisition failed.", error.Message);
        Assert.True(File.Exists(LockFileSlotWriter.GetLockFilePath(directory.Path)));
        Assert.True(File.Exists(RecoverableWriterLock.ReceiptPath(directory.Path)));
        Assert.True(RecoverableWriterLock.Recover(directory.Path));
        Assert.False(File.Exists(LockFileSlotWriter.GetLockFilePath(directory.Path)));
        Assert.False(File.Exists(RecoverableWriterLock.ReceiptPath(directory.Path)));
    }

    /// <summary>Only an exact lock generation can be recovered, even when ordinary identity slots are identical.</summary>
    [Fact]
    public void Recover_ReceiptFromEarlierGeneration_PreservesForeignLock()
    {
        using var directory = new LockDirectory();
        (byte[] oldLock, byte[] oldReceipt) = Capture(directory.Path);
        (byte[] newLock, _) = Capture(directory.Path);
        Assert.Equal(oldLock.AsSpan(0, 64).ToArray(), newLock.AsSpan(0, 64).ToArray());
        Assert.NotEqual(oldLock, newLock);
        Restore(directory.Path, newLock, oldReceipt);
        Assert.Throws<JetLockException>(() => RecoverableWriterLock.Recover(directory.Path));
        Assert.Equal(newLock, File.ReadAllBytes(LockFileSlotWriter.GetLockFilePath(directory.Path)));
        Assert.Equal(oldReceipt, File.ReadAllBytes(RecoverableWriterLock.ReceiptPath(directory.Path)));
    }

    /// <summary>Incomplete, corrupt and wrong-path receipts never authorize cleanup.</summary>
    /// <param name="damage">The independently chosen receipt damage.</param>
    [Theory]
    [InlineData("partial")]
    [InlineData("checksum")]
    [InlineData("path")]
    public void Recover_InvalidReceipt_PreservesEvidence(string damage)
    {
        using var directory = new LockDirectory();
        (byte[] lockBytes, byte[] receiptBytes) = Capture(directory.Path);
        string path = directory.Path;
        if (damage == "partial")
        {
            receiptBytes = receiptBytes.AsSpan(0, 17).ToArray();
        }
        else if (damage == "checksum")
        {
            receiptBytes[^1] ^= 1;
        }
        else
        {
            path += "-other.accdb";
        }

        Restore(path, lockBytes, receiptBytes);
        Assert.Throws<JetLockException>(() => RecoverableWriterLock.Recover(path));
        Assert.Equal(lockBytes, File.ReadAllBytes(LockFileSlotWriter.GetLockFilePath(path)));
        Assert.Equal(receiptBytes, File.ReadAllBytes(RecoverableWriterLock.ReceiptPath(path)));
    }

    /// <summary>A matching receipt does not permit deletion while the writer or any ordinary lock holder survives.</summary>
    [Fact]
    public void Recover_LiveHolder_PreservesEvidenceUntilRetry()
    {
        using var directory = new LockDirectory();
        using (var owner = RecoverableWriterLock.Acquire(directory.Path, "host", "user"))
        {
            Assert.Throws<IOException>(() => RecoverableWriterLock.Recover(directory.Path));
        }

        (byte[] lockBytes, byte[] receiptBytes) = Capture(directory.Path);
        Restore(directory.Path, lockBytes, receiptBytes);
        using (var holder = new FileStream(LockFileSlotWriter.GetLockFilePath(directory.Path), FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
        {
            Assert.Throws<IOException>(() => RecoverableWriterLock.Recover(directory.Path));
            Assert.Equal(receiptBytes, File.ReadAllBytes(RecoverableWriterLock.ReceiptPath(directory.Path)));
        }

        Assert.True(RecoverableWriterLock.Recover(directory.Path));
        Assert.False(File.Exists(LockFileSlotWriter.GetLockFilePath(directory.Path)));
        Assert.False(File.Exists(RecoverableWriterLock.ReceiptPath(directory.Path)));
        Assert.False(RecoverableWriterLock.Recover(directory.Path));
    }

    /// <summary>A surviving receipt can clean up after an interrupted lock deletion.</summary>
    [Fact]
    public void Recover_LockAlreadyDeleted_RemovesOnlyValidatedReceipt()
    {
        using var directory = new LockDirectory();
        (_, byte[] receiptBytes) = Capture(directory.Path);
        File.WriteAllBytes(RecoverableWriterLock.ReceiptPath(directory.Path), receiptBytes);
        Assert.True(RecoverableWriterLock.Recover(directory.Path));
        Assert.False(File.Exists(RecoverableWriterLock.ReceiptPath(directory.Path)));
    }

    /// <summary>Completed owner disposal cannot delete a successor generation.</summary>
    [Fact]
    public void Dispose_CompletedOwner_PreservesSuccessor()
    {
        using var directory = new LockDirectory();
        using var original = RecoverableWriterLock.Acquire(directory.Path, "host", "user");
        original.Dispose();
        using var successor = RecoverableWriterLock.Acquire(directory.Path, "host", "user");
        original.Dispose();
        Assert.True(File.Exists(LockFileSlotWriter.GetLockFilePath(directory.Path)));
        Assert.True(File.Exists(RecoverableWriterLock.ReceiptPath(directory.Path)));
    }

    /// <summary>Every incomplete receipt prefix retains both names through repeated recovery refusal.</summary>
    [Fact]
    public void AcquireRecovery_IncompleteReceiptCuts_PreserveEvidenceAcrossRetries()
    {
        using var directory = new LockDirectory();
        (byte[] lockBytes, byte[] receiptBytes) = Capture(directory.Path);
        for (int length = 0; length < receiptBytes.Length; length++)
        {
            byte[] partialReceipt = receiptBytes.AsSpan(0, length).ToArray();
            Restore(directory.Path, lockBytes, partialReceipt);
            for (int retry = 0; retry < 2; retry++)
            {
                Assert.Throws<JetLockException>(() => RecoverableWriterLock.AcquireRecovery(directory.Path));
                Assert.Equal(lockBytes, File.ReadAllBytes(LockFileSlotWriter.GetLockFilePath(directory.Path)));
                Assert.Equal(partialReceipt, File.ReadAllBytes(RecoverableWriterLock.ReceiptPath(directory.Path)));
            }
        }
    }

    /// <summary>A valid receipt cannot authorize replacing any incomplete native lock prefix.</summary>
    [Fact]
    public void AcquireRecovery_IncompleteNativeCuts_PreserveEvidenceAcrossRetries()
    {
        using var directory = new LockDirectory();
        (byte[] lockBytes, byte[] receiptBytes) = Capture(directory.Path);
        for (int length = 0; length < lockBytes.Length; length++)
        {
            byte[] partialLock = lockBytes.AsSpan(0, length).ToArray();
            Restore(directory.Path, partialLock, receiptBytes);
            for (int retry = 0; retry < 2; retry++)
            {
                Assert.Throws<JetLockException>(() => RecoverableWriterLock.AcquireRecovery(directory.Path));
                Assert.Equal(partialLock, File.ReadAllBytes(LockFileSlotWriter.GetLockFilePath(directory.Path)));
                Assert.Equal(receiptBytes, File.ReadAllBytes(RecoverableWriterLock.ReceiptPath(directory.Path)));
            }
        }
    }

    /// <summary>Failed fresh authority bootstrap retains the unexplained native file and refuses retry.</summary>
    [Fact]
    public void AcquireRecovery_InterruptedFreshBootstrap_PreservesNativeOnlyEvidence()
    {
        using var directory = new LockDirectory();
        IOException error = Assert.Throws<IOException>(() => RecoverableWriterLock.AcquireRecovery(
            directory.Path,
            boundary =>
            {
                Assert.Equal("recovery-lock-created", boundary);
                throw new IOException("Authority bootstrap interrupted.");
            }));
        Assert.Equal("Authority bootstrap interrupted.", error.Message);
        string nativePath = LockFileSlotWriter.GetLockFilePath(directory.Path);
        byte[] nativeBytes = File.ReadAllBytes(nativePath);
        Assert.NotEmpty(nativeBytes);
        Assert.False(File.Exists(RecoverableWriterLock.ReceiptPath(directory.Path)));
        for (int retry = 0; retry < 2; retry++)
        {
            Assert.Throws<JetLockException>(() => RecoverableWriterLock.AcquireRecovery(directory.Path));
            Assert.Equal(nativeBytes, File.ReadAllBytes(nativePath));
            Assert.False(File.Exists(RecoverableWriterLock.ReceiptPath(directory.Path)));
        }
    }

    /// <summary>An exact surviving receipt reconstructs the missing native generation before cleanup.</summary>
    [Fact]
    public void AcquireRecovery_ReceiptWithoutNative_ReconstructsExactGeneration()
    {
        using var directory = new LockDirectory();
        (byte[] lockBytes, byte[] receiptBytes) = Capture(directory.Path);
        File.WriteAllBytes(RecoverableWriterLock.ReceiptPath(directory.Path), receiptBytes);
        using (RecoverableWriterLock.RecoveryLease lease = RecoverableWriterLock.AcquireRecovery(directory.Path))
        {
            // Recovery keeps exclusive handles until cleanup, so inspect after
            // disposal rather than relaxing the lease's sharing contract.
        }

        Assert.Equal(lockBytes, File.ReadAllBytes(LockFileSlotWriter.GetLockFilePath(directory.Path)));
        Assert.Equal(receiptBytes, File.ReadAllBytes(RecoverableWriterLock.ReceiptPath(directory.Path)));
        using RecoverableWriterLock.RecoveryLease retry = RecoverableWriterLock.AcquireRecovery(directory.Path);
        retry.Cleanup();
        Assert.False(File.Exists(LockFileSlotWriter.GetLockFilePath(directory.Path)));
        Assert.False(File.Exists(RecoverableWriterLock.ReceiptPath(directory.Path)));
    }

    private static (byte[] Lock, byte[] Receipt) Capture(string path)
    {
        using var owner = RecoverableWriterLock.Acquire(path, "host", "user");
        return (ReadShared(LockFileSlotWriter.GetLockFilePath(path)), ReadShared(RecoverableWriterLock.ReceiptPath(path)));
    }

    private static byte[] ReadShared(string path)
    {
        using var input = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        byte[] bytes = new byte[checked((int)input.Length)];
        input.ReadExactly(bytes);
        return bytes;
    }

    private static void Restore(string path, byte[] lockBytes, byte[] receiptBytes)
    {
        File.WriteAllBytes(LockFileSlotWriter.GetLockFilePath(path), lockBytes);
        File.WriteAllBytes(RecoverableWriterLock.ReceiptPath(path), receiptBytes);
    }

    private sealed class LockDirectory : IDisposable
    {
        private readonly string directory = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "jdw-lock-receipt-" + Guid.NewGuid().ToString("N"));

        internal LockDirectory()
        {
            Directory.CreateDirectory(this.directory);
            this.Path = System.IO.Path.Combine(this.directory, "database.accdb");
        }

        internal string Path { get; }

        public void Dispose() => Directory.Delete(this.directory, recursive: true);
    }
}
