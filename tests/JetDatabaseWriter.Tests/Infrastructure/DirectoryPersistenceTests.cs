namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Transactions;
using Xunit;

/// <summary>Directory fences require an actual successful managed Windows flush.</summary>
public sealed class DirectoryPersistenceTests
{
    /// <summary>Writable directory handles can fence a newly published child name.</summary>
    [Fact]
    public async Task FlushParent_AcknowledgesOnlySupportedDirectoryFlushes()
    {
        string directory = Path.Combine(Path.GetTempPath(), "jdw-directory-fence-" + Guid.NewGuid().ToString("N"));
        string child = Path.Combine(directory, "child.bin");
        Directory.CreateDirectory(directory);
        try
        {
            await File.WriteAllBytesAsync(child, [1, 2, 3], TestContext.Current.CancellationToken);

            bool acknowledged = DirectoryPersistence.FlushParent(child);

            Assert.Equal(OperatingSystem.IsWindows() && !LibraryTarget.IsNetStandard, acknowledged);
        }
        finally
        {
            File.Delete(child);
            Directory.Delete(directory);
        }
    }

    /// <summary>The runtime's suppressed access-denied error cannot count as a successful fence.</summary>
    [Fact]
    public void ReadOnlyDirectoryHandle_RefusesSuppressedFlushFailure()
    {
        if (!DirectoryPersistence.IsSupported)
        {
            Assert.False(OperatingSystem.IsWindows() && !LibraryTarget.IsNetStandard);
            return;
        }

        string directory = Path.Combine(Path.GetTempPath(), "jdw-directory-fence-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            using Microsoft.Win32.SafeHandles.SafeFileHandle handle = File.OpenHandle(directory, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, (FileOptions)0x02000000);

            IOException error = Assert.Throws<IOException>(() => DirectoryPersistence.FlushHandle(handle));

            Assert.Equal(5, error.HResult & 0xFFFF);
        }
        finally
        {
            Directory.Delete(directory);
        }
    }

    /// <summary>A failed publication fence retains the complete destination and reports failure.</summary>
    [Fact]
    public async Task Creation_DirectoryFenceFailureRetainsPublishedBytes()
    {
        string directory = Path.Combine(Path.GetTempPath(), "jdw-directory-fence-" + Guid.NewGuid().ToString("N"));
        string child = Path.Combine(directory, "child.bin");
        Directory.CreateDirectory(directory);
        try
        {
            bool acknowledged = false;
            await Assert.ThrowsAsync<IOException>(async () => await DatabaseCreationPublication.PublishAsync(
                child,
                static async (stream, ct) => await stream.WriteAsync(new byte[] { 1, 2, 3 }, ct),
                (phase, _) =>
                {
                    if (phase == "DirectoryFlushed")
                    {
                        acknowledged = true;
                    }

                    if (phase == "DirectoryFlushing")
                    {
                        throw new IOException("Refuse the namespace acknowledgement.");
                    }
                },
                TestContext.Current.CancellationToken));

            Assert.False(acknowledged);
            Assert.Equal(new byte[] { 1, 2, 3 }, await File.ReadAllBytesAsync(child, TestContext.Current.CancellationToken));
            Assert.Equal(child, Assert.Single(Directory.GetFiles(directory)));
        }
        finally
        {
            File.Delete(child);
            Directory.Delete(directory);
        }
    }

    /// <summary>Ownership is never returned before its generation names reach the directory fence.</summary>
    [Fact]
    public void Ownership_PreparationFenceFailureCannotReturnAuthority()
    {
        string directory = Path.Combine(Path.GetTempPath(), "jdw-directory-fence-" + Guid.NewGuid().ToString("N"));
        string child = Path.Combine(directory, "child.mdb");
        Directory.CreateDirectory(directory);
        try
        {
            Assert.Throws<IOException>(() =>
            {
                using var owner = RecoverableWriterLock.Acquire(child, "host", "user", phase =>
                {
                    if (phase == "prepared-before-directory-flush")
                    {
                        throw new IOException("Refuse the namespace acknowledgement.");
                    }
                });
            });

            Assert.Empty(Directory.GetFiles(directory));
        }
        finally
        {
            File.Delete(Path.ChangeExtension(child, ".ldb"));
            File.Delete(RecoverableWriterLock.ReceiptPath(child));
            Directory.Delete(directory);
        }
    }

    /// <summary>A retry must fence native absence before retiring its surviving receipt.</summary>
    [Fact]
    public void Ownership_AbsentNativeFenceFailureRetainsReceiptUntilRetry()
    {
        string directory = Path.Combine(Path.GetTempPath(), "jdw-directory-fence-" + Guid.NewGuid().ToString("N"));
        string child = Path.Combine(directory, "child.mdb");
        string receipt = RecoverableWriterLock.ReceiptPath(child);
        Directory.CreateDirectory(directory);
        try
        {
            using (RecoverableWriterLock.RecoveryLease ownership = RecoverableWriterLock.AcquireRecovery(child))
            {
            }

            File.Delete(LockFileSlotWriter.GetLockFilePath(child));
            byte[] retained = File.ReadAllBytes(receipt);
            if (DirectoryPersistence.IsSupported)
            {
                // Deny the writable directory acquisition required by the fence,
                // while permitting independent child-file cleanup operations.
                using Microsoft.Win32.SafeHandles.SafeFileHandle blocker = File.OpenHandle(directory, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, (FileOptions)0x02000000);
                Assert.Throws<IOException>(() => DirectoryPersistence.FlushParent(child));

                Assert.Throws<IOException>(() => RecoverableWriterLock.Recover(child));

                Assert.Equal(retained, File.ReadAllBytes(receipt));
                Assert.False(File.Exists(LockFileSlotWriter.GetLockFilePath(child)));
            }

            Assert.True(RecoverableWriterLock.Recover(child));
            Assert.False(File.Exists(receipt));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Failed acquisition retains its receipt when native retirement cannot be fenced.</summary>
    [Fact]
    public void Ownership_AcquisitionCleanupFenceFailureRetainsReceiptUntilRetry()
    {
        if (!DirectoryPersistence.IsSupported)
        {
            Assert.False(OperatingSystem.IsWindows() && !LibraryTarget.IsNetStandard);
            return;
        }

        string directory = Path.Combine(Path.GetTempPath(), "jdw-directory-fence-" + Guid.NewGuid().ToString("N"));
        string child = Path.Combine(directory, "child.mdb");
        string receipt = RecoverableWriterLock.ReceiptPath(child);
        Directory.CreateDirectory(directory);
        try
        {
            using (Microsoft.Win32.SafeHandles.SafeFileHandle blocker = File.OpenHandle(directory, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, (FileOptions)0x02000000))
            {
                Assert.Throws<IOException>(() => DirectoryPersistence.FlushParent(child));

                Assert.Throws<IOException>(() => RecoverableWriterLock.Acquire(child, "host", "user"));

                Assert.False(File.Exists(LockFileSlotWriter.GetLockFilePath(child)));
                Assert.True(File.Exists(receipt));
            }

            Assert.True(RecoverableWriterLock.Recover(child));
            Assert.False(File.Exists(receipt));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
