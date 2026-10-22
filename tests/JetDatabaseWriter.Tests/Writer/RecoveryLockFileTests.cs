namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Transactions;
using Xunit;

/// <summary>Checks retryable recovery when lock-file cleanup is refused.</summary>
public sealed class RecoveryLockFileTests
{
    /// <summary>Disabling new lock creation must still clean validated ownership left by an interrupted writer.</summary>
    /// <param name="pendingJournal">Whether the interrupted writer left an undecided transaction.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Open_LockFileDisabled_CleansValidatedOwnershipReceipt(bool pendingJournal)
    {
        string directory = Path.Combine(Path.GetTempPath(), $"RecoveryLockFileTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "database.mdb");
        try
        {
            await using (AccessWriter created = await AccessWriter.CreateDatabaseAsync(path, DatabaseFormat.Jet4Mdb, cancellationToken: TestContext.Current.CancellationToken))
            {
            }

            byte[] original = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
            LeaveOwnershipReceipt(path);
            if (pendingJournal)
            {
                await using var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                using var journal = PersistentRollbackJournal.Create(database, 4096);
                journal.RecordBeforeTruncate(original.Length - 4096);
                database.SetLength(original.Length - 4096);
#pragma warning disable CA1849 // The modeled interrupted raw state needs a device flush before public recovery.
                database.Flush(true);
#pragma warning restore CA1849
            }

            await using (AccessWriter reopened = await AccessWriter.OpenAsync(path, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
            {
                Assert.False(File.Exists(LockFileSlotWriter.GetLockFilePath(path)));
                Assert.False(File.Exists(RecoverableWriterLock.ReceiptPath(path)));
                Assert.False(File.Exists(PersistentRollbackJournal.JournalPath(path)));
            }

            Assert.Equal(original, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
            await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
            Assert.Empty(await reader.ListTablesAsync(TestContext.Current.CancellationToken));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Disabling new lock creation does not authorize ignoring invalid ownership evidence.</summary>
    [Fact]
    public async Task Open_LockFileDisabled_InvalidOwnershipReceiptPreservesEvidence()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"RecoveryLockFileTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "database.mdb");
        try
        {
            await using (AccessWriter created = await AccessWriter.CreateDatabaseAsync(path, DatabaseFormat.Jet4Mdb, cancellationToken: TestContext.Current.CancellationToken))
            {
            }

            byte[] original = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
            LeaveOwnershipReceipt(path);
            string receiptPath = RecoverableWriterLock.ReceiptPath(path);
            byte[] receipt = await File.ReadAllBytesAsync(receiptPath, TestContext.Current.CancellationToken);
            receipt[^1] ^= 1;
            await File.WriteAllBytesAsync(receiptPath, receipt, TestContext.Current.CancellationToken);
            byte[] lockBytes = await File.ReadAllBytesAsync(LockFileSlotWriter.GetLockFilePath(path), TestContext.Current.CancellationToken);
            JetLockException error = await Assert.ThrowsAsync<JetLockException>(async () =>
            {
                await using AccessWriter rejected = await AccessWriter.OpenAsync(path, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
            });
            Assert.Equal(JetErrorCode.DatabaseInUse, error.ErrorCode);
            Assert.Equal(original, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
            Assert.Equal(receipt, await File.ReadAllBytesAsync(receiptPath, TestContext.Current.CancellationToken));
            Assert.Equal(lockBytes, await File.ReadAllBytesAsync(LockFileSlotWriter.GetLockFilePath(path), TestContext.Current.CancellationToken));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>A valid database journal cannot establish ownership of an unrelated populated lock file.</summary>
    [Fact]
    public async Task Open_ValidJournalWithoutOwnershipReceipt_PreservesPopulatedLock()
    {
        string path = Path.Combine(Path.GetTempPath(), $"RecoveryLockFileTests_{Guid.NewGuid():N}.mdb");
        string lockPath = LockFileSlotWriter.GetLockFilePath(path);
        byte[] lockBytes = new byte[64];
        lockBytes[0] = 71;
        try
        {
            await using (AccessWriter created = await AccessWriter.CreateDatabaseAsync(path, DatabaseFormat.Jet4Mdb, cancellationToken: TestContext.Current.CancellationToken))
            {
            }

            await using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                using var journal = PersistentRollbackJournal.Create(database, 4096);
            }

            await File.WriteAllBytesAsync(lockPath, lockBytes, TestContext.Current.CancellationToken);
            JetLockException error = await Assert.ThrowsAsync<JetLockException>(async () =>
            {
                await using AccessWriter rejected = await AccessWriter.OpenAsync(path, cancellationToken: TestContext.Current.CancellationToken);
            });
            Assert.Equal(JetErrorCode.DatabaseInUse, error.ErrorCode);
            Assert.Equal(lockBytes, await File.ReadAllBytesAsync(lockPath, TestContext.Current.CancellationToken));
            Assert.True(File.Exists(PersistentRollbackJournal.JournalPath(path)));
        }
        finally
        {
            File.Delete(path);
            File.Delete(PersistentRollbackJournal.JournalPath(path));
            File.Delete(lockPath);
        }
    }

    /// <summary>A surviving lock-file holder must not consume recovery evidence.</summary>
    [Fact]
    public async Task Open_LiveLockFile_PreservesJournalUntilRetry()
    {
        string path = Path.Combine(Path.GetTempPath(), $"RecoveryLockFileTests_{Guid.NewGuid():N}.mdb");
        string lockPath = LockFileSlotWriter.GetLockFilePath(path);
        try
        {
            await using (AccessWriter created = await AccessWriter.CreateDatabaseAsync(path, DatabaseFormat.Jet4Mdb, cancellationToken: TestContext.Current.CancellationToken))
            {
            }

            byte[] original = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
            await using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                using var journal = PersistentRollbackJournal.Create(database, 4096);
                await using var store = new StreamPageStore(database, leaveOpen: true) { RollbackJournal = journal };
                await store.SetLengthAsync(original.Length - 4096, TestContext.Current.CancellationToken);
            }

            await File.WriteAllBytesAsync(lockPath, new byte[64], TestContext.Current.CancellationToken);
            await using (var holder = new FileStream(lockPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                await Assert.ThrowsAsync<IOException>(async () =>
                {
                    await using AccessWriter rejected = await AccessWriter.OpenAsync(path, cancellationToken: TestContext.Current.CancellationToken);
                });
                Assert.True(File.Exists(path + ".jdw-journal"));
                Assert.True(File.Exists(lockPath));
            }

            await using (AccessWriter reopened = await AccessWriter.OpenAsync(path, cancellationToken: TestContext.Current.CancellationToken))
            {
            }

            Assert.False(File.Exists(path + ".jdw-journal"));
            Assert.Equal(original, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
            File.Delete(lockPath);
        }
    }

    /// <summary>Invalid recovery evidence must leave the existing lock file intact.</summary>
    [Fact]
    public async Task Open_InvalidJournal_PreservesLockFile()
    {
        string path = Path.Combine(Path.GetTempPath(), $"RecoveryLockFileTests_{Guid.NewGuid():N}.mdb");
        string lockPath = LockFileSlotWriter.GetLockFilePath(path);
        byte[] lockBytes = [1, 2, 3, 4];
        try
        {
            await using (AccessWriter created = await AccessWriter.CreateDatabaseAsync(path, DatabaseFormat.Jet4Mdb, cancellationToken: TestContext.Current.CancellationToken))
            {
            }

            await File.WriteAllBytesAsync(path + ".jdw-journal", [1], TestContext.Current.CancellationToken);
            await File.WriteAllBytesAsync(lockPath, lockBytes, TestContext.Current.CancellationToken);
            JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            {
                await using AccessWriter rejected = await AccessWriter.OpenAsync(path, cancellationToken: TestContext.Current.CancellationToken);
            });
            Assert.Equal(JetErrorCode.CorruptRecoveryJournal, failure.ErrorCode);
            Assert.Equal(lockBytes, await File.ReadAllBytesAsync(lockPath, TestContext.Current.CancellationToken));
            Assert.True(File.Exists(path + ".jdw-journal"));
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
            File.Delete(lockPath);
        }
    }

    private static void LeaveOwnershipReceipt(string path)
        => Assert.Throws<IOException>(() => RecoverableWriterLock.Acquire(path, "host", "user", static boundary => throw new IOException("Interrupted ownership " + boundary)));
}
