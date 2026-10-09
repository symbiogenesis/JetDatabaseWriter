namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Transactions;
using Xunit;

/// <summary>Checks retryable recovery when lock-file cleanup is refused.</summary>
public sealed class RecoveryLockFileTests
{
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
            await Assert.ThrowsAsync<IOException>(async () =>
            {
                await using AccessWriter rejected = await AccessWriter.OpenAsync(path, cancellationToken: TestContext.Current.CancellationToken);
            });
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
}
