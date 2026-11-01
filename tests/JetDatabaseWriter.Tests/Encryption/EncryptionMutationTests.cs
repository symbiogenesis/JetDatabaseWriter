namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Transactions;
using Xunit;

/// <summary>Exercises native file maintenance and encrypted bootstrap creation.</summary>
public sealed class EncryptionMutationTests
{
    /// <summary>Maintenance releases only its own best-effort lock slot after success or password refusal.</summary>
    /// <param name="refused">Whether the password is deliberately incorrect.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ChangePassword_BestEffortLock_ReleasesItsSlotAndSource(bool refused)
    {
        string directory = Path.Combine(Path.GetTempPath(), $"EncryptionLockOwnership_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "native.mdb");
        string lockPath = LockFileSlotWriter.GetLockFilePath(path);
        try
        {
            File.Copy(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Rc4.mdb"), path);
            byte[] original = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
            using (var other = LockFileSlotWriter.Open(path, "OtherOwner", respectExisting: false, "OtherMachine", "OtherUser"))
            {
                Assert.NotNull(other);
                byte[] firstSlot = await ReadLockSlotsAsync(lockPath);
                var options = new AccessWriterOptions { RespectExistingLockFile = false };
                if (refused)
                {
                    await StructuredFormatAssert.ThrowsAsync<JetAccessDeniedException>(JetErrorCode.PasswordIncorrect, async () => await AccessDatabaseEncryption.ChangePasswordAsync(path, "Wrong123".AsMemory(), "Changed123".AsMemory(), options, TestContext.Current.CancellationToken));
                    Assert.Equal(original, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
                }
                else
                {
                    await AccessDatabaseEncryption.ChangePasswordAsync(path, "Native123".AsMemory(), "Changed123".AsMemory(), options, TestContext.Current.CancellationToken);
                }

                byte[] slots = await ReadLockSlotsAsync(lockPath);
                Assert.Equal(LockFileSlotWriter.SlotSize * 2, slots.Length);
                Assert.Equal(firstSlot, slots.AsSpan(0, LockFileSlotWriter.SlotSize).ToArray());
                Assert.All(slots.AsSpan(LockFileSlotWriter.SlotSize).ToArray(), value => Assert.Equal(0, value));
            }

            Assert.False(File.Exists(lockPath));
            await using var exclusive = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            Assert.True(exclusive.Length > 0);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Creation writes native encryption before opening the writer.</summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Create_WithPassword_WritesEncryptedDatabase(DatabaseFormat format)
    {
        await using var stream = new FirstWriteStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions("Native123") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Created", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Created", [42], TestContext.Current.CancellationToken);
        }

        Assert.NotEqual(AccessEncryptionFormat.None, JetDatabaseWriter.Encryption.EncryptionConverter.Detect(stream.FirstWrite!));
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions("Native123"), leaveOpen: true, TestContext.Current.CancellationToken);
        DataTable rows = await reader.ReadTableAsync("Created", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(42, Assert.Single(rows.Rows.Cast<DataRow>())["Id"]);
    }

    /// <summary>Native encrypt, change-password and decrypt stages produce readable databases.</summary>
    /// <param name="fixture">The independently produced native fixture.</param>
    /// <param name="oldPassword">The fixture password.</param>
    [Theory]
    [InlineData("NativeJet3Rc4.mdb", "Pássword")]
    [InlineData("NativeJet4Rc4.mdb", "Native123")]
    [InlineData("NativeAceAgile.accdb", "Native123")]
    public async Task Maintenance_NativeFixture_ChangesAndRemovesPassword(string fixture, string oldPassword)
    {
        string directory = Path.Combine(Path.GetTempPath(), $"EncryptionMutationTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, fixture);
        try
        {
            File.Copy(Path.Combine(TestDatabases.EncryptedRoot, fixture), path);
            await AccessDatabaseEncryption.ChangePasswordAsync(path, oldPassword.AsMemory(), "Changed123".AsMemory(), new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
            await using (AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions("Changed123"), TestContext.Current.CancellationToken))
            {
                await AssertNativeRowAsync(reader);
            }

            await StructuredFormatAssert.ThrowsAsync<JetAccessDeniedException>(JetErrorCode.PasswordIncorrect, async () =>
            {
                await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions(oldPassword), TestContext.Current.CancellationToken);
            });
            await AccessDatabaseEncryption.DecryptAsync(path, "Changed123".AsMemory(), new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
            Assert.Equal(AccessEncryptionFormat.None, await AccessDatabaseEncryption.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
            await using (AccessReader reader = await AccessReader.OpenAsync(path, cancellationToken: TestContext.Current.CancellationToken))
            {
                await AssertNativeRowAsync(reader);
            }

            await AccessDatabaseEncryption.EncryptAsync(path, "Again123".AsMemory(), options: new AccessWriterOptions { UseLockFile = false }, cancellationToken: TestContext.Current.CancellationToken);
            await using (AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions("Again123"), TestContext.Current.CancellationToken))
            {
                await AssertNativeRowAsync(reader);
            }

            Assert.Empty(Directory.GetFiles(directory, "*.tmp"));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>A wrong password leaves ciphertext and the directory unchanged.</summary>
    [Fact]
    public async Task ChangePassword_WrongPassword_PreservesCiphertext()
    {
        string path = Path.Combine(Path.GetTempPath(), $"EncryptionMutationTests_{Guid.NewGuid():N}.mdb");
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Rc4.mdb"), TestContext.Current.CancellationToken);
        try
        {
            await File.WriteAllBytesAsync(path, original, TestContext.Current.CancellationToken);
            await StructuredFormatAssert.ThrowsAsync<JetAccessDeniedException>(JetErrorCode.PasswordIncorrect, async () => await AccessDatabaseEncryption.ChangePasswordAsync(path, "Wrong123".AsMemory(), "Changed123".AsMemory(), new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken));
            Assert.Equal(original, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
            Assert.Empty(Directory.GetFiles(Path.GetDirectoryName(path)!, Path.GetFileName(path) + ".reenc-*.tmp"));
        }
        finally
        {
            File.Delete(path);
        }
    }

    /// <summary>Opening and disposing native encrypted files preserves their complete ciphertext.</summary>
    /// <param name="fixture">The independently created native fixture.</param>
    [Theory]
    [InlineData("NativeJet4Rc4.mdb")]
    [InlineData("NativeAceAgile.accdb")]
    public async Task OpenDispose_NativeFixture_LeavesCiphertextUntouched(string fixture)
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, fixture), TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(original, TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions("Native123") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
        }

        Assert.Equal(original, stream.ToArray());
    }

    /// <summary>Maintenance refuses a pending recovery journal without changing either file.</summary>
    [Fact]
    public async Task ChangePassword_PendingJournal_PreservesDatabaseAndJournal()
    {
        string path = Path.Combine(Path.GetTempPath(), $"EncryptionMutationTests_{Guid.NewGuid():N}.mdb");
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Rc4.mdb"), TestContext.Current.CancellationToken);
        string journalPath = path + ".jdw-journal";
        try
        {
            await File.WriteAllBytesAsync(path, original, TestContext.Current.CancellationToken);
            await using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                using var journal = PersistentRollbackJournal.Create(database, 4096);
                journal.RecordBeforeWrite(4096, new byte[4096]);
            }

            byte[] journalBytes = await File.ReadAllBytesAsync(journalPath, TestContext.Current.CancellationToken);
            JetOperationException error = await Assert.ThrowsAsync<JetOperationException>(async () => await AccessDatabaseEncryption.ChangePasswordAsync(path, "Native123".AsMemory(), "Changed123".AsMemory(), new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken));
            Assert.Equal(JetErrorCode.HotJournalConflict, error.ErrorCode);
            Assert.Equal(original, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
            Assert.Equal(journalBytes, await File.ReadAllBytesAsync(journalPath, TestContext.Current.CancellationToken));
            Assert.Empty(Directory.GetFiles(Path.GetDirectoryName(path)!, Path.GetFileName(path) + ".reenc-*.tmp"));
        }
        finally
        {
            File.Delete(path);
            File.Delete(journalPath);
        }
    }

    /// <summary>Changing a password-only Jet4 database retains unencrypted pages.</summary>
    [Fact]
    public async Task ChangePassword_PasswordOnly_PreservesPageEncryptionState()
    {
        string path = Path.Combine(Path.GetTempPath(), $"EncryptionMutationTests_{Guid.NewGuid():N}.mdb");
        try
        {
            File.Copy(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Password.mdb"), path);
            await AccessDatabaseEncryption.ChangePasswordAsync(path, "Native123".AsMemory(), "Changed123".AsMemory(), new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
            Assert.Equal(AccessEncryptionFormat.None, await AccessDatabaseEncryption.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
            await using (AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions("Changed123"), TestContext.Current.CancellationToken))
            {
                await AssertNativeRowAsync(reader);
            }

            await StructuredFormatAssert.ThrowsAsync<JetAccessDeniedException>(JetErrorCode.PasswordIncorrect, async () =>
            {
                await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions("Native123"), TestContext.Current.CancellationToken);
            });
        }
        finally
        {
            File.Delete(path);
        }
    }

    private static async Task<byte[]> ReadLockSlotsAsync(string path)
    {
        await using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        byte[] bytes = new byte[checked((int)stream.Length)];
        await stream.ReadExactlyAsync(bytes, TestContext.Current.CancellationToken);
        return bytes;
    }

    private static async Task AssertNativeRowAsync(AccessReader reader)
    {
        DataTable rows = await reader.ReadTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        DataRow row = Assert.Single(rows.Rows.Cast<DataRow>());
        Assert.Equal(7, row["Id"]);
        Assert.Equal("Native encrypted row", row["Label"]);
    }

    private sealed class FirstWriteStream : MemoryStream
    {
        internal byte[]? FirstWrite { get; private set; }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, System.Threading.CancellationToken cancellationToken = default)
        {
            this.FirstWrite ??= buffer.ToArray();
            return base.WriteAsync(buffer, cancellationToken);
        }
    }
}
