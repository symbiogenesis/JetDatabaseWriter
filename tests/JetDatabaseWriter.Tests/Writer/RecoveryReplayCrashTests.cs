namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Transactions;
using Xunit;

#pragma warning disable CA1849 // Synchronous raw I/O and durable flushes locate exact process-termination boundaries.

/// <summary>Actual process deaths exercise raw recovery replay and cleanup, without asserting power-loss durability.</summary>
/// <param name="output">Reports retained evidence directories if a case fails.</param>
public sealed class RecoveryReplayCrashTests(ITestOutputHelper output)
{
    private const string ChildPath = "JDW_REPLAY_CRASH_PATH";
    private const string ChildBoundary = "JDW_REPLAY_CRASH_BOUNDARY";
    private const string ChildPassword = "JDW_REPLAY_CRASH_PASSWORD";
    private const string Method = "JetDatabaseWriter.Tests.Writer.RecoveryReplayCrashTests.ProcessInterruptedReplay_RestoresOriginalOrKeepsCommittedRawImage";

    private static readonly string[] RollbackBoundaries = ["start", "replay-marker", "partial-page", "partial-tail", "first-page", "second-page", "length", "database-flush", "decision", "before-journal-delete", "after-journal-delete"];
    private static readonly string[] CleanupBoundaries = ["decision", "before-journal-delete", "after-journal-delete"];
    private static readonly string[] ConflictBoundaries = ["partial-page", "decision"];
    private static readonly bool[] EncryptionStates = [false, true];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact(DisableParallelization = true)]
    public async Task ProcessInterruptedReplay_RestoresOriginalOrKeepsCommittedRawImage()
    {
        if (Environment.GetEnvironmentVariable(ChildPath) is { } childPath)
        {
            CrashRecoveryProcess.MarkReady(childPath);
            await RunChildAsync(childPath);
            Assert.Fail("The requested recovery boundary was not interrupted.");
            return;
        }

        await this.WithDirectoryAsync(async directory =>
        {
            (DatabaseFormat Format, string Fixture)[] images =
            [
                (DatabaseFormat.Jet3Mdb, string.Empty),
                (DatabaseFormat.Jet4Mdb, string.Empty),
                (DatabaseFormat.AceAccdb, string.Empty),
                (DatabaseFormat.Jet3Mdb, "UpstreamJet3Rc4.mdb"),
                (DatabaseFormat.Jet4Mdb, "NativeJet4Rc4.mdb"),
                (DatabaseFormat.AceAccdb, "NativeAceStandard.accdb"),
                (DatabaseFormat.AceAccdb, "NativeAceAgile.accdb"),
            ];
            for (int index = 0; index < images.Length; index++)
            {
                (DatabaseFormat format, string fixture) = images[index];
                string password = Password(format, fixture);
                string extension = format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb";
                string baselinePath = Path.Combine(directory, $"baseline-{index}{extension}");
                byte[] original = await CreateImageAsync(baselinePath, format, fixture, password);
                int pageSize = format == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
                foreach (string boundary in RollbackBoundaries)
                {
                    string path = Path.Combine(directory, $"rollback-{index}-{boundary}{extension}");
                    File.WriteAllBytes(path, original);
                    PrepareInterruptedImage(path, pageSize);
                    AssertChild(path, password, boundary);
                    if (boundary == "partial-tail")
                    {
                        Assert.InRange(new FileInfo(path).Length, original.Length - (2L * pageSize) + 1, original.Length - 1L);
                    }

                    if (boundary == "replay-marker")
                    {
                        byte[] pending = await File.ReadAllBytesAsync(PersistentRollbackJournal.JournalPath(path), Ct);
                        Assert.Equal(-3, BinaryPrimitives.ReadInt64LittleEndian(pending.AsSpan(pending.Length - pageSize - 40)));
                    }

                    bool cleaned = boundary == "after-journal-delete";
                    Assert.Equal(!cleaned, File.Exists(PersistentRollbackJournal.JournalPath(path)));
                    if (!cleaned)
                    {
                        await RejectReaderAsync(path, password);
                    }

                    if (boundary is "partial-page" or "partial-tail" or "decision")
                    {
                        // Kill a second recovering process against the same evidence.
                        AssertChild(path, password, boundary);
                        Assert.True(File.Exists(PersistentRollbackJournal.JournalPath(path)));
                    }

                    if (boundary is "database-flush" or "decision" or "before-journal-delete" or "after-journal-delete")
                    {
                        Assert.Equal(original, await File.ReadAllBytesAsync(path, Ct));
                    }

                    Assert.Equal(boundary is not "before-journal-delete" and not "after-journal-delete", File.Exists(LockFileSlotWriter.GetLockFilePath(path)));
                    await AssertReopensAsync(path, password, original, expectedRows: 1);
                }

                string committedPath = Path.Combine(directory, $"expected-{index}{extension}");
                File.WriteAllBytes(committedPath, original);
                await using (AccessWriter writer = await AccessWriter.OpenAsync(committedPath, Options(password), Ct))
                {
                    await writer.InsertRowAsync("ReplayRows", [2, "committed"], Ct);
                }

                byte[] committed = await File.ReadAllBytesAsync(committedPath, Ct);
                Assert.NotEqual(original, committed);
                foreach (string boundary in CleanupBoundaries)
                {
                    string path = Path.Combine(directory, $"committed-{index}-{boundary}{extension}");
                    PrepareCommittedImage(path, original, committed, pageSize);
                    AssertChild(path, password, boundary);
                    Assert.Equal(committed, await File.ReadAllBytesAsync(path, Ct));
                    Assert.Equal(boundary != "after-journal-delete", File.Exists(PersistentRollbackJournal.JournalPath(path)));
                    await AssertReopensAsync(path, password, committed, expectedRows: 2);
                }

                Assert.Equal(original, await File.ReadAllBytesAsync(baselinePath, Ct));
                output.WriteLine($"Completed rollback/committed replay process boundaries for {format}, provider specimen {fixture switch { "" => "unencrypted", _ => fixture }}.");
            }
        });
    }

    [Fact(DisableParallelization = true)]
    public Task InterruptedReplay_ConflictingBytesPreserveDatabaseJournalAndLock()
        => this.WithDirectoryAsync(async directory =>
        {
            foreach (bool encrypted in EncryptionStates)
            {
                string fixture = encrypted ? "NativeAceAgile.accdb" : string.Empty;
                string password = Password(DatabaseFormat.AceAccdb, fixture);
                string baseline = Path.Combine(directory, $"baseline-{encrypted}.accdb");
                byte[] original = await CreateImageAsync(baseline, DatabaseFormat.AceAccdb, fixture, password);
                foreach (string boundary in ConflictBoundaries)
                {
                    string path = Path.Combine(directory, $"conflict-{encrypted}-{boundary}.accdb");
                    File.WriteAllBytes(path, original);
                    PrepareInterruptedImage(path, 4096);
                    AssertChild(path, password, boundary);
                    await RejectReaderAsync(path, password);
                    byte[] changed = await File.ReadAllBytesAsync(path, Ct);
                    changed[4096 + 31] ^= 0x80; // An untouched original page has no authorized replacement.
                    await File.WriteAllBytesAsync(path, changed, Ct);
                    string sidecar = PersistentRollbackJournal.JournalPath(path);
                    string lockPath = LockFileSlotWriter.GetLockFilePath(path);
                    byte[] journal = await File.ReadAllBytesAsync(sidecar, Ct);
                    byte[] lockBytes = await File.ReadAllBytesAsync(lockPath, Ct);
                    for (int attempt = 0; attempt < 2; attempt++)
                    {
                        JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                        {
                            await using AccessWriter rejected = await AccessWriter.OpenAsync(path, Options(password), Ct);
                        });
                        Assert.Equal(JetErrorCode.CorruptRecoveryJournal, failure.ErrorCode);
                        Assert.Equal(changed, await File.ReadAllBytesAsync(path, Ct));
                        Assert.Equal(journal, await File.ReadAllBytesAsync(sidecar, Ct));
                        Assert.Equal(lockBytes, await File.ReadAllBytesAsync(lockPath, Ct));
                    }
                }
            }
        });

    [Theory(DisableParallelization = true)]
    [InlineData(DatabaseFormat.Jet3Mdb, "")]
    [InlineData(DatabaseFormat.Jet4Mdb, "")]
    [InlineData(DatabaseFormat.AceAccdb, "")]
    [InlineData(DatabaseFormat.Jet3Mdb, "UpstreamJet3Rc4.mdb")]
    [InlineData(DatabaseFormat.Jet4Mdb, "NativeJet4Rc4.mdb")]
    [InlineData(DatabaseFormat.AceAccdb, "NativeAceStandard.accdb")]
    [InlineData(DatabaseFormat.AceAccdb, "NativeAceAgile.accdb")]
    public Task AfterRecovery_NewWriterLockWithoutJournal_RecoversGenerationReceipt(DatabaseFormat format, string fixture)
        => this.WithDirectoryAsync(async directory =>
        {
            string password = Password(format, fixture);
            string path = Path.Combine(directory, format == DatabaseFormat.AceAccdb ? "after-recovery.accdb" : "after-recovery.mdb");
            byte[] original = await CreateImageAsync(path, format, fixture, password);
            PrepareInterruptedImage(path, format == DatabaseFormat.Jet3Mdb ? 2048 : 4096);
            AssertChild(path, password, "writer-opened");
            Assert.Equal(original, await File.ReadAllBytesAsync(path, Ct));
            Assert.False(File.Exists(PersistentRollbackJournal.JournalPath(path)));
            await RejectReaderAsync(path, password);
            string lockPath = LockFileSlotWriter.GetLockFilePath(path);
            Assert.NotEmpty(await File.ReadAllBytesAsync(lockPath, Ct));
            for (int attempt = 0; attempt < 2; attempt++)
            {
                AssertChild(path, password, "writer-opened");
                await AssertReopensAsync(path, password, original, expectedRows: 1);
                Assert.Equal(original, await File.ReadAllBytesAsync(path, Ct));
            }
        });

    [Fact(DisableParallelization = true)]
    public Task CleanupRefused_ProcessKilled_RetainsStableDecisionForRetry()
        => this.WithDirectoryAsync(async directory =>
        {
            string path = Path.Combine(directory, "cleanup-refused.accdb");
            byte[] original = await CreateImageAsync(path, DatabaseFormat.AceAccdb, string.Empty, string.Empty);
            PrepareInterruptedImage(path, 4096);
            string lockPath = LockFileSlotWriter.GetLockFilePath(path);
            string sidecar = PersistentRollbackJournal.JournalPath(path);
            byte[] lockBytes = await File.ReadAllBytesAsync(lockPath, Ct);
            await using (var holder = new FileStream(lockPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                AssertChild(path, string.Empty, "cleanup-refused");
                Assert.Equal(original, await File.ReadAllBytesAsync(path, Ct));
                byte[] decision = await File.ReadAllBytesAsync(sidecar, Ct);
                Assert.Equal(-1, BinaryPrimitives.ReadInt64LittleEndian(decision.AsSpan(decision.Length - 4136)));
                AssertChild(path, string.Empty, "cleanup-refused");
                Assert.Equal(decision, await File.ReadAllBytesAsync(sidecar, Ct));
                byte[] retainedLock = new byte[lockBytes.Length];
                holder.Position = 0;
                await holder.ReadExactlyAsync(retainedLock, Ct);
                Assert.Equal(lockBytes, retainedLock);
            }

            await AssertReopensAsync(path, string.Empty, original, expectedRows: 1);
        });

    private static string Password(DatabaseFormat format, string fixture)
        => (format, fixture) switch
        {
            (_, "NativeAceStandard.accdb") => "Password1234_",
            (DatabaseFormat.Jet3Mdb, _) or (_, "") => string.Empty,
            _ => "Native123",
        };

    private static AccessWriterOptions Options(string password) => new(password) { UseLockFile = true, UseByteRangeLocks = false };

    private static async Task<byte[]> CreateImageAsync(string path, DatabaseFormat format, string fixture, string password)
    {
        if (fixture.Length > 0)
        {
            File.Copy(Path.Combine(TestDatabases.EncryptedRoot, fixture), path);
        }
        else
        {
            await using AccessWriter created = await AccessWriter.CreateDatabaseAsync(path, format, cancellationToken: Ct);
        }

        await using (AccessWriter writer = await AccessWriter.OpenAsync(path, Options(password), Ct))
        {
            await writer.CreateTableAsync("ReplayRows", [new("Id", typeof(int)), new("State", typeof(string), 40)], Ct);
            await writer.InsertRowAsync("ReplayRows", [1, "original"], Ct);
        }

        return await File.ReadAllBytesAsync(path, Ct);
    }

    private static void PrepareInterruptedImage(string path, int pageSize)
    {
        using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
        {
            Assert.True(database.Length >= pageSize * 6L);
            using var journal = PersistentRollbackJournal.Create(database, pageSize);
            foreach (long offset in new[] { 0L, pageSize * 2L })
            {
                byte[] after = new byte[pageSize];
                Array.Fill(after, (byte)0xA5);
                journal.RecordBeforeWrite(offset, after);
                database.Position = offset;
                database.Write(after, 0, after.Length);
            }

            journal.RecordBeforeTruncate(database.Length - (2L * pageSize));
            database.SetLength(database.Length - (2L * pageSize));
            database.Flush(true);
        }

        File.WriteAllBytes(LockFileSlotWriter.GetLockFilePath(path), new byte[64]);
    }

    private static void PrepareCommittedImage(string path, byte[] original, byte[] committed, int pageSize)
    {
        File.WriteAllBytes(path, original);
        using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
        {
            using var journal = PersistentRollbackJournal.Create(database, pageSize);
            Assert.True(committed.Length >= original.Length);
            for (int offset = 0; offset < committed.Length; offset += pageSize)
            {
                if (offset >= original.Length || !original.AsSpan(offset, pageSize).SequenceEqual(committed.AsSpan(offset, pageSize)))
                {
                    journal.RecordBeforeWrite(offset, committed.AsSpan(offset, pageSize));
                    database.Position = offset;
                    database.Write(committed, offset, pageSize);
                }
            }

            database.Flush(true);
        }

        // Model a complete durable terminal record independently, as in the raw
        // journal format tests. The committed database itself is writer-authored.
        byte[] decision = new byte[pageSize + 40];
        BinaryPrimitives.WriteInt64LittleEndian(decision, -1);
        SHA256.HashData(committed).CopyTo(decision, 8);
        SHA256.HashData(decision.AsSpan(0, decision.Length - 32)).CopyTo(decision, decision.Length - 32);
        using (var journal = new FileStream(PersistentRollbackJournal.JournalPath(path), FileMode.Append, FileAccess.Write, FileShare.None))
        {
            journal.Write(decision, 0, decision.Length);
            journal.Flush(true);
        }

        File.WriteAllBytes(LockFileSlotWriter.GetLockFilePath(path), new byte[64]);
    }

    private static async Task RejectReaderAsync(string path, string password)
    {
        JetOperationException failure = await Assert.ThrowsAsync<JetOperationException>(async () =>
        {
            await using AccessReader rejected = await AccessReader.OpenAsync(path, new AccessReaderOptions(password) { UseLockFile = false }, Ct);
        });
        Assert.Equal(JetErrorCode.HotJournalConflict, failure.ErrorCode);
    }

    private static async Task AssertReopensAsync(string path, string password, byte[] expected, int expectedRows)
    {
        for (int attempt = 0; attempt < 2; attempt++)
        {
            await using (AccessWriter writer = await AccessWriter.OpenAsync(path, Options(password), Ct))
            {
                Assert.NotNull(writer);
            }

            Assert.Equal(expected, await File.ReadAllBytesAsync(path, Ct));
            Assert.False(File.Exists(PersistentRollbackJournal.JournalPath(path)));
            Assert.False(File.Exists(LockFileSlotWriter.GetLockFilePath(path)));
            Assert.False(File.Exists(RecoverableWriterLock.ReceiptPath(path)));
            await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions(password) { UseLockFile = false }, Ct);
            using DataTable rows = await reader.ReadTableAsync("ReplayRows", cancellationToken: Ct);
            Assert.Equal(expectedRows, rows.Rows.Count);
            Assert.Equal("original", rows.Rows[0]["State"]);
            if (expectedRows == 2)
            {
                Assert.Equal("committed", rows.Rows[1]["State"]);
            }
        }
    }

    private static void AssertChild(string path, string password, string boundary)
        => CrashRecoveryProcess.AssertChild(
            Method,
            path,
            boundary,
            $"raw recovery, boundary={boundary}",
            new Dictionary<string, string> { [ChildPath] = path, [ChildBoundary] = boundary, [ChildPassword] = password },
            password);

    private static async Task RunChildAsync(string path)
    {
        string boundary = Environment.GetEnvironmentVariable(ChildBoundary)!;
        if (boundary == "writer-opened")
        {
            await using AccessWriter writer = await AccessWriter.OpenAsync(path, Options(Environment.GetEnvironmentVariable(ChildPassword) ?? string.Empty), Ct);
            CrashRecoveryProcess.Terminate(path, boundary);
            return;
        }

        if (boundary == "cleanup-refused")
        {
            try
            {
                await using AccessWriter writer = await AccessWriter.OpenAsync(path, Options(string.Empty), Ct);
            }
            catch (IOException exception) when (exception is not IJetException)
            {
                CrashRecoveryProcess.Terminate(path, boundary);
            }

            return;
        }

        await using var database = new ReplayCrashStream(path, boundary);
        if (boundary == "start")
        {
            CrashRecoveryProcess.Terminate(path, boundary);
        }

        PersistentRollbackJournal.Recover(database, () =>
        {
            if (boundary == "decision")
            {
                CrashRecoveryProcess.Terminate(path, boundary);
            }

            // These slots were created by this test and bound to the validated
            // journal. Match production's exclusive cleanup before journal removal.
            string lockPath = LockFileSlotWriter.GetLockFilePath(path);
            if (File.Exists(lockPath))
            {
                using var abandoned = new FileStream(lockPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.DeleteOnClose);
            }

            if (boundary == "before-journal-delete")
            {
                CrashRecoveryProcess.Terminate(path, boundary);
            }
        });
        if (boundary == "after-journal-delete")
        {
            CrashRecoveryProcess.Terminate(path, boundary);
        }
    }

    private async Task WithDirectoryAsync(Func<string, Task> operation)
    {
        string directory = Path.Combine(Path.GetTempPath(), $"RecoveryReplayCrashTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        bool completed = false;
        try
        {
            await operation(directory);
            completed = true;
        }
        finally
        {
            if (completed)
            {
                Directory.Delete(directory, recursive: true);
            }
            else
            {
                output.WriteLine("Retained interrupted recovery evidence: " + directory);
            }
        }
    }

    private sealed class ReplayCrashStream(string path, string boundary) : FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None)
    {
        private int writes;

        public override void Write(byte[] buffer, int offset, int count)
        {
            this.writes++;
            if (this.writes == 1 && boundary == "replay-marker")
            {
                CrashRecoveryProcess.Terminate(this.Name, boundary);
            }

            if (this.writes == 1 && boundary == "partial-page")
            {
                base.Write(buffer, offset, count / 2);
                base.Flush(true);
                CrashRecoveryProcess.Terminate(this.Name, boundary);
            }

            if (boundary == "partial-tail" && this.Position >= this.Length)
            {
                base.Write(buffer, offset, count / 2);
                base.Flush(true);
                CrashRecoveryProcess.Terminate(this.Name, boundary);
            }

            base.Write(buffer, offset, count);
            if ((this.writes == 1 && boundary == "first-page") || (this.writes == 2 && boundary == "second-page"))
            {
                base.Flush(true);
                CrashRecoveryProcess.Terminate(this.Name, boundary);
            }
        }

        public override void SetLength(long value)
        {
            base.SetLength(value);
            if (boundary == "length")
            {
                base.Flush(true);
                CrashRecoveryProcess.Terminate(this.Name, boundary);
            }
        }

        public override void Flush(bool flushToDisk)
        {
            base.Flush(flushToDisk);
            if (flushToDisk && boundary == "database-flush")
            {
                CrashRecoveryProcess.Terminate(this.Name, boundary);
            }
        }
    }
}

#pragma warning restore CA1849
