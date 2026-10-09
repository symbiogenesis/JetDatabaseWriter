namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Security.Cryptography;
using JetDatabaseWriter.Pages.Paging;
using Xunit;

/// <summary>Checks durable raw rollback and refusal of conflicting content.</summary>
public sealed class PersistentRollbackJournalTests
{
#pragma warning disable CA1849 // Synchronous device flush and raw reads model interrupted recovery.
    /// <summary>An authorized removed tail is restored from the raw snapshot.</summary>
    /// <param name="restart">Whether restoration already started.</param>
    /// <param name="partialRecord">Whether an interrupted append remains.</param>
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async System.Threading.Tasks.Task Recover_TruncatedTail_RestoresOriginal(bool restart, bool partialRecord)
    {
        string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".mdb");
        byte[] original = new byte[8192];
        Array.Fill(original, (byte)3);
        File.WriteAllBytes(path, original);
        try
        {
            await using var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            using (var journal = PersistentRollbackJournal.Create(database, 2048))
            {
                // A store truncation must persist its intent before changing the file.
                await using var store = new StreamPageStore(database, leaveOpen: true) { RollbackJournal = journal };
                await store.SetLengthAsync(4096, TestContext.Current.CancellationToken);
                if (restart)
                {
                    journal.RecordRecoveryStart();
                    database.SetLength(original.Length);
                    database.Position = 4096;
                    database.Write(original, 4096, 17);
                    database.Flush(true);
                }
            }

            if (partialRecord)
            {
                await using var sidecar = new FileStream(path + ".jdw-journal", FileMode.Append, FileAccess.Write, FileShare.None);
                sidecar.WriteByte(123);
            }

            PersistentRollbackJournal.Recover(database);
            database.Position = 0;
            byte[] restored = new byte[original.Length];
            Assert.Equal(restored.Length, database.Read(restored, 0, restored.Length));
            Assert.Equal(original, restored);
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
        }
    }

#pragma warning restore CA1849

    /// <summary>Neither unexplained truncation nor unrelated retained bytes authorize rollback.</summary>
    /// <param name="authorize">Whether the truncation has an intent record.</param>
    /// <param name="corrupt">Whether retained content conflicts.</param>
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(true, false)]
    public void Recover_TruncatedConflict_RefusesWithoutWrites(bool authorize, bool corrupt)
    {
        string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".mdb");
        byte[] original = new byte[8192];
        Array.Fill(original, (byte)3);
        File.WriteAllBytes(path, original);
        try
        {
            using var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            using (var journal = PersistentRollbackJournal.Create(database, 2048))
            {
                if (authorize)
                {
                    journal.RecordBeforeTruncate(4096);
                    if (corrupt)
                    {
                        journal.RecordRecoveryStart();
                    }
                }
            }

            long length = authorize && !corrupt ? 5000 : 4096;
            database.SetLength(length);
            if (corrupt)
            {
                database.Position = 50;
                database.WriteByte(0);
            }

            database.Flush(true);
            Assert.Throws<IOException>(() => PersistentRollbackJournal.Recover(database));
            Assert.Equal(length, database.Length);
            Assert.True(File.Exists(path + ".jdw-journal"));
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
        }
    }

    /// <summary>A failure after a real replay write retains evidence for a second recovery.</summary>
    [Fact]
    public void Recover_InterruptedReplay_RestoresOriginalOnReopen()
    {
        string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".mdb");
        byte[] original = new byte[24576];
        Array.Fill(original, (byte)23);
        File.WriteAllBytes(path, original);
        try
        {
            using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                using var journal = PersistentRollbackJournal.Create(database, 2048);
                journal.RecordBeforeTruncate(4096);
                database.SetLength(4096);
                database.Flush(true);
            }

            using (var interrupted = new ReplayFaultStream(path))
            {
                Assert.Throws<IOException>(() => PersistentRollbackJournal.Recover(interrupted));
                Assert.Equal(8192, interrupted.Length);
                Assert.Equal(1, interrupted.ReplayWrites);
                Assert.True(File.Exists(path + ".jdw-journal"));
            }

            using (var recovery = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                PersistentRollbackJournal.Recover(recovery);
                PersistentRollbackJournal.Recover(recovery);
            }

            Assert.Equal(original, File.ReadAllBytes(path));
            Assert.False(File.Exists(path + ".jdw-journal"));
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
        }
    }

    /// <summary>A partial overwrite restores the exact original bytes on repeated recovery.</summary>
    [Fact]
    public void Recover_TornWrite_IsIdempotent()
    {
        string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".mdb");
        byte[] original = new byte[8192];
        Array.Fill(original, (byte)3);
        File.WriteAllBytes(path, original);
        try
        {
            using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                using (var journal = PersistentRollbackJournal.Create(database, 2048))
                {
                    byte[] after = new byte[2048];
                    Array.Fill(after, (byte)8);
                    journal.RecordBeforeWrite(2048, after);
                    database.Position = 2048;
                    database.Write(after, 0, 7);
                    database.Flush(true);
                }

                PersistentRollbackJournal.Recover(database);
                PersistentRollbackJournal.Recover(database);
            }

            Assert.Equal(original, File.ReadAllBytes(path));
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
        }
    }

    /// <summary>Unrelated external bytes are refused before recovery writes anything.</summary>
    [Fact]
    public void Recover_ConflictingContent_RefusesWithoutWrites()
    {
        string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".mdb");
        File.WriteAllBytes(path, new byte[8192]);
        try
        {
            using var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            using (var journal = PersistentRollbackJournal.Create(database, 2048))
            {
                journal.RecordBeforeWrite(2048, new byte[2048]);
            }

            database.Position = 40;
            database.WriteByte(99);
            Assert.Throws<IOException>(() => PersistentRollbackJournal.Recover(database));
            database.Position = 40;
            Assert.Equal(99, database.ReadByte());
            Assert.True(File.Exists(path + ".jdw-journal"));
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
        }
    }

    /// <summary>Incomplete append records are ignored because they never authorized a database write.</summary>
    /// <param name="tailLength">The incomplete record byte count.</param>
    [Theory]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(40)]
    [InlineData(71)]
    [InlineData(2056)]
    [InlineData(2087)]
    public void Recover_IncompleteTail_RestoresPriorWrites(int tailLength)
    {
        string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".mdb");
        byte[] original = new byte[8192];
        File.WriteAllBytes(path, original);
        try
        {
            using var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            using (var journal = PersistentRollbackJournal.Create(database, 2048))
            {
                byte[] after = new byte[2048];
                Array.Fill(after, (byte)23);
                journal.RecordBeforeWrite(2048, after);
                database.Position = 2048;
                database.Write(after);
                database.Flush(true);
            }

            using (var journal = new FileStream(path + ".jdw-journal", FileMode.Open, FileAccess.Write, FileShare.None))
            {
                journal.Position = journal.Length;
                journal.Write(new byte[tailLength]);
                journal.Flush(true);
            }

            Assert.Throws<IOException>(() => PersistentRollbackJournal.RejectHotJournal(database));
            PersistentRollbackJournal.Recover(database);
            database.Position = 0;
            byte[] restored = new byte[8192];
            Assert.Equal(restored.Length, database.Read(restored));
            Assert.Equal(original, restored);
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
        }
    }

    /// <summary>Snapshot, record, identity and hostile size corruption are refused without writes.</summary>
    /// <param name="location">The journal byte offset to corrupt.</param>
    [Theory]
    [InlineData(8)]
    [InlineData(24)]
    [InlineData(128)]
    [InlineData(8330)]
    public void Recover_CorruptJournal_RefusesWithoutWrites(int location)
    {
        string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".mdb");
        File.WriteAllBytes(path, new byte[8192]);
        try
        {
            using var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            using (var journal = PersistentRollbackJournal.Create(database, 2048))
            {
                journal.RecordBeforeWrite(2048, new byte[2048]);
            }

            using (var journal = new FileStream(path + ".jdw-journal", FileMode.Open, FileAccess.Write, FileShare.None))
            {
                journal.Position = location;
                journal.WriteByte(255);
                journal.Flush(true);
            }

            Assert.Throws<IOException>(() => PersistentRollbackJournal.Recover(database));
            database.Position = 0;
            byte[] current = new byte[8192];
            Assert.Equal(current.Length, database.Read(current));
            Assert.Equal(new byte[8192], current);
            Assert.True(File.Exists(path + ".jdw-journal"));
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
        }
    }

    /// <summary>A durable commit marker left before cleanup retains the committed bytes.</summary>
    /// <param name="corrupt">Whether the marker is corrupt.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Recover_StaleCommitDecision_ValidatesCommittedContent(bool corrupt)
    {
        string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".mdb");
        byte[] original = new byte[8192];
        byte[] committed = new byte[8192];
        Array.Fill(committed, (byte)42, 2048, 2048);
        File.WriteAllBytes(path, original);
        try
        {
            using var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            using (var journal = PersistentRollbackJournal.Create(database, 2048))
            {
                journal.RecordBeforeWrite(2048, committed.AsSpan(2048, 2048));
                database.Position = 0;
                database.Write(committed);
                database.Flush(true);
            }

            byte[] marker = new byte[2088];
            BinaryPrimitives.WriteInt64LittleEndian(marker, -1);
            SHA256.HashData(committed).CopyTo(marker, 8);
            SHA256.HashData(marker.AsSpan(0, 2056)).CopyTo(marker, 2056);
            if (corrupt)
            {
                marker[45] ^= 1;
            }

            using (var journal = new FileStream(path + ".jdw-journal", FileMode.Open, FileAccess.Write, FileShare.None))
            {
                journal.Position = journal.Length;
                journal.Write(marker);
                journal.Flush(true);
            }

            if (corrupt)
            {
                Assert.Throws<IOException>(() => PersistentRollbackJournal.Recover(database));
                Assert.True(File.Exists(path + ".jdw-journal"));
            }
            else
            {
                PersistentRollbackJournal.Recover(database);
                Assert.False(File.Exists(path + ".jdw-journal"));
            }

            database.Position = 0;
            byte[] current = new byte[8192];
            Assert.Equal(current.Length, database.Read(current));
            Assert.Equal(committed, current);
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
        }
    }

    /// <summary>A journal copied to another canonical database path cannot authorize replay.</summary>
    [Fact]
    public void Recover_WrongPath_RefusesWithoutWrites()
    {
        string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".mdb");
        string other = path + ".other";
        File.WriteAllBytes(path, new byte[8192]);
        File.WriteAllBytes(other, new byte[8192]);
        try
        {
            using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                using var journal = PersistentRollbackJournal.Create(database, 2048);
                journal.RecordBeforeWrite(2048, new byte[2048]);
            }

            File.Copy(path + ".jdw-journal", other + ".jdw-journal");
            using var copiedDatabase = new FileStream(other, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            Assert.Throws<IOException>(() => PersistentRollbackJournal.Recover(copiedDatabase));
            Assert.True(File.Exists(other + ".jdw-journal"));
        }
        finally
        {
            File.Delete(path);
            File.Delete(other);
            File.Delete(path + ".jdw-journal");
            File.Delete(other + ".jdw-journal");
        }
    }

    /// <summary>Interrupted baseline preparation is refused without touching the database.</summary>
    /// <param name="length">The interrupted sidecar length.</param>
    [Theory]
    [InlineData(7)]
    [InlineData(128)]
    [InlineData(160)]
    public void Recover_IncompletePreparation_RefusesWithoutWrites(int length)
    {
        string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".mdb");
        File.WriteAllBytes(path, new byte[8192]);
        File.WriteAllBytes(path + ".jdw-journal", new byte[length]);
        try
        {
            using var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            Assert.Throws<IOException>(() => PersistentRollbackJournal.Recover(database));
            byte[] current = new byte[8192];
            database.Position = 0;
            Assert.Equal(current.Length, database.Read(current));
            Assert.Equal(new byte[8192], current);
            Assert.True(File.Exists(path + ".jdw-journal"));
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
        }
    }

    /// <summary>Checksum-valid unsupported offsets and unexplained file growth cannot authorize recovery.</summary>
    /// <param name="invalidOffset">Whether to inject a bad offset rather than file growth.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Recover_InvalidBounds_RefusesWithoutWrites(bool invalidOffset)
    {
        string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".mdb");
        File.WriteAllBytes(path, new byte[8192]);
        try
        {
            using var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            using (var journal = PersistentRollbackJournal.Create(database, 2048))
            {
                journal.RecordBeforeWrite(2048, new byte[2048]);
            }

            if (invalidOffset)
            {
                byte[] record = new byte[2088];
                BinaryPrimitives.WriteInt64LittleEndian(record, -4);
                SHA256.HashData(record.AsSpan(0, 2056)).CopyTo(record, 2056);
                using var journal = new FileStream(path + ".jdw-journal", FileMode.Open, FileAccess.Write, FileShare.None);
                journal.Position = journal.Length;
                journal.Write(record);
                journal.Flush(true);
            }
            else
            {
                database.SetLength(8193);
            }

            long length = database.Length;
            Assert.Throws<IOException>(() => PersistentRollbackJournal.Recover(database));
            Assert.Equal(length, database.Length);
            Assert.True(File.Exists(path + ".jdw-journal"));
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
        }
    }

    /// <summary>Every recorded raw version and baseline bytes remain valid across interrupted rollback.</summary>
    [Fact]
    public void Recover_RepeatedVersionsAndPartialUndo_RestoresOriginal()
    {
        string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".mdb");
        byte[] original = new byte[8192];
        RandomNumberGenerator.Fill(original);
        File.WriteAllBytes(path, original);
        try
        {
            using var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            using (var journal = PersistentRollbackJournal.Create(database, 2048))
            {
                byte[] first = new byte[2048];
                byte[] second = new byte[2048];
                RandomNumberGenerator.Fill(first);
                RandomNumberGenerator.Fill(second);
                journal.RecordBeforeWrite(2048, first);
                database.Position = 2048;
                database.Write(first);
                journal.RecordBeforeWrite(2048, second);
                database.Position = 2048;
                database.Write(second.AsSpan(0, 1000));
                database.Position = 2048;
                database.Write(original.AsSpan(2048, 500));
                database.Flush(true);
            }

            PersistentRollbackJournal.Recover(database);
            PersistentRollbackJournal.Recover(database);
            byte[] current = new byte[8192];
            database.Position = 0;
            Assert.Equal(current.Length, database.Read(current));
            Assert.Equal(original, current);
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
        }
    }

    /// <summary>Abandoned staging bytes never authorize writes and can be replaced under exclusive ownership.</summary>
    [Fact]
    public void Create_AbandonedPreparation_IsIgnoredAndReplaced()
    {
        string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".mdb");
        string staging = path + ".jdw-journal.preparing";
        byte[] original = new byte[8192];
        RandomNumberGenerator.Fill(original);
        File.WriteAllBytes(path, original);
        File.WriteAllBytes(staging, new byte[7]);
        try
        {
            using var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            PersistentRollbackJournal.Recover(database);
            PersistentRollbackJournal.RejectHotJournal(database);
            Assert.True(File.Exists(staging));
            using (var journal = PersistentRollbackJournal.Create(database, 2048))
            {
                Assert.False(File.Exists(staging));
                Assert.True(File.Exists(path + ".jdw-journal"));
            }

            PersistentRollbackJournal.Recover(database);
            byte[] current = new byte[8192];
            database.Position = 0;
            Assert.Equal(current.Length, database.Read(current));
            Assert.Equal(original, current);
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
            File.Delete(staging);
        }
    }

    /// <summary>A preexisting final journal cannot be overwritten or deleted by a new preparation attempt.</summary>
    [Fact]
    public void Create_PreexistingFinalJournal_PreservesRecoveryEvidence()
    {
        string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".mdb");
        File.WriteAllBytes(path, new byte[8192]);
        byte[] evidence = [1, 2, 3];
        File.WriteAllBytes(path + ".jdw-journal", evidence);
        try
        {
            using var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            Assert.Throws<IOException>(() => PersistentRollbackJournal.Create(database, 2048));
            Assert.Equal(evidence, File.ReadAllBytes(path + ".jdw-journal"));
            Assert.False(File.Exists(path + ".jdw-journal.preparing"));
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
            File.Delete(path + ".jdw-journal.preparing");
        }
    }

    /// <summary>Windows case aliases resolve the same physical database and journal identity.</summary>
    [Fact]
    public void Recover_WindowsCaseAlias_RestoresOriginal()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".mdb");
        File.WriteAllBytes(path, new byte[8192]);
        try
        {
            using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                using var journal = PersistentRollbackJournal.Create(database, 2048);
            }

            using var alias = new FileStream(path.ToUpperInvariant(), FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            PersistentRollbackJournal.Recover(alias);
            Assert.False(File.Exists(path + ".jdw-journal"));
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
            File.Delete(path + ".jdw-journal.preparing");
        }
    }

    private sealed class ReplayFaultStream(string path) : FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None)
    {
        internal int ReplayWrites { get; private set; }

#pragma warning disable CA1849 // The recovery replay uses synchronous writes and requires a durable interruption boundary.
        public override void Write(byte[] buffer, int offset, int count)
        {
            base.Write(buffer, offset, count);
            this.Flush(flushToDisk: true);
            if (++this.ReplayWrites == 1)
            {
                throw new IOException("Injected interruption after the first recovery replay buffer.");
            }
        }
#pragma warning restore CA1849
    }
}
