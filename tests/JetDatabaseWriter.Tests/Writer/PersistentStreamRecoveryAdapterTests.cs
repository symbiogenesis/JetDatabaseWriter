namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Exercises persistent caller-stream recovery over <see cref="IPersistentStreamRecoveryAdapter"/>
/// and <see cref="IPersistentStreamRecoveryLease"/>, including cross-open recovery, fault sweeps,
/// lease/generation/identity enforcement, hot-journal refusal, and native encryption providers.
/// </summary>
public sealed class PersistentStreamRecoveryAdapterTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>
    /// Verifies that <see cref="AccessWriter.CreateDatabaseAsync(Stream, DatabaseFormat, AccessWriterOptions?, bool, CancellationToken)"/>
    /// and <see cref="AccessWriter.OpenAsync(Stream, AccessWriterOptions?, bool, CancellationToken)"/> execute statement-level
    /// and explicit-transaction commits and rollbacks through a persistent caller-stream recovery adapter.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateAndOpen_WithPersistentAdapter_CommitsAndRollsBackCleanly(DatabaseFormat format)
    {
        var store = new InMemoryRecoveryBackingStore("db:" + format);
        await using Stream stream = store.OpenDatabaseStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, CreateWriterOptions(store), leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync(
                "Items",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Name", typeof(string), 64),
                ],
                Ct);
            await writer.InsertRowAsync("Items", [1, "First"], Ct);

            await using (JetTransaction tx = await writer.BeginTransactionAsync(Ct))
            {
                await writer.InsertRowAsync("Items", [2, "RolledBack"], Ct);
                await tx.RollbackAsync(Ct);
            }

            await using (JetTransaction tx = await writer.BeginTransactionAsync(Ct))
            {
                await writer.InsertRowAsync("Items", [3, "Committed"], Ct);
                await tx.CommitAsync(Ct);
            }
        }

        Assert.False(store.HasPendingJournal(stream));
        Assert.True(store.Generation >= 4);
        Assert.True(store.DatabaseFenceCount > 0);
        Assert.True(store.JournalFenceCount > 0);

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, CreateReaderOptions(store), leaveOpen: true, Ct);
        using DataTable table = await reader.ReadTableAsync("Items", cancellationToken: Ct);
        Assert.Equal(2, table.Rows.Count);
        Assert.Equal("First", Assert.Single(table.Select("Id = 1"))["Name"]);
        Assert.Equal("Committed", Assert.Single(table.Select("Id = 3"))["Name"]);
    }

    /// <summary>
    /// Verifies that interrupted writes, truncations, replay markers, and post-decision states
    /// are deterministically recovered or retired when a subsequent writer opens the stream.
    /// </summary>
    /// <param name="interruption">The interruption stage to simulate.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="interruption"/> is unknown.</exception>
    [Theory]
    [InlineData("promoted-untouched")]
    [InlineData("torn-write")]
    [InlineData("truncated-tail")]
    [InlineData("interrupted-replay")]
    [InlineData("completed-unretired")]
    [InlineData("unpromoted-preparing")]
    public async Task Reopen_InterruptedMutation_RestoresExactBytesOrRetiresCompletedJournal(string interruption)
    {
        var store = new InMemoryRecoveryBackingStore("db:interruption:" + interruption);
        await using Stream stream = store.OpenDatabaseStream();
        await using (AccessWriter creator = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, CreateWriterOptions(store), leaveOpen: true, Ct))
        {
            await creator.CreateTableAsync(
                "Items",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Payload", typeof(string), 200),
                ],
                Ct);
            for (int i = 1; i <= 40; i++)
            {
                await creator.InsertRowAsync("Items", [i, new string('A', 180)], Ct);
            }
        }

        byte[] committedImage = store.SnapshotDatabaseBytes();
        if (interruption == "unpromoted-preparing")
        {
            using (IPersistentStreamRecoveryLease lease = store.AcquireExclusiveLease(stream))
            {
                using var journal = PersistentRollbackJournal.Create(stream, lease, 4096);
            }

            store.DemoteJournalToPreparing();
            Assert.True(store.HasPendingJournal(stream));
        }
        else
        {
            using IPersistentStreamRecoveryLease lease = store.AcquireExclusiveLease(stream);
            using var journal = PersistentRollbackJournal.Create(stream, lease, 4096);
            byte[] mutatedPage = new byte[4096];
            Array.Fill(mutatedPage, (byte)0x5A);
            switch (interruption)
            {
                case "promoted-untouched":
                    journal.RecordBeforeWrite(4096, mutatedPage);
                    break;
                case "torn-write":
                    journal.RecordBeforeWrite(4096, mutatedPage);
                    stream.Position = 4096;
                    await stream.WriteAsync(mutatedPage.AsMemory(0, 513), Ct);
                    break;
                case "truncated-tail":
                    journal.RecordBeforeTruncate(8192);
                    stream.SetLength(8192);
                    break;
                case "interrupted-replay":
                    journal.RecordBeforeTruncate(8192);
                    stream.SetLength(8192);
                    journal.RecordRecoveryStart();
                    stream.SetLength(committedImage.Length);
                    stream.Position = 8192;
                    await stream.WriteAsync(committedImage.AsMemory(8192, 333), Ct);
                    break;
                case "completed-unretired":
                    store.SuppressNextRetire = true;
                    journal.Complete();
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(interruption));
            }
        }

        Assert.True(store.HasPendingJournal(stream));

        // Reader and CreateDatabase must refuse while hot journal evidence exists.
        stream.Position = 0;
        await StructuredFormatAssert.ThrowsAsync<JetOperationException>(
            JetErrorCode.HotJournalConflict,
            async () =>
            {
                await using AccessReader refused = await AccessReader.OpenAsync(stream, CreateReaderOptions(store), leaveOpen: true, Ct);
            });

        stream.Position = 0;
        await StructuredFormatAssert.ThrowsAsync<JetOperationException>(
            JetErrorCode.HotJournalConflict,
            async () =>
            {
                await using AccessWriter refusedCreator = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, CreateWriterOptions(store), leaveOpen: true, Ct);
            });

        // Reopening with AccessWriter recovers the database to the exact committed bytes and retires the journal.
        stream.Position = 0;
        await using (AccessWriter reopened = await AccessWriter.OpenAsync(stream, CreateWriterOptions(store), leaveOpen: true, Ct))
        {
            Assert.False(store.HasPendingJournal(stream));
            Assert.Equal(committedImage, store.SnapshotDatabaseBytes());
            await reopened.InsertRowAsync("Items", [41, "AfterRecovery"], Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, CreateReaderOptions(store), leaveOpen: true, Ct);
        using DataTable rows = await reader.ReadTableAsync("Items", cancellationToken: Ct);
        Assert.Equal(41, rows.Rows.Count);
    }

    /// <summary>
    /// Verifies that every pre-decision write, database fence, and journal fence failure
    /// during commit or statement execution restores the exact baseline image and leaves the writer usable,
    /// while restore failure marks the writer faulted and retains journal evidence for reopen recovery.
    /// </summary>
    /// <param name="automatic">Whether to use auto-commit statements or explicit transactions.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CommitAndFenceFaultSweep_RestoresExactBytesAndRecoversFromFailedUndo(bool automatic)
    {
        var store = new InMemoryRecoveryBackingStore("db:fault-sweep:" + automatic);
        await using Stream stream = store.OpenDatabaseStream();
        await using (AccessWriter creator = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, CreateWriterOptions(store, automatic: false), leaveOpen: true, Ct))
        {
            await creator.CreateTableAsync(
                "Items",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Payload", typeof(string), 200),
                ],
                Ct);
            await creator.InsertRowAsync("Items", [1, new string('B', 180)], Ct);
        }

        byte[] baseline = store.SnapshotDatabaseBytes();

        // Sweep journal fence faults across preparation, before-image append, and completion.
        for (int fenceFault = 1; fenceFault <= 6; fenceFault++)
        {
            store.ResetTo(baseline);
            await using Stream runStream = store.OpenDatabaseStream();
            await using AccessWriter writer = await AccessWriter.OpenAsync(runStream, CreateWriterOptions(store, automatic), leaveOpen: true, Ct);
            store.FailOnJournalFence(fenceFault);
            try
            {
                if (automatic)
                {
                    await writer.InsertRowAsync("Items", [2, new string('C', 180)], Ct);
                }
                else
                {
                    await using JetTransaction tx = await writer.BeginTransactionAsync(Ct);
                    await writer.InsertRowAsync("Items", [2, new string('C', 180)], Ct);
                    await tx.CommitAsync(Ct);
                }

                break;
            }
            catch (IOException)
            {
                store.ClearFaults();
                if (!store.HasPendingJournal(runStream))
                {
                    Assert.Equal(baseline, store.SnapshotDatabaseBytes());
                    await writer.InsertRowAsync("Items", [99, "HealthyAfterJournalFenceFault"], Ct);
                }
                else
                {
                    // Completion marker fence fault after database flush marks the writer faulted
                    // and retains the journal until reopen retires or replays it.
                    await StructuredFormatAssert.ThrowsAsync<JetOperationException>(
                        JetErrorCode.WriterFaulted,
                        async () => await writer.InsertRowAsync("Items", [99, "Refused"], Ct));
                    await writer.DisposeAsync();

                    await using Stream recoveryStream = store.OpenDatabaseStream();
                    await using AccessWriter recovered = await AccessWriter.OpenAsync(recoveryStream, CreateWriterOptions(store, automatic), leaveOpen: true, Ct);
                    Assert.False(store.HasPendingJournal(recoveryStream));
                }
            }
            catch (AggregateException)
            {
                // If the journal fence failed while writing the completion or recovery marker,
                // the writer faults and retains the journal for reopen recovery.
                store.ClearFaults();
                Assert.True(store.HasPendingJournal(runStream));
                await StructuredFormatAssert.ThrowsAsync<JetOperationException>(
                    JetErrorCode.WriterFaulted,
                    async () => await writer.InsertRowAsync("Items", [99, "Refused"], Ct));
                await writer.DisposeAsync();

                await using Stream recoveryStream = store.OpenDatabaseStream();
                await using AccessWriter recovered = await AccessWriter.OpenAsync(recoveryStream, CreateWriterOptions(store, automatic), leaveOpen: true, Ct);
                Assert.Equal(baseline, store.SnapshotDatabaseBytes());
                Assert.False(store.HasPendingJournal(recoveryStream));
            }
        }

        // Database fence fault during commit restores in-process and leaves writer healthy.
        store.ResetTo(baseline);
        await using (Stream dbFenceStream = store.OpenDatabaseStream())
        {
            await using AccessWriter writer = await AccessWriter.OpenAsync(dbFenceStream, CreateWriterOptions(store, automatic), leaveOpen: true, Ct);
            store.FailOnDatabaseFence(1);
            await Assert.ThrowsAsync<IOException>(async () =>
            {
                if (automatic)
                {
                    await writer.InsertRowAsync("Items", [2, new string('D', 180)], Ct);
                }
                else
                {
                    await using JetTransaction tx = await writer.BeginTransactionAsync(Ct);
                    await writer.InsertRowAsync("Items", [2, new string('D', 180)], Ct);
                    await tx.CommitAsync(Ct);
                }
            });

            store.ClearFaults();
            Assert.Equal(baseline, store.SnapshotDatabaseBytes());
            Assert.False(store.HasPendingJournal(dbFenceStream));
            await writer.InsertRowAsync("Items", [3, "AfterDbFenceFault"], Ct);
        }

        // Failed in-process undo faults the writer, retains the journal, and recovers on reopen.
        store.ResetTo(baseline);
        await using (Stream undoFailStream = store.OpenDatabaseStream())
        {
            AccessWriter writer = await AccessWriter.OpenAsync(undoFailStream, CreateWriterOptions(store, automatic), leaveOpen: true, Ct);
            try
            {
                store.FailDatabaseWritesFrom(2);
                await Assert.ThrowsAsync<AggregateException>(async () =>
                {
                    if (automatic)
                    {
                        await writer.InsertRowAsync("Items", [2, new string('E', 180)], Ct);
                    }
                    else
                    {
                        await using JetTransaction tx = await writer.BeginTransactionAsync(Ct);
                        await writer.InsertRowAsync("Items", [2, new string('E', 180)], Ct);
                        await tx.CommitAsync(Ct);
                    }
                });

                Assert.True(store.HasPendingJournal(undoFailStream));
                await StructuredFormatAssert.ThrowsAsync<JetOperationException>(
                    JetErrorCode.WriterFaulted,
                    async () => await writer.InsertRowAsync("Items", [4, "Refused"], Ct));
            }
            finally
            {
                await writer.DisposeAsync();
            }
        }

        store.ClearFaults();
        await using (Stream reopenStream = store.OpenDatabaseStream())
        {
            await using AccessWriter recovered = await AccessWriter.OpenAsync(reopenStream, CreateWriterOptions(store, automatic), leaveOpen: true, Ct);
            Assert.Equal(baseline, store.SnapshotDatabaseBytes());
            Assert.False(store.HasPendingJournal(reopenStream));
        }
    }

    /// <summary>
    /// Verifies that identity mismatches, stale generation tokens, missing crash fencing,
    /// lease contention, stale-lease writes, and path/<see cref="FileStream"/> combinations are rejected.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task AdapterValidation_IdentityGenerationLeaseAndConfigurationGuards_EnforceSafety()
    {
        var store = new InMemoryRecoveryBackingStore("db:primary-identity");
        await using Stream stream = store.OpenDatabaseStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, CreateWriterOptions(store), leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            await writer.InsertRowAsync("T", [1], Ct);
        }

        byte[] intact = store.SnapshotDatabaseBytes();

        // 1. HasQuiescentCrashFencing == false is refused with InvalidOperationState.
        store.HasQuiescentCrashFencing = false;
        stream.Position = 0;
        await StructuredFormatAssert.ThrowsAsync<JetOperationException>(
            JetErrorCode.InvalidOperationState,
            async () =>
            {
                await using AccessWriter refused = await AccessWriter.OpenAsync(stream, CreateWriterOptions(store), leaveOpen: true, Ct);
            });
        store.HasQuiescentCrashFencing = true;

        // 2. Exclusive lease contention is refused with DatabaseInUse while another writer holds the lease.
        stream.Position = 0;
        await using (AccessWriter activeWriter = await AccessWriter.OpenAsync(stream, CreateWriterOptions(store), leaveOpen: true, Ct))
        {
            await using Stream competingStream = store.OpenDatabaseStream();
            await StructuredFormatAssert.ThrowsAsync<JetOperationException>(
                JetErrorCode.DatabaseInUse,
                async () =>
                {
                    await using AccessWriter competing = await AccessWriter.OpenAsync(competingStream, CreateWriterOptions(store), leaveOpen: true, Ct);
                });
        }

        // 3. Stale lease cannot mutate database or journal state after a successor acquires the lease.
        IPersistentStreamRecoveryLease staleLease = store.AcquireExclusiveLease(stream);
        store.ForceEvictLease();
        using (IPersistentStreamRecoveryLease currentLease = store.AcquireExclusiveLease(stream))
        {
            Assert.Throws<JetOperationException>(() => staleLease.BindGeneration(currentLease.Generation + 1));
            Assert.Throws<JetOperationException>(() => staleLease.FenceDatabase(stream));
        }

        staleLease.Dispose();

        // 4. Mismatched DatabaseIdentity in a pending journal is refused without modifying the database.
        using (IPersistentStreamRecoveryLease lease = store.AcquireExclusiveLease(stream))
        {
            using var journal = PersistentRollbackJournal.Create(stream, lease, 4096);
            byte[] page = new byte[4096];
            journal.RecordBeforeWrite(4096, page);
        }

        InMemoryRecoveryBackingStore wrongIdentityStore = store.CloneWithIdentity("db:wrong-identity");
        await using (Stream wrongStream = wrongIdentityStore.OpenDatabaseStream())
        {
            await StructuredFormatAssert.ThrowsAsync<JetCorruptDataException>(
                JetErrorCode.CorruptRecoveryJournal,
                async () =>
                {
                    await using AccessWriter refused = await AccessWriter.OpenAsync(wrongStream, CreateWriterOptions(wrongIdentityStore), leaveOpen: true, Ct);
                });
            Assert.Equal(intact, wrongIdentityStore.SnapshotDatabaseBytes());
            Assert.True(wrongIdentityStore.HasPendingJournal(wrongStream));
        }

        // 5. Stale Generation token in a pending journal is refused without modifying the database.
        InMemoryRecoveryBackingStore staleGenStore = store.CloneWithGeneration(store.Generation + 5);
        await using (Stream staleGenStream = staleGenStore.OpenDatabaseStream())
        {
            await StructuredFormatAssert.ThrowsAsync<JetCorruptDataException>(
                JetErrorCode.CorruptRecoveryJournal,
                async () =>
                {
                    await using AccessWriter refused = await AccessWriter.OpenAsync(staleGenStream, CreateWriterOptions(staleGenStore), leaveOpen: true, Ct);
                });
            Assert.Equal(intact, staleGenStore.SnapshotDatabaseBytes());
            Assert.True(staleGenStore.HasPendingJournal(staleGenStream));
        }

        // 6. Original store with matching identity and generation recovers cleanly.
        stream.Position = 0;
        await using (AccessWriter recovered = await AccessWriter.OpenAsync(stream, CreateWriterOptions(store), leaveOpen: true, Ct))
        {
            Assert.False(store.HasPendingJournal(stream));
            Assert.Equal(intact, store.SnapshotDatabaseBytes());
        }

        // 6b. Truncated unpromoted preparing journal is refused without modifying the database.
        using (IPersistentStreamRecoveryLease lease = store.AcquireExclusiveLease(stream))
        {
            long nextGen = checked(lease.Generation + 1);
            lease.BindGeneration(nextGen);
            await using Stream preparing = lease.CreatePreparingJournal();
            await preparing.WriteAsync(new byte[] { 1, 2, 3, 4, 5 }, Ct);
        }

        stream.Position = 0;
        await StructuredFormatAssert.ThrowsAsync<JetCorruptDataException>(
            JetErrorCode.CorruptRecoveryJournal,
            async () =>
            {
                await using AccessWriter refused = await AccessWriter.OpenAsync(stream, CreateWriterOptions(store), leaveOpen: true, Ct);
            });
        Assert.Equal(intact, store.SnapshotDatabaseBytes());
        Assert.True(store.HasPendingJournal(stream));
        store.ResetTo(intact);

        // 7. Combining PersistentStreamRecoveryAdapter with path or FileStream opens throws ArgumentException.
        string tempPath = Path.Combine(Path.GetTempPath(), $"jdw-adapter-guard-{Guid.NewGuid():N}.accdb");
        await File.WriteAllBytesAsync(tempPath, intact, Ct);
        try
        {
            await Assert.ThrowsAsync<ArgumentException>(async () =>
            {
                await using AccessReader reader = await AccessReader.OpenAsync(tempPath, CreateReaderOptions(store), Ct);
            });
            await Assert.ThrowsAsync<ArgumentException>(async () =>
            {
                await using AccessWriter writer = await AccessWriter.OpenAsync(tempPath, CreateWriterOptions(store), Ct);
            });
            await using var fileStream = new FileStream(tempPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            await Assert.ThrowsAsync<ArgumentException>(async () =>
            {
                await using AccessWriter writer = await AccessWriter.OpenAsync(fileStream, CreateWriterOptions(store), leaveOpen: true, Ct);
            });
        }
        finally
        {
            File.Delete(tempPath);
        }
    }

    /// <summary>
    /// Verifies that all supported native encryption providers recover exact ciphertext across
    /// interrupted writes and truncations when backed by <see cref="IPersistentStreamRecoveryAdapter"/>.
    /// </summary>
    /// <param name="fixture">The native encrypted database fixture.</param>
    /// <param name="password">The database password.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Theory]
    [InlineData("NativeJet3Rc4.mdb", "Pássword")]
    [InlineData("NativeJet4Rc4.mdb", "Native123")]
    [InlineData("NativeJet4Password.mdb", "Native123")]
    [InlineData("NativeAceAgile.accdb", "Native123")]
    [InlineData("NativeAceStandard.accdb", "Password1234_")]
    [InlineData("Upstream-db2007-oldenc.accdb", "Test123")]
    [InlineData("Upstream-db2007-enc.accdb", "Test123")]
    [InlineData("Upstream-db-nonstandard.accdb", "password")]
    public async Task NativeEncryptedFixtures_WithPersistentAdapter_RecoverFromTornShrinkAndCommitCleanly(string fixture, string password)
    {
        byte[] native = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, fixture), Ct);
        var store = new InMemoryRecoveryBackingStore("db:encrypted:" + fixture, native);
        await using Stream stream = store.OpenDatabaseStream();
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, CreateWriterOptions(store, password: password), leaveOpen: true, Ct))
        {
            await FreePageTailFixture.AddFreeTailAsync(writer, 6);
        }

        byte[] expandedBaseline = store.SnapshotDatabaseBytes();

        // Inject a failure during ShrinkDatabaseAsync after physical truncation so the in-process restore fails too,
        // leaving a truncated encrypted stream and a persistent rollback journal.
        await using (Stream faultStream = store.OpenDatabaseStream())
        {
            AccessWriter writer = await AccessWriter.OpenAsync(faultStream, CreateWriterOptions(store, password: password), leaveOpen: true, Ct);
            try
            {
                store.FailAfterTruncateAndFailRestore = true;
                await Assert.ThrowsAsync<AggregateException>(async () => await writer.ShrinkDatabaseAsync(Ct));
                Assert.True(store.HasPendingJournal(faultStream));
                Assert.True(faultStream.Length < expandedBaseline.Length);
            }
            finally
            {
                await writer.DisposeAsync();
            }
        }

        store.ClearFaults();

        // Reopening with AccessWriter replays the persistent journal and restores the exact encrypted tail before reading headers/pages.
        await using (Stream recoveryStream = store.OpenDatabaseStream())
        {
            await using AccessWriter recovered = await AccessWriter.OpenAsync(recoveryStream, CreateWriterOptions(store, password: password), leaveOpen: true, Ct);
            Assert.False(store.HasPendingJournal(recoveryStream));
            Assert.Equal(expandedBaseline, store.SnapshotDatabaseBytes());
            Assert.True(await recovered.ShrinkDatabaseAsync(Ct) >= 6);
        }
    }

    private static AccessWriterOptions CreateWriterOptions(InMemoryRecoveryBackingStore store, bool automatic = true, string? password = null) => new(password)
    {
        UseLockFile = false,
        UseByteRangeLocks = false,
        UseTransactionalWrites = automatic,
        PersistentStreamRecoveryAdapter = store,
        SecureEraseMode = SecureEraseMode.DeletedRowsAndFreedPages,
    };

    private static AccessReaderOptions CreateReaderOptions(InMemoryRecoveryBackingStore store, string? password = null) => new(password)
    {
        UseLockFile = false,
        PersistentStreamRecoveryAdapter = store,
    };

    private sealed class InMemoryRecoveryBackingStore(string identity, byte[]? initialBytes = null, long initialGeneration = 1) : IPersistentStreamRecoveryAdapter
    {
        private byte[] databaseBytes = initialBytes is null ? [] : (byte[])initialBytes.Clone();
        private byte[]? promotedJournalBytes;
        private byte[]? preparingJournalBytes;
        private ActiveLease? activeLease;
        private int failOnDbFence;
        private int failOnJournalFence;
        private int failDbWritesFrom;
        private int dbWriteCount;

        public string DatabaseIdentity { get; } = identity;

        public long Generation { get; private set; } = initialGeneration;

        public bool HasQuiescentCrashFencing { get; set; } = true;

        public int DatabaseFenceCount { get; private set; }

        public int JournalFenceCount { get; private set; }

        public bool SuppressNextRetire { get; set; }

        public bool FailAfterTruncateAndFailRestore { get; set; }

        public byte[] SnapshotDatabaseBytes() => (byte[])this.databaseBytes.Clone();

        public void ResetTo(byte[] bytes)
        {
            this.databaseBytes = (byte[])bytes.Clone();
            this.promotedJournalBytes = null;
            this.preparingJournalBytes = null;
            this.activeLease = null;
            this.ClearFaults();
        }

        public void ClearFaults()
        {
            this.failOnDbFence = 0;
            this.failOnJournalFence = 0;
            this.failDbWritesFrom = 0;
            this.dbWriteCount = 0;
            this.SuppressNextRetire = false;
            this.FailAfterTruncateAndFailRestore = false;
        }

        public void FailOnDatabaseFence(int callNumber) => this.failOnDbFence = callNumber;

        public void FailOnJournalFence(int callNumber) => this.failOnJournalFence = callNumber;

        public void FailDatabaseWritesFrom(int writeNumber)
        {
            this.failDbWritesFrom = writeNumber;
            this.dbWriteCount = 0;
        }

        public void ForceEvictLease() => this.activeLease = null;

        public void DemoteJournalToPreparing()
        {
            this.preparingJournalBytes = this.promotedJournalBytes;
            this.promotedJournalBytes = null;
        }

        public InMemoryRecoveryBackingStore CloneWithIdentity(string newIdentity) => new(newIdentity, this.databaseBytes, this.Generation)
        {
            promotedJournalBytes = this.promotedJournalBytes is null ? null : (byte[])this.promotedJournalBytes.Clone(),
            preparingJournalBytes = this.preparingJournalBytes is null ? null : (byte[])this.preparingJournalBytes.Clone(),
        };

        public InMemoryRecoveryBackingStore CloneWithGeneration(long newGeneration) => new(this.DatabaseIdentity, this.databaseBytes, newGeneration)
        {
            promotedJournalBytes = this.promotedJournalBytes is null ? null : (byte[])this.promotedJournalBytes.Clone(),
            preparingJournalBytes = this.preparingJournalBytes is null ? null : (byte[])this.preparingJournalBytes.Clone(),
        };

        public Stream OpenDatabaseStream() => new BackingStream(this, isJournal: false);

        public bool HasPendingJournal(Stream databaseStream)
        {
            ArgumentNullException.ThrowIfNull(databaseStream);
            return (this.promotedJournalBytes is { Length: > 0 }) || (this.preparingJournalBytes is { Length: > 0 });
        }

        public IPersistentStreamRecoveryLease AcquireExclusiveLease(Stream databaseStream)
        {
            ArgumentNullException.ThrowIfNull(databaseStream);
            if (this.activeLease is { IsDisposed: false })
            {
                throw new JetOperationException(
                    JetErrorCode.DatabaseInUse,
                    "The caller stream is already leased to another writer.");
            }

            var lease = new ActiveLease(this);
            this.activeLease = lease;
            return lease;
        }

        private void EnsureActiveLease(ActiveLease lease)
        {
            if (lease.IsDisposed || !ReferenceEquals(this.activeLease, lease))
            {
                throw new JetOperationException(
                    JetErrorCode.DatabaseInUse,
                    "The recovery lease is stale or disposed.");
            }
        }

        private sealed class ActiveLease(InMemoryRecoveryBackingStore owner) : IPersistentStreamRecoveryLease
        {
            public bool IsDisposed { get; private set; }

            public string DatabaseIdentity => owner.DatabaseIdentity;

            public long Generation => owner.Generation;

            public void BindGeneration(long nextGeneration)
            {
                owner.EnsureActiveLease(this);
                ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(nextGeneration, owner.Generation);
                owner.Generation = nextGeneration;
            }

            public bool TryOpenPendingJournal(out Stream? committedJournal, out Stream? preparingJournal)
            {
                owner.EnsureActiveLease(this);
                committedJournal = owner.promotedJournalBytes is { Length: > 0 }
                    ? new BackingStream(owner, isJournal: true, isPreparing: false)
                    : null;
                preparingJournal = owner.preparingJournalBytes is { Length: > 0 }
                    ? new BackingStream(owner, isJournal: true, isPreparing: true)
                    : null;
                return committedJournal is not null || preparingJournal is not null;
            }

            public Stream CreatePreparingJournal()
            {
                owner.EnsureActiveLease(this);
                owner.preparingJournalBytes = [];
                return new BackingStream(owner, isJournal: true, isPreparing: true);
            }

            public Stream PromotePreparingJournal()
            {
                owner.EnsureActiveLease(this);
                owner.promotedJournalBytes = owner.preparingJournalBytes ?? [];
                owner.preparingJournalBytes = null;
                return new BackingStream(owner, isJournal: true, isPreparing: false);
            }

            public void DiscardPreparingJournal()
            {
                owner.EnsureActiveLease(this);
                owner.preparingJournalBytes = null;
            }

            public void RetireJournal()
            {
                owner.EnsureActiveLease(this);
                if (owner.SuppressNextRetire)
                {
                    owner.SuppressNextRetire = false;
                    return;
                }

                owner.promotedJournalBytes = null;
                owner.preparingJournalBytes = null;
            }

            public void FenceDatabase(Stream databaseStream)
            {
                owner.EnsureActiveLease(this);
                owner.DatabaseFenceCount++;
                if (owner.failOnDbFence > 0 && --owner.failOnDbFence == 0)
                {
                    throw new IOException("Simulated database fence failure.");
                }
            }

            public void FenceJournal(Stream journalStream)
            {
                owner.EnsureActiveLease(this);
                owner.JournalFenceCount++;
                if (owner.failOnJournalFence > 0 && --owner.failOnJournalFence == 0)
                {
                    throw new IOException("Simulated journal fence failure.");
                }
            }

            public void Dispose()
            {
                if (!this.IsDisposed)
                {
                    this.IsDisposed = true;
                    if (ReferenceEquals(owner.activeLease, this))
                    {
                        owner.activeLease = null;
                    }
                }
            }
        }

        private sealed class BackingStream(InMemoryRecoveryBackingStore owner, bool isJournal, bool isPreparing = false) : Stream
        {
            private long position;

            public override bool CanRead => true;

            public override bool CanSeek => true;

            public override bool CanWrite => true;

            public override long Length => this.GetBuffer().Length;

            public override long Position
            {
                get => this.position;
                set => this.SetPosition(value);
            }

            public override void Flush()
            {
            }

            public override int Read(byte[] buffer, int offset, int count) => this.Read(buffer.AsSpan(offset, count));

            public override int Read(Span<byte> buffer)
            {
                byte[] data = this.GetBuffer();
                if (this.position >= data.Length)
                {
                    return 0;
                }

                int available = (int)Math.Min(buffer.Length, data.Length - this.position);
                data.AsSpan((int)this.position, available).CopyTo(buffer);
                this.position += available;
                return available;
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                long target = origin switch
                {
                    SeekOrigin.Begin => offset,
                    SeekOrigin.Current => this.position + offset,
                    SeekOrigin.End => this.Length + offset,
                    _ => throw new ArgumentOutOfRangeException(nameof(origin)),
                };
                ArgumentOutOfRangeException.ThrowIfNegative(target);
                this.position = target;
                return this.position;
            }

            public override void SetLength(long value)
            {
                ArgumentOutOfRangeException.ThrowIfNegative(value);
                byte[] current = this.GetBuffer();
                if (!isJournal && owner.FailAfterTruncateAndFailRestore && value < current.Length)
                {
                    Array.Resize(ref current, checked((int)value));
                    this.SetBuffer(current);
                    owner.failDbWritesFrom = 1;
                    owner.dbWriteCount = 0;
                    throw new IOException("Simulated failure after physical truncation.");
                }

                if (!isJournal && owner.failDbWritesFrom > 0 && ++owner.dbWriteCount >= owner.failDbWritesFrom)
                {
                    throw new IOException("Simulated database resize failure.");
                }

                Array.Resize(ref current, checked((int)value));
                this.SetBuffer(current);
                if (this.position > value)
                {
                    this.position = value;
                }
            }

            public override void Write(byte[] buffer, int offset, int count) => this.Write(buffer.AsSpan(offset, count));

            public override void Write(ReadOnlySpan<byte> buffer)
            {
                if (!isJournal && owner.failDbWritesFrom > 0 && ++owner.dbWriteCount >= owner.failDbWritesFrom)
                {
                    throw new IOException("Simulated database write failure.");
                }

                byte[] current = this.GetBuffer();
                int end = checked((int)this.position + buffer.Length);
                if (end > current.Length)
                {
                    Array.Resize(ref current, end);
                }

                buffer.CopyTo(current.AsSpan((int)this.position, buffer.Length));
                this.SetBuffer(current);
                this.position = end;
            }

            private void SetPosition(long value)
            {
                ArgumentOutOfRangeException.ThrowIfNegative(value);
                this.position = value;
            }

            private byte[] GetBuffer()
            {
                if (!isJournal)
                {
                    return owner.databaseBytes;
                }

                return isPreparing ? (owner.preparingJournalBytes ?? []) : (owner.promotedJournalBytes ?? []);
            }

            private void SetBuffer(byte[] bytes)
            {
                if (!isJournal)
                {
                    owner.databaseBytes = bytes;
                }
                else if (isPreparing)
                {
                    owner.preparingJournalBytes = bytes;
                }
                else
                {
                    owner.promotedJournalBytes = bytes;
                }
            }
        }
    }
}
