namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Paging;
using Xunit;

/// <summary>Replays selected buffered-protocol traces against the real journal and recovery loop.</summary>
public sealed class BufferedJournalConformanceTests
{
    /// <summary>Reordered after-image bytes and repeated torn replay retain the first complete original image.</summary>
    /// <param name="pageSize">The physical page size.</param>
    /// <param name="append">Whether the surviving image has an appended page instead of a removed tail.</param>
    [Theory]
    [InlineData(2048, false)]
    [InlineData(4096, false)]
    [InlineData(2048, true)]
    [InlineData(4096, true)]
    public void Recover_BufferedMixtureAndRepeatedReplayCuts_RestoresExactOriginal(int pageSize, bool append)
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-buffered-trace-" + Guid.NewGuid().ToString("N") + ".mdb");
        string sidecar = PersistentRollbackJournal.JournalPath(path);
        byte[] original = new byte[(pageSize * 3) + 17];
        for (int index = 0; index < original.Length; index++)
        {
            original[index] = (byte)((index % 251) + 1);
        }

        File.WriteAllBytes(path, original);
        try
        {
            using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                using var journal = PersistentRollbackJournal.Create(database, pageSize);
                byte[] first = new byte[pageSize];
                byte[] second = new byte[pageSize];
                Array.Fill(first, (byte)252);
                Array.Fill(second, (byte)253);

                // AppendBefore/AppendWrite/FlushJournal precede each issued write.
                // Persist a byte mixture rather than either sequential after-image.
                journal.RecordBeforeWrite(pageSize, first);
                journal.RecordBeforeWrite(pageSize, second);
                byte[] mixture = original.AsSpan(pageSize, pageSize).ToArray();
                for (int index = 0; index < mixture.Length; index++)
                {
                    mixture[index] = (index % 3) switch
                    {
                        0 => first[index],
                        1 => second[index],
                        _ => mixture[index],
                    };
                }

                database.Position = pageSize;
                database.Write(mixture, 0, mixture.Length);
                journal.RecordBeforeWrite(pageSize * 3L, first);
                database.Position = pageSize * 3L;
                database.Write(first, 0, first.Length);
                if (append)
                {
                    long offset = pageSize * 4L;
                    journal.RecordBeforeWrite(offset, first);
                    database.Position = offset;
                    database.Write(first, 0, first.Length);
                }
                else
                {
                    // Capture the entire removed tail, including its partial last page.
                    journal.RecordBeforeTruncate(pageSize * 2L);
                    database.SetLength(pageSize * 2L);
                }

                database.Flush(flushToDisk: true);
            }

            // Crash/BeginUndo/ReplayNext is repeated against the same generation.
            // The last cut is after SetLength, including removal of appended bytes.
            foreach (int cut in new[] { 1, 2, 0 })
            {
                using var interrupted = new ReplayCutStream(path, cut);
                Assert.Throws<IOException>(() => PersistentRollbackJournal.Recover(interrupted));
                Assert.True(File.Exists(sidecar));
            }

            Assert.Equal(original, File.ReadAllBytes(path));
            using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                // FlushDatabase/FinishUndo precedes the completion decision and cleanup.
                Assert.Throws<IOException>(() => PersistentRollbackJournal.Recover(database, RefuseCleanup));
                byte[] decision = File.ReadAllBytes(sidecar);
                Assert.Throws<IOException>(() => PersistentRollbackJournal.Recover(database, RefuseCleanup));
                Assert.Equal(decision, File.ReadAllBytes(sidecar));
                PersistentRollbackJournal.Recover(database);
                PersistentRollbackJournal.Recover(database);
            }

            Assert.Equal(original, File.ReadAllBytes(path));
            Assert.False(File.Exists(sidecar));
        }
        finally
        {
            File.Delete(path);
            File.Delete(sidecar);
            File.Delete(sidecar + ".preparing");
        }
    }

    /// <summary>Uncertain database flushes cannot decide a commit, and interrupted replay publishes only the required tail marker.</summary>
    /// <param name="pageSize">The physical page size.</param>
    /// <param name="truncate">Whether the mutation removes an original tail.</param>
    /// <param name="persistBeforeFailure">Whether the injected flush persists bytes before reporting failure.</param>
    [Theory]
    [InlineData(2048, false, false)]
    [InlineData(2048, false, true)]
    [InlineData(2048, true, false)]
    [InlineData(2048, true, true)]
    [InlineData(4096, false, false)]
    [InlineData(4096, false, true)]
    [InlineData(4096, true, false)]
    [InlineData(4096, true, true)]
    public void Recover_FailedCommitAndReplayFlushes_RetainsUndecidedEvidence(int pageSize, bool truncate, bool persistBeforeFailure)
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-flush-trace-" + Guid.NewGuid().ToString("N") + ".mdb");
        string sidecar = PersistentRollbackJournal.JournalPath(path);
        byte[] original = new byte[(pageSize * 3) + 17];
        Array.Fill(original, (byte)71);
        File.WriteAllBytes(path, original);
        try
        {
            using (var database = new FlushCutStream(path, persistBeforeFailure))
            {
                using var journal = PersistentRollbackJournal.Create(database, pageSize);
                byte[] after = new byte[pageSize];
                Array.Fill(after, (byte)93);
                journal.RecordBeforeWrite(0, after);
                database.Position = 0;
                database.Write(after, 0, after.Length);
                if (truncate)
                {
                    journal.RecordBeforeTruncate(pageSize);
                    database.SetLength(pageSize);
                }

                database.RefuseFlush = true;
                Assert.Throws<IOException>(journal.Complete);
                Assert.True(File.Exists(sidecar));
            }

            Assert.Equal(0, CountRecords(sidecar, pageSize, original.Length, -1));
            using (var database = new FlushCutStream(path, persistBeforeFailure) { RefuseFlush = true })
            {
                Assert.Throws<IOException>(() => PersistentRollbackJournal.Recover(database));
                byte[] evidence = File.ReadAllBytes(sidecar);
                Assert.Equal(truncate ? 1 : 0, CountRecords(sidecar, pageSize, original.Length, -3));
                Assert.Equal(0, CountRecords(sidecar, pageSize, original.Length, -1));
                Assert.Throws<IOException>(() => PersistentRollbackJournal.Recover(database));
                Assert.Equal(evidence, File.ReadAllBytes(sidecar));
                database.RefuseFlush = false;
                PersistentRollbackJournal.Recover(database);
            }

            Assert.Equal(original, File.ReadAllBytes(path));
            Assert.False(File.Exists(sidecar));
        }
        finally
        {
            File.Delete(path);
            File.Delete(sidecar);
            File.Delete(sidecar + ".preparing");
        }
    }

    /// <summary>Removing a replay marker or zeroing a retained captured prefix rejects the same interrupted image without writes.</summary>
    /// <param name="pageSize">The physical page size.</param>
    /// <param name="removeMarker">Whether to remove the complete marker from interrupted evidence.</param>
    /// <param name="corruptPrefix">Whether to zero an unauthorized byte before the removed tail.</param>
    [Theory]
    [InlineData(2048, false, false)]
    [InlineData(2048, false, true)]
    [InlineData(2048, true, false)]
    [InlineData(2048, true, true)]
    [InlineData(4096, false, false)]
    [InlineData(4096, false, true)]
    [InlineData(4096, true, false)]
    [InlineData(4096, true, true)]
    public void Recover_InterruptedZeroTailMutationControls_RequireMarkerAndRetainedPrefix(int pageSize, bool removeMarker, bool corruptPrefix)
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-marker-control-" + Guid.NewGuid().ToString("N") + ".mdb");
        string sidecar = PersistentRollbackJournal.JournalPath(path);
        byte[] original = new byte[(pageSize * 3) + 17];
        Array.Fill(original, (byte)71);
        File.WriteAllBytes(path, original);
        try
        {
            using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                using var journal = PersistentRollbackJournal.Create(database, pageSize);
                byte[] after = new byte[pageSize];
                Array.Fill(after, (byte)93);
                foreach (long offset in new[] { 0L, pageSize * 3L })
                {
                    journal.RecordBeforeWrite(offset, after);
                    database.Position = offset;
                    database.Write(after, 0, after.Length);
                }

                journal.RecordBeforeTruncate(pageSize);
                database.SetLength(pageSize);
                database.Flush(flushToDisk: true);
            }

            // Captures are replayed in insertion order. The second write restores
            // the final partial page first, exposing a zero-filled removed gap.
            using (var interrupted = new ReplayCutStream(path, 2))
            {
                Assert.Throws<IOException>(() => PersistentRollbackJournal.Recover(interrupted));
            }

            Assert.Equal(1, CountRecords(sidecar, pageSize, original.Length, -3));
            using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                // A final length restoration is also a permitted replay mutation.
                // Equal original length makes marker deletion fail content admission,
                // rather than merely the intermediate-length guard.
                database.SetLength(original.Length);
                if (corruptPrefix)
                {
                    database.Position = 17;
                    database.WriteByte(0);
                }

                database.Flush(flushToDisk: true);
            }

            byte[] evidence = File.ReadAllBytes(sidecar);
            if (removeMarker)
            {
                int marker = evidence.Length - pageSize - 40;
                Assert.Equal(-3, BinaryPrimitives.ReadInt64LittleEndian(evidence.AsSpan(marker, 8)));
                evidence = evidence.AsSpan(0, marker).ToArray();
                File.WriteAllBytes(sidecar, evidence);
            }

            if (removeMarker || corruptPrefix)
            {
                byte[] current = File.ReadAllBytes(path);
                using var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                for (int attempt = 0; attempt < 2; attempt++)
                {
                    Assert.Throws<JetCorruptDataException>(() => PersistentRollbackJournal.Recover(database));
                    Assert.Equal(evidence, File.ReadAllBytes(sidecar));
                    byte[] unchanged = new byte[current.Length];
                    database.Position = 0;
                    database.ReadExactly(unchanged);
                    Assert.Equal(current, unchanged);
                }
            }
            else
            {
                using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                {
                    PersistentRollbackJournal.Recover(database);
                }

                Assert.Equal(original, File.ReadAllBytes(path));
                Assert.False(File.Exists(sidecar));
            }
        }
        finally
        {
            File.Delete(path);
            File.Delete(sidecar);
            File.Delete(sidecar + ".preparing");
        }
    }

    /// <summary>A write beyond the shortened file retains recovery for the zero-filled removed gap.</summary>
    /// <param name="pageSize">The physical page size.</param>
    /// <param name="targetPage">The reextended page, including contiguous and sparse writes below the original length.</param>
    /// <param name="partial">Whether an earlier partial write leaves a retained unaligned prefix.</param>
    [Theory]
    [InlineData(2048, 1, false)]
    [InlineData(2048, 3, false)]
    [InlineData(2048, 4, false)]
    [InlineData(2048, 4, true)]
    [InlineData(4096, 1, false)]
    [InlineData(4096, 3, false)]
    [InlineData(4096, 4, false)]
    [InlineData(4096, 4, true)]
    public void Recover_TruncateThenSparseReextension_RestoresOriginal(int pageSize, int targetPage, bool partial)
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-sparse-reextend-" + Guid.NewGuid().ToString("N") + ".mdb");
        string sidecar = PersistentRollbackJournal.JournalPath(path);
        byte[] original = new byte[(pageSize * 4) + 17];
        Array.Fill(original, (byte)71);
        File.WriteAllBytes(path, original);
        try
        {
            using var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            using (var journal = PersistentRollbackJournal.Create(database, pageSize))
            {
                journal.RecordBeforeTruncate(pageSize);
                database.SetLength(pageSize);
                byte[] after = new byte[pageSize];
                Array.Fill(after, (byte)93);
                if (partial)
                {
                    journal.RecordBeforeWrite(pageSize, after);
                    database.Position = pageSize;
                    database.Write(after, 0, 17);
                }

                journal.RecordBeforeWrite(pageSize * (long)targetPage, after);
                database.Position = pageSize * (long)targetPage;
                database.Write(after, 0, after.Length);
                database.Flush(flushToDisk: true);
            }

            PersistentRollbackJournal.Recover(database);
            byte[] restored = new byte[original.Length];
            database.Position = 0;
            database.ReadExactly(restored);
            Assert.Equal(original.Length, database.Length);
            Assert.Equal(original, restored);
            Assert.False(File.Exists(sidecar));
        }
        finally
        {
            File.Delete(path);
            File.Delete(sidecar);
            File.Delete(sidecar + ".preparing");
        }
    }

    /// <summary>Explicit sparse zero intents admit neither unrelated gap bytes nor zeros in an existing partial prefix.</summary>
    /// <param name="pageSize">The physical page size.</param>
    /// <param name="partial">Whether to corrupt the retained partial prefix instead of the new zero gap.</param>
    [Theory]
    [InlineData(2048, false)]
    [InlineData(2048, true)]
    [InlineData(4096, false)]
    [InlineData(4096, true)]
    public void Recover_SparseReextensionMutationControls_RefuseWithoutWrites(int pageSize, bool partial)
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-sparse-control-" + Guid.NewGuid().ToString("N") + ".mdb");
        string sidecar = PersistentRollbackJournal.JournalPath(path);
        byte[] original = new byte[(pageSize * 3) + 17];
        Array.Fill(original, (byte)71);
        File.WriteAllBytes(path, original);
        try
        {
            using var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            using (var journal = PersistentRollbackJournal.Create(database, pageSize))
            {
                journal.RecordBeforeTruncate(pageSize);
                database.SetLength(pageSize);
                byte[] after = new byte[pageSize];
                Array.Fill(after, (byte)93);
                if (partial)
                {
                    journal.RecordBeforeWrite(pageSize, after);
                    database.Position = pageSize;
                    database.Write(after, 0, 17);
                }

                journal.RecordBeforeWrite(pageSize * 3L, after);
                database.Position = pageSize * 3L;
                database.Write(after, 0, after.Length);
                database.Position = partial ? pageSize + 7L : (pageSize * 2L) + 17;
                database.WriteByte(partial ? (byte)0 : (byte)94);
                database.Flush(flushToDisk: true);
            }

            byte[] evidence = File.ReadAllBytes(sidecar);
            byte[] current = new byte[checked((int)database.Length)];
            database.Position = 0;
            database.ReadExactly(current);
            Assert.Throws<JetCorruptDataException>(() => PersistentRollbackJournal.Recover(database));
            Assert.Equal(evidence, File.ReadAllBytes(sidecar));
            byte[] unchanged = new byte[current.Length];
            database.Position = 0;
            database.ReadExactly(unchanged);
            Assert.Equal(current, unchanged);
        }
        finally
        {
            File.Delete(path);
            File.Delete(sidecar);
            File.Delete(sidecar + ".preparing");
        }
    }

    /// <summary>Recorded page intervals admit partial requested writes and sparse-gap length cuts before restartable replay.</summary>
    /// <param name="pageSize">The physical page size.</param>
    /// <param name="cut">A contiguous partial write, sparse partial write or length persisted within its zero gap.</param>
    [Theory]
    [InlineData(2048, 0)]
    [InlineData(2048, 1)]
    [InlineData(2048, 2)]
    [InlineData(4096, 0)]
    [InlineData(4096, 1)]
    [InlineData(4096, 2)]
    public void Recover_TornRegrowthLengthCuts_RestoresAcrossRepeatedReplay(int pageSize, int cut)
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-torn-regrowth-" + Guid.NewGuid().ToString("N") + ".mdb");
        string sidecar = PersistentRollbackJournal.JournalPath(path);
        byte[] original = new byte[(pageSize * 4) + 17];
        Array.Fill(original, (byte)71);
        File.WriteAllBytes(path, original);
        try
        {
            using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                using var journal = PersistentRollbackJournal.Create(database, pageSize);
                journal.RecordBeforeTruncate(pageSize);
                database.SetLength(pageSize);
                byte[] after = new byte[pageSize];
                Array.Fill(after, (byte)93);
                long offset = pageSize * (cut == 0 ? 1L : 2L);
                journal.RecordBeforeWrite(offset, after);
                if (cut == 2)
                {
                    // TornWrite permits a surviving length within the sparse gap.
                    // Its zeros have their own complete, flushed page intent.
                    database.SetLength(pageSize + 17L);
                }
                else
                {
                    database.Position = offset;
                    database.Write(after, 0, 17);
                }

                database.Flush(flushToDisk: true);
            }

            foreach (int replayCut in new[] { 1, 2, 0 })
            {
                using var interrupted = new ReplayCutStream(path, replayCut);
                Assert.Throws<IOException>(() => PersistentRollbackJournal.Recover(interrupted));
                Assert.True(File.Exists(sidecar));
            }

            using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                PersistentRollbackJournal.Recover(database);
            }

            Assert.Equal(original, File.ReadAllBytes(path));
            Assert.False(File.Exists(sidecar));
        }
        finally
        {
            File.Delete(path);
            File.Delete(sidecar);
            File.Delete(sidecar + ".preparing");
        }
    }

    /// <summary>A truncate minimum and recorded write extents still bound admitted reextension lengths.</summary>
    /// <param name="pageSize">The physical page size.</param>
    /// <param name="belowMinimum">Whether the length is below the minimum instead of above every recorded write extent.</param>
    [Theory]
    [InlineData(2048, false)]
    [InlineData(2048, true)]
    [InlineData(4096, false)]
    [InlineData(4096, true)]
    public void Recover_RegrowthLengthOutsideRecordedIntervals_RefusesWithoutWrites(int pageSize, bool belowMinimum)
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-regrowth-bound-" + Guid.NewGuid().ToString("N") + ".mdb");
        string sidecar = PersistentRollbackJournal.JournalPath(path);
        byte[] original = new byte[(pageSize * 4) + 17];
        Array.Fill(original, (byte)71);
        File.WriteAllBytes(path, original);
        try
        {
            using var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            using (var journal = PersistentRollbackJournal.Create(database, pageSize))
            {
                journal.RecordBeforeTruncate(pageSize);
                database.SetLength(pageSize);
                byte[] after = new byte[pageSize];
                Array.Fill(after, (byte)93);
                journal.RecordBeforeWrite(pageSize * 2L, after);
                database.SetLength(belowMinimum ? pageSize - 1L : (pageSize * 3L) + 1);
                database.Flush(flushToDisk: true);
            }

            byte[] evidence = File.ReadAllBytes(sidecar);
            byte[] current = new byte[checked((int)database.Length)];
            database.Position = 0;
            database.ReadExactly(current);
            Assert.Throws<JetCorruptDataException>(() => PersistentRollbackJournal.Recover(database));
            Assert.Equal(evidence, File.ReadAllBytes(sidecar));
            byte[] unchanged = new byte[current.Length];
            database.Position = 0;
            database.ReadExactly(unchanged);
            Assert.Equal(current, unchanged);
        }
        finally
        {
            File.Delete(path);
            File.Delete(sidecar);
            File.Delete(sidecar + ".preparing");
        }
    }

    /// <summary>An older write exposing a zero gap after truncation needs a durable replay marker for admission.</summary>
    /// <param name="pageSize">The physical page size.</param>
    /// <param name="marker">Whether recovery durably authorized zero-filled tail reextension.</param>
    [Theory]
    [InlineData(2048, false)]
    [InlineData(2048, true)]
    [InlineData(4096, false)]
    [InlineData(4096, true)]
    public void Recover_ReorderedWriteAfterTruncate_RequiresMarkerForExposedZeroGap(int pageSize, bool marker)
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-reordered-gap-" + Guid.NewGuid().ToString("N") + ".mdb");
        string sidecar = PersistentRollbackJournal.JournalPath(path);
        byte[] original = new byte[(pageSize * 3) + 17];
        Array.Fill(original, (byte)71);
        File.WriteAllBytes(path, original);
        try
        {
            using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                using var journal = PersistentRollbackJournal.Create(database, pageSize);
                byte[] after = new byte[pageSize];
                Array.Fill(after, (byte)93);
                journal.RecordBeforeWrite(pageSize * 2L, after);
                journal.RecordBeforeTruncate(pageSize);
                database.SetLength(pageSize);
                if (marker)
                {
                    journal.RecordRecoveryStart();
                }

                // Model PersistWrite after PersistLength: the old write intent
                // predates truncation, so it records no sparse zero-gap intent.
                // This is a synthetic persistence survivor, not an OS cache test.
                database.Position = pageSize * 2L;
                database.Write(after, 0, after.Length);
                database.Flush(flushToDisk: true);
            }

            byte[] evidence = File.ReadAllBytes(sidecar);
            byte[] current = File.ReadAllBytes(path);
            Assert.Equal(new byte[pageSize], current.AsSpan(pageSize, pageSize).ToArray());
            if (!marker)
            {
                using var database = new RefusalStream(path);
                for (int attempt = 0; attempt < 2; attempt++)
                {
                    Assert.Throws<JetCorruptDataException>(() => PersistentRollbackJournal.Recover(database));
                    Assert.Equal(evidence, File.ReadAllBytes(sidecar));
                    byte[] unchanged = new byte[current.Length];
                    database.Position = 0;
                    database.ReadExactly(unchanged);
                    Assert.Equal(current, unchanged);
                    Assert.Equal(0, database.Writes);
                    Assert.Equal(0, database.Resizes);
                }
            }
            else
            {
                for (int attempt = 0; attempt < 2; attempt++)
                {
                    using var interrupted = new ReplayCutStream(path, 1);
                    Assert.Throws<IOException>(() => PersistentRollbackJournal.Recover(interrupted));
                    Assert.Equal(evidence, File.ReadAllBytes(sidecar));
                }

                using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                {
                    PersistentRollbackJournal.Recover(database);
                }

                Assert.Equal(original, File.ReadAllBytes(path));
                Assert.False(File.Exists(sidecar));
            }
        }
        finally
        {
            File.Delete(path);
            File.Delete(sidecar);
            File.Delete(sidecar + ".preparing");
        }
    }

    /// <summary>Live rollback preserves its conditional marker through interrupted page, resize and flush effects.</summary>
    /// <param name="pageSize">The physical page size.</param>
    /// <param name="truncate">Whether an original tail was removed.</param>
    /// <param name="cut">The failed effect: torn second write, resize before or after mutation, or final flush.</param>
    [Theory]
    [InlineData(2048, false, 0)]
    [InlineData(2048, false, 1)]
    [InlineData(2048, false, 2)]
    [InlineData(2048, false, 3)]
    [InlineData(2048, true, 0)]
    [InlineData(2048, true, 1)]
    [InlineData(2048, true, 2)]
    [InlineData(2048, true, 3)]
    [InlineData(4096, false, 0)]
    [InlineData(4096, false, 1)]
    [InlineData(4096, false, 2)]
    [InlineData(4096, false, 3)]
    [InlineData(4096, true, 0)]
    [InlineData(4096, true, 1)]
    [InlineData(4096, true, 2)]
    [InlineData(4096, true, 3)]
    public async Task RestoreAsync_RepeatedEffectCuts_RetainsEvidenceAndConvergesOnReopen(int pageSize, bool truncate, int cut)
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-live-replay-" + Guid.NewGuid().ToString("N") + ".mdb");
        string sidecar = PersistentRollbackJournal.JournalPath(path);
        byte[] original = new byte[(pageSize * 3) + 17];
        for (int index = 0; index < original.Length; index++)
        {
            original[index] = (byte)((index % 251) + 1);
        }

        await File.WriteAllBytesAsync(path, original, TestContext.Current.CancellationToken);
        try
        {
            await using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                using var journal = PersistentRollbackJournal.Create(database, pageSize);
                byte[] after = new byte[pageSize];
                Array.Fill(after, (byte)253);
                foreach (long offset in new[] { pageSize * 3L, 0, pageSize * 2L })
                {
                    journal.RecordBeforeWrite(offset, after);
                    database.Position = offset;
                    await database.WriteAsync(after, TestContext.Current.CancellationToken);
                }

                if (truncate)
                {
                    journal.RecordBeforeTruncate(pageSize);
                    database.SetLength(pageSize);
                }

                long replayLength = new FileInfo(sidecar).Length + (truncate ? pageSize + 40L : 0);
                await using var store = new ReplayCutStore(database, cut, sidecar, replayLength);
                for (int attempt = 0; attempt < 2; attempt++)
                {
                    store.BeginAttempt();
                    await Assert.ThrowsAsync<IOException>(async () => await journal.RestoreAsync(store));
                    Assert.Equal(replayLength, new FileInfo(sidecar).Length);

                    // The first capture is the partial original page, although it
                    // precedes lower offsets in the production dictionary loop.
                    var expected = new List<(long Offset, int Count)> { (pageSize * 3L, 17), (0, pageSize) };
                    if (cut != 0)
                    {
                        expected.Add((pageSize * 2L, pageSize));
                        if (truncate)
                        {
                            expected.Add((pageSize, pageSize));
                        }
                    }

                    Assert.Equal(expected, store.Writes);
                    Assert.Equal(cut != 0, store.ResizeAttempted);
                    Assert.Equal(cut == 3, store.FlushAttempted);
                    if (!truncate && cut == 1)
                    {
                        Assert.Equal(pageSize * 4L, database.Length);
                    }
                    else if (cut >= 2)
                    {
                        Assert.Equal(original.Length, database.Length);
                    }
                }
            }

            Assert.Equal(truncate ? 1 : 0, CountRecords(sidecar, pageSize, original.Length, -3));
            Assert.Equal(0, CountRecords(sidecar, pageSize, original.Length, -1));
            await using (var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                Assert.Throws<IOException>(() => PersistentRollbackJournal.Recover(database, RefuseCleanup));
                byte[] decision = await File.ReadAllBytesAsync(sidecar, TestContext.Current.CancellationToken);
                Assert.Equal(truncate ? 1 : 0, CountRecords(sidecar, pageSize, original.Length, -3));
                Assert.Equal(1, CountRecords(sidecar, pageSize, original.Length, -1));
                Assert.Throws<IOException>(() => PersistentRollbackJournal.Recover(database, RefuseCleanup));
                Assert.Equal(decision, await File.ReadAllBytesAsync(sidecar, TestContext.Current.CancellationToken));
                PersistentRollbackJournal.Recover(database);
            }

            Assert.Equal(original, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
            Assert.False(File.Exists(sidecar));
        }
        finally
        {
            File.Delete(path);
            File.Delete(sidecar);
            File.Delete(sidecar + ".preparing");
        }
    }

    /// <summary>The shared replay control orders real page effects before resize, flush and a terminal decision.</summary>
    /// <param name="pageSize">The supported physical page size.</param>
    /// <param name="truncate">Whether preparation removes an original tail.</param>
    /// <param name="live">Whether to use the live asynchronous restoration path.</param>
    [Theory]
    [InlineData(2048, false, false)]
    [InlineData(2048, true, false)]
    [InlineData(2048, false, true)]
    [InlineData(2048, true, true)]
    [InlineData(4096, false, false)]
    [InlineData(4096, true, false)]
    [InlineData(4096, false, true)]
    [InlineData(4096, true, true)]
    public async Task Replay_SuccessfulEffects_FollowGeneratedControlOrder(int pageSize, bool truncate, bool live)
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-replay-order-" + Guid.NewGuid().ToString("N") + ".mdb");
        string sidecar = PersistentRollbackJournal.JournalPath(path);
        byte[] original = new byte[(pageSize * 3) + 17];
        Array.Fill(original, (byte)71);
        await File.WriteAllBytesAsync(path, original, TestContext.Current.CancellationToken);
        try
        {
            await using var database = new ReplayOrderStream(path);
            var journal = PersistentRollbackJournal.Create(database, pageSize);
            try
            {
                PrepareReplayOrder(journal, database, pageSize, truncate);
                database.Observe = true;
                if (live)
                {
                    await using var store = new ReplayOrderStore(database);
                    await journal.RestoreAsync(store);
                }
                else
                {
                    journal.Dispose();
                    PersistentRollbackJournal.Recover(database);
                }

                var expected = new List<(string Kind, long Offset, int Count)>
                {
                    ("page", pageSize * 3L, 17),
                    ("page", 0, pageSize),
                };
                if (truncate)
                {
                    expected.Add(("page", pageSize, pageSize));
                    expected.Add(("page", pageSize * 2L, pageSize));
                }

                expected.Add(("resize", original.Length, 0));
                expected.Add(("flush", 0, 0));
                if (live)
                {
                    // Complete also flushes the underlying database before its decision.
                    expected.Add(("flush", 0, 0));
                }

                database.Observe = false;
                Assert.Equal(expected, database.Effects);
                Assert.False(File.Exists(sidecar));
                byte[] restored = new byte[original.Length];
                database.Position = 0;
                database.ReadExactly(restored);
                Assert.Equal(original, restored);
            }
            finally
            {
                journal.Dispose();
            }
        }
        finally
        {
            File.Delete(path);
            File.Delete(sidecar);
            File.Delete(sidecar + ".preparing");
        }
    }

    private static void PrepareReplayOrder(PersistentRollbackJournal journal, ReplayOrderStream database, int pageSize, bool truncate)
    {
        byte[] after = new byte[pageSize];
        Array.Fill(after, (byte)93);
        foreach (long offset in new[] { pageSize * 3L, 0 })
        {
            journal.RecordBeforeWrite(offset, after);
            database.Position = offset;
            database.Write(after, 0, after.Length);
        }

        if (truncate)
        {
            journal.RecordBeforeTruncate(pageSize);
            database.SetLength(pageSize);
        }

        database.Flush(flushToDisk: true);
    }

    private static int CountRecords(string sidecar, int pageSize, int originalLength, long tag)
    {
        byte[] evidence = File.ReadAllBytes(sidecar);
        int start = 128 + ((originalLength + pageSize - 1) / pageSize * 32);
        int count = 0;
        for (int offset = start; offset + pageSize + 40 <= evidence.Length; offset += pageSize + 40)
        {
            if (BinaryPrimitives.ReadInt64LittleEndian(evidence.AsSpan(offset, 8)) == tag)
            {
                count++;
            }
        }

        return count;
    }

    /// <summary>Interrupts recovery after its decision is durable.</summary>
    /// <exception cref="IOException">Always interrupts the cleanup boundary.</exception>
    private static void RefuseCleanup() => throw new IOException("Injected interruption before evidence cleanup.");

    private sealed class ReplayOrderStream(string path)
        : FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None)
    {
        internal bool Observe { get; set; }

        internal List<(string Kind, long Offset, int Count)> Effects { get; } = [];

        public override void Write(byte[] buffer, int offset, int count)
        {
            this.RecordEffect("page", this.Position, count);
            base.Write(buffer, offset, count);
        }

        public override void SetLength(long value)
        {
            this.RecordEffect("resize", value, 0);
            base.SetLength(value);
        }

        public override void Flush(bool flushToDisk)
        {
            if (this.Observe)
            {
                Assert.True(flushToDisk);
            }

            this.RecordEffect("flush", 0, 0);
            base.Flush(flushToDisk);
        }

        private void RecordEffect(string kind, long offset, int count)
        {
            if (this.Observe)
            {
                this.Effects.Add((kind, offset, count));
            }
        }
    }

    private sealed class ReplayOrderStore(ReplayOrderStream database) : IPageStore
    {
        public StoreCapabilities Capabilities => new(true, false, true, false, false, true);

        public long Length => database.Length;

        public ValueTask ReadAsync(long offset, Memory<byte> buffer, bool inline, CancellationToken cancellationToken)
        {
            database.Position = offset;
            database.ReadExactly(buffer.Span);
            return ValueTask.CompletedTask;
        }

        public ValueTask WriteAsync(long offset, ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
        {
            database.Position = offset;
            byte[] bytes = buffer.ToArray();
            database.Write(bytes, 0, bytes.Length);
            return ValueTask.CompletedTask;
        }

        public ValueTask SetLengthAsync(long length, CancellationToken cancellationToken)
        {
            database.SetLength(length);
            return ValueTask.CompletedTask;
        }

        public ValueTask FlushAsync(bool toDisk, CancellationToken cancellationToken)
        {
            database.Flush(toDisk);
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public void DisposeManagedResources()
        {
        }
    }

    private sealed class FlushCutStream(string path, bool persistBeforeFailure) : FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None)
    {
        internal bool RefuseFlush { get; set; }

        public override void Flush(bool flushToDisk)
        {
            if (this.RefuseFlush)
            {
                if (persistBeforeFailure)
                {
                    base.Flush(flushToDisk);
                }

                throw new IOException("Injected uncertain database flush.");
            }

            base.Flush(flushToDisk);
        }
    }

    private sealed class ReplayCutStream(string path, int cut) : FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None)
    {
        private int writes;

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (++this.writes == cut)
            {
                base.Write(buffer, offset, Math.Max(1, count / 2));
                this.Flush(flushToDisk: true);
                throw new IOException("Injected interruption during a replay page.");
            }

            base.Write(buffer, offset, count);
        }

        public override void SetLength(long value)
        {
            base.SetLength(value);
            if (cut == 0)
            {
                this.Flush(flushToDisk: true);
                throw new IOException("Injected interruption after replay length restoration.");
            }
        }
    }

    private sealed class RefusalStream(string path) : FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None)
    {
        internal int Writes { get; private set; }

        internal int Resizes { get; private set; }

        public override void Write(byte[] buffer, int offset, int count)
        {
            this.Writes++;
            base.Write(buffer, offset, count);
        }

        public override void SetLength(long value)
        {
            this.Resizes++;
            base.SetLength(value);
        }
    }

    private sealed class ReplayCutStore(FileStream database, int cut, string sidecar, long replayLength) : IPageStore
    {
        private readonly StreamPageStore inner = new(database, leaveOpen: true);

        public StoreCapabilities Capabilities => this.inner.Capabilities;

        public long Length => this.inner.Length;

        internal List<(long Offset, int Count)> Writes { get; } = [];

        internal bool ResizeAttempted { get; private set; }

        internal bool FlushAttempted { get; private set; }

        public ValueTask ReadAsync(long offset, Memory<byte> buffer, bool inline, CancellationToken cancellationToken)
            => this.inner.ReadAsync(offset, buffer, inline, cancellationToken);

        public async ValueTask WriteAsync(long offset, ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
        {
            // A required marker must already be flushed before any replay effect.
            Assert.Equal(replayLength, new FileInfo(sidecar).Length);
            this.Writes.Add((offset, buffer.Length));
            if (cut == 0 && this.Writes.Count == 2)
            {
                await this.inner.WriteAsync(offset, buffer[..(buffer.Length / 2)], cancellationToken);
                await this.inner.FlushAsync(true, cancellationToken);
                throw new IOException("Injected live rollback interruption during a page.");
            }

            await this.inner.WriteAsync(offset, buffer, cancellationToken);
        }

        public async ValueTask SetLengthAsync(long length, CancellationToken cancellationToken)
        {
            this.ResizeAttempted = true;
            if (cut == 1)
            {
                throw new IOException("Injected live rollback interruption before resize.");
            }

            await this.inner.SetLengthAsync(length, cancellationToken);
            if (cut == 2)
            {
                throw new IOException("Injected live rollback interruption after resize.");
            }
        }

        public async ValueTask FlushAsync(bool toDisk, CancellationToken cancellationToken)
        {
            this.FlushAttempted = true;
            Assert.True(toDisk);
            await this.inner.FlushAsync(toDisk, cancellationToken);
            throw new IOException("Injected live rollback interruption after flush.");
        }

        public ValueTask DisposeAsync() => this.inner.DisposeAsync();

        public void DisposeManagedResources() => this.inner.DisposeManagedResources();

        internal void BeginAttempt()
        {
            this.Writes.Clear();
            this.ResizeAttempted = false;
            this.FlushAttempted = false;
        }
    }
}
