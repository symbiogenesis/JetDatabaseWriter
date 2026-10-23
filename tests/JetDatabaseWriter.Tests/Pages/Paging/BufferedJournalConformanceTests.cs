namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using System.Buffers.Binary;
using System.IO;
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
}
