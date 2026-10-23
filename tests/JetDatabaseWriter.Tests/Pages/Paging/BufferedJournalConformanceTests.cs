namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using System.IO;
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

    /// <summary>Interrupts recovery after its decision is durable.</summary>
    /// <exception cref="IOException">Always interrupts the cleanup boundary.</exception>
    private static void RefuseCleanup() => throw new IOException("Injected interruption before evidence cleanup.");

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
