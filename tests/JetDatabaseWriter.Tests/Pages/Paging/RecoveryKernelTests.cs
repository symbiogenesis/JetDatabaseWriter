namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Security.Cryptography;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Checks the generated recovery boundary against independent bounded and edge-case oracles.</summary>
public sealed class RecoveryKernelTests
{
    /// <summary>Original payload and manifest slices select only the exact bytes of an aligned original page.</summary>
    [Fact]
    public void OriginalPageSlices_BoundaryInputs_MatchIndependentOracle()
    {
        uint[] pageSizes = [0, 1, 2048, 4096, uint.MaxValue];
        ulong[] originalLengths = [0, 1, 2047, 2048, 2049, 4095, 4096, 4097, 2147483647, 2147483648, 2147483649, ulong.MaxValue];
        ulong[] offsets = [0, 1, 2048, 4096, 2147479552, 2147481600, 2147483648, ulong.MaxValue];
        foreach (uint pageSize in pageSizes)
        {
            foreach (ulong originalLength in originalLengths)
            {
                foreach (ulong offset in offsets)
                {
                    bool valid = pageSize is 2048 or 4096 && originalLength <= 2147483648
                        && offset < originalLength && offset % pageSize == 0;
                    uint expectedLength = valid ? checked((uint)Math.Min(pageSize, originalLength - offset)) : 0;
                    ulong expectedHashOffset = valid ? 128 + (32 * (offset / pageSize)) : 0;
                    Assert.Equal(expectedLength, RecoveryKernel.OriginalPageLength(pageSize, originalLength, offset));
                    Assert.Equal(expectedHashOffset, RecoveryKernel.OriginalPageHashOffset(pageSize, originalLength, offset));
                    if (valid)
                    {
                        Assert.InRange(offset + expectedLength, offset + 1, originalLength);
                        Assert.InRange(expectedHashOffset + 32, 160UL, RecoveryKernel.JournalRecordStart(pageSize, originalLength));
                    }
                }
            }
        }
    }

    /// <summary>A payload source must belong to an entire aligned record inside the complete prefix.</summary>
    [Fact]
    public void JournalRecordPayloadOffset_PrefixCutsAndPositions_MatchesIndependentOracle()
    {
        foreach (uint pageSize in new uint[] { 2048, 4096 })
        {
            foreach (ulong originalLength in new ulong[] { 0, 1, 2049, 4097, 2147483648 })
            {
                ulong start = 128 + (32 * ((originalLength / pageSize) + (originalLength % pageSize == 0 ? 0UL : 1UL)));
                ulong size = pageSize + 40UL;
                ulong[] positions = [0, start - 1, start, start + 1, start + size - 1, start + size, start + (2 * size), 17179869184, ulong.MaxValue];
                for (ulong cut = 0; cut <= 2 * size; cut++)
                {
                    ulong journalLength = start + cut;
                    ulong completeEnd = start + (cut / size * size);
                    foreach (ulong position in positions)
                    {
                        bool valid = position >= start && position < completeEnd && (position - start) % size == 0;
                        ulong expected = valid ? position + 8 : 0;
                        Assert.Equal(expected, RecoveryKernel.JournalRecordPayloadOffset(pageSize, originalLength, journalLength, position));
                        if (valid)
                        {
                            Assert.True(expected + pageSize + 32 <= completeEnd);
                        }
                    }
                }

                ulong maximumCompleteEnd = start + ((17179869184 - start) / size * size);
                ulong lastPosition = maximumCompleteEnd - size;
                Assert.Equal(lastPosition + 8, RecoveryKernel.JournalRecordPayloadOffset(pageSize, originalLength, 17179869184, lastPosition));
                Assert.Equal(0UL, RecoveryKernel.JournalRecordPayloadOffset(pageSize, originalLength, 17179869184, maximumCompleteEnd));
            }
        }

        Assert.Equal(0UL, RecoveryKernel.JournalRecordPayloadOffset(0, 0, 128, 128));
        Assert.Equal(0UL, RecoveryKernel.JournalRecordPayloadOffset(2048, 2147483649, 17179869184, 128));
        Assert.Equal(0UL, RecoveryKernel.JournalRecordPayloadOffset(2048, 0, 17179869185, 128));
        Assert.Equal(0UL, RecoveryKernel.JournalRecordPayloadOffset(2048, 0, ulong.MaxValue, 128));
    }

    /// <summary>Recovery copies exactly a captured partial page's original prefix before restoring the file length.</summary>
    /// <param name="pageSize">The supported physical page size.</param>
    /// <param name="tailLength">The nonempty original final-page prefix.</param>
    [Theory]
    [InlineData(2048, 1)]
    [InlineData(2048, 17)]
    [InlineData(2048, 2047)]
    [InlineData(4096, 1)]
    [InlineData(4096, 17)]
    [InlineData(4096, 4095)]
    public void Recover_PartialOriginalPayload_WritesOnlyOriginalPrefix(int pageSize, int tailLength)
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-payload-kernel-" + Guid.NewGuid().ToString("N") + ".mdb");
        byte[] original = new byte[pageSize + tailLength];
        RandomNumberGenerator.Fill(original);
        File.WriteAllBytes(path, original);
        try
        {
            using var database = new ReplayWriteStream(path);
            using (var journal = PersistentRollbackJournal.Create(database, pageSize))
            {
                byte[] after = new byte[pageSize];
                Array.Fill(after, (byte)165);
                journal.RecordBeforeWrite(pageSize, after);
                database.Position = pageSize;
                database.Write(after, 0, after.Length);
                database.Flush(flushToDisk: true);
            }

            database.ObserveReplay = true;
            PersistentRollbackJournal.Recover(database);
            Assert.Equal(1, database.ReplayWriteCalls);
            Assert.Equal(pageSize, database.ReplayOffset);
            Assert.Equal(tailLength, database.ReplayCount);
            Assert.Equal(original.Length, database.Length);
            byte[] restored = new byte[original.Length];
            database.Position = 0;
            database.ReadExactly(restored);
            Assert.Equal(original, restored);
            Assert.False(File.Exists(PersistentRollbackJournal.JournalPath(path)));
        }
        finally
        {
            File.Delete(path);
            File.Delete(PersistentRollbackJournal.JournalPath(path));
        }
    }

    /// <summary>Framing rejects invalid bounds and selects exactly the maximal complete record prefix.</summary>
    [Fact]
    public void JournalFraming_BoundaryInputs_MatchesIndependentOracle()
    {
        uint[] pageSizes = [0, 1, 2048, 4096, uint.MaxValue];
        ulong[] originalLengths = [0, 1, 2047, 2048, 2049, 4095, 4096, 4097, 2147483648, 2147483649, ulong.MaxValue];
        foreach (uint pageSize in pageSizes)
        {
            foreach (ulong originalLength in originalLengths)
            {
                bool valid = pageSize is 2048 or 4096 && originalLength <= 2147483648;
                ulong expectedStart = valid ? 128 + (32 * ((originalLength / pageSize) + (originalLength % pageSize == 0 ? 0UL : 1UL))) : 0;
                Assert.Equal(expectedStart, RecoveryKernel.JournalRecordStart(pageSize, originalLength));
                ulong recordSize = pageSize + 40UL;
                ulong[] journalLengths =
                [
                    0, 1, 127, 128, 129, 17179869184, 17179869185, ulong.MaxValue,
                    expectedStart, expectedStart + 1, expectedStart + recordSize - 1,
                    expectedStart + recordSize, expectedStart + recordSize + 1,
                    expectedStart + (2 * recordSize) - 1, expectedStart + (2 * recordSize),
                ];
                foreach (ulong journalLength in journalLengths)
                {
                    ulong expectedEnd = 0;
                    if (valid && journalLength >= expectedStart && journalLength <= 17179869184)
                    {
                        ulong count = (journalLength - expectedStart) / recordSize;
                        expectedEnd = expectedStart + (count * recordSize);
                    }

                    Assert.Equal(expectedEnd, RecoveryKernel.CompleteJournalEnd(pageSize, originalLength, journalLength));
                    if (expectedEnd != 0)
                    {
                        Assert.InRange(journalLength - expectedEnd, 0UL, recordSize - 1);
                    }
                }
            }
        }
    }

    /// <summary>Recovery replaces incomplete bytes with complete replay/decision records and preserves the complete prefix.</summary>
    /// <param name="pageSize">The supported physical page size.</param>
    /// <param name="incompleteLength">The nonempty trailing fragment length.</param>
    /// <param name="preparationCopy">Whether discovery must also compare a preparation copy's complete prefix.</param>
    [Theory]
    [InlineData(2048, 1, false)]
    [InlineData(2048, 2087, false)]
    [InlineData(4096, 1, false)]
    [InlineData(4096, 4135, false)]
    [InlineData(2048, 2087, true)]
    [InlineData(4096, 4135, true)]
    public void Recover_IncompleteTail_ReplacesOnlyFragmentBeforeReplayAndDecision(int pageSize, int incompleteLength, bool preparationCopy)
    {
        using var fixture = new RecordFixture(pageSize);
        using (var journal = PersistentRollbackJournal.Create(fixture.Database, pageSize))
        {
            journal.RecordBeforeTruncate(0);
            fixture.Database.SetLength(0);
            fixture.Database.Flush(flushToDisk: true);
        }

        byte[] completePrefix = File.ReadAllBytes(fixture.JournalPath);
        fixture.AppendIncomplete(incompleteLength);
        if (preparationCopy)
        {
            File.Copy(fixture.JournalPath, fixture.JournalPath + ".preparing");
        }

        Assert.Throws<IOException>(() => PersistentRollbackJournal.Recover(fixture.Database, () => throw new IOException("Retain completed evidence for inspection.")));
        byte[] completed = File.ReadAllBytes(fixture.JournalPath);
        Assert.Equal(completePrefix, completed.AsSpan(0, completePrefix.Length).ToArray());
        Assert.Equal(completePrefix.Length + (2 * (pageSize + 40)), completed.Length);
        Assert.Equal(-3, BinaryPrimitives.ReadInt64LittleEndian(completed.AsSpan(completePrefix.Length)));
        Assert.Equal(-1, BinaryPrimitives.ReadInt64LittleEndian(completed.AsSpan(completePrefix.Length + pageSize + 40)));
        Assert.Equal(fixture.Original, fixture.ReadDatabase());

        PersistentRollbackJournal.Recover(fixture.Database);
        Assert.False(File.Exists(fixture.JournalPath));
        Assert.False(File.Exists(fixture.JournalPath + ".preparing"));
        Assert.Equal(fixture.Original, fixture.ReadDatabase());
    }

    /// <summary>A partial page-hash manifest cannot be treated as a journal with an incomplete record.</summary>
    [Fact]
    public void Recover_IncompleteManifest_RetainsDatabaseAndEvidence()
    {
        using var fixture = new RecordFixture();
        using (PersistentRollbackJournal.Create(fixture.Database, 2048))
        {
            // Retain the manifest without mutation records.
        }

        byte[] evidence = File.ReadAllBytes(fixture.JournalPath)[..^1];
        File.WriteAllBytes(fixture.JournalPath, evidence);
        Assert.Throws<JetCorruptDataException>(() => PersistentRollbackJournal.Recover(fixture.Database));
        Assert.Equal(fixture.Original, fixture.ReadDatabase());
        Assert.Equal(evidence, File.ReadAllBytes(fixture.JournalPath));
    }

    /// <summary>Signed persisted tags and every control-state combination match an independent classifier.</summary>
    [Fact]
    public void ClassifyJournalRecord_EdgeTagsAndStates_MatchesIndependentOracle()
    {
        long[] tags =
        [
            long.MinValue, long.MinValue + 1, long.MinValue + 2048, long.MinValue + 4096,
            long.MinValue + 2147483648, long.MinValue + 2147483649, -4, -3, -2, -1,
            0, 1, 2048, 4096, 2147479552, 2147481600, 2147483648, long.MaxValue,
        ];
        uint[] pageSizes = [0, 1, 2048, 4096, uint.MaxValue];
        ulong[] originalLengths = [0, 1, 2048, 4097, 2147483648, 2147483649, ulong.MaxValue];
        foreach (long tag in tags)
        {
            foreach (uint pageSize in pageSizes)
            {
                foreach (ulong originalLength in originalLengths)
                {
                    for (int flags = 0; flags < 8; flags++)
                    {
                        bool recovering = (flags & 1) != 0;
                        bool removedTail = (flags & 2) != 0;
                        bool finalRecord = (flags & 4) != 0;
                        byte expected = ExpectedRecordKind(tag, pageSize, originalLength, recovering, removedTail, finalRecord);
                        Assert.Equal(expected, RecoveryKernel.ClassifyJournalRecord(
                            unchecked((ulong)tag), pageSize, originalLength, recovering, removedTail, finalRecord));
                    }
                }
            }
        }
    }

    /// <summary>Checksum-valid rejected record classes leave both database bytes and recovery evidence untouched.</summary>
    /// <param name="tag">The persisted signed record tag.</param>
    /// <param name="removedTail">Whether an earlier truncate intent removed an original tail.</param>
    /// <param name="recovering">Whether an earlier replay marker exists.</param>
    /// <param name="trailingByte">Whether an incomplete record follows the tested record.</param>
    [Theory]
    [InlineData(-3L, false, false, false)]
    [InlineData(-4L, false, false, false)]
    [InlineData(long.MinValue + 1, false, false, false)]
    [InlineData(long.MinValue + 4096, false, false, false)]
    [InlineData(1L, false, false, false)]
    [InlineData(2147483648L, false, false, false)]
    [InlineData(-2L, true, true, false)]
    [InlineData(long.MinValue, true, true, false)]
    [InlineData(0L, true, true, false)]
    [InlineData(-1L, false, false, true)]
    [InlineData(-1L, true, true, true)]
    public void Recover_RejectedRecordClass_RetainsDatabaseAndEvidence(long tag, bool removedTail, bool recovering, bool trailingByte)
    {
        using var fixture = new RecordFixture();
        using (var journal = PersistentRollbackJournal.Create(fixture.Database, 2048))
        {
            if (removedTail)
            {
                journal.RecordBeforeTruncate(0);
            }

            if (recovering)
            {
                journal.RecordRecoveryStart();
            }
        }

        fixture.Append(tag, trailingByte);
        byte[] evidence = File.ReadAllBytes(fixture.JournalPath);
        Assert.Throws<JetCorruptDataException>(() => PersistentRollbackJournal.Recover(fixture.Database));
        Assert.Equal(fixture.Original, fixture.ReadDatabase());
        Assert.Equal(evidence, File.ReadAllBytes(fixture.JournalPath));
    }

    /// <summary>A commit followed by another complete record cannot discard contradictory evidence.</summary>
    [Fact]
    public void Recover_NonterminalCommit_RetainsDatabaseAndEvidence()
    {
        using var fixture = new RecordFixture();
        using (PersistentRollbackJournal.Create(fixture.Database, 2048))
        {
            // Preserve the prepared manifest for independently appended records.
        }

        fixture.Append(-1, false);
        fixture.Append(-3, false);
        byte[] evidence = File.ReadAllBytes(fixture.JournalPath);
        Assert.Throws<JetCorruptDataException>(() => PersistentRollbackJournal.Recover(fixture.Database));
        Assert.Equal(fixture.Original, fixture.ReadDatabase());
        Assert.Equal(evidence, File.ReadAllBytes(fixture.JournalPath));
    }

    /// <summary>Repeated replay markers remain valid and may be followed by a terminal validated decision.</summary>
    /// <param name="committed">Whether to append a terminal decision before recovery.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Recover_RepeatedReplayMarkers_AllowsRecoveryOrTerminalDecision(bool committed)
    {
        using var fixture = new RecordFixture();
        using (var journal = PersistentRollbackJournal.Create(fixture.Database, 2048))
        {
            journal.RecordBeforeTruncate(0);
            journal.RecordRecoveryStart();
            journal.RecordRecoveryStart();
        }

        if (committed)
        {
            fixture.Append(-1, false);
        }
        else
        {
            fixture.Database.SetLength(0);
            fixture.Database.Flush(flushToDisk: true);
        }

        PersistentRollbackJournal.Recover(fixture.Database);
        Assert.Equal(fixture.Original, fixture.ReadDatabase());
        Assert.False(File.Exists(fixture.JournalPath));
    }

    /// <summary>Page offsets accept exactly supported complete aligned pages within the Access bound.</summary>
    [Fact]
    public void IsPageOffset_Boundaries_RequireCompleteAlignedPage()
    {
        uint[] pageSizes = [0, 1, 2048, 4096, uint.MaxValue];
        ulong[] offsets = [0, 1, 2048, 4096, 2147479552, 2147481600, 2147483648, ulong.MaxValue];
        foreach (uint pageSize in pageSizes)
        {
            foreach (ulong offset in offsets)
            {
                bool expected = pageSize is 2048 or 4096 && offset < 2147483648
                    && offset % pageSize == 0 && 2147483648 - offset >= pageSize;
                Assert.Equal(expected, RecoveryKernel.IsPageOffset(offset, pageSize));
            }
        }
    }

    /// <summary>Database lengths include the exact Access size limit and reject unsigned negative representations.</summary>
    [Fact]
    public void IsDatabaseLength_Boundaries_RequireAccessSizeBound()
    {
        Assert.True(RecoveryKernel.IsDatabaseLength(0));
        Assert.True(RecoveryKernel.IsDatabaseLength(2147483648));
        Assert.False(RecoveryKernel.IsDatabaseLength(2147483649));
        Assert.False(RecoveryKernel.IsDatabaseLength(ulong.MaxValue));
    }

    /// <summary>Truncate intents cannot authorize an unaligned or longer file.</summary>
    [Fact]
    public void IsTruncateLength_BoundedInputs_MatchesRetainedPrefix()
    {
        for (uint pageSize = 2048; pageSize <= 4096; pageSize *= 2)
        {
            for (ulong maximum = 0; maximum <= 12288; maximum += 1024)
            {
                for (ulong length = 0; length <= 16384; length += 1024)
                {
                    Assert.Equal(length <= maximum && length % pageSize == 0, RecoveryKernel.IsTruncateLength(length, pageSize, maximum));
                }
            }
        }

        Assert.False(RecoveryKernel.IsTruncateLength(0, 0, 0));
        Assert.False(RecoveryKernel.IsTruncateLength(ulong.MaxValue, 2048, 2147483648));
    }

    /// <summary>Exhaustive byte pairs and boundary addresses enforce original, zero-tail and exact after-image authorization.</summary>
    [Fact]
    public void ByteAuthorization_AllBytePairs_MatchesIndependentOracle()
    {
        ulong[] offsets = [0, 1, 4095, 4096, 4097, 2147483647];
        foreach (bool recovering in new[] { false, true })
        {
            foreach (ulong offset in offsets)
            {
                for (int actual = 0; actual <= byte.MaxValue; actual++)
                {
                    for (int original = 0; original <= byte.MaxValue; original++)
                    {
                        bool expected = actual == original;
                        if (recovering && offset >= 4096 && actual == 0)
                        {
                            expected = true;
                        }

                        bool initial = RecoveryKernel.OriginalOrZeroByte(recovering, offset, 4096, (byte)actual, (byte)original);
                        Assert.Equal(expected, initial);
                        Assert.Equal(expected || actual == 127, RecoveryKernel.IncludeAfterImageByte(initial, (byte)actual, 127));
                    }
                }
            }
        }
    }

    /// <summary>Real writer call sites retain signed validation before entering unsigned generated code.</summary>
    /// <param name="offset">An invalid signed page offset or retained length.</param>
    [Theory]
    [InlineData(-1L)]
    [InlineData(long.MinValue)]
    [InlineData(1L)]
    [InlineData(long.MaxValue)]
    public void JournalCallSites_InvalidSignedRanges_RefuseWithoutRecording(long offset)
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-kernel-" + Guid.NewGuid().ToString("N") + ".mdb");
        File.WriteAllBytes(path, new byte[4096]);
        try
        {
            using var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            using var journal = PersistentRollbackJournal.Create(database, 2048);
            long preparedLength = new FileInfo(path + ".jdw-journal").Length;
            Assert.Throws<JetOperationException>(() => journal.RecordBeforeWrite(offset, new byte[2048]));
            Assert.Throws<JetOperationException>(() => journal.RecordBeforeTruncate(offset));
            Assert.Equal(preparedLength, new FileInfo(path + ".jdw-journal").Length);
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
        }
    }

    private static byte ExpectedRecordKind(long tag, uint pageSize, ulong originalLength, bool recovering, bool removedTail, bool finalRecord)
    {
        if (pageSize is not (2048 or 4096) || originalLength > 2147483648)
        {
            return 0;
        }

        if (tag == -1)
        {
            return finalRecord ? (byte)1 : (byte)0;
        }

        if (tag == -3)
        {
            return removedTail ? (byte)2 : (byte)0;
        }

        if (recovering)
        {
            return 0;
        }

        if (tag == -2)
        {
            return 3;
        }

        if (tag <= long.MinValue + 2147483648)
        {
            long offset = tag - long.MinValue;
            return (ulong)offset < originalLength && offset % pageSize == 0 ? (byte)4 : (byte)0;
        }

        return tag >= 0 && tag < 2147483648 && tag % pageSize == 0 && 2147483648 - tag >= pageSize ? (byte)5 : (byte)0;
    }

    private sealed class ReplayWriteStream(string path) : FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None)
    {
        internal bool ObserveReplay { get; set; }

        internal int ReplayWriteCalls { get; private set; }

        internal long ReplayOffset { get; private set; }

        internal int ReplayCount { get; private set; }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (this.ObserveReplay)
            {
                this.ReplayWriteCalls++;
                this.ReplayOffset = this.Position;
                this.ReplayCount = count;
            }

            base.Write(buffer, offset, count);
        }
    }

    private sealed class RecordFixture : IDisposable
    {
        private readonly int pageSize;
        private readonly string path = Path.Combine(Path.GetTempPath(), "jdw-record-kernel-" + Guid.NewGuid().ToString("N") + ".mdb");

        internal RecordFixture(int pageSize = 2048)
        {
            this.pageSize = pageSize;
            this.Original = new byte[pageSize * 2];
            RandomNumberGenerator.Fill(this.Original);
            File.WriteAllBytes(this.path, this.Original);
            this.Database = new FileStream(this.path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        }

        internal FileStream Database { get; }

        internal byte[] Original { get; }

        internal string JournalPath => PersistentRollbackJournal.JournalPath(this.path);

        internal byte[] ReadDatabase()
        {
            byte[] bytes = new byte[checked((int)this.Database.Length)];
            this.Database.Position = 0;
            this.Database.ReadExactly(bytes);
            return bytes;
        }

        internal void Append(long tag, bool trailingByte)
        {
            byte[] record = new byte[this.pageSize + 40];
            BinaryPrimitives.WriteInt64LittleEndian(record, tag);
            if (tag == -1)
            {
                SHA256.HashData(this.ReadDatabase()).CopyTo(record, 8);
            }

            SHA256.HashData(record.AsSpan(0, record.Length - 32)).CopyTo(record.AsSpan(record.Length - 32));
            using var journal = new FileStream(this.JournalPath, FileMode.Append, FileAccess.Write, FileShare.None);
            journal.Write(record);
            if (trailingByte)
            {
                journal.WriteByte(0);
            }

            journal.Flush(flushToDisk: true);
        }

        internal void AppendIncomplete(int length)
        {
            byte[] fragment = new byte[length];
            Array.Fill(fragment, (byte)165);
            using var journal = new FileStream(this.JournalPath, FileMode.Append, FileAccess.Write, FileShare.None);
            journal.Write(fragment);
            journal.Flush(flushToDisk: true);
        }

        public void Dispose()
        {
            this.Database.Dispose();
            File.Delete(this.path);
            File.Delete(this.JournalPath);
            File.Delete(this.JournalPath + ".preparing");
        }
    }
}
