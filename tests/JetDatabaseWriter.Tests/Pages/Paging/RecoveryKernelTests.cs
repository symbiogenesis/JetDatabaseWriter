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

    private sealed class RecordFixture : IDisposable
    {
        private readonly string path = Path.Combine(Path.GetTempPath(), "jdw-record-kernel-" + Guid.NewGuid().ToString("N") + ".mdb");

        internal RecordFixture()
        {
            this.Original = new byte[4096];
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
            byte[] record = new byte[2088];
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

        public void Dispose()
        {
            this.Database.Dispose();
            File.Delete(this.path);
            File.Delete(this.JournalPath);
        }
    }
}
