namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Security.Cryptography;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Checks complete correspondence between admitted replay maps and serialized captures.</summary>
public sealed class JournalReplayInventoryTests
{
    /// <summary>Missing, extra or redirected map entries cannot authorize any replay effect.</summary>
    /// <param name="pageSize">The physical page size.</param>
    /// <param name="mutation">The invalid map transformation.</param>
    [Theory]
    [InlineData(2048, 0)]
    [InlineData(2048, 1)]
    [InlineData(2048, 2)]
    [InlineData(2048, 3)]
    [InlineData(4096, 0)]
    [InlineData(4096, 1)]
    [InlineData(4096, 2)]
    [InlineData(4096, 3)]
    public void RestoreBeforeImages_AlteredInventory_RefusesBeforeEffects(int pageSize, int mutation)
    {
        using var fixture = new ReplayFixture(pageSize);
        using var journal = new FileStream(fixture.JournalPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        object state = ValidateState(fixture.Database, journal);
        Dictionary<long, long> inventory = Assert.IsType<Dictionary<long, long>>(state.GetType().GetProperty("BeforeImages")!.GetValue(state));
        long first = inventory[0];
        switch (mutation)
        {
            case 0:
                Assert.True(inventory.Remove(0));
                break;
            case 1:
                inventory.Add(pageSize, first);
                break;
            case 2:
                inventory[0] = inventory[pageSize * 2L];
                break;
            case 3:
                inventory[0] = first + 1;
                break;
            default:
                Assert.Fail("Unknown inventory mutation.");
                break;
        }

        fixture.Database.RecordWrites = true;
        byte[] databaseBefore = fixture.ReadDatabase();
        byte[] evidenceBefore = ReadAll(journal);
        for (int attempt = 0; attempt < 2; attempt++)
        {
            TargetInvocationException failure = Assert.Throws<TargetInvocationException>(() => ReplayMethod().Invoke(null, [fixture.Database, journal, state]));
            JetCorruptDataException corruption = Assert.IsType<JetCorruptDataException>(failure.InnerException);
            Assert.Equal(JetErrorCode.CorruptRecoveryJournal, corruption.ErrorCode);
            Assert.Empty(fixture.Database.WriteOffsets);
            Assert.Equal(databaseBefore, fixture.ReadDatabase());
            Assert.Equal(evidenceBefore, ReadAll(journal));
        }
    }

    /// <summary>Dictionary insertion order does not change serialized replay order or the exact selected image.</summary>
    /// <param name="pageSize">The physical page size.</param>
    [Theory]
    [InlineData(2048)]
    [InlineData(4096)]
    public void RestoreBeforeImages_ReorderedInventory_RestoresSerializedCaptureOrder(int pageSize)
    {
        using var fixture = new ReplayFixture(pageSize);
        using var journal = new FileStream(fixture.JournalPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        object state = ValidateState(fixture.Database, journal);
        Dictionary<long, long> inventory = Assert.IsType<Dictionary<long, long>>(state.GetType().GetProperty("BeforeImages")!.GetValue(state));
        KeyValuePair<long, long>[] entries = [.. inventory];
        inventory.Clear();
        for (int index = entries.Length - 1; index >= 0; index--)
        {
            inventory.Add(entries[index].Key, entries[index].Value);
        }

        fixture.Database.RecordWrites = true;
        ReplayMethod().Invoke(null, [fixture.Database, journal, state]);
        Assert.Equal(new long[] { pageSize * 2L, 0, pageSize * 3L }, fixture.Database.WriteOffsets);
        Assert.Equal(fixture.Original, fixture.ReadDatabase());
    }

    /// <summary>The selector requires complete framing, exact source membership and a nonempty bounded inventory.</summary>
    [Fact]
    public void JournalReplayPageOffset_BoundedSources_MatchesIndependentOracle()
    {
        foreach (uint pageSize in new uint[] { 2048, 4096 })
        {
            foreach (ulong originalLength in new ulong[] { 1, pageSize, pageSize + 17UL, 2147483648 })
            {
                ulong pageCount = ((originalLength - 1) / pageSize) + 1;
                ulong start = 128 + (pageCount * 32);
                ulong recordSize = pageSize + 40UL;
                ulong journalLength = start + (2 * recordSize) + 3;
                foreach (ulong position in new ulong[] { 0, start - 1, start, start + 1, start + recordSize, start + (2 * recordSize), ulong.MaxValue })
                {
                    foreach (ulong tag in new ulong[] { 0, 9223372036854775808, 9223372036854775809, 9223372036854775808 + pageSize, 9223372039002259456, ulong.MaxValue })
                    {
                        foreach (ulong mappedPosition in new ulong[] { 0, position, start + 1 })
                        {
                            foreach (uint remaining in new uint[] { 0, 1, 2, 1048576, uint.MaxValue })
                            {
                                bool source = position >= start && position - start < 2 * recordSize && (position - start) % recordSize == 0;
                                bool original = tag >= 9223372036854775808 && tag - 9223372036854775808 < originalLength
                                    && (tag - 9223372036854775808) % pageSize == 0;
                                ulong expected = source && original && mappedPosition == position && remaining > 0 && remaining <= pageCount
                                    ? tag - 9223372036854775808
                                    : 2147483648;
                                Assert.Equal(expected, RecoveryKernel.JournalReplayPageOffset(pageSize, originalLength, journalLength, position, tag, mappedPosition, remaining));
                            }
                        }
                    }
                }
            }
        }

        Assert.Equal(2147483648UL, RecoveryKernel.JournalReplayPageOffset(0, 1, 2248, 160, 9223372036854775808, 160, 1));
        Assert.Equal(2147483648UL, RecoveryKernel.JournalReplayPageOffset(2048, ulong.MaxValue, 2248, 160, 9223372036854775808, 160, 1));
        Assert.Equal(2147483648UL, RecoveryKernel.JournalReplayPageOffset(2048, 1, ulong.MaxValue, 160, 9223372036854775808, 160, 1));
    }

    /// <summary>Unused checksum-covered padding after original EOF does not enter the restored image.</summary>
    /// <param name="pageSize">The physical page size.</param>
    [Theory]
    [InlineData(2048)]
    [InlineData(4096)]
    public void Recover_NonzeroUnusedRecordPadding_RestoresOnlyOriginalPrefix(int pageSize)
    {
        using var fixture = new ReplayFixture(pageSize);
        using (var journal = new FileStream(fixture.JournalPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
        {
            object state = ValidateState(fixture.Database, journal);
            Dictionary<long, long> inventory = Assert.IsType<Dictionary<long, long>>(state.GetType().GetProperty("BeforeImages")!.GetValue(state));
            long position = inventory[pageSize * 3L];
            byte[] record = new byte[pageSize + 40];
            journal.Position = position;
            journal.ReadExactly(record);
            record.AsSpan(8 + 17, pageSize - 17).Fill(213);
            SHA256.HashData(record.AsSpan(0, record.Length - 32)).CopyTo(record.AsSpan(record.Length - 32));
            journal.Position = position;
            journal.Write(record);
            journal.Flush(flushToDisk: true);
        }

        PersistentRollbackJournal.Recover(fixture.Database);
        Assert.Equal(fixture.Original, fixture.ReadDatabase());
        Assert.False(File.Exists(fixture.JournalPath));
    }

    private static object ValidateState(FileStream database, FileStream journal)
    {
        MethodInfo? validate = typeof(PersistentRollbackJournal).GetMethod("ValidateJournal", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(validate);
        object? state = validate.Invoke(null, [database, journal]);
        Assert.NotNull(state);
        return state;
    }

    private static MethodInfo ReplayMethod()
    {
        MethodInfo? replay = typeof(PersistentRollbackJournal).GetMethod("RestoreBeforeImages", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(replay);
        return replay;
    }

    private static byte[] ReadAll(FileStream stream)
    {
        byte[] bytes = new byte[checked((int)stream.Length)];
        stream.Position = 0;
        stream.ReadExactly(bytes);
        return bytes;
    }

    private sealed class ReplayDatabaseStream(string path) : FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None)
    {
        internal bool RecordWrites { get; set; }

        internal List<long> WriteOffsets { get; } = [];

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (this.RecordWrites)
            {
                this.WriteOffsets.Add(this.Position);
            }

            base.Write(buffer, offset, count);
        }
    }

    private sealed class ReplayFixture : IDisposable
    {
        private readonly string path = Path.Combine(Path.GetTempPath(), "jdw-replay-inventory-" + Guid.NewGuid().ToString("N") + ".mdb");

        internal ReplayFixture(int pageSize)
        {
            this.Original = new byte[(3 * pageSize) + 17];
            RandomNumberGenerator.Fill(this.Original);
            File.WriteAllBytes(this.path, this.Original);
            this.Database = new ReplayDatabaseStream(this.path);
            using var journal = PersistentRollbackJournal.Create(this.Database, pageSize);
            byte[] after = new byte[pageSize];
            Array.Fill(after, (byte)187);
            foreach (long offset in new long[] { pageSize * 2L, 0, pageSize * 3L })
            {
                journal.RecordBeforeWrite(offset, after);
                this.Database.Position = offset;
                this.Database.Write(after);
            }

            this.Database.Flush(flushToDisk: true);
        }

        internal ReplayDatabaseStream Database { get; }

        internal byte[] Original { get; }

        internal string JournalPath => PersistentRollbackJournal.JournalPath(this.path);

        internal byte[] ReadDatabase() => ReadAll(this.Database);

        public void Dispose()
        {
            this.Database.Dispose();
            File.Delete(this.path);
            File.Delete(this.JournalPath);
            File.Delete(this.JournalPath + ".preparing");
        }
    }
}
