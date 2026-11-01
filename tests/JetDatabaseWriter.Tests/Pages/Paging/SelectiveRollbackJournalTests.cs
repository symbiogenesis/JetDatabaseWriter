namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Security.Cryptography;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Paging;
using Xunit;

/// <summary>Checks selective original-image coverage against independent manifest and record damage.</summary>
public sealed class SelectiveRollbackJournalTests
{
    private const int HeaderLength = 128;
    private const int HashLength = 32;

    /// <summary>The first capture refuses a page changed after the manifest was prepared.</summary>
    /// <param name="pageSize">The physical page size.</param>
    [Theory]
    [InlineData(2048)]
    [InlineData(4096)]
    public void RecordBeforeWrite_ChangedOriginal_RefusesBeforeRecordingIntent(int pageSize)
    {
        using var fixture = new JournalFixture(pageSize, pageSize * 3);
        using var journal = PersistentRollbackJournal.Create(fixture.Database, pageSize);
        fixture.Database.Position = pageSize + 17;
        fixture.Database.WriteByte(checked((byte)(fixture.Original[pageSize + 17] ^ 1)));
        fixture.Database.Flush(flushToDisk: true);
        byte[] current = ReadDatabase(fixture.Database);

        Assert.Throws<JetCorruptDataException>(() => journal.RecordBeforeWrite(pageSize, new byte[pageSize]));
        Assert.Equal(current, ReadDatabase(fixture.Database));
        Assert.Equal(HeaderLength + (3L * HashLength), new FileInfo(fixture.JournalPath).Length);
    }

    /// <summary>A self-consistent record checksum cannot replace the original page's manifest hash.</summary>
    /// <param name="pageSize">The physical page size.</param>
    [Theory]
    [InlineData(2048)]
    [InlineData(4096)]
    public void Recover_ChecksumValidChangedOriginalImage_RefusesWithoutWrites(int pageSize)
    {
        using var fixture = new JournalFixture(pageSize, (pageSize * 4) + 17);
        fixture.WritePage(pageSize, 83);
        byte[] evidence = File.ReadAllBytes(fixture.JournalPath);
        int position = BeforeRecord(evidence, fixture.Original.Length, pageSize, pageSize);
        evidence[position + 25] ^= 1;
        SHA256.HashData(evidence.AsSpan(position, RecordSize(pageSize) - HashLength)).CopyTo(evidence.AsSpan(position + RecordSize(pageSize) - HashLength));
        File.WriteAllBytes(fixture.JournalPath, evidence);

        AssertRefused(fixture, evidence);
    }

    /// <summary>Every original-page write requires an earlier complete original image.</summary>
    /// <param name="pageSize">The physical page size.</param>
    /// <param name="reorder">Whether the original image follows its intent instead of being absent.</param>
    [Theory]
    [InlineData(2048, false)]
    [InlineData(2048, true)]
    [InlineData(4096, false)]
    [InlineData(4096, true)]
    public void Recover_MissingOrLateOriginalImage_RefusesWithoutWrites(int pageSize, bool reorder)
    {
        using var fixture = new JournalFixture(pageSize, pageSize * 4);
        fixture.WritePage(pageSize, 83);
        byte[] evidence = File.ReadAllBytes(fixture.JournalPath);
        int position = BeforeRecord(evidence, fixture.Original.Length, pageSize, pageSize);
        int recordSize = RecordSize(pageSize);
        Assert.Equal(pageSize, BinaryPrimitives.ReadInt64LittleEndian(evidence.AsSpan(position + recordSize)));
        if (reorder)
        {
            byte[] before = evidence.AsSpan(position, recordSize).ToArray();
            evidence.AsSpan(position + recordSize, recordSize).CopyTo(evidence.AsSpan(position));
            before.CopyTo(evidence, position + recordSize);
        }
        else
        {
            byte[] withoutOriginal = new byte[evidence.Length - recordSize];
            evidence.AsSpan(0, position).CopyTo(withoutOriginal);
            evidence.AsSpan(position + recordSize).CopyTo(withoutOriginal.AsSpan(position));
            evidence = withoutOriginal;
        }

        File.WriteAllBytes(fixture.JournalPath, evidence);
        AssertRefused(fixture, evidence);
    }

    /// <summary>A repeated checksum-valid original capture refuses before recovery effects on every retry.</summary>
    /// <param name="pageSize">The physical page size.</param>
    [Theory]
    [InlineData(2048)]
    [InlineData(4096)]
    public void Recover_DuplicateOriginalImage_RefusesRepeatedlyWithoutWrites(int pageSize)
    {
        using var fixture = new JournalFixture(pageSize, (pageSize * 3) + 17);
        fixture.WritePage(pageSize, 83);
        byte[] evidence = File.ReadAllBytes(fixture.JournalPath);
        int position = BeforeRecord(evidence, fixture.Original.Length, pageSize, pageSize);
        int recordSize = RecordSize(pageSize);
        byte[] duplicated = new byte[evidence.Length + recordSize];
        evidence.AsSpan(0, position + recordSize).CopyTo(duplicated);
        evidence.AsSpan(position, recordSize).CopyTo(duplicated.AsSpan(position + recordSize));
        evidence.AsSpan(position + recordSize).CopyTo(duplicated.AsSpan(position + (recordSize * 2)));
        File.WriteAllBytes(fixture.JournalPath, duplicated);

        AssertRefused(fixture, duplicated);
        AssertRefused(fixture, duplicated);
    }

    /// <summary>An incomplete original record authorizes no database change.</summary>
    /// <param name="pageSize">The physical page size.</param>
    /// <param name="changed">Whether unauthorized database bytes also exist.</param>
    [Theory]
    [InlineData(2048, false)]
    [InlineData(2048, true)]
    [InlineData(4096, false)]
    [InlineData(4096, true)]
    public void Recover_IncompleteOriginalImage_RequiresUnchangedDatabase(int pageSize, bool changed)
    {
        using var fixture = new JournalFixture(pageSize, pageSize * 4);
        fixture.WritePage(pageSize, 83, changed);
        byte[] evidence = File.ReadAllBytes(fixture.JournalPath);
        int position = BeforeRecord(evidence, fixture.Original.Length, pageSize, pageSize);
        evidence = evidence.AsSpan(0, position + RecordSize(pageSize) - 1).ToArray();
        File.WriteAllBytes(fixture.JournalPath, evidence);
        if (changed)
        {
            AssertRefused(fixture, evidence);
        }
        else
        {
            PersistentRollbackJournal.Recover(fixture.Database);
            Assert.Equal(fixture.Original, ReadDatabase(fixture.Database));
            Assert.False(File.Exists(fixture.JournalPath));
        }
    }

    /// <summary>Manifest-only untouched pages retain full-content conflict detection.</summary>
    /// <param name="pageSize">The physical page size.</param>
    [Theory]
    [InlineData(2048)]
    [InlineData(4096)]
    public void Recover_UntouchedPageConflict_RefusesWithoutWrites(int pageSize)
    {
        using var fixture = new JournalFixture(pageSize, pageSize * 4);
        fixture.WritePage(pageSize, 83);
        fixture.Database.Position = (pageSize * 2L) + 17;
        fixture.Database.WriteByte(checked((byte)(fixture.Original[(pageSize * 2) + 17] ^ 1)));
        fixture.Database.Flush(flushToDisk: true);

        AssertRefused(fixture, File.ReadAllBytes(fixture.JournalPath));
    }

    /// <summary>A truncate intent requires every removed original page even when that page was never overwritten.</summary>
    /// <param name="pageSize">The physical page size.</param>
    [Theory]
    [InlineData(2048)]
    [InlineData(4096)]
    public void Recover_TruncateMissingOriginalTailImage_RefusesWithoutWrites(int pageSize)
    {
        using var fixture = new JournalFixture(pageSize, (pageSize * 4) + 17);
        using (var journal = PersistentRollbackJournal.Create(fixture.Database, pageSize))
        {
            journal.RecordBeforeTruncate(pageSize * 2L);
            fixture.Database.SetLength(pageSize * 2L);
            fixture.Database.Flush(flushToDisk: true);
        }

        byte[] evidence = File.ReadAllBytes(fixture.JournalPath);
        int position = BeforeRecord(evidence, fixture.Original.Length, pageSize, pageSize * 3L);
        int recordSize = RecordSize(pageSize);
        byte[] withoutOriginal = new byte[evidence.Length - recordSize];
        evidence.AsSpan(0, position).CopyTo(withoutOriginal);
        evidence.AsSpan(position + recordSize).CopyTo(withoutOriginal.AsSpan(position));
        File.WriteAllBytes(fixture.JournalPath, withoutOriginal);

        AssertRefused(fixture, withoutOriginal);
    }

    /// <summary>Checksum-valid original-image tags still require aligned physical offsets.</summary>
    /// <param name="pageSize">The physical page size.</param>
    [Theory]
    [InlineData(2048)]
    [InlineData(4096)]
    public void Recover_UnalignedOriginalImageTag_RefusesWithoutWrites(int pageSize)
    {
        using var fixture = new JournalFixture(pageSize, pageSize * 4);
        fixture.WritePage(pageSize, 83);
        byte[] evidence = File.ReadAllBytes(fixture.JournalPath);
        int position = BeforeRecord(evidence, fixture.Original.Length, pageSize, pageSize);
        BinaryPrimitives.WriteInt64LittleEndian(evidence.AsSpan(position), long.MinValue + pageSize + 1);
        SHA256.HashData(evidence.AsSpan(position, RecordSize(pageSize) - HashLength)).CopyTo(evidence.AsSpan(position + RecordSize(pageSize) - HashLength));
        File.WriteAllBytes(fixture.JournalPath, evidence);

        AssertRefused(fixture, evidence);
    }

    /// <summary>Truncation retains the first raw originals when subsequent writes reextend removed pages.</summary>
    /// <param name="pageSize">The physical page size.</param>
    [Theory]
    [InlineData(2048)]
    [InlineData(4096)]
    public void Recover_WriteTruncateReextend_RestoresFirstOriginalImages(int pageSize)
    {
        using var fixture = new JournalFixture(pageSize, pageSize * 5);
        using (var journal = PersistentRollbackJournal.Create(fixture.Database, pageSize))
        {
            byte[] after = new byte[pageSize];
            Array.Fill(after, (byte)83);
            journal.RecordBeforeWrite(pageSize, after);
            fixture.Database.Position = pageSize;
            fixture.Database.Write(after);
            journal.RecordBeforeTruncate(pageSize * 2L);
            fixture.Database.SetLength(pageSize * 2L);
            for (long offset = pageSize * 2L; offset < fixture.Original.Length; offset += pageSize)
            {
                after[0]++;
                journal.RecordBeforeWrite(offset, after);
                fixture.Database.Position = offset;
                fixture.Database.Write(after);
            }

            fixture.Database.Flush(flushToDisk: true);
        }

        PersistentRollbackJournal.Recover(fixture.Database);
        PersistentRollbackJournal.Recover(fixture.Database);
        Assert.Equal(fixture.Original, ReadDatabase(fixture.Database));
        Assert.False(File.Exists(fixture.JournalPath));
    }

    /// <summary>A final partial original page is hashed and restored with its exact original length.</summary>
    /// <param name="pageSize">The physical page size.</param>
    [Theory]
    [InlineData(2048)]
    [InlineData(4096)]
    public void Recover_PartialOriginalTail_RestoresExactLengthAndBytes(int pageSize)
    {
        using var fixture = new JournalFixture(pageSize, (pageSize * 2) + 17);
        fixture.WritePage(pageSize * 2L, 83);
        Assert.Equal(pageSize * 3L, fixture.Database.Length);

        PersistentRollbackJournal.Recover(fixture.Database);
        Assert.Equal(fixture.Original, ReadDatabase(fixture.Database));
        Assert.False(File.Exists(fixture.JournalPath));
    }

    private static void AssertRefused(JournalFixture fixture, byte[] evidence)
    {
        byte[] current = ReadDatabase(fixture.Database);
        Assert.Throws<JetCorruptDataException>(() => PersistentRollbackJournal.Recover(fixture.Database));
        Assert.Equal(current, ReadDatabase(fixture.Database));
        Assert.Equal(evidence, File.ReadAllBytes(fixture.JournalPath));
    }

    private static int BeforeRecord(byte[] evidence, int originalLength, int pageSize, long offset)
    {
        int pageCount = (originalLength + pageSize - 1) / pageSize;
        int start = checked(HeaderLength + (pageCount * HashLength));
        int recordSize = RecordSize(pageSize);
        for (int position = start; position <= evidence.Length - recordSize; position += recordSize)
        {
            if (BinaryPrimitives.ReadInt64LittleEndian(evidence.AsSpan(position)) == long.MinValue + offset)
            {
                return position;
            }
        }

        Assert.Fail("The original page has no selective before-image record.");
        return 0;
    }

    private static int RecordSize(int pageSize) => 8 + pageSize + HashLength;

    private static byte[] ReadDatabase(FileStream database)
    {
        byte[] bytes = new byte[checked((int)database.Length)];
        database.Position = 0;
        database.ReadExactly(bytes);
        return bytes;
    }

    private sealed class JournalFixture : IDisposable
    {
        private readonly int pageSize;
        private readonly string directory = Path.Combine(Path.GetTempPath(), "jdw-selective-journal-" + Guid.NewGuid().ToString("N"));

        internal JournalFixture(int pageSize, int length)
        {
            this.pageSize = pageSize;
            this.Original = new byte[length];
            RandomNumberGenerator.Fill(this.Original);
            Directory.CreateDirectory(this.directory);
            string path = Path.Combine(this.directory, "database.mdb");
            File.WriteAllBytes(path, this.Original);
            this.Database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            this.JournalPath = PersistentRollbackJournal.JournalPath(path);
        }

        internal FileStream Database { get; }

        internal string JournalPath { get; }

        internal byte[] Original { get; }

        internal void WritePage(long offset, byte value, bool apply = true)
        {
            using var journal = PersistentRollbackJournal.Create(this.Database, this.pageSize);
            byte[] after = new byte[this.pageSize];
            Array.Fill(after, value);
            journal.RecordBeforeWrite(offset, after);
            if (apply)
            {
                this.Database.Position = offset;
                this.Database.Write(after);
                this.Database.Flush(flushToDisk: true);
            }
        }

        public void Dispose()
        {
            this.Database.Dispose();
            Directory.Delete(this.directory, recursive: true);
        }
    }
}
