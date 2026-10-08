namespace JetDatabaseWriter.Pages.Paging;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Security.Cryptography;
using System.Text;

#pragma warning disable CA1849 // Journal ordering requires synchronous device flushes before physical database writes.

/// <summary>Persists raw rollback bytes and validated intended writes with bounded working buffers.</summary>
/// <remarks>The initial snapshot costs one database copy. File flushes do not establish directory-entry power-loss durability.</remarks>
internal sealed class PersistentRollbackJournal : IDisposable
{
    private const int HeaderLength = 128;
    private const long MaximumDatabaseLength = 2L * 1024 * 1024 * 1024;
    private const long MaximumJournalLength = 16L * 1024 * 1024 * 1024;
    private static readonly byte[] Magic = Encoding.ASCII.GetBytes("JDWUNDO1");
    private readonly FileStream database;
    private readonly FileStream journal;
    private readonly int pageSize;
    private bool completed;

    /// <summary>Gets a value indicating whether the commit decision is durably recorded.</summary>
    internal bool IsCommitted { get; private set; }

    private PersistentRollbackJournal(FileStream database, FileStream journal, int pageSize)
    {
        this.database = database;
        this.journal = journal;
        this.pageSize = pageSize;
    }

    /// <summary>Gets the sidecar name associated with a database.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The sidecar path.</returns>
    internal static string JournalPath(string path) => path + ".jdw-journal";

    /// <summary>Creates and durably prepares the original raw database snapshot.</summary>
    /// <param name="database">The exclusively coordinated physical database.</param>
    /// <param name="pageSize">The raw page size.</param>
    /// <returns>The live journal.</returns>
    /// <exception cref="IOException">An existing recovery journal prevents preparation.</exception>
    internal static PersistentRollbackJournal Create(FileStream database, int pageSize)
    {
        ValidatePageSize(pageSize);
        long length = database.Length;
        ValidateLength(length);
        string finalPath = JournalPath(database.Name);
        string stagingPath = finalPath + ".preparing";
        if (File.Exists(finalPath))
        {
            throw new IOException("An existing recovery journal must be recovered before starting another transaction.");
        }

        // The caller holds exclusive database ownership. Staging bytes never authorize database writes.
        File.Delete(stagingPath);
        var journal = new FileStream(stagingPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
        bool promoted = false;
        try
        {
            byte[] header = new byte[HeaderLength];
            Magic.CopyTo(header, 0);
            BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(8), pageSize);
            BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(16), length);
            Identity(database.Name).CopyTo(header, 24);
            journal.Write(header, 0, header.Length);
            using (var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256))
            {
                byte[] buffer = new byte[8192];
                for (long offset = 0; offset < length;)
                {
                    int count = checked((int)Math.Min(buffer.Length, length - offset));
                    Read(database, offset, buffer, count);
                    journal.Write(buffer, 0, count);
                    hash.AppendData(buffer, 0, count);
                    offset += count;
                }

                hash.GetHashAndReset().CopyTo(header, 56);
            }

            Hash(header.AsSpan(0, 96).ToArray()).CopyTo(header, 96);
            journal.Position = 0;
            journal.Write(header, 0, header.Length);
            journal.Flush(true);
            journal.Dispose();
            File.Move(stagingPath, finalPath);
            promoted = true;
            journal = new FileStream(finalPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            return new PersistentRollbackJournal(database, journal, pageSize);
        }
        catch
        {
            try
            {
                journal.Dispose();
                if (!promoted)
                {
                    File.Delete(stagingPath);
                }
            }
            catch (Exception cleanupFailure) when (cleanupFailure is IOException or UnauthorizedAccessException)
            {
                // Preserve the original preparation failure. A promoted snapshot remains recoverable.
            }

            throw;
        }
    }

    /// <summary>Refuses independent readers while a sidecar exists.</summary>
    /// <param name="database">The opened database.</param>
    /// <exception cref="IOException">A recovery journal exists.</exception>
    internal static void RejectHotJournal(FileStream database)
    {
        if (File.Exists(JournalPath(database.Name)))
        {
            throw new IOException("The database has a transaction recovery journal; open it with AccessWriter to recover first.");
        }
    }

    /// <summary>Validates all original and current content before idempotently restoring an undecided transaction.</summary>
    /// <param name="database">The exclusively coordinated writable database.</param>
    /// <exception cref="IOException">The journal or database does not validate.</exception>
    internal static void Recover(FileStream database)
    {
        string path = JournalPath(database.Name);
        if (!File.Exists(path))
        {
            return;
        }

        using (var journal = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.None))
        {
            if (journal.Length is < HeaderLength or > MaximumJournalLength)
            {
                throw new IOException("The recovery journal length is invalid.");
            }

            byte[] header = new byte[HeaderLength];
            Read(journal, 0, header, header.Length);
            int pageSize = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(8));
            long originalLength = BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(16));
            ValidatePageSize(pageSize);
            ValidateLength(originalLength);
            ValidateLength(database.Length);
            if (!header.AsSpan(0, 8).SequenceEqual(Magic) || !header.AsSpan(96, 32).SequenceEqual(Hash(header.AsSpan(0, 96).ToArray())) || !header.AsSpan(24, 32).SequenceEqual(Identity(database.Name)) || journal.Length < HeaderLength + originalLength)
            {
                throw new IOException("The recovery journal identity or header is invalid.");
            }

            using (var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256))
            {
                byte[] buffer = new byte[8192];
                for (long offset = 0; offset < originalLength;)
                {
                    int count = checked((int)Math.Min(buffer.Length, originalLength - offset));
                    Read(journal, HeaderLength + offset, buffer, count);
                    hash.AppendData(buffer, 0, count);
                    offset += count;
                }

                if (!header.AsSpan(56, 32).SequenceEqual(hash.GetHashAndReset()))
                {
                    throw new IOException("The recovery snapshot checksum is invalid.");
                }
            }

            long start = HeaderLength + originalLength;
            int recordSize = checked(8 + Math.Max(pageSize, 32) + 32);
            long completeEnd = start + ((journal.Length - start) / recordSize * recordSize);
            byte[] record = new byte[recordSize];
            long maximumLength = originalLength;
            bool committed = false;
            for (long position = start; position < completeEnd; position += recordSize)
            {
                long offset = ReadRecord(journal, position, record);
                if (offset == -1)
                {
                    if (position + recordSize != journal.Length || !record.AsSpan(8, 32).SequenceEqual(DatabaseHash(database)))
                    {
                        throw new IOException("The recovery commit decision conflicts with database content.");
                    }

                    committed = true;
                    break;
                }

                if (offset < 0 || offset % pageSize != 0 || offset > MaximumDatabaseLength - pageSize)
                {
                    throw new IOException("The recovery page offset is invalid.");
                }

                maximumLength = Math.Max(maximumLength, offset + pageSize);
            }

            if (!committed)
            {
                if (database.Length < originalLength || database.Length > maximumLength)
                {
                    throw new IOException("The database length conflicts with its recovery journal.");
                }

                byte[] original = new byte[pageSize];
                byte[] current = new byte[pageSize];
                bool[] valid = new bool[pageSize];
                for (long offset = 0; offset < database.Length; offset += pageSize)
                {
                    Array.Clear(original, 0, original.Length);
                    int oldCount = checked((int)Math.Min(pageSize, Math.Max(0, originalLength - offset)));
                    if (oldCount != 0)
                    {
                        Read(journal, HeaderLength + offset, original, oldCount);
                    }

                    int count = checked((int)Math.Min(pageSize, database.Length - offset));
                    Read(database, offset, current, count);
                    for (int index = 0; index < count; index++)
                    {
                        valid[index] = current[index] == original[index];
                    }

                    bool matchesOriginal = true;
                    for (int index = 0; index < count; index++)
                    {
                        matchesOriginal &= valid[index];
                    }

                    if (matchesOriginal)
                    {
                        continue;
                    }

                    for (long position = start; position < completeEnd; position += recordSize)
                    {
                        if (ReadRecord(journal, position, record) == offset)
                        {
                            for (int index = 0; index < count; index++)
                            {
                                valid[index] |= current[index] == record[8 + index];
                            }
                        }
                    }

                    for (int index = 0; index < count; index++)
                    {
                        if (!valid[index])
                        {
                            throw new IOException("Database content conflicts with the recovery journal; no recovery bytes were written.");
                        }
                    }
                }

                byte[] buffer = new byte[8192];
                for (long offset = 0; offset < originalLength;)
                {
                    int count = checked((int)Math.Min(buffer.Length, originalLength - offset));
                    Read(journal, HeaderLength + offset, buffer, count);
                    database.Position = offset;
                    database.Write(buffer, 0, count);
                    offset += count;
                }

                database.SetLength(originalLength);
                database.Flush(true);
            }
        }

        File.Delete(path);
    }

    private static long ReadRecord(FileStream journal, long position, byte[] record)
    {
        Read(journal, position, record, record.Length);
        if (!record.AsSpan(record.Length - 32).SequenceEqual(Hash(record.AsSpan(0, record.Length - 32).ToArray())))
        {
            throw new IOException("The recovery page record checksum is invalid.");
        }

        return BinaryPrimitives.ReadInt64LittleEndian(record);
    }

    private static byte[] DatabaseHash(FileStream database)
    {
        ValidateLength(database.Length);
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        byte[] buffer = new byte[8192];
        for (long offset = 0; offset < database.Length;)
        {
            int count = checked((int)Math.Min(buffer.Length, database.Length - offset));
            Read(database, offset, buffer, count);
            hash.AppendData(buffer, 0, count);
            offset += count;
        }

        return hash.GetHashAndReset();
    }

    private static byte[] Identity(string path)
    {
        string fullPath = Path.GetFullPath(path);
        if (Path.DirectorySeparatorChar == (char)92)
        {
            fullPath = fullPath.ToUpperInvariant();
        }

        return Hash(Encoding.UTF8.GetBytes(fullPath));
    }

    private static byte[] Hash(byte[] bytes)
    {
#if NET6_0_OR_GREATER
        return SHA256.HashData(bytes);
#else
        using var hash = SHA256.Create();
        return hash.ComputeHash(bytes);
#endif
    }

    private static void Read(FileStream stream, long offset, byte[] bytes, int count)
    {
        stream.Position = offset;
        int read = 0;
        while (read < count)
        {
            int next = stream.Read(bytes, read, count - read);
            if (next == 0)
            {
                throw new IOException("Unexpected end of recovery data.");
            }

            read += next;
        }
    }

    private static void ValidatePageSize(int pageSize)
    {
        if (pageSize is not (2048 or 4096))
        {
            throw new IOException("The recovery page size is invalid.");
        }
    }

    private static void ValidateLength(long length)
    {
        if (length is < 0 or > MaximumDatabaseLength)
        {
            throw new IOException("The recovery database length exceeds the Access size bound.");
        }
    }

    /// <summary>Durably records a possible raw page image before its database write.</summary>
    /// <param name="offset">The page-aligned physical offset.</param>
    /// <param name="after">The encoded raw bytes, including ciphertext when encrypted.</param>
    /// <exception cref="IOException">The physical write is outside the supported bounds.</exception>
    internal void RecordBeforeWrite(long offset, ReadOnlySpan<byte> after)
    {
        if (this.completed || offset < 0 || offset % this.pageSize != 0 || offset > MaximumDatabaseLength - this.pageSize || after.Length != this.pageSize)
        {
            throw new IOException("The rollback journal cannot record this physical page write.");
        }

        this.AppendRecord(offset, after);
    }

    /// <summary>Durably records a commit decision after the database has been durably flushed.</summary>
    internal void MarkCommitted()
    {
        this.database.Flush(true);
        this.AppendRecord(-1, DatabaseHash(this.database));
        this.IsCommitted = true;
        this.completed = true;
        this.CleanupCompleted();
    }

    /// <summary>Removes a journal after successful durable rollback.</summary>
    internal void CompleteRollback()
    {
        this.database.Flush(true);
        this.completed = true;
        this.CleanupCompleted();
    }

    /// <summary>Closes the sidecar while retaining incomplete recovery evidence.</summary>
    public void Dispose()
    {
        if (this.completed)
        {
            this.CleanupCompleted();
        }
        else
        {
            this.journal.Dispose();
        }
    }

    private void CleanupCompleted()
    {
        try
        {
            this.journal.Dispose();
            File.Delete(JournalPath(this.database.Name));
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            // The durable decision is authoritative. Retain a stale sidecar for the next recovery.
        }
    }

    private void AppendRecord(long offset, ReadOnlySpan<byte> bytes)
    {
        int payloadLength = Math.Max(this.pageSize, 32);
        byte[] record = new byte[checked(8 + payloadLength + 32)];
        if (this.journal.Length > MaximumJournalLength - record.Length)
        {
            throw new IOException("The bounded recovery journal size limit has been reached.");
        }

        BinaryPrimitives.WriteInt64LittleEndian(record, offset);
        bytes.CopyTo(record.AsSpan(8, payloadLength));
        Hash(record.AsSpan(0, record.Length - 32).ToArray()).CopyTo(record, record.Length - 32);
        this.journal.Position = this.journal.Length;
        this.journal.Write(record, 0, record.Length);
        this.journal.Flush(true);
    }
}
