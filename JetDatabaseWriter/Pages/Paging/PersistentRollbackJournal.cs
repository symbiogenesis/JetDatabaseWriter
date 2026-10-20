namespace JetDatabaseWriter.Pages.Paging;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;

/// <summary>Persists raw rollback bytes and validated intended writes with bounded working buffers.</summary>
/// <remarks>Preparation hashes every original page; raw before-images are retained only for overwritten or removed pages. File flushes do not establish directory-entry power-loss durability.</remarks>
internal sealed class PersistentRollbackJournal : IDisposable
{
    private const int HeaderLength = 128;
    private const long MaximumDatabaseLength = 2L * 1024 * 1024 * 1024;
    private const long MaximumJournalLength = 16L * 1024 * 1024 * 1024;
    private static readonly byte[] Magic = Encoding.ASCII.GetBytes("JDWUNDO2");
    private readonly FileStream database;
    private readonly FileStream journal;
    private readonly int pageSize;
    private readonly long originalLength;
    private readonly Dictionary<long, long> beforeImages = [];
    private bool completed;
    private bool cleanupAttempted;

    private PersistentRollbackJournal(FileStream database, FileStream journal, int pageSize, long originalLength)
    {
        this.database = database;
        this.journal = journal;
        this.pageSize = pageSize;
        this.originalLength = originalLength;
    }

    /// <summary>Gets the sidecar name associated with a database.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The sidecar path.</returns>
    internal static string JournalPath(string path) => path + ".jdw-journal";

    /// <summary>Creates and durably prepares the original raw page-hash manifest.</summary>
    /// <param name="database">The exclusively coordinated physical database.</param>
    /// <param name="pageSize">The raw page size.</param>
    /// <returns>The live journal.</returns>
    /// <exception cref="IOException">An existing recovery journal prevents preparation.</exception>
    /// <exception cref="JetOperationException">A recovery journal already exists.</exception>
    internal static PersistentRollbackJournal Create(FileStream database, int pageSize)
    {
        ValidatePageSize(pageSize);
        long length = database.Length;
        ValidateLength(length);
        string finalPath = JournalPath(database.Name);
        string stagingPath = finalPath + ".preparing";
        if (File.Exists(finalPath))
        {
            throw new JetOperationException(JetErrorCode.HotJournalConflict, "An existing recovery journal must be recovered before starting another transaction.");
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
                using var pageHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
                byte[] buffer = new byte[pageSize];
                byte[] digest = new byte[32];
                for (long offset = 0; offset < length;)
                {
                    int count = checked((int)Math.Min(buffer.Length, length - offset));
                    Read(database, offset, buffer, count);
                    pageHash.AppendData(buffer, 0, count);
                    if (!pageHash.TryGetHashAndReset(digest, out int written) || written != digest.Length)
                    {
                        throw new CryptographicException("The original page digest could not be prepared.");
                    }

                    journal.Write(digest, 0, digest.Length);
                    hash.AppendData(digest);
                    offset += count;
                }

                hash.GetHashAndReset().CopyTo(header, 56);
            }

            Hash(header, 96).CopyTo(header, 96);
            journal.Position = 0;
            journal.Write(header, 0, header.Length);
            journal.Flush(true);
            journal.Dispose();
            File.Move(stagingPath, finalPath);
            promoted = true;
            journal = new FileStream(finalPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            return new PersistentRollbackJournal(database, journal, pageSize, length);
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
                // Preserve the original preparation failure. A promoted manifest remains recoverable.
            }

            throw;
        }
    }

    /// <summary>Refuses independent readers while a sidecar exists.</summary>
    /// <param name="database">The opened database.</param>
    /// <exception cref="JetOperationException">A recovery journal exists.</exception>
    internal static void RejectHotJournal(FileStream database)
    {
        if (File.Exists(JournalPath(database.Name)))
        {
            throw new JetOperationException(JetErrorCode.HotJournalConflict, "The database has a transaction recovery journal; open it with AccessWriter to recover first.");
        }
    }

    /// <summary>Validates all original and current content before idempotently restoring an undecided transaction.</summary>
    /// <param name="database">The exclusively coordinated writable database.</param>
    /// <param name="beforeCleanup">An optional coordinated cleanup performed before recovery evidence is removed.</param>
    /// <exception cref="IOException">The journal or database does not validate.</exception>
    internal static void Recover(FileStream database, Action? beforeCleanup = null)
    {
        string path = JournalPath(database.Name);
        if (!File.Exists(path))
        {
            return;
        }

        using (var journal = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
        {
            JournalState state = ValidateJournal(database, journal);
            if (!state.Committed)
            {
                ValidateCurrentContent(database, journal, state);
                if (state.MinimumLength < state.OriginalLength && !state.Recovering)
                {
                    _ = AppendRecord(journal, state.PageSize, -3, []);
                }

                byte[] page = new byte[state.PageSize];
                foreach (KeyValuePair<long, long> entry in state.BeforeImages)
                {
                    int count = OriginalPageLength(state.PageSize, state.OriginalLength, entry.Key);
                    Read(journal, entry.Value + 8, page, count);
                    database.Position = entry.Key;
                    database.Write(page, 0, count);
                }

                database.SetLength(state.OriginalLength);
                database.Flush(true);
                _ = AppendRecord(journal, state.PageSize, -1, DatabaseHash(database));
            }
        }

        beforeCleanup?.Invoke();
        File.Delete(path);
    }

    private static (int PageSize, long OriginalLength) ValidateManifest(string databasePath, FileStream journal)
    {
        if (journal.Length is < HeaderLength or > MaximumJournalLength)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery journal length is invalid.");
        }

        byte[] header = new byte[HeaderLength];
        Read(journal, 0, header, header.Length);
        int pageSize = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(8));
        long originalLength = BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(16));
        ValidatePageSize(pageSize);
        ValidateLength(originalLength);
        long manifestLength = RecordStart(pageSize, originalLength) - HeaderLength;
        if (!header.AsSpan(0, 8).SequenceEqual(Magic) || !header.AsSpan(96, 32).SequenceEqual(Hash(header, 96)) || !header.AsSpan(24, 32).SequenceEqual(Identity(databasePath)) || journal.Length < HeaderLength + manifestLength)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery journal identity or header is invalid.");
        }

        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        byte[] buffer = new byte[8192];
        for (long offset = 0; offset < manifestLength;)
        {
            int count = checked((int)Math.Min(buffer.Length, manifestLength - offset));
            Read(journal, HeaderLength + offset, buffer, count);
            hash.AppendData(buffer, 0, count);
            offset += count;
        }

        if (!header.AsSpan(56, 32).SequenceEqual(hash.GetHashAndReset()))
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery page-hash manifest checksum is invalid.");
        }

        return (pageSize, originalLength);
    }

    private static JournalState ValidateJournal(FileStream database, FileStream journal)
    {
        (int pageSize, long originalLength) = ValidateManifest(database.Name, journal);
        ValidateLength(database.Length);
        long start = RecordStart(pageSize, originalLength);
        int recordSize = pageSize + 40;
        long completeEnd = start + ((journal.Length - start) / recordSize * recordSize);
        var beforeImages = new Dictionary<long, long>();
        byte[] record = new byte[recordSize];
        long maximumLength = originalLength;
        long minimumLength = originalLength;
        bool recovering = false;
        bool authorizedLength = database.Length >= originalLength;
        bool committed = false;
        for (long position = start; position < completeEnd; position += recordSize)
        {
            long offset = ReadRecord(journal, position, record);
            if (offset == -1)
            {
                if (position + recordSize != journal.Length || !record.AsSpan(8, 32).SequenceEqual(DatabaseHash(database)))
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery commit decision conflicts with database content.");
                }

                committed = true;
                break;
            }

            if (offset == -3)
            {
                if (minimumLength == originalLength)
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery replay marker has no removed original tail.");
                }

                recovering = true;
                continue;
            }

            if (recovering)
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "A recovering journal cannot authorize further database mutations.");
            }

            if (offset == -2)
            {
                long length = BinaryPrimitives.ReadInt64LittleEndian(record.AsSpan(8));
                if (length < 0 || length % pageSize != 0 || length > maximumLength)
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery truncate intent is invalid.");
                }

                for (long removed = length; removed < originalLength; removed += pageSize)
                {
                    if (!beforeImages.ContainsKey(removed))
                    {
                        throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "A recovery truncation is missing an original before-image.");
                    }
                }

                minimumLength = Math.Min(minimumLength, length);
                authorizedLength |= database.Length == length;
                continue;
            }

            if (offset <= long.MinValue + MaximumDatabaseLength)
            {
                long originalOffset = offset - long.MinValue;
                if (originalOffset >= originalLength || originalOffset % pageSize != 0 || !beforeImages.TryAdd(originalOffset, position))
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery before-image offset is invalid or repeated.");
                }

                VerifyOriginalPage(journal, pageSize, originalLength, originalOffset, record.AsSpan(8, pageSize));
                continue;
            }

            if (offset < 0 || offset % pageSize != 0 || offset > MaximumDatabaseLength - pageSize
                || (offset < originalLength && !beforeImages.ContainsKey(offset)))
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery page offset is invalid or has no original before-image.");
            }

            maximumLength = Math.Max(maximumLength, offset + pageSize);
        }

        if (!committed && (database.Length < minimumLength || database.Length > maximumLength || (!authorizedLength && !recovering)))
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The database length conflicts with its recovery journal.");
        }

        return new JournalState(pageSize, originalLength, start, completeEnd, minimumLength, recovering, committed, beforeImages);
    }

    private static void ValidateCurrentContent(FileStream database, FileStream journal, JournalState state)
    {
        byte[] original = new byte[state.PageSize];
        byte[] current = new byte[state.PageSize];
        byte[] record = new byte[state.PageSize + 40];
        bool[] valid = new bool[state.PageSize];
        for (long offset = 0; offset < database.Length; offset += state.PageSize)
        {
            int count = checked((int)Math.Min(state.PageSize, database.Length - offset));
            Read(database, offset, current, count);
            if (offset < state.OriginalLength && !state.BeforeImages.ContainsKey(offset))
            {
                int oldCount = OriginalPageLength(state.PageSize, state.OriginalLength, offset);
                if (count < oldCount)
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "An untouched original page has been truncated.");
                }

                VerifyOriginalPage(journal, state.PageSize, state.OriginalLength, offset, current);
                for (int index = oldCount; index < count; index++)
                {
                    if (current[index] != 0)
                    {
                        throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "An unassigned extension of an original page is not zero-filled.");
                    }
                }

                continue;
            }

            Array.Clear(original, 0, original.Length);
            if (state.BeforeImages.TryGetValue(offset, out long beforePosition))
            {
                Read(journal, beforePosition + 8, original, OriginalPageLength(state.PageSize, state.OriginalLength, offset));
            }

            bool matchesOriginal = true;
            for (int index = 0; index < count; index++)
            {
                valid[index] = current[index] == original[index]
                    || (state.Recovering && offset + index >= state.MinimumLength && current[index] == 0);
                matchesOriginal &= valid[index];
            }

            if (matchesOriginal)
            {
                continue;
            }

            for (long position = state.Start; position < state.CompleteEnd; position += record.Length)
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
                    throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "Database content conflicts with the recovery journal; no recovery bytes were written.");
                }
            }
        }
    }

    private static long RecordStart(int pageSize, long originalLength)
        => HeaderLength + ((originalLength + pageSize - 1) / pageSize * 32);

    private static int OriginalPageLength(int pageSize, long originalLength, long offset)
        => checked((int)Math.Min(pageSize, originalLength - offset));

    private static void VerifyOriginalPage(FileStream journal, int pageSize, long originalLength, long offset, ReadOnlySpan<byte> page)
    {
        byte[] expected = new byte[32];
        Read(journal, HeaderLength + (offset / pageSize * 32), expected, expected.Length);
        if (!expected.AsSpan().SequenceEqual(Hash(page[..OriginalPageLength(pageSize, originalLength, offset)])))
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "An original page does not match its prepared recovery manifest.");
        }
    }

    private static long AppendRecord(FileStream journal, int pageSize, long offset, ReadOnlySpan<byte> bytes, bool durable = true)
    {
        int payloadLength = Math.Max(pageSize, 32);
        byte[] record = new byte[checked(8 + payloadLength + 32)];
        if (journal.Length > MaximumJournalLength - record.Length)
        {
            throw new JetLimitationException(JetErrorCode.JournalBudgetExceeded, "The bounded recovery journal size limit has been reached.");
        }

        byte[] lengthBytes = new byte[8];
        Read(journal, 16, lengthBytes, lengthBytes.Length);
        long start = RecordStart(pageSize, BinaryPrimitives.ReadInt64LittleEndian(lengthBytes));
        long completeEnd = start + ((journal.Length - start) / record.Length * record.Length);
        journal.SetLength(completeEnd);

        BinaryPrimitives.WriteInt64LittleEndian(record, offset);
        bytes.CopyTo(record.AsSpan(8, payloadLength));
        Hash(record, record.Length - 32).CopyTo(record, record.Length - 32);
        journal.Position = journal.Length;
        long position = journal.Position;
        journal.Write(record, 0, record.Length);
        if (durable)
        {
            journal.Flush(true);
        }

        return position;
    }

    private static long ReadRecord(FileStream journal, long position, byte[] record)
    {
        Read(journal, position, record, record.Length);
        if (!record.AsSpan(record.Length - 32).SequenceEqual(Hash(record, record.Length - 32)))
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery page record checksum is invalid.");
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

        byte[] bytes = Encoding.UTF8.GetBytes(fullPath);
        return Hash(bytes, bytes.Length);
    }

    private static byte[] Hash(byte[] bytes, int length)
    {
#if NET6_0_OR_GREATER
        return SHA256.HashData(bytes.AsSpan(0, length));
#else
        using var hash = SHA256.Create();
        return hash.ComputeHash(bytes, 0, length);
#endif
    }

    private static byte[] Hash(ReadOnlySpan<byte> bytes)
    {
#if NET6_0_OR_GREATER
        return SHA256.HashData(bytes);
#else
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(bytes);
        return hash.GetHashAndReset();
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
                throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "Unexpected end of recovery data.");
            }

            read += next;
        }
    }

    private static void ValidatePageSize(int pageSize)
    {
        if (pageSize is not (2048 or 4096))
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery page size is invalid.");
        }
    }

    private static void ValidateLength(long length)
    {
        if (length is < 0 or > MaximumDatabaseLength)
        {
            throw new JetLimitationException(JetErrorCode.ValueTooLarge, "The recovery database length exceeds the Access size bound.");
        }
    }

    /// <summary>Durably records a possible raw page image before its database write.</summary>
    /// <param name="offset">The page-aligned physical offset.</param>
    /// <param name="after">The encoded raw bytes, including ciphertext when encrypted.</param>
    /// <exception cref="JetOperationException">The physical write is outside the supported bounds.</exception>
    internal void RecordBeforeWrite(long offset, ReadOnlySpan<byte> after)
    {
        if (this.completed || offset < 0 || offset % this.pageSize != 0 || offset > MaximumDatabaseLength - this.pageSize || after.Length != this.pageSize)
        {
            throw new JetOperationException(JetErrorCode.InvalidOperationState, "The rollback journal cannot record this physical page write.");
        }

        this.RequireRecordCapacity(offset < this.originalLength && !this.beforeImages.ContainsKey(offset) ? 2 : 1);
        this.CaptureOriginalPage(offset);
        _ = this.AppendRecord(offset, after);
    }

    /// <summary>Records that restoration may re-extend an authorized truncated tail.</summary>
    /// <exception cref="JetOperationException">The journal is already completed.</exception>
    internal void RecordRecoveryStart()
    {
        if (this.completed)
        {
            throw new JetOperationException(JetErrorCode.InvalidOperationState, "A completed rollback journal cannot begin recovery.");
        }

        _ = this.AppendRecord(-3, []);
    }

    /// <summary>Durably authorizes removal of a raw tail before changing file length.</summary>
    /// <param name="length">The retained page-aligned length.</param>
    /// <exception cref="JetOperationException">The intent is outside the supported bounds.</exception>
    internal void RecordBeforeTruncate(long length)
    {
        if (this.completed || length < 0 || length % this.pageSize != 0 || length > this.database.Length)
        {
            throw new JetOperationException(JetErrorCode.InvalidOperationState, "The rollback journal cannot record this truncation.");
        }

        int missing = 0;
        for (long offset = length; offset < this.originalLength; offset += this.pageSize)
        {
            if (!this.beforeImages.ContainsKey(offset))
            {
                missing++;
            }
        }

        this.RequireRecordCapacity(missing + 1);
        for (long offset = length; offset < this.originalLength; offset += this.pageSize)
        {
            this.CaptureOriginalPage(offset);
        }

        byte[] payload = new byte[8];
        BinaryPrimitives.WriteInt64LittleEndian(payload, length);
        _ = this.AppendRecord(-2, payload);
    }

    /// <summary>Restores overwritten pages and removed tail bytes from verified raw before-images.</summary>
    /// <param name="store">The physical store with intent recording detached.</param>
    /// <returns>The restoration and durable cleanup completion.</returns>
    /// <exception cref="JetCorruptDataException">The original manifest or before-images are invalid.</exception>
    /// <exception cref="JetOperationException">The journal is completed.</exception>
    internal async ValueTask RestoreAsync(IPageStore store)
    {
        if (this.completed)
        {
            throw new JetOperationException(JetErrorCode.InvalidOperationState, "A completed rollback journal cannot restore database bytes.");
        }

        JournalState state = ValidateJournal(this.database, this.journal);
        if (state.PageSize != this.pageSize || state.OriginalLength != this.originalLength || state.Committed)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery manifest differs from the prepared database image.");
        }

        if (state.MinimumLength < state.OriginalLength && !state.Recovering)
        {
            this.RecordRecoveryStart();
        }

        byte[] page = new byte[this.pageSize];
        foreach (KeyValuePair<long, long> entry in state.BeforeImages)
        {
            int count = OriginalPageLength(this.pageSize, this.originalLength, entry.Key);
            Read(this.journal, entry.Value + 8, page, count);
            await store.WriteAsync(entry.Key, page.AsMemory(0, count), CancellationToken.None).ConfigureAwait(false);
        }

        await store.SetLengthAsync(this.originalLength, CancellationToken.None).ConfigureAwait(false);
        await store.FlushAsync(true, CancellationToken.None).ConfigureAwait(false);
        this.Complete();
    }

    private void CaptureOriginalPage(long offset)
    {
        if (offset >= this.originalLength || this.beforeImages.ContainsKey(offset))
        {
            return;
        }

        byte[] page = new byte[this.pageSize];
        Read(this.database, offset, page, OriginalPageLength(this.pageSize, this.originalLength, offset));
        VerifyOriginalPage(this.journal, this.pageSize, this.originalLength, offset, page);
        long position = this.AppendRecord(long.MinValue + offset, page, durable: false);
        this.beforeImages.Add(offset, position);
    }

    /// <summary>Durably records the final database hash and removes the completed journal.</summary>
    internal void Complete()
    {
        this.database.Flush(true);
        _ = this.AppendRecord(-1, DatabaseHash(this.database));
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
        if (this.cleanupAttempted)
        {
            return;
        }

        // A later transaction may own this path once our handle closes. Recovery retries stale cleanup.
        this.cleanupAttempted = true;
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

    private long AppendRecord(long offset, ReadOnlySpan<byte> bytes, bool durable = true)
        => AppendRecord(this.journal, this.pageSize, offset, bytes, durable);

    private void RequireRecordCapacity(int records)
    {
        if (this.journal.Length > MaximumJournalLength - ((records + 2L) * (this.pageSize + 40)))
        {
            throw new JetLimitationException(JetErrorCode.JournalBudgetExceeded, "The bounded recovery journal must retain decision record capacity.");
        }
    }

    private sealed record JournalState(int PageSize, long OriginalLength, long Start, long CompleteEnd, long MinimumLength, bool Recovering, bool Committed, Dictionary<long, long> BeforeImages);
}
