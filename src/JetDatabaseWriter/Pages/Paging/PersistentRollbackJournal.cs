namespace JetDatabaseWriter.Pages.Paging;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Transactions;
using JetDatabaseWriter.Verification;

/// <summary>Persists raw rollback bytes and validated intended writes with bounded working buffers.</summary>
/// <remarks>Preparation persists each original page digest, reusing tracked digests under one exclusive store owner. Raw before-images are selective. File flushes do not establish directory-entry power-loss durability.</remarks>
internal sealed class PersistentRollbackJournal : IDisposable
{
    private const int HeaderLength = 128;
    private const long MaximumJournalLength = 16L * 1024 * 1024 * 1024;
    private static readonly byte[] Magic = Encoding.ASCII.GetBytes("JDWUNDO3");
    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "The database stream is owned by StreamPageStore, not the single-transaction journal.")]
    private readonly Stream database;
    private readonly Stream journal;
    private readonly int pageSize;
    private readonly long originalLength;
    private readonly FilePageDigests? pageDigests;
    [SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "The recovery lease is owned by StreamPageStore for the writer's lifetime, not the single-transaction journal.")]
    private readonly IPersistentStreamRecoveryLease? lease;
    private readonly Dictionary<long, long> beforeImages = [];
    private bool completed;
    private bool cleanupAttempted;

    private enum JournalRecordKind : byte
    {
        Invalid = 0,
        Commit = 1,
        Replay = 2,
        Truncate = 3,
        BeforeImage = 4,
        AfterImage = 5,
    }

    private enum JournalReplayAction : byte
    {
        Invalid = 0,
        Marker = 1,
        Page = 2,
        Resize = 3,
        Flush = 4,
        Complete = 5,
    }

    private PersistentRollbackJournal(Stream database, Stream journal, int pageSize, long originalLength, FilePageDigests? pageDigests, IPersistentStreamRecoveryLease? lease = null)
    {
        this.database = database;
        this.journal = journal;
        this.pageSize = pageSize;
        this.originalLength = originalLength;
        this.pageDigests = pageDigests;
        this.lease = lease;
    }

    /// <summary>Gets the sidecar name associated with a database.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The sidecar path.</returns>
    internal static string JournalPath(string path) => path + ".jdw-journal";

    /// <summary>Creates and durably prepares the original raw page-hash manifest.</summary>
    /// <param name="database">The exclusively coordinated physical database.</param>
    /// <param name="pageSize">The raw page size.</param>
    /// <param name="pageDigests">Optional digest reuse owned by the physical store that tracks every successful write.</param>
    /// <returns>The live journal.</returns>
    /// <exception cref="IOException">An existing recovery journal prevents preparation.</exception>
    /// <exception cref="JetOperationException">A recovery journal already exists.</exception>
    internal static PersistentRollbackJournal Create(FileStream database, int pageSize, FilePageDigests? pageDigests = null)
    {
        ValidatePageSize(pageSize);
        long length = database.Length;
        ValidateLength(length);
        string finalPath = JournalPath(database.Name);
        string stagingPath = finalPath + ".preparing";
        if (File.Exists(finalPath) || File.Exists(stagingPath))
        {
            throw new JetOperationException(JetErrorCode.HotJournalConflict, "An existing recovery journal must be recovered before starting another transaction.");
        }

        // Namespace loss can leave preparation as the only recovery evidence. Never replace it blindly.
        var journal = new FileStream(stagingPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
        bool promoted = false;
        try
        {
            byte[] header = new byte[HeaderLength];
            Magic.CopyTo(header, 0);
            BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(8), pageSize);
            BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(16), length);
            Identity(database.Name).CopyTo(header, 24);
            RandomNumberGenerator.Fill(header.AsSpan(88, 8));
            journal.Write(header, 0, header.Length);
            FilePageDigests? tracked = pageDigests is { IsValid: true } && pageDigests.Length == length && pageDigests.PageSize == pageSize
                ? pageDigests
                : null;
            FilePageDigests manifest = tracked ?? FilePageDigests.Load(database, pageSize);
            journal.Write(manifest.Manifest);
            using (var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256))
            {
                hash.AppendData(manifest.Manifest);
                hash.GetHashAndReset().CopyTo(header, 56);
            }

            Hash(header, 96).CopyTo(header, 96);
            journal.Position = 0;
            journal.Write(header, 0, header.Length);
            journal.Flush(true);
            journal.Dispose();
            File.Move(stagingPath, finalPath);
            promoted = true;
            _ = DirectoryPersistence.FlushParent(finalPath);
            journal = new FileStream(finalPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            return new PersistentRollbackJournal(database, journal, pageSize, length, tracked);
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

    /// <summary>Creates and fences the original raw page-hash manifest through an explicit caller-stream recovery lease.</summary>
    /// <param name="database">The exclusively leased database stream.</param>
    /// <param name="lease">The active persistent recovery lease.</param>
    /// <param name="pageSize">The raw page size.</param>
    /// <param name="pageDigests">Optional digest reuse owned by the physical store that tracks every successful write.</param>
    /// <param name="onJournalRetained">Invoked if preparation fails after promotion or when discarding the preparing journal fails.</param>
    /// <returns>The live journal.</returns>
    /// <exception cref="IOException">The preparing or promoted journal stream could not be opened or fenced.</exception>
    /// <exception cref="JetOperationException">A recovery journal already exists.</exception>
    internal static PersistentRollbackJournal Create(Stream database, IPersistentStreamRecoveryLease lease, int pageSize, FilePageDigests? pageDigests = null, Action? onJournalRetained = null)
    {
        ValidatePageSize(pageSize);
        long length = database.Length;
        ValidateLength(length);
        ValidateLeaseIdentity(lease);
        long priorGeneration = lease.Generation;
        long nextGeneration = checked(priorGeneration + 1);
        Stream? staging = lease.CreatePreparingJournal();
        if (staging is not { CanRead: true, CanWrite: true, CanSeek: true, Length: 0 })
        {
            onJournalRetained?.Invoke();
            staging?.Dispose();
            throw new JetOperationException(JetErrorCode.HotJournalConflict, "An existing recovery journal must be recovered before starting another transaction.");
        }

        Stream? journal = null;
        bool promoted = false;
        try
        {
            byte[] header = new byte[HeaderLength];
            Magic.CopyTo(header, 0);
            BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(8), pageSize);
            BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(16), length);
            AdapterIdentity(lease.DatabaseIdentity).CopyTo(header, 24);
            BinaryPrimitives.WriteInt64LittleEndian(header.AsSpan(88, 8), nextGeneration);
            staging.Write(header, 0, header.Length);
            FilePageDigests? tracked = pageDigests is { IsValid: true } && pageDigests.Length == length && pageDigests.PageSize == pageSize
                ? pageDigests
                : null;
            FilePageDigests manifest = tracked ?? FilePageDigests.Load(database, pageSize);
            staging.Write(manifest.Manifest);
            using (var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256))
            {
                hash.AppendData(manifest.Manifest);
                hash.GetHashAndReset().CopyTo(header, 56);
            }

            Hash(header, 96).CopyTo(header, 96);
            staging.Position = 0;
            staging.Write(header, 0, header.Length);
            FlushJournal(staging, lease);
            staging.Dispose();
            staging = null;
            lease.BindGeneration(nextGeneration);
            journal = lease.PromotePreparingJournal() ?? throw new IOException("The persistent stream recovery adapter failed to open the promoted journal stream.");
            promoted = true;
            if (!journal.CanRead || !journal.CanWrite || !journal.CanSeek)
            {
                throw new IOException("The promoted recovery journal stream must be readable, writable, and seekable.");
            }

            return new PersistentRollbackJournal(database, journal, pageSize, length, tracked, lease);
        }
        catch
        {
            try
            {
                staging?.Dispose();
                journal?.Dispose();
                if (!promoted)
                {
                    lease.DiscardPreparingJournal();
                }
                else
                {
                    onJournalRetained?.Invoke();
                }
            }
            catch (Exception cleanupFailure) when (cleanupFailure is IOException or UnauthorizedAccessException)
            {
                onJournalRetained?.Invoke();

                // Preserve the original preparation failure. A promoted manifest remains recoverable.
            }

            throw;
        }
    }

    /// <summary>Refuses independent readers while recovery or writer-ownership evidence exists.</summary>
    /// <param name="database">The opened database.</param>
    /// <exception cref="JetOperationException">Recovery or writer-ownership evidence exists.</exception>
    internal static void RejectHotJournal(FileStream database)
        => RejectHotJournal(database.Name);

    /// <summary>Refuses independent reads or creation while deterministic recovery evidence is occupied.</summary>
    /// <param name="path">The database path.</param>
    /// <exception cref="JetOperationException">Recovery or writer-ownership evidence exists.</exception>
    internal static void RejectHotJournal(string path)
    {
        EncryptionReplacementIntent.RejectPending(path);
        string journal = JournalPath(path);
        string receipt = RecoverableWriterLock.ReceiptPath(path);
        if (File.Exists(journal) || Directory.Exists(journal) || File.Exists(journal + ".preparing") || Directory.Exists(journal + ".preparing") || File.Exists(receipt) || Directory.Exists(receipt))
        {
            throw new JetOperationException(JetErrorCode.HotJournalConflict, "The database has transaction recovery or writer-ownership evidence; open it with AccessWriter to recover first.");
        }
    }

    /// <summary>Validates all original and current content before idempotently restoring an undecided transaction.</summary>
    /// <param name="database">The exclusively coordinated writable database.</param>
    /// <param name="beforeCleanup">An optional coordinated cleanup performed before recovery evidence is removed.</param>
    /// <exception cref="IOException">The journal or database does not validate.</exception>
    /// <exception cref="JetCorruptDataException">A directory occupies a deterministic journal name.</exception>
    internal static void Recover(FileStream database, Action? beforeCleanup = null)
    {
        string path = JournalPath(database.Name);
        string preparation = path + ".preparing";
        if (Directory.Exists(path) || Directory.Exists(preparation))
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "Transaction recovery evidence cannot be a directory.");
        }

        if (!File.Exists(path))
        {
            if (!File.Exists(preparation))
            {
                return;
            }

            path = preparation;
        }

        bool matchingPreparation = false;
        using (var journal = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
        {
            if (path != preparation && File.Exists(preparation))
            {
                using var staged = new FileStream(preparation, FileMode.Open, FileAccess.Read, FileShare.None);
                ValidatePreparationCopy(database.Name, journal, staged);
                matchingPreparation = true;
            }

            JournalState state = ValidateJournal(database, journal);
            if (!state.Committed)
            {
                ValidateCurrentContent(database, journal, state);
                RestoreBeforeImages(database, journal, state);
            }
        }

        beforeCleanup?.Invoke();
        if (matchingPreparation)
        {
            File.Delete(preparation);
            _ = DirectoryPersistence.FlushParent(preparation);
        }

        File.Delete(path);
        _ = DirectoryPersistence.FlushParent(path);
    }

    /// <summary>Validates all original and current content before idempotently restoring an undecided caller-stream transaction.</summary>
    /// <param name="database">The exclusively leased writable database stream.</param>
    /// <param name="lease">The active persistent recovery lease.</param>
    /// <exception cref="IOException">The journal or database stream could not be read or restored.</exception>
    /// <exception cref="JetCorruptDataException">The journal or database content conflicts with the recovery manifest.</exception>
    internal static void Recover(Stream database, IPersistentStreamRecoveryLease lease)
    {
        ValidateLeaseIdentity(lease);
        if (!lease.TryOpenPendingJournal(out Stream? published, out Stream? preparing))
        {
            return;
        }

        try
        {
            Stream? primary = published ?? preparing;
            if (primary is null)
            {
                return;
            }

            if (!primary.CanRead || !primary.CanWrite || !primary.CanSeek)
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The persistent recovery journal stream must be readable, writable, and seekable.");
            }

            if (published is not null && preparing is not null)
            {
                if (!preparing.CanRead || !preparing.CanSeek)
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The preparing recovery journal stream must be readable and seekable.");
                }

                ValidatePreparationCopy(string.Empty, published, preparing, lease);
            }

            JournalState state = ValidateJournal(database, primary, lease);
            if (!state.Committed)
            {
                ValidateCurrentContent(database, primary, state);
                RestoreBeforeImages(database, primary, state, lease);
            }
        }
        finally
        {
            preparing?.Dispose();
            published?.Dispose();
        }

        lease.RetireJournal();
    }

    private static void RestoreBeforeImages(Stream database, Stream journal, JournalState state, IPersistentStreamRecoveryLease? lease = null)
    {
        byte[] page = new byte[state.PageSize];
        byte[] record = new byte[state.PageSize + 40];
        ValidateReplayInventory(journal, state, record);
        long replayPosition = state.Start;
        uint remaining = checked((uint)state.BeforeImages.Count);
        bool recovering = state.Recovering;
        byte phase = 0;
        while (true)
        {
            switch (NextReplayAction(state, recovering, remaining, phase))
            {
                case JournalReplayAction.Marker:
                    _ = AppendRecord(journal, state.PageSize, -3, [], durable: true, lease);
                    recovering = true;
                    break;
                case JournalReplayAction.Page:
                    KeyValuePair<long, long> entry = NextReplayPage(journal, state, ref replayPosition, remaining, record);
                    int count = ReadOriginalPage(journal, state, entry.Key, entry.Value, page, record);
                    database.Position = entry.Key;
                    database.Write(page, 0, count);
                    remaining--;
                    break;
                case JournalReplayAction.Resize:
                    database.SetLength(state.OriginalLength);
                    phase = 1;
                    break;
                case JournalReplayAction.Flush:
                    FlushDatabase(database, lease);
                    phase = 2;
                    break;
                case JournalReplayAction.Complete:
                    _ = AppendRecord(journal, state.PageSize, -1, DatabaseHash(database, state.PageSize), durable: true, lease);
                    return;
                case JournalReplayAction.Invalid:
                    throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery replay control state is invalid.");
                default:
                    throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery kernel returned an unknown replay action.");
            }
        }
    }

    private static JournalReplayAction NextReplayAction(JournalState state, bool recovering, uint remaining, byte phase)
        => (JournalReplayAction)RecoveryKernel.JournalReplayAction(
            (uint)state.PageSize, (ulong)state.OriginalLength, (ulong)state.MinimumLength, recovering, remaining, phase);

    private static void ValidateReplayInventory(Stream journal, JournalState state, byte[] record)
    {
        uint remaining = checked((uint)state.BeforeImages.Count);
        for (long position = state.Start; position < state.CompleteEnd; position = NextRecord(state.PageSize, state.OriginalLength, state.CompleteEnd, position))
        {
            long tag = ReadRecord(journal, state.PageSize, state.OriginalLength, state.CompleteEnd, position, record);
            if (RecoveryKernel.IsBeforeImageRecord(unchecked((ulong)tag), (uint)state.PageSize, (ulong)state.OriginalLength))
            {
                _ = ReplayPageOffset(state, position, tag, remaining);
                remaining--;
            }
        }

        if (remaining != 0)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery replay inventory contains an original page absent from its admitted records.");
        }
    }

    private static KeyValuePair<long, long> NextReplayPage(Stream journal, JournalState state, ref long position, uint remaining, byte[] record)
    {
        while (position < state.CompleteEnd)
        {
            long source = position;
            long tag = ReadRecord(journal, state.PageSize, state.OriginalLength, state.CompleteEnd, source, record);
            position = NextRecord(state.PageSize, state.OriginalLength, state.CompleteEnd, source);
            if (RecoveryKernel.IsBeforeImageRecord(unchecked((ulong)tag), (uint)state.PageSize, (ulong)state.OriginalLength))
            {
                return new KeyValuePair<long, long>(ReplayPageOffset(state, source, tag, remaining), source);
            }
        }

        throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery replay schedule is missing an admitted original page.");
    }

    private static long ReplayPageOffset(JournalState state, long position, long tag, uint remaining)
    {
        long originalOffset = tag - long.MinValue;
        bool found = state.BeforeImages.TryGetValue(originalOffset, out long mappedPosition);
        ulong offset = RecoveryKernel.JournalReplayPageOffset(
            (uint)state.PageSize,
            (ulong)state.OriginalLength,
            (ulong)state.CompleteEnd,
            unchecked((ulong)position),
            unchecked((ulong)tag),
            found ? unchecked((ulong)mappedPosition) : 0,
            remaining);
        if (offset == 2147483648)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery replay inventory does not identify the exact admitted original-page record.");
        }

        return checked((long)offset);
    }

    private static void ValidatePreparationCopy(string databasePath, Stream journal, Stream staged, IPersistentStreamRecoveryLease? lease = null)
    {
        (int pageSize, long originalLength) = ValidateManifest(databasePath, staged, lease);
        long completeEnd = CompleteRecordEnd(pageSize, originalLength, staged.Length);
        if (completeEnd > journal.Length)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The preparation journal conflicts with the published recovery generation.");
        }

        byte[] expected = new byte[8192];
        byte[] actual = new byte[8192];

        // An incomplete tail authorizes no database write. Replay can replace it
        // with a marker or decision before cleanup fails; compare only complete evidence.
        for (long offset = 0; offset < completeEnd;)
        {
            int count = checked((int)Math.Min(expected.Length, completeEnd - offset));
            Read(staged, offset, expected, count);
            Read(journal, offset, actual, count);
            if (!expected.AsSpan(0, count).SequenceEqual(actual.AsSpan(0, count)))
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The preparation journal conflicts with the published recovery generation.");
            }

            offset += count;
        }
    }

    private static (int PageSize, long OriginalLength) ValidateManifest(string databasePath, Stream journal, IPersistentStreamRecoveryLease? lease = null)
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
        _ = CompleteRecordEnd(pageSize, originalLength, journal.Length);
        byte[] expectedIdentity = lease is not null ? AdapterIdentity(lease.DatabaseIdentity) : Identity(databasePath);
        long journalGeneration = BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(88, 8));
        bool validGeneration = lease is null || (journalGeneration > 0 && journalGeneration == lease.Generation);
        if (!header.AsSpan(0, 8).SequenceEqual(Magic) || !header.AsSpan(96, 32).SequenceEqual(Hash(header, 96)) || !header.AsSpan(24, 32).SequenceEqual(expectedIdentity) || !validGeneration)
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

    private static JournalState ValidateJournal(Stream database, Stream journal, IPersistentStreamRecoveryLease? lease = null)
    {
        string databasePath = lease is null && database is FileStream fileStream ? fileStream.Name : string.Empty;
        (int pageSize, long originalLength) = ValidateManifest(databasePath, journal, lease);
        ValidateLength(database.Length);
        long start = RecordStart(pageSize, originalLength);
        int recordSize = pageSize + 40;
        long completeEnd = CompleteRecordEnd(pageSize, originalLength, journal.Length);
        var beforeImages = new Dictionary<long, long>();
        byte[] record = new byte[recordSize];
        ulong currentLength = (ulong)database.Length;
        ulong maximumLength = (ulong)originalLength;
        ulong minimumLength = (ulong)originalLength;
        bool recovering = false;
        bool authorizedLength = currentLength >= (ulong)originalLength;
        bool committed = false;
        for (long position = start; position < completeEnd; position = NextRecord(pageSize, originalLength, completeEnd, position))
        {
            long offset = ReadRecord(journal, pageSize, originalLength, completeEnd, position, record);
            bool finalRecord = RecoveryKernel.IsFinalJournalRecord((uint)pageSize, (ulong)originalLength, (ulong)journal.Length, (ulong)position);
            var kind = (JournalRecordKind)RecoveryKernel.ClassifyJournalRecord(
                unchecked((ulong)offset),
                (uint)pageSize,
                (ulong)originalLength,
                recovering,
                minimumLength < (ulong)originalLength,
                finalRecord);
            long capturedOffset = kind == JournalRecordKind.BeforeImage ? offset - long.MinValue : offset;
            bool capturePresent = kind is JournalRecordKind.BeforeImage or JournalRecordKind.AfterImage && beforeImages.ContainsKey(capturedOffset);
            ulong truncateLength = BinaryPrimitives.ReadUInt64LittleEndian(record.AsSpan(8));
            bool tailCovered = kind != JournalRecordKind.Truncate || HasTruncateCoverage(pageSize, originalLength, truncateLength, beforeImages);
            bool payloadValid = kind switch
            {
                JournalRecordKind.Commit => record.AsSpan(8, 32).SequenceEqual(DatabaseHash(database, pageSize)),
                JournalRecordKind.BeforeImage => OriginalPageMatches(journal, pageSize, originalLength, capturedOffset, record.AsSpan(8, pageSize)),
                JournalRecordKind.Invalid or JournalRecordKind.Replay or JournalRecordKind.Truncate or JournalRecordKind.AfterImage => true,
                _ => false,
            };
            RecoveryKernel.AdvanceJournalAdmission(
                (uint)pageSize,
                (ulong)originalLength,
                currentLength,
                minimumLength,
                maximumLength,
                recovering,
                authorizedLength,
                unchecked((ulong)offset),
                truncateLength,
                finalRecord,
                capturePresent,
                tailCovered,
                payloadValid,
                out byte admittedKind,
                out minimumLength,
                out maximumLength,
                out recovering,
                out authorizedLength);
            if (admittedKind == (byte)JournalRecordKind.Invalid)
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery record conflicts with its admitted journal prefix or database content.");
            }

            if (admittedKind == (byte)JournalRecordKind.BeforeImage)
            {
                beforeImages.Add(capturedOffset, position);
            }

            if (admittedKind == (byte)JournalRecordKind.Commit)
            {
                committed = true;
                break;
            }
        }

        if (!RecoveryKernel.CompleteJournalAdmission((ulong)originalLength, currentLength, minimumLength, maximumLength, recovering, authorizedLength, committed))
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The database length conflicts with its recovery journal.");
        }

        return new JournalState(pageSize, originalLength, start, completeEnd, checked((long)minimumLength), recovering, committed, beforeImages);
    }

    private static bool HasTruncateCoverage(int pageSize, long originalLength, ulong length, Dictionary<long, long> beforeImages)
    {
        uint requiredPages = RecoveryKernel.TruncateOriginalPageCount((uint)pageSize, (ulong)originalLength, length);
        for (uint index = 0; index < requiredPages; index++)
        {
            ulong offset = RecoveryKernel.TruncateOriginalPageOffset((uint)pageSize, (ulong)originalLength, length, index);
            if (!beforeImages.ContainsKey(checked((long)offset)))
            {
                return false;
            }
        }

        return true;
    }

    private static void ValidateCurrentContent(Stream database, Stream journal, JournalState state)
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
                if (!RecoveryKernel.CheckJournalZeroExtension(current, (uint)oldCount, (uint)count))
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "An unassigned extension of an original page is not zero-filled.");
                }

                continue;
            }

            Array.Clear(original, 0, original.Length);
            if (state.BeforeImages.TryGetValue(offset, out long beforePosition))
            {
                _ = ReadOriginalPage(journal, state, offset, beforePosition, original, record);
            }

            byte authorization = RecoveryKernel.InitializeJournalAuthorization(
                current, original, valid, (uint)count, state.Recovering, (ulong)offset, (ulong)state.MinimumLength);
            if (authorization == 0)
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery content buffers have invalid bounds.");
            }

            if (authorization == 2)
            {
                continue;
            }

            for (long position = state.Start; position < state.CompleteEnd; position = NextRecord(state.PageSize, state.OriginalLength, state.CompleteEnd, position))
            {
                if (ReadRecord(journal, state.PageSize, state.OriginalLength, state.CompleteEnd, position, record) == offset
                    && !RecoveryKernel.IncludeJournalAfterImage(current, record, valid, (uint)count))
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery content buffers have invalid bounds.");
                }
            }

            if (!RecoveryKernel.CheckJournalAuthorization(valid, (uint)count))
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "Database content conflicts with the recovery journal; no recovery bytes were written.");
            }
        }
    }

    private static long RecordStart(int pageSize, long originalLength)
    {
        ulong start = RecoveryKernel.JournalRecordStart(unchecked((uint)pageSize), unchecked((ulong)originalLength));
        if (start == 0)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery manifest framing is invalid.");
        }

        return checked((long)start);
    }

    private static long CompleteRecordEnd(int pageSize, long originalLength, long journalLength)
    {
        ulong end = RecoveryKernel.CompleteJournalEnd(unchecked((uint)pageSize), unchecked((ulong)originalLength), unchecked((ulong)journalLength));
        if (end == 0)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery journal does not contain a complete bounded manifest.");
        }

        return checked((long)end);
    }

    private static int OriginalPageLength(int pageSize, long originalLength, long offset)
    {
        uint count = RecoveryKernel.OriginalPageLength(unchecked((uint)pageSize), unchecked((ulong)originalLength), unchecked((ulong)offset));
        if (count == 0)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery original-page payload bounds are invalid.");
        }

        return checked((int)count);
    }

    private static long TruncatePageOffset(int pageSize, long originalLength, long length, uint index)
    {
        ulong offset = RecoveryKernel.TruncateOriginalPageOffset(
            unchecked((uint)pageSize), unchecked((ulong)originalLength), unchecked((ulong)length), index);
        if (offset == 2147483648)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery truncation coverage page is invalid.");
        }

        return checked((long)offset);
    }

    private static long NextRecord(int pageSize, long originalLength, long completeEnd, long position)
    {
        ulong next = RecoveryKernel.NextJournalRecord(
            unchecked((uint)pageSize), unchecked((ulong)originalLength), unchecked((ulong)completeEnd), unchecked((ulong)position));
        if (next == 0)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery record source is not a complete bounded journal record.");
        }

        return checked((long)next);
    }

    private static int ReadOriginalPage(Stream journal, JournalState state, long offset, long position, byte[] page, byte[] record)
    {
        int count = OriginalPageLength(state.PageSize, state.OriginalLength, offset);
        long tag = ReadRecord(journal, state.PageSize, state.OriginalLength, state.CompleteEnd, position, record);
        ulong payload = RecoveryKernel.OriginalPageRecordPayloadOffset(
            (uint)state.PageSize, (ulong)state.OriginalLength, (ulong)state.CompleteEnd, unchecked((ulong)position), unchecked((ulong)tag), unchecked((ulong)offset));
        if (payload == 0)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery original-page source does not identify the requested original page.");
        }

        ReadOnlySpan<byte> original = record.AsSpan(checked((int)(payload - (ulong)position)), state.PageSize);
        VerifyOriginalPage(journal, state.PageSize, state.OriginalLength, offset, original);
        original[..count].CopyTo(page);
        return count;
    }

    private static void VerifyOriginalPage(Stream journal, int pageSize, long originalLength, long offset, ReadOnlySpan<byte> page)
    {
        if (!OriginalPageMatches(journal, pageSize, originalLength, offset, page))
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "An original page does not match its prepared recovery manifest.");
        }
    }

    private static bool OriginalPageMatches(Stream journal, int pageSize, long originalLength, long offset, ReadOnlySpan<byte> page)
    {
        ulong hashOffset = RecoveryKernel.OriginalPageHashOffset(unchecked((uint)pageSize), unchecked((ulong)originalLength), unchecked((ulong)offset));
        if (hashOffset == 0)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery original-page manifest bounds are invalid.");
        }

        byte[] expected = new byte[32];
        Read(journal, checked((long)hashOffset), expected, expected.Length);
        return expected.AsSpan().SequenceEqual(Hash(page[..OriginalPageLength(pageSize, originalLength, offset)]));
    }

    private static long AppendRecord(Stream journal, int pageSize, long offset, ReadOnlySpan<byte> bytes, bool durable = true, IPersistentStreamRecoveryLease? lease = null)
    {
        int payloadLength = Math.Max(pageSize, 32);
        byte[] record = new byte[checked(8 + payloadLength + 32)];
        if (journal.Length > MaximumJournalLength - record.Length)
        {
            throw new JetLimitationException(JetErrorCode.JournalBudgetExceeded, "The bounded recovery journal size limit has been reached.");
        }

        byte[] lengthBytes = new byte[8];
        Read(journal, 16, lengthBytes, lengthBytes.Length);
        long completeEnd = CompleteRecordEnd(pageSize, BinaryPrimitives.ReadInt64LittleEndian(lengthBytes), journal.Length);
        journal.SetLength(completeEnd);

        BinaryPrimitives.WriteInt64LittleEndian(record, offset);
        bytes.CopyTo(record.AsSpan(8, payloadLength));
        Hash(record, record.Length - 32).CopyTo(record, record.Length - 32);
        journal.Position = journal.Length;
        long position = journal.Position;
        journal.Write(record, 0, record.Length);
        if (durable)
        {
            FlushJournal(journal, lease);
        }

        return position;
    }

    private static long ReadRecord(Stream journal, int pageSize, long originalLength, long completeEnd, long position, byte[] record)
    {
        long next = NextRecord(pageSize, originalLength, completeEnd, position);
        if (record.Length != next - position)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery record buffer does not match its complete bounded source.");
        }

        Read(journal, position, record, record.Length);
        if (!record.AsSpan(record.Length - 32).SequenceEqual(Hash(record, record.Length - 32)))
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery page record checksum is invalid.");
        }

        return BinaryPrimitives.ReadInt64LittleEndian(record);
    }

    private static byte[] DatabaseHash(Stream database, int pageSize)
    {
        ValidateLength(database.Length);
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        FilePageDigests.AppendIdentity(hash, database.Length, pageSize);
        byte[] buffer = new byte[pageSize];
        for (long offset = 0; offset < database.Length;)
        {
            int count = checked((int)Math.Min(buffer.Length, database.Length - offset));
            Read(database, offset, buffer, count);
            hash.AppendData(Hash(buffer, count));
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

    private static byte[] AdapterIdentity(string identity)
    {
        byte[] bytes = Encoding.UTF8.GetBytes("jdw-stream-adapter:" + identity);
        return Hash(bytes, bytes.Length);
    }

    private static void ValidateLeaseIdentity(IPersistentStreamRecoveryLease lease)
    {
        if (string.IsNullOrWhiteSpace(lease.DatabaseIdentity) || lease.Generation < 0)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The persistent recovery lease identity or generation is invalid.");
        }
    }

    private static void FlushDatabase(Stream database, IPersistentStreamRecoveryLease? lease)
    {
        if (lease is not null)
        {
            database.Flush();
            lease.FenceDatabase(database);
        }
        else
        {
            ((FileStream)database).Flush(true);
        }
    }

    private static void FlushJournal(Stream journal, IPersistentStreamRecoveryLease? lease)
    {
        if (lease is not null)
        {
            journal.Flush();
            lease.FenceJournal(journal);
        }
        else
        {
            ((FileStream)journal).Flush(true);
        }
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

    private static void Read(Stream stream, long offset, byte[] bytes, int count)
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
        if (pageSize < 0 || !RecoveryKernel.IsPageSize((uint)pageSize))
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery page size is invalid.");
        }
    }

    private static void ValidateLength(long length)
    {
        if (length < 0 || !RecoveryKernel.IsDatabaseLength((ulong)length))
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
        if (this.completed || offset < 0 || !RecoveryKernel.IsPageOffset((ulong)offset, (uint)this.pageSize) || after.Length != this.pageSize)
        {
            throw new JetOperationException(JetErrorCode.InvalidOperationState, "The rollback journal cannot record this physical page write.");
        }

        long currentLength = this.database.Length;
        long gapEnd = Math.Min(offset, this.originalLength);
        long gapStart = currentLength - (currentLength % this.pageSize);
        int records = offset < this.originalLength && !this.beforeImages.ContainsKey(offset) ? 2 : 1;
        if (currentLength < gapEnd)
        {
            for (long gapOffset = gapStart; gapOffset < gapEnd; gapOffset += this.pageSize)
            {
                records += this.beforeImages.ContainsKey(gapOffset) ? 1 : 2;
            }
        }

        this.RequireRecordCapacity(records);
        if (currentLength < gapEnd)
        {
            for (long gapOffset = gapStart; gapOffset < gapEnd; gapOffset += this.pageSize)
            {
                this.CaptureOriginalPage(gapOffset);
            }
        }

        this.CaptureOriginalPage(offset);
        if (currentLength < gapEnd)
        {
            // A sparse write reextends removed original bytes as zeros. Persist
            // their exact after-images before mutation, without a replay marker.
            // Preserve an existing partial page prefix after a torn earlier write.
            byte[] gap = new byte[this.pageSize];
            int prefixLength = checked((int)(currentLength - gapStart));
            if (prefixLength > 0)
            {
                Read(this.database, gapStart, gap, prefixLength);
            }

            for (long gapOffset = gapStart; gapOffset < gapEnd; gapOffset += this.pageSize)
            {
                _ = this.AppendRecord(gapOffset, gap, durable: false);
                if (prefixLength > 0)
                {
                    Array.Clear(gap, 0, prefixLength);
                    prefixLength = 0;
                }
            }
        }

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
        if (this.completed || length < 0 || !RecoveryKernel.IsTruncateLength((ulong)length, (uint)this.pageSize, (ulong)this.database.Length))
        {
            throw new JetOperationException(JetErrorCode.InvalidOperationState, "The rollback journal cannot record this truncation.");
        }

        uint requiredPages = RecoveryKernel.TruncateOriginalPageCount((uint)this.pageSize, (ulong)this.originalLength, (ulong)length);
        int missing = 0;
        for (uint index = 0; index < requiredPages; index++)
        {
            long offset = TruncatePageOffset(this.pageSize, this.originalLength, length, index);
            if (!this.beforeImages.ContainsKey(offset))
            {
                missing++;
            }
        }

        this.RequireRecordCapacity(missing + 1);
        for (uint index = 0; index < requiredPages; index++)
        {
            this.CaptureOriginalPage(TruncatePageOffset(this.pageSize, this.originalLength, length, index));
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

        JournalState state = ValidateJournal(this.database, this.journal, this.lease);
        if (state.PageSize != this.pageSize || state.OriginalLength != this.originalLength || state.Committed)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery manifest differs from the prepared database image.");
        }

        byte[] page = new byte[this.pageSize];
        byte[] record = new byte[this.pageSize + 40];
        ValidateReplayInventory(this.journal, state, record);
        long replayPosition = state.Start;
        uint remaining = checked((uint)state.BeforeImages.Count);
        bool recovering = state.Recovering;
        byte phase = 0;
        while (true)
        {
            switch (NextReplayAction(state, recovering, remaining, phase))
            {
                case JournalReplayAction.Marker:
                    this.RecordRecoveryStart();
                    recovering = true;
                    break;
                case JournalReplayAction.Page:
                    KeyValuePair<long, long> entry = NextReplayPage(this.journal, state, ref replayPosition, remaining, record);
                    int count = ReadOriginalPage(this.journal, state, entry.Key, entry.Value, page, record);
                    await store.WriteAsync(entry.Key, page.AsMemory(0, count), CancellationToken.None).ConfigureAwait(false);
                    remaining--;
                    break;
                case JournalReplayAction.Resize:
                    await store.SetLengthAsync(this.originalLength, CancellationToken.None).ConfigureAwait(false);
                    phase = 1;
                    break;
                case JournalReplayAction.Flush:
                    await store.FlushAsync(true, CancellationToken.None).ConfigureAwait(false);
                    phase = 2;
                    break;
                case JournalReplayAction.Complete:
                    this.Complete();
                    return;
                case JournalReplayAction.Invalid:
                    throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery replay control state is invalid.");
                default:
                    throw new JetCorruptDataException(JetErrorCode.CorruptRecoveryJournal, "The recovery kernel returned an unknown replay action.");
            }
        }
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
        FlushDatabase(this.database, this.lease);
        byte[] digest = this.pageDigests is { IsValid: true } && this.pageDigests.Length == this.database.Length
            ? this.pageDigests.DatabaseHash()
            : DatabaseHash(this.database, this.pageSize);
        _ = this.AppendRecord(-1, digest);
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
            if (this.lease is not null)
            {
                this.lease.RetireJournal();
            }
            else
            {
                string name = ((FileStream)this.database).Name;
                File.Delete(JournalPath(name));
                _ = DirectoryPersistence.FlushParent(name);
            }
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            // The durable decision is authoritative. Retain a stale sidecar for the next recovery.
        }
    }

    private long AppendRecord(long offset, ReadOnlySpan<byte> bytes, bool durable = true)
        => AppendRecord(this.journal, this.pageSize, offset, bytes, durable, this.lease);

    private void RequireRecordCapacity(int records)
    {
        if (this.journal.Length > MaximumJournalLength - ((records + 2L) * (this.pageSize + 40)))
        {
            throw new JetLimitationException(JetErrorCode.JournalBudgetExceeded, "The bounded recovery journal must retain decision record capacity.");
        }
    }

    private sealed record JournalState(int PageSize, long OriginalLength, long Start, long CompleteEnd, long MinimumLength, bool Recovering, bool Committed, Dictionary<long, long> BeforeImages);
}
