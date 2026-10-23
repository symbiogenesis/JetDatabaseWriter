namespace JetDatabaseWriter.Encryption;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Transactions;
using JetDatabaseWriter.Verification;

/// <summary>Binds bounded replacement copies and a process-recovery decision to one exclusive path generation.</summary>
/// <remarks>Contents are flushed; directory-entry persistence and physical power-loss ordering are not established.</remarks>
internal sealed class EncryptionReplacementIntent : IDisposable
{
    private const int HeaderLength = 256;
    private const int DecisionLength = 96;
    private const int BufferLength = 65536;
    private const long MaximumLength = 2L * 1024 * 1024 * 1024;
    private static readonly byte[] Magic = Encoding.ASCII.GetBytes("JDWREPL1");
    private static readonly byte[] CommitMagic = Encoding.ASCII.GetBytes("JDWCMT01");
    private readonly string path;
    private readonly FileStream intent;
    private readonly Guid generation = Guid.NewGuid();
    private readonly Action<string>? boundary;
    private FileStream? source;
    private Image original = new(0, []);
    private bool oldCreated;
    private bool newCreated;
    private bool disposed;

    private EncryptionReplacementIntent(string path, FileStream intent, FileStream source, Action<string>? boundary)
    {
        this.path = path;
        this.intent = intent;
        this.source = source;
        this.boundary = boundary;
    }

    private static FileShare DestinationSharing => Path.DirectorySeparatorChar == '\\' ? FileShare.Delete : FileShare.None;

    /// <summary>Gets the immutable old copy name.</summary>
    internal string OldPath => Candidate(this.path, this.generation, ".old");

    /// <summary>Gets the immutable replacement copy name.</summary>
    internal string NewPath => Candidate(this.path, this.generation, ".new.tmp");

    /// <summary>Gets a value indicating whether preparation may already authorize recovery.</summary>
    internal bool IsPrepared { get; private set; }

    /// <summary>Gets the deterministic discovery name.</summary>
    /// <param name="path">The destination path.</param>
    /// <returns>The intent path.</returns>
    internal static string IntentPath(string path) => path + ".jdw-replacement";

    /// <summary>Refuses an open that cannot safely replace an already opened file.</summary>
    /// <param name="path">The destination path.</param>
    /// <exception cref="JetOperationException">Replacement evidence must be recovered through the path writer API.</exception>
    internal static void RejectPending(string path)
    {
        if (File.Exists(IntentPath(path)) || Directory.Exists(IntentPath(path)))
        {
            throw new JetOperationException(JetErrorCode.HotJournalConflict, "The database has replacement recovery evidence; recover it through AccessWriter.OpenAsync(path) before opening a stream or reader.");
        }
    }

    /// <summary>Acquires exclusive source ownership before publishing an exclusive intent name.</summary>
    /// <param name="path">The destination path.</param>
    /// <param name="source">An optional already exclusive source whose ownership transfers to this operation.</param>
    /// <param name="boundary">An optional per-operation fault observer.</param>
    /// <param name="cancellationToken">Cancels preparation before publication.</param>
    /// <returns>The owner of the source, intent and owned preparation files.</returns>
    /// <exception cref="IOException">Exclusive ownership or bounded preparation failed.</exception>
    internal static EncryptionReplacementIntent Begin(string path, FileStream? source = null, Action<string>? boundary = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        path = Path.GetFullPath(path);
        FileStream? intent = null;
        EncryptionReplacementIntent? owner = null;
        try
        {
            RejectPending(path);
            source ??= new FileStream(path, FileMode.Open, FileAccess.Read, DestinationSharing);
            ValidateLength(source.Length);
            if (!Identity(source.Name).AsSpan().SequenceEqual(Identity(path)))
            {
                throw new IOException("The replacement source has a different path identity.");
            }

            intent = EncryptionMaintenanceFile.Create(IntentPath(path));
            owner = new EncryptionReplacementIntent(path, intent, source, boundary);
            using (FileStream old = EncryptionMaintenanceFile.Create(owner.OldPath))
            {
                owner.oldCreated = true;
                owner.original = Copy(source, old, cancellationToken);
            }

            return owner;
        }
        catch
        {
            if (owner is not null)
            {
                owner.Dispose();
            }
            else
            {
                intent?.Dispose();
                source?.Dispose();
            }

            throw;
        }
    }

    /// <summary>Recovers a known path before opening the destination, including when its name is absent.</summary>
    /// <param name="path">The canonical destination path.</param>
    /// <param name="boundary">An optional per-operation recovery fault observer.</param>
    /// <param name="cancellationToken">Cancels validation before publication or cleanup begins.</param>
    /// <returns>Whether a replacement intent was present.</returns>
    /// <exception cref="JetCorruptDataException">Evidence conflicts, is unbounded, or lacks its selected complete content.</exception>
    internal static bool Recover(string path, Action<string>? boundary = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            path = Path.GetFullPath(path);
        }
        catch (Exception error) when (error is ArgumentException or NotSupportedException or PathTooLongException)
        {
            // Retain the ordinary File.Exists-based path guard for invalid spellings.
            return false;
        }

        if (Directory.Exists(IntentPath(path)))
        {
            throw Corrupt("The replacement intent is a directory.");
        }

        if (!File.Exists(IntentPath(path)))
        {
            return false;
        }

        using var intent = new FileStream(IntentPath(path), FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        State state = ReadState(path, intent);
        FileStream? database = OpenDestination(path);
        try
        {
            Image? observed = database is null ? null : Hash(database, boundary, cancellationToken);
            ValidateState(path, state, database, observed, boundary, cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();
            using RecoverableWriterLock.RecoveryLease ownership = RecoverableWriterLock.AcquireRecovery(path, boundary);
            boundary?.Invoke("validated");

            // Retained source/native leases exclude new cooperating owners.
            // Once replay starts, finish the same decision without cancellation.
            Image selected = state.Committed ? state.New : state.Old;
            if (!Matches(observed, selected))
            {
                FileStream published = Publish(path, state, selected, Candidate(path, state.Generation, state.Committed ? ".new.tmp" : ".old"), database is not null, boundary);
                database?.Dispose();
                database = published;
            }

            Cleanup(path, intent, state, database!, boundary, ownership);
        }
        finally
        {
            database?.Dispose();
        }

        return true;
    }

    /// <summary>Creates the generation's replacement copy without overwriting another file.</summary>
    /// <returns>The caller-owned staging stream.</returns>
    internal FileStream CreateReplacement()
    {
        FileStream result = EncryptionMaintenanceFile.Create(this.NewPath);
        this.newCreated = true;
        return result;
    }

    /// <summary>Copies a caller-owned staging file into immutable generation storage.</summary>
    /// <param name="temporary">The complete staging file.</param>
    internal void CopyReplacement(string temporary)
    {
        using var input = new FileStream(temporary, FileMode.Open, FileAccess.Read, FileShare.None);
        using FileStream output = this.CreateReplacement();
        _ = Copy(input, output, CancellationToken.None);
    }

    /// <summary>Flushes Prepared before publication and an append-only Committed decision afterwards.</summary>
    /// <param name="cancellationToken">Cancels before the Prepared record is written.</param>
    internal void Commit(CancellationToken cancellationToken)
    {
        Image next = ReadImage(this.NewPath, this.boundary, cancellationToken);
        RequireImage(ReadImage(this.OldPath, this.boundary, cancellationToken), this.original);
        cancellationToken.ThrowIfCancellationRequested();
        var state = new State(this.generation, this.original, next, Committed: false);
        byte[] header = Header(this.path, state);
        this.IsPrepared = true; // An uncertain header flush must retain its evidence.
        this.intent.Write(header, 0, header.Length);
        this.intent.Flush(true);
        this.boundary?.Invoke("prepared");
        ValidateState(this.path, state, this.source, this.original, cancellationToken: CancellationToken.None);
        using FileStream database = Publish(this.path, state, next, this.NewPath, destinationExisted: true, this.boundary);
        this.source?.Dispose();
        this.source = null;
        this.boundary?.Invoke("database-flush");
        byte[] decision = Decision(state);
        this.intent.Position = HeaderLength;
        this.intent.Write(decision, 0, DecisionLength / 2);
        this.boundary?.Invoke("decision-partial");
        this.intent.Write(decision, DecisionLength / 2, DecisionLength / 2);
        this.intent.Flush(true);
        this.boundary?.Invoke("committed");
        try
        {
            Cleanup(this.path, this.intent, state with { Committed = true }, database, this.boundary, ownership: null);
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        {
            // A completed decision remains authoritative when cleanup is refused.
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (this.disposed)
        {
            return;
        }

        this.disposed = true;
        if (!this.IsPrepared)
        {
            TryDelete(this.OldPath, this.oldCreated);
            TryDelete(this.NewPath, this.newCreated);
            this.intent.Dispose();
            TryDelete(IntentPath(this.path), owned: true);
        }
        else
        {
            this.intent.Dispose();
        }

        this.source?.Dispose();
        this.source = null;
    }

    private static void ValidateState(string path, State state, FileStream? database, Image? destination, Action<string>? boundary = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string journal = PersistentRollbackJournal.JournalPath(path);
        if (File.Exists(journal) || Directory.Exists(journal) || File.Exists(journal + ".preparing") || Directory.Exists(journal + ".preparing"))
        {
            throw Corrupt("Replacement and transaction recovery evidence cannot be combined automatically.");
        }

        string old = Candidate(path, state.Generation, ".old");
        string next = Candidate(path, state.Generation, ".new.tmp");
        if (Directory.Exists(old) || Directory.Exists(next) || Directory.Exists(Candidate(path, state.Generation, ".publish.tmp")))
        {
            throw Corrupt("A replacement generation candidate is a directory.");
        }

        if (File.Exists(old))
        {
            RequireImage(ReadImage(old, boundary, cancellationToken), state.Old);
        }

        if (File.Exists(next))
        {
            RequireImage(ReadImage(next, boundary, cancellationToken), state.New);
        }

        if (destination is not null && !Matches(destination, state.Old) && !Matches(destination, state.New))
        {
            throw Corrupt("The destination is not an expected replacement generation.");
        }

        Image selected = state.Committed ? state.New : state.Old;
        string selectedPath = state.Committed ? next : old;
        if (!Matches(destination, selected) && !File.Exists(selectedPath))
        {
            throw Corrupt("The complete selected replacement generation is missing.");
        }

        ValidatePublication(path, state, database, boundary, cancellationToken);
    }

    private static void ValidatePublication(string path, State state, FileStream? database, Action<string>? boundary, CancellationToken cancellationToken)
    {
        string publication = Candidate(path, state.Generation, ".publish.tmp");
        if (!File.Exists(publication))
        {
            return;
        }

        Image observed = ReadImage(publication, boundary, cancellationToken);
        if (Matches(observed, state.Old) || Matches(observed, state.New))
        {
            return;
        }

        // A process death can leave a prefix of either previously selected copy.
        foreach (string candidate in new[] { Candidate(path, state.Generation, ".old"), Candidate(path, state.Generation, ".new.tmp") })
        {
            if (File.Exists(candidate) && IsPrefix(publication, candidate, cancellationToken))
            {
                return;
            }
        }

        if (database is not null && IsPrefix(publication, database, cancellationToken))
        {
            return;
        }

        throw Corrupt("The publication staging file conflicts with its recorded generation.");
    }

    private static FileStream Publish(string path, State state, Image selected, string selectedPath, bool destinationExisted, Action<string>? boundary)
    {
        string publication = Candidate(path, state.Generation, ".publish.tmp");
        File.Delete(publication); // ValidateState verified any surviving owned prefix before mutation.
        using (var input = new FileStream(selectedPath, FileMode.Open, FileAccess.Read, FileShare.None))
        using (FileStream output = EncryptionMaintenanceFile.Create(publication))
        {
            boundary?.Invoke("publication-created");
            RequireImage(Copy(input, output, CancellationToken.None, boundary), selected);
        }

        RequireImage(ReadImage(publication), selected);
        boundary?.Invoke("publication-ready");
        if (destinationExisted)
        {
            File.Replace(publication, path, destinationBackupFileName: null, ignoreMetadataErrors: true);
        }
        else
        {
            File.Move(publication, path);
        }

        boundary?.Invoke("renamed");
        FileStream database = OpenDestination(path) ?? throw Corrupt("The published destination is missing.");
        try
        {
            RequireImage(Hash(database), selected);
            boundary?.Invoke("destination-before-flush");
            database.Flush(true);
            boundary?.Invoke("destination-flush");
            return database;
        }
        catch
        {
            database.Dispose();
            throw;
        }
    }

    private static void Cleanup(string path, FileStream intent, State state, FileStream database, Action<string>? boundary, RecoverableWriterLock.RecoveryLease? ownership)
    {
        Image observed = Hash(database);
        RequireImage(observed, state.Committed ? state.New : state.Old);
        ValidateState(path, state, database, observed);
        boundary?.Invoke("cleanup-before-flush");
        database.Flush(true);
        boundary?.Invoke("cleanup-flush");
        foreach (string suffix in new[] { ".publish.tmp", ".old", ".new.tmp" })
        {
            File.Delete(Candidate(path, state.Generation, suffix));
            boundary?.Invoke("cleanup" + suffix);
        }

        boundary?.Invoke("before-intent-delete");
        if (ownership is not null)
        {
            ownership.Cleanup(boundary);
            boundary?.Invoke("ownership-cleanup");
        }

        intent.Dispose();
        File.Delete(IntentPath(path)); // Database ownership excludes a successor generation until deletion finishes.
    }

    private static State ReadState(string path, FileStream intent)
    {
        ulong length = checked((ulong)intent.Length);
        if (!RecoveryKernel.IsReplacementIntentLength(length))
        {
            throw Corrupt("The replacement intent length is invalid.");
        }

        byte[] bytes = new byte[HeaderLength];
        Read(intent, bytes);
        Guid generation = new(bytes.AsSpan(16, 16));
        bool headerFieldsValid = bytes.AsSpan(0, 8).SequenceEqual(Magic) && BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(8)) == 1
            && BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(12)) == HeaderLength && generation != Guid.Empty
            && bytes.AsSpan(32, 32).SequenceEqual(Identity(path)) && bytes.AsSpan(224, 32).SequenceEqual(Digest(bytes.AsSpan(0, 224)));
        ulong oldLength = DecodeLength(bytes.AsSpan(64, 8));
        ulong newLength = DecodeLength(bytes.AsSpan(72, 8));
        if (!RecoveryKernel.IsReplacementHeader(headerFieldsValid, oldLength, newLength))
        {
            throw Corrupt(!headerFieldsValid
                ? "The replacement intent version, identity or checksum is invalid."
                : "Replacement content exceeds the bounded 2 GiB file limit.");
        }

        bool decisionValid = false;
        if (length == HeaderLength + DecisionLength)
        {
            byte[] decision = new byte[DecisionLength];
            Read(intent, decision);
            decisionValid = decision.AsSpan(0, 8).SequenceEqual(CommitMagic) && decision.AsSpan(8, 16).SequenceEqual(generation.ToByteArray())
                && decision.AsSpan(24, 32).SequenceEqual(bytes.AsSpan(112, 32)) && decision.AsSpan(64, 32).SequenceEqual(Digest(decision.AsSpan(0, 64)));
        }

        byte admission = RecoveryKernel.ReplacementAdmission(length, headerFieldsValid, oldLength, newLength, decisionValid);
        if (admission == 0)
        {
            throw Corrupt("The replacement commit decision is invalid.");
        }

        // Admission proves both raw signed-field encodings fit the nonnegative
        // 2 GiB domain before either value is narrowed to the managed length.
        return new(generation, new(checked((long)oldLength), bytes.AsSpan(80, 32).ToArray()), new(checked((long)newLength), bytes.AsSpan(112, 32).ToArray()), Committed: admission == 2);
    }

    private static ulong DecodeLength(ReadOnlySpan<byte> bytes) => RecoveryKernel.DecodeReplacementLength(bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]);

    private static byte[] Header(string path, State state)
    {
        byte[] bytes = new byte[HeaderLength];
        Magic.CopyTo(bytes, 0);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(8), 1);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(12), HeaderLength);
        state.Generation.ToByteArray().CopyTo(bytes, 16);
        Identity(path).CopyTo(bytes, 32);
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(64), state.Old.Length);
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(72), state.New.Length);
        state.Old.Hash.CopyTo(bytes, 80);
        state.New.Hash.CopyTo(bytes, 112);
        Digest(bytes.AsSpan(0, 224)).CopyTo(bytes, 224);
        return bytes;
    }

    private static byte[] Decision(State state)
    {
        byte[] bytes = new byte[DecisionLength];
        CommitMagic.CopyTo(bytes, 0);
        state.Generation.ToByteArray().CopyTo(bytes, 8);
        state.New.Hash.CopyTo(bytes, 24);
        Digest(bytes.AsSpan(0, 64)).CopyTo(bytes, 64);
        return bytes;
    }

    private static Image Copy(FileStream input, FileStream output, CancellationToken cancellationToken, Action<string>? boundary = null)
    {
        ValidateLength(input.Length);
        long prior = input.Position;
        input.Position = 0;
        try
        {
            using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            byte[] bytes = new byte[BufferLength];
            long remaining = input.Length;
            long length = remaining;
            while (remaining > 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                boundary?.Invoke("publication-read");
                int count = input.Read(bytes, 0, checked((int)Math.Min(bytes.Length, remaining)));
                if (count == 0)
                {
                    throw Corrupt("A replacement copy ended before its admitted length.");
                }

                output.Write(bytes, 0, count);
                boundary?.Invoke("publication-written");
                hash.AppendData(bytes, 0, count);
                remaining -= count;
            }

            boundary?.Invoke("publication-before-flush");
            output.Flush(true);
            boundary?.Invoke("publication-flush");
            return new(length, hash.GetHashAndReset());
        }
        finally
        {
            input.Position = prior;
        }
    }

    private static Image Hash(FileStream stream, Action<string>? boundary = null, CancellationToken cancellationToken = default)
    {
        ValidateLength(stream.Length);
        stream.Position = 0;
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        byte[] bytes = new byte[BufferLength];
        long remaining = stream.Length;
        long length = remaining;
        while (remaining > 0)
        {
            boundary?.Invoke("validation-chunk");
            cancellationToken.ThrowIfCancellationRequested();
            int count = stream.Read(bytes, 0, checked((int)Math.Min(bytes.Length, remaining)));
            if (count == 0)
            {
                throw Corrupt("Replacement content ended before its admitted length.");
            }

            hash.AppendData(bytes, 0, count);
            remaining -= count;
        }

        return new(length, hash.GetHashAndReset());
    }

    private static Image ReadImage(string path, Action<string>? boundary = null, CancellationToken cancellationToken = default)
    {
        using var input = new FileStream(path, FileMode.Open, FileAccess.Read, DestinationSharing);
        return Hash(input, boundary, cancellationToken);
    }

    private static FileStream? OpenDestination(string path)
    {
        if (Directory.Exists(path))
        {
            throw Corrupt("The replacement destination is a directory.");
        }

        return File.Exists(path) ? new FileStream(path, FileMode.Open, FileAccess.ReadWrite, DestinationSharing) : null;
    }

    private static bool IsPrefix(string prefixPath, string candidatePath, CancellationToken cancellationToken)
    {
        using var candidate = new FileStream(candidatePath, FileMode.Open, FileAccess.Read, FileShare.None);
        return IsPrefix(prefixPath, candidate, cancellationToken);
    }

    private static bool IsPrefix(string prefixPath, FileStream candidate, CancellationToken cancellationToken)
    {
        using var prefix = new FileStream(prefixPath, FileMode.Open, FileAccess.Read, FileShare.None);
        candidate.Position = 0;
        ValidateLength(candidate.Length);
        if (prefix.Length > candidate.Length)
        {
            return false;
        }

        byte[] actual = new byte[BufferLength];
        byte[] expected = new byte[BufferLength];
        long remaining = prefix.Length;
        while (remaining > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int count = checked((int)Math.Min(actual.Length, remaining));
            Read(prefix, actual.AsSpan(0, count));
            Read(candidate, expected.AsSpan(0, count));
            if (!actual.AsSpan(0, count).SequenceEqual(expected.AsSpan(0, count)))
            {
                return false;
            }

            remaining -= count;
        }

        return true;
    }

    private static void Read(FileStream stream, Span<byte> bytes)
    {
        while (!bytes.IsEmpty)
        {
            int count = stream.Read(bytes);
            if (count == 0)
            {
                throw Corrupt("The replacement evidence is incomplete.");
            }

            bytes = bytes[count..];
        }
    }

    private static bool Matches(Image? actual, Image expected) => actual is not null && actual.Length == expected.Length && actual.Hash.AsSpan().SequenceEqual(expected.Hash);

    private static void RequireImage(Image actual, Image expected)
    {
        if (!Matches(actual, expected))
        {
            throw Corrupt("Replacement content conflicts with its recorded length or SHA-256.");
        }
    }

    private static void ValidateLength(long length)
    {
        if (length is < 0 or > MaximumLength)
        {
            throw Corrupt("Replacement content exceeds the bounded 2 GiB file limit.");
        }
    }

    private static string Candidate(string path, Guid generation, string suffix) => path + ".jdw-replace-" + generation.ToString("N") + suffix;

    private static byte[] Identity(string path) => Digest(Encoding.UTF8.GetBytes(Path.DirectorySeparatorChar == '\\' ? Path.GetFullPath(path).ToUpperInvariant() : Path.GetFullPath(path)));

    private static byte[] Digest(ReadOnlySpan<byte> bytes)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(bytes);
        return hash.GetHashAndReset();
    }

    private static JetCorruptDataException Corrupt(string message) => new(JetErrorCode.CorruptRecoveryJournal, message);

    private static void TryDelete(string path, bool owned)
    {
        if (!owned)
        {
            return;
        }

        try
        {
            File.Delete(path);
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        {
            // Preserve the original failure and retain evidence rather than deleting another generation.
        }
    }

    private sealed record Image(long Length, byte[] Hash);

    private sealed record State(Guid Generation, Image Old, Image New, bool Committed);
}
