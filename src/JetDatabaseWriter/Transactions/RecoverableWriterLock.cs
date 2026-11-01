namespace JetDatabaseWriter.Transactions;

using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;

/// <summary>Binds a strict writer's lock to a checksummed, randomly generated ownership receipt.</summary>
/// <remarks>The caller must exclusively own the database. File flushes do not establish directory-entry power-loss durability.</remarks>
internal sealed class RecoverableWriterLock : IDisposable
{
    private const int LockLength = 2 * LockFileSlotWriter.SlotSize;
    private const int ReceiptLength = 8 + 32 + LockLength + 32;
    private static readonly byte[] Magic = Encoding.ASCII.GetBytes("JDWLOCK1");
    private readonly FileStream lockFile;
    private readonly FileStream receipt;
    private bool disposed;

    private RecoverableWriterLock(FileStream lockFile, FileStream receipt)
    {
        this.lockFile = lockFile;
        this.receipt = receipt;
    }

    /// <summary>Gets the deterministic receipt name associated with a database.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <returns>The ownership receipt path.</returns>
    internal static string ReceiptPath(string databasePath) => databasePath + ".jdw-owner";

    /// <summary>Creates a new generation and flushes its receipt before returning ownership.</summary>
    /// <param name="databasePath">The exclusively owned database path.</param>
    /// <param name="machineName">The ordinary lock slot machine name.</param>
    /// <param name="userName">The ordinary lock slot user name.</param>
    /// <param name="boundary">An optional fault observer after receipt preparation and before failed-acquisition lock cleanup.</param>
    /// <returns>The ownership lease.</returns>
    /// <exception cref="IOException">Existing ownership is live, invalid or conflicts with the lock.</exception>
    /// <exception cref="JetLockException">An unowned lock or invalid ownership evidence prevents acquisition.</exception>
    internal static RecoverableWriterLock Acquire(string databasePath, string machineName, string userName, Action<string>? boundary = null)
    {
        _ = Recover(databasePath);
        string lockPath = LockFileSlotWriter.GetLockFilePath(databasePath);
        if (File.Exists(lockPath) || Directory.Exists(lockPath))
        {
            throw new JetLockException(JetErrorCode.DatabaseInUse, $"Database is already in use. A lockfile exists at: {lockPath}");
        }

        byte[] contents = new byte[LockLength];
        WriteField(contents.AsSpan(0, 32), machineName);
        WriteField(contents.AsSpan(32, 32), userName);
        WriteField(contents.AsSpan(64, 32), "JetDatabaseWriter");
        WriteField(contents.AsSpan(96, 32), Guid.NewGuid().ToString("N").AsSpan(0, 31));
        byte[] proof = new byte[ReceiptLength];
        Magic.CopyTo(proof, 0);
        Identity(databasePath).CopyTo(proof, 8);
        contents.CopyTo(proof, 40);
        Hash(proof.AsSpan(0, ReceiptLength - 32)).CopyTo(proof, ReceiptLength - 32);

        FileStream? lease = null;
        FileStream? owner = null;
        try
        {
            lease = new FileStream(lockPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete);
            lease.Write(contents, 0, contents.Length);
            lease.Flush(true);
            owner = new FileStream(ReceiptPath(databasePath), FileMode.CreateNew, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete);
            owner.Write(proof, 0, proof.Length);
            owner.Flush(true);
            boundary?.Invoke("prepared-before-directory-flush");
            _ = DirectoryPersistence.FlushParent(databasePath);
            boundary?.Invoke("prepared");
            return new RecoverableWriterLock(lease, owner);
        }
        catch
        {
            if (TryDeleteOwned(lease, boundary))
            {
                lease?.Dispose();
                if (TryFenceRetirement(lockPath) && TryDeleteOwned(owner))
                {
                    owner?.Dispose();
                    _ = TryFenceRetirement(ReceiptPath(databasePath));
                }
            }

            owner?.Dispose();
            lease?.Dispose();
            throw;
        }
    }

    /// <summary>Removes only an exclusively available lock with an exact matching generation receipt.</summary>
    /// <param name="databasePath">The exclusively owned database path.</param>
    /// <returns>Whether a receipt was present and successfully cleaned up.</returns>
    /// <exception cref="IOException">The receipt is invalid, the lock conflicts or a live holder prevents cleanup.</exception>
    /// <exception cref="JetLockException">The ownership receipt is invalid or does not match the existing lock.</exception>
    internal static bool Recover(string databasePath) => RecoverCore(databasePath, cleanup: true);

    /// <summary>Retains exclusive native ownership through replacement recovery and cleanup.</summary>
    /// <param name="databasePath">The exclusively coordinated database path.</param>
    /// <param name="boundary">An optional observer of fresh recovery ownership preparation.</param>
    /// <returns>The exact-generation recovery lease.</returns>
    /// <exception cref="JetLockException">Ownership is malformed, foreign or conflicting.</exception>
    internal static RecoveryLease AcquireRecovery(string databasePath, Action<string>? boundary = null)
    {
        RejectDirectories(databasePath);
        string lockPath = LockFileSlotWriter.GetLockFilePath(databasePath);
        string ownerPath = ReceiptPath(databasePath);
        FileStream? native = null;
        FileStream? owner = null;
        try
        {
            if (File.Exists(ownerPath))
            {
                owner = new FileStream(ownerPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                byte[] proof = ReadProof(databasePath, owner);
                if (File.Exists(lockPath))
                {
                    native = new FileStream(lockPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                    byte[] actual = new byte[LockLength];
                    if (native.Length != actual.Length)
                    {
                        throw Conflict("The native lock length conflicts with its ownership receipt.");
                    }

                    Read(native, actual);
                    if (!actual.AsSpan().SequenceEqual(proof.AsSpan(40, LockLength)))
                    {
                        throw Conflict("The native lock conflicts with its ownership receipt.");
                    }
                }
                else
                {
                    native = new FileStream(lockPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
                    native.Write(proof, 40, LockLength);
                    native.Flush(true);
                }
            }
            else
            {
                if (File.Exists(lockPath))
                {
                    throw Conflict("The replacement destination has an unowned native lock.");
                }

                byte[] proof = new byte[ReceiptLength];
                Magic.CopyTo(proof, 0);
                Identity(databasePath).CopyTo(proof, 8);
                WriteField(proof.AsSpan(40, 32), "JetDatabaseWriter");
                WriteField(proof.AsSpan(72, 32), "Recovery");
                WriteField(proof.AsSpan(104, 32), "JetDatabaseWriter");
                WriteField(proof.AsSpan(136, 32), Guid.NewGuid().ToString("N").AsSpan(0, 31));
                Hash(proof.AsSpan(0, ReceiptLength - 32)).CopyTo(proof, ReceiptLength - 32);
                native = new FileStream(lockPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
                native.Write(proof, 40, LockLength);
                native.Flush(true);
                boundary?.Invoke("recovery-lock-created");
                owner = new FileStream(ownerPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
                owner.Write(proof, 0, proof.Length);
                owner.Flush(true);
            }

            boundary?.Invoke("recovery-before-directory-flush");
            _ = DirectoryPersistence.FlushParent(databasePath);
            return new RecoveryLease(native, owner);
        }
        catch
        {
            // An uncertain creation retains unexplained evidence; never infer
            // ownership of a partial receipt after an interruption.
            owner?.Dispose();
            native?.Dispose();
            throw;
        }
    }

    private static bool RecoverCore(string databasePath, bool cleanup)
    {
        string ownerPath = ReceiptPath(databasePath);
        if (Directory.Exists(ownerPath))
        {
            throw Conflict("The writer ownership receipt cannot be a directory.");
        }

        if (!File.Exists(ownerPath))
        {
            // Without a receipt, ordinary recovery has no authority over the
            // native name. Strict acquisition validates it separately.
            return false;
        }

        RejectDirectories(databasePath);
        using (var owner = new FileStream(ownerPath, FileMode.Open, FileAccess.Read, FileShare.None))
        {
            byte[] proof = new byte[ReceiptLength];
            if (owner.Length != proof.Length)
            {
                throw Conflict("The writer ownership receipt length is invalid.");
            }

            Read(owner, proof);
            if (!proof.AsSpan(0, 8).SequenceEqual(Magic)
                || !proof.AsSpan(8, 32).SequenceEqual(Identity(databasePath))
                || !proof.AsSpan(ReceiptLength - 32).SequenceEqual(Hash(proof.AsSpan(0, ReceiptLength - 32))))
            {
                throw Conflict("The writer ownership receipt identity or checksum is invalid.");
            }

            string lockPath = LockFileSlotWriter.GetLockFilePath(databasePath);
            if (File.Exists(lockPath))
            {
                // FileShare.None requests an exclusive lease on Windows and Unix.
                // Keep the receipt and caller's database ownership while releasing
                // this lease before deletion, which Windows otherwise refuses.
                using (var abandoned = new FileStream(lockPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                {
                    byte[] actual = new byte[LockLength];
                    if (abandoned.Length != actual.Length)
                    {
                        throw Conflict("The existing lock does not belong to the recorded writer generation.");
                    }

                    Read(abandoned, actual);
                    if (!actual.AsSpan().SequenceEqual(proof.AsSpan(40, LockLength)))
                    {
                        throw Conflict("The existing lock does not belong to the recorded writer generation.");
                    }
                }

                if (cleanup)
                {
                    File.Delete(lockPath);
                }
            }

            if (cleanup)
            {
                // A previous attempt may already have removed the native name.
                // Acknowledge that absence while its receipt is still retained.
                _ = DirectoryPersistence.FlushParent(lockPath);
            }
        }

        if (cleanup)
        {
            File.Delete(ownerPath);
            _ = DirectoryPersistence.FlushParent(ownerPath);
        }

        return true;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (this.disposed)
        {
            return;
        }

        this.disposed = true;
        try
        {
            // Keep the receipt lease while native deletion is completed and
            // fenced. A cooperating successor cannot consume that live receipt.
            string nativePath = this.lockFile.Name;
            bool nativeDeleted = TryDeleteOwned(this.lockFile);
            this.lockFile.Dispose();
            if (nativeDeleted && TryFenceRetirement(nativePath))
            {
                string ownerPath = this.receipt.Name;
                bool ownerDeleted = TryDeleteOwned(this.receipt);
                this.receipt.Dispose();
                if (ownerDeleted)
                {
                    _ = TryFenceRetirement(ownerPath);
                }
            }
        }
        finally
        {
            this.receipt.Dispose();
            this.lockFile.Dispose();
        }
    }

    private static bool TryFenceRetirement(string path)
    {
        try
        {
            _ = DirectoryPersistence.FlushParent(path);
            return true;
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        {
            DiagnosticTrace.WriteLine($"[AccessWriter] Ownership retirement directory fence failed: {error.GetType().Name}.");
            return false;
        }
    }

    private static void RejectDirectories(string databasePath)
    {
        if (Directory.Exists(ReceiptPath(databasePath)) || Directory.Exists(LockFileSlotWriter.GetLockFilePath(databasePath)))
        {
            throw Conflict("Writer ownership evidence cannot be a directory.");
        }
    }

    private static byte[] ReadProof(string databasePath, FileStream owner)
    {
        byte[] proof = new byte[ReceiptLength];
        if (owner.Length != proof.Length)
        {
            throw Conflict("The writer ownership receipt length is invalid.");
        }

        Read(owner, proof);
        if (!proof.AsSpan(0, 8).SequenceEqual(Magic)
            || !proof.AsSpan(8, 32).SequenceEqual(Identity(databasePath))
            || !proof.AsSpan(ReceiptLength - 32).SequenceEqual(Hash(proof.AsSpan(0, ReceiptLength - 32))))
        {
            throw Conflict("The writer ownership receipt identity or checksum is invalid.");
        }

        return proof;
    }

    private static JetLockException Conflict(string message) => new(JetErrorCode.DatabaseInUse, message);

    private static bool TryDeleteOwned(FileStream? owned, Action<string>? boundary = null)
    {
        if (owned is null)
        {
            return false;
        }

        try
        {
            boundary?.Invoke("lock-cleanup");
            File.Delete(owned.Name);
            return true;
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        {
            DiagnosticTrace.WriteLine($"[AccessWriter] Retained ownership evidence after cleanup failure: {error.GetType().Name}.");
            return false;
        }
    }

    private static byte[] Identity(string path)
    {
        string fullPath = Path.GetFullPath(path);
        if (Path.DirectorySeparatorChar == '\\')
        {
            fullPath = fullPath.ToUpperInvariant();
        }

        return Hash(Encoding.UTF8.GetBytes(fullPath));
    }

    private static byte[] Hash(ReadOnlySpan<byte> bytes)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(bytes);
        return hash.GetHashAndReset();
    }

    private static void Read(FileStream stream, byte[] bytes)
    {
        int position = 0;
        while (position < bytes.Length)
        {
            int count = stream.Read(bytes, position, bytes.Length - position);
            if (count == 0)
            {
                throw Conflict("The writer ownership evidence is incomplete.");
            }

            position += count;
        }
    }

    private static void WriteField(Span<byte> field, ReadOnlySpan<char> value)
    {
        int length = Math.Min(field.Length - 1, value.Length);
        for (int index = 0; index < length; index++)
        {
            field[index] = value[index] <= 127 ? (byte)value[index] : (byte)'?';
        }
    }

    /// <summary>Retains exact native and receipt handles until coordinated cleanup.</summary>
    /// <param name="native">The exclusively held native lock.</param>
    /// <param name="owner">Its exclusively held exact-generation receipt.</param>
    internal sealed class RecoveryLease(FileStream native, FileStream owner) : IDisposable
    {
        /// <summary>Removes the validated generation while the caller retains database ownership.</summary>
        /// <param name="boundary">An optional observer after each ownership name is deleted.</param>
        internal void Cleanup(Action<string>? boundary = null)
        {
            string nativePath = native.Name;
            string ownerPath = owner.Name;
            native.Dispose();
            File.Delete(nativePath);
            boundary?.Invoke("cleanup-before-native-directory-flush");
            _ = DirectoryPersistence.FlushParent(nativePath);
            boundary?.Invoke("cleanup-native");
            owner.Dispose();
            File.Delete(ownerPath);
            boundary?.Invoke("cleanup-before-receipt-directory-flush");
            _ = DirectoryPersistence.FlushParent(ownerPath);
            boundary?.Invoke("cleanup-receipt");
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            owner.Dispose();
            native.Dispose();
        }
    }
}
