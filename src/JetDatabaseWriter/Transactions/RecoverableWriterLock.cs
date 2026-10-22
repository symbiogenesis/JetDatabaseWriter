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
        if (File.Exists(lockPath))
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
            boundary?.Invoke("prepared");
            return new RecoverableWriterLock(lease, owner);
        }
        catch
        {
            if (TryDeleteOwned(lease, boundary))
            {
                _ = TryDeleteOwned(owner);
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
    internal static bool Recover(string databasePath)
    {
        string ownerPath = ReceiptPath(databasePath);
        if (!File.Exists(ownerPath))
        {
            return false;
        }

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

                File.Delete(lockPath);
            }
        }

        File.Delete(ownerPath);
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
            // Keep both handles until both names are removed: a concurrent writer may
            // already own the database once its previous physical store has closed.
            if (TryDeleteOwned(this.lockFile))
            {
                _ = TryDeleteOwned(this.receipt);
            }
        }
        finally
        {
            this.receipt.Dispose();
            this.lockFile.Dispose();
        }
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
}
