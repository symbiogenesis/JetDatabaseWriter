namespace JetDatabaseWriter.Internal;

using System;
using System.IO;

/// <summary>
/// Owns the JET lock-file companion (.ldb / .laccdb) for the lifetime of an open
/// <see cref="JetDatabaseWriter.Core.AccessReader"/> or <see cref="JetDatabaseWriter.Core.AccessWriter"/> connection.
/// </summary>
/// <remarks>
/// <para>
/// JET / Microsoft Access tracks concurrent openers of a database by appending a fixed-size
/// <c>64</c>-byte slot record to a sibling lock-file (<c>.ldb</c> for <c>.mdb</c> /
/// <c>.mde</c>, <c>.laccdb</c> for <c>.accdb</c> / <c>.accde</c>). Each slot stores the
/// opener's machine name (32 ASCII bytes) followed by user / security name (32 ASCII bytes),
/// both null-padded. The file has no header and no slot count; openers scan from the
/// beginning, claim the first slot whose machine-name byte is <c>0</c>, and overwrite it
/// in place. Access caps the array at 255 slots; exceeding the cap surfaces the familiar
/// "you have reached your limit of concurrent users" error.
/// </para>
/// <para>
/// This type populates a slot on <see cref="Open"/> and zeroes it on <see cref="Dispose"/>,
/// then makes a best-effort attempt to delete the file (the last opener wins; concurrent
/// openers tolerate the resulting <see cref="IOException"/>). The <see cref="FileStream"/>
/// is held open for the lifetime of the connection so that other processes (Microsoft Access,
/// the OLE DB JET / ACE provider, <c>LDBView</c>) detect us as an active opener even if a
/// concurrent dispose races us to delete the file.
/// </para>
/// </remarks>
internal sealed class LockFileSlotWriter : IDisposable
{
    /// <summary>Size in bytes of a single slot record.</summary>
    public const int SlotSize = 64;

    /// <summary>Length of the machine-name field within a slot.</summary>
    public const int MachineNameFieldLength = 32;

    /// <summary>Length of the user-name field within a slot.</summary>
    public const int UserNameFieldLength = 32;

    /// <summary>Maximum number of openers JET / Access supports per database.</summary>
    public const int MaxSlots = 255;

    private readonly string _lockPath;
    private readonly string _ownerType;
    private FileStream? _stream;
    private long _slotOffset = -1;
    private bool _disposed;

    private LockFileSlotWriter(string lockPath, string ownerType, FileStream stream, long slotOffset)
    {
        _lockPath = lockPath;
        _ownerType = ownerType;
        _stream = stream;
        _slotOffset = slotOffset;
    }

    /// <summary>Gets the path of the lock-file companion this instance owns.</summary>
    public string LockFilePath => _lockPath;

    /// <summary>Gets the byte offset of the slot this instance claimed within the lock-file.</summary>
    internal long SlotOffset => _slotOffset;

    /// <summary>
    /// Opens (or creates) the lock-file companion of <paramref name="databasePath"/>, claims
    /// the first empty 64-byte slot, and writes the supplied machine / user identity into it.
    /// </summary>
    /// <param name="databasePath">Path to the Access database (.mdb / .accdb).</param>
    /// <param name="ownerTypeName">Type name of the caller, used in trace messages.</param>
    /// <param name="respectExisting">
    /// When <see langword="true"/>, throws <see cref="IOException"/> if the lock-file is
    /// already present (matching Access's "fail fast on in-use" behaviour). When
    /// <see langword="false"/>, the lock-file is created or appended-to in best-effort mode
    /// and any I/O / permission failures are swallowed.
    /// </param>
    /// <param name="machineName">
    /// Machine name to record in the slot. <see langword="null"/> uses
    /// <see cref="Environment.MachineName"/>.
    /// </param>
    /// <param name="userName">
    /// User name to record in the slot. <see langword="null"/> uses
    /// <see cref="Environment.UserName"/>.
    /// </param>
    /// <returns>
    /// A <see cref="LockFileSlotWriter"/> that owns the slot and the underlying file handle,
    /// or <see langword="null"/> when creation failed in best-effort mode.
    /// </returns>
    /// <exception cref="IOException">
    /// Thrown when <paramref name="respectExisting"/> is <see langword="true"/> and the
    /// lock-file already exists, or when all <see cref="MaxSlots"/> slots are populated.
    /// </exception>
    public static LockFileSlotWriter? Open(
        string databasePath,
        string ownerTypeName,
        bool respectExisting,
        string? machineName = null,
        string? userName = null)
    {
        string lockPath = LockFileManager.GetLockFilePath(databasePath);

        if (respectExisting && File.Exists(lockPath))
        {
            throw new IOException($"Database is already in use. A lockfile exists at: {lockPath}");
        }

        FileStream? stream = null;
        try
        {
            // FileShare.ReadWrite | FileShare.Delete mirrors Access — every opener can
            // append a slot concurrently and the last closer can delete the file even
            // while we still hold a handle.
            stream = new FileStream(
                lockPath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.ReadWrite | FileShare.Delete);

            byte[] slot = BuildSlotRecord(machineName, userName);
            long slotOffset = FindAndClaimSlot(stream, slot);

            return new LockFileSlotWriter(lockPath, ownerTypeName, stream, slotOffset);
        }
        catch (IOException ex)
        {
            stream?.Dispose();
            if (respectExisting)
            {
                throw;
            }

            System.Diagnostics.Trace.WriteLine($"[{ownerTypeName}] Best-effort lock-file suppression in LockFileSlotWriter.Open: '{lockPath}' ({ex.GetType().Name}: {ex.Message})");
            return null;
        }
        catch (UnauthorizedAccessException ex)
        {
            stream?.Dispose();
            if (respectExisting)
            {
                throw;
            }

            System.Diagnostics.Trace.WriteLine($"[{ownerTypeName}] Best-effort lock-file suppression in LockFileSlotWriter.Open: '{lockPath}' ({ex.GetType().Name}: {ex.Message})");
            return null;
        }
    }

    /// <summary>
    /// Zeroes this instance's slot and releases the underlying file handle. Makes a
    /// best-effort attempt to delete the lock-file (the last opener wins; concurrent
    /// openers see the resulting <see cref="IOException"/> and tolerate it).
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        FileStream? stream = _stream;
        _stream = null;

        bool fileLooksEmpty = false;

        if (stream != null)
        {
            try
            {
                if (_slotOffset >= 0 && stream.Length >= _slotOffset + SlotSize)
                {
                    stream.Seek(_slotOffset, SeekOrigin.Begin);
                    Span<byte> zeros = stackalloc byte[SlotSize];
                    stream.Write(zeros);
                    stream.Flush();
                }

                fileLooksEmpty = ScanForLiveSlots(stream) == false;
            }
            catch (IOException ex)
            {
                System.Diagnostics.Trace.WriteLine($"[{_ownerType}] Best-effort lock-file slot zero suppression: '{_lockPath}' ({ex.GetType().Name}: {ex.Message})");
            }
            catch (UnauthorizedAccessException ex)
            {
                System.Diagnostics.Trace.WriteLine($"[{_ownerType}] Best-effort lock-file slot zero suppression: '{_lockPath}' ({ex.GetType().Name}: {ex.Message})");
            }
            catch (ObjectDisposedException)
            {
                // Stream was already disposed; nothing to do.
            }
            finally
            {
                stream.Dispose();
            }
        }

        // Only the last opener (no remaining live slots) attempts to delete the file.
        // Calling File.Delete while another opener still holds a handle marks the
        // file as "delete pending" on Windows and blocks subsequent opens, so we
        // intentionally leave the file alone when other slots are still populated.
        if (!fileLooksEmpty)
        {
            return;
        }

        try
        {
            File.Delete(_lockPath);
        }
        catch (IOException ex)
        {
            System.Diagnostics.Trace.WriteLine($"[{_ownerType}] Best-effort lock-file delete suppression: '{_lockPath}' ({ex.GetType().Name}: {ex.Message})");
        }
        catch (UnauthorizedAccessException ex)
        {
            System.Diagnostics.Trace.WriteLine($"[{_ownerType}] Best-effort lock-file delete suppression: '{_lockPath}' ({ex.GetType().Name}: {ex.Message})");
        }
    }

    private static bool ScanForLiveSlots(FileStream stream)
    {
        long length = stream.Length;
        Span<byte> probe = stackalloc byte[1];
        int slots = (int)Math.Min(MaxSlots, length / SlotSize);

        for (int i = 0; i < slots; i++)
        {
            stream.Seek((long)i * SlotSize, SeekOrigin.Begin);
            int read = stream.Read(probe);
            if (read == 1 && probe[0] != 0)
            {
                return true;
            }
        }

        return false;
    }

    private static long FindAndClaimSlot(FileStream stream, ReadOnlySpan<byte> slotBytes)
    {
        long length = stream.Length;
        Span<byte> probe = stackalloc byte[1];

        for (int i = 0; i < MaxSlots; i++)
        {
            long offset = (long)i * SlotSize;
            bool isEmpty;

            if (offset >= length)
            {
                isEmpty = true;
            }
            else
            {
                stream.Seek(offset, SeekOrigin.Begin);
                int read = stream.Read(probe);
                isEmpty = read == 0 || probe[0] == 0;
            }

            if (isEmpty)
            {
                stream.Seek(offset, SeekOrigin.Begin);
                stream.Write(slotBytes);
                stream.Flush();
                return offset;
            }
        }

        throw new IOException(
            $"Cannot open the database. The database has been opened by another user, or you have reached your limit of concurrent users (max {MaxSlots} slots in lock-file).");
    }

    private static byte[] BuildSlotRecord(string? machineName, string? userName)
    {
        byte[] record = new byte[SlotSize];
        WriteAsciiField(record.AsSpan(0, MachineNameFieldLength), machineName ?? Environment.MachineName);
        WriteAsciiField(record.AsSpan(MachineNameFieldLength, UserNameFieldLength), userName ?? Environment.UserName);
        return record;
    }

    private static void WriteAsciiField(Span<byte> field, string value)
    {
        // Reserve one byte for the trailing null so the field is always null-terminated.
        int max = field.Length - 1;
        int copyChars = Math.Min(value.Length, max);

        for (int i = 0; i < copyChars; i++)
        {
            char c = value[i];

            // ASCII subset only — match Access's behaviour for slot text and avoid
            // pulling in OEM code-page resolution.
            field[i] = (c >= 0x20 && c < 0x7F) ? (byte)c : (byte)'?';
        }

        // Zero-pad the rest (already zero from the array initialiser, but be explicit
        // so an in-place rewrite of an existing slot also clears stale bytes).
        field[copyChars..].Clear();
    }
}
