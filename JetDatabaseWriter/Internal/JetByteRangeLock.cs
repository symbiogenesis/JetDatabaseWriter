namespace JetDatabaseWriter.Internal;

using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;

/// <summary>
/// Cooperative byte-range locking against the database file using the JET
/// page-lock protocol that Microsoft Access, the OLE DB JET provider, and the
/// ACE engine all observe.
/// </summary>
/// <remarks>
/// <para>
/// JET overlays a logical lock map onto the database file via Win32
/// <c>LockFileEx</c>/<c>UnlockFileEx</c>. Writers acquire an exclusive
/// page-sized range at <c>pageNumber * pageSize</c> for the duration of a
/// page mutation. Other openers that follow the same protocol see the lock
/// and block (or, here, time out). The locks are advisory — they only matter
/// against cooperating openers — but every Microsoft tool that touches a JET
/// file does cooperate, so honouring the protocol is what closes the
/// coexistence gap with Access.
/// </para>
/// <para>
/// <c>LockFileEx</c> is Windows-only. On non-Windows platforms — and when
/// the underlying <see cref="Stream"/> is not a <see cref="FileStream"/>
/// (e.g. <see cref="MemoryStream"/> for in-memory ACCDB rewrap) — every
/// public method on this type is a no-op and returns a sentinel disposable.
/// </para>
/// <para>
/// Acquisition uses a poll loop with <c>LOCKFILE_FAIL_IMMEDIATELY</c>: try to
/// take the lock, sleep <see cref="PollIntervalMilliseconds"/>, retry until
/// the configured timeout elapses. This keeps the implementation portable to
/// the synchronous and async call sites in <c>AccessBase</c> without
/// depending on overlapped I/O completion.
/// </para>
/// </remarks>
internal sealed class JetByteRangeLock
{
    /// <summary>How often the acquisition poll loop retries the lock.</summary>
    internal const int PollIntervalMilliseconds = 20;

    private const int ErrorLockViolation = 33;
    private const int ErrorIoPending = 997;

    private readonly SafeFileHandle? _handle;
    private readonly int _lockTimeoutMs;
    private readonly bool _enabled;

    private JetByteRangeLock(SafeFileHandle? handle, bool enabled, int lockTimeoutMs)
    {
        _handle = handle;
        _enabled = enabled;
        _lockTimeoutMs = lockTimeoutMs;
    }

    /// <summary>
    /// Gets a value indicating whether byte-range locking is active. False on non-Windows
    /// hosts, when the backing <see cref="Stream"/> has no Win32 file handle, or when the
    /// caller opted out via options.
    /// </summary>
    public bool IsEnabled => _enabled;

    /// <summary>
    /// Creates a <see cref="JetByteRangeLock"/> bound to the supplied database stream.
    /// Returns an inert (disabled) instance when <paramref name="enabled"/> is false,
    /// the host OS is not Windows, or <paramref name="stream"/> is not a backed by a
    /// Win32 file handle.
    /// </summary>
    /// <param name="stream">The database file stream.</param>
    /// <param name="enabled">Caller's opt-in flag from options.</param>
    /// <param name="lockTimeoutMilliseconds">Maximum milliseconds to wait for a contended lock.</param>
    public static JetByteRangeLock Create(Stream stream, bool enabled, int lockTimeoutMilliseconds)
    {
        if (!enabled || !PlatformIsWindows() || stream is not FileStream fs)
        {
            return new JetByteRangeLock(handle: null, enabled: false, lockTimeoutMilliseconds);
        }

        SafeFileHandle handle = fs.SafeFileHandle;
        if (handle is null || handle.IsInvalid || handle.IsClosed)
        {
            return new JetByteRangeLock(handle: null, enabled: false, lockTimeoutMilliseconds);
        }

        return new JetByteRangeLock(handle, enabled: true, lockTimeoutMilliseconds);
    }

    /// <summary>
    /// Acquires an exclusive byte-range lock on the database page at
    /// <paramref name="pageNumber"/>, blocking up to the configured timeout.
    /// Returns a disposable that releases the lock when disposed; on a disabled
    /// instance returns a no-op sentinel.
    /// </summary>
    /// <exception cref="IOException">Thrown if the lock cannot be acquired within the timeout.</exception>
    public IDisposable AcquirePageLock(long pageNumber, int pageSize)
    {
        if (!_enabled)
        {
            return NoOpDisposable.Instance;
        }

        long offset = pageNumber * pageSize;
        AcquireBlocking(offset, pageSize);
        return new ReleaseToken(this, offset, pageSize);
    }

    /// <summary>
    /// Asynchronously acquires an exclusive byte-range lock on the database page at
    /// <paramref name="pageNumber"/>, polling up to the configured timeout.
    /// </summary>
    public async ValueTask<IDisposable> AcquirePageLockAsync(long pageNumber, int pageSize, CancellationToken cancellationToken = default)
    {
        if (!_enabled)
        {
            return NoOpDisposable.Instance;
        }

        long offset = pageNumber * pageSize;
        await AcquireBlockingAsync(offset, pageSize, cancellationToken).ConfigureAwait(false);
        return new ReleaseToken(this, offset, pageSize);
    }

    /// <summary>
    /// Acquires the JET commit-lock sentinel: a 1-byte exclusive lock at the
    /// fixed offset Microsoft Access / OLE DB JET / ACE all use to gate
    /// schema-changing transaction commits and increments of the page-0
    /// commit-lock byte (header offset <c>0x14</c>). Held only across the
    /// atomic-replay window inside
    /// <see cref="JetDatabaseWriter.Core.AccessWriter.CommitTransactionAsync"/>.
    /// </summary>
    /// <param name="isAccdb">True when the target database is ACE (.accdb), which uses sentinel offset <c>0xFFFFFFFC</c>; otherwise <c>0xFFFFFFFE</c> (Jet3/Jet4).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A disposable that releases the commit-lock sentinel; a no-op disposable on a disabled instance.</returns>
    public async ValueTask<IDisposable> AcquireCommitLockAsync(bool isAccdb, CancellationToken cancellationToken = default)
    {
        if (!_enabled)
        {
            return NoOpDisposable.Instance;
        }

        long offset = isAccdb ? 0xFFFFFFFCL : 0xFFFFFFFEL;
        await AcquireBlockingAsync(offset, length: 1, cancellationToken).ConfigureAwait(false);
        return new ReleaseToken(this, offset, length: 1);
    }

    private static bool PlatformIsWindows() => RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

    [DllImport("kernel32.dll", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool LockFile(
        SafeFileHandle hFile,
        uint dwFileOffsetLow,
        uint dwFileOffsetHigh,
        uint nNumberOfBytesToLockLow,
        uint nNumberOfBytesToLockHigh);

    [DllImport("kernel32.dll", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool UnlockFile(
        SafeFileHandle hFile,
        uint dwFileOffsetLow,
        uint dwFileOffsetHigh,
        uint nNumberOfBytesToUnlockLow,
        uint nNumberOfBytesToUnlockHigh);

    private void AcquireBlocking(long offset, long length)
    {
        if (TryAcquire(offset, length))
        {
            return;
        }

        var stopwatch = Stopwatch.StartNew();
        do
        {
            Thread.Sleep(PollIntervalMilliseconds);
            if (TryAcquire(offset, length))
            {
                return;
            }
        }
        while (stopwatch.ElapsedMilliseconds < _lockTimeoutMs);

        ThrowTimeout(offset, length);
    }

    private async ValueTask AcquireBlockingAsync(long offset, long length, CancellationToken cancellationToken)
    {
        if (TryAcquire(offset, length))
        {
            return;
        }

        var stopwatch = Stopwatch.StartNew();
        do
        {
            await Task.Delay(PollIntervalMilliseconds, cancellationToken).ConfigureAwait(false);
            if (TryAcquire(offset, length))
            {
                return;
            }
        }
        while (stopwatch.ElapsedMilliseconds < _lockTimeoutMs);

        ThrowTimeout(offset, length);
    }

    private bool TryAcquire(long offset, long length)
    {
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return true;
        }

        uint offsetLow = unchecked((uint)(offset & 0xFFFFFFFFL));
        uint offsetHigh = unchecked((uint)(offset >> 32));
        uint lengthLow = unchecked((uint)(length & 0xFFFFFFFFL));
        uint lengthHigh = unchecked((uint)(length >> 32));

        bool ok = LockFile(_handle!, offsetLow, offsetHigh, lengthLow, lengthHigh);
        if (ok)
        {
            return true;
        }

        int err = Marshal.GetLastWin32Error();
        if (err == ErrorLockViolation || err == ErrorIoPending)
        {
            return false;
        }

        throw new IOException(
            $"LockFile failed at offset 0x{offset:X} length {length} (Win32 error {err}).");
    }

    private void Release(long offset, long length)
    {
        if (!_enabled || _handle is null || _handle.IsInvalid || _handle.IsClosed)
        {
            return;
        }

        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return;
        }

        uint offsetLow = unchecked((uint)(offset & 0xFFFFFFFFL));
        uint offsetHigh = unchecked((uint)(offset >> 32));
        uint lengthLow = unchecked((uint)(length & 0xFFFFFFFFL));
        uint lengthHigh = unchecked((uint)(length >> 32));

        // Release failures are not actionable from a finally block; the handle
        // will release any outstanding locks when it is closed.
        _ = UnlockFile(_handle, offsetLow, offsetHigh, lengthLow, lengthHigh);
    }

    private void ThrowTimeout(long offset, long length)
    {
        long pageNumber = length > 0 ? offset / length : -1;
        throw new IOException(
            $"Timed out after {_lockTimeoutMs} ms acquiring JET byte-range lock on page {pageNumber} (offset 0x{offset:X}). Another opener is holding the lock.");
    }

    private sealed class ReleaseToken : IDisposable
    {
        private readonly JetByteRangeLock _owner;
        private readonly long _offset;
        private readonly long _length;
        private bool _released;

        public ReleaseToken(JetByteRangeLock owner, long offset, long length)
        {
            _owner = owner;
            _offset = offset;
            _length = length;
        }

        public void Dispose()
        {
            if (_released)
            {
                return;
            }

            _released = true;

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                _owner.Release(_offset, _length);
            }
        }
    }

    private sealed class NoOpDisposable : IDisposable
    {
        public static readonly NoOpDisposable Instance = new();

        public void Dispose()
        {
        }
    }
}
