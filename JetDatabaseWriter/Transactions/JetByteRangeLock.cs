namespace JetDatabaseWriter.Transactions;

using System;
using System.Diagnostics;
using System.IO;
#if NETSTANDARD2_1
using System.Runtime.InteropServices;
#else
using System.Runtime.Versioning;
#endif
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Cooperative byte-range locking against the database file using the JET
/// page-lock protocol.
/// </summary>
/// <remarks>
/// <para>
/// JET overlays a logical lock map onto the database file. Writers acquire an
/// exclusive page-sized range at <c>pageNumber * pageSize</c> for the duration
/// of a page mutation. Other openers that follow the same protocol see the lock
/// and block (or, here, time out). The locks are advisory: they only matter
/// against cooperating openers.
/// </para>
/// <para>
/// The implementation uses the managed <see cref="FileStream.Lock(long, long)"/>
/// and <see cref="FileStream.Unlock(long, long)"/> APIs. The runtime maps those
/// calls to the platform's native byte-range lock where supported, including
/// Windows, Linux, and Android. On platforms where the BCL marks range locking
/// unsupported, and when the underlying <see cref="Stream"/> is not a
/// <see cref="FileStream"/> (e.g. <see cref="MemoryStream"/> for in-memory ACCDB
/// rewrap), every public method on this type is a no-op and returns a sentinel
/// disposable.
/// </para>
/// <para>
/// Acquisition uses a poll loop: try to take the lock, sleep
/// <see cref="PollIntervalMilliseconds"/>, retry until the configured timeout
/// elapses. This keeps the implementation portable to the synchronous and async
/// call sites in <c>AccessBase</c>.
/// </para>
/// </remarks>
internal sealed class JetByteRangeLock
{
    /// <summary>How often the acquisition poll loop retries the lock.</summary>
    internal const int PollIntervalMilliseconds = 20;

    private readonly FileStream? fileStream;
    private readonly int lockTimeoutMs;

    private JetByteRangeLock(FileStream? fileStream, bool enabled, int lockTimeoutMs)
    {
        this.fileStream = fileStream;
        IsEnabled = enabled;
        this.lockTimeoutMs = lockTimeoutMs;
    }

    /// <summary>
    /// Gets a value indicating whether byte-range locking is active. False on
    /// unsupported hosts, when the backing <see cref="Stream"/> is not a
    /// <see cref="FileStream"/>, or when the caller opted out via options.
    /// </summary>
    public bool IsEnabled { get; }

    /// <summary>
    /// Creates a <see cref="JetByteRangeLock"/> bound to the supplied database stream.
    /// Returns an inert (disabled) instance when <paramref name="enabled"/> is false,
    /// byte-range locks are not supported by the host OS, or
    /// <paramref name="stream"/> is not backed by a file.
    /// </summary>
    /// <param name="stream">The database file stream.</param>
    /// <param name="enabled">Caller's opt-in flag from options.</param>
    /// <param name="lockTimeoutMilliseconds">Maximum milliseconds to wait for a contended lock.</param>
    public static JetByteRangeLock Create(Stream stream, bool enabled, int lockTimeoutMilliseconds)
    {
        if (!enabled || !PlatformSupportsByteRangeLocks() || stream is not FileStream fileStream)
        {
            return new JetByteRangeLock(fileStream: null, enabled: false, lockTimeoutMilliseconds);
        }

        return new JetByteRangeLock(fileStream, enabled: true, lockTimeoutMilliseconds);
    }

    /// <summary>
    /// Gets a shared inert instance whose acquire methods always return the no-op
    /// disposable. Used as the default for <see cref="AccessBase"/> before a derived
    /// reader/writer constructor has had a chance to bind real options, so callers can
    /// dispatch through a non-nullable field without per-call null checks.
    /// </summary>
    public static JetByteRangeLock Disabled { get; } = new(fileStream: null, enabled: false, lockTimeoutMs: 0);

    /// <summary>
    /// Acquires an exclusive byte-range lock on the database page at
    /// <paramref name="pageNumber"/>, blocking up to the configured timeout.
    /// Returns a disposable that releases the lock when disposed; on a disabled
    /// instance returns a no-op sentinel.
    /// </summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="pageSize">The page size.</param>
    /// <exception cref="IOException">Thrown if the lock cannot be acquired within the timeout.</exception>
    public IDisposable AcquirePageLock(long pageNumber, int pageSize)
    {
        if (!IsEnabled)
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
    /// <param name="pageNumber">The page number.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    public async ValueTask<IDisposable> AcquirePageLockAsync(long pageNumber, int pageSize, CancellationToken cancellationToken = default)
    {
        if (!IsEnabled)
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
    /// <see cref="AccessWriter.CommitTransactionAsync"/>.
    /// </summary>
    /// <param name="isAccdb">True when the target database is ACE (.accdb), which uses sentinel offset <c>0xFFFFFFFC</c>; otherwise <c>0xFFFFFFFE</c> (Jet3/Jet4).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The locked offset, or <see langword="null"/> when locking is disabled.</returns>
    public async ValueTask<long?> AcquireCommitLockOffsetAsync(bool isAccdb, CancellationToken cancellationToken = default)
    {
        if (!IsEnabled)
        {
            return null;
        }

        long offset = isAccdb ? 0xFFFFFFFCL : 0xFFFFFFFEL;
        await AcquireBlockingAsync(offset, length: 1, cancellationToken).ConfigureAwait(false);
        return offset;
    }

    /// <summary>Releases a commit-lock sentinel acquired by <see cref="AcquireCommitLockOffsetAsync"/>.</summary>
    /// <param name="offset">The offset.</param>
    public void ReleaseCommitLock(long? offset)
    {
        if (offset.HasValue && IsEnabled)
        {
            Release(offset.Value, length: 1);
        }
    }

#if NET5_0_OR_GREATER
    [SupportedOSPlatformGuard("windows")]
    [SupportedOSPlatformGuard("linux")]
    [SupportedOSPlatformGuard("android")]
    internal static bool PlatformSupportsByteRangeLocks() =>
           OperatingSystem.IsWindows()
        || OperatingSystem.IsLinux()
        || OperatingSystem.IsAndroid();
#else
    internal static bool PlatformSupportsByteRangeLocks() =>
           RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
        || RuntimeInformation.IsOSPlatform(OSPlatform.Linux)
        || RuntimeInformation.IsOSPlatform(OSPlatform.Create("ANDROID"));
#endif

    private void AcquireBlocking(long offset, long length)
    {
        if (TryAcquire(offset, length))
        {
            return;
        }

        var stopwatch = Stopwatch.StartNew();
        do
        {
            Task.Delay(PollIntervalMilliseconds).ConfigureAwait(false).GetAwaiter().GetResult();

            if (TryAcquire(offset, length))
            {
                return;
            }
        }
        while (stopwatch.ElapsedMilliseconds < lockTimeoutMs);

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
        while (stopwatch.ElapsedMilliseconds < lockTimeoutMs);

        ThrowTimeout(offset, length);
    }

    private bool TryAcquire(long offset, long length)
    {
        if (!PlatformSupportsByteRangeLocks())
        {
            return true;
        }

        try
        {
            fileStream!.Lock(offset, length);
            return true;
        }
        catch (IOException)
        {
            return false;
        }
    }

    private void Release(long offset, long length)
    {
        if (!IsEnabled || fileStream is null || !PlatformSupportsByteRangeLocks())
        {
            return;
        }

        try
        {
            fileStream.Unlock(offset, length);
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or PlatformNotSupportedException)
        {
            // Release failures are not actionable from a finally block; closing
            // the stream releases any outstanding native file locks.
        }
    }

    private void ThrowTimeout(long offset, long length)
    {
        long pageNumber = length > 0 ? offset / length : -1;
        throw new IOException(
            $"Timed out after {lockTimeoutMs} ms acquiring JET byte-range lock on page {pageNumber} (offset 0x{offset:X}). Another opener is holding the lock.");
    }

    private sealed class ReleaseToken(JetByteRangeLock owner, long offset, long length) : IDisposable
    {
        private bool released;

        public void Dispose()
        {
            if (released)
            {
                return;
            }

            released = true;

            owner.Release(offset, length);
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
