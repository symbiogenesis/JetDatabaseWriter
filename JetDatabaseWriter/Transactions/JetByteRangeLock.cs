namespace JetDatabaseWriter.Transactions;

using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
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
/// Acquisition runs a single asynchronous exponential-backoff poll loop
/// (<see cref="AcquireBlockingAsync"/>): try to take the lock, then await a
/// growing delay — starting at <see cref="InitialPollIntervalMilliseconds"/> and
/// capped at <see cref="MaxPollIntervalMilliseconds"/> — before retrying, until
/// the configured timeout elapses. The backoff schedule and timeout accounting
/// live in <see cref="PollBackoff"/>. The synchronous entry point
/// (<see cref="AcquirePageLock"/>) bridges onto that same async primitive once at
/// the call boundary instead of maintaining a duplicate blocking loop.
/// </para>
/// </remarks>
internal sealed class JetByteRangeLock
{
    /// <summary>Initial backoff before the first lock retry, in milliseconds.</summary>
    private const int InitialPollIntervalMilliseconds = 2;

    /// <summary>Maximum backoff between successive lock retries, in milliseconds.</summary>
    private const int MaxPollIntervalMilliseconds = 64;

    private readonly FileStream? fileStream;
    private readonly int lockTimeoutMs;

    private JetByteRangeLock(FileStream? fileStream, bool enabled, int lockTimeoutMs)
    {
        this.fileStream = fileStream;
        this.IsEnabled = enabled;
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
        lockTimeoutMilliseconds = AccessOptions.ValidateLockTimeoutMilliseconds(lockTimeoutMilliseconds);

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
        if (!this.IsEnabled)
        {
            return NoOpDisposable.Instance;
        }

        long offset = pageNumber * pageSize;

        // Bridge this synchronous entry point onto the single async acquisition
        // primitive exactly once, at the call boundary. AcquireBlockingAsync uses
        // ConfigureAwait(false) throughout, so blocking here cannot deadlock on a
        // captured synchronization context.
        this.AcquireBlockingAsync(offset, pageSize, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        return new ReleaseToken(this, offset, pageSize);
    }

    /// <summary>
    /// Asynchronously acquires an exclusive byte-range lock on the database page at
    /// <paramref name="pageNumber"/>, polling up to the configured timeout.
    /// </summary>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public async ValueTask<IDisposable> AcquirePageLockAsync(long pageNumber, int pageSize, CancellationToken cancellationToken = default)
    {
        if (!this.IsEnabled)
        {
            return NoOpDisposable.Instance;
        }

        long offset = pageNumber * pageSize;
        await this.AcquireBlockingAsync(offset, pageSize, cancellationToken).ConfigureAwait(false);
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
        if (!this.IsEnabled)
        {
            return null;
        }

        long offset = isAccdb ? 0xFFFFFFFCL : 0xFFFFFFFEL;
        await this.AcquireBlockingAsync(offset, length: 1, cancellationToken).ConfigureAwait(false);
        return offset;
    }

    /// <summary>Releases a commit-lock sentinel acquired by <see cref="AcquireCommitLockOffsetAsync"/>.</summary>
    /// <param name="offset">The offset.</param>
    public void ReleaseCommitLock(long? offset)
    {
        if (offset.HasValue && this.IsEnabled)
        {
            this.Release(offset.Value, length: 1);
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

    private async ValueTask AcquireBlockingAsync(long offset, long length, CancellationToken cancellationToken)
    {
        var backoff = new PollBackoff(this.lockTimeoutMs);
        while (true)
        {
            if (this.TryAcquire(offset, length))
            {
                return;
            }

            int delayMilliseconds = backoff.NextDelayMilliseconds();
            if (delayMilliseconds < 0)
            {
                this.ThrowTimeout(offset, length);
            }

            await Task.Delay(delayMilliseconds, cancellationToken).ConfigureAwait(false);
        }
    }

    private bool TryAcquire(long offset, long length)
    {
        if (!PlatformSupportsByteRangeLocks())
        {
            return true;
        }

        try
        {
            this.fileStream!.Lock(offset, length);
            return true;
        }
        catch (IOException)
        {
            return false;
        }
    }

    private void Release(long offset, long length)
    {
        if (!this.IsEnabled || this.fileStream is null || !PlatformSupportsByteRangeLocks())
        {
            return;
        }

        try
        {
            this.fileStream.Unlock(offset, length);
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or PlatformNotSupportedException)
        {
            // Release failures are not actionable from a finally block; closing
            // the stream releases any outstanding native file locks.
        }
    }

    [DoesNotReturn]
    private void ThrowTimeout(long offset, long length)
    {
        long pageNumber = length > 0 ? offset / length : -1;
        throw new IOException(
            $"Timed out after {this.lockTimeoutMs} ms acquiring JET byte-range lock on page {pageNumber} (offset 0x{offset:X}). Another opener is holding the lock.");
    }

    /// <summary>
    /// Exponential-backoff schedule shared by the synchronous and asynchronous
    /// acquisition loops. Derives a deadline from the configured timeout and
    /// yields capped, deadline-clamped retry delays.
    /// </summary>
    /// <param name="timeoutMilliseconds">Maximum milliseconds to keep polling before the schedule reports a timeout.</param>
    private struct PollBackoff(int timeoutMilliseconds)
    {
        private readonly long deadlineTimestamp = Stopwatch.GetTimestamp() + (Stopwatch.Frequency * timeoutMilliseconds / 1000L);
        private int delayMilliseconds = InitialPollIntervalMilliseconds;

        /// <summary>
        /// Returns the next retry delay in milliseconds — growing exponentially up
        /// to <see cref="MaxPollIntervalMilliseconds"/> and clamped so the wait
        /// never runs past the deadline — or <c>-1</c> once the timeout has elapsed
        /// and the caller must stop polling.
        /// </summary>
        public int NextDelayMilliseconds()
        {
            long remainingTicks = this.deadlineTimestamp - Stopwatch.GetTimestamp();
            if (remainingTicks <= 0)
            {
                return -1;
            }

            int delay = this.delayMilliseconds;
            this.delayMilliseconds = Math.Min(MaxPollIntervalMilliseconds, this.delayMilliseconds * 2);

            // Only the small (<= cap) delay is converted to ticks, so the clamp
            // stays overflow-safe even for very large timeouts under checked
            // arithmetic.
            long delayTicks = Stopwatch.Frequency * delay / 1000L;
            if (delayTicks <= remainingTicks)
            {
                return delay;
            }

            long clampedMilliseconds = remainingTicks * 1000L / Stopwatch.Frequency;
            return clampedMilliseconds <= 0 ? 1 : (int)clampedMilliseconds;
        }
    }

    private sealed class ReleaseToken(JetByteRangeLock owner, long offset, long length) : IDisposable
    {
        private bool released;

        public void Dispose()
        {
            if (this.released)
            {
                return;
            }

            this.released = true;

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
