namespace JetDatabaseWriter.Infrastructure;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;

internal static class FileStreamFactory
{
    private const int DefaultBufferSize = 4096;

    /// <summary>
    /// The paths this factory opened on the current asynchronous flow while
    /// an <see cref="OpenTracker"/> is active; <see langword="null"/> otherwise.
    /// </summary>
    private static readonly AsyncLocal<ConcurrentQueue<string>?> OpenLog = new();

    public static FileStream Open(
        string path,
        FileMode mode,
        FileAccess access,
        FileShare share,
        FileOptions options = FileOptions.None,
        long preallocationSize = 0)
    {
        OpenLog.Value?.Enqueue(path);
#if NET6_0_OR_GREATER
        return new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = mode,
                Access = access,
                Share = share,
                Options = options,
                BufferSize = DefaultBufferSize,
                PreallocationSize = preallocationSize,
            });
#else
        _ = preallocationSize;
        return new FileStream(path, mode, access, share, DefaultBufferSize, options);
#endif
    }

    /// <summary>
    /// Starts recording the path of every file this factory opens on the
    /// current asynchronous flow, and on the flows it starts, until the
    /// returned tracker is disposed. A test seam: it lets a test count how
    /// often an operation opens a file without seeing other tests' opens.
    /// </summary>
    /// <returns>The tracker holding the opened paths.</returns>
    internal static OpenTracker TrackOpens()
    {
        var log = new ConcurrentQueue<string>();
        OpenLog.Value = log;
        return new OpenTracker(log);
    }

    /// <summary>The file opens recorded since <see cref="TrackOpens"/>.</summary>
    /// <param name="log">The queue the factory appends opened paths to.</param>
    internal sealed class OpenTracker(ConcurrentQueue<string> log) : IDisposable
    {
        /// <summary>Gets the opened paths, in open order.</summary>
        internal IReadOnlyList<string> OpenedPaths => log.ToArray();

        /// <summary>Stops recording on the current asynchronous flow.</summary>
        public void Dispose() => OpenLog.Value = null;
    }
}
