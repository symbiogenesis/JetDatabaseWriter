namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;

/// <summary>
/// xUnit class fixture that caches <see cref="AccessReader"/> instances by path.
/// Avoids re-opening the same database for every test method within a class.
/// Disposed automatically by xUnit when the test class completes.
/// </summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "xUnit IClassFixture<T> requires public accessibility")]
public sealed class DatabaseCache : IAsyncDisposable
{
    private static readonly AccessReaderOptions DefaultOptions = new() { UseLockFile = false };

    private readonly ConcurrentDictionary<string, Lazy<Task<byte[]>>> _fileCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, Lazy<Task<AccessReader>>> _readers = new(StringComparer.OrdinalIgnoreCase);

    public Task<byte[]> GetFileAsync(string path, CancellationToken cancellationToken = default) =>
        _fileCache.GetOrAdd(
            path,
            static (p, ct) => new Lazy<Task<byte[]>>(() => File.ReadAllBytesAsync(p, ct)),
            cancellationToken).Value;

    /// <summary>
    /// Returns a writable <see cref="MemoryStream"/> containing a copy of
    /// the file at <paramref name="path"/>. The cached bytes are never
    /// mutated — each call produces an independent stream positioned at 0.
    /// </summary>
    /// <returns>A <see cref="MemoryStream"/> containing the file's bytes, positioned at 0.</returns>
    public async ValueTask<MemoryStream> CopyToStreamAsync(string path, CancellationToken cancellationToken = default)
    {
        byte[] bytes = await GetFileAsync(path, cancellationToken);
        var ms = new MemoryStream(bytes.Length);
        ms.Write(bytes);
        ms.Position = 0;
        return ms;
    }

    public Task<AccessReader> GetReaderAsync(string path, AccessReaderOptions options, CancellationToken cancellationToken = default) =>
        _readers.GetOrAdd(
            path,
            static (p, state) => new Lazy<Task<AccessReader>>(() => AccessReader.OpenAsync(p, state.Options, state.Token).AsTask()),
            (Options: options, Token: cancellationToken)).Value;

    public Task<AccessReader> GetReaderAsync(string path, CancellationToken cancellationToken = default) =>
        GetReaderAsync(path, DefaultOptions, cancellationToken);

    public async ValueTask DisposeAsync()
    {
        List<Exception>? exceptions = null;

        foreach (var (key, lazy) in _readers)
        {
            if (!lazy.IsValueCreated)
            {
                exceptions ??= [];
                exceptions.Add(new InvalidOperationException("A reader was never created for path: " + key));
                continue;
            }

            try
            {
                var task = await lazy.Value;

                if (task is not null)
                {
                    await task.DisposeAsync();
                }
            }
#pragma warning disable CA1031 // Collect all failures so every reader is disposed
            catch (Exception ex)
#pragma warning restore CA1031
            {
                exceptions ??= [];
                exceptions.Add(ex);
            }
        }

        _readers.Clear();
        _fileCache.Clear();

        if (exceptions is { Count: > 0 })
        {
            throw new AggregateException("One or more readers failed to dispose.", exceptions);
        }
    }
}
