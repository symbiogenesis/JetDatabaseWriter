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
    private readonly ConcurrentDictionary<string, Lazy<ValueTask<AccessReader>>> _readers = new(StringComparer.OrdinalIgnoreCase);

    public Task<byte[]> GetFileAsync(string path, CancellationToken cancellationToken = default) =>
        _fileCache.GetOrAdd(path, (p) => new Lazy<Task<byte[]>>(
            () => File.ReadAllBytesAsync(p, cancellationToken))).Value;

    public ValueTask<AccessReader> GetReaderAsync(string path, AccessReaderOptions options, CancellationToken cancellationToken = default) =>
        _readers.GetOrAdd(path, (p) => new Lazy<ValueTask<AccessReader>>(
            () => AccessReader.OpenAsync(p, options, cancellationToken))).Value;

    public ValueTask<AccessReader> GetReaderAsync(string path, CancellationToken cancellationToken = default) =>
        GetReaderAsync(path, DefaultOptions, cancellationToken);

    public async ValueTask DisposeAsync()
    {
        List<Exception>? exceptions = null;

        foreach (var (key, lazy) in _readers)
        {
            if (!lazy.IsValueCreated)
            {
                throw new InvalidOperationException("A reader was never created for path: " + key);
            }

            try
            {
                var valueTask = lazy.Value;

                if (valueTask.IsCompletedSuccessfully)
                {
                    await valueTask.Result.DisposeAsync();
                }
                else if (!valueTask.IsFaulted && !valueTask.IsCanceled)
                {
                    var reader = await valueTask;
                    await reader.DisposeAsync();
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

        if (exceptions is { Count: > 0 })
        {
            throw new AggregateException("One or more readers failed to dispose.", exceptions);
        }
    }
}
