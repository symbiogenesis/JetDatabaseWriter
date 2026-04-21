namespace JetDatabaseReader.Tests;

using System;
using System.Collections.Concurrent;

/// <summary>
/// xUnit class fixture that caches <see cref="AccessReader"/> instances by path.
/// Avoids re-opening the same database for every test method within a class.
/// Disposed automatically by xUnit when the test class completes.
/// </summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "xUnit IClassFixture<T> requires public accessibility")]
public sealed class DatabaseCache : IDisposable
{
    private readonly ConcurrentDictionary<string, AccessReader> _readers = new(StringComparer.OrdinalIgnoreCase);

    public AccessReader Get(string path) =>
        _readers.GetOrAdd(path, static p => AccessReader.OpenAsync(p, new AccessReaderOptions { UseLockFile = false }).AsTask().GetAwaiter().GetResult());

    public void Dispose()
    {
        foreach (var reader in _readers.Values)
        {
            reader.Dispose();
        }

        _readers.Clear();
    }
}
