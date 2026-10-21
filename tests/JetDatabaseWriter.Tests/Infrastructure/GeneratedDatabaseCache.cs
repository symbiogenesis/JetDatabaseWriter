namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading.Tasks;

/// <summary>
/// Builds each generated database seed once per test class and gives every
/// caller an independent byte array or expandable stream containing its bytes.
/// </summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "xUnit IClassFixture<T> requires public accessibility")]
public sealed class GeneratedDatabaseCache
{
    private readonly ConcurrentDictionary<string, Lazy<Task<byte[]>>> seeds = new(StringComparer.Ordinal);

    /// <summary>Copies a generated seed into a private writable byte array.</summary>
    /// <param name="key">A key identifying the complete seed shape.</param>
    /// <param name="createSeed">Creates the seed bytes if this key has not been requested yet.</param>
    /// <returns>The caller's independent database image.</returns>
    public async ValueTask<byte[]> CopyToArrayAsync(string key, Func<Task<byte[]>> createSeed)
    {
        byte[] bytes = await this.GetSeedAsync(key, createSeed);
        return (byte[])bytes.Clone();
    }

    /// <summary>Copies a generated seed into a private writable stream positioned at zero.</summary>
    /// <param name="key">A key identifying the complete seed shape.</param>
    /// <param name="createSeed">Creates the seed bytes if this key has not been requested yet.</param>
    /// <returns>The caller's independent database stream.</returns>
    public async ValueTask<MemoryStream> CopyToStreamAsync(string key, Func<Task<byte[]>> createSeed)
    {
        byte[] bytes = await this.GetSeedAsync(key, createSeed);
        var stream = new MemoryStream(bytes.Length);
        stream.Write(bytes);
        stream.Position = 0;
        return stream;
    }

    private Task<byte[]> GetSeedAsync(string key, Func<Task<byte[]>> createSeed) =>
        this.seeds.GetOrAdd(key, static (_, factory) => new Lazy<Task<byte[]>>(factory), createSeed).Value;
}
