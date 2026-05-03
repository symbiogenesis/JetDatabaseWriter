namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// End-to-end concurrency coverage for <see cref="AccessReader"/>: multiple
/// independent reader instances opened concurrently against the same file,
/// and a single shared reader fanned-out across parallel table iterations.
/// Complements the lower-level <c>JetByteRangeLockTests</c> by exercising
/// the public read path end-to-end.
/// </summary>
public sealed class AccessReaderConcurrencyTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Fact]
    public async Task MultipleReaders_OpenedConcurrentlyOnSameFile_AllReadIdenticalRowCounts()
    {
        string path = TestDatabases.NorthwindTraders;
        if (!File.Exists(path))
        {
            return;
        }

        const int ReaderCount = 8;
        CancellationToken ct = TestContext.Current.CancellationToken;

        // Establish ground-truth row counts via the shared cached reader.
        AccessReader baseline = await db.GetReaderAsync(path, ct);
        List<string> tables = await baseline.ListTablesAsync(ct);
        Assert.NotEmpty(tables);

        var expected = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        foreach (string t in tables)
        {
            expected[t] = await baseline.GetRealRowCountAsync(t, ct);
        }

        // Open ReaderCount independent readers in parallel and have each one
        // count rows in every table. Any locking / shared-state bug in the
        // page cache, lock-file coordinator, or stream wrapper surfaces as a
        // mismatched row count, an exception, or a deadlock.
        var tasks = new Task<Dictionary<string, long>>[ReaderCount];
        for (int i = 0; i < ReaderCount; i++)
        {
            tasks[i] = Task.Run(
                async () =>
                {
                    await using var reader = await AccessReader.OpenAsync(
                        path,
                        new AccessReaderOptions { UseLockFile = false },
                        ct);

                    var counts = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
                    foreach (string t in tables)
                    {
                        long n = 0;
                        await foreach (object[] row in reader.Rows(t, cancellationToken: ct))
                        {
                            _ = row;
                            n++;
                        }

                        counts[t] = n;
                    }

                    return counts;
                },
                ct);
        }

        Dictionary<string, long>[] results = await Task.WhenAll(tasks);

        foreach (Dictionary<string, long> result in results)
        {
            foreach (KeyValuePair<string, long> kvp in expected)
            {
                Assert.True(
                    result.TryGetValue(kvp.Key, out long observed),
                    $"Reader missing table '{kvp.Key}'.");
                Assert.Equal(kvp.Value, observed);
            }
        }
    }

    [Fact]
    public async Task SingleReader_FannedOutAcrossParallelTableReads_ProducesConsistentRowCounts()
    {
        string path = TestDatabases.NorthwindTraders;
        if (!File.Exists(path))
        {
            return;
        }

        CancellationToken ct = TestContext.Current.CancellationToken;

        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions { UseLockFile = false },
            ct);

        List<string> tables = await reader.ListTablesAsync(ct);
        Assert.NotEmpty(tables);

        // Compute serial baseline row counts.
        var expected = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        foreach (string t in tables)
        {
            long n = 0;
            await foreach (object[] row in reader.Rows(t, cancellationToken: ct))
            {
                _ = row;
                n++;
            }

            expected[t] = n;
        }

        // Now fan all tables out to parallel tasks against the same reader.
        // The reader's page cache + I/O gate must serialise them safely.
        Task<KeyValuePair<string, long>>[] tasks = tables
            .Select(t => Task.Run(
                async () =>
                {
                    long n = 0;
                    await foreach (object[] row in reader.Rows(t, cancellationToken: ct))
                    {
                        _ = row;
                        n++;
                    }

                    return new KeyValuePair<string, long>(t, n);
                },
                ct))
            .ToArray();

        KeyValuePair<string, long>[] results = await Task.WhenAll(tasks);

        foreach (KeyValuePair<string, long> kvp in results)
        {
            Assert.Equal(expected[kvp.Key], kvp.Value);
        }
    }

    [Fact]
    public async Task SingleReader_ParallelDataTableReadsOnSameTable_AllReturnIdenticalRowCount()
    {
        string path = TestDatabases.NorthwindTraders;
        if (!File.Exists(path))
        {
            return;
        }

        CancellationToken ct = TestContext.Current.CancellationToken;

        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions { UseLockFile = false },
            ct);

        // Pick the largest table so the parallel reads have meaningful overlap.
        List<string> tables = await reader.ListTablesAsync(ct);
        string target = tables[0];
        long maxRows = 0;
        foreach (string t in tables)
        {
            long n = await reader.GetRealRowCountAsync(t, ct);
            if (n > maxRows)
            {
                maxRows = n;
                target = t;
            }
        }

        const int ReadCount = 6;
        Task<int>[] tasks = Enumerable.Range(0, ReadCount)
            .Select(_ => Task.Run(
                async () =>
                {
                    DataTable dt = (await reader.ReadDataTableAsync(target, cancellationToken: ct))!;
                    return dt.Rows.Count;
                },
                ct))
            .ToArray();

        int[] counts = await Task.WhenAll(tasks);
        Assert.All(counts, c => Assert.Equal(counts[0], c));
    }
}
