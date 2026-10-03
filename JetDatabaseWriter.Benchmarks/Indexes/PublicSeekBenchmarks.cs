namespace JetDatabaseWriter.Benchmarks.Indexes;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Benchmarks.Infrastructure;
using JetDatabaseWriter.Benchmarks.Models;

/// <summary>
/// The public index-seek APIs over the 10,000-row synthetic <c>Orders</c> table:
/// 1,000 primary-key lookups through <c>SeekRowsAsync</c>,
/// <c>FromIndex(...).WhereEquals</c> and a <c>Rows&lt;T&gt;(predicate)</c> whose
/// predicate the reader turns into a seek, a 250-row <c>WhereBetween</c> range on
/// the <c>OrderDate</c> index, and the client-side alternative: one full
/// <c>Rows&lt;T&gt;()</c> scan that keeps the same 1,000 keys. The keys come from a
/// fixed-seed <see cref="Random"/>. Setup runs each benchmark once and throws
/// unless it returns the expected row count.
/// </summary>
[MemoryDiagnoser]
public class PublicSeekBenchmarks
{
    private const int KeyCount = 1_000;
    private const int RangeRows = 250;
    private const int RangeStart = 5_000;

    private readonly int[] keys = new int[KeyCount];
    private readonly HashSet<int> keySet = [];
    private AccessReader? reader;

    private AccessReader Reader => this.reader ?? throw new InvalidOperationException("The reader was not initialized.");

    [GlobalSetup]
    public async Task Setup()
    {
        await SyntheticDatabases.EnsureRelationalAsync().ConfigureAwait(false);
        this.reader = await AccessReader.OpenAsync(SyntheticDatabases.RelationalDbPath).ConfigureAwait(false);

#pragma warning disable CA5394 // A fixed seed keeps the keys the same in every run; nothing here is security-sensitive.
        var random = new Random(20261003);
        while (this.keySet.Count < KeyCount)
        {
            int key = random.Next(1, SyntheticDatabases.RelationalOrders + 1);
            if (this.keySet.Add(key))
            {
                this.keys[this.keySet.Count - 1] = key;
            }
        }
#pragma warning restore CA5394

        Expect(nameof(this.SeekRowsAsync_PrimaryKey_1000Keys), KeyCount, await this.SeekRowsAsync_PrimaryKey_1000Keys().ConfigureAwait(false));
        Expect(nameof(this.FromIndex_WhereEquals_1000Keys), KeyCount, await this.FromIndex_WhereEquals_1000Keys().ConfigureAwait(false));
        Expect(nameof(this.FromIndex_WhereBetween_250Rows), RangeRows, await this.FromIndex_WhereBetween_250Rows().ConfigureAwait(false));
        Expect(nameof(this.RowsT_InferredSeek_1000Keys), KeyCount, await this.RowsT_InferredSeek_1000Keys().ConfigureAwait(false));
        Expect(nameof(this.RowsT_ClientSideScan_1000Keys), KeyCount, await this.RowsT_ClientSideScan_1000Keys().ConfigureAwait(false));
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (this.reader is not null)
        {
            await this.reader.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>One <c>SeekRowsAsync</c> call on the primary key per key.</summary>
    /// <returns>The rows found.</returns>
    [Benchmark]
    public async Task<int> SeekRowsAsync_PrimaryKey_1000Keys()
    {
        int found = 0;
        foreach (int key in this.keys)
        {
            await foreach (object[] row in this.Reader.SeekRowsAsync(SyntheticDatabases.OrdersTable, SyntheticDatabases.PrimaryKeyIndex, [key]).ConfigureAwait(false))
            {
                _ = row;
                found++;
            }
        }

        return found;
    }

    /// <summary>One <c>FromIndex(...).WhereEquals</c> query on the primary key per key.</summary>
    /// <returns>The rows found.</returns>
    [Benchmark]
    public async Task<int> FromIndex_WhereEquals_1000Keys()
    {
        int found = 0;
        foreach (int key in this.keys)
        {
            await foreach (object[] row in this.Reader.FromIndex(SyntheticDatabases.OrdersTable, SyntheticDatabases.PrimaryKeyIndex)
                .WhereEquals(key)
                .ToRowsAsync()
                .ConfigureAwait(false))
            {
                _ = row;
                found++;
            }
        }

        return found;
    }

    /// <summary>One inclusive <c>WhereBetween</c> range of 250 order dates on <c>IX_Orders_OrderDate</c>.</summary>
    /// <returns>The rows found.</returns>
    [Benchmark]
    public async Task<int> FromIndex_WhereBetween_250Rows()
    {
        int found = 0;
        await foreach (Order row in this.Reader.FromIndex<Order>(SyntheticDatabases.OrdersTable, SyntheticDatabases.OrderDateIndex)
            .WhereBetween(SyntheticDatabases.OrderDate(RangeStart), SyntheticDatabases.OrderDate(RangeStart + RangeRows - 1))
            .ToRowsAsync()
            .ConfigureAwait(false))
        {
            _ = row;
            found++;
        }

        return found;
    }

    /// <summary>One <c>Rows&lt;T&gt;(o =&gt; o.OrderId == key)</c> per key; the reader seeks the primary key.</summary>
    /// <returns>The rows found.</returns>
    [Benchmark]
    public async Task<int> RowsT_InferredSeek_1000Keys()
    {
        int found = 0;
        foreach (int key in this.keys)
        {
            await foreach (Order row in this.Reader.Rows<Order>(SyntheticDatabases.OrdersTable, o => o.OrderId == key).ConfigureAwait(false))
            {
                _ = row;
                found++;
            }
        }

        return found;
    }

    /// <summary>One full <c>Rows&lt;T&gt;()</c> scan that keeps the rows whose key is in the set: the no-index alternative.</summary>
    /// <returns>The rows kept.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> RowsT_ClientSideScan_1000Keys()
    {
        int found = 0;
        await foreach (Order row in this.Reader.Rows<Order>(SyntheticDatabases.OrdersTable).ConfigureAwait(false))
        {
            if (this.keySet.Contains(row.OrderId))
            {
                found++;
            }
        }

        return found;
    }

    private static void Expect(string benchmark, int expected, int actual)
    {
        if (actual != expected)
        {
            throw new InvalidOperationException($"{benchmark} returned {actual} rows; expected {expected}.");
        }
    }
}
