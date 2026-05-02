namespace JetDatabaseWriter.Benchmarks;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Benchmarks.Models;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Models;

/// <summary>
/// End-to-end benchmarks for <see cref="AccessReader"/> against the Northwind .accdb test database.
/// Benchmarks are skipped if the database file is not found on disk.
/// </summary>
[MemoryDiagnoser]
public class AccessReaderBenchmarks
{
    /// <summary>
    /// Numeric/date-heavy table used to exercise the typed fixed-width decode path.
    /// 5 ints, 1 short, 1 currency (decimal), 1 single (float), 2 datetimes, 2 short text columns.
    /// </summary>
    private const string NumericTable = "OrderDetails";

    private static readonly string DbPath =
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "NorthwindTraders.accdb");

    private string _tableName = string.Empty;

    [GlobalSetup]
    public async Task Setup()
    {
        if (!File.Exists(DbPath))
        {
            throw new FileNotFoundException(
                $"Benchmark database not found at '{DbPath}'. " +
                "Copy NorthwindTraders.accdb to the benchmark output directory.");
        }

        await using var reader = await AccessReader.OpenAsync(DbPath);
        _tableName = (await reader.ListTablesAsync()).First();
    }

    [Benchmark]
    public async Task<List<string>> ListTables()
    {
        await using var reader = await AccessReader.OpenAsync(DbPath);
        return await reader.ListTablesAsync();
    }

    [Benchmark]
    public async Task<DataTable?> ReadTable_100()
    {
        await using var reader = await AccessReader.OpenAsync(DbPath);
        return await reader.ReadDataTableAsync(_tableName, 100);
    }

    [Benchmark]
    public async Task<DataTable?> ReadTable_1000()
    {
        await using var reader = await AccessReader.OpenAsync(DbPath);
        return await reader.ReadDataTableAsync(_tableName, 1000);
    }

    [Benchmark]
    public async Task<DataTable> ReadTableAsStrings_100()
    {
        await using var reader = await AccessReader.OpenAsync(DbPath);
        return await reader.ReadTableAsStringsAsync(_tableName, 100);
    }

    [Benchmark]
    public async Task<int> StreamRows_All()
    {
        await using var reader = await AccessReader.OpenAsync(DbPath);
        return await reader.Rows(_tableName).CountAsync();
    }

    [Benchmark]
    public async Task<int> StreamRowsAsStrings_All()
    {
        await using var reader = await AccessReader.OpenAsync(DbPath);
        return await reader.RowsAsStrings(_tableName).CountAsync();
    }

    /// <summary>
    /// Phase 0 baseline: untyped row stream over a numeric/date-heavy table.
    /// Targets the per-column ReadFixedString → ParseValue round-trip the typed
    /// path will eventually replace.
    /// </summary>
    /// <returns>Row count for the numeric/date-heavy table.</returns>
    [Benchmark]
    public async Task<int> StreamRows_All_Numeric()
    {
        await using var reader = await AccessReader.OpenAsync(DbPath);
        return await reader.Rows(NumericTable).CountAsync();
    }

    /// <summary>
    /// Phase 0 baseline: typed row stream (Rows&lt;T&gt;) over the same numeric/date-heavy
    /// table. Exercises RowMapper&lt;T&gt; on top of the untyped row path.
    /// </summary>
    /// <returns>Row count for the typed numeric/date-heavy table stream.</returns>
    [Benchmark]
    public async Task<int> StreamRowsTyped_All_Numeric()
    {
        await using var reader = await AccessReader.OpenAsync(DbPath);
        var count = 0;
        await foreach (OrderDetails row in reader.Rows<OrderDetails>(NumericTable))
        {
            _ = row;
            count++;
        }

        return count;
    }

    [Benchmark]
    public async Task<List<ColumnMetadata>> GetColumnMetadata()
    {
        await using var reader = await AccessReader.OpenAsync(DbPath);
        return await reader.GetColumnMetadataAsync(_tableName);
    }

    [Benchmark]
    public async Task<DatabaseStatistics> GetStatistics()
    {
        await using var reader = await AccessReader.OpenAsync(DbPath);
        return await reader.GetStatisticsAsync();
    }

    [Benchmark]
    public async Task<DataTable?> ReadTable_AsDataTable()
    {
        await using var reader = await AccessReader.OpenAsync(DbPath);
        return await reader.ReadDataTableAsync(_tableName, 100);
    }

    [Benchmark]
    public async Task<int> Query_Where_Count()
    {
        await using var reader = await AccessReader.OpenAsync(DbPath);
        return await reader.Rows(_tableName).Where(_ => true).CountAsync();
    }

    [Benchmark]
    public async Task<object[]?> Query_FirstOrDefault()
    {
        await using var reader = await AccessReader.OpenAsync(DbPath);
        return await reader.Rows(_tableName).FirstOrDefaultAsync();
    }
}
