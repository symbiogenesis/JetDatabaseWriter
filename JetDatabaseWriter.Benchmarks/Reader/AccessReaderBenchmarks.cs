namespace JetDatabaseWriter.Benchmarks.Reader;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Benchmarks.Models;
using JetDatabaseWriter.Models;

/// <summary>
/// End-to-end benchmarks for <see cref="AccessReader"/> against the Northwind .accdb test database.
/// Benchmarks are skipped if the database file is not found on disk.
/// </summary>
[MemoryDiagnoser]
public class AccessReaderBenchmarks
{
    /// <summary>
    /// Name of the numeric/date-heavy <c>OrderDetails</c> table from NorthwindTraders.accdb.
    /// </summary>
    private const string NumericTable = "OrderDetails";

    private static readonly string DbPath =
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "NorthwindTraders.accdb");

    private string tableName = string.Empty;

    [GlobalSetup]
    public async Task Setup()
    {
        if (!File.Exists(DbPath))
        {
            throw new FileNotFoundException(
                $"Benchmark database not found at '{DbPath}'. " +
                "Copy NorthwindTraders.accdb to the benchmark output directory.");
        }

        await using AccessReader reader = await AccessReader.OpenAsync(DbPath);
        IReadOnlyList<string> tableNames = await reader.ListTablesAsync();
        this.tableName = tableNames[0];
    }

    [Benchmark]
    public async Task<IReadOnlyList<string>> ListTables()
    {
        await using AccessReader reader = await AccessReader.OpenAsync(DbPath);
        return await reader.ListTablesAsync();
    }

    [Benchmark]
    public async Task<DataTable?> ReadTable_100()
    {
        await using AccessReader reader = await AccessReader.OpenAsync(DbPath);
        return await reader.ReadDataTableAsync(this.tableName, 100);
    }

    [Benchmark]
    public async Task<DataTable?> ReadTable_1000()
    {
        await using AccessReader reader = await AccessReader.OpenAsync(DbPath);
        return await reader.ReadDataTableAsync(this.tableName, 1000);
    }

    [Benchmark]
    public async Task<DataTable> ReadTableAsStrings_100()
    {
        await using AccessReader reader = await AccessReader.OpenAsync(DbPath);
        return await reader.ReadTableAsStringsAsync(this.tableName, 100);
    }

    [Benchmark]
    public async Task<int> StreamRows_All()
    {
        await using AccessReader reader = await AccessReader.OpenAsync(DbPath);
        return await reader.Rows(this.tableName).CountAsync();
    }

    [Benchmark]
    public async Task<int> StreamRowsAsStrings_All()
    {
        await using AccessReader reader = await AccessReader.OpenAsync(DbPath);
        return await reader.RowsAsStrings(this.tableName).CountAsync();
    }

    /// <summary>
    /// Baseline untyped row stream over a numeric/date-heavy table. Compare with
    /// <see cref="StreamRowsTyped_All_Numeric"/> to measure typed mapper overhead.
    /// </summary>
    /// <returns>Row count for the numeric/date-heavy table.</returns>
    [Benchmark]
    public async Task<int> StreamRows_All_Numeric()
    {
        await using AccessReader reader = await AccessReader.OpenAsync(DbPath);
        return await reader.Rows(NumericTable).CountAsync();
    }

    /// <summary>
    /// Typed row stream (Rows&lt;T&gt;) over the same numeric/date-heavy table.
    /// Exercises RowMapper&lt;T&gt; on top of the decoded row path.
    /// </summary>
    /// <returns>Row count for the typed numeric/date-heavy table stream.</returns>
    [Benchmark]
    public async Task<int> StreamRowsTyped_All_Numeric()
    {
        await using AccessReader reader = await AccessReader.OpenAsync(DbPath);
        int count = 0;
        await foreach (OrderDetails row in reader.Rows<OrderDetails>(NumericTable))
        {
            _ = row;
            count++;
        }

        return count;
    }

    [Benchmark]
    public async Task<IReadOnlyList<ColumnMetadata>> GetColumnMetadata()
    {
        await using AccessReader reader = await AccessReader.OpenAsync(DbPath);
        return await reader.GetColumnMetadataAsync(this.tableName);
    }

    [Benchmark]
    public async Task<DatabaseStatistics> GetStatistics()
    {
        await using AccessReader reader = await AccessReader.OpenAsync(DbPath);
        return await reader.GetStatisticsAsync();
    }

    [Benchmark]
    public async Task<DataTable?> ReadTable_AsDataTable()
    {
        await using AccessReader reader = await AccessReader.OpenAsync(DbPath);
        return await reader.ReadDataTableAsync(this.tableName, 100);
    }

    [Benchmark]
    public async Task<int> Query_Where_Count()
    {
        await using AccessReader reader = await AccessReader.OpenAsync(DbPath);
        return await reader.Rows(this.tableName).Where(_ => true).CountAsync();
    }

    [Benchmark]
    public async Task<object[]?> Query_FirstOrDefault()
    {
        await using AccessReader reader = await AccessReader.OpenAsync(DbPath);
        return await reader.Rows(this.tableName).FirstOrDefaultAsync();
    }
}
