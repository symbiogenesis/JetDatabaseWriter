namespace JetDatabaseReader.Benchmarks;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using BenchmarkDotNet.Attributes;

/// <summary>
/// End-to-end benchmarks for <see cref="AccessReader"/> against the Northwind .accdb test database.
/// Benchmarks are skipped if the database file is not found on disk.
/// </summary>
[MemoryDiagnoser]
public class AccessReaderBenchmarks
{
    private static readonly string DbPath =
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "NorthwindTraders.accdb");

    private string _tableName = string.Empty;

    [GlobalSetup]
    public void Setup()
    {
        if (!File.Exists(DbPath))
        {
            throw new FileNotFoundException(
                $"Benchmark database not found at '{DbPath}'. " +
                "Copy NorthwindTraders.accdb to the benchmark output directory.");
        }

        using var reader = AccessReader.Open(DbPath);
        _tableName = reader.ListTables().First();
    }

    [Benchmark]
    public List<string> ListTables()
    {
        using var reader = AccessReader.Open(DbPath);
        return reader.ListTables();
    }

    [Benchmark]
    public TableResult ReadTable_100()
    {
        using var reader = AccessReader.Open(DbPath);
        return reader.ReadTable(_tableName, 100);
    }

    [Benchmark]
    public TableResult ReadTable_1000()
    {
        using var reader = AccessReader.Open(DbPath);
        return reader.ReadTable(_tableName, 1000);
    }

    [Benchmark]
    public StringTableResult ReadTableAsStrings_100()
    {
        using var reader = AccessReader.Open(DbPath);
        return reader.ReadTableAsStrings(_tableName, 100);
    }

    [Benchmark]
    public int StreamRows_All()
    {
        using var reader = AccessReader.Open(DbPath);
        return reader.StreamRows(_tableName).Count();
    }

    [Benchmark]
    public int StreamRowsAsStrings_All()
    {
        using var reader = AccessReader.Open(DbPath);
        return reader.StreamRowsAsStrings(_tableName).Count();
    }

    [Benchmark]
    public List<ColumnMetadata> GetColumnMetadata()
    {
        using var reader = AccessReader.Open(DbPath);
        return reader.GetColumnMetadata(_tableName);
    }

    [Benchmark]
    public DatabaseStatistics GetStatistics()
    {
        using var reader = AccessReader.Open(DbPath);
        return reader.GetStatistics();
    }

    [Benchmark]
    public DataTable ReadTable_AsDataTable()
    {
        using var reader = AccessReader.Open(DbPath);
        return reader.ReadTable(_tableName, 100).ToDataTable();
    }

    [Benchmark]
    public int Query_Where_Count()
    {
        using var reader = AccessReader.Open(DbPath);
        return reader.Query(_tableName).Where(_ => true).Count();
    }

    [Benchmark]
    public object[] Query_FirstOrDefault()
    {
        using var reader = AccessReader.Open(DbPath);
        return reader.Query(_tableName).FirstOrDefault();
    }
}
