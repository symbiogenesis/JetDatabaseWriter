namespace JetDatabaseReader.Benchmarks;

using System;
using System.Collections.Generic;
using BenchmarkDotNet.Attributes;

[MemoryDiagnoser]
public class TableResultBenchmarks
{
    private TableResult _typedResult = null!;
    private StringTableResult _stringResult = null!;

    [Params(10, 100, 1000)]
    public int RowCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var headers = new List<string> { "Id", "Name", "Value", "Created", "Active" };
        var schema = new List<TableColumn>
        {
            new() { Name = "Id", Type = typeof(int) },
            new() { Name = "Name", Type = typeof(string) },
            new() { Name = "Value", Type = typeof(double) },
            new() { Name = "Created", Type = typeof(DateTime) },
            new() { Name = "Active", Type = typeof(bool) },
        };

        var typedRows = new List<object[]>(RowCount);
        var stringRows = new List<List<string>>(RowCount);
        for (int i = 0; i < RowCount; i++)
        {
            typedRows.Add([i, $"Name_{i}", i * 1.5, DateTime.Now, i % 2 == 0]);
            stringRows.Add([i.ToString(System.Globalization.CultureInfo.InvariantCulture), $"Name_{i}", (i * 1.5).ToString(System.Globalization.CultureInfo.InvariantCulture), DateTime.Now.ToString(System.Globalization.CultureInfo.InvariantCulture), (i % 2 == 0).ToString()]);
        }

        _typedResult = new TableResult
        {
            TableName = "Benchmark",
            Headers = headers,
            Schema = schema,
            Rows = typedRows,
        };

        _stringResult = new StringTableResult
        {
            TableName = "Benchmark",
            Headers = headers,
            Schema = schema,
            Rows = stringRows,
        };
    }

    [Benchmark]
    public object TypedResult_ToDataTable() => _typedResult.ToDataTable();

    [Benchmark]
    public object StringResult_ToDataTable() => _stringResult.ToDataTable();
}
