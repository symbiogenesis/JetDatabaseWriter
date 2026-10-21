namespace JetDatabaseWriter.Benchmarks.ValueDecoding;

using System;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;

[MemoryDiagnoser]
public class RowMapperBenchmarks
{
    private Func<object?[], SampleEntity> factory = null!;
    private object[] row = null!;
    private string[] headers = null!;
    private TableDef tableDef = null!;
    private SampleEntity entity = null!;

    [GlobalSetup]
    public void Setup()
    {
        this.headers = ["Id", "Name", "Value", "Description", "IsActive"];
        this.factory = RowMapper<SampleEntity>.Build(this.headers);
        this.row = [42, "TestName", 3.14, "A description", true];
        this.tableDef = new TableDef
        {
            Columns =
            [
                new ColumnInfo { Name = "Id" },
                new ColumnInfo { Name = "Name" },
                new ColumnInfo { Name = "Value" },
                new ColumnInfo { Name = "Description" },
                new ColumnInfo { Name = "IsActive" },
            ],
        };
        this.entity = new SampleEntity
        {
            Id = 42,
            Name = "TestName",
            Value = 3.14,
            Description = "A description",
            IsActive = true,
        };
    }

    [Benchmark]
    public SampleEntity Map() => this.factory(this.row);

    [Benchmark]
    public object[] ToRow() => RowMapper<SampleEntity>.ToRow(this.tableDef, this.entity);

    [Benchmark]
    public SampleEntity MapWithConversion()
    {
        // Int64 -> Int32 forces Convert.ChangeType path
        object[] row = [42L, "TestName", 3.14f, "Desc", true];
        return this.factory(row);
    }

    public class SampleEntity
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public double Value { get; set; }

        public string Description { get; set; } = string.Empty;

        public bool IsActive { get; set; }
    }
}
