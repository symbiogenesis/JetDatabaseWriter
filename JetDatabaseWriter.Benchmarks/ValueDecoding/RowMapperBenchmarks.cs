namespace JetDatabaseWriter.Benchmarks.ValueDecoding;

using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;

[MemoryDiagnoser]
public class RowMapperBenchmarks
{
    private RowMapper<SampleEntity>.Accessor?[] index = null!;
    private object[] row = null!;
    private string[] headers = null!;
    private TableDef tableDef = null!;
    private SampleEntity entity = null!;

    [GlobalSetup]
    public void Setup()
    {
        this.headers = ["Id", "Name", "Value", "Description", "IsActive"];
        this.index = RowMapper<SampleEntity>.BuildIndex(this.headers);
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
    public object BuildIndex() => RowMapper<SampleEntity>.BuildIndex(this.headers);

    [Benchmark]
    public SampleEntity Map() => RowMapper<SampleEntity>.Map(this.row, this.index);

    [Benchmark]
    public object[] ToRow() => RowMapper<SampleEntity>.ToRow(this.tableDef, this.entity);

    [Benchmark]
    public SampleEntity MapWithConversion()
    {
        // Int64 -> Int32 forces Convert.ChangeType path
        object[] row = [42L, "TestName", 3.14f, "Desc", true];
        return RowMapper<SampleEntity>.Map(row, this.index);
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
