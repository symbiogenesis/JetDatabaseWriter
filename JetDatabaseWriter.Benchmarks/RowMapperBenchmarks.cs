namespace JetDatabaseWriter.Benchmarks;

using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Internal.Models;
using JetDatabaseWriter.Schema.Models;

[MemoryDiagnoser]
public class RowMapperBenchmarks
{
    private RowMapper<SampleEntity>.Accessor?[] _index = null!;
    private object[] _row = null!;
    private string[] _headers = null!;
    private TableDef _tableDef = null!;
    private SampleEntity _entity = null!;

    [GlobalSetup]
    public void Setup()
    {
        _headers = ["Id", "Name", "Value", "Description", "IsActive"];
        _index = RowMapper<SampleEntity>.BuildIndex(_headers);
        _row = [42, "TestName", 3.14, "A description", true];
        _tableDef = new TableDef
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
        _entity = new SampleEntity
        {
            Id = 42,
            Name = "TestName",
            Value = 3.14,
            Description = "A description",
            IsActive = true,
        };
    }

    [Benchmark]
    public object BuildIndex() => RowMapper<SampleEntity>.BuildIndex(_headers);

    [Benchmark]
    public SampleEntity Map() => RowMapper<SampleEntity>.Map(_row, _index);

    [Benchmark]
    public object[] ToRow() => RowMapper<SampleEntity>.ToRow(_tableDef, _entity);

    [Benchmark]
    public SampleEntity MapWithConversion()
    {
        // Int64 -> Int32 forces Convert.ChangeType path
        object[] row = [42L, "TestName", 3.14f, "Desc", true];
        return RowMapper<SampleEntity>.Map(row, _index);
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
