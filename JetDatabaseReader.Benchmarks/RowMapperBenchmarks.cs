namespace JetDatabaseReader.Benchmarks;

using BenchmarkDotNet.Attributes;

[MemoryDiagnoser]
public class RowMapperBenchmarks
{
    private RowMapper<SampleEntity>.Accessor?[] _index = null!;
    private object[] _row = null!;
    private SampleEntity _entity = null!;
    private string[] _headers = null!;

    [GlobalSetup]
    public void Setup()
    {
        _headers = ["Id", "Name", "Value", "Description", "IsActive"];
        _index = RowMapper<SampleEntity>.BuildIndex(_headers);
        _row = [42, "TestName", 3.14, "A description", true];
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
    public object[] ToRow() => RowMapper<SampleEntity>.ToRow(_entity, _index);

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
