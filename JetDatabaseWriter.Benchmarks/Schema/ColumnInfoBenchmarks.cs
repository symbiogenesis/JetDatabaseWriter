namespace JetDatabaseWriter.Benchmarks.Schema;

using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Schema.Models;

[MemoryDiagnoser]
public class ColumnInfoBenchmarks
{
    private ColumnInfo[] _columns = null!;

    [GlobalSetup]
    public void Setup()
    {
        _columns =
        [
            new() { Type = 0x01, Flags = 0x00, Name = "Bool" },       // Boolean → fixed
            new() { Type = 0x04, Flags = 0x00, Name = "Long" },       // LongInteger → fixed
            new() { Type = 0x07, Flags = 0x00, Name = "Double" },     // Double → fixed
            new() { Type = 0x08, Flags = 0x00, Name = "DateTime" },   // DateTime → fixed
            new() { Type = 0x0F, Flags = 0x00, Name = "Guid" },       // Guid → fixed
            new() { Type = 0x0A, Flags = 0x01, Name = "Text" },       // Text → variable
            new() { Type = 0x0C, Flags = 0x01, Name = "Memo" },       // Memo → variable
            new() { Type = 0x0B, Flags = 0x00, Name = "OLE" },        // Ole → variable
            new() { Type = 0x09, Flags = 0x00, Name = "Binary" },     // Binary → variable
            new() { Type = 0xFF, Flags = 0x01, Name = "Custom_Fixed" },   // unknown type, FLAG_FIXED set
            new() { Type = 0xFF, Flags = 0x00, Name = "Custom_Var" },     // unknown type, FLAG_FIXED clear
        ];
    }

    [Benchmark]
    public int IsFixed_AllColumns()
    {
        int fixedCount = 0;
        for (int i = 0; i < _columns.Length; i++)
        {
            if (_columns[i].IsFixed)
            {
                fixedCount++;
            }
        }

        return fixedCount;
    }

    [Benchmark]
    public bool IsFixed_FixedType() => _columns[0].IsFixed;

    [Benchmark]
    public bool IsFixed_VariableType() => _columns[5].IsFixed;

    [Benchmark]
    public bool IsFixed_FallbackFlag() => _columns[9].IsFixed;
}
