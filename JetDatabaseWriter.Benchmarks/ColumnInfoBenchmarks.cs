namespace JetDatabaseWriter.Benchmarks;

using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Internal.Models;

[MemoryDiagnoser]
public class ColumnInfoBenchmarks
{
    private ColumnInfo[] _columns = null!;

    [GlobalSetup]
    public void Setup()
    {
        _columns =
        [
            new() { Type = 0x01, Flags = 0x00, Name = "Bool" },       // T_BOOL → fixed
            new() { Type = 0x04, Flags = 0x00, Name = "Long" },       // T_LONG → fixed
            new() { Type = 0x07, Flags = 0x00, Name = "Double" },     // T_DOUBLE → fixed
            new() { Type = 0x08, Flags = 0x00, Name = "DateTime" },   // T_DATETIME → fixed
            new() { Type = 0x0F, Flags = 0x00, Name = "Guid" },       // T_GUID → fixed
            new() { Type = 0x0A, Flags = 0x01, Name = "Text" },       // T_TEXT → variable
            new() { Type = 0x0C, Flags = 0x01, Name = "Memo" },       // T_MEMO → variable
            new() { Type = 0x0B, Flags = 0x00, Name = "OLE" },        // T_OLE → variable
            new() { Type = 0x09, Flags = 0x00, Name = "Binary" },     // T_BINARY → variable
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
