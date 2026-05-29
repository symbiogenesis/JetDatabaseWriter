using JetDatabaseWriter.Enums;

namespace JetDatabaseWriter.Benchmarks.Schema;

using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Enums.ColumnType;

[MemoryDiagnoser]
public class ColumnInfoBenchmarks
{
    private ColumnInfo[] _columns = null!;

    [GlobalSetup]
    public void Setup() => this._columns =
        [
            new() { Type = BooleanType, Flags = 0x00, Name = "Bool" },       // Boolean → fixed
            new() { Type = LongIntegerType, Flags = 0x00, Name = "Long" },       // LongInteger → fixed
            new() { Type = DoubleType, Flags = 0x00, Name = "Double" },     // Double → fixed
            new() { Type = DateTimeType, Flags = 0x00, Name = "DateTime" },   // DateTime → fixed
            new() { Type = GuidType, Flags = 0x00, Name = "Guid" },       // Guid → fixed
            new() { Type = TextType, Flags = 0x01, Name = "Text" },       // Text → variable
            new() { Type = MemoType, Flags = 0x01, Name = "Memo" },       // Memo → variable
            new() { Type = OleType, Flags = 0x00, Name = "OLE" },        // Ole → variable
            new() { Type = BinaryType, Flags = 0x00, Name = "Binary" },     // Binary → variable
            new() { Type = (ColumnType)0xFF, Flags = 0x01, Name = "Custom_Fixed" },   // unknown type, FLAG_FIXED set
            new() { Type = (ColumnType)0xFF, Flags = 0x00, Name = "Custom_Var" },     // unknown type, FLAG_FIXED clear
        ];

    [Benchmark]
    public int IsFixed_AllColumns()
    {
        int fixedCount = 0;
        for (int i = 0; i < this._columns.Length; i++)
        {
            if (this._columns[i].IsFixed)
            {
                fixedCount++;
            }
        }

        return fixedCount;
    }

    [Benchmark]
    public bool IsFixed_FixedType() => this._columns[0].IsFixed;

    [Benchmark]
    public bool IsFixed_VariableType() => this._columns[5].IsFixed;

    [Benchmark]
    public bool IsFixed_FallbackFlag() => this._columns[9].IsFixed;
}
