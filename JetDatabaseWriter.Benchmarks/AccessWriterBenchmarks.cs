namespace JetDatabaseWriter.Benchmarks;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

/// <summary>
/// Benchmarks for <see cref="AccessWriter"/> operations.
/// Each iteration works on a fresh temp copy of NorthwindTraders.accdb.
/// </summary>
[MemoryDiagnoser]
public class AccessWriterBenchmarks
{
    private static readonly string SourceDbPath =
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "NorthwindTraders.accdb");

    private string _tempPath = string.Empty;
    private string _tableName = string.Empty;
    private List<ColumnMetadata> _columns = null!;
    private object[] _dummyRow = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        if (!File.Exists(SourceDbPath))
        {
            throw new FileNotFoundException(
                $"Benchmark database not found at '{SourceDbPath}'. " +
                "Copy NorthwindTraders.accdb to the benchmark output directory.");
        }

        // Determine table name and column metadata once.
        await using var reader = await AccessReader.OpenAsync(SourceDbPath);
        _tableName = (await reader.ListTablesAsync()).First();
        _columns = await reader.GetColumnMetadataAsync(_tableName);
        _dummyRow = BuildDummyRow(_columns);
    }

    [IterationSetup]
    public void IterationSetup()
    {
        // Fresh copy for every iteration so writes don't accumulate.
        _tempPath = Path.Combine(Path.GetTempPath(), $"JetBench_{Guid.NewGuid():N}.accdb");
        File.Copy(SourceDbPath, _tempPath, overwrite: true);
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        if (File.Exists(_tempPath))
        {
            File.Delete(_tempPath);
        }
    }

    // ── Insert ────────────────────────────────────────────────────────

    [Benchmark]
    public async Task InsertRow_Single()
    {
        await using var writer = await AccessWriter.OpenAsync(_tempPath);
        await writer.InsertRowAsync(_tableName, _dummyRow);
    }

    [Benchmark]
    [Arguments(10)]
    [Arguments(100)]
    public async Task<int> InsertRows_Batch(int count)
    {
        IEnumerable<object[]> rows = Enumerable.Range(0, count).Select(_ => (object[])_dummyRow.Clone());
        await using var writer = await AccessWriter.OpenAsync(_tempPath);
        return await writer.InsertRowsAsync(_tableName, rows);
    }

    [Benchmark]
    public async Task InsertRow_Typed()
    {
        await using var writer = await AccessWriter.OpenAsync(_tempPath);
        await writer.InsertRowAsync(_tableName, new SimpleEntity
        {
            Id = 999,
            Name = "BenchTyped",
        });
    }

    // ── CreateTable + DropTable ───────────────────────────────────────

    [Benchmark]
    public async Task CreateTable()
    {
        await using var writer = await AccessWriter.OpenAsync(_tempPath);
        await writer.CreateTableAsync(
            "BenchTable",
            [
                new("Id", typeof(int)),
                new("Name", typeof(string), 255),
                new("Value", typeof(double)),
                new("Created", typeof(DateTime)),
                new("Active", typeof(bool)),
            ]);
    }

    [Benchmark]
    public async Task CreateAndDropTable()
    {
        await using var writer = await AccessWriter.OpenAsync(_tempPath);
        await writer.CreateTableAsync(
            "BenchDrop",
            [
                new("Id", typeof(int)),
                new("Name", typeof(string), 255),
            ]);
        await writer.DropTableAsync("BenchDrop");
    }

    // ── Update / Delete ───────────────────────────────────────────────

    [Benchmark]
    public async Task<int> UpdateRows()
    {
        // Insert a known row, then update it.
        await using var writer = await AccessWriter.OpenAsync(_tempPath);
        await writer.InsertRowAsync(_tableName, _dummyRow);

        string predicateCol = _columns[0].Name;
        object predicateVal = _dummyRow[0];
        var updates = new Dictionary<string, object>
        {
            [_columns.Count > 1 ? _columns[1].Name : _columns[0].Name] = "UpdatedBench",
        };
        return await writer.UpdateRowsAsync(_tableName, predicateCol, predicateVal, updates);
    }

    [Benchmark]
    public async Task<int> DeleteRows()
    {
        await using var writer = await AccessWriter.OpenAsync(_tempPath);
        await writer.InsertRowAsync(_tableName, _dummyRow);

        string predicateCol = _columns[0].Name;
        object predicateVal = _dummyRow[0];
        return await writer.DeleteRowsAsync(_tableName, predicateCol, predicateVal);
    }

    // ── Helpers ───────────────────────────────────────────────────────

    private static object[] BuildDummyRow(List<ColumnMetadata> columns)
    {
        var values = new object[columns.Count];
        for (int i = 0; i < columns.Count; i++)
        {
            values[i] = GetDummyValue(columns[i].ClrType);
        }

        return values;
    }

    private static object GetDummyValue(Type clrType) => clrType switch
    {
        _ when clrType == typeof(string) => "BenchWrite",
        _ when clrType == typeof(int) => 999,
        _ when clrType == typeof(short) => (short)99,
        _ when clrType == typeof(long) => 99999L,
        _ when clrType == typeof(byte) => (byte)1,
        _ when clrType == typeof(bool) => true,
        _ when clrType == typeof(DateTime) => new DateTime(2025, 1, 1),
        _ when clrType == typeof(double) => 1.23,
        _ when clrType == typeof(float) => 1.23f,
        _ when clrType == typeof(decimal) => 1.23m,
        _ when clrType == typeof(Guid) => Guid.NewGuid(),
        _ when clrType == typeof(byte[]) => new byte[] { 0x01, 0x02 },
        _ => DBNull.Value,
    };

    public class SimpleEntity
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }
}
