namespace JetDatabaseWriter.Benchmarks.Writer;

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
    private const string BenchmarkTableName = "BenchWriterRows";
    private const string IdColumnName = "Id";
    private const string NameColumnName = "Name";
    private const int IdBase = 900_000;

    private static readonly string SourceDbPath =
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "NorthwindTraders.accdb");

    private string _baselinePath = string.Empty;
    private string _tempPath = string.Empty;
    private object?[] _dummyRow = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        if (!File.Exists(SourceDbPath))
        {
            throw new FileNotFoundException(
                $"Benchmark database not found at '{SourceDbPath}'. " +
                "Copy NorthwindTraders.accdb to the benchmark output directory.");
        }

        _baselinePath = Path.Combine(Path.GetTempPath(), $"JetBenchBaseline_{Guid.NewGuid():N}.accdb");
        File.Copy(SourceDbPath, _baselinePath, overwrite: true);

        await using var writer = await AccessWriter.OpenAsync(_baselinePath);
        await writer.CreateTableAsync(
            BenchmarkTableName,
            [
                new(IdColumnName, typeof(int)) { IsPrimaryKey = true },
                new(NameColumnName, typeof(string), 255),
            ]);

        _dummyRow = BuildDummyRow(0);
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        if (File.Exists(_baselinePath))
        {
            File.Delete(_baselinePath);
        }
    }

    [IterationSetup]
    public void IterationSetup()
    {
        // Fresh copy for every iteration so writes don't accumulate.
        _tempPath = Path.Combine(Path.GetTempPath(), $"JetBench_{Guid.NewGuid():N}.accdb");
        File.Copy(_baselinePath, _tempPath, overwrite: true);
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
        await writer.InsertRowAsync(BenchmarkTableName, _dummyRow);
    }

    [Benchmark]
    [Arguments(10)]
    [Arguments(100)]
    public async Task<int> InsertRows_Batch(int count)
    {
        IEnumerable<object?[]> rows = Enumerable.Range(1, count).Select(BuildDummyRow);
        await using var writer = await AccessWriter.OpenAsync(_tempPath);
        return await writer.InsertRowsAsync(BenchmarkTableName, rows);
    }

    [Benchmark]
    public async Task InsertRow_Typed()
    {
        await using var writer = await AccessWriter.OpenAsync(_tempPath);
        await writer.InsertRowAsync(BenchmarkTableName, new SimpleEntity
        {
            Id = IdBase,
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
        await writer.InsertRowAsync(BenchmarkTableName, _dummyRow);

        string predicateCol = IdColumnName;
        object? predicateVal = _dummyRow[0];
        var updates = new Dictionary<string, object?>
        {
            [NameColumnName] = "UpdatedBench",
        };
        return await writer.UpdateRowsAsync(BenchmarkTableName, predicateCol, predicateVal, updates);
    }

    [Benchmark]
    public async Task<int> DeleteRows()
    {
        await using var writer = await AccessWriter.OpenAsync(_tempPath);
        await writer.InsertRowAsync(BenchmarkTableName, _dummyRow);

        string predicateCol = IdColumnName;
        object? predicateVal = _dummyRow[0];
        return await writer.DeleteRowsAsync(BenchmarkTableName, predicateCol, predicateVal);
    }

    // ── Helpers ───────────────────────────────────────────────────────

    private static object?[] BuildDummyRow(int seed) => [IdBase + seed, $"BenchWrite_{seed}"];

    public class SimpleEntity
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }
}
