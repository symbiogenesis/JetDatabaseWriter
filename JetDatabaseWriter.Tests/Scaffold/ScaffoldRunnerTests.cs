namespace JetDatabaseWriter.Tests.Scaffold;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Core.Interfaces;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Scaffold;
using Xunit;

public sealed class ScaffoldRunnerTests : IDisposable
{
    private readonly string _outputDir;

    public ScaffoldRunnerTests()
    {
        _outputDir = Path.Combine(Path.GetTempPath(), "ScaffoldRunnerTests_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_outputDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_outputDir))
        {
            Directory.Delete(_outputDir, recursive: true);
        }
    }

    [Fact]
    public async Task RunAsync_NoTables_ReturnsZero_And_PrintsMessage()
    {
        await using var reader = new FakeAccessReader(tables: []);
        await using var stdout = new StringWriter();
        await using var stderr = new StringWriter();
        var runner = new ScaffoldRunner(reader, stdout, stderr);

        int result = await runner.RunAsync(_outputDir, "TestNs", useRecords: false, nullable: true, TestContext.Current.CancellationToken);

        Assert.Equal(0, result);
        Assert.Contains("No user tables found", stdout.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task RunAsync_SingleTable_GeneratesFile()
    {
        var columns = new List<ColumnMetadata>
        {
            new() { Name = "Id", ClrType = typeof(int), IsNullable = false, TypeName = "Long Integer", Size = ColumnSize.FromBytes(4) },
            new() { Name = "Name", ClrType = typeof(string), IsNullable = true, TypeName = "Text", Size = ColumnSize.FromBytes(255) },
        };

        await using var reader = new FakeAccessReader(
            tables: ["Customers"],
            columnsByTable: new() { ["Customers"] = columns });

        await using var stdout = new StringWriter();
        await using var stderr = new StringWriter();
        var runner = new ScaffoldRunner(reader, stdout, stderr);

        int result = await runner.RunAsync(_outputDir, "MyApp.Models", useRecords: false, nullable: true, TestContext.Current.CancellationToken);

        Assert.Equal(1, result);
        string filePath = Path.Combine(_outputDir, "Customers.cs");
        Assert.True(File.Exists(filePath));
        string content = await File.ReadAllTextAsync(filePath, TestContext.Current.CancellationToken);
        Assert.Contains("namespace MyApp.Models", content, StringComparison.Ordinal);
        Assert.Contains("class Customers", content, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RunAsync_MultipleTables_GeneratesAllFiles()
    {
        var idCol = new ColumnMetadata { Name = "Id", ClrType = typeof(int), IsNullable = false, TypeName = "Long Integer", Size = ColumnSize.FromBytes(4) };

        await using var reader = new FakeAccessReader(
            tables: ["Orders", "Products", "Categories"],
            columnsByTable: new()
            {
                ["Orders"] = [idCol],
                ["Products"] = [idCol],
                ["Categories"] = [idCol],
            });

        await using var stdout = new StringWriter();
        await using var stderr = new StringWriter();
        var runner = new ScaffoldRunner(reader, stdout, stderr);

        int result = await runner.RunAsync(_outputDir, "NS", useRecords: false, nullable: false, TestContext.Current.CancellationToken);

        Assert.Equal(3, result);
        Assert.True(File.Exists(Path.Combine(_outputDir, "Orders.cs")));
        Assert.True(File.Exists(Path.Combine(_outputDir, "Products.cs")));
        Assert.True(File.Exists(Path.Combine(_outputDir, "Categories.cs")));
        Assert.Contains("Done. 3 model(s) generated.", stdout.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task RunAsync_UseRecords_EmitsRecordKeyword()
    {
        var columns = new List<ColumnMetadata>
        {
            new() { Name = "Code", ClrType = typeof(string), IsNullable = false, TypeName = "Text", Size = ColumnSize.FromBytes(50) },
        };

        await using var reader = new FakeAccessReader(
            tables: ["Items"],
            columnsByTable: new() { ["Items"] = columns });

        await using var stdout = new StringWriter();
        await using var stderr = new StringWriter();
        var runner = new ScaffoldRunner(reader, stdout, stderr);

        await runner.RunAsync(_outputDir, "NS", useRecords: true, nullable: false, TestContext.Current.CancellationToken);

        string content = await File.ReadAllTextAsync(Path.Combine(_outputDir, "Items.cs"), TestContext.Current.CancellationToken);
        Assert.Contains("record Items", content, StringComparison.Ordinal);
        Assert.DoesNotContain("class Items", content, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RunAsync_NullableEnabled_EmitsNullableDirective()
    {
        var columns = new List<ColumnMetadata>
        {
            new() { Name = "Name", ClrType = typeof(string), IsNullable = true, TypeName = "Text", Size = ColumnSize.FromBytes(100) },
        };

        await using var reader = new FakeAccessReader(
            tables: ["People"],
            columnsByTable: new() { ["People"] = columns });

        await using var stdout = new StringWriter();
        await using var stderr = new StringWriter();
        var runner = new ScaffoldRunner(reader, stdout, stderr);

        await runner.RunAsync(_outputDir, "NS", useRecords: false, nullable: true, TestContext.Current.CancellationToken);

        string content = await File.ReadAllTextAsync(Path.Combine(_outputDir, "People.cs"), TestContext.Current.CancellationToken);
        Assert.Contains("#nullable enable", content, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RunAsync_NullableDisabled_OmitsNullableDirective()
    {
        var columns = new List<ColumnMetadata>
        {
            new() { Name = "Name", ClrType = typeof(string), IsNullable = true, TypeName = "Text", Size = ColumnSize.FromBytes(100) },
        };

        await using var reader = new FakeAccessReader(
            tables: ["People"],
            columnsByTable: new() { ["People"] = columns });

        await using var stdout = new StringWriter();
        await using var stderr = new StringWriter();
        var runner = new ScaffoldRunner(reader, stdout, stderr);

        await runner.RunAsync(_outputDir, "NS", useRecords: false, nullable: false, TestContext.Current.CancellationToken);

        string content = await File.ReadAllTextAsync(Path.Combine(_outputDir, "People.cs"), TestContext.Current.CancellationToken);
        Assert.DoesNotContain("#nullable", content, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RunAsync_InvalidOperationException_SkipsTable_ContinuesOthers()
    {
        var goodColumns = new List<ColumnMetadata>
        {
            new() { Name = "Id", ClrType = typeof(int), IsNullable = false, TypeName = "Long Integer", Size = ColumnSize.FromBytes(4) },
        };

        await using var reader = new FakeAccessReader(
            tables: ["Good", "Bad", "AlsoGood"],
            columnsByTable: new()
            {
                ["Good"] = goodColumns,
                ["AlsoGood"] = goodColumns,
            },
            failingTables: new() { ["Bad"] = new InvalidOperationException("corrupt table") });

        await using var stdout = new StringWriter();
        await using var stderr = new StringWriter();
        var runner = new ScaffoldRunner(reader, stdout, stderr);

        int result = await runner.RunAsync(_outputDir, "NS", useRecords: false, nullable: false, TestContext.Current.CancellationToken);

        Assert.Equal(2, result);
        Assert.True(File.Exists(Path.Combine(_outputDir, "Good.cs")));
        Assert.True(File.Exists(Path.Combine(_outputDir, "AlsoGood.cs")));
        Assert.False(File.Exists(Path.Combine(_outputDir, "Bad.cs")));
        Assert.Contains("Warning: skipping table 'Bad'", stderr.ToString(), StringComparison.Ordinal);
        Assert.Contains("corrupt table", stderr.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task RunAsync_IOException_SkipsTable_ContinuesOthers()
    {
        var goodColumns = new List<ColumnMetadata>
        {
            new() { Name = "Id", ClrType = typeof(int), IsNullable = false, TypeName = "Long Integer", Size = ColumnSize.FromBytes(4) },
        };

        await using var reader = new FakeAccessReader(
            tables: ["First", "Broken"],
            columnsByTable: new()
            {
                ["First"] = goodColumns,
            },
            failingTables: new() { ["Broken"] = new IOException("page read failed") });

        await using var stdout = new StringWriter();
        await using var stderr = new StringWriter();
        var runner = new ScaffoldRunner(reader, stdout, stderr);

        int result = await runner.RunAsync(_outputDir, "NS", useRecords: false, nullable: false, TestContext.Current.CancellationToken);

        Assert.Equal(1, result);
        Assert.True(File.Exists(Path.Combine(_outputDir, "First.cs")));
        Assert.False(File.Exists(Path.Combine(_outputDir, "Broken.cs")));
        Assert.Contains("Warning: skipping table 'Broken'", stderr.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task RunAsync_TableNameNeedsCleaning_UsesCleanedClassName()
    {
        var columns = new List<ColumnMetadata>
        {
            new() { Name = "Value", ClrType = typeof(double), IsNullable = false, TypeName = "Double", Size = ColumnSize.FromBytes(8) },
        };

        await using var reader = new FakeAccessReader(
            tables: ["my table 1"],
            columnsByTable: new() { ["my table 1"] = columns });

        await using var stdout = new StringWriter();
        await using var stderr = new StringWriter();
        var runner = new ScaffoldRunner(reader, stdout, stderr);

        await runner.RunAsync(_outputDir, "NS", useRecords: false, nullable: false, TestContext.Current.CancellationToken);

        string className = NameCleaner.ToClassName("my table 1");
        string filePath = Path.Combine(_outputDir, $"{className}.cs");
        Assert.True(File.Exists(filePath), $"Expected file {filePath} to exist");
    }

    [Fact]
    public async Task RunAsync_CreatesOutputDirectory_WhenNotExists()
    {
        string nested = Path.Combine(_outputDir, "sub", "deep");
        var columns = new List<ColumnMetadata>
        {
            new() { Name = "X", ClrType = typeof(int), IsNullable = false, TypeName = "Long Integer", Size = ColumnSize.FromBytes(4) },
        };

        await using var reader = new FakeAccessReader(
            tables: ["T"],
            columnsByTable: new() { ["T"] = columns });

        await using var stdout = new StringWriter();
        await using var stderr = new StringWriter();
        var runner = new ScaffoldRunner(reader, stdout, stderr);

        int result = await runner.RunAsync(nested, "NS", useRecords: false, nullable: false, TestContext.Current.CancellationToken);

        Assert.Equal(1, result);
        Assert.True(Directory.Exists(nested));
        Assert.True(File.Exists(Path.Combine(nested, "T.cs")));
    }

    [Fact]
    public async Task RunAsync_OutputIncludesTableCount()
    {
        var col = new ColumnMetadata { Name = "A", ClrType = typeof(int), IsNullable = false, TypeName = "Long Integer", Size = ColumnSize.FromBytes(4) };

        await using var reader = new FakeAccessReader(
            tables: ["T1", "T2"],
            columnsByTable: new() { ["T1"] = [col], ["T2"] = [col] });

        await using var stdout = new StringWriter();
        await using var stderr = new StringWriter();
        var runner = new ScaffoldRunner(reader, stdout, stderr);

        await runner.RunAsync(_outputDir, "NS", useRecords: false, nullable: false, TestContext.Current.CancellationToken);

        Assert.Contains("Found 2 table(s)", stdout.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task RunAsync_OutputIncludesColumnCount()
    {
        var columns = new List<ColumnMetadata>
        {
            new() { Name = "A", ClrType = typeof(int), IsNullable = false, TypeName = "Long Integer", Size = ColumnSize.FromBytes(4) },
            new() { Name = "B", ClrType = typeof(string), IsNullable = true, TypeName = "Text", Size = ColumnSize.FromBytes(100) },
            new() { Name = "C", ClrType = typeof(DateTime), IsNullable = true, TypeName = "Date/Time", Size = ColumnSize.FromBytes(8) },
        };

        await using var reader = new FakeAccessReader(
            tables: ["Wide"],
            columnsByTable: new() { ["Wide"] = columns });

        await using var stdout = new StringWriter();
        await using var stderr = new StringWriter();
        var runner = new ScaffoldRunner(reader, stdout, stderr);

        await runner.RunAsync(_outputDir, "NS", useRecords: false, nullable: false, TestContext.Current.CancellationToken);

        Assert.Contains("3 columns", stdout.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task RunAsync_AllTablesFail_ReturnsZeroGenerated()
    {
        await using var reader = new FakeAccessReader(
            tables: ["Bad1", "Bad2"],
            columnsByTable: new(),
            failingTables: new()
            {
                ["Bad1"] = new InvalidOperationException("fail1"),
                ["Bad2"] = new IOException("fail2"),
            });

        await using var stdout = new StringWriter();
        await using var stderr = new StringWriter();
        var runner = new ScaffoldRunner(reader, stdout, stderr);

        int result = await runner.RunAsync(_outputDir, "NS", useRecords: false, nullable: false, TestContext.Current.CancellationToken);

        Assert.Equal(0, result);
        Assert.Contains("Done. 0 model(s) generated.", stdout.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task RunAsync_CustomNamespace_AppearsInOutput()
    {
        var columns = new List<ColumnMetadata>
        {
            new() { Name = "Id", ClrType = typeof(int), IsNullable = false, TypeName = "Long Integer", Size = ColumnSize.FromBytes(4) },
        };

        await using var reader = new FakeAccessReader(
            tables: ["Foo"],
            columnsByTable: new() { ["Foo"] = columns });

        await using var stdout = new StringWriter();
        await using var stderr = new StringWriter();
        var runner = new ScaffoldRunner(reader, stdout, stderr);

        await runner.RunAsync(_outputDir, "Acme.Data.Entities", useRecords: false, nullable: true, TestContext.Current.CancellationToken);

        string content = await File.ReadAllTextAsync(Path.Combine(_outputDir, "Foo.cs"), TestContext.Current.CancellationToken);
        Assert.Contains("namespace Acme.Data.Entities", content, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RunAsync_Cancellation_ThrowsOperationCanceled()
    {
        var columns = new List<ColumnMetadata>
        {
            new() { Name = "Id", ClrType = typeof(int), IsNullable = false, TypeName = "Long Integer", Size = ColumnSize.FromBytes(4) },
        };

        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await using var reader = new FakeAccessReader(
            tables: ["T"],
            columnsByTable: new() { ["T"] = columns });

        var runner = new ScaffoldRunner(reader, TextWriter.Null, TextWriter.Null);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => runner.RunAsync(_outputDir, "NS", useRecords: false, nullable: false, cts.Token));
    }

    /// <summary>
    /// Minimal fake implementing only the methods ScaffoldRunner uses.
    /// </summary>
    private sealed class FakeAccessReader(
        List<string> tables,
        Dictionary<string, List<ColumnMetadata>>? columnsByTable = null,
        Dictionary<string, Exception>? failingTables = null) : IAccessReader
    {
        private readonly Dictionary<string, List<ColumnMetadata>> _columnsByTable = columnsByTable ?? new();
        private readonly Dictionary<string, Exception> _failingTables = failingTables ?? new();

        public DatabaseFormat DatabaseFormat => DatabaseFormat.Jet4Mdb;

        public int PageSize => 4096;

        public int CodePage => 1252;

        public bool DiagnosticsEnabled => false;

        public int PageCacheSize => 0;

        public bool ParallelPageReadsEnabled => false;

        public string LastDiagnostics => string.Empty;

        public ValueTask<List<string>> ListTablesAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new ValueTask<List<string>>(new List<string>(tables));
        }

        public ValueTask<List<ColumnMetadata>> GetColumnMetadataAsync(string tableName, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_failingTables.TryGetValue(tableName, out var ex))
            {
                throw ex;
            }

            if (_columnsByTable.TryGetValue(tableName, out var cols))
            {
                return new ValueTask<List<ColumnMetadata>>(cols);
            }

            throw new InvalidOperationException($"Table '{tableName}' not configured in fake");
        }

        public ValueTask DisposeAsync() => default;

        public ValueTask<System.Data.DataTable> ReadFirstTableAsync(uint? maxRows = null, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<List<LinkedTableInfo>> ListLinkedTablesAsync(CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<List<TableStat>> GetTableStatsAsync(CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<System.Data.DataTable> GetTablesAsDataTableAsync(CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<long> GetRealRowCountAsync(string tableName, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<List<T>> ReadTableAsync<T>(string tableName, uint? maxRows = null, CancellationToken cancellationToken = default)
            where T : class, new() =>
            throw new NotImplementedException();

        public ValueTask<System.Data.DataTable> ReadTableAsStringsAsync(string tableName, uint? maxRows = null, IProgress<long>? progress = null, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<IReadOnlyList<IndexMetadata>> ListIndexesAsync(string tableName, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<IReadOnlyList<ComplexColumnInfo>> GetComplexColumnsAsync(string tableName, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<IReadOnlyList<AttachmentRecord>> GetAttachmentsAsync(string tableName, string columnName, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<IReadOnlyList<(int ConceptualTableId, object? Value)>> GetMultiValueItemsAsync(string tableName, string columnName, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<System.Data.DataTable> ReadDataTableAsync(string? tableName = null, uint? maxRows = null, IProgress<long>? progress = null, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<DatabaseStatistics> GetStatisticsAsync(CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<Dictionary<string, System.Data.DataTable>> ReadAllTablesAsync(IProgress<TableProgress>? progress = null, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<Dictionary<string, System.Data.DataTable>> ReadAllTablesAsStringsAsync(IProgress<TableProgress>? progress = null, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public IAsyncEnumerable<object[]> Rows(string tableName, IProgress<long>? progress = null, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public IAsyncEnumerable<T> Rows<T>(string tableName, IProgress<long>? progress = null, CancellationToken cancellationToken = default)
            where T : class, new() =>
            throw new NotImplementedException();

        public IAsyncEnumerable<string[]> RowsAsStrings(string tableName, IProgress<long>? progress = null, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();
    }
}
