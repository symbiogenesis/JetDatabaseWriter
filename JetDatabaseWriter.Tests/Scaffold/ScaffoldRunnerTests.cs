namespace JetDatabaseWriter.Tests.Scaffold;

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Scaffold;
using Xunit;

public sealed class ScaffoldRunnerTests : IDisposable
{
    private readonly string outputDir;

    public ScaffoldRunnerTests()
    {
        this.outputDir = Path.Combine(Path.GetTempPath(), "ScaffoldRunnerTests_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(this.outputDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(this.outputDir))
        {
            Directory.Delete(this.outputDir, recursive: true);
        }
    }

    [Fact]
    public async Task RunAsync_NoTables_ReturnsZero_And_PrintsMessage()
    {
        await using var reader = new FakeAccessReader(tables: []);
        await using var stdout = new StringWriter();
        await using var stderr = new StringWriter();
        var runner = new ScaffoldRunner(reader, stdout, stderr);

        int result = await runner.RunAsync(this.outputDir, "TestNs", useRecords: false, nullable: true, TestContext.Current.CancellationToken);

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

        int result = await runner.RunAsync(this.outputDir, "MyApp.Models", useRecords: false, nullable: true, TestContext.Current.CancellationToken);

        Assert.Equal(1, result);
        string filePath = Path.Combine(this.outputDir, "Customers.cs");
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

        int result = await runner.RunAsync(this.outputDir, "NS", useRecords: false, nullable: false, TestContext.Current.CancellationToken);

        Assert.Equal(3, result);
        Assert.True(File.Exists(Path.Combine(this.outputDir, "Orders.cs")));
        Assert.True(File.Exists(Path.Combine(this.outputDir, "Products.cs")));
        Assert.True(File.Exists(Path.Combine(this.outputDir, "Categories.cs")));
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

        await runner.RunAsync(this.outputDir, "NS", useRecords: true, nullable: false, TestContext.Current.CancellationToken);

        string content = await File.ReadAllTextAsync(Path.Combine(this.outputDir, "Items.cs"), TestContext.Current.CancellationToken);
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

        await runner.RunAsync(this.outputDir, "NS", useRecords: false, nullable: true, TestContext.Current.CancellationToken);

        string content = await File.ReadAllTextAsync(Path.Combine(this.outputDir, "People.cs"), TestContext.Current.CancellationToken);
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

        await runner.RunAsync(this.outputDir, "NS", useRecords: false, nullable: false, TestContext.Current.CancellationToken);

        string content = await File.ReadAllTextAsync(Path.Combine(this.outputDir, "People.cs"), TestContext.Current.CancellationToken);
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

        int result = await runner.RunAsync(this.outputDir, "NS", useRecords: false, nullable: false, TestContext.Current.CancellationToken);

        Assert.Equal(2, result);
        Assert.True(File.Exists(Path.Combine(this.outputDir, "Good.cs")));
        Assert.True(File.Exists(Path.Combine(this.outputDir, "AlsoGood.cs")));
        Assert.False(File.Exists(Path.Combine(this.outputDir, "Bad.cs")));
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

        int result = await runner.RunAsync(this.outputDir, "NS", useRecords: false, nullable: false, TestContext.Current.CancellationToken);

        Assert.Equal(1, result);
        Assert.True(File.Exists(Path.Combine(this.outputDir, "First.cs")));
        Assert.False(File.Exists(Path.Combine(this.outputDir, "Broken.cs")));
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

        await runner.RunAsync(this.outputDir, "NS", useRecords: false, nullable: false, TestContext.Current.CancellationToken);

        string className = NameCleaner.ToClassName("my table 1");
        string filePath = Path.Combine(this.outputDir, $"{className}.cs");
        Assert.True(File.Exists(filePath), $"Expected file {filePath} to exist");
    }

    [Fact]
    public async Task RunAsync_CreatesOutputDirectory_WhenNotExists()
    {
        string nested = Path.Combine(this.outputDir, "sub", "deep");
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

        await runner.RunAsync(this.outputDir, "NS", useRecords: false, nullable: false, TestContext.Current.CancellationToken);

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

        await runner.RunAsync(this.outputDir, "NS", useRecords: false, nullable: false, TestContext.Current.CancellationToken);

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

        int result = await runner.RunAsync(this.outputDir, "NS", useRecords: false, nullable: false, TestContext.Current.CancellationToken);

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

        await runner.RunAsync(this.outputDir, "Acme.Data.Entities", useRecords: false, nullable: true, TestContext.Current.CancellationToken);

        string content = await File.ReadAllTextAsync(Path.Combine(this.outputDir, "Foo.cs"), TestContext.Current.CancellationToken);
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
            () => runner.RunAsync(this.outputDir, "NS", useRecords: false, nullable: false, cts.Token));
    }

    /// <summary>
    /// Tables whose cleaned names collide with each other, or with a type the generated
    /// code names, each get a file and a class of their own, and the files compile together
    /// with every class mapped to its table and every property typed as its column.
    /// </summary>
    /// <param name="useRecords">Whether to emit records.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RunAsync_CollidingNames_WritesOneCompilableFilePerTable(bool useRecords)
    {
        Dictionary<string, List<ColumnMetadata>> columnsByTable = new(StringComparer.Ordinal)
        {
            ["Orders"] = [Col("Id", typeof(int)), Col("Order Date", typeof(DateTime)), Col("Key", typeof(Guid)), Col("Link", typeof(Hyperlink)), Col("Photo", typeof(byte[]))],
            ["Order Details"] = [Col("Id", typeof(int)), Col("OrderId", typeof(int)), Col("Unit Price", typeof(decimal))],
            ["OrderDetails"] = [Col("Id", typeof(int)), Col("OrderId", typeof(int)), Col("Note", typeof(string))],
            ["Column Attribute"] = [Col("Id", typeof(int)), Col("Last Name", typeof(string)), Col("DateTimeID", typeof(int))],
            ["DateTime"] = [Col("Id", typeof(int)), Col("When", typeof(DateTime))],
            ["A_b"] = [Col("Id", typeof(int))],
            ["Ab"] = [Col("Id", typeof(int))],
            ["Column"] = [Col("Id", typeof(int)), Col("Value", typeof(string))],
            ["Table"] = [Col("Id", typeof(int))],
            ["System"] = [Col("Id", typeof(int)), Col("Stamp", typeof(TimeSpan))],
            ["ToString"] = [Col("Id", typeof(int))],
            ["###"] = [Col("Id", typeof(int))],
            ["$$$"] = [Col("Id", typeof(int))],
        };
        List<RelationshipMetadata> relationships =
        [
            new() { Name = "OrdersOrderDetails", PrimaryTable = "Orders", PrimaryColumns = ["Id"], ForeignTable = "Order Details", ForeignColumns = ["OrderId"] },
            new() { Name = "OrdersOrderDetails2", PrimaryTable = "Orders", PrimaryColumns = ["Id"], ForeignTable = "OrderDetails", ForeignColumns = ["OrderId"] },
            new() { Name = "DateTimeColumnAttribute", PrimaryTable = "DateTime", PrimaryColumns = ["Id"], ForeignTable = "Column Attribute", ForeignColumns = ["DateTimeID"] },
        ];

        await using var reader = new FakeAccessReader([.. columnsByTable.Keys], columnsByTable, relationships: relationships);
        await using var stdout = new StringWriter();
        await using var stderr = new StringWriter();
        var runner = new ScaffoldRunner(reader, stdout, stderr);

        int result = await runner.RunAsync(this.outputDir, "MyApp.Models", useRecords, nullable: true, TestContext.Current.CancellationToken);

        Assert.Equal(columnsByTable.Count, result);
        string[] files = Directory.GetFiles(this.outputDir, "*.cs");
        Assert.Equal(columnsByTable.Count, files.Length);
        Assert.Equal(
            ["AB2.cs", "Ab.cs", "Column.cs", "ColumnAttribute.cs", "DateTime.cs", "OrderDetails.cs", "OrderDetails2.cs", "Orders.cs", "System.cs", "Table.cs", "ToStringEntity.cs", "Unknown.cs", "Unknown2.cs"],
            files.Select(Path.GetFileName).Order(StringComparer.Ordinal));

        List<string> sources = [];
        foreach (string file in files)
        {
            sources.Add(await File.ReadAllTextAsync(file, TestContext.Current.CancellationToken));
        }

        Assembly assembly = ScaffoldCompilation.CompileCleanly(sources);
        Dictionary<string, Type> typeByTable = new(StringComparer.OrdinalIgnoreCase);
        foreach (Type type in assembly.GetTypes().Where(t => t.Namespace == "MyApp.Models"))
        {
            typeByTable.Add(type.GetCustomAttribute<TableAttribute>()?.Name ?? type.Name, type);
        }

        Assert.Equal(columnsByTable.Keys.Order(StringComparer.OrdinalIgnoreCase), typeByTable.Keys.Order(StringComparer.OrdinalIgnoreCase));
        foreach ((string table, List<ColumnMetadata> columns) in columnsByTable)
        {
            Type type = typeByTable[table];
            foreach (ColumnMetadata column in columns)
            {
                PropertyInfo property = Assert.Single(
                    type.GetProperties(),
                    p => string.Equals(p.GetCustomAttribute<ColumnAttribute>()?.Name ?? p.Name, column.Name, StringComparison.OrdinalIgnoreCase));
                Assert.Equal(column.ClrType, Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType);
            }
        }

        Type orders = typeByTable["Orders"];
        Assert.Equal(
            new[] { typeByTable["Order Details"], typeByTable["OrderDetails"] }.OrderBy(t => t.Name, StringComparer.Ordinal),
            orders.GetProperties()
                .Where(p => p.PropertyType.IsGenericType && p.PropertyType.GetGenericTypeDefinition() == typeof(ICollection<>))
                .Select(p => p.PropertyType.GetGenericArguments()[0])
                .OrderBy(t => t.Name, StringComparer.Ordinal));
        Assert.Contains(typeByTable["Column Attribute"].GetProperties(), p => p.PropertyType == typeByTable["DateTime"]);
    }

    /// <summary>
    /// A navigation names the related table's class, so a relationship whose parent table
    /// could not be read must not produce one: the child would not compile on its own.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task RunAsync_SkippedParentTable_EmitsNoNavigationToIt()
    {
        await using var reader = new FakeAccessReader(
            tables: ["Parent", "Child"],
            columnsByTable: new() { ["Child"] = [Col("Id", typeof(int)), Col("ParentId", typeof(int))] },
            failingTables: new() { ["Parent"] = new InvalidOperationException("corrupt table") },
            relationships: [new() { Name = "ParentChild", PrimaryTable = "Parent", PrimaryColumns = ["Id"], ForeignTable = "Child", ForeignColumns = ["ParentId"] }]);
        await using var stdout = new StringWriter();
        await using var stderr = new StringWriter();
        var runner = new ScaffoldRunner(reader, stdout, stderr);

        int result = await runner.RunAsync(this.outputDir, "NS", useRecords: false, nullable: true, TestContext.Current.CancellationToken);

        Assert.Equal(1, result);
        string child = await File.ReadAllTextAsync(Path.Combine(this.outputDir, "Child.cs"), TestContext.Current.CancellationToken);
        Type type = ScaffoldCompilation.CompileCleanly(child).GetType("NS.Child", throwOnError: true)!;
        Assert.Equal(["Id", "ParentId"], type.GetProperties().Select(p => p.Name).Order(StringComparer.Ordinal));
    }

    [Theory]
    [InlineData("My-App")]
    [InlineData("1Models")]
    [InlineData("class")]
    [InlineData("MyApp.namespace")]
    [InlineData("MyApp..Models")]
    [InlineData("MyApp.")]
    [InlineData("")]
    public async Task RunAsync_InvalidNamespace_ReturnsMinusOneAndWritesNoFiles(string ns)
    {
        await using var reader = new FakeAccessReader(
            tables: ["Customers"],
            columnsByTable: new() { ["Customers"] = [Col("Id", typeof(int))] });
        await using var stdout = new StringWriter();
        await using var stderr = new StringWriter();
        var runner = new ScaffoldRunner(reader, stdout, stderr);

        int result = await runner.RunAsync(this.outputDir, ns, useRecords: false, nullable: true, TestContext.Current.CancellationToken);

        Assert.Equal(-1, result);
        Assert.Contains($"Error: '{ns}' is not a valid C# namespace.", stderr.ToString(), StringComparison.Ordinal);
        Assert.Empty(Directory.GetFiles(this.outputDir));
    }

    /// <summary>Framework-shaped namespace segments compile through global type references.</summary>
    /// <param name="ns">The namespace.</param>
    /// <param name="columnType">The type of the table's one column besides the key.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData("MyApp.DateTime", typeof(DateTime))]
    [InlineData("X.Hyperlink", typeof(Hyperlink))]
    [InlineData("Guid.Models", typeof(Guid))]
    [InlineData("MyApp.TimeSpan.Data", typeof(TimeSpan))]
    [InlineData("MyApp.Version", typeof(Version))]
    [InlineData("MyApp.Hyperlink", typeof(int))]
    [InlineData("DateOnly.TimeOnly", typeof(string))]
    [InlineData("MyApp.ColumnAttribute", typeof(int))]
    [InlineData("TableAttribute.Models", typeof(int))]
    public async Task RunAsync_NamespaceSegmentNamedLikeGeneratedCodeType_Compiles(string ns, Type columnType)
    {
        ArgumentNullException.ThrowIfNull(columnType);
        string nested = Path.Combine(this.outputDir, "Models");
        await using var reader = new FakeAccessReader(
            tables: ["Orders"],
            columnsByTable: new() { ["Orders"] = [Col("Id", typeof(int)), Col("Value", columnType)] });
        await using var stdout = new StringWriter();
        await using var stderr = new StringWriter();
        var runner = new ScaffoldRunner(reader, stdout, stderr);

        int result = await runner.RunAsync(nested, ns, useRecords: false, nullable: true, TestContext.Current.CancellationToken);

        Assert.Equal(1, result);
        Assert.Empty(stderr.ToString());
        ScaffoldCompilation.CompileCleanly(await File.ReadAllTextAsync(Path.Combine(nested, "Orders.cs"), TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// A namespace segment that names no type the generated code uses works, even when it
    /// is named like a namespace the usings import, an attribute's short name, a generic
    /// collection type, or a reserved type name in another case: the generated files
    /// compile, with the attributes and property types bound to the right types.
    /// </summary>
    /// <param name="ns">The namespace.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData("MyApp.System")]
    [InlineData("MyCompany.JetDatabaseWriter.Models")]
    [InlineData("MyApp.Column")]
    [InlineData("Table.Models")]
    [InlineData("MyApp.ICollection.List")]
    [InlineData("MyApp.datetime")]
    [InlineData("MyApp.Columnattribute")]
    public async Task RunAsync_NamespaceSegmentNamingNoGeneratedCodeType_WritesCompilableFiles(string ns)
    {
        Dictionary<string, List<ColumnMetadata>> columnsByTable = new(StringComparer.Ordinal)
        {
            ["tbl Orders"] = [Col("Order ID", typeof(int)), Col("Order Date", typeof(DateTime)), Col("Key", typeof(Guid)), Col("Link", typeof(Hyperlink)), Col("Photo", typeof(byte[]))],
            ["Order Lines"] = [Col("Id", typeof(int)), Col("Order ID", typeof(int))],
        };
        await using var reader = new FakeAccessReader(
            [.. columnsByTable.Keys],
            columnsByTable,
            relationships: [new() { Name = "OrdersLines", PrimaryTable = "tbl Orders", PrimaryColumns = ["Order ID"], ForeignTable = "Order Lines", ForeignColumns = ["Order ID"] }]);
        await using var stdout = new StringWriter();
        await using var stderr = new StringWriter();
        var runner = new ScaffoldRunner(reader, stdout, stderr);

        int result = await runner.RunAsync(this.outputDir, ns, useRecords: false, nullable: true, TestContext.Current.CancellationToken);

        Assert.Equal(2, result);
        List<string> sources = [];
        foreach (string file in Directory.GetFiles(this.outputDir, "*.cs"))
        {
            sources.Add(await File.ReadAllTextAsync(file, TestContext.Current.CancellationToken));
        }

        Assembly assembly = ScaffoldCompilation.CompileCleanly(sources);
        Type orders = assembly.GetType(ns + ".TblOrders", throwOnError: true)!;
        Type lines = assembly.GetType(ns + ".OrderLines", throwOnError: true)!;
        Assert.Equal("tbl Orders", orders.GetCustomAttribute<TableAttribute>()!.Name);
        Assert.Equal("Order ID", orders.GetProperty("OrderID")!.GetCustomAttribute<ColumnAttribute>()!.Name);
        Assert.Equal(typeof(DateTime?), orders.GetProperty("OrderDate")!.PropertyType);
        Assert.Equal(typeof(Guid?), orders.GetProperty("Key")!.PropertyType);
        Assert.Equal(typeof(Hyperlink), orders.GetProperty("Link")!.PropertyType);
        Assert.Equal(typeof(ICollection<>).MakeGenericType(lines), orders.GetProperty("OrderLines")!.PropertyType);
    }

    private static ColumnMetadata Col(string name, Type clrType) =>
        new() { Name = name, ClrType = clrType, IsNullable = clrType != typeof(byte[]), TypeName = clrType.Name, Size = ColumnSize.FromBytes(4) };

    /// <summary>
    /// Minimal fake implementing only the methods ScaffoldRunner uses.
    /// </summary>
    /// <param name="tables">The tables.</param>
    /// <param name="columnsByTable">The columns by table.</param>
    /// <param name="failingTables">The failing tables.</param>
    /// <param name="relationships">The foreign-key relationships.</param>
    private sealed class FakeAccessReader(
        List<string> tables,
        Dictionary<string, List<ColumnMetadata>>? columnsByTable = null,
        Dictionary<string, Exception>? failingTables = null,
        List<RelationshipMetadata>? relationships = null) : IAccessReader
    {
        private readonly Dictionary<string, List<ColumnMetadata>> columnsByTable = columnsByTable ?? [];
        private readonly Dictionary<string, Exception> failingTables = failingTables ?? [];
        private readonly List<RelationshipMetadata> relationships = relationships ?? [];

        public DatabaseFormat DatabaseFormat => DatabaseFormat.Jet4Mdb;

        public int PageSize => 4096;

        public int CodePage => 1252;

        public bool DiagnosticsEnabled => false;

        public int PageCacheSize => 0;

        public PageReadOptimizationMode PageReadOptimizationMode => PageReadOptimizationMode.Disabled;

        public string LastDiagnostics => string.Empty;

        public ValueTask<IReadOnlyList<string>> ListTablesAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new ValueTask<IReadOnlyList<string>>([.. tables]);
        }

        public ValueTask<TableValidationRule?> GetTableValidationRuleAsync(string tableName, CancellationToken cancellationToken = default) => ValueTask.FromResult<TableValidationRule?>(null);

        public ValueTask<IReadOnlyList<ColumnMetadata>> GetColumnMetadataAsync(string tableName, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (this.failingTables.TryGetValue(tableName, out Exception? ex))
            {
                throw ex;
            }

            if (this.columnsByTable.TryGetValue(tableName, out List<ColumnMetadata>? cols))
            {
                return new ValueTask<IReadOnlyList<ColumnMetadata>>(cols);
            }

            throw new InvalidOperationException($"Table '{tableName}' not configured in fake");
        }

        public ValueTask DisposeAsync() => default;

        public ValueTask<System.Data.DataTable> ReadFirstTableAsStringsAsync(uint? maxRows = null, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<IReadOnlyList<LinkedTableInfo>> ListLinkedTablesAsync(CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<IReadOnlyList<TableStat>> GetTableStatsAsync(CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<System.Data.DataTable> GetTablesAsDataTableAsync(CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<long> GetRealRowCountAsync(string tableName, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<System.Data.DataTable> ReadTableAsync(string? tableName = null, uint? maxRows = null, IProgress<long>? progress = null, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<IReadOnlyList<T>> ReadTableAsync<T>(string tableName, uint? maxRows = null, CancellationToken cancellationToken = default)
            where T : class, new() =>
            throw new NotImplementedException();

        public ValueTask<System.Data.DataTable> ReadTableAsStringsAsync(string tableName, uint? maxRows = null, IProgress<long>? progress = null, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<IReadOnlyList<IndexMetadata>> ListIndexesAsync(string tableName, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<IReadOnlyList<RelationshipMetadata>> ListRelationshipsAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new ValueTask<IReadOnlyList<RelationshipMetadata>>([.. this.relationships]);
        }

        public IAccessIndexQuery<object[]> FromIndex(string tableName, string indexName) =>
            throw new NotImplementedException();

        public IAccessIndexQuery<T> FromIndex<T>(string tableName, string indexName)
            where T : class, new() =>
            throw new NotImplementedException();

        public IQueryable<T> Query<T>(string tableName)
            where T : class, new() =>
            throw new NotImplementedException();

        public IAsyncEnumerable<object[]> SeekRowsAsync(string tableName, string indexName, IReadOnlyList<object?> keyValues, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<IReadOnlyList<ComplexColumnInfo>> GetComplexColumnsAsync(string tableName, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<IReadOnlyList<AttachmentRecord>> GetAttachmentsAsync(string tableName, string columnName, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<IReadOnlyList<MultiValueItem>> GetMultiValueItemsAsync(string tableName, string columnName, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<DatabaseStatistics> GetStatisticsAsync(CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public ValueTask<IReadOnlyDictionary<string, System.Data.DataTable>> ReadAllTablesAsync(IProgress<TableProgress>? progress = null, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public IAsyncEnumerable<object[]> Rows(string tableName, IProgress<long>? progress = null, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();

        public IAsyncEnumerable<T> Rows<T>(string tableName, IProgress<long>? progress = null, CancellationToken cancellationToken = default)
            where T : class, new() =>
            throw new NotImplementedException();

        public IAsyncEnumerable<T> Rows<T>(string tableName, Expression<Func<T, bool>> predicate, IProgress<long>? progress = null, CancellationToken cancellationToken = default)
            where T : class, new() =>
            throw new NotImplementedException();

        public IAsyncEnumerable<string[]> RowsAsStrings(string tableName, IProgress<long>? progress = null, CancellationToken cancellationToken = default) =>
            throw new NotImplementedException();
    }
}
