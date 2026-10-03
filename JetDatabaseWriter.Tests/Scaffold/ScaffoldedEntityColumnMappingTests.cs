namespace JetDatabaseWriter.Tests.Scaffold;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Scaffold;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Emit;
using Xunit;

/// <summary>
/// Round-trip tests for scaffolded entities whose table and column names are not valid C#
/// identifiers (<c>tbl People</c>, <c>Person ID</c>, <c>Last Name</c>). The scaffolder emits
/// <c>[Table("...")]</c> and <c>[Column("...")]</c> for every renamed name; the generated source
/// is compiled and the resulting type is used to insert into and read back the original table.
/// </summary>
public sealed class ScaffoldedEntityColumnMappingTests
{
    private const string TableName = "tbl People";

    [Fact]
    public void Emit_RenamedColumnsAndTable_EmitsMappingAttributes()
    {
        string source = EntityEmitter.Emit("TblPeople", TableName, PeopleColumns(), [], "NS", useRecords: false, nullable: true);

        Assert.Contains("using System.ComponentModel.DataAnnotations.Schema;", source, StringComparison.Ordinal);
        Assert.Contains("[Table(\"tbl People\")]", source, StringComparison.Ordinal);
        Assert.Contains("[Column(\"Person ID\")]", source, StringComparison.Ordinal);
        Assert.Contains("[Column(\"Last Name\")]", source, StringComparison.Ordinal);

        // "Note" already binds by name, so it needs no attribute.
        Assert.DoesNotContain("[Column(\"Note\")]", source, StringComparison.Ordinal);
        Assert.DoesNotContain("\r\n\r\n\r\n", source, StringComparison.Ordinal);
    }

    [Fact]
    public void Emit_MatchingNames_EmitsNoMappingAttributes()
    {
        List<ColumnMetadata> columns =
        [
            new() { Name = "Id", ClrType = typeof(int), IsNullable = false, TypeName = "Long Integer", Size = ColumnSize.FromBytes(4) },
            new() { Name = "name", ClrType = typeof(string), IsNullable = true, TypeName = "Text", Size = ColumnSize.FromBytes(50) },
        ];

        string source = EntityEmitter.Emit("Customer", "customer", columns, [], "NS", useRecords: false, nullable: false);

        Assert.DoesNotContain("[Column(", source, StringComparison.Ordinal);
        Assert.DoesNotContain("[Table(", source, StringComparison.Ordinal);
        Assert.DoesNotContain("DataAnnotations", source, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ScaffoldedEntity_RoundTripsThroughInsertAndRead()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = new();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new("Person ID", typeof(int)) { IsPrimaryKey = true },
                    new("Last Name", typeof(string), maxLength: 50),
                    new("Note", typeof(string), maxLength: 50),
                ],
                ct);
            await writer.InsertRowAsync(TableName, [1, "Smith", "n1"], ct);
        }

        ms.Position = 0;
        IReadOnlyList<ColumnMetadata> columns;
        await using (AccessReader schemaReader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            columns = await schemaReader.GetColumnMetadataAsync(TableName, ct);
        }

        string className = NameCleaner.ToClassName(TableName);
        string source = EntityEmitter.Emit(className, TableName, columns, [], "Generated", useRecords: false, nullable: true);
        Type entityType = Compile(source).GetType("Generated." + className, throwOnError: true)!;
        PropertyInfo personId = entityType.GetProperty("PersonID")!;
        PropertyInfo lastName = entityType.GetProperty("LastName")!;

        // Insert through the generated type.
        object jones = entityType.GetConstructor(Type.EmptyTypes)!.Invoke(null);
        personId.SetValue(jones, 2);
        lastName.SetValue(jones, "Jones");
        ms.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(ms, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            MethodInfo insert = typeof(AccessWriter).GetMethods()
                .Single(m => m.Name == nameof(AccessWriter.InsertRowAsync) && m.IsGenericMethodDefinition)
                .MakeGenericMethod(entityType);
            await (ValueTask)insert.Invoke(writer, [TableName, jones, ct])!;
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);

        // The raw row shows the generated type wrote the renamed column.
        List<object[]> raw = await reader.Rows(TableName, cancellationToken: ct).ToListAsync(ct);
        Assert.Equal("Jones", raw.Single(r => (int)r[0] == 2)[1]);

        // Rows<T> reads it back through the [Column] attributes.
        MethodInfo rows = typeof(AccessReader).GetMethods()
            .Single(m => m.Name == nameof(AccessReader.Rows) && m.IsGenericMethodDefinition && m.GetParameters().Length == 3)
            .MakeGenericMethod(entityType);
        var typed = (IAsyncEnumerable<object>)rows.Invoke(reader, [TableName, null, ct])!;
        List<object> entities = await typed.ToListAsync(ct);
        Assert.Equal(["Jones", "Smith"], entities.Select(e => (string?)lastName.GetValue(e)).Order(StringComparer.Ordinal));
    }

    private static List<ColumnMetadata> PeopleColumns() =>
    [
        new() { Name = "Person ID", ClrType = typeof(int), IsNullable = false, TypeName = "Long Integer", Size = ColumnSize.FromBytes(4) },
        new() { Name = "Last Name", ClrType = typeof(string), IsNullable = true, TypeName = "Text", Size = ColumnSize.FromBytes(50) },
        new() { Name = "Note", ClrType = typeof(string), IsNullable = true, TypeName = "Text", Size = ColumnSize.FromBytes(50) },
    ];

    private static Assembly Compile(string source)
    {
        string trustedAssemblies = (string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!;
        IEnumerable<MetadataReference> references = trustedAssemblies
            .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries)
            .Append(typeof(AccessReader).Assembly.Location)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Select(path => MetadataReference.CreateFromFile(path));

        var compilation = CSharpCompilation.Create(
            "ScaffoldedEntities_" + Guid.NewGuid().ToString("N"),
            [CSharpSyntaxTree.ParseText(source)],
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));

        using var image = new MemoryStream();
        EmitResult result = compilation.Emit(image);
        Assert.True(result.Success, string.Join(Environment.NewLine, result.Diagnostics) + Environment.NewLine + source);
        return Assembly.Load(image.ToArray());
    }
}
