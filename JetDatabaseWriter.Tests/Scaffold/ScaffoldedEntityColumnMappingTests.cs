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
        Assert.Contains("[global::System.ComponentModel.DataAnnotations.Schema.TableAttribute(\"tbl People\")]", source, StringComparison.Ordinal);
        Assert.Contains("[global::System.ComponentModel.DataAnnotations.Schema.ColumnAttribute(\"Person ID\")]", source, StringComparison.Ordinal);
        Assert.Contains("[global::System.ComponentModel.DataAnnotations.Schema.ColumnAttribute(\"Last Name\")]", source, StringComparison.Ordinal);

        // "Note" already binds by name, so it needs no attribute.
        Assert.DoesNotContain("[global::System.ComponentModel.DataAnnotations.Schema.ColumnAttribute(\"Note\")]", source, StringComparison.Ordinal);
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

        Assert.DoesNotContain("[global::System.ComponentModel.DataAnnotations.Schema.ColumnAttribute(", source, StringComparison.Ordinal);
        Assert.DoesNotContain("[global::System.ComponentModel.DataAnnotations.Schema.TableAttribute(", source, StringComparison.Ordinal);
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
        Type entityType = ScaffoldCompilation.CompileCleanly(source).GetType("Generated." + className, throwOnError: true)!;
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

    /// <summary>
    /// A table named "Column Attribute" used to give a class named ColumnAttribute, which
    /// captured every <c>[Column]</c> in its namespace, and a table named DateTime a class
    /// that typed every date column as itself. Global qualifications preserve their natural class names, and the
    /// generated types insert and read through the original tables.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task ScaffoldedEntity_TableNamedColumnAttribute_RoundTrips()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = new();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("Column Attribute", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Last Name", typeof(string), maxLength: 50)], ct);
            await writer.CreateTableAsync("DateTime", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("When", typeof(DateTime))], ct);
        }

        ms.Position = 0;
        List<(string Table, IReadOnlyList<ColumnMetadata> Columns)> tables = [];
        await using (AccessReader schemaReader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            foreach (string table in await schemaReader.ListTablesAsync(ct))
            {
                tables.Add((table, await schemaReader.GetColumnMetadataAsync(table, ct)));
            }
        }

        Dictionary<string, string> classNames = ScaffoldNames.AllocateClassNames(tables);
        Assert.Equal("ColumnAttribute", classNames["Column Attribute"]);
        Assert.Equal("DateTime", classNames["DateTime"]);

        Assembly assembly = ScaffoldCompilation.CompileCleanly(
            tables.Select(t => EntityEmitter.Emit(classNames[t.Table], t.Table, t.Columns, [], "Generated", useRecords: false, nullable: true)));
        Type people = assembly.GetType("Generated.ColumnAttribute", throwOnError: true)!;
        Type dates = assembly.GetType("Generated.DateTime", throwOnError: true)!;
        Assert.Equal(typeof(DateTime?), dates.GetProperty("When")!.PropertyType);

        object person = people.GetConstructor(Type.EmptyTypes)!.Invoke(null);
        people.GetProperty("Id")!.SetValue(person, 1);
        people.GetProperty("LastName")!.SetValue(person, "Smith");
        object date = dates.GetConstructor(Type.EmptyTypes)!.Invoke(null);
        dates.GetProperty("Id")!.SetValue(date, 1);
        dates.GetProperty("When")!.SetValue(date, new DateTime(2024, 2, 29, 8, 30, 0, DateTimeKind.Unspecified));

        ms.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(ms, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            MethodInfo insert = typeof(AccessWriter).GetMethods()
                .Single(m => m.Name == nameof(AccessWriter.InsertRowAsync) && m.IsGenericMethodDefinition);
            await (ValueTask)insert.MakeGenericMethod(people).Invoke(writer, ["Column Attribute", person, ct])!;
            await (ValueTask)insert.MakeGenericMethod(dates).Invoke(writer, ["DateTime", date, ct])!;
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        MethodInfo rows = typeof(AccessReader).GetMethods()
            .Single(m => m.Name == nameof(AccessReader.Rows) && m.IsGenericMethodDefinition && m.GetParameters().Length == 3);
        object readPerson = Assert.Single(await ((IAsyncEnumerable<object>)rows.MakeGenericMethod(people).Invoke(reader, ["Column Attribute", null, ct])!).ToListAsync(ct));
        Assert.Equal("Smith", people.GetProperty("LastName")!.GetValue(readPerson));
        object readDate = Assert.Single(await ((IAsyncEnumerable<object>)rows.MakeGenericMethod(dates).Invoke(reader, ["DateTime", null, ct])!).ToListAsync(ct));
        Assert.Equal(new DateTime(2024, 2, 29, 8, 30, 0, DateTimeKind.Unspecified), dates.GetProperty("When")!.GetValue(readDate));
    }

    private static List<ColumnMetadata> PeopleColumns() =>
    [
        new() { Name = "Person ID", ClrType = typeof(int), IsNullable = false, TypeName = "Long Integer", Size = ColumnSize.FromBytes(4) },
        new() { Name = "Last Name", ClrType = typeof(string), IsNullable = true, TypeName = "Text", Size = ColumnSize.FromBytes(50) },
        new() { Name = "Note", ClrType = typeof(string), IsNullable = true, TypeName = "Text", Size = ColumnSize.FromBytes(50) },
    ];
}
