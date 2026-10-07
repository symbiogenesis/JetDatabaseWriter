namespace JetDatabaseWriter.Tests.Scaffold;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Scaffold;
using Microsoft.CodeAnalysis.CSharp;
using Xunit;

/// <summary>
/// <see cref="EntityEmitter"/> output compiles, without warnings, whatever the
/// column names and the namespace are: names that would hide or clash with
/// object and record members get a suffix, the usings sit above the
/// namespace so a namespace segment named <c>System</c> or
/// <c>JetDatabaseWriter</c> cannot capture them, and the code names nothing
/// that a property could shadow.
/// </summary>
public sealed class EntityEmitterIdentifierTests
{
    [Theory]
    [InlineData("MyApp.DateTime", false)]
    [InlineData("Guid.Models", true)]
    [InlineData("MyApp.ColumnAttribute.Hyperlink", false)]
    [InlineData("MyApp.System.JetDatabaseWriter", true)]
    public void Emit_FrameworkShapedNames_BindTypesAttributesAndNavigations(string ns, bool useRecords)
    {
        string[] names = ["DateTime", "Guid", "Hyperlink", "ColumnAttribute", "TableAttribute", "System", "JetDatabaseWriter", "ICollection", "List", "Column", "Table", "DateTimeOffset", "TimeSpan"];
        List<ColumnMetadata> columns =
        [
            Column("Created On", typeof(DateTime), isNullable: false),
            Column("Identity", typeof(Guid), isNullable: false),
            Column("Website", typeof(Hyperlink)),
        ];
        var tables = names.Select(name => (name, (IReadOnlyList<ColumnMetadata>)columns)).ToArray();
        Dictionary<string, string> classNames = ScaffoldNames.AllocateClassNames(tables);
        Assert.Equal(names, names.Select(name => classNames[name]));
        List<string> sources = [.. names.Select(name => EntityEmitter.Emit(name, name + " Table", columns,
            [new(IsCollection: true, TargetClassName: "Guid", PreferredName: "Children"), new(IsCollection: false, TargetClassName: "DateTime", PreferredName: "Parent")],
            ns, useRecords, nullable: true))];
        Assembly assembly = ScaffoldCompilation.CompileCleanly(sources);
        Type entity = assembly.GetType(ns + ".Hyperlink", throwOnError: true)!;
        Assert.Equal(typeof(DateTime), entity.GetProperty("CreatedOn")!.PropertyType);
        Assert.Equal(typeof(Guid), entity.GetProperty("Identity")!.PropertyType);
        Assert.Equal(typeof(Hyperlink), entity.GetProperty("Website")!.PropertyType);
        Assert.Equal(ns + ".DateTime", entity.GetProperty("Parent")!.PropertyType.FullName);
        Assert.Equal(ns + ".Guid", entity.GetProperty("Children")!.PropertyType.GetGenericArguments()[0].FullName);
        Assert.Equal("Created On", entity.GetProperty("CreatedOn")!.GetCustomAttribute<System.ComponentModel.DataAnnotations.Schema.ColumnAttribute>()!.Name);
        Assert.Equal("Hyperlink Table", entity.GetCustomAttribute<System.ComponentModel.DataAnnotations.Schema.TableAttribute>()!.Name);
    }

    [Fact]
    public void Emit_ColumnNamedArrayWithNonNullBlob_Compiles()
    {
        List<ColumnMetadata> columns =
        [
            Column("Array", typeof(int), isNullable: false),
            Column("Data", typeof(byte[]), isNullable: false),
        ];

        string source = EntityEmitter.Emit("Item", "Item", columns, [], "NS", useRecords: false, nullable: true);

        Assembly assembly = ScaffoldCompilation.CompileCleanly(source);
        object item = assembly.GetType("NS.Item", throwOnError: true)!.GetConstructor(Type.EmptyTypes)!.Invoke(null);
        Assert.Equal(typeof(int), item.GetType().GetProperty("Array")!.PropertyType);
        Assert.Equal(Array.Empty<byte>(), item.GetType().GetProperty("Data")!.GetValue(item));
    }

    [Fact]
    public void Emit_ColumnsClassValueAndClass_DeduplicatesSuffixedName()
    {
        List<ColumnMetadata> columns =
        [
            Column("CustomerValue", typeof(int)),
            Column("Customer", typeof(string)),
        ];

        string source = EntityEmitter.Emit("Customer", "Customer", columns, [], "NS", useRecords: false, nullable: true);

        Type type = ScaffoldCompilation.CompileCleanly(source).GetType("NS.Customer", throwOnError: true)!;
        Assert.Equal(typeof(int?), type.GetProperty("CustomerValue")!.PropertyType);
        Assert.Equal(typeof(string), type.GetProperty("CustomerValue2")!.PropertyType);
        Assert.Equal("Customer", type.GetProperty("CustomerValue2")!.GetCustomAttribute<System.ComponentModel.DataAnnotations.Schema.ColumnAttribute>()!.Name);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Emit_ObjectMemberColumnNames_CompileWithoutWarnings(bool useRecords)
    {
        string[] names = ["ToString", "Equals", "GetHashCode", "GetType", "MemberwiseClone", "ReferenceEquals", "EqualityContract", "PrintMembers", "Clone"];
        List<ColumnMetadata> columns = [.. names.Select(n => Column(n, typeof(int)))];

        string source = EntityEmitter.Emit("Item", "Item", columns, [], "NS", useRecords, nullable: true);

        Type type = ScaffoldCompilation.CompileCleanly(source).GetType("NS.Item", throwOnError: true)!;
        foreach (string name in names)
        {
            PropertyInfo property = type.GetProperty(name + "Value")!;
            Assert.Equal(typeof(int?), property.PropertyType);
            Assert.Equal(name, property.GetCustomAttribute<System.ComponentModel.DataAnnotations.Schema.ColumnAttribute>()!.Name);
        }
    }

    [Fact]
    public void Emit_NavigationNamedLikeObjectMember_GetsSuffix()
    {
        List<ScaffoldNavigation> navigations = [new(IsCollection: false, TargetClassName: "Parent", PreferredName: "GetType")];

        string child = EntityEmitter.Emit("Child", "Child", [Column("Id", typeof(int))], navigations, "NS", useRecords: false, nullable: true);
        string parent = EntityEmitter.Emit("Parent", "Parent", [Column("Id", typeof(int))], [], "NS", useRecords: false, nullable: true);

        Type type = ScaffoldCompilation.CompileCleanly(child, parent).GetType("NS.Child", throwOnError: true)!;
        Assert.Equal("Parent", type.GetProperty("GetTypeNavigation")!.PropertyType.Name);
    }

    /// <summary>
    /// A column name holding a character that ends a C# comment line (a line break,
    /// U+0085, U+2028 or U+2029) or a character XML cannot hold still gives a doc
    /// comment on one line, and the <c>[Column]</c> attribute keeps the exact name.
    /// </summary>
    /// <param name="name">The column name.</param>
    [Theory]
    [InlineData("Line\nBreak")]
    [InlineData("Cr\r\nLf")]
    [InlineData("Tab\tAnd\u0001")]
    [InlineData("Next\u0085Line")]
    [InlineData("Line\u2028Sep")]
    [InlineData("Para\u2029Sep")]
    public void Emit_ColumnNameWithControlCharacters_KeepsDocCommentOnOneLine(string name)
    {
        string source = EntityEmitter.Emit("Item", "Item", [Column(name, typeof(int))], [], "NS", useRecords: false, nullable: true);

        Type type = ScaffoldCompilation.CompileCleanly(source).GetType("NS.Item", throwOnError: true)!;
        PropertyInfo property = Assert.Single(type.GetProperties());
        Assert.Equal(name, property.GetCustomAttribute<System.ComponentModel.DataAnnotations.Schema.ColumnAttribute>()!.Name);
        string summary = Assert.Single(source.Split("\r\n"), line => line.Contains("<summary>", StringComparison.Ordinal));
        Assert.EndsWith("</summary>", summary, StringComparison.Ordinal);
        Assert.DoesNotContain(summary, SyntaxFacts.IsNewLine);
    }

    [Fact]
    public void Emit_NamespaceWithSystemSegment_Compiles()
    {
        List<ColumnMetadata> columns =
        [
            Column("When", typeof(DateTime)),
            Column("Key", typeof(Guid)),
            Column("Blob", typeof(byte[]), isNullable: false),
        ];

        string source = EntityEmitter.Emit("Item", "Item", columns, [], "MyApp.System", useRecords: false, nullable: true);

        Type type = ScaffoldCompilation.CompileCleanly(source).GetType("MyApp.System.Item", throwOnError: true)!;
        Assert.Equal(typeof(DateTime?), type.GetProperty("When")!.PropertyType);
        Assert.Equal(typeof(Guid?), type.GetProperty("Key")!.PropertyType);
    }

    [Fact]
    public void Emit_NamespaceWithLibrarySegment_Compiles()
    {
        string source = EntityEmitter.Emit("Site", "Site", [Column("Website", typeof(Hyperlink))], [], "MyCompany.JetDatabaseWriter.Models", useRecords: false, nullable: true);

        Type type = ScaffoldCompilation.CompileCleanly(source).GetType("MyCompany.JetDatabaseWriter.Models.Site", throwOnError: true)!;
        Assert.Equal(typeof(Hyperlink), type.GetProperty("Website")!.PropertyType);
    }

    /// <summary>
    /// The whole layout: header, usings, namespace and type, one blank line between
    /// groups and between members, CRLF throughout, one final line break.
    /// </summary>
    [Fact]
    public void Emit_GoldenLayout_NullableOn_MatchesExpectedText()
    {
        string source = EntityEmitter.Emit("TblPeople", "tbl People", GoldenColumns(), GoldenNavigations(), "MyApp.Models", useRecords: false, nullable: true);

        const string expected =
            "// <auto-generated />\r\n" +
            "#nullable enable\r\n" +
            "\r\n" +
            "using System;\r\n" +
            "using System.Collections.Generic;\r\n" +
            "using System.ComponentModel.DataAnnotations.Schema;\r\n" +
            "using JetDatabaseWriter.Models;\r\n" +
            "\r\n" +
            "namespace MyApp.Models;\r\n" +
            "\r\n" +
            "[global::System.ComponentModel.DataAnnotations.Schema.TableAttribute(\"tbl People\")]\r\n" +
            "public sealed class TblPeople\r\n" +
            "{\r\n" +
            "    /// <summary>Column: Person ID (Long Integer, 4 bytes).</summary>\r\n" +
            "    [global::System.ComponentModel.DataAnnotations.Schema.ColumnAttribute(\"Person ID\")]\r\n" +
            "    public int PersonID { get; set; }\r\n" +
            "\r\n" +
            "    /// <summary>Column: Photo (OLE Object, LVAL).</summary>\r\n" +
            "    public byte[] Photo { get; set; } = global::System.Array.Empty<byte>();\r\n" +
            "\r\n" +
            "    /// <summary>Column: Website (Hyperlink, LVAL).</summary>\r\n" +
            "    public global::JetDatabaseWriter.Models.Hyperlink? Website { get; set; }\r\n" +
            "\r\n" +
            "    /// <summary>Navigation: related Orders children.</summary>\r\n" +
            "    public global::System.Collections.Generic.ICollection<global::MyApp.Models.Orders> Orders { get; set; } = new global::System.Collections.Generic.List<global::MyApp.Models.Orders>();\r\n" +
            "}\r\n";
        Assert.Equal(expected, source);
    }

    [Fact]
    public void Emit_GoldenLayout_NullableOff_MatchesExpectedText()
    {
        string source = EntityEmitter.Emit("TblPeople", "tbl People", GoldenColumns(), GoldenNavigations(), "MyApp.Models", useRecords: false, nullable: false);

        const string expected =
            "// <auto-generated />\r\n" +
            "\r\n" +
            "using System;\r\n" +
            "using System.Collections.Generic;\r\n" +
            "using System.ComponentModel.DataAnnotations.Schema;\r\n" +
            "using JetDatabaseWriter.Models;\r\n" +
            "\r\n" +
            "namespace MyApp.Models;\r\n" +
            "\r\n" +
            "[global::System.ComponentModel.DataAnnotations.Schema.TableAttribute(\"tbl People\")]\r\n" +
            "public sealed class TblPeople\r\n" +
            "{\r\n" +
            "    /// <summary>Column: Person ID (Long Integer, 4 bytes).</summary>\r\n" +
            "    [global::System.ComponentModel.DataAnnotations.Schema.ColumnAttribute(\"Person ID\")]\r\n" +
            "    public int PersonID { get; set; }\r\n" +
            "\r\n" +
            "    /// <summary>Column: Photo (OLE Object, LVAL).</summary>\r\n" +
            "    public byte[] Photo { get; set; }\r\n" +
            "\r\n" +
            "    /// <summary>Column: Website (Hyperlink, LVAL).</summary>\r\n" +
            "    public global::JetDatabaseWriter.Models.Hyperlink Website { get; set; }\r\n" +
            "\r\n" +
            "    /// <summary>Navigation: related Orders children.</summary>\r\n" +
            "    public global::System.Collections.Generic.ICollection<global::MyApp.Models.Orders> Orders { get; set; } = new global::System.Collections.Generic.List<global::MyApp.Models.Orders>();\r\n" +
            "}\r\n";
        Assert.Equal(expected, source);
    }

    private static List<ColumnMetadata> GoldenColumns() =>
    [
        new() { Name = "Person ID", ClrType = typeof(int), IsNullable = false, TypeName = "Long Integer", Size = ColumnSize.FromBytes(4) },
        new() { Name = "Photo", ClrType = typeof(byte[]), IsNullable = false, TypeName = "OLE Object", Size = ColumnSize.Lval },
        new() { Name = "Website", ClrType = typeof(Hyperlink), IsNullable = true, TypeName = "Hyperlink", Size = ColumnSize.Lval },
    ];

    private static List<ScaffoldNavigation> GoldenNavigations() => [new(IsCollection: true, TargetClassName: "Orders", PreferredName: "Orders")];

    private static ColumnMetadata Column(string name, Type clrType, bool isNullable = true) =>
        new() { Name = name, ClrType = clrType, IsNullable = isNullable, TypeName = clrType.Name, Size = ColumnSize.FromBytes(4) };
}
