namespace JetDatabaseWriter.Tests.Scaffold;

using System;
using System.Collections.Generic;
using System.Linq;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Scaffold;
using Xunit;

/// <summary>
/// <see cref="ScaffoldNames.AllocateClassNames"/> gives every table a class name no other
/// table, ignoring case, and no type the generated code names has;
/// <see cref="ScaffoldNames.IsValidNamespace"/> accepts only C# namespaces; and
/// <see cref="ScaffoldNames.FindTypeHidingSegment"/> finds a namespace segment that would
/// hide a type the generated code names.
/// </summary>
public sealed class ScaffoldNamesTests
{
    [Theory]
    [InlineData("Column Attribute", "ColumnAttributeEntity")]
    [InlineData("ColumnAttribute", "ColumnAttributeEntity")]
    [InlineData("Table Attribute", "TableAttributeEntity")]
    [InlineData("System", "SystemEntity")]
    [InlineData("JetDatabaseWriter", "JetDatabaseWriterEntity")]
    [InlineData("DateTime", "DateTimeEntity")]
    [InlineData("date time", "DateTimeEntity")]
    [InlineData("Guid", "GuidEntity")]
    [InlineData("Hyperlink", "HyperlinkEntity")]
    [InlineData("TimeSpan", "TimeSpanEntity")]
    [InlineData("ToString", "ToStringEntity")]
    [InlineData("EqualityContract", "EqualityContractEntity")]
    public void AllocateClassNames_ReservedNames_GetEntitySuffix(string table, string expected) =>
        Assert.Equal(expected, Allocate(table)[table]);

    [Theory]
    [InlineData("Column")]
    [InlineData("Table")]
    [InlineData("Orders")]
    [InlineData("datetime")]
    public void AllocateClassNames_UnreservedNames_AreKept(string table) =>
        Assert.Equal(NameCleaner.ToClassName(table), Allocate(table)[table]);

    [Fact]
    public void AllocateClassNames_ExactNameWinsOverCleanedDuplicate()
    {
        Dictionary<string, string> names = Allocate("Order Details", "OrderDetails");

        Assert.Equal("OrderDetails2", names["Order Details"]);
        Assert.Equal("OrderDetails", names["OrderDetails"]);
    }

    [Fact]
    public void AllocateClassNames_CaseOnlyClash_GetsNumericSuffix()
    {
        Dictionary<string, string> names = Allocate("A_b", "Ab");

        Assert.Equal("AB2", names["A_b"]);
        Assert.Equal("Ab", names["Ab"]);
    }

    [Fact]
    public void AllocateClassNames_UnknownNames_AreDistinct()
    {
        Dictionary<string, string> names = Allocate("###", "$$$", "%%%");

        Assert.Equal(["Unknown", "Unknown2", "Unknown3"], [names["###"], names["$$$"], names["%%%"]]);
    }

    [Fact]
    public void AllocateClassNames_SuffixedNameAlreadyTaken_GetsNumericSuffix()
    {
        Dictionary<string, string> names = Allocate("DateTime", "DateTimeEntity");

        Assert.Equal("DateTimeEntity2", names["DateTime"]);
        Assert.Equal("DateTimeEntity", names["DateTimeEntity"]);
    }

    [Fact]
    public void AllocateClassNames_TypeOfAnyColumn_IsReserved()
    {
        IReadOnlyList<ColumnMetadata> columns =
        [
            new() { Name = "Build", ClrType = typeof(Version), IsNullable = true, TypeName = "Text", Size = ColumnSize.FromBytes(4) },
        ];

        Dictionary<string, string> names = ScaffoldNames.AllocateClassNames([("Releases", columns), ("Version", [])]);

        Assert.Equal("VersionEntity", names["Version"]);
        Assert.Equal("Releases", names["Releases"]);
    }

    [Fact]
    public void AllocateClassNames_LooksUpTablesIgnoringCase()
    {
        Dictionary<string, string> names = Allocate("Order Details");

        Assert.Equal("OrderDetails", names["ORDER DETAILS"]);
    }

    [Theory]
    [InlineData("GeneratedModels")]
    [InlineData("MyApp.Models")]
    [InlineData("MyApp.System")]
    [InlineData("A1._B.Ünïcode")]
    public void IsValidNamespace_ValidNames_ReturnsTrue(string ns) =>
        Assert.True(ScaffoldNames.IsValidNamespace(ns));

    [Theory]
    [InlineData("")]
    [InlineData("My-App")]
    [InlineData("1Models")]
    [InlineData("class")]
    [InlineData("MyApp.namespace")]
    [InlineData("MyApp..Models")]
    [InlineData(".Models")]
    [InlineData("MyApp.")]
    [InlineData("My App")]
    [InlineData("@class")]
    public void IsValidNamespace_InvalidNames_ReturnsFalse(string ns) =>
        Assert.False(ScaffoldNames.IsValidNamespace(ns));

    [Theory]
    [InlineData("MyApp.DateTime", "DateTime")]
    [InlineData("Guid.Models", "Guid")]
    [InlineData("A.Hyperlink.B.DateTimeOffset", "Hyperlink")]
    [InlineData("MyApp.TimeSpan", "TimeSpan")]
    [InlineData("MyApp.DateOnly", "DateOnly")]
    [InlineData("MyApp.TimeOnly", "TimeOnly")]
    [InlineData("MyApp.ColumnAttribute", "ColumnAttribute")]
    [InlineData("TableAttribute", "TableAttribute")]
    public void FindTypeHidingSegment_GeneratedCodeTypeName_ReturnsFirstSuchSegment(string ns, string expected) =>
        Assert.Equal(expected, ScaffoldNames.FindTypeHidingSegment(ns, []));

    [Theory]
    [InlineData("GeneratedModels")]
    [InlineData("MyApp.System")]
    [InlineData("MyCompany.JetDatabaseWriter.Models")]
    [InlineData("MyApp.Column.Table")]
    [InlineData("MyApp.ICollection.List")]
    [InlineData("MyApp.datetime")]
    [InlineData("MyApp.ToString")]
    [InlineData("MyApp.Version")]
    public void FindTypeHidingSegment_OtherSegments_ReturnsNull(string ns) =>
        Assert.Null(ScaffoldNames.FindTypeHidingSegment(ns, []));

    [Fact]
    public void FindTypeHidingSegment_TypeOfAnyColumn_IsFound()
    {
        IReadOnlyList<ColumnMetadata> columns =
        [
            new() { Name = "Build", ClrType = typeof(Version), IsNullable = true, TypeName = "Text", Size = ColumnSize.FromBytes(4) },
            new() { Name = "Address", ClrType = typeof(Uri[]), IsNullable = true, TypeName = "Text", Size = ColumnSize.FromBytes(4) },
        ];

        Assert.Equal("Version", ScaffoldNames.FindTypeHidingSegment("MyApp.Version", [("Releases", columns)]));
        Assert.Equal("Uri", ScaffoldNames.FindTypeHidingSegment("Uri.Models", [("Empty", []), ("Releases", columns)]));
        Assert.Null(ScaffoldNames.FindTypeHidingSegment("MyApp.Build", [("Releases", columns)]));
    }

    private static Dictionary<string, string> Allocate(params string[] tables) =>
        ScaffoldNames.AllocateClassNames([.. tables.Select(table => (table, (IReadOnlyList<ColumnMetadata>)[]))]);
}
