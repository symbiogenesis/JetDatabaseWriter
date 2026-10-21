namespace JetDatabaseWriter.Tests.Scaffold;

using System;
using System.Collections.Generic;
using System.Linq;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Scaffold;
using Xunit;

/// <summary>
/// <see cref="ScaffoldNames.AllocateClassNames"/> gives every table a class name no other
/// table, ignoring case, has;
/// <see cref="ScaffoldNames.IsValidNamespace"/> accepts only C# namespaces; and
/// generated type references use global qualifications.
/// </summary>
public sealed class ScaffoldNamesTests
{
    [Theory]
    [InlineData("ToString", "ToStringEntity")]
    [InlineData("EqualityContract", "EqualityContractEntity")]
    public void AllocateClassNames_ReservedNames_GetEntitySuffix(string table, string expected) =>
        Assert.Equal(expected, Allocate(table)[table]);

    [Theory]
    [InlineData("Column")]
    [InlineData("Table")]
    [InlineData("Orders")]
    [InlineData("DateTime")]
    [InlineData("Guid")]
    [InlineData("ColumnAttribute")]
    [InlineData("System")]
    [InlineData("Hyperlink")]
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
        Dictionary<string, string> names = Allocate("ToString", "ToStringEntity");

        Assert.Equal("ToStringEntity2", names["ToString"]);
        Assert.Equal("ToStringEntity", names["ToStringEntity"]);
    }

    [Fact]
    public void AllocateClassNames_TypeOfAnyColumn_DoesNotReserveFrameworkNames()
    {
        IReadOnlyList<ColumnMetadata> columns =
        [
            new() { Name = "Build", ClrType = typeof(Version), IsNullable = true, TypeName = "Text", Size = ColumnSize.FromBytes(4) },
        ];

        Dictionary<string, string> names = ScaffoldNames.AllocateClassNames([("Releases", columns), ("Version", [])], "NS");

        Assert.Equal("Version", names["Version"]);
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

    private static Dictionary<string, string> Allocate(params string[] tables) =>
        ScaffoldNames.AllocateClassNames([.. tables.Select(table => (table, (IReadOnlyList<ColumnMetadata>)[]))], "NS");
}
