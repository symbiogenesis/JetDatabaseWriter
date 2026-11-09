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
    [InlineData("CON", "CONEntity")]
    [InlineData("con", "ConEntity")]
    [InlineData("PRN", "PRNEntity")]
    [InlineData("AUX", "AUXEntity")]
    [InlineData("NUL", "NULEntity")]
    [InlineData("nul", "NulEntity")]
    [InlineData("COM1", "COM1Entity")]
    [InlineData("COM2", "COM2Entity")]
    [InlineData("COM3", "COM3Entity")]
    [InlineData("COM4", "COM4Entity")]
    [InlineData("COM5", "COM5Entity")]
    [InlineData("COM6", "COM6Entity")]
    [InlineData("COM7", "COM7Entity")]
    [InlineData("COM8", "COM8Entity")]
    [InlineData("COM9", "COM9Entity")]
    [InlineData("com1", "Com1Entity")]
    [InlineData("LPT1", "LPT1Entity")]
    [InlineData("LPT2", "LPT2Entity")]
    [InlineData("LPT3", "LPT3Entity")]
    [InlineData("LPT4", "LPT4Entity")]
    [InlineData("LPT5", "LPT5Entity")]
    [InlineData("LPT6", "LPT6Entity")]
    [InlineData("LPT7", "LPT7Entity")]
    [InlineData("LPT8", "LPT8Entity")]
    [InlineData("LPT9", "LPT9Entity")]
    [InlineData("lpt9", "Lpt9Entity")]
    public void AllocateClassNames_DeviceNames_GetEntitySuffix(string table, string expected) =>
        Assert.Equal(expected, Allocate(table)[table]);

    [Fact]
    public void AllocateClassNames_DeviceSuffixAlreadyTaken_GetsNumericSuffix()
    {
        Dictionary<string, string> names = Allocate("con", "ConEntity", "ConEntity2", "c_on");

        Assert.Equal("ConEntity3", names["con"]);
        Assert.Equal("ConEntity", names["ConEntity"]);
        Assert.Equal("ConEntity2", names["ConEntity2"]);
        Assert.Equal("COnEntity4", names["c_on"]);
    }

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
    [InlineData("CONEntity")]
    [InlineData("COM0")]
    [InlineData("COM10")]
    [InlineData("LPT0")]
    [InlineData("LPT10")]
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
