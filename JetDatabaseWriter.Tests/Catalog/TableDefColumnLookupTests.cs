namespace JetDatabaseWriter.Tests.Catalog;

using System.Collections.Generic;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Schema.Models;
using Xunit;
using static JetDatabaseWriter.Enums.ColumnType;

public sealed class TableDefColumnLookupTests
{
    [Fact]
    public void FindColumn_ResolvesNamesCaseInsensitively()
    {
        var first = new ColumnInfo { Name = "RecordId", Type = LongIntegerType };
        var second = new ColumnInfo { Name = "DisplayName", Type = TextType };
        var definition = new TableDef { Columns = [first, second] };

        Assert.Equal(0, definition.FindColumnIndex("RECORDID"));
        Assert.Equal(1, definition.FindColumnIndex("displayname"));
        Assert.Same(first, definition.FindColumn("recordid"));
        Assert.Same(second, definition.FindColumn("DISPLAYNAME"));
        Assert.Equal(-1, definition.FindColumnIndex("missing"));
        Assert.Null(definition.FindColumn("missing"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void FindColumn_EmptyDefinitionHasNoMatches(bool initializeColumns)
    {
        TableDef definition = initializeColumns ? new TableDef { Columns = [] } : new TableDef();

        Assert.Equal(-1, definition.FindColumnIndex("missing"));
        Assert.Null(definition.FindColumn("missing"));
        Assert.Equal(-1, definition.FindColumnIndex((string)null!));
        Assert.Null(definition.FindColumn(null!));
    }

    [Fact]
    public void FindColumn_DuplicateNamesReturnFirstColumn()
    {
        var first = new ColumnInfo { Name = "Value", Type = LongIntegerType };
        var second = new ColumnInfo { Name = "VALUE", Type = TextType };
        var empty = new ColumnInfo { Name = string.Empty, Type = TextType };
        var definition = new TableDef { Columns = [first, second, empty] };

        Assert.Equal(0, definition.FindColumnIndex("value"));
        Assert.Same(first, definition.FindColumn("vAlUe"));
        Assert.Equal(2, definition.FindColumnIndex(string.Empty));
        Assert.Same(empty, definition.FindColumn(string.Empty));
    }

    [Fact]
    public void FindColumn_NullNameRetainsFirstMatch()
    {
        var first = new ColumnInfo { Name = null!, Type = LongIntegerType };
        var named = new ColumnInfo { Name = "Value", Type = TextType };
        var second = new ColumnInfo { Name = null!, Type = TextType };
        var definition = new TableDef { Columns = [first, named, second] };

        Assert.Equal(0, definition.FindColumnIndex((string)null!));
        Assert.Same(first, definition.FindColumn(null!));
        Assert.Equal(1, definition.FindColumnIndex("VALUE"));
        Assert.Same(named, definition.FindColumn("value"));
    }

    [Fact]
    public void FindColumn_UsesOwnedColumnSnapshot()
    {
        var original = new ColumnInfo { Name = "Original", Type = LongIntegerType };
        List<ColumnInfo> columns = [original];
        var definition = new TableDef { Columns = columns };

        columns[0] = original with { Name = "Replacement" };
        columns.Add(new ColumnInfo { Name = "Added", Type = TextType });

        Assert.Single(definition.Columns);
        Assert.Equal(0, definition.FindColumnIndex("ORIGINAL"));
        Assert.Same(original, definition.FindColumn("original"));
        Assert.Equal(-1, definition.FindColumnIndex("Replacement"));
        Assert.Null(definition.FindColumn("Added"));
    }
}
