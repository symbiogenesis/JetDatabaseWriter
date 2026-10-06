namespace JetDatabaseWriter.Tests.ValueDecoding;

using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

#pragma warning disable CA1812 // RowMapper constructs this POCO through compiled expressions.

/// <summary>Generated-key flags distinguish typed insert delegates.</summary>
public sealed class TypedInsertShapeTests
{
    /// <summary>Zero generates a key only for the AutoNumber table.</summary>
    [Fact]
    public void AutoNumberFlag_IsPartOfInsertShape()
    {
        var generated = new TableDef { Columns = [new ColumnInfo { Name = "Id", Type = ColumnType.LongIntegerType, Flags = 5 }] };
        var explicitValue = new TableDef { Columns = [new ColumnInfo { Name = "Id", Type = ColumnType.LongIntegerType, Flags = 1 }] };
        Assert.NotEqual(generated.Shape, explicitValue.Shape);
        Assert.Same(DbDefault.Value, RowMapper<Entity>.ToRow(generated, new Entity())[0]);
        Assert.Equal(0, RowMapper<Entity>.ToRow(explicitValue, new Entity())[0]);
        Assert.Equal(7, RowMapper<Entity>.ToRow(generated, new Entity { Id = 7 })[0]);
    }

    private sealed class Entity
    {
        public int Id { get; set; }
    }
}
