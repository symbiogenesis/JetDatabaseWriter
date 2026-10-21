namespace JetDatabaseWriter.Tests.Relationships;

using System;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema.Models;
using Xunit;

public sealed class RelationshipKeyBuilderTests
{
    [Fact]
    public void ForeignKeyExemption_DiffersFromNullableParentDependencyKey()
    {
        var definition = new TableDef
        {
            Columns = [new ColumnInfo { Name = "A", Type = ColumnType.LongIntegerType }, new ColumnInfo { Name = "B", Type = ColumnType.LongIntegerType }],
        };
        Assert.Null(RelationshipKeyBuilder.BuildForeignKey([null, DBNull.Value], [0, 1], definition));
        Assert.NotNull(RelationshipKeyBuilder.BuildKey([null, DBNull.Value], [0, 1], definition));
        Assert.Equal(RelationshipKeyBuilder.BuildKey([null, 7], [0, 1], definition), RelationshipKeyBuilder.BuildForeignKey([DBNull.Value, 7], [0, 1], definition));
        Assert.NotEqual(RelationshipKeyBuilder.BuildKey([null, 7], [0, 1]), RelationshipKeyBuilder.BuildKey([7, null], [0, 1]));
    }

    [Fact]
    public void CompositeTextKeys_CannotMoveSeparatorBetweenColumns()
    {
        string? first = RelationshipKeyBuilder.BuildKey(["a|S:b", "c"], [0, 1]);
        string? second = RelationshipKeyBuilder.BuildKey(["a", "b|S:c"], [0, 1]);
        Assert.NotEqual(first, second);
    }

    [Fact]
    public void CompositeTextKeys_RetainCaseInsensitiveMatching()
        => Assert.Equal(
            RelationshipKeyBuilder.BuildKey(["alpha", "BETA"], [0, 1]),
            RelationshipKeyBuilder.BuildKey(["ALPHA", "beta"], [0, 1]));

    [Fact]
    public void FixedBinaryKey_MatchesStoredPaddingButVariableBinaryDoesNot()
    {
        var fixedTable = new TableDef
        {
            Columns = [new ColumnInfo { Name = "Key", Type = ColumnType.BinaryType, Flags = 1, Size = 4 }],
        };
        Assert.True(fixedTable.Columns[0].IsFixed);
        Assert.Equal(
            RelationshipKeyBuilder.BuildKey([(byte[])[1]], [0], fixedTable),
            RelationshipKeyBuilder.BuildKey([(byte[])[1, 0, 0, 0]], [0], fixedTable));
        Assert.Equal(
            RelationshipKeyBuilder.BuildKey([(byte[])[1]], [0]),
            RelationshipKeyBuilder.BuildKey([new ReadOnlyMemory<byte>([1])], [0]));
        Assert.NotEqual(
            RelationshipKeyBuilder.BuildKey([(byte[])[1]], [0]),
            RelationshipKeyBuilder.BuildKey([(byte[])[1, 0, 0, 0]], [0]));
    }
}
