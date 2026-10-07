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
    public void CompositeTextKeys_CannotMoveSeparatorBetweenColumns()
    {
        string? first = RelationshipKeyBuilder.Build(["a|S:b", "c"], [0, 1]);
        string? second = RelationshipKeyBuilder.Build(["a", "b|S:c"], [0, 1]);
        Assert.NotEqual(first, second);
    }

    [Fact]
    public void CompositeTextKeys_RetainCaseInsensitiveMatching()
        => Assert.Equal(
            RelationshipKeyBuilder.Build(["alpha", "BETA"], [0, 1]),
            RelationshipKeyBuilder.Build(["ALPHA", "beta"], [0, 1]));

    [Fact]
    public void FixedBinaryKey_MatchesStoredPaddingButVariableBinaryDoesNot()
    {
        var fixedTable = new TableDef
        {
            Columns = [new ColumnInfo { Name = "Key", Type = ColumnType.BinaryType, Flags = 1, Size = 4 }],
        };
        Assert.True(fixedTable.Columns[0].IsFixed);
        Assert.Equal(
            RelationshipKeyBuilder.Build([(byte[])[1]], [0], fixedTable),
            RelationshipKeyBuilder.Build([(byte[])[1, 0, 0, 0]], [0], fixedTable));
        Assert.Equal(
            RelationshipKeyBuilder.Build([(byte[])[1]], [0]),
            RelationshipKeyBuilder.Build([new ReadOnlyMemory<byte>([1])], [0]));
        Assert.NotEqual(
            RelationshipKeyBuilder.Build([(byte[])[1]], [0]),
            RelationshipKeyBuilder.Build([(byte[])[1, 0, 0, 0]], [0]));
    }
}
