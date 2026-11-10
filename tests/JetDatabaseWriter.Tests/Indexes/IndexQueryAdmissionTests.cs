namespace JetDatabaseWriter.Tests.Indexes;

using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Shared reader and EF writer admission retains descriptor and key boundaries.</summary>
public sealed class IndexQueryAdmissionTests
{
    [Fact]
    public void SeekableIndexes_ResolveNativeColumnNumbersAndRetainCandidateOrder()
    {
        var table = new TableDef
        {
            Columns =
            [
                new ColumnInfo { Name = "Label", ColNum = 7, Type = ColumnType.TextType },
                new ColumnInfo { Name = "Memo", ColNum = 12, Type = ColumnType.MemoType, TextSortOrder = new TextSortOrder(0x041D, 0, true) },
                new ColumnInfo { Name = "Id", ColNum = 3, Type = ColumnType.LongIntegerType },
            ],
        };
        var label = new IndexMetadata { Name = "Label", Columns = [new IndexColumnReference { ColumnNumber = 7 }] };
        var missing = new IndexMetadata { Name = "Missing", Columns = [new IndexColumnReference { Name = "Label", ColumnNumber = 1 }] };
        var memo = new IndexMetadata { Name = "Memo", Columns = [new IndexColumnReference { ColumnNumber = 12 }] };
        var id = new IndexMetadata { Name = "Id", Columns = [new IndexColumnReference { ColumnNumber = 3 }] };

        Assert.Equal([label, id], IndexQueryAdmission.FilterSeekableIndexes(table, [label, missing, memo, id]));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void PlanEncoding_ChecksExactPrefixAndEachRangeBound(DatabaseFormat databaseFormat)
    {
        var format = JetFormat.ForNewDatabase(databaseFormat);
        var table = new TableDef { Columns = [new ColumnInfo { ColNum = 7, Type = ColumnType.BinaryType, Size = 4, Flags = Constants.ColumnDescriptorFlags.Fixed }] };
        var index = new IndexMetadata { Name = "Data", Columns = [new IndexColumnReference { ColumnNumber = 7 }] };
        object?[] valid = [new byte[] { 1 }];
        object?[] invalid = [new byte[] { 1, 2, 3, 4, 5 }];

        Assert.True(Admits(IndexQueryCriteria.All));
        Assert.True(Admits(IndexQueryCriteria.Exact(valid)));
        Assert.True(Admits(IndexQueryCriteria.Range(IndexKeyBound.Inclusive(valid), IndexKeyBound.Exclusive(valid))));
        Assert.False(Admits(IndexQueryCriteria.Exact(invalid)));
        Assert.False(Admits(IndexQueryCriteria.KeyPrefix(invalid)));
        Assert.False(Admits(IndexQueryCriteria.Range(IndexKeyBound.Inclusive(invalid), IndexKeyBound.Inclusive(valid))));
        Assert.False(Admits(IndexQueryCriteria.Range(IndexKeyBound.Inclusive(valid), IndexKeyBound.Inclusive(invalid))));

        bool Admits(IndexQueryCriteria criteria) => IndexQueryAdmission.CanEncodePlan(format, table, "Samples", new IndexPlan(index, criteria, 1));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void JoinKeys_RequireMatchingCollationOnlyForBoundPrefixColumns(DatabaseFormat databaseFormat)
    {
        var format = JetFormat.ForNewDatabase(databaseFormat);
        var mismatchedOrder = new TextSortOrder(0x0409, 1, true);
        var id = new ColumnInfo { Name = "Id", ColNum = 7, Type = ColumnType.LongIntegerType };
        var label = new ColumnInfo { Name = "Label", ColNum = 3, Type = ColumnType.TextType, TextSortOrder = mismatchedOrder };
        var table = new TableDef { Columns = [label, id] };
        var index = new IndexMetadata
        {
            Name = "IdLabel",
            Columns = [new IndexColumnReference { Name = "Id", ColumnNumber = 7 }, new IndexColumnReference { Name = "Label", ColumnNumber = 3 }],
        };

        Assert.True(IndexQueryAdmission.CanSeekJoinKeys(format, table, "Samples", index, [[1]]));
        Assert.False(IndexQueryAdmission.CanSeekJoinKeys(format, table, "Samples", index, [[1, "a"]]));

        var matching = new TableDef { Columns = [label with { TextSortOrder = format.DefaultTextSortOrder }, id] };
        Assert.True(IndexQueryAdmission.CanSeekJoinKeys(format, matching, "Samples", index, []));
        Assert.False(IndexQueryAdmission.CanSeekJoinKeys(format, matching, "Samples", index, [[]]));
        Assert.True(IndexQueryAdmission.CanSeekJoinKeys(format, matching, "Samples", index, [[1, "a"], [2, "b"]]));
        Assert.False(IndexQueryAdmission.CanSeekJoinKeys(format, matching, "Samples", index, [[1, "a"], ["invalid", "b"]]));
    }
}
