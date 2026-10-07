namespace JetDatabaseWriter.Tests.Indexes;

using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using Xunit;
using static JetDatabaseWriter.Enums.ColumnType;

/// <summary>
/// Tests for relationship seek-key encoding metadata. Numeric keys need the
/// column descriptor's declared scale, so the parent/child seek descriptors
/// must carry that scale through to <see cref="IndexKeyEncoder"/>.
/// </summary>
public sealed class IndexSeekKeyEncodingTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void FixedBinary_KeyPaddingMatchesPersistedWidth(bool ascending)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        var column = new ColumnInfo { Type = BinaryType, Size = 4, Flags = Constants.ColumnDescriptorFlags.Fixed };
        byte[] shortKey = IndexKeyEncoder.EncodeColumnEntry(format, column, new byte[] { 1 }, ascending);
        byte[] paddedKey = IndexKeyEncoder.EncodeColumnEntry(format, column, new byte[] { 1, 0, 0, 0 }, ascending);
        Assert.Equal(paddedKey, shortKey);
        ColumnInfo variable = column with { Flags = 0 };
        Assert.NotEqual(IndexKeyEncoder.EncodeColumnEntry(format, variable, new byte[] { 1 }, ascending), IndexKeyEncoder.EncodeColumnEntry(format, variable, new byte[] { 1, 0, 0, 0 }, ascending));
        var parent = new ParentSeekIndex(123, [new ParentSeekKeyColumn(BinaryType, ascending, 0, 0, false, FixedBinaryLength: 4)]);
        var child = new ChildSeekIndex(456, [new ChildSeekKeyColumn(BinaryType, ascending, 0, false, FixedBinaryLength: 4)]);
        Assert.Equal(paddedKey, IndexHelpers.TryEncodeSeekKey(parent, [new byte[] { 1 }]));
        Assert.Equal(paddedKey, IndexHelpers.TryEncodeChildSeekKey(child, [new byte[] { 1 }]));
    }

    [Fact]
    public void BinaryCriteria_FixedPaddingAndVariableLengthRemainDistinct()
    {
        var column = new ColumnInfo { Name = "Data", Type = BinaryType, Size = 4, Flags = Constants.ColumnDescriptorFlags.Fixed };
        var fixedDefinition = new TableDef { Columns = [column] };
        var criteria = RowCriteria.Where("Data", new byte[] { 1 });
        Assert.True(RowCriteriaEvaluator.Compile(criteria, fixedDefinition, "Samples", "criteria").Matches([new byte[] { 1, 0, 0, 0 }]));
        var variableDefinition = new TableDef { Columns = [column with { Flags = 0 }] };
        Assert.False(RowCriteriaEvaluator.Compile(criteria, variableDefinition, "Samples", "criteria").Matches([new byte[] { 1, 0, 0, 0 }]));
        var range = RowCriteria.Where(ColumnPredicate.Between("Data", new byte[] { 1 }, new byte[] { 2 }));
        Assert.True(RowCriteriaEvaluator.Compile(range, fixedDefinition, "Samples", "criteria").Matches([new byte[] { 1, 0, 0, 0 }]));
    }

    [Fact]
    public void ParentSeekKey_NumericColumn_UsesDeclaredScale()
    {
        var index = new ParentSeekIndex(
            RootPage: 123,
            KeyColumns: [new ParentSeekKeyColumn(NumericType, Ascending: true, ForeignColumnIndex: 0, NumericScale: 2, LegacyNumeric: false)]);

        byte[]? actual = IndexHelpers.TryEncodeSeekKey(index, [1.235m]);
        byte[] expected = IndexKeyEncoder.EncodeNumericEntryAtDeclaredScale(1.235m, ascending: true, declaredScale: 2, legacy: false);

        Assert.NotNull(actual);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void ChildSeekKey_NumericColumn_UsesDeclaredScaleAndDirection()
    {
        var index = new ChildSeekIndex(
            RootPage: 456,
            KeyColumns: [new ChildSeekKeyColumn(NumericType, Ascending: false, NumericScale: 1, LegacyNumeric: true)]);

        byte[]? actual = IndexHelpers.TryEncodeChildSeekKey(index, [12.34m]);
        byte[] expected = IndexKeyEncoder.EncodeNumericEntryAtDeclaredScale(12.34m, ascending: false, declaredScale: 1, legacy: true);

        Assert.NotNull(actual);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void NumericColumnType_IsSeekableWhenDescriptorScaleIsResolved() => Assert.True(IndexKeyEncoder.IsColumnTypeSeekable(NumericType));
}
