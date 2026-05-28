namespace JetDatabaseWriter.Tests.Indexes;

using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using Xunit;
using static JetDatabaseWriter.Constants.ColumnTypes;

/// <summary>
/// Tests for relationship seek-key encoding metadata. Numeric keys need the
/// column descriptor's declared scale, so the parent/child seek descriptors
/// must carry that scale through to <see cref="IndexKeyEncoder"/>.
/// </summary>
public sealed class IndexSeekKeyEncodingTests
{
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
    public void NumericColumnType_IsSeekableWhenDescriptorScaleIsResolved()
    {
        Assert.True(IndexKeyEncoder.IsColumnTypeSeekable(NumericType));
    }
}
