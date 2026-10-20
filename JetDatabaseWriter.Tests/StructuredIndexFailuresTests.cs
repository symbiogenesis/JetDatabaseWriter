namespace JetDatabaseWriter.Tests;

using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Indexes.Models;
using Xunit;
using static JetDatabaseWriter.Enums.ColumnType;

/// <summary>Structured failures and ordinary candidate refusals for index encoding and page capacity.</summary>
public sealed class StructuredIndexFailuresTests
{
    [Theory]
    [InlineData(ByteType, 256)]
    [InlineData(IntegerType, 32768)]
    [InlineData(LongIntegerType, long.MaxValue)]
    [InlineData(BigIntType, double.PositiveInfinity)]
    [InlineData(LongIntegerType, "1.0")]
    [InlineData(LongIntegerType, "invalid")]
    [InlineData(BooleanType, true)]
    [InlineData(GuidType, "invalid")]
    [InlineData(DateTimeType, "invalid")]
    public void CandidateKey_UnsupportedValueReturnsNull(ColumnType columnType, object value)
        => Assert.Null(IndexKeyEncoder.TryEncodeColumnEntry(columnType, value, true, 0, false, new TextSortOrder(0, 0, true)));

    [Theory]
    [InlineData(ByteType, 254)]
    [InlineData(IntegerType, 32000)]
    [InlineData(LongIntegerType, "123")]
    [InlineData(BigIntType, long.MaxValue)]
    [InlineData(FloatType, 1.25)]
    [InlineData(DoubleType, 1.25)]
    public void CandidateKey_ValidValueMatchesEncoder(ColumnType columnType, object value)
        => Assert.Equal(IndexKeyEncoder.EncodeEntry(columnType, value), IndexKeyEncoder.TryEncodeColumnEntry(columnType, value, true, 0, false, new TextSortOrder(0, 0, true)));

    [Fact]
    public void CandidateTree_CapacityFailureReturnsNull()
    {
        Assert.Null(IndexBTreeBuilder.TryBuild(IndexPageLayout.Jet4, 4096, 2, [new IndexEntry(new byte[4096], 1, 0)], 4));
        Assert.Null(IndexBTreeBuilder.TryBuild(IndexPageLayout.Jet4, 4096, 2, [], 0x1000000));
        Assert.Null(IndexBTreeBuilder.TryBuild(IndexPageLayout.Jet4, 4096, 2, [new IndexEntry(new byte[2000], 1, 0), new IndexEntry(new byte[2000], 2, 0)], 4));
    }

    [Fact]
    public void CandidateKey_FormatLimitsReturnNull()
    {
        TextSortOrder order = new(0x0409, 1, true);
        Assert.Null(IndexKeyEncoder.TryEncodeColumnEntry(TextType, new string('é', 255), true, 0, false, order));
        Assert.Null(IndexKeyEncoder.TryEncodeColumnEntry(NumericType, decimal.MaxValue, true, 28, false, order));
        Assert.Null(IndexKeyEncoder.TryEncodeColumnEntry(NumericType, null, true, 29, false, order));
        Assert.Null(IndexKeyEncoder.TryEncodeColumnEntry(MoneyType, decimal.MaxValue, true, 0, false, order));
        Assert.Null(IndexKeyEncoder.TryEncodeColumnEntry(DateTimeType, new System.DateTime(99, 1, 1), true, 0, false, order));
        Assert.Equal(IndexKeyEncoder.EncodeEntry(DateTimeType, System.DateTime.MinValue), IndexKeyEncoder.TryEncodeColumnEntry(DateTimeType, System.DateTime.MinValue, true, 0, false, order));
    }

    [Fact]
    public void TooManyIndexColumns_ReportsStableFailure()
    {
        JetLimitationException failure = Assert.Throws<JetLimitationException>(() => IndexLayout.Jet4.WriteRealIdxDescriptor(new byte[128], 0, new int[11], 0, 4));
        Assert.Equal(JetErrorCode.ColumnCountLimit, failure.ErrorCode);
    }

    [Fact]
    public void UnsupportedCollation_ReportsStableFailure()
    {
        JetNotSupportedException failure = Assert.Throws<JetNotSupportedException>(() => IndexKeyEncoder.EncodeTextEntry(new TextSortOrder(0x1234, 9, true), "value"));
        Assert.Equal(JetErrorCode.UnsupportedTextCollation, failure.ErrorCode);
        Assert.NotNull(failure.ErrorInfo.Reason);
    }

    [Fact]
    public void OversizedIndexPage_ReportsStableFailure()
    {
        IndexEntry[] entries = [new(new byte[4096], 1, 0)];
        JetLimitationException failure = Assert.Throws<JetLimitationException>(() => IndexPageCodec.BuildLeafPage(IndexPageLayout.Jet4, 4096, 2, entries, enablePrefixCompression: true));
        Assert.Equal(JetErrorCode.IndexEntryTooLarge, failure.ErrorCode);
        Assert.Equal(2, failure.ErrorInfo.PageNumber);
    }

    [Fact]
    public void UnsupportedColumnType_ReportsStableFailure()
    {
        JetNotSupportedException failure = Assert.Throws<JetNotSupportedException>(() => IndexKeyEncoder.EncodeEntry(BooleanType, true));
        Assert.Equal(JetErrorCode.FeatureNotSupported, failure.ErrorCode);
    }
}
