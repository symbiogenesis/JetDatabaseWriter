namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.IO;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using Xunit;
using static JetDatabaseWriter.Enums.ColumnType;

public sealed class RowValueDecodePolicyTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void MalformedValueFilter_ExcludesIoAndCancellation(bool strict)
    {
        Assert.False(RowValueDecodePolicy.IsMalformedValueException(new IOException("disk failure")));
        Assert.False(RowValueDecodePolicy.IsMalformedValueException(new OperationCanceledException()));
        Assert.True(RowValueDecodePolicy.IsMalformedValueException(new ArgumentException("invalid bytes")));
        Assert.True(RowValueDecodePolicy.IsMalformedValueException(new IndexOutOfRangeException()));
        Assert.True(RowValueDecodePolicy.IsMalformedValueException(new OverflowException()));
        Assert.True(RowValueDecodePolicy.HasFixedPayload(new ColumnInfo { Type = MoneyType }, 8, strict));
    }
    [Fact]
    public void EmptyVariableValue_TextAndMemo_ReturnsEmptyString()
    {
        Assert.Equal(string.Empty, RowValueDecodePolicy.EmptyVariableValue(new ColumnInfo { Type = TextType }));
        Assert.Equal(string.Empty, RowValueDecodePolicy.EmptyVariableValue(new ColumnInfo { Type = MemoType }));
    }

    [Fact]
    public void EmptyVariableValue_BinaryAndOle_ReturnsEmptyByteArray()
    {
        Assert.Same(Array.Empty<byte>(), RowValueDecodePolicy.EmptyVariableValue(new ColumnInfo { Type = BinaryType }));
        Assert.Same(Array.Empty<byte>(), RowValueDecodePolicy.EmptyVariableValue(new ColumnInfo { Type = OleType }));
    }

    [Fact]
    public void FixedVariableSlotTooShort_NonStrict_ReturnsDBNull()
    {
        object value = RowValueDecodePolicy.FixedVariableSlotTooShort(
            new ColumnInfo { Name = "Amount", Type = MoneyType },
            actualLength: 3,
            requiredLength: 8,
            strictParsing: false);

        Assert.Equal(DBNull.Value, value);
    }

    [Fact]
    public void FixedVariableSlotTooShort_Strict_ThrowsInvalidDataException()
    {
        InvalidDataException ex = Assert.Throws<InvalidDataException>(() =>
            RowValueDecodePolicy.FixedVariableSlotTooShort(
                new ColumnInfo { Name = "Amount", Type = MoneyType },
                actualLength: 3,
                requiredLength: 8,
                strictParsing: true));

        Assert.Contains("Amount", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void MalformedVariableValue_NonStrict_ReturnsDBNull()
    {
        object value = RowValueDecodePolicy.MalformedVariableValue(
            new ColumnInfo { Name = "When", Type = DateTimeType },
            new ArgumentException("bad date"),
            strictParsing: false);

        Assert.Equal(DBNull.Value, value);
    }

    [Fact]
    public void MalformedVariableValue_Strict_WrapsAsInvalidDataException()
    {
        var inner = new ArgumentException("bad date");
        InvalidDataException ex = Assert.Throws<InvalidDataException>(() =>
            RowValueDecodePolicy.MalformedVariableValue(
                new ColumnInfo { Name = "When", Type = DateTimeType },
                inner,
                strictParsing: true));

        Assert.Same(inner, ex.InnerException);
        Assert.Contains("When", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void MalformedVariableValue_JetLimitationException_RethrowsOriginalException()
    {
        var limitation = new JetLimitationException("numeric overflow");

        JetLimitationException ex = Assert.Throws<JetLimitationException>(() =>
            RowValueDecodePolicy.MalformedVariableValue(
                new ColumnInfo { Name = "DecimalValue", Type = NumericType },
                limitation,
                strictParsing: false));

        Assert.Same(limitation, ex);
    }
}
