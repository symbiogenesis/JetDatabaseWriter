namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.IO;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using Xunit;
using static JetDatabaseWriter.Constants.ColumnTypes;

public sealed class TypedRowFallbackPolicyTests
{
    [Fact]
    public void EmptyVariableValue_TextAndMemo_ReturnsEmptyString()
    {
        Assert.Equal(string.Empty, TypedRowFallbackPolicy.EmptyVariableValue(new ColumnInfo { Type = TextType }));
        Assert.Equal(string.Empty, TypedRowFallbackPolicy.EmptyVariableValue(new ColumnInfo { Type = MemoType }));
    }

    [Fact]
    public void EmptyVariableValue_BinaryAndOle_ReturnsEmptyByteArray()
    {
        Assert.Same(Array.Empty<byte>(), TypedRowFallbackPolicy.EmptyVariableValue(new ColumnInfo { Type = BinaryType }));
        Assert.Same(Array.Empty<byte>(), TypedRowFallbackPolicy.EmptyVariableValue(new ColumnInfo { Type = OleType }));
    }

    [Fact]
    public void FixedVariableSlotTooShort_NonStrict_ReturnsDBNull()
    {
        object value = TypedRowFallbackPolicy.FixedVariableSlotTooShort(
            new ColumnInfo { Name = "Amount", Type = MoneyType },
            actualLength: 3,
            requiredLength: 8,
            strictParsing: false);

        Assert.Equal(DBNull.Value, value);
    }

    [Fact]
    public void FixedVariableSlotTooShort_Strict_ThrowsInvalidDataException()
    {
        var ex = Assert.Throws<InvalidDataException>(() =>
            TypedRowFallbackPolicy.FixedVariableSlotTooShort(
                new ColumnInfo { Name = "Amount", Type = MoneyType },
                actualLength: 3,
                requiredLength: 8,
                strictParsing: true));

        Assert.Contains("Amount", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void MalformedVariableValue_NonStrict_ReturnsDBNull()
    {
        object value = TypedRowFallbackPolicy.MalformedVariableValue(
            new ColumnInfo { Name = "When", Type = DateTime },
            new ArgumentException("bad date"),
            strictParsing: false);

        Assert.Equal(DBNull.Value, value);
    }

    [Fact]
    public void MalformedVariableValue_Strict_WrapsAsInvalidDataException()
    {
        var inner = new ArgumentException("bad date");
        var ex = Assert.Throws<InvalidDataException>(() =>
            TypedRowFallbackPolicy.MalformedVariableValue(
                new ColumnInfo { Name = "When", Type = DateTime },
                inner,
                strictParsing: true));

        Assert.Same(inner, ex.InnerException);
        Assert.Contains("When", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void MalformedVariableValue_JetLimitationException_RethrowsOriginalException()
    {
        var limitation = new JetLimitationException("numeric overflow");

        var ex = Assert.Throws<JetLimitationException>(() =>
            TypedRowFallbackPolicy.MalformedVariableValue(
                new ColumnInfo { Name = "DecimalValue", Type = NumericType },
                limitation,
                strictParsing: false));

        Assert.Same(limitation, ex);
    }
}
