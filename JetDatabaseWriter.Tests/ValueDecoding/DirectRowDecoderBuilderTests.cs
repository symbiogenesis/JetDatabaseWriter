namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using Xunit;
using static JetDatabaseWriter.Enums.ColumnType;

#pragma warning disable CA1812 // Test POCOs are instantiated by direct decoder delegates.

public sealed class DirectRowDecoderBuilderTests
{
    [Fact]
    public void TryBuild_BinaryColumn_ReturnsDecoder()
    {
        DirectRowDecoder<BinaryRow>? decoder = DirectRowDecoderBuilder.TryBuild<BinaryRow>(
            ["Payload"],
            [new ColumnInfo { Name = "Payload", Type = BinaryType }],
            [typeof(byte[])]);

        Assert.NotNull(decoder);
    }

    [Fact]
    public void TryBuild_DateTimeExtendedColumn_ReturnsDecoder()
    {
        DirectRowDecoder<DateTimeExtendedRow>? decoder = DirectRowDecoderBuilder.TryBuild<DateTimeExtendedRow>(
            ["ExtendedAt"],
            [new ColumnInfo { Name = "ExtendedAt", Type = DateTimeExtendedType }],
            [typeof(DateTime)]);

        Assert.NotNull(decoder);
    }

    [Theory]
    [InlineData(MemoType)]
    [InlineData(OleType)]
    public void TryBuild_LongValueColumn_ReturnsNull(ColumnType columnType)
    {
        DirectRowDecoder<MemoRow>? decoder = DirectRowDecoderBuilder.TryBuild<MemoRow>(
            ["Payload"],
            [new ColumnInfo { Name = "Payload", Type = columnType }],
            [typeof(string)]);

        Assert.Null(decoder);
    }

    [Theory]
    [InlineData(ByteType)] // byte -> long
    [InlineData(IntegerType)] // short -> long
    [InlineData(LongIntegerType)] // int -> long
    [InlineData(BigIntType)] // long -> long (exact)
    public void TryBuild_LongTarget_AcceptsLosslessWidening(ColumnType columnType)
    {
        DirectRowDecoder<LongRow>? decoder = DirectRowDecoderBuilder.TryBuild<LongRow>(
            ["Value"],
            [new ColumnInfo { Name = "Value", Type = columnType }],
            [JetTypeInfo.GetClrType(columnType)!]);

        Assert.NotNull(decoder);
    }

    [Theory]
    [InlineData(ByteType)] // byte -> long?
    [InlineData(IntegerType)] // short -> long?
    [InlineData(LongIntegerType)] // int -> long?
    public void TryBuild_NullableLongTarget_AcceptsLosslessWidening(ColumnType columnType)
    {
        DirectRowDecoder<NullableLongRow>? decoder = DirectRowDecoderBuilder.TryBuild<NullableLongRow>(
            ["Value"],
            [new ColumnInfo { Name = "Value", Type = columnType }],
            [JetTypeInfo.GetClrType(columnType)!]);

        Assert.NotNull(decoder);
    }

    [Theory]
    [InlineData(ByteType)] // byte -> double
    [InlineData(IntegerType)] // short -> double
    [InlineData(LongIntegerType)] // int -> double
    [InlineData(FloatType)] // float -> double
    [InlineData(DoubleType)] // double -> double (exact)
    public void TryBuild_DoubleTarget_AcceptsLosslessWidening(ColumnType columnType)
    {
        DirectRowDecoder<DoubleRow>? decoder = DirectRowDecoderBuilder.TryBuild<DoubleRow>(
            ["Value"],
            [new ColumnInfo { Name = "Value", Type = columnType }],
            [JetTypeInfo.GetClrType(columnType)!]);

        Assert.NotNull(decoder);
    }

    [Theory]
    [InlineData(ByteType)] // byte -> decimal
    [InlineData(IntegerType)] // short -> decimal
    [InlineData(LongIntegerType)] // int -> decimal
    [InlineData(BigIntType)] // long -> decimal
    [InlineData(MoneyType)] // decimal -> decimal (exact)
    [InlineData(NumericType)] // decimal -> decimal (exact)
    public void TryBuild_DecimalTarget_AcceptsLosslessWidening(ColumnType columnType)
    {
        DirectRowDecoder<DecimalRow>? decoder = DirectRowDecoderBuilder.TryBuild<DecimalRow>(
            ["Value"],
            [new ColumnInfo { Name = "Value", Type = columnType }],
            [JetTypeInfo.GetClrType(columnType)!]);

        Assert.NotNull(decoder);
    }

    [Theory]
    [InlineData(FloatType, typeof(float))] // float -> float (exact)
    [InlineData(ByteType, typeof(float))] // byte -> float
    [InlineData(IntegerType, typeof(float))] // short -> float
    public void TryBuild_FloatTarget_AcceptsLosslessWidening(ColumnType columnType, Type clrType)
    {
        DirectRowDecoder<FloatRow>? decoder = DirectRowDecoderBuilder.TryBuild<FloatRow>(
            ["Value"],
            [new ColumnInfo { Name = "Value", Type = columnType }],
            [clrType]);

        Assert.NotNull(decoder);
    }

    [Theory]
    [InlineData(LongIntegerType)] // int -> float: 24-bit mantissa drops precision
    [InlineData(BigIntType)] // long -> float: drops precision
    [InlineData(DoubleType)] // double -> float: narrowing
    public void TryBuild_FloatTarget_RejectsPrecisionLoss(ColumnType columnType)
    {
        DirectRowDecoder<FloatRow>? decoder = DirectRowDecoderBuilder.TryBuild<FloatRow>(
            ["Value"],
            [new ColumnInfo { Name = "Value", Type = columnType }],
            [JetTypeInfo.GetClrType(columnType)!]);

        Assert.Null(decoder);
    }

    [Theory]
    [InlineData(BigIntType)] // long -> double: 53-bit mantissa drops precision
    [InlineData(MoneyType)] // decimal -> double: narrowing
    public void TryBuild_DoubleTarget_RejectsPrecisionLoss(ColumnType columnType)
    {
        DirectRowDecoder<DoubleRow>? decoder = DirectRowDecoderBuilder.TryBuild<DoubleRow>(
            ["Value"],
            [new ColumnInfo { Name = "Value", Type = columnType }],
            [JetTypeInfo.GetClrType(columnType)!]);

        Assert.Null(decoder);
    }

    [Theory]
    [InlineData(FloatType)] // float -> decimal: range + NaN/Infinity
    [InlineData(DoubleType)] // double -> decimal: range/precision + overflow
    public void TryBuild_DecimalTarget_RejectsLossySources(ColumnType columnType)
    {
        DirectRowDecoder<DecimalRow>? decoder = DirectRowDecoderBuilder.TryBuild<DecimalRow>(
            ["Value"],
            [new ColumnInfo { Name = "Value", Type = columnType }],
            [JetTypeInfo.GetClrType(columnType)!]);

        Assert.Null(decoder);
    }

    [Theory]
    [InlineData(FloatType)] // float -> long: narrowing
    [InlineData(DoubleType)] // double -> long: narrowing
    [InlineData(MoneyType)] // decimal -> long: narrowing
    public void TryBuild_LongTarget_RejectsNarrowingSources(ColumnType columnType)
    {
        DirectRowDecoder<LongRow>? decoder = DirectRowDecoderBuilder.TryBuild<LongRow>(
            ["Value"],
            [new ColumnInfo { Name = "Value", Type = columnType }],
            [JetTypeInfo.GetClrType(columnType)!]);

        Assert.Null(decoder);
    }

    private sealed class BinaryRow
    {
        public byte[] Payload { get; set; } = [];
    }

    private sealed class DateTimeExtendedRow
    {
        public DateTime ExtendedAt { get; set; }
    }

    private sealed class MemoRow
    {
        public string Payload { get; set; } = string.Empty;
    }

    private sealed class LongRow
    {
        public long Value { get; set; }
    }

    private sealed class NullableLongRow
    {
        public long? Value { get; set; }
    }

    private sealed class DoubleRow
    {
        public double Value { get; set; }
    }

    private sealed class DecimalRow
    {
        public decimal Value { get; set; }
    }

    private sealed class FloatRow
    {
        public float Value { get; set; }
    }
}
