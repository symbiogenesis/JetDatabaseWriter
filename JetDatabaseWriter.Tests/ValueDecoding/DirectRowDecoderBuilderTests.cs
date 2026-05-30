namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using JetDatabaseWriter.Enums;
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
}
