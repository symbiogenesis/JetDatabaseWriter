namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.ComponentModel.DataAnnotations.Schema;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>
/// Tests for <see cref="IndexSeekFilter"/>: a comparison is sought only when the property holds
/// every stored key exactly and the operand is a value the key holds exactly, so the seek finds
/// the rows the residual filter keeps. Everything else is left to the residual filter.
/// </summary>
public sealed class IndexSeekFilterTests
{
    /// <summary>An enum over <see cref="int"/>, as a property bound to a Long Integer or Text column.</summary>
    internal enum Shade
    {
        /// <summary>No shade.</summary>
        None = 0,

        /// <summary>Blue.</summary>
        Blue = 3,
    }

    [Fact]
    public void PropertyHoldingEveryKey_AndOperandTheKeyHolds_IsSought()
    {
        Assert.True(Seekable(ColumnType.LongIntegerType, typeof(int), 5));
        Assert.True(Seekable(ColumnType.LongIntegerType, typeof(int?), 5));
        Assert.True(Seekable(ColumnType.LongIntegerType, typeof(Shade), 3));
        Assert.True(Seekable(ColumnType.LongIntegerType, typeof(Shade?), 3));
        Assert.True(Seekable(ColumnType.LongIntegerType, typeof(long), 5L));
        Assert.True(Seekable(ColumnType.LongIntegerType, typeof(double), 5.0));

        // (double)r.Id > 5.0 compares in double, which holds every Long Integer exactly.
        Assert.True(Seekable(ColumnType.LongIntegerType, typeof(int), 5.0));
        Assert.True(Seekable(ColumnType.LongIntegerType, typeof(int), 5m));
        Assert.True(Seekable(ColumnType.ByteType, typeof(byte), 255));
        Assert.True(Seekable(ColumnType.ByteType, typeof(int), 0UL));
        Assert.True(Seekable(ColumnType.IntegerType, typeof(short), -3f));
        Assert.True(Seekable(ColumnType.BigIntType, typeof(long), long.MaxValue));
        Assert.True(Seekable(ColumnType.BigIntType, typeof(long), 5m));
        Assert.True(Seekable(ColumnType.MoneyType, typeof(decimal), 9.9999m));
        Assert.True(Seekable(ColumnType.NumericType, typeof(decimal), 1.25m, numericScale: 2));
        Assert.True(Seekable(ColumnType.FloatType, typeof(float), 0.1f));
        Assert.True(Seekable(ColumnType.FloatType, typeof(float), 0.5));
        Assert.True(Seekable(ColumnType.DoubleType, typeof(double), 0.1));
        Assert.True(Seekable(ColumnType.DateTimeType, typeof(DateTime), new DateTime(2024, 1, 2, 3, 4, 5, 678)));
        Assert.True(Seekable(ColumnType.DateTimeType, typeof(DateTime?), new DateTime(1850, 6, 1, 12, 0, 0)));
        Assert.True(Seekable(ColumnType.TextType, typeof(string), "x"));
        Assert.True(Seekable(ColumnType.MemoType, typeof(string), "x"));
        Assert.True(Seekable(ColumnType.GuidType, typeof(Guid), Guid.NewGuid()));
        Assert.True(Seekable(ColumnType.BinaryType, typeof(byte[]), new byte[] { 1, 2 }));
    }

    [Fact]
    public void NullOperand_IsSought()
    {
        Assert.True(Seekable(ColumnType.LongIntegerType, typeof(int?), null));
        Assert.True(Seekable(ColumnType.TextType, typeof(string), null));
        Assert.True(Seekable(ColumnType.LongIntegerType, typeof(int?), DBNull.Value));
    }

    [Fact]
    public void PropertyHoldingConvertedValues_IsNotSought()
    {
        // The index keys hold the text, the bytes or the double, not the converted value.
        Assert.False(Seekable(ColumnType.TextType, typeof(Shade), 3));
        Assert.False(Seekable(ColumnType.TextType, typeof(Shade?), 3));
        Assert.False(Seekable(ColumnType.TextType, typeof(Guid), Guid.NewGuid()));
        Assert.False(Seekable(ColumnType.BinaryType, typeof(Guid), Guid.NewGuid()));
        Assert.False(Seekable(ColumnType.TextType, typeof(DateTime), new DateTime(2024, 1, 2)));
        Assert.False(Seekable(ColumnType.TextType, typeof(int), 42));
        Assert.False(Seekable(ColumnType.TextType, typeof(char), 65));
        Assert.False(Seekable(ColumnType.LongIntegerType, typeof(string), "5"));

        // A Double of 2.4 maps to an int property as 2, but its key is 2.4.
        Assert.False(Seekable(ColumnType.DoubleType, typeof(int), 2.5));
        Assert.False(Seekable(ColumnType.MoneyType, typeof(double), 2.5));
        Assert.False(Seekable(ColumnType.LongIntegerType, typeof(float), 2f));
    }

    [Fact]
    public void OperandOfATypeThatDoesNotHoldEveryKey_IsNotSought()
    {
        // The comparison runs in the operand's type. r.Id == 5f compares in float, which rounds
        // Long Integers above 2^24, and r.Big == 5.0 in double, which rounds Large Numbers above
        // 2^53. C# compares an enum through its underlying type, so a boxed enum is not a key.
        Assert.False(Seekable(ColumnType.LongIntegerType, typeof(int), 5f));
        Assert.False(Seekable(ColumnType.BigIntType, typeof(long), 5.0));
        Assert.False(Seekable(ColumnType.LongIntegerType, typeof(int), "5"));
        Assert.False(Seekable(ColumnType.LongIntegerType, typeof(Shade), Shade.Blue));
    }

    [Fact]
    public void OperandTheKeyCannotHold_IsNotSought()
    {
        // The encoder would round these, half to even, so r.Id > 1.5 would seek Id > 2 and
        // drop row 2, or throw past the key's range.
        Assert.False(Seekable(ColumnType.LongIntegerType, typeof(int), 1.5, ColumnPredicateOperator.GreaterThan));
        Assert.False(Seekable(ColumnType.LongIntegerType, typeof(int), 1.5m, ColumnPredicateOperator.LessThan));
        Assert.False(Seekable(ColumnType.LongIntegerType, typeof(int), 3_000_000_000L));
        Assert.False(Seekable(ColumnType.LongIntegerType, typeof(int), double.PositiveInfinity));
        Assert.False(Seekable(ColumnType.ByteType, typeof(byte), 300));
        Assert.False(Seekable(ColumnType.ByteType, typeof(byte), -1));
        Assert.False(Seekable(ColumnType.ByteType, typeof(byte), 256UL));
        Assert.False(Seekable(ColumnType.IntegerType, typeof(short), 40_000));
        Assert.False(Seekable(ColumnType.MoneyType, typeof(decimal), 1.23456m));
        Assert.False(Seekable(ColumnType.MoneyType, typeof(decimal), 1_000_000_000_000_000m));
        Assert.False(Seekable(ColumnType.NumericType, typeof(decimal), 1.005m, numericScale: 2));
        Assert.False(Seekable(ColumnType.FloatType, typeof(float), 0.1));
        Assert.False(Seekable(ColumnType.FloatType, typeof(float), float.NaN));
        Assert.False(Seekable(ColumnType.DoubleType, typeof(double), double.NaN));
    }

    [Fact]
    public void DateKeyThatMovesTheOperand_IsSoughtOnlyWhenItMovesAwayFromTheMatches()
    {
        // The key keeps whole milliseconds and rounds toward 1899-12-30. After that date it
        // rounds down, which a lower bound and an equality tolerate but an upper bound does not:
        // r.When < t would seek keys below t's millisecond and drop a row stored at it.
        DateTime late = new DateTime(2024, 1, 2).AddTicks(5);
        Assert.True(Seekable(ColumnType.DateTimeType, typeof(DateTime), late, ColumnPredicateOperator.GreaterThan));
        Assert.True(Seekable(ColumnType.DateTimeType, typeof(DateTime), late, ColumnPredicateOperator.GreaterThanOrEqual));
        Assert.True(Seekable(ColumnType.DateTimeType, typeof(DateTime), late, ColumnPredicateOperator.Equal));
        Assert.False(Seekable(ColumnType.DateTimeType, typeof(DateTime), late, ColumnPredicateOperator.LessThan));
        Assert.False(Seekable(ColumnType.DateTimeType, typeof(DateTime), late, ColumnPredicateOperator.LessThanOrEqual));

        // Before it the key rounds up.
        DateTime early = new DateTime(1850, 6, 1).AddTicks(5);
        Assert.True(Seekable(ColumnType.DateTimeType, typeof(DateTime), early, ColumnPredicateOperator.LessThan));
        Assert.True(Seekable(ColumnType.DateTimeType, typeof(DateTime), early, ColumnPredicateOperator.LessThanOrEqual));
        Assert.False(Seekable(ColumnType.DateTimeType, typeof(DateTime), early, ColumnPredicateOperator.GreaterThan));
        Assert.False(Seekable(ColumnType.DateTimeType, typeof(DateTime), early, ColumnPredicateOperator.GreaterThanOrEqual));

        // DateTime.MinValue encodes as 1899-12-30, far above it, and the encoder throws before
        // the year 100.
        Assert.True(Seekable(ColumnType.DateTimeType, typeof(DateTime), DateTime.MinValue, ColumnPredicateOperator.LessThan));
        Assert.False(Seekable(ColumnType.DateTimeType, typeof(DateTime), DateTime.MinValue, ColumnPredicateOperator.GreaterThan));
        Assert.False(Seekable(ColumnType.DateTimeType, typeof(DateTime), new DateTime(50, 1, 1), ColumnPredicateOperator.Equal));
    }

    [Fact]
    public void ComparisonsASeekCannotBound_AreNotSought()
    {
        Assert.False(Seekable(ColumnType.LongIntegerType, typeof(int), 5, ColumnPredicateOperator.NotEqual));
        Assert.False(Seekable(ColumnType.LongIntegerType, typeof(int), 5, ColumnPredicateOperator.In));
        Assert.False(Seekable(ColumnType.LongIntegerType, typeof(int?), null, ColumnPredicateOperator.GreaterThan));
    }

    [Fact]
    public void ColumnWithoutAnExactKey_IsNotSought()
    {
        // Yes/No values live in the null mask, not in key bytes.
        Assert.False(Seekable(ColumnType.BooleanType, typeof(bool), true));
        Assert.False(Seekable(ColumnType.OleType, typeof(byte[]), new byte[] { 1 }));

        // A hyperlink column decodes to a Hyperlink, not to the text its keys hold.
        var hyperlink = new ColumnInfo { Name = "Link", Type = ColumnType.MemoType, Flags = Constants.ColumnDescriptorFlags.Hyperlink };
        Assert.False(IndexSeekFilter.IsSeekable(hyperlink, typeof(string), ColumnPredicateOperator.Equal, "x"));
        Assert.False(IndexSeekFilter.IsSeekable(hyperlink, typeof(Hyperlink), ColumnPredicateOperator.Equal, "x"));
    }

    [Fact]
    public void SelectSeekable_KeepsOnlyTheComparisonsASeekAnswersExactly()
    {
        var table = new TableDef
        {
            Columns =
            [
                new ColumnInfo { Name = "Id", Type = ColumnType.LongIntegerType },
                new ColumnInfo { Name = "Shade Name", Type = ColumnType.TextType },
                new ColumnInfo { Name = "Score", Type = ColumnType.LongIntegerType },
                new ColumnInfo { Name = "Unmapped", Type = ColumnType.LongIntegerType },
            ],
        };
        RowCriteria pushable =
        [
            ColumnPredicate.GreaterThan("Id", 1),
            ColumnPredicate.EqualTo("shade name", 3),
            ColumnPredicate.LessThan("Score", 1.5),
            ColumnPredicate.Between("Id", 1, 1.5),
            ColumnPredicate.Between("Id", 1, 9),
            ColumnPredicate.In("Id", 1, 2),
            ColumnPredicate.EqualTo("Missing", 1),
            ColumnPredicate.EqualTo("Unmapped", 1),
        ];

        RowCriteria seekable = IndexSeekFilter.SelectSeekable(pushable, typeof(Row), table);

        // Kept: Id > 1 and Id between 1 and 9. Dropped: an enum on a Text column, a fractional
        // bound, a Between with a fractional bound, In, a column the table lacks, and a column
        // no property maps.
        Assert.Equal([pushable.Predicates[0], pushable.Predicates[4]], seekable.Predicates);
    }

    private static bool Seekable(
        ColumnType type,
        Type propertyType,
        object? operand,
        ColumnPredicateOperator @operator = ColumnPredicateOperator.Equal,
        byte numericScale = 0) =>
        IndexSeekFilter.IsSeekable(new ColumnInfo { Name = "C", Type = type, NumericScale = numericScale }, propertyType, @operator, operand);

#pragma warning disable CA1812 // Only its mapping is read.
    private sealed class Row
#pragma warning restore CA1812
    {
        public int Id { get; set; }

        [Column("Shade Name")]
        public Shade ShadeName { get; set; }

        public int Score { get; set; }

        public int Missing { get; set; }
    }
}
