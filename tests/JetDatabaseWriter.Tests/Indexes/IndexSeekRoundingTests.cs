namespace JetDatabaseWriter.Tests.Indexes;

using System;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Checks rounded seek admission against independent numeric and encoded-key comparisons.</summary>
public sealed class IndexSeekRoundingTests
{
    [Fact]
    public void FractionalBounds_AdmitOnlyRoundingAwayFromMatches()
    {
        foreach ((double operand, bool lower) in ((double, bool)[])[(1.5, false), (2.5, true), (-1.5, true), (-2.5, false)])
        {
            var column = new ColumnInfo { Name = "C", Type = ColumnType.LongIntegerType };
            Assert.Equal(lower, IndexSeekFilter.IsSeekable(column, typeof(int), ColumnPredicateOperator.GreaterThan, operand));
            Assert.Equal(lower, IndexSeekFilter.IsSeekable(column, typeof(int), ColumnPredicateOperator.GreaterThanOrEqual, operand));
            Assert.Equal(!lower, IndexSeekFilter.IsSeekable(column, typeof(int), ColumnPredicateOperator.LessThan, operand));
            Assert.Equal(!lower, IndexSeekFilter.IsSeekable(column, typeof(int), ColumnPredicateOperator.LessThanOrEqual, operand));
            Assert.False(IndexSeekFilter.IsSeekable(column, typeof(int), ColumnPredicateOperator.Equal, operand));
        }
    }

    [Fact]
    public void ScaledAndSingleBounds_RetainDirectionAndExactEquality()
    {
        foreach (ColumnType type in (ColumnType[])[ColumnType.MoneyType, ColumnType.NumericType])
        {
            var column = new ColumnInfo { Name = "C", Type = type, NumericScale = 4 };
            Assert.True(IndexSeekFilter.IsSeekable(column, typeof(decimal), ColumnPredicateOperator.GreaterThan, 1.23445m));
            Assert.False(IndexSeekFilter.IsSeekable(column, typeof(decimal), ColumnPredicateOperator.LessThan, 1.23445m));
            Assert.True(IndexSeekFilter.IsSeekable(column, typeof(decimal), ColumnPredicateOperator.LessThan, 1.23455m));
            Assert.False(IndexSeekFilter.IsSeekable(column, typeof(decimal), ColumnPredicateOperator.GreaterThan, 1.23455m));
            Assert.False(IndexSeekFilter.IsSeekable(column, typeof(decimal), ColumnPredicateOperator.Equal, 1.23455m));
        }

        var single = new ColumnInfo { Name = "C", Type = ColumnType.FloatType };
        Assert.True(IndexSeekFilter.IsSeekable(single, typeof(float), ColumnPredicateOperator.LessThan, 0.1));
        Assert.False(IndexSeekFilter.IsSeekable(single, typeof(float), ColumnPredicateOperator.GreaterThan, 0.1));
        Assert.True(IndexSeekFilter.IsSeekable(single, typeof(float), ColumnPredicateOperator.GreaterThan, 0.7));
        Assert.False(IndexSeekFilter.IsSeekable(single, typeof(float), ColumnPredicateOperator.LessThan, 0.7));
        Assert.False(IndexSeekFilter.IsSeekable(single, typeof(float), ColumnPredicateOperator.Equal, 0.1));
        Assert.False(IndexSeekFilter.IsSeekable(single, typeof(float), ColumnPredicateOperator.LessThan, double.MaxValue));
        Assert.False(IndexSeekFilter.IsSeekable(single, typeof(float), ColumnPredicateOperator.GreaterThan, double.NaN));
        Assert.False(IndexSeekFilter.IsSeekable(single, typeof(float), ColumnPredicateOperator.Equal, 0f));
        Assert.False(IndexSeekFilter.IsSeekable(single, typeof(float), ColumnPredicateOperator.GreaterThanOrEqual, 0f));
        Assert.True(IndexSeekFilter.IsSeekable(single, typeof(float), ColumnPredicateOperator.GreaterThanOrEqual, BitConverter.Int32BitsToSingle(int.MinValue)));
        Assert.True(IndexSeekFilter.IsSeekable(single, typeof(float), ColumnPredicateOperator.LessThanOrEqual, 0f));
    }

    [Fact]
    public void EveryAdmittedByteBound_PreservesAllOriginalMatchesInBothIndexDirections()
    {
        var column = new ColumnInfo { Name = "C", Type = ColumnType.ByteType };
        foreach (bool ascending in (bool[])[true, false])
        {
            for (int quarter = -3; quarter <= 1023; quarter++)
            {
                double operand = quarter / 4.0;
                byte[]? bound = Encode(ColumnType.ByteType, operand, ascending);
                foreach (ColumnPredicateOperator op in (ColumnPredicateOperator[])[ColumnPredicateOperator.Equal, ColumnPredicateOperator.GreaterThan, ColumnPredicateOperator.GreaterThanOrEqual, ColumnPredicateOperator.LessThan, ColumnPredicateOperator.LessThanOrEqual])
                {
                    if (!IndexSeekFilter.IsSeekable(column, typeof(byte), op, operand))
                    {
                        continue;
                    }

                    Assert.NotNull(bound);
                    for (int key = byte.MinValue; key <= byte.MaxValue; key++)
                    {
                        if (!Matches(((double)key).CompareTo(operand), op))
                        {
                            continue;
                        }

                        byte[] encoded = Assert.IsType<byte[]>(Encode(ColumnType.ByteType, (byte)key, ascending));
                        int comparison = encoded.AsSpan().SequenceCompareTo(bound);
                        Assert.True(Matches(ascending ? comparison : -comparison, op), $"Lost {key} for {op} {operand}, ascending={ascending}.");
                    }
                }
            }
        }
    }

    [Fact]
    public void AdmittedScaledAndFloatingBounds_PreserveIndependentEncodedMatches()
    {
        CheckEncodedMatches(ColumnType.MoneyType, typeof(decimal), 4, [1.2344m, 1.2345m, 1.2346m, -1.2344m, -1.2346m, 0m], [1.23445m, 1.23455m, -1.23445m, -1.23455m, 0m]);
        CheckEncodedMatches(ColumnType.MoneyType, typeof(decimal), 4, [-922337203685477.5808m, 922337203685477.5807m], [-922337203685477.58081m, 922337203685477.58071m, -922337203685477.5809m, 922337203685477.5808m]);
        CheckEncodedMatches(ColumnType.BigIntType, typeof(long), 0, [-9223372036854775808m, 9223372036854775807m], [-9223372036854775808.1m, 9223372036854775807.1m]);
        CheckEncodedMatches(ColumnType.NumericType, typeof(decimal), 2, [1.00m, 1.01m, -1.00m, -1.01m, 0m], [1.005m, 1.015m, -1.005m, -1.015m, 0m]);
        float negativeSingleZero = BitConverter.Int32BitsToSingle(int.MinValue);
        double negativeDoubleZero = BitConverter.Int64BitsToDouble(long.MinValue);
        CheckEncodedMatches(ColumnType.FloatType, typeof(float), 0, [0f, negativeSingleZero, 0.1f, 0.7f, -0.1f, -0.7f, float.Epsilon, -float.Epsilon], [0.0, negativeDoubleZero, 0.1, 0.7, -0.1, -0.7, 1e-46, -1e-46]);
        CheckEncodedMatches(ColumnType.DoubleType, typeof(double), 0, [0.0, negativeDoubleZero, double.Epsilon, -double.Epsilon, 1.0, -1.0], [0.0, negativeDoubleZero, double.Epsilon, -double.Epsilon]);
    }

    private static void CheckEncodedMatches(ColumnType type, Type property, byte scale, object[] keys, object[] operands)
    {
        var column = new ColumnInfo { Name = "C", Type = type, NumericScale = scale };
        foreach (object operand in operands)
        {
            foreach (ColumnPredicateOperator op in (ColumnPredicateOperator[])[ColumnPredicateOperator.Equal, ColumnPredicateOperator.GreaterThan, ColumnPredicateOperator.GreaterThanOrEqual, ColumnPredicateOperator.LessThan, ColumnPredicateOperator.LessThanOrEqual])
            {
                if (!IndexSeekFilter.IsSeekable(column, property, op, operand))
                {
                    continue;
                }

                foreach (bool legacy in (bool[])[false, true])
                {
                    foreach (bool ascending in (bool[])[true, false])
                    {
                        // Legacy Numeric keeps its sign byte across directions. Descending ranges
                        // remain residual; IndexPlannerTests guards that unsupported order premise.
                        if (type == ColumnType.NumericType && legacy && !ascending && op != ColumnPredicateOperator.Equal)
                        {
                            continue;
                        }

                        byte[] bound = Assert.IsType<byte[]>(Encode(type, operand, ascending, scale, legacy));
                        foreach (object key in keys)
                        {
                            int original = operand is decimal number
                                ? ((decimal)key).CompareTo(number)
                                : Convert.ToDouble(key, System.Globalization.CultureInfo.InvariantCulture).CompareTo((double)operand);
                            if (Matches(original, op))
                            {
                                byte[] encoded = Assert.IsType<byte[]>(Encode(type, key, ascending, scale, legacy));
                                int comparison = encoded.AsSpan().SequenceCompareTo(bound);
                                Assert.True(Matches(ascending ? comparison : -comparison, op), $"Lost {key} for {type} {op} {operand}, ascending={ascending}, legacy={legacy}.");
                            }
                        }
                    }
                }
            }
        }
    }

    private static byte[]? Encode(ColumnType type, object value, bool ascending, byte scale = 0, bool legacy = false) =>
        IndexKeyEncoder.TryEncodeColumnEntry(type, value, ascending, scale, legacy, default);

    private static bool Matches(int comparison, ColumnPredicateOperator op) => op switch
    {
        ColumnPredicateOperator.Equal => comparison == 0,
        ColumnPredicateOperator.GreaterThan => comparison > 0,
        ColumnPredicateOperator.GreaterThanOrEqual => comparison >= 0,
        ColumnPredicateOperator.LessThan => comparison < 0,
        ColumnPredicateOperator.LessThanOrEqual => comparison <= 0,
        ColumnPredicateOperator.NotEqual or ColumnPredicateOperator.Between or ColumnPredicateOperator.In or ColumnPredicateOperator.IsNull or ColumnPredicateOperator.IsNotNull => throw new ArgumentOutOfRangeException(nameof(op)),
        _ => throw new ArgumentOutOfRangeException(nameof(op)),
    };
}
