namespace JetDatabaseWriter.Indexes;

using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Reflection;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Mapping;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Verification;
using static JetDatabaseWriter.Enums.ColumnType;

/// <summary>
/// Keeps the comparisons <see cref="IndexPredicateTranslator"/> extracts that an index seek
/// can answer without missing a row. A seek compares the operand with the column's stored
/// keys, while the residual filter compares it with the mapped property, so the seek returns
/// every row the filter keeps only when the property holds every stored value exactly and the
/// key the operand encodes to bounds the matches.
/// </summary>
/// <remarks>
/// <para>A comparison is kept when all of these hold:</para>
/// <list type="number">
/// <item><description>The column's type can be sought, and the column decodes to its key's own CLR type, so it is not a hyperlink or a calculated column.</description></item>
/// <item><description>The property's type, unwrapped from <see cref="Nullable{T}"/> and from an enum to its underlying type, is the column's CLR type or a lossless widening of it (<see cref="ClosureValueReader.IsLosslessWidening(Type, Type)"/>). An enum or <see cref="Guid"/> property bound to a Text or Binary column, or an <see cref="int"/> property bound to a Double column, holds converted values that do not compare like the stored keys.</description></item>
/// <item><description>The operand is null, which an equality seeks among the null keys, or its type is the column's CLR type or a lossless widening of it, since the comparison runs in the operand's type.</description></item>
/// <item><description>For integral, Currency, Numeric, and Single columns whose operand rounds to the nearest representable key, any inclusive range bound retains all matches, and an inward-rounded strict range bound (<c>&gt;</c> when <c>encoded &gt; operand</c>, <c>&lt;</c> when <c>encoded &lt; operand</c>) is rewritten to its inclusive counterpart (<c>&gt;=</c> or <c>&lt;=</c>) per <see cref="RecoveryKernel.RewriteRoundedSeekOperator"/>. Numeric equality stays exact; NaN, values that overflow the key, and unsafe signed-zero bounds are refused. A Date/Time equality may move because no stored whole-millisecond value then matches the original operand.</description></item>
/// </list>
/// <para>
/// A comparison that fails any of them is not sought. The residual filter, which the reader
/// applies to every row a seek or scan returns, still checks it, so the result is the same,
/// only read from more rows.
/// </para>
/// </remarks>
internal static class IndexSeekFilter
{
    /// <summary>The smallest value a Currency column holds.</summary>
    private const decimal CurrencyMin = -922_337_203_685_477.5808m;

    /// <summary>The largest value a Currency column holds.</summary>
    private const decimal CurrencyMax = 922_337_203_685_477.5807m;

    /// <summary>The largest scale a <see cref="decimal"/> rounds to.</summary>
    private const int MaxDecimalScale = 28;

    /// <summary>
    /// Returns the comparisons of <paramref name="pushable"/> an index seek on
    /// <paramref name="table"/> can answer without missing a row, for rows mapped to
    /// <paramref name="rowType"/>, rewriting inward-rounded strict bounds to inclusive bounds when sound.
    /// </summary>
    /// <param name="pushable">The comparisons the translator extracted.</param>
    /// <param name="rowType">The type the rows map to, whose properties the comparisons read.</param>
    /// <param name="table">The table's definition, which gives each column's type.</param>
    /// <returns>The comparisons to seek; possibly empty.</returns>
    public static RowCriteria SelectSeekable(RowCriteria pushable, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type rowType, TableDef table)
    {
        Guard.NotNull(pushable, nameof(pushable));
        Guard.NotNull(rowType, nameof(rowType));
        Guard.NotNull(table, nameof(table));

        var map = EntityMap.For(rowType);
        var seekable = new RowCriteria();
        foreach (ColumnPredicate predicate in pushable.Predicates)
        {
            if (table.FindColumn(predicate.ColumnName) is ColumnInfo column
                && map.FindByColumn(predicate.ColumnName)?.Property is PropertyInfo property
                && TryRewriteSeekPredicate(column, property.PropertyType, predicate, out ColumnPredicate? rewritten))
            {
                seekable.Add(rewritten);
            }
        }

        return seekable;
    }

    /// <summary>
    /// Returns <see langword="true"/> when an index seek on <paramref name="column"/> for
    /// <paramref name="operand"/> returns every row whose <paramref name="propertyType"/>
    /// property satisfies the comparison without changing <paramref name="operator"/>.
    /// </summary>
    /// <param name="column">The key column.</param>
    /// <param name="propertyType">The type of the property the column maps to.</param>
    /// <param name="operator">The comparison: an equality or a one-sided range.</param>
    /// <param name="operand">The comparison's operand.</param>
    /// <returns><see langword="true"/> when the comparison can be sought with <paramref name="operator"/> unchanged.</returns>
    public static bool IsSeekable(ColumnInfo column, Type propertyType, ColumnPredicateOperator @operator, object? operand) =>
        TryRewriteSeekOperator(column, propertyType, @operator, operand, out ColumnPredicateOperator rewrittenOperator)
        && rewrittenOperator == @operator;

    /// <summary>
    /// Determines whether an index seek on <paramref name="column"/> for <paramref name="operand"/>
    /// can answer <paramref name="operator"/>, rewriting a strict bound (<c>&gt;</c> or <c>&lt;</c>)
    /// to its inclusive counterpart (<c>&gt;=</c> or <c>&lt;=</c>) when nearest-key rounding moves
    /// the bound inward.
    /// </summary>
    /// <param name="column">The key column.</param>
    /// <param name="propertyType">The type of the property the column maps to.</param>
    /// <param name="operator">The comparison: an equality or a one-sided range.</param>
    /// <param name="operand">The comparison's operand.</param>
    /// <param name="rewrittenOperator">Receives the seek operator that retains all matching rows.</param>
    /// <returns><see langword="true"/> when the comparison can be sought using <paramref name="rewrittenOperator"/>.</returns>
    public static bool TryRewriteSeekOperator(
        ColumnInfo column,
        Type propertyType,
        ColumnPredicateOperator @operator,
        object? operand,
        out ColumnPredicateOperator rewrittenOperator)
    {
        Guard.NotNull(column, nameof(column));
        Guard.NotNull(propertyType, nameof(propertyType));
        rewrittenOperator = @operator;

        if (@operator is not (ColumnPredicateOperator.Equal
            or ColumnPredicateOperator.GreaterThan
            or ColumnPredicateOperator.GreaterThanOrEqual
            or ColumnPredicateOperator.LessThan
            or ColumnPredicateOperator.LessThanOrEqual))
        {
            return false;
        }

        if (!IndexKeyEncoder.IsColumnTypeSeekable(column.Type)
            || JetTypeInfo.GetClrType(column.Type) is not Type keyType
            || JetTypeInfo.ResolveClrType(column) != keyType)
        {
            return false;
        }

        Type property = Nullable.GetUnderlyingType(propertyType) ?? propertyType;
        if (property.IsEnum)
        {
            property = Enum.GetUnderlyingType(property);
        }

        if (!HoldsEveryKey(keyType, property))
        {
            return false;
        }

        if (operand is null or DBNull)
        {
            return @operator == ColumnPredicateOperator.Equal;
        }

        return HoldsEveryKey(keyType, operand.GetType())
            && TryResolveKeyBound(column, @operator, operand, out rewrittenOperator);
    }

    private static bool TryRewriteSeekPredicate(
        ColumnInfo column,
        Type propertyType,
        ColumnPredicate predicate,
        [NotNullWhen(true)] out ColumnPredicate? rewritten)
    {
        rewritten = null;
        if (predicate.Operator == ColumnPredicateOperator.Between)
        {
            if (IsSeekable(column, propertyType, ColumnPredicateOperator.GreaterThanOrEqual, predicate.Operand)
                && IsSeekable(column, propertyType, ColumnPredicateOperator.LessThanOrEqual, predicate.UpperOperand))
            {
                rewritten = predicate;
                return true;
            }

            return false;
        }

        if (!TryRewriteSeekOperator(column, propertyType, predicate.Operator, predicate.Operand, out ColumnPredicateOperator rewrittenOp))
        {
            return false;
        }

        if (rewrittenOp == predicate.Operator)
        {
            rewritten = predicate;
            return true;
        }

        rewritten = rewrittenOp switch
        {
            ColumnPredicateOperator.GreaterThanOrEqual => ColumnPredicate.GreaterThanOrEqual(predicate.ColumnName, predicate.Operand!),
            ColumnPredicateOperator.LessThanOrEqual => ColumnPredicate.LessThanOrEqual(predicate.ColumnName, predicate.Operand!),
            ColumnPredicateOperator.Equal
                or ColumnPredicateOperator.NotEqual
                or ColumnPredicateOperator.GreaterThan
                or ColumnPredicateOperator.LessThan
                or ColumnPredicateOperator.Between
                or ColumnPredicateOperator.In
                or ColumnPredicateOperator.IsNull
                or ColumnPredicateOperator.IsNotNull
                or _ => predicate,
        };
        return true;
    }

    private static bool HoldsEveryKey(Type keyType, Type type) =>
        type == keyType || ClosureValueReader.IsLosslessWidening(keyType, type);

    private static bool TryResolveKeyBound(
        ColumnInfo column,
        ColumnPredicateOperator @operator,
        object operand,
        out ColumnPredicateOperator rewrittenOperator)
    {
        rewrittenOperator = @operator;
        return column.Type switch
        {
            ByteType => TryRewriteIntegralKeyBound(operand, byte.MinValue, byte.MaxValue, @operator, out rewrittenOperator),
            IntegerType => TryRewriteIntegralKeyBound(operand, short.MinValue, short.MaxValue, @operator, out rewrittenOperator),
            LongIntegerType => TryRewriteIntegralKeyBound(operand, int.MinValue, int.MaxValue, @operator, out rewrittenOperator),
            BigIntType => TryRewriteIntegralKeyBound(operand, long.MinValue, long.MaxValue, @operator, out rewrittenOperator),
            MoneyType => operand is decimal money
                && TryRewriteScaledKeyBound(money, 4, @operator, currency: true, out rewrittenOperator),
            NumericType => operand is decimal number
                && column.NumericScale <= MaxDecimalScale
                && TryRewriteScaledKeyBound(number, column.NumericScale, @operator, currency: false, out rewrittenOperator),
            FloatType => operand switch
            {
                float single => IsExactFloatingBound(single, @operator),
                double wide => TryRewriteSingleKeyBound(wide, @operator, out rewrittenOperator),
                _ => false,
            },
            DoubleType => operand is double real && IsExactFloatingBound(real, @operator),
            DateTimeType => operand is DateTime date && IsDateKeyBound(date, @operator),

            // The operand is the key's own string, Guid, byte array or DateTime.
            TextType or MemoType => column.TextSortOrder.IsSupported,
            GuidType or BinaryType or DateTimeExtendedType => true,
            BooleanType or OleType or BigBinaryType or ComplexType or _ => false,
        };
    }

    private static bool IsOutward(ColumnPredicateOperator @operator, int comparison) =>
        RecoveryKernel.IsOutwardSeekBound((uint)@operator, comparison);

    private static bool TryApplyNearestRounding(
        ColumnPredicateOperator @operator,
        int comparison,
        out ColumnPredicateOperator rewrittenOperator)
    {
        uint rewritten = RecoveryKernel.RewriteRoundedSeekOperator((uint)@operator, comparison);
        if (rewritten == uint.MaxValue)
        {
            rewrittenOperator = @operator;
            return false;
        }

        rewrittenOperator = (ColumnPredicateOperator)rewritten;
        return true;
    }

    private static bool TryRewriteScaledKeyBound(
        decimal operand,
        byte scale,
        ColumnPredicateOperator @operator,
        bool currency,
        out ColumnPredicateOperator rewrittenOperator)
    {
        rewrittenOperator = @operator;
        decimal rounded = decimal.Round(operand, scale, MidpointRounding.ToEven);
        return (!currency || rounded is >= CurrencyMin and <= CurrencyMax)
            && TryApplyNearestRounding(@operator, rounded.CompareTo(operand), out rewrittenOperator);
    }

    private static bool TryRewriteSingleKeyBound(
        double operand,
        ColumnPredicateOperator @operator,
        out ColumnPredicateOperator rewrittenOperator)
    {
        rewrittenOperator = @operator;
        if (!IsExactFloatingBound(operand, @operator))
        {
            return false;
        }

        double rounded = (float)operand;
        if (rounded == operand)
        {
            return true;
        }

        return !double.IsInfinity(rounded)
            && TryApplyNearestRounding(@operator, rounded.CompareTo(operand), out rewrittenOperator);
    }

    private static bool IsExactFloatingBound(double operand, ColumnPredicateOperator @operator) =>
        !double.IsNaN(operand) && (operand != 0 || RecoveryKernel.IsFloatingZeroSeekBound((uint)@operator, BitConverter.DoubleToInt64Bits(operand) < 0));

    private static bool TryRewriteIntegralKeyBound(
        object operand,
        long min,
        long max,
        ColumnPredicateOperator @operator,
        out ColumnPredicateOperator rewrittenOperator)
    {
        rewrittenOperator = @operator;
        if (IsWholeNumberIn(operand, min, max))
        {
            return true;
        }

        return operand switch
        {
            double real => TryRewriteRoundedIntegralBound(real, min, max, @operator, out rewrittenOperator),
            float single => TryRewriteRoundedIntegralBound(single, min, max, @operator, out rewrittenOperator),
            decimal number => TryRewriteRoundedIntegralBound(number, min, max, @operator, out rewrittenOperator),
            _ => false,
        };
    }

    private static bool TryRewriteRoundedIntegralBound(
        double operand,
        long min,
        long max,
        ColumnPredicateOperator @operator,
        out ColumnPredicateOperator rewrittenOperator)
    {
        rewrittenOperator = @operator;
        double rounded = Math.Round(operand, MidpointRounding.ToEven);
        return rounded >= min
            && rounded <= max
            && TryApplyNearestRounding(@operator, rounded.CompareTo(operand), out rewrittenOperator);
    }

    private static bool TryRewriteRoundedIntegralBound(
        decimal operand,
        long min,
        long max,
        ColumnPredicateOperator @operator,
        out ColumnPredicateOperator rewrittenOperator)
    {
        rewrittenOperator = @operator;
        decimal rounded = decimal.Round(operand, 0, MidpointRounding.ToEven);
        return rounded >= min
            && rounded <= max
            && TryApplyNearestRounding(@operator, rounded.CompareTo(operand), out rewrittenOperator);
    }

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="operand"/>, a number of a type that
    /// holds every value of the key, is a whole number from <paramref name="min"/> to
    /// <paramref name="max"/>. The encoder would round a fraction, half to even, so a bound
    /// could move past a matching key, and it throws past the range.
    /// </summary>
    /// <param name="operand">The operand.</param>
    /// <param name="min">The key type's smallest value.</param>
    /// <param name="max">The key type's largest value.</param>
    /// <returns><see langword="true"/> when the key holds the operand exactly.</returns>
    private static bool IsWholeNumberIn(object operand, long min, long max) => operand switch
    {
        double real => Math.Truncate(real) == real && real >= min && real <= max,
        float single => Math.Truncate(single) == single && single >= min && single <= max,
        decimal number => decimal.Truncate(number) == number && number >= min && number <= max,
        ulong unsigned => unsigned <= (ulong)max,
        _ => IsInRange(Convert.ToInt64(operand, CultureInfo.InvariantCulture), min, max),
    };

    private static bool IsInRange(long value, long min, long max) => value >= min && value <= max;

    /// <summary>
    /// Returns <see langword="true"/> when the Date/Time key for <paramref name="value"/> bounds
    /// the comparison's matches. The key is <see cref="DateTime.ToOADate"/>, which keeps whole
    /// milliseconds, rounding toward 1899-12-30, turns <see cref="DateTime.MinValue"/> into
    /// 1899-12-30, and throws before the year 100. A key that moved the value down still bounds
    /// a lower bound from below, and one that moved it up an upper bound from above. Every stored
    /// date decodes, through <see cref="DateTime.FromOADate(double)"/>, to whole milliseconds, so
    /// an equality with a moved value matches no row and any key will do.
    /// </summary>
    /// <param name="value">The operand.</param>
    /// <param name="operator">The comparison.</param>
    /// <returns><see langword="true"/> when a seek for the key misses no matching row.</returns>
    private static bool IsDateKeyBound(DateTime value, ColumnPredicateOperator @operator)
    {
        if (value.Ticks >= TimeSpan.TicksPerDay && value.Year < 100)
        {
            return false;
        }

        long keyTicks = DateTime.FromOADate(value.ToOADate()).Ticks;

        if (keyTicks == value.Ticks || @operator == ColumnPredicateOperator.Equal)
        {
            return true;
        }

        return IsOutward(@operator, keyTicks.CompareTo(value.Ticks));
    }
}
