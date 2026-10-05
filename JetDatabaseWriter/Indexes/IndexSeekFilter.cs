namespace JetDatabaseWriter.Indexes;

using System;
using System.Globalization;
using System.Reflection;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Mapping;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
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
/// <item><description>The column holds the operand's value exactly: a whole number in range for Byte, Integer, Long Integer and Large Number; at most four decimal places in range for Currency; at most the declared scale for Decimal; a value a <see cref="float"/> holds for Single; and never NaN. A Date/Time key keeps whole milliseconds, so it may also move the operand, as long as it moves away from the matches: down for a lower bound, up for an upper bound, either way for an equality, which then matches no row.</description></item>
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
    /// <paramref name="rowType"/>.
    /// </summary>
    /// <param name="pushable">The comparisons the translator extracted.</param>
    /// <param name="rowType">The type the rows map to, whose properties the comparisons read.</param>
    /// <param name="table">The table's definition, which gives each column's type.</param>
    /// <returns>The comparisons to seek; possibly empty.</returns>
    public static RowCriteria SelectSeekable(RowCriteria pushable, Type rowType, TableDef table)
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
                && CanSeek(column, property.PropertyType, predicate))
            {
                seekable.Add(predicate);
            }
        }

        return seekable;
    }

    /// <summary>
    /// Returns <see langword="true"/> when an index seek on <paramref name="column"/> for
    /// <paramref name="operand"/> returns every row whose <paramref name="propertyType"/>
    /// property satisfies the comparison.
    /// </summary>
    /// <param name="column">The key column.</param>
    /// <param name="propertyType">The type of the property the column maps to.</param>
    /// <param name="operator">The comparison: an equality or a one-sided range.</param>
    /// <param name="operand">The comparison's operand.</param>
    /// <returns><see langword="true"/> when the comparison can be sought.</returns>
    public static bool IsSeekable(ColumnInfo column, Type propertyType, ColumnPredicateOperator @operator, object? operand)
    {
        Guard.NotNull(column, nameof(column));
        Guard.NotNull(propertyType, nameof(propertyType));

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

        return HoldsEveryKey(keyType, operand.GetType()) && IsKeyBound(column, @operator, operand);
    }

    private static bool CanSeek(ColumnInfo column, Type propertyType, ColumnPredicate predicate) =>
        predicate.Operator == ColumnPredicateOperator.Between
            ? IsSeekable(column, propertyType, ColumnPredicateOperator.GreaterThanOrEqual, predicate.Operand)
                && IsSeekable(column, propertyType, ColumnPredicateOperator.LessThanOrEqual, predicate.UpperOperand)
            : IsSeekable(column, propertyType, predicate.Operator, predicate.Operand);

    private static bool HoldsEveryKey(Type keyType, Type type) =>
        type == keyType || ClosureValueReader.IsLosslessWidening(keyType, type);

    private static bool IsKeyBound(ColumnInfo column, ColumnPredicateOperator @operator, object operand) => column.Type switch
    {
        ByteType => IsWholeNumberIn(operand, byte.MinValue, byte.MaxValue),
        IntegerType => IsWholeNumberIn(operand, short.MinValue, short.MaxValue),
        LongIntegerType => IsWholeNumberIn(operand, int.MinValue, int.MaxValue),
        BigIntType => IsWholeNumberIn(operand, long.MinValue, long.MaxValue),
        MoneyType => operand is decimal money && money == decimal.Round(money, 4) && money >= CurrencyMin && money <= CurrencyMax,
        NumericType => operand is decimal number && column.NumericScale <= MaxDecimalScale && number == decimal.Round(number, column.NumericScale),
        FloatType => operand switch
        {
            float single => !float.IsNaN(single),
            double wide => !double.IsNaN(wide) && (double)(float)wide == wide,
            _ => false,
        },
        DoubleType => operand is double real && !double.IsNaN(real),
        DateTimeType => operand is DateTime date && IsDateKeyBound(date, @operator),

        // The operand is the key's own string, Guid, byte array or DateTime.
        TextType or MemoType => column.TextSortOrder.IsSupported,
        GuidType or BinaryType or DateTimeExtendedType => true,
        BooleanType or OleType or AttachmentType or ComplexType or _ => false,
    };

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
        long keyTicks;
        try
        {
            keyTicks = DateTime.FromOADate(value.ToOADate()).Ticks;
        }
        catch (OverflowException)
        {
            return false;
        }

        if (keyTicks == value.Ticks || @operator == ColumnPredicateOperator.Equal)
        {
            return true;
        }

        return keyTicks < value.Ticks
            ? @operator is ColumnPredicateOperator.GreaterThan or ColumnPredicateOperator.GreaterThanOrEqual
            : @operator is ColumnPredicateOperator.LessThan or ColumnPredicateOperator.LessThanOrEqual;
    }
}
