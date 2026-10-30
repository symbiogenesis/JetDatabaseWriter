namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Globalization;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;

internal static class CalculatedExpressionCoercion
{
    /// <summary>The OLE Automation serial of 0100-01-01, the earliest Access date.</summary>
    private const double MinOleDate = -657434d;

    /// <summary>The OLE Automation serial of 10000-01-01, one day past the latest Access date.</summary>
    private const double MaxOleDateExclusive = 2958466d;

    private const long MillisecondsPerDay = TimeSpan.TicksPerDay / TimeSpan.TicksPerMillisecond;

    private const long MinOleDateMilliseconds = (long)MinOleDate * MillisecondsPerDay;

    private const long MaxOleDateMillisecondsExclusive = (long)MaxOleDateExclusive * MillisecondsPerDay;

    /// <summary>OLE Automation day 0, 1899-12-30, whose date part General Date text leaves out.</summary>
    private static readonly DateTime OleDateZero = new(1899, 12, 30);

    private static readonly string[] DateParseFormats = [
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd",
        "M/d/yyyy h:mm:ss tt",
        "M/d/yyyy",
        "MM/dd/yyyy",
    ];

    private static ReadOnlySpan<double> DecimalPowersOfTen =>
    [
        1d, 1E1, 1E2, 1E3, 1E4, 1E5, 1E6, 1E7, 1E8, 1E9,
        1E10, 1E11, 1E12, 1E13, 1E14, 1E15, 1E16, 1E17, 1E18, 1E19,
        1E20, 1E21, 1E22, 1E23, 1E24, 1E25, 1E26, 1E27, 1E28,
    ];

    internal static object CoerceResult(object? value, Type targetType, bool isCalculatedColumn = false)
    {
        if (IsNull(value))
        {
            return DBNull.Value;
        }

        if (isCalculatedColumn && targetType == typeof(byte[]))
        {
            return CalculatedExpressionBinaryCoercion.ConvertValue(value!);
        }

        if (value is AccessCurrency currency)
        {
            if (targetType == typeof(double))
            {
                return currency.ToDouble();
            }

            if (targetType == typeof(float))
            {
                return (float)currency.ToDouble();
            }

            value = currency.Value;
        }

        // A column of no specific type keeps the value as evaluated.
        if (targetType == typeof(object))
        {
            return value!;
        }

        // Access converts True to -1 in numeric calculated results. A Byte
        // result then overflows; the calculated context stores a native error
        // envelope, distinct from 255 and ordinary Null. Defaults keep the OLE Automation
        // Bool-to-Byte conversion used by CByte.
        if (value is bool boolean && targetType != typeof(bool) && targetType != typeof(string))
        {
            if (targetType == typeof(byte) && !isCalculatedColumn)
            {
                return ToByte(boolean);
            }

            value = boolean ? -1 : 0;
        }

        if (targetType == typeof(string))
        {
            return ToText(value);
        }

        if (targetType == typeof(bool))
        {
            return isCalculatedColumn ? ToCalculatedBooleanResult(value!) : ToBoolean(value);
        }

        if (targetType == typeof(byte))
        {
            return ToByte(value);
        }

        if (targetType == typeof(short))
        {
            return Convert.ToInt16(value, CultureInfo.InvariantCulture);
        }

        if (targetType == typeof(int))
        {
            return Convert.ToInt32(value, CultureInfo.InvariantCulture);
        }

        if (targetType == typeof(long))
        {
            return Convert.ToInt64(value, CultureInfo.InvariantCulture);
        }

        if (targetType == typeof(float))
        {
            return ToSingle(value);
        }

        if (targetType == typeof(double))
        {
            return ToDouble(value);
        }

        if (targetType == typeof(decimal))
        {
            return ToDecimal(value);
        }

        if (targetType == typeof(DateTime))
        {
            return ToDateTime(value);
        }

        if (targetType == typeof(Guid))
        {
            return value is Guid guid ? guid : Guid.Parse(ToText(value));
        }

        return value!;
    }

    /// <summary>
    /// Converts a caller-supplied column value to the column's declared type
    /// before an expression sees it, with the same <see cref="Convert"/> call
    /// the row encoder uses to store it. The writer accepts <c>"10"</c> for an
    /// Integer column and <c>10</c> for a Text column, and the evaluator's text
    /// rules (<c>+</c> joins two strings, two strings compare as text) must
    /// follow the field's type, not the CLR type the caller happened to pass.
    /// A value the encoder could not store either is passed through unchanged,
    /// so the encoder still reports it.
    /// </summary>
    /// <param name="value">The value supplied for the column.</param>
    /// <param name="declaredType">The column's declared CLR type.</param>
    /// <returns>The value as the column will hold it.</returns>
    internal static object CoerceInput(object? value, Type declaredType)
    {
        if (IsNull(value))
        {
            return DBNull.Value;
        }

        if (declaredType.IsInstanceOfType(value))
        {
            return value!;
        }

        // Hyperlinks, byte[] payloads and other non-scalar values keep their shape.
        if (value is not IConvertible)
        {
            return value!;
        }

        return (IsStoredScalarType(declaredType) || declaredType == typeof(Guid)) && ScalarConversion.TryConvert(value, declaredType, out object? converted)
            ? converted!
            : value!;
    }

    internal static object CompareValues(object left, object right, Func<int, bool> predicate, JetDatabaseWriter.Indexes.Collation.JetTextCollation? textCollation = null)
    {
        if (IsNull(left) || IsNull(right))
        {
            return DBNull.Value;
        }

        return CompareNonNullValues(left, right, predicate, textCollation);
    }

    internal static bool CompareNonNullValues(object left, object right, Func<int, bool> predicate, JetDatabaseWriter.Indexes.Collation.JetTextCollation? textCollation = null)
    {
        int comparison;
        if (left is string or Guid && right is string or Guid)
        {
            // Two strings compare as text in Access, even when both look numeric ("10" < "9").
            comparison = (textCollation ?? JetDatabaseWriter.Indexes.Collation.JetTextCollation.GeneralLegacy).Compare(ToText(left), ToText(right));
        }
        else if ((left is DateTime || right is DateTime)
            && TryConvertDateTime(left, out DateTime leftDateValue)
            && TryConvertDateTime(right, out DateTime rightDateValue))
        {
            // A date against a number (a date serial) or date text compares as dates.
            // Comparing DateTimes rather than serials keeps dates before 1899-12-30
            // in order, where a negative serial's fraction counts forward.
            comparison = leftDateValue.CompareTo(rightDateValue);
        }
        else if (TryConvertDecimal(left, out decimal leftDecimal) && TryConvertDecimal(right, out decimal rightDecimal))
        {
            comparison = leftDecimal.CompareTo(rightDecimal);
        }
        else if (TryConvertDateTime(left, out DateTime leftDate) && TryConvertDateTime(right, out DateTime rightDate))
        {
            comparison = leftDate.CompareTo(rightDate);
        }
        else if (left is bool || right is bool)
        {
            comparison = ToBoolean(left).CompareTo(ToBoolean(right));
        }
        else
        {
            comparison = (textCollation ?? JetDatabaseWriter.Indexes.Collation.JetTextCollation.GeneralLegacy).Compare(ToText(left), ToText(right));
        }

        return predicate(comparison);
    }

    internal static bool IsNull(object? value) => value is null or DBNull;

    internal static double DeterministicRandomValue(double seed)
    {
        uint state = unchecked((uint)(int)Math.Truncate(seed * 1000000d));
        state ^= 0x6C8E9CF5u;
        state = unchecked((state * 1664525u) + 1013904223u);
        return (state & 0x00FFFFFFu) / 16777216d;
    }

    internal static string NormalizeFunctionName(string name)
    {
        string upperName = name.ToUpperInvariant();
        return upperName.EndsWith('$') ? upperName[..^1] : upperName;
    }

    internal static bool TryGetBuiltinConstant(string name, out object value)
    {
        switch (name.ToUpperInvariant())
        {
            case "VBEMPTY":
            case "VBFALSE":
            case "VBUSESYSTEM":
            case "VBGENERALDATE":
            case "VBBINARYCOMPARE":
                value = 0;
                return true;
            case "VBTRUE":
            case "VBUSECOMPAREOPTION":
                value = -1;
                return true;
            case "VBINTEGER":
            case "VBMONDAY":
            case "VBSHORTDATE":
            case "VBLOWERCASE":
            case "VBTEXTCOMPARE":
            case "VBFIRSTFOURDAYS":
                value = 2;
                return true;
            case "VBLONG":
            case "VBTUESDAY":
            case "VBLONGTIME":
            case "VBPROPERCASE":
            case "VBFIRSTFULLWEEK":
                value = 3;
                return true;
            case "VBSINGLE":
            case "VBWEDNESDAY":
            case "VBSHORTTIME":
                value = 4;
                return true;
            case "VBDOUBLE":
            case "VBTHURSDAY":
                value = 5;
                return true;
            case "VBCURRENCY":
            case "VBFRIDAY":
                value = 6;
                return true;
            case "VBDATE":
            case "VBSATURDAY":
                value = 7;
                return true;
            case "VBSTRING":
                value = 8;
                return true;
            case "VBOBJECT":
                value = 9;
                return true;
            case "VBERROR":
                value = 10;
                return true;
            case "VBBOOLEAN":
                value = 11;
                return true;
            case "VBVARIANT":
                value = 12;
                return true;
            case "VBDECIMAL":
                value = 14;
                return true;
            case "VBBYTE":
                value = 17;
                return true;
            case "VBUSEDEFAULT":
                value = -2;
                return true;
            case "VBSUNDAY":
            case "VBNULL":
            case "VBLONGDATE":
            case "VBUPPERCASE":
            case "VBFIRSTJAN1":
                value = 1;
                return true;
            case "VBUNICODE":
                value = 64;
                return true;
            case "VBDATABASECOMPARE":
                value = 2;
                return true;
            default:
                value = DBNull.Value;
                return false;
        }
    }

    /// <summary>
    /// Converts a value to text the way the Access expression service does:
    /// True is <c>"-1"</c> and False is <c>"0"</c>, so <c>"x" &amp; (1 &lt; 2)</c>
    /// is <c>"x-1"</c> (<see cref="Convert"/> gives <c>"True"</c>), and a date is
    /// General Date text (<see cref="ToGeneralDateText"/>).
    /// </summary>
    /// <param name="value">The value to convert.</param>
    /// <returns>The text value; empty for Null.</returns>
    internal static string ToText(object? value)
    {
        if (value is Guid guid)
        {
            return guid.ToString("B").ToUpperInvariant();
        }

        if (value is AccessCurrency currency)
        {
            return currency.ToString();
        }

        if (value is bool boolean)
        {
            return boolean ? "-1" : "0";
        }

        if (value is DateTime date)
        {
            return ToGeneralDateText(date);
        }

        if (value is Guid guid)
        {
            return guid.ToString("B", CultureInfo.InvariantCulture).ToUpperInvariant();
        }

        if (value is double doubleValue)
        {
            return ToFloatingText(doubleValue, 15);
        }

        if (value is float singleValue)
        {
            return ToFloatingText(singleValue, 7);
        }

        if (value is decimal decimalValue)
        {
            return decimalValue.ToString("0.############################", CultureInfo.InvariantCulture);
        }

        return IsNull(value) ? string.Empty : Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
    }

    /// <summary>
    /// Converts a date to text in VBA's General Date form for the en-US locale, as OLE
    /// Automation's <c>VarBstrFromDate</c> gives it to <c>CStr</c> and <c>&amp;</c>
    /// (measured with oleaut32 on Windows, LCID 1033), whatever the current culture:
    /// <c>1/31/2020 6:00:00 AM</c>. A date at midnight has no time part
    /// (<c>1/31/2020</c>), and a time on day 0, 1899-12-30, has no date part
    /// (<c>6:00:00 AM</c>; day 0 at midnight is <c>12:00:00 AM</c>). More than half a
    /// second rounds up to the next second; half a second or less is dropped.
    /// </summary>
    /// <param name="value">The date.</param>
    /// <returns>The General Date text.</returns>
    internal static string ToGeneralDateText(DateTime value)
    {
        long fraction = value.Ticks % TimeSpan.TicksPerSecond;
        DateTime rounded = value.AddTicks(-fraction);
        if (fraction > TimeSpan.TicksPerSecond / 2 && (DateTime.MaxValue - rounded).Ticks >= TimeSpan.TicksPerSecond)
        {
            rounded = rounded.AddSeconds(1);
        }

        if (rounded.Date == OleDateZero)
        {
            return rounded.ToString("h:mm:ss tt", CultureInfo.InvariantCulture);
        }

        string date = rounded.Month.ToString(CultureInfo.InvariantCulture)
            + "/" + rounded.Day.ToString(CultureInfo.InvariantCulture)
            + "/" + rounded.Year.ToString(CultureInfo.InvariantCulture);
        return rounded.TimeOfDay == TimeSpan.Zero ? date : date + " " + rounded.ToString("h:mm:ss tt", CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Maps a Boolean argument to Access's -1 / 0 before a <see cref="Convert"/>
    /// call, which would otherwise turn True into 1 (<c>CInt(True)</c> is -1).
    /// </summary>
    /// <param name="value">The function argument.</param>
    /// <returns>The argument, with a Boolean replaced by -1 or 0.</returns>
    internal static object? AsAccessNumber(object? value)
    {
        if (value is AccessCurrency currency)
        {
            return currency.Value;
        }

        if (value is bool boolean)
        {
            return boolean ? -1 : 0;
        }

        return value;
    }

    /// <summary>
    /// Converts a value to a number the way Access does: True is -1 and False
    /// is 0 (<see cref="Convert"/> and Excel map True to 1).
    /// </summary>
    /// <param name="value">The value to convert.</param>
    /// <returns>The numeric value.</returns>
    /// <exception cref="OverflowException">A floating value is nonfinite or outside the Access Decimal range.</exception>
    internal static decimal ToDecimal(object? value)
    {
        if (value is AccessCurrency currency)
        {
            return currency.Value;
        }

        if (value is bool boolean)
        {
            return boolean ? -1m : 0m;
        }

        if (value is float or double)
        {
            return TryConvertFloatingDecimal(ToDouble(value), value is float, out decimal result)
                ? result
                : throw new OverflowException("The value is outside the range of an Access Decimal.");
        }

        return Convert.ToDecimal(value, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Converts a value to a Byte the way OLE Automation does: True is 255 and
    /// False is 0 (<c>CByte(True)</c> is 255), other values round half to even,
    /// and anything outside 0..255 throws <see cref="OverflowException"/>.
    /// </summary>
    /// <param name="value">The value to convert.</param>
    /// <returns>The byte value.</returns>
    internal static byte ToByte(object? value)
    {
        if (value is AccessCurrency currency)
        {
            return Convert.ToByte(currency.Value, CultureInfo.InvariantCulture);
        }

        if (value is bool boolean)
        {
            return boolean ? byte.MaxValue : (byte)0;
        }

        return Convert.ToByte(value, CultureInfo.InvariantCulture);
    }

    internal static double ToDouble(object? value)
    {
        if (value is AccessCurrency currency)
        {
            return currency.ToDouble();
        }

        if (value is bool boolean)
        {
            return boolean ? -1d : 0d;
        }

        return value is decimal number ? DecimalToDouble(number) : Convert.ToDouble(value, CultureInfo.InvariantCulture);
    }

    internal static float ToSingle(object? value)
        => value switch
        {
            AccessCurrency currency => (float)currency.ToDouble(),
            decimal number => (float)DecimalToDouble(number),
            _ => Convert.ToSingle(AsAccessNumber(value), CultureInfo.InvariantCulture),
        };

    internal static AccessCurrency ToCurrency(object? value)
        => value is float or double
            ? AccessCurrency.FromDouble(ToDouble(value))
            : AccessCurrency.FromDecimal(ToDecimal(value));

    private static double DecimalToDouble(decimal value)
    {
#if NET6_0_OR_GREATER
        Span<int> bits = stackalloc int[4];
        decimal.GetBits(value, bits);
#else
        int[] bits = decimal.GetBits(value);
#endif
        ulong low = unchecked((uint)bits[0]) | ((ulong)unchecked((uint)bits[1]) << 32);
        double high = unchecked((uint)bits[2]) * 18446744073709551616d;

        // Keep OLE's two limb conversions, addition and scale division. The
        // correctly rounded .NET 11 conversion changes native expression bits.
        double result = (low + high) / DecimalPowersOfTen[(bits[3] >> 16) & 0xFF];
        return bits[3] < 0 ? -result : result;
    }

    internal static bool ToBoolean(object? value)
    {
        if (IsNull(value))
        {
            return false;
        }

        if (value is bool boolean)
        {
            return boolean;
        }

        if (TryConvertDecimal(value, out decimal numeric))
        {
            return numeric != 0m;
        }

        string text = ToText(value);
        return bool.TryParse(text, out bool parsed) ? parsed : !string.IsNullOrEmpty(text);
    }

    internal static bool ToCalculatedCondition(object value) => value switch
    {
        DBNull => false,
        string or Guid => true,
        float number => number != 0f,
        double number => number != 0d,
        NativeDate date => date.Serial != 0d,
        DateTime date => date.ToOADate() != 0d,
        _ => ToBoolean(value),
    };

    /// <summary>Maps nonnumeric strings, including empty strings, to native True operands.</summary>
    /// <param name="value">The operand.</param>
    /// <returns>The native bitwise operand; numeric strings retain integer conversion.</returns>
    internal static object ToCalculatedBitwiseOperand(object value)
        => value is string text && !double.TryParse(text, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out _) ? true : value;

    private static bool ToCalculatedBooleanResult(object value)
    {
        if (value is bool boolean)
        {
            return boolean;
        }

        try
        {
            return checked((short)AccessVariantOperators.ToRoundedLong(value)) != 0;
        }
        catch (OverflowException exception) when (value is decimal)
        {
            // Native Decimal-to-Boolean overflow reports type mismatch; Long
            // and Double sources instead retain numeric overflow status.
            throw new InvalidCastException("The Decimal result cannot be converted to a calculated Boolean.", exception);
        }
    }

    /// <summary>
    /// Converts a value to a date the way OLE Automation does: a number or a
    /// Boolean is a date serial, the days since 1899-12-30 (True is -1, so
    /// 1899-12-29), and text is parsed as a date (<see cref="ParseDate"/>), where a
    /// time with no date is on day 0.
    /// </summary>
    /// <param name="value">The value to convert.</param>
    /// <returns>The date.</returns>
    /// <exception cref="OverflowException">The serial is outside the dates Access can hold (years 100 to 9999).</exception>
    internal static DateTime ToDateTime(object? value)
    {
        if (value is DateTime dateTime)
        {
            return dateTime;
        }

        if (value is bool or byte or short or int or long or float or double or decimal or AccessCurrency)
        {
            return FromOleDate(ToDouble(value));
        }

        return ParseDate(ToText(value));
    }

    /// <summary>
    /// Converts an OLE Automation date serial to a date, rejecting a serial
    /// outside the range Access dates can hold (years 100 to 9999).
    /// </summary>
    /// <param name="serial">The days since 1899-12-30; the fraction is the time of day.</param>
    /// <returns>The date.</returns>
    /// <exception cref="OverflowException">The serial is outside the supported range.</exception>
    internal static DateTime FromOleDate(double serial)
    {
        if (!IsOleDateInRange(serial))
        {
            throw new OverflowException($"Date serial {serial.ToString("R", CultureInfo.InvariantCulture)} is outside the range of an Access date.");
        }

        return DateTime.FromOADate(serial);
    }

    private static bool IsOleDateInRange(double serial)
    {
        if (double.IsNaN(serial) || serial < MinOleDate || serial >= MaxOleDateExclusive)
        {
            return false;
        }

        // OLE Automation rounds to whole milliseconds. Negative fractions
        // describe a positive time of day on the serial's negative date.
        long milliseconds = (long)((serial * MillisecondsPerDay) + (serial >= 0d ? 0.5d : -0.5d));
        if (milliseconds < 0)
        {
            milliseconds -= 2 * (milliseconds % MillisecondsPerDay);
        }

        return milliseconds is >= MinOleDateMilliseconds and < MaxOleDateMillisecondsExclusive;
    }

    /// <summary>
    /// Returns the time of day of <paramref name="value"/> on OLE Automation day 0,
    /// 1899-12-30, which is where VBA puts a time that has no date: <c>Time</c>,
    /// <c>TimeValue</c> and <c>CDate("6:00")</c>.
    /// </summary>
    /// <param name="value">The date whose time of day is kept.</param>
    /// <returns>The time on day 0.</returns>
    internal static DateTime TimeOnDayZero(DateTime value) => OleDateZero + value.TimeOfDay;

    /// <summary>
    /// Parses date text. Text with a time and no date is a time on day 0, 1899-12-30,
    /// as in VBA (<c>CDate("6:00:00 AM")</c> is 0.25). So the General Date text of a
    /// day-0 date (<see cref="ToGeneralDateText"/>) reads back as the same date, and a
    /// <c>#6:00#</c> literal is day 0 too.
    /// </summary>
    /// <param name="text">The text to parse.</param>
    /// <returns>The date.</returns>
    /// <exception cref="FormatException">The text is not a date.</exception>
    internal static DateTime ParseDate(string text)
    {
        if (DateTime.TryParseExact(text, DateParseFormats, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out DateTime exact))
        {
            return exact;
        }

        // Without NoCurrentDateDefault, text with no date part takes today's date.
        // With it, that text gets 0001-01-01, a date no Access date can have.
        var parsed = DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.NoCurrentDateDefault);
        return parsed.Date == DateTime.MinValue ? TimeOnDayZero(parsed) : parsed;
    }

    internal static bool TryConvertDecimal(object? value, out decimal result)
    {
        if (value is AccessCurrency currency)
        {
            result = currency.Value;
            return true;
        }

        if (value is bool boolean)
        {
            result = boolean ? -1m : 0m;
            return true;
        }

        if (value is float or double)
        {
            return TryConvertFloatingDecimal(ToDouble(value), value is float, out result);
        }

        if (ScalarConversion.TryConvert(value, typeof(decimal), out object? converted))
        {
            result = (decimal)converted!;
            return true;
        }

        result = 0;
        return false;
    }

    internal static bool TryConvertDateTime(object? value, out DateTime result)
    {
        if (value is DateTime dateTime)
        {
            result = dateTime;
            return true;
        }

        if (value is bool or byte or short or int or long or float or double or decimal or AccessCurrency)
        {
            double serial = ToDouble(value);
            if (IsOleDateInRange(serial))
            {
                result = FromOleDate(serial);
                return true;
            }
        }
        else if (!IsNull(value))
        {
            string text = ToText(value);
            if (DateTime.TryParseExact(text, DateParseFormats, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out result))
            {
                return true;
            }

            if (DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.NoCurrentDateDefault, out DateTime parsed))
            {
                result = parsed.Date == DateTime.MinValue ? TimeOnDayZero(parsed) : parsed;
                return true;
            }
        }

        result = default;
        return false;
    }

    private static bool TryConvertFloatingDecimal(double number, bool single, out decimal result)
    {
        // Adapted from dotnet/runtime v10.0.0 Decimal.DecCalc VarDecFromR4/R8;
        // see THIRD-PARTY-NOTICES.md. Native Jet4 field controls pin this OLE
        // conversion policy independently of the executing .NET runtime.
        result = 0m;
        int exponent = (int)((BitConverter.DoubleToInt64Bits(number) >> 52) & 0x7FF) - 1022;
        if (exponent > 96)
        {
            return false;
        }

        if (exponent < -94)
        {
            return true;
        }

        bool negative = number < 0d;
        number = Math.Abs(number);
        int scale = Math.Min(28, (single ? 6 : 14) - ((exponent * 19728) >> 16));
        double lower = single ? 1E6 : 1E14;
        if (scale >= 0)
        {
            number *= DecimalPowersOfTen[scale];
        }
        else if (scale != -1 || number >= lower * 10d)
        {
            number /= DecimalPowersOfTen[-scale];
        }
        else
        {
            scale = 0;
        }

        if (number < lower && scale < 28)
        {
            number *= 10d;
            scale++;
        }

        ulong mantissa = (ulong)Math.Round(number, MidpointRounding.ToEven);
        if (mantissa == 0)
        {
            return true;
        }

        if (scale < 0)
        {
            result = mantissa;
            for (; scale < 0; scale++)
            {
                result *= 10m;
            }

            result = negative ? -result : result;
        }
        else
        {
            while (scale > 0 && mantissa % 10 == 0)
            {
                mantissa /= 10;
                scale--;
            }

            result = new decimal(unchecked((int)(uint)mantissa), (int)(mantissa >> 32), 0, negative, (byte)scale);
        }

        return true;
    }

    private static string ToFloatingText(double value, int precision)
    {
        if (value == 0d)
        {
            return "0";
        }

        string text = value.ToString("G" + precision.ToString(CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
        int exponentPosition = text.IndexOf('E', StringComparison.Ordinal);
        if (exponentPosition < 0)
        {
            int decimalPosition = text.IndexOf('.', StringComparison.Ordinal);
            return decimalPosition >= 0 && text.Length - decimalPosition - 1 > precision
                ? value.ToString("0." + new string('#', precision - 1) + "E+00", CultureInfo.InvariantCulture)
                : text;
        }

        int exponent = int.Parse(text.AsSpan(exponentPosition + 1), NumberStyles.Integer, CultureInfo.InvariantCulture);
        int digits = exponentPosition - (text[0] == '-' ? 1 : 0) - (text.Contains('.', StringComparison.Ordinal) ? 1 : 0);
        return exponent < 0 && digits - 1 - exponent <= precision
            ? value.ToString("0." + new string('#', precision), CultureInfo.InvariantCulture)
            : text;
    }

    private static bool IsStoredScalarType(Type type)
        => type == typeof(string) || type == typeof(bool) || type == typeof(byte) || type == typeof(short)
            || type == typeof(int) || type == typeof(long) || type == typeof(float) || type == typeof(double)
            || type == typeof(decimal) || type == typeof(DateTime);
}
