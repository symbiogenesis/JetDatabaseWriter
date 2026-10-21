namespace JetDatabaseWriter.Infrastructure;

using System;
using System.Globalization;

/// <summary>Probes built-in scalar conversions without throwing for ordinary incompatible values.</summary>
internal static class ScalarConversion
{
    internal static bool TryConvert(object? value, Type target, out object? result)
    {
        result = null;
        if (value is null or DBNull)
        {
            return false;
        }

        if (target.IsInstanceOfType(value))
        {
            result = value;
            return true;
        }

        if (target.IsEnum)
        {
            return false;
        }

        if (value is Enum)
        {
            if (target == typeof(string))
            {
                result = value.ToString();
                return true;
            }

            value = Convert.ChangeType(value, Enum.GetUnderlyingType(value.GetType()), CultureInfo.InvariantCulture);
        }

        if (target == typeof(Guid))
        {
            if (value is string guidText && Guid.TryParse(guidText, out Guid guid))
            {
                result = guid;
                return true;
            }

            return false;
        }

        if (value is not (string or bool or char or sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal or DateTime))
        {
            return false;
        }

        TypeCode code = Type.GetTypeCode(target);
        if (code == TypeCode.String)
        {
            result = Convert.ToString(value, CultureInfo.InvariantCulture);
            return true;
        }

        if (value is string text)
        {
            return TryParse(text, code, out result);
        }

        if (code == TypeCode.Char)
        {
            if (value is sbyte or byte or short or ushort or int or uint or long or ulong
                && Convert.ToDecimal(value, CultureInfo.InvariantCulture) is >= char.MinValue and <= char.MaxValue)
            {
                result = Convert.ToChar(value, CultureInfo.InvariantCulture);
                return true;
            }

            return false;
        }

        if (value is DateTime || code is TypeCode.DateTime or TypeCode.Object or TypeCode.Empty or TypeCode.DBNull)
        {
            return false;
        }

        if (value is char character)
        {
            if (code is TypeCode.Boolean or TypeCode.Single or TypeCode.Double or TypeCode.Decimal)
            {
                return false;
            }

            value = (ushort)character;
        }

        if (code == TypeCode.Boolean)
        {
            result = Convert.ToDouble(value, CultureInfo.InvariantCulture) != 0d;
            return true;
        }

        if (code is TypeCode.Single or TypeCode.Double)
        {
            result = code == TypeCode.Single
                ? (object)Convert.ToSingle(value, CultureInfo.InvariantCulture)
                : Convert.ToDouble(value, CultureInfo.InvariantCulture);
            return true;
        }

        if (code == TypeCode.Decimal)
        {
            if (value is float or double)
            {
                double number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
                if (double.IsNaN(number) || number < -7.922816251426433E28 || number > 7.922816251426433E28)
                {
                    return false;
                }
            }

            result = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            return true;
        }

        if (code is not (TypeCode.SByte or TypeCode.Byte or TypeCode.Int16 or TypeCode.UInt16 or TypeCode.Int32 or TypeCode.UInt32 or TypeCode.Int64 or TypeCode.UInt64))
        {
            return false;
        }

        bool fits;
        if (value is float or double)
        {
            double rounded = Math.Round(Convert.ToDouble(value, CultureInfo.InvariantCulture), MidpointRounding.ToEven);
            fits = code switch
            {
                TypeCode.SByte => rounded is >= sbyte.MinValue and <= sbyte.MaxValue,
                TypeCode.Byte => rounded is >= byte.MinValue and <= byte.MaxValue,
                TypeCode.Int16 => rounded is >= short.MinValue and <= short.MaxValue,
                TypeCode.UInt16 => rounded is >= ushort.MinValue and <= ushort.MaxValue,
                TypeCode.Int32 => rounded is >= int.MinValue and <= int.MaxValue,
                TypeCode.UInt32 => rounded is >= uint.MinValue and <= uint.MaxValue,
                TypeCode.Int64 => rounded is >= -9223372036854775808d and < 9223372036854775808d,
                TypeCode.UInt64 => rounded is >= 0d and < 18446744073709551616d,
                TypeCode.Empty or TypeCode.Object or TypeCode.DBNull or TypeCode.Boolean or TypeCode.Char or TypeCode.Single or TypeCode.Double or TypeCode.Decimal or TypeCode.DateTime or TypeCode.String => false,
                _ => false,
            };
        }
        else
        {
            decimal rounded = decimal.Round(Convert.ToDecimal(value, CultureInfo.InvariantCulture), 0, MidpointRounding.ToEven);
            fits = code switch
            {
                TypeCode.SByte => rounded is >= sbyte.MinValue and <= sbyte.MaxValue,
                TypeCode.Byte => rounded is >= byte.MinValue and <= byte.MaxValue,
                TypeCode.Int16 => rounded is >= short.MinValue and <= short.MaxValue,
                TypeCode.UInt16 => rounded is >= ushort.MinValue and <= ushort.MaxValue,
                TypeCode.Int32 => rounded is >= int.MinValue and <= int.MaxValue,
                TypeCode.UInt32 => rounded is >= uint.MinValue and <= uint.MaxValue,
                TypeCode.Int64 => rounded is >= long.MinValue and <= long.MaxValue,
                TypeCode.UInt64 => rounded is >= ulong.MinValue and <= ulong.MaxValue,
                TypeCode.Empty or TypeCode.Object or TypeCode.DBNull or TypeCode.Boolean or TypeCode.Char or TypeCode.Single or TypeCode.Double or TypeCode.Decimal or TypeCode.DateTime or TypeCode.String => false,
                _ => false,
            };
        }

        if (fits)
        {
            result = Convert.ChangeType(value, target, CultureInfo.InvariantCulture);
        }

        return fits;
    }

    private static bool TryParse(string text, TypeCode code, out object? result)
    {
        const NumberStyles integer = NumberStyles.Integer;
        const NumberStyles floating = NumberStyles.Float | NumberStyles.AllowThousands;
        CultureInfo culture = CultureInfo.InvariantCulture;
        result = null;
        switch ((int)code)
        {
            case (int)TypeCode.Boolean when bool.TryParse(text, out bool value):
                result = value;
                break;
            case (int)TypeCode.Char when text.Length == 1:
                result = text[0];
                break;
            case (int)TypeCode.SByte when sbyte.TryParse(text, integer, culture, out sbyte value):
                result = value;
                break;
            case (int)TypeCode.Byte when byte.TryParse(text, integer, culture, out byte value):
                result = value;
                break;
            case (int)TypeCode.Int16 when short.TryParse(text, integer, culture, out short value):
                result = value;
                break;
            case (int)TypeCode.UInt16 when ushort.TryParse(text, integer, culture, out ushort value):
                result = value;
                break;
            case (int)TypeCode.Int32 when int.TryParse(text, integer, culture, out int value):
                result = value;
                break;
            case (int)TypeCode.UInt32 when uint.TryParse(text, integer, culture, out uint value):
                result = value;
                break;
            case (int)TypeCode.Int64 when long.TryParse(text, integer, culture, out long value):
                result = value;
                break;
            case (int)TypeCode.UInt64 when ulong.TryParse(text, integer, culture, out ulong value):
                result = value;
                break;
            case (int)TypeCode.Single when float.TryParse(text, floating, culture, out float value):
                result = value;
                break;
            case (int)TypeCode.Double when double.TryParse(text, floating, culture, out double value):
                result = value;
                break;
            case (int)TypeCode.Decimal when decimal.TryParse(text, NumberStyles.Number, culture, out decimal value):
                result = value;
                break;
            case (int)TypeCode.DateTime when DateTime.TryParse(text, culture, DateTimeStyles.None, out DateTime value):
                result = value;
                break;
            default:
                return false;
        }

        return true;
    }
}
