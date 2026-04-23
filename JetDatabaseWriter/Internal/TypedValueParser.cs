namespace JetDatabaseWriter;

using System;
using System.Globalization;

/// <summary>
/// Helper class for parsing string values into proper CLR types.
/// </summary>
internal static class TypedValueParser
{
#pragma warning disable CA1031 // Catch a more specific exception type
    public static object ParseValue(string value, Type targetType, bool strictMode = true)
    {
        if (string.IsNullOrEmpty(value))
        {
            return DBNull.Value;
        }

        try
        {
            return Type.GetTypeCode(targetType) switch
            {
                TypeCode.String => value,
                TypeCode.Boolean => Convert.ToBoolean(value, CultureInfo.InvariantCulture),
                TypeCode.Byte => byte.Parse(value, CultureInfo.InvariantCulture),
                TypeCode.Int16 => short.Parse(value, CultureInfo.InvariantCulture),
                TypeCode.Int32 => int.Parse(value, CultureInfo.InvariantCulture),
                TypeCode.Int64 => long.Parse(value, CultureInfo.InvariantCulture),
                TypeCode.Single => float.Parse(value, CultureInfo.InvariantCulture),
                TypeCode.Double => double.Parse(value, CultureInfo.InvariantCulture),
                TypeCode.Decimal => decimal.Parse(value, CultureInfo.InvariantCulture),
                TypeCode.DateTime => DateTime.Parse(value, CultureInfo.InvariantCulture),
                _ when targetType == typeof(Guid) => Guid.Parse(value),
                _ when targetType == typeof(byte[]) => ParseByteArray(value),
                _ => value,
            };
        }
        catch (Exception) when (!strictMode)
        {
            return DBNull.Value;
        }
        catch (Exception ex)
        {
            throw new FormatException(
                $"Failed to parse value '{value}' as {targetType.FullName}. " +
                "Disable strict mode (strictMode: false) to silently coerce unparseable values to DBNull.",
                ex);
        }
    }
#pragma warning restore CA1031

    // No longer needed: replaced by Convert.ToBoolean in ParseValue

    private static byte[] ParseByteArray(string hexString)
    {
        // Format: "XX-XX-XX-XX" from BitConverter.ToString
        if (string.IsNullOrEmpty(hexString))
        {
            return [];
        }

        // Try to use Convert.FromHexString if input is a plain hex string (no dashes)
#if NET5_0_OR_GREATER
        if (!hexString.Contains('-'))
        {
            return Convert.FromHexString(hexString);
        }
#endif

        // Fallback: dash-separated format ("XX-XX-XX-XX")
        string[] hexValues = hexString.Split('-', StringSplitOptions.RemoveEmptyEntries);
        byte[] bytes = new byte[hexValues.Length];
        for (int i = 0; i < hexValues.Length; i++)
        {
            bytes[i] = Convert.ToByte(hexValues[i], 16);
        }

        return bytes;
    }
}
