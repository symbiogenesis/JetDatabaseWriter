namespace JetDatabaseReader;

using System;
using System.Globalization;

/// <summary>
/// Helper class for parsing string values into proper CLR types.
/// </summary>
internal static class TypedValueParser
{
#pragma warning disable CA1031 // Catch a more specific exception type
    public static object ParseValue(string value, Type targetType)
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
                TypeCode.Boolean => ParseBoolean(value),
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
        catch
        {
            return DBNull.Value;
        }
    }
#pragma warning restore CA1031

    private static bool ParseBoolean(string value)
    {
        if (string.Equals(value, "True", StringComparison.OrdinalIgnoreCase) ||
            value == "1" || value == "-1")
        {
            return true;
        }

        if (string.Equals(value, "False", StringComparison.OrdinalIgnoreCase) ||
            value == "0")
        {
            return false;
        }

        return bool.Parse(value);
    }

    private static byte[] ParseByteArray(string hexString)
    {
        // Format: "XX-XX-XX-XX" from BitConverter.ToString
        if (string.IsNullOrEmpty(hexString))
        {
            return [];
        }

        int count = (hexString.Length + 1) / 3;
        byte[] result = new byte[count];

        for (int i = 0; i < count; i++)
        {
            int offset = i * 3;
            result[i] = (byte)((HexCharToNibble(hexString[offset]) << 4) | HexCharToNibble(hexString[offset + 1]));
        }

        return result;
    }

    private static int HexCharToNibble(char c) =>
        c <= '9' ? c - '0' : (c & ~0x20) - 'A' + 10;
}
