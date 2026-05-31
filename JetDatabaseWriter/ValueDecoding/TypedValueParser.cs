namespace JetDatabaseWriter.ValueDecoding;

using System;
using System.Globalization;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;

/// <summary>
/// Helper class for parsing string values into proper CLR types.
/// </summary>
internal static class TypedValueParser
{
    public static object ParseValue(string value, Type targetType, bool strictMode = true)
    {
        if (string.IsNullOrEmpty(value))
        {
            return DBNull.Value;
        }

        return TryParseValue(value, targetType, out object parsedValue, out string? failure)
            ? parsedValue
            : ApplyParseFailurePolicy(value, targetType, strictMode, failure);
    }

    private static DBNull ApplyParseFailurePolicy(string value, Type targetType, bool strictMode, string? failure)
    {
        if (!strictMode)
        {
            return DBNull.Value;
        }

        throw new FormatException(
            $"Failed to parse value '{value}' as {targetType.FullName}: {failure ?? "unrecognized value"}. " +
            "Disable strict mode (strictMode: false) to coerce unparseable values to DBNull.Value.");
    }

    private static bool TryParseValue(string value, Type targetType, out object parsedValue, out string? failure)
    {
        parsedValue = DBNull.Value;
        failure = null;

        switch (Type.GetTypeCode(targetType))
        {
            case TypeCode.String:
                parsedValue = value;
                return true;
            case TypeCode.Boolean:
                if (bool.TryParse(value, out bool boolValue))
                {
                    parsedValue = boolValue;
                    return true;
                }

                failure = "expected True or False";
                return false;
            case TypeCode.Byte:
                if (byte.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out byte byteValue))
                {
                    parsedValue = byteValue;
                    return true;
                }

                failure = "expected an unsigned 8-bit integer";
                return false;
            case TypeCode.Int16:
                if (short.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out short shortValue))
                {
                    parsedValue = shortValue;
                    return true;
                }

                failure = "expected a signed 16-bit integer";
                return false;
            case TypeCode.Int32:
                if (int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int intValue))
                {
                    parsedValue = intValue;
                    return true;
                }

                failure = "expected a signed 32-bit integer";
                return false;
            case TypeCode.Int64:
                if (long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out long longValue))
                {
                    parsedValue = longValue;
                    return true;
                }

                failure = "expected a signed 64-bit integer";
                return false;
            case TypeCode.Single:
                if (float.TryParse(value, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out float floatValue))
                {
                    parsedValue = floatValue;
                    return true;
                }

                failure = "expected a single-precision floating-point number";
                return false;
            case TypeCode.Double:
                if (double.TryParse(value, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out double doubleValue))
                {
                    parsedValue = doubleValue;
                    return true;
                }

                failure = "expected a double-precision floating-point number";
                return false;
            case TypeCode.Decimal:
                if (decimal.TryParse(value, NumberStyles.Number, CultureInfo.InvariantCulture, out decimal decimalValue))
                {
                    parsedValue = decimalValue;
                    return true;
                }

                failure = "expected a decimal number";
                return false;
            case TypeCode.DateTime:
                if (DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime dateTimeValue))
                {
                    parsedValue = dateTimeValue;
                    return true;
                }

                failure = "expected a date/time value";
                return false;
            case TypeCode.Empty:
            case TypeCode.Object:
            case TypeCode.DBNull:
            case TypeCode.Char:
            case TypeCode.SByte:
            case TypeCode.UInt16:
            case TypeCode.UInt32:
            case TypeCode.UInt64:
                break;
            default:
                failure = $"unsupported target type {targetType}";
                return false;
        }

        if (targetType == typeof(Guid))
        {
            if (Guid.TryParse(value, out Guid guidValue))
            {
                parsedValue = guidValue;
                return true;
            }

            failure = "expected a GUID";
            return false;
        }

        if (targetType == typeof(byte[]))
        {
            if (TryParseByteArray(value, out byte[] bytes, out failure))
            {
                parsedValue = bytes;
                return true;
            }

            return false;
        }

        if (targetType == typeof(Hyperlink))
        {
            parsedValue = (object?)Hyperlink.Parse(value) ?? DBNull.Value;
            return true;
        }

        parsedValue = value;
        return true;
    }

    private static bool TryParseByteArray(string value, out byte[] bytes, out string? failure)
    {
        bytes = [];
        failure = null;

        // OLE Object payloads are surfaced as RFC-2397 base64 data URLs by
        // AccessReader.DecodeLongValue (any MIME type, e.g. image/jpeg,
        // image/png, application/octet-stream); round-trip them back to raw bytes.
        if (value.StartsWith("data:", StringComparison.Ordinal))
        {
            if (!BinaryStringParser.TryGetBase64DataUriPayload(value.AsSpan(), out ReadOnlySpan<char> payload))
            {
                failure = "expected a base64 data URI payload";
                return false;
            }

            if (BinaryStringParser.TryDecodeBase64(payload, out bytes))
            {
                return true;
            }

            failure = "invalid Base64 data URI payload";
            return false;
        }

        if (IsLongValueDiagnosticString(value))
        {
            failure = "long-value decoder returned diagnostic text instead of binary data";
            return false;
        }

        if (BinaryStringParser.TryParseHexString(value.AsSpan(), out bytes))
        {
            return true;
        }

        failure = "expected plain hex, dash-separated hex, or a base64 data URI";
        return false;
    }

    private static bool IsLongValueDiagnosticString(string value) =>
        value == "(OLE)" ||
        value == "(memo)" ||
        value == "(memo on LVAL page)" ||
        value.StartsWith("(OLE chain error: ", StringComparison.Ordinal) ||
        value.StartsWith("(memo chain error: ", StringComparison.Ordinal);
}
