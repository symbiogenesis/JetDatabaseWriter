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
                _ when targetType == typeof(Hyperlink) => (object?)Hyperlink.Parse(value) ?? DBNull.Value,
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

    private static byte[] ParseByteArray(string hexString)
    {
        // Format: "XX-XX-XX-XX" from BitConverter.ToString
        if (string.IsNullOrEmpty(hexString))
        {
            return [];
        }

        // OLE Object payloads are surfaced as RFC-2397 base64 data URLs by
        // AccessReader.DecodeLongValue (any MIME type, e.g. image/jpeg,
        // image/png, application/octet-stream); round-trip them back to raw bytes.
        if (hexString.StartsWith("data:", StringComparison.Ordinal))
        {
            int comma = hexString.IndexOf(',', StringComparison.Ordinal);
            if (comma > 0 && hexString.AsSpan(0, comma).IndexOf(";base64".AsSpan(), StringComparison.Ordinal) >= 0)
            {
                if (BinaryStringParser.TryDecodeBase64(hexString.AsSpan(comma + 1), out byte[] bytes))
                {
                    return bytes;
                }

                throw new FormatException("Invalid Base64 data URI payload.");
            }
        }

        // Try to use Convert.FromHexString if input is a plain hex string (no dashes)
#if NET5_0_OR_GREATER
        if (hexString.AsSpan().IndexOf('-') < 0)
        {
            try
            {
                return Convert.FromHexString(hexString.AsSpan());
            }
            catch (FormatException)
            {
                // OLE / memo decoders surface diagnostic strings like
                // "(OLE chain error: ...)" or "(memo on LVAL page)" when the
                // long-value chain cannot be walked. These are not real binary
                // payloads; surface them as empty byte arrays so callers can
                // distinguish "no data decoded" from a parse failure.
                return [];
            }
        }
#endif

        // Fallback: dash-separated format ("XX-XX-XX-XX")
        return BinaryStringParser.TryParseDashSeparatedHex(hexString.AsSpan(), out byte[] dashSeparatedBytes) ? dashSeparatedBytes : [];
    }
}
