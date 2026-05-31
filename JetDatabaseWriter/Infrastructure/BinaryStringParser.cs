namespace JetDatabaseWriter.Infrastructure;

using System;

internal static class BinaryStringParser
{
    public static bool TryDecodeBase64DataUri(string value, out byte[] bytes) =>
        TryDecodeBase64DataUri(value, requiredMediaType: null, out bytes);

    public static bool TryDecodeBase64DataUri(string value, string? requiredMediaType, out byte[] bytes)
    {
        bytes = [];
        if (string.IsNullOrEmpty(value)
            || !TryGetBase64DataUriPayload(value.AsSpan(), requiredMediaType, out ReadOnlySpan<char> payload))
        {
            return false;
        }

        return TryDecodeBase64(payload, out bytes);
    }

    public static bool TryGetBase64DataUriPayload(ReadOnlySpan<char> value, out ReadOnlySpan<char> payload) =>
        TryGetBase64DataUriPayload(value, requiredMediaType: null, out payload);

    public static bool TryGetBase64DataUriPayload(
        ReadOnlySpan<char> value,
        string? requiredMediaType,
        out ReadOnlySpan<char> payload)
    {
        payload = default;
        const string prefix = "data:";
        if (!value.StartsWith(prefix.AsSpan(), StringComparison.Ordinal))
        {
            return false;
        }

        int comma = value.IndexOf(',');
        if (comma < 0)
        {
            return false;
        }

        ReadOnlySpan<char> metadata = value[prefix.Length..comma];
        if (metadata.IndexOf(";base64".AsSpan(), StringComparison.Ordinal) < 0)
        {
            return false;
        }

        if (requiredMediaType != null)
        {
            int metadataSeparator = metadata.IndexOf(';');
            ReadOnlySpan<char> mediaType = metadataSeparator < 0 ? metadata : metadata[..metadataSeparator];
            if (!mediaType.Equals(requiredMediaType.AsSpan(), StringComparison.Ordinal))
            {
                return false;
            }
        }

        payload = value[(comma + 1)..];
        return true;
    }

    public static bool TryDecodeBase64(ReadOnlySpan<char> value, out byte[] bytes)
    {
        bytes = [];

        if (!TryGetBase64DecodedLength(value, out int decodedLength))
        {
            return false;
        }

        if (decodedLength == 0)
        {
            return true;
        }

        byte[] buffer = new byte[decodedLength];
        if (!Convert.TryFromBase64Chars(value, buffer, out int bytesWritten) || bytesWritten != decodedLength)
        {
            return false;
        }

        bytes = buffer;
        return true;
    }

    public static bool TryParseHexString(ReadOnlySpan<char> value, out byte[] bytes)
    {
        bytes = [];

        if (value.IsEmpty)
        {
            return true;
        }

        if (value.IndexOf('-') >= 0)
        {
            return TryParseDashSeparatedHex(value, out bytes);
        }

        if ((value.Length & 1) != 0)
        {
            return false;
        }

#if NET5_0_OR_GREATER
        try
        {
            bytes = Convert.FromHexString(value);
            return true;
        }
        catch (FormatException)
        {
            bytes = [];
            return false;
        }
#else
        return TryParseHexPairs(value, value.Length / 2, separator: '\0', out bytes);
#endif
    }

    private static bool TryParseDashSeparatedHex(ReadOnlySpan<char> value, out byte[] bytes)
    {
        bytes = [];

        if (value.IsEmpty)
        {
            return true;
        }

        if (value.Length % 3 != 2)
        {
            return false;
        }

        return TryParseHexPairs(value, (value.Length + 1) / 3, separator: '-', out bytes);
    }

    private static bool TryParseHexPairs(ReadOnlySpan<char> value, int byteCount, char separator, out byte[] bytes)
    {
        bytes = [];

        byte[] buffer = new byte[byteCount];
        int sourceIndex = 0;
        for (int i = 0; i < buffer.Length; i++)
        {
            int high = HexToNibble(value[sourceIndex]);
            int low = HexToNibble(value[sourceIndex + 1]);
            if (high < 0 || low < 0)
            {
                return false;
            }

            buffer[i] = (byte)((high << 4) | low);
            sourceIndex += 2;
            if (separator == '\0' || sourceIndex == value.Length)
            {
                continue;
            }

            if (value[sourceIndex] != '-')
            {
                return false;
            }

            sourceIndex++;
        }

        bytes = buffer;
        return true;
    }

    private static int HexToNibble(char value)
    {
        if (value is >= '0' and <= '9')
        {
            return value - '0';
        }

        if (value is >= 'A' and <= 'F')
        {
            return value - 'A' + 10;
        }

        if (value is >= 'a' and <= 'f')
        {
            return value - 'a' + 10;
        }

        return -1;
    }

    private static bool TryGetBase64DecodedLength(ReadOnlySpan<char> value, out int decodedLength)
    {
        decodedLength = 0;
        int charCount = 0;
        foreach (char c in value)
        {
            if (!char.IsWhiteSpace(c))
            {
                charCount++;
            }
        }

        if (charCount == 0)
        {
            return true;
        }

        if (charCount % 4 != 0)
        {
            return false;
        }

        int paddingCount = 0;
        for (int i = value.Length - 1; i >= 0; i--)
        {
            char c = value[i];
            if (char.IsWhiteSpace(c))
            {
                continue;
            }

            if (c == '=')
            {
                paddingCount++;
                continue;
            }

            break;
        }

        if (paddingCount > 2)
        {
            return false;
        }

        decodedLength = (charCount / 4 * 3) - paddingCount;
        return decodedLength >= 0;
    }
}
