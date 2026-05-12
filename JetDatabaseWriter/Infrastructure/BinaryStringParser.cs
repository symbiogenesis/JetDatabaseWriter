namespace JetDatabaseWriter.Infrastructure;

using System;

internal static class BinaryStringParser
{
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

        var buffer = new byte[decodedLength];
        if (!Convert.TryFromBase64Chars(value, buffer, out int bytesWritten) || bytesWritten != decodedLength)
        {
            return false;
        }

        bytes = buffer;
        return true;
    }

    public static bool TryParseDashSeparatedHex(ReadOnlySpan<char> value, out byte[] bytes)
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

        var buffer = new byte[(value.Length + 1) / 3];
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
            if (sourceIndex == value.Length)
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
        if (value >= '0' && value <= '9')
        {
            return value - '0';
        }

        if (value >= 'A' && value <= 'F')
        {
            return value - 'A' + 10;
        }

        if (value >= 'a' && value <= 'f')
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
