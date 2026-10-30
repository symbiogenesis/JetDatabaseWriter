namespace JetDatabaseWriter;

using JetDatabaseWriter.Exceptions;

internal sealed partial class JetFormat
{
    /// <summary>Checks the independently verified escape for leading U+FEFF in compressed variable Text.</summary>
    /// <param name="value">The complete Text value.</param>
    /// <param name="maxBytes">The descriptor's uncompressed byte limit.</param>
    /// <param name="errorInfo">Available table and column context.</param>
    /// <exception cref="JetLimitationException">The value has an unverified character shape or exceeds the declared Text size.</exception>
    internal static void ValidateEscapedVariableText(string value, int maxBytes, JetErrorInfo? errorInfo = null)
    {
        if (value.Length < 6 || value[0] != '\uFEFF')
        {
            throw UnsupportedEscapedVariableText(errorInfo);
        }

        if (value.Length > 255 || (maxBytes > 0 && value.Length > maxBytes / 2))
        {
            throw new JetLimitationException(JetErrorCode.ValueTooLarge, "The leading U+FEFF Text value exceeds the column's declared character limit or the verified 255-character Text limit.", errorInfo);
        }

        int prefixLength = GetEscapedTextWidePrefixLength(value);
        if (value.Length - prefixLength - 1 < (prefixLength > 0 ? 4 : 5))
        {
            throw UnsupportedEscapedVariableText(errorInfo);
        }

        for (int index = prefixLength + 1; index < value.Length; index++)
        {
            if (value[index] is '\0' or > '\u00FF')
            {
                throw UnsupportedEscapedVariableText(errorInfo);
            }
        }
    }

    /// <summary>Encodes a previously verified literal leading marker without truncating the escape or its tail.</summary>
    /// <param name="value">A Text value beginning with U+FEFF.</param>
    /// <param name="maxBytes">The descriptor's uncompressed byte limit.</param>
    /// <returns>The complete native compressed Text payload.</returns>
    internal static byte[] EncodeEscapedVariableText(string value, int maxBytes)
    {
        ValidateEscapedVariableText(value, maxBytes);
        int prefixLength = GetEscapedTextWidePrefixLength(value);
        byte[] bytes = new byte[value.Length + prefixLength + 5];
        bytes[0] = 0xFF;
        bytes[1] = 0xFE;
        bytes[2] = 0;
        bytes[3] = 0xFF;
        bytes[4] = 0xFE;

        // Keep the marker and contiguous wide prefix in native Unicode mode.
        int offset = 5;
        for (int index = 1; index <= prefixLength; index++)
        {
            bytes[offset++] = (byte)(value[index] & 0xFF);
            bytes[offset++] = (byte)(value[index] >> 8);
        }

        bytes[offset++] = 0;
        for (int index = prefixLength + 1; index < value.Length; index++)
        {
            bytes[offset++] = (byte)value[index];
        }

        return bytes;
    }

    private static int GetEscapedTextWidePrefixLength(string value)
    {
        int index = 1;
        while (index < value.Length && value[index] > '\u00FF' && (value[index] & 0xFF) != 0 && !char.IsSurrogate(value[index]))
        {
            index++;
        }

        return index - 1;
    }

    private static JetLimitationException UnsupportedEscapedVariableText(JetErrorInfo? errorInfo)
        => new(JetErrorCode.FeatureNotSupported, "Leading U+FEFF in compressed Text requires a nonzero Latin-1 tail of at least 5 characters, or at least 4 following a contiguous nonsurrogate wide prefix with nonzero low bytes, for verified writing.", errorInfo);
}
