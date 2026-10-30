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

        for (int index = 1; index < value.Length; index++)
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
        byte[] bytes = new byte[value.Length + 5];
        bytes[0] = 0xFF;
        bytes[1] = 0xFE;
        bytes[2] = 0;
        bytes[3] = 0xFF;
        bytes[4] = 0xFE;
        bytes[5] = 0;
        for (int index = 1; index < value.Length; index++)
        {
            bytes[index + 5] = (byte)value[index];
        }

        return bytes;
    }

    private static JetLimitationException UnsupportedEscapedVariableText(JetErrorInfo? errorInfo)
        => new(JetErrorCode.FeatureNotSupported, "Leading U+FEFF in compressed Text requires at least 6 characters and a nonzero Latin-1 tail for verified writing.", errorInfo);
}
