namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using System.Globalization;

/// <summary>Preserves native DateSerial's special unparenthesized year-literal grammar.</summary>
internal static class CalculatedDateSerialAdmission
{
    /// <summary>Checks the first argument only when it is a bare numeric literal or its negative.</summary>
    /// <param name="tokens">The expression tokens.</param>
    /// <param name="start">The first argument's first token.</param>
    /// <param name="end">The exclusive end of its tokens.</param>
    /// <exception cref="ArgumentException">The source literal has no native four-digit year coefficient.</exception>
    internal static void ValidateYear(List<CalculatedExpressionParser.Token> tokens, int start, int end)
    {
        if (start < end && tokens[start] is { Kind: CalculatedExpressionParser.TokenKind.Operator, Text: "-" })
        {
            start++;
        }

        if (end - start != 1 || tokens[start] is not { Kind: CalculatedExpressionParser.TokenKind.Value } token
            || token.Text.Length == 0 || !(token.Text[0] is (>= '0' and <= '9') or '.'))
        {
            return;
        }

        if (!HasNativeYearCoefficient(token.Text))
        {
            throw new ArgumentException($"DateSerial year literal '{token.Text}' does not have a native supported four-digit coefficient. A parenthesized year expression uses the ordinary runtime conversion.");
        }
    }

    private static bool HasNativeYearCoefficient(string text)
    {
        if (!double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double value) || value <= 0d || value >= 10000d)
        {
            return false;
        }

        int exponentAt = text.IndexOfAny(['e', 'E']);
        int exponent = 0;
        if (exponentAt >= 0 && !int.TryParse(text.AsSpan(exponentAt + 1), NumberStyles.Integer, CultureInfo.InvariantCulture, out exponent))
        {
            return false;
        }

        string mantissa = exponentAt < 0 ? text : text[..exponentAt];
        int point = mantissa.IndexOf('.', StringComparison.Ordinal);
        int scale = point < 0 ? 0 : mantissa.Length - point - 1;
        string coefficient = mantissa.Replace(".", string.Empty, StringComparison.Ordinal).TrimStart('0');
        if (point >= 0)
        {
            // Native literal admission strips all coefficient zeros here,
            // including integer-place zeros once the fractional scale is gone.
            // Thus 2020.0 has coefficient202, while 999.50 has coefficient9995.
            string trimmed = coefficient.TrimEnd('0');
            scale = Math.Max(0, scale - (coefficient.Length - trimmed.Length));
            coefficient = trimmed;
        }

        long remainingScale = (long)scale - exponent;
        int end = coefficient.Length;
        while (remainingScale > 0 && end > 0 && coefficient[end - 1] == '0')
        {
            end--;
            remainingScale--;
        }

        long digits = end + Math.Max(0, -remainingScale);
        return digits == 4;
    }
}
