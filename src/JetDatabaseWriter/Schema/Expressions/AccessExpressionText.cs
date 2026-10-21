namespace JetDatabaseWriter.Schema.Expressions;

/// <summary>Scans Access source text without interpreting its values or reference scope.</summary>
internal static class AccessExpressionText
{
    internal static int SkipQuoted(string text, int index)
    {
        char quote = text[index];
        while ((index = text.IndexOf(quote, index + 1)) >= 0)
        {
            if (index + 1 == text.Length || text[index + 1] != quote)
            {
                return index + 1;
            }

            index++;
        }

        return -1;
    }

    internal static int SkipWhiteSpace(string text, int index)
    {
        while (index < text.Length && char.IsWhiteSpace(text[index]))
        {
            index++;
        }

        return index;
    }

    internal static bool IsNameStart(char c) => char.IsLetter(c) || c == '_';

    internal static bool IsNamePart(char c) => char.IsLetterOrDigit(c) || c == '_';

    internal static int SkipNameSegment(string text, int index)
    {
        if (text[index] == '[')
        {
            int close = text.IndexOf(']', index + 1);
            return close < 0 ? -1 : close + 1;
        }

        while (index < text.Length && IsNamePart(text[index]))
        {
            index++;
        }

        return index;
    }

    internal static int SkipNumber(string text, int index)
    {
        // Numeric text may be malformed; evaluation validates it, while renames preserve it.
        while (index < text.Length && (char.IsDigit(text[index]) || text[index] == '.'))
        {
            index++;
        }

        if (index < text.Length && text[index] is 'E' or 'e')
        {
            int exponent = index + 1;
            if (exponent < text.Length && text[exponent] is '+' or '-')
            {
                exponent++;
            }

            // An incomplete exponent leaves its sign visible as an operator.
            if (exponent < text.Length && char.IsDigit(text[exponent]))
            {
                index = exponent;
                while (index < text.Length && char.IsDigit(text[index]))
                {
                    index++;
                }
            }
        }

        return index;
    }

    internal static int ReadRadixPrefix(string text, ref int index)
    {
        if (index + 1 >= text.Length)
        {
            return 0;
        }

        char marker = text[index + 1];
        int radix = marker switch
        {
            'H' or 'h' => 16,
            'O' or 'o' or (>= '0' and <= '7') => 8,
            _ => 0,
        };
        if (radix != 0)
        {
            index += marker is >= '0' and <= '7' ? 1 : 2;
        }

        return radix;
    }

    internal static bool TryGetDigit(char ch, int radix, out int digit)
    {
        digit = ch switch
        {
            >= '0' and <= '9' => ch - '0',
            >= 'A' and <= 'F' => ch - 'A' + 10,
            >= 'a' and <= 'f' => ch - 'a' + 10,
            _ => radix,
        };
        return digit < radix;
    }
}
