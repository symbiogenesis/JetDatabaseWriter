namespace JetDatabaseWriter.Schema.Expressions;

/// <summary>Scans delimited Access source text without interpreting its contents.</summary>
internal static class AccessExpressionText
{
    internal static int SkipQuoted(string text, int index)
    {
        char quote = text[index];
        for (int cursor = index + 1; cursor < text.Length; cursor++)
        {
            if (text[cursor] != quote)
            {
                continue;
            }

            if (cursor + 1 < text.Length && text[cursor + 1] == quote)
            {
                cursor++;
                continue;
            }

            return cursor + 1;
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
}
