namespace JetDatabaseWriter.DelimitedText;

using System;

internal readonly record struct DelimitedTextFormat
{
    internal DelimitedTextFormat(bool hasHeaderRow, char delimiter, bool trimValues = false)
    {
        ValidateDelimiter(delimiter);
        this.HasHeaderRow = hasHeaderRow;
        this.Delimiter = delimiter;
        this.TrimValues = trimValues;
    }

    internal bool HasHeaderRow { get; }

    internal char Delimiter { get; }

    internal bool TrimValues { get; }

    private static void ValidateDelimiter(char delimiter)
    {
        if (delimiter == '\0' || delimiter == '"' || delimiter == '\r' || delimiter == '\n')
        {
            throw new ArgumentException("Delimited text separator cannot be NUL, quote, carriage return, or line feed.", nameof(delimiter));
        }
    }
}
