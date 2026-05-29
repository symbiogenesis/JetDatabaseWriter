namespace JetDatabaseWriter.DelimitedText;

internal readonly record struct DelimitedTextRecord
{
    private readonly DelimitedTextReader? reader;
    private readonly int version;

    internal DelimitedTextRecord(
        DelimitedTextReader reader,
        int fieldCount,
        int rowIndex,
        int lineNumberFrom,
        int lineNumberToExclusive,
        int version)
    {
        this.reader = reader;
        this.FieldCount = fieldCount;
        this.RowIndex = rowIndex;
        this.LineNumberFrom = lineNumberFrom;
        this.LineNumberToExclusive = lineNumberToExclusive;
        this.version = version;
    }

    internal string[] Fields => this.reader?.MaterializeFields(this.FieldCount, this.version) ?? [];

    internal int FieldCount { get; }

    internal int RowIndex { get; }

    internal int LineNumberFrom { get; }

    internal int LineNumberToExclusive { get; }
}

internal readonly record struct DelimitedTextField(string? Value, int BufferStart, int BufferLength)
{
    internal bool IsBuffered => this.Value is null;

    internal static DelimitedTextField FromBuffer(int bufferStart, int bufferLength) => new(null, bufferStart, bufferLength);

    internal static DelimitedTextField FromString(string value) => new(value, -1, 0);
}
