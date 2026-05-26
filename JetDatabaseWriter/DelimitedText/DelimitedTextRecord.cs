namespace JetDatabaseWriter.DelimitedText;

internal readonly record struct DelimitedTextRecord
{
    private readonly DelimitedTextReader? _reader;
    private readonly int _version;

    internal DelimitedTextRecord(
        DelimitedTextReader reader,
        int fieldCount,
        int rowIndex,
        int lineNumberFrom,
        int lineNumberToExclusive,
        int version)
    {
        _reader = reader;
        FieldCount = fieldCount;
        RowIndex = rowIndex;
        LineNumberFrom = lineNumberFrom;
        LineNumberToExclusive = lineNumberToExclusive;
        _version = version;
    }

    internal string[] Fields => _reader?.MaterializeFields(FieldCount, _version) ?? [];

    internal int FieldCount { get; }

    internal int RowIndex { get; }

    internal int LineNumberFrom { get; }

    internal int LineNumberToExclusive { get; }
}

internal readonly record struct DelimitedTextField(string? Value, int BufferStart, int BufferLength)
{
    internal bool IsBuffered => Value is null;

    internal static DelimitedTextField FromBuffer(int bufferStart, int bufferLength) => new(null, bufferStart, bufferLength);

    internal static DelimitedTextField FromString(string value) => new(value, -1, 0);
}
