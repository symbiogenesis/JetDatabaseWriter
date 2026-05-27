namespace JetDatabaseWriter.Tests.DelimitedText;

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.DelimitedText;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

public sealed class DelimitedTextReaderTests
{
    private static readonly DelimitedTextLimits DefaultLimits = new(
        1_048_576,
        1_048_576,
        255,
        "MaxRecordLength",
        "MaxFieldLength",
        "MaxColumnCount");

    public static TheoryData<string, string> QuotedFieldCases => new()
    {
        { "\"\"", string.Empty },
        { "\"\"\"\"", "\"" },
        { "\"\"\"\"\"\"", "\"\"" },
        { "\"a\"", "a" },
        { "\"a\"\"a\"", "a\"a" },
        { "\"a\"\"a\"\"a\"", "a\"a\"a" },
        { "a\"\"a", "a\"\"a" },
        { "a\"a\"a", "a\"a\"a" },
        { " \"\" ", " \"\" " },
        { " \"a\" ", " \"a\" " },
        { "\"a\"a\"a\"", "aa\"a\"" },
        { "\"a\"a\"", "aa\"" },
    };

    public static TheoryData<string> UnterminatedQuotedFieldCases => new()
    {
        { "\"" },
        { "\"a" },
        { "\"a\n" },
        { "\"a\r\nb" },
        { "\"\"\"" },
        { "\"\"\"\"\"" },
    };

    public static TheoryData<byte[], int, int, int, bool, char> FuzzSeedRoundTripCases => new()
    {
        { CreateFuzzSeed(23768213), 1, 1, 0, false, ',' },
        { CreateFuzzSeed(71832791), 48, 6, 8, false, ',' },
        { CreateFuzzSeed(91827364), 150, 18, 24, true, ',' },
        { CreateFuzzSeed(11035152), 96, 12, 32, true, ';' },
        { CreateFuzzSeed(24681357), 72, 10, 16, true, '\t' },
    };

    [Fact]
    public async Task ReadRecordAsync_EmptyText_ReturnsNull()
    {
        using var stringReader = new StringReader(string.Empty);
        using var reader = CreateReader(stringReader);

        var record = await reader.ReadRecordAsync(TestContext.Current.CancellationToken);

        Assert.Null(record);
    }

    [Fact]
    public async Task ReadRecordAsync_LineEndings_ReturnsEmptySingleColumnRows()
    {
        var records = await ReadAllAsync("\r\n\n\r");

        Assert.Collection(
            records,
            record => AssertRecord(record, 0, 1, 2, string.Empty),
            record => AssertRecord(record, 1, 2, 3, string.Empty),
            record => AssertRecord(record, 2, 3, 4, string.Empty));
    }

    [Fact]
    public async Task ReadRecordAsync_QuotedFields_SpanLinesAndUnescape()
    {
        var records = await ReadAllAsync(
            "A,B,C\r\n1,\"two\r\nlines\",\"He said \"\"hi\"\"\"\n");

        Assert.Collection(
            records,
            record => AssertRecord(record, 0, 1, 2, "A", "B", "C"),
            record => AssertRecord(record, 1, 2, 4, "1", "two\r\nlines", "He said \"hi\""));
    }

    [Theory]
    [MemberData(nameof(QuotedFieldCases))]
    public async Task ReadRecordAsync_QuotedFieldCases_UnescapeConsistently(string source, string expected)
    {
        var record = Assert.Single(await ReadAllAsync(source));

        Assert.Equal([expected], record.Fields);
    }

    [Fact]
    public async Task ReadRecordAsync_DoesNotDependOnTextReaderPeek()
    {
        using var stringReader = new NoPeekStringReader("A,B\r\n1,\"x\r\ny\"\r\n");
        using var reader = CreateReader(stringReader);

        var header = await ReadRecordSnapshotAsync(reader);
        var row = await ReadRecordSnapshotAsync(reader);
        var end = await ReadRecordSnapshotAsync(reader);

        Assert.NotNull(header);
        Assert.NotNull(row);
        Assert.Null(end);
        AssertRecord(header, 0, 1, 2, "A", "B");
        AssertRecord(row, 1, 2, 4, "1", "x\r\ny");
    }

    [Fact]
    public async Task ReadRecordAsync_ReusedParserBuffers_DoNotMutateReturnedRecords()
    {
        using var stringReader = new StringReader("A,B\n1,2\nlong-value,\"quoted\"\n");
        using var reader = CreateReader(stringReader);

        var header = await ReadRecordSnapshotAsync(reader);
        var firstRow = await ReadRecordSnapshotAsync(reader);
        var secondRow = await ReadRecordSnapshotAsync(reader);

        Assert.NotNull(header);
        Assert.NotNull(firstRow);
        Assert.NotNull(secondRow);
        AssertRecord(header, 0, 1, 2, "A", "B");
        AssertRecord(firstRow, 1, 2, 3, "1", "2");
        AssertRecord(secondRow, 2, 3, 4, "long-value", "quoted");
    }

    [Fact]
    public async Task ReadRecordAsync_UnquotedRunsAcrossBufferBoundaries_ParseCorrectly()
    {
        using var stringReader = new StringReader("abcdefg\"literal\",tail\nnext,row\n");
        using var reader = CreateReader(stringReader);

        var firstRow = await ReadRecordSnapshotAsync(reader);
        var secondRow = await ReadRecordSnapshotAsync(reader);

        Assert.NotNull(firstRow);
        Assert.NotNull(secondRow);
        AssertRecord(firstRow, 0, 1, 2, "abcdefg\"literal\"", "tail");
        AssertRecord(secondRow, 1, 2, 3, "next", "row");
    }

    [Fact]
    public async Task ReadRecordAsync_SeparatorsOnly_ReturnsEmptyColumns()
    {
        var record = Assert.Single(await ReadAllAsync(",,,\n"));

        AssertRecord(record, 0, 1, 2, string.Empty, string.Empty, string.Empty, string.Empty);
    }

    [Fact]
    public async Task ReadRecordAsync_CustomSeparator_KeepsSeparatorInsideQuotes()
    {
        var records = await ReadAllAsync(
            "C1;C2;C3\n10;\"A;\";\"20\"\";\"\"11\"\n",
            delimiter: ';');

        Assert.Collection(
            records,
            record => AssertRecord(record, 0, 1, 2, "C1", "C2", "C3"),
            record => AssertRecord(record, 1, 2, 3, "10", "A;", "20\";\"11"));
    }

    [Fact]
    public async Task ReadRecordAsync_MissingClosingQuote_ThrowsInvalidData()
    {
        using var stringReader = new StringReader("A,B\n1,\"unterminated\n");
        using var reader = CreateReader(stringReader);
        _ = await reader.ReadRecordAsync(TestContext.Current.CancellationToken);

        var exception = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.ReadRecordAsync(TestContext.Current.CancellationToken));
        Assert.Contains("closing quote", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [MemberData(nameof(UnterminatedQuotedFieldCases))]
    public async Task ReadRecordAsync_UnterminatedQuotedFieldCases_ThrowInvalidData(string source)
    {
        using var stringReader = new StringReader(source);
        using var reader = CreateReader(stringReader);

        var exception = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.ReadRecordAsync(TestContext.Current.CancellationToken));
        Assert.Contains("closing quote", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ReadRecordAsync_FieldLengthLimit_ThrowsInvalidData()
    {
        var limits = new DelimitedTextLimits(128, 3, 8, "MaxRecordLength", "MaxFieldLength", "MaxColumnCount");
        using var stringReader = new StringReader("abcd\n");
        using var reader = CreateReader(stringReader, limits: limits);

        var exception = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.ReadRecordAsync(TestContext.Current.CancellationToken));
        Assert.Contains("MaxFieldLength", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReadRecordAsync_RecordLengthLimit_ThrowsInvalidData()
    {
        var limits = new DelimitedTextLimits(4, 128, 8, "MaxRecordLength", "MaxFieldLength", "MaxColumnCount");
        using var stringReader = new StringReader("ab,cd\n");
        using var reader = CreateReader(stringReader, limits: limits);

        var exception = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.ReadRecordAsync(TestContext.Current.CancellationToken));
        Assert.Contains("MaxRecordLength", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReadRecordAsync_ColumnCountLimit_ThrowsInvalidData()
    {
        var limits = new DelimitedTextLimits(128, 128, 2, "MaxRecordLength", "MaxFieldLength", "MaxColumnCount");
        using var stringReader = new StringReader("a,b,c\n");
        using var reader = CreateReader(stringReader, limits: limits);

        var exception = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.ReadRecordAsync(TestContext.Current.CancellationToken));
        Assert.Contains("MaxColumnCount", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CountRecordsAsync_EmptyText_ReturnsZero()
    {
        using var stringReader = new StringReader(string.Empty);
        using var reader = CreateReader(stringReader);

        long count = await reader.CountRecordsAsync(skipFirstRecord: false, TestContext.Current.CancellationToken);

        Assert.Equal(0, count);
    }

    [Fact]
    public async Task CountRecordsAsync_SkipsHeaderAndHonorsQuotedLineEndings()
    {
        using var stringReader = new StringReader("A,B\r\n1,\"two\r\nlines\"\n2,\"cr\ronly\"\r3,plain");
        using var reader = CreateReader(stringReader);

        long count = await reader.CountRecordsAsync(skipFirstRecord: true, TestContext.Current.CancellationToken);

        Assert.Equal(3, count);
    }

    [Fact]
    public async Task CountRecordsAsync_MissingClosingQuote_ThrowsInvalidData()
    {
        using var stringReader = new StringReader("A,B\n1,\"unterminated\n");
        using var reader = CreateReader(stringReader);

        var exception = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.CountRecordsAsync(skipFirstRecord: true, TestContext.Current.CancellationToken));
        Assert.Contains("closing quote", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task CountRecordsAsync_RecordLengthLimit_ThrowsInvalidData()
    {
        var limits = new DelimitedTextLimits(4, 128, 8, "MaxRecordLength", "MaxFieldLength", "MaxColumnCount");
        using var stringReader = new StringReader("ab,cd\n");
        using var reader = CreateReader(stringReader, limits: limits);

        var exception = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.CountRecordsAsync(skipFirstRecord: false, TestContext.Current.CancellationToken));
        Assert.Contains("MaxRecordLength", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CountRecordsAsync_FieldLengthLimit_ThrowsInvalidData()
    {
        var limits = new DelimitedTextLimits(128, 3, 8, "MaxRecordLength", "MaxFieldLength", "MaxColumnCount");
        using var stringReader = new StringReader("abcd\n");
        using var reader = CreateReader(stringReader, limits: limits);

        var exception = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.CountRecordsAsync(skipFirstRecord: false, TestContext.Current.CancellationToken));
        Assert.Contains("MaxFieldLength", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CountRecordsAsync_ColumnCountLimit_ThrowsInvalidData()
    {
        var limits = new DelimitedTextLimits(128, 128, 2, "MaxRecordLength", "MaxFieldLength", "MaxColumnCount");
        using var stringReader = new StringReader("a,b,c\n");
        using var reader = CreateReader(stringReader, limits: limits);

        var exception = await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.CountRecordsAsync(skipFirstRecord: false, TestContext.Current.CancellationToken));
        Assert.Contains("MaxColumnCount", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Normalize_DuplicateAndBlankHeaders_ProducesStableColumnNames()
    {
        string[] columnNames = DelimitedTextColumnNames.Normalize(["A", "a", " ", "F3", "A"]);

        Assert.Equal(["A", "a2", "F3", "F32", "A3"], columnNames);
    }

    [Theory]
    [MemberData(nameof(FuzzSeedRoundTripCases))]
    public async Task ReadRecordAsync_FuzzSeedCorpus_RoundTripsEscapedRows(
        byte[] fuzzSeed,
        int rowCount,
        int maxColumnCount,
        int maxFieldLength,
        bool raggedRows,
        char delimiter)
    {
        string[] lineEndings = ["\r\n", "\n", "\r"];
        FuzzRandom random = FuzzRandom.Create(fuzzSeed);
        var expectedRows = new List<string[]>();
        var text = new StringBuilder();
        int fixedColumnCount = random.Next(1, maxColumnCount + 1);
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            int columnCount = raggedRows ? random.Next(1, maxColumnCount + 1) : fixedColumnCount;
            var fields = new string[columnCount];
            for (int columnIndex = 0; columnIndex < fields.Length; columnIndex++)
            {
                fields[columnIndex] = GenerateRandomField(random, maxFieldLength, delimiter);
                if (columnIndex > 0)
                {
                    text.Append(delimiter);
                }

                text.Append(EscapeField(fields[columnIndex], delimiter));
            }

            expectedRows.Add(fields);
            if (rowIndex < rowCount - 1 || random.Next(2) == 0)
            {
                text.Append(lineEndings[rowIndex % lineEndings.Length]);
            }
        }

        var actualRows = await ReadAllAsync(text.ToString(), delimiter);

        Assert.Equal(expectedRows.Count, actualRows.Count);
        for (int i = 0; i < expectedRows.Count; i++)
        {
            Assert.Equal(expectedRows[i], actualRows[i].Fields);
        }
    }

    private static DelimitedTextReader CreateReader(
        TextReader reader,
        char delimiter = ',',
        DelimitedTextLimits? limits = null)
    {
        return new DelimitedTextReader(
            reader,
            new DelimitedTextFormat(hasHeaderRow: false, delimiter),
            limits ?? DefaultLimits,
            bufferLength: 7);
    }

    private static async ValueTask<List<ParsedDelimitedTextRecord>> ReadAllAsync(string text, char delimiter = ',')
    {
        using var stringReader = new StringReader(text);
        using var reader = CreateReader(stringReader, delimiter);
        var records = new List<ParsedDelimitedTextRecord>();
        while (true)
        {
            var record = await ReadRecordSnapshotAsync(reader);
            if (!record.HasValue)
            {
                return records;
            }

            records.Add(record.Value);
        }
    }

    private static async ValueTask<ParsedDelimitedTextRecord?> ReadRecordSnapshotAsync(DelimitedTextReader reader)
    {
        var record = await reader.ReadRecordAsync(TestContext.Current.CancellationToken);
        return record.HasValue ? Snapshot(record.Value) : null;
    }

    private static ParsedDelimitedTextRecord Snapshot(DelimitedTextRecord record)
        => new(record.Fields, record.RowIndex, record.LineNumberFrom, record.LineNumberToExclusive);

    private static void AssertRecord(
        ParsedDelimitedTextRecord? record,
        int rowIndex,
        int lineNumberFrom,
        int lineNumberToExclusive,
        params string[] fields)
    {
        Assert.NotNull(record);
        AssertRecord(record.Value, rowIndex, lineNumberFrom, lineNumberToExclusive, fields);
    }

    private static void AssertRecord(
        ParsedDelimitedTextRecord record,
        int rowIndex,
        int lineNumberFrom,
        int lineNumberToExclusive,
        params string[] fields)
    {
        Assert.Equal(rowIndex, record.RowIndex);
        Assert.Equal(lineNumberFrom, record.LineNumberFrom);
        Assert.Equal(lineNumberToExclusive, record.LineNumberToExclusive);
        Assert.Equal(fields, record.Fields);
    }

    private readonly record struct ParsedDelimitedTextRecord(
        string[] Fields,
        int RowIndex,
        int LineNumberFrom,
        int LineNumberToExclusive);

    private static byte[] CreateFuzzSeed(int seed)
    {
        byte[] bytes = new byte[64];
        unchecked
        {
            uint state = (uint)seed;
            for (int i = 0; i < bytes.Length; i++)
            {
                state = (state * 1664525u) + 1013904223u;
                bytes[i] = (byte)(state >> 24);
            }
        }

        return bytes;
    }

    private static string GenerateRandomField(FuzzRandom random, int maxLength, char delimiter)
    {
        int length = random.Next(0, maxLength + 1);
        var builder = new StringBuilder(length);
        for (int i = 0; i < length; i++)
        {
            char ch = random.Next(18) switch
            {
                0 => delimiter,
                1 => '"',
                2 => '\r',
                3 => '\n',
                4 => ' ',
                5 => '\t',
                6 => ';',
                7 => '|',
                8 => '\u00A0',
                9 => '\u00E9',
                10 => '\u03A9',
                11 => '\u4E2D',
                12 => '\u2028',
                13 => '\u2029',
                14 => (char)random.Next('0', '9' + 1),
                15 => (char)random.Next('A', 'Z' + 1),
                _ => (char)('a' + random.Next(26)),
            };
            builder.Append(ch);
        }

        return builder.ToString();
    }

    private static string EscapeField(string value, char delimiter)
    {
        if (value.IndexOf(delimiter, StringComparison.Ordinal) < 0 &&
            value.IndexOf('"', StringComparison.Ordinal) < 0 &&
            value.IndexOf('\r', StringComparison.Ordinal) < 0 &&
            value.IndexOf('\n', StringComparison.Ordinal) < 0)
        {
            return value;
        }

        return "\"" + value.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
    }

    private sealed class NoPeekStringReader(string value) : StringReader(value)
    {
        public override int Peek() => -1;
    }
}
