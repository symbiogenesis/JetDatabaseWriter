namespace JetDatabaseWriter.Tests.Fuzz;

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.DelimitedText;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Process-isolated harness for the internal delimited text reader. Run as an explicit <c>Category=Fuzz</c> test.
/// </summary>
public sealed class DelimitedTextReaderFuzzTests
{
    private const int MaxGeneratedRows = 64;
    private const int MaxGeneratedColumns = 24;
    private const int MaxGeneratedFieldLength = 48;

    private static readonly char[] Delimiters = [',', ';', '\t', '|'];

    private static readonly DelimitedTextLimits FuzzLimits = new(
        64 * 1024,
        16 * 1024,
        512,
        "MaxRecordLength",
        "MaxFieldLength",
        "MaxColumnCount");

    /// <summary>
    /// Runs one awaited delimited text fuzz input.
    /// </summary>
    [Trait("Category", "Fuzz")]
    [Fact(Explicit = true)]
    public async Task FuzzDelimitedTextReader() => await RunFuzzIterationAsync(await FuzzInput.ReadAsync(TestContext.Current.CancellationToken));

    /// <summary>Malformed raw text may refuse without aborting the generated round-trip control.</summary>
    [Fact]
    public async Task RawInput_UnterminatedQuote_ContinuesGeneratedRoundTrip()
        => await RunFuzzIterationAsync(Encoding.UTF8.GetBytes("\"unterminated"));

    /// <summary>The configured raw-field limit may refuse without aborting the generated control.</summary>
    [Fact]
    public async Task RawInput_OverFieldBudget_ContinuesGeneratedRoundTrip()
        => await RunFuzzIterationAsync(Encoding.UTF8.GetBytes(new string('x', FuzzLimits.MaxFieldLength + 1)));

    /// <summary>
    /// Empty single-column records need explicit field syntax to survive EOF and mixed line endings.
    /// </summary>
    /// <param name="mixedLineEndings">Whether to place an empty row between CR and LF terminators.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RoundTripGeneratedRows_EmptySingleColumnRecords_PreservesRowCount(bool mixedLineEndings)
    {
        // FuzzRandom consumes one big-endian integer per choice: delimiter, row/column counts,
        // raggedness, then field lengths and optional line endings.
        int[] choices = mixedLineEndings ? [0, 2, 0, 1, 0, 2, 0, 0, 1] : [0, 1, 0, 1, 0, 1];
        byte[] input = new byte[choices.Length * sizeof(int)];
        for (int index = 0; index < choices.Length; index++)
        {
            System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(input.AsSpan(index * sizeof(int)), choices[index]);
        }

        await RunFuzzIterationAsync(input);
    }

    /// <summary>
    /// Keeps generated empty records distinct from end-of-input and adjacent record terminators.
    /// </summary>
    /// <param name="prefix">The preceding encoded record, if any.</param>
    /// <param name="lineEnding">The empty record's terminator.</param>
    /// <param name="expectedRowCount">The number of encoded records.</param>
    [Theory]
    [InlineData("", "", 1)]
    [InlineData("", "\r\n", 1)]
    [InlineData("", "\n", 1)]
    [InlineData("", "\r", 1)]
    [InlineData("previous\r", "\n", 2)]
    [InlineData("previous\r", "\r\n", 2)]
    public async Task RoundTripEmptySingleColumnRecord_PreservesRow(string prefix, string lineEnding, int expectedRowCount)
    {
        string source = prefix + EscapeField(string.Empty, ',') + lineEnding;

        List<string[]> rows = await ReadAllAsync(source, ',');

        Assert.Equal(expectedRowCount, rows.Count);
        Assert.Equal([string.Empty], rows[^1]);
    }

    private static async Task RunFuzzIterationAsync(byte[] fuzzedBytes)
    {
        var random = FuzzRandom.Create(fuzzedBytes);
        char delimiter = Delimiters[random.Next(Delimiters.Length)];

        await TryParseRawInputAsync(fuzzedBytes, delimiter);
        await RoundTripGeneratedRowsAsync(random, delimiter);
    }

    private static async Task TryParseRawInputAsync(byte[] fuzzedBytes, char delimiter)
    {
        string source = Encoding.UTF8.GetString(fuzzedBytes);
        try
        {
            _ = await ReadAllAsync(source, delimiter);
        }
        catch (JetCorruptDataException exception) when (exception.ErrorCode == JetErrorCode.CorruptDelimitedText)
        {
        }
        catch (JetLimitationException exception) when (exception.ErrorCode == JetErrorCode.ValueTooLarge
            && exception.ErrorInfo.ObjectName is "MaxRecordLength" or "MaxFieldLength" or "MaxColumnCount")
        {
        }
    }

    private static async Task RoundTripGeneratedRowsAsync(FuzzRandom random, char delimiter)
    {
        string[] lineEndings = ["\r\n", "\n", "\r"];
        int rowCount = random.Next(0, MaxGeneratedRows + 1);
        int fixedColumnCount = random.Next(1, MaxGeneratedColumns + 1);
        bool raggedRows = random.Next(2) == 0;
        var expectedRows = new List<string[]>(rowCount);
        var source = new StringBuilder();

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            int columnCount = raggedRows ? random.Next(1, MaxGeneratedColumns + 1) : fixedColumnCount;
            string[] fields = new string[columnCount];
            for (int columnIndex = 0; columnIndex < fields.Length; columnIndex++)
            {
                fields[columnIndex] = GenerateField(random, delimiter);
                if (columnIndex > 0)
                {
                    source.Append(delimiter);
                }

                source.Append(EscapeField(fields[columnIndex], delimiter));
            }

            expectedRows.Add(fields);
            if (rowIndex < rowCount - 1 || random.Next(2) == 0)
            {
                source.Append(lineEndings[random.Next(lineEndings.Length)]);
            }
        }

        List<string[]> actualRows = await ReadAllAsync(source.ToString(), delimiter);
        if (actualRows.Count != expectedRows.Count)
        {
            throw new InvalidDataException("Delimited text fuzz round-trip returned an unexpected row count.");
        }

        for (int rowIndex = 0; rowIndex < expectedRows.Count; rowIndex++)
        {
            if (!FieldsEqual(expectedRows[rowIndex], actualRows[rowIndex]))
            {
                throw new InvalidDataException("Delimited text fuzz round-trip returned unexpected field values.");
            }
        }
    }

    private static async Task<List<string[]>> ReadAllAsync(string source, char delimiter)
    {
        using var stringReader = new StringReader(source);
        using var reader = new DelimitedTextReader(
            stringReader,
            new DelimitedTextFormat(hasHeaderRow: false, delimiter),
            FuzzLimits,
            bufferLength: 7);

        var records = new List<string[]>();
        while (true)
        {
            DelimitedTextRecord? record = await reader.ReadRecordAsync(TestContext.Current.CancellationToken);
            if (record is not { } current)
            {
                return records;
            }

            records.Add(current.Fields);
        }
    }

    private static bool FieldsEqual(string[] expected, string[] actual)
    {
        if (expected.Length != actual.Length)
        {
            return false;
        }

        for (int i = 0; i < expected.Length; i++)
        {
            if (!string.Equals(expected[i], actual[i], StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    private static string GenerateField(FuzzRandom random, char delimiter)
    {
        int length = random.Next(0, MaxGeneratedFieldLength + 1);
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
        // An unquoted empty record can disappear at EOF or merge adjacent CR/LF terminators.
        if (value.Length > 0 &&
            value.IndexOf(delimiter, StringComparison.Ordinal) < 0 &&
            value.IndexOf('"', StringComparison.Ordinal) < 0 &&
            value.IndexOf('\r', StringComparison.Ordinal) < 0 &&
            value.IndexOf('\n', StringComparison.Ordinal) < 0)
        {
            return value;
        }

        return "\"" + value.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
    }
}
