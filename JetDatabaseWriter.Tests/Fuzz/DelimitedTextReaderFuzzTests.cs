namespace JetDatabaseWriter.Tests.Fuzz;

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using JetDatabaseWriter.DelimitedText;
using JetDatabaseWriter.Tests.Infrastructure;
using SharpFuzz;
using Xunit;

/// <summary>
/// SharpFuzz harness for the internal delimited text reader. Run as an explicit <c>Category=Fuzz</c> test.
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
    /// Runs the SharpFuzz delimited text harness.
    /// </summary>
    [Trait("Category", "Fuzz")]
    [Fact(Explicit = true)]
    public void FuzzDelimitedTextReader()
    {
        Fuzzer.Run(stream =>
        {
            byte[] fuzzedBytes = ReadAllBytes(stream);
            RunFuzzIteration(fuzzedBytes);
        });
    }

    private static void RunFuzzIteration(byte[] fuzzedBytes)
    {
        FuzzRandom random = FuzzRandom.Create(fuzzedBytes);
        char delimiter = Delimiters[random.Next(Delimiters.Length)];

        TryParseRawInput(fuzzedBytes, delimiter);
        RoundTripGeneratedRows(random, delimiter);
    }

    private static byte[] ReadAllBytes(Stream stream)
    {
        using var memory = new MemoryStream();
        stream.CopyTo(memory);
        return memory.ToArray();
    }

    private static void TryParseRawInput(byte[] fuzzedBytes, char delimiter)
    {
        string source = Encoding.UTF8.GetString(fuzzedBytes);
        try
        {
            _ = ReadAll(source, delimiter);
        }
        catch (InvalidDataException)
        {
        }
    }

    private static void RoundTripGeneratedRows(FuzzRandom random, char delimiter)
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
            var fields = new string[columnCount];
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

        var actualRows = ReadAll(source.ToString(), delimiter);
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

    private static List<string[]> ReadAll(string source, char delimiter)
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
            var record = reader.ReadRecordAsync(default).AsTask().GetAwaiter().GetResult();
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
        if (value.IndexOf(delimiter, StringComparison.Ordinal) < 0 &&
            value.IndexOf('"', StringComparison.Ordinal) < 0 &&
            value.IndexOf('\r', StringComparison.Ordinal) < 0 &&
            value.IndexOf('\n', StringComparison.Ordinal) < 0)
        {
            return value;
        }

        return "\"" + value.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
    }
}
