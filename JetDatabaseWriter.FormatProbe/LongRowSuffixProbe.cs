// One-shot research probes for the unresolved V2010 long-row 2-byte suffix.
//
// Usage:
//   dotnet run --project JetDatabaseWriter.FormatProbe -- long-row-suffix
//   dotnet run --project JetDatabaseWriter.FormatProbe -- long-row-crc-sweep

namespace JetDatabaseWriter.FormatProbe;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.ValueDecoding;

internal static class LongRowSuffixProbe
{
    private const string GeneralResource = "JetDatabaseWriter.IndexCodeTables.index_codes_gen.txt.gz";
    private const string GeneralExtResource = "JetDatabaseWriter.IndexCodeTables.index_codes_ext_gen.txt.gz";
    private const char FirstChar = (char)0x0000;
    private const char LastChar = (char)0x00FF;
    private const char FirstExtChar = (char)0x0100;
    private const char LastExtChar = (char)0xFFFF;

    private static readonly Lazy<GeneralLegacyTextIndexEncoder.CharHandler[]> GeneralCodes = new(
        () => GeneralLegacyTextIndexEncoder.LoadCodes(GeneralResource, FirstChar, LastChar));

    private static readonly Lazy<GeneralLegacyTextIndexEncoder.CharHandler[]> GeneralExtCodes = new(
        () => GeneralLegacyTextIndexEncoder.LoadCodes(GeneralExtResource, FirstExtChar, LastExtChar));

    private static readonly string[] InputCandidateNames =
    [
        "full[508..]",
        "full[510..]",
        "full[508..^1]",
        "text[255..] CP1252",
        "text[255..] UTF16LE",
        "text UTF16LE",
        "text[255..] upper CP1252",
        "text upper CP1252",
        "extras only",
        "unprint only",
        "extras+unprint",
        "full[..508]",
        "full[1..508]",
        "full[..510] suffix zeroed",
    ];

    public static async Task<int> RunAnalysisAsync(string fixturesDir, string outFile)
    {
        var sb = new StringBuilder();
        AppendHeader(sb, "V2010 long-row suffix source analysis", "long-row-suffix");

        await DumpV2010SuffixAnalysisAsync(GetV2010Fixture(fixturesDir), sb, CancellationToken.None);
        await WriteOutputAsync(outFile, sb);
        return 0;
    }

    public static async Task<int> RunCrcSweepAsync(string fixturesDir, string outFile)
    {
        var sb = new StringBuilder();
        AppendHeader(sb, "V2010 long-row suffix CRC-16 sweep", "long-row-crc-sweep");
        sb.AppendLine("This mode is intentionally slow. The last known local run took about 3 minutes.");
        sb.AppendLine();

        await DumpV2010CrcFullSweepAsync(GetV2010Fixture(fixturesDir), sb, CancellationToken.None);
        await WriteOutputAsync(outFile, sb);
        return 0;
    }

    private static async Task DumpV2010SuffixAnalysisAsync(string fixturePath, StringBuilder sb, CancellationToken ct)
    {
        await using var reader = await AccessReader.OpenAsync(
            fixturePath,
            new AccessReaderOptions { UseLockFile = false },
            ct);
        DataTable dataTable = await reader.ReadDataTableAsync("Table11", cancellationToken: ct);
        IndexLeafPageBuilder.LeafPageLayout ascLayout = IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);
        List<IndexEntry> ascKeys = await CollectAllLeafKeysAsync(reader, ascLayout, reader.PageSize, firstPage: 112, ct);

        GeneralLegacyTextIndexEncoder.CharHandler[] codes = GeneralCodes.Value;
        GeneralLegacyTextIndexEncoder.CharHandler[] extCodes = GeneralExtCodes.Value;

        var rowData = new List<RowData>();
        var rowToLeaf = new (int RowIndex, int LeafIndex)[]
        {
            (2, 2),
            (3, 4),
            (4, 3),
        };

        sb.AppendLine(CultureInfo.InvariantCulture, $"Fixture: `{fixturePath}`");
        sb.AppendLine();
        sb.AppendLine("## Constraint rows");
        sb.AppendLine();

        foreach ((int rowIndex, int leafIndex) in rowToLeaf)
        {
            string text = (string)dataTable.Rows[rowIndex]["data"];
            byte[] expected = ascKeys[leafIndex].Key;
            ushort expectedSuffix = (ushort)((expected[508] << 8) | expected[509]);
            byte[] full = BuildFullV2010Entry(text, ascending: true, codes, extCodes);
            rowData.Add(new RowData(rowIndex, expectedSuffix, full, text));
            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"- row[{rowIndex}] asc leaf[{leafIndex}] expected=0x{expectedSuffix:X4} fullLen={full.Length} textLen={text.Length}");
        }

        AppendInputCandidateSummary(rowData, sb);

        sb.AppendLine();
        sb.AppendLine("## Char-by-char inline analysis around position 508");
        sb.AppendLine();

        foreach (RowData row in rowData)
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"### row[{row.RowIndex}] expected=0x{row.ExpectedSuffix:X4}");
            sb.AppendLine();

            int inlinePosition = 1;
            int lastCharBefore508 = -1;
            int firstCharAt508 = -1;

            for (int charIndex = 0; charIndex < Math.Min(row.Text.Length, 300); charIndex++)
            {
                char currentChar = row.Text[charIndex];
                GeneralLegacyTextIndexEncoder.CharHandler handler = currentChar <= LastChar
                    ? codes[currentChar]
                    : extCodes[currentChar - FirstExtChar];
                byte[]? inlineBytes = handler.GetInlineBytes(currentChar);
                int inlineLength = inlineBytes?.Length ?? 0;

                if (inlinePosition + inlineLength > 508 && firstCharAt508 < 0)
                {
                    firstCharAt508 = charIndex;
                }

                if (inlinePosition <= 508)
                {
                    lastCharBefore508 = charIndex;
                }

                if (charIndex >= 250 && charIndex <= 260)
                {
                    sb.AppendLine(
                        CultureInfo.InvariantCulture,
                        $"  char[{charIndex}]='{currentChar}' (0x{(int)currentChar:X4}) inlinePos={inlinePosition} inlLen={inlineLength} inl={InlineHex(inlineBytes)}");
                }

                inlinePosition += inlineLength;
            }

            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"  lastCharBefore508={lastCharBefore508} firstCharAt508={firstCharAt508}");

            var inlineOnly = new List<byte>(512) { GeneralLegacyTextIndexEncoder.FlagAscendingNonNull };
            int charsUsed = 0;
            for (int charIndex = 0; charIndex < row.Text.Length; charIndex++)
            {
                char currentChar = row.Text[charIndex];
                GeneralLegacyTextIndexEncoder.CharHandler handler = currentChar <= LastChar
                    ? codes[currentChar]
                    : extCodes[currentChar - FirstExtChar];
                byte[]? inlineBytes = handler.GetInlineBytes(currentChar);
                if (inlineBytes is not null)
                {
                    inlineOnly.AddRange(inlineBytes);
                }

                charsUsed++;
                if (inlineOnly.Count >= GeneralLegacyTextIndexEncoder.MaxEntryLengthGeneralV2010)
                {
                    break;
                }
            }

            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"  pure inline charsUsed={charsUsed} totalLen={inlineOnly.Count}");
            if (inlineOnly.Count >= GeneralLegacyTextIndexEncoder.MaxEntryLengthGeneralV2010)
            {
                ushort tail = (ushort)((inlineOnly[508] << 8) | inlineOnly[509]);
                sb.AppendLine(CultureInfo.InvariantCulture, $"  tail[508..509]=0x{tail:X4} match={tail == row.ExpectedSuffix}");
                sb.AppendLine(
                    CultureInfo.InvariantCulture,
                    $"  hex[506..509]={Convert.ToHexString(inlineOnly.GetRange(506, 4).ToArray())}");
            }

            sb.AppendLine();
        }
    }

    private static async Task DumpV2010CrcFullSweepAsync(string fixturePath, StringBuilder sb, CancellationToken ct)
    {
        await using var reader = await AccessReader.OpenAsync(
            fixturePath,
            new AccessReaderOptions { UseLockFile = false },
            ct);
        DataTable dataTable = await reader.ReadDataTableAsync("Table11", cancellationToken: ct);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);

        List<IndexEntry> ascKeys = await CollectAllLeafKeysAsync(reader, layout, reader.PageSize, firstPage: 112, ct);
        List<IndexEntry> descKeys = await CollectAllLeafKeysAsync(reader, layout, reader.PageSize, firstPage: 119, ct);

        GeneralLegacyTextIndexEncoder.CharHandler[] codes = GeneralCodes.Value;
        GeneralLegacyTextIndexEncoder.CharHandler[] extCodes = GeneralExtCodes.Value;
        Encoding cp1252 = Encoding.GetEncoding(1252);

        var constraints = new List<ConstraintSet>();
        var rowToLeaf = new (int RowIndex, int AscLeafIndex)[]
        {
            (2, 2),
            (3, 4),
            (4, 3),
        };

        sb.AppendLine("## Constraint set");
        sb.AppendLine();

        foreach ((int rowIndex, int ascLeafIndex) in rowToLeaf)
        {
            string text = (string)dataTable.Rows[rowIndex]["data"];

            byte[] expectedAsc = ascKeys[ascLeafIndex].Key;
            ushort suffixAsc = (ushort)((expectedAsc[508] << 8) | expectedAsc[509]);

            byte[] fullAsc = BuildFullV2010Entry(text, ascending: true, codes, extCodes);
            byte[][] inputsAsc = BuildInputCandidates(fullAsc, text, cp1252);
            constraints.Add(new ConstraintSet($"row[{rowIndex}].asc", inputsAsc, suffixAsc));
            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"- row[{rowIndex}] asc leaf[{ascLeafIndex}] expected=0x{suffixAsc:X4} fullLen={fullAsc.Length}");

            int descLeafIndex = FindComplementedDescLeaf(descKeys, expectedAsc);
            if (descLeafIndex < 0)
            {
                sb.AppendLine(CultureInfo.InvariantCulture, $"- row[{rowIndex}] desc: NOT FOUND in descKeys");
                continue;
            }

            byte[] expectedDesc = descKeys[descLeafIndex].Key;
            ushort suffixDesc = (ushort)((expectedDesc[508] << 8) | expectedDesc[509]);

            byte[] fullDesc = BuildFullV2010Entry(text, ascending: false, codes, extCodes);
            byte[][] inputsDesc = BuildInputCandidates(fullDesc, text, cp1252);
            constraints.Add(new ConstraintSet($"row[{rowIndex}].desc", inputsDesc, suffixDesc));
            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"- row[{rowIndex}] desc leaf[{descLeafIndex}] expected=0x{suffixDesc:X4} fullLen={fullDesc.Length}");
        }

        int candidateCount = constraints[0].Inputs.Length;
        sb.AppendLine();
        sb.AppendLine(
            CultureInfo.InvariantCulture,
            $"Sweep: {candidateCount} input candidates x 65536 polys x 16 modes = {candidateCount * 65536 * 16:N0} combos per constraint");
        sb.AppendLine("Filter: a (poly, mode, inputIdx) survives only if it satisfies all constraints simultaneously.");
        sb.AppendLine();

        var hits = new List<string>();
        ConstraintSet firstConstraint = constraints[0];

        for (int inputIndex = 0; inputIndex < candidateCount; inputIndex++)
        {
            byte[] firstInput = firstConstraint.Inputs[inputIndex];
            if (firstInput.Length == 0)
            {
                continue;
            }

            for (int polynomial = 0; polynomial <= 0xFFFF; polynomial++)
            {
                ushort polynomialValue = (ushort)polynomial;
                ushort reflectedPolynomial = ReflectU16(polynomialValue);
                for (int mode = 0; mode < 16; mode++)
                {
                    bool refIn = (mode & 1) != 0;
                    bool refOut = (mode & 2) != 0;
                    ushort init = (mode & 4) != 0 ? (ushort)0xFFFF : (ushort)0;
                    ushort xorOut = (mode & 8) != 0 ? (ushort)0xFFFF : (ushort)0;

                    ushort got = CrcFull(firstInput, polynomialValue, reflectedPolynomial, init, xorOut, refIn, refOut);
                    if (got != firstConstraint.Expected)
                    {
                        continue;
                    }

                    bool allMatch = true;
                    for (int constraintIndex = 1; constraintIndex < constraints.Count; constraintIndex++)
                    {
                        ConstraintSet constraint = constraints[constraintIndex];
                        ushort constraintGot = CrcFull(
                            constraint.Inputs[inputIndex],
                            polynomialValue,
                            reflectedPolynomial,
                            init,
                            xorOut,
                            refIn,
                            refOut);
                        if (constraintGot != constraint.Expected)
                        {
                            allMatch = false;
                            break;
                        }
                    }

                    if (allMatch)
                    {
                        string hit = string.Create(
                            CultureInfo.InvariantCulture,
                            $"HIT poly=0x{polynomialValue:X4} init=0x{init:X4} xorOut=0x{xorOut:X4} refIn={refIn} refOut={refOut} inputIdx={inputIndex} input={InputCandidateNames[inputIndex]}");
                        hits.Add(hit);
                        sb.AppendLine(hit);
                    }
                }
            }
        }

        sb.AppendLine();
        sb.AppendLine(CultureInfo.InvariantCulture, $"Total hits: {hits.Count}");
    }

    private static void AppendInputCandidateSummary(List<RowData> rowData, StringBuilder sb)
    {
        Encoding cp1252 = Encoding.GetEncoding(1252);

        sb.AppendLine();
        sb.AppendLine("## Input candidate lengths");
        sb.AppendLine();

        foreach (RowData row in rowData)
        {
            byte[][] inputs = BuildInputCandidates(row.Full, row.Text, cp1252);
            sb.AppendLine(CultureInfo.InvariantCulture, $"### row[{row.RowIndex}]");
            for (int inputIndex = 0; inputIndex < inputs.Length; inputIndex++)
            {
                sb.AppendLine(
                    CultureInfo.InvariantCulture,
                    $"- {inputIndex}: `{InputCandidateNames[inputIndex]}` len={inputs[inputIndex].Length}");
            }

            sb.AppendLine();
        }
    }

    private static byte[] BuildFullV2010Entry(
        string text,
        bool ascending,
        GeneralLegacyTextIndexEncoder.CharHandler[] codes,
        GeneralLegacyTextIndexEncoder.CharHandler[] extCodes)
        => GeneralLegacyTextIndexEncoder.EncodeWithTables(
            text,
            ascending,
            codes,
            extCodes,
            GeneralLegacyTextIndexEncoder.LongRowSeparatorGeneral,
            maxEntryLength: int.MaxValue);

    private static int FindComplementedDescLeaf(List<IndexEntry> descKeys, byte[] expectedAsc)
    {
        unchecked
        {
            for (int leafIndex = 0; leafIndex < descKeys.Count; leafIndex++)
            {
                byte[] descKey = descKeys[leafIndex].Key;
                if (descKey.Length != GeneralLegacyTextIndexEncoder.MaxEntryLengthGeneralV2010
                    || descKey[0] != GeneralLegacyTextIndexEncoder.FlagDescendingNonNull)
                {
                    continue;
                }

                bool match = true;
                for (int byteIndex = 1; byteIndex < 508; byteIndex++)
                {
                    if (descKey[byteIndex] != (byte)~expectedAsc[byteIndex])
                    {
                        match = false;
                        break;
                    }
                }

                if (match)
                {
                    return leafIndex;
                }
            }
        }

        return -1;
    }

    private static byte[][] BuildInputCandidates(byte[] full, string text, Encoding cp1252)
    {
        string remaining = text.Length > 255 ? text[255..] : string.Empty;
        string upper = text.ToUpperInvariant();
        string remainUpper = upper.Length > 255 ? upper[255..] : string.Empty;

        (byte[] extras, byte[] unprint) = SplitExtraAndUnprint(full);

        byte[] selfCheck = full.Length >= GeneralLegacyTextIndexEncoder.MaxEntryLengthGeneralV2010
            ? full[..GeneralLegacyTextIndexEncoder.MaxEntryLengthGeneralV2010]
            : (byte[])full.Clone();
        if (selfCheck.Length >= GeneralLegacyTextIndexEncoder.MaxEntryLengthGeneralV2010)
        {
            selfCheck[508] = 0;
            selfCheck[509] = 0;
        }

        return
        [
            full.Length > 508 ? full[508..] : [],
            full.Length > 510 ? full[510..] : [],
            full.Length > 509 ? full[508..^1] : [],
            cp1252.GetBytes(remaining),
            Encoding.Unicode.GetBytes(remaining),
            Encoding.Unicode.GetBytes(text),
            cp1252.GetBytes(remainUpper),
            cp1252.GetBytes(upper),
            extras,
            unprint,
            [.. extras, .. unprint],
            full.Length >= 508 ? full[..508] : full,
            full.Length >= 508 ? full[1..508] : full,
            selfCheck,
        ];
    }

    private static (byte[] Extras, byte[] Unprint) SplitExtraAndUnprint(byte[] full)
    {
        int endTextPos = -1;
        for (int index = 508; index < full.Length; index++)
        {
            if (full[index] == GeneralLegacyTextIndexEncoder.EndText)
            {
                endTextPos = index;
                break;
            }
        }

        byte[] extras = endTextPos >= 0 && endTextPos + 1 < full.Length
            ? full[(endTextPos + 1)..^1]
            : [];
        byte[] unprint = [];
        if (extras.Length > 3)
        {
            for (int index = 0; index < extras.Length - 2; index++)
            {
                if (extras[index] == GeneralLegacyTextIndexEncoder.EndText
                    && extras[index + 1] == GeneralLegacyTextIndexEncoder.EndText)
                {
                    unprint = extras[(index + 2)..];
                    extras = extras[..index];
                    break;
                }
            }
        }

        return (extras, unprint);
    }

    private static ushort CrcFull(
        byte[] data,
        ushort poly,
        ushort polyReflected,
        ushort init,
        ushort xorOut,
        bool refIn,
        bool refOut)
    {
        unchecked
        {
            ushort crc = init;
            if (refIn)
            {
                foreach (byte value in data)
                {
                    crc ^= value;
                    for (int bitIndex = 0; bitIndex < 8; bitIndex++)
                    {
                        crc = (crc & 1) != 0
                            ? (ushort)((crc >> 1) ^ polyReflected)
                            : (ushort)(crc >> 1);
                    }
                }
            }
            else
            {
                foreach (byte value in data)
                {
                    crc ^= (ushort)(value << 8);
                    for (int bitIndex = 0; bitIndex < 8; bitIndex++)
                    {
                        crc = (crc & 0x8000) != 0
                            ? (ushort)((crc << 1) ^ poly)
                            : (ushort)(crc << 1);
                    }
                }
            }

            if (refIn != refOut)
            {
                crc = ReflectU16(crc);
            }

            return (ushort)(crc ^ xorOut);
        }
    }

    private static ushort ReflectU16(ushort value)
    {
        unchecked
        {
            ushort result = 0;
            for (int bitIndex = 0; bitIndex < 16; bitIndex++)
            {
                result = (ushort)((result << 1) | (value & 1));
                value >>= 1;
            }

            return result;
        }
    }

    private static async Task<List<IndexEntry>> CollectAllLeafKeysAsync(
        AccessReader reader,
        IndexLeafPageBuilder.LeafPageLayout layout,
        int pageSize,
        long firstPage,
        CancellationToken ct)
    {
        long current = firstPage;
        var result = new List<IndexEntry>();
        while (current != 0)
        {
            byte[] page = await reader.GetRawPageBytesAsync(current, ct);
            List<IndexEntry> entries = IndexLeafIncremental.DecodeEntries(layout, page, pageSize);
            result.AddRange(entries);

            (long _, long next, long _) = IndexLeafIncremental.ReadSiblingPointers(layout, page);
            current = next;
        }

        return result;
    }

    private static string GetV2010Fixture(string fixturesDir)
        => Path.Combine(fixturesDir, "Jackcess", "V2010", "testIndexCodesV2010.accdb");

    private static void AppendHeader(StringBuilder sb, string title, string mode)
    {
        sb.AppendLine(CultureInfo.InvariantCulture, $"# {title}");
        sb.AppendLine();
        sb.AppendLine(CultureInfo.InvariantCulture, $"Generated by: `dotnet run --project JetDatabaseWriter.FormatProbe -- {mode}`");
        sb.AppendLine(CultureInfo.InvariantCulture, $"Generated at: {DateTimeOffset.UtcNow:u}");
        sb.AppendLine();
    }

    private static async Task WriteOutputAsync(string outFile, StringBuilder sb)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(outFile)!);
        await File.WriteAllTextAsync(outFile, sb.ToString());
        Console.WriteLine($"Wrote {outFile}");
    }

    private static string InlineHex(byte[]? bytes)
        => bytes is null ? "(none)" : Convert.ToHexString(bytes);

    private readonly record struct RowData(int RowIndex, ushort ExpectedSuffix, byte[] Full, string Text);

    private readonly record struct ConstraintSet(string Label, byte[][] Inputs, ushort Expected);
}
