// One-shot research probes for the unresolved V2010 long-row 2-byte suffix.
//
// Usage:
//   dotnet run --project JetDatabaseWriter.FormatProbe -- long-row-suffix
//   dotnet run --project JetDatabaseWriter.FormatProbe -- long-row-crc-sweep

namespace JetDatabaseWriter.FormatProbe;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.ValueDecoding;

internal static class LongRowSuffixProbe
{
    private const int PrefixMatchLength = 508;
    private const int LongRowEntryLength = GeneralLegacyTextIndexEncoder.MaxEntryLengthGeneralV2010;
    private const int DaoLabAlphabetLength = 65;
    private const string GeneralResource = "JetDatabaseWriter.IndexCodeTables.index_codes_gen.txt.gz";
    private const string GeneralExtResource = "JetDatabaseWriter.IndexCodeTables.index_codes_ext_gen.txt.gz";
    private const char FirstChar = (char)0x0000;
    private const char LastChar = (char)0x00FF;
    private const char FirstExtChar = (char)0x0100;
    private const char LastExtChar = (char)0xFFFF;
    private const int DaoLabBaseRowCount = 256;
    private const int DaoLabPairMatrixStart = DaoLabBaseRowCount;
    private const int DaoLabPairMatrixRowCount = DaoLabAlphabetLength * DaoLabAlphabetLength;
    private const int DaoLabAuxMatrixStart = DaoLabPairMatrixStart + DaoLabPairMatrixRowCount;
    private const int DaoLabAuxMatrixRowCount = DaoLabAlphabetLength * DaoLabAlphabetLength;
    private const int DaoLabRowCount = DaoLabAuxMatrixStart + DaoLabAuxMatrixRowCount;
    private const string DaoLabAlphabet = " abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_+";
    private const uint LcMapSortKey = 0x00000400;
    private const uint LcMapHash = 0x00040000;
    private const uint NormIgnoreCase = 0x00000001;
    private const uint NormIgnoreNonSpace = 0x00000002;
    private const uint NormIgnoreSymbols = 0x00000004;
    private const uint SortStringsSort = 0x00001000;

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
        "full[508..511]",
        "full[508..512]",
        "full[508..513]",
        "full[..508]",
        "full[1..508]",
        "full[..510] suffix zeroed",
    ];

    private static readonly int[] RollingInputIndexes = [0, 1, 2, 8, 9, 10, 11, 12, 13];

    private static readonly SearchValues<char> MarkdownEscapeSearch = SearchValues.Create("`|\r\n");

    private static readonly SearchValues<byte> EndTextSearch = SearchValues.Create([GeneralLegacyTextIndexEncoder.EndText]);

    private static readonly Lazy<List<CandidateRule>> SuffixCandidateRules = new(BuildSuffixCandidateRules);

    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, ExactSpelling = true, SetLastError = true)]
    private static extern int LCMapStringEx(
        string lpLocaleName,
        uint dwMapFlags,
        string lpSrcStr,
        int cchSrc,
        byte[] lpDestStr,
        int cchDest,
        IntPtr lpVersionInformation,
        IntPtr lpReserved,
        IntPtr sortHandle);

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

    public static async Task<int> RunCorpusScanAsync(string fixturesDir, string outFile)
    {
        var sb = new StringBuilder();
        AppendHeader(sb, "V2010 long-row corpus scan", "long-row-corpus");
        sb.AppendLine("Scans every Jackcess V2010 fixture for single-column index leaf keys exactly 510 bytes long.");
        sb.AppendLine("For Text/Memo and Binary columns, the probe re-encodes table values and checks whether the current encoder matches Access through byte 507.");
        sb.AppendLine();
        int summaryInsertOffset = sb.Length;

        string v2010Dir = Path.Combine(fixturesDir, "Jackcess", "V2010");
        if (!Directory.Exists(v2010Dir))
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"Missing fixture directory: `{v2010Dir}`");
            await WriteOutputAsync(outFile, sb);
            return 1;
        }

        var totals = new CorpusScanTotals();
        foreach (string fixturePath in Directory.EnumerateFiles(v2010Dir, "*.accdb").OrderBy(path => path, StringComparer.OrdinalIgnoreCase))
        {
            totals.FixturesScanned++;
            await ScanFixtureForLongRowsAsync(fixturePath, sb, totals, CancellationToken.None);
        }

        sb.Insert(summaryInsertOffset, BuildCorpusSummary(totals));
        await WriteOutputAsync(outFile, sb);
        return 0;
    }

    public static async Task<int> RunDaoLabAsync(string fixturesDir, string outFile, string workRoot)
    {
        var sb = new StringBuilder();
        AppendHeader(sb, "V2010 long-row DAO lab scan", "long-row-dao-lab");
        sb.AppendLine("Copies the V2010 index-code fixture, asks DAO/ACE to append generated long strings to the existing long-row stress tables, then scans the result for 510-byte keys.");
        sb.AppendLine();

        string baseFixture = GetV2010Fixture(fixturesDir);
        if (!File.Exists(baseFixture))
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"Missing fixture: `{baseFixture}`");
            await WriteOutputAsync(outFile, sb);
            return 1;
        }

        DaoPowerShellHostResolver.DaoPowerShellHostProbeResult hostProbe = DaoPowerShellHostResolver.Probe();
        if (hostProbe.HostPath is null)
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"DAO unavailable: {hostProbe.FailureReason}");
            await WriteOutputAsync(outFile, sb);
            return 1;
        }

        FormatProbeArtifacts.EnsureDirectory(workRoot);
        string labPath = FormatProbeArtifacts.GetFilePath(workRoot, "long-row-dao-lab.accdb");
        string scriptPath = FormatProbeArtifacts.GetFilePath(workRoot, "long-row-dao-lab-author.ps1");
        FormatProbeArtifacts.Copy(baseFixture, labPath, overwrite: true);

        (int exitCode, string stdout, string stderr) = RunPowerShell(
            hostProbe.HostPath,
            BuildDaoLabScript(labPath, DaoLabRowCount),
            scriptPath,
            TimeSpan.FromMinutes(5));

        sb.AppendLine("## DAO authoring");
        sb.AppendLine();
        sb.AppendLine(CultureInfo.InvariantCulture, $"- PowerShell host: `{hostProbe.HostPath}`");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- Lab database: `{labPath}`");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- Script: `{scriptPath}`");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- Requested rows per table: {DaoLabRowCount}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- Exit code: {exitCode}");
        if (!string.IsNullOrWhiteSpace(stdout))
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"- stdout: `{EscapeMarkdown(stdout.Trim())}`");
        }

        if (!string.IsNullOrWhiteSpace(stderr))
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"- stderr: `{EscapeMarkdown(stderr.Trim())}`");
        }

        sb.AppendLine();
        if (exitCode != 0)
        {
            await WriteOutputAsync(outFile, sb);
            return exitCode;
        }

        int summaryInsertOffset = sb.Length;
        var totals = new CorpusScanTotals { FixturesScanned = 1 };
        await ScanFixtureForLongRowsAsync(labPath, sb, totals, CancellationToken.None, maxExamples: 600);
        sb.Insert(summaryInsertOffset, BuildCorpusSummary(totals));

        await AppendDaoLabPatternSummaryAsync(labPath, sb, CancellationToken.None);

        await WriteOutputAsync(outFile, sb);
        return 0;
    }

    private static async Task AppendDaoLabPatternSummaryAsync(string labPath, StringBuilder sb, CancellationToken ct)
    {
        await using var reader = await AccessReader.OpenAsync(
            labPath,
            new AccessReaderOptions { UseLockFile = false },
            ct);

        sb.AppendLine("## DAO lab suffix pattern summary");
        sb.AppendLine();
        sb.AppendLine("Groups are synthetic text families emitted by `New-LabText`: seed 0-63 varies char[253], 64-127 varies char[254], 128-191 varies char[20], 192-255 adds international/unprintable characters plus optional CR/LF, then later ranges form plain and auxiliary char[253]/char[254] pair matrices over the DAO lab alphabet.");
        sb.AppendLine();

        foreach ((string tableName, int seedBase) in new[] { ("Table11", 100000), ("Table11_desc", 101000) })
        {
            SuffixPatternTable table = await BuildSuffixPatternTableAsync(reader, tableName, seedBase, ct);
            AppendSyntheticGroupSummary(sb, table);
            AppendDuplicateValueSummary(sb, table);
            AppendSuffixCandidateSummary(sb, table);
            AppendWideAffineTailSummary(sb, table);
            AppendRawLeafCompressionSummary(sb, tableName, table.Index, table.Layout, table.ReaderPageSize, table.RawLeafPages);
        }
    }

    private static async Task<SuffixPatternTable> BuildSuffixPatternTableAsync(
        AccessReader reader,
        string tableName,
        int seedBase,
        CancellationToken ct)
    {
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);
        int pageSize = reader.PageSize;

        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(tableName, ct);
        IndexMetadata index = indexes.First(idx => idx.Columns.Count == 1 && idx.Columns[0].Name.Equals("data", StringComparison.OrdinalIgnoreCase));
        IndexColumnReference keyColumn = index.Columns[0];

        List<LeafEntryDetail> leafEntries = await CollectDetailedLeafEntriesFromRootAsync(
            reader,
            layout,
            pageSize,
            index.FirstDp,
            ct);
        List<RawLeafPageSummary> rawLeafPages = await CollectRawLeafPageSummariesAsync(reader, layout, pageSize, index.FirstDp, ct);
        Dictionary<long, PhysicalRowSnapshot> rowByPointer = await BuildPhysicalRowSnapshotMapAsync(reader, tableName, ct);

        var rows = new List<SuffixPatternRow>();
        foreach (LeafEntryDetail leafEntry in leafEntries)
        {
            IndexEntry entry = leafEntry.Entry;
            if (entry.Key.Length != LongRowEntryLength
                || !rowByPointer.TryGetValue(EncodeDataPointer(entry.DataPage, entry.DataRow), out PhysicalRowSnapshot rowSnapshot)
                || rowSnapshot.Value is not string text)
            {
                continue;
            }

            byte[] encodedKey = GeneralTextIndexEncoder.Encode(text, keyColumn.IsAscending);
            byte[] fullKey = BuildFullV2010Entry(text, keyColumn.IsAscending, GeneralCodes.Value, GeneralExtCodes.Value);
            ushort accessSuffix = (ushort)((entry.Key[508] << 8) | entry.Key[509]);
            ushort encoderSuffix = encodedKey.Length >= LongRowEntryLength
                ? (ushort)((encodedKey[508] << 8) | encodedKey[509])
                : (ushort)0;
            bool prefixMatch = encodedKey.Length >= PrefixMatchLength
                && entry.Key.AsSpan(0, PrefixMatchLength).SequenceEqual(encodedKey.AsSpan(0, PrefixMatchLength));

            rows.Add(new SuffixPatternRow(
                rowSnapshot.RowLabel,
                TryParseLabSeed(rowSnapshot.RowLabel, seedBase),
                leafEntry.Position,
                entry.DataPage,
                entry.DataRow,
                accessSuffix,
                encoderSuffix,
                fullKey.Length,
                DescribeFullTail(fullKey),
                prefixMatch,
                leafEntry.LeafPage,
                leafEntry.EntryIndex,
                leafEntry.PrefixLength,
                leafEntry.RawKeyLength,
                leafEntry.EntryStart,
                fullKey,
                BuildTrimmedFullV2010Entry(text, keyColumn.IsAscending),
                text));
        }

        return new SuffixPatternTable(
            tableName,
            seedBase,
            index,
            keyColumn.IsAscending,
            layout,
            pageSize,
            rows,
            rawLeafPages);
    }

    private static int? TryParseLabSeed(string rowLabel, int seedBase)
    {
        string label = rowLabel.Trim('`');
        if (!label.StartsWith("lab", StringComparison.OrdinalIgnoreCase)
            || label.Length != 9
            || !int.TryParse(label.AsSpan(3), NumberStyles.None, CultureInfo.InvariantCulture, out int labNumber))
        {
            return null;
        }

        int seed = labNumber - seedBase;
        return seed is >= 0 && seed < DaoLabRowCount ? seed : null;
    }

    private static void AppendSyntheticGroupSummary(StringBuilder sb, SuffixPatternTable table)
    {
        sb.AppendLine(CultureInfo.InvariantCulture, $"### {table.TableName}.DataIndex synthetic groups");
        sb.AppendLine();
        sb.AppendLine(CultureInfo.InvariantCulture, $"- ascending: {table.Ascending}");
        sb.AppendLine();
        sb.AppendLine("| Group | Seed range | Count | Access suffixes | Encoder suffixes | Full lengths | First examples | Last examples |");
        sb.AppendLine("|---:|---|---:|---:|---:|---|---|---|");

        for (int group = 0; group < 4; group++)
        {
            int minSeed = group * 64;
            int maxSeed = minSeed + 63;
            List<SuffixPatternRow> rows = table.Rows
                .Where(row => row.Seed is >= 0 && row.Seed >= minSeed && row.Seed <= maxSeed)
                .OrderBy(row => row.Seed)
                .ToList();

            string accessCount = rows.Select(row => row.AccessSuffix).Distinct().Count().ToString(CultureInfo.InvariantCulture);
            string encoderCount = rows.Select(row => row.EncoderSuffix).Distinct().Count().ToString(CultureInfo.InvariantCulture);
            string lengths = rows.Count == 0
                ? "-"
                : string.Join(", ", rows.Select(row => row.FullLength).Distinct().OrderBy(length => length).Select(length => length?.ToString(CultureInfo.InvariantCulture) ?? "-"));

            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"| {group} | {minSeed}-{maxSeed} | {rows.Count} | {accessCount} | {encoderCount} | {lengths} | {DescribeSeedExamples(rows.Take(4))} | {DescribeSeedExamples(rows.TakeLast(4))} |");
        }

        sb.AppendLine();

        foreach (int group in new[] { 0, 1 })
        {
            int minSeed = group * 64;
            int maxSeed = minSeed + 63;
            List<SuffixPatternRow> rows = table.Rows
                .Where(row => row.Seed is >= 0 && row.Seed >= minSeed && row.Seed <= maxSeed)
                .OrderBy(row => row.Seed)
                .ToList();
            if (rows.Count == 0)
            {
                continue;
            }

            sb.AppendLine(CultureInfo.InvariantCulture, $"Seed detail for group {group} ({minSeed}-{maxSeed}):");
            sb.AppendLine();
            sb.AppendLine("| Seed | Access suffix | Encoder suffix | Prefix | Data ptr | Leaf entry | pref_len | raw len | raw start | Full tail |");
            sb.AppendLine("|---:|:---:|:---:|:---:|---:|---|---:|---:|---:|---|");
            foreach (SuffixPatternRow row in rows)
            {
                sb.AppendLine(
                    CultureInfo.InvariantCulture,
                    $"| {row.Seed} | `{row.AccessSuffix:X4}` | `{row.EncoderSuffix:X4}` | {(row.PrefixMatch ? "yes" : "no")} | {row.DataPage}:{row.DataRow} | {row.LeafPage}:{row.LeafEntryIndex} | {row.PrefixLength} | {row.RawKeyLength} | {row.EntryStart} | {row.FullTail} |");
            }

            sb.AppendLine();
        }

        AppendPairMatrixSummary(sb, table, DaoLabPairMatrixStart, "Pair matrix");
        AppendPairMatrixSummary(sb, table, DaoLabAuxMatrixStart, "Auxiliary pair matrix");
    }

    private static void AppendPairMatrixSummary(StringBuilder sb, SuffixPatternTable table, int matrixStart, string title)
    {
        List<SuffixPatternRow> rows = table.Rows
            .Where(row => row.Seed is not null && row.Seed.Value >= matrixStart && row.Seed.Value < matrixStart + DaoLabPairMatrixRowCount)
            .OrderBy(row => row.Seed)
            .ToList();
        if (rows.Count == 0)
        {
            return;
        }

        int accessDistinct = rows.Select(row => row.AccessSuffix).Distinct().Count();
        int encoderDistinct = rows.Select(row => row.EncoderSuffix).Distinct().Count();
        int accessCollisionBuckets = rows.GroupBy(row => row.AccessSuffix).Count(group => group.Count() > 1);

        sb.AppendLine(CultureInfo.InvariantCulture, $"{title} summary for seeds {matrixStart}-{matrixStart + DaoLabPairMatrixRowCount - 1}:");
        sb.AppendLine();
        sb.AppendLine(CultureInfo.InvariantCulture, $"- rows: {rows.Count}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- Access suffixes: {accessDistinct}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- encoder suffixes: {encoderDistinct}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- Access suffix collision buckets: {accessCollisionBuckets}");
        sb.AppendLine();
        AppendPairMatrixModelSummary(sb, table, rows, matrixStart);

        sb.AppendLine("| Pair | Access suffix | Encoder suffix | Full tail |");
        sb.AppendLine("|---|:---:|:---:|---|");

        foreach (SuffixPatternRow row in rows.Take(8).Concat(rows.Skip(Math.Max(0, rows.Count - 8))))
        {
            int pair = row.Seed!.Value - matrixStart;
            char first = DaoLabAlphabet[pair / DaoLabAlphabet.Length];
            char second = DaoLabAlphabet[pair % DaoLabAlphabet.Length];
            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"| `{EscapeMarkdown(new string(new[] { first, second }))}` | `{row.AccessSuffix:X4}` | `{row.EncoderSuffix:X4}` | {row.FullTail} |");
        }

        sb.AppendLine();
    }

    private static void AppendPairMatrixModelSummary(StringBuilder sb, SuffixPatternTable table, List<SuffixPatternRow> rows, int matrixStart)
    {
        int size = DaoLabAlphabet.Length;
        var suffixes = new ushort[size * size];
        var present = new bool[size * size];
        foreach (SuffixPatternRow row in rows)
        {
            int pair = row.Seed!.Value - matrixStart;
            int first = pair / size;
            int second = pair % size;
            if (first >= 0 && first < size && second >= 0 && second < size)
            {
                int index = (first * size) + second;
                suffixes[index] = row.AccessSuffix;
                present[index] = true;
            }
        }

        int baseIndex = DaoLabAlphabet.IndexOf('a', StringComparison.Ordinal);
        int baseOffset = (baseIndex * size) + baseIndex;
        if (baseIndex < 0 || !present[baseOffset])
        {
            return;
        }

        ushort baseValue = suffixes[baseOffset];
        int xorMatches = 0;
        int addMatches = 0;
        int highXorMatches = 0;
        int highAddMatches = 0;
        int lowXorMatches = 0;
        int lowAddMatches = 0;
        int total = 0;

        for (int first = 0; first < size; first++)
        {
            for (int second = 0; second < size; second++)
            {
                int index = (first * size) + second;
                if (!present[index])
                {
                    continue;
                }

                ushort actual = suffixes[index];
                int rowBaseOffset = (first * size) + baseIndex;
                int columnBaseOffset = (baseIndex * size) + second;
                if (!present[rowBaseOffset] || !present[columnBaseOffset])
                {
                    continue;
                }

                ushort rowBase = suffixes[rowBaseOffset];
                ushort columnBase = suffixes[columnBaseOffset];
                ushort xorPredicted = (ushort)(rowBase ^ columnBase ^ baseValue);
                ushort addPredicted = unchecked((ushort)(rowBase + columnBase - baseValue));

                if (xorPredicted == actual)
                {
                    xorMatches++;
                }

                if (addPredicted == actual)
                {
                    addMatches++;
                }

                byte actualHigh = (byte)(actual >> 8);
                byte actualLow = unchecked((byte)actual);
                byte xorHigh = (byte)((rowBase >> 8) ^ (columnBase >> 8) ^ (baseValue >> 8));
                byte xorLow = unchecked((byte)(rowBase ^ columnBase ^ baseValue));
                byte addHigh = unchecked((byte)((rowBase >> 8) + (columnBase >> 8) - (baseValue >> 8)));
                byte addLow = unchecked((byte)(rowBase + columnBase - baseValue));

                if (xorHigh == actualHigh)
                {
                    highXorMatches++;
                }

                if (addHigh == actualHigh)
                {
                    highAddMatches++;
                }

                if (xorLow == actualLow)
                {
                    lowXorMatches++;
                }

                if (addLow == actualLow)
                {
                    lowAddMatches++;
                }

                total++;
            }
        }

        sb.AppendLine("Pair matrix model checks:");
        sb.AppendLine();
        sb.AppendLine(CultureInfo.InvariantCulture, $"- XOR row/column decomposition: {xorMatches}/{total}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- Add row/column decomposition: {addMatches}/{total}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- High-byte XOR/add decomposition: {highXorMatches}/{total}, {highAddMatches}/{total}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- Low-byte XOR/add decomposition: {lowXorMatches}/{total}, {lowAddMatches}/{total}");

        List<Crc16AffineHit> crcHits = FindCrc16AffineHits(table, rows, maxHits: 8);
        string crcHitText = crcHits.Count == 0
            ? "-"
            : "`" + string.Join(" ", crcHits.Select(hit => $"poly={hit.Polynomial:X4}/xor={hit.XorConstant:X4}/refIn={hit.RefIn}/refOut={hit.RefOut}")) + "`";
        sb.AppendLine(CultureInfo.InvariantCulture, $"- CRC-16 affine hits over `full[508..]`: {crcHits.Count} {crcHitText}");
        AppendPairMatrixAffineBitSummary(sb, table, rows, matrixStart);
        sb.AppendLine();
        sb.AppendLine("Pair contribution examples (`H(x,a) ^ H(a,a)` and `H(a,x) ^ H(a,a)`):");
        sb.AppendLine();
        sb.AppendLine("| Char | Row contribution | Column contribution | Row suffix | Column suffix |");
        sb.AppendLine("|---|:---:|:---:|:---:|:---:|");
        foreach (int index in Enumerable.Range(0, Math.Min(16, size)).Concat(Enumerable.Range(Math.Max(16, size - 8), Math.Min(8, size - Math.Max(16, size - 8)))))
        {
            int rowOffset = (index * size) + baseIndex;
            int columnOffset = (baseIndex * size) + index;
            if (!present[rowOffset] || !present[columnOffset])
            {
                continue;
            }

            ushort rowSuffix = suffixes[rowOffset];
            ushort columnSuffix = suffixes[columnOffset];
            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"| `{EscapeMarkdown(DaoLabAlphabet[index].ToString())}` | `{rowSuffix ^ baseValue:X4}` | `{columnSuffix ^ baseValue:X4}` | `{rowSuffix:X4}` | `{columnSuffix:X4}` |");
        }

        sb.AppendLine();
    }

    private static List<Crc16AffineHit> FindCrc16AffineHits(
        SuffixPatternTable table,
        List<SuffixPatternRow> rows,
        int maxHits)
    {
        var constraints = rows
            .Where(row => row.Text is not null)
            .Select(row => new RollingConstraint(
                SliceOrEmpty(row.FullKey, 508),
                row.AccessSuffix))
            .Where(constraint => constraint.Input.Length > 0)
            .ToArray();

        var hits = new List<Crc16AffineHit>();
        if (constraints.Length == 0)
        {
            return hits;
        }

        for (int polynomial = 0; polynomial <= 0xFFFF; polynomial++)
        {
            ushort polynomialValue = (ushort)polynomial;
            ushort reflectedPolynomial = ReflectU16(polynomialValue);
            for (int mode = 0; mode < 4; mode++)
            {
                bool refIn = (mode & 1) != 0;
                bool refOut = (mode & 2) != 0;
                ushort first = CrcFull(constraints[0].Input, polynomialValue, reflectedPolynomial, 0, 0, refIn, refOut);
                ushort xorConstant = (ushort)(constraints[0].Target ^ first);

                bool allMatch = true;
                for (int index = 1; index < constraints.Length; index++)
                {
                    ushort crc = CrcFull(constraints[index].Input, polynomialValue, reflectedPolynomial, 0, 0, refIn, refOut);
                    if ((ushort)(crc ^ xorConstant) != constraints[index].Target)
                    {
                        allMatch = false;
                        break;
                    }
                }

                if (allMatch)
                {
                    hits.Add(new Crc16AffineHit(polynomialValue, xorConstant, refIn, refOut));
                    if (hits.Count >= maxHits)
                    {
                        return hits;
                    }
                }
            }
        }

        return hits;
    }

    private static void AppendPairMatrixAffineBitSummary(
        StringBuilder sb,
        SuffixPatternTable table,
        List<SuffixPatternRow> rows,
        int matrixStart)
    {
        SuffixPatternRow[] usableRows = rows.Where(row => row.Text is not null).ToArray();
        ushort[] targets = usableRows.Select(row => row.AccessSuffix).ToArray();

        sb.AppendLine("- Affine bit models:");
        AppendAffineBitResult(sb, "full[508..511] raw bits", usableRows, targets, table, 24, BuildRawTailFeature);
        AppendAffineBitResult(sb, "trimmed-full[508..511] raw bits", usableRows, targets, table, 24, BuildTrimmedRawTailFeature);
        AppendAffineBitResult(sb, "full[507..511] raw bits", usableRows, targets, table, 32, BuildRawWindowFeature507);
        AppendAffineBitResult(sb, "trimmed-full[507..511] raw bits", usableRows, targets, table, 32, BuildTrimmedRawWindowFeature507);
        AppendAffineBitResult(sb, "full[506..511] raw bits", usableRows, targets, table, 40, BuildRawWindowFeature506);
        AppendAffineBitResult(sb, "trimmed-full[506..511] raw bits", usableRows, targets, table, 40, BuildTrimmedRawWindowFeature506);
        AppendAffineBitResult(sb, "full[508..512] raw bits", usableRows, targets, table, 32, BuildRawTailFeature);
        AppendAffineBitResult(sb, "full[508..513] raw bits", usableRows, targets, table, 40, BuildRawTailFeature);
        if (OperatingSystem.IsWindows())
        {
            AppendAffineBitResult(sb, "text[253..255] LCMapHash ignore-case", usableRows, targets, table, 32, BuildLcMapHashFeature);
        }

        AppendTrainedAffineScore(sb, "trained full[508..511] model", table, usableRows, targets, 24, BuildRawTailFeature);
        AppendTrainedAffineScore(sb, "trained trimmed-full[508..511] model", table, usableRows, targets, 24, BuildTrimmedRawTailFeature);
        AppendTrainedAffineScore(sb, "trained full[506..511] model", table, usableRows, targets, 40, BuildRawWindowFeature506);
        AppendTrainedAffineScore(sb, "trained trimmed-full[506..511] model", table, usableRows, targets, 40, BuildTrimmedRawWindowFeature506);
        AppendSecondSpaceAffineScore(sb, table, usableRows, matrixStart, 24, BuildRawTailFeature);
    }

    private static void AppendAffineBitResult(
        StringBuilder sb,
        string label,
        SuffixPatternRow[] rows,
        ushort[] targets,
        SuffixPatternTable table,
        int bitCount,
        Func<SuffixPatternRow, SuffixPatternTable, int, ulong?> buildFeature)
    {
        ulong[] features = new ulong[rows.Length];
        ushort[] featureTargets = new ushort[rows.Length];
        int featureCount = 0;
        for (int index = 0; index < rows.Length; index++)
        {
            ulong? feature = buildFeature(rows[index], table, bitCount);
            if (feature.HasValue)
            {
                features[featureCount] = feature.Value | (1UL << bitCount);
                featureTargets[featureCount] = targets[index];
                featureCount++;
            }
        }

        if (featureCount != rows.Length)
        {
            Array.Resize(ref features, featureCount);
            Array.Resize(ref featureTargets, featureCount);
        }

        bool fits = featureCount > 0 && TryFitAffineBinaryModel(features, featureTargets, bitCount + 1, out _);
        sb.AppendLine(CultureInfo.InvariantCulture, $"  - {label}: {(fits ? "fits" : "no fit")} ({featureCount}/{rows.Length} rows)");
    }

    private static void AppendTrainedAffineScore(
        StringBuilder sb,
        string label,
        SuffixPatternTable table,
        SuffixPatternRow[] pairRows,
        ushort[] pairTargets,
        int bitCount,
        Func<SuffixPatternRow, SuffixPatternTable, int, ulong?> buildFeature)
    {
        ulong[] pairFeatures = new ulong[pairRows.Length];
        int featureCount = 0;
        for (int index = 0; index < pairRows.Length; index++)
        {
            ulong? feature = buildFeature(pairRows[index], table, bitCount);
            if (feature.HasValue)
            {
                pairFeatures[featureCount++] = feature.Value | (1UL << bitCount);
            }
        }

        if (featureCount != pairTargets.Length
            || !TryFitAffineBinaryModel(pairFeatures, pairTargets, bitCount + 1, out ulong[] coefficients))
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"  - {label}: no fit");
            return;
        }

        int evaluated = 0;
        int exact = 0;
        foreach (SuffixPatternRow row in table.Rows.Where(row => row.Text is not null))
        {
            ulong? feature = buildFeature(row, table, bitCount);
            if (!feature.HasValue)
            {
                continue;
            }

            evaluated++;
            ushort predicted = PredictAffineBinary(feature.Value | (1UL << bitCount), coefficients);
            if (predicted == row.AccessSuffix)
            {
                exact++;
            }
        }

        string coefficientText = string.Join(" ", coefficients.Select(coefficient => coefficient.ToString("X7", CultureInfo.InvariantCulture)));
        sb.AppendLine(CultureInfo.InvariantCulture, $"  - {label} scored on all rows: {exact}/{evaluated}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"  - {label} coefficients: `{coefficientText}`");
    }

    private static void AppendSecondSpaceAffineScore(
        StringBuilder sb,
        SuffixPatternTable table,
        SuffixPatternRow[] pairRows,
        int matrixStart,
        int bitCount,
        Func<SuffixPatternRow, SuffixPatternTable, int, ulong?> buildFeature)
    {
        SuffixPatternRow[] normalRows = pairRows.Where(row => !IsMatrixSecondSpace(row, matrixStart)).ToArray();
        SuffixPatternRow[] secondSpaceRows = pairRows.Where(row => IsMatrixSecondSpace(row, matrixStart)).ToArray();
        if (!TryTrainAffineRows(normalRows, table, bitCount, buildFeature, out ulong[] normalCoefficients)
            || !TryTrainAffineRows(secondSpaceRows, table, bitCount, buildFeature, out ulong[] secondSpaceCoefficients))
        {
            sb.AppendLine("  - piecewise second-space full[508..511] model: no fit");
            return;
        }

        int evaluated = 0;
        int exact = 0;
        foreach (SuffixPatternRow row in table.Rows.Where(row => row.Text is not null))
        {
            ulong? feature = buildFeature(row, table, bitCount);
            if (!feature.HasValue)
            {
                continue;
            }

            bool secondSpace = HasSecondIndexedSpace(row.Text!);
            ulong[] coefficients = secondSpace ? secondSpaceCoefficients : normalCoefficients;
            ushort predicted = PredictAffineBinary(feature.Value | (1UL << bitCount), coefficients);
            evaluated++;
            if (predicted == row.AccessSuffix)
            {
                exact++;
            }
        }

        sb.AppendLine(CultureInfo.InvariantCulture, $"  - piecewise second-space full[508..511] model scored on all rows: {exact}/{evaluated} (normal train {normalRows.Length}, second-space train {secondSpaceRows.Length})");
    }

    private static bool TryTrainAffineRows(
        SuffixPatternRow[] rows,
        SuffixPatternTable table,
        int bitCount,
        Func<SuffixPatternRow, SuffixPatternTable, int, ulong?> buildFeature,
        out ulong[] coefficients)
    {
        coefficients = [];
        ulong[] features = new ulong[rows.Length];
        ushort[] targets = new ushort[rows.Length];
        int featureCount = 0;
        for (int index = 0; index < rows.Length; index++)
        {
            ulong? feature = buildFeature(rows[index], table, bitCount);
            if (feature.HasValue)
            {
                features[featureCount++] = feature.Value | (1UL << bitCount);
            }

            targets[index] = rows[index].AccessSuffix;
        }

        return featureCount == targets.Length
            && TryFitAffineBinaryModel(features, targets, bitCount + 1, out coefficients);
    }

    private static bool IsMatrixSecondSpace(SuffixPatternRow row, int matrixStart)
    {
        if (row.Seed is null || row.Seed.Value < matrixStart || row.Seed.Value >= matrixStart + DaoLabPairMatrixRowCount)
        {
            return false;
        }

        int pair = row.Seed.Value - matrixStart;
        return pair % DaoLabAlphabet.Length == 0;
    }

    private static bool HasSecondIndexedSpace(string text)
        => text.Length >= 255 && text[254] == ' ';

    private static ulong? BuildRawTailFeature(SuffixPatternRow row, SuffixPatternTable table, int bitCount)
        => BuildRawWindowFeature(row, table, start: 508, bitCount, trimIndexedText: false);

    private static ulong? BuildTrimmedRawTailFeature(SuffixPatternRow row, SuffixPatternTable table, int bitCount)
        => BuildRawWindowFeature(row, table, start: 508, bitCount, trimIndexedText: true);

    private static ulong? BuildRawWindowFeature507(SuffixPatternRow row, SuffixPatternTable table, int bitCount)
        => BuildRawWindowFeature(row, table, start: 507, bitCount, trimIndexedText: false);

    private static ulong? BuildTrimmedRawWindowFeature507(SuffixPatternRow row, SuffixPatternTable table, int bitCount)
        => BuildRawWindowFeature(row, table, start: 507, bitCount, trimIndexedText: true);

    private static ulong? BuildRawWindowFeature506(SuffixPatternRow row, SuffixPatternTable table, int bitCount)
        => BuildRawWindowFeature(row, table, start: 506, bitCount, trimIndexedText: false);

    private static ulong? BuildTrimmedRawWindowFeature506(SuffixPatternRow row, SuffixPatternTable table, int bitCount)
        => BuildRawWindowFeature(row, table, start: 506, bitCount, trimIndexedText: true);

    private static ulong? BuildRawWindowFeature(
        SuffixPatternRow row,
        SuffixPatternTable table,
        int start,
        int bitCount,
        bool trimIndexedText)
    {
        int bytesToTake = bitCount / 8;
        _ = table;
        byte[] full = trimIndexedText ? row.TrimmedFullKey : row.FullKey;
        if (bytesToTake == 0)
        {
            return 0;
        }

        if (full.Length <= start || full.Length - start < bytesToTake)
        {
            return null;
        }

        ulong feature = 0;
        ReadOnlySpan<byte> tail = full.AsSpan(start, bytesToTake);
        for (int index = 0; index < tail.Length; index++)
        {
            feature |= (ulong)tail[index] << (index * 8);
        }

        return feature;
    }

    private static ulong? BuildLcMapHashFeature(SuffixPatternRow row, SuffixPatternTable table, int bitCount)
    {
        _ = table;
        _ = bitCount;
        byte[] hash = LcMapHashBytes("en-US", LcMapHash | NormIgnoreCase, TextWindow(row.Text!, 253, 255));
        return hash.Length == 4 ? BinaryPrimitives.ReadUInt32LittleEndian(hash) : null;
    }

    private static bool TryFitAffineBinaryModel(
        ulong[] features,
        ushort[] targets,
        int variableCount,
        out ulong[] coefficients)
    {
        coefficients = new ulong[16];
        for (int targetBit = 0; targetBit < 16; targetBit++)
        {
            var basis = new ulong[variableCount];
            var basisRhs = new int[variableCount];
            for (int row = 0; row < features.Length; row++)
            {
                ulong mask = features[row];
                int rhs = (targets[row] >> targetBit) & 1;
                while (mask != 0)
                {
                    int pivot = 63 - BitOperations.LeadingZeroCount(mask);
                    if (basis[pivot] == 0)
                    {
                        basis[pivot] = mask;
                        basisRhs[pivot] = rhs;
                        break;
                    }

                    mask ^= basis[pivot];
                    rhs ^= basisRhs[pivot];
                }

                if (mask == 0 && rhs != 0)
                {
                    coefficients = [];
                    return false;
                }
            }

            ulong solution = 0;
            for (int pivot = 0; pivot < variableCount; pivot++)
            {
                if (basis[pivot] == 0)
                {
                    continue;
                }

                ulong dependencyMask = pivot == 0 ? 0 : basis[pivot] & ((1UL << pivot) - 1);
                int value = basisRhs[pivot] ^ (BitOperations.PopCount(solution & dependencyMask) & 1);
                if (value != 0)
                {
                    solution |= 1UL << pivot;
                }
            }

            coefficients[targetBit] = solution;
        }

        return true;
    }

    private static ushort PredictAffineBinary(ulong feature, ulong[] coefficients)
    {
        ushort result = 0;
        for (int bit = 0; bit < coefficients.Length; bit++)
        {
            if ((BitOperations.PopCount(feature & coefficients[bit]) & 1) != 0)
            {
                result |= (ushort)(1 << bit);
            }
        }

        return result;
    }

    private static void AppendDuplicateValueSummary(StringBuilder sb, SuffixPatternTable table)
    {
        List<IGrouping<string, SuffixPatternRow>> duplicateGroups = table.Rows
            .Where(row => row.Text is not null)
            .GroupBy(row => row.Text!, StringComparer.Ordinal)
            .Where(group => group.Count() > 1)
            .OrderByDescending(group => group.Select(row => row.AccessSuffix).Distinct().Count())
            .ThenByDescending(group => group.Count())
            .ToList();

        int conflictingGroups = duplicateGroups.Count(group => group.Select(row => row.AccessSuffix).Distinct().Count() > 1);

        sb.AppendLine(CultureInfo.InvariantCulture, $"Exact duplicate value check for {table.TableName}.DataIndex:");
        sb.AppendLine();
        sb.AppendLine(CultureInfo.InvariantCulture, $"- duplicate text groups: {duplicateGroups.Count}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- duplicate groups with multiple Access suffixes: {conflictingGroups}");

        if (duplicateGroups.Count == 0)
        {
            sb.AppendLine();
            return;
        }

        sb.AppendLine();
        sb.AppendLine("| Rows | Access suffixes | Seeds | Data ptrs |");
        sb.AppendLine("|---:|---|---|---|");
        foreach (IGrouping<string, SuffixPatternRow> group in duplicateGroups.Take(8))
        {
            SuffixPatternRow[] rows = group.OrderBy(row => row.Position).ToArray();
            string suffixes = string.Join(" ", rows.Select(row => row.AccessSuffix).Distinct().OrderBy(value => value).Select(value => $"`{value:X4}`"));
            string seeds = string.Join(" ", rows.Select(row => row.Seed?.ToString(CultureInfo.InvariantCulture) ?? row.RowLabel));
            string ptrs = string.Join(" ", rows.Select(row => string.Create(CultureInfo.InvariantCulture, $"{row.DataPage}:{row.DataRow}")));
            sb.AppendLine(CultureInfo.InvariantCulture, $"| {rows.Length} | {suffixes} | `{seeds}` | `{ptrs}` |");
        }

        sb.AppendLine();
    }

    private static void AppendSuffixCandidateSummary(StringBuilder sb, SuffixPatternTable table)
    {
        SuffixCandidateContext[] contexts = table.Rows
            .Where(row => row.Text is not null)
            .Select(row => new SuffixCandidateContext(row))
            .ToArray();

        sb.AppendLine(CultureInfo.InvariantCulture, $"Suffix candidate score for {table.TableName}.DataIndex:");
        sb.AppendLine();
        if (contexts.Length == 0)
        {
            sb.AppendLine("- no text rows available for candidate scoring");
            sb.AppendLine();
            return;
        }

        List<CandidateRule> rules = SuffixCandidateRules.Value;
        var xorCounts = new CountAccumulator();
        var addCounts = new CountAccumulator();
        List<CandidateScore> scores = rules
            .Select(rule => ScoreCandidate(rule, contexts, xorCounts, addCounts))
            .Where(score => score.Evaluated > 0)
            .OrderByDescending(score => score.Exact)
            .ThenByDescending(score => score.BestXorCount)
            .ThenByDescending(score => score.BestAddCount)
            .ThenBy(score => score.Name, StringComparer.Ordinal)
            .Take(16)
            .ToList();

        sb.AppendLine(CultureInfo.InvariantCulture, $"- rows scored: {contexts.Length}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- candidates tested: {rules.Count}");
        sb.AppendLine();
        sb.AppendLine("| Candidate | Exact | Best XOR | XOR constant | Best add | Add constant |");
        sb.AppendLine("|---|---:|---:|:---:|---:|:---:|");
        foreach (CandidateScore score in scores)
        {
            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"| {score.Name} | {score.Exact}/{score.Evaluated} | {score.BestXorCount}/{score.Evaluated} | `{score.BestXorConstant:X4}` | {score.BestAddCount}/{score.Evaluated} | `{score.BestAddConstant:X4}` |");
        }

        sb.AppendLine();
        AppendRollingPolynomialSolverSummary(sb, contexts);
        AppendAuxSignatureSummary(sb, contexts);
        if (OperatingSystem.IsWindows())
        {
            AppendLcMapSortKeySweepSummary(sb, contexts);
        }
    }

    private static void AppendAuxSignatureSummary(StringBuilder sb, SuffixCandidateContext[] contexts)
    {
        Encoding cp1252 = Encoding.GetEncoding(1252);
        var groups = contexts
            .Select(context =>
            {
                byte[][] inputs = context.GetInputCandidates(cp1252);
                return new
                {
                    Context = context,
                    Signature = Convert.ToHexString(inputs[10]),
                    Length = inputs[10].Length,
                };
            })
            .Where(item => item.Length > 0)
            .GroupBy(item => item.Signature, StringComparer.Ordinal)
            .OrderByDescending(group => group.Count())
            .ThenBy(group => group.Key, StringComparer.Ordinal)
            .Take(12)
            .ToList();

        sb.AppendLine("Auxiliary stream signatures:");
        sb.AppendLine();
        if (groups.Count == 0)
        {
            sb.AppendLine("- no auxiliary streams in scored rows");
            sb.AppendLine();
            return;
        }

        sb.AppendLine("| Rows | Access suffixes | First rows | Signature |");
        sb.AppendLine("|---:|---|---|---|");
        foreach (var group in groups)
        {
            string suffixes = string.Join(" ", group.Select(item => item.Context.Row.AccessSuffix).Distinct().OrderBy(value => value).Select(value => $"`{value:X4}`"));
            string rows = string.Join(" ", group.Take(6).Select(item => item.Context.Row.RowLabel));
            sb.AppendLine(CultureInfo.InvariantCulture, $"| {group.Count()} | {suffixes} | `{rows}` | `{group.Key}` |");
        }

        sb.AppendLine();
    }

    private static void AppendLcMapSortKeySweepSummary(StringBuilder sb, SuffixCandidateContext[] contexts)
    {
        var hits = new List<CandidateScore>();
        var xorCounts = new CountAccumulator();
        var addCounts = new CountAccumulator();
        foreach ((string label, uint flags) in new[]
        {
            ("en-US none", LcMapSortKey),
            ("en-US ignore-case", LcMapSortKey | NormIgnoreCase),
            ("en-US ignore-case-string", LcMapSortKey | NormIgnoreCase | SortStringsSort),
            ("en-US ignore-case-nonspace", LcMapSortKey | NormIgnoreCase | NormIgnoreNonSpace),
        })
        {
            byte[][] sortKeys = contexts
                .Select(context => LcMapSortKeyBytes("en-US", flags, context.TextInputs[3]))
                .ToArray();
            int maxLength = sortKeys.Length == 0 ? 0 : sortKeys.Max(key => key.Length);
            for (int offset = 0; offset + 1 < maxLength; offset++)
            {
                hits.Add(ScoreSortKeyOffset(contexts, sortKeys, $"{label} offset {offset} BE", offset, bigEndian: true, xorCounts: xorCounts, addCounts: addCounts));
                hits.Add(ScoreSortKeyOffset(contexts, sortKeys, $"{label} offset {offset} LE", offset, bigEndian: false, xorCounts: xorCounts, addCounts: addCounts));
            }
        }

        List<CandidateScore> best = hits
            .Where(score => score.Evaluated > 0)
            .OrderByDescending(score => score.Exact)
            .ThenByDescending(score => score.BestXorCount)
            .ThenByDescending(score => score.Evaluated)
            .ThenBy(score => score.Name, StringComparer.Ordinal)
            .Take(8)
            .ToList();

        sb.AppendLine("LCMap sort-key offset sweep:");
        sb.AppendLine();
        sb.AppendLine("| Candidate | Exact | Best XOR | XOR constant | Best add | Add constant |");
        sb.AppendLine("|---|---:|---:|:---:|---:|:---:|");
        foreach (CandidateScore score in best)
        {
            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"| {score.Name} | {score.Exact}/{score.Evaluated} | {score.BestXorCount}/{score.Evaluated} | `{score.BestXorConstant:X4}` | {score.BestAddCount}/{score.Evaluated} | `{score.BestAddConstant:X4}` |");
        }

        sb.AppendLine();
    }

    private static CandidateScore ScoreSortKeyOffset(
        SuffixCandidateContext[] contexts,
        byte[][] sortKeys,
        string name,
        int offset,
        bool bigEndian,
        CountAccumulator xorCounts,
        CountAccumulator addCounts)
    {
        int evaluated = 0;
        int exact = 0;
        for (int index = 0; index < contexts.Length; index++)
        {
            byte[] sortKey = sortKeys[index];
            ushort? candidate = ReadWordOrNull(sortKey, offset, bigEndian);
            if (!candidate.HasValue)
            {
                continue;
            }

            evaluated++;
            ushort access = contexts[index].Row.AccessSuffix;
            if (candidate.Value == access)
            {
                exact++;
            }

            xorCounts.Increment((ushort)(access ^ candidate.Value));
            addCounts.Increment(unchecked((ushort)(access - candidate.Value)));
        }

        (ushort bestXor, int bestXorCount) = xorCounts.Best();
        (ushort bestAdd, int bestAddCount) = addCounts.Best();
        xorCounts.Clear();
        addCounts.Clear();
        return new CandidateScore(name, evaluated, exact, bestXorCount, bestXor, bestAddCount, bestAdd);
    }

    private static void AppendWideAffineTailSummary(StringBuilder sb, SuffixPatternTable table)
    {
        SuffixPatternRow[] trainRows = table.Rows
            .Where(row => row is { Text: not null, Seed: not null })
            .ToArray();
        SuffixPatternRow[] allRows = table.Rows
            .Where(row => row.Text is not null)
            .ToArray();
        SuffixPatternRow[] originalRows = allRows.Where(row => row.Seed is null).ToArray();
        if (trainRows.Length == 0 || allRows.Length == 0)
        {
            return;
        }

        sb.AppendLine(CultureInfo.InvariantCulture, $"Wide affine tail models for {table.TableName}.DataIndex:");
        sb.AppendLine();
        sb.AppendLine("Trains on DAO-generated rows only; scores all rows, including the original fixture rows.");
        sb.AppendLine();
        sb.AppendLine("| Feature | Fit | Synthetic score | Original score | All score | Variables |");
        sb.AppendLine("|---|:---:|---:|---:|---:|---:|");
        AppendWideAffineTailResult(sb, table, trainRows, originalRows, allRows, start: 0, includeLength: true);
        AppendWideAffineTailResult(sb, table, trainRows, originalRows, allRows, start: 508, includeLength: false);
        AppendWideAffineTailResult(sb, table, trainRows, originalRows, allRows, start: 508, includeLength: true);
        AppendWideAffineTailResult(sb, table, trainRows, originalRows, allRows, start: 510, includeLength: true);
        sb.AppendLine();
    }

    private static void AppendWideAffineTailResult(
        StringBuilder sb,
        SuffixPatternTable table,
        SuffixPatternRow[] trainRows,
        SuffixPatternRow[] originalRows,
        SuffixPatternRow[] allRows,
        int start,
        bool includeLength)
    {
        _ = table;
        int byteCount = trainRows
            .Select(row => Math.Max(0, row.FullKey.Length - start))
            .DefaultIfEmpty(0)
            .Max();
        int featureBytes = byteCount + (includeLength ? 1 : 0);
        int variableCount = (featureBytes * 8) + 1;
        BigInteger[] trainFeatures = trainRows
            .Select(row => BuildWideTailFeature(row, table, start, byteCount, includeLength))
            .ToArray();
        ushort[] trainTargets = trainRows.Select(row => row.AccessSuffix).ToArray();

        bool fits = TryFitWideAffineBinaryModel(trainFeatures, trainTargets, variableCount, out BigInteger[] coefficients);
        string label = includeLength
            ? $"full[{start}..]+len"
            : $"full[{start}..]";
        if (!fits)
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"| {label} | no | - | - | - | {variableCount} |");
            return;
        }

        (int syntheticExact, int syntheticTotal) = ScoreWideAffineRows(trainRows, table, coefficients, start, byteCount, includeLength);
        (int originalExact, int originalTotal) = ScoreWideAffineRows(originalRows, table, coefficients, start, byteCount, includeLength);
        (int allExact, int allTotal) = ScoreWideAffineRows(allRows, table, coefficients, start, byteCount, includeLength);

        sb.AppendLine(
            CultureInfo.InvariantCulture,
            $"| {label} | yes | {syntheticExact}/{syntheticTotal} | {originalExact}/{originalTotal} | {allExact}/{allTotal} | {variableCount} |");
    }

    private static (int Exact, int Total) ScoreWideAffineRows(
        SuffixPatternRow[] rows,
        SuffixPatternTable table,
        BigInteger[] coefficients,
        int start,
        int byteCount,
        bool includeLength)
    {
        int exact = 0;
        foreach (SuffixPatternRow row in rows)
        {
            BigInteger feature = BuildWideTailFeature(row, table, start, byteCount, includeLength);
            ushort predicted = PredictWideAffineBinary(feature, coefficients);
            if (predicted == row.AccessSuffix)
            {
                exact++;
            }
        }

        return (exact, rows.Length);
    }

    private static BigInteger BuildWideTailFeature(
        SuffixPatternRow row,
        SuffixPatternTable table,
        int start,
        int byteCount,
        bool includeLength)
    {
        _ = table;
        byte[] full = row.FullKey;
        byte[] bytes = new byte[byteCount + (includeLength ? 1 : 0) + 1];
        int available = Math.Max(0, full.Length - start);
        int take = Math.Min(byteCount, available);
        if (take > 0)
        {
            full.AsSpan(start, take).CopyTo(bytes);
        }

        if (includeLength)
        {
            bytes[byteCount] = unchecked((byte)Math.Min(available, 255));
        }

        int interceptBit = (byteCount + (includeLength ? 1 : 0)) * 8;
        bytes[interceptBit / 8] |= (byte)(1 << (interceptBit % 8));
        return new BigInteger(bytes, isUnsigned: true, isBigEndian: false);
    }

    private static bool TryFitWideAffineBinaryModel(
        BigInteger[] features,
        ushort[] targets,
        int variableCount,
        out BigInteger[] coefficients)
    {
        coefficients = new BigInteger[16];
        for (int targetBit = 0; targetBit < 16; targetBit++)
        {
            var basis = new BigInteger[variableCount];
            var basisRhs = new int[variableCount];
            for (int row = 0; row < features.Length; row++)
            {
                BigInteger mask = features[row];
                int rhs = (targets[row] >> targetBit) & 1;
                while (!mask.IsZero)
                {
                    int pivot = HighestSetBit(mask);
                    if (basis[pivot].IsZero)
                    {
                        basis[pivot] = mask;
                        basisRhs[pivot] = rhs;
                        break;
                    }

                    mask ^= basis[pivot];
                    rhs ^= basisRhs[pivot];
                }

                if (mask.IsZero && rhs != 0)
                {
                    coefficients = [];
                    return false;
                }
            }

            BigInteger solution = BigInteger.Zero;
            for (int pivot = 0; pivot < variableCount; pivot++)
            {
                if (basis[pivot].IsZero)
                {
                    continue;
                }

                BigInteger dependencyMask = pivot == 0 ? BigInteger.Zero : basis[pivot] & ((BigInteger.One << pivot) - BigInteger.One);
                int value = basisRhs[pivot] ^ Parity(solution & dependencyMask);
                if (value != 0)
                {
                    solution |= BigInteger.One << pivot;
                }
            }

            coefficients[targetBit] = solution;
        }

        return true;
    }

    private static ushort PredictWideAffineBinary(BigInteger feature, BigInteger[] coefficients)
    {
        ushort result = 0;
        for (int bit = 0; bit < coefficients.Length; bit++)
        {
            if (Parity(feature & coefficients[bit]) != 0)
            {
                result |= (ushort)(1 << bit);
            }
        }

        return result;
    }

    private static int HighestSetBit(BigInteger value)
    {
        byte[] bytes = value.ToByteArray(isUnsigned: true, isBigEndian: false);
        int top = bytes.Length - 1;
        return (top * 8) + (7 - BitOperations.LeadingZeroCount(bytes[top]) + 24);
    }

    private static int Parity(BigInteger value)
    {
        int parity = 0;
        foreach (byte item in value.ToByteArray(isUnsigned: true, isBigEndian: false))
        {
            parity ^= BitOperations.PopCount(item) & 1;
        }

        return parity;
    }

    private static void AppendRollingPolynomialSolverSummary(StringBuilder sb, SuffixCandidateContext[] contexts)
    {
        Encoding cp1252 = Encoding.GetEncoding(1252);

        sb.AppendLine("Rolling polynomial solver:");
        sb.AppendLine();
        sb.AppendLine("Tests `h = h * multiplier + byte (mod 65536)` with every odd multiplier, solving the seed from the first row and requiring an exact match on all rows.");
        sb.AppendLine();
        sb.AppendLine("| Input | Matches | First hits |");
        sb.AppendLine("|---|---:|---|");

        foreach (int inputIndex in RollingInputIndexes)
        {
            RollingConstraint[] constraints = contexts
                .Select(context =>
                {
                    byte[][] inputs = context.GetInputCandidates(cp1252);
                    return new RollingConstraint(inputs[inputIndex], context.Row.AccessSuffix);
                })
                .Where(constraint => constraint.Input.Length > 0)
                .ToArray();

            List<RollingPolynomialHit> hits = FindRollingPolynomialHits(constraints, maxHits: 8);
            string hitText = hits.Count == 0
                ? "-"
                : "`" + string.Join(" ", hits.Select(hit => $"m={hit.Multiplier:X4}/seed={hit.Seed:X4}")) + "`";
            sb.AppendLine(CultureInfo.InvariantCulture, $"| {InputCandidateNames[inputIndex]} | {hits.Count} | {hitText} |");
        }

        sb.AppendLine();
    }

    private static List<CandidateRule> BuildSuffixCandidateRules()
    {
        var rules = new List<CandidateRule>();
        foreach ((string label, Func<SuffixCandidateContext, byte[]> getBytes) in BuildByteInputs())
        {
            rules.Add(new CandidateRule($"{label} word BE", context => ReadWordOrNull(getBytes(context), 0, bigEndian: true)));
            rules.Add(new CandidateRule($"{label} word LE", context => ReadWordOrNull(getBytes(context), 0, bigEndian: false)));
            rules.Add(new CandidateRule($"{label} FNV1a16", context => Fnv1A16(getBytes(context))));
            AddHash32WordRules(rules, label, "FNV1a32", context => Fnv1A32(getBytes(context)));
            rules.Add(new CandidateRule($"{label} DJB2-16", context => Djb216(getBytes(context))));
            AddHash32WordRules(rules, label, "DJB2-32", context => Djb232(getBytes(context)));
            AddHash32WordRules(rules, label, "SDBM-32", context => Sdbm32(getBytes(context)));
            AddHash32WordRules(rules, label, "JenkinsOAAT-32", context => JenkinsOneAtATime32(getBytes(context)));
            AddHash32WordRules(rules, label, "Murmur3-32 seed0", context => Murmur3X86_32(getBytes(context), 0));
            AddHash32WordRules(rules, label, "Murmur3-32 seedFFFF", context => Murmur3X86_32(getBytes(context), 0xFFFF));
            AddHash32WordRules(rules, label, "CRC32", context => Crc32(getBytes(context)));
#pragma warning disable CA5350, CA5351 // Research-only scoring of legacy hash candidates; not used for security.
            AddDigestWordRules(rules, label, "MD5", context => MD5.HashData(getBytes(context)));
            AddDigestWordRules(rules, label, "SHA1", context => SHA1.HashData(getBytes(context)));
#pragma warning restore CA5350, CA5351
            rules.Add(new CandidateRule($"{label} Adler16", context => Adler16(getBytes(context))));
            rules.Add(new CandidateRule($"{label} Fletcher16", context => Fletcher16(getBytes(context))));
        }

        foreach ((string label, Func<SuffixCandidateContext, string> getText) in BuildTextInputs())
        {
            AddCompareInfoRules(rules, label, getText, CultureInfo.InvariantCulture.CompareInfo, "Invariant");
            AddCompareInfoRules(rules, label, getText, CultureInfo.GetCultureInfo("en-US").CompareInfo, "en-US");
            if (OperatingSystem.IsWindows())
            {
                AddLcMapHashRules(rules, label, getText, "en-US");
                AddLcMapSortKeyRules(rules, label, getText, "en-US");
            }
        }

        return rules;
    }

    private static IEnumerable<(string Label, Func<SuffixCandidateContext, byte[]> GetBytes)> BuildByteInputs()
    {
        yield return ("full[508..]", context => context.ByteInputs[0]);
        yield return ("full[510..]", context => context.ByteInputs[1]);
        yield return ("full[508..511]", context => context.ByteInputs[2]);
        yield return ("full[508..512]", context => context.ByteInputs[3]);
        yield return ("full[508..513]", context => context.ByteInputs[4]);
        yield return ("full[..508]", context => context.ByteInputs[5]);
        yield return ("full[..510] zero", context => context.ByteInputs[6]);
    }

    private static IEnumerable<(string Label, Func<SuffixCandidateContext, string> GetText)> BuildTextInputs()
    {
        yield return ("text[253..255]", context => context.TextInputs[0]);
        yield return ("text[254..255]", context => context.TextInputs[1]);
        yield return ("text[253..]", context => context.TextInputs[2]);
        yield return ("text[..255]", context => context.TextInputs[3]);
    }

    private static void AddCompareInfoRules(
        List<CandidateRule> rules,
        string label,
        Func<SuffixCandidateContext, string> getText,
        CompareInfo compareInfo,
        string compareLabel)
    {
        foreach (CompareOptions options in new[]
        {
            CompareOptions.None,
            CompareOptions.IgnoreCase,
            CompareOptions.IgnoreCase | CompareOptions.IgnoreNonSpace,
            CompareOptions.IgnoreCase | CompareOptions.IgnoreNonSpace | CompareOptions.IgnoreSymbols,
        })
        {
            string name = $"{label} {compareLabel} CompareHash {options}";
            rules.Add(new CandidateRule($"{name} lo16", context => Low16(compareInfo.GetHashCode(getText(context), options))));
            rules.Add(new CandidateRule($"{name} hi16", context => High16(compareInfo.GetHashCode(getText(context), options))));
            rules.Add(new CandidateRule($"{name} lo16swap", context => ByteSwap(Low16(compareInfo.GetHashCode(getText(context), options)))));
            rules.Add(new CandidateRule($"{name} hi16swap", context => ByteSwap(High16(compareInfo.GetHashCode(getText(context), options)))));
        }
    }

    private static void AddHash32WordRules(
        List<CandidateRule> rules,
        string label,
        string hashName,
        Func<SuffixCandidateContext, uint> compute)
    {
        string name = $"{label} {hashName}";
        rules.Add(new CandidateRule($"{name} lo16", context => unchecked((ushort)compute(context))));
        rules.Add(new CandidateRule($"{name} hi16", context => unchecked((ushort)(compute(context) >> 16))));
        rules.Add(new CandidateRule($"{name} lo16swap", context => ByteSwap(unchecked((ushort)compute(context)))));
        rules.Add(new CandidateRule($"{name} hi16swap", context => ByteSwap(unchecked((ushort)(compute(context) >> 16)))));
    }

    private static void AddDigestWordRules(
        List<CandidateRule> rules,
        string label,
        string hashName,
        Func<SuffixCandidateContext, byte[]> compute)
    {
        foreach (int offset in new[] { 0, 2, 4, 6 })
        {
            string name = $"{label} {hashName} word{offset / 2}";
            rules.Add(new CandidateRule($"{name} BE", context => ReadWordOrNull(compute(context), offset, bigEndian: true)));
            rules.Add(new CandidateRule($"{name} LE", context => ReadWordOrNull(compute(context), offset, bigEndian: false)));
        }
    }

    private static void AddLcMapHashRules(
        List<CandidateRule> rules,
        string label,
        Func<SuffixCandidateContext, string> getText,
        string localeName)
    {
        foreach ((string optionLabel, uint flags) in new[]
        {
            ("none", LcMapHash),
            ("ignore-case", LcMapHash | NormIgnoreCase),
            ("ignore-case-nonspace", LcMapHash | NormIgnoreCase | NormIgnoreNonSpace),
            ("ignore-case-nonspace-symbols", LcMapHash | NormIgnoreCase | NormIgnoreNonSpace | NormIgnoreSymbols),
        })
        {
            string name = $"{label} LCMapHash {localeName} {optionLabel}";
            rules.Add(new CandidateRule($"{name} word0 BE", context => ReadWordOrNull(context.GetLcMapHashBytes(localeName, flags, getText(context)), 0, bigEndian: true)));
            rules.Add(new CandidateRule($"{name} word0 LE", context => ReadWordOrNull(context.GetLcMapHashBytes(localeName, flags, getText(context)), 0, bigEndian: false)));
            rules.Add(new CandidateRule($"{name} word1 BE", context => ReadWordOrNull(context.GetLcMapHashBytes(localeName, flags, getText(context)), 2, bigEndian: true)));
            rules.Add(new CandidateRule($"{name} word1 LE", context => ReadWordOrNull(context.GetLcMapHashBytes(localeName, flags, getText(context)), 2, bigEndian: false)));
        }
    }

    private static void AddLcMapSortKeyRules(
        List<CandidateRule> rules,
        string label,
        Func<SuffixCandidateContext, string> getText,
        string localeName)
    {
        foreach ((string optionLabel, uint flags) in new[]
        {
            ("none", LcMapSortKey),
            ("ignore-case", LcMapSortKey | NormIgnoreCase),
            ("ignore-case-nonspace", LcMapSortKey | NormIgnoreCase | NormIgnoreNonSpace),
            ("ignore-case-nonspace-symbols", LcMapSortKey | NormIgnoreCase | NormIgnoreNonSpace | NormIgnoreSymbols),
        })
        {
            string name = $"{label} LCMapSortKey {localeName} {optionLabel}";
            rules.Add(new CandidateRule($"{name} word0 BE", context => ReadWordOrNull(context.GetLcMapSortKeyBytes(localeName, flags, getText(context)), 0, bigEndian: true)));
            rules.Add(new CandidateRule($"{name} word0 LE", context => ReadWordOrNull(context.GetLcMapSortKeyBytes(localeName, flags, getText(context)), 0, bigEndian: false)));
            rules.Add(new CandidateRule($"{name} word1 BE", context => ReadWordOrNull(context.GetLcMapSortKeyBytes(localeName, flags, getText(context)), 2, bigEndian: true)));
            rules.Add(new CandidateRule($"{name} word1 LE", context => ReadWordOrNull(context.GetLcMapSortKeyBytes(localeName, flags, getText(context)), 2, bigEndian: false)));
            rules.Add(new CandidateRule($"{name} last BE", context => ReadLastWordOrNull(context.GetLcMapSortKeyBytes(localeName, flags, getText(context)), bigEndian: true)));
            rules.Add(new CandidateRule($"{name} last LE", context => ReadLastWordOrNull(context.GetLcMapSortKeyBytes(localeName, flags, getText(context)), bigEndian: false)));
            rules.Add(new CandidateRule($"{name} FNV1a16", context => Fnv1A16(context.GetLcMapSortKeyBytes(localeName, flags, getText(context)))));
            rules.Add(new CandidateRule($"{name} Adler16", context => Adler16(context.GetLcMapSortKeyBytes(localeName, flags, getText(context)))));
            rules.Add(new CandidateRule($"{name} Fletcher16", context => Fletcher16(context.GetLcMapSortKeyBytes(localeName, flags, getText(context)))));
        }
    }

    private static CandidateScore ScoreCandidate(
        CandidateRule rule,
        SuffixCandidateContext[] contexts,
        CountAccumulator xorCounts,
        CountAccumulator addCounts)
    {
        int evaluated = 0;
        int exact = 0;
        foreach (SuffixCandidateContext context in contexts)
        {
            ushort? candidate = rule.Compute(context);
            if (!candidate.HasValue)
            {
                continue;
            }

            evaluated++;
            ushort access = context.Row.AccessSuffix;
            if (candidate.Value == access)
            {
                exact++;
            }

            xorCounts.Increment((ushort)(access ^ candidate.Value));
            addCounts.Increment(unchecked((ushort)(access - candidate.Value)));
        }

        (ushort bestXor, int bestXorCount) = xorCounts.Best();
        (ushort bestAdd, int bestAddCount) = addCounts.Best();
        xorCounts.Clear();
        addCounts.Clear();
        return new CandidateScore(rule.Name, evaluated, exact, bestXorCount, bestXor, bestAddCount, bestAdd);
    }

    private static List<RollingPolynomialHit> FindRollingPolynomialHits(
        RollingConstraint[] constraints,
        int maxHits)
    {
        var hits = new List<RollingPolynomialHit>();
        if (constraints.Length == 0)
        {
            return hits;
        }

        RollingConstraint first = constraints[0];
        for (int multiplierValue = 1; multiplierValue <= 0xFFFF; multiplierValue += 2)
        {
            ushort multiplier = (ushort)multiplierValue;
            ushort power = PowModU16(multiplier, first.Input.Length);
            ushort seedFactorInverse = ModInverseOdd(power);
            ushort noSeed = RollingAddNoSeed(first.Input, multiplier);
            ushort seed = unchecked((ushort)((first.Target - noSeed) * seedFactorInverse));

            bool allMatch = true;
            for (int constraintIndex = 0; constraintIndex < constraints.Length; constraintIndex++)
            {
                RollingConstraint constraint = constraints[constraintIndex];
                if (RollingAdd(constraint.Input, multiplier, seed) != constraint.Target)
                {
                    allMatch = false;
                    break;
                }
            }

            if (allMatch)
            {
                hits.Add(new RollingPolynomialHit(multiplier, seed));
                if (hits.Count >= maxHits)
                {
                    break;
                }
            }
        }

        return hits;
    }

    private static ushort RollingAdd(byte[] input, ushort multiplier, ushort seed)
    {
        unchecked
        {
            int hash = seed;
            foreach (byte value in input)
            {
                hash = ((hash * multiplier) + value) & 0xFFFF;
            }

            return (ushort)hash;
        }
    }

    private static ushort RollingAddNoSeed(byte[] input, ushort multiplier) =>
        RollingAdd(input, multiplier, 0);

    private static ushort PowModU16(ushort value, int exponent)
    {
        unchecked
        {
            int result = 1;
            int factor = value;
            int remaining = exponent;
            while (remaining > 0)
            {
                if ((remaining & 1) != 0)
                {
                    result = (result * factor) & 0xFFFF;
                }

                factor = (factor * factor) & 0xFFFF;
                remaining >>= 1;
            }

            return (ushort)result;
        }
    }

    private static ushort ModInverseOdd(ushort value)
    {
        if ((value & 1) == 0)
        {
            throw new ArgumentException("Only odd values are invertible modulo 65536.", nameof(value));
        }

        unchecked
        {
            int inverse = value;
            inverse *= 2 - (value * inverse);
            inverse *= 2 - (value * inverse);
            inverse *= 2 - (value * inverse);
            inverse *= 2 - (value * inverse);
            return (ushort)inverse;
        }
    }

    private static byte[] SliceOrEmpty(byte[] bytes, int start) =>
        bytes.Length > start ? bytes[start..] : [];

    private static byte[] SliceOrEmpty(byte[] bytes, int start, int length)
    {
        if (length <= 0 || bytes.Length <= start)
        {
            return [];
        }

        int available = Math.Min(length, bytes.Length - start);
        return bytes.AsSpan(start, available).ToArray();
    }

    private static byte[] ZeroSuffixCopy(byte[] bytes)
    {
        byte[] copy = bytes.Length >= LongRowEntryLength ? bytes[..LongRowEntryLength] : (byte[])bytes.Clone();
        if (copy.Length >= LongRowEntryLength)
        {
            copy[508] = 0;
            copy[509] = 0;
        }

        return copy;
    }

    private static string TextWindow(string text, int start, int endExclusive)
    {
        if (start >= text.Length)
        {
            return string.Empty;
        }

        int end = Math.Min(text.Length, Math.Max(start, endExclusive));
        return text[start..end];
    }

    private static ushort? ReadWordOrNull(byte[] bytes, int offset, bool bigEndian)
    {
        if (offset < 0 || offset + 2 > bytes.Length)
        {
            return null;
        }

        return bigEndian
            ? BinaryPrimitives.ReadUInt16BigEndian(bytes.AsSpan(offset, 2))
            : BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(offset, 2));
    }

    private static ushort? ReadLastWordOrNull(byte[] bytes, bool bigEndian)
    {
        if (bytes.Length < 2)
        {
            return null;
        }

        int offset = bytes[^1] == 0 && bytes.Length >= 3 ? bytes.Length - 3 : bytes.Length - 2;
        return ReadWordOrNull(bytes, offset, bigEndian);
    }

    private static ushort Fnv1A16(byte[] bytes)
    {
        unchecked
        {
            uint hash = Fnv1A32(bytes);

            return (ushort)((hash >> 16) ^ hash);
        }
    }

    private static uint Fnv1A32(byte[] bytes)
    {
        unchecked
        {
            uint hash = 2166136261u;
            foreach (byte value in bytes)
            {
                hash ^= value;
                hash *= 16777619u;
            }

            return hash;
        }
    }

    private static ushort Djb216(byte[] bytes)
    {
        unchecked
        {
            uint hash = Djb232(bytes);

            return (ushort)((hash >> 16) ^ hash);
        }
    }

    private static uint Djb232(byte[] bytes)
    {
        unchecked
        {
            uint hash = 5381u;
            foreach (byte value in bytes)
            {
                hash = ((hash << 5) + hash) + value;
            }

            return hash;
        }
    }

    private static uint Sdbm32(byte[] bytes)
    {
        unchecked
        {
            uint hash = 0;
            foreach (byte value in bytes)
            {
                hash = value + (hash << 6) + (hash << 16) - hash;
            }

            return hash;
        }
    }

    private static uint JenkinsOneAtATime32(byte[] bytes)
    {
        unchecked
        {
            uint hash = 0;
            foreach (byte value in bytes)
            {
                hash += value;
                hash += hash << 10;
                hash ^= hash >> 6;
            }

            hash += hash << 3;
            hash ^= hash >> 11;
            hash += hash << 15;
            return hash;
        }
    }

    private static uint Murmur3X86_32(byte[] bytes, uint seed)
    {
        unchecked
        {
            const uint C1 = 0xCC9E2D51;
            const uint C2 = 0x1B873593;
            uint hash = seed;
            int roundedEnd = bytes.Length & ~3;

            for (int index = 0; index < roundedEnd; index += 4)
            {
                uint k1 = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(index, 4));
                k1 *= C1;
                k1 = RotateLeft(k1, 15);
                k1 *= C2;

                hash ^= k1;
                hash = RotateLeft(hash, 13);
                hash = (hash * 5) + 0xE6546B64;
            }

            uint tail = 0;
            switch (bytes.Length & 3)
            {
                case 3:
                    tail ^= (uint)bytes[roundedEnd + 2] << 16;
                    goto case 2;
                case 2:
                    tail ^= (uint)bytes[roundedEnd + 1] << 8;
                    goto case 1;
                case 1:
                    tail ^= bytes[roundedEnd];
                    tail *= C1;
                    tail = RotateLeft(tail, 15);
                    tail *= C2;
                    hash ^= tail;
                    break;
            }

            hash ^= (uint)bytes.Length;
            hash ^= hash >> 16;
            hash *= 0x85EBCA6B;
            hash ^= hash >> 13;
            hash *= 0xC2B2AE35;
            hash ^= hash >> 16;
            return hash;
        }
    }

    private static uint RotateLeft(uint value, int offset) =>
        (value << offset) | (value >> (32 - offset));

    private static uint Crc32(byte[] bytes)
    {
        unchecked
        {
            uint crc = 0xFFFFFFFFu;
            foreach (byte value in bytes)
            {
                crc ^= value;
                for (int bit = 0; bit < 8; bit++)
                {
                    crc = (crc & 1) != 0 ? (crc >> 1) ^ 0xEDB88320u : crc >> 1;
                }
            }

            return ~crc;
        }
    }

    private static ushort Adler16(byte[] bytes)
    {
        const int Mod = 251;
        int a = 1;
        int b = 0;
        foreach (byte value in bytes)
        {
            a = (a + value) % Mod;
            b = (b + a) % Mod;
        }

        return (ushort)((b << 8) | a);
    }

    private static ushort Fletcher16(byte[] bytes)
    {
        int sum1 = 0;
        int sum2 = 0;
        foreach (byte value in bytes)
        {
            sum1 = (sum1 + value) % 255;
            sum2 = (sum2 + sum1) % 255;
        }

        return (ushort)((sum2 << 8) | sum1);
    }

    private static ushort Low16(int value) => unchecked((ushort)value);

    private static ushort High16(int value) => unchecked((ushort)(value >> 16));

    private static ushort ByteSwap(ushort value) => unchecked((ushort)((value << 8) | (value >> 8)));

    private static byte[] LcMapHashBytes(string localeName, uint flags, string value)
    {
        byte[] buffer = new byte[4];
        int result = LCMapStringEx(localeName, flags, value, value.Length, buffer, buffer.Length, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero);
        return result > 0 ? buffer : [];
    }

    private static byte[] LcMapSortKeyBytes(string localeName, uint flags, string value)
    {
        byte[] buffer = new byte[Math.Max(32, (value.Length * 8) + 32)];
        int result = LCMapStringEx(localeName, flags, value, value.Length, buffer, buffer.Length, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero);
        if (result <= 0)
        {
            return [];
        }

        int length = Math.Min(result, buffer.Length);
        return buffer[..length];
    }

    private static string DescribeSeedExamples(IEnumerable<SuffixPatternRow> rows)
    {
        string[] parts = rows
            .Select(row => string.Create(
                CultureInfo.InvariantCulture,
                $"{row.Seed}:{row.AccessSuffix:X4}/{row.EncoderSuffix:X4}"))
            .ToArray();
        return parts.Length == 0 ? "-" : $"`{string.Join(" ", parts)}`";
    }

    private static void AppendRawLeafCompressionSummary(
        StringBuilder sb,
        string tableName,
        IndexMetadata index,
        IndexLeafPageBuilder.LeafPageLayout layout,
        int pageSize,
        IReadOnlyList<RawLeafPageSummary> pages)
    {
        _ = layout;
        _ = pageSize;
        sb.AppendLine(CultureInfo.InvariantCulture, $"### {tableName}.DataIndex raw leaf compression");
        sb.AppendLine();
        sb.AppendLine(CultureInfo.InvariantCulture, $"- first_dp: {index.FirstDp}");
        sb.AppendLine();
        sb.AppendLine("| Leaf page | pref_len | payload end | entries | 510-byte decoded entries | First long raw key len | First long raw key tail | First long decoded suffix |");
        sb.AppendLine("|---:|---:|---:|---:|---:|---:|---|:---:|");

        foreach (RawLeafPageSummary page in pages)
        {
            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"| {page.PageNumber} | {page.PrefixLength} | {page.PayloadEnd} | {page.EntryCount} | {page.LongEntryCount} | {page.FirstLongRawKeyLength?.ToString(CultureInfo.InvariantCulture) ?? "-"} | {page.FirstLongRawKeyTail} | {(page.FirstLongDecodedSuffix.HasValue ? $"`{page.FirstLongDecodedSuffix.Value:X4}`" : "-")} |");
        }

        sb.AppendLine();
    }

    private static async Task ScanFixtureForLongRowsAsync(
        string fixturePath,
        StringBuilder sb,
        CorpusScanTotals totals,
        CancellationToken ct,
        int maxExamples = 12)
    {
        var fixtureReport = new StringBuilder();
        int fixtureLongIndexes = 0;
        int fixtureLongKeys = 0;

        try
        {
            await using var reader = await AccessReader.OpenAsync(
                fixturePath,
                new AccessReaderOptions { UseLockFile = false },
                ct);
            IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);
            int pageSize = reader.PageSize;

            List<string> tables = await reader.ListTablesAsync(ct);
            foreach (string tableName in tables)
            {
                List<ColumnMetadata> columns;
                IReadOnlyList<IndexMetadata> indexes;
                try
                {
                    columns = await reader.GetColumnMetadataAsync(tableName, ct);
                    indexes = await reader.ListIndexesAsync(tableName, ct);
                }
                catch (NotSupportedException ex)
                {
                    fixtureReport.AppendLine(CultureInfo.InvariantCulture, $"- `{tableName}` skipped: {ex.Message}");
                    continue;
                }

                var columnByName = columns.ToDictionary(column => column.Name, StringComparer.OrdinalIgnoreCase);
                foreach (IndexMetadata index in indexes)
                {
                    if (index.Columns.Count != 1 || index.IsForeignKey || index.FirstDp <= 0)
                    {
                        continue;
                    }

                    List<IndexEntry> onDiskEntries = await CollectAllLeafEntriesFromRootAsync(
                        reader, layout, pageSize, index.FirstDp, ct);
                    int onDiskLongCount = onDiskEntries.Count(entry => entry.Key.Length == LongRowEntryLength);
                    if (onDiskLongCount == 0)
                    {
                        continue;
                    }

                    IndexColumnReference keyColumn = index.Columns[0];
                    if (!columnByName.TryGetValue(keyColumn.Name, out ColumnMetadata? columnMeta))
                    {
                        continue;
                    }

                    fixtureLongIndexes++;
                    fixtureLongKeys += onDiskLongCount;
                    totals.IndexesWithLongKeys++;
                    totals.LongKeysOnDisk += onDiskLongCount;

                    CorpusIndexScanResult scan = await CompareLongRowIndexAsync(
                        reader,
                        tableName,
                        index,
                        keyColumn,
                        columnMeta,
                        onDiskEntries,
                        maxExamples,
                        ct);

                    totals.LongKeysEncoded += scan.EncodedLongCount;
                    totals.PrefixMatches += scan.PrefixMatchCount;
                    if (columnMeta.ClrType == typeof(string))
                    {
                        totals.TextLongKeysOnDisk += onDiskLongCount;
                    }
                    else if (columnMeta.ClrType == typeof(byte[]))
                    {
                        totals.BinaryLongKeysOnDisk += onDiskLongCount;
                    }
                    else
                    {
                        totals.OtherLongKeysOnDisk += onDiskLongCount;
                    }

                    AppendCorpusIndexReport(
                        fixtureReport,
                        tableName,
                        index,
                        keyColumn,
                        columnMeta,
                        onDiskLongCount,
                        scan);
                }
            }
        }
        catch (Exception ex) when (ex is IOException or InvalidDataException or UnauthorizedAccessException or NotSupportedException)
        {
            fixtureReport.AppendLine(CultureInfo.InvariantCulture, $"_open failed: {ex.GetType().Name}: {ex.Message}_");
        }

        if (fixtureReport.Length == 0)
        {
            return;
        }

        sb.AppendLine(CultureInfo.InvariantCulture, $"## {Path.GetFileName(fixturePath)}");
        sb.AppendLine();
        sb.AppendLine(CultureInfo.InvariantCulture, $"Long indexes: {fixtureLongIndexes}; long keys: {fixtureLongKeys}");
        sb.AppendLine();
        sb.Append(fixtureReport);
        sb.AppendLine();
    }

    private static async Task<CorpusIndexScanResult> CompareLongRowIndexAsync(
        AccessReader reader,
        string tableName,
        IndexMetadata index,
        IndexColumnReference keyColumn,
        ColumnMetadata columnMeta,
        List<IndexEntry> onDiskEntries,
        int maxExamples,
        CancellationToken ct)
    {
        if (columnMeta.ClrType != typeof(string) && columnMeta.ClrType != typeof(byte[]))
        {
            return new CorpusIndexScanResult(0, 0, []);
        }

        DataTable dataTable;
        try
        {
            dataTable = await reader.ReadDataTableAsync(tableName, cancellationToken: ct);
        }
        catch (NotSupportedException)
        {
            return new CorpusIndexScanResult(0, 0, []);
        }

        var encoded = new List<EncodedCorpusKey>(dataTable.Rows.Count);
        foreach (DataRow row in dataTable.Rows)
        {
            object boxed = row[keyColumn.Name];
            object? value = boxed is DBNull ? null : boxed;
            if (value is null && index.IgnoreNulls)
            {
                continue;
            }

            byte[]? key = TryEncodeCorpusValue(value, columnMeta, keyColumn.IsAscending);
            if (key is null)
            {
                continue;
            }

            byte[]? fullKey = value is string text
                ? BuildFullV2010Entry(text, keyColumn.IsAscending, GeneralCodes.Value, GeneralExtCodes.Value)
                : null;
            encoded.Add(new EncodedCorpusKey(value, key, fullKey, DescribeCorpusRowLabel(row)));
        }

        encoded.Sort((left, right) => CompareBytesUnsignedPrefix(left.Key, right.Key));
        if (encoded.Count == 0)
        {
            return new CorpusIndexScanResult(0, 0, []);
        }

        List<IndexEntry> sortedOnDisk = onDiskEntries
            .OrderBy(entry => entry.Key, BytePrefixComparer.Instance)
            .ToList();

        int encodedLongCount = encoded.Count(encodedKey => encodedKey.Key.Length == LongRowEntryLength);
        int prefixMatches = 0;
        var examples = new List<CorpusSuffixExample>();
        var usedEncodedIndexes = new bool[encoded.Count];
        for (int indexPosition = 0; indexPosition < sortedOnDisk.Count; indexPosition++)
        {
            byte[] onDiskKey = sortedOnDisk[indexPosition].Key;
            if (onDiskKey.Length != LongRowEntryLength)
            {
                continue;
            }

            int encodedIndex = FindEncodedPrefixMatch(encoded, usedEncodedIndexes, onDiskKey);
            bool prefixMatch = encodedIndex >= 0;
            if (prefixMatch)
            {
                prefixMatches++;
                usedEncodedIndexes[encodedIndex] = true;
            }

            if (examples.Count < maxExamples)
            {
                EncodedCorpusKey encodedKey = prefixMatch
                    ? encoded[encodedIndex]
                    : encoded[Math.Min(indexPosition, encoded.Count - 1)];
                ushort expectedSuffix = (ushort)((onDiskKey[508] << 8) | onDiskKey[509]);
                ushort actualSuffix = encodedKey.Key.Length >= LongRowEntryLength
                    ? (ushort)((encodedKey.Key[508] << 8) | encodedKey.Key[509])
                    : (ushort)0;
                examples.Add(new CorpusSuffixExample(
                    indexPosition,
                    sortedOnDisk[indexPosition].DataPage,
                    sortedOnDisk[indexPosition].DataRow,
                    prefixMatch,
                    expectedSuffix,
                    actualSuffix,
                    encodedKey.Key.Length,
                    encodedKey.FullKey?.Length,
                    DescribeFullTail(encodedKey.FullKey),
                    encodedKey.RowLabel,
                    DescribeCorpusValue(encodedKey.Value)));
            }
        }

        return new CorpusIndexScanResult(encodedLongCount, prefixMatches, examples);
    }

    private static int FindEncodedPrefixMatch(
        List<EncodedCorpusKey> encoded,
        bool[] usedEncodedIndexes,
        byte[] onDiskKey)
    {
        for (int encodedIndex = 0; encodedIndex < encoded.Count; encodedIndex++)
        {
            if (usedEncodedIndexes[encodedIndex])
            {
                continue;
            }

            byte[] encodedKey = encoded[encodedIndex].Key;
            if (encodedKey.Length >= PrefixMatchLength
                && onDiskKey.AsSpan(0, PrefixMatchLength).SequenceEqual(encodedKey.AsSpan(0, PrefixMatchLength)))
            {
                return encodedIndex;
            }
        }

        return -1;
    }

    private static void AppendCorpusIndexReport(
        StringBuilder sb,
        string tableName,
        IndexMetadata index,
        IndexColumnReference keyColumn,
        ColumnMetadata columnMeta,
        int onDiskLongCount,
        CorpusIndexScanResult scan)
    {
        sb.AppendLine(CultureInfo.InvariantCulture, $"### {tableName}.{index.Name}");
        sb.AppendLine();
        sb.AppendLine(
            CultureInfo.InvariantCulture,
            $"- column: `{keyColumn.Name}` ({columnMeta.TypeName}, CLR `{columnMeta.ClrType.Name}`), ascending={keyColumn.IsAscending}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- on-disk 510-byte keys: {onDiskLongCount}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- encoded 510-byte keys: {scan.EncodedLongCount}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- first-508-byte prefix matches: {scan.PrefixMatchCount}");

        if (scan.Examples.Count == 0)
        {
            sb.AppendLine();
            return;
        }

        sb.AppendLine();
        sb.AppendLine("| Position | Data ptr | Row | Prefix match | Access suffix | Encoder suffix | Encoded len | Full len | Full tail | Value |");
        sb.AppendLine("|---:|---:|---|:---:|:---:|:---:|---:|---:|---|---|");
        foreach (CorpusSuffixExample example in scan.Examples)
        {
            string fullLength = example.FullLength?.ToString(CultureInfo.InvariantCulture) ?? "-";
            string encoderSuffix = example.EncodedLength >= LongRowEntryLength
                ? $"`{example.ActualSuffix:X4}`"
                : "-";
            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"| {example.Position} | {example.DataPage}:{example.DataRow} | {example.RowLabel} | {(example.PrefixMatch ? "yes" : "no")} | `{example.ExpectedSuffix:X4}` | {encoderSuffix} | {example.EncodedLength} | {fullLength} | {example.FullTail} | {example.ValuePreview} |");
        }

        sb.AppendLine();
    }

    private static string BuildCorpusSummary(CorpusScanTotals totals)
    {
        var sb = new StringBuilder();
        sb.AppendLine("## Summary");
        sb.AppendLine();
        sb.AppendLine(CultureInfo.InvariantCulture, $"- Fixtures scanned: {totals.FixturesScanned}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- Indexes with 510-byte keys: {totals.IndexesWithLongKeys}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- On-disk 510-byte keys: {totals.LongKeysOnDisk}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- Text/Memo 510-byte keys: {totals.TextLongKeysOnDisk}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- Binary 510-byte keys: {totals.BinaryLongKeysOnDisk}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- Other 510-byte keys: {totals.OtherLongKeysOnDisk}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- Encoded 510-byte keys: {totals.LongKeysEncoded}");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- First-508-byte prefix matches: {totals.PrefixMatches}");
        sb.AppendLine();
        return sb.ToString();
    }

    private static byte[]? TryEncodeCorpusValue(object? value, ColumnMetadata columnMeta, bool ascending)
    {
        if (columnMeta.ClrType == typeof(string))
        {
            return GeneralTextIndexEncoder.Encode((string?)value, ascending);
        }

        if (columnMeta.ClrType == typeof(byte[]))
        {
            return IndexKeyEncoder.EncodeEntry(0x09, value, ascending);
        }

        return null;
    }

    private static string DescribeCorpusValue(object? value)
    {
        return value switch
        {
            null => "`<null>`",
            byte[] bytes => $"`0x{Convert.ToHexString(bytes.AsSpan(0, Math.Min(bytes.Length, 24)))}{(bytes.Length > 24 ? "..." : string.Empty)}` ({bytes.Length} bytes)",
            string text => $"`{EscapeMarkdown(TruncateForReport(text, 60))}` ({text.Length} chars)",
            _ => $"`{EscapeMarkdown(value.ToString() ?? string.Empty)}`",
        };
    }

    private static string DescribeCorpusRowLabel(DataRow row)
    {
        string label = DescribeCorpusRowLabelValue(row);
        return label == "-" ? "-" : $"`{EscapeMarkdown(label)}`";
    }

    private static string DescribeCorpusRowLabelValue(DataRow row)
    {
        DataColumnCollection columns = row.Table.Columns;
        if (columns.Contains("name"))
        {
            object value = row["name"];
            return value is DBNull ? "-" : Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
        }

        return "-";
    }

    private static string DescribeFullTail(byte[]? fullKey)
    {
        if (fullKey is null || fullKey.Length <= 500)
        {
            return "-";
        }

        int tailLength = Math.Min(fullKey.Length - 500, 32);
        return $"`{Convert.ToHexString(fullKey.AsSpan(500, tailLength))}`";
    }

    private static string TruncateForReport(string value, int maxLength) =>
        value.Length <= maxLength
            ? value
            : value[..maxLength] + "...";

    private static string EscapeMarkdown(string value)
    {
        if (value.AsSpan().IndexOfAny(MarkdownEscapeSearch) < 0)
        {
            return value;
        }

        return value.Replace("`", "'", StringComparison.Ordinal)
            .Replace("|", "\\|", StringComparison.Ordinal)
            .Replace("\r", "\\r", StringComparison.Ordinal)
            .Replace("\n", "\\n", StringComparison.Ordinal);
    }

    private static string BuildDaoLabScript(string labPath, int rowCount)
    {
        string db = PowerShellLiteral(labPath);
        return $$"""
            $ErrorActionPreference = 'Stop'
            $dbPath = {{db}}
            $rowCount = {{rowCount}}
            $alphabet = ' abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_+'

            function New-LabText([int] $seed) {
                $chars = New-Object 'char[]' 360
                for ($position = 0; $position -lt $chars.Length; $position++) {
                    $chars[$position] = 'a'
                }

                if ($seed -ge {{DaoLabAuxMatrixStart}}) {
                    $pair = $seed - {{DaoLabAuxMatrixStart}}
                    $first = [int] [Math]::Floor($pair / $alphabet.Length)
                    $second = [int] ($pair % $alphabet.Length)
                    $chars[12] = [char]0x00C1
                    $chars[25] = [char]0x00ED
                    $chars[86] = '-'
                    $chars[102] = '-'
                    $chars[253] = $alphabet[$first]
                    $chars[254] = $alphabet[$second]
                    return [string]::new($chars)
                }

                if ($seed -ge {{DaoLabPairMatrixStart}}) {
                    $pair = $seed - {{DaoLabPairMatrixStart}}
                    $first = [int] [Math]::Floor($pair / $alphabet.Length)
                    $second = [int] ($pair % $alphabet.Length)
                    $chars[253] = $alphabet[$first]
                    $chars[254] = $alphabet[$second]
                    return [string]::new($chars)
                }

                $group = [Math]::Floor($seed / 64)
                $variant = $seed % 64
                $variantChar = $alphabet[$variant]

                switch ($group) {
                    0 { $chars[253] = $variantChar }
                    1 { $chars[254] = $variantChar }
                    2 { $chars[20] = $variantChar }
                    default {
                        $chars[12] = [char]0x00C1
                        $chars[25] = [char]0x00ED
                        $chars[86] = '-'
                        $chars[102] = '-'
                        $chars[253] = $variantChar
                        if (($variant % 3) -eq 0) { $chars[179] = "`r"[0]; $chars[180] = "`n"[0] }
                    }
                }

                return [string]::new($chars)
            }

            function Write-TableFields([object] $db, [string] $tableName) {
                $td = $db.TableDefs.Item($tableName)
                for ($fieldIndex = 0; $fieldIndex -lt $td.Fields.Count; $fieldIndex++) {
                    $field = $td.Fields.Item($fieldIndex)
                    Write-Output ("field {0}[{1}] name={2} type={3} size={4} required={5} attrs={6}" -f $tableName, $fieldIndex, $field.Name, $field.Type, $field.Size, $field.Required, $field.Attributes)
                }
            }

            function Set-LabFieldValue([object] $field, [int] $seed) {
                if (($field.Attributes -band 16) -ne 0) { return }

                switch ([int] $field.Type) {
                    1 { $field.Value = [byte] ($seed % 255) }
                    2 { $field.Value = [int16] $seed }
                    3 { $field.Value = [int16] $seed }
                    4 { $field.Value = [int32] $seed }
                    5 { $field.Value = [double] $seed }
                    7 { $field.Value = ([datetime] '2000-01-01').AddDays($seed % 365) }
                    8 { $field.Value = [double] $seed }
                    10 { $field.Value = 'lab' + $seed.ToString('000000') }
                    12 { $field.AppendChunk('lab' + $seed.ToString('000000')) }
                    default { $field.Value = 'lab' + $seed.ToString('000000') }
                }
            }

            function Add-LabRows([object] $db, [string] $tableName, [int] $offset) {
                $rs = $db.OpenRecordset($tableName, 2)
                try {
                    for ($seed = 0; $seed -lt $rowCount; $seed++) {
                        $text = [string] (New-LabText $seed)
                        $rs.AddNew()
                        for ($fieldIndex = 0; $fieldIndex -lt $rs.Fields.Count; $fieldIndex++) {
                            $field = $rs.Fields.Item($fieldIndex)
                            if ($field.Name -ieq 'data') { continue }
                            Set-LabFieldValue $field ($seed + $offset + 100000)
                        }

                        $rs.Fields.Item('data').AppendChunk($text)
                        $rs.Update()
                    }
                } finally {
                    $rs.Close()
                }
            }

            $engine = New-Object -ComObject DAO.DBEngine.120
            try {
                $db = $engine.OpenDatabase($dbPath)
                try {
                    Write-TableFields $db 'Table11'
                    Write-TableFields $db 'Table11_desc'
                    Add-LabRows $db 'Table11' 0
                    Add-LabRows $db 'Table11_desc' 1000
                } finally {
                    $db.Close()
                }
            } finally {
                if ($null -ne $engine) {
                    [System.Runtime.InteropServices.Marshal]::ReleaseComObject($engine) | Out-Null
                }

                [GC]::Collect()
                [GC]::WaitForPendingFinalizers()
            }

            Write-Output "inserted=$rowCount per table"
            """;
    }

    private static (int ExitCode, string StdOut, string StdErr) RunPowerShell(
        string powerShellPath,
        string script,
        string scriptPath,
        TimeSpan timeout)
    {
        FormatProbeArtifacts.WriteAllText(scriptPath, script);

        var psi = new ProcessStartInfo(powerShellPath)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
            WorkingDirectory = Path.GetDirectoryName(scriptPath) ?? Environment.CurrentDirectory,
        };
        psi.ArgumentList.Add("-NoProfile");
        psi.ArgumentList.Add("-ExecutionPolicy");
        psi.ArgumentList.Add("Bypass");
        psi.ArgumentList.Add("-File");
        psi.ArgumentList.Add(scriptPath);

        using Process process = Process.Start(psi)
            ?? throw new InvalidOperationException($"Failed to start PowerShell host '{powerShellPath}'.");
        string stdout = process.StandardOutput.ReadToEnd();
        string stderr = process.StandardError.ReadToEnd();
        if (!process.WaitForExit((int)timeout.TotalMilliseconds))
        {
            TryKill(process);
            return (-1, stdout, stderr + $"{Environment.NewLine}[timeout after {timeout.TotalSeconds:N0}s]");
        }

        return (process.ExitCode, stdout, stderr);
    }

    private static void TryKill(Process process)
    {
        try
        {
#if NETSTANDARD2_1
            process.Kill();
#else
            process.Kill(entireProcessTree: true);
#endif
        }
        catch (InvalidOperationException)
        {
        }
        catch (NotSupportedException)
        {
        }
    }

    private static string PowerShellLiteral(string value) =>
        "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";

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

    private static byte[] BuildTrimmedFullV2010Entry(string text, bool ascending)
    {
        int take = Math.Min(text.Length, 255);
        string trimmedText = text.AsSpan(0, take).TrimEnd(' ').ToString();
        return BuildFullV2010Entry(trimmedText, ascending, GeneralCodes.Value, GeneralExtCodes.Value);
    }

    private static byte[][] BuildCandidateByteInputs(byte[] full) =>
    [
        SliceOrEmpty(full, 508),
        SliceOrEmpty(full, 510),
        SliceOrEmpty(full, 508, 3),
        SliceOrEmpty(full, 508, 4),
        SliceOrEmpty(full, 508, 5),
        full.Length >= 508 ? full[..508] : full,
        ZeroSuffixCopy(full),
    ];

    private static string[] BuildCandidateTextInputs(string text) =>
    [
        TextWindow(text, 253, 255),
        TextWindow(text, 254, 255),
        TextWindow(text, 253, text.Length),
        TextWindow(text, 0, 255),
    ];

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
            full.Length > 508 ? full[508..Math.Min(full.Length, 511)] : [],
            full.Length > 508 ? full[508..Math.Min(full.Length, 512)] : [],
            full.Length > 508 ? full[508..Math.Min(full.Length, 513)] : [],
            full.Length >= 508 ? full[..508] : full,
            full.Length >= 508 ? full[1..508] : full,
            selfCheck,
        ];
    }

    private static (byte[] Extras, byte[] Unprint) SplitExtraAndUnprint(byte[] full)
    {
        int relativeEndTextPos = full.Length > 508
            ? full.AsSpan(508).IndexOfAny(EndTextSearch)
            : -1;
        int endTextPos = relativeEndTextPos >= 0 ? 508 + relativeEndTextPos : -1;

        byte[] extras = endTextPos >= 0 && endTextPos + 1 < full.Length
            ? full[(endTextPos + 1)..^1]
            : [];
        byte[] unprint = [];
        if (extras.Length > 3)
        {
            int searchLimit = extras.Length - 2;
            int searchStart = 0;
            while (searchStart < searchLimit)
            {
                int relativeIndex = extras.AsSpan(searchStart, searchLimit - searchStart).IndexOfAny(EndTextSearch);
                if (relativeIndex < 0)
                {
                    break;
                }

                int index = searchStart + relativeIndex;
                if (extras[index + 1] == GeneralLegacyTextIndexEncoder.EndText)
                {
                    unprint = extras[(index + 2)..];
                    extras = extras[..index];
                    break;
                }

                searchStart = index + 1;
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

    private static async Task<List<IndexEntry>> CollectAllLeafEntriesFromRootAsync(
        AccessReader reader,
        IndexLeafPageBuilder.LeafPageLayout layout,
        int pageSize,
        long rootPage,
        CancellationToken ct)
    {
        long current = rootPage;
        for (int depth = 0; depth < 32; depth++)
        {
            byte[] page = await reader.GetRawPageBytesAsync(current, ct);
            byte pageType = page[0];
            if (pageType == Constants.IndexLeafPage.PageTypeLeaf)
            {
                break;
            }

            if (pageType != Constants.IndexLeafPage.PageTypeIntermediate)
            {
                throw new InvalidOperationException(
                    $"Unexpected page_type 0x{pageType:X2} at page {current} (expected 0x03 or 0x04).");
            }

            List<DecodedIntermediateEntry> entries =
                IndexLeafIncremental.DecodeIntermediateEntries(layout, page, pageSize);
            if (entries.Count == 0)
            {
                throw new InvalidOperationException($"Intermediate page {current} has no entries.");
            }

            current = entries[0].ChildPage;
        }

        var result = new List<IndexEntry>();
        long visitGuard = 0;
        while (current != 0)
        {
            if (++visitGuard > 100_000)
            {
                throw new InvalidOperationException("Leaf chain exceeds visit guard; possible cycle.");
            }

            byte[] page = await reader.GetRawPageBytesAsync(current, ct);
            if (page[0] != Constants.IndexLeafPage.PageTypeLeaf)
            {
                throw new InvalidOperationException(
                    $"Expected leaf page (0x04) at page {current}; got 0x{page[0]:X2}.");
            }

            result.AddRange(IndexLeafIncremental.DecodeEntries(layout, page, pageSize));

            (long _, long next, long _) = IndexLeafIncremental.ReadSiblingPointers(layout, page);
            current = next;
        }

        return result;
    }

    private static async Task<List<LeafEntryDetail>> CollectDetailedLeafEntriesFromRootAsync(
        AccessReader reader,
        IndexLeafPageBuilder.LeafPageLayout layout,
        int pageSize,
        long rootPage,
        CancellationToken ct)
    {
        long current = rootPage;
        for (int depth = 0; depth < 32; depth++)
        {
            byte[] page = await reader.GetRawPageBytesAsync(current, ct);
            byte pageType = page[0];
            if (pageType == Constants.IndexLeafPage.PageTypeLeaf)
            {
                break;
            }

            if (pageType != Constants.IndexLeafPage.PageTypeIntermediate)
            {
                throw new InvalidOperationException(
                    $"Unexpected page_type 0x{pageType:X2} at page {current} (expected 0x03 or 0x04).");
            }

            List<DecodedIntermediateEntry> entries =
                IndexLeafIncremental.DecodeIntermediateEntries(layout, page, pageSize);
            if (entries.Count == 0)
            {
                throw new InvalidOperationException($"Intermediate page {current} has no entries.");
            }

            current = entries[0].ChildPage;
        }

        var result = new List<LeafEntryDetail>();
        int position = 0;
        long visitGuard = 0;
        while (current != 0)
        {
            if (++visitGuard > 100_000)
            {
                throw new InvalidOperationException("Leaf chain exceeds visit guard; possible cycle.");
            }

            byte[] page = await reader.GetRawPageBytesAsync(current, ct);
            if (page[0] != Constants.IndexLeafPage.PageTypeLeaf)
            {
                throw new InvalidOperationException(
                    $"Expected leaf page (0x04) at page {current}; got 0x{page[0]:X2}.");
            }

            result.AddRange(DecodeLeafEntryDetails(layout, page, pageSize, current, ref position));

            (long _, long next, long _) = IndexLeafIncremental.ReadSiblingPointers(layout, page);
            current = next;
        }

        return result;
    }

    private static List<LeafEntryDetail> DecodeLeafEntryDetails(
        IndexLeafPageBuilder.LeafPageLayout layout,
        byte[] page,
        int pageSize,
        long leafPage,
        ref int position)
    {
        var result = new List<LeafEntryDetail>();
        int pref = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(layout.PrefLenOffset, 2));
        int freeSpace = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(2, 2));
        int payloadEnd = pageSize - freeSpace;
        if (payloadEnd <= layout.FirstEntryOffset)
        {
            return result;
        }

        byte[]? sharedPrefix = null;
        int entryStart = layout.FirstEntryOffset;
        int entryIndex = 0;
        bool isFirst = true;
        while (entryStart < payloadEnd)
        {
            int next = NextEntryStartForProbe(layout, page, payloadEnd, entryStart);
            int entryEnd = next < 0 ? payloadEnd : next;
            int totalLength = entryEnd - entryStart;
            int rawKeyLength = totalLength - 4;
            if (rawKeyLength < 0)
            {
                break;
            }

            byte[] canonical;
            if (isFirst)
            {
                canonical = new byte[rawKeyLength];
                Buffer.BlockCopy(page, entryStart, canonical, 0, rawKeyLength);
                if (pref > 0 && rawKeyLength >= pref)
                {
                    sharedPrefix = canonical[..pref];
                }
            }
            else
            {
                canonical = new byte[pref + rawKeyLength];
                if (pref > 0 && sharedPrefix is not null)
                {
                    Buffer.BlockCopy(sharedPrefix, 0, canonical, 0, pref);
                }

                Buffer.BlockCopy(page, entryStart, canonical, pref, rawKeyLength);
            }

            int dataPointerOffset = entryStart + rawKeyLength;
            long dataPage = JetTypeInfo.ReadUInt24BigEndian(page.AsSpan(dataPointerOffset, 3));
            byte dataRow = page[dataPointerOffset + 3];
            result.Add(new LeafEntryDetail(
                new IndexEntry(canonical, dataPage, dataRow),
                position++,
                leafPage,
                entryIndex++,
                pref,
                rawKeyLength,
                entryStart));

            isFirst = false;
            if (next < 0)
            {
                break;
            }

            entryStart = next;
        }

        return result;
    }

    private static async Task<Dictionary<long, PhysicalRowSnapshot>> BuildPhysicalRowSnapshotMapAsync(
        AccessReader reader,
        string tableName,
        CancellationToken ct)
    {
        DataTable dataTable = await reader.ReadDataTableAsync(tableName, cancellationToken: ct);
        CatalogEntry catalogEntry = await reader.GetCatalogEntryAsync(tableName, ct)
            ?? throw new InvalidOperationException($"Table '{tableName}' was not found in the catalog.");
        List<RowLocation> locations = await CollectPhysicalRowLocationsAsync(reader, catalogEntry.TDefPage, ct);
        if (locations.Count != dataTable.Rows.Count)
        {
            throw new InvalidOperationException(
                $"Physical row count mismatch for {tableName}: locations={locations.Count}, rows={dataTable.Rows.Count}.");
        }

        var result = new Dictionary<long, PhysicalRowSnapshot>(locations.Count);
        for (int rowIndex = 0; rowIndex < locations.Count; rowIndex++)
        {
            RowLocation location = locations[rowIndex];
            DataRow row = dataTable.Rows[rowIndex];
            object boxed = row["data"];
            object? value = boxed is DBNull ? null : boxed;
            result[EncodeDataPointer(location.PageNumber, (byte)location.RowIndex)] =
                new PhysicalRowSnapshot(DescribeCorpusRowLabelValue(row), value);
        }

        return result;
    }

    private static async Task<List<RowLocation>> CollectPhysicalRowLocationsAsync(
        AccessReader reader,
        long tdefPage,
        CancellationToken ct)
    {
        if (string.IsNullOrEmpty(reader.HostDatabasePath))
        {
            throw new InvalidOperationException("Physical row mapping requires a file-backed AccessReader.");
        }

        long totalPages = new FileInfo(reader.HostDatabasePath).Length / reader.PageSize;
        var result = new List<RowLocation>();
        for (long pageNumber = 3; pageNumber < totalPages; pageNumber++)
        {
            byte[] page = await reader.GetRawPageBytesAsync(pageNumber, ct);
            if (page[0] != 0x01)
            {
                continue;
            }

            long owner = BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(reader._dataPage.TDefOff, 4));
            if (owner != tdefPage)
            {
                continue;
            }

            foreach (RowLocation location in reader.EnumerateLiveRowLocations(pageNumber, page))
            {
                if (location.RowSize >= reader._rowSz.NumCols)
                {
                    result.Add(location);
                }
            }
        }

        return result;
    }

    private static long EncodeDataPointer(long page, byte row) => (page << 8) | row;

    private static async Task<List<RawLeafPageSummary>> CollectRawLeafPageSummariesAsync(
        AccessReader reader,
        IndexLeafPageBuilder.LeafPageLayout layout,
        int pageSize,
        long rootPage,
        CancellationToken ct)
    {
        long current = rootPage;
        for (int depth = 0; depth < 32; depth++)
        {
            byte[] page = await reader.GetRawPageBytesAsync(current, ct);
            byte pageType = page[0];
            if (pageType == Constants.IndexLeafPage.PageTypeLeaf)
            {
                break;
            }

            if (pageType != Constants.IndexLeafPage.PageTypeIntermediate)
            {
                throw new InvalidOperationException(
                    $"Unexpected page_type 0x{pageType:X2} at page {current} (expected 0x03 or 0x04).");
            }

            List<DecodedIntermediateEntry> entries =
                IndexLeafIncremental.DecodeIntermediateEntries(layout, page, pageSize);
            if (entries.Count == 0)
            {
                throw new InvalidOperationException($"Intermediate page {current} has no entries.");
            }

            current = entries[0].ChildPage;
        }

        var result = new List<RawLeafPageSummary>();
        long visitGuard = 0;
        while (current != 0)
        {
            if (++visitGuard > 100_000)
            {
                throw new InvalidOperationException("Leaf chain exceeds visit guard; possible cycle.");
            }

            byte[] page = await reader.GetRawPageBytesAsync(current, ct);
            if (page[0] != Constants.IndexLeafPage.PageTypeLeaf)
            {
                throw new InvalidOperationException(
                    $"Expected leaf page (0x04) at page {current}; got 0x{page[0]:X2}.");
            }

            result.Add(SummarizeRawLeafPage(layout, page, pageSize, current));

            (long _, long next, long _) = IndexLeafIncremental.ReadSiblingPointers(layout, page);
            current = next;
        }

        return result;
    }

    private static RawLeafPageSummary SummarizeRawLeafPage(
        IndexLeafPageBuilder.LeafPageLayout layout,
        byte[] page,
        int pageSize,
        long pageNumber)
    {
        int prefixLength = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(layout.PrefLenOffset, 2));
        int freeSpace = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(2, 2));
        int payloadEnd = pageSize - freeSpace;

        int entryCount = 0;
        int longEntryCount = 0;
        int? firstLongRawKeyLength = null;
        string firstLongRawKeyTail = "-";
        ushort? firstLongDecodedSuffix = null;

        if (payloadEnd <= layout.FirstEntryOffset)
        {
            return new RawLeafPageSummary(pageNumber, prefixLength, payloadEnd, 0, 0, null, "-", null);
        }

        int entryStart = layout.FirstEntryOffset;
        bool isFirst = true;
        while (entryStart < payloadEnd)
        {
            int next = NextEntryStartForProbe(layout, page, payloadEnd, entryStart);
            int entryEnd = next < 0 ? payloadEnd : next;
            int totalLength = entryEnd - entryStart;
            int rawKeyLength = totalLength - 4;
            if (rawKeyLength < 0)
            {
                break;
            }

            int decodedKeyLength = isFirst ? rawKeyLength : prefixLength + rawKeyLength;
            if (decodedKeyLength == LongRowEntryLength)
            {
                longEntryCount++;
                if (!firstLongRawKeyLength.HasValue)
                {
                    firstLongRawKeyLength = rawKeyLength;
                    firstLongRawKeyTail = FormatTail(page.AsSpan(entryStart, rawKeyLength));
                    if (rawKeyLength >= 2)
                    {
                        int suffixOffset = entryStart + rawKeyLength - 2;
                        firstLongDecodedSuffix = (ushort)((page[suffixOffset] << 8) | page[suffixOffset + 1]);
                    }
                }
            }

            entryCount++;
            isFirst = false;
            if (next < 0)
            {
                break;
            }

            entryStart = next;
        }

        return new RawLeafPageSummary(
            pageNumber,
            prefixLength,
            payloadEnd,
            entryCount,
            longEntryCount,
            firstLongRawKeyLength,
            firstLongRawKeyTail,
            firstLongDecodedSuffix);
    }

    private static int NextEntryStartForProbe(
        IndexLeafPageBuilder.LeafPageLayout layout,
        byte[] page,
        int payloadEnd,
        int currentStart)
    {
        int searchStart = currentStart - layout.FirstEntryOffset + 1;
        for (int bit = searchStart; bit < payloadEnd - layout.FirstEntryOffset; bit++)
        {
            int byteOffset = layout.BitmaskOffset + (bit / 8);
            if (byteOffset >= layout.FirstEntryOffset)
            {
                return -1;
            }

            if ((page[byteOffset] & (1 << (bit % 8))) != 0)
            {
                int candidate = layout.FirstEntryOffset + bit;
                return candidate < payloadEnd ? candidate : -1;
            }
        }

        return -1;
    }

    private static string FormatTail(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty)
        {
            return "-";
        }

        int length = Math.Min(bytes.Length, 16);
        return $"`{Convert.ToHexString(bytes[^length..])}`";
    }

    private static int CompareBytesUnsignedPrefix(byte[] left, byte[] right)
    {
        int prefixLength = Math.Min(Math.Min(left.Length, right.Length), PrefixMatchLength);
        for (int byteIndex = 0; byteIndex < prefixLength; byteIndex++)
        {
            int diff = left[byteIndex] - right[byteIndex];
            if (diff != 0)
            {
                return diff;
            }
        }

        int minLength = Math.Min(left.Length, right.Length);
        for (int byteIndex = prefixLength; byteIndex < minLength; byteIndex++)
        {
            int diff = left[byteIndex] - right[byteIndex];
            if (diff != 0)
            {
                return diff;
            }
        }

        return left.Length - right.Length;
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
        await FormatProbeArtifacts.WriteAllTextAsync(outFile, sb.ToString());
        Console.WriteLine($"Wrote {outFile}");
    }

    private static string InlineHex(byte[]? bytes)
        => bytes is null ? "(none)" : Convert.ToHexString(bytes);

    private readonly record struct RowData(int RowIndex, ushort ExpectedSuffix, byte[] Full, string Text);

    private readonly record struct ConstraintSet(string Label, byte[][] Inputs, ushort Expected);

    private readonly record struct SuffixPatternTable(
        string TableName,
        int SeedBase,
        IndexMetadata Index,
        bool Ascending,
        IndexLeafPageBuilder.LeafPageLayout Layout,
        int ReaderPageSize,
        IReadOnlyList<SuffixPatternRow> Rows,
        IReadOnlyList<RawLeafPageSummary> RawLeafPages);

    private readonly record struct SuffixPatternRow(
        string RowLabel,
        int? Seed,
        int Position,
        long DataPage,
        byte DataRow,
        ushort AccessSuffix,
        ushort EncoderSuffix,
        int? FullLength,
        string FullTail,
        bool PrefixMatch,
        long LeafPage,
        int LeafEntryIndex,
        int PrefixLength,
        int RawKeyLength,
        int EntryStart,
        byte[] FullKey,
        byte[] TrimmedFullKey,
        string? Text);

    private readonly record struct LeafEntryDetail(
        IndexEntry Entry,
        int Position,
        long LeafPage,
        int EntryIndex,
        int PrefixLength,
        int RawKeyLength,
        int EntryStart);

    private readonly record struct PhysicalRowSnapshot(string RowLabel, object? Value);

    private sealed class SuffixCandidateContext
    {
        private Dictionary<LcMapCacheKey, byte[]> lcMapHashBytes = [];
        private Dictionary<LcMapCacheKey, byte[]> lcMapSortKeyBytes = [];
        private byte[][]? inputCandidates;

        public SuffixCandidateContext(SuffixPatternRow row)
        {
            Row = row;
            FullKey = row.FullKey;
            ByteInputs = BuildCandidateByteInputs(row.FullKey);
            TextInputs = BuildCandidateTextInputs(row.Text!);
        }

        public SuffixPatternRow Row { get; }

        public byte[] FullKey { get; }

        public byte[][] ByteInputs { get; }

        public string[] TextInputs { get; }

        public byte[][] GetInputCandidates(Encoding cp1252) =>
            inputCandidates ??= BuildInputCandidates(FullKey, Row.Text!, cp1252);

        public byte[] GetLcMapHashBytes(string localeName, uint flags, string value)
        {
            var key = new LcMapCacheKey(localeName, flags, value);
            if (!lcMapHashBytes.TryGetValue(key, out byte[]? bytes))
            {
                bytes = LcMapHashBytes(localeName, flags, value);
                lcMapHashBytes.Add(key, bytes);
            }

            return bytes;
        }

        public byte[] GetLcMapSortKeyBytes(string localeName, uint flags, string value)
        {
            var key = new LcMapCacheKey(localeName, flags, value);
            if (!lcMapSortKeyBytes.TryGetValue(key, out byte[]? bytes))
            {
                bytes = LcMapSortKeyBytes(localeName, flags, value);
                lcMapSortKeyBytes.Add(key, bytes);
            }

            return bytes;
        }
    }

    private readonly record struct LcMapCacheKey(string LocaleName, uint Flags, string Value);

    private readonly record struct CandidateRule(string Name, Func<SuffixCandidateContext, ushort?> Compute);

    private readonly record struct CandidateScore(
        string Name,
        int Evaluated,
        int Exact,
        int BestXorCount,
        ushort BestXorConstant,
        int BestAddCount,
        ushort BestAddConstant);

    private readonly record struct RollingConstraint(byte[] Input, ushort Target);

    private readonly record struct RollingPolynomialHit(ushort Multiplier, ushort Seed);

    private readonly record struct Crc16AffineHit(ushort Polynomial, ushort XorConstant, bool RefIn, bool RefOut);

    private readonly record struct RawLeafPageSummary(
        long PageNumber,
        int PrefixLength,
        int PayloadEnd,
        int EntryCount,
        int LongEntryCount,
        int? FirstLongRawKeyLength,
        string FirstLongRawKeyTail,
        ushort? FirstLongDecodedSuffix);

    private sealed class CorpusScanTotals
    {
        public int FixturesScanned { get; set; }

        public int IndexesWithLongKeys { get; set; }

        public int LongKeysOnDisk { get; set; }

        public int TextLongKeysOnDisk { get; set; }

        public int BinaryLongKeysOnDisk { get; set; }

        public int OtherLongKeysOnDisk { get; set; }

        public int LongKeysEncoded { get; set; }

        public int PrefixMatches { get; set; }
    }

    private readonly record struct EncodedCorpusKey(object? Value, byte[] Key, byte[]? FullKey, string RowLabel);

    private readonly record struct CorpusIndexScanResult(
        int EncodedLongCount,
        int PrefixMatchCount,
        IReadOnlyList<CorpusSuffixExample> Examples);

    private readonly record struct CorpusSuffixExample(
        int Position,
        long DataPage,
        byte DataRow,
        bool PrefixMatch,
        ushort ExpectedSuffix,
        ushort ActualSuffix,
        int EncodedLength,
        int? FullLength,
        string FullTail,
        string RowLabel,
        string ValuePreview);

    private sealed class CountAccumulator
    {
        private readonly int[] counts = new int[ushort.MaxValue + 1];
        private readonly List<ushort> touched = [];

        public void Increment(ushort key)
        {
            if (counts[key] == 0)
            {
                touched.Add(key);
            }

            counts[key]++;
        }

        public (ushort Key, int Count) Best()
        {
            ushort bestKey = 0;
            int bestCount = 0;
            foreach (ushort key in touched)
            {
                int count = counts[key];
                if (count > bestCount)
                {
                    bestKey = key;
                    bestCount = count;
                }
            }

            return (bestKey, bestCount);
        }

        public void Clear()
        {
            foreach (ushort key in touched)
            {
                counts[key] = 0;
            }

            touched.Clear();
        }
    }

    private sealed class BytePrefixComparer : IComparer<byte[]>
    {
        public static readonly BytePrefixComparer Instance = new();

        public int Compare(byte[]? left, byte[]? right)
        {
            if (ReferenceEquals(left, right))
            {
                return 0;
            }

            if (left is null)
            {
                return -1;
            }

            if (right is null)
            {
                return 1;
            }

            return CompareBytesUnsignedPrefix(left, right);
        }
    }
}
