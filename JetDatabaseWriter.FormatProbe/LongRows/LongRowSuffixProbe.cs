// One-shot research probes for the unresolved V2010 long-row 2-byte suffix.
//
// Usage:
//   dotnet run --project JetDatabaseWriter.FormatProbe -- long-row-suffix
//   dotnet run --project JetDatabaseWriter.FormatProbe -- long-row-crc-sweep
//   dotnet run --project JetDatabaseWriter.FormatProbe -- long-row-corpus
//   dotnet run --project JetDatabaseWriter.FormatProbe -- long-row-dao-lab
//   dotnet run --project JetDatabaseWriter.FormatProbe -- long-row-dao-tables

namespace JetDatabaseWriter.FormatProbe.LongRows;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.FormatProbe;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using static JetDatabaseWriter.Enums.ColumnType;

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
    private const int DaoLabRow12MatrixStart = DaoLabAuxMatrixStart + DaoLabAuxMatrixRowCount;
    private const int DaoLabRow12MatrixRowCount = DaoLabAlphabetLength * DaoLabAlphabetLength;
    private const int DaoLabTrailingSpaceMatrixStart = DaoLabRow12MatrixStart + DaoLabRow12MatrixRowCount;
    private const int DaoLabTrailingSpaceMatrixRowCount = DaoLabAlphabetLength * DaoLabAlphabetLength;
    private const int DaoLabRow10MatrixStart = DaoLabTrailingSpaceMatrixStart + DaoLabTrailingSpaceMatrixRowCount;
    private const int DaoLabRow10MatrixRowCount = DaoLabAlphabetLength * DaoLabAlphabetLength;
    private const int DaoLabRow11MatrixStart = DaoLabRow10MatrixStart + DaoLabRow10MatrixRowCount;
    private const int DaoLabRow11MatrixRowCount = DaoLabAlphabetLength * DaoLabAlphabetLength;
    private const int DaoLabDoubleSpaceSweepStart = DaoLabRow11MatrixStart + DaoLabRow11MatrixRowCount;
    private const int DaoLabDoubleSpaceSweepContextCount = 4;
    private const int DaoLabDoubleSpaceSweepRowCount = DaoLabDoubleSpaceSweepContextCount * DaoLabAlphabetLength;
    private const int DaoLabTemplateSampleStart = DaoLabDoubleSpaceSweepStart + DaoLabDoubleSpaceSweepRowCount;
    private const int DaoLabTemplateSampleRowCount = 12;
    private const int DaoLabRowCount = DaoLabTemplateSampleStart + DaoLabTemplateSampleRowCount;
    private const string DaoLabAlphabet = " abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_+";
    private const int AuxInputCandidateIndex = 10;
    private const int CrcDerivedInitSolverMaxSeedBytes = 192;

    private static readonly CompareInfo EnUsCompareInfo = CultureInfo.GetCultureInfo("en-US").CompareInfo;

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
        "full[503..511]",
        "full[500..511]",
        "full[503..521]",
        "full[508..511]",
        "full[508..512]",
        "full[508..513]",
        "full[..508]",
        "full[1..508]",
        "full[..510] suffix zeroed",
        "full[0..]",
        "full[1..]",
        "full[^2..]",
    ];

    private static readonly int[] RollingInputIndexes = [0, 1, 2, 8, 9, 10, 11, 12, 13, 14, 15, 16];

    private static readonly SearchValues<char> MarkdownEscapeSearch = SearchValues.Create("`|\r\n");

    private static readonly SearchValues<byte> EndTextSearch = SearchValues.Create([GeneralLegacyTextIndexEncoder.EndText]);

    private static readonly Encoding Cp1252Encoding = Encoding.GetEncoding(1252);

    private static readonly Lazy<List<CandidateRule>> SuffixCandidateRules = new(BuildSuffixCandidateRules);

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
        sb.AppendLine("This mode enumerates the full CRC-16 search space with table-driven CRCs. The 2026-05-26 local Debug run took about 14 seconds.")
            .AppendLine();

        await DumpV2010CrcFullSweepAsync(GetV2010Fixture(fixturesDir), sb, CancellationToken.None);
        await WriteOutputAsync(outFile, sb);
        return 0;
    }

    public static async Task<int> RunCorpusScanAsync(string fixturesDir, string outFile)
    {
        var sb = new StringBuilder();
        AppendHeader(sb, "V2010 long-row corpus scan", "long-row-corpus");
        sb.AppendLine("Scans every Jackcess V2010 fixture for single-column index leaf keys exactly 510 bytes long.")
            .AppendLine("For Text/Memo and Binary columns, the probe re-encodes table values and reports V2010 long-row prefix/suffix parity with Access.")
            .AppendLine();
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
        sb.AppendLine("Copies the V2010 index-code fixture, asks DAO/ACE to append generated long strings to the existing long-row stress tables, then scans the result for 510-byte keys.")
            .AppendLine();

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
            TimeSpan.FromMinutes(15));

        sb.AppendLine("## DAO authoring")
            .AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"- PowerShell host: `{hostProbe.HostPath}`")
            .AppendLine(CultureInfo.InvariantCulture, $"- Lab database: `{labPath}`")
            .AppendLine(CultureInfo.InvariantCulture, $"- Script: `{scriptPath}`")
            .AppendLine(CultureInfo.InvariantCulture, $"- Requested rows per table: {DaoLabRowCount}")
            .AppendLine(CultureInfo.InvariantCulture, $"- Exit code: {exitCode}");
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

    public static async Task<int> RunDaoTableExportAsync(string outFile, string probeDir)
    {
        var sb = new StringBuilder();
        AppendHeader(sb, "V2010 long-row DAO suffix table export", "long-row-dao-tables");
        sb.AppendLine("Reads the most recent DAO-authored long-row lab database and emits compact contribution tables for the production encoder.")
            .AppendLine();

        string? labPath = FindLatestDaoLabDatabase(probeDir);
        if (labPath is null)
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"No DAO lab database was found under `{probeDir}`.");
            await WriteOutputAsync(outFile, sb);
            return 1;
        }

        sb.AppendLine(CultureInfo.InvariantCulture, $"Lab database: `{labPath}`")
            .AppendLine();

        await using AccessReader reader = await AccessReader.OpenAsync(
            labPath,
            new AccessReaderOptions { UseLockFile = false },
            CancellationToken.None);

        foreach ((string tableName, int seedBase) in new[] { ("Table11", 100000), ("Table11_desc", 101000) })
        {
            SuffixPatternTable table = await BuildSuffixPatternTableAsync(
                reader,
                tableName,
                seedBase,
                CancellationToken.None);
            AppendDaoSuffixTableExport(sb, table);
        }

        await WriteOutputAsync(outFile, sb);
        return 0;
    }

    private static string? FindLatestDaoLabDatabase(string probeDir)
    {
        if (!Directory.Exists(probeDir))
        {
            return null;
        }

        return Directory.EnumerateDirectories(probeDir, FormatProbeArtifacts.FilePrefix + "long-row-dao-lab-*")
            .Select(directory => new
            {
                Directory = directory,
                LastWriteTime = Directory.GetLastWriteTimeUtc(directory),
                DatabasePath = Path.Combine(directory, FormatProbeArtifacts.FilePrefix + "long-row-dao-lab.accdb"),
            })
            .Where(candidate => File.Exists(candidate.DatabasePath))
            .OrderByDescending(candidate => candidate.LastWriteTime)
            .Select(candidate => candidate.DatabasePath)
            .FirstOrDefault();
    }

    private static void AppendDaoSuffixTableExport(StringBuilder sb, SuffixPatternTable table)
    {
        sb.AppendLine(CultureInfo.InvariantCulture, $"## {table.TableName}")
            .AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"- ascending: {table.Ascending}")
            .AppendLine();

        foreach ((string contextName, int matrixStart, int doubleSpaceContext) in new[]
        {
            ("Plain", DaoLabPairMatrixStart, 0),
            ("Auxiliary", DaoLabAuxMatrixStart, -1),
            ("Row12", DaoLabRow12MatrixStart, 3),
            ("Row10", DaoLabRow10MatrixStart, 1),
            ("Row11", DaoLabRow11MatrixStart, 2),
        })
        {
            AppendDaoSuffixTableContextExport(sb, table, contextName, matrixStart, doubleSpaceContext);
        }
    }

    private static void AppendDaoSuffixTableContextExport(
        StringBuilder sb,
        SuffixPatternTable table,
        string contextName,
        int matrixStart,
        int doubleSpaceContext)
    {
        var matrixRows = table.Rows
            .Where(row => row.Seed is not null && row.Seed.Value >= matrixStart && row.Seed.Value < matrixStart + DaoLabPairMatrixRowCount)
            .OrderBy(row => row.Seed)
            .ToList();
        if (!TryBuildMatrix(table, matrixStart, out ushort[] suffixes, out bool[] present))
        {
            return;
        }

        const int size = DaoLabAlphabetLength;
        int baseIndex = DaoLabAlphabet.IndexOf('a', StringComparison.Ordinal);
        int spaceIndex = DaoLabAlphabet.IndexOf(' ', StringComparison.Ordinal);
        int baseOffset = (baseIndex * size) + baseIndex;
        if (baseIndex < 0 || spaceIndex < 0 || !present[baseOffset])
        {
            return;
        }

        ushort baseValue = suffixes[baseOffset];
        var rowContributions = new ushort[size];
        var columnContributions = new ushort[size];
        var shiftedSuffixes = new ushort[size];
        var rowPresent = new bool[size];
        var columnPresent = new bool[size];
        var shiftedPresent = new bool[size];

        for (int index = 0; index < size; index++)
        {
            int rowOffset = (index * size) + baseIndex;
            if (present[rowOffset])
            {
                rowContributions[index] = (ushort)(suffixes[rowOffset] ^ baseValue);
                rowPresent[index] = true;
            }

            int columnOffset = (baseIndex * size) + index;
            if (present[columnOffset])
            {
                columnContributions[index] = (ushort)(suffixes[columnOffset] ^ baseValue);
                columnPresent[index] = true;
            }

            int shiftedOffset = (index * size) + spaceIndex;
            if (present[shiftedOffset])
            {
                shiftedSuffixes[index] = suffixes[shiftedOffset];
                shiftedPresent[index] = true;
            }
        }

        int normalMatches = 0;
        int normalTotal = 0;
        int shiftedMatches = 0;
        int shiftedTotal = 0;
        for (int first = 0; first < size; first++)
        {
            for (int second = 0; second < size; second++)
            {
                int offset = (first * size) + second;
                if (!present[offset])
                {
                    continue;
                }

                if (second == spaceIndex)
                {
                    if (shiftedPresent[first])
                    {
                        shiftedTotal++;
                        if (shiftedSuffixes[first] == suffixes[offset])
                        {
                            shiftedMatches++;
                        }
                    }

                    continue;
                }

                if (!rowPresent[first] || !columnPresent[second])
                {
                    continue;
                }

                normalTotal++;
                ushort predicted = (ushort)(baseValue ^ rowContributions[first] ^ columnContributions[second]);
                if (predicted == suffixes[offset])
                {
                    normalMatches++;
                }
            }
        }

        ushort? tripleSpaceSuffix = TryGetDoubleSpaceSuffix(table, doubleSpaceContext, spaceIndex);
        int matrixMismatches = matrixRows.Count(row => row.AccessSuffix != row.EncoderSuffix);
        List<SuffixPatternRow> doubleSpaceRows = GetDoubleSpaceRows(table, doubleSpaceContext);
        int doubleSpaceMismatches = doubleSpaceRows.Count(row => row.AccessSuffix != row.EncoderSuffix);

        sb.AppendLine(CultureInfo.InvariantCulture, $"### {contextName}")
            .AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"- matrix seeds: {matrixStart}-{matrixStart + DaoLabPairMatrixRowCount - 1}")
            .AppendLine(CultureInfo.InvariantCulture, $"- present rows: {present.Count(value => value)}/{present.Length}")
            .AppendLine(CultureInfo.InvariantCulture, $"- normal fit: {normalMatches}/{normalTotal}")
            .AppendLine(CultureInfo.InvariantCulture, $"- boundary-space fit: {shiftedMatches}/{shiftedTotal}")
            .AppendLine(CultureInfo.InvariantCulture, $"- production encoder matrix mismatches: {matrixMismatches}/{matrixRows.Count}");
        if (doubleSpaceContext >= 0)
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"- production encoder double-space mismatches: {doubleSpaceMismatches}/{doubleSpaceRows.Count}");
        }

        sb.AppendLine(CultureInfo.InvariantCulture, $"- base suffix: `{baseValue:X4}`")
            .AppendLine(CultureInfo.InvariantCulture, $"- shifted `SP` suffix present: {shiftedPresent[spaceIndex]}")
            .AppendLine(CultureInfo.InvariantCulture, $"- triple-space suffix: `{FormatNullableHex(tripleSpaceSuffix)}`")
            .AppendLine();
        AppendDaoRemainderSummary(sb, matrixRows, matrixStart);
        sb.AppendLine()
            .AppendLine("```csharp");
        string prefix = table.Ascending ? "Ascending" : "Descending";
        sb.AppendLine(CultureInfo.InvariantCulture, $"// {prefix} {contextName}")
            .AppendLine(CultureInfo.InvariantCulture, $"Base = 0x{baseValue:X4}, TripleSpace = {FormatNullableCSharpHex(tripleSpaceSuffix)}");
        AppendCSharpUShortArray(sb, "Row", rowContributions);
        AppendCSharpUShortArray(sb, "Column", columnContributions);
        AppendCSharpUShortArray(sb, "BoundarySpace", shiftedSuffixes);
        sb.AppendLine("```")
            .AppendLine();
    }

    private static ushort? TryGetDoubleSpaceSuffix(
        SuffixPatternTable table,
        int doubleSpaceContext,
        int alphabetIndex)
    {
        if (doubleSpaceContext < 0)
        {
            return null;
        }

        int seed = DaoLabDoubleSpaceSweepStart + (doubleSpaceContext * DaoLabAlphabetLength) + alphabetIndex;
        return table.Rows.FirstOrDefault(row => row.Seed == seed) is { Seed: not null } row
            ? row.AccessSuffix
            : null;
    }

    private static List<SuffixPatternRow> GetDoubleSpaceRows(SuffixPatternTable table, int doubleSpaceContext)
    {
        if (doubleSpaceContext < 0)
        {
            return [];
        }

        int firstSeed = DaoLabDoubleSpaceSweepStart + (doubleSpaceContext * DaoLabAlphabetLength);
        return table.Rows
            .Where(row => row.Seed is not null && row.Seed.Value >= firstSeed && row.Seed.Value < firstSeed + DaoLabAlphabetLength)
            .OrderBy(row => row.Seed)
            .ToList();
    }

    private static void AppendDaoRemainderSummary(StringBuilder sb, List<SuffixPatternRow> rows, int matrixStart)
    {
        sb.AppendLine("Full-entry remainder signatures after byte 510:")
            .AppendLine()
            .AppendLine("| Full length | Remainder | Rows | Examples | First suffixes |")
            .AppendLine("|---:|---|---:|---|---|");

        foreach (IGrouping<string, SuffixPatternRow>? group in rows
            .GroupBy(row => string.Concat(
                row.FullLength?.ToString(CultureInfo.InvariantCulture) ?? "-",
                ":",
                ToHexStringOrEmpty(row.FullKey, LongRowEntryLength, row.FullKey.Length - LongRowEntryLength)))
            .OrderByDescending(group => group.Count())
            .ThenBy(group => group.Key, StringComparer.Ordinal)
            .Take(12))
        {
            string[] keyParts = group.Key.Split(':', 2);
            string examples = string.Join(" ", group.Take(6).Select(row => $"`{EscapeMarkdown(GetMatrixPairText(row, matrixStart))}`"));
            string suffixes = string.Join(" ", group.Take(6).Select(row => $"`{row.AccessSuffix:X4}`"));
            sb.AppendLine(CultureInfo.InvariantCulture, $"| {keyParts[0]} | `{keyParts[1]}` | {group.Count()} | {examples} | {suffixes} |");
        }
    }

    private static void AppendCSharpUShortArray(StringBuilder sb, string name, ushort[] values)
    {
        sb.AppendLine(CultureInfo.InvariantCulture, $"{name} = [");
        for (int offset = 0; offset < values.Length; offset += 13)
        {
            ushort[] row = values.Skip(offset).Take(13).ToArray();
            sb.Append("    ")
                .AppendJoin(", ", row.Select(value => string.Create(CultureInfo.InvariantCulture, $"0x{value:X4}")))
                .AppendLine(offset + row.Length < values.Length ? "," : string.Empty);
        }

        sb.AppendLine("]; ");
    }

    private static string FormatNullableHex(ushort? value) =>
        value is ushort concreteValue
            ? concreteValue.ToString("X4", CultureInfo.InvariantCulture)
            : "null";

    private static string FormatNullableCSharpHex(ushort? value) =>
        value is ushort concreteValue
            ? string.Create(CultureInfo.InvariantCulture, $"0x{concreteValue:X4}")
            : "null";

    private static async Task AppendDaoLabPatternSummaryAsync(string labPath, StringBuilder sb, CancellationToken ct)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(
            labPath,
            new AccessReaderOptions { UseLockFile = false },
            ct);

        sb.AppendLine("## DAO lab suffix pattern summary")
            .AppendLine()
            .AppendLine("Groups are synthetic text families emitted by `New-LabText`: seed 0-63 varies char[253], 64-127 varies char[254], 128-191 varies char[20], 192-255 adds international/unprintable characters plus optional CR/LF, then later ranges form plain, auxiliary, row12-template char[253]/char[254], trailing-space char[252]/char[253], row10/row11-template char[253]/char[254], and double-trailing-space char[252] sweeps over the DAO lab alphabet plus a small row10/row11/row12 template sample set.")
            .AppendLine();

        foreach ((string tableName, int seedBase) in new[] { ("Table11", 100000), ("Table11_desc", 101000) })
        {
            SuffixPatternTable table = await BuildSuffixPatternTableAsync(reader, tableName, seedBase, ct);
            AppendSyntheticGroupSummary(sb, table);
            AppendDuplicateValueSummary(sb, table);
            AppendSuffixOrderSummary(sb, table);
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
        return seed is >= 0 and < DaoLabRowCount ? seed : null;
    }

    private static void AppendSyntheticGroupSummary(StringBuilder sb, SuffixPatternTable table)
    {
        sb.AppendLine(CultureInfo.InvariantCulture, $"### {table.TableName}.DataIndex synthetic groups")
            .AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"- ascending: {table.Ascending}")
            .AppendLine()
            .AppendLine("| Group | Seed range | Count | Access suffixes | Encoder suffixes | Full lengths | First examples | Last examples |")
            .AppendLine("|---:|---|---:|---:|---:|---|---|---|");

        for (int group = 0; group < 4; group++)
        {
            int minSeed = group * 64;
            int maxSeed = minSeed + 63;
            var rows = table.Rows
                .Where(row => row.Seed is >= 0 && row.Seed >= minSeed && row.Seed <= maxSeed)
                .OrderBy(row => row.Seed)
                .ToList();

            string accessCount = rows.Select(row => row.AccessSuffix).Distinct().Count().ToString(CultureInfo.InvariantCulture);
            string encoderCount = rows.Select(row => row.EncoderSuffix).Distinct().Count().ToString(CultureInfo.InvariantCulture);
            string lengths = rows.Count == 0
                ? "-"
                : string.Join(", ", rows.Select(row => row.FullLength).Distinct().Order().Select(length => length?.ToString(CultureInfo.InvariantCulture) ?? "-"));

            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"| {group} | {minSeed}-{maxSeed} | {rows.Count} | {accessCount} | {encoderCount} | {lengths} | {DescribeSeedExamples(rows.Take(4))} | {DescribeSeedExamples(rows.TakeLast(4))} |");
        }

        sb.AppendLine();

        foreach (int group in new[] { 0, 1 })
        {
            int minSeed = group * 64;
            int maxSeed = minSeed + 63;
            var rows = table.Rows
                .Where(row => row.Seed is >= 0 && row.Seed >= minSeed && row.Seed <= maxSeed)
                .OrderBy(row => row.Seed)
                .ToList();
            if (rows.Count == 0)
            {
                continue;
            }

            sb.AppendLine(CultureInfo.InvariantCulture, $"Seed detail for group {group} ({minSeed}-{maxSeed}):")
                .AppendLine()
                .AppendLine("| Seed | Access suffix | Encoder suffix | Prefix | Data ptr | Leaf entry | pref_len | raw len | raw start | Full tail |")
                .AppendLine("|---:|:---:|:---:|:---:|---:|---|---:|---:|---:|---|");
            foreach (SuffixPatternRow row in rows)
            {
                sb.AppendLine(
                    CultureInfo.InvariantCulture,
                    $"| {row.Seed} | `{row.AccessSuffix:X4}` | `{row.EncoderSuffix:X4}` | {(row.PrefixMatch ? "yes" : "no")} | {row.DataPage}:{row.DataRow} | {row.LeafPage}:{row.LeafEntryIndex} | {row.PrefixLength} | {row.RawKeyLength} | {row.EntryStart} | {row.FullTail} |");
            }

            sb.AppendLine();
        }

        AppendPairMatrixSummary(sb, table, DaoLabPairMatrixStart, "Pair matrix", includeCrc16: false);
        AppendPairMatrixSummary(sb, table, DaoLabAuxMatrixStart, "Auxiliary pair matrix", includeCrc16: false);
        AppendPairMatrixSummary(sb, table, DaoLabRow12MatrixStart, "Row12 template pair matrix", includeCrc16: false);
        AppendPairMatrixSummary(sb, table, DaoLabTrailingSpaceMatrixStart, "Trailing-space pair matrix", includeCrc16: false);
        AppendPairMatrixSummary(sb, table, DaoLabRow10MatrixStart, "Row10 template pair matrix", includeCrc16: false);
        AppendPairMatrixSummary(sb, table, DaoLabRow11MatrixStart, "Row11 template pair matrix", includeCrc16: false);
        AppendBoundarySpaceShiftModelSummary(sb, table);
        AppendDoubleTrailingSpaceSweepSummary(sb, table);
        AppendGf2CrossMultiplicationSolverSummary(sb, table, DaoLabPairMatrixStart);
        AppendTemplateSampleSummary(sb, table);
    }

    private static void AppendTemplateSampleSummary(StringBuilder sb, SuffixPatternTable table)
    {
        var rows = table.Rows
            .Where(row => row.Seed is not null && row.Seed.Value >= DaoLabTemplateSampleStart && row.Seed.Value < DaoLabTemplateSampleStart + DaoLabTemplateSampleRowCount)
            .OrderBy(row => row.Seed)
            .ToList();
        if (rows.Count == 0)
        {
            return;
        }

        sb.AppendLine(CultureInfo.InvariantCulture, $"Template sample summary for seeds {DaoLabTemplateSampleStart}-{DaoLabTemplateSampleStart + DaoLabTemplateSampleRowCount - 1}:")
            .AppendLine()
            .AppendLine("| Seed | Template | Variant | Access suffix | Encoder suffix | Full tail |")
            .AppendLine("|---:|---|---|:---:|:---:|---|");
        foreach (SuffixPatternRow row in rows)
        {
            int sample = row.Seed!.Value - DaoLabTemplateSampleStart;
            string template = (sample / 4) switch
            {
                0 => "row10",
                1 => "row11",
                _ => "row12",
            };
            string variant = (sample % 4) switch
            {
                0 => "original",
                1 => "space-space",
                2 => "a-space",
                _ => "space-a",
            };
            sb.AppendLine(CultureInfo.InvariantCulture, $"| {row.Seed} | `{template}` | `{variant}` | `{row.AccessSuffix:X4}` | `{row.EncoderSuffix:X4}` | {row.FullTail} |");
        }

        sb.AppendLine();
    }

    private static void AppendDoubleTrailingSpaceSweepSummary(StringBuilder sb, SuffixPatternTable table)
    {
        var rows = table.Rows
            .Where(row => row.Seed is not null && row.Seed.Value >= DaoLabDoubleSpaceSweepStart && row.Seed.Value < DaoLabDoubleSpaceSweepStart + DaoLabDoubleSpaceSweepRowCount)
            .OrderBy(row => row.Seed)
            .ToList();
        if (rows.Count == 0)
        {
            return;
        }

        sb.AppendLine(CultureInfo.InvariantCulture, $"Double-trailing-space sweep summary for seeds {DaoLabDoubleSpaceSweepStart}-{DaoLabDoubleSpaceSweepStart + DaoLabDoubleSpaceSweepRowCount - 1}:")
            .AppendLine()
            .AppendLine("For each context, varies char[252] while forcing char[253] and char[254] to spaces. This targets the all-space corner left by the pair matrices.")
            .AppendLine()
            .AppendLine("| Context | Rows | Access suffixes | Encoder suffixes | First examples | Last examples |")
            .AppendLine("|---|---:|---:|---:|---|---|");

        for (int contextIndex = 0; contextIndex < DaoLabDoubleSpaceSweepContextCount; contextIndex++)
        {
            var contextRows = rows
                .Where(row => GetDoubleSpaceSweepContext(row.Seed!.Value) == contextIndex)
                .OrderBy(row => row.Seed)
                .ToList();
            if (contextRows.Count == 0)
            {
                continue;
            }

            int accessDistinct = contextRows.Select(row => row.AccessSuffix).Distinct().Count();
            int encoderDistinct = contextRows.Select(row => row.EncoderSuffix).Distinct().Count();
            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"| `{GetDoubleSpaceSweepContextName(contextIndex)}` | {contextRows.Count} | {accessDistinct} | {encoderDistinct} | {DescribeDoubleSpaceSweepExamples(contextRows.Take(8))} | {DescribeDoubleSpaceSweepExamples(contextRows.TakeLast(8))} |");
        }

        sb.AppendLine();
    }

    private static int GetDoubleSpaceSweepContext(int seed)
        => (seed - DaoLabDoubleSpaceSweepStart) / DaoLabAlphabetLength;

    private static string GetDoubleSpaceSweepContextName(int contextIndex) => contextIndex switch
    {
        0 => "plain",
        1 => "row10",
        2 => "row11",
        _ => "row12",
    };

    private static string DescribeDoubleSpaceSweepExamples(IEnumerable<SuffixPatternRow> rows) =>
        string.Join(" ", rows.Select(row =>
        {
            int charIndex = (row.Seed!.Value - DaoLabDoubleSpaceSweepStart) % DaoLabAlphabetLength;
            return $"`{FormatMatrixChar(DaoLabAlphabet[charIndex])}:{row.AccessSuffix:X4}`";
        }));

    private static void AppendSuffixOrderSummary(StringBuilder sb, SuffixPatternTable table)
    {
        var sharedPrefixGroups = table.Rows
            .Where(row => row.Text is not null && row.FullKey.Length >= PrefixMatchLength)
            .GroupBy(row => row.FullKey, LongRowPrefixEqualityComparer.Instance)
            .Select(group => group.ToArray())
            .Where(rows => rows.Length > 1)
            .OrderByDescending(rows => rows.Length)
            .ToList();

        int nonMonotonicGroups = 0;
        int orderMismatchGroups = 0;
        var examples = new List<(SuffixPatternRow[] FullOrder, SuffixPatternRow[] LeafOrder)>();

        foreach (SuffixPatternRow[] group in sharedPrefixGroups)
        {
            SuffixPatternRow[] fullOrder = group
                .OrderBy(row => row.FullKey, BytePrefixComparer.Instance)
                .ThenBy(row => row.DataPage)
                .ThenBy(row => row.DataRow)
                .ToArray();
            SuffixPatternRow[] leafOrder = group
                .OrderBy(row => row.Position)
                .ToArray();

            bool monotonic = true;
            for (int index = 1; index < fullOrder.Length; index++)
            {
                if (fullOrder[index - 1].AccessSuffix > fullOrder[index].AccessSuffix)
                {
                    monotonic = false;
                    break;
                }
            }

            if (!monotonic)
            {
                nonMonotonicGroups++;
            }

            bool sameOrder = fullOrder.Select(row => row.RowLabel).SequenceEqual(leafOrder.Select(row => row.RowLabel));
            if (!sameOrder)
            {
                orderMismatchGroups++;
                if (examples.Count < 5)
                {
                    examples.Add((fullOrder, leafOrder));
                }
            }
        }

        int sharedPrefixRows = sharedPrefixGroups.Sum(rows => rows.Length);

        sb.AppendLine(CultureInfo.InvariantCulture, $"Suffix order check for {table.TableName}.DataIndex:")
            .AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"- shared first-508-byte groups: {sharedPrefixGroups.Count}")
            .AppendLine(CultureInfo.InvariantCulture, $"- rows in shared-prefix groups: {sharedPrefixRows}")
            .AppendLine(CultureInfo.InvariantCulture, $"- groups where Access suffix is non-monotonic by full-key order: {nonMonotonicGroups}")
            .AppendLine(CultureInfo.InvariantCulture, $"- groups where leaf order differs from full-key order: {orderMismatchGroups}");

        if (examples.Count == 0)
        {
            sb.AppendLine();
            return;
        }

        sb.AppendLine()
            .AppendLine("| Rows | Full-key order | Leaf order |")
            .AppendLine("|---:|---|---|");
        foreach ((SuffixPatternRow[]? fullOrder, SuffixPatternRow[]? leafOrder) in examples)
        {
            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"| {fullOrder.Length} | {FormatOrderSample(fullOrder)} | {FormatOrderSample(leafOrder)} |");
        }

        sb.AppendLine();
    }

    private static void AppendPairMatrixSummary(StringBuilder sb, SuffixPatternTable table, int matrixStart, string title, bool includeCrc16)
    {
        var rows = table.Rows
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

        sb.AppendLine(CultureInfo.InvariantCulture, $"{title} summary for seeds {matrixStart}-{matrixStart + DaoLabPairMatrixRowCount - 1}:")
            .AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"- rows: {rows.Count}")
            .AppendLine(CultureInfo.InvariantCulture, $"- Access suffixes: {accessDistinct}")
            .AppendLine(CultureInfo.InvariantCulture, $"- encoder suffixes: {encoderDistinct}")
            .AppendLine(CultureInfo.InvariantCulture, $"- Access suffix collision buckets: {accessCollisionBuckets}")
            .AppendLine();
        AppendPairMatrixDistribution(sb, rows, matrixStart);
        AppendPairMatrixModelSummary(sb, table, rows, matrixStart, includeCrc16);

        sb.AppendLine("| Pair | Access suffix | Encoder suffix | Full tail |")
            .AppendLine("|---|:---:|:---:|---|");

        foreach (SuffixPatternRow row in rows.Take(8).Concat(rows.Skip(Math.Max(0, rows.Count - 8))))
        {
            int pair = row.Seed!.Value - matrixStart;
            char first = DaoLabAlphabet[pair / DaoLabAlphabet.Length];
            char second = DaoLabAlphabet[pair % DaoLabAlphabet.Length];
            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"| `{EscapeMarkdown(new string([first, second]))}` | `{row.AccessSuffix:X4}` | `{row.EncoderSuffix:X4}` | {row.FullTail} |");
        }

        sb.AppendLine();
    }

    private static void AppendPairMatrixDistribution(StringBuilder sb, List<SuffixPatternRow> rows, int matrixStart)
    {
        sb.AppendLine("Access suffix distribution:")
            .AppendLine()
            .AppendLine("| Access suffix | Rows | First pairs | First seeds |")
            .AppendLine("|:---:|---:|---|---|");
        foreach (IGrouping<ushort, SuffixPatternRow>? group in rows
            .GroupBy(row => row.AccessSuffix)
            .OrderByDescending(group => group.Count())
            .ThenBy(group => group.Key)
            .Take(8))
        {
            string pairs = string.Join(" ", group.Take(6).Select(row => $"`{EscapeMarkdown(GetMatrixPairText(row, matrixStart))}`"));
            string seeds = string.Join(" ", group.Take(6).Select(row => row.Seed!.Value.ToString(CultureInfo.InvariantCulture)));
            sb.AppendLine(CultureInfo.InvariantCulture, $"| `{group.Key:X4}` | {group.Count()} | {pairs} | `{seeds}` |");
        }

        sb.AppendLine();
    }

    private static string GetMatrixPairText(SuffixPatternRow row, int matrixStart)
    {
        int pair = row.Seed!.Value - matrixStart;
        return new string([DaoLabAlphabet[pair / DaoLabAlphabet.Length], DaoLabAlphabet[pair % DaoLabAlphabet.Length]]);
    }

    private static void AppendPairMatrixModelSummary(StringBuilder sb, SuffixPatternTable table, List<SuffixPatternRow> rows, int matrixStart, bool includeCrc16)
    {
        int size = DaoLabAlphabet.Length;
        var suffixes = new ushort[size * size];
        var present = new bool[size * size];
        var rowByOffset = new SuffixPatternRow?[size * size];
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
                rowByOffset[index] = row;
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
        int xorSecondSpaceMatches = 0;
        int xorSecondSpaceTotal = 0;
        int xorNonSecondSpaceMatches = 0;
        int xorNonSecondSpaceTotal = 0;
        var xorFailures = new List<PairMatrixXorFailure>();

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
                bool secondSpace = DaoLabAlphabet[second] == ' ';

                if (xorPredicted == actual)
                {
                    xorMatches++;
                    if (secondSpace)
                    {
                        xorSecondSpaceMatches++;
                    }
                    else
                    {
                        xorNonSecondSpaceMatches++;
                    }
                }
                else if (rowByOffset[index] is SuffixPatternRow failedRow)
                {
                    xorFailures.Add(new PairMatrixXorFailure(
                        DaoLabAlphabet[first],
                        DaoLabAlphabet[second],
                        failedRow,
                        actual,
                        xorPredicted,
                        ToHexStringOrEmpty(failedRow.FullKey, 508, 5),
                        ToHexStringOrEmpty(failedRow.TrimmedFullKey, 508, 5)));
                }

                if (secondSpace)
                {
                    xorSecondSpaceTotal++;
                }
                else
                {
                    xorNonSecondSpaceTotal++;
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

        sb.AppendLine("Pair matrix model checks:")
            .AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"- XOR row/column decomposition: {xorMatches}/{total}")
            .AppendLine(CultureInfo.InvariantCulture, $"- XOR split by second char: non-space {xorNonSecondSpaceMatches}/{xorNonSecondSpaceTotal}, space {xorSecondSpaceMatches}/{xorSecondSpaceTotal}")
            .AppendLine(CultureInfo.InvariantCulture, $"- Add row/column decomposition: {addMatches}/{total}")
            .AppendLine(CultureInfo.InvariantCulture, $"- High-byte XOR/add decomposition: {highXorMatches}/{total}, {highAddMatches}/{total}")
            .AppendLine(CultureInfo.InvariantCulture, $"- Low-byte XOR/add decomposition: {lowXorMatches}/{total}, {lowAddMatches}/{total}");

        if (includeCrc16)
        {
            AppendCrc16AffineHitLine(sb, table, rows, "full[508..511]", row => SliceOrEmpty(row.FullKey, 508, 4));
            AppendCrc16AffineHitLine(sb, table, rows, "full[503..511]", row => SliceOrEmpty(row.FullKey, 503, 8));
            AppendCrc16AffineHitLine(sb, table, rows, "full[503..521]", row => SliceOrEmpty(row.FullKey, 503, 19));
        }
        else
        {
            sb.AppendLine("- CRC-16 affine hits over local boundary windows: skipped in the fast DAO lab");
        }

        AppendPairMatrixAffineBitSummary(sb, table, rows, matrixStart);
        AppendPairMatrixXorFailureSummary(sb, xorFailures);
        sb.AppendLine()
            .AppendLine("Pair contribution examples (`H(x,a) ^ H(a,a)` and `H(a,x) ^ H(a,a)`):")
            .AppendLine()
            .AppendLine("| Char | Row contribution | Column contribution | Row suffix | Column suffix |")
            .AppendLine("|---|:---:|:---:|:---:|:---:|");
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

    private static void AppendPairMatrixXorFailureSummary(StringBuilder sb, List<PairMatrixXorFailure> failures)
    {
        sb.AppendLine(CultureInfo.InvariantCulture, $"- XOR row/column failures: {failures.Count}");
        if (failures.Count == 0)
        {
            return;
        }

        sb.AppendLine()
            .AppendLine("XOR failure breakdown by second char:")
            .AppendLine()
            .AppendLine("| Second char | Failures | First chars | Full tails | Trimmed tails |")
            .AppendLine("|---|---:|---|---|---|");
        foreach (IGrouping<char, PairMatrixXorFailure>? group in failures
            .GroupBy(failure => failure.SecondChar)
            .OrderByDescending(group => group.Count())
            .ThenBy(group => group.Key)
            .Take(8))
        {
            string firstChars = string.Join(" ", group
                .Select(failure => failure.FirstChar)
                .Distinct()
                .Take(12)
                .Select(ch => $"`{FormatMatrixChar(ch)}`"));
            string fullTails = string.Join(" ", group
                .Select(failure => failure.FullTail)
                .Distinct(StringComparer.Ordinal)
                .Take(4));
            string trimmedTails = string.Join(" ", group
                .Select(failure => failure.TrimmedTail)
                .Distinct(StringComparer.Ordinal)
                .Take(4));

            sb.AppendLine(CultureInfo.InvariantCulture, $"| `{FormatMatrixChar(group.Key)}` | {group.Count()} | {firstChars} | {fullTails} | {trimmedTails} |");
        }

        sb.AppendLine()
            .AppendLine("Sample XOR failures:")
            .AppendLine()
            .AppendLine("| Pair | Actual | Predicted | Residual | Row | full[508..512] | trimmed[508..512] |")
            .AppendLine("|---|:---:|:---:|:---:|---|---|---|");
        foreach (PairMatrixXorFailure failure in failures.Take(12))
        {
            ushort residual = (ushort)(failure.Actual ^ failure.Predicted);
            string pair = string.Concat(FormatMatrixChar(failure.FirstChar), FormatMatrixChar(failure.SecondChar));
            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"| `{pair}` | `{failure.Actual:X4}` | `{failure.Predicted:X4}` | `{residual:X4}` | `{failure.Row.RowLabel}` | {failure.FullTail} | {failure.TrimmedTail} |");
        }
    }

    private static string FormatMatrixChar(char value) =>
        value == ' ' ? "SP" : EscapeMarkdown(value.ToString());

    private static void AppendBoundarySpaceShiftModelSummary(StringBuilder sb, SuffixPatternTable table)
    {
        if (!TryBuildMatrix(table, DaoLabPairMatrixStart, out ushort[] normalSuffixes, out bool[] normalPresent)
            || !TryBuildMatrix(table, DaoLabTrailingSpaceMatrixStart, out ushort[] trailingSuffixes, out bool[] trailingPresent))
        {
            return;
        }

        const int size = DaoLabAlphabetLength;
        int baseIndex = DaoLabAlphabet.IndexOf('a', StringComparison.Ordinal);
        int spaceIndex = DaoLabAlphabet.IndexOf(' ', StringComparison.Ordinal);
        int normalBaseOffset = (baseIndex * size) + baseIndex;
        int trailingBaseOffset = (baseIndex * size) + baseIndex;
        if (baseIndex < 0
            || spaceIndex < 0
            || !normalPresent[normalBaseOffset]
            || !trailingPresent[trailingBaseOffset])
        {
            return;
        }

        ushort normalBase = normalSuffixes[normalBaseOffset];
        ushort trailingBase = trailingSuffixes[trailingBaseOffset];

        var rowContributions = new ushort[size];
        var columnContributions = new ushort[size];
        var trailingContributions = new ushort[size];
        var hasRowContribution = new bool[size];
        var hasColumnContribution = new bool[size];
        var hasTrailingContribution = new bool[size];

        for (int index = 0; index < size; index++)
        {
            int rowOffset = (index * size) + baseIndex;
            if (normalPresent[rowOffset])
            {
                rowContributions[index] = (ushort)(normalSuffixes[rowOffset] ^ normalBase);
                hasRowContribution[index] = true;
            }

            int columnOffset = (baseIndex * size) + index;
            if (normalPresent[columnOffset])
            {
                columnContributions[index] = (ushort)(normalSuffixes[columnOffset] ^ normalBase);
                hasColumnContribution[index] = true;
            }

            int trailingColumnOffset = (baseIndex * size) + index;
            if (trailingPresent[trailingColumnOffset])
            {
                trailingContributions[index] = (ushort)(trailingSuffixes[trailingColumnOffset] ^ trailingBase);
                hasTrailingContribution[index] = true;
            }
        }

        int normalMatches = 0;
        int normalTotal = 0;
        int shiftedMatches = 0;
        int shiftedTotal = 0;
        int trailingMatches = 0;
        int trailingTotal = 0;

        for (int first = 0; first < size; first++)
        {
            for (int second = 0; second < size; second++)
            {
                int offset = (first * size) + second;
                if (normalPresent[offset])
                {
                    if (second == spaceIndex)
                    {
                        if (hasTrailingContribution[first])
                        {
                            shiftedTotal++;
                            ushort predicted = (ushort)(trailingBase ^ trailingContributions[first]);
                            if (predicted == normalSuffixes[offset])
                            {
                                shiftedMatches++;
                            }
                        }
                    }
                    else if (hasRowContribution[first] && hasColumnContribution[second])
                    {
                        normalTotal++;
                        ushort predicted = (ushort)(normalBase ^ rowContributions[first] ^ columnContributions[second]);
                        if (predicted == normalSuffixes[offset])
                        {
                            normalMatches++;
                        }
                    }
                }

                if (trailingPresent[offset] && hasTrailingContribution[second])
                {
                    trailingTotal++;
                    ushort predicted = (ushort)(trailingBase ^ trailingContributions[second]);
                    if (predicted == trailingSuffixes[offset])
                    {
                        trailingMatches++;
                    }
                }
            }
        }

        int sharedContributionMatches = 0;
        int sharedContributionTotal = 0;
        for (int index = 0; index < size; index++)
        {
            if (!hasColumnContribution[index] || !hasTrailingContribution[index])
            {
                continue;
            }

            sharedContributionTotal++;
            if (columnContributions[index] == trailingContributions[index])
            {
                sharedContributionMatches++;
            }
        }

        int shiftedRowsPresent = Enumerable.Range(0, size).Count(first => normalPresent[(first * size) + spaceIndex]);
        int trailingRowsPresent = trailingPresent.Count(present => present);

        sb.AppendLine("Boundary-space shift model:")
            .AppendLine()
            .AppendLine("Trains on the plain pair matrix and the trailing-space matrix. The model treats a non-space byte at the 255-character boundary as a normal two-axis XOR table; when the boundary char is a space, it shifts the column role left to the previous indexed character and ignores the earlier row axis.")
            .AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"- normal non-space two-axis fit: {normalMatches}/{normalTotal}")
            .AppendLine(CultureInfo.InvariantCulture, $"- normal second-space rows predicted by shifted model: {shiftedMatches}/{shiftedTotal} (present rows {shiftedRowsPresent}/{size})")
            .AppendLine(CultureInfo.InvariantCulture, $"- trailing-space matrix predicted by shifted model: {trailingMatches}/{trailingTotal} (present rows {trailingRowsPresent}/{size * size})")
            .AppendLine(CultureInfo.InvariantCulture, $"- shifted deltas equal normal column deltas where both observed: {sharedContributionMatches}/{sharedContributionTotal}");
        if (hasColumnContribution[spaceIndex] && hasTrailingContribution[spaceIndex])
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"- observed space column delta: normal `{columnContributions[spaceIndex]:X4}`, shifted `{trailingContributions[spaceIndex]:X4}`");
        }

        sb.AppendLine();
    }

    private static bool TryBuildMatrix(
        SuffixPatternTable table,
        int matrixStart,
        out ushort[] suffixes,
        out bool[] present)
    {
        const int size = DaoLabAlphabetLength;
        suffixes = new ushort[size * size];
        present = new bool[size * size];

        foreach (SuffixPatternRow row in table.Rows)
        {
            if (row.Seed is not int seed || seed < matrixStart || seed >= matrixStart + DaoLabPairMatrixRowCount)
            {
                continue;
            }

            int pair = seed - matrixStart;
            int first = pair / size;
            int second = pair % size;
            if (first < 0 || first >= size || second < 0 || second >= size)
            {
                continue;
            }

            int offset = (first * size) + second;
            suffixes[offset] = row.AccessSuffix;
            present[offset] = true;
        }

        return present.Any(value => value);
    }

    private static List<Crc16AffineHit> FindCrc16AffineHits(
        SuffixPatternTable table,
        List<SuffixPatternRow> rows,
        Func<SuffixPatternRow, byte[]> getInput,
        int maxHits)
    {
        _ = table;
        RollingConstraint[] constraints = rows
            .Where(row => row.Text is not null)
            .Select(row => new RollingConstraint(
                getInput(row),
                row.AccessSuffix))
            .Where(constraint => constraint.Input.Length > 0)
            .ToArray();

        var hits = new List<Crc16AffineHit>();
        if (constraints.Length == 0)
        {
            return hits;
        }

        RollingConstraint[] searchConstraints = BuildCrcSearchSample(constraints, maxCount: 64);
        var normalTable = new ushort[256];
        var reflectedTable = new ushort[256];

        for (int polynomial = 0; polynomial <= 0xFFFF; polynomial++)
        {
            ushort polynomialValue = (ushort)polynomial;
            ushort reflectedPolynomial = ReflectU16(polynomialValue);
            BuildCrcTable(polynomialValue, normalTable, reflected: false);
            BuildCrcTable(reflectedPolynomial, reflectedTable, reflected: true);
            for (int mode = 0; mode < 4; mode++)
            {
                bool refIn = (mode & 1) != 0;
                bool refOut = (mode & 2) != 0;
                ushort first = CrcFullWithTable(searchConstraints[0].Input, normalTable, reflectedTable, 0, 0, refIn, refOut);
                ushort xorConstant = (ushort)(searchConstraints[0].Target ^ first);

                bool allMatch = true;
                for (int index = 1; index < searchConstraints.Length; index++)
                {
                    ushort crc = CrcFullWithTable(searchConstraints[index].Input, normalTable, reflectedTable, 0, 0, refIn, refOut);
                    if ((ushort)(crc ^ xorConstant) != searchConstraints[index].Target)
                    {
                        allMatch = false;
                        break;
                    }
                }

                if (allMatch && searchConstraints.Length != constraints.Length)
                {
                    for (int index = 0; index < constraints.Length; index++)
                    {
                        ushort crc = CrcFullWithTable(constraints[index].Input, normalTable, reflectedTable, 0, 0, refIn, refOut);
                        if ((ushort)(crc ^ xorConstant) != constraints[index].Target)
                        {
                            allMatch = false;
                            break;
                        }
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

    private static RollingConstraint[] BuildCrcSearchSample(RollingConstraint[] constraints, int maxCount)
    {
        if (constraints.Length <= maxCount)
        {
            return constraints;
        }

        return constraints
            .OrderBy(static constraint => constraint.Input.Length)
            .ThenBy(static constraint => constraint.Target)
            .Take(maxCount)
            .ToArray();
    }

    private static void AppendCrc16AffineHitLine(
        StringBuilder sb,
        SuffixPatternTable table,
        List<SuffixPatternRow> rows,
        string label,
        Func<SuffixPatternRow, byte[]> getInput)
    {
        List<Crc16AffineHit> crcHits = FindCrc16AffineHits(table, rows, getInput, maxHits: 8);
        string crcHitText = crcHits.Count == 0
            ? "-"
            : "`" + string.Join(" ", crcHits.Select(hit => $"poly={hit.Polynomial:X4}/xor={hit.XorConstant:X4}/refIn={hit.RefIn}/refOut={hit.RefOut}")) + "`";
        sb.AppendLine(CultureInfo.InvariantCulture, $"- CRC-16 affine hits over `{label}`: {crcHits.Count} {crcHitText}");
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
        AppendAffineBitResult(sb, "text[253..255] en-US CompareHash IgnoreCase", usableRows, targets, table, 32, BuildCompareHashFeature);

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
        sb.AppendLine(CultureInfo.InvariantCulture, $"  - {label} scored on all rows: {exact}/{evaluated}")
            .AppendLine(CultureInfo.InvariantCulture, $"  - {label} coefficients: `{coefficientText}`");
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

    private static ulong? BuildCompareHashFeature(SuffixPatternRow row, SuffixPatternTable table, int bitCount)
    {
        _ = table;
        _ = bitCount;
        int hash = EnUsCompareInfo.GetHashCode(
            TextWindow(row.Text!, 253, 255),
            CompareOptions.IgnoreCase);
        return unchecked((uint)hash);
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
        var duplicateGroups = table.Rows
            .Where(row => row.Text is not null)
            .GroupBy(row => row.Text!, StringComparer.Ordinal)
            .Where(group => group.Count() > 1)
            .OrderByDescending(group => group.Select(row => row.AccessSuffix).Distinct().Count())
            .ThenByDescending(group => group.Count())
            .ToList();

        int conflictingGroups = duplicateGroups.Count(group => group.Select(row => row.AccessSuffix).Distinct().Count() > 1);

        sb.AppendLine(CultureInfo.InvariantCulture, $"Exact duplicate value check for {table.TableName}.DataIndex:")
            .AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"- duplicate text groups: {duplicateGroups.Count}")
            .AppendLine(CultureInfo.InvariantCulture, $"- duplicate groups with multiple Access suffixes: {conflictingGroups}");

        if (duplicateGroups.Count == 0)
        {
            sb.AppendLine();
            return;
        }

        sb.AppendLine()
            .AppendLine("| Rows | Access suffixes | Seeds | Data ptrs |")
            .AppendLine("|---:|---|---|---|");
        foreach (IGrouping<string, SuffixPatternRow>? group in duplicateGroups.Take(8))
        {
            SuffixPatternRow[] rows = group.OrderBy(row => row.Position).ToArray();
            string suffixes = string.Join(" ", rows.Select(row => row.AccessSuffix).Distinct().Order().Select(value => $"`{value:X4}`"));
            string seeds = string.Join(" ", rows.Select(row => row.Seed?.ToString(CultureInfo.InvariantCulture) ?? row.RowLabel));
            string ptrs = string.Join(" ", rows.Select(row => string.Create(CultureInfo.InvariantCulture, $"{row.DataPage}:{row.DataRow}")));
            sb.AppendLine(CultureInfo.InvariantCulture, $"| {rows.Length} | {suffixes} | `{seeds}` | `{ptrs}` |");
        }

        sb.AppendLine();
    }

    private static string FormatOrderSample(IReadOnlyList<SuffixPatternRow> rows)
    {
        IEnumerable<string> parts = rows
            .Take(8)
            .Select(row =>
            {
                string label = row.Seed?.ToString(CultureInfo.InvariantCulture) ?? row.RowLabel.Trim('`');
                return $"`{label}:{row.AccessSuffix:X4}:{ToHexStringOrEmpty(row.FullKey, 508, 4)}`";
            });

        if (rows.Count > 8)
        {
            parts = parts.Append("...");
        }

        return string.Join(" ", parts);
    }

    private static void AppendSuffixCandidateSummary(StringBuilder sb, SuffixPatternTable table)
    {
        SuffixCandidateContext[] contexts = table.Rows
            .Where(row => row.Text is not null)
            .Select(row => new SuffixCandidateContext(row, table.Ascending))
            .ToArray();

        sb.AppendLine(CultureInfo.InvariantCulture, $"Suffix candidate score for {table.TableName}.DataIndex:")
            .AppendLine();
        if (contexts.Length == 0)
        {
            sb.AppendLine("- no text rows available for candidate scoring")
                .AppendLine();
            return;
        }

        List<CandidateRule> rules = SuffixCandidateRules.Value;
        var xorCounts = new CountAccumulator();
        var addCounts = new CountAccumulator();
        var scores = rules
            .Select(rule => ScoreCandidate(rule, contexts, xorCounts, addCounts))
            .Where(score => score.Evaluated > 0)
            .OrderByDescending(score => Math.Max(score.Exact, Math.Max(score.BestXorCount, score.BestAddCount)))
            .ThenByDescending(score => score.Exact)
            .ThenByDescending(score => score.BestXorCount)
            .ThenByDescending(score => score.BestAddCount)
            .ThenBy(score => score.Name, StringComparer.Ordinal)
            .Take(16)
            .ToList();

        sb.AppendLine(CultureInfo.InvariantCulture, $"- rows scored: {contexts.Length}")
            .AppendLine(CultureInfo.InvariantCulture, $"- candidates tested: {rules.Count}")
            .AppendLine()
            .AppendLine("| Candidate | Exact | Best XOR | XOR constant | Best add | Add constant |")
            .AppendLine("|---|---:|---:|:---:|---:|:---:|");
        foreach (CandidateScore score in scores)
        {
            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"| {score.Name} | {score.Exact}/{score.Evaluated} | {score.BestXorCount}/{score.Evaluated} | `{score.BestXorConstant:X4}` | {score.BestAddCount}/{score.Evaluated} | `{score.BestAddConstant:X4}` |");
        }

        sb.AppendLine();
        AppendRollingPolynomialSolverSummary(sb, contexts);
        AppendCrcDerivedInitSolverSummary(sb, contexts);
        AppendCrc32DerivedInitSolverSummary(sb, contexts);
        AppendLinearTableExtractorSummary(sb, contexts);
        AppendRotlFoldSolverSummary(sb, contexts);
        AppendBitContributionMatrixSummary(sb, contexts);
        AppendAuxSignatureSummary(sb, contexts);
        AppendTruncationPhaseSummary(sb, contexts);
        AppendTruncationWindowSweepSummary(sb, contexts);
        AppendCompareSortKeySweepSummary(sb, contexts);
    }

    private static void AppendAuxSignatureSummary(StringBuilder sb, SuffixCandidateContext[] contexts)
    {
        Encoding cp1252 = Cp1252Encoding;
        var groups = contexts
            .Select(context =>
            {
                byte[][] inputs = context.GetInputCandidates(cp1252);
                return new
                {
                    Context = context,
                    Signature = Convert.ToHexString(inputs[AuxInputCandidateIndex]),
                    inputs[AuxInputCandidateIndex].Length,
                };
            })
            .Where(item => item.Length > 0)
            .GroupBy(item => item.Signature, StringComparer.Ordinal)
            .OrderByDescending(group => group.Count())
            .ThenBy(group => group.Key, StringComparer.Ordinal)
            .Take(12)
            .ToList();

        sb.AppendLine("Auxiliary stream signatures:")
            .AppendLine();
        if (groups.Count == 0)
        {
            sb.AppendLine("- no auxiliary streams in scored rows")
                .AppendLine();
            return;
        }

        sb.AppendLine("| Rows | Access suffixes | First rows | Signature |")
            .AppendLine("|---:|---|---|---|");
        foreach (var group in groups)
        {
            string suffixes = string.Join(" ", group.Select(item => item.Context.Row.AccessSuffix).Distinct().Order().Select(value => $"`{value:X4}`"));
            string rows = string.Join(" ", group.Take(6).Select(item => item.Context.Row.RowLabel));
            sb.AppendLine(CultureInfo.InvariantCulture, $"| {group.Count()} | {suffixes} | `{rows}` | `{group.Key}` |");
        }

        sb.AppendLine();
    }

    private static void AppendTruncationPhaseSummary(StringBuilder sb, SuffixCandidateContext[] contexts)
    {
        Encoding cp1252 = Cp1252Encoding;
        var allGroups = contexts
            .Select(context =>
            {
                byte[][] inputs = context.GetNormalizedInputCandidates(cp1252);
                string auxSignature = Convert.ToHexString(inputs[AuxInputCandidateIndex]);
                string window = ToHexStringOrEmpty(context.NormalizedFullKey, 500, 16);
                string boundary = ToHexStringOrEmpty(context.NormalizedFullKey, 506, 6);
                return new
                {
                    Context = context,
                    Phase = ClassifyTruncationPhase(context.NormalizedFullKey),
                    Boundary = boundary,
                    Window = window,
                    AuxSignature = auxSignature,
                };
            })
            .GroupBy(item => (item.Phase, item.Boundary, item.Window, item.AuxSignature))
            .Select(group => new
            {
                group.Key.Phase,
                group.Key.Boundary,
                group.Key.Window,
                group.Key.AuxSignature,
                Rows = group.ToArray(),
                Suffixes = group.Select(item => item.Context.Row.AccessSuffix).Distinct().Order().ToArray(),
            })
            .ToList();

        var groups = allGroups
            .OrderByDescending(group => group.Suffixes.Length)
            .ThenByDescending(group => group.Rows.Length)
            .ThenBy(group => group.Phase, StringComparer.Ordinal)
            .Take(12)
            .ToList();

        sb.AppendLine("Truncation phase signatures:")
            .AppendLine();
        if (groups.Count == 0)
        {
            sb.AppendLine("- no phase groups")
                .AppendLine();
            return;
        }

        int conflictingGroups = allGroups.Count(group => group.Suffixes.Length > 1);
        int conflictingRows = allGroups.Where(group => group.Suffixes.Length > 1).Sum(group => group.Rows.Length);
        sb.AppendLine(CultureInfo.InvariantCulture, $"- phase groups: {allGroups.Count}")
            .AppendLine(CultureInfo.InvariantCulture, $"- phase groups with multiple Access suffixes: {conflictingGroups} ({conflictingRows} rows)")
            .AppendLine()
            .AppendLine("| Rows | Access suffixes | Phase | Boundary | Window | Aux signature | First rows |")
            .AppendLine("|---:|---|---|---|---|---|---|");
        foreach (var group in groups)
        {
            string suffixes = string.Join(" ", group.Suffixes.Select(value => $"`{value:X4}`"));
            string rows = string.Join(" ", group.Rows.Take(6).Select(item => item.Context.Row.RowLabel));
            string aux = group.AuxSignature.Length == 0 ? "-" : TruncateForReport(group.AuxSignature, 40);
            sb.AppendLine(CultureInfo.InvariantCulture, $"| {group.Rows.Length} | {suffixes} | `{group.Phase}` | `{group.Boundary}` | `{group.Window}` | `{aux}` | `{rows}` |");
        }

        sb.AppendLine();
    }

    private static void AppendTruncationWindowSweepSummary(StringBuilder sb, SuffixCandidateContext[] contexts)
    {
        Encoding cp1252 = Cp1252Encoding;
        WindowSweepRow[] rows = contexts
            .Select(context =>
            {
                byte[][] inputs = context.GetNormalizedInputCandidates(cp1252);
                return new WindowSweepRow(
                    context.Row.AccessSuffix,
                    context.NormalizedFullKey,
                    ClassifyTruncationPhase(context.NormalizedFullKey),
                    Convert.ToHexString(inputs[AuxInputCandidateIndex]));
            })
            .ToArray();

        var candidates = new List<WindowSweepResult>();
        foreach (bool includeAux in new[] { false, true })
        {
            for (int start = 496; start <= 510; start++)
            {
                for (int length = 2; length <= 20; length++)
                {
                    WindowConflictCounts groupCounts = CountTruncationWindowConflicts(rows, start, length, includeAux);

                    candidates.Add(new WindowSweepResult(
                        start,
                        length,
                        includeAux,
                        groupCounts.Groups,
                        groupCounts.ConflictingGroups,
                        groupCounts.ConflictingRows));
                }
            }
        }

        sb.AppendLine("Truncation local-window sweep:")
            .AppendLine();
        foreach (WindowSweepResult result in candidates
            .GroupBy(result => result.IncludeAux)
            .Select(group => group
                .OrderBy(result => result.ConflictingRows)
                .ThenBy(result => result.ConflictingGroups)
                .ThenBy(result => result.Length)
                .ThenBy(result => Math.Abs(result.Start - 500))
                .First())
            .OrderByDescending(result => result.IncludeAux))
        {
            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"- best {(result.IncludeAux ? "with" : "without")} aux: start {result.Start}, length {result.Length}, conflicts {result.ConflictingGroups} groups / {result.ConflictingRows} rows");
        }

        sb.AppendLine()
            .AppendLine("| Start | Length | Aux | Groups | Conflicting groups | Conflicting rows |")
            .AppendLine("|---:|---:|:---:|---:|---:|---:|");
        foreach (WindowSweepResult result in candidates
            .OrderBy(result => result.ConflictingRows)
            .ThenBy(result => result.ConflictingGroups)
            .ThenBy(result => result.Length)
            .ThenByDescending(result => result.IncludeAux)
            .ThenBy(result => Math.Abs(result.Start - 500))
            .Take(12))
        {
            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"| {result.Start} | {result.Length} | {(result.IncludeAux ? "yes" : "no")} | {result.Groups} | {result.ConflictingGroups} | {result.ConflictingRows} |");
        }

        sb.AppendLine();
    }

    private static WindowConflictCounts CountTruncationWindowConflicts(
        WindowSweepRow[] rows,
        int start,
        int length,
        bool includeAux)
    {
        var groups = new Dictionary<TruncationWindowKey, WindowGroupAccumulator>(TruncationWindowKeyComparer.Instance);
        foreach (WindowSweepRow row in rows)
        {
            int windowLength = GetSliceLength(row.NormalizedFullKey, start, length);
            string? auxSignature = includeAux ? row.AuxSignature : null;
            var key = new TruncationWindowKey(row.Phase, auxSignature, row.NormalizedFullKey, start, windowLength);
            if (!groups.TryGetValue(key, out WindowGroupAccumulator? group))
            {
                groups.Add(key, new WindowGroupAccumulator(row.AccessSuffix));
                continue;
            }

            group.Add(row.AccessSuffix);
        }

        int conflictingGroups = 0;
        int conflictingRows = 0;
        foreach (WindowGroupAccumulator group in groups.Values)
        {
            if (!group.HasConflict)
            {
                continue;
            }

            conflictingGroups++;
            conflictingRows += group.Rows;
        }

        return new WindowConflictCounts(groups.Count, conflictingGroups, conflictingRows);
    }

    private static int GetSliceLength(byte[] bytes, int start, int length)
    {
        if (length <= 0 || bytes.Length <= start)
        {
            return 0;
        }

        return Math.Min(length, bytes.Length - start);
    }

    private static string ClassifyTruncationPhase(byte[] fullKey)
    {
        if (fullKey.Length <= 508)
        {
            return "short";
        }

        byte b508 = fullKey[508];
        byte b507 = fullKey.Length > 507 ? fullKey[507] : (byte)0;
        return (b507, b508) switch
        {
            (_, GeneralLegacyTextIndexEncoder.EndText) => "at-end-text",
            (GeneralLegacyTextIndexEncoder.EndText, _) => "after-end-text",
            (_, 0x02) => "extra-placeholder",
            (_, 0xFD) => "desc-extra-placeholder",
            (_, 0xFE) => "desc-at-end-text",
            (0xFE, _) => "desc-after-end-text",
            _ => "inline-boundary",
        };
    }

    private static void AppendCompareSortKeySweepSummary(StringBuilder sb, SuffixCandidateContext[] contexts)
    {
        var hits = new List<CandidateScore>();
        var xorCounts = new CountAccumulator();
        var addCounts = new CountAccumulator();
        foreach ((string inputLabel, Func<SuffixCandidateContext, string>? getText) in BuildTextInputs())
        {
            foreach ((string label, CompareOptions options) in new[]
            {
                ("en-US none", CompareOptions.None),
                ("en-US string", CompareOptions.StringSort),
                ("en-US ignore-case", CompareOptions.IgnoreCase),
                ("en-US ignore-case-string", CompareOptions.IgnoreCase | CompareOptions.StringSort),
                ("en-US ignore-case-nonspace", CompareOptions.IgnoreCase | CompareOptions.IgnoreNonSpace),
            })
            {
                byte[][] sortKeys = contexts
                    .Select(context => context.GetCompareSortKeyBytes("en-US", options, getText(context)))
                    .ToArray();
                int maxLength = sortKeys.Length == 0 ? 0 : sortKeys.Max(key => key.Length);
                for (int offset = 0; offset + 1 < maxLength; offset++)
                {
                    hits.Add(ScoreSortKeyOffset(contexts, sortKeys, $"{inputLabel} {label} offset {offset} BE", offset, bigEndian: true, xorCounts: xorCounts, addCounts: addCounts));
                    hits.Add(ScoreSortKeyOffset(contexts, sortKeys, $"{inputLabel} {label} offset {offset} LE", offset, bigEndian: false, xorCounts: xorCounts, addCounts: addCounts));
                }
            }
        }

        var best = hits
            .Where(score => score.Evaluated > 0)
            .OrderByDescending(score => score.Exact)
            .ThenByDescending(score => score.BestXorCount)
            .ThenByDescending(score => score.BestAddCount)
            .ThenByDescending(score => score.Evaluated)
            .ThenBy(score => score.Name, StringComparer.Ordinal)
            .Take(12)
            .ToList();

        sb.AppendLine("CompareInfo sort-key offset sweep:")
            .AppendLine()
            .AppendLine("| Candidate | Exact | Best XOR | XOR constant | Best add | Add constant |")
            .AppendLine("|---|---:|---:|:---:|---:|:---:|");
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

        sb.AppendLine(CultureInfo.InvariantCulture, $"Wide affine tail models for {table.TableName}.DataIndex:")
            .AppendLine()
            .AppendLine("Trains on DAO-generated rows only; scores all rows, including the original fixture rows.")
            .AppendLine()
            .AppendLine("| Feature | Fit | Synthetic score | Original score | All score | Variables |")
            .AppendLine("|---|:---:|---:|---:|---:|---:|");
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

        bool fits = TryFitWideAffineBinaryModel(trainFeatures, trainTargets, variableCount, out BigInteger[]? coefficients);
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
        Encoding cp1252 = Cp1252Encoding;

        sb.AppendLine("Rolling polynomial solver:")
            .AppendLine()
            .AppendLine("Tests `h = h * multiplier + byte (mod 65536)` with every odd multiplier, solving the seed from the first row and requiring an exact match on all rows.")
            .AppendLine()
            .AppendLine("| Input | Matches | First hits |")
            .AppendLine("|---|---:|---|");

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

        // Sliding window: test full[508+k..] for k=2,4,6,8,10,12 (filling gaps between existing inputs).
        foreach (int offset in new[] { 2, 4, 6, 8, 10, 12 })
        {
            int absoluteStart = 508 + offset;
            RollingConstraint[] constraints = contexts
                .Select(context => new RollingConstraint(
                    SliceOrEmpty(context.FullKey, absoluteStart),
                    context.Row.AccessSuffix))
                .Where(constraint => constraint.Input.Length > 0)
                .ToArray();

            List<RollingPolynomialHit> hits = FindRollingPolynomialHits(constraints, maxHits: 8);
            string hitText = hits.Count == 0
                ? "-"
                : "`" + string.Join(" ", hits.Select(hit => $"m={hit.Multiplier:X4}/seed={hit.Seed:X4}")) + "`";
            sb.AppendLine(CultureInfo.InvariantCulture, $"| full[{absoluteStart}..] | {hits.Count} | {hitText} |");
        }

        // Also test odd offsets in case the boundary is not 2-byte aligned.
        foreach (int offset in new[] { 1, 3, 5, 7, 9, 11 })
        {
            int absoluteStart = 508 + offset;
            RollingConstraint[] constraints = contexts
                .Select(context => new RollingConstraint(
                    SliceOrEmpty(context.FullKey, absoluteStart),
                    context.Row.AccessSuffix))
                .Where(constraint => constraint.Input.Length > 0)
                .ToArray();

            List<RollingPolynomialHit> hits = FindRollingPolynomialHits(constraints, maxHits: 8);
            string hitText = hits.Count == 0
                ? "-"
                : "`" + string.Join(" ", hits.Select(hit => $"m={hit.Multiplier:X4}/seed={hit.Seed:X4}")) + "`";
            sb.AppendLine(CultureInfo.InvariantCulture, $"| full[{absoluteStart}..] | {hits.Count} | {hitText} |");
        }

        sb.AppendLine();
    }

    private static void AppendCrcDerivedInitSolverSummary(StringBuilder sb, SuffixCandidateContext[] contexts)
    {
        sb.AppendLine("CRC-16 derived-init solver (sliding window):")
            .AppendLine()
            .AppendLine("For each (polynomial, mode, start-offset), solves the constant from the shortest non-empty row, filters against the next two shortest rows, then verifies against all remaining rows.")
            .AppendLine("Tests tail slices full[508+k..] for k=0..12, plus full[0..] and full[1..].")
            .AppendLine(CultureInfo.InvariantCulture, $"Slices whose three-row rejection seed exceeds {CrcDerivedInitSolverMaxSeedBytes} bytes are skipped here; use `long-row-crc-sweep` for exhaustive heavy CRC windows.")
            .AppendLine();

        if (contexts.Length < 3)
        {
            sb.AppendLine("- insufficient rows for solver (need >= 3)")
                .AppendLine();
            return;
        }

        // Build slice definitions: (label, byte-extractor) so we can sweep arbitrary structural slices,
        // not just positional tails. New slices test "is the suffix a 16-bit CRC fingerprint of the
        // prefix bytes / aux block / raw source text?"
        var sliceDefs = new List<(string Label, Func<SuffixCandidateContext, byte[]> Extract)>
        {
            ("full[0..]", context => context.FullKey),
            ("full[1..]", context => SliceOrEmpty(context.FullKey, 1)),
        };
        for (int k = 0; k <= 12; k++)
        {
            int start = 508 + k;
            sliceDefs.Add(($"full[{start}..]", context => SliceOrEmpty(context.FullKey, start)));
        }

        // Prefix slices — fingerprint of the bytes that survived truncation.
        sliceDefs.Add(("full[..508]", context => context.FullKey.Length >= 508 ? context.FullKey[..508] : context.FullKey));
        sliceDefs.Add(("full[..510]", context => context.FullKey.Length >= 510 ? context.FullKey[..510] : context.FullKey));
        sliceDefs.Add(("full[1..508]", context => context.FullKey.Length >= 508 ? context.FullKey[1..508] : SliceOrEmpty(context.FullKey, 1)));

        // Aux block alone.
        sliceDefs.Add(("aux-only", context => BuildByteRuleInputs(context)[24]));

        // Raw source text encodings — short rows still produce non-empty slices.
        sliceDefs.Add(("text-utf16le", context => EncodeTextOrEmpty(context.Row.Text, Encoding.Unicode)));
        sliceDefs.Add(("text-cp1252", context => EncodeTextOrEmpty(context.Row.Text, Cp1252Encoding)));
        sliceDefs.Add(("text[255..]-utf16le", context => EncodeTextTailOrEmpty(context.Row.Text, 255, Encoding.Unicode)));
        sliceDefs.Add(("text[255..]-cp1252", context => EncodeTextTailOrEmpty(context.Row.Text, 255, Cp1252Encoding)));

        sb.AppendLine("| Slice | Hits | Details |")
            .AppendLine("|---|---:|---|");

        foreach ((string label, Func<SuffixCandidateContext, byte[]>? extract) in sliceDefs)
        {
            RollingConstraint[] constraints = contexts
                .Select(context => new RollingConstraint(
                    extract(context),
                    context.Row.AccessSuffix))
                .Where(static constraint => constraint.Input.Length > 0)
                .ToArray();
            if (constraints.Length < 3)
            {
                sb.AppendLine(CultureInfo.InvariantCulture, $"| {label} | 0 | (insufficient non-empty rows) |");
                continue;
            }

            RollingConstraint[] searchConstraints = constraints
                .OrderBy(static constraint => constraint.Input.Length)
                .ThenBy(static constraint => constraint.Target)
                .Take(3)
                .ToArray();
            byte[] data0 = searchConstraints[0].Input;
            byte[] data1 = searchConstraints[1].Input;
            byte[] data2 = searchConstraints[2].Input;
            int seedByteCount = data0.Length + data1.Length + data2.Length;
            if (seedByteCount > CrcDerivedInitSolverMaxSeedBytes)
            {
                sb.AppendLine(CultureInfo.InvariantCulture, $"| {label} | 0 | (skipped; seed bytes {seedByteCount}) |");
                continue;
            }

            ushort expected0 = searchConstraints[0].Target;
            ushort expected1 = searchConstraints[1].Target;
            ushort expected2 = searchConstraints[2].Target;

            int hitCount = 0;
            var hitDetails = new List<string>();
            var normalTable = new ushort[256];
            var reflectedTable = new ushort[256];

            for (int poly = 0; poly <= 0xFFFF; poly++)
            {
                ushort polyVal = (ushort)poly;
                ushort polyRef = ReflectU16(polyVal);
                BuildCrcTable(polyVal, normalTable, reflected: false);
                BuildCrcTable(polyRef, reflectedTable, reflected: true);

                // Test 4 modes: (refIn=false, refIn=true) x (refOut matches refIn, refOut differs).
                // For each, derive the constant from row0 and check rows 1-2.
                for (int mode = 0; mode < 4; mode++)
                {
                    bool refIn = (mode & 1) != 0;
                    bool refOut = (mode & 2) != 0;

                    ushort crc0 = CrcFullWithTable(data0, normalTable, reflectedTable, 0, 0, refIn, refOut);
                    ushort constant = (ushort)(expected0 ^ crc0);

                    ushort crc1 = CrcFullWithTable(data1, normalTable, reflectedTable, 0, 0, refIn, refOut);
                    if ((ushort)(crc1 ^ constant) != expected1)
                    {
                        continue;
                    }

                    ushort crc2 = CrcFullWithTable(data2, normalTable, reflectedTable, 0, 0, refIn, refOut);
                    if ((ushort)(crc2 ^ constant) != expected2)
                    {
                        continue;
                    }

                    // Passed 3 constraints. Validate against ALL contexts.
                    bool allMatch = true;
                    for (int i = 0; i < constraints.Length; i++)
                    {
                        byte[] dataI = constraints[i].Input;
                        ushort crcI = CrcFullWithTable(dataI, normalTable, reflectedTable, 0, 0, refIn, refOut);
                        if ((ushort)(crcI ^ constant) != constraints[i].Target)
                        {
                            allMatch = false;
                            break;
                        }
                    }

                    if (allMatch)
                    {
                        hitCount++;
                        if (hitDetails.Count < 4)
                        {
                            hitDetails.Add($"poly={polyVal:X4} refIn={refIn} refOut={refOut} C={constant:X4}");
                        }
                    }
                }
            }

            string details = hitCount == 0 ? "-" : "`" + string.Join("; ", hitDetails) + "`";
            sb.AppendLine(CultureInfo.InvariantCulture, $"| {label} | {hitCount} | {details} |");
        }

        sb.AppendLine();
    }

    /// <summary>
    /// CRC-32 derived-init affine solver. For each (polynomial, refIn, refOut, output-half), solves the
    /// 16-bit XOR constant from row[0], then validates that crc32(input) projected to the chosen half
    /// XOR constant equals every other row's target. Tests standard CRC-32 polynomials over the same
    /// structural slices as the CRC-16 solver. Catches patterns where the suffix is a 16-bit projection
    /// of a wider 32-bit hash with absorbed init/finalXor.
    /// </summary>
    /// <param name="sb">The string builder.</param>
    /// <param name="contexts">The contexts.</param>
    private static void AppendCrc32DerivedInitSolverSummary(StringBuilder sb, SuffixCandidateContext[] contexts)
    {
        sb.AppendLine("CRC-32 derived-init solver (standard polys, 16-bit projection):")
            .AppendLine()
            .AppendLine("For each (CRC-32 polynomial, refIn, refOut, half), solves the XOR constant from row[0] then verifies all other rows.")
            .AppendLine("Tests Ethernet/Castagnoli/Koopman/Q/D/Aixm/JAMCRC/MPEG-2 polys across structural slices.")
            .AppendLine();

        if (contexts.Length < 3)
        {
            sb.AppendLine("- insufficient rows for solver (need >= 3)")
                .AppendLine();
            return;
        }

        (string Name, uint Poly)[] polys =
        [
            ("Ethernet",   0x04C11DB7u),
            ("Castagnoli", 0x1EDC6F41u),
            ("Koopman",    0x741B8CD7u),
            ("CRC-32Q",    0x814141ABu),
            ("CRC-32D",    0xA833982Bu),
            ("Aixm",       0x814141ABu),
            ("MPEG-2",     0x04C11DB7u),
            ("JAMCRC",     0x04C11DB7u),
            ("BZIP2",      0x04C11DB7u),
            ("POSIX",      0x04C11DB7u),
        ];

        var sliceDefs = new List<(string Label, Func<SuffixCandidateContext, byte[]> Extract)>
        {
            ("full[0..]",            context => context.FullKey),
            ("full[1..]",            context => SliceOrEmpty(context.FullKey, 1)),
            ("full[508..]",          context => SliceOrEmpty(context.FullKey, 508)),
            ("full[510..]",          context => SliceOrEmpty(context.FullKey, 510)),
            ("full[..508]",          context => context.FullKey.Length >= 508 ? context.FullKey[..508] : context.FullKey),
            ("full[..510]",          context => context.FullKey.Length >= 510 ? context.FullKey[..510] : context.FullKey),
            ("aux-only",             context => BuildByteRuleInputs(context)[24]),
            ("text-utf16le",         context => EncodeTextOrEmpty(context.Row.Text, Encoding.Unicode)),
            ("text-cp1252",          context => EncodeTextOrEmpty(context.Row.Text, Cp1252Encoding)),
            ("text[255..]-utf16le",  context => EncodeTextTailOrEmpty(context.Row.Text, 255, Encoding.Unicode)),
            ("text[255..]-cp1252",   context => EncodeTextTailOrEmpty(context.Row.Text, 255, Cp1252Encoding)),
        };

        sb.AppendLine("| Slice | Hits | Details |")
            .AppendLine("|---|---:|---|");

        foreach ((string sliceLabel, Func<SuffixCandidateContext, byte[]>? extract) in sliceDefs)
        {
            byte[][] inputs = contexts.Select(extract).ToArray();
            if (inputs.Any(i => i.Length == 0))
            {
                sb.AppendLine(CultureInfo.InvariantCulture, $"| {sliceLabel} | 0 | (empty slice) |");
                continue;
            }

            ushort[] targets = contexts.Select(c => c.Row.AccessSuffix).ToArray();

            int hitCount = 0;
            var hitDetails = new List<string>();

            foreach ((string polyName, uint polyVal) in polys)
            {
                uint polyRef = ReflectU32(polyVal);
                for (int mode = 0; mode < 4; mode++)
                {
                    bool refIn = (mode & 1) != 0;
                    bool refOut = (mode & 2) != 0;

                    uint crc0Full = Crc32Generic(inputs[0], polyVal, polyRef, refIn, refOut);

                    for (int half = 0; half < 4; half++)
                    {
                        ushort crc0Half = ProjectU16(crc0Full, half);
                        ushort constant = (ushort)(targets[0] ^ crc0Half);

                        bool allMatch = true;
                        for (int i = 1; i < inputs.Length; i++)
                        {
                            uint crcI = Crc32Generic(inputs[i], polyVal, polyRef, refIn, refOut);
                            ushort crcHalf = ProjectU16(crcI, half);
                            if ((ushort)(crcHalf ^ constant) != targets[i])
                            {
                                allMatch = false;
                                break;
                            }
                        }

                        if (allMatch)
                        {
                            hitCount++;
                            if (hitDetails.Count < 4)
                            {
                                hitDetails.Add($"{polyName} refIn={refIn} refOut={refOut} half={half} C={constant:X4}");
                            }
                        }
                    }
                }
            }

            string details = hitCount == 0 ? "-" : "`" + string.Join("; ", hitDetails) + "`";
            sb.AppendLine(CultureInfo.InvariantCulture, $"| {sliceLabel} | {hitCount} | {details} |");
        }

        sb.AppendLine();
    }

    private static uint Crc32Generic(byte[] data, uint poly, uint polyRef, bool refIn, bool refOut)
    {
        uint crc = 0;
        if (refIn)
        {
            foreach (byte b in data)
            {
                crc ^= b;
                for (int i = 0; i < 8; i++)
                {
                    crc = (crc & 1) != 0 ? (crc >> 1) ^ polyRef : crc >> 1;
                }
            }
        }
        else
        {
            foreach (byte b in data)
            {
                crc ^= (uint)b << 24;
                for (int i = 0; i < 8; i++)
                {
                    crc = (crc & 0x80000000u) != 0 ? (crc << 1) ^ poly : crc << 1;
                }
            }
        }

        if (refIn != refOut)
        {
            crc = ReflectU32(crc);
        }

        return crc;
    }

    private static ushort ProjectU16(uint value, int half)
    {
        unchecked
        {
            return half switch
            {
                0 => (ushort)(value & 0xFFFFu),                       // low16
                1 => (ushort)((value >> 16) & 0xFFFFu),               // high16
                2 => ByteSwap((ushort)(value & 0xFFFFu)),             // low16 byte-swapped
                3 => ByteSwap((ushort)((value >> 16) & 0xFFFFu)),     // high16 byte-swapped
                _ => 0,
            };
        }
    }

    private static uint ReflectU32(uint value)
    {
        value = ((value & 0x55555555u) << 1) | ((value >> 1) & 0x55555555u);
        value = ((value & 0x33333333u) << 2) | ((value >> 2) & 0x33333333u);
        value = ((value & 0x0F0F0F0Fu) << 4) | ((value >> 4) & 0x0F0F0F0Fu);
        value = ((value & 0x00FF00FFu) << 8) | ((value >> 8) & 0x00FF00FFu);
        return (value << 16) | (value >> 16);
    }

    /// <summary>
    /// Per-position XOR contribution-table extractor. Exploits the established structural finding
    /// (phase + 9-byte window at absolute positions 503–511 + aux signature uniquely determines
    /// suffix; pair-matrix XOR decomposition fits 98.5%). Within each aux-signature group, pairs
    /// of rows that differ in exactly ONE window byte give a direct constraint:
    /// `T[pos][bA] XOR T[pos][bB] = suffix(A) XOR suffix(B)`. Accumulates these constraints;
    /// same (pos, {bA,bB}) producing different deltas across rows is hard evidence of
    /// nonlinearity localized to that position/byte.
    /// </summary>
    /// <param name="sb">The string builder.</param>
    /// <param name="contexts">The contexts.</param>
    private static void AppendLinearTableExtractorSummary(StringBuilder sb, SuffixCandidateContext[] contexts)
    {
        const int windowStart = 503;
        const int windowLength = 9;
        const int maxRowsPerGroupForPairs = 256; // O(n^2) pair enumeration cap.

        sb.AppendLine("Per-position XOR contribution-table extractor (9-byte window at positions 503–511):")
            .AppendLine()
            .AppendLine("Groups rows by aux signature. Within each group, enumerates row pairs (capped at 256 rows per group, ~32k pairs)")
            .AppendLine("that differ in exactly ONE byte of the 9-byte window (positions 503–511), giving direct linear constraint `T[pos][bA] XOR T[pos][bB] = dSuffix`.")
            .AppendLine("Contradictions (same (pos, byte-pair) producing different deltas) are direct evidence of nonlinearity at that position.")
            .AppendLine();

        Encoding cp1252 = Cp1252Encoding;
        var rows = contexts
            .Where(c => c.NormalizedFullKey.Length >= windowStart + windowLength)
            .Select(c =>
            {
                byte[][] aux = c.GetNormalizedInputCandidates(cp1252);
                return new
                {
                    AuxSig = Convert.ToHexString(aux[AuxInputCandidateIndex]),
                    Window = c.NormalizedFullKey[windowStart..(windowStart + windowLength)],
                    Suffix = c.Row.AccessSuffix,
                };
            })
            .GroupBy(r => r.AuxSig, StringComparer.Ordinal)
            .Where(g => g.Count() >= 2)
            .OrderByDescending(g => g.Count())
            .ToArray();

        if (rows.Length == 0)
        {
            sb.AppendLine("- no aux-signature groups with >= 2 rows")
                .AppendLine();
            return;
        }

        // Observations: (position, low_byte, high_byte) -> distinct observed XOR deltas.
        var observations = new Dictionary<(int Position, byte ByteLo, byte ByteHi), HashSet<ushort>>();
        long totalPairsExamined = 0;
        long singlePosPairs = 0;

        foreach (var group in rows)
        {
            var groupRows = group.Take(maxRowsPerGroupForPairs).ToArray();
            for (int i = 0; i < groupRows.Length; i++)
            {
                byte[] wi = groupRows[i].Window;
                ushort si = groupRows[i].Suffix;
                for (int j = i + 1; j < groupRows.Length; j++)
                {
                    totalPairsExamined++;
                    byte[] wj = groupRows[j].Window;
                    int diffCount = 0;
                    int diffPos = -1;
                    for (int p = 0; p < windowLength; p++)
                    {
                        if (wi[p] != wj[p])
                        {
                            diffCount++;
                            diffPos = p;
                            if (diffCount > 1)
                            {
                                break;
                            }
                        }
                    }

                    if (diffCount != 1)
                    {
                        continue;
                    }

                    singlePosPairs++;
                    ushort delta = (ushort)(si ^ groupRows[j].Suffix);
                    byte ba = wi[diffPos];
                    byte bb = wj[diffPos];
                    (int, byte, byte) key = ba < bb
                        ? (diffPos, ba, bb)
                        : (diffPos, bb, ba);
                    if (!observations.TryGetValue(key, out HashSet<ushort>? set))
                    {
                        set = [];
                        observations[key] = set;
                    }

                    set.Add(delta);
                }
            }
        }

        int contradictoryKeys = observations.Count(kv => kv.Value.Count > 1);
        var positionStats = new int[windowLength][]; // [pos] -> {distinct keys, total observations, contradictory keys}
        for (int p = 0; p < windowLength; p++)
        {
            positionStats[p] = new int[3];
        }

        foreach (KeyValuePair<(int Position, byte ByteLo, byte ByteHi), HashSet<ushort>> kv in observations)
        {
            int pos = kv.Key.Position;
            positionStats[pos][0]++;
            positionStats[pos][1] += kv.Value.Count;
            if (kv.Value.Count > 1)
            {
                positionStats[pos][2]++;
            }
        }

        sb.AppendLine(CultureInfo.InvariantCulture, $"- aux-signature groups (>=2 rows): {rows.Length}")
            .AppendLine(CultureInfo.InvariantCulture, $"- row pairs examined (within groups, capped): {totalPairsExamined}")
            .AppendLine(CultureInfo.InvariantCulture, $"- single-position-diff constraints collected: {singlePosPairs}")
            .AppendLine(CultureInfo.InvariantCulture, $"- distinct (position, byte-pair) keys: {observations.Count}")
            .AppendLine(CultureInfo.InvariantCulture, $"- CONTRADICTORY keys (same input → different delta): {contradictoryKeys}")
            .AppendLine()
            .AppendLine("Per-position coverage and contradictions:")
            .AppendLine("| Position | Byte index | Distinct byte-pairs | Distinct deltas observed | Contradictory pairs |")
            .AppendLine("|---:|---:|---:|---:|---:|");
        for (int i = 0; i < windowLength; i++)
        {
            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"| {i} | byte[{windowStart + i}] | {positionStats[i][0]} | {positionStats[i][1]} | {positionStats[i][2]} |");
        }

        sb.AppendLine();
        if (contradictoryKeys > 0)
        {
            sb.AppendLine("Sample contradictions (first 20, sorted by position):")
                .AppendLine("| Pos | ByteLo | ByteHi | Observed deltas |")
                .AppendLine("|---:|:---:|:---:|---|");
            foreach (KeyValuePair<(int Position, byte ByteLo, byte ByteHi), HashSet<ushort>> kv in observations
                .Where(o => o.Value.Count > 1)
                .OrderBy(o => o.Key.Position)
                .ThenBy(o => o.Key.ByteLo)
                .Take(20))
            {
                string deltas = string.Join(" ", kv.Value.Order().Select(d => $"`{d:X4}`"));
                sb.AppendLine(
                    CultureInfo.InvariantCulture,
                    $"| {kv.Key.Position} | `{kv.Key.ByteLo:X2}` | `{kv.Key.ByteHi:X2}` | {deltas} |");
            }

            sb.AppendLine();
        }
        else if (observations.Count > 0)
        {
            sb.AppendLine("NO contradictions detected: suffix appears to be a LINEAR function of the 9-byte window (positions 503–511) within each aux-signature group.")
                .AppendLine("Per-position XOR contribution tables can be reconstructed directly.")
                .AppendLine();
        }
    }

    /// <summary>
    /// Exhaustively tests rotate-left + add/XOR models: h = rotl16(h, k) OP byte for each byte in full[508..].
    /// Tests all 16 rotation amounts × 65536 init values × 2 operations (add, XOR) plus ESE-style rotl+add.
    /// </summary>
    /// <param name="sb">The string builder.</param>
    /// <param name="contexts">The contexts.</param>
    private static void AppendRotlFoldSolverSummary(StringBuilder sb, SuffixCandidateContext[] contexts)
    {
        sb.AppendLine("Rotate-fold solver (rotl16 + add/XOR, byte-at-a-time):")
            .AppendLine()
            .AppendLine("Tests `h = rotl16(h, k) + byte` and `h = rotl16(h, k) XOR byte` for each byte in full[508..end].")
            .AppendLine("Sweeps k=0..15, init=0..65535. Also tests `h = rotl16(h XOR byte, k)` and `h = rotl16(h + byte, k)` variants.")
            .AppendLine();

        if (contexts.Length < 3)
        {
            sb.AppendLine("- insufficient rows")
                .AppendLine();
            return;
        }

        // Build constraint data: for each context, the input is full[508..end].
        (byte[] Input, ushort Target)[] constraints = contexts
            .Select(c => (Input: SliceOrEmpty(c.FullKey, 508), Target: c.Row.AccessSuffix))
            .Where(c => c.Input.Length > 0)
            .ToArray();

        if (constraints.Length < 3)
        {
            sb.AppendLine("- insufficient non-empty slices")
                .AppendLine();
            return;
        }

        sb.AppendLine(CultureInfo.InvariantCulture, $"- rows tested: {constraints.Length}")
            .AppendLine(CultureInfo.InvariantCulture, $"- input lengths: {constraints.Min(c => c.Input.Length)}-{constraints.Max(c => c.Input.Length)} bytes");

        // Pre-compute targets for quick comparison.
        ushort target0 = constraints[0].Target;
        ushort target1 = constraints[1].Target;
        ushort target2 = constraints[2].Target;
        byte[] input0 = constraints[0].Input;
        byte[] input1 = constraints[1].Input;
        byte[] input2 = constraints[2].Input;

        int totalHits = 0;
        var hitDetails = new List<string>();

        // Test 4 model variants × 16 rotation amounts × 65536 inits.
        for (int variant = 0; variant < 4; variant++)
        {
            string variantName = variant switch
            {
                0 => "rotl(h,k)+b",
                1 => "rotl(h,k)^b",
                2 => "rotl(h^b,k)",
                3 => "rotl(h+b,k)",
                _ => "?",
            };

            for (int k = 0; k < 16; k++)
            {
                // For given variant and k, compute hash of input0 for all 65536 inits and find which match target0.
                // Then verify those against input1 and input2.
                // Optimization: compute hash(input0, init=0) and derive the init-dependent offset.
                // For variant 0 (rotl+add): h_final depends on init as rotl(init, k*n) + data_dependent_part.
                // Due to carries in addition, we can't easily separate init from data. Do full search.

                // But 65536 × 3 hashes per (variant, k) is fast enough.
                for (int init = 0; init <= 0xFFFF; init++)
                {
                    ushort h0 = RotlFoldHash(input0, (ushort)init, k, variant);
                    if (h0 != target0)
                    {
                        continue;
                    }

                    ushort h1 = RotlFoldHash(input1, (ushort)init, k, variant);
                    if (h1 != target1)
                    {
                        continue;
                    }

                    ushort h2 = RotlFoldHash(input2, (ushort)init, k, variant);
                    if (h2 != target2)
                    {
                        continue;
                    }

                    // Passed 3 constraints — verify all.
                    int matchCount = 3;
                    bool allMatch = true;
                    for (int i = 3; i < constraints.Length; i++)
                    {
                        ushort hi = RotlFoldHash(constraints[i].Input, (ushort)init, k, variant);
                        if (hi == constraints[i].Target)
                        {
                            matchCount++;
                        }
                        else
                        {
                            allMatch = false;
                        }
                    }

                    totalHits++;
                    if (hitDetails.Count < 8)
                    {
                        hitDetails.Add($"{variantName} k={k} init=0x{init:X4}: {matchCount}/{constraints.Length}{(allMatch ? " **ALL MATCH**" : string.Empty)}");
                    }
                }
            }
        }

        sb.AppendLine(CultureInfo.InvariantCulture, $"- total hits (3+ match): {totalHits}");
        foreach (string detail in hitDetails)
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"  - {detail}");
        }

        sb.AppendLine();
    }

    private static ushort RotlFoldHash(byte[] input, ushort init, int k, int variant)
    {
        ushort h = init;
        for (int i = 0; i < input.Length; i++)
        {
            byte b = input[i];
            h = variant switch
            {
                // h = rotl(h, k) + b
                0 => unchecked((ushort)(Rotl16(h, k) + b)),

                // h = rotl(h, k) ^ b
                1 => unchecked((ushort)(Rotl16(h, k) ^ b)),

                // h = rotl(h ^ b, k)
                2 => Rotl16(unchecked((ushort)(h ^ b)), k),

                // h = rotl(h + b, k)
                3 => Rotl16(unchecked((ushort)(h + b)), k),

                _ => throw new ArgumentOutOfRangeException(nameof(variant), variant, null),
            };
        }

        return h;
    }

    private static ushort Rotl16(ushort value, int shift)
    {
        shift &= 15;
        if (shift == 0)
        {
            return value;
        }

        return unchecked((ushort)((value << shift) | (value >> (16 - shift))));
    }

    /// <summary>
    /// Analyzes the XOR decomposition residuals for the pair matrix to characterize
    /// the non-linear coupling between byte positions. For each failing entry, reports
    /// the residual (actual XOR predicted) and checks for carry-induced patterns.
    /// Also tests whether the function could be addition-based with XOR lookup tables.
    /// </summary>
    /// <param name="sb">The string builder.</param>
    /// <param name="contexts">The contexts.</param>
    private static void AppendBitContributionMatrixSummary(StringBuilder sb, SuffixCandidateContext[] contexts)
    {
        sb.AppendLine("Bit contribution matrix and XOR residual analysis:")
            .AppendLine();

        if (contexts.Length < 3)
        {
            sb.AppendLine("- insufficient rows")
                .AppendLine();
            return;
        }

        // Build the byte-at-position data from the full[508..] inputs.
        (byte[] Input, ushort Target)[] inputs = contexts
            .Select(c => (Input: SliceOrEmpty(c.FullKey, 508), Target: c.Row.AccessSuffix))
            .Where(c => c.Input.Length >= 5)
            .ToArray();

        if (inputs.Length < 3)
        {
            sb.AppendLine("- insufficient 5+ byte inputs")
                .AppendLine();
            return;
        }

        sb.AppendLine(CultureInfo.InvariantCulture, $"- rows with 5+ byte inputs: {inputs.Length}");

        // Find a base row (first one with input starting 02 0E 02 01 00 = 'a','a' pattern).
        int baseRow = -1;
        for (int i = 0; i < inputs.Length; i++)
        {
            if (inputs[i].Input.Length >= 5
                && inputs[i].Input[0] == 0x02
                && inputs[i].Input[1] == 0x0E
                && inputs[i].Input[2] == 0x02
                && inputs[i].Input[3] == 0x01
                && inputs[i].Input[4] == 0x00)
            {
                baseRow = i;
                break;
            }
        }

        if (baseRow < 0)
        {
            sb.AppendLine("- base row (02 0E 02 01 00) not found")
                .AppendLine();
            return;
        }

        ushort baseSuffix = inputs[baseRow].Target;
        sb.AppendLine(CultureInfo.InvariantCulture, $"- base suffix: 0x{baseSuffix:X4}");

        // Group inputs by which positions differ from base.
        // For 5-byte inputs: positions 0,1,2 vary; positions 3,4 are constant (01 00).
        byte[] baseInput = inputs[baseRow].Input;

        // Extract per-position contribution tables.
        // Pos0-only changes: rows where only input[0] differs from base.
        var pos0Only = new Dictionary<byte, ushort>(); // byte value → suffix
        var pos1Only = new Dictionary<byte, ushort>();
        var pos2Only = new Dictionary<byte, ushort>();
        var pos01Change = new List<(byte B0, byte B1, ushort Suffix)>();
        var pos12Change = new List<(byte B1, byte B2, ushort Suffix)>();
        var pos012Change = new List<(byte B0, byte B1, byte B2, ushort Suffix)>();

        foreach ((byte[]? input, ushort target) in inputs)
        {
            if (input.Length < 5 || input[3] != 0x01 || input[4] != 0x00)
            {
                continue;
            }

            bool d0 = input[0] != baseInput[0];
            bool d1 = input[1] != baseInput[1];
            bool d2 = input[2] != baseInput[2];

            if (!d0 && !d1 && !d2)
            {
                continue; // base row
            }

            if (d0 && !d1 && !d2)
            {
                pos0Only.TryAdd(input[0], target);
            }
            else if (!d0 && d1 && !d2)
            {
                pos1Only.TryAdd(input[1], target);
            }
            else if (!d0 && !d1 && d2)
            {
                pos2Only.TryAdd(input[2], target);
            }

            if (d0 && d1 && !d2)
            {
                pos01Change.Add((input[0], input[1], target));
            }

            if (!d0 && d1 && d2)
            {
                pos12Change.Add((input[1], input[2], target));
            }

            if (d0 || d1 || d2)
            {
                pos012Change.Add((input[0], input[1], input[2], target));
            }
        }

        sb.AppendLine(CultureInfo.InvariantCulture, $"- pos0-only distinct values: {pos0Only.Count}")
            .AppendLine(CultureInfo.InvariantCulture, $"- pos1-only distinct values: {pos1Only.Count}")
            .AppendLine(CultureInfo.InvariantCulture, $"- pos2-only distinct values: {pos2Only.Count}")
            .AppendLine(CultureInfo.InvariantCulture, $"- pos0+pos1 change rows: {pos01Change.Count}")
            .AppendLine(CultureInfo.InvariantCulture, $"- pos1+pos2 change rows: {pos12Change.Count}")
            .AppendLine(CultureInfo.InvariantCulture, $"- total varying rows: {pos012Change.Count}")
            .AppendLine();

        // Build XOR contribution tables.
        // Contribution of value v at position p = suffix(v@p, base@others) XOR baseSuffix.
        var contrib0 = new Dictionary<byte, ushort>();
        foreach ((byte val, ushort suffix) in pos0Only)
        {
            contrib0[val] = (ushort)(suffix ^ baseSuffix);
        }

        var contrib1 = new Dictionary<byte, ushort>();
        foreach ((byte val, ushort suffix) in pos1Only)
        {
            contrib1[val] = (ushort)(suffix ^ baseSuffix);
        }

        var contrib2 = new Dictionary<byte, ushort>();
        foreach ((byte val, ushort suffix) in pos2Only)
        {
            contrib2[val] = (ushort)(suffix ^ baseSuffix);
        }

        // Display contribution tables.
        sb.AppendLine("Per-position XOR contribution tables (value → delta from base):")
            .AppendLine()
            .AppendLine("| Position | Values | Sample entries |")
            .AppendLine("|---|---:|---|")
            .AppendLine(CultureInfo.InvariantCulture, $"| pos[0] (full[508]) | {contrib0.Count} | {FormatContribSamples(contrib0)} |")
            .AppendLine(CultureInfo.InvariantCulture, $"| pos[1] (full[509]) | {contrib1.Count} | {FormatContribSamples(contrib1)} |")
            .AppendLine(CultureInfo.InvariantCulture, $"| pos[2] (full[510]) | {contrib2.Count} | {FormatContribSamples(contrib2)} |")
            .AppendLine();

        // XOR decomposition residual analysis.
        // For each row with multiple positions changed, predict using XOR of individual contributions.
        int xorPass = 0;
        int xorFail = 0;
        var residuals = new Dictionary<ushort, int>();
        var failExamples = new List<string>();

        foreach ((byte b0, byte b1, byte b2, ushort suffix) in pos012Change)
        {
            ushort c0 = (b0 != baseInput[0] && contrib0.TryGetValue(b0, out ushort v0)) ? v0 : (ushort)0;
            ushort c1 = (b1 != baseInput[1] && contrib1.TryGetValue(b1, out ushort v1)) ? v1 : (ushort)0;
            ushort c2 = (b2 != baseInput[2] && contrib2.TryGetValue(b2, out ushort v2)) ? v2 : (ushort)0;

            ushort predicted = (ushort)(baseSuffix ^ c0 ^ c1 ^ c2);
            if (predicted == suffix)
            {
                xorPass++;
            }
            else
            {
                xorFail++;
                ushort residual = (ushort)(suffix ^ predicted);
                residuals.TryGetValue(residual, out int count);
                residuals[residual] = count + 1;

                if (failExamples.Count < 20)
                {
                    failExamples.Add($"[{b0:X2},{b1:X2},{b2:X2}] actual=0x{suffix:X4} pred=0x{predicted:X4} residual=0x{residual:X4}");
                }
            }
        }

        sb.AppendLine("XOR decomposition residual analysis:")
            .AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"- pass: {xorPass}, fail: {xorFail}, total: {xorPass + xorFail}")
            .AppendLine(CultureInfo.InvariantCulture, $"- distinct residuals: {residuals.Count}")
            .AppendLine();

        if (residuals.Count > 0)
        {
            sb.AppendLine("Residual distribution:")
                .AppendLine()
                .AppendLine("| Residual | Count | Binary | Trailing zeros |")
                .AppendLine("|:---:|---:|---|---:|");
            foreach ((ushort residual, int count) in residuals.OrderByDescending(kvp => kvp.Value).ThenBy(kvp => kvp.Key).Take(20))
            {
                int tz = BitOperations.TrailingZeroCount(residual);
                sb.AppendLine(CultureInfo.InvariantCulture, $"| `0x{residual:X4}` | {count} | `{Convert.ToString(residual, 2).PadLeft(16, '0')}` | {tz} |");
            }

            sb.AppendLine();

            // Check if all residuals are powers of 2 (single carry bit flip).
            bool allPow2 = residuals.Keys.All(r => BitOperations.PopCount(r) == 1);
            bool allLowBits = residuals.Keys.All(r => (r & 0xFF00) == 0);
            bool allHighBits = residuals.Keys.All(r => (r & 0x00FF) == 0);
            int maxPopCount = residuals.Keys.Max(r => BitOperations.PopCount(r));
            int minPopCount = residuals.Keys.Min(r => BitOperations.PopCount(r));

            // Check if residuals match carry patterns from (A+B) vs (A XOR B).
            // For addition: a+b = (a XOR b) XOR carry_chain.
            // Carry residual at bit k means a carry propagated from bit k-1.
            sb.AppendLine(CultureInfo.InvariantCulture, $"- all single-bit (power of 2): {allPow2}")
                .AppendLine(CultureInfo.InvariantCulture, $"- all in low byte: {allLowBits}")
                .AppendLine(CultureInfo.InvariantCulture, $"- all in high byte: {allHighBits}")
                .AppendLine(CultureInfo.InvariantCulture, $"- popcount range: {minPopCount}-{maxPopCount}")
                .AppendLine()
                .AppendLine("Failing entries (first 20):")
                .AppendLine()
                .AppendLine("| Input [pos0,pos1,pos2] | Actual | Predicted | Residual |")
                .AppendLine("|---|:---:|:---:|:---:|");
            foreach (string example in failExamples)
            {
                // Parse the example string back (it's already formatted).
                sb.AppendLine(CultureInfo.InvariantCulture, $"| {example} |");
            }

            sb.AppendLine();
        }

        // Test addition-based model: suffix = (T0[b0] + T1[b1] + T2[b2] + C) mod 65536.
        // For single-position changes: T0[x] = (suffix_of_x - baseSuffix) mod 65536, etc.
        // Then verify multi-position changes.
        var addContrib0 = new Dictionary<byte, ushort>();
        foreach ((byte val, ushort suffix) in pos0Only)
        {
            addContrib0[val] = unchecked((ushort)(suffix - baseSuffix));
        }

        var addContrib1 = new Dictionary<byte, ushort>();
        foreach ((byte val, ushort suffix) in pos1Only)
        {
            addContrib1[val] = unchecked((ushort)(suffix - baseSuffix));
        }

        var addContrib2 = new Dictionary<byte, ushort>();
        foreach ((byte val, ushort suffix) in pos2Only)
        {
            addContrib2[val] = unchecked((ushort)(suffix - baseSuffix));
        }

        int addPass = 0;
        int addFail = 0;
        foreach ((byte b0, byte b1, byte b2, ushort suffix) in pos012Change)
        {
            ushort ac0 = (b0 != baseInput[0] && addContrib0.TryGetValue(b0, out ushort av0)) ? av0 : (ushort)0;
            ushort ac1 = (b1 != baseInput[1] && addContrib1.TryGetValue(b1, out ushort av1)) ? av1 : (ushort)0;
            ushort ac2 = (b2 != baseInput[2] && addContrib2.TryGetValue(b2, out ushort av2)) ? av2 : (ushort)0;

            ushort predicted = unchecked((ushort)(baseSuffix + ac0 + ac1 + ac2));
            if (predicted == suffix)
            {
                addPass++;
            }
            else
            {
                addFail++;
            }
        }

        sb.AppendLine("Addition-based separable model (suffix = T0[b0] + T1[b1] + T2[b2] + C mod 65536):")
            .AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"- pass: {addPass}, fail: {addFail}, total: {addPass + addFail}")
            .AppendLine();

        // Test hybrid: suffix = T0[b0] XOR (T1[b1] + T2[b2]).
        // Derive: T0[x] = suffix(x, base1, base2) XOR baseSuffix (from pos0-only).
        // For pos1/pos2 combined: suffix = T0[base0] XOR (T1[b1] + T2[b2]) = 0 XOR (T1[b1] + T2[b2])
        //   → suffix(base0, b1, b2) = T0[base0] XOR (T1[b1] + T2[b2]) = (T1[b1] + T2[b2]) XOR T0[base0].
        // We know baseSuffix = T0[base0] XOR (T1[base1] + T2[base2]).
        // And suffix(base0, b1, base2) = T0[base0] XOR (T1[b1] + T2[base2]).
        // So delta1[b1] = suffix(base0, b1, base2) XOR baseSuffix = (T1[b1] + T2[base2]) XOR (T1[base1] + T2[base2])
        //   = (T1[b1] - T1[base1]) contributed through addition.
        // For the full prediction: suffix(b0, b1, b2) = T0[b0] XOR (T1[b1] + T2[b2]).
        // suffix(b0, base1, base2) = T0[b0] XOR (T1[base1] + T2[base2]).
        // So T0[b0] = suffix(b0, base1, base2) XOR (T1[base1] + T2[base2]).
        // We don't know T1[base1] + T2[base2] directly. Let K = T1[base1] + T2[base2].
        // baseSuffix = T0[base0] XOR K → K = baseSuffix XOR T0[base0].
        // And T0[base0] is defined relative to itself (=0 in our XOR contribution table).
        // Let's set T0[base0] = 0. Then K = baseSuffix.
        // T0[b0] = suffix(b0, base1, base2) XOR baseSuffix = contrib0_xor[b0] (same as XOR contribution).
        // T1[b1] + T2[base2] = suffix(base0, b1, base2) XOR T0[base0] = suffix(base0, b1, base2) XOR 0
        //   → T1[b1] + T2[base2] = suffix(base0, b1, base2). Hmm, but we set T0[base0]=0...
        // Actually suffix(base0, b1, base2) = T0[base0] XOR (T1[b1] + T2[base2]) = 0 XOR (T1[b1] + T2[base2])
        //   = T1[b1] + T2[base2].
        // And suffix(base0, base1, base2) = T1[base1] + T2[base2] = baseSuffix (since T0[base0]=0 XOR ... = baseSuffix means K=baseSuffix).
        // Wait: baseSuffix = T0[base0] XOR (T1[base1] + T2[base2]) = 0 XOR (T1[base1] + T2[base2]) = T1[base1] + T2[base2].
        // So K = baseSuffix. And:
        // T1[b1] + T2[base2] = suffix(base0, b1, base2).
        // → T1[b1] = suffix(base0, b1, base2) - T2[base2].
        // T1[base1] + T2[b2] = suffix(base0, base1, b2).
        // → T2[b2] = suffix(base0, base1, b2) - T1[base1].
        // And T1[base1] + T2[base2] = baseSuffix.
        // Set T2[base2] = 0 → T1[base1] = baseSuffix.
        // Then T1[b1] = suffix(base0, b1, base2) - 0 = suffix(base0, b1, base2) = pos1Only[b1].
        //   Wait: pos1Only stores suffixes where only pos1 differs. suffix(base0, b1, base2) = pos1Only[b1].
        // T2[b2] = suffix(base0, base1, b2) - baseSuffix = pos2Only[b2] - baseSuffix.
        //   Wait: pos2Only stores suffixes where only pos2 differs. suffix(base0, base1, b2) = pos2Only[b2].
        //   T2[b2] = pos2Only[b2] - T1[base1] = pos2Only[b2] - baseSuffix.

        // Prediction for any (b0, b1, b2):
        //   suffix = T0[b0] XOR (T1[b1] + T2[b2])
        //   = contrib0_xor[b0] XOR (pos1Suffix[b1] + (pos2Suffix[b2] - baseSuffix))
        //   where contrib0_xor is the XOR contribution of b0 (= suffix(b0,base1,base2) XOR baseSuffix).

        int hybridXorAddPass = 0;
        int hybridXorAddFail = 0;
        var hybridFailExamples = new List<string>();

        foreach ((byte b0, byte b1, byte b2, ushort suffix) in pos012Change)
        {
            // T0[b0]:
            ushort t0;
            if (b0 == baseInput[0])
            {
                t0 = 0;
            }
            else if (!contrib0.TryGetValue(b0, out t0))
            {
                continue; // skip if we don't have a single-position reference
            }

            // T1[b1]:
            ushort t1;
            if (b1 == baseInput[1])
            {
                t1 = baseSuffix;
            }
            else if (pos1Only.TryGetValue(b1, out ushort s1))
            {
                t1 = s1;
            }
            else
            {
                continue;
            }

            // T2[b2]:
            ushort t2;
            if (b2 == baseInput[2])
            {
                t2 = 0;
            }
            else if (pos2Only.TryGetValue(b2, out ushort s2))
            {
                t2 = unchecked((ushort)(s2 - baseSuffix));
            }
            else
            {
                continue;
            }

            ushort predicted = unchecked((ushort)(t0 ^ (t1 + t2)));
            if (predicted == suffix)
            {
                hybridXorAddPass++;
            }
            else
            {
                hybridXorAddFail++;
                if (hybridFailExamples.Count < 10)
                {
                    ushort residual = (ushort)(suffix ^ predicted);
                    hybridFailExamples.Add($"[{b0:X2},{b1:X2},{b2:X2}] T0=0x{t0:X4} T1=0x{t1:X4} T2=0x{t2:X4} actual=0x{suffix:X4} pred=0x{predicted:X4} res=0x{residual:X4}");
                }
            }
        }

        sb.AppendLine("Hybrid model A: suffix = T0[b0] XOR (T1[b1] + T2[b2]):")
            .AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"- pass: {hybridXorAddPass}, fail: {hybridXorAddFail}, total: {hybridXorAddPass + hybridXorAddFail}");
        if (hybridFailExamples.Count > 0)
        {
            sb.AppendLine();
            foreach (string ex in hybridFailExamples)
            {
                sb.AppendLine(CultureInfo.InvariantCulture, $"  - {ex}");
            }
        }

        sb.AppendLine();

        // Test hybrid B: suffix = (T0[b0] + T1[b1]) XOR T2[b2].
        // Set T0[base0]=0, T1[base1]=0, T2[base2]=0.
        // baseSuffix = (T0[base0] + T1[base1]) XOR T2[base2] = 0 XOR 0 = 0. But baseSuffix = 0x27A5 ≠ 0!
        // So we need a constant: suffix = (T0[b0] + T1[b1]) XOR T2[b2] XOR C.
        // baseSuffix = (0 + 0) XOR 0 XOR C = C → C = baseSuffix.
        // suffix(b0, base1, base2) = (T0[b0] + 0) XOR 0 XOR baseSuffix = T0[b0] XOR baseSuffix.
        // → T0[b0] = suffix(b0, base1, base2) XOR baseSuffix = contrib0_xor[b0].
        // suffix(base0, b1, base2) = (0 + T1[b1]) XOR 0 XOR baseSuffix = T1[b1] XOR baseSuffix.
        // → T1[b1] = suffix(base0, b1, base2) XOR baseSuffix = contrib1_xor[b1].
        // suffix(base0, base1, b2) = (0 + 0) XOR T2[b2] XOR baseSuffix = T2[b2] XOR baseSuffix.
        // → T2[b2] = suffix(base0, base1, b2) XOR baseSuffix = contrib2_xor[b2].
        // Prediction: suffix = (contrib0[b0] + contrib1[b1]) XOR contrib2[b2] XOR baseSuffix.

        int hybridAddXorPass = 0;
        int hybridAddXorFail = 0;
        var hybridBFailExamples = new List<string>();

        foreach ((byte b0, byte b1, byte b2, ushort suffix) in pos012Change)
        {
            ushort c0 = (b0 != baseInput[0] && contrib0.TryGetValue(b0, out ushort cv0)) ? cv0 : (ushort)0;
            ushort c1 = (b1 != baseInput[1] && contrib1.TryGetValue(b1, out ushort cv1)) ? cv1 : (ushort)0;
            ushort c2 = (b2 != baseInput[2] && contrib2.TryGetValue(b2, out ushort cv2)) ? cv2 : (ushort)0;

            ushort predicted = unchecked((ushort)((c0 + c1) ^ c2 ^ baseSuffix));
            if (predicted == suffix)
            {
                hybridAddXorPass++;
            }
            else
            {
                hybridAddXorFail++;
                if (hybridBFailExamples.Count < 10)
                {
                    ushort residual = (ushort)(suffix ^ predicted);
                    hybridBFailExamples.Add($"[{b0:X2},{b1:X2},{b2:X2}] c0=0x{c0:X4} c1=0x{c1:X4} c2=0x{c2:X4} actual=0x{suffix:X4} pred=0x{predicted:X4} res=0x{residual:X4}");
                }
            }
        }

        sb.AppendLine("Hybrid model B: suffix = (T0[b0] + T1[b1]) XOR T2[b2] XOR C:")
            .AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"- pass: {hybridAddXorPass}, fail: {hybridAddXorFail}, total: {hybridAddXorPass + hybridAddXorFail}");
        if (hybridBFailExamples.Count > 0)
        {
            sb.AppendLine();
            foreach (string ex in hybridBFailExamples)
            {
                sb.AppendLine(CultureInfo.InvariantCulture, $"  - {ex}");
            }
        }

        sb.AppendLine();

        // Test hybrid C: suffix = T0[b0] XOR T1[b1] XOR (T2[b2] + T3_const) where b2 may interact with b1.
        // Actually test: suffix = (T0[b0] XOR T1[b1]) + T2[b2] + C (addition wrapping XOR inputs).
        int hybridXorPlusPass = 0;
        int hybridXorPlusFail = 0;

        // For this model: suffix(b0, base1, base2) = (T0[b0] XOR T1[base1]) + T2[base2] + C.
        // Let T1[base1]=0, T2[base2]=0: suffix(b0,..) = T0[b0] + C.
        // baseSuffix = T0[base0] + C. Let T0[base0]=0: C = baseSuffix.
        // T0[b0] = suffix(b0,..) - baseSuffix = addContrib0[b0].
        // suffix(base0, b1, base2) = (0 XOR T1[b1]) + 0 + baseSuffix = T1[b1] + baseSuffix.
        // T1[b1] = suffix(..,b1,..) - baseSuffix = addContrib1[b1].
        // suffix(base0, base1, b2) = (0 XOR 0) + T2[b2] + baseSuffix = T2[b2] + baseSuffix.
        // T2[b2] = suffix(..,b2) - baseSuffix = addContrib2[b2].
        // Prediction: suffix = (addC0[b0] XOR addC1[b1]) + addC2[b2] + baseSuffix.

        foreach ((byte b0, byte b1, byte b2, ushort suffix) in pos012Change)
        {
            ushort ac0 = (b0 != baseInput[0] && addContrib0.TryGetValue(b0, out ushort av0)) ? av0 : (ushort)0;
            ushort ac1 = (b1 != baseInput[1] && addContrib1.TryGetValue(b1, out ushort av1)) ? av1 : (ushort)0;
            ushort ac2 = (b2 != baseInput[2] && addContrib2.TryGetValue(b2, out ushort av2)) ? av2 : (ushort)0;

            ushort predicted = unchecked((ushort)((ac0 ^ ac1) + ac2 + baseSuffix));
            if (predicted == suffix)
            {
                hybridXorPlusPass++;
            }
            else
            {
                hybridXorPlusFail++;
            }
        }

        sb.AppendLine("Hybrid model C: suffix = (T0[b0] XOR T1[b1]) + T2[b2] + C:")
            .AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"- pass: {hybridXorPlusPass}, fail: {hybridXorPlusFail}, total: {hybridXorPlusPass + hybridXorPlusFail}")
            .AppendLine();

        // Test all 6 orderings of 2-op hybrid: {+, ^} applied in 2 positions.
        // Already tested: A = XOR(+), B = (+)XOR, C = (XOR)+.
        // Additional: D = (+XOR), E = XOR+XOR (=just XOR), F = +(+) (=just +).
        // D: suffix = T0[b0] + (T1[b1] XOR T2[b2]) + C.
        int hybridDPass = 0;
        int hybridDFail = 0;

        foreach ((byte b0, byte b1, byte b2, ushort suffix) in pos012Change)
        {
            ushort ac0 = (b0 != baseInput[0] && addContrib0.TryGetValue(b0, out ushort av0)) ? av0 : (ushort)0;

            // For this model: T1[b1] XOR T2[b2] contribution.
            // suffix(base0, b1, base2) = 0 + (T1[b1] XOR T2[base2]) + baseSuffix.
            // If T2[base2]=0: T1[b1] = suffix(..,b1,..) - baseSuffix = addContrib1[b1].
            // suffix(base0, base1, b2) = 0 + (T1[base1] XOR T2[b2]) + baseSuffix.
            // T1[base1] XOR T2[b2] = suffix(..) - baseSuffix = addContrib2[b2].
            // If T1[base1]=0: T2[b2] = addContrib2[b2].
            // Full: suffix = addC0[b0] + (addC1[b1] XOR addC2[b2]) + baseSuffix.
            ushort ac1 = (b1 != baseInput[1] && addContrib1.TryGetValue(b1, out ushort av1)) ? av1 : (ushort)0;
            ushort ac2 = (b2 != baseInput[2] && addContrib2.TryGetValue(b2, out ushort av2)) ? av2 : (ushort)0;

            ushort predicted = unchecked((ushort)(ac0 + (ac1 ^ ac2) + baseSuffix));
            if (predicted == suffix)
            {
                hybridDPass++;
            }
            else
            {
                hybridDFail++;
            }
        }

        sb.AppendLine("Hybrid model D: suffix = T0[b0] + (T1[b1] XOR T2[b2]) + C:")
            .AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"- pass: {hybridDPass}, fail: {hybridDFail}, total: {hybridDPass + hybridDFail}")
            .AppendLine();
    }

    private static string FormatContribSamples(Dictionary<byte, ushort> contrib) => string.Join(" ", contrib
            .OrderBy(kvp => kvp.Key)
            .Take(8)
            .Select(kvp => $"0x{kvp.Key:X2}→0x{kvp.Value:X4}"));

    /// <summary>
    /// Tests whether the suffix is a GF(2^16) linear accumulator with a per-byte lookup table:
    /// suffix = XOR(W[byte_i] * alpha^i) in GF(2^16) mod P.
    /// Uses cross-multiplication constraints from the pair matrix to derive candidate P values.
    /// </summary>
    /// <param name="sb">The string builder.</param>
    /// <param name="table">The table.</param>
    /// <param name="matrixStart">The matrix start.</param>
    private static void AppendGf2CrossMultiplicationSolverSummary(StringBuilder sb, SuffixPatternTable table, int matrixStart)
    {
        sb.AppendLine("GF(2^16) cross-multiplication solver:")
            .AppendLine()
            .AppendLine("Tests whether pair matrix deltas are consistent with `suffix = XOR(W[b_i] * alpha^i)` in GF(2^16) mod P.")
            .AppendLine("Derives constraints on P from cross-products of row/column contributions.")
            .AppendLine();

        const int size = DaoLabAlphabetLength;
        var suffixes = new ushort[size * size];
        var present = new bool[size * size];

        foreach (SuffixPatternRow row in table.Rows)
        {
            if (row.Seed is not int seed)
            {
                continue;
            }

            int pair = seed - matrixStart;
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
        if (baseIndex < 0 || !present[(baseIndex * size) + baseIndex])
        {
            sb.AppendLine("- base entry 'a,a' not present; skipping.")
                .AppendLine();
            return;
        }

        ushort baseValue = suffixes[(baseIndex * size) + baseIndex];

        // Collect row contributions (varying char[253] at window offsets 2-3) and column contributions (varying char[254] at window offsets 4-5).
        var rowContribs = new List<(int CharIndex, ushort Delta)>();
        var colContribs = new List<(int CharIndex, ushort Delta)>();
        for (int i = 0; i < size; i++)
        {
            int rowIdx = (i * size) + baseIndex;
            int colIdx = (baseIndex * size) + i;
            if (present[rowIdx])
            {
                ushort rc = (ushort)(suffixes[rowIdx] ^ baseValue);
                if (rc != 0)
                {
                    rowContribs.Add((i, Delta: rc));
                }
            }

            if (present[colIdx])
            {
                ushort cc = (ushort)(suffixes[colIdx] ^ baseValue);
                if (cc != 0)
                {
                    colContribs.Add((i, Delta: cc));
                }
            }
        }

        sb.AppendLine(CultureInfo.InvariantCulture, $"- non-zero row contributions: {rowContribs.Count}")
            .AppendLine(CultureInfo.InvariantCulture, $"- non-zero column contributions: {colContribs.Count}");

        if (rowContribs.Count < 2 || colContribs.Count < 2)
        {
            sb.AppendLine("- insufficient non-zero contributions for cross-multiplication.")
                .AppendLine();
            return;
        }

        // For the model to hold: GF_mul(row[i], col[j], P) == GF_mul(row[j], col[i], P)
        // for all i, j where both row and col are non-zero AND use the SAME underlying byte delta.
        // Since row and col contributions for the SAME character come from the same W-delta
        // (char253 2nd byte = char254 2nd byte for same char), we need chars that appear in BOTH lists.
        var sharedChars = rowContribs
            .Where(r => colContribs.Any(c => c.CharIndex == r.CharIndex))
            .Select(r => (r.CharIndex, Row: r.Delta, Col: colContribs.First(c => c.CharIndex == r.CharIndex).Delta))
            .ToList();

        sb.AppendLine(CultureInfo.InvariantCulture, $"- chars with both row and col contributions: {sharedChars.Count}");

        if (sharedChars.Count < 2)
        {
            sb.AppendLine("- insufficient shared chars for cross-multiplication constraints.")
                .AppendLine();
            return;
        }

        // Build constraint polynomials: for each pair (i, j) of shared chars,
        // R_ij = clmul(row[i], col[j]) XOR clmul(row[j], col[i])
        // P must divide R_ij.
        // Start by computing GCD of the first few R values.
        uint gcdPoly = 0;
        int constraintCount = 0;
        var constraintDetails = new List<string>();

        for (int i = 0; i < sharedChars.Count && i < 8; i++)
        {
            for (int j = i + 1; j < sharedChars.Count && j < 8; j++)
            {
                uint prod1 = CarrylessMultiply16(sharedChars[i].Row, sharedChars[j].Col);
                uint prod2 = CarrylessMultiply16(sharedChars[j].Row, sharedChars[i].Col);
                uint r = prod1 ^ prod2;
                if (r == 0)
                {
                    // Trivially satisfied for all P — doesn't constrain.
                    continue;
                }

                constraintCount++;
                if (gcdPoly == 0)
                {
                    gcdPoly = r;
                }
                else
                {
                    gcdPoly = Gf2PolyGcd(gcdPoly, r);
                }

                if (constraintCount <= 3)
                {
                    constraintDetails.Add($"R({DaoLabAlphabet[sharedChars[i].CharIndex]},{DaoLabAlphabet[sharedChars[j].CharIndex]})=0x{r:X8}");
                }
            }
        }

        sb.AppendLine(CultureInfo.InvariantCulture, $"- cross-multiplication constraints generated: {constraintCount}")
            .AppendLine(CultureInfo.InvariantCulture, $"- GCD polynomial (hex): 0x{gcdPoly:X8} (degree {Gf2PolyDegree(gcdPoly)})");
        foreach (string detail in constraintDetails)
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"  - {detail}");
        }

        // Find all degree-16 factors of the GCD polynomial.
        var candidatePolynomials = new List<ushort>();
        if (Gf2PolyDegree(gcdPoly) >= 16)
        {
            // Try all degree-16 polynomials (x^16 + p) as divisors of gcdPoly.
            for (int p = 0; p <= 0xFFFF; p++)
            {
                uint divisor = 0x10000u | (uint)p; // x^16 + p
                if (Gf2PolyRemainder(gcdPoly, divisor) == 0)
                {
                    candidatePolynomials.Add((ushort)p);
                }
            }
        }

        sb.AppendLine(CultureInfo.InvariantCulture, $"- degree-16 factor candidates: {candidatePolynomials.Count}");
        if (candidatePolynomials.Count is > 0 and <= 16)
        {
            foreach (ushort p in candidatePolynomials)
            {
                sb.AppendLine(CultureInfo.InvariantCulture, $"  - P = x^16 + 0x{p:X4}");
            }
        }

        // For each candidate P, verify against ALL constraint pairs (not just those used for GCD).
        var validatedPolynomials = new List<ushort>();
        foreach (ushort p in candidatePolynomials)
        {
            uint pFull = 0x10000u | p;
            bool allMatch = true;
            for (int i = 0; i < sharedChars.Count && allMatch; i++)
            {
                for (int j = i + 1; j < sharedChars.Count && allMatch; j++)
                {
                    ushort lhs = Gf2Multiply(sharedChars[i].Row, sharedChars[j].Col, pFull);
                    ushort rhs = Gf2Multiply(sharedChars[j].Row, sharedChars[i].Col, pFull);
                    if (lhs != rhs)
                    {
                        allMatch = false;
                    }
                }
            }

            if (allMatch)
            {
                validatedPolynomials.Add(p);
            }
        }

        sb.AppendLine(CultureInfo.InvariantCulture, $"- validated against all shared chars: {validatedPolynomials.Count}");
        foreach (ushort p in validatedPolynomials)
        {
            // Compute alpha^2 = col[0] / row[0] in GF(2^16) mod (x^16 + p)
            uint pFull = 0x10000u | p;
            ushort alpha2 = Gf2Divide(sharedChars[0].Col, sharedChars[0].Row, pFull);
            sb.AppendLine(CultureInfo.InvariantCulture, $"  - P = 0x{p:X4}, alpha^2 = 0x{alpha2:X4}");
        }

        sb.AppendLine();
    }

    /// <summary>Carryless (polynomial) multiplication of two 16-bit values → 32-bit result.</summary>
    /// <param name="a">The first value to compare.</param>
    /// <param name="b">The second value or byte buffer.</param>
    private static uint CarrylessMultiply16(ushort a, ushort b)
    {
        uint result = 0;
        uint bb = b;
        for (int i = 0; i < 16; i++)
        {
            if (((a >> i) & 1) != 0)
            {
                result ^= bb << i;
            }
        }

        return result;
    }

    /// <summary>GCD of two GF(2) polynomials (represented as bit vectors).</summary>
    /// <param name="a">The first value to compare.</param>
    /// <param name="b">The second value or byte buffer.</param>
    private static uint Gf2PolyGcd(uint a, uint b)
    {
        while (b != 0)
        {
            uint r = Gf2PolyRemainder(a, b);
            a = b;
            b = r;
        }

        return a;
    }

    /// <summary>Remainder of polynomial a divided by b in GF(2)[x].</summary>
    /// <param name="a">The first value to compare.</param>
    /// <param name="b">The second value or byte buffer.</param>
    private static uint Gf2PolyRemainder(uint a, uint b)
    {
        int degA = Gf2PolyDegree(a);
        int degB = Gf2PolyDegree(b);
        while (degA >= degB && a != 0)
        {
            a ^= b << (degA - degB);
            degA = Gf2PolyDegree(a);
        }

        return a;
    }

    /// <summary>Degree of a GF(2) polynomial (highest set bit position).</summary>
    /// <param name="p">The page or position value.</param>
    private static int Gf2PolyDegree(uint p)
    {
        if (p == 0)
        {
            return -1;
        }

        return 31 - BitOperations.LeadingZeroCount(p);
    }

    /// <summary>Multiply two elements in GF(2^16) mod P (P given as full 17-bit polynomial).</summary>
    /// <param name="a">The first value to compare.</param>
    /// <param name="b">The second value or byte buffer.</param>
    /// <param name="p">The page or position value.</param>
    private static ushort Gf2Multiply(ushort a, ushort b, uint p)
    {
        uint product = CarrylessMultiply16(a, b);
        return (ushort)Gf2PolyRemainder(product, p);
    }

    /// <summary>Divide a by b in GF(2^16) mod P: returns a * b^(-1).</summary>
    /// <param name="a">The first value to compare.</param>
    /// <param name="b">The second value or byte buffer.</param>
    /// <param name="p">The page or position value.</param>
    private static ushort Gf2Divide(ushort a, ushort b, uint p)
    {
        ushort bInv = Gf2Inverse(b, p);
        return Gf2Multiply(a, bInv, p);
    }

    /// <summary>Multiplicative inverse in GF(2^16) via extended Euclidean algorithm.</summary>
    /// <param name="a">The first value to compare.</param>
    /// <param name="p">The page or position value.</param>
    private static ushort Gf2Inverse(ushort a, uint p)
    {
        if (a == 0)
        {
            return 0;
        }

        // Extended GCD in GF(2)[x]: find t such that a * t ≡ 1 mod P.
        uint r0 = p;
        uint r1 = a;
        uint t0 = 0;
        uint t1 = 1;
        while (r1 != 0)
        {
            int degR0 = Gf2PolyDegree(r0);
            int degR1 = Gf2PolyDegree(r1);
            if (degR0 < degR1)
            {
                (r0, r1) = (r1, r0);
                (t0, t1) = (t1, t0);
                continue;
            }

            int shift = degR0 - degR1;
            r0 ^= r1 << shift;
            t0 ^= t1 << shift;
        }

        // r0 should be 1 (GCD = 1 for irreducible P, or if a is coprime with P).
        return (ushort)Gf2PolyRemainder(t0, p);
    }

    private static List<CandidateRule> BuildSuffixCandidateRules()
    {
        var rules = new List<CandidateRule>();
        foreach ((string label, int inputIndex) in BuildByteInputs())
        {
            rules.Add(new CandidateRule($"{label} word BE", context => ReadWordOrNull(context.GetBytes(inputIndex), 0, bigEndian: true)));
            rules.Add(new CandidateRule($"{label} word LE", context => ReadWordOrNull(context.GetBytes(inputIndex), 0, bigEndian: false)));
            rules.Add(new CandidateRule($"{label} FNV1a16", context => Fnv1A16(context.GetBytes(inputIndex))));
            AddHash32WordRules(rules, label, "FNV1a32", context => Fnv1A32(context.GetBytes(inputIndex)));
            rules.Add(new CandidateRule($"{label} DJB2-16", context => Djb216(context.GetBytes(inputIndex))));
            AddHash32WordRules(rules, label, "DJB2-32", context => Djb232(context.GetBytes(inputIndex)));
            AddHash32WordRules(rules, label, "SDBM-32", context => Sdbm32(context.GetBytes(inputIndex)));
            AddHash32WordRules(rules, label, "JenkinsOAAT-32", context => JenkinsOneAtATime32(context.GetBytes(inputIndex)));
            AddHash32WordRules(rules, label, "Murmur3-32 seed0", context => Murmur3X86_32(context.GetBytes(inputIndex), 0));
            AddHash32WordRules(rules, label, "Murmur3-32 seedFFFF", context => Murmur3X86_32(context.GetBytes(inputIndex), 0xFFFF));
            AddHash32WordRules(rules, label, "CRC32", context => Crc32(context.GetBytes(inputIndex)));
            AddRotateMix16Rules(rules, label, context => context.GetBytes(inputIndex));
#pragma warning disable CA5350, CA5351, RS0030 // Research-only scoring of legacy hash candidates; not used for security.
            AddDigestWordRules(rules, label, "MD5", context => context.GetDigestBytes("MD5", inputIndex, bytes => MD5.HashData(bytes)));
            AddDigestWordRules(rules, label, "SHA1", context => context.GetDigestBytes("SHA1", inputIndex, bytes => SHA1.HashData(bytes)));
#pragma warning restore CA5350, CA5351, RS0030
            rules.Add(new CandidateRule($"{label} Adler16", context => Adler16(context.GetBytes(inputIndex))));
            rules.Add(new CandidateRule($"{label} Fletcher16", context => Fletcher16(context.GetBytes(inputIndex))));
            rules.Add(new CandidateRule($"{label} EseChecksum lo16", context => EseChecksum16(context.GetBytes(inputIndex), low: true)));
            rules.Add(new CandidateRule($"{label} EseChecksum hi16", context => EseChecksum16(context.GetBytes(inputIndex), low: false)));
            rules.Add(new CandidateRule($"{label} InternetCksum", context => InternetChecksum(context.GetBytes(inputIndex))));
            rules.Add(new CandidateRule($"{label} XorFold16", context => XorFold16(context.GetBytes(inputIndex))));
            rules.Add(new CandidateRule($"{label} AddFold16", context => AddFold16(context.GetBytes(inputIndex))));
            rules.Add(new CandidateRule($"{label} XorFoldWord16 BE", context => XorFoldWord16(context.GetBytes(inputIndex), bigEndian: true)));
            rules.Add(new CandidateRule($"{label} XorFoldWord16 LE", context => XorFoldWord16(context.GetBytes(inputIndex), bigEndian: false)));
            rules.Add(new CandidateRule($"{label} AddFoldWord16 BE", context => AddFoldWord16(context.GetBytes(inputIndex), bigEndian: true)));
            rules.Add(new CandidateRule($"{label} AddFoldWord16 LE", context => AddFoldWord16(context.GetBytes(inputIndex), bigEndian: false)));
            AddCrc16DirectRules(rules, label, context => context.GetBytes(inputIndex));
        }

        // Seeded CRC-16: init from key length or truncation boundary bytes.
        AddSeededCrc16Rules(rules);

        foreach ((string label, Func<SuffixCandidateContext, string>? getText) in BuildTextInputs())
        {
            AddHash32WordRules(rules, label, "NLS-65599 UTF16", context => Nls65599Utf16(getText(context), ignoreCase: false));
            AddHash32WordRules(rules, label, "NLS-65599 UTF16 upper", context => Nls65599Utf16(getText(context), ignoreCase: true));
            AddCompareInfoRules(rules, label, getText, CultureInfo.InvariantCulture.CompareInfo, "Invariant");
            AddCompareInfoRules(rules, label, getText, EnUsCompareInfo, "en-US");
            AddCompareSortKeyRules(rules, label, getText, "en-US");
        }

        return rules;
    }

    private static byte[] GetBytes(this SuffixCandidateContext context, int inputIndex) => context.GetByteRuleInput(inputIndex);

    private static IEnumerable<(string Label, int InputIndex)> BuildByteInputs()
    {
        yield return ("full[508..]", 0);
        yield return ("full[510..]", 1);
        yield return ("full[503..511]", 2);
        yield return ("full[500..511]", 3);
        yield return ("full[503..521]", 4);
        yield return ("trimmed[503..511]", 5);
        yield return ("trimmed[500..511]", 6);
        yield return ("trimmed[503..521]", 7);
        yield return ("trimmed[508..]", 8);
        yield return ("full[503..511]+aux", 9);
        yield return ("norm[503..511]+aux", 10);
        yield return ("full[503..521]+aux", 11);
        yield return ("norm[503..521]+aux", 12);
        yield return ("full[508..511]", 13);
        yield return ("full[508..512]", 14);
        yield return ("full[508..513]", 15);
        yield return ("full[..508]", 16);
        yield return ("full[..510] zero", 17);
        yield return ("full[0..]", 18);
        yield return ("full[1..]", 19);
        yield return ("full[^2..]", 20);
        yield return ("norm[0..]", 21);
        yield return ("full[0..]+lenLE", 22);
        yield return ("full[510..]+lenLE", 23);
        yield return ("aux-only", 24);
        yield return ("full[1..^1]", 25);
        yield return ("full[508..]+textLenBE", 26);
        yield return ("full[508..]+textLenLE", 27);
        yield return ("text-utf16le", 28);
        yield return ("text-cp1252", 29);
        yield return ("text[255..]-utf16le", 30);
        yield return ("text[255..]-cp1252", 31);
    }

    private static byte[][] BuildByteRuleInputs(SuffixCandidateContext context)
    {
        byte[][] inputs = context.GetInputCandidates(Cp1252Encoding);
        byte[][] normalizedInputs = context.GetNormalizedInputCandidates(Cp1252Encoding);
        return
        [
            context.ByteInputs[0],
            context.ByteInputs[1],
            SliceOrEmpty(context.FullKey, 503, 8),
            SliceOrEmpty(context.FullKey, 500, 11),
            SliceOrEmpty(context.FullKey, 503, 19),
            SliceOrEmpty(context.TrimmedFullKey, 503, 8),
            SliceOrEmpty(context.TrimmedFullKey, 500, 11),
            SliceOrEmpty(context.TrimmedFullKey, 503, 19),
            SliceOrEmpty(context.TrimmedFullKey, 508),
            ConcatBytes(SliceOrEmpty(context.FullKey, 503, 8), inputs[AuxInputCandidateIndex]),
            ConcatBytes(SliceOrEmpty(context.NormalizedFullKey, 503, 8), normalizedInputs[AuxInputCandidateIndex]),
            ConcatBytes(SliceOrEmpty(context.FullKey, 503, 19), inputs[AuxInputCandidateIndex]),
            ConcatBytes(SliceOrEmpty(context.NormalizedFullKey, 503, 19), normalizedInputs[AuxInputCandidateIndex]),
            context.ByteInputs[2],
            context.ByteInputs[3],
            context.ByteInputs[4],
            context.ByteInputs[5],
            context.ByteInputs[6],
            context.FullKey,
            SliceOrEmpty(context.FullKey, 1),
            LastNOrEmpty(context.FullKey, 2),
            context.NormalizedFullKey,
            AppendLengthLE(context.FullKey),
            AppendLengthLE(context.ByteInputs[1]),
            inputs[AuxInputCandidateIndex],
            context.FullKey.Length >= 2 ? context.FullKey[1..^1] : [],
            AppendTextLengthBE(SliceOrEmpty(context.FullKey, 508), context.Row.Text),
            AppendTextLengthLE(SliceOrEmpty(context.FullKey, 508), context.Row.Text),
            EncodeTextOrEmpty(context.Row.Text, Encoding.Unicode),
            EncodeTextOrEmpty(context.Row.Text, Cp1252Encoding),
            EncodeTextTailOrEmpty(context.Row.Text, 255, Encoding.Unicode),
            EncodeTextTailOrEmpty(context.Row.Text, 255, Cp1252Encoding),
        ];
    }

    private static byte[] EncodeTextOrEmpty(string? text, Encoding encoding)
    {
        if (string.IsNullOrEmpty(text))
        {
            return [];
        }

        return encoding.GetBytes(text);
    }

    private static byte[] EncodeTextTailOrEmpty(string? text, int startIndex, Encoding encoding)
    {
        if (string.IsNullOrEmpty(text) || text.Length <= startIndex)
        {
            return [];
        }

        return encoding.GetBytes(text, startIndex, text.Length - startIndex);
    }

    private static byte[] AppendTextLengthBE(byte[] bytes, string? text)
    {
        int textLength = text?.Length ?? 0;
        var result = new byte[bytes.Length + 2];
        bytes.CopyTo(result, 0);
        BinaryPrimitives.WriteUInt16BigEndian(result.AsSpan(bytes.Length), (ushort)textLength);
        return result;
    }

    private static byte[] AppendTextLengthLE(byte[] bytes, string? text)
    {
        int textLength = text?.Length ?? 0;
        var result = new byte[bytes.Length + 2];
        bytes.CopyTo(result, 0);
        BinaryPrimitives.WriteUInt16LittleEndian(result.AsSpan(bytes.Length), (ushort)textLength);
        return result;
    }

    private static byte[] ConcatBytes(byte[] first, byte[] second)
    {
        if (first.Length == 0)
        {
            return second;
        }

        if (second.Length == 0)
        {
            return first;
        }

        var result = new byte[first.Length + second.Length];
        first.CopyTo(result, 0);
        second.CopyTo(result, first.Length);
        return result;
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

    private static void AddRotateMix16Rules(
        List<CandidateRule> rules,
        string label,
        Func<SuffixCandidateContext, byte[]> getBytes)
    {
        foreach (int rotate in new[] { 1, 3, 5, 7, 8, 9, 11, 13, 15 })
        {
            rules.Add(new CandidateRule($"{label} RotXor16 r{rotate}", context => RotateXor16(getBytes(context), rotate)));
            rules.Add(new CandidateRule($"{label} RotAdd16 r{rotate}", context => RotateAdd16(getBytes(context), rotate)));
            rules.Add(new CandidateRule($"{label} RotXorWord16 r{rotate} BE", context => RotateXorWord16(getBytes(context), rotate, bigEndian: true)));
            rules.Add(new CandidateRule($"{label} RotXorWord16 r{rotate} LE", context => RotateXorWord16(getBytes(context), rotate, bigEndian: false)));
            rules.Add(new CandidateRule($"{label} RotAddWord16 r{rotate} BE", context => RotateAddWord16(getBytes(context), rotate, bigEndian: true)));
            rules.Add(new CandidateRule($"{label} RotAddWord16 r{rotate} LE", context => RotateAddWord16(getBytes(context), rotate, bigEndian: false)));
        }
    }

    private static ushort RotateXor16(byte[] bytes, int rotate)
    {
        ushort hash = 0;
        foreach (byte value in bytes)
        {
            hash = unchecked((ushort)(RotateLeft16(hash, rotate) ^ value));
        }

        return hash;
    }

    private static ushort RotateAdd16(byte[] bytes, int rotate)
    {
        ushort hash = 0;
        foreach (byte value in bytes)
        {
            hash = unchecked((ushort)(RotateLeft16(hash, rotate) + value));
        }

        return hash;
    }

    private static ushort RotateXorWord16(byte[] bytes, int rotate, bool bigEndian)
    {
        ushort hash = 0;
        for (int index = 0; index + 1 < bytes.Length; index += 2)
        {
            ushort word = bigEndian
                ? BinaryPrimitives.ReadUInt16BigEndian(bytes.AsSpan(index, 2))
                : BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(index, 2));
            hash = unchecked((ushort)(RotateLeft16(hash, rotate) ^ word));
        }

        return hash;
    }

    private static ushort RotateAddWord16(byte[] bytes, int rotate, bool bigEndian)
    {
        ushort hash = 0;
        for (int index = 0; index + 1 < bytes.Length; index += 2)
        {
            ushort word = bigEndian
                ? BinaryPrimitives.ReadUInt16BigEndian(bytes.AsSpan(index, 2))
                : BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(index, 2));
            hash = unchecked((ushort)(RotateLeft16(hash, rotate) + word));
        }

        return hash;
    }

    private static ushort RotateLeft16(ushort value, int offset) =>
        unchecked((ushort)((value << offset) | (value >> (16 - offset))));

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

    private static void AddCompareSortKeyRules(
        List<CandidateRule> rules,
        string label,
        Func<SuffixCandidateContext, string> getText,
        string localeName)
    {
        foreach ((string optionLabel, CompareOptions options) in new[]
        {
            ("none", CompareOptions.None),
            ("ignore-case", CompareOptions.IgnoreCase),
            ("ignore-case-nonspace", CompareOptions.IgnoreCase | CompareOptions.IgnoreNonSpace),
            ("ignore-case-nonspace-symbols", CompareOptions.IgnoreCase | CompareOptions.IgnoreNonSpace | CompareOptions.IgnoreSymbols),
        })
        {
            string name = $"{label} CompareSortKey {localeName} {optionLabel}";
            rules.Add(new CandidateRule($"{name} word0 BE", context => ReadWordOrNull(context.GetCompareSortKeyBytes(localeName, options, getText(context)), 0, bigEndian: true)));
            rules.Add(new CandidateRule($"{name} word0 LE", context => ReadWordOrNull(context.GetCompareSortKeyBytes(localeName, options, getText(context)), 0, bigEndian: false)));
            rules.Add(new CandidateRule($"{name} word1 BE", context => ReadWordOrNull(context.GetCompareSortKeyBytes(localeName, options, getText(context)), 2, bigEndian: true)));
            rules.Add(new CandidateRule($"{name} word1 LE", context => ReadWordOrNull(context.GetCompareSortKeyBytes(localeName, options, getText(context)), 2, bigEndian: false)));
            rules.Add(new CandidateRule($"{name} last BE", context => ReadLastWordOrNull(context.GetCompareSortKeyBytes(localeName, options, getText(context)), bigEndian: true)));
            rules.Add(new CandidateRule($"{name} last LE", context => ReadLastWordOrNull(context.GetCompareSortKeyBytes(localeName, options, getText(context)), bigEndian: false)));
            rules.Add(new CandidateRule($"{name} FNV1a16", context => Fnv1A16(context.GetCompareSortKeyBytes(localeName, options, getText(context)))));
            rules.Add(new CandidateRule($"{name} Adler16", context => Adler16(context.GetCompareSortKeyBytes(localeName, options, getText(context)))));
            rules.Add(new CandidateRule($"{name} Fletcher16", context => Fletcher16(context.GetCompareSortKeyBytes(localeName, options, getText(context)))));
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

    private static string ToHexStringOrEmpty(byte[] bytes, int start, int length)
    {
        int available = GetSliceLength(bytes, start, length);
        return available == 0 ? string.Empty : Convert.ToHexString(bytes.AsSpan(start, available));
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
                hash = (hash << 5) + hash + value;
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

    private static uint Nls65599Utf16(string value, bool ignoreCase)
    {
        unchecked
        {
            uint hash = 0;
            foreach (char item in ignoreCase ? value.ToUpperInvariant() : value)
            {
                hash = (hash * 65599u) + item;
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
            const uint c1 = 0xCC9E2D51;
            const uint c2 = 0x1B873593;
            uint hash = seed;
            int roundedEnd = bytes.Length & ~3;

            for (int index = 0; index < roundedEnd; index += 4)
            {
                uint k1 = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(index, 4));
                k1 *= c1;
                k1 = RotateLeft(k1, 15);
                k1 *= c2;

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
                    tail *= c1;
                    tail = RotateLeft(tail, 15);
                    tail *= c2;
                    hash ^= tail;
                    break;
                case 0:
                    break;
                default:
                    throw new UnreachableException("Invalid tail length");
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
        const int mod = 251;
        int a = 1;
        int b = 0;
        foreach (byte value in bytes)
        {
            a = (a + value) % mod;
            b = (b + a) % mod;
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

    /// <summary>
    /// ESE-style checksum: shift-left-1 + add per byte (from ESE's UlChecksum in checksum.cxx).
    /// </summary>
    /// <param name="bytes">The bytes.</param>
    /// <param name="low">The lower bound value.</param>
    private static ushort EseChecksum16(byte[] bytes, bool low)
    {
        unchecked
        {
            uint hash = 0;
            foreach (byte value in bytes)
            {
                hash += value;
                hash <<= 1;
            }

            return low ? (ushort)hash : (ushort)(hash >> 16);
        }
    }

    /// <summary>
    /// Internet checksum: one's complement 16-bit sum (RFC 1071).
    /// </summary>
    /// <param name="bytes">The bytes.</param>
    private static ushort InternetChecksum(byte[] bytes)
    {
        unchecked
        {
            uint sum = 0;
            int index = 0;
            for (; index + 1 < bytes.Length; index += 2)
            {
                sum += (uint)((bytes[index] << 8) | bytes[index + 1]);
            }

            if (index < bytes.Length)
            {
                sum += (uint)(bytes[index] << 8);
            }

            while ((sum >> 16) != 0)
            {
                sum = (sum & 0xFFFF) + (sum >> 16);
            }

            return (ushort)~sum;
        }
    }

    /// <summary>
    /// Simple XOR fold: XOR all bytes into a 16-bit accumulator (alternating high/low byte).
    /// </summary>
    /// <param name="bytes">The bytes.</param>
    private static ushort XorFold16(byte[] bytes)
    {
        unchecked
        {
            ushort hash = 0;
            for (int index = 0; index < bytes.Length; index++)
            {
                if ((index & 1) == 0)
                {
                    hash ^= (ushort)(bytes[index] << 8);
                }
                else
                {
                    hash ^= bytes[index];
                }
            }

            return hash;
        }
    }

    /// <summary>
    /// Simple ADD fold: add all bytes into a 16-bit accumulator (alternating high/low byte).
    /// </summary>
    /// <param name="bytes">The bytes.</param>
    private static ushort AddFold16(byte[] bytes)
    {
        unchecked
        {
            ushort hash = 0;
            for (int index = 0; index < bytes.Length; index++)
            {
                if ((index & 1) == 0)
                {
                    hash += (ushort)(bytes[index] << 8);
                }
                else
                {
                    hash += bytes[index];
                }
            }

            return hash;
        }
    }

    /// <summary>
    /// XOR fold of 16-bit words (big or little endian).
    /// </summary>
    /// <param name="bytes">The bytes.</param>
    /// <param name="bigEndian">The big endian.</param>
    private static ushort XorFoldWord16(byte[] bytes, bool bigEndian)
    {
        unchecked
        {
            ushort hash = 0;
            for (int index = 0; index + 1 < bytes.Length; index += 2)
            {
                ushort word = bigEndian
                    ? BinaryPrimitives.ReadUInt16BigEndian(bytes.AsSpan(index, 2))
                    : BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(index, 2));
                hash ^= word;
            }

            if (bytes.Length % 2 != 0)
            {
                hash ^= bigEndian ? (ushort)(bytes[^1] << 8) : bytes[^1];
            }

            return hash;
        }
    }

    /// <summary>
    /// ADD fold of 16-bit words (big or little endian).
    /// </summary>
    /// <param name="bytes">The bytes.</param>
    /// <param name="bigEndian">The big endian.</param>
    private static ushort AddFoldWord16(byte[] bytes, bool bigEndian)
    {
        unchecked
        {
            ushort hash = 0;
            for (int index = 0; index + 1 < bytes.Length; index += 2)
            {
                ushort word = bigEndian
                    ? BinaryPrimitives.ReadUInt16BigEndian(bytes.AsSpan(index, 2))
                    : BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(index, 2));
                hash += word;
            }

            if (bytes.Length % 2 != 0)
            {
                hash += bigEndian ? (ushort)(bytes[^1] << 8) : bytes[^1];
            }

            return hash;
        }
    }

    private static byte[] LastNOrEmpty(byte[] bytes, int n) =>
        bytes.Length >= n ? bytes[^n..] : [];

    private static byte[] AppendLengthLE(byte[] bytes)
    {
        var result = new byte[bytes.Length + 2];
        bytes.CopyTo(result, 0);
        BinaryPrimitives.WriteUInt16LittleEndian(result.AsSpan(bytes.Length), (ushort)bytes.Length);
        return result;
    }

    private static void AddCrc16DirectRules(
        List<CandidateRule> rules,
        string label,
        Func<SuffixCandidateContext, byte[]> getBytes)
    {
        // Test CRC-16 with the most common standard polynomials directly.
        (string Name, ushort Poly)[] standardPolys =
        [
            ("CCITT", 0x1021),
            ("IBM", 0x8005),
            ("DNP", 0x3D65),
            ("T10-DIF", 0x8BB7),
            ("DECT-R", 0x0589),
            ("CDMA2000", 0xC867),
        ];

        foreach ((string polyName, ushort poly) in standardPolys)
        {
            ushort reflectedPoly = ReflectU16(poly);
            rules.Add(new CandidateRule($"{label} CRC16-{polyName} init0", context =>
                CrcFull(getBytes(context), poly, reflectedPoly, 0, 0, refIn: false, refOut: false)));
            rules.Add(new CandidateRule($"{label} CRC16-{polyName} initFF", context =>
                CrcFull(getBytes(context), poly, reflectedPoly, 0xFFFF, 0, refIn: false, refOut: false)));
            rules.Add(new CandidateRule($"{label} CRC16-{polyName} init0 ref", context =>
                CrcFull(getBytes(context), poly, reflectedPoly, 0, 0, refIn: true, refOut: true)));
            rules.Add(new CandidateRule($"{label} CRC16-{polyName} initFF ref", context =>
                CrcFull(getBytes(context), poly, reflectedPoly, 0xFFFF, 0xFFFF, refIn: true, refOut: true)));
        }
    }

    private static void AddSeededCrc16Rules(List<CandidateRule> rules)
    {
        // Data-dependent init: seed from key length or truncation boundary bytes.
        (string Name, ushort Poly)[] seedPolys =
        [
            ("CCITT", 0x1021),
            ("IBM", 0x8005),
        ];

        foreach ((string polyName, ushort poly) in seedPolys)
        {
            ushort reflectedPoly = ReflectU16(poly);

            // Seed = full key length as uint16.
            rules.Add(new CandidateRule($"full[510..] CRC16-{polyName} seed=len", context =>
                CrcFull(
                    SliceOrEmpty(context.FullKey, 510),
                    poly,
                    reflectedPoly,
                    (ushort)context.FullKey.Length,
                    0,
                    refIn: false,
                    refOut: false)));
            rules.Add(new CandidateRule($"full[510..] CRC16-{polyName} seed=len ref", context =>
                CrcFull(
                    SliceOrEmpty(context.FullKey, 510),
                    poly,
                    reflectedPoly,
                    (ushort)context.FullKey.Length,
                    0,
                    refIn: true,
                    refOut: true)));

            // Seed = word at truncation boundary full[508..509].
            rules.Add(new CandidateRule($"full[510..] CRC16-{polyName} seed=bnd BE", context =>
            {
                ushort init = context.FullKey.Length >= 510
                    ? BinaryPrimitives.ReadUInt16BigEndian(context.FullKey.AsSpan(508, 2))
                    : (ushort)0;
                return CrcFull(
                    SliceOrEmpty(context.FullKey, 510),
                    poly,
                    reflectedPoly,
                    init,
                    0,
                    refIn: false,
                    refOut: false);
            }));
            rules.Add(new CandidateRule($"full[510..] CRC16-{polyName} seed=bnd LE", context =>
            {
                ushort init = context.FullKey.Length >= 510
                    ? BinaryPrimitives.ReadUInt16LittleEndian(context.FullKey.AsSpan(508, 2))
                    : (ushort)0;
                return CrcFull(
                    SliceOrEmpty(context.FullKey, 510),
                    poly,
                    reflectedPoly,
                    init,
                    0,
                    refIn: false,
                    refOut: false);
            }));

            // Seed = key length, input = entire full key.
            rules.Add(new CandidateRule($"full[0..] CRC16-{polyName} seed=len", context =>
                CrcFull(
                    context.FullKey,
                    poly,
                    reflectedPoly,
                    (ushort)context.FullKey.Length,
                    0,
                    refIn: false,
                    refOut: false)));
            rules.Add(new CandidateRule($"full[0..] CRC16-{polyName} seed=len ref", context =>
                CrcFull(
                    context.FullKey,
                    poly,
                    reflectedPoly,
                    (ushort)context.FullKey.Length,
                    0,
                    refIn: true,
                    refOut: true)));
        }
    }

    private static ushort Low16(int value) => unchecked((ushort)value);

    private static ushort High16(int value) => unchecked((ushort)(value >> 16));

    private static ushort ByteSwap(ushort value) => unchecked((ushort)((value << 8) | (value >> 8)));

    private static byte[] CompareSortKeyBytes(string localeName, CompareOptions options, string value) =>
        CultureInfo.GetCultureInfo(localeName).CompareInfo.GetSortKey(value, options).KeyData;

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
        sb.AppendLine(CultureInfo.InvariantCulture, $"### {tableName}.DataIndex raw leaf compression")
            .AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"- first_dp: {index.FirstDp}")
            .AppendLine()
            .AppendLine("| Leaf page | pref_len | payload end | entries | 510-byte decoded entries | First long raw key len | First long raw key tail | First long decoded suffix |")
            .AppendLine("|---:|---:|---:|---:|---:|---:|---|:---:|");

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
            await using AccessReader reader = await AccessReader.OpenAsync(
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

        sb.AppendLine(CultureInfo.InvariantCulture, $"## {Path.GetFileName(fixturePath)}")
            .AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"Long indexes: {fixtureLongIndexes}; long keys: {fixtureLongKeys}")
            .AppendLine()
            .Append(fixtureReport)
            .AppendLine();
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

        var sortedOnDisk = onDiskEntries
            .OrderBy(entry => entry.Key, BytePrefixComparer.Instance)
            .ToList();

        int encodedLongCount = encoded.Count(encodedKey => encodedKey.Key.Length == LongRowEntryLength);
        int prefixMatches = 0;
        var examples = new List<CorpusSuffixExample>();
        Dictionary<byte[], Queue<int>> encodedPrefixLookup = BuildEncodedPrefixLookup(encoded);
        for (int indexPosition = 0; indexPosition < sortedOnDisk.Count; indexPosition++)
        {
            byte[] onDiskKey = sortedOnDisk[indexPosition].Key;
            if (onDiskKey.Length != LongRowEntryLength)
            {
                continue;
            }

            int encodedIndex = FindEncodedPrefixMatch(encodedPrefixLookup, onDiskKey);
            bool prefixMatch = encodedIndex >= 0;
            if (prefixMatch)
            {
                prefixMatches++;
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

    private static Dictionary<byte[], Queue<int>> BuildEncodedPrefixLookup(List<EncodedCorpusKey> encoded)
    {
        var lookup = new Dictionary<byte[], Queue<int>>(LongRowPrefixEqualityComparer.Instance);
        for (int encodedIndex = 0; encodedIndex < encoded.Count; encodedIndex++)
        {
            byte[] encodedKey = encoded[encodedIndex].Key;
            if (encodedKey.Length < PrefixMatchLength)
            {
                continue;
            }

            if (!lookup.TryGetValue(encodedKey, out Queue<int>? indexes))
            {
                indexes = new Queue<int>();
                lookup.Add(encodedKey, indexes);
            }

            indexes.Enqueue(encodedIndex);
        }

        return lookup;
    }

    private static int FindEncodedPrefixMatch(
        Dictionary<byte[], Queue<int>> encodedPrefixLookup,
        byte[] onDiskKey)
    {
        if (onDiskKey.Length < PrefixMatchLength
            || !encodedPrefixLookup.TryGetValue(onDiskKey, out Queue<int>? indexes)
            || indexes.Count == 0)
        {
            return -1;
        }

        return indexes.Dequeue();
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
        sb.AppendLine(CultureInfo.InvariantCulture, $"### {tableName}.{index.Name}")
            .AppendLine()
            .AppendLine(
            CultureInfo.InvariantCulture,
            $"- column: `{keyColumn.Name}` ({columnMeta.TypeName}, CLR `{columnMeta.ClrType.Name}`), ascending={keyColumn.IsAscending}")
            .AppendLine(CultureInfo.InvariantCulture, $"- on-disk 510-byte keys: {onDiskLongCount}")
            .AppendLine(CultureInfo.InvariantCulture, $"- encoded 510-byte keys: {scan.EncodedLongCount}")
            .AppendLine(CultureInfo.InvariantCulture, $"- first-508-byte prefix matches: {scan.PrefixMatchCount}");

        if (scan.Examples.Count == 0)
        {
            sb.AppendLine();
            return;
        }

        sb.AppendLine()
            .AppendLine("| Position | Data ptr | Row | Prefix match | Access suffix | Encoder suffix | Encoded len | Full len | Full tail | Value |")
            .AppendLine("|---:|---:|---|:---:|:---:|:---:|---:|---:|---|---|");
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
        sb.AppendLine("## Summary")
            .AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"- Fixtures scanned: {totals.FixturesScanned}")
            .AppendLine(CultureInfo.InvariantCulture, $"- Indexes with 510-byte keys: {totals.IndexesWithLongKeys}")
            .AppendLine(CultureInfo.InvariantCulture, $"- On-disk 510-byte keys: {totals.LongKeysOnDisk}")
            .AppendLine(CultureInfo.InvariantCulture, $"- Text/Memo 510-byte keys: {totals.TextLongKeysOnDisk}")
            .AppendLine(CultureInfo.InvariantCulture, $"- Binary 510-byte keys: {totals.BinaryLongKeysOnDisk}")
            .AppendLine(CultureInfo.InvariantCulture, $"- Other 510-byte keys: {totals.OtherLongKeysOnDisk}")
            .AppendLine(CultureInfo.InvariantCulture, $"- Encoded 510-byte keys: {totals.LongKeysEncoded}")
            .AppendLine(CultureInfo.InvariantCulture, $"- First-508-byte prefix matches: {totals.PrefixMatches}")
            .AppendLine();
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
            return IndexKeyEncoder.EncodeEntry(BinaryType, value, ascending);
        }

        return null;
    }

    private static string DescribeCorpusValue(object? value) => value switch
    {
        null => "`<null>`",
        byte[] bytes => $"`0x{Convert.ToHexString(bytes.AsSpan(0, Math.Min(bytes.Length, 24)))}{(bytes.Length > 24 ? "..." : string.Empty)}` ({bytes.Length} bytes)",
        string text => $"`{EscapeMarkdown(TruncateForReport(text, 60))}` ({text.Length} chars)",
        _ => $"`{EscapeMarkdown(value.ToString() ?? string.Empty)}`",
    };

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

            function New-TemplateMatrixText([int] $seed, [int] $matrixStart, [string] $template) {
                $pair = $seed - $matrixStart
                $first = [int] [Math]::Floor($pair / $alphabet.Length)
                $second = [int] ($pair % $alphabet.Length)
                $chars = $template.ToCharArray()
                $chars[253] = $alphabet[$first]
                $chars[254] = $alphabet[$second]
                return [string]::new($chars)
            }

            function New-TrailingSpaceMatrixText([int] $seed) {
                $pair = $seed - {{DaoLabTrailingSpaceMatrixStart}}
                $first = [int] [Math]::Floor($pair / $alphabet.Length)
                $second = [int] ($pair % $alphabet.Length)
                $chars = New-Object 'char[]' 360
                for ($position = 0; $position -lt $chars.Length; $position++) {
                    $chars[$position] = 'a'
                }

                $chars[252] = $alphabet[$first]
                $chars[253] = $alphabet[$second]
                $chars[254] = ' '
                return [string]::new($chars)
            }

            function New-DoubleTrailingSpaceText([int] $seed, [string] $row10Template, [string] $row11Template, [string] $row12Template) {
                $sample = $seed - {{DaoLabDoubleSpaceSweepStart}}
                $contextIndex = [int] [Math]::Floor($sample / $alphabet.Length)
                $variant = [int] ($sample % $alphabet.Length)
                switch ($contextIndex) {
                    0 {
                        $chars = New-Object 'char[]' 360
                        for ($position = 0; $position -lt $chars.Length; $position++) {
                            $chars[$position] = 'a'
                        }
                    }
                    1 { $chars = $row10Template.ToCharArray() }
                    2 { $chars = $row11Template.ToCharArray() }
                    default { $chars = $row12Template.ToCharArray() }
                }

                $chars[252] = $alphabet[$variant]
                $chars[253] = ' '
                $chars[254] = ' '
                return [string]::new($chars)
            }

            function New-TemplateSampleText([int] $seed, [string] $row10Template, [string] $row11Template, [string] $row12Template) {
                $sample = $seed - {{DaoLabTemplateSampleStart}}
                $templateIndex = [int] [Math]::Floor($sample / 4)
                $variant = [int] ($sample % 4)
                switch ($templateIndex) {
                    0 { $template = $row10Template }
                    1 { $template = $row11Template }
                    default { $template = $row12Template }
                }

                $chars = $template.ToCharArray()
                switch ($variant) {
                    1 { $chars[253] = ' '; $chars[254] = ' ' }
                    2 { $chars[253] = 'a'; $chars[254] = ' ' }
                    3 { $chars[253] = ' '; $chars[254] = 'a' }
                }

                return [string]::new($chars)
            }

            function New-LabText([int] $seed, [string] $row10Template, [string] $row11Template, [string] $row12Template) {
                if ($seed -ge {{DaoLabTemplateSampleStart}}) {
                    return New-TemplateSampleText $seed $row10Template $row11Template $row12Template
                }

                if ($seed -ge {{DaoLabDoubleSpaceSweepStart}}) {
                    return New-DoubleTrailingSpaceText $seed $row10Template $row11Template $row12Template
                }

                if ($seed -ge {{DaoLabRow11MatrixStart}}) {
                    return New-TemplateMatrixText $seed {{DaoLabRow11MatrixStart}} $row11Template
                }

                if ($seed -ge {{DaoLabRow10MatrixStart}}) {
                    return New-TemplateMatrixText $seed {{DaoLabRow10MatrixStart}} $row10Template
                }

                if ($seed -ge {{DaoLabTrailingSpaceMatrixStart}}) {
                    return New-TrailingSpaceMatrixText $seed
                }

                if ($seed -ge {{DaoLabRow12MatrixStart}}) {
                    return New-TemplateMatrixText $seed {{DaoLabRow12MatrixStart}} $row12Template
                }

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

            function Get-TemplateText([object] $db, [string] $tableName, [string] $rowName) {
                $rs = $db.OpenRecordset($tableName, 2)
                try {
                    while (-not $rs.EOF) {
                        if ([string] $rs.Fields.Item('name').Value -eq $rowName) {
                            return [string] $rs.Fields.Item('data').Value
                        }

                        $rs.MoveNext()
                    }
                } finally {
                    $rs.Close()
                }

                throw "Template row not found: $tableName.$rowName"
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
                $row10Template = Get-TemplateText $db $tableName 'row10'
                $row11Template = Get-TemplateText $db $tableName 'row11'
                $row12Template = Get-TemplateText $db $tableName 'row12'
                $rs = $db.OpenRecordset($tableName, 2)
                try {
                    $fields = $rs.Fields
                    $dataField = $fields.Item('data')
                    $labFields = New-Object System.Collections.ArrayList
                    for ($fieldIndex = 0; $fieldIndex -lt $fields.Count; $fieldIndex++) {
                        $field = $fields.Item($fieldIndex)
                        if ($field.Name -ine 'data') {
                            [void] $labFields.Add($field)
                        }
                    }

                    for ($seed = 0; $seed -lt $rowCount; $seed++) {
                        $text = [string] (New-LabText $seed $row10Template $row11Template $row12Template)
                        $rs.AddNew()
                        foreach ($field in $labFields) {
                            Set-LabFieldValue $field ($seed + $offset + 100000)
                        }

                        $dataField.AppendChunk($text)
                        $rs.Update()
                    }
                } finally {
                    $rs.Close()
                }
            }

            $engine = New-Object -ComObject DAO.DBEngine.120
            try {
                $workspace = $engine.Workspaces.Item(0)
                $db = $engine.OpenDatabase($dbPath)
                try {
                    Write-TableFields $db 'Table11'
                    Write-TableFields $db 'Table11_desc'
                    $transactionStarted = $false
                    try {
                        $workspace.BeginTrans()
                        $transactionStarted = $true
                        Add-LabRows $db 'Table11' 0
                        Add-LabRows $db 'Table11_desc' 1000
                        $workspace.CommitTrans()
                        $transactionStarted = $false
                    } catch {
                        if ($transactionStarted) {
                            $workspace.Rollback()
                        }

                        throw
                    }
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
        Task<string> stdoutTask = process.StandardOutput.ReadToEndAsync();
        Task<string> stderrTask = process.StandardError.ReadToEndAsync();
        if (!process.WaitForExit((int)timeout.TotalMilliseconds))
        {
            TryKill(process);
            process.WaitForExit();
            string timeoutStdout = stdoutTask.GetAwaiter().GetResult();
            string timeoutStderr = stderrTask.GetAwaiter().GetResult();
            return (-1, timeoutStdout, timeoutStderr + $"{Environment.NewLine}[timeout after {timeout.TotalSeconds:N0}s]");
        }

        string stdout = stdoutTask.GetAwaiter().GetResult();
        string stderr = stderrTask.GetAwaiter().GetResult();
        return (process.ExitCode, stdout, stderr);
    }

    private static void TryKill(Process process)
    {
        try
        {
            process.Kill(entireProcessTree: true);
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
        await using AccessReader reader = await AccessReader.OpenAsync(
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

        sb.AppendLine(CultureInfo.InvariantCulture, $"Fixture: `{fixturePath}`")
            .AppendLine()
            .AppendLine("## Constraint rows")
            .AppendLine();

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

        sb.AppendLine()
            .AppendLine("## Char-by-char inline analysis around position 508")
            .AppendLine();

        foreach (RowData row in rowData)
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"### row[{row.RowIndex}] expected=0x{row.ExpectedSuffix:X4}")
                .AppendLine();

            int inlinePosition = 1;
            int lastCharBefore508 = -1;
            int firstCharAt508 = -1;

            for (int charIndex = 0; charIndex < Math.Min(row.Text.Length, 300); charIndex++)
            {
                char currentChar = row.Text[charIndex];
                GeneralLegacyTextIndexEncoder.CharHandler handler = currentChar <= LastChar
                    ? codes[currentChar]
                    : extCodes[currentChar - FirstExtChar];
                ReadOnlySpan<byte> inlineBytes = handler.GetInlineBytes(currentChar);
                int inlineLength = inlineBytes.Length;

                if (inlinePosition + inlineLength > 508 && firstCharAt508 < 0)
                {
                    firstCharAt508 = charIndex;
                }

                if (inlinePosition <= 508)
                {
                    lastCharBefore508 = charIndex;
                }

                if (charIndex is >= 250 and <= 260)
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

            var inlineOnly = new List<byte>(512) { Constants.IndexEntryFlags.AscendingNonNull };
            int charsUsed = 0;
            for (int charIndex = 0; charIndex < row.Text.Length; charIndex++)
            {
                char currentChar = row.Text[charIndex];
                GeneralLegacyTextIndexEncoder.CharHandler handler = currentChar <= LastChar
                    ? codes[currentChar]
                    : extCodes[currentChar - FirstExtChar];
                ReadOnlySpan<byte> inlineBytes = handler.GetInlineBytes(currentChar);
                if (!inlineBytes.IsEmpty)
                {
                    AppendBytes(inlineOnly, inlineBytes);
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
                sb.AppendLine(CultureInfo.InvariantCulture, $"  tail[508..509]=0x{tail:X4} match={tail == row.ExpectedSuffix}")
                    .AppendLine(
                    CultureInfo.InvariantCulture,
                    $"  hex[506..509]={Convert.ToHexString(inlineOnly.GetRange(506, 4).ToArray())}");
            }

            sb.AppendLine();
        }
    }

    private static async Task DumpV2010CrcFullSweepAsync(string fixturePath, StringBuilder sb, CancellationToken ct)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(
            fixturePath,
            new AccessReaderOptions { UseLockFile = false },
            ct);
        DataTable dataTable = await reader.ReadDataTableAsync("Table11", cancellationToken: ct);
        IndexLeafPageBuilder.LeafPageLayout layout = IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);

        List<IndexEntry> ascKeys = await CollectAllLeafKeysAsync(reader, layout, reader.PageSize, firstPage: 112, ct);
        List<IndexEntry> descKeys = await CollectAllLeafKeysAsync(reader, layout, reader.PageSize, firstPage: 119, ct);

        GeneralLegacyTextIndexEncoder.CharHandler[] codes = GeneralCodes.Value;
        GeneralLegacyTextIndexEncoder.CharHandler[] extCodes = GeneralExtCodes.Value;
        Encoding cp1252 = Cp1252Encoding;

        var constraints = new List<ConstraintSet>();
        var rowToLeaf = new (int RowIndex, int AscLeafIndex)[]
        {
            (2, 2),
            (3, 4),
            (4, 3),
        };

        sb.AppendLine("## Constraint set")
            .AppendLine();

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
        sb.AppendLine()
            .AppendLine(
            CultureInfo.InvariantCulture,
            $"Sweep: {candidateCount} input candidates x 65536 polys x 16 modes = {candidateCount * 65536 * 16:N0} combos per constraint")
            .AppendLine("Filter: a (poly, mode, inputIdx) survives only if it satisfies all constraints simultaneously.")
            .AppendLine();

        var hits = new List<CrcSweepHit>();
        ConstraintSet firstConstraint = constraints[0];
        var normalTable = new ushort[256];
        var reflectedTable = new ushort[256];

        for (int polynomial = 0; polynomial <= 0xFFFF; polynomial++)
        {
            ushort polynomialValue = (ushort)polynomial;
            ushort reflectedPolynomial = ReflectU16(polynomialValue);
            BuildCrcTable(polynomialValue, normalTable, reflected: false);
            BuildCrcTable(reflectedPolynomial, reflectedTable, reflected: true);

            for (int inputIndex = 0; inputIndex < candidateCount; inputIndex++)
            {
                byte[] firstInput = firstConstraint.Inputs[inputIndex];
                if (firstInput.Length == 0)
                {
                    continue;
                }

                for (int mode = 0; mode < 8; mode++)
                {
                    bool refIn = (mode & 1) != 0;
                    bool refOut = (mode & 2) != 0;
                    ushort init = (mode & 4) != 0 ? (ushort)0xFFFF : (ushort)0;

                    ushort got = CrcFullWithTable(firstInput, normalTable, reflectedTable, init, 0, refIn, refOut);
                    ushort xorOut;
                    if (got == firstConstraint.Expected)
                    {
                        xorOut = 0;
                    }
                    else if ((ushort)(got ^ 0xFFFF) == firstConstraint.Expected)
                    {
                        xorOut = 0xFFFF;
                    }
                    else
                    {
                        continue;
                    }

                    int fullMode = mode | (xorOut == 0 ? 0 : 8);

                    bool allMatch = true;
                    for (int constraintIndex = 1; constraintIndex < constraints.Count; constraintIndex++)
                    {
                        ConstraintSet constraint = constraints[constraintIndex];
                        ushort constraintGot = CrcFullWithTable(
                            constraint.Inputs[inputIndex],
                            normalTable,
                            reflectedTable,
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
                        hits.Add(new CrcSweepHit(polynomialValue, init, xorOut, refIn, refOut, inputIndex, fullMode));
                    }
                }
            }
        }

        foreach (CrcSweepHit hit in hits
            .OrderBy(static hit => hit.InputIndex)
            .ThenBy(static hit => hit.Polynomial)
            .ThenBy(static hit => hit.Mode))
        {
            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"HIT poly=0x{hit.Polynomial:X4} init=0x{hit.Init:X4} xorOut=0x{hit.XorOut:X4} refIn={hit.RefIn} refOut={hit.RefOut} inputIdx={hit.InputIndex} input={InputCandidateNames[hit.InputIndex]}");
        }

        sb.AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"Total hits: {hits.Count}");
    }

    private static void AppendInputCandidateSummary(List<RowData> rowData, StringBuilder sb)
    {
        Encoding cp1252 = Cp1252Encoding;

        sb.AppendLine()
            .AppendLine("## Input candidate lengths")
            .AppendLine();

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
                    || descKey[0] != Constants.IndexEntryFlags.DescendingNonNull)
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
            SliceOrEmpty(full, 503, 8),
            SliceOrEmpty(full, 500, 11),
            SliceOrEmpty(full, 503, 19),
            full.Length > 508 ? full[508..Math.Min(full.Length, 511)] : [],
            full.Length > 508 ? full[508..Math.Min(full.Length, 512)] : [],
            full.Length > 508 ? full[508..Math.Min(full.Length, 513)] : [],
            full.Length >= 508 ? full[..508] : full,
            full.Length >= 508 ? full[1..508] : full,
            selfCheck,
            full,
            full.Length >= 1 ? full[1..] : [],
            full.Length >= 2 ? full[^2..] : [],
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

    private static void BuildCrcTable(ushort polynomial, ushort[] table, bool reflected)
    {
        unchecked
        {
            for (int tableIndex = 0; tableIndex < 256; tableIndex++)
            {
                ushort crc = reflected
                    ? (ushort)tableIndex
                    : (ushort)(tableIndex << 8);

                for (int bitIndex = 0; bitIndex < 8; bitIndex++)
                {
                    if (reflected)
                    {
                        if ((crc & 1) != 0)
                        {
                            crc = (ushort)((crc >> 1) ^ polynomial);
                        }
                        else
                        {
                            crc = (ushort)(crc >> 1);
                        }
                    }
                    else if ((crc & 0x8000) != 0)
                    {
                        crc = (ushort)((crc << 1) ^ polynomial);
                    }
                    else
                    {
                        crc = (ushort)(crc << 1);
                    }
                }

                table[tableIndex] = crc;
            }
        }
    }

    private static ushort CrcFullWithTable(
        byte[] data,
        ushort[] normalTable,
        ushort[] reflectedTable,
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
                    crc = (ushort)((crc >> 8) ^ reflectedTable[(crc ^ value) & 0xFF]);
                }
            }
            else
            {
                foreach (byte value in data)
                {
                    crc = (ushort)((crc << 8) ^ normalTable[((crc >> 8) ^ value) & 0xFF]);
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

            long owner = BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(reader.DataPage.TDefOff, 4));
            if (owner != tdefPage)
            {
                continue;
            }

            foreach (RowLocation location in reader.EnumerateLiveRowLocations(pageNumber, page))
            {
                if (location.RowSize >= reader.RowFields.NumCols)
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
        => sb.AppendLine(CultureInfo.InvariantCulture, $"# {title}")
            .AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"Generated by: `dotnet run --project JetDatabaseWriter.FormatProbe -- {mode}`")
            .AppendLine(CultureInfo.InvariantCulture, $"Generated at: {DateTimeOffset.UtcNow:u}")
            .AppendLine();

    private static async Task WriteOutputAsync(string outFile, StringBuilder sb)
    {
        await FormatProbeArtifacts.WriteAllTextAsync(outFile, sb.ToString());
        Console.WriteLine($"Wrote {outFile}");
    }

    private static string InlineHex(ReadOnlySpan<byte> bytes)
        => bytes.IsEmpty ? "(none)" : Convert.ToHexString(bytes);

    private static void AppendBytes(List<byte> sink, ReadOnlySpan<byte> bytes)
    {
        foreach (byte value in bytes)
        {
            sink.Add(value);
        }
    }

    private readonly record struct RowData(int RowIndex, ushort ExpectedSuffix, byte[] Full, string Text);

    private readonly record struct ConstraintSet(string Label, byte[][] Inputs, ushort Expected);

    private readonly record struct CrcSweepHit(
        ushort Polynomial,
        ushort Init,
        ushort XorOut,
        bool RefIn,
        bool RefOut,
        int InputIndex,
        int Mode);

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

    private readonly record struct WindowSweepResult(
        int Start,
        int Length,
        bool IncludeAux,
        int Groups,
        int ConflictingGroups,
        int ConflictingRows);

    private readonly record struct WindowSweepRow(
        ushort AccessSuffix,
        byte[] NormalizedFullKey,
        string Phase,
        string AuxSignature);

    private readonly record struct WindowConflictCounts(int Groups, int ConflictingGroups, int ConflictingRows);

    private readonly record struct TruncationWindowKey(
        string Phase,
        string? AuxSignature,
        byte[] Bytes,
        int Start,
        int Length);

    private sealed class WindowGroupAccumulator(ushort firstSuffix)
    {
        public int Rows { get; private set; } = 1;

        public bool HasConflict { get; private set; }

        public void Add(ushort suffix)
        {
            this.Rows++;
            if (suffix != firstSuffix)
            {
                this.HasConflict = true;
            }
        }
    }

    private sealed class TruncationWindowKeyComparer : IEqualityComparer<TruncationWindowKey>
    {
        public static readonly TruncationWindowKeyComparer Instance = new();

        public bool Equals(TruncationWindowKey x, TruncationWindowKey y) =>
            x.Length == y.Length
            && StringComparer.Ordinal.Equals(x.Phase, y.Phase)
            && StringComparer.Ordinal.Equals(x.AuxSignature, y.AuxSignature)
            && (x.Length == 0 || x.Bytes.AsSpan(x.Start, x.Length).SequenceEqual(y.Bytes.AsSpan(y.Start, y.Length)));

        public int GetHashCode(TruncationWindowKey obj)
        {
            HashCode hash = default;
            hash.Add(obj.Phase, StringComparer.Ordinal);
            hash.Add(obj.AuxSignature, StringComparer.Ordinal);
            if (obj.Length > 0)
            {
                ReadOnlySpan<byte> bytes = obj.Bytes.AsSpan(obj.Start, obj.Length);
                for (int index = 0; index < bytes.Length; index++)
                {
                    hash.Add(bytes[index]);
                }
            }

            return hash.ToHashCode();
        }
    }

    private sealed class SuffixCandidateContext(LongRowSuffixProbe.SuffixPatternRow row, bool ascending)
    {
        private readonly Dictionary<CompareSortKeyCacheKey, byte[]> compareSortKeyBytes = [];
        private readonly Dictionary<DigestCacheKey, byte[]> digestBytes = [];
        private byte[][]? inputCandidates;
        private byte[][]? normalizedInputCandidates;
        private byte[][]? byteRuleInputs;

        public SuffixPatternRow Row { get; } = row;

        public byte[] FullKey { get; } = row.FullKey;

        public byte[] TrimmedFullKey { get; } = row.TrimmedFullKey;

        public byte[] NormalizedFullKey { get; } = ascending ? row.FullKey : BuildFullV2010Entry(row.Text!, ascending: true, GeneralCodes.Value, GeneralExtCodes.Value);

        public byte[][] ByteInputs { get; } = BuildCandidateByteInputs(row.FullKey);

        public string[] TextInputs { get; } = BuildCandidateTextInputs(row.Text!);

        public byte[][] GetInputCandidates(Encoding cp1252) =>
            this.inputCandidates ??= BuildInputCandidates(this.FullKey, this.Row.Text!, cp1252);

        public byte[][] GetNormalizedInputCandidates(Encoding cp1252) =>
            this.normalizedInputCandidates ??= BuildInputCandidates(this.NormalizedFullKey, this.Row.Text!, cp1252);

        public byte[] GetByteRuleInput(int inputIndex) =>
            (this.byteRuleInputs ??= BuildByteRuleInputs(this))[inputIndex];

        public byte[] GetDigestBytes(string hashName, int byteInputIndex, Func<byte[], byte[]> compute)
        {
            var key = new DigestCacheKey(hashName, byteInputIndex);
            if (!this.digestBytes.TryGetValue(key, out byte[]? bytes))
            {
                bytes = compute(this.GetByteRuleInput(byteInputIndex));
                this.digestBytes.Add(key, bytes);
            }

            return bytes;
        }

        public byte[] GetCompareSortKeyBytes(string localeName, CompareOptions options, string value)
        {
            var key = new CompareSortKeyCacheKey(localeName, options, value);
            if (!this.compareSortKeyBytes.TryGetValue(key, out byte[]? bytes))
            {
                bytes = CompareSortKeyBytes(localeName, options, value);
                this.compareSortKeyBytes.Add(key, bytes);
            }

            return bytes;
        }
    }

    private readonly record struct CompareSortKeyCacheKey(string LocaleName, CompareOptions Options, string Value);

    private readonly record struct DigestCacheKey(string HashName, int ByteInputIndex);

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

    private readonly record struct PairMatrixXorFailure(
        char FirstChar,
        char SecondChar,
        SuffixPatternRow Row,
        ushort Actual,
        ushort Predicted,
        string FullTail,
        string TrimmedTail);

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
            if (this.counts[key] == 0)
            {
                this.touched.Add(key);
            }

            this.counts[key]++;
        }

        public (ushort Key, int Count) Best()
        {
            ushort bestKey = 0;
            int bestCount = 0;
            foreach (ushort key in this.touched)
            {
                int count = this.counts[key];
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
            foreach (ushort key in this.touched)
            {
                this.counts[key] = 0;
            }

            this.touched.Clear();
        }
    }

    private sealed class BytePrefixComparer : IComparer<byte[]>
    {
        public static readonly BytePrefixComparer Instance = new();

        public int Compare(byte[]? x, byte[]? y)
        {
            if (ReferenceEquals(x, y))
            {
                return 0;
            }

            if (x is null)
            {
                return -1;
            }

            if (y is null)
            {
                return 1;
            }

            return CompareBytesUnsignedPrefix(x, y);
        }
    }

    private sealed class LongRowPrefixEqualityComparer : IEqualityComparer<byte[]>
    {
        public static readonly LongRowPrefixEqualityComparer Instance = new();

        public bool Equals(byte[]? x, byte[]? y)
        {
            if (ReferenceEquals(x, y))
            {
                return true;
            }

            if (x is null || y is null || x.Length < PrefixMatchLength || y.Length < PrefixMatchLength)
            {
                return false;
            }

            return x.AsSpan(0, PrefixMatchLength).SequenceEqual(y.AsSpan(0, PrefixMatchLength));
        }

        public int GetHashCode(byte[] obj)
        {
            unchecked
            {
                int hash = 17;
                ReadOnlySpan<byte> prefix = obj.AsSpan(0, Math.Min(obj.Length, PrefixMatchLength));
                for (int index = 0; index < prefix.Length; index++)
                {
                    hash = (hash * 31) + prefix[index];
                }

                return hash;
            }
        }
    }
}
