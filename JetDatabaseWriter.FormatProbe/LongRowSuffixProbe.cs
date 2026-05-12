// One-shot research probes for the unresolved V2010 long-row 2-byte suffix.
//
// Usage:
//   dotnet run --project JetDatabaseWriter.FormatProbe -- long-row-suffix
//   dotnet run --project JetDatabaseWriter.FormatProbe -- long-row-crc-sweep

namespace JetDatabaseWriter.FormatProbe;

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.ValueDecoding;

internal static class LongRowSuffixProbe
{
    private const int PrefixMatchLength = 508;
    private const int LongRowEntryLength = GeneralLegacyTextIndexEncoder.MaxEntryLengthGeneralV2010;
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

        const int LabRowCount = 48;
        (int exitCode, string stdout, string stderr) = RunPowerShell(
            hostProbe.HostPath,
            BuildDaoLabScript(labPath, LabRowCount),
            scriptPath,
            TimeSpan.FromMinutes(2));

        sb.AppendLine("## DAO authoring");
        sb.AppendLine();
        sb.AppendLine(CultureInfo.InvariantCulture, $"- PowerShell host: `{hostProbe.HostPath}`");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- Lab database: `{labPath}`");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- Script: `{scriptPath}`");
        sb.AppendLine(CultureInfo.InvariantCulture, $"- Requested rows per table: {LabRowCount}");
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
        await ScanFixtureForLongRowsAsync(labPath, sb, totals, CancellationToken.None, maxExamples: 200);
        sb.Insert(summaryInsertOffset, BuildCorpusSummary(totals));

        await WriteOutputAsync(outFile, sb);
        return 0;
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

            int? fullLength = value is string text
                ? BuildFullV2010Entry(text, keyColumn.IsAscending, GeneralCodes.Value, GeneralExtCodes.Value).Length
                : null;
            encoded.Add(new EncodedCorpusKey(value, key, fullLength));
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
                    encodedKey.FullLength,
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
        sb.AppendLine("| Position | Data ptr | Prefix match | Access suffix | Encoder suffix | Encoded len | Full len | Value |");
        sb.AppendLine("|---:|---:|:---:|:---:|:---:|---:|---:|---|");
        foreach (CorpusSuffixExample example in scan.Examples)
        {
            string fullLength = example.FullLength?.ToString(CultureInfo.InvariantCulture) ?? "-";
            string encoderSuffix = example.EncodedLength >= LongRowEntryLength
                ? $"`{example.ActualSuffix:X4}`"
                : "-";
            sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"| {example.Position} | {example.DataPage}:{example.DataRow} | {(example.PrefixMatch ? "yes" : "no")} | `{example.ExpectedSuffix:X4}` | {encoderSuffix} | {example.EncodedLength} | {fullLength} | {example.ValuePreview} |");
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

    private static string TruncateForReport(string value, int maxLength) =>
        value.Length <= maxLength
            ? value
            : value[..maxLength] + "...";

    private static string EscapeMarkdown(string value) =>
        value.Replace("`", "'", StringComparison.Ordinal)
            .Replace("|", "\\|", StringComparison.Ordinal)
            .Replace("\r", "\\r", StringComparison.Ordinal)
            .Replace("\n", "\\n", StringComparison.Ordinal);

    private static string BuildDaoLabScript(string labPath, int rowCount)
    {
        string db = PowerShellLiteral(labPath);
        return $$"""
            $ErrorActionPreference = 'Stop'
            $dbPath = {{db}}
            $rowCount = {{rowCount}}

            function New-LabText([int] $seed) {
                $builder = [System.Text.StringBuilder]::new()
                $fragments = @(
                    'a;sldjfl;aksj dfl;kasj ldfkaslhdfkjhasjk dhfkljas djfhaskljd ',
                    'a;s-ldjfl;aksj dfl;kasj l' + [char]0x00ED + 'dfkaslhdfkjhasjk dhfkljas djfhaskl ',
                    '-a;sldjfl;ak' + [char]0x00C1 + 'sj dfl;kasj ldfkaslhdfkjhasjk dhfkljas djfhaskl '
                )

                for ($repeat = 0; $repeat -lt 15; $repeat++) {
                    [void] $builder.Append($fragments[($seed + $repeat) % $fragments.Length])
                    if ((($repeat + $seed) % 4) -eq 0) { [void] $builder.Append("`r`n") }
                    if ((($repeat * 7 + $seed) % 9) -eq 0) { [void] $builder.Append('-') }
                    if ((($repeat * 5 + $seed) % 11) -eq 0) { [void] $builder.Append([char]0x00C1) }
                }

                return $builder.ToString()
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
                        $text = [string] (New-LabText ($seed + $offset))
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

    private readonly record struct EncodedCorpusKey(object? Value, byte[] Key, int? FullLength);

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
        string ValuePreview);

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
