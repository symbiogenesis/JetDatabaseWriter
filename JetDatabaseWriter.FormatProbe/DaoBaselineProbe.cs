// DAO baseline probe.
//
// Step 1 of docs/design/round-trip-test-failures.md "Recommended next steps":
// build an Access-engine-authored ground-truth copy of NorthwindTraders.accdb
// + a single empty `RT_Customers` table to diff byte-for-byte against the
// writer's N1 output.
//
// Rather than asking a human to do this in the Microsoft Access UI (the design
// doc's literal recommendation), we shell to a bitness-matched PowerShell host
// and drive `DAO.DBEngine.120` to create the table via the TableDef API. ACE (the
// engine behind both DAO and modern Access UI) emits identical on-disk bytes
// regardless of which entry point creates the TableDef — there is no separate
// "Access UI" file format, so this is a true substitute for the manual step.
//
// USAGE
//   dotnet run --project JetDatabaseWriter.FormatProbe -- rt-dao-baseline
//   Legacy: $env:DIAG_RT_DAO_BASELINE = "1"; dotnet run --project JetDatabaseWriter.FormatProbe
//
// Outputs land under %TEMP%\JetDatabaseWriter.RtDaoBaseline\:
//   - writer\source.accdb              (NorthwindTraders + RT_Customers via writer)
//   - writer\source.compacted.accdb    (DAO compact output, if it succeeds)
//   - dao\source.accdb                 (NorthwindTraders + RT_Customers via DAO)
//   - dao\source.compacted.accdb       (DAO compact output, if it succeeds)
//   - pages\<page>_writer.bin          (raw bytes of every "interesting" page, unless DIAG_SKIP_PAGE_BINS=1)
//   - pages\<page>_dao.bin
//   - dao-baseline-diff.md             (the report)

namespace JetDatabaseWriter.FormatProbe;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;

internal static class DaoBaselineProbe
{
    private static readonly AccessReaderOptions ProbeReaderOptions = new() { UseLockFile = false };

    public static async Task<int> RunAsync(string baselinePath, string workRoot)
    {
        if (!File.Exists(baselinePath))
        {
            await Console.Error.WriteLineAsync($"[dao-baseline] baseline not found: {baselinePath}");
            return 1;
        }

        DaoPowerShellHostResolver.DaoPowerShellHostProbeResult hostProbe = DaoPowerShellHostResolver.Probe();
        if (hostProbe.HostPath is null)
        {
            await Console.Error.WriteLineAsync($"[dao-baseline] {hostProbe.FailureReason}");
            return 1;
        }

        workRoot = PrepareWorkRoot(workRoot);
        _ = Directory.CreateDirectory(workRoot);
        string writerDir = Path.Combine(workRoot, "writer");
        string daoDir = Path.Combine(workRoot, "dao");
        string pagesDir = Path.Combine(workRoot, "pages");
        _ = Directory.CreateDirectory(writerDir);
        _ = Directory.CreateDirectory(daoDir);
        _ = Directory.CreateDirectory(pagesDir);

        string writerPath = Path.Combine(writerDir, "source.accdb");
        string daoPath = Path.Combine(daoDir, "source.accdb");
        File.Copy(baselinePath, writerPath, overwrite: true);
        File.Copy(baselinePath, daoPath, overwrite: true);

        Console.WriteLine($"[dao-baseline] baseline = {baselinePath}");
        Console.WriteLine($"[dao-baseline] workRoot = {workRoot}");
        bool writePageBins = ShouldWritePageBins();

        // 1. Writer authoring.
        string writerErr = string.Empty;
        try
        {
            await using var w = await AccessWriter.OpenAsync(writerPath, new AccessWriterOptions { UseLockFile = false });
            await w.CreateTableAsync(
                "RT_Customers",
                [
                    new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },

                    // H47: DAO authors new TEXT columns with ExtraFlags byte 16 == 0x00
                    // (no COMPRESSED_UNICODE_EXT_FLAG_MASK). Our ColumnDefinition default
                    // is IsCompressedUnicode=true, which would stamp 0x01 here and
                    // diverge from the DAO baseline. Pass false to match.
                    new("Name", typeof(string), maxLength: 100) { IsNullable = false },
                ]);
        }
        catch (Exception ex)
        {
            writerErr = $"{ex.GetType().Name}: {ex.Message}";
            await Console.Error.WriteLineAsync($"[dao-baseline] writer authoring FAILED: {writerErr}");
        }

        // 2. DAO authoring.
        string powerShellPath = hostProbe.HostPath;

        string writerCompactPath = Path.Combine(writerDir, "source.compacted.accdb");
        string daoCompactPath = Path.Combine(daoDir, "source.compacted.accdb");
        DaoProbeResults daoResults = RunDaoProbeBatch(
            powerShellPath,
            writerPath,
            writerCompactPath,
            daoPath,
            daoCompactPath,
            runWriterChecks: string.IsNullOrEmpty(writerErr));

        (int daoCreateCode, string daoCreateErr) = daoResults.DaoCreate;
        Console.WriteLine($"[dao-baseline] DAO authoring: exit={daoCreateCode}{(daoCreateCode == 0 ? string.Empty : "  err=" + Truncate(daoCreateErr))}");
        if (daoCreateCode != 0)
        {
            await Console.Error.WriteLineAsync($"""
                [dao-baseline] DAO refused to add RT_Customers — cannot produce baseline. stderr below:
                {daoCreateErr}
                """);

            // Still emit a partial report so the writer side is at least dumped.
        }

        // 3. DAO compact verdicts (sanity).
        (int writerCompactCode, string writerCompactErr) = daoResults.WriterCompact;
        (int daoCompactCode, string daoCompactErr) = daoResults.DaoCompact;

        Console.WriteLine($"[dao-baseline] DAO compact (writer copy): exit={writerCompactCode}{(writerCompactCode == 0 ? string.Empty : "  err=" + Truncate(writerCompactErr))}");
        Console.WriteLine($"[dao-baseline] DAO compact (DAO copy):    exit={daoCompactCode}{(daoCompactCode == 0 ? string.Empty : "  err=" + Truncate(daoCompactErr))}");

        // 3b. DAO OpenRecordset("RT_Customers") smoke (H46 verdict).
        (int writerOrCode, string writerOrErr) = daoResults.WriterOpenRecordset;
        (int daoOrCode, string daoOrErr) = daoResults.DaoOpenRecordset;

        Console.WriteLine($"[dao-baseline] DAO OpenRecordset (writer copy): exit={writerOrCode}{(writerOrCode == 0 ? string.Empty : "  err=" + Truncate(writerOrErr))}");
        Console.WriteLine($"[dao-baseline] DAO OpenRecordset (DAO copy):    exit={daoOrCode}{(daoOrCode == 0 ? string.Empty : "  err=" + Truncate(daoOrErr))}");

        // 4. Build the report.
        var sb = new StringBuilder();
        _ = sb.AppendLine("# DAO-authored baseline vs writer-authored output");
        _ = sb.AppendLine();
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Generated: {DateTimeOffset.UtcNow:u}");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Baseline NorthwindTraders.accdb: `{baselinePath}`");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Writer copy: `{writerPath}`");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- DAO copy:    `{daoPath}`");
        _ = sb.AppendLine();
        _ = sb.AppendLine("## §0 Outcomes");
        _ = sb.AppendLine();
        _ = sb.AppendLine("| Step | Writer copy | DAO copy |");
        _ = sb.AppendLine("|---|---|---|");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| Authoring `RT_Customers` | {(string.IsNullOrEmpty(writerErr) ? "✅ OK" : "❌ " + Md(Truncate(writerErr)))} | {(daoCreateCode == 0 ? "✅ OK" : "❌ exit=" + daoCreateCode + " " + Md(Truncate(daoCreateErr)))} |");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| `DBEngine.CompactDatabase` | {DescribeCompact(writerCompactCode, writerCompactErr)} | {DescribeCompact(daoCompactCode, daoCompactErr)} |");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| `OpenRecordset('RT_Customers')` | {DescribeCompact(writerOrCode, writerOrErr)} | {DescribeCompact(daoOrCode, daoOrErr)} |");
        string? lvPropEnv = Environment.GetEnvironmentVariable("DIAG_RT_NO_LVPROP");
        if (!string.IsNullOrEmpty(lvPropEnv))
        {
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| _Env:_ `DIAG_RT_NO_LVPROP` | `{lvPropEnv}` (writer-side `LvProp` blob suppressed) | n/a |");
        }

        _ = sb.AppendLine();

        await using (var basReader = await AccessReader.OpenAsync(baselinePath, ProbeReaderOptions))
        {
            BaselinePageCache baselinePages = await BaselinePageCache.LoadAsync(basReader);
            int pgSz = baselinePages.PageSize;
            long basPageCount = baselinePages.PageCount;

            ReaderSnapshot? writerSnap = null;
            ReaderSnapshot? daoSnap = null;

            if (string.IsNullOrEmpty(writerErr))
            {
                await using var r = await AccessReader.OpenAsync(writerPath, ProbeReaderOptions);
                writerSnap = await ReaderSnapshot.CaptureAsync(r, baselinePages, "writer", pagesDir, writePageBins);
            }

            if (daoCreateCode == 0)
            {
                await using var r = await AccessReader.OpenAsync(daoPath, ProbeReaderOptions);
                daoSnap = await ReaderSnapshot.CaptureAsync(r, baselinePages, "dao", pagesDir, writePageBins);
            }

            EmitFileLevel(sb, basPageCount, pgSz, writerSnap, daoSnap);
            EmitCatalogDiff(sb, writerSnap, daoSnap);
            EmitChangedPagesTable(sb, basPageCount, writerSnap, daoSnap);
            await EmitRtCustomersTdefDumpsAsync(sb, writerSnap, daoSnap, pagesDir);
            await EmitNewMSysObjectsRowDumpsAsync(sb, writerSnap, daoSnap, pagesDir, pgSz);
            await EmitHypothesisAccumulatingDiffAsync(sb, writerSnap, daoSnap, writerPath, daoPath);
            EmitDaoOnlyPageHex(sb, baselinePages, writerSnap, daoSnap);
        }

        _ = sb.AppendLine();
        _ = sb.AppendLine("## How to use this report");
        _ = sb.AppendLine();
        _ = sb.AppendLine("1. **Compare the new TDEF pages.** §4 dumps the writer's RT_Customers TDEF and the DAO baseline's TDEF side-by-side as hex. Any byte that differs is a candidate explanation for the DAO err 3011 `'MSysDb'` rejection.");
        _ = sb.AppendLine("2. **Compare the new MSysObjects row bytes.** §5 dumps the catalog row body for the writer's and DAO's RT_Customers entries. The `LvProp` (varIdx 8) bytes here are the empirical answer to whether DAO actually requires a non-null `LvProp` for an empty user-table catalog row, and (if so) what the payload looks like.");
        _ = sb.AppendLine("3. **Compare per-table usage-map and PK-leaf pages** by binary-diffing the per-page `.bin` files under `pages\\` (writer vs dao) for the page numbers identified in §3.");
        _ = sb.AppendLine("4. **Read the §7 hypothesis table.** Each H26-H45 row gives a one-line PASS/FAIL verdict, letting a single probe execution rule 5-10 hypotheses in or out without a full hex walk-through.");
        _ = sb.AppendLine();

        string outPath = Path.Combine(workRoot, "dao-baseline-diff.md");
        await File.WriteAllTextAsync(outPath, sb.ToString());
        Console.WriteLine($"[dao-baseline] wrote {outPath}");
        return 0;
    }

    private static bool ShouldWritePageBins() =>
        string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("DIAG_SKIP_PAGE_BINS"))
        && string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("DIAG_NO_PAGE_BINS"));

    private static string PrepareWorkRoot(string workRoot)
    {
        if (!Directory.Exists(workRoot) || !Directory.EnumerateFileSystemEntries(workRoot).Any())
        {
            return workRoot;
        }

        return Path.Combine(
            workRoot,
            DateTimeOffset.UtcNow.ToString("yyyyMMdd-HHmmss-fffffff", CultureInfo.InvariantCulture));
    }

    // ────────────────────────── DAO interop ─────────────────────────────────

    private readonly record struct DaoStepResult(int Code, string StdErr);

    private sealed record DaoProbeResults(
        DaoStepResult DaoCreate,
        DaoStepResult WriterCompact,
        DaoStepResult DaoCompact,
        DaoStepResult WriterOpenRecordset,
        DaoStepResult DaoOpenRecordset);

    private static DaoProbeResults RunDaoProbeBatch(
        string powerShellPath,
        string writerPath,
        string writerCompactPath,
        string daoPath,
        string daoCompactPath,
        bool runWriterChecks)
    {
        if (File.Exists(writerCompactPath))
        {
            File.Delete(writerCompactPath);
        }

        if (File.Exists(daoCompactPath))
        {
            File.Delete(daoCompactPath);
        }

        string script = BuildDaoProbeBatchScript(
            writerPath,
            writerCompactPath,
            daoPath,
            daoCompactPath,
            runWriterChecks);
        (int code, string stdout, string stderr) = RunPwsh(
            powerShellPath,
            script,
            Path.Combine(Path.GetDirectoryName(daoPath)!, "dao-probe-batch.ps1"));
        Dictionary<string, DaoStepResult> results = ParseDaoStepResults(stdout);
        DaoStepResult missing = code == 0
            ? new DaoStepResult(1, "PowerShell batch did not emit a result for this step.")
            : new DaoStepResult(code, stderr);

        return new DaoProbeResults(
            GetDaoStepResult(results, "dao-create", missing),
            GetDaoStepResult(results, "writer-compact", missing),
            GetDaoStepResult(results, "dao-compact", missing),
            GetDaoStepResult(results, "writer-open-recordset", missing),
            GetDaoStepResult(results, "dao-open-recordset", missing));
    }

    private static string BuildDaoProbeBatchScript(
        string writerPath,
        string writerCompactPath,
        string daoPath,
        string daoCompactPath,
        bool runWriterChecks)
    {
        string runWriter = runWriterChecks ? "$true" : "$false";
        return $$"""
            $ErrorActionPreference='Stop'
            $writerPath = {{PowerShellLiteral(writerPath)}}
            $writerCompactPath = {{PowerShellLiteral(writerCompactPath)}}
            $daoPath = {{PowerShellLiteral(daoPath)}}
            $daoCompactPath = {{PowerShellLiteral(daoCompactPath)}}
            $runWriterChecks = {{runWriter}}
            $script:lastStepCode = 0

            function Write-StepResult([string]$name, [int]$code, [string]$message) {
                if ($null -eq $message) { $message = '' }
                $encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($message))
                Write-Output "$name|$code|$encoded"
            }

            function Get-ErrorCode($errorRecord) {
                $code = [int]$errorRecord.Exception.HResult
                $errorCode = $errorRecord.Exception.ErrorCode
                if ($code -eq 0 -and $null -ne $errorCode) { $code = [int]$errorCode }
                if ($code -eq 0) { $code = 1 }
                return $code
            }

            function Invoke-DaoStep([string]$name, [scriptblock]$body) {
                try {
                    & $body
                    $script:lastStepCode = 0
                    Write-StepResult $name 0 ''
                } catch {
                    $code = Get-ErrorCode $_
                    $script:lastStepCode = $code
                    Write-StepResult $name $code $_.Exception.Message
                }
            }

            function Compact-Database([string]$sourcePath, [string]$destinationPath) {
                if (Test-Path -LiteralPath $destinationPath) { Remove-Item -LiteralPath $destinationPath -Force }
                $engine.CompactDatabase($sourcePath, $destinationPath)
            }

            function Open-RtCustomers([string]$path) {
                $db = $engine.OpenDatabase($path, $false, $true)
                try {
                    $recordset = $db.OpenRecordset('RT_Customers', 2)
                    try { } finally { $recordset.Close() }
                } finally {
                    $db.Close()
                }
            }

            $engine = New-Object -ComObject DAO.DBEngine.120
            try {
                Invoke-DaoStep 'dao-create' {
                    $db = $engine.OpenDatabase($daoPath)
                    try {
                        $td = $db.CreateTableDef('RT_Customers')
                        $f1 = $td.CreateField('CustomerID', 4)
                        $f1.Attributes = $f1.Attributes -bor 16
                        $f1.Required = $true
                        $td.Fields.Append($f1)
                        $f2 = $td.CreateField('Name', 10, 100)
                        $f2.Required = $true
                        $td.Fields.Append($f2)
                        $idx = $td.CreateIndex('PrimaryKey')
                        $idx.Primary = $true
                        $idx.Unique = $true
                        $idx.Required = $true
                        $idxFld = $idx.CreateField('CustomerID')
                        $idx.Fields.Append($idxFld)
                        $td.Indexes.Append($idx)
                        $db.TableDefs.Append($td)
                    } finally {
                        $db.Close()
                    }
                }

                $daoCreateCode = $script:lastStepCode

                if ($runWriterChecks) {
                    Invoke-DaoStep 'writer-compact' { Compact-Database $writerPath $writerCompactPath }
                    Invoke-DaoStep 'writer-open-recordset' { Open-RtCustomers $writerPath }
                } else {
                    Write-StepResult 'writer-compact' -1 'skipped - writer authoring failed'
                    Write-StepResult 'writer-open-recordset' -1 'skipped - writer authoring failed'
                }

                if ($daoCreateCode -eq 0) {
                    Invoke-DaoStep 'dao-compact' { Compact-Database $daoPath $daoCompactPath }
                    Invoke-DaoStep 'dao-open-recordset' { Open-RtCustomers $daoPath }
                } else {
                    Write-StepResult 'dao-compact' -1 'skipped - DAO authoring failed'
                    Write-StepResult 'dao-open-recordset' -1 'skipped - DAO authoring failed'
                }
            } finally {
                [System.Runtime.InteropServices.Marshal]::ReleaseComObject($engine) | Out-Null
                [GC]::Collect()
                [GC]::WaitForPendingFinalizers()
            }
            """;
    }

    private static string PowerShellLiteral(string value) =>
        "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";

    private static Dictionary<string, DaoStepResult> ParseDaoStepResults(string stdout)
    {
        var results = new Dictionary<string, DaoStepResult>(StringComparer.OrdinalIgnoreCase);
        foreach (string line in stdout.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries))
        {
            string[] parts = line.Split('|', 3);
            if (parts.Length != 3 || !int.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out int code))
            {
                continue;
            }

            string message;
            try
            {
                message = Encoding.UTF8.GetString(Convert.FromBase64String(parts[2]));
            }
            catch (FormatException)
            {
                message = parts[2];
            }

            results[parts[0]] = new DaoStepResult(code, message);
        }

        return results;
    }

    private static DaoStepResult GetDaoStepResult(
        Dictionary<string, DaoStepResult> results,
        string name,
        DaoStepResult fallback) =>
        results.TryGetValue(name, out DaoStepResult result) ? result : fallback;

    private static (int Code, string StdOut, string StdErr) RunPwsh(string powerShellPath, string script, string scriptPath)
    {
        File.WriteAllText(scriptPath, script);
        var psi = new ProcessStartInfo(powerShellPath)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        psi.ArgumentList.Add("-NoProfile");
        psi.ArgumentList.Add("-ExecutionPolicy");
        psi.ArgumentList.Add("Bypass");
        psi.ArgumentList.Add("-File");
        psi.ArgumentList.Add(scriptPath);

        using var p = Process.Start(psi)!;
        string stdout = p.StandardOutput.ReadToEnd();
        string err = p.StandardError.ReadToEnd();
        _ = p.WaitForExit(120_000);
        return (p.ExitCode, stdout, err);
    }

    // ────────────────────────── Snapshot / per-file analysis ────────────────

    private static async Task<bool> CanReadPagesDirectlyAsync(AccessReader reader)
    {
        if (reader.DatabaseFormat == DatabaseFormat.Jet3Mdb)
        {
            return false;
        }

        AccessEncryptionFormat encryption = await AccessWriter.DetectEncryptionFormatAsync(reader.HostDatabasePath);
        return encryption is AccessEncryptionFormat.None or AccessEncryptionFormat.AccdbLegacyPassword;
    }

    private static FileStream OpenPageReadStream(string path, int pageSize)
        => new(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete,
            pageSize,
            FileOptions.Asynchronous | FileOptions.SequentialScan);

    private static async Task<byte[]> ReadPageDirectAsync(FileStream stream, int pageSize, long pageNumber)
    {
        var page = new byte[pageSize];
        stream.Position = checked(pageNumber * pageSize);
        int offset = 0;
        while (offset < page.Length)
        {
            int read = await stream.ReadAsync(page.AsMemory(offset));
            if (read == 0)
            {
                throw new EndOfStreamException("Unexpected end of database file while reading a page.");
            }

            offset += read;
        }

        return page;
    }

    private static async Task<byte[][]> LoadPagesDirectAsync(string path, int pageSize, int pageCount)
    {
        var pages = new byte[pageCount][];
        await using FileStream stream = OpenPageReadStream(path, pageSize);
        for (int pageNumber = 0; pageNumber < pages.Length; pageNumber++)
        {
            pages[pageNumber] = await ReadPageDirectAsync(stream, pageSize, pageNumber);
        }

        return pages;
    }

    private sealed class BaselinePageCache
    {
        public static async Task<BaselinePageCache> LoadAsync(AccessReader reader)
        {
            int pageSize = reader.PageSize;
            long pageCount = new FileInfo(reader.HostDatabasePath).Length / pageSize;
            if (pageCount > int.MaxValue)
            {
                throw new InvalidOperationException("Baseline is too large to cache page bytes in memory.");
            }

            bool directFilePages = await CanReadPagesDirectlyAsync(reader);
            byte[][] pages;
            if (directFilePages)
            {
                pages = await LoadPagesDirectAsync(reader.HostDatabasePath, pageSize, (int)pageCount);
            }
            else
            {
                pages = new byte[(int)pageCount][];
                for (int pageNumber = 0; pageNumber < pages.Length; pageNumber++)
                {
                    pages[pageNumber] = await reader.GetRawPageBytesAsync(pageNumber, default);
                }
            }

            return new BaselinePageCache(pageSize, pages, directFilePages);
        }

        private readonly byte[][] pages;

        private BaselinePageCache(int pageSize, byte[][] pages, bool usesDirectFilePages)
        {
            PageSize = pageSize;
            this.pages = pages;
            UsesDirectFilePages = usesDirectFilePages;
        }

        public int PageSize { get; }

        public bool UsesDirectFilePages { get; }

        public long PageCount => pages.LongLength;

        public byte[] GetPage(long pageNumber) => pages[checked((int)pageNumber)];
    }

    private sealed class ReaderSnapshot
    {
        public required string Tag { get; init; }

        public required long PageCount { get; init; }

        public required List<long> PagesDifferingFromBaseline { get; init; } // pages in shared range that differ

        public required List<long> PagesAddedBeyondBaseline { get; init; } // pages with index >= baseline count

        public required Dictionary<long, byte> PageTypes { get; init; } // for the union of the above

        public required Dictionary<long, byte[]> PageBytes { get; init; } // same key set as PageTypes

        public required CatalogEntry? RtCustomers { get; init; }

        public required byte[] RtTdefBytes { get; init; } // empty if not found

        public static async Task<ReaderSnapshot> CaptureAsync(
            AccessReader r,
            BaselinePageCache baseline,
            string tag,
            string pagesDir,
            bool writePageBins)
        {
            int pgSz = r.PageSize;
            long pageCount = new FileInfo(r.HostDatabasePath).Length / pgSz;
            long shared = Math.Min(pageCount, baseline.PageCount);
            bool directFilePages = baseline.UsesDirectFilePages && await CanReadPagesDirectlyAsync(r);

            var differing = new List<long>();
            var pageBytes = new Dictionary<long, byte[]>();
            if (directFilePages)
            {
                await using FileStream stream = OpenPageReadStream(r.HostDatabasePath, pgSz);
                for (long p = 0; p < shared; p++)
                {
                    byte[] page = await ReadPageDirectAsync(stream, pgSz, p);
                    if (!page.AsSpan().SequenceEqual(baseline.GetPage(p)))
                    {
                        differing.Add(p);
                        pageBytes[p] = page;
                    }
                }

                for (long p = baseline.PageCount; p < pageCount; p++)
                {
                    pageBytes[p] = await ReadPageDirectAsync(stream, pgSz, p);
                }
            }
            else
            {
                for (long p = 0; p < shared; p++)
                {
                    byte[] page = await r.GetRawPageBytesAsync(p, default);
                    if (!page.AsSpan().SequenceEqual(baseline.GetPage(p)))
                    {
                        differing.Add(p);
                        pageBytes[p] = page;
                    }
                }

                for (long p = baseline.PageCount; p < pageCount; p++)
                {
                    pageBytes[p] = await r.GetRawPageBytesAsync(p, default);
                }
            }

            var added = new List<long>();
            for (long p = baseline.PageCount; p < pageCount; p++)
            {
                added.Add(p);
            }

            var types = new Dictionary<long, byte>();
            foreach ((long pageNumber, byte[] bytes) in pageBytes.OrderBy(static kvp => kvp.Key))
            {
                types[pageNumber] = bytes.Length > 0 ? bytes[0] : (byte)0xFF;
                if (writePageBins)
                {
                    await File.WriteAllBytesAsync(Path.Combine(pagesDir, $"page{pageNumber:D5}_{tag}.bin"), bytes);
                }
            }

            var catalog = await ReadCatalogAsync(r);
            var rt = catalog.FirstOrDefault(c => string.Equals(c.Name, "RT_Customers", StringComparison.Ordinal));

            byte[] tdefBytes = Array.Empty<byte>();
            if (rt is not null && rt.TdefPage > 0)
            {
                tdefBytes = (await r.GetRawTDefBytesAsync(rt.TdefPage, default)) ?? Array.Empty<byte>();
            }

            return new ReaderSnapshot
            {
                Tag = tag,
                PageCount = pageCount,
                PagesDifferingFromBaseline = differing,
                PagesAddedBeyondBaseline = added,
                PageTypes = types,
                PageBytes = pageBytes,
                RtCustomers = rt,
                RtTdefBytes = tdefBytes,
            };
        }
    }

    // ────────────────────────── Report sections ─────────────────────────────

    private static void EmitFileLevel(StringBuilder sb, long basPages, int pgSz, ReaderSnapshot? w, ReaderSnapshot? d)
    {
        _ = sb.AppendLine("## §1 File-level summary");
        _ = sb.AppendLine();
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Page size: {pgSz} bytes");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Baseline pages: {basPages}");
        if (w is not null)
        {
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Writer copy pages: {w.PageCount} (Δ {w.PageCount - basPages:+#;-#;0})");
        }

        if (d is not null)
        {
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- DAO    copy pages: {d.PageCount} (Δ {d.PageCount - basPages:+#;-#;0})");
        }

        if (w is not null)
        {
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Writer pages diff'd in shared range: {w.PagesDifferingFromBaseline.Count} → {Join(w.PagesDifferingFromBaseline)}");
        }

        if (d is not null)
        {
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- DAO    pages diff'd in shared range: {d.PagesDifferingFromBaseline.Count} → {Join(d.PagesDifferingFromBaseline)}");
        }

        if (w is not null)
        {
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Writer pages added beyond baseline:  {w.PagesAddedBeyondBaseline.Count} → {Join(w.PagesAddedBeyondBaseline)}");
        }

        if (d is not null)
        {
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- DAO    pages added beyond baseline:  {d.PagesAddedBeyondBaseline.Count} → {Join(d.PagesAddedBeyondBaseline)}");
        }

        _ = sb.AppendLine();
    }

    private static void EmitCatalogDiff(StringBuilder sb, ReaderSnapshot? w, ReaderSnapshot? d)
    {
        _ = sb.AppendLine("## §2 RT_Customers catalog row");
        _ = sb.AppendLine();
        _ = sb.AppendLine("| Source | Id | ParentId | Type | Flags | TDEF page |");
        _ = sb.AppendLine("|---|---:|---:|---:|---|---:|");
        if (w?.RtCustomers is { } wc)
        {
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| writer | {wc.Id} | {wc.ParentId} | {wc.Type} | 0x{unchecked((uint)wc.Flags):X8} | {wc.TdefPage} |");
        }

        if (d?.RtCustomers is { } dc)
        {
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| dao    | {dc.Id} | {dc.ParentId} | {dc.Type} | 0x{unchecked((uint)dc.Flags):X8} | {dc.TdefPage} |");
        }

        _ = sb.AppendLine();
    }

    private static void EmitChangedPagesTable(StringBuilder sb, long basPages, ReaderSnapshot? w, ReaderSnapshot? d)
    {
        _ = sb.AppendLine("## §3 Changed/added pages by type");
        _ = sb.AppendLine();
        _ = sb.AppendLine("Page-type byte legend (offset 0): 0x00=DB header, 0x01=data, 0x02=TDEF, 0x03=intermediate idx, 0x04=leaf idx, 0x05=LVAL, 0x08=usage-map.");
        _ = sb.AppendLine();
        _ = sb.AppendLine("| Page | Writer type | DAO type | Notes |");
        _ = sb.AppendLine("|---:|:---:|:---:|---|");

        var allPages = new SortedSet<long>();
        if (w is not null)
        {
            allPages.UnionWith(w.PagesDifferingFromBaseline);
            allPages.UnionWith(w.PagesAddedBeyondBaseline);
        }

        if (d is not null)
        {
            allPages.UnionWith(d.PagesDifferingFromBaseline);
            allPages.UnionWith(d.PagesAddedBeyondBaseline);
        }

        foreach (long p in allPages)
        {
            string wt = w is not null && w.PageTypes.TryGetValue(p, out byte wb) ? $"0x{wb:X2}" : "—";
            string dt = d is not null && d.PageTypes.TryGetValue(p, out byte db) ? $"0x{db:X2}" : "—";
            string note = p >= basPages ? "added (beyond baseline)" : "differs in shared range";
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| {p} | {wt} | {dt} | {note} |");
        }

        _ = sb.AppendLine();
    }

    private static async Task EmitRtCustomersTdefDumpsAsync(StringBuilder sb, ReaderSnapshot? w, ReaderSnapshot? d, string pagesDir)
    {
        _ = sb.AppendLine("## §4 RT_Customers TDEF (raw bytes)");
        _ = sb.AppendLine();
        if (w is null && d is null)
        {
            _ = sb.AppendLine("> Both copies missing — nothing to compare.");
            return;
        }

        if (w?.RtTdefBytes.Length > 0)
        {
            await File.WriteAllBytesAsync(Path.Combine(pagesDir, "rt_customers_tdef_writer.bin"), w.RtTdefBytes);
        }

        if (d?.RtTdefBytes.Length > 0)
        {
            await File.WriteAllBytesAsync(Path.Combine(pagesDir, "rt_customers_tdef_dao.bin"), d.RtTdefBytes);
        }

        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Writer TDEF: {w?.RtTdefBytes.Length ?? 0} bytes (`pages/rt_customers_tdef_writer.bin`)");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- DAO    TDEF: {d?.RtTdefBytes.Length ?? 0} bytes (`pages/rt_customers_tdef_dao.bin`)");
        _ = sb.AppendLine();

        if (w?.RtTdefBytes.Length > 0 && d?.RtTdefBytes.Length > 0)
        {
            int common = Math.Min(w.RtTdefBytes.Length, d.RtTdefBytes.Length);
            var diffs = new List<int>();
            for (int i = 0; i < common; i++)
            {
                if (w.RtTdefBytes[i] != d.RtTdefBytes[i])
                {
                    diffs.Add(i);
                }
            }

            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Length match: {(w.RtTdefBytes.Length == d.RtTdefBytes.Length ? "yes" : "**NO**")}");
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Byte-level diffs in shared prefix: **{diffs.Count}** of {common}");
            if (diffs.Count > 0)
            {
                _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- First 64 differing offsets: {string.Join(", ", diffs.Take(64).Select(o => $"0x{o:X4}"))}{(diffs.Count > 64 ? ", …" : string.Empty)}");
            }

            _ = sb.AppendLine();
            EmitSideBySideHex(sb, "TDEF", w.RtTdefBytes, d.RtTdefBytes, maxBytes: 1024);
        }
        else if (w?.RtTdefBytes.Length > 0)
        {
            _ = sb.AppendLine("> DAO TDEF unavailable; writer TDEF dumped to file only.");
            _ = sb.AppendLine();
        }
        else if (d?.RtTdefBytes.Length > 0)
        {
            _ = sb.AppendLine("> Writer TDEF unavailable; DAO TDEF dumped to file only.");
            _ = sb.AppendLine();
            EmitSingleHex(sb, "DAO TDEF", d.RtTdefBytes, maxBytes: 1024);
        }
    }

    private static async Task EmitNewMSysObjectsRowDumpsAsync(StringBuilder sb, ReaderSnapshot? w, ReaderSnapshot? d, string pagesDir, int pgSz)
    {
        _ = sb.AppendLine("## §5 New MSysObjects row bytes");
        _ = sb.AppendLine();
        _ = sb.AppendLine("Locates the data page that hosts the RT_Customers catalog row in each copy (the MSysObjects data page that differs from baseline), extracts the row body via the on-page row-offset table, and dumps it as hex. The `LvProp` (varIdx 8) and `LvExtra` (varIdx 10) payloads are the empirical answer to docs/design/round-trip-test-failures.md hypothesis #6.");
        _ = sb.AppendLine();

        if (w is not null)
        {
            await EmitNewMSysObjectsRowAsync(sb, w, "writer", pagesDir, pgSz);
        }

        if (d is not null)
        {
            await EmitNewMSysObjectsRowAsync(sb, d, "dao", pagesDir, pgSz);
        }
    }

    private static async Task EmitNewMSysObjectsRowAsync(StringBuilder sb, ReaderSnapshot snap, string label, string pagesDir, int pgSz)
    {
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"### {label}");
        _ = sb.AppendLine();
        if (snap.RtCustomers is null)
        {
            _ = sb.AppendLine("> RT_Customers row not in catalog.");
            _ = sb.AppendLine();
            return;
        }

        long? hostPage = null;
        byte[]? hostBytes = null;
        int? rowOffset = null;
        int? rowLen = null;

        long targetId = snap.RtCustomers.Id;
        byte[] targetIdBytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(targetIdBytes, (int)targetId);

        // Jet4 data-page header layout (per DataPageLayout.cs): NumRows @ offset 12 (u16 LE),
        // RowsStart @ offset 14. Bit 15 of an offset = deleted; bit 14 = pointer-to-next-page (overflow).
        // Rows are packed from page tail downward.
        const int NumRowsOff = 12;
        const int RowsStartOff = 14;

        var candidates = snap.PagesDifferingFromBaseline.Concat(snap.PagesAddedBeyondBaseline);
        foreach (long p in candidates)
        {
            if (!snap.PageTypes.TryGetValue(p, out byte pt) || pt != 0x01)
            {
                continue;
            }

            if (!snap.PageBytes.TryGetValue(p, out byte[]? page))
            {
                continue;
            }

            if (page.Length < RowsStartOff + 2)
            {
                continue;
            }

            try
            {
                ushort rowCount = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(NumRowsOff, 2));
                if (rowCount == 0 || RowsStartOff + (rowCount * 2) > page.Length)
                {
                    continue;
                }

                // Collect every offset (live + deleted + overflow), masked to 13 bits, then sort
                // ascending. For any given slot at offset O, its row end is the next-larger value
                // in this list (or pgSz if O is the largest). Deleted/overflow markers occupy
                // physical row space, so they participate in the boundary calculation.
                var allOffsets = new List<int>(rowCount);
                for (int i = 0; i < rowCount; i++)
                {
                    ushort slot = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(RowsStartOff + (i * 2), 2));
                    allOffsets.Add(slot & 0x1FFF);
                }

                allOffsets.Sort();

                for (int i = 0; i < rowCount; i++)
                {
                    ushort slot = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(RowsStartOff + (i * 2), 2));
                    if ((slot & 0xC000) != 0)
                    {
                        continue; // deleted or overflow
                    }

                    int start = slot & 0x1FFF;
                    if (start < 0 || start + 6 > page.Length)
                    {
                        continue;
                    }

                    int end = pgSz;
                    foreach (int candidate in allOffsets)
                    {
                        if (candidate > start)
                        {
                            end = candidate;
                            break;
                        }
                    }

                    if (end <= start || end > page.Length)
                    {
                        continue;
                    }

                    if (end - start < 6)
                    {
                        continue;
                    }

                    // MSysObjects row: numCols u16 LE @ row[0..2], then fixed cols. Id is fixed col[0] at row[2..6].
                    if (page.AsSpan(start + 2, 4).SequenceEqual(targetIdBytes))
                    {
                        hostPage = p;
                        hostBytes = page;
                        rowOffset = start;
                        rowLen = end - start;
                        break;
                    }
                }
            }
            catch (Exception ex) when (ex is ArgumentOutOfRangeException or IndexOutOfRangeException)
            {
                continue;
            }

            if (hostPage is not null)
            {
                break;
            }
        }

        if (hostPage is null || hostBytes is null || rowOffset is null || rowLen is null)
        {
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"> Could not locate the new MSysObjects row for Id={targetId} on any differing data page. Falling back to dumping all differing data pages to disk.");
            _ = sb.AppendLine();
            return;
        }

        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Host page: **{hostPage}** (page-type 0x01 data page)");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Row offset on page: 0x{rowOffset:X4}");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Row length: {rowLen} bytes");
        byte[] rowBytes = new byte[rowLen.Value];
        Buffer.BlockCopy(hostBytes, rowOffset.Value, rowBytes, 0, rowLen.Value);
        string outBin = Path.Combine(pagesDir, $"msysobjects_row_{label}.bin");
        await File.WriteAllBytesAsync(outBin, rowBytes);
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Raw bytes: `pages/msysobjects_row_{label}.bin`");
        _ = sb.AppendLine();
        EmitSingleHex(sb, $"{label} MSysObjects row body", rowBytes, maxBytes: 512);
    }

    // ─────── §7 Accumulating hypothesis diff (H26-H45) ─────────────────────
    //
    // For each TDEF-byte-level hypothesis recorded in
    // docs/design/round-trip-openrecordset-hypothesis.md §H26+, extract the
    // relevant slice from the writer-authored RT_Customers TDEF and the
    // DAO-authored RT_Customers TDEF and emit one PASS/FAIL row. A single
    // probe execution rules 5-10 hypotheses in or out without a manual hex
    // walk-through.

    private sealed record HypothesisRow(string Id, string Title, string Verdict, string Writer, string Dao, string Notes);

    private static async Task EmitHypothesisAccumulatingDiffAsync(
        StringBuilder sb,
        ReaderSnapshot? w,
        ReaderSnapshot? d,
        string writerPath,
        string daoPath)
    {
        _ = sb.AppendLine("## §7 H26-H45 accumulating hypothesis diff");
        _ = sb.AppendLine();
        _ = sb.AppendLine("Each row checks one hypothesis from `docs/design/round-trip-openrecordset-hypothesis.md` against the writer-authored vs DAO-authored RT_Customers TDEF (and, where indicated, the per-table usage-map page). `✅ PASS` = writer matches DAO; `❌ FAIL` = writer differs (candidate root cause); `⚠️ N/A` = hypothesis does not apply to this fixture's column / index shape.");
        _ = sb.AppendLine();

        if (w?.RtTdefBytes is null or { Length: < 64 } || d?.RtTdefBytes is null or { Length: < 64 })
        {
            _ = sb.AppendLine("> Need both writer and DAO TDEF bytes (both ≥ 64 bytes) — skipped.");
            return;
        }

        byte[] wt = w.RtTdefBytes;
        byte[] dt = d.RtTdefBytes;

        // Parse TDEF header — Jet4/ACE only.
        // Offsets per Jackcess JetFormat.Jet12Format / writer's TDefHeaderLayout:
        //   0x18 (24) = autonum_flag        0x1C (28) = next_complex_auto_number
        //   0x2D (45) = num_cols (u16)      0x2F (47) = num_logical_idx (i32)
        //   0x33 (51) = num_real_idx (i32)  0x37 (55) = owned_pages (4)
        //   0x3B (59) = free_space_pages (4)0x3F (63) = index_def_block start (12 × num_real_idx)
        var wHdr = ParseTDefHeader(wt);
        var dHdr = ParseTDefHeader(dt);

        var rows = new List<HypothesisRow>();

        // ── H22: redundant col_num at descriptor byte 9 == col_num at byte 5
        rows.Add(CheckRedundantColNum(wt, wHdr, dt, dHdr));

        // ── H25: TDEF[0x18] autonum_flag == 0x01
        rows.Add(new HypothesisRow(
            "H25",
            "TDEF[0x18] autonum_flag == 0x01",
            wt[0x18] == 0x01 && dt[0x18] == 0x01 ? "✅ PASS" : (wt[0x18] != dt[0x18] ? "❌ FAIL" : "⚠️ INFO"),
            $"0x{wt[0x18]:X2}",
            $"0x{dt[0x18]:X2}",
            wt[0x18] == dt[0x18] ? "matches DAO" : "diverges from DAO"));

        // ── H27: every non-FK logical-idx descriptor's RelIdxNum (bytes [13..16]) == 0xFFFFFFFF
        rows.Add(CheckLogIdxRelIdxNum(wt, wHdr, dt, dHdr));

        // ── H28: every logical-idx descriptor's putInt(0) at bytes [24..27] preserved
        rows.Add(CheckLogIdxTrailerInt(wt, wHdr, dt, dHdr));

        // ── H31/H32/H33/H34: column-descriptor zero-fields
        rows.Add(CheckColDescZeroFields(wt, wHdr, dt, dHdr, "H31", "NUMERIC col-desc bytes [13..14] == 0", isApplicable: (b, off) => b[off + 0 /*type*/] == 0x10, range: (13, 2)));
        rows.Add(CheckColDescZeroFields(wt, wHdr, dt, dHdr, "H32", "non-text/numeric/complex col-desc bytes [11..14] == 0", isApplicable: (b, off) => b[off] is not 0x0A and not 0x0C and not 0x10 and not 0x12, range: (11, 4)));
        rows.Add(CheckColDescZeroFields(wt, wHdr, dt, dHdr, "H33", "col-desc bytes [17..20] (always-0 putInt) == 0", isApplicable: (_, _) => true, range: (17, 4)));
        rows.Add(CheckColDescZeroFields(wt, wHdr, dt, dHdr, "H34", "ExtraFlags byte [16] == 0 for non-TEXT/MEMO", isApplicable: (b, off) => b[off] is not 0x0A and not 0x0C, range: (16, 1)));

        // ── H37: real-idx flags byte at offset 46 has UNIQUE|REQUIRED|UNKNOWN bits for the PK
        rows.Add(CheckRealIdxFlags(wt, wHdr, dt, dHdr));

        // ── H38: real-idx 4-byte "unknown" gap at offsets 42..45 == 0
        rows.Add(CheckRealIdxUnknownGap(wt, wHdr, dt, dHdr));

        // ── H41: TDEF[0x1C..0x1F] (next_complex_auto_number on ACCDB) == 0
        uint wNext = BinaryPrimitives.ReadUInt32LittleEndian(wt.AsSpan(0x1C, 4));
        uint dNext = BinaryPrimitives.ReadUInt32LittleEndian(dt.AsSpan(0x1C, 4));
        rows.Add(new HypothesisRow(
            "H41",
            "TDEF[0x1C..0x1F] next_complex_auto_number == 0",
            wNext == 0 && dNext == 0 ? "✅ PASS" : (wNext == dNext ? "⚠️ INFO" : "❌ FAIL"),
            $"0x{wNext:X8}",
            $"0x{dNext:X8}",
            wNext == dNext ? "matches DAO" : "diverges from DAO"));

        // ── H42: index-name length-prefix is byte count (== chars * 2) for every logical-idx name
        rows.Add(CheckIndexNameLengthPrefix(wt, wHdr, dt, dHdr));

        // ── H44: TDEF[0x37..0x3A] (owned_pages) and [0x3B..0x3E] (free_space_pages) are non-zero
        rows.Add(CheckOwnedFreePages(wt, dt));

        // ── H45: per-table usage-map row tdef_back_pointer matches host TDEF page
        // ── H26: per-table usage-map row 0 type byte == 0x01 (MAP_TYPE_REFERENCE)
        if (w.RtCustomers is { TdefPage: > 0 } wRt && d.RtCustomers is { TdefPage: > 0 } dRt)
        {
            await using var wr = await AccessReader.OpenAsync(writerPath, ProbeReaderOptions);
            await using var dr = await AccessReader.OpenAsync(daoPath, ProbeReaderOptions);
            (HypothesisRow h26, HypothesisRow h45) = await CheckUsageMapRowAsync(wr, dr, wt, dt, wRt.TdefPage, dRt.TdefPage);
            rows.Add(h26);
            rows.Add(h45);
        }
        else
        {
            rows.Add(new HypothesisRow("H26", "per-table usage-map row 0 type == 0x01", "⚠️ N/A", "—", "—", "RT_Customers catalog row missing"));
            rows.Add(new HypothesisRow("H45", "per-table usage-map row tdef_back_pointer matches", "⚠️ N/A", "—", "—", "RT_Customers catalog row missing"));
        }

        _ = sb.AppendLine("| ID | Hypothesis | Verdict | Writer | DAO | Notes |");
        _ = sb.AppendLine("|---|---|:---:|---|---|---|");
        foreach (var r in rows)
        {
            _ = sb.AppendLine(
                CultureInfo.InvariantCulture,
                $"| {r.Id} | {Md(r.Title)} | {r.Verdict} | {Md(r.Writer)} | {Md(r.Dao)} | {Md(r.Notes)} |");
        }

        _ = sb.AppendLine();
        int pass = rows.Count(r => r.Verdict.Contains("PASS", StringComparison.Ordinal));
        int fail = rows.Count(r => r.Verdict.Contains("FAIL", StringComparison.Ordinal));
        int na = rows.Count - pass - fail;
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"**Summary:** {pass} ✅ PASS · {fail} ❌ FAIL · {na} ⚠️ N/A/INFO ({rows.Count} total).");
        _ = sb.AppendLine();
    }

    private readonly record struct TDefHeader(int NumCols, int NumLogIdx, int NumRealIdx, int ColStart, int ColNamesEnd, int RealIdxPhysStart, int LogIdxStart, int LogIdxNamesStart);

    private static TDefHeader ParseTDefHeader(byte[] td)
    {
        int numCols = BinaryPrimitives.ReadUInt16LittleEndian(td.AsSpan(0x2D, 2));
        int numLogIdx = BinaryPrimitives.ReadInt32LittleEndian(td.AsSpan(0x2F, 4));
        int numRealIdx = BinaryPrimitives.ReadInt32LittleEndian(td.AsSpan(0x33, 4));
        int colStart = 63 + (Math.Max(0, numRealIdx) * 12);
        int colDescBlockEnd = colStart + (numCols * 25);
        int colNamesEnd = SkipUcs2NamesPrefixed(td, colDescBlockEnd, numCols);
        int realIdxPhysStart = colNamesEnd;
        int logIdxStart = realIdxPhysStart + (numRealIdx * 52);
        int logIdxNamesStart = logIdxStart + (numLogIdx * 28);
        return new TDefHeader(numCols, numLogIdx, numRealIdx, colStart, colNamesEnd, realIdxPhysStart, logIdxStart, logIdxNamesStart);
    }

    private static int SkipUcs2NamesPrefixed(byte[] td, int start, int count)
    {
        int p = start;
        for (int i = 0; i < count; i++)
        {
            if (p + 2 > td.Length)
            {
                return Math.Min(td.Length, p);
            }

            int len = BinaryPrimitives.ReadUInt16LittleEndian(td.AsSpan(p, 2));
            p += 2 + len;
            if (p > td.Length)
            {
                return td.Length;
            }
        }

        return p;
    }

    private static HypothesisRow CheckRedundantColNum(byte[] wt, TDefHeader wHdr, byte[] dt, TDefHeader dHdr)
    {
        var wMismatches = new List<int>();
        for (int i = 0; i < wHdr.NumCols; i++)
        {
            int o = wHdr.ColStart + (i * 25);
            if (o + 11 > wt.Length)
            {
                break;
            }

            ushort primary = BinaryPrimitives.ReadUInt16LittleEndian(wt.AsSpan(o + 5, 2));
            ushort redundant = BinaryPrimitives.ReadUInt16LittleEndian(wt.AsSpan(o + 9, 2));
            if (primary != redundant)
            {
                wMismatches.Add(i);
            }
        }

        var dMismatches = new List<int>();
        for (int i = 0; i < dHdr.NumCols; i++)
        {
            int o = dHdr.ColStart + (i * 25);
            if (o + 11 > dt.Length)
            {
                break;
            }

            ushort primary = BinaryPrimitives.ReadUInt16LittleEndian(dt.AsSpan(o + 5, 2));
            ushort redundant = BinaryPrimitives.ReadUInt16LittleEndian(dt.AsSpan(o + 9, 2));
            if (primary != redundant)
            {
                dMismatches.Add(i);
            }
        }

        string verdict = wMismatches.Count == 0 ? "✅ PASS" : "❌ FAIL";
        return new HypothesisRow(
            "H22",
            "col-desc byte 9 redundant col_num == byte 5 col_num",
            verdict,
            wMismatches.Count == 0 ? "all match" : $"mismatches at cols [{string.Join(",", wMismatches)}]",
            dMismatches.Count == 0 ? "all match" : $"mismatches at cols [{string.Join(",", dMismatches)}]",
            verdict == "✅ PASS" ? "writer matches DAO ground truth" : "writer diverges; see Schema/WriterColumnDescriptorRedundantColNumTests");
    }

    private static HypothesisRow CheckLogIdxRelIdxNum(byte[] wt, TDefHeader wHdr, byte[] dt, TDefHeader dHdr)
    {
        var wBad = new List<(int Slot, uint Val)>();
        for (int i = 0; i < wHdr.NumLogIdx; i++)
        {
            int o = wHdr.LogIdxStart + (i * 28);
            if (o + 17 > wt.Length)
            {
                break;
            }

            byte relTblType = wt[o + 12];
            if (relTblType != 0)
            {
                continue; // FK entry — skip
            }

            uint relIdx = BinaryPrimitives.ReadUInt32LittleEndian(wt.AsSpan(o + 13, 4));
            if (relIdx != 0xFFFFFFFF)
            {
                wBad.Add((i, relIdx));
            }
        }

        var dBad = new List<(int Slot, uint Val)>();
        for (int i = 0; i < dHdr.NumLogIdx; i++)
        {
            int o = dHdr.LogIdxStart + (i * 28);
            if (o + 17 > dt.Length)
            {
                break;
            }

            byte relTblType = dt[o + 12];
            if (relTblType != 0)
            {
                continue;
            }

            uint relIdx = BinaryPrimitives.ReadUInt32LittleEndian(dt.AsSpan(o + 13, 4));
            if (relIdx != 0xFFFFFFFF)
            {
                dBad.Add((i, relIdx));
            }
        }

        string verdict = wBad.Count == 0 ? "✅ PASS" : "❌ FAIL";
        return new HypothesisRow(
            "H27",
            "non-FK logical-idx RelIdxNum [13..16] == 0xFFFFFFFF",
            verdict,
            wBad.Count == 0 ? "all 0xFFFFFFFF" : string.Join(";", wBad.Select(b => $"slot{b.Slot}=0x{b.Val:X8}")),
            dBad.Count == 0 ? "all 0xFFFFFFFF" : string.Join(";", dBad.Select(b => $"slot{b.Slot}=0x{b.Val:X8}")),
            verdict == "✅ PASS" ? "writer matches DAO" : "writer leaves slot at 0; DAO may treat PK as malformed FK");
    }

    private static HypothesisRow CheckLogIdxTrailerInt(byte[] wt, TDefHeader wHdr, byte[] dt, TDefHeader dHdr)
    {
        bool wOk = true, dOk = true;
        uint wFirst = 0, dFirst = 0;
        for (int i = 0; i < wHdr.NumLogIdx; i++)
        {
            int o = wHdr.LogIdxStart + (i * 28);
            if (o + 28 > wt.Length)
            {
                break;
            }

            uint v = BinaryPrimitives.ReadUInt32LittleEndian(wt.AsSpan(o + 24, 4));
            if (i == 0)
            {
                wFirst = v;
            }

            if (v != 0)
            {
                wOk = false;
            }
        }

        for (int i = 0; i < dHdr.NumLogIdx; i++)
        {
            int o = dHdr.LogIdxStart + (i * 28);
            if (o + 28 > dt.Length)
            {
                break;
            }

            uint v = BinaryPrimitives.ReadUInt32LittleEndian(dt.AsSpan(o + 24, 4));
            if (i == 0)
            {
                dFirst = v;
            }

            if (v != 0)
            {
                dOk = false;
            }
        }

        string verdict = wOk == dOk && wOk ? "✅ PASS" : (wOk == dOk ? "⚠️ INFO" : "❌ FAIL");
        return new HypothesisRow(
            "H28",
            "logical-idx bytes [24..27] putInt(0) preserved",
            verdict,
            wOk ? "all 0" : $"slot0=0x{wFirst:X8}",
            dOk ? "all 0" : $"slot0=0x{dFirst:X8}",
            verdict == "✅ PASS" ? "writer matches DAO" : "writer/DAO diverge");
    }

    private static HypothesisRow CheckColDescZeroFields(
        byte[] wt,
        TDefHeader wHdr,
        byte[] dt,
        TDefHeader dHdr,
        string id,
        string title,
        Func<byte[], int, bool> isApplicable,
        (int Off, int Len) range)
    {
        int wApplicable = 0, wBad = 0;
        for (int i = 0; i < wHdr.NumCols; i++)
        {
            int o = wHdr.ColStart + (i * 25);
            if (o + 25 > wt.Length || !isApplicable(wt, o))
            {
                continue;
            }

            wApplicable++;
            for (int k = 0; k < range.Len; k++)
            {
                if (wt[o + range.Off + k] != 0)
                {
                    wBad++;
                    break;
                }
            }
        }

        int dApplicable = 0, dBad = 0;
        for (int i = 0; i < dHdr.NumCols; i++)
        {
            int o = dHdr.ColStart + (i * 25);
            if (o + 25 > dt.Length || !isApplicable(dt, o))
            {
                continue;
            }

            dApplicable++;
            for (int k = 0; k < range.Len; k++)
            {
                if (dt[o + range.Off + k] != 0)
                {
                    dBad++;
                    break;
                }
            }
        }

        if (wApplicable == 0 && dApplicable == 0)
        {
            return new HypothesisRow(id, title, "⚠️ N/A", "0 cols apply", "0 cols apply", "no applicable column shape in this fixture");
        }

        string verdict = wBad == 0 ? "✅ PASS" : "❌ FAIL";
        return new HypothesisRow(
            id,
            title,
            verdict,
            $"{wApplicable - wBad}/{wApplicable} zero",
            $"{dApplicable - dBad}/{dApplicable} zero",
            verdict == "✅ PASS" ? "writer matches DAO" : "writer leaves non-zero; potential buffer-reuse risk");
    }

    private static HypothesisRow CheckRealIdxFlags(byte[] wt, TDefHeader wHdr, byte[] dt, TDefHeader dHdr)
    {
        // PK = first index (RT_Customers has only one).
        const byte UNIQUE = 0x01, IGNORE_NULLS = 0x02, REQUIRED = 0x08, UNKNOWN = 0x80;
        const byte ExpectedMask = UNIQUE | REQUIRED | UNKNOWN;
        if (wHdr.NumRealIdx == 0 || dHdr.NumRealIdx == 0)
        {
            return new HypothesisRow("H37", "PK real-idx flags @46 has UNIQUE|REQUIRED|UNKNOWN", "⚠️ N/A", "—", "—", "no real-idx slot");
        }

        int wOff = wHdr.RealIdxPhysStart + 46;
        int dOff = dHdr.RealIdxPhysStart + 46;
        if (wOff >= wt.Length || dOff >= dt.Length)
        {
            return new HypothesisRow("H37", "PK real-idx flags @46 has UNIQUE|REQUIRED|UNKNOWN", "⚠️ N/A", "—", "—", "TDEF too short");
        }

        byte wFlags = wt[wOff];
        byte dFlags = dt[dOff];
        bool ok = (wFlags & ExpectedMask) == ExpectedMask;
        string verdict = ok ? "✅ PASS" : "❌ FAIL";
        _ = IGNORE_NULLS; // suppress unused warning
        return new HypothesisRow(
            "H37",
            "PK real-idx flags @46 has UNIQUE|REQUIRED|UNKNOWN",
            verdict,
            $"0x{wFlags:X2}",
            $"0x{dFlags:X2}",
            ok ? "writer carries PK bits" : "writer missing UNIQUE/REQUIRED/UNKNOWN bit");
    }

    private static HypothesisRow CheckRealIdxUnknownGap(byte[] wt, TDefHeader wHdr, byte[] dt, TDefHeader dHdr)
    {
        if (wHdr.NumRealIdx == 0 || dHdr.NumRealIdx == 0)
        {
            return new HypothesisRow("H38", "real-idx [42..45] (unknown putInt) == 0", "⚠️ N/A", "—", "—", "no real-idx slot");
        }

        int wBad = 0, dBad = 0;
        uint wFirst = 0, dFirst = 0;
        for (int i = 0; i < wHdr.NumRealIdx; i++)
        {
            int o = wHdr.RealIdxPhysStart + (i * 52) + 42;
            if (o + 4 > wt.Length)
            {
                break;
            }

            uint v = BinaryPrimitives.ReadUInt32LittleEndian(wt.AsSpan(o, 4));
            if (i == 0)
            {
                wFirst = v;
            }

            if (v != 0)
            {
                wBad++;
            }
        }

        for (int i = 0; i < dHdr.NumRealIdx; i++)
        {
            int o = dHdr.RealIdxPhysStart + (i * 52) + 42;
            if (o + 4 > dt.Length)
            {
                break;
            }

            uint v = BinaryPrimitives.ReadUInt32LittleEndian(dt.AsSpan(o, 4));
            if (i == 0)
            {
                dFirst = v;
            }

            if (v != 0)
            {
                dBad++;
            }
        }

        string verdict = wBad == 0 ? (dBad == 0 ? "✅ PASS" : "⚠️ INFO") : "❌ FAIL";
        return new HypothesisRow(
            "H38",
            "real-idx [42..45] (unknown putInt) == 0",
            verdict,
            wBad == 0 ? "all 0" : $"slot0=0x{wFirst:X8}",
            dBad == 0 ? "all 0" : $"slot0=0x{dFirst:X8}",
            verdict == "✅ PASS" ? "writer matches DAO" : "writer/DAO diverge");
    }

    private static HypothesisRow CheckIndexNameLengthPrefix(byte[] wt, TDefHeader wHdr, byte[] dt, TDefHeader dHdr)
    {
        bool wOk = true, dOk = true;
        int wp = wHdr.LogIdxNamesStart;
        for (int i = 0; i < wHdr.NumLogIdx; i++)
        {
            if (wp + 2 > wt.Length)
            {
                wOk = false;
                break;
            }

            int len = BinaryPrimitives.ReadUInt16LittleEndian(wt.AsSpan(wp, 2));
            if ((len & 1) != 0)
            {
                wOk = false; // UCS-2 byte count must be even
            }

            wp += 2 + len;
            if (wp > wt.Length)
            {
                wOk = false;
            }
        }

        int dp = dHdr.LogIdxNamesStart;
        for (int i = 0; i < dHdr.NumLogIdx; i++)
        {
            if (dp + 2 > dt.Length)
            {
                dOk = false;
                break;
            }

            int len = BinaryPrimitives.ReadUInt16LittleEndian(dt.AsSpan(dp, 2));
            if ((len & 1) != 0)
            {
                dOk = false;
            }

            dp += 2 + len;
            if (dp > dt.Length)
            {
                dOk = false;
            }
        }

        string verdict = wOk && dOk ? "✅ PASS" : (wOk != dOk ? "❌ FAIL" : "⚠️ INFO");
        return new HypothesisRow(
            "H42",
            "logical-idx name length-prefix is byte count (even)",
            verdict,
            wOk ? "valid" : "odd / overrun",
            dOk ? "valid" : "odd / overrun",
            verdict == "✅ PASS" ? "writer matches DAO" : "writer wrote char count instead of byte count");
    }

    private static HypothesisRow CheckOwnedFreePages(byte[] wt, byte[] dt)
    {
        uint wOwned = BinaryPrimitives.ReadUInt32LittleEndian(wt.AsSpan(0x37, 4));
        uint wFree = BinaryPrimitives.ReadUInt32LittleEndian(wt.AsSpan(0x3B, 4));
        uint dOwned = BinaryPrimitives.ReadUInt32LittleEndian(dt.AsSpan(0x37, 4));
        uint dFree = BinaryPrimitives.ReadUInt32LittleEndian(dt.AsSpan(0x3B, 4));

        bool wOk = wOwned != 0 && wFree != 0;
        bool dOk = dOwned != 0 && dFree != 0;
        string verdict = wOk && dOk ? "✅ PASS" : (wOk == dOk ? "⚠️ INFO" : "❌ FAIL");
        return new HypothesisRow(
            "H44",
            "TDEF owned_pages[0x37] / free_space_pages[0x3B] non-zero",
            verdict,
            $"owned=0x{wOwned:X8} free=0x{wFree:X8}",
            $"owned=0x{dOwned:X8} free=0x{dFree:X8}",
            verdict == "✅ PASS" ? "writer matches DAO" : "writer left usage-map slot zero — cursor cannot iterate data pages");
    }

    private static async Task<(HypothesisRow H26, HypothesisRow H45)> CheckUsageMapRowAsync(
        AccessReader wr, AccessReader dr, byte[] wt, byte[] dt, long wTdefPage, long dTdefPage)
    {
        // owned_pages slot at TDEF[0x37..0x3A]: byte[0]=row, bytes[1..3]=24-bit page LE.
        (int wRow, long wPage) = ReadUsageMapPointer(wt, 0x37);
        (int dRow, long dPage) = ReadUsageMapPointer(dt, 0x37);

        byte? wType = null, dType = null;
        long? wBack = null, dBack = null;
        try
        {
            byte[] wPageBytes = await wr.GetRawPageBytesAsync(wPage, default);
            (wType, wBack) = ExtractUsageMapRow(wPageBytes, wRow);
        }
        catch (Exception ex) when (ex is IOException or ArgumentException or IndexOutOfRangeException)
        {
            // leave nulls
        }

        try
        {
            byte[] dPageBytes = await dr.GetRawPageBytesAsync(dPage, default);
            (dType, dBack) = ExtractUsageMapRow(dPageBytes, dRow);
        }
        catch (Exception ex) when (ex is IOException or ArgumentException or IndexOutOfRangeException)
        {
            // leave nulls
        }

        // H26: writer's row-0 type byte matches DAO's. The original Jackcess-derived
        // hypothesis (DAO uses MAP_TYPE_REFERENCE = 0x01) is empirically false for
        // small/empty tables — DAO actually uses INLINE (0x00) here. The meaningful
        // check is "writer matches DAO".
        string h26Verdict;
        string h26Notes;
        if (wType is null || dType is null)
        {
            h26Verdict = "⚠️ N/A";
            h26Notes = "row unreachable";
        }
        else if (wType == dType)
        {
            h26Verdict = "✅ PASS";
            h26Notes = dType == 0x01 ? "writer matches DAO (REFERENCE)" : "writer matches DAO (INLINE; original hypothesis disconfirmed)";
        }
        else
        {
            h26Verdict = "❌ FAIL";
            h26Notes = $"writer stamps 0x{wType:X2}, DAO stamps 0x{dType:X2}";
        }

        var h26 = new HypothesisRow(
            "H26",
            "per-table usage-map row 0 type byte (writer == DAO)",
            h26Verdict,
            wType is null ? "row unreachable" : $"page={wPage} row={wRow} type=0x{wType:X2}",
            dType is null ? "row unreachable" : $"page={dPage} row={dRow} type=0x{dType:X2}",
            h26Notes);

        // H45: tdef_back_pointer only applies to REFERENCE rows. For INLINE rows
        // (type 0x00) bytes [1..3] are bitmap data, not a back-pointer. Compare
        // writer-vs-DAO structurally.
        string h45Verdict;
        string h45Notes;
        if (wBack is null || dBack is null || wType is null || dType is null)
        {
            h45Verdict = "⚠️ N/A";
            h45Notes = "row unreachable";
        }
        else if (wType != 0x01 && dType != 0x01)
        {
            h45Verdict = "⚠️ N/A";
            h45Notes = "both rows are INLINE (no back-pointer slot)";
        }
        else if (wType != dType)
        {
            h45Verdict = "❌ FAIL";
            h45Notes = "writer/DAO row types diverge (covered by H26)";
        }
        else
        {
            bool wMatch = wBack == wTdefPage;
            bool dMatch = dBack == dTdefPage;
            h45Verdict = wMatch == dMatch ? (wMatch ? "✅ PASS" : "⚠️ INFO") : "❌ FAIL";
            h45Notes = h45Verdict == "✅ PASS" ? "writer back-pointer matches own TDEF" : "writer/DAO back-pointer semantics diverge";
        }

        var h45 = new HypothesisRow(
            "H45",
            "usage-map row tdef_back_pointer (REFERENCE rows only)",
            h45Verdict,
            wBack is null ? "row unreachable" : $"back=0x{wBack:X6} tdef={wTdefPage}",
            dBack is null ? "row unreachable" : $"back=0x{dBack:X6} tdef={dTdefPage}",
            h45Notes);

        return (h26, h45);
    }

    private static (int Row, long Page) ReadUsageMapPointer(byte[] td, int off)
    {
        // 4 bytes: byte[0]=row, bytes[1..3]=24-bit page LE.
        if (off + 4 > td.Length)
        {
            return (0, 0);
        }

        int row = td[off];
        long page = td[off + 1] | (long)(td[off + 2] << 8) | (long)(td[off + 3] << 16);
        return (row, page);
    }

    private static (byte? Type, long? BackPointer) ExtractUsageMapRow(byte[] page, int rowIndex)
    {
        // Usage-map page layout: page-type byte at 0; row offsets array at offset 14 onward
        // (Jet4 data-page-style header). Each slot is 2 bytes; mask 0x1FFF for offset.
        const int RowsStartOff = 14;
        const int NumRowsOff = 12;
        if (page.Length < RowsStartOff + 2)
        {
            return (null, null);
        }

        ushort numRows = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(NumRowsOff, 2));
        if (rowIndex >= numRows)
        {
            return (null, null);
        }

        int slotOff = RowsStartOff + (rowIndex * 2);
        if (slotOff + 2 > page.Length)
        {
            return (null, null);
        }

        ushort slot = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(slotOff, 2));
        if ((slot & 0xC000) != 0)
        {
            return (null, null);
        }

        int start = slot & 0x1FFF;
        if (start >= page.Length)
        {
            return (null, null);
        }

        byte type = page[start];
        long? back = null;
        if (start + 4 <= page.Length)
        {
            // For MAP_TYPE_REFERENCE the row payload is type(1) + page(3) → bytes [start+1..start+3]
            back = page[start + 1] | (long)(page[start + 2] << 8) | (long)(page[start + 3] << 16);
        }

        return (type, back);
    }

    // ─────── §6 DAO-only diffs (pages writer never touches) ────────────────

    private static void EmitDaoOnlyPageHex(StringBuilder sb, BaselinePageCache baseline, ReaderSnapshot? w, ReaderSnapshot? d)
    {
        _ = sb.AppendLine("## §6 Pages DAO modifies that the writer never touches");
        _ = sb.AppendLine();
        _ = sb.AppendLine("These are the structural updates the writer is missing. Pages 0/1/3 are file-wide system metadata; pages in the high range (e.g. 2843, 2998, 3002) are existing system-table data pages DAO mutates instead of allocating new pages.");
        _ = sb.AppendLine();

        if (d is null)
        {
            _ = sb.AppendLine("> DAO snapshot unavailable.");
            return;
        }

        var writerDiff = w is null
            ? new HashSet<long>()
            : new HashSet<long>(w.PagesDifferingFromBaseline);

        var daoOnly = d.PagesDifferingFromBaseline.Where(p => !writerDiff.Contains(p)).ToList();
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- DAO-only pages (in shared range): {daoOnly.Count} → {string.Join(", ", daoOnly.Select(p => p.ToString(CultureInfo.InvariantCulture)))}");
        _ = sb.AppendLine();

        foreach (long p in daoOnly)
        {
            byte[] basBytes = baseline.GetPage(p);
            if (!d.PageBytes.TryGetValue(p, out byte[]? daoBytes))
            {
                continue;
            }

            int common = Math.Min(basBytes.Length, daoBytes.Length);
            var diffs = new List<int>();
            for (int i = 0; i < common; i++)
            {
                if (basBytes[i] != daoBytes[i])
                {
                    diffs.Add(i);
                }
            }

            string typeStr = daoBytes.Length > 0 ? $"0x{daoBytes[0]:X2}" : "??";
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"### page {p} (type {typeStr})");
            _ = sb.AppendLine();
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Bytes differing from baseline: {diffs.Count}");
            if (diffs.Count > 0)
            {
                _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- First 32 differing offsets: {string.Join(", ", diffs.Take(32).Select(o => $"0x{o:X4}"))}{(diffs.Count > 32 ? ", …" : string.Empty)}");

                // Compute a contiguous window covering the first ~256 bytes of differences.
                int windowStart = Math.Max(0, (diffs[0] / 16) * 16);
                int windowEnd = Math.Min(common, windowStart + 256);
                _ = sb.AppendLine();
                EmitDiffHex(sb, $"page {p}: baseline (left) vs dao (right)", basBytes, daoBytes, windowStart, windowEnd);
            }
        }
    }

    private static void EmitDiffHex(StringBuilder sb, string label, byte[] a, byte[] b, int from, int to)
    {
        _ = sb.AppendLine("```text");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"{label} — bytes 0x{from:X4}..0x{to:X4}");
        for (int row = from; row < to; row += 16)
        {
            var lhs = new StringBuilder();
            var rhs = new StringBuilder();
            var marker = new StringBuilder();
            for (int i = 0; i < 16; i++)
            {
                int o = row + i;
                byte? av = o < a.Length ? a[o] : null;
                byte? bv = o < b.Length ? b[o] : null;
                _ = lhs.Append(av is null ? "   " : $"{av.Value:X2} ");
                _ = rhs.Append(bv is null ? "   " : $"{bv.Value:X2} ");
                _ = marker.Append(av is not null && bv is not null && av.Value != bv.Value ? "<> " : "   ");
            }

            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"{row:X4}  {lhs}| {rhs}| {marker}");
        }

        _ = sb.AppendLine("```");
        _ = sb.AppendLine();
    }

    // ────────────────────────── helpers ─────────────────────────────────────

    private static void EmitSideBySideHex(StringBuilder sb, string label, byte[] a, byte[] b, int maxBytes)
    {
        int n = Math.Min(maxBytes, Math.Max(a.Length, b.Length));
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"```text");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"{label}: writer (left) vs dao (right) — first {n} bytes; '<>' marks per-byte diffs");
        for (int row = 0; row < n; row += 16)
        {
            var lhs = new StringBuilder();
            var rhs = new StringBuilder();
            var marker = new StringBuilder();
            for (int i = 0; i < 16; i++)
            {
                int o = row + i;
                byte? av = o < a.Length ? a[o] : null;
                byte? bv = o < b.Length ? b[o] : null;
                _ = lhs.Append(av is null ? "   " : $"{av.Value:X2} ");
                _ = rhs.Append(bv is null ? "   " : $"{bv.Value:X2} ");
                _ = marker.Append(av is not null && bv is not null && av.Value != bv.Value ? "<> " : "   ");
            }

            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"{row:X4}  {lhs}| {rhs}| {marker}");
        }

        _ = sb.AppendLine("```");
        _ = sb.AppendLine();
    }

    private static void EmitSingleHex(StringBuilder sb, string label, byte[] bytes, int maxBytes)
    {
        int n = Math.Min(maxBytes, bytes.Length);
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"```text");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"{label} — first {n} bytes");
        for (int row = 0; row < n; row += 16)
        {
            var hex = new StringBuilder();
            var ascii = new StringBuilder();
            for (int i = 0; i < 16; i++)
            {
                int o = row + i;
                if (o >= n)
                {
                    _ = hex.Append("   ");
                    _ = ascii.Append(' ');
                    continue;
                }

                _ = hex.Append(CultureInfo.InvariantCulture, $"{bytes[o]:X2} ");
                char c = (char)bytes[o];
                _ = ascii.Append(c is >= ' ' and < (char)127 ? c : '.');
            }

            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"{row:X4}  {hex} {ascii}");
        }

        _ = sb.AppendLine("```");
        _ = sb.AppendLine();
    }

    private static string Join(List<long> pages) =>
        pages.Count == 0 ? "(none)"
        : string.Join(", ", pages.Take(64).Select(p => p.ToString(CultureInfo.InvariantCulture)))
          + (pages.Count > 64 ? ", …" : string.Empty);

    private static string Md(string s) =>
        (s ?? string.Empty).Replace("|", "\\|", StringComparison.Ordinal).Replace("`", "'", StringComparison.Ordinal);

    private static string Truncate(string s)
    {
        s = (s ?? string.Empty).Replace("\r\n", " ", StringComparison.Ordinal).Replace('\n', ' ');
        return s.Length > 250 ? string.Concat(s.AsSpan(0, 250), "…") : s;
    }

    private static string DescribeCompact(int code, string err)
    {
        if (code == 0)
        {
            return "✅ OK";
        }

        if (code == -1)
        {
            return "— " + Md(err);
        }

        if (err.Contains("MSysDb", StringComparison.Ordinal))
        {
            return "❌ MSysDb (3011)";
        }

        if (err.Contains("Object invalid", StringComparison.Ordinal))
        {
            return "❌ Object invalid";
        }

        return $"❌ exit={code} " + Md(Truncate(err));
    }

    private static async Task<List<CatalogEntry>> ReadCatalogAsync(AccessReader r)
    {
        var msys = await r.GetMSysObjectsTableDefAsync(default)
            ?? throw new InvalidOperationException("MSysObjects TDEF missing");
        int idxId = msys.Columns.FindIndex(c => c.Name.Equals("Id", StringComparison.OrdinalIgnoreCase));
        int idxName = msys.Columns.FindIndex(c => c.Name.Equals("Name", StringComparison.OrdinalIgnoreCase));
        int idxType = msys.Columns.FindIndex(c => c.Name.Equals("Type", StringComparison.OrdinalIgnoreCase));
        int idxFlags = msys.Columns.FindIndex(c => c.Name.Equals("Flags", StringComparison.OrdinalIgnoreCase));
        int idxParent = msys.Columns.FindIndex(c => c.Name.Equals("ParentId", StringComparison.OrdinalIgnoreCase));
        var list = new List<CatalogEntry>();
        await foreach (var row in r.EnumerateMSysObjectsRowsAsync(msys, default))
        {
            string Get(int i) => i >= 0 && i < row.Length ? row[i] ?? string.Empty : string.Empty;
            long id = long.TryParse(Get(idxId), NumberStyles.Integer, CultureInfo.InvariantCulture, out long v1) ? v1 : 0;
            long parent = long.TryParse(Get(idxParent), NumberStyles.Integer, CultureInfo.InvariantCulture, out long v2) ? v2 : 0;
            int type = int.TryParse(Get(idxType), NumberStyles.Integer, CultureInfo.InvariantCulture, out int v3) ? v3 : 0;
            long flags = long.TryParse(Get(idxFlags), NumberStyles.Integer, CultureInfo.InvariantCulture, out long v4) ? v4 : 0;
            list.Add(new CatalogEntry(id, Get(idxName), type, flags, id & 0x00FFFFFFL, parent));
        }

        return list;
    }

    private sealed record CatalogEntry(long Id, string Name, int Type, long Flags, long TdefPage, long ParentId);
}
