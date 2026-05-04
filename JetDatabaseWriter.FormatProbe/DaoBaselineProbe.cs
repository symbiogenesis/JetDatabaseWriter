// DAO baseline probe.
//
// Step 1 of docs/design/round-trip-test-failures.md "Recommended next steps":
// build an Access-engine-authored ground-truth copy of NorthwindTraders.accdb
// + a single empty `RT_Customers` table to diff byte-for-byte against the
// writer's N1 output.
//
// Rather than asking a human to do this in the Microsoft Access UI (the design
// doc's literal recommendation), we shell to a SysWOW64 PowerShell host and
// drive `DAO.DBEngine.120` to create the table via the TableDef API. ACE (the
// engine behind both DAO and modern Access UI) emits identical on-disk bytes
// regardless of which entry point creates the TableDef — there is no separate
// "Access UI" file format, so this is a true substitute for the manual step.
//
// USAGE
//   $env:DIAG_RT_DAO_BASELINE = "1"
//   dotnet run --project JetDatabaseWriter.FormatProbe
//
// Outputs land under %TEMP%\JetDatabaseWriter.RtDaoBaseline\:
//   - writer\source.accdb              (NorthwindTraders + RT_Customers via writer)
//   - writer\source.compacted.accdb    (DAO compact output, if it succeeds)
//   - dao\source.accdb                 (NorthwindTraders + RT_Customers via DAO)
//   - dao\source.compacted.accdb       (DAO compact output, if it succeeds)
//   - pages\<page>_writer.bin          (raw bytes of every "interesting" page)
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
using JetDatabaseWriter.Models;

internal static class DaoBaselineProbe
{
    private const string SysWow64PowerShell = @"C:\Windows\SysWOW64\WindowsPowerShell\v1.0\powershell.exe";

    public static async Task<int> RunAsync(string baselinePath, string workRoot)
    {
        if (!File.Exists(baselinePath))
        {
            await Console.Error.WriteLineAsync($"[dao-baseline] baseline not found: {baselinePath}");
            return 1;
        }

        if (!File.Exists(SysWow64PowerShell))
        {
            await Console.Error.WriteLineAsync($"[dao-baseline] SysWOW64 PowerShell not found at {SysWow64PowerShell}; cannot host DAO.DBEngine.120.");
            return 1;
        }

        if (Directory.Exists(workRoot))
        {
            Directory.Delete(workRoot, recursive: true);
        }

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

        // 1. Writer authoring.
        string writerErr = string.Empty;
        try
        {
            await using var w = await AccessWriter.OpenAsync(writerPath, new AccessWriterOptions { UseLockFile = false });
            await w.CreateTableAsync(
                "RT_Customers",
                [
                    new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                    new("Name", typeof(string), maxLength: 100) { IsNullable = false },
                ]);
        }
        catch (Exception ex)
        {
            writerErr = $"{ex.GetType().Name}: {ex.Message}";
            await Console.Error.WriteLineAsync($"[dao-baseline] writer authoring FAILED: {writerErr}");
        }

        // 2. DAO authoring.
        (int daoCreateCode, string daoCreateErr) = RunDaoCreateRtCustomers(daoPath);
        Console.WriteLine($"[dao-baseline] DAO authoring: exit={daoCreateCode}{(daoCreateCode == 0 ? string.Empty : "  err=" + Truncate(daoCreateErr))}");
        if (daoCreateCode != 0)
        {
            await Console.Error.WriteLineAsync($"[dao-baseline] DAO refused to add RT_Customers — cannot produce baseline. stderr below:\n{daoCreateErr}");

            // Still emit a partial report so the writer side is at least dumped.
        }

        // 3. DAO compact verdicts (sanity).
        (int writerCompactCode, string writerCompactErr) = string.IsNullOrEmpty(writerErr)
            ? RunDaoCompact(writerPath, Path.Combine(writerDir, "source.compacted.accdb"))
            : (-1, "skipped — writer authoring failed");
        (int daoCompactCode, string daoCompactErr) = daoCreateCode == 0
            ? RunDaoCompact(daoPath, Path.Combine(daoDir, "source.compacted.accdb"))
            : (-1, "skipped — DAO authoring failed");

        Console.WriteLine($"[dao-baseline] DAO compact (writer copy): exit={writerCompactCode}{(writerCompactCode == 0 ? string.Empty : "  err=" + Truncate(writerCompactErr))}");
        Console.WriteLine($"[dao-baseline] DAO compact (DAO copy):    exit={daoCompactCode}{(daoCompactCode == 0 ? string.Empty : "  err=" + Truncate(daoCompactErr))}");

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
        _ = sb.AppendLine();

        await using (var basReader = await AccessReader.OpenAsync(baselinePath))
        {
            int pgSz = basReader.PageSize;
            long basPageCount = new FileInfo(baselinePath).Length / pgSz;

            ReaderSnapshot? writerSnap = null;
            ReaderSnapshot? daoSnap = null;

            if (string.IsNullOrEmpty(writerErr))
            {
                await using var r = await AccessReader.OpenAsync(writerPath);
                writerSnap = await ReaderSnapshot.CaptureAsync(r, basReader, basPageCount, "writer", pagesDir);
            }

            if (daoCreateCode == 0)
            {
                await using var r = await AccessReader.OpenAsync(daoPath);
                daoSnap = await ReaderSnapshot.CaptureAsync(r, basReader, basPageCount, "dao", pagesDir);
            }

            EmitFileLevel(sb, basPageCount, pgSz, writerSnap, daoSnap);
            EmitCatalogDiff(sb, writerSnap, daoSnap);
            EmitChangedPagesTable(sb, basPageCount, writerSnap, daoSnap);
            await EmitRtCustomersTdefDumpsAsync(sb, writerSnap, daoSnap, pagesDir);
            await EmitNewMSysObjectsRowDumpsAsync(sb, basReader, writerSnap, daoSnap, pagesDir, basPageCount, pgSz);
            await EmitDaoOnlyPageHexAsync(sb, basReader, writerPath, daoPath, basPageCount, pgSz, daoSnap);
        }

        _ = sb.AppendLine();
        _ = sb.AppendLine("## How to use this report");
        _ = sb.AppendLine();
        _ = sb.AppendLine("1. **Compare the new TDEF pages.** §4 dumps the writer's RT_Customers TDEF and the DAO baseline's TDEF side-by-side as hex. Any byte that differs is a candidate explanation for the DAO err 3011 `'MSysDb'` rejection.");
        _ = sb.AppendLine("2. **Compare the new MSysObjects row bytes.** §5 dumps the catalog row body for the writer's and DAO's RT_Customers entries. The `LvProp` (varIdx 8) bytes here are the empirical answer to whether DAO actually requires a non-null `LvProp` for an empty user-table catalog row, and (if so) what the payload looks like.");
        _ = sb.AppendLine("3. **Compare per-table usage-map and PK-leaf pages** by binary-diffing the per-page `.bin` files under `pages\\` (writer vs dao) for the page numbers identified in §3.");
        _ = sb.AppendLine();

        string outPath = Path.Combine(workRoot, "dao-baseline-diff.md");
        await File.WriteAllTextAsync(outPath, sb.ToString());
        Console.WriteLine($"[dao-baseline] wrote {outPath}");
        return 0;
    }

    // ────────────────────────── DAO interop ─────────────────────────────────

    private static (int Code, string StdErr) RunDaoCreateRtCustomers(string dbPath)
    {
        // dbLong=4 (Int32), dbText=10. dbAutoIncrField=16. Required=true on all
        // not-null fields. Index "PrimaryKey" with Primary/Unique/Required and
        // a single CustomerID field.
        string dbLit = dbPath.Replace("'", "''", StringComparison.Ordinal);
        string script = string.Join('\n', new[]
        {
            "$ErrorActionPreference='Stop'",
            $"$path='{dbLit}'",
            "$e = New-Object -ComObject DAO.DBEngine.120",
            "try {",
            "  $db = $e.OpenDatabase($path)",
            "  $td = $db.CreateTableDef('RT_Customers')",
            "  $f1 = $td.CreateField('CustomerID', 4)",          // dbLong
            "  $f1.Attributes = $f1.Attributes -bor 16",          // dbAutoIncrField
            "  $f1.Required = $true",
            "  $td.Fields.Append($f1)",
            "  $f2 = $td.CreateField('Name', 10, 100)",           // dbText, size 100
            "  $f2.Required = $true",
            "  $td.Fields.Append($f2)",
            "  $idx = $td.CreateIndex('PrimaryKey')",
            "  $idx.Primary = $true",
            "  $idx.Unique  = $true",
            "  $idx.Required = $true",
            "  $idxFld = $idx.CreateField('CustomerID')",
            "  $idx.Fields.Append($idxFld)",
            "  $td.Indexes.Append($idx)",
            "  $db.TableDefs.Append($td)",
            "  $db.Close()",
            "  exit 0",
            "} finally { [GC]::Collect(); [GC]::WaitForPendingFinalizers() }",
        });
        return RunPwsh(script, Path.Combine(Path.GetDirectoryName(dbPath)!, "dao-create.ps1"));
    }

    private static (int Code, string StdErr) RunDaoCompact(string src, string dst)
    {
        if (File.Exists(dst))
        {
            File.Delete(dst);
        }

        string srcLit = src.Replace("'", "''", StringComparison.Ordinal);
        string dstLit = dst.Replace("'", "''", StringComparison.Ordinal);
        string script =
            "$ErrorActionPreference='Stop'\n" +
            $"$src='{srcLit}'\n$dst='{dstLit}'\n" +
            "$e=New-Object -ComObject DAO.DBEngine.120\n" +
            "try { $e.CompactDatabase($src,$dst); exit 0 } finally { [GC]::Collect() }\n";
        return RunPwsh(script, Path.Combine(Path.GetDirectoryName(dst)!, "compact.ps1"));
    }

    private static (int Code, string StdErr) RunPwsh(string script, string scriptPath)
    {
        File.WriteAllText(scriptPath, script);
        var psi = new ProcessStartInfo(SysWow64PowerShell)
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
        string err = p.StandardError.ReadToEnd();
        _ = p.StandardOutput.ReadToEnd();
        _ = p.WaitForExit(120_000);
        return (p.ExitCode, err);
    }

    // ────────────────────────── Snapshot / per-file analysis ────────────────

    private sealed class ReaderSnapshot
    {
        public required string Tag { get; init; }

        public required long PageCount { get; init; }

        public required List<long> PagesDifferingFromBaseline { get; init; } // pages in shared range that differ

        public required List<long> PagesAddedBeyondBaseline { get; init; } // pages with index >= baseline count

        public required Dictionary<long, byte> PageTypes { get; init; } // for the union of the above

        public required CatalogEntry? RtCustomers { get; init; }

        public required byte[] RtTdefBytes { get; init; } // empty if not found

        public static async Task<ReaderSnapshot> CaptureAsync(AccessReader r, AccessReader baseline, long baselinePageCount, string tag, string pagesDir)
        {
            int pgSz = r.PageSize;
            long pageCount = new FileInfo(r.HostDatabasePath).Length / pgSz;
            long shared = Math.Min(pageCount, baselinePageCount);

            var differing = new List<long>();
            for (long p = 0; p < shared; p++)
            {
                byte[] a = await r.GetRawPageBytesAsync(p, default);
                byte[] b = await baseline.GetRawPageBytesAsync(p, default);
                if (!a.AsSpan().SequenceEqual(b))
                {
                    differing.Add(p);
                }
            }

            var added = new List<long>();
            for (long p = baselinePageCount; p < pageCount; p++)
            {
                added.Add(p);
            }

            var types = new Dictionary<long, byte>();
            foreach (long p in differing.Concat(added))
            {
                byte[] bytes = await r.GetRawPageBytesAsync(p, default);
                types[p] = bytes.Length > 0 ? bytes[0] : (byte)0xFF;
                await File.WriteAllBytesAsync(Path.Combine(pagesDir, $"page{p:D5}_{tag}.bin"), bytes);
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

    private static async Task EmitNewMSysObjectsRowDumpsAsync(StringBuilder sb, AccessReader baseline, ReaderSnapshot? w, ReaderSnapshot? d, string pagesDir, long basPages, int pgSz)
    {
        _ = sb.AppendLine("## §5 New MSysObjects row bytes");
        _ = sb.AppendLine();
        _ = sb.AppendLine("Locates the data page that hosts the RT_Customers catalog row in each copy (the MSysObjects data page that differs from baseline), extracts the row body via the on-page row-offset table, and dumps it as hex. The `LvProp` (varIdx 8) and `LvExtra` (varIdx 10) payloads are the empirical answer to docs/design/round-trip-test-failures.md hypothesis #6.");
        _ = sb.AppendLine();

        if (w is not null)
        {
            await EmitNewMSysObjectsRowAsync(sb, baseline, w, "writer", pagesDir, basPages, pgSz);
        }

        if (d is not null)
        {
            await EmitNewMSysObjectsRowAsync(sb, baseline, d, "dao",    pagesDir, basPages, pgSz);
        }
    }

    private static async Task EmitNewMSysObjectsRowAsync(StringBuilder sb, AccessReader baseline, ReaderSnapshot snap, string label, string pagesDir, long basPages, int pgSz)
    {
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"### {label}");
        _ = sb.AppendLine();
        if (snap.RtCustomers is null)
        {
            _ = sb.AppendLine("> RT_Customers row not in catalog.");
            _ = sb.AppendLine();
            return;
        }

        // Find the MSysObjects data page (page_type=0x01) in this snapshot that differs from baseline AND
        // contains the catalog row. Since MSysObjects data lives in the shared range (existed before),
        // look in PagesDifferingFromBaseline for type-0x01 pages.
        await using var r = await AccessReader.OpenAsync(snap.Tag == "writer"
            ? Path.Combine(Path.GetDirectoryName(pagesDir)!, "writer", "source.accdb")
            : Path.Combine(Path.GetDirectoryName(pagesDir)!, "dao",    "source.accdb"));

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

        foreach (long p in snap.PagesDifferingFromBaseline)
        {
            if (snap.PageTypes[p] != 0x01)
            {
                continue;
            }

            byte[] page = await r.GetRawPageBytesAsync(p, default);
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

    // ─────── §6 DAO-only diffs (pages writer never touches) ────────────────

    private static async Task EmitDaoOnlyPageHexAsync(StringBuilder sb, AccessReader baseline, string writerPath, string daoPath, long basPages, int pgSz, ReaderSnapshot? d)
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

        await using var w = await AccessReader.OpenAsync(writerPath);
        await using var dr = await AccessReader.OpenAsync(daoPath);

        // Build writer's set of differing-from-baseline pages on the fly.
        // Pages DAO touches in the shared range that the writer does NOT.
        var writerDiff = new HashSet<long>();
        for (long p = 0; p < basPages; p++)
        {
            byte[] a = await w.GetRawPageBytesAsync(p, default);
            byte[] b = await baseline.GetRawPageBytesAsync(p, default);
            if (!a.AsSpan().SequenceEqual(b))
            {
                _ = writerDiff.Add(p);
            }
        }

        var daoOnly = d.PagesDifferingFromBaseline.Where(p => !writerDiff.Contains(p)).ToList();
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- DAO-only pages (in shared range): {daoOnly.Count} → {string.Join(", ", daoOnly.Select(p => p.ToString(CultureInfo.InvariantCulture)))}");
        _ = sb.AppendLine();

        foreach (long p in daoOnly)
        {
            byte[] basBytes = await baseline.GetRawPageBytesAsync(p, default);
            byte[] daoBytes = await dr.GetRawPageBytesAsync(p, default);
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
