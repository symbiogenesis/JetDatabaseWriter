// Diagnostic probe: reads test fixtures and emits annotated TDEF hex dumps to
// docs/format-probe/*-format-probe-appendix.md. One-shot research tool; not part of
// the shipping library. See docs/design/index-and-relationship-format-notes.md
// and docs/design/complex-columns-format-notes.md for what these dumps validate.

using System.Collections.Generic;
using System.Globalization;
using System.Text;
using JetDatabaseWriter;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.ValueDecoding;

string repoRoot = FindRepoRoot();
string fixtures = Path.Combine(repoRoot, "JetDatabaseWriter.Tests", "Databases");
string probeDir = Path.Combine(repoRoot, "docs", "format-probe");

IReadOnlyList<string> selectedModes = ResolveModes(args);
if (selectedModes.Count == 0)
{
    PrintUsage();
    return 0;
}

if (selectedModes.Count == 1 && selectedModes[0].Equals("help", StringComparison.OrdinalIgnoreCase))
{
    PrintUsage();
    return 0;
}

foreach (string selectedMode in selectedModes)
{
    int result = await RunModeAsync(selectedMode, fixtures, probeDir);
    if (result != 0)
    {
        return result;
    }
}

Console.WriteLine($"Done. Check generated files under {probeDir} or the probe temp directory for results.");
return 0;

static IReadOnlyList<string> ResolveModes(string[] args)
{
    var modes = new List<string>();
    for (int index = 0; index < args.Length; index++)
    {
        string arg = args[index];
        if (arg is "-h" or "--help" or "help")
        {
            return ["help"];
        }

        if (arg.Equals("--mode", StringComparison.OrdinalIgnoreCase))
        {
            if (index + 1 >= args.Length)
            {
                return ["help"];
            }

            AddModeValues(modes, args[++index]);
            continue;
        }

        const string modePrefix = "--mode=";
        if (arg.StartsWith(modePrefix, StringComparison.OrdinalIgnoreCase))
        {
            AddModeValues(modes, arg[modePrefix.Length..]);
            continue;
        }

        AddModeValues(modes, arg.StartsWith("--", StringComparison.Ordinal) ? arg[2..] : arg);
    }

    if (modes.Count == 0)
    {
        AddLegacyEnvironmentModes(modes);
    }

    return modes
        .Select(NormalizeMode)
        .Distinct(StringComparer.OrdinalIgnoreCase)
        .ToArray();
}

static void AddModeValues(List<string> modes, string value)
{
    foreach (string mode in value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
    {
        modes.Add(mode);
    }
}

static void AddLegacyEnvironmentModes(List<string> modes)
{
    AddEnvMode(modes, "DIAG_FK_DAO_BASELINE", "fk-dao-baseline");
    AddEnvMode(modes, "DIAG_RT_BISECT", "rt-bisect");
    AddEnvMode(modes, "DIAG_LONG_ROW_PROBE", "long-row-probe");
    AddEnvMode(modes, "DIAG_LONG_ROW_BISECT", "long-row-bisect");
    AddEnvMode(modes, "DIAG_LONG_ROW_SUFFIX", "long-row-suffix");
    AddEnvMode(modes, "DIAG_LONG_ROW_CRC_SWEEP", "long-row-crc-sweep");
    AddEnvMode(modes, "DIAG_MEMO_READBACK", "memo-readback");
    AddEnvMode(modes, "DIAG_RT_DAO_BASELINE", "rt-dao-baseline");
}

static void AddEnvMode(List<string> modes, string variableName, string mode)
{
    if (!string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(variableName)))
    {
        modes.Add(mode);
    }
}

static string NormalizeMode(string mode) => mode.ToUpperInvariant() switch
{
    "HELP" => "help",
    "ALL" or "APPENDIX" or "APPENDICES" => "appendices",
    "INDEX" or "INDEXES" or "INDEX-APPENDIX" => "index",
    "COMPLEX" or "COMPLEX-COLUMNS" or "COMPLEX-APPENDIX" => "complex",
    "JET3" or "JET3-INDEX" or "JET3-INDEXES" => "jet3-index",
    "CATALOG" or "CATALOGS" or "MDB-CATALOG" or "MDB-CATALOGS" => "mdb-catalog",
    "FK" or "FK-DAO" or "FK-DAO-BASELINE" => "fk-dao-baseline",
    "RT" or "RT-DAO" or "RT-DAO-BASELINE" => "rt-dao-baseline",
    "RT-BISECT" or "ROUNDTRIP-BISECT" or "ROUND-TRIP-BISECT" => "rt-bisect",
    "LONG-ROW" or "LONG-ROW-PROBE" => "long-row-probe",
    "LONG-ROW-BISECT" or "LONG-ROW-BOUNDARY" => "long-row-bisect",
    "LONG-ROW-SUFFIX" or "LONG-ROW-SUFFIX-ANALYSIS" => "long-row-suffix",
    "LONG-ROW-CRC" or "LONG-ROW-CRC-SWEEP" or "LONG-ROW-SUFFIX-CRC" => "long-row-crc-sweep",
    "MEMO" or "MEMO-READBACK" => "memo-readback",
    _ => mode,
};

static async Task<int> RunModeAsync(string mode, string fixtures, string probeDir)
{
    switch (mode)
    {
        case "help":
            PrintUsage();
            return 0;
        case "appendices":
            await RunAppendicesAsync(fixtures, probeDir);
            return 0;
        case "index":
            await WriteIndexAppendixAsync(
                Path.Combine(fixtures, "NorthwindTraders.accdb"),
                Path.Combine(probeDir, "format-probe-appendix-index.md"));
            return 0;
        case "complex":
            await WriteComplexAppendixAsync(
                Path.Combine(fixtures, "ComplexFields.accdb"),
                Path.Combine(probeDir, "format-probe-appendix-complex.md"));
            return 0;
        case "jet3-index":
            await WriteJet3IndexAppendixAsync(
                fixtures,
                Path.Combine(probeDir, "format-probe-appendix-jet3-index.md"));
            return 0;
        case "mdb-catalog":
            await WriteMdbCatalogAppendixAsync(
                fixtures,
                Path.Combine(probeDir, "format-probe-appendix-mdb-catalogs.md"));
            return 0;
        case "fk-dao-baseline":
            return await JetDatabaseWriter.FormatProbe.FkDaoBaselineProbe.RunAsync(
                GetRoundTripBaseline(fixtures),
                CreateProbeWorkRoot("JetDatabaseWriter.FkDaoBaseline"));
        case "rt-bisect":
            return await RunRoundTripBisectAsync(fixtures);
        case "long-row-probe":
            return await JetDatabaseWriter.FormatProbe.LongRowProbe.RunAsync(
                fixtures,
                Path.Combine(probeDir, "format-probe-long-row-dump.md"));
        case "long-row-bisect":
            return await JetDatabaseWriter.FormatProbe.LongRowBisect.RunAsync(
                fixtures,
                Path.Combine(probeDir, "format-probe-long-row-bisect.md"));
        case "long-row-suffix":
            return await JetDatabaseWriter.FormatProbe.LongRowSuffixProbe.RunAnalysisAsync(
                fixtures,
                Path.Combine(probeDir, "format-probe-long-row-suffix-analysis.md"));
        case "long-row-crc-sweep":
            return await JetDatabaseWriter.FormatProbe.LongRowSuffixProbe.RunCrcSweepAsync(
                fixtures,
                Path.Combine(probeDir, "format-probe-long-row-crc-sweep.md"));
        case "memo-readback":
            return await RunMemoReadbackAsync();
        case "rt-dao-baseline":
            return await JetDatabaseWriter.FormatProbe.DaoBaselineProbe.RunAsync(
                GetRoundTripBaseline(fixtures),
                CreateProbeWorkRoot("JetDatabaseWriter.RtDaoBaseline"));
        default:
            await Console.Error.WriteLineAsync($"Unknown FormatProbe mode: {mode}");
            PrintUsage();
            return 1;
    }
}

static async Task RunAppendicesAsync(string fixtures, string probeDir)
{
    var appendices = new List<Task>
    {
        WriteIndexAppendixAsync(
            Path.Combine(fixtures, "NorthwindTraders.accdb"),
            Path.Combine(probeDir, "format-probe-appendix-index.md")),

        WriteComplexAppendixAsync(
            Path.Combine(fixtures, "ComplexFields.accdb"),
            Path.Combine(probeDir, "format-probe-appendix-complex.md")),
    };

    // probe: Jet3 (.mdb Access 97) index TDEF + leaf-page layouts. The format probe
    // limitation in docs/design/index-and-relationship-format-notes.md says Jet3
    // rejects IndexDefinition entirely; format probe establishes empirical ground truth
    // for the Jet3 real-idx physical descriptor (39 bytes per mdbtools), the
    // logical-idx entry (20 bytes per mdbtools), and the leaf-page (0x04)
    // bitmask layout (§4.2: bitmask at 0x16, first entry at 0xF8) by dumping
    // the TDEFs and one leaf page per index from the Jackcess V1997 corpus.
    appendices.Add(WriteJet3IndexAppendixAsync(
        fixtures,
        Path.Combine(probeDir, "format-probe-appendix-jet3-index.md")));

    // Catalog probe: Jet3 + Jet4 .mdb + ACCDB catalog scan. The catalog probe in
    // docs/design/index-and-relationship-format-notes.md asks whether
    // MSysIndexes / MSysIndexColumns system tables exist in legacy .mdb
    // formats (they are absent from modern .accdb). The probe recursively
    // discovers every .mdb / .accdb fixture under the Databases/ tree
    // (including the Jackcess/ corpus) so the answer is grounded across
    // every format and Access version we have on disk.
    appendices.Add(WriteMdbCatalogAppendixAsync(
        fixtures,
        Path.Combine(probeDir, "format-probe-appendix-mdb-catalogs.md")));

    await Task.WhenAll(appendices);
}

static async Task<int> RunRoundTripBisectAsync(string fixtures)
{
    string workRoot = CreateProbeWorkRoot("JetDatabaseWriter.RtBisect");
    return await JetDatabaseWriter.FormatProbe.RoundTripBisect.RunAsync(GetRoundTripBaseline(fixtures), workRoot);
}

static string CreateProbeWorkRoot(string probeName)
{
    string root = Path.Combine(
        Path.GetTempPath(),
        probeName,
        DateTimeOffset.UtcNow.ToString("yyyyMMdd-HHmmss-fffffff", CultureInfo.InvariantCulture));
    _ = Directory.CreateDirectory(root);
    return root;
}

static string GetRoundTripBaseline(string fixtures) =>
    Environment.GetEnvironmentVariable("DIAG_RT_BASELINE")
    ?? Path.Combine(fixtures, "NorthwindTraders.accdb");

static void PrintUsage()
{
    Console.WriteLine("FormatProbe modes:");
    Console.WriteLine("  appendices       Generate every appendix that the old default run produced");
    Console.WriteLine("  index            Generate the index/relationship appendix");
    Console.WriteLine("  complex          Generate the complex-column appendix");
    Console.WriteLine("  jet3-index       Generate the Jet3 index appendix");
    Console.WriteLine("  mdb-catalog      Scan the fixture corpus for catalog tables");
    Console.WriteLine("  fk-dao-baseline  Run the FK DAO baseline probe");
    Console.WriteLine("  rt-dao-baseline  Run the round-trip DAO baseline probe");
    Console.WriteLine("  rt-bisect        Run the round-trip bisection probe");
    Console.WriteLine("  long-row-probe   Dump long-row index leaf entries");
    Console.WriteLine("  long-row-bisect  Run long-row chunk-boundary bisection");
    Console.WriteLine("  long-row-suffix  Dump V2010 long-row suffix source diagnostics");
    Console.WriteLine("  long-row-crc-sweep  Run the slow V2010 long-row CRC-16 suffix sweep");
    Console.WriteLine("  memo-readback    Run the memo readback diagnostic");
    Console.WriteLine();
    Console.WriteLine("Usage: dotnet run --project JetDatabaseWriter.FormatProbe -- <mode>[,<mode>]");
    Console.WriteLine("Legacy DIAG_* environment variables still select the matching mode when no CLI mode is supplied.");
}

static async Task<int> RunMemoReadbackAsync()
{
    string dbPath = Environment.GetEnvironmentVariable("DIAG_MEMO_PATH")
        ?? Path.Combine(Path.GetTempPath(), "JetDatabaseWriter.MemoDiag", "writer_memo.accdb");
    Console.WriteLine($"Reading {dbPath}");
    await using var rdr = await AccessReader.OpenAsync(dbPath, new AccessReaderOptions { UseLockFile = false });
    string tableName = Environment.GetEnvironmentVariable("DIAG_MEMO_TABLE") ?? "MemoFidelity";
    var dt = await rdr.ReadDataTableAsync(tableName);
    Console.WriteLine($"Reader sees RowCount={dt!.Rows.Count}");
    foreach (System.Data.DataRow row in dt!.Rows)
    {
        Console.WriteLine($"  Id={row[0]}");
        if (dt.Rows.Count > 5)
        {
            break;
        }
    }

    var catalog = await ReadCatalogAsync(rdr);
    var entry = catalog.First(c => c.Name == tableName);
    Console.WriteLine($"TDEF page = {entry.TdefPage}");
    byte[] tdefPage = await rdr.ReadPageAsync(entry.TdefPage);
    Console.WriteLine($"TDEF[0]={tdefPage[0]:X2} (0x02 expected)");
    int tdefLen = BitConverter.ToInt32(tdefPage, 8);
    int rowCount = BitConverter.ToInt32(tdefPage, 16);
    _ = BitConverter.ToInt32(tdefPage, 36); // data-page hint retained for debugger inspection.
    Console.WriteLine($"TDEF tdef_len={tdefLen} row_count={rowCount}");

    var hex = new StringBuilder("TDEF[0..96] = ");
    for (int index = 0; index < 96; index++)
    {
        _ = hex.Append(tdefPage[index].ToString("X2", CultureInfo.InvariantCulture));
        _ = hex.Append(index % 16 == 15 ? "\n              " : " ");
    }

    Console.WriteLine(hex.ToString());
    Console.WriteLine($"first 4 bytes after position 36 (data page hint): {BitConverter.ToInt32(tdefPage, 36)}");

    int ownedRow = tdefPage[0x37];
    int ownedPage = tdefPage[0x38] | (tdefPage[0x39] << 8) | (tdefPage[0x3A] << 16);
    int freeRow = tdefPage[0x3B];
    int freePage = tdefPage[0x3C] | (tdefPage[0x3D] << 8) | (tdefPage[0x3E] << 16);
    Console.WriteLine($"owned-pages usage-map: row={ownedRow} page={ownedPage}");
    Console.WriteLine($"free-space usage-map: row={freeRow} page={freePage}");

    if (ownedPage > 0 && ownedPage < 100000)
    {
        byte[] usageMap = await rdr.ReadPageAsync(ownedPage);
        Console.WriteLine($"usage-map page {ownedPage}: tag={usageMap[0]:X2}{usageMap[1]:X2} numRows={BitConverter.ToUInt16(usageMap, 12)}");
        var usageMapHex = new StringBuilder($"page[{ownedPage}][0..160] = ");
        for (int index = 0; index < 160; index++)
        {
            _ = usageMapHex.Append(usageMap[index].ToString("X2", CultureInfo.InvariantCulture));
            _ = usageMapHex.Append(index % 16 == 15 ? "\n              " : " ");
        }

        Console.WriteLine(usageMapHex.ToString());

        int rowOffset = usageMap[14 + (ownedRow * 2)] | (usageMap[15 + (ownedRow * 2)] << 8);
        rowOffset &= 0x1FFF;
        Console.WriteLine($"usage-map row{ownedRow} offset = {rowOffset}");

        var rowHex = new StringBuilder("usage-map row body = ");
        for (int index = rowOffset; index < Math.Min(rowOffset + 80, usageMap.Length); index++)
        {
            _ = rowHex.Append(usageMap[index].ToString("X2", CultureInfo.InvariantCulture));
            _ = rowHex.Append((index - rowOffset) % 16 == 15 ? "\n              " : " ");
        }

        Console.WriteLine(rowHex.ToString());

        int freeRowOffset = usageMap[14 + (freeRow * 2)] | (usageMap[15 + (freeRow * 2)] << 8);
        freeRowOffset &= 0x1FFF;
        Console.WriteLine($"usage-map row{freeRow} (free) offset = {freeRowOffset}");
        var freeRowHex = new StringBuilder("usage-map row1 body = ");
        for (int index = freeRowOffset; index < Math.Min(freeRowOffset + 16, usageMap.Length); index++)
        {
            _ = freeRowHex.Append(usageMap[index].ToString("X2", CultureInfo.InvariantCulture));
            _ = freeRowHex.Append(' ');
        }

        Console.WriteLine(freeRowHex.ToString());
    }

    long fileLength = new FileInfo(dbPath).Length;
    int pageSize = rdr.PageSize;
    long maxPage = fileLength / pageSize;
    Console.WriteLine($"file pages = {maxPage}");
    for (long pageNumber = entry.TdefPage + 1; pageNumber <= entry.TdefPage + 60 && pageNumber < maxPage; pageNumber++)
    {
        byte[] page = await rdr.ReadPageAsync(pageNumber);
        if (page[0] != 0x01)
        {
            continue;
        }

        int parentTdef = BitConverter.ToInt32(page, 4);
        if (parentTdef != entry.TdefPage)
        {
            continue;
        }

        int numRows = BitConverter.ToUInt16(page, 12);
        int freeSpace = BitConverter.ToUInt16(page, 2);
        Console.WriteLine($"data page {pageNumber}: numRows={numRows} freeSpace={freeSpace} parentTdef={parentTdef}");
        int rowOffsetWord = page[14] | (page[15] << 8);
        int rowStart = rowOffsetWord & 0x1FFF;
        Console.WriteLine($"  row0 raw offset word = 0x{rowOffsetWord:X4}, start = {rowStart}");
        var pageHex = new StringBuilder($"page[{pageNumber}][0..96] = ");
        for (int index = 0; index < 96; index++)
        {
            _ = pageHex.Append(page[index].ToString("X2", CultureInfo.InvariantCulture));
            _ = pageHex.Append(index % 16 == 15 ? "\n              " : " ");
        }

        Console.WriteLine(pageHex.ToString());

        int rowEnd = page.Length;
        if (numRows > 1)
        {
            int previousRowOffset = page[16] | (page[17] << 8);
            rowEnd = previousRowOffset & 0x1FFF;
        }

        int rowLength = rowEnd - rowStart;
        Console.WriteLine($"  row0 length = {rowLength}");
        var rowBodyHex = new StringBuilder($"page[{pageNumber}] row0 body = ");
        for (int index = rowStart; index < rowEnd && index < rowStart + 200; index++)
        {
            _ = rowBodyHex.Append(page[index].ToString("X2", CultureInfo.InvariantCulture));
            _ = rowBodyHex.Append((index - rowStart) % 16 == 15 ? "\n              " : " ");
        }

        Console.WriteLine(rowBodyHex.ToString());
    }

    return 0;
}

static string FindRepoRoot()
{
    string? d = AppContext.BaseDirectory;
    while (d != null && !File.Exists(Path.Combine(d, "JetDatabaseWriter.slnx")))
    {
        d = Path.GetDirectoryName(d);
    }

    return d ?? throw new InvalidOperationException("Could not locate repo root (JetDatabaseWriter.slnx).");
}

static async Task WriteIndexAppendixAsync(string fixturePath, string outPath)
{
    Console.WriteLine($"Probing {Path.GetFileName(fixturePath)} ...");
    await using var reader = await AccessReader.OpenAsync(fixturePath, new AccessReaderOptions { UseLockFile = false });
    var catalog = await ReadCatalogAsync(reader);

    var sb = new StringBuilder();
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"# Format probe appendix: indexes, PKs, FKs, relationships");
    _ = sb.AppendLine();
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"Source fixture: `JetDatabaseWriter.Tests/Databases/{Path.GetFileName(fixturePath)}`");
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"Generated by: `JetDatabaseWriter.FormatProbe` on {DateTimeOffset.UtcNow:u}");
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"Format: {reader.DatabaseFormat}, page size: {reader.PageSize}");
    _ = sb.AppendLine();
    _ = sb.AppendLine("This appendix is regenerated by re-running `dotnet run --project JetDatabaseWriter.FormatProbe -- index`.");
    _ = sb.AppendLine("It exists to ground the offset claims in [`index-and-relationship-format-notes.md`](../design/index-and-relationship-format-notes.md) against real Access-produced bytes rather than the partial mdbtools spec.");
    _ = sb.AppendLine();

    // Catalog summary
    _ = sb.AppendLine("## Catalog summary (all `MSysObjects` rows)");
    _ = sb.AppendLine();
    _ = sb.AppendLine("| Id | Name | Type | Flags (hex) | TDEF page |");
    _ = sb.AppendLine("|---:|---|---:|---|---:|");
    foreach (var c in catalog.OrderBy(c => c.Id))
    {
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| {c.Id} | `{Md(c.Name)}` | {c.Type} | 0x{unchecked((uint)c.Flags):X8} | {c.TdefPage} |");
    }

    _ = sb.AppendLine();

    // Tables of interest: pick anything with indexes plus a few catalog tables.
    // Note: NorthwindTraders.accdb has NO MSysIndexes/MSysIndexColumns catalog tables —
    // index metadata in modern ACCDB lives ONLY in TDEF blocks. Confirms a key correction
    // to docs/design/index-and-relationship-format-notes.md §6.
    string[] alwaysInclude = ["MSysObjects", "MSysIndexes", "MSysIndexColumns",
                              "MSysRelationships", "MSysComplexColumns", "MSysQueries"];

    var picked = new HashSet<long>();
    foreach (string name in alwaysInclude)
    {
        var match = catalog.FirstOrDefault(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase));
        if (match.TdefPage > 0 && picked.Add(match.TdefPage))
        {
            await EmitTDefAsync(reader, sb, match.Name, match.TdefPage, includeIndexAnnotations: true);
        }
    }

    // Pick up to 4 user tables (no system mask) that actually have indexes
    int userTablesEmitted = 0;
    foreach (var c in catalog
        .Where(c => c.Type == 1 && unchecked((uint)c.Flags & 0x80000000u) == 0 && c.TdefPage > 0)
        .OrderBy(c => c.Id))
    {
        if (userTablesEmitted >= 6)
        {
            break;
        }

        if (picked.Contains(c.TdefPage))
        {
            continue;
        }

        byte[]? td = await reader.GetRawTDefBytesAsync(c.TdefPage, default);
        if (td is null)
        {
            continue;
        }

        int numIdx = (int)U32(td, 47);
        if (numIdx == 0)
        {
            continue;
        }

        _ = picked.Add(c.TdefPage);
        await EmitTDefAsync(reader, sb, c.Name, c.TdefPage, includeIndexAnnotations: true, preloadedBytes: td);
        userTablesEmitted++;
    }

    _ = Directory.CreateDirectory(Path.GetDirectoryName(outPath)!);
    await File.WriteAllTextAsync(outPath, sb.ToString());
    Console.WriteLine($"Wrote {outPath}");
}

static async Task WriteComplexAppendixAsync(string fixturePath, string outPath)
{
    Console.WriteLine($"Probing {Path.GetFileName(fixturePath)} ...");
    await using var reader = await AccessReader.OpenAsync(fixturePath, new AccessReaderOptions { UseLockFile = false });
    var catalog = await ReadCatalogAsync(reader);

    var sb = new StringBuilder();
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"# Format probe appendix: complex columns (Attachment, Multi-value)");
    _ = sb.AppendLine();
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"Source fixture: `JetDatabaseWriter.Tests/Databases/{Path.GetFileName(fixturePath)}`");
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"Generated by: `JetDatabaseWriter.FormatProbe` on {DateTimeOffset.UtcNow:u}");
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"Format: {reader.DatabaseFormat}, page size: {reader.PageSize}");
    _ = sb.AppendLine();
    _ = sb.AppendLine("Regenerate with `dotnet run --project JetDatabaseWriter.FormatProbe -- complex`. See [`complex-columns-format-notes.md`](../design/complex-columns-format-notes.md) for the unverified spec this appendix grounds.");
    _ = sb.AppendLine();

    // Catalog summary
    _ = sb.AppendLine("## Catalog summary (all `MSysObjects` rows)");
    _ = sb.AppendLine();
    _ = sb.AppendLine("| Id | Name | Type | Flags (hex) | TDEF page |");
    _ = sb.AppendLine("|---:|---|---:|---|---:|");
    foreach (var c in catalog.OrderBy(c => c.Id))
    {
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| {c.Id} | `{Md(c.Name)}` | {c.Type} | 0x{unchecked((uint)c.Flags):X8} | {c.TdefPage} |");
    }

    _ = sb.AppendLine();

    // Find every table with a complex column, plus MSysComplexColumns,
    // every flat table (`f_*` naming convention), every type/template table (`MSysComplexType_*`).
    var hits = new List<(string Name, long Page, string Reason, byte[]? Bytes)>();

    foreach (var c in catalog.Where(c => c.Type == 1 && c.TdefPage > 0))
    {
        byte[]? bytes = await reader.GetRawTDefBytesAsync(c.TdefPage, default);
        if (bytes is null)
        {
            continue;
        }

        bool hasComplex = HasComplexColumn(bytes, reader.DatabaseFormat);
        if (hasComplex && !c.Name.StartsWith("f_", StringComparison.Ordinal))
        {
            hits.Add((c.Name, c.TdefPage, "parent table — has T_ATTACHMENT (0x11) or T_COMPLEX (0x12) column", bytes));
        }
    }

    foreach (var c in catalog.Where(c => c.Name.Equals("MSysComplexColumns", StringComparison.OrdinalIgnoreCase)))
    {
        hits.Insert(0, (c.Name, c.TdefPage, "catalog: `MSysComplexColumns` schema", null));
    }

    foreach (var c in catalog.Where(c => c.Type == 1 && c.Name.StartsWith("MSysComplexType_", StringComparison.Ordinal)))
    {
        hits.Add((c.Name, c.TdefPage, "complex-type template table (Jackcess `typeObjTable`)", null));
    }

    foreach (var c in catalog.Where(c => c.Type == 1 && c.Name.StartsWith("f_", StringComparison.Ordinal)))
    {
        hits.Add((c.Name, c.TdefPage, "hidden flat (child) table — `f_<guid>_<userColName>`", null));
    }

    foreach (var hit in hits.DistinctBy(h => h.Page))
    {
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"### Reason: {hit.Reason}");
        await EmitTDefAsync(reader, sb, hit.Name, hit.Page, includeIndexAnnotations: false, includeComplexAnnotations: true, preloadedBytes: hit.Bytes);
    }

    _ = Directory.CreateDirectory(Path.GetDirectoryName(outPath)!);
    await File.WriteAllTextAsync(outPath, sb.ToString());
    Console.WriteLine($"Wrote {outPath}");
}

static async Task WriteMdbCatalogAppendixAsync(string fixturesDir, string outPath)
{
    // Recursively discover every .mdb / .accdb under fixturesDir.
    var fixturePaths = Directory
        .EnumerateFiles(fixturesDir, "*.*", SearchOption.AllDirectories)
        .Where(p =>
        {
            string ext = Path.GetExtension(p);
            return string.Equals(ext, ".mdb", StringComparison.OrdinalIgnoreCase)
                || string.Equals(ext, ".accdb", StringComparison.OrdinalIgnoreCase);
        })
        .OrderBy(p => p, StringComparer.OrdinalIgnoreCase)
        .ToArray();

    var sb = new StringBuilder();
    _ = sb.AppendLine("# Format probe appendix: legacy `.mdb` and `.accdb` catalog tables");
    _ = sb.AppendLine();
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"Generated by: `JetDatabaseWriter.FormatProbe` on {DateTimeOffset.UtcNow:u}");
    _ = sb.AppendLine();
    _ = sb.AppendLine("Regenerate with `dotnet run --project JetDatabaseWriter.FormatProbe -- mdb-catalog`.");
    _ = sb.AppendLine("Set `DIAG_CATALOG_DOP=N` to override the bounded parallel scan degree (default: up to 4).");
    _ = sb.AppendLine();
    _ = sb.AppendLine("This appendix grounds the catalog probe question in [`index-and-relationship-format-notes.md`](../design/index-and-relationship-format-notes.md) §6:");
    _ = sb.AppendLine("**do legacy `.mdb` (Jet3 / Jet4) or `.accdb` files contain `MSysIndexes` / `MSysIndexColumns` system tables, or is index metadata always carried inside the per-table TDEF block?**");
    _ = sb.AppendLine();
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"Probed every `.mdb` / `.accdb` under `JetDatabaseWriter.Tests/Databases/` ({fixturePaths.Length} files), including the upstream Jackcess corpus (`Databases/Jackcess/V1997/` … `V2019/`).");
    _ = sb.AppendLine();

    using var catalogScanThrottle = new System.Threading.SemaphoreSlim(GetCatalogProbeDegreeOfParallelism());
    CatalogScanResult[] scans = await Task.WhenAll(
        fixturePaths.Select(path => ScanCatalogFixtureAsync(fixturesDir, path, catalogScanThrottle)));

    var verdicts = scans
        .Select(scan => (scan.RelPath, scan.Format, scan.CatalogRows, scan.HasIndexes, scan.HasIndexColumns, scan.HasRelationships, scan.AnyIndexNamed, scan.Error))
        .ToList();
    var withIndexCatalog = scans
        .Where(scan => scan.IndexCatalog is not null)
        .Select(scan => (scan.RelPath, Catalog: scan.IndexCatalog!))
        .ToList();

    var msysNameCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
    var anyIndexNameCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
    foreach (CatalogScanResult scan in scans)
    {
        foreach (string name in scan.MsysNames)
        {
            msysNameCounts[name] = msysNameCounts.GetValueOrDefault(name) + 1;
        }

        foreach (string name in scan.IndexNames)
        {
            anyIndexNameCounts[name] = anyIndexNameCounts.GetValueOrDefault(name) + 1;
        }
    }

    int idxHits = verdicts.Count(v => v.HasIndexes == true);
    int idxColHits = verdicts.Count(v => v.HasIndexColumns == true);
    int errors = verdicts.Count(v => v.Error is not null);
    int totalCatalogRows = verdicts.Sum(v => v.CatalogRows);

    _ = sb.AppendLine("## Headline result");
    _ = sb.AppendLine();
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Files probed: **{fixturePaths.Length}**");
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Files that opened cleanly: **{fixturePaths.Length - errors}**");
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Total `MSysObjects` rows enumerated across all fixtures: **{totalCatalogRows:N0}**");
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Files containing `MSysIndexes`: **{idxHits}**");
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Files containing `MSysIndexColumns`: **{idxColHits}**");
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Files that failed to open (encryption, format, etc.): **{errors}**");
    _ = sb.AppendLine();
    if (idxHits == 0 && idxColHits == 0)
    {
        _ = sb.AppendLine("**Verdict.** No fixture in the corpus — across every Access format and version we test against — contains either index-catalog table. Index metadata lives entirely in the per-table TDEF block (sections §3.1 / §3.2) for every JET format this library writes. catalog probe has no work item.");
    }
    else
    {
        _ = sb.AppendLine("**Verdict.** At least one fixture contains an index-catalog table. The detailed catalogs for those fixtures are reproduced below — catalog probe must populate the matching system table when writing for that format/version.");
    }

    _ = sb.AppendLine();

    // Sanity check #1: did we actually enumerate the system rows? If yes, every
    // fixture's catalog should include `MSysObjects` itself, plus the usual
    // companions (`MSysACEs`, `MSysQueries`, `MSysRelationships`, …). If our
    // enumeration were silently dropping rows with the system flag, this table
    // would be empty or much shorter than expected.
    _ = sb.AppendLine("## Sanity check: every distinct `MSys*` name observed across the corpus");
    _ = sb.AppendLine();
    _ = sb.AppendLine("If the catalog enumeration were silently filtering system rows, this table would be empty or missing entries like `MSysObjects` / `MSysACEs`. The fact that it lists ~20+ system names with realistic occurrence counts confirms the enumeration is exhaustive.");
    _ = sb.AppendLine();
    _ = sb.AppendLine("| `MSys*` name | Fixtures it appears in (out of " + (fixturePaths.Length - errors) + ") |");
    _ = sb.AppendLine("|---|---:|");
    foreach (var kv in msysNameCounts.OrderByDescending(kv => kv.Value).ThenBy(kv => kv.Key, StringComparer.OrdinalIgnoreCase))
    {
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| `{Md(kv.Key)}` | {kv.Value} |");
    }

    _ = sb.AppendLine();

    // Sanity check #2: any name (system or user) whose text contains the
    // substring "index". This catches hypothetical alternate spellings
    // (`MSys IndexColumns`, `MSysIndexInfo`, etc.) AND surfaces user-named
    // tables in the index-test fixtures so it's obvious the enumeration is
    // returning them.
    _ = sb.AppendLine("## Sanity check: every distinct name containing the substring \"index\" (case-insensitive)");
    _ = sb.AppendLine();
    _ = sb.AppendLine("If `MSysIndexes` existed under a slightly different spelling (e.g. extra whitespace or alternate casing), it would surface here even though the headline counter (which uses an exact-string match) would miss it. The Jackcess index-exercising fixtures (`indexTest*`, `bigIndexTest*`, etc.) put many user tables in the corpus whose names contain \"Index\" — those naturally appear, but **no `MSys*Index*` row does**.");
    _ = sb.AppendLine();
    _ = sb.AppendLine("| Name | Fixtures it appears in |");
    _ = sb.AppendLine("|---|---:|");
    foreach (var kv in anyIndexNameCounts.OrderByDescending(kv => kv.Value).ThenBy(kv => kv.Key, StringComparer.OrdinalIgnoreCase))
    {
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| `{Md(kv.Key)}` | {kv.Value} |");
    }

    _ = sb.AppendLine();

    _ = sb.AppendLine("## Per-fixture verdict");
    _ = sb.AppendLine();
    _ = sb.AppendLine("| Fixture (relative to `Databases/`) | Format | Catalog rows | `MSysIndexes` | `MSysIndexColumns` | `MSysRelationships` | First name containing \"index\" (if any) | Notes |");
    _ = sb.AppendLine("|---|---|---:|:---:|:---:|:---:|---|---|");
    foreach (var v in verdicts)
    {
        string idx = v.HasIndexes is null ? "—" : (v.HasIndexes.Value ? "**yes**" : "no");
        string idxCols = v.HasIndexColumns is null ? "—" : (v.HasIndexColumns.Value ? "**yes**" : "no");
        string rel = v.HasRelationships is null ? "—" : (v.HasRelationships.Value ? "yes" : "no");
        string anyIdx = v.AnyIndexNamed is null ? string.Empty : $"`{Md(v.AnyIndexNamed)}`";
        string note = v.Error is null ? string.Empty : $"open failed: {Md(v.Error)}";
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| `{Md(v.RelPath)}` | {v.Format} | {v.CatalogRows} | {idx} | {idxCols} | {rel} | {anyIdx} | {note} |");
    }

    _ = sb.AppendLine();

    if (withIndexCatalog.Count > 0)
    {
        _ = sb.AppendLine("## Catalogs of fixtures with index-catalog tables");
        _ = sb.AppendLine();
        foreach (var (relPath, catalog) in withIndexCatalog)
        {
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"### `{Md(relPath)}`");
            _ = sb.AppendLine();
            _ = sb.AppendLine("| Id | Name | Type | Flags (hex) | TDEF page |");
            _ = sb.AppendLine("|---:|---|---:|---|---:|");
            foreach (var c in catalog.OrderBy(c => c.Id))
            {
                _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| {c.Id} | `{Md(c.Name)}` | {c.Type} | 0x{unchecked((uint)c.Flags):X8} | {c.TdefPage} |");
            }

            _ = sb.AppendLine();
        }
    }

    _ = Directory.CreateDirectory(Path.GetDirectoryName(outPath)!);
    await File.WriteAllTextAsync(outPath, sb.ToString());
    Console.WriteLine($"Wrote {outPath}");
}

static int GetCatalogProbeDegreeOfParallelism()
{
    string? configured = Environment.GetEnvironmentVariable("DIAG_CATALOG_DOP");
    if (int.TryParse(configured, NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsed) && parsed > 0)
    {
        return Math.Min(parsed, 32);
    }

    return Math.Max(1, Math.Min(Environment.ProcessorCount, 4));
}

static async Task<CatalogScanResult> ScanCatalogFixtureAsync(
    string fixturesDir,
    string fixturePath,
    System.Threading.SemaphoreSlim throttle)
{
    string relPath = Path.GetRelativePath(fixturesDir, fixturePath).Replace('\\', '/');
    await throttle.WaitAsync();
    try
    {
        Console.WriteLine($"Probing {relPath} ...");
        await using var reader = await AccessReader.OpenAsync(fixturePath, new AccessReaderOptions { UseLockFile = false });
        var catalog = await ReadCatalogAsync(reader);
        bool hasIdx = catalog.Any(c => c.Name.Equals("MSysIndexes", StringComparison.OrdinalIgnoreCase));
        bool hasIdxCols = catalog.Any(c => c.Name.Equals("MSysIndexColumns", StringComparison.OrdinalIgnoreCase));
        bool hasRel = catalog.Any(c => c.Name.Equals("MSysRelationships", StringComparison.OrdinalIgnoreCase));
        string? anyIdx = catalog
            .Where(c => c.Name.Contains("index", StringComparison.OrdinalIgnoreCase))
            .Select(c => c.Name)
            .FirstOrDefault();

        List<string> msysNames = catalog
            .Where(c => c.Name.StartsWith("MSys", StringComparison.OrdinalIgnoreCase))
            .Select(c => c.Name)
            .ToList();
        List<string> indexNames = catalog
            .Where(c => c.Name.Contains("index", StringComparison.OrdinalIgnoreCase))
            .Select(c => c.Name)
            .ToList();
        List<(long Id, string Name, int Type, long Flags, long TdefPage)>? indexCatalog = hasIdx || hasIdxCols
            ? catalog
            : null;

        return new CatalogScanResult(
            relPath,
            reader.DatabaseFormat.ToString(),
            catalog.Count,
            hasIdx,
            hasIdxCols,
            hasRel,
            anyIdx,
            null,
            indexCatalog,
            msysNames,
            indexNames);
    }
    catch (Exception ex)
    {
        return new CatalogScanResult(
            relPath,
            "?",
            0,
            null,
            null,
            null,
            null,
            ex.GetType().Name + ": " + ex.Message,
            null,
            [],
            []);
    }
    finally
    {
        _ = throttle.Release();
    }
}

static async Task<List<(long Id, string Name, int Type, long Flags, long TdefPage)>> ReadCatalogAsync(AccessReader reader)
{
    var msys = await reader.GetMSysObjectsTableDefAsync(default)
               ?? throw new InvalidOperationException("MSysObjects TDEF not found.");
    int idxId = msys.Columns.FindIndex(c => string.Equals(c.Name, "Id", StringComparison.OrdinalIgnoreCase));
    int idxName = msys.Columns.FindIndex(c => string.Equals(c.Name, "Name", StringComparison.OrdinalIgnoreCase));
    int idxType = msys.Columns.FindIndex(c => string.Equals(c.Name, "Type", StringComparison.OrdinalIgnoreCase));
    int idxFlags = msys.Columns.FindIndex(c => string.Equals(c.Name, "Flags", StringComparison.OrdinalIgnoreCase));

    var result = new List<(long Id, string Name, int Type, long Flags, long TdefPage)>();
    await foreach (var row in reader.EnumerateMSysObjectsRowsAsync(msys, default))
    {
        long id = ParseLong(SafeGet(row, idxId));
        string name = SafeGet(row, idxName);
        int type = (int)ParseLong(SafeGet(row, idxType));
        long flags = ParseLong(SafeGet(row, idxFlags));
        long page = id & 0x00FFFFFFL;
        result.Add((id, name, type, flags, page));
    }

    return result;
}

static string SafeGet(string[] row, int idx) =>
    (idx >= 0 && idx < row.Length) ? row[idx] : string.Empty;

static long ParseLong(string s) =>
    long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out long v) ? v : 0;

static bool HasComplexColumn(byte[] td, DatabaseFormat fmt)
{
    if (fmt == DatabaseFormat.Jet3Mdb)
    {
        return false;
    }

    int numCols = U16(td, 45);
    int numRealIdx = (int)U32(td, 51);
    if (numCols < 0 || numCols > 4096 || numRealIdx < 0 || numRealIdx > 1000)
    {
        return false;
    }

    int colStart = 63 + (numRealIdx * 12);
    if (colStart + (numCols * 25) > td.Length)
    {
        return false;
    }

    for (int i = 0; i < numCols; i++)
    {
        byte t = td[colStart + (i * 25)];
        if (t == 0x11 || t == 0x12)
        {
            return true;
        }
    }

    return false;
}

static async Task WriteJet3IndexAppendixAsync(string fixturesDir, string outPath)
{
    // Curated short-list of Jackcess V1997 fixtures whose user tables are known
    // to carry indexes. These ground format probe — see
    // docs/design/index-and-relationship-format-notes.md §7 format probe row.
    string[] candidates =
    [
        Path.Combine("Jackcess", "V1997", "indexTestV1997.mdb"),
        Path.Combine("Jackcess", "V1997", "compIndexTestV1997.mdb"),
        Path.Combine("Jackcess", "V1997", "testIndexCodesV1997.mdb"),
        Path.Combine("Jackcess", "V1997", "testV1997.mdb"),
        Path.Combine("mdbtools", "nwind.mdb"),
        "Jet3Test.mdb",
    ];

    var sb = new StringBuilder();
    _ = sb.AppendLine("# Format probe appendix: Jet3 index TDEF + leaf-page layouts");
    _ = sb.AppendLine();
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"Generated by: `JetDatabaseWriter.FormatProbe` on {DateTimeOffset.UtcNow:u}");
    _ = sb.AppendLine();
    _ = sb.AppendLine("This appendix grounds the format probe phase in [`index-and-relationship-format-notes.md`](index-and-relationship-format-notes.md):");
    _ = sb.AppendLine("**lift the \"Jet3 (`.mdb` Access 97) rejects `IndexDefinition` entirely\" limitation by pinning the on-disk layout of the Jet3 real-idx physical descriptor (39 bytes per mdbtools), the Jet3 logical-idx entry (20 bytes per mdbtools), and the Jet3 index leaf page (`0x04`) bitmask layout (§4.2: bitmask at `0x16`, first entry at `0xF8`).**");
    _ = sb.AppendLine();
    _ = sb.AppendLine("Each fixture below is dumped TDEF-by-TDEF for the user tables that carry at least one logical index, plus a hex dump of the first index leaf page reachable from each real-idx `first_dp`. Compare against the §3.1 / §3.2 / §4.2 mappings; mismatches indicate the spec needs amending before Jet3 empty-leaf can ship safely.");
    _ = sb.AppendLine();
    _ = sb.AppendLine("Regenerate with `dotnet run --project JetDatabaseWriter.FormatProbe -- jet3-index`.");
    _ = sb.AppendLine();

    foreach (string rel in candidates)
    {
        string full = Path.Combine(fixturesDir, rel);
        if (!File.Exists(full))
        {
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"## `{Md(rel)}` — _missing_");
            _ = sb.AppendLine();
            continue;
        }

        Console.WriteLine($"Probing Jet3 indexes in {rel} ...");
        await using var reader = await AccessReader.OpenAsync(full, new AccessReaderOptions { UseLockFile = false });
        if (reader.DatabaseFormat != DatabaseFormat.Jet3Mdb)
        {
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"## `{Md(rel)}` — _skipped (not Jet3, format = {reader.DatabaseFormat})_");
            _ = sb.AppendLine();
            continue;
        }

        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"## `{Md(rel)}`");
        _ = sb.AppendLine();
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Format: {reader.DatabaseFormat}, page size: {reader.PageSize}, code page: {reader.CodePage}");
        _ = sb.AppendLine();

        var catalog = await ReadCatalogAsync(reader);
        int userTablesEmitted = 0;

        // Pick up to 4 user tables (Type=1, no system bit) whose TDEF declares numIdx > 0.
        foreach (var c in catalog
            .Where(c => c.Type == 1 && unchecked((uint)c.Flags & 0x80000000u) == 0 && c.TdefPage > 0)
            .OrderBy(c => c.Id))
        {
            if (userTablesEmitted >= 4)
            {
                break;
            }

            byte[]? td = await reader.GetRawTDefBytesAsync(c.TdefPage, default);
            if (td is null)
            {
                continue;
            }

            // Jet3 num_idx lives at TDEF offset 27 (4 bytes), num_real_idx at 31.
            int numIdx = (int)U32(td, 27);
            int numRealIdx = (int)U32(td, 31);
            if (numIdx == 0)
            {
                continue;
            }

            await EmitTDefAsync(reader, sb, c.Name, c.TdefPage, includeIndexAnnotations: true, preloadedBytes: td);
            await EmitJet3LeafPagesAsync(reader, sb, td, numRealIdx);
            userTablesEmitted++;
        }

        if (userTablesEmitted == 0)
        {
            _ = sb.AppendLine("> No user table in this fixture carried `numIdx > 0`.");
            _ = sb.AppendLine();
        }
    }

    _ = Directory.CreateDirectory(Path.GetDirectoryName(outPath)!);
    await File.WriteAllTextAsync(outPath, sb.ToString());
    Console.WriteLine($"Wrote {outPath}");
}

// leaf-page dumper. For every real-idx in the supplied Jet3 TDEF, locates
// the `first_dp` page number and dumps the first leaf (page_type 0x04) bytes
// in their entirety. The dump is intended to be cross-referenced with §4.1
// (page header) and §4.2 (Jet3 bitmask at 0x16, first entry at 0xF8).
//
// Jet3's leading 8-byte real-idx skip entry block lives between the TDEF
// header (ends at 43) and the column descriptors (start at 43 + numRealIdx*8).
// mdbtools labels these per-entry fields `unknown(?) + num_idx_rows(?) + unknown(?)`
// — but per the Jet3 format-probe of `indexTestV1997.mdb` they almost certainly
// carry the `first_dp` page pointer (Jet4 carries it inside the 52-byte phys
// descriptor; Jet3 only has 39 bytes there with no obvious page-pointer slot).
// This helper tries multiple candidate offsets and reports which one resolves
// to a valid index page (`page_type == 0x03 || 0x04`).
static async Task EmitJet3LeafPagesAsync(AccessReader reader, StringBuilder sb, byte[] td, int numRealIdx)
{
    if (numRealIdx == 0)
    {
        return;
    }

    _ = sb.AppendLine("### format probe leaf-page resolution probe");
    _ = sb.AppendLine();
    _ = sb.AppendLine("For each real-idx, this section probes plausible `first_dp` candidates inside the 8-byte skip entry (offset 43 + i*8) AND inside the 39-byte phys descriptor, then dumps the first page that resolves to `page_type` `0x03` (intermediate) or `0x04` (leaf).");
    _ = sb.AppendLine();

    // Recompute Jet3 section offsets to reach the phys descriptor block.
    const int blockEnd = 43;
    const int realIdxEntrySz = 8;
    const int colDescSz = 18;
    const int physSz = 39;
    const int numColsOff = 25;
    int numCols = U16(td, numColsOff);
    int colStart = blockEnd + (numRealIdx * realIdxEntrySz);
    int namesStart = colStart + (numCols * colDescSz);

    // Walk column-name length-prefix to find phys-descriptor start.
    int pos = namesStart;
    for (int i = 0; i < numCols; i++)
    {
        if (pos >= td.Length)
        {
            break;
        }

        int len = td[pos++];
        if (pos + len > td.Length)
        {
            pos = -1;
            break;
        }

        pos += len;
    }

    int physStart = pos;

    for (int i = 0; i < numRealIdx; i++)
    {
        // Candidate 1: leading 8-byte skip entry, bytes 0..3 (LE u32).
        int skipStart = blockEnd + (i * realIdxEntrySz);
        long? skip0 = skipStart + 4 <= td.Length ? U32(td, skipStart + 0) : null;
        long? skip4 = skipStart + 8 <= td.Length ? U32(td, skipStart + 4) : null;

        // Candidate 2: somewhere inside the 39-byte phys descriptor.
        // mdbtools layout doesn't cleanly leave 4 bytes for first_dp, but probe
        // anyway — there is a 5-byte tail at offset 34..38 we can interrogate.
        long? phys34 = physStart >= 0 && physStart + (i * physSz) + 38 <= td.Length
            ? U32(td, physStart + (i * physSz) + 34)
            : null;

        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"#### Real-idx #{i} candidates");
        _ = sb.AppendLine();
        _ = sb.AppendLine("| Candidate | Source bytes | Value | Resolves? |");
        _ = sb.AppendLine("|---|---|---:|---|");
        long pageCount = new FileInfo(reader.HostDatabasePath).Length / reader.PageSize;
        await DescribeCandidateAsync(reader, sb, "skip entry [0..3]", skip0, pageCount);
        await DescribeCandidateAsync(reader, sb, "skip entry [4..7]", skip4, pageCount);
        await DescribeCandidateAsync(reader, sb, "phys desc [34..37]", phys34, pageCount);
        _ = sb.AppendLine();

        long? leafPage = await PickFirstResolvableAsync(reader, [skip0, skip4, phys34], pageCount);
        if (leafPage is null)
        {
            _ = sb.AppendLine("> No candidate resolved to a `0x03`/`0x04` page.");
            _ = sb.AppendLine();
            continue;
        }

        byte[] page;
        try
        {
            page = await reader.GetRawPageBytesAsync(leafPage.Value, default);
        }
        catch (Exception ex)
        {
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"> Read of page {leafPage} failed: {ex.GetType().Name}: {Md(ex.Message)}");
            _ = sb.AppendLine();
            continue;
        }

        byte pt = page[0];
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"##### Resolved page {leafPage} (`page_type = 0x{pt:X2}` — {(pt == 0x04 ? "leaf" : pt == 0x03 ? "intermediate" : pt == 0x01 ? "data (tiny-table fallback)" : "?")})");
        _ = sb.AppendLine();
        _ = sb.AppendLine("- §4.1 header decode:");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"  - byte 0 page_type:    0x{page[0]:X2}");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"  - byte 1 unknown:      0x{page[1]:X2}");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"  - bytes 2..3 free_space: {U16(page, 2)}");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"  - bytes 4..7 parent_page: {U32(page, 4)}");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"  - bytes 8..11 prev_page:  {U32(page, 8)}");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"  - bytes 12..15 next_page: {U32(page, 12)}");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"  - bytes 16..19 tail_page: {U32(page, 16)}");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"  - bytes 20..21 pref_len:  {U16(page, 20)}");
        _ = sb.AppendLine();
        _ = sb.AppendLine("- §4.2 Jet3 entry-start bitmask spans `[0x16 .. 0xF7]` inclusive (226 bytes = 1808 bits); first entry begins at `0xF8`.");
        _ = sb.AppendLine();
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- First 0x16 bytes of the bitmask ({Hex(page, 0x16, 0x16)}).");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- First 32 bytes of the entry payload starting at 0xF8: `{Hex(page, 0xF8, 32)}`");
        _ = sb.AppendLine();
        _ = sb.AppendLine("Full page hex dump:");
        _ = sb.AppendLine();
        _ = sb.AppendLine("```");
        _ = sb.Append(HexDump(page, 0, page.Length, baseOffset: 0));
        _ = sb.AppendLine("```");
        _ = sb.AppendLine();
    }
}

static async Task DescribeCandidateAsync(AccessReader reader, StringBuilder sb, string label, long? value, long pageCount)
{
    if (value is null)
    {
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| {label} | _out of range_ | — | — |");
        return;
    }

    long v = value.Value;
    string verdict = "no";
    if (IsValidPageNumber(v, pageCount))
    {
        try
        {
            byte[] p = await reader.GetRawPageBytesAsync(v, default);
            byte pt = p[0];
            if (pt is 0x03 or 0x04 or 0x01)
            {
                verdict = $"yes (page_type=0x{pt:X2})";
            }
        }
        catch
        { /* unreachable / EOF — leave 'no' */
        }
    }

    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| {label} | LE u32 | {v} | {verdict} |");
}

static async Task<long?> PickFirstResolvableAsync(AccessReader reader, IEnumerable<long?> candidates, long pageCount)
{
    foreach (long? c in candidates)
    {
        if (c is null)
        {
            continue;
        }

        long v = c.Value;
        if (!IsValidPageNumber(v, pageCount))
        {
            continue;
        }

        try
        {
            byte[] p = await reader.GetRawPageBytesAsync(v, default);
            if (p[0] is 0x03 or 0x04)
            {
                return v;
            }
        }
        catch
        { /* keep looking */
        }
    }

    return null;
}

static bool IsValidPageNumber(long pageNumber, long pageCount) => pageNumber > 0 && pageNumber < pageCount;

static async Task EmitTDefAsync(
    AccessReader reader,
    StringBuilder sb,
    string name,
    long page,
    bool includeIndexAnnotations = false,
    bool includeComplexAnnotations = false,
    byte[]? preloadedBytes = null)
{
    byte[]? bytes = preloadedBytes ?? await reader.GetRawTDefBytesAsync(page, default);
    if (bytes is null)
    {
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"## `{Md(name)}` — TDEF page {page}");
        _ = sb.AppendLine();
        _ = sb.AppendLine("> ⚠️ TDEF read failed (page is not type 0x02).");
        _ = sb.AppendLine();
        return;
    }

    bool jet4 = reader.DatabaseFormat != DatabaseFormat.Jet3Mdb;
    int blockEnd = jet4 ? 63 : 43;
    int realIdxEntrySz = jet4 ? 12 : 8;
    int colDescSz = jet4 ? 25 : 18;
    int numColsOff = jet4 ? 45 : 25;
    int numIdxOff = jet4 ? 47 : 27;
    int numRealIdxOff = jet4 ? 51 : 31;

    int numCols = U16(bytes, numColsOff);
    int numIdx = (int)U32(bytes, numIdxOff);
    int numRealIdx = (int)U32(bytes, numRealIdxOff);

    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"## `{Md(name)}` — TDEF page {page}");
    _ = sb.AppendLine();
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- TDEF total bytes (after page-chain stitch): **{bytes.Length}**");
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- `num_cols` (offset {numColsOff}, 2 bytes): **{numCols}**");
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- `num_idx` (offset {numIdxOff}, 4 bytes): **{numIdx}**");
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- `num_real_idx` (offset {numRealIdxOff}, 4 bytes): **{numRealIdx}**");
    if (jet4)
    {
        long ctAutonum = U32(bytes, 28);
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- `ct_autonum` (Jet4 only, offset 28, 4 bytes): **{ctAutonum}** — next ConceptualTableID for this table's complex columns");
    }

    _ = sb.AppendLine();

    int colStart = blockEnd + (numRealIdx * realIdxEntrySz);
    int namesStart = colStart + (numCols * colDescSz);
    int realIdxDescStart = -1;
    int logicalIdxStart = -1;
    int logicalIdxNamesStart = -1;

    // Walk column names to find where they end. Then real-idx physical descriptors begin.
    int pos = namesStart;
    var colNames = new List<string>(numCols);
    for (int i = 0; i < numCols; i++)
    {
        if (jet4)
        {
            if (pos + 2 > bytes.Length)
            {
                break;
            }

            int len = U16(bytes, pos);
            pos += 2;

            if (pos + len > bytes.Length)
            {
                break;
            }

            colNames.Add(Encoding.Unicode.GetString(bytes, pos, len));
            pos += len;
        }
        else
        {
            if (pos >= bytes.Length)
            {
                break;
            }

            int len = bytes[pos++];
            if (pos + len > bytes.Length)
            {
                break;
            }

            colNames.Add(Encoding.GetEncoding(reader.CodePage).GetString(bytes, pos, len));
            pos += len;
        }
    }

    int afterColNames = pos;

    // Real-idx physical descriptor block: numRealIdx × 52 (Jet4) / 39 (Jet3)
    if (numRealIdx > 0)
    {
        realIdxDescStart = afterColNames;
        int physSz = jet4 ? 52 : 39;
        logicalIdxStart = realIdxDescStart + (numRealIdx * physSz);
    }
    else
    {
        logicalIdxStart = afterColNames;
    }

    // Logical-idx entries: numIdx × 28 (Jet4) / 20 (Jet3)
    int logicalEntrySz = jet4 ? 28 : 20;
    int afterLogicalIdx = logicalIdxStart + (numIdx * logicalEntrySz);
    if (numIdx > 0)
    {
        logicalIdxNamesStart = afterLogicalIdx;
    }

    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"### Section offsets (computed)");
    _ = sb.AppendLine();
    _ = sb.AppendLine("| Section | Start | Size | End (excl) |");
    _ = sb.AppendLine("|---|---:|---:|---:|");
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| Page header | 0 | 8 | 8 |");
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| TDEF block ({(jet4 ? "Jet4: 55" : "Jet3: 35")} bytes) | 8 | {blockEnd - 8} | {blockEnd} |");
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| Real-idx skip entries (numRealIdx × {realIdxEntrySz}) | {blockEnd} | {numRealIdx * realIdxEntrySz} | {colStart} |");
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| Column descriptors (numCols × {colDescSz}) | {colStart} | {numCols * colDescSz} | {namesStart} |");
    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| Column names (length-prefixed) | {namesStart} | {afterColNames - namesStart} | {afterColNames} |");
    if (numRealIdx > 0)
    {
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| Real-idx physical descriptors (numRealIdx × {(jet4 ? 52 : 39)}) | {realIdxDescStart} | {numRealIdx * (jet4 ? 52 : 39)} | {logicalIdxStart} |");
    }

    if (numIdx > 0)
    {
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| Logical-idx entries (numIdx × {logicalEntrySz}) | {logicalIdxStart} | {numIdx * logicalEntrySz} | {afterLogicalIdx} |");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| Logical-idx names | {logicalIdxNamesStart} | (variable) | (computed by walk) |");
    }

    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| Variable trailing block (per-var-col page ptrs) | (after names) | (variable, terminated by col_num=0xFFFF) | — |");
    _ = sb.AppendLine();

    _ = sb.AppendLine("### Column descriptors");
    _ = sb.AppendLine();
    _ = sb.AppendLine("| # | Name | Type (hex) | Type meaning | col_num | offset_F | col_len | flags (hex) | misc (hex, 4 bytes) |");
    _ = sb.AppendLine("|---:|---|---|---|---:|---:|---:|---|---|");
    for (int i = 0; i < numCols && i < colNames.Count; i++)
    {
        int o = colStart + (i * colDescSz);
        if (o + colDescSz > bytes.Length)
        {
            break;
        }

        byte type = bytes[o + 0];
        int colNum = jet4 ? U16(bytes, o + 5) : U16(bytes, o + 1);
        int offF = jet4 ? U16(bytes, o + 21) : U16(bytes, o + 14);
        int colLen = jet4 ? U16(bytes, o + 23) : U16(bytes, o + 16);
        byte flags = jet4 ? bytes[o + 15] : bytes[o + 13];
        string misc4 = jet4
            ? Hex(bytes, o + 11, 4) // misc(2) + misc_ext(2) — for complex cols this is the 4-byte ComplexID
            : Hex(bytes, o + 9, 4);
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| {i} | `{Md(colNames[i])}` | 0x{type:X2} | {TypeName(type)} | {colNum} | {offF} | {colLen} | 0x{flags:X2} | `{misc4}` |");
    }

    _ = sb.AppendLine();

    if (includeComplexAnnotations)
    {
        var complexCols = new List<(int Idx, string Name, byte Type, string ComplexId)>();
        for (int i = 0; i < numCols && i < colNames.Count; i++)
        {
            int o = colStart + (i * colDescSz);
            if (o + colDescSz > bytes.Length)
            {
                break;
            }

            byte type = bytes[o + 0];
            if (type == 0x11 || type == 0x12)
            {
                string id = jet4 ? Hex(bytes, o + 11, 4) : Hex(bytes, o + 9, 4);
                complexCols.Add((i, colNames[i], type, id));
            }
        }

        if (complexCols.Count > 0)
        {
            _ = sb.AppendLine("**Complex columns detected:**");
            _ = sb.AppendLine();
            foreach (var (i, n, t, id) in complexCols)
                _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Column #{i} `{Md(n)}` — type 0x{t:X2} ({TypeName(t)}) — ComplexID bytes (LE u32 in misc): `{id}` = **{U32FromHex(id)}**");
            _ = sb.AppendLine();
        }
    }

    if (includeIndexAnnotations && numRealIdx > 0)
    {
        _ = sb.AppendLine("### Real-index physical descriptors (raw bytes per entry)");
        _ = sb.AppendLine();
        int physSz = jet4 ? 52 : 39;
        for (int i = 0; i < numRealIdx; i++)
        {
            int start = realIdxDescStart + (i * physSz);
            if (start + physSz > bytes.Length)
            {
                break;
            }

            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"#### Real-idx #{i} (offset {start}, {physSz} bytes)");
            _ = sb.AppendLine();
            _ = sb.AppendLine("```");
            _ = sb.Append(HexDump(bytes, start, physSz, baseOffset: start));
            _ = sb.AppendLine("```");

            // Best-effort field decode per index-and-relationship-format-notes.md §3.1.
            if (jet4)
            {
                _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- bytes  0..3  unknown(4): `{Hex(bytes, start + 0, 4)}`");
                _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- bytes  4..33 col_map (10 × {{col_num(2),col_order(1)}}):");
                for (int slot = 0; slot < 10; slot++)
                {
                    int so = start + 4 + (slot * 3);
                    int cn = U16(bytes, so);
                    byte order = bytes[so + 2];
                    if (cn != 0xFFFF)
                    {
                        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"    - slot {slot}: col_num={cn}, col_order=0x{order:X2}");
                    }
                }

                _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- bytes 34..37 used_pages: {U32(bytes, start + 34)}");
                _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- bytes 38..41 first_dp:   {U32(bytes, start + 38)}");
                _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- byte  42     flags:       0x{bytes[start + 42]:X2}");
                _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- bytes 43..51 unknown(9): `{Hex(bytes, start + 43, 9)}`");
            }
            else
            {
                // Jet3 real-idx physical descriptor (39 bytes per mdbtools HACKING.md).
                // Field offsets are an empirical proposal cross-referenced with the
                // Access 97 fixtures from the Jackcess V1997 corpus. Confirm in the
                // hex dump above: most plausible mapping is the col_map at offset 4
                // (10 × 3 bytes), used_pages at offset 34, and a flags-or-first_dp tail
                // running from offset 38 to 38 inclusive, leaving 4 bytes unaccounted
                // for. The exact layout of those last 5 bytes is what format probe is trying
                // to pin down.
                _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- bytes  0..3  unknown(4):  `{Hex(bytes, start + 0, 4)}`");
                _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- bytes  4..33 col_map (10 × {{col_num(2),col_order(1)}}):");
                for (int slot = 0; slot < 10; slot++)
                {
                    int so = start + 4 + (slot * 3);
                    if (so + 3 > start + physSz)
                    {
                        break;
                    }

                    int cn = U16(bytes, so);
                    byte order = bytes[so + 2];
                    if (cn != 0xFFFF)
                    {
                        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"    - slot {slot}: col_num={cn}, col_order=0x{order:X2}");
                    }
                }

                _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- bytes 34..37 used_pages: {U32(bytes, start + 34)}");
                _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- byte  38     trailing0:  0x{bytes[start + 38]:X2}");

                // The 8-byte LEADING real-idx skip entry that lives at _tdBlockEnd
                // (43 .. 43 + numRealIdx*8) on Jet3 carries `first_dp` for the matching
                // physical-descriptor index `i`. mdbtools labels it unknown(?) +
                // num_idx_rows(4) + unknown(?). The dump emitted earlier in the
                // "Real-idx skip entries" row above is the source for those 8 bytes.
            }

            _ = sb.AppendLine();
        }

        if (!jet4 && numRealIdx > 0)
        {
            // Jet3 leading real-idx "skip" entry block: 8 bytes per real-idx, sitting
            // between the TDEF block (ends at 43) and the column descriptors (start
            // at 43 + numRealIdx*8). mdbtools labels these per-entry fields
            // unknown(?) + num_idx_rows(?) + unknown(?). The probe surfaces the raw
            // bytes so we can confirm which 4-byte slice carries the `first_dp` page
            // pointer for the matching physical-descriptor entry.
            _ = sb.AppendLine("### Jet3 leading real-idx skip entries (8 bytes each)");
            _ = sb.AppendLine();
            for (int i = 0; i < numRealIdx; i++)
            {
                int start = blockEnd + (i * realIdxEntrySz);
                if (start + 8 > bytes.Length)
                {
                    break;
                }

                _ = sb.AppendLine(CultureInfo.InvariantCulture, $"#### Skip entry #{i} (offset {start}, 8 bytes)");
                _ = sb.AppendLine();
                _ = sb.AppendLine("```");
                _ = sb.Append(HexDump(bytes, start, 8, baseOffset: start));
                _ = sb.AppendLine("```");
                _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- bytes 0..3 (LE u32): {U32(bytes, start + 0)}  (mdbtools: `unknown(4)`)");
                _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- bytes 4..7 (LE u32): {U32(bytes, start + 4)}  (mdbtools: `num_idx_rows(4)` per Jet4 — unverified for Jet3)");
            }

            _ = sb.AppendLine();
        }

        if (numIdx > 0)
        {
            _ = sb.AppendLine("### Logical-index entries (raw bytes per entry)");
            _ = sb.AppendLine();
            for (int i = 0; i < numIdx; i++)
            {
                int start = logicalIdxStart + (i * logicalEntrySz);
                if (start + logicalEntrySz > bytes.Length)
                {
                    break;
                }

                _ = sb.AppendLine(CultureInfo.InvariantCulture, $"#### Logical-idx #{i} (offset {start}, {logicalEntrySz} bytes)");
                _ = sb.AppendLine();
                _ = sb.AppendLine("```");
                _ = sb.Append(HexDump(bytes, start, logicalEntrySz, baseOffset: start));
                _ = sb.AppendLine("```");
                if (jet4)
                {
                    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- bytes  0..3  unknown(4):    `{Hex(bytes, start + 0, 4)}`");
                    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- bytes  4..7  index_num:     {U32(bytes, start + 4)}");
                    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- bytes  8..11 index_num2:    {U32(bytes, start + 8)}");
                    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- byte  12     rel_tbl_type:  0x{bytes[start + 12]:X2}");
                    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- bytes 13..16 rel_idx_num:   {unchecked((int)U32(bytes, start + 13))} (0xFFFFFFFF = not a FK)");
                    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- bytes 17..20 rel_tbl_page:  {U32(bytes, start + 17)}");
                    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- byte  21     cascade_ups:   0x{bytes[start + 21]:X2}");
                    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- byte  22     cascade_dels:  0x{bytes[start + 22]:X2}");
                    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- byte  23     index_type:    0x{bytes[start + 23]:X2}  (0x01=PK, 0x02=FK)");
                    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- bytes 24..27 trailing(4):   `{Hex(bytes, start + 24, 4)}`");
                }
                else
                {
                    // Jet3 logical-idx entry (20 bytes per mdbtools). Most fields
                    // are present at the same conceptual offsets as Jet4, but the
                    // 4-byte `unknown` cookie at the head and the 4-byte `trailing`
                    // tail of the Jet4 layout are removed (28 − 8 = 20). The most
                    // likely Jet3 mapping (see format probe notes) is:
                    //
                    //   bytes 0..3  index_num(4)
                    //   bytes 4..7  index_num2(4)
                    //   byte  8     rel_tbl_type(1)
                    //   bytes 9..12 rel_idx_num(4)
                    //   bytes 13..16 rel_tbl_page(4)
                    //   byte 17     cascade_ups(1)
                    //   byte 18     cascade_dels(1)
                    //   byte 19     index_type(1)   — 0x01 PK, 0x02 FK
                    //
                    // This matches what AccessReader.ParseIndexMetadata reads today
                    // for the Jet3 best-effort path. Verify in the hex dump above:
                    // index_type at byte 19 should be 0x01 for the PK entry.
                    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- bytes  0..3  index_num:     {U32(bytes, start + 0)}");
                    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- bytes  4..7  index_num2:    {U32(bytes, start + 4)}");
                    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- byte   8     rel_tbl_type:  0x{bytes[start + 8]:X2}");
                    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- bytes  9..12 rel_idx_num:   {unchecked((int)U32(bytes, start + 9))} (0xFFFFFFFF = not a FK)");
                    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- bytes 13..16 rel_tbl_page:  {U32(bytes, start + 13)}");
                    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- byte  17     cascade_ups:   0x{bytes[start + 17]:X2}");
                    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- byte  18     cascade_dels:  0x{bytes[start + 18]:X2}");
                    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- byte  19     index_type:    0x{bytes[start + 19]:X2}  (0x01=PK, 0x02=FK)");
                }

                _ = sb.AppendLine();
            }

            // Logical-idx names
            _ = sb.AppendLine("### Logical-index names (raw)");
            _ = sb.AppendLine();
            int npos = logicalIdxNamesStart;
            for (int i = 0; i < numIdx; i++)
            {
                if (jet4)
                {
                    if (npos + 2 > bytes.Length)
                    {
                        break;
                    }

                    int len = U16(bytes, npos);
                    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- #{i} (offset {npos}): len={len} → `{Encoding.Unicode.GetString(bytes, npos + 2, Math.Min(len, bytes.Length - npos - 2))}`");
                    npos += 2 + len;
                }
                else
                {
                    if (npos >= bytes.Length)
                    {
                        break;
                    }

                    int len = bytes[npos];
                    _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- #{i} (offset {npos}): len={len} → `{Encoding.GetEncoding(reader.CodePage).GetString(bytes, npos + 1, Math.Min(len, bytes.Length - npos - 1))}`");
                    npos += 1 + len;
                }
            }

            _ = sb.AppendLine();
        }
    }

    _ = sb.AppendLine("### Full TDEF hex dump");
    _ = sb.AppendLine();
    _ = sb.AppendLine("```");
    _ = sb.Append(HexDump(bytes, 0, bytes.Length, baseOffset: 0));
    _ = sb.AppendLine("```");
    _ = sb.AppendLine();
}

static string HexDump(byte[] bytes, int offset, int len, int baseOffset)
{
    var sb = new StringBuilder();
    int end = offset + len;
    for (int i = offset; i < end; i += 16)
    {
        _ = sb.Append(CultureInfo.InvariantCulture, $"{baseOffset + (i - offset),6:X4}  ");
        for (int j = 0; j < 16; j++)
        {
            int idx = i + j;
            if (idx < end)
            {
                _ = sb.Append(CultureInfo.InvariantCulture, $"{bytes[idx]:X2} ");
            }
            else
            {
                _ = sb.Append("   ");
            }

            if (j == 7)
            {
                _ = sb.Append(' ');
            }
        }

        _ = sb.Append(' ');
        for (int j = 0; j < 16; j++)
        {
            int idx = i + j;
            if (idx < end)
            {
                byte b = bytes[idx];
                _ = sb.Append(b is >= 0x20 and < 0x7F ? (char)b : '.');
            }
        }

        _ = sb.AppendLine();
    }

    return sb.ToString();
}

static string Hex(byte[] b, int o, int len)
{
    var sb = new StringBuilder(len * 3);
    for (int i = 0; i < len && o + i < b.Length; i++)
    {
        _ = sb.Append(CultureInfo.InvariantCulture, $"{b[o + i]:X2} ");
    }

    return sb.ToString().TrimEnd();
}

static uint U32FromHex(string spaced)
{
    string compact = spaced.Replace(" ", string.Empty, StringComparison.Ordinal);
    if (compact.Length < 8)
    {
        return 0;
    }

    byte b0 = Convert.ToByte(compact.Substring(0, 2), 16);
    byte b1 = Convert.ToByte(compact.Substring(2, 2), 16);
    byte b2 = Convert.ToByte(compact.Substring(4, 2), 16);
    byte b3 = Convert.ToByte(compact.Substring(6, 2), 16);
    return (uint)(b0 | (b1 << 8) | (b2 << 16) | (b3 << 24));
}

static int U16(byte[] b, int o) => b[o] | (b[o + 1] << 8);
static long U32(byte[] b, int o) => unchecked((uint)(b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24)));
static string Md(string s) => s.Replace("|", "\\|", StringComparison.Ordinal).Replace("`", "\\`", StringComparison.Ordinal);

static string TypeName(byte t) => t switch
{
    0x01 => "BOOL", 0x02 => "BYTE", 0x03 => "INT", 0x04 => "LONG",
    0x05 => "MONEY", 0x06 => "FLOAT", 0x07 => "DOUBLE", 0x08 => "DATETIME",
    0x09 => "BINARY", 0x0A => "TEXT", 0x0B => "OLE", 0x0C => "MEMO",
    0x0F => "GUID", 0x10 => "NUMERIC", 0x11 => "ATTACHMENT (complex)",
    0x12 => "COMPLEX (multi-value/version)", _ => "?",
};

internal partial class Program
{
    private sealed record CatalogScanResult(
        string RelPath,
        string Format,
        int CatalogRows,
        bool? HasIndexes,
        bool? HasIndexColumns,
        bool? HasRelationships,
        string? AnyIndexNamed,
        string? Error,
        List<(long Id, string Name, int Type, long Flags, long TdefPage)>? IndexCatalog,
        IReadOnlyList<string> MsysNames,
        IReadOnlyList<string> IndexNames);
}
