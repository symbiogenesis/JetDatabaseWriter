namespace JetDatabaseWriter.FormatProbe;

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;

#pragma warning disable CA1303 // Diagnostic console output is not localized.

internal static class FkDaoBaselineProbe
{
    private const string Parent = "DaoFkParent";
    private const string Child = "DaoFkChild";
    private const string FkName = "DaoFK_Child_Parent";

    public static async Task<int> RunAsync(string baselinePath, string workRoot)
    {
        if (!File.Exists(baselinePath))
        {
            await Console.Error.WriteLineAsync($"[fk-dao-baseline] baseline not found: {baselinePath}");
            return 1;
        }

        DaoPowerShellHostResolver.DaoPowerShellHostProbeResult hostProbe = DaoPowerShellHostResolver.Probe();
        if (hostProbe.HostPath is null)
        {
            await Console.Error.WriteLineAsync($"[fk-dao-baseline] {hostProbe.FailureReason}");
            return 1;
        }

        workRoot = PrepareWorkRoot(workRoot);
        _ = Directory.CreateDirectory(workRoot);
        string writerPath = Path.Combine(workRoot, "writer.accdb");
        string daoPath = Path.Combine(workRoot, "dao.accdb");
        string writerCompactPath = Path.Combine(workRoot, "writer-compact.accdb");
        string daoCompactPath = Path.Combine(workRoot, "dao-compact.accdb");
        File.Copy(baselinePath, writerPath, overwrite: true);
        File.Copy(baselinePath, daoPath, overwrite: true);

        await AuthorWriterAsync(writerPath);
        (int daoCode, string daoOut, string daoErr) = RunPowerShell(hostProbe.HostPath, workRoot, BuildDaoAuthoringScript(daoPath));
        Console.WriteLine(FormattableString.Invariant($"[fk-dao-baseline] DAO authoring exit={daoCode}"));
        if (daoCode != 0)
        {
            Console.WriteLine(daoOut);
            await Console.Error.WriteLineAsync(daoErr);
            return daoCode;
        }

        bool dumpSnapshots = ShouldDumpSnapshots();
        if (dumpSnapshots)
        {
            await DumpDatabaseAsync("writer", writerPath);
            await DumpDatabaseAsync("dao", daoPath);
        }
        else
        {
            Console.WriteLine("[fk-dao-baseline] detailed table dumps skipped by DIAG_FK_SUMMARY_ONLY/DIAG_FK_SKIP_DUMPS");
        }

        PostAuthoringProbeResults postAuthoringResults = RunPostAuthoringProbes(
            hostProbe.HostPath,
            workRoot,
            writerPath,
            writerCompactPath,
            daoPath,
            daoCompactPath);
        (int writerCompact, string writerCompactOut, string writerCompactErr) = postAuthoringResults.WriterCompact;
        (int daoCompact, string daoCompactOut, string daoCompactErr) = postAuthoringResults.DaoCompact;
        Console.WriteLine(FormattableString.Invariant($"writer compact: exit={writerCompact}, stdout={writerCompactOut.Trim()}, stderr={writerCompactErr.Trim()}"));
        Console.WriteLine(FormattableString.Invariant($"dao compact:    exit={daoCompact}, stdout={daoCompactOut.Trim()}, stderr={daoCompactErr.Trim()}"));
        if (dumpSnapshots && writerCompact == 0 && File.Exists(writerCompactPath))
        {
            await DumpDatabaseAsync("writer compact", writerCompactPath);
        }

        if (dumpSnapshots && daoCompact == 0 && File.Exists(daoCompactPath))
        {
            await DumpDatabaseAsync("dao compact", daoCompactPath);
        }

        (int writerEnforce, string writerOut, string writerErr) = postAuthoringResults.WriterOrphanInsert;
        (int daoEnforce, string daoOut2, string daoErr2) = postAuthoringResults.DaoOrphanInsert;
        Console.WriteLine(FormattableString.Invariant($"writer orphan insert probe: exit={writerEnforce}, stdout={writerOut.Trim()}, stderr={writerErr.Trim()}"));
        Console.WriteLine(FormattableString.Invariant($"dao orphan insert probe:    exit={daoEnforce}, stdout={daoOut2.Trim()}, stderr={daoErr2.Trim()}"));
        Console.WriteLine(FormattableString.Invariant($"[fk-dao-baseline] files: {workRoot}"));
        return 0;
    }

    private static bool ShouldDumpSnapshots() =>
        string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("DIAG_FK_SUMMARY_ONLY"))
        && string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("DIAG_FK_SKIP_DUMPS"));

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

    private static async Task AuthorWriterAsync(string path)
    {
        await using var writer = await AccessWriter.OpenAsync(path, new AccessWriterOptions { UseLockFile = false });
        await writer.CreateTableAsync(
            Parent,
            [
                new("ParentId", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                new("Label", typeof(string), maxLength: 50) { IsNullable = false },
            ]);

        await writer.CreateTableAsync(
            Child,
            [
                new("ChildId", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                new("ParentId", typeof(int)) { IsNullable = false },
                new("Detail", typeof(string), maxLength: 50),
            ]);

        await writer.CreateRelationshipAsync(
            new RelationshipDefinition(
                FkName,
                primaryTable: Parent,
                primaryColumn: "ParentId",
                foreignTable: Child,
                foreignColumn: "ParentId")
            {
                EnforceReferentialIntegrity = true,
            });

        await writer.InsertRowAsync(Parent, [DBNull.Value, "ValidParent"]);
        await writer.InsertRowAsync(Child, [DBNull.Value, 1, "ValidChild"]);
    }

    private static async Task DumpDatabaseAsync(string label, string path)
    {
        Console.WriteLine();
        Console.WriteLine($"== {label} ==");
        await using var reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false });
        Dictionary<string, int> catalogIds = await DumpCatalogRowsAsync(reader);
        await DumpAcesAsync(reader, catalogIds);
        await DumpRelationshipsAsync(reader);
        await DumpTDefAsync(reader, Parent);
        await DumpTDefAsync(reader, Child);
    }

    private static async Task<Dictionary<string, int>> DumpCatalogRowsAsync(AccessReader reader)
    {
        var ids = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        DataTable? objects = await reader.ReadDataTableAsync("MSysObjects");
        if (objects is null)
        {
            Console.WriteLine("MSysObjects: <missing>");
            return ids;
        }

        Console.WriteLine("MSysObjects:");
        foreach (DataRow row in objects.Rows)
        {
            string name = row["Name"]?.ToString() ?? string.Empty;
            if (!string.Equals(name, Parent, StringComparison.Ordinal)
                && !string.Equals(name, Child, StringComparison.Ordinal))
            {
                continue;
            }

            int id = Convert.ToInt32(row["Id"], CultureInfo.InvariantCulture);
            ids[name] = id;

            object owner = objects.Columns.Contains("Owner") ? row["Owner"] : DBNull.Value;
            object lvProp = objects.Columns.Contains("LvProp") ? row["LvProp"] : DBNull.Value;
            Console.WriteLine(
                FormattableString.Invariant(
                    $"  name={name} id={row["Id"]} parent={row["ParentId"]} type={row["Type"]} flags={row["Flags"]} owner={BlobSummary(owner)} lvProp={BlobSummary(lvProp)}"));
        }

        return ids;
    }

    private static async Task DumpAcesAsync(AccessReader reader, Dictionary<string, int> catalogIds)
    {
        Console.WriteLine("MSysACEs:");
        if (catalogIds.Count == 0)
        {
            return;
        }

        DataTable? aces = await reader.ReadDataTableAsync("MSysACEs");
        if (aces is null || !aces.Columns.Contains("ObjectId"))
        {
            Console.WriteLine("  <missing>");
            return;
        }

        foreach ((string name, int id) in catalogIds.OrderBy(static kvp => kvp.Key, StringComparer.Ordinal))
        {
            foreach (DataRow row in aces.Rows)
            {
                int objectId = Convert.ToInt32(row["ObjectId"], CultureInfo.InvariantCulture);
                if (objectId != id)
                {
                    continue;
                }

                object acm = aces.Columns.Contains("ACM") ? row["ACM"] : DBNull.Value;
                object inheritable = aces.Columns.Contains("FInheritable") ? row["FInheritable"] : DBNull.Value;
                object sid = aces.Columns.Contains("SID") ? row["SID"] : DBNull.Value;
                Console.WriteLine(
                    FormattableString.Invariant(
                        $"  table={name} objectId={objectId} acm={acm} inheritable={inheritable} sid={BlobSummary(sid)}"));
            }
        }
    }

    private static async Task DumpRelationshipsAsync(AccessReader reader)
    {
        DataTable? rels = await reader.ReadDataTableAsync("MSysRelationships");
        if (rels is null)
        {
            Console.WriteLine("MSysRelationships: <missing>");
            return;
        }

        Console.WriteLine("MSysRelationships:");
        foreach (DataRow row in rels.Rows)
        {
            string name = row["szRelationship"]?.ToString() ?? string.Empty;
            if (!string.Equals(name, FkName, StringComparison.Ordinal))
            {
                continue;
            }

            string line =
                $"  ccolumn={row["ccolumn"]} grbit={row["grbit"]} icolumn={row["icolumn"]} " +
                $"szColumn={row["szColumn"]} szObject={row["szObject"]} " +
                $"szReferencedColumn={row["szReferencedColumn"]} szReferencedObject={row["szReferencedObject"]}";
            Console.WriteLine(line);
        }
    }

    private static async Task DumpTDefAsync(AccessReader reader, string tableName)
    {
        DataTable? rows = await reader.ReadDataTableAsync(tableName);
        Console.WriteLine(FormattableString.Invariant($"{tableName}: rows={rows?.Rows.Count ?? -1}"));

        CatalogEntry? entry = await reader.GetCatalogEntryAsync(tableName);
        if (entry is null)
        {
            Console.WriteLine($"{tableName}: <missing>");
            return;
        }

        byte[] page = await reader.ReadPageAsync(entry.TDefPage);
        int ownedRow = page[0x37];
        int ownedPage = ReadUInt24(page, 0x38);
        int freeRow = page[0x3B];
        int freePage = ReadUInt24(page, 0x3C);
        int numCols = BitConverter.ToUInt16(page, 0x2D);
        int numIdx = BitConverter.ToInt32(page, 0x2F);
        int numRealIdx = BitConverter.ToInt32(page, 0x33);
        int colStart = 63 + (numRealIdx * 12);
        int namePos = colStart + (numCols * 25);
        for (int i = 0; i < numCols; i++)
        {
            int nameLen = BitConverter.ToUInt16(page, namePos);
            namePos += 2 + nameLen;
        }

        int realStart = namePos;
        int logStart = realStart + (numRealIdx * 52);
        int logNamesStart = logStart + (numIdx * 28);
        List<string> names = ReadNames(page, logNamesStart, numIdx);

        Console.WriteLine(FormattableString.Invariant($"{tableName}: tdefPage={entry.TDefPage} numIdx={numIdx} numRealIdx={numRealIdx}"));
        for (int i = 0; i < numRealIdx; i++)
        {
            int phys = realStart + (i * 52);
            string line = FormattableString.Invariant(
                $"  real[{i}] cols={ReadColMap(page, phys)} used={BitConverter.ToInt32(page, phys + 34)} firstDp={BitConverter.ToInt32(page, phys + 38)} flags=0x{page[phys + 46]:X2}");
            int used = BitConverter.ToInt32(page, phys + 34);
            int usedRow = used & 0xFF;
            int usedPage = (used >> 8) & 0x00FFFFFF;
            Console.WriteLine(line);
            Console.WriteLine(FormattableString.Invariant($"  real[{i}] cols={ReadColMap(page, phys)} used={used} firstDp={BitConverter.ToInt32(page, phys + 38)} flags=0x{page[phys + 46]:X2} usedMap row={usedRow} page={usedPage} {await UsageMapRowSummaryAsync(reader, usedPage, usedRow)}"));
        }

        for (int i = 0; i < numIdx; i++)
        {
            int logical = logStart + (i * 28);
            string line =
                $"  log[{i}] name={names[i]} index_num={BitConverter.ToInt32(page, logical + 4)} index_num2={BitConverter.ToInt32(page, logical + 8)} " +
                $"rel_tbl_type=0x{page[logical + 12]:X2} rel_idx_num={BitConverter.ToInt32(page, logical + 13)} " +
                $"rel_tbl_page={BitConverter.ToInt32(page, logical + 17)} cascade=0x{page[logical + 21]:X2}/0x{page[logical + 22]:X2} type=0x{page[logical + 23]:X2}";
            Console.WriteLine(line);
        }
    }

    private static async Task<string> UsageMapRowSummaryAsync(AccessReader reader, int pageNumber, int rowIndex)
    {
        if (pageNumber <= 0)
        {
            return "<none>";
        }

        byte[] page = await reader.ReadPageAsync(pageNumber);
        const int RowsStart = 14;
        const int NumRowsOffset = 12;
        int rowCount = BitConverter.ToUInt16(page, NumRowsOffset);
        if (rowIndex < 0 || rowIndex >= rowCount)
        {
            return FormattableString.Invariant($"<row outside count={rowCount}>");
        }

        int rowStart = BitConverter.ToUInt16(page, RowsStart + (rowIndex * 2)) & 0x1FFF;
        if (rowStart <= 0 || rowStart + 69 > page.Length)
        {
            return FormattableString.Invariant($"<bad rowStart={rowStart} count={rowCount}>");
        }

        byte type = page[rowStart];
        int basePage = BitConverter.ToInt32(page, rowStart + 1);
        var setPages = new List<int>();
        for (int bit = 0; bit < 512; bit++)
        {
            if ((page[rowStart + 5 + (bit / 8)] & (1 << (bit % 8))) != 0)
            {
                setPages.Add(basePage + bit);
            }
        }

        string pages = setPages.Count == 0 ? "-" : string.Join(',', setPages.Take(12));
        if (setPages.Count > 12)
        {
            pages += ",...";
        }

        return FormattableString.Invariant($"type=0x{type:X2} base={basePage} bits={setPages.Count} pages={pages}");
    }

    private static int ReadUInt24(byte[] bytes, int offset) =>
        bytes[offset] | (bytes[offset + 1] << 8) | (bytes[offset + 2] << 16);

    private static List<string> ReadNames(byte[] page, int start, int count)
    {
        var names = new List<string>(count);
        int pos = start;
        for (int i = 0; i < count; i++)
        {
            int len = BitConverter.ToUInt16(page, pos);
            names.Add(Encoding.Unicode.GetString(page, pos + 2, len));
            pos += 2 + len;
        }

        return names;
    }

    private static string ReadColMap(byte[] page, int phys)
    {
        var parts = new List<string>();
        for (int slot = 0; slot < 10; slot++)
        {
            int offset = phys + 4 + (slot * 3);
            int col = BitConverter.ToUInt16(page, offset);
            if (col == 0xFFFF)
            {
                continue;
            }

            parts.Add(FormattableString.Invariant($"{col}:0x{page[offset + 2]:X2}"));
        }

        return string.Join(",", parts);
    }

    private static string BlobSummary(object value) => value switch
    {
        byte[] bytes => $"byte[{bytes.Length}] {ToHex(bytes)}",
        string text => $"string[{text.Length}] {text}",
        DBNull => "NULL",
        null => "NULL",
        _ => $"{value.GetType().Name}[{value.ToString()?.Length ?? -1}] {value}",
    };

    private static string ToHex(byte[] bytes)
    {
        int take = Math.Min(bytes.Length, 160);
        var sb = new StringBuilder((take * 2) + 3);
        for (int i = 0; i < take; i++)
        {
            _ = sb.Append(bytes[i].ToString("X2", CultureInfo.InvariantCulture));
        }

        if (take < bytes.Length)
        {
            _ = sb.Append("...");
        }

        return sb.ToString();
    }

    private static string BuildDaoAuthoringScript(string path)
    {
        string db = Quote(path);
        return $$"""
            $ErrorActionPreference='Stop'
            $e = New-Object -ComObject DAO.DBEngine.120
            try {
              $db = $e.OpenDatabase({{db}})
              try {
                $td = $db.CreateTableDef('{{Parent}}')
                $f = $td.CreateField('ParentId', 4)
                $f.Attributes = $f.Attributes -bor 16
                $f.Required = $true
                $td.Fields.Append($f)
                $f = $td.CreateField('Label', 10, 50)
                $f.Required = $true
                $td.Fields.Append($f)
                $idx = $td.CreateIndex('PrimaryKey')
                $idx.Primary = $true
                $idx.Unique = $true
                $idx.Required = $true
                $idx.Fields.Append($idx.CreateField('ParentId'))
                $td.Indexes.Append($idx)
                $db.TableDefs.Append($td)

                $td = $db.CreateTableDef('{{Child}}')
                $f = $td.CreateField('ChildId', 4)
                $f.Attributes = $f.Attributes -bor 16
                $f.Required = $true
                $td.Fields.Append($f)
                $f = $td.CreateField('ParentId', 4)
                $f.Required = $true
                $td.Fields.Append($f)
                $td.Fields.Append($td.CreateField('Detail', 10, 50))
                $idx = $td.CreateIndex('PrimaryKey')
                $idx.Primary = $true
                $idx.Unique = $true
                $idx.Required = $true
                $idx.Fields.Append($idx.CreateField('ChildId'))
                $td.Indexes.Append($idx)
                $db.TableDefs.Append($td)

                $rel = $db.CreateRelation('{{FkName}}', '{{Parent}}', '{{Child}}', 0)
                $field = $rel.CreateField('ParentId')
                $field.ForeignName = 'ParentId'
                $rel.Fields.Append($field)
                $db.Relations.Append($rel)

                $db.Execute("INSERT INTO [{{Parent}}] (Label) VALUES ('ValidParent')", 128)
                $db.Execute("INSERT INTO [{{Child}}] (ParentId, Detail) VALUES (1, 'ValidChild')", 128)
              } finally {
                $db.Close()
              }
            } finally {
              [System.Runtime.InteropServices.Marshal]::ReleaseComObject($e) | Out-Null
            }
            """;
    }

    private readonly record struct PowerShellStepResult(int Code, string StdOut, string StdErr);

    private sealed record PostAuthoringProbeResults(
        PowerShellStepResult WriterCompact,
        PowerShellStepResult DaoCompact,
        PowerShellStepResult WriterOrphanInsert,
        PowerShellStepResult DaoOrphanInsert);

    private static PostAuthoringProbeResults RunPostAuthoringProbes(
        string powerShellPath,
        string workRoot,
        string writerPath,
        string writerCompactPath,
        string daoPath,
        string daoCompactPath)
    {
        (int code, string stdout, string stderr) = RunPowerShell(
            powerShellPath,
            workRoot,
            BuildPostAuthoringProbeScript(writerPath, writerCompactPath, daoPath, daoCompactPath));
        Dictionary<string, PowerShellStepResult> results = ParsePowerShellStepResults(stdout);
        PowerShellStepResult fallback = code == 0
            ? new PowerShellStepResult(1, string.Empty, "PowerShell batch did not emit a result for this step.")
            : new PowerShellStepResult(code, stdout, stderr);

        return new PostAuthoringProbeResults(
            GetPowerShellStepResult(results, "writer-compact", fallback),
            GetPowerShellStepResult(results, "dao-compact", fallback),
            GetPowerShellStepResult(results, "writer-orphan", fallback),
            GetPowerShellStepResult(results, "dao-orphan", fallback));
    }

    private static string BuildPostAuthoringProbeScript(
        string writerPath,
        string writerCompactPath,
        string daoPath,
        string daoCompactPath)
    {
        return $$"""
            $ErrorActionPreference='Stop'
            $writerPath = {{Quote(writerPath)}}
            $writerCompactPath = {{Quote(writerCompactPath)}}
            $daoPath = {{Quote(daoPath)}}
            $daoCompactPath = {{Quote(daoCompactPath)}}

            function Write-StepResult([string]$name, [int]$code, [string]$stdout, [string]$stderr) {
                if ($null -eq $stdout) { $stdout = '' }
                if ($null -eq $stderr) { $stderr = '' }
                $stdout64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($stdout))
                $stderr64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($stderr))
                Write-Output "$name|$code|$stdout64|$stderr64"
            }

            function Get-ErrorCode($errorRecord) {
                $code = [int]$errorRecord.Exception.HResult
                $errorCode = $errorRecord.Exception.ErrorCode
                if ($code -eq 0 -and $null -ne $errorCode) { $code = [int]$errorCode }
                if ($code -eq 0) { $code = 1 }
                return $code
            }

            function Invoke-Step([string]$name, [scriptblock]$body) {
                try {
                    $output = & $body | Out-String
                    Write-StepResult $name 0 $output ''
                } catch {
                    Write-StepResult $name (Get-ErrorCode $_) '' $_.Exception.Message
                }
            }

            function Compact-Database([string]$sourcePath, [string]$destinationPath) {
                if (Test-Path -LiteralPath $destinationPath) { Remove-Item -LiteralPath $destinationPath -Force }
                $engine.CompactDatabase($sourcePath, $destinationPath)
                Write-Output 'OK'
            }

            function Probe-OrphanInsert([string]$path) {
                $db = $engine.OpenDatabase($path)
                try {
                    $errorCode = 0
                    try {
                        $db.Execute("INSERT INTO [{{Child}}] (ParentId, Detail) VALUES (99999, 'Orphan')", 128)
                    } catch {
                        $errorCode = $_.Exception.ErrorCode
                        if ($errorCode -eq 0 -and $_.Exception.HResult -ne 0) { $errorCode = $_.Exception.HResult }
                    }

                    $recordset = $db.OpenRecordset('SELECT COUNT(*) AS Cnt FROM [{{Child}}]', 4)
                    try {
                        Write-Output "$errorCode|$($recordset.Fields('Cnt').Value)"
                    } finally {
                        $recordset.Close()
                    }
                } finally {
                    $db.Close()
                }
            }

            $engine = New-Object -ComObject DAO.DBEngine.120
            try {
                Invoke-Step 'writer-compact' { Compact-Database $writerPath $writerCompactPath }
                Invoke-Step 'dao-compact' { Compact-Database $daoPath $daoCompactPath }
                Invoke-Step 'writer-orphan' { Probe-OrphanInsert $writerPath }
                Invoke-Step 'dao-orphan' { Probe-OrphanInsert $daoPath }
            } finally {
                [System.Runtime.InteropServices.Marshal]::ReleaseComObject($engine) | Out-Null
            }
            """;
    }

    private static Dictionary<string, PowerShellStepResult> ParsePowerShellStepResults(string stdout)
    {
        var results = new Dictionary<string, PowerShellStepResult>(StringComparer.OrdinalIgnoreCase);
        foreach (string line in stdout.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries))
        {
            string[] parts = line.Split('|', 4);
            if (parts.Length != 4 || !int.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out int code))
            {
                continue;
            }

            results[parts[0]] = new PowerShellStepResult(
                code,
                DecodeBase64(parts[2]),
                DecodeBase64(parts[3]));
        }

        return results;
    }

    private static PowerShellStepResult GetPowerShellStepResult(
        Dictionary<string, PowerShellStepResult> results,
        string name,
        PowerShellStepResult fallback) =>
        results.TryGetValue(name, out PowerShellStepResult result) ? result : fallback;

    private static string DecodeBase64(string value)
    {
        try
        {
            return Encoding.UTF8.GetString(Convert.FromBase64String(value));
        }
        catch (FormatException)
        {
            return value;
        }
    }

    private static (int Code, string StdOut, string StdErr) RunPowerShell(string powerShellPath, string workRoot, string script)
    {
        string scriptPath = Path.Combine(workRoot, $"fk-dao-{Guid.NewGuid():N}.ps1");
        File.WriteAllText(scriptPath, script);
        try
        {
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

            using Process process = Process.Start(psi)!;
            string stdout = process.StandardOutput.ReadToEnd();
            string stderr = process.StandardError.ReadToEnd();
            process.WaitForExit();
            return (process.ExitCode, stdout, stderr);
        }
        finally
        {
            File.Delete(scriptPath);
        }
    }

    private static string Quote(string value) =>
        "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";
}
