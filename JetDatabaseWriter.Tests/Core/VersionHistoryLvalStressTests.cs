namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// DAO-authored stress test for the version-history complex column LVAL chain.
/// Builds a fixture with &gt; 100 historical Memo versions — several long
/// enough to spill into multi-page LVAL chains — then validates that our
/// reader can enumerate every version row from the per-row complex sub-table
/// (the <c>f_*_VersionHistory_*</c> flat table) and recover the exact
/// historical Memo content. Closes §2.2: "Versioned-text column with &gt; 100
/// historical versions to exercise the LVAL chain inside the per-row complex
/// sub-table.".
/// </summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class VersionHistoryLvalStressTests : IDisposable
{
    private const int VersionCount = 110;
    private const int LongVersionLength = 5000;
    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(2);

    // Versions whose payload should be long enough to spill into an LVAL chain
    // (Memo inline limit is well below 5000 chars). The remaining versions stay
    // short so the flat table mixes inline + chained values.
    private static readonly HashSet<int> LongVersionIndices = [25, 50, 75, 100, 109];

    private readonly string _workDir;

    public VersionHistoryLvalStressTests()
    {
        _workDir = Path.Combine(
            Path.GetTempPath(),
            "JetDatabaseWriter.Tests.VhLval",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_workDir);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_workDir))
            {
                Directory.Delete(_workDir, recursive: true);
            }
        }
        catch (IOException)
        {
            // Best-effort cleanup.
        }
        catch (UnauthorizedAccessException)
        {
            // Best-effort cleanup.
        }
    }

    /// <summary>
    /// Gets a value indicating whether DAO (Microsoft Access) is available.
    /// Tests using <c>SkipUnless</c> on this property will report as skipped
    /// rather than failing on hosts without Access installed.
    /// </summary>
    public static bool RoundTripAvailable => AccessRoundTripEnvironment.IsAvailable;

    /// <summary>
    /// Authors an Access database via DAO containing a single AppendOnly Memo
    /// column with <see cref="VersionCount"/> historical versions, then opens
    /// the resulting file with our reader and asserts every version row is
    /// readable from the version-history flat child table — including the
    /// long versions whose Memo payloads are stored as multi-page LVAL chains
    /// inside the per-row complex sub-table.
    /// </summary>
    [Fact(Skip = "Requires Microsoft Access (DAO.DBEngine.120)", SkipUnless = nameof(RoundTripAvailable))]
    public async Task LongVersionedTextColumn_AllVersionsReadableFromFlatTable()
    {
        string dbPath = Path.Combine(_workDir, "vh_lval_stress.accdb");
        const string TableName = "VhLval";
        const string MemoColumn = "Notes";

        // Each version's text is a deterministic, easy-to-verify string of
        // a known length. Long versions repeat a single character so we can
        // recompute them in-process for byte-exact comparison.
        string[] versions = BuildVersionPayloads();

        string script = BuildAuthoringScript(dbPath, TableName, MemoColumn, versions);
        var result = AccessRoundTripEnvironment.RunDaoScript(script, _workDir, DaoTimeout);
        Assert.True(
            result.ExitCode == 0,
            $"DAO authoring script failed (exit={result.ExitCode}).\nstdout: {result.StdOut}\nstderr: {result.StdErr}");

        await using var reader = await AccessReader.OpenAsync(
            dbPath,
            new AccessReaderOptions { UseLockFile = false },
            TestContext.Current.CancellationToken);

        IReadOnlyList<ComplexColumnInfo> complex = await reader.GetComplexColumnsAsync(
            TableName,
            TestContext.Current.CancellationToken);

        ComplexColumnInfo vhCol = Assert.Single(
            complex,
            c => c.Kind == Enums.ComplexColumnKind.VersionHistory);

        Assert.False(string.IsNullOrWhiteSpace(vhCol.FlatTableName));

        // Locate the Memo column inside the flat child table.
        List<ColumnMetadata> flatCols = await reader.GetColumnMetadataAsync(
            vhCol.FlatTableName,
            TestContext.Current.CancellationToken);

        int memoOrdinal = flatCols.FindIndex(
            c => string.Equals(c.TypeName, "Memo", StringComparison.OrdinalIgnoreCase));
        Assert.True(memoOrdinal >= 0, "Version-history flat table must expose a Memo column for the historical text.");

        var observedValues = new HashSet<string>(VersionCount, StringComparer.Ordinal);
        int rowCount = 0;

        await foreach (object[] row in reader.Rows(
            vhCol.FlatTableName,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            string text = Assert.IsType<string>(row[memoOrdinal]);
            observedValues.Add(text);
            rowCount++;
        }

        // Access seeds an empty initial version when AppendOnly is enabled,
        // so the flat table holds at least VersionCount rows (and possibly
        // VersionCount + 1 once the seed is counted).
        Assert.True(
            rowCount >= VersionCount,
            $"Expected at least {VersionCount} version rows in '{vhCol.FlatTableName}', observed {rowCount}.");

        // Every authored version must appear in the flat table. Long versions
        // prove the LVAL chain was walked in full and not truncated.
        for (int i = 0; i < versions.Length; i++)
        {
            Assert.Contains(versions[i], observedValues);
        }
    }

    private static string[] BuildVersionPayloads()
    {
        var versions = new string[VersionCount];
        for (int i = 0; i < VersionCount; i++)
        {
            if (LongVersionIndices.Contains(i))
            {
                char filler = (char)('A' + (i % 26));
                versions[i] = new string(filler, LongVersionLength);
            }
            else
            {
                versions[i] = $"version-{i:D4}";
            }
        }

        return versions;
    }

    private static string BuildAuthoringScript(
        string dbPath,
        string tableName,
        string memoColumn,
        string[] versions)
    {
        string dbLiteral = dbPath.Replace("'", "''", StringComparison.Ordinal);

        // Stage all version payloads to a sidecar text file (one line per
        // version, with newlines escaped as \n) so we don't blow past the
        // PowerShell command-line / script size limits when embedding 5000-
        // char strings inline.
        string payloadFile = Path.Combine(GetSidecarDir(dbPath), "vh-payloads.txt");
        File.WriteAllLines(payloadFile, versions.Select(EscapeForLine));
        string payloadLiteral = payloadFile.Replace("'", "''", StringComparison.Ordinal);

        return $$"""
            $ErrorActionPreference = 'Stop'
            $payloads = Get-Content -LiteralPath '{{payloadLiteral}}' -Encoding UTF8 | ForEach-Object { $_ -replace '\\n', "`n" }
            $engine = New-Object -ComObject DAO.DBEngine.120
            try {
              $db = $engine.CreateDatabase('{{dbLiteral}}', ';LANGID=0x0409;CP=1252;COUNTRY=0')
              try {
                $db.Execute('CREATE TABLE [{{tableName}}] (Id AUTOINCREMENT PRIMARY KEY, [{{memoColumn}}] MEMO)')
                # Enable AppendOnly on the Memo column so each update produces a
                # version-history row in the flat child table.
                $tdf = $db.TableDefs('{{tableName}}')
                $fld = $tdf.Fields('{{memoColumn}}')
                $fld.AppendOnly = $true
                $tdf = $null
                $fld = $null
                [System.Runtime.InteropServices.Marshal]::ReleaseComObject($db) | Out-Null
                [GC]::Collect(); [GC]::WaitForPendingFinalizers()
                $db = $engine.OpenDatabase('{{dbLiteral}}')
                $rs = $db.OpenRecordset('{{tableName}}', 2)
                $rs.AddNew()
                $rs.Fields('{{memoColumn}}').Value = $payloads[0]
                $rs.Update()
                $rs.MoveLast()
                for ($i = 1; $i -lt $payloads.Count; $i++) {
                  $rs.Edit()
                  $rs.Fields('{{memoColumn}}').Value = $payloads[$i]
                  $rs.Update()
                }
                $rs.Close()
                $db.Close()
                Write-Output "WROTE=$($payloads.Count)"
              } finally {
                if ($db -ne $null) { try { $db.Close() } catch {} }
              }
            } finally {
              [System.Runtime.InteropServices.Marshal]::ReleaseComObject($engine) | Out-Null
              [GC]::Collect(); [GC]::WaitForPendingFinalizers()
            }
            """;
    }

    private static string EscapeForLine(string value) =>
        value.Replace("\\", "\\\\", StringComparison.Ordinal)
             .Replace("\n", "\\n", StringComparison.Ordinal)
             .Replace("\r", string.Empty, StringComparison.Ordinal);

    private static string GetSidecarDir(string dbPath) =>
        Path.GetDirectoryName(dbPath) ?? Path.GetTempPath();
}
