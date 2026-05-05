namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Data;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// DAO-driven validation tests that shell out to a bitness-matched
/// <c>powershell.exe</c> host to exercise writer output via the canonical
/// Access engine (<c>DAO.DBEngine.120</c>). Skipped automatically when
/// Microsoft Access is not installed.
/// </summary>
/// <remarks>
/// Closes §5 coverage gaps: DAO OpenRecordset row-count, DAO index traversal,
/// DAO AutoNumber continuation, and §2.1: DAO-authored Memo with embedded NULs.
/// </remarks>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoValidationTests : IDisposable
{
    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(1);
    private readonly string _workDir;

    public DaoValidationTests()
    {
        _workDir = Path.Combine(Path.GetTempPath(), "JetDatabaseWriter.Tests.DaoValidation", Guid.NewGuid().ToString("N"));
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
    /// After writing N rows with AccessWriter, opens the database via DAO and
    /// asserts SELECT COUNT(*) matches. Catches TDEF row-count drift and
    /// page-corruption that would silently survive our own reader round-trip.
    /// </summary>
    [Fact(
        Skip = "DAO OpenRecordset rejects writer-created tables ('Unrecognized database format') — TDEF page layout not yet fully compatible with DAO.DBEngine.120. See docs/design/round-trip-test-failures.md.",
        SkipUnless = nameof(RoundTripAvailable))]
    public async Task DaoOpenRecordset_RowCount_MatchesWriterOutput()
    {
        string dbPath = await CopyNorthwindAsync();
        const string TableName = "DaoCount";
        const int RowCount = 50;

        await using (var writer = await AccessWriter.OpenAsync(dbPath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                    new("Name", typeof(string), maxLength: 100) { IsNullable = false },
                ],
                TestContext.Current.CancellationToken);

            var rows = new object[RowCount][];
            for (int i = 0; i < RowCount; i++)
            {
                rows[i] = [DBNull.Value, $"Row_{i}"];
            }

            await writer.InsertRowsAsync(TableName, rows, TestContext.Current.CancellationToken);
        }

        string dbLiteral = dbPath.Replace("'", "''", StringComparison.Ordinal);
        string script =
            "$ErrorActionPreference = 'Stop'\n" +
            "$engine = New-Object -ComObject DAO.DBEngine.120\n" +
            "try {\n" +
            $"  $db = $engine.OpenDatabase('{dbLiteral}')\n" +
            "  try {\n" +
            $"    $rs = $db.OpenRecordset('SELECT COUNT(*) AS Cnt FROM [{TableName}]', 4)\n" +
            "    Write-Output $rs.Fields('Cnt').Value\n" +
            "    $rs.Close()\n" +
            "  } finally {\n" +
            "    $db.Close()\n" +
            "  }\n" +
            "} finally {\n" +
            "  [System.Runtime.InteropServices.Marshal]::ReleaseComObject($engine) | Out-Null\n" +
            "  [GC]::Collect(); [GC]::WaitForPendingFinalizers()\n" +
            "}\n";

        var result = AccessRoundTripEnvironment.RunDaoScript(script, _workDir, DaoTimeout);
        Assert.True(
            result.ExitCode == 0,
            $"DAO script failed (exit={result.ExitCode}).\nstdout: {result.StdOut}\nstderr: {result.StdErr}");

        int daoCount = int.Parse(result.StdOut.Trim(), CultureInfo.InvariantCulture);
        Assert.Equal(RowCount, daoCount);
    }

    /// <summary>
    /// Opens a writer-produced table via DAO and uses Seek on the primary
    /// key to locate a specific row. Catches index pages that parse cleanly in
    /// our reader but are rejected by the canonical engine.
    /// </summary>
    [Fact(
        Skip = "DAO OpenRecordset rejects writer-created tables ('Unrecognized database format') — TDEF page layout not yet fully compatible with DAO.DBEngine.120. See docs/design/round-trip-test-failures.md.",
        SkipUnless = nameof(RoundTripAvailable))]
    public async Task DaoIndexTraversal_Seek_LocatesRowByPrimaryKey()
    {
        string dbPath = await CopyNorthwindAsync();
        const string TableName = "DaoSeek";
        const int TargetId = 25;

        await using (var writer = await AccessWriter.OpenAsync(dbPath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                    new("Label", typeof(string), maxLength: 50) { IsNullable = false },
                ],
                TestContext.Current.CancellationToken);

            var rows = new object[30][];
            for (int i = 0; i < 30; i++)
            {
                rows[i] = [DBNull.Value, $"Item_{i + 1}"];
            }

            await writer.InsertRowsAsync(TableName, rows, TestContext.Current.CancellationToken);
        }

        string dbLiteral = dbPath.Replace("'", "''", StringComparison.Ordinal);
        string script =
            "$ErrorActionPreference = 'Stop'\n" +
            "$engine = New-Object -ComObject DAO.DBEngine.120\n" +
            "try {\n" +
            $"  $db = $engine.OpenDatabase('{dbLiteral}')\n" +
            "  try {\n" +
            $"    $rs = $db.OpenRecordset('{TableName}', 1)\n" +
            "    $rs.Index = 'PrimaryKey'\n" +
            $"    $rs.Seek('=', {TargetId})\n" +
            "    if ($rs.NoMatch) { Write-Error 'Seek did not find the row'; exit 1 }\n" +
            "    Write-Output $rs.Fields('Label').Value\n" +
            "    $rs.Close()\n" +
            "  } finally {\n" +
            "    $db.Close()\n" +
            "  }\n" +
            "} finally {\n" +
            "  [System.Runtime.InteropServices.Marshal]::ReleaseComObject($engine) | Out-Null\n" +
            "  [GC]::Collect(); [GC]::WaitForPendingFinalizers()\n" +
            "}\n";

        var result = AccessRoundTripEnvironment.RunDaoScript(script, _workDir, DaoTimeout);
        Assert.True(
            result.ExitCode == 0,
            $"DAO Seek failed (exit={result.ExitCode}).\nstdout: {result.StdOut}\nstderr: {result.StdErr}");

        string label = result.StdOut.Trim();
        Assert.Equal($"Item_{TargetId}", label);
    }

    /// <summary>
    /// Writes rows with auto-increment IDs, reopens with DAO, inserts a new
    /// row, and verifies the next AutoNumber value is one past our last
    /// inserted ID. Catches seed/counter byte-layout bugs.
    /// </summary>
    [Fact(
        Skip = "DAO OpenRecordset rejects writer-created tables ('Unrecognized database format') — TDEF page layout not yet fully compatible with DAO.DBEngine.120. See docs/design/round-trip-test-failures.md.",
        SkipUnless = nameof(RoundTripAvailable))]
    public async Task DaoAutoNumber_Continuation_NextIdFollowsLastWriterInsert()
    {
        string dbPath = await CopyNorthwindAsync();
        const string TableName = "DaoAutoNum";
        const int WriterRowCount = 10;

        await using (var writer = await AccessWriter.OpenAsync(dbPath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                    new("Value", typeof(string), maxLength: 50) { IsNullable = false },
                ],
                TestContext.Current.CancellationToken);

            var rows = new object[WriterRowCount][];
            for (int i = 0; i < WriterRowCount; i++)
            {
                rows[i] = [DBNull.Value, $"Writer_{i + 1}"];
            }

            await writer.InsertRowsAsync(TableName, rows, TestContext.Current.CancellationToken);
        }

        string dbLiteral = dbPath.Replace("'", "''", StringComparison.Ordinal);
        string script =
            "$ErrorActionPreference = 'Stop'\n" +
            "$engine = New-Object -ComObject DAO.DBEngine.120\n" +
            "try {\n" +
            $"  $db = $engine.OpenDatabase('{dbLiteral}')\n" +
            "  try {\n" +
            $"    $rs = $db.OpenRecordset('{TableName}', 1)\n" +
            "    $rs.AddNew()\n" +
            "    $rs.Fields('Value').Value = 'DaoInserted'\n" +
            "    $rs.Update()\n" +
            "    $rs.MoveLast()\n" +
            "    Write-Output $rs.Fields('Id').Value\n" +
            "    $rs.Close()\n" +
            "  } finally {\n" +
            "    $db.Close()\n" +
            "  }\n" +
            "} finally {\n" +
            "  [System.Runtime.InteropServices.Marshal]::ReleaseComObject($engine) | Out-Null\n" +
            "  [GC]::Collect(); [GC]::WaitForPendingFinalizers()\n" +
            "}\n";

        var result = AccessRoundTripEnvironment.RunDaoScript(script, _workDir, DaoTimeout);
        Assert.True(
            result.ExitCode == 0,
            $"DAO AutoNumber insert failed (exit={result.ExitCode}).\nstdout: {result.StdOut}\nstderr: {result.StdErr}");

        int daoId = int.Parse(result.StdOut.Trim(), CultureInfo.InvariantCulture);

        // The writer inserted IDs 1..WriterRowCount. DAO's next should be WriterRowCount+1.
        Assert.Equal(WriterRowCount + 1, daoId);
    }

    /// <summary>
    /// Uses DAO to write a Memo column with embedded NUL bytes into a fresh
    /// database, then reads it back with our reader. Closes §2.1 gap:
    /// "Reader-side coverage of an Access-authored fixture containing a Memo
    /// with embedded 0x00 bytes.".
    /// </summary>
    [Fact(Skip = "Requires Microsoft Access (DAO.DBEngine.120)", SkipUnless = nameof(RoundTripAvailable))]
    public async Task DaoAuthoredMemo_WithEmbeddedNuls_ReaderReturnsExactContent()
    {
        string dbPath = Path.Combine(_workDir, "memo_nuls.accdb");
        const string TableName = "MemoNuls";

        // DAO will create the table and insert a memo with embedded NULs.
        // We use a known pattern that can be reconstructed for assertion.
        // Pattern: "Hello\0World\0End" (15 chars)
        const string ExpectedValue = "Hello\0World\0End";

        string dbLiteral = dbPath.Replace("'", "''", StringComparison.Ordinal);
        string script =
            "$ErrorActionPreference = 'Stop'\n" +
            "$engine = New-Object -ComObject DAO.DBEngine.120\n" +
            "try {\n" +
            $"  $db = $engine.CreateDatabase('{dbLiteral}', ';LANGID=0x0409;CP=1252;COUNTRY=0')\n" +
            "  try {\n" +
            $"    $db.Execute('CREATE TABLE {TableName} (Id AUTOINCREMENT PRIMARY KEY, Content MEMO)')\n" +
            $"    $rs = $db.OpenRecordset('{TableName}', 2)\n" +
            "    $rs.AddNew()\n" +
            "    $chars = @([char[]]'Hello') + @([char]0) + @([char[]]'World') + @([char]0) + @([char[]]'End')\n" +
            "    $memo = [string]::new($chars)\n" +
            "    $rs.Fields('Content').Value = $memo\n" +
            "    $rs.Update()\n" +
            "    $rs.Close()\n" +
            "    Write-Output 'OK'\n" +
            "  } finally {\n" +
            "    $db.Close()\n" +
            "  }\n" +
            "} finally {\n" +
            "  [System.Runtime.InteropServices.Marshal]::ReleaseComObject($engine) | Out-Null\n" +
            "  [GC]::Collect(); [GC]::WaitForPendingFinalizers()\n" +
            "}\n";

        var result = AccessRoundTripEnvironment.RunDaoScript(script, _workDir, DaoTimeout);
        Assert.True(
            result.ExitCode == 0,
            $"DAO Memo creation failed (exit={result.ExitCode}).\nstdout: {result.StdOut}\nstderr: {result.StdErr}");

        // Now read with our reader and verify the Memo comes back with NULs intact.
        await using var reader = await AccessReader.OpenAsync(
            dbPath,
            new AccessReaderOptions { UseLockFile = false },
            TestContext.Current.CancellationToken);

        DataTable dt = (await reader.ReadDataTableAsync(TableName, cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.Equal(1, dt.Rows.Count);

        string actual = Assert.IsType<string>(dt.Rows[0]["Content"]);
        Assert.Equal(ExpectedValue.Length, actual.Length);
        Assert.Equal(ExpectedValue, actual);
    }

    /// <summary>
    /// Gets a value indicating whether the round-trip environment is available.
    /// When <c>false</c>, DAO tests report as skipped rather than failing.
    /// </summary>
    public static bool RoundTripAvailable => AccessRoundTripEnvironment.IsAvailable;

    private async Task<string> CopyNorthwindAsync()
    {
        string dest = Path.Combine(_workDir, $"nw_{Guid.NewGuid():N}.accdb");
        await using FileStream src = File.OpenRead(TestDatabases.NorthwindTraders);
        await using FileStream dst = File.Create(dest);
        await src.CopyToAsync(dst, TestContext.Current.CancellationToken);
        dst.Close();
        File.SetAttributes(dest, File.GetAttributes(dest) & ~FileAttributes.ReadOnly);
        return dest;
    }
}
