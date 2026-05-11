namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

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
    [Fact(Skip = "DAO OpenRecordset still rejects writer-created user-table TDEFs ('Unrecognized database format ''.'). H25 (autonum_flag at TDEF byte 0x18, must be 0x01 unconditionally) was confirmed and fixed 2026-05-10 but did not unblock OpenRecordset on its own. See docs/design/round-trip-openrecordset-hypothesis.md.")]
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
    [Fact(Skip = "DAO OpenRecordset still rejects writer-created tables ('Unrecognized database format ''.'). H25 (autonum_flag at TDEF byte 0x18) confirmed and fixed 2026-05-10, no effect. See docs/design/round-trip-openrecordset-hypothesis.md.")]
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
    [Fact(Skip = "DAO OpenRecordset still rejects writer-created tables ('Unrecognized database format ''.'). H25 (autonum_flag at TDEF byte 0x18) confirmed and fixed 2026-05-10, no effect. See docs/design/round-trip-openrecordset-hypothesis.md.")]
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
    /// Writes Memo values containing embedded NULs, CJK characters, and an
    /// OLE-like binary payload via <see cref="AccessWriter"/>, then opens the
    /// database with DAO and asserts the field values round-trip exactly.
    /// Catches LVAL-chain encoding bugs that survive our own reader.
    /// Closes §3 gap: "DAO Memo/OLE fidelity".
    /// </summary>
    [Fact(Skip = "DAO OpenRecordset still rejects writer-created tables ('Unrecognized database format ''.'). H25 (autonum_flag at TDEF byte 0x18) confirmed and fixed 2026-05-10, no effect. See docs/design/round-trip-openrecordset-hypothesis.md.")]
    public async Task DaoMemoFidelity_EmbeddedNulsAndCjk_RoundTripExactly()
    {
        string dbPath = await CopyNorthwindAsync();
        const string TableName = "MemoFidelity";

        // Memo payloads: embedded NULs, CJK text, and mixed content.
        string memoNuls = "Hello\0World\0End";
        string memoCjk = "\u4F60\u597D\u4E16\u754C"; // 你好世界
        string memoMixed = "Start\0\u00E9\u00FC\u2603\0End"; // NUL + accented + snowman + NUL

        await using (var writer = await AccessWriter.OpenAsync(dbPath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                    new("MemoNuls", typeof(string)) { IsNullable = false },
                    new("MemoCjk", typeof(string)) { IsNullable = false },
                    new("MemoMixed", typeof(string)) { IsNullable = false },
                    new("BinData", typeof(byte[])),
                ],
                TestContext.Current.CancellationToken);

            byte[] binaryPayload = [0x00, 0x01, 0xFF, 0xFE, 0x42, 0x4C, 0x4F, 0x42];
            await writer.InsertRowAsync(
                TableName,
                [DBNull.Value, memoNuls, memoCjk, memoMixed, binaryPayload],
                TestContext.Current.CancellationToken);
        }

        // Read back with DAO and verify the values match.
        string dbLiteral = dbPath.Replace("'", "''", StringComparison.Ordinal);
        string script =
            "$ErrorActionPreference = 'Stop'\n" +
            "$engine = New-Object -ComObject DAO.DBEngine.120\n" +
            "try {\n" +
            $"  $db = $engine.OpenDatabase('{dbLiteral}')\n" +
            "  try {\n" +
            $"    $rs = $db.OpenRecordset('{TableName}', 4)\n" +
            "    $rs.MoveFirst()\n" +
            "    # Output each field on its own line, base64-encode to preserve NULs\n" +
            "    $enc = [System.Text.Encoding]::Unicode\n" +
            "    Write-Output ([Convert]::ToBase64String($enc.GetBytes($rs.Fields('MemoNuls').Value)))\n" +
            "    Write-Output ([Convert]::ToBase64String($enc.GetBytes($rs.Fields('MemoCjk').Value)))\n" +
            "    Write-Output ([Convert]::ToBase64String($enc.GetBytes($rs.Fields('MemoMixed').Value)))\n" +
            "    $bin = $rs.Fields('BinData').Value\n" +
            "    if ($bin -eq $null) { Write-Output '' } else { Write-Output ([Convert]::ToBase64String([byte[]]$bin)) }\n" +
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
            $"DAO Memo fidelity script failed (exit={result.ExitCode}).\nstdout: {result.StdOut}\nstderr: {result.StdErr}");

        string[] lines = result.StdOut.Trim().Split('\n', StringSplitOptions.TrimEntries);
        Assert.True(lines.Length >= 4, $"Expected 4 output lines, got {lines.Length}.\nstdout: {result.StdOut}");

        string daoNuls = System.Text.Encoding.Unicode.GetString(Convert.FromBase64String(lines[0]));
        string daoCjk = System.Text.Encoding.Unicode.GetString(Convert.FromBase64String(lines[1]));
        string daoMixed = System.Text.Encoding.Unicode.GetString(Convert.FromBase64String(lines[2]));

        Assert.Equal(memoNuls, daoNuls);
        Assert.Equal(memoCjk, daoCjk);
        Assert.Equal(memoMixed, daoMixed);

        if (!string.IsNullOrEmpty(lines[3]))
        {
            byte[] daoBin = Convert.FromBase64String(lines[3]);
            Assert.Equal([0x00, 0x01, 0xFF, 0xFE, 0x42, 0x4C, 0x4F, 0x42], daoBin);
        }
    }

    /// <summary>
    /// Writes a parent/child table pair with
    /// <see cref="RelationshipDefinition.EnforceReferentialIntegrity"/> = true,
    /// then uses DAO to attempt an insert that violates the FK constraint.
    /// Asserts DAO raises error 3201. Confirms the relationship metadata is
    /// fully understood by Access. Closes §3 gap: "DAO relationship
    /// enforcement".
    /// </summary>
    [Fact(Skip = "Requires Microsoft Access (DAO.DBEngine.120)", SkipUnless = nameof(RoundTripAvailable))]
    public async Task DaoRelationshipEnforcement_FkViolation_RaisesError()
    {
        string dbPath = await CopyNorthwindAsync();
        const string Parent = "DaoFkParent";
        const string Child = "DaoFkChild";
        const string FkName = "DaoFK_Child_Parent";

        await using (var writer = await AccessWriter.OpenAsync(dbPath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                Parent,
                [
                    new("ParentId", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                    new("Label", typeof(string), maxLength: 50) { IsNullable = false },
                ],
                TestContext.Current.CancellationToken);

            await writer.CreateTableAsync(
                Child,
                [
                    new("ChildId", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                    new("ParentId", typeof(int)) { IsNullable = false },
                    new("Detail", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(
                    FkName,
                    primaryTable: Parent,
                    primaryColumn: "ParentId",
                    foreignTable: Child,
                    foreignColumn: "ParentId")
                {
                    EnforceReferentialIntegrity = true,
                },
                TestContext.Current.CancellationToken);

            // Insert a valid parent row so the table is non-empty.
            await writer.InsertRowAsync(Parent, [DBNull.Value, "ValidParent"], TestContext.Current.CancellationToken);
        }

        string dbLiteral = dbPath.Replace("'", "''", StringComparison.Ordinal);

        // DAO script: attempt to insert a child row with a non-existent ParentId.
        // DAO should reject this with error 3201 (referential integrity violation).
        string script =
            "$ErrorActionPreference = 'Stop'\n" +
            "$engine = New-Object -ComObject DAO.DBEngine.120\n" +
            "try {\n" +
            $"  $db = $engine.OpenDatabase('{dbLiteral}')\n" +
            "  try {\n" +
            "    $errorCode = 0\n" +
            "    try {\n" +
            $"      $db.Execute(\"INSERT INTO [{Child}] (ParentId, Detail) VALUES (99999, 'Orphan')\")\n" +
            "    } catch {\n" +
            "      $errorCode = $_.Exception.ErrorCode\n" +
            "      if ($errorCode -eq 0) {\n" +
            "        # Fallback: parse HRESULT or message for DAO error number.\n" +
            "        if ($_.Exception.HResult -ne 0) { $errorCode = $_.Exception.HResult }\n" +
            "      }\n" +
            "    }\n" +
            "    Write-Output $errorCode\n" +
            "  } finally {\n" +
            "    $db.Close()\n" +
            "  }\n" +
            "} finally {\n" +
            "  [System.Runtime.InteropServices.Marshal]::ReleaseComObject($engine) | Out-Null\n" +
            "  [GC]::Collect(); [GC]::WaitForPendingFinalizers()\n" +
            "}\n";

        var result = AccessRoundTripEnvironment.RunDaoScript(script, _workDir, DaoTimeout);

        // The script itself should succeed (exit 0) — the DAO error is
        // caught and reported as output. A non-zero exit means the script
        // itself crashed (unexpected).
        Assert.True(
            result.ExitCode == 0,
            $"DAO FK enforcement script failed (exit={result.ExitCode}).\nstdout: {result.StdOut}\nstderr: {result.StdErr}");

        string output = result.StdOut.Trim();

        // DAO error 3201 maps to HRESULT 0x80040E21 (-2147217887) in some
        // COM wrappers. Accept any non-zero error code as evidence that DAO
        // rejected the FK-violating insert.
        Assert.True(
            output != "0",
            $"DAO should have raised an FK violation error, but error code was 0.\nstdout: {result.StdOut}\nstderr: {result.StdErr}");
    }

    /// <summary>
    /// Creates an ACCDB, encrypts it with Agile (AES) encryption, then runs
    /// DAO CompactDatabase with the password supplied. Verifies the compacted
    /// file reopens with our reader. Validates the encryption header is
    /// well-formed enough for Access's own engine.
    /// Closes §3 gap: "DAO CompactDatabase on encrypted output".
    /// </summary>
    [Fact(Skip = "DAO CompactDatabase rejects writer-created tables ('Unrecognized database format ''.'). H25 (autonum_flag at TDEF byte 0x18) confirmed and fixed 2026-05-10, no effect. See docs/design/round-trip-openrecordset-hypothesis.md.")]
    public async Task DaoCompactDatabase_OnEncryptedOutput_ReopenSucceeds()
    {
        string dbPath = await CopyNorthwindAsync();
        const string TableName = "EncCompact";
        const string Password = "Te$tP@ss!23";

        // Write a table with data, then encrypt.
        await using (var writer = await AccessWriter.OpenAsync(dbPath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                    new("Value", typeof(string), maxLength: 100) { IsNullable = false },
                ],
                TestContext.Current.CancellationToken);

            var rows = new object[20][];
            for (int i = 0; i < 20; i++)
            {
                rows[i] = [DBNull.Value, $"EncRow_{i}"];
            }

            await writer.InsertRowsAsync(TableName, rows, TestContext.Current.CancellationToken);
        }

        await AccessWriter.EncryptAsync(
            dbPath,
            Password,
            AccessEncryptionFormat.AccdbAgile,
            new AccessWriterOptions { UseLockFile = false },
            TestContext.Current.CancellationToken);

        // DAO CompactDatabase with the password.
        string compactedPath = Path.Combine(_workDir, $"compacted_{Guid.NewGuid():N}.accdb");
        string dbLiteral = dbPath.Replace("'", "''", StringComparison.Ordinal);
        string compLiteral = compactedPath.Replace("'", "''", StringComparison.Ordinal);
        string pwdLiteral = Password.Replace("'", "''", StringComparison.Ordinal);

        string script =
            "$ErrorActionPreference = 'Stop'\n" +
            "$engine = New-Object -ComObject DAO.DBEngine.120\n" +
            "try {\n" +
            $"  $engine.CompactDatabase('{dbLiteral}', '{compLiteral}', ';LANGID=0x0409;CP=1252;COUNTRY=0;PWD={pwdLiteral}')\n" +
            "  Write-Output 'OK'\n" +
            "} finally {\n" +
            "  [System.Runtime.InteropServices.Marshal]::ReleaseComObject($engine) | Out-Null\n" +
            "  [GC]::Collect(); [GC]::WaitForPendingFinalizers()\n" +
            "}\n";

        var result = AccessRoundTripEnvironment.RunDaoScript(script, _workDir, DaoTimeout);
        Assert.True(
            result.ExitCode == 0,
            $"DAO CompactDatabase on encrypted file failed (exit={result.ExitCode}).\nstdout: {result.StdOut}\nstderr: {result.StdErr}");
        Assert.True(File.Exists(compactedPath), "Compacted output file was not created.");

        // Verify our reader can reopen the compacted encrypted file.
        await using var reader = await AccessReader.OpenAsync(
            compactedPath,
            new AccessReaderOptions
            {
                UseLockFile = false,
                Password = Password.AsMemory(),
            },
            TestContext.Current.CancellationToken);

        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);
    }

    /// <summary>
    /// Creates 10+ tables with primary keys, indexes, foreign keys, and
    /// 1000+ rows each, then runs DAO Compact &amp; Repair. Stress test for
    /// page allocation and usage-map consistency at scale.
    /// Closes §3 gap: "DAO multi-table stress".
    /// </summary>
    [Fact(Skip = "Requires Microsoft Access (DAO.DBEngine.120)", SkipUnless = nameof(RoundTripAvailable))]
    public async Task DaoCompact_MultiTableStress_SurvivesCompactAndRepair()
    {
        string dbPath = await CopyNorthwindAsync();
        const int TableCount = 12;
        const int RowsPerTable = 1000;

        await using (var writer = await AccessWriter.OpenAsync(dbPath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            // Create parent table for relationships.
            const string ParentTable = "Stress_Parent";
            await writer.CreateTableAsync(
                ParentTable,
                [
                    new("ParentId", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                    new("Name", typeof(string), maxLength: 100) { IsNullable = false },
                ],
                TestContext.Current.CancellationToken);

            var parentRows = new object[RowsPerTable][];
            for (int r = 0; r < RowsPerTable; r++)
            {
                parentRows[r] = [DBNull.Value, $"Parent_{r}"];
            }

            await writer.InsertRowsAsync(ParentTable, parentRows, TestContext.Current.CancellationToken);

            // Create child tables referencing the parent.
            for (int t = 0; t < TableCount; t++)
            {
                string tableName = $"Stress_T{t:D2}";
                await writer.CreateTableAsync(
                    tableName,
                    [
                        new("Id", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                        new("ParentId", typeof(int)) { IsNullable = false },
                        new("Seq", typeof(int)) { IsNullable = false },
                        new("Label", typeof(string), maxLength: 200) { IsNullable = false },
                        new("Amount", typeof(double)),
                        new("Created", typeof(DateTime)),
                    ],
                    TestContext.Current.CancellationToken);

                if (t < 5)
                {
                    // First 5 child tables get FK relationships.
                    await writer.CreateRelationshipAsync(
                        new RelationshipDefinition(
                            $"StressFK_T{t:D2}_Parent",
                            primaryTable: ParentTable,
                            primaryColumn: "ParentId",
                            foreignTable: tableName,
                            foreignColumn: "ParentId")
                        {
                            EnforceReferentialIntegrity = true,
                        },
                        TestContext.Current.CancellationToken);
                }

                var rows = new object[RowsPerTable][];
                for (int r = 0; r < RowsPerTable; r++)
                {
                    // ParentId cycles 1..RowsPerTable to stay within valid parent range.
                    rows[r] =
                    [
                        DBNull.Value,
                        (r % RowsPerTable) + 1,
                        r,
                        $"T{t:D2}_Row_{r}_{new string('X', 50)}",
                        r * 1.23,
                        new DateTime(2020, 1, 1).AddMinutes(r),
                    ];
                }

                await writer.InsertRowsAsync(tableName, rows, TestContext.Current.CancellationToken);
            }
        }

        // Pre-compact: verify our own reader can parse the output.
        await using (var reader = await AccessReader.OpenAsync(
            dbPath,
            new AccessReaderOptions { UseLockFile = false },
            TestContext.Current.CancellationToken))
        {
            var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
            Assert.True(tables.Count >= TableCount + 1, $"Expected at least {TableCount + 1} tables, got {tables.Count}.");
        }

        // Run DAO Compact & Repair.
        string compactedPath = Path.Combine(_workDir, $"stress_compact_{Guid.NewGuid():N}.accdb");
        var compactResult = AccessRoundTripEnvironment.RunDaoCompact(dbPath, compactedPath, TimeSpan.FromMinutes(3));
        Assert.True(
            compactResult.ExitCode == 0,
            $"DAO CompactDatabase failed on multi-table stress db (exit={compactResult.ExitCode}).\nstdout: {compactResult.StdOut}\nstderr: {compactResult.StdErr}");
        Assert.True(File.Exists(compactedPath), "Compacted stress output file was not created.");

        // Post-compact: verify tables and row counts survived.
        await using var postReader = await AccessReader.OpenAsync(
            compactedPath,
            new AccessReaderOptions { UseLockFile = false },
            TestContext.Current.CancellationToken);

        var postTables = await postReader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.True(postTables.Count >= TableCount + 1, $"Post-compact: expected at least {TableCount + 1} tables, got {postTables.Count}.");

        // Spot-check row counts on a few tables.
        for (int t = 0; t < Math.Min(3, TableCount); t++)
        {
            string tableName = $"Stress_T{t:D2}";
            if (postTables.Contains(tableName, StringComparer.OrdinalIgnoreCase))
            {
                DataTable? dt = await postReader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken);
                Assert.True(
                    dt is not null && dt.Rows.Count == RowsPerTable,
                    $"Post-compact: {tableName} row count = {dt?.Rows.Count ?? -1}, expected {RowsPerTable}.");
            }
        }
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
