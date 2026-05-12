namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit.Sdk;

[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "xUnit IClassFixture<T> requires public accessibility")]
public sealed class DaoValidationFixture : IAsyncDisposable
{
    internal const int CoreRowCount = 50;
    internal const int CoreTargetId = 25;
    internal const int CoreWriterRowCount = 10;
    internal const int StressRowsPerTable = 250;
    internal const int StressTableCount = 6;
    internal const string ExpectedMemoWithNuls = "Hello\0World\0End";

    private const string AutoNumberTable = "DaoAutoNum";
    private const string ChildTable = "DaoFkChild";
    private const string CountTable = "DaoCount";
    private const string CreateDatabaseAttributes = ";LANGID=0x0409;CP=1252;COUNTRY=0";
    private const string EncryptedCompactTable = "EncCompact";
    private const string FkName = "DaoFK_Child_Parent";
    private const string MemoFidelityTable = "MemoFidelity";
    private const string MemoNulsTable = "MemoNuls";
    private const string ParentTable = "DaoFkParent";
    private const string Password = "Te$tP@ss!23";
    private const string SeekTable = "DaoSeek";
    private const string TempDirectoryName = "JetDatabaseWriter.Tests.DaoValidation";

    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan StressCompactTimeout = TimeSpan.FromMinutes(3);

    private readonly object _sync = new();
    private readonly List<AccessRoundTripSession> _sessions = [];
    private Task<CoreValidationResult>? _coreResultTask;
    private Task<EncryptedCompactResult>? _encryptedCompactResultTask;
    private Task<StressCompactResult>? _stressCompactResultTask;

    internal Task<CoreValidationResult> GetCoreResultAsync(CancellationToken cancellationToken)
    {
        lock (_sync)
        {
            _coreResultTask ??= BuildCoreResultAsync(cancellationToken);
            return _coreResultTask;
        }
    }

    internal async Task<DaoMemoResult> GetDaoMemoResultAsync(CancellationToken cancellationToken)
    {
        CoreValidationResult result = await GetCoreResultAsync(cancellationToken).ConfigureAwait(false);
        return new DaoMemoResult(result.DaoAuthoredMemo);
    }

    internal Task<EncryptedCompactResult> GetEncryptedCompactResultAsync(CancellationToken cancellationToken)
    {
        lock (_sync)
        {
            _encryptedCompactResultTask ??= BuildEncryptedCompactResultAsync(cancellationToken);
            return _encryptedCompactResultTask;
        }
    }

    internal Task<StressCompactResult> GetStressCompactResultAsync(CancellationToken cancellationToken)
    {
        lock (_sync)
        {
            _stressCompactResultTask ??= BuildStressCompactResultAsync(cancellationToken);
            return _stressCompactResultTask;
        }
    }

    public async ValueTask DisposeAsync()
    {
        AccessRoundTripSession[] sessions;
        lock (_sync)
        {
            sessions = [.. _sessions];
            _sessions.Clear();
        }

        foreach (AccessRoundTripSession session in sessions)
        {
            await session.DisposeAsync().ConfigureAwait(false);
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "The fixture tracks sessions and disposes them during teardown.")]
    private async Task<CoreValidationResult> BuildCoreResultAsync(CancellationToken cancellationToken)
    {
        AccessRoundTripSession session = await CreateNorthwindSessionAsync(cancellationToken).ConfigureAwait(false);
        await PrepareCoreValidationDatabaseAsync(session.SourcePath, cancellationToken).ConfigureAwait(false);

        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScript(
            session.SourcePath,
            BuildCoreValidationScript(),
            DaoTimeout);
        EnsureDaoSuccess(result, "DAO validation script failed.");

        Dictionary<string, string> values = ParseKeyValueOutput(result.StdOut);
        string daoAuthoredMemo = await ReadDaoAuthoredMemoAsync(session.SourcePath, cancellationToken).ConfigureAwait(false);
        return new CoreValidationResult(
            ParseInt(values, "ROWCOUNT"),
            GetRequired(values, "SEEK_LABEL"),
            ParseInt(values, "AUTONUM_ID"),
            DecodeUnicodeBase64(values, "MEMO_NULS"),
            DecodeUnicodeBase64(values, "MEMO_CJK"),
            DecodeUnicodeBase64(values, "MEMO_MIXED"),
            DecodeBinaryBase64(values, "MEMO_BIN"),
            GetRequired(values, "FK_ERROR"),
            daoAuthoredMemo);
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "The fixture tracks sessions and disposes them during teardown.")]
    private async Task<EncryptedCompactResult> BuildEncryptedCompactResultAsync(CancellationToken cancellationToken)
    {
        AccessRoundTripSession session = await CreateNorthwindSessionAsync(cancellationToken).ConfigureAwait(false);
        await PrepareEncryptedDatabaseAsync(session.SourcePath, cancellationToken).ConfigureAwait(false);

        string compactedPath = session.CreateDatabasePath("compacted");
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(
            BuildEncryptedCompactScript(session.SourcePath, compactedPath),
            DaoTimeout);
        EnsureDaoSuccess(result, "DAO CompactDatabase on encrypted file failed.");

        int tableCount = 0;
        if (File.Exists(compactedPath))
        {
            await using var reader = await AccessReader.OpenAsync(
                compactedPath,
                new AccessReaderOptions
                {
                    UseLockFile = false,
                    Password = Password.AsMemory(),
                },
                cancellationToken).ConfigureAwait(false);

            List<string> tables = await reader.ListTablesAsync(cancellationToken).ConfigureAwait(false);
            tableCount = tables.Count;
        }

        return new EncryptedCompactResult(File.Exists(compactedPath), tableCount);
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "The fixture tracks sessions and disposes them during teardown.")]
    private async Task<StressCompactResult> BuildStressCompactResultAsync(CancellationToken cancellationToken)
    {
        AccessRoundTripSession session = await CreateNorthwindSessionAsync(cancellationToken, StressCompactTimeout).ConfigureAwait(false);
        await PrepareStressDatabaseAsync(session.SourcePath, cancellationToken).ConfigureAwait(false);

        int preCompactTableCount = await CountTablesAsync(session.SourcePath, cancellationToken).ConfigureAwait(false);

        session.RunDaoCompact();

        await using var postReader = await AccessReader.OpenAsync(
            session.CompactedPath,
            new AccessReaderOptions { UseLockFile = false },
            cancellationToken).ConfigureAwait(false);

        List<string> postTables = await postReader.ListTablesAsync(cancellationToken).ConfigureAwait(false);
        var rowCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < 3; i++)
        {
            string tableName = $"Stress_T{i:D2}";
            if (postTables.Contains(tableName, StringComparer.OrdinalIgnoreCase))
            {
                DataTable? table = await postReader.ReadDataTableAsync(
                    tableName,
                    cancellationToken: cancellationToken).ConfigureAwait(false);
                rowCounts[tableName] = table?.Rows.Count ?? -1;
            }
        }

        return new StressCompactResult(preCompactTableCount, postTables.Count, rowCounts);
    }

    private async Task<AccessRoundTripSession> CreateNorthwindSessionAsync(
        CancellationToken cancellationToken,
        TimeSpan? compactTimeout = null)
    {
        AccessRoundTripSession session = await AccessRoundTripSession.CreateFromNorthwindAsync(
            cancellationToken,
            TempDirectoryName,
            compactTimeout).ConfigureAwait(false);
        Track(session);
        return session;
    }

    private void Track(AccessRoundTripSession session)
    {
        lock (_sync)
        {
            _sessions.Add(session);
        }
    }

#pragma warning disable CA1822 // Keep helper workflow grouped after instance members without tripping static member ordering.
    private async Task PrepareCoreValidationDatabaseAsync(string dbPath, CancellationToken cancellationToken)
    {
        await using var writer = await AccessWriter.OpenAsync(
            dbPath,
            new AccessWriterOptions { UseLockFile = false },
            cancellationToken).ConfigureAwait(false);

        await writer.CreateTableAsync(
            CountTable,
            [
                new("Id", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                new("Name", typeof(string), maxLength: 100) { IsNullable = false },
            ],
            cancellationToken).ConfigureAwait(false);

        var countRows = new object[CoreRowCount][];
        for (int i = 0; i < CoreRowCount; i++)
        {
            countRows[i] = [DBNull.Value, $"Row_{i}"];
        }

        await writer.InsertRowsAsync(CountTable, countRows, cancellationToken).ConfigureAwait(false);

        await writer.CreateTableAsync(
            SeekTable,
            [
                new("Id", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                new("Label", typeof(string), maxLength: 50) { IsNullable = false },
            ],
            cancellationToken).ConfigureAwait(false);

        var seekRows = new object[30][];
        for (int i = 0; i < seekRows.Length; i++)
        {
            seekRows[i] = [DBNull.Value, $"Item_{i + 1}"];
        }

        await writer.InsertRowsAsync(SeekTable, seekRows, cancellationToken).ConfigureAwait(false);

        await writer.CreateTableAsync(
            AutoNumberTable,
            [
                new("Id", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                new("Value", typeof(string), maxLength: 50) { IsNullable = false },
            ],
            cancellationToken).ConfigureAwait(false);

        var autoNumberRows = new object[CoreWriterRowCount][];
        for (int i = 0; i < CoreWriterRowCount; i++)
        {
            autoNumberRows[i] = [DBNull.Value, $"Writer_{i + 1}"];
        }

        await writer.InsertRowsAsync(AutoNumberTable, autoNumberRows, cancellationToken).ConfigureAwait(false);

        await writer.CreateTableAsync(
            MemoFidelityTable,
            [
                new("Id", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                new("MemoNuls", typeof(string)) { IsNullable = false },
                new("MemoCjk", typeof(string)) { IsNullable = false },
                new("MemoMixed", typeof(string)) { IsNullable = false },
                new("BinData", typeof(byte[])),
            ],
            cancellationToken).ConfigureAwait(false);

        await writer.InsertRowAsync(
            MemoFidelityTable,
            [DBNull.Value, ExpectedMemoWithNuls, "\u4F60\u597D\u4E16\u754C", "Start\0\u00E9\u00FC\u2603\0End", new byte[] { 0x00, 0x01, 0xFF, 0xFE, 0x42, 0x4C, 0x4F, 0x42 }],
            cancellationToken).ConfigureAwait(false);

        await writer.CreateTableAsync(
            ParentTable,
            [
                new("ParentId", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                new("Label", typeof(string), maxLength: 50) { IsNullable = false },
            ],
            cancellationToken).ConfigureAwait(false);

        await writer.CreateTableAsync(
            ChildTable,
            [
                new("ChildId", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                new("ParentId", typeof(int)) { IsNullable = false },
                new("Detail", typeof(string), maxLength: 50),
            ],
            cancellationToken).ConfigureAwait(false);

        await writer.CreateRelationshipAsync(
            new RelationshipDefinition(
                FkName,
                primaryTable: ParentTable,
                primaryColumn: "ParentId",
                foreignTable: ChildTable,
                foreignColumn: "ParentId")
            {
                EnforceReferentialIntegrity = true,
            },
            cancellationToken).ConfigureAwait(false);

        await writer.InsertRowAsync(ParentTable, [DBNull.Value, "ValidParent"], cancellationToken).ConfigureAwait(false);
    }

    private async Task PrepareEncryptedDatabaseAsync(string dbPath, CancellationToken cancellationToken)
    {
        await using (var writer = await AccessWriter.OpenAsync(
            dbPath,
            new AccessWriterOptions { UseLockFile = false },
            cancellationToken).ConfigureAwait(false))
        {
            await writer.CreateTableAsync(
                EncryptedCompactTable,
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                    new("Value", typeof(string), maxLength: 100) { IsNullable = false },
                ],
                cancellationToken).ConfigureAwait(false);

            var rows = new object[20][];
            for (int i = 0; i < rows.Length; i++)
            {
                rows[i] = [DBNull.Value, $"EncRow_{i}"];
            }

            await writer.InsertRowsAsync(EncryptedCompactTable, rows, cancellationToken).ConfigureAwait(false);
        }

        await AccessWriter.EncryptAsync(
            dbPath,
            Password,
            AccessEncryptionFormat.AccdbAgile,
            new AccessWriterOptions { UseLockFile = false },
            cancellationToken).ConfigureAwait(false);
    }

    private async Task PrepareStressDatabaseAsync(string dbPath, CancellationToken cancellationToken)
    {
        await using var writer = await AccessWriter.OpenAsync(
            dbPath,
            new AccessWriterOptions { UseLockFile = false },
            cancellationToken).ConfigureAwait(false);

        const string StressParentTable = "Stress_Parent";
        await writer.CreateTableAsync(
            StressParentTable,
            [
                new("ParentId", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                new("Name", typeof(string), maxLength: 100) { IsNullable = false },
            ],
            cancellationToken).ConfigureAwait(false);

        var parentRows = new object[StressRowsPerTable][];
        for (int row = 0; row < StressRowsPerTable; row++)
        {
            parentRows[row] = [DBNull.Value, $"Parent_{row}"];
        }

        await writer.InsertRowsAsync(StressParentTable, parentRows, cancellationToken).ConfigureAwait(false);

        for (int table = 0; table < StressTableCount; table++)
        {
            string tableName = $"Stress_T{table:D2}";
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
                cancellationToken).ConfigureAwait(false);

            if (table < 5)
            {
                await writer.CreateRelationshipAsync(
                    new RelationshipDefinition(
                        $"StressFK_T{table:D2}_Parent",
                        primaryTable: StressParentTable,
                        primaryColumn: "ParentId",
                        foreignTable: tableName,
                        foreignColumn: "ParentId")
                    {
                        EnforceReferentialIntegrity = true,
                    },
                    cancellationToken).ConfigureAwait(false);
            }

            var rows = new object[StressRowsPerTable][];
            for (int row = 0; row < StressRowsPerTable; row++)
            {
                rows[row] =
                [
                    DBNull.Value,
                    (row % StressRowsPerTable) + 1,
                    row,
                    $"T{table:D2}_Row_{row}_{new string('X', 50)}",
                    row * 1.23,
                    new DateTime(2020, 1, 1).AddMinutes(row),
                ];
            }

            await writer.InsertRowsAsync(tableName, rows, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<int> CountTablesAsync(string dbPath, CancellationToken cancellationToken)
    {
        await using var reader = await AccessReader.OpenAsync(
            dbPath,
            new AccessReaderOptions { UseLockFile = false },
            cancellationToken).ConfigureAwait(false);

        List<string> tables = await reader.ListTablesAsync(cancellationToken).ConfigureAwait(false);
        return tables.Count;
    }

    private async Task<string> ReadDaoAuthoredMemoAsync(string dbPath, CancellationToken cancellationToken)
    {
        await using var reader = await AccessReader.OpenAsync(
            dbPath,
            new AccessReaderOptions { UseLockFile = false },
            cancellationToken).ConfigureAwait(false);

        DataTable? table = await reader.ReadDataTableAsync(
            MemoNulsTable,
            cancellationToken: cancellationToken).ConfigureAwait(false);
        if (table is null || table.Rows.Count != 1)
        {
            throw new XunitException($"Expected one row in {MemoNulsTable}, got {table?.Rows.Count ?? -1}.");
        }

        if (table.Rows[0]["Content"] is not string content)
        {
            throw new XunitException($"Expected {MemoNulsTable}.Content to be a string.");
        }

        return content;
    }

    private string BuildCoreValidationScript() =>
        $$"""
        $enc = [System.Text.Encoding]::Unicode
        function ToBase64($value) {
            return [Convert]::ToBase64String($enc.GetBytes([string]$value))
        }

        $rs = $null
        try {
            $rs = $db.OpenRecordset('SELECT COUNT(*) AS Cnt FROM [{{CountTable}}]', 4)
            Write-Output "ROWCOUNT=$($rs.Fields('Cnt').Value)"
        } finally {
            if ($rs -ne $null) { $rs.Close(); $rs = $null }
        }

        try {
            $rs = $db.OpenRecordset('{{SeekTable}}', 1)
            $rs.Index = 'PrimaryKey'
            $rs.Seek('=', {{CoreTargetId}})
            if ($rs.NoMatch) { throw 'Seek did not find the row' }
            Write-Output "SEEK_LABEL=$($rs.Fields('Label').Value)"
        } finally {
            if ($rs -ne $null) { $rs.Close(); $rs = $null }
        }

        try {
            $rs = $db.OpenRecordset('{{AutoNumberTable}}', 1)
            $rs.AddNew()
            $rs.Fields('Value').Value = 'DaoInserted'
            $rs.Update()
            $rs.MoveLast()
            Write-Output "AUTONUM_ID=$($rs.Fields('Id').Value)"
        } finally {
            if ($rs -ne $null) { $rs.Close(); $rs = $null }
        }

        try {
            $rs = $db.OpenRecordset('{{MemoFidelityTable}}', 4)
            $rs.MoveFirst()
            $memoNuls = $rs.Fields('MemoNuls').Value
            $memoCjk = $rs.Fields('MemoCjk').Value
            $memoMixed = $rs.Fields('MemoMixed').Value
            Write-Output "MEMO_NULS=$(ToBase64 $memoNuls)"
            Write-Output "MEMO_CJK=$(ToBase64 $memoCjk)"
            Write-Output "MEMO_MIXED=$(ToBase64 $memoMixed)"
            $bin = $rs.Fields('BinData').Value
            if ($bin -eq $null) { Write-Output 'MEMO_BIN=' } else { Write-Output "MEMO_BIN=$([Convert]::ToBase64String([byte[]]$bin))" }
        } finally {
            if ($rs -ne $null) { $rs.Close(); $rs = $null }
        }

        $fkErrorCode = 0
        try {
            $db.Execute("INSERT INTO [{{ChildTable}}] (ParentId, Detail) VALUES (99999, 'Orphan')", 128)
        } catch {
            $fkErrorCode = $_.Exception.ErrorCode
            if ($fkErrorCode -eq 0) {
                if ($_.Exception.HResult -ne 0) { $fkErrorCode = $_.Exception.HResult }
            }
        }
        Write-Output "FK_ERROR=$fkErrorCode"

        $rs = $null
        try {
            $db.Execute('CREATE TABLE {{MemoNulsTable}} (Id AUTOINCREMENT PRIMARY KEY, Content MEMO)')
            $rs = $db.OpenRecordset('{{MemoNulsTable}}', 2)
            $rs.AddNew()
            $chars = @([char[]]'Hello') + @([char]0) + @([char[]]'World') + @([char]0) + @([char[]]'End')
            $memo = [string]::new($chars)
            $rs.Fields('Content').Value = $memo
            $rs.Update()
            Write-Output 'DAO_MEMO_CREATE=OK'
        } finally {
            if ($rs -ne $null) { $rs.Close() }
        }
        """;

    private string BuildEncryptedCompactScript(string dbPath, string compactedPath)
    {
        string dbLiteral = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(dbPath);
        string compactedLiteral = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(compactedPath);
        string attributesLiteral = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral($"{CreateDatabaseAttributes};PWD={Password}");
        string sourcePasswordLiteral = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral($";PWD={Password}");

        return $$"""
        $engine.CompactDatabase({{dbLiteral}}, {{compactedLiteral}}, {{attributesLiteral}}, 0, {{sourcePasswordLiteral}})
        Write-Output 'ENCRYPTED_COMPACT=OK'
        """;
    }

    private void EnsureDaoSuccess(AccessRoundTripEnvironment.CompactResult result, string message)
    {
        if (result.ExitCode != 0)
        {
            throw new XunitException(
                $"""
                {message} (exit={result.ExitCode}).
                --- stdout ---
                {result.StdOut}
                --- stderr ---
                {result.StdErr}
                """);
        }
    }

    private Dictionary<string, string> ParseKeyValueOutput(string stdout)
    {
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        string[] lines = stdout.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        foreach (string line in lines)
        {
            int separator = line.IndexOf('=', StringComparison.Ordinal);
            if (separator <= 0)
            {
                continue;
            }

            values[line[..separator]] = line[(separator + 1)..];
        }

        return values;
    }

    private int ParseInt(Dictionary<string, string> values, string key) =>
        int.Parse(GetRequired(values, key), CultureInfo.InvariantCulture);

    private string DecodeUnicodeBase64(Dictionary<string, string> values, string key) =>
        Encoding.Unicode.GetString(Convert.FromBase64String(GetRequired(values, key)));

    private byte[] DecodeBinaryBase64(Dictionary<string, string> values, string key)
    {
        string value = GetRequired(values, key);
        return string.IsNullOrEmpty(value) ? [] : Convert.FromBase64String(value);
    }

    private string GetRequired(Dictionary<string, string> values, string key)
    {
        if (!values.TryGetValue(key, out string? value))
        {
            throw new XunitException($"DAO script output did not include '{key}'.");
        }

        return value;
    }
#pragma warning restore CA1822

    internal sealed record CoreValidationResult(
        int RowCount,
        string SeekLabel,
        int AutoNumberId,
        string MemoNuls,
        string MemoCjk,
        string MemoMixed,
        byte[] MemoBinary,
        string FkErrorCode,
        string DaoAuthoredMemo);

    internal sealed record DaoMemoResult(string Content);

    internal sealed record EncryptedCompactResult(bool CompactedFileExists, int ReopenedTableCount);

    internal sealed record StressCompactResult(
        int PreCompactTableCount,
        int PostCompactTableCount,
        IReadOnlyDictionary<string, int> PostCompactRowCounts);
}
