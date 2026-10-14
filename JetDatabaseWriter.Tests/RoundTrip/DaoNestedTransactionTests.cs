namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Verifies native nesting, rollback, depth and compacted writer output against DAO.</summary>
/// <param name="output">Captures native evidence.</param>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoNestedTransactionTests(ITestOutputHelper output)
{
    private static readonly int[] ExpectedIds = [4, 5];
    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(2);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>DAO120 and the writer resolve nested scopes identically in Jet4 and ACE.</summary>
    /// <param name="format">The native database format.</param>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public Task DaoAndWriter_NestedScopes_HaveMatchingRowsAndDepth(DatabaseFormat format) => this.VerifyAsync(format);

    /// <summary>DAO3.6 and the writer resolve nested scopes identically in Jet3.</summary>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    public Task Dao36AndWriter_NestedScopes_HaveMatchingRowsAndDepth() => this.VerifyAsync(DatabaseFormat.Jet3Mdb);

    private static AccessRoundTripEnvironment.CompactResult RunEngineScript(DatabaseFormat format, string script, AccessRoundTripSession session)
    {
        if (format != DatabaseFormat.Jet3Mdb)
        {
            return AccessRoundTripEnvironment.RunDaoEngineScript(script, session.WorkDir, DaoTimeout);
        }

        PowerShellRunResult run = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, DaoTimeout);
        Assert.False(run.TimedOut);
        Assert.True(run.OutputComplete);
        return new AccessRoundTripEnvironment.CompactResult(run.ExitCode, run.StandardOutput, run.StandardError);
    }

    private static AccessRoundTripEnvironment.CompactResult RunDatabaseScript(DatabaseFormat format, string path, string script, AccessRoundTripSession session)
    {
        string databaseScript = $$"""
            $db = $engine.OpenDatabase({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(path)}})
            try {
                {{script}}
            } finally { $db.Close() }
            """;
        return RunEngineScript(format, databaseScript, session);
    }

    private static void AssertDaoSuccess(AccessRoundTripEnvironment.CompactResult result) =>
        Assert.True(result.ExitCode == 0, $"DAO exit {result.ExitCode}: {result.StdOut}\n{result.StdErr}");

    private static async Task<int[]> ReadIdsAsync(string path)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, Ct);
        var ids = new List<int>();
        await foreach (object[] row in reader.Rows("Items", cancellationToken: Ct))
        {
            ids.Add((int)row[0]);
        }

        ids.Sort();
        return [.. ids];
    }

    private async Task VerifyAsync(DatabaseFormat format)
    {
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string createVersion = format switch { DatabaseFormat.Jet3Mdb => ", 32", DatabaseFormat.Jet4Mdb => ", 64", DatabaseFormat.AceAccdb => string.Empty, _ => throw new ArgumentOutOfRangeException(nameof(format), format, null) };
        string setup = $$"""
            $db = $engine.CreateDatabase({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath)}}, ';LANGID=0x0409;CP=1252;COUNTRY=0'{{createVersion}})
            try { $db.Execute('CREATE TABLE [Items] ([Id] LONG PRIMARY KEY)', 128) } finally { $db.Close() }
            """;
        AssertDaoSuccess(RunEngineScript(format, setup, session));
        string writerPath = session.CreateDatabasePath("writer");
        File.Copy(session.SourcePath, writerPath);
        const string nativeScript = """
            function Count-Rows {
                $rs = $db.OpenRecordset('SELECT COUNT(*) AS N FROM Items', 4)
                try { return $rs.Fields.Item('N').Value } finally { $rs.Close() }
            }
            $ws = $engine.Workspaces.Item(0)
            $ws.BeginTrans(); $db.Execute('INSERT INTO Items VALUES (1)', 128)
            $ws.BeginTrans(); $db.Execute('INSERT INTO Items VALUES (2)', 128)
            $ws.Rollback()
            Write-Output "CHILD_ROLLBACK=$(Count-Rows)"
            $ws.BeginTrans(); $db.Execute('INSERT INTO Items VALUES (3)', 128); $ws.CommitTrans()
            $ws.Rollback()
            Write-Output "PARENT_ROLLBACK=$(Count-Rows)"
            $ws.BeginTrans(); $db.Execute('INSERT INTO Items VALUES (4)', 128)
            $ws.BeginTrans(); $db.Execute('INSERT INTO Items VALUES (5)', 128); $ws.CommitTrans()
            $ws.CommitTrans(1)
            foreach ($operation in @('execute0', 'execute128', 'recordset')) {
                $depth = 0; $rs = $null
                try {
                    if ($operation -eq 'recordset') { $rs = $db.OpenRecordset('Items', 2) }
                    for ($level = 1; $level -le 7; $level++) {
                        $ws.BeginTrans(); $depth++
                        $errorNumber = 0
                        try {
                            if ($operation -eq 'recordset') {
                                $rs.AddNew(); $rs.Fields.Item('Id').Value = 100 + $level; $rs.Update()
                            } else {
                                $flags = 0; if ($operation -eq 'execute128') { $flags = 128 }
                                $db.Execute(('INSERT INTO Items VALUES (' + (100 + $level) + ')'), $flags)
                            }
                        } catch {
                            $errorNumber = $engine.Errors.Item(0).Number
                            if ($null -ne $rs) { try { $rs.CancelUpdate() } catch {} }
                        }
                        $expectedError = 0; if ($level -gt 5) { $expectedError = 3003 }
                        if ($errorNumber -ne $expectedError) { throw "Unexpected $operation at depth $level returned $errorNumber" }
                        Write-Output "$operation;DEPTH=$level;ERROR=$errorNumber"
                    }
                    $errorNumber = 0
                    try { $ws.BeginTrans(); $depth++ } catch { $errorNumber = $engine.Errors.Item(0).Number }
                    if ($errorNumber -ne 3003) { throw "Eighth BeginTrans returned $errorNumber" }
                    Write-Output 'MAX_DEPTH=7'
                } finally {
                    while ($depth -gt 0) { $ws.Rollback(); $depth-- }
                    if ($null -ne $rs) { $rs.Close() }
                }
            }
            Write-Output "FINAL_COUNT=$(Count-Rows)"
            """;
        AccessRoundTripEnvironment.CompactResult native = RunDatabaseScript(format, session.SourcePath, nativeScript, session);
        AssertDaoSuccess(native);
        output.WriteLine(native.StdOut);
        Assert.Contains("CHILD_ROLLBACK=1", native.StdOut, StringComparison.Ordinal);
        Assert.Contains("PARENT_ROLLBACK=0", native.StdOut, StringComparison.Ordinal);
        Assert.Contains("MAX_DEPTH=7", native.StdOut, StringComparison.Ordinal);
        Assert.Contains("FINAL_COUNT=2", native.StdOut, StringComparison.Ordinal);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(writerPath, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await using (JetTransaction outer = await writer.BeginTransactionAsync(Ct))
            {
                await writer.InsertRowAsync("Items", [1], Ct);
                await using (JetTransaction child = await writer.BeginTransactionAsync(Ct))
                {
                    await writer.InsertRowAsync("Items", [2], Ct);
                    await child.RollbackAsync(Ct);
                }

                await using (JetTransaction child = await writer.BeginTransactionAsync(Ct))
                {
                    await writer.InsertRowAsync("Items", [3], Ct);
                    await child.CommitAsync(Ct);
                }

                await outer.RollbackAsync(Ct);
            }

            await using (JetTransaction outer = await writer.BeginTransactionAsync(Ct))
            {
                await writer.InsertRowAsync("Items", [4], Ct);
                await using (JetTransaction child = await writer.BeginTransactionAsync(Ct))
                {
                    await writer.InsertRowAsync("Items", [5], Ct);
                    await child.CommitAsync(Ct);
                }

                await outer.CommitAsync(Ct);
            }
        }

        Assert.Equal(ExpectedIds, await ReadIdsAsync(session.SourcePath));
        Assert.Equal(ExpectedIds, await ReadIdsAsync(writerPath));
        string compactedPath = session.CreateDatabasePath("writer_compacted");
        string compactScript = $"$engine.CompactDatabase({AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(writerPath)}, {AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(compactedPath)})";
        AssertDaoSuccess(RunEngineScript(format, compactScript, session));
        Assert.Equal(ExpectedIds, await ReadIdsAsync(compactedPath));
        const string readScript = """
            $rs = $db.OpenRecordset('SELECT Id FROM Items ORDER BY Id', 4)
            try {
                $ids = @()
                while (-not $rs.EOF) { $ids += $rs.Fields.Item('Id').Value; $rs.MoveNext() }
                Write-Output ('ROWS=' + ($ids -join ','))
            } finally { $rs.Close() }
            """;
        AccessRoundTripEnvironment.CompactResult read = RunDatabaseScript(format, compactedPath, readScript, session);
        AssertDaoSuccess(read);
        Assert.Contains("ROWS=4,5", read.StdOut, StringComparison.Ordinal);
    }
}
