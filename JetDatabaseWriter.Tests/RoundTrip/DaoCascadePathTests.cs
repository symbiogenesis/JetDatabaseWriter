namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Characterizes unequal cascade paths, merged assignments and rollback with native DAO graphs.</summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoCascadePathTests
{
    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(2);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Unequal-depth paths can update separate columns or converge on one terminal column.</summary>
    /// <param name="format">The DAO-authored format.</param>
    /// <param name="overlap">Whether both paths target the same terminal column.</param>
    /// <returns>The asynchronous native comparison.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    public async Task DaoAndWriter_UnequalDepthPaths_HaveMatchingOutcomes(DatabaseFormat format, bool overlap)
    {
        string setup = $$"""
            foreach ($name in @('A','B','C')) {
                $db.Execute("CREATE TABLE [$name] ([Id] LONG CONSTRAINT [PK_$name] PRIMARY KEY)", 128)
                $db.Execute("INSERT INTO [$name] VALUES (1)", 128)
            }
            $db.Execute('CREATE TABLE [D] ([Id] LONG CONSTRAINT [PK_D] PRIMARY KEY, [DirectId] LONG, [IndirectId] LONG)', 128)
            $db.Execute('INSERT INTO [D] VALUES (10,1,1)', 128)
            foreach ($edge in @('A,Id,B,Id', 'B,Id,C,Id', 'C,Id,D,IndirectId', 'A,Id,D,{{(overlap ? "IndirectId" : "DirectId")}}')) {
                $parts = $edge.Split(',')
                $rel = $db.CreateRelation(('FK_' + $parts[2] + '_' + $parts[0]), $parts[0], $parts[2], 256)
                $field = $rel.CreateField($parts[1])
                $field.ForeignName = $parts[3]
                $rel.Fields.Append($field)
                $db.Relations.Append($rel)
            }
            """;
        await using AccessRoundTripSession session = await CreateAsync(format, setup);
        string writerPath = CopyForWriter(session);
        const string update = "$db.Execute('UPDATE [A] SET [Id]=5 WHERE [Id]=1', 128); Write-Output 'UPDATE=ACCEPTED'";
        AccessRoundTripEnvironment.CompactResult native = RunDatabaseScript(session, format, session.SourcePath, update);
        AssertDaoSucceeded(native);
        Assert.Contains("UPDATE=ACCEPTED", native.StdOut, StringComparison.Ordinal);
        string[] tables = ["A", "B", "C", "D"];
        List<string> expected = overlap ? ["A|5", "B|5", "C|5", "D|10|1|5"] : ["A|5", "B|5", "C|5", "D|10|5|5"];
        await AssertReopenAndCompactAsync(session, format, session.SourcePath, tables, expected);

        await using (AccessWriter writer = await OpenWriterAsync(writerPath))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("A", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5 }, Ct));
        }

        await AssertReopenAndCompactAsync(session, format, writerPath, tables, expected);
    }

    /// <summary>An earlier direct path does not hide the conflict between two equal-depth parent tables.</summary>
    /// <param name="format">The DAO-authored format.</param>
    /// <returns>The asynchronous native comparison and direct writer refusal check.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public async Task DaoAndWriter_EqualDepthPathsAfterDirectPath_RefuseWithoutChangingBytes(DatabaseFormat format)
    {
        const string setup = """
            foreach ($name in @('A','B','C','D')) {
                $db.Execute("CREATE TABLE [$name] ([Id] LONG CONSTRAINT [PK_$name] PRIMARY KEY)", 128)
                $db.Execute("INSERT INTO [$name] VALUES (1)", 128)
            }
            foreach ($edge in @('A,D', 'A,B', 'A,C', 'B,D', 'C,D')) {
                $parts = $edge.Split(',')
                $rel = $db.CreateRelation(('FK_' + $parts[1] + '_' + $parts[0]), $parts[0], $parts[1], 256)
                $field = $rel.CreateField('Id')
                $field.ForeignName = 'Id'
                $rel.Fields.Append($field)
                $db.Relations.Append($rel)
            }
            """;
        await using AccessRoundTripSession session = await CreateAsync(format, setup);
        string writerPath = CopyForWriter(session);
        const string update = """
            try { $db.Execute('UPDATE [A] SET [Id]=5 WHERE [Id]=1', 128); Write-Output 'UPDATE=ACCEPTED' }
            catch {
                $codes = @($engine.Errors | ForEach-Object { $_.Number })
                if ($codes.Count -ne 1 -or $codes[0] -ne 3397) { throw }
                Write-Output 'UPDATE=REFUSED:3397'
            }
            """;
        AccessRoundTripEnvironment.CompactResult native = RunDatabaseScript(session, format, session.SourcePath, update);
        AssertDaoSucceeded(native);
        Assert.Contains("UPDATE=REFUSED:3397", native.StdOut, StringComparison.Ordinal);
        string[] tables = ["A", "B", "C", "D"];
        List<string> expected = ["A|1", "B|1", "C|1", "D|1"];
        await AssertReopenAndCompactAsync(session, format, session.SourcePath, tables, expected);
        byte[] before = await File.ReadAllBytesAsync(writerPath, Ct);
        await using (AccessWriter writer = await OpenWriterAsync(writerPath))
        {
            JetConstraintException exception = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                await writer.UpdateRowsAsync("A", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5 }, Ct));
            Assert.Equal(JetErrorCode.ForeignKeyRestrictUpdate, exception.ErrorCode);
        }

        Assert.Equal(before, await File.ReadAllBytesAsync(writerPath, Ct));
        await AssertReopenAndCompactAsync(session, format, writerPath, tables, expected);
    }

    /// <summary>Caller assignments merge with self and downstream cascades, and explicit rollback restores the graph.</summary>
    /// <param name="format">The DAO-authored format.</param>
    /// <param name="rollback">Whether the successful update is rolled back.</param>
    /// <returns>The asynchronous native comparison.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    public async Task DaoAndWriter_MergedCallerAssignments_PreserveRowsAndRollback(DatabaseFormat format, bool rollback)
    {
        const string setup = """
            $db.Execute('CREATE TABLE [Tree] ([Id] LONG CONSTRAINT [PK_Tree] PRIMARY KEY, [ParentId] LONG, [Label] TEXT(20))', 128)
            foreach ($values in @("1,1,'root'", "2,1,'child'", "3,NULL,'other'")) {
                $db.Execute(('INSERT INTO [Tree] VALUES (' + $values + ')'), 128)
            }
            $db.Execute('CREATE TABLE [Leaf] ([Id] LONG CONSTRAINT [PK_Leaf] PRIMARY KEY, [TreeId] LONG)', 128)
            foreach ($values in @('10,1', '11,2', '12,3')) {
                $db.Execute(('INSERT INTO [Leaf] VALUES (' + $values + ')'), 128)
            }
            foreach ($edge in @('Tree,ParentId', 'Leaf,TreeId')) {
                $parts = $edge.Split(',')
                $rel = $db.CreateRelation(('FK_' + $parts[0] + '_Tree'), 'Tree', $parts[0], 256)
                $field = $rel.CreateField('Id')
                $field.ForeignName = $parts[1]
                $rel.Fields.Append($field)
                $db.Relations.Append($rel)
            }
            """;
        await using AccessRoundTripSession session = await CreateAsync(format, setup);
        string writerPath = CopyForWriter(session);
        string[] tables = ["Leaf", "Tree"];
        List<string> beforeRows = ["Leaf|10|1", "Leaf|11|2", "Leaf|12|3", "Tree|1|1|root", "Tree|2|1|child", "Tree|3||other"];
        List<string> updatedRows = ["Leaf|10|5", "Leaf|11|2", "Leaf|12|3", "Tree|2|5|child", "Tree|3||other", "Tree|5|3|changed"];
        string update = $$"""
            $workspace = $engine.Workspaces.Item(0)
            $workspace.BeginTrans()
            try {
                try {
                    $db.Execute('UPDATE [Tree] SET [Id]=5, [ParentId]=1 WHERE [Id]=1', 128)
                    throw 'The explicit old self-key unexpectedly succeeded.'
                } catch {
                    $codes = @($engine.Errors | ForEach-Object { $_.Number })
                    if ($codes.Count -ne 1 -or $codes[0] -ne 3201) { throw }
                    Write-Output 'REFUSAL=3201'
                }
                $db.Execute("UPDATE [Tree] SET [Id]=5, [ParentId]=3, [Label]='changed' WHERE [Id]=1", 128)
                {{ReadDaoRowsScript(tables)}}
                {{(rollback ? "$workspace.Rollback()" : "$workspace.CommitTrans()")}}
            } catch { $workspace.Rollback(); throw }
            """;
        AccessRoundTripEnvironment.CompactResult native = RunDatabaseScript(session, format, session.SourcePath, update);
        AssertDaoSucceeded(native);
        Assert.Contains("REFUSAL=3201", native.StdOut, StringComparison.Ordinal);
        Assert.Equal(updatedRows, ParseDaoRows(native));
        List<string> expected = rollback ? beforeRows : updatedRows;
        await AssertReopenAndCompactAsync(session, format, session.SourcePath, tables, expected);

        byte[] before = await File.ReadAllBytesAsync(writerPath, Ct);
        await using (AccessWriter writer = await OpenWriterAsync(writerPath))
        {
            JetConstraintException exception = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                await writer.UpdateRowsAsync("Tree", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5, ["ParentId"] = 1 }, Ct));
            Assert.Equal(JetErrorCode.ForeignKeyMissingParent, exception.ErrorCode);
        }

        Assert.Equal(before, await File.ReadAllBytesAsync(writerPath, Ct));
        await using (AccessWriter writer = await OpenWriterAsync(writerPath))
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
            Assert.Equal(1, await writer.UpdateRowsAsync("Tree", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5, ["ParentId"] = 3, ["Label"] = "changed" }, Ct));
            if (rollback)
            {
                await transaction.RollbackAsync(Ct);
            }
            else
            {
                await transaction.CommitAsync(Ct);
            }
        }

        if (rollback)
        {
            Assert.Equal(before, await File.ReadAllBytesAsync(writerPath, Ct));
        }

        await AssertReopenAndCompactAsync(session, format, writerPath, tables, expected);
    }

    /// <summary>Contradictory assignments refuse safely even when native DAO leaves one relationship unmatched.</summary>
    /// <param name="format">The DAO-authored format.</param>
    /// <returns>The asynchronous native characterization and writer refusal check.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public async Task DaoAcceptsContradictoryAssignments_WriterRefusesWithoutChangingBytes(DatabaseFormat format)
    {
        const string setup = """
            $db.Execute('CREATE TABLE [P] ([A] LONG CONSTRAINT [PK_P] PRIMARY KEY, [B] LONG)', 128)
            $db.Execute('CREATE UNIQUE INDEX [UX_P_B] ON [P] ([B])', 128)
            $db.Execute('CREATE TABLE [C] ([Id] LONG CONSTRAINT [PK_C] PRIMARY KEY, [ParentId] LONG)', 128)
            $db.Execute('INSERT INTO [P] VALUES (1,1)', 128)
            $db.Execute('INSERT INTO [C] VALUES (10,1)', 128)
            foreach ($name in @('A','B')) {
                $rel = $db.CreateRelation(('FK_C_' + $name), 'P', 'C', 256)
                $field = $rel.CreateField($name)
                $field.ForeignName = 'ParentId'
                $rel.Fields.Append($field)
                $db.Relations.Append($rel)
            }
            """;
        await using AccessRoundTripSession session = await CreateAsync(format, setup);
        string writerPath = CopyForWriter(session);
        const string update = """
            $db.Execute('UPDATE [P] SET [A]=5, [B]=6 WHERE [A]=1', 128)
            Write-Output 'UPDATE=ACCEPTED'
            """;
        AccessRoundTripEnvironment.CompactResult native = RunDatabaseScript(session, format, session.SourcePath, update);
        AssertDaoSucceeded(native);
        Assert.Contains("UPDATE=ACCEPTED", native.StdOut, StringComparison.Ordinal);
        string[] tables = ["C", "P"];
        List<string> nativeRows = await ReadRowsAsync(session.SourcePath, tables);
        Assert.Equal("P|5|6", nativeRows[1]);
        Assert.True(nativeRows[0] is "C|10|5" or "C|10|6", nativeRows[0]);
        await AssertReopenAndCompactAsync(session, format, session.SourcePath, tables, nativeRows);

        // DAO accepts a value that cannot match both parent keys. The writer intentionally
        // chooses the safer policy: refuse contradictory paths before changing any bytes.
        byte[] before = await File.ReadAllBytesAsync(writerPath, Ct);
        await using (AccessWriter writer = await OpenWriterAsync(writerPath))
        {
            JetConstraintException exception = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                await writer.UpdateRowsAsync("P", RowCriteria.Where("A", 1), new RowValues { ["A"] = 5, ["B"] = 6 }, Ct));
            Assert.Equal(JetErrorCode.ForeignKeyRestrictUpdate, exception.ErrorCode);
        }

        Assert.Equal(before, await File.ReadAllBytesAsync(writerPath, Ct));
        await AssertReopenAndCompactAsync(session, format, writerPath, tables, ["C|10|1", "P|1|1"]);
    }

    /// <summary>DAO3.6 verifies separate and converging unequal-depth paths in Access97.</summary>
    /// <param name="overlap">Whether the paths target the same terminal column.</param>
    /// <returns>The asynchronous native comparison.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [InlineData(false)]
    [InlineData(true)]
    public Task Dao36AndWriter_UnequalDepthPaths_HaveMatchingOutcomes(bool overlap) =>
        this.DaoAndWriter_UnequalDepthPaths_HaveMatchingOutcomes(DatabaseFormat.Jet3Mdb, overlap);

    /// <summary>DAO3.6 verifies equal-depth refusal after an earlier direct path in Access97.</summary>
    /// <returns>The asynchronous native comparison.</returns>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    public Task Dao36AndWriter_EqualDepthPathsAfterDirectPath_RefuseWithoutChangingBytes() =>
        this.DaoAndWriter_EqualDepthPathsAfterDirectPath_RefuseWithoutChangingBytes(DatabaseFormat.Jet3Mdb);

    /// <summary>DAO3.6 verifies merged caller assignments and explicit rollback in Access97.</summary>
    /// <param name="rollback">Whether the successful update is rolled back.</param>
    /// <returns>The asynchronous native comparison.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [InlineData(false)]
    [InlineData(true)]
    public Task Dao36AndWriter_MergedCallerAssignments_PreserveRowsAndRollback(bool rollback) =>
        this.DaoAndWriter_MergedCallerAssignments_PreserveRowsAndRollback(DatabaseFormat.Jet3Mdb, rollback);

    /// <summary>DAO3.6 characterizes contradictory assignments while the writer retains its safer refusal.</summary>
    /// <returns>The asynchronous native characterization and writer refusal check.</returns>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    public Task Dao36AcceptsContradictoryAssignments_WriterRefusesWithoutChangingBytes() =>
        this.DaoAcceptsContradictoryAssignments_WriterRefusesWithoutChangingBytes(DatabaseFormat.Jet3Mdb);

    private static async Task<AccessRoundTripSession> CreateAsync(DatabaseFormat format, string setup)
    {
        var session = AccessRoundTripSession.CreateEmpty(databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        try
        {
            string script = $$"""
                $db = $engine.CreateDatabase({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath)}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{NativeVersion(format)}})
                try {
                    {{setup}}
                } finally { $db.Close() }
                """;
            AssertDaoSucceeded(RunEngineScript(session, format, script));
            return session;
        }
        catch
        {
            await session.DisposeAsync();
            throw;
        }
    }

    private static string CopyForWriter(AccessRoundTripSession session)
    {
        string path = session.CreateDatabasePath("writer");
        File.Copy(session.SourcePath, path);
        return path;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(string path) =>
        AccessWriter.OpenAsync(path, new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = false }, Ct);

    private static async Task AssertReopenAndCompactAsync(AccessRoundTripSession session, DatabaseFormat format, string path, string[] tables, List<string> expected)
    {
        Assert.Equal(expected, await ReadRowsAsync(path, tables));
        AccessRoundTripEnvironment.CompactResult reopened = RunDatabaseScript(session, format, path, ReadDaoRowsScript(tables));
        AssertDaoSucceeded(reopened);
        Assert.Equal(expected, ParseDaoRows(reopened));
        string compacted = session.CreateDatabasePath("compacted");
        string script = $$"""
            $engine.CompactDatabase({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(path)}}, {{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(compacted)}})
            $db = $engine.OpenDatabase({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(compacted)}})
            try {
                {{ReadDaoRowsScript(tables)}}
            } finally { $db.Close() }
            """;
        AccessRoundTripEnvironment.CompactResult compact = RunEngineScript(session, format, script);
        AssertDaoSucceeded(compact);
        Assert.Equal(expected, ParseDaoRows(compact));
        Assert.Equal(expected, await ReadRowsAsync(compacted, tables));
    }

    private static int NativeVersion(DatabaseFormat format) => format switch
    {
        DatabaseFormat.Jet3Mdb => 32,
        DatabaseFormat.Jet4Mdb => 64,
        DatabaseFormat.AceAccdb => 128,
        _ => throw new ArgumentOutOfRangeException(nameof(format)),
    };

    private static AccessRoundTripEnvironment.CompactResult RunDatabaseScript(AccessRoundTripSession session, DatabaseFormat format, string path, string script)
    {
        string body = $$"""
            $db = $engine.OpenDatabase({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(path)}})
            try {
                {{script}}
            } finally { $db.Close() }
            """;
        return RunEngineScript(session, format, body);
    }

    private static AccessRoundTripEnvironment.CompactResult RunEngineScript(AccessRoundTripSession session, DatabaseFormat format, string script)
    {
        if (format != DatabaseFormat.Jet3Mdb)
        {
            return session.RunDaoEngineScript(script, DaoTimeout);
        }

        PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, DaoTimeout);
        Assert.True(!result.TimedOut && result.OutputComplete, result.StandardOutput + result.StandardError);
        return new AccessRoundTripEnvironment.CompactResult(result.ExitCode, result.StandardOutput, result.StandardError);
    }

    private static string ReadDaoRowsScript(string[] tables) => $$"""
        foreach ($name in @({{string.Join(",", tables.Select(AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral))}})) {
            $rs = $db.OpenRecordset(('SELECT * FROM [' + $name + ']'), 4)
            try {
                while (-not $rs.EOF) {
                    $values = for ($index = 0; $index -lt $rs.Fields.Count; $index++) { [string]$rs.Fields.Item($index).Value }
                    Write-Output ('ROW=' + $name + '|' + ($values -join '|'))
                    $rs.MoveNext()
                }
            } finally { $rs.Close() }
        }
        """;

    private static List<string> ParseDaoRows(AccessRoundTripEnvironment.CompactResult result) =>
        result.StdOut.Split('\n').Where(line => line.StartsWith("ROW=", StringComparison.Ordinal)).Select(line => line[4..].TrimEnd('\r')).Order(StringComparer.Ordinal).ToList();

    private static async Task<List<string>> ReadRowsAsync(string path, string[] tables)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, cancellationToken: Ct);
        var rows = new List<string>();
        foreach (string table in tables)
        {
            await foreach (object[] row in reader.Rows(table, cancellationToken: Ct))
            {
                rows.Add(table + "|" + string.Join("|", row.Select(value => value is DBNull ? string.Empty : Convert.ToString(value, CultureInfo.InvariantCulture))));
            }
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    private static void AssertDaoSucceeded(AccessRoundTripEnvironment.CompactResult result) =>
        Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
}
