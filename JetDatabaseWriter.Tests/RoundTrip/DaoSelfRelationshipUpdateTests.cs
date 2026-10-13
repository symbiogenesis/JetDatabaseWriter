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
using Xunit;

/// <summary>Compares simultaneous self-referencing key assignments with native DAO updates.</summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoSelfRelationshipUpdateTests
{
    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(2);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets scalar self-relationship update cases for ACE and Jet4.</summary>
    /// <returns>The format, cascade policy, presence of another dependent row and assigned foreign key.</returns>
    public static TheoryData<DatabaseFormat, bool, bool, string> SelfAssignments()
    {
        var data = new TheoryData<DatabaseFormat, bool, bool, string>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.AceAccdb, DatabaseFormat.Jet4Mdb })
        {
            foreach (bool cascade in new[] { false, true })
            {
                foreach (bool dependent in new[] { false, true })
                {
                    foreach (string assignment in new[] { "unchanged", "1", "3", "5", "9", "NULL" })
                    {
                        data.Add(format, cascade, dependent, assignment);
                    }
                }
            }
        }

        return data;
    }

    /// <summary>The writer preserves the native final rows or refuses without changing bytes.</summary>
    /// <param name="format">The native database format.</param>
    /// <param name="cascade">Whether the self-relationship cascades updates.</param>
    /// <param name="dependent">Whether another row references the moving primary key.</param>
    /// <param name="assignment">The caller's simultaneous foreign-key assignment.</param>
    /// <returns>The asynchronous comparison.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [MemberData(nameof(SelfAssignments))]
    public async Task DaoAndWriter_SimultaneousSelfKeyUpdate_HaveMatchingOutcomes(DatabaseFormat format, bool cascade, bool dependent, string assignment)
    {
        ArgumentNullException.ThrowIfNull(assignment);
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: format == DatabaseFormat.Jet4Mdb ? ".mdb" : ".accdb");
        string setup = $$"""
            $db = $engine.CreateDatabase({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath)}}, ';LANGID=0x0409;CP=1252;COUNTRY=0'{{(format == DatabaseFormat.Jet4Mdb ? ", 64" : string.Empty)}})
            try {
                $db.Execute('CREATE TABLE [Tree] ([Id] LONG CONSTRAINT [PK_Tree] PRIMARY KEY, [ParentId] LONG)', 128)
                $db.Execute('INSERT INTO [Tree] VALUES (1,1)', 128)
                $db.Execute('INSERT INTO [Tree] VALUES (3,NULL)', 128)
                {{(dependent ? "$db.Execute('INSERT INTO [Tree] VALUES (2,1)', 128)" : string.Empty)}}
                $rel = $db.CreateRelation('FK_Tree_Self', 'Tree', 'Tree', {{(cascade ? 256 : 0)}})
                $field = $rel.CreateField('Id')
                $field.ForeignName = 'ParentId'
                $rel.Fields.Append($field)
                $db.Relations.Append($rel)
            } finally { $db.Close() }
            """;
        AssertDaoSucceeded(session.RunDaoEngineScript(setup, DaoTimeout));
        string writerPath = session.CreateDatabasePath("writer");
        File.Copy(session.SourcePath, writerPath);
        string assignedSql = assignment == "unchanged" ? string.Empty : ", [ParentId]=" + assignment;
        string sql = "UPDATE [Tree] SET [Id]=5" + assignedSql + " WHERE [Id]=1";
        string updateScript = $$"""
            try { $db.Execute({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(sql)}}, 128); Write-Output 'UPDATE=ACCEPTED' }
            catch {
                $codes = @($engine.Errors | ForEach-Object { $_.Number })
                if ($codes.Count -ne 1 -or $codes[0] -notin @(3200, 3201)) { throw }
                Write-Output ('UPDATE=REFUSED:' + $codes[0])
            }
            """;
        AccessRoundTripEnvironment.CompactResult native = session.RunDaoDatabaseScript(session.SourcePath, updateScript, DaoTimeout);
        AssertDaoSucceeded(native);
        bool accepted = (cascade || !dependent) && assignment is not "1" and not "9" && (cascade || assignment != "unchanged");
        int refusalCode = !cascade && (dependent || assignment == "unchanged") ? 3200 : 3201;
        Assert.Contains(accepted ? "UPDATE=ACCEPTED" : "UPDATE=REFUSED:" + refusalCode.ToString(CultureInfo.InvariantCulture), native.StdOut, StringComparison.Ordinal);
        List<string> expected = await ReadRowsAsync(session.SourcePath);
        var expectedRows = new List<string> { "3|" };
        string finalParent = assignment switch
        {
            "NULL" => string.Empty,
            "3" => "3",
            _ => "5",
        };
        expectedRows.Add(accepted ? "5|" + finalParent : "1|1");
        if (dependent)
        {
            expectedRows.Add(accepted ? "2|5" : "2|1");
        }

        expectedRows.Sort(StringComparer.Ordinal);
        Assert.Equal(expectedRows, expected);
        byte[] before = await File.ReadAllBytesAsync(writerPath, Ct);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(writerPath, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            var values = new RowValues { ["Id"] = 5 };
            if (assignment != "unchanged")
            {
                values["ParentId"] = assignment == "NULL" ? null : int.Parse(assignment, CultureInfo.InvariantCulture);
            }

            if (accepted)
            {
                Assert.Equal(1, await writer.UpdateRowsAsync("Tree", RowCriteria.Where("Id", 1), values, Ct));
            }
            else
            {
                _ = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.UpdateRowsAsync("Tree", RowCriteria.Where("Id", 1), values, Ct));
            }
        }

        if (!accepted)
        {
            Assert.Equal(before, await File.ReadAllBytesAsync(writerPath, Ct));
        }

        Assert.Equal(expected, await ReadRowsAsync(writerPath));
        AssertDaoRows(session, writerPath, expected);
        string compacted = session.CreateDatabasePath("writer_compacted");
        AssertDaoSucceeded(AccessRoundTripEnvironment.RunDaoCompact(writerPath, compacted, DaoTimeout));
        Assert.Equal(expected, await ReadRowsAsync(compacted));
        AssertDaoRows(session, compacted, expected);
    }

    /// <summary>Composite self-updates preserve explicit caller keys, including references to another moved row.</summary>
    /// <param name="format">The native database format.</param>
    /// <param name="assignment">The foreign-key assignment, or unchanged for no assignment.</param>
    /// <returns>The asynchronous comparison.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.AceAccdb, "unchanged")]
    [InlineData(DatabaseFormat.AceAccdb, "ParentPart=1")]
    [InlineData(DatabaseFormat.AceAccdb, "ParentId=1")]
    [InlineData(DatabaseFormat.AceAccdb, "ParentId=5")]
    [InlineData(DatabaseFormat.AceAccdb, "ParentId=3")]
    [InlineData(DatabaseFormat.Jet4Mdb, "unchanged")]
    [InlineData(DatabaseFormat.Jet4Mdb, "ParentPart=1")]
    [InlineData(DatabaseFormat.Jet4Mdb, "ParentId=1")]
    [InlineData(DatabaseFormat.Jet4Mdb, "ParentId=5")]
    [InlineData(DatabaseFormat.Jet4Mdb, "ParentId=3")]
    public async Task DaoAndWriter_CompositeSelfKeyBatchUpdate_HaveMatchingOutcomes(DatabaseFormat format, string assignment)
    {
        ArgumentNullException.ThrowIfNull(assignment);
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: format == DatabaseFormat.Jet4Mdb ? ".mdb" : ".accdb");
        string setup = $$"""
            $db = $engine.CreateDatabase({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath)}}, ';LANGID=0x0409;CP=1252;COUNTRY=0'{{(format == DatabaseFormat.Jet4Mdb ? ", 64" : string.Empty)}})
            try {
                $db.Execute('CREATE TABLE [Tree] ([Id] LONG, [Part] LONG, [ParentId] LONG, [ParentPart] LONG, CONSTRAINT [PK_Tree] PRIMARY KEY ([Id], [Part]))', 128)
                foreach ($row in @('1,1,1,1', '1,2,1,1', '2,1,1,1', '3,1,NULL,NULL')) {
                    $db.Execute(('INSERT INTO [Tree] VALUES (' + $row + ')'), 128)
                }
                $rel = $db.CreateRelation('FK_Tree_Self', 'Tree', 'Tree', 256)
                foreach ($name in @('Id', 'Part')) {
                    $field = $rel.CreateField($name)
                    $field.ForeignName = 'Parent' + $name
                    $rel.Fields.Append($field)
                }
                $db.Relations.Append($rel)
            } finally { $db.Close() }
            """;
        AssertDaoSucceeded(session.RunDaoEngineScript(setup, DaoTimeout));
        string writerPath = session.CreateDatabasePath("writer");
        File.Copy(session.SourcePath, writerPath);
        string assignedSql = assignment == "unchanged" ? string.Empty : ", " + assignment;
        string sql = "UPDATE [Tree] SET [Id]=5" + assignedSql + " WHERE [Id]=1";
        string updateScript = $$"""
            try { $db.Execute({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(sql)}}, 128); Write-Output 'UPDATE=ACCEPTED' }
            catch {
                $codes = @($engine.Errors | ForEach-Object { $_.Number })
                if ($codes.Count -ne 1 -or $codes[0] -ne 3201) { throw }
                Write-Output 'UPDATE=REFUSED:3201'
            }
            """;
        AccessRoundTripEnvironment.CompactResult native = session.RunDaoDatabaseScript(session.SourcePath, updateScript, DaoTimeout);
        AssertDaoSucceeded(native);
        bool accepted = assignment is not "ParentPart=1" and not "ParentId=1";
        Assert.Contains(accepted ? "UPDATE=ACCEPTED" : "UPDATE=REFUSED:3201", native.StdOut, StringComparison.Ordinal);
        List<string> expected = await ReadRowsAsync(session.SourcePath);
        int finalParent = assignment == "ParentId=3" ? 3 : 5;
        Assert.Equal(
            accepted ? ["2|1|5|1", "3|1||", $"5|1|{finalParent}|1", $"5|2|{finalParent}|1"] : ["1|1|1|1", "1|2|1|1", "2|1|1|1", "3|1||"],
            expected);
        byte[] before = await File.ReadAllBytesAsync(writerPath, Ct);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(writerPath, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            var values = new RowValues { ["Id"] = 5 };
            if (assignment != "unchanged")
            {
                string[] parts = assignment.Split('=');
                values[parts[0]] = int.Parse(parts[1], CultureInfo.InvariantCulture);
            }

            if (accepted)
            {
                Assert.Equal(2, await writer.UpdateRowsAsync("Tree", RowCriteria.Where("Id", 1), values, Ct));
            }
            else
            {
                JetConstraintException exception = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.UpdateRowsAsync("Tree", RowCriteria.Where("Id", 1), values, Ct));
                Assert.Equal(JetErrorCode.ForeignKeyMissingParent, exception.ErrorCode);
            }
        }

        if (!accepted)
        {
            Assert.Equal(before, await File.ReadAllBytesAsync(writerPath, Ct));
        }

        Assert.Equal(expected, await ReadRowsAsync(writerPath));
        AssertDaoRows(session, writerPath, expected);
        string compacted = session.CreateDatabasePath("writer_compacted");
        AssertDaoSucceeded(AccessRoundTripEnvironment.RunDaoCompact(writerPath, compacted, DaoTimeout));
        Assert.Equal(expected, await ReadRowsAsync(compacted));
        AssertDaoRows(session, compacted, expected);
    }

    private static void AssertDaoSucceeded(AccessRoundTripEnvironment.CompactResult result) =>
        Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);

    private static void AssertDaoRows(AccessRoundTripSession session, string path, List<string> expected)
    {
        const string readScript = """
            $rs = $db.OpenRecordset('SELECT * FROM [Tree] ORDER BY [Id]', 4)
            try {
                while (-not $rs.EOF) {
                    $values = for ($index = 0; $index -lt $rs.Fields.Count; $index++) { [string]$rs.Fields.Item($index).Value }
                    Write-Output ('ROW=' + ($values -join '|'))
                    $rs.MoveNext()
                }
            } finally { $rs.Close() }
            """;
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScript(path, readScript, DaoTimeout);
        AssertDaoSucceeded(result);
        Assert.Equal(expected.Count, result.StdOut.Split('\n').Count(line => line.StartsWith("ROW=", StringComparison.Ordinal)));
        foreach (string row in expected)
        {
            Assert.Contains("ROW=" + row, result.StdOut, StringComparison.Ordinal);
        }
    }

    private static async Task<List<string>> ReadRowsAsync(string path)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, cancellationToken: Ct);
        var rows = new List<string>();
        await foreach (object[] row in reader.Rows("Tree", cancellationToken: Ct))
        {
            rows.Add(string.Join("|", row.Select(value => value is DBNull ? string.Empty : Convert.ToString(value, CultureInfo.InvariantCulture))));
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }
}
