namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>DAO3.6 independently verifies Access97 cascade cycles and caller rollback.</summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoJet3CascadeCycleTests
{
    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(2);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Two- and three-table cycles update and delete once, and survive rollback and compact.</summary>
    /// <param name="length">The number of tables in the cycle.</param>
    /// <param name="delete">Whether to delete the updated cycle.</param>
    /// <returns>The asynchronous native comparison.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [InlineData(2, false)]
    [InlineData(3, false)]
    [InlineData(2, true)]
    [InlineData(3, true)]
    public async Task Dao36AndWriter_CascadeCycle_MatchThroughRollbackAndCompact(int length, bool delete)
    {
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string setup = $$"""
            $db=$engine.CreateDatabase({{source}},';LANGID=0x0409;CP=1252;COUNTRY=0',32)
            try {
                for($i=0;$i -lt {{length}};$i++) {
                    $name='T'+$i
                    $db.Execute(('CREATE TABLE ['+$name+'] (Id LONG CONSTRAINT [PK_'+$name+'] PRIMARY KEY)'),128)
                    $db.Execute(('INSERT INTO ['+$name+'] VALUES (1)'),128)
                    $db.Execute(('INSERT INTO ['+$name+'] VALUES (2)'),128)
                }
                for($i=0;$i -lt {{length}};$i++) {
                    $next=($i+1)%{{length}}
                    $rel=$db.CreateRelation(('FK_'+$i),'T'+$i,'T'+$next,4352)
                    $field=$rel.CreateField('Id');$field.ForeignName='Id';$rel.Fields.Append($field)
                    $db.Relations.Append($rel)
                }
            } finally {$db.Close()}
            """;
        RunNative(session, setup);
        string writerPath = session.CreateDatabasePath("writer");
        File.Copy(session.SourcePath, writerPath);
        string nativeMutation = $$"""
            $db=$engine.OpenDatabase({{source}})
            try {
                $workspace=$engine.Workspaces.Item(0)
                $workspace.BeginTrans()
                try {
                    $db.Execute('UPDATE T0 SET Id=5 WHERE Id=1',128)
                    {{ReadNativeRows(length, "2,5")}}
                } finally {$workspace.Rollback()}
                {{ReadNativeRows(length, "1,2")}}
                $db.Execute('UPDATE T0 SET Id=5 WHERE Id=1',128)
                {{(delete ? "$db.Execute('DELETE FROM T0 WHERE Id=5',128)" : string.Empty)}}
                {{ReadNativeRows(length, delete ? "2" : "2,5")}}
            } finally {$db.Close()}
            """;
        RunNative(session, nativeMutation);
        byte[] before = await File.ReadAllBytesAsync(writerPath, Ct);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(writerPath, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
            Assert.Equal(1, await writer.UpdateRowsAsync("T0", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5 }, Ct));
            await transaction.RollbackAsync(Ct);
        }

        Assert.Equal(before, await File.ReadAllBytesAsync(writerPath, Ct));
        await using (AccessWriter writer = await AccessWriter.OpenAsync(writerPath, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
            Assert.Equal(1, await writer.UpdateRowsAsync("T0", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5 }, Ct));
            if (delete)
            {
                Assert.Equal(1, await writer.DeleteRowsAsync("T0", RowCriteria.Where("Id", 5), Ct));
            }

            await transaction.CommitAsync(Ct);
        }

        string expected = delete ? "2" : "2,5";
        await AssertRowsAsync(session.SourcePath, length, delete);
        await AssertRowsAsync(writerPath, length, delete);
        string writerLiteral = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(writerPath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string verifyAndCompact = $$"""
            foreach($path in @({{writerLiteral}},{{source}})) {
                $db=$engine.OpenDatabase($path)
                try { {{ReadNativeRows(length, expected)}} } finally {$db.Close()}
            }
            $engine.CompactDatabase({{writerLiteral}},{{compacted}})
            $db=$engine.OpenDatabase({{compacted}})
            try { {{ReadNativeRows(length, expected)}} } finally {$db.Close()}
            """;
        RunNative(session, verifyAndCompact);
        await AssertRowsAsync(session.CompactedPath, length, delete);
    }

    private static string ReadNativeRows(int length, string expected) => $$"""
        for($table=0;$table -lt {{length}};$table++) {
            $rs=$db.OpenRecordset(('SELECT Id FROM [T'+$table+'] ORDER BY Id'),4)
            try {
                $values=@(while(-not $rs.EOF) {$rs.Fields('Id').Value;$rs.MoveNext()})
                if(($values -join ',') -ne '{{expected}}'){throw ('Cycle rows differ in T'+$table+': '+($values -join ','))}
            } finally {$rs.Close()}
        }
        """;

    private static void RunNative(AccessRoundTripSession session, string script)
    {
        PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, DaoTimeout);
        Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, result.StandardOutput + result.StandardError);
    }

    private static async Task AssertRowsAsync(string path, int length, bool delete)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, cancellationToken: Ct);
        for (int table = 0; table < length; table++)
        {
            int count = 0;
            await foreach (object[] row in reader.Rows("T" + table, cancellationToken: Ct))
            {
                int id = Assert.IsType<int>(row[0]);
                Assert.True(id == 2 || (!delete && id == 5));
                count++;
            }

            Assert.Equal(delete ? 1 : 2, count);
        }
    }
}
