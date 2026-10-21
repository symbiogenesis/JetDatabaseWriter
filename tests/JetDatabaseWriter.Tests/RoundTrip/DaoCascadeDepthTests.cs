namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Separates native update resource limits from the writer's bounded cascade planner.</summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoCascadeDepthTests
{
    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(2);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>DAO120 accepts fifty update edges and refuses fifty-one, in both modern formats.</summary>
    /// <param name="depth">The number of relationship edges.</param>
    /// <param name="mdb">Whether to create a Jet4 MDB instead of an ACE database.</param>
    /// <returns>The native boundary check.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(50, false)]
    [InlineData(51, false)]
    [InlineData(50, true)]
    [InlineData(51, true)]
    public Task Dao120_UpdateDepthBoundary_IsAtomicThroughRollbackAndCompact(int depth, bool mdb) =>
        AssertNativeBoundaryAsync(mdb ? DatabaseFormat.Jet4Mdb : DatabaseFormat.AceAccdb, depth);

    /// <summary>DAO3.6 has the same observed fifty-edge update boundary on Access97 files.</summary>
    /// <param name="depth">The number of relationship edges.</param>
    /// <returns>The native boundary check.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [InlineData(50)]
    [InlineData(51)]
    public Task Dao36_UpdateDepthBoundary_IsAtomicThroughRollbackAndCompact(int depth) =>
        AssertNativeBoundaryAsync(DatabaseFormat.Jet3Mdb, depth);

    /// <summary>The writer allows sixty-four edges, refuses sixty-five before mutation, and rolls back accepted work.</summary>
    /// <param name="delete">Whether to cascade a delete instead of an update.</param>
    /// <param name="mode">The statement or caller transaction mode.</param>
    /// <returns>The comparison with independently authored and reopened DAO databases.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(false, WriteMode.Direct)]
    [InlineData(false, WriteMode.AutoCommit)]
    [InlineData(false, WriteMode.ExplicitCommit)]
    [InlineData(true, WriteMode.Direct)]
    [InlineData(true, WriteMode.AutoCommit)]
    [InlineData(true, WriteMode.ExplicitCommit)]
    public async Task Writer_DepthSafetyBound_PreservesBytesAndNativeReadability(bool delete, WriteMode mode)
    {
        foreach (int depth in new[] { 64, 65 })
        {
            await using var session = AccessRoundTripSession.CreateEmpty();
            CreateNativeChain(session, DatabaseFormat.AceAccdb, depth);
            byte[] before = await File.ReadAllBytesAsync(session.SourcePath, Ct);
            if (depth == 64)
            {
                await using (AccessWriter writer = await OpenWriterAsync(session.SourcePath, mode))
                {
                    await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
                    Assert.Equal(1, await MutateAsync(writer, delete));
                    await transaction.RollbackAsync(Ct);
                }

                Assert.Equal(before, await File.ReadAllBytesAsync(session.SourcePath, Ct));
            }

            await using (AccessWriter writer = await OpenWriterAsync(session.SourcePath, mode))
            {
                JetTransaction? transaction = mode == WriteMode.ExplicitCommit ? await writer.BeginTransactionAsync(Ct) : null;
                try
                {
                    if (depth == 65)
                    {
                        JetConstraintException exception = await Assert.ThrowsAsync<JetConstraintException>(async () => await MutateAsync(writer, delete));
                        Assert.Equal(JetErrorCode.CascadeDepthExceeded, exception.ErrorCode);
                    }
                    else
                    {
                        Assert.Equal(1, await MutateAsync(writer, delete));
                    }

                    if (transaction is not null)
                    {
                        await transaction.CommitAsync(Ct);
                    }
                }
                finally
                {
                    if (transaction is not null)
                    {
                        await transaction.DisposeAsync();
                    }
                }
            }

            if (depth == 65)
            {
                Assert.Equal(before, await File.ReadAllBytesAsync(session.SourcePath, Ct));
            }

            string expected = delete ? "2" : "2,5";
            if (depth == 65)
            {
                expected = "1,2";
            }

            await AssertReaderRowsAsync(session.SourcePath, depth, expected);
            AssertNativeReopenAndCompact(session, DatabaseFormat.AceAccdb, depth, expected);
            await AssertReaderRowsAsync(session.CompactedPath, depth, expected);
        }
    }

    /// <summary>DAO120 cascades deletes beyond the writer's safety bound.</summary>
    /// <param name="mdb">Whether to create a Jet4 MDB instead of an ACE database.</param>
    /// <returns>The rollback, commit and native compact check.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(false)]
    [InlineData(true)]
    public Task Dao120_DeleteSixtyFiveEdges_RollsBackAndCompacts(bool mdb) =>
        AssertNativeDeleteAsync(mdb ? DatabaseFormat.Jet4Mdb : DatabaseFormat.AceAccdb);

    /// <summary>DAO3.6 also cascades deletes beyond the writer's safety bound.</summary>
    /// <returns>The rollback, commit and native compact check.</returns>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    public Task Dao36_DeleteSixtyFiveEdges_RollsBackAndCompacts() =>
        AssertNativeDeleteAsync(DatabaseFormat.Jet3Mdb);

    private static async Task AssertNativeDeleteAsync(DatabaseFormat format)
    {
        const int depth = 65;
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        CreateNativeChain(session, format, depth);
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string script = $$"""
            $db=$engine.OpenDatabase({{source}})
            try {
                $workspace=$engine.Workspaces.Item(0)
                $workspace.BeginTrans()
                try {
                    $db.Execute('DELETE FROM T0 WHERE Id=1',128)
                    {{ReadNativeRows(depth, "2")}}
                } finally {$workspace.Rollback()}
                {{ReadNativeRows(depth, "1,2")}}
                $db.Execute('DELETE FROM T0 WHERE Id=1',128)
                {{ReadNativeRows(depth, "2")}}
            } finally {$db.Close()}
            """;
        RunNative(session, format, script);
        await AssertReaderRowsAsync(session.SourcePath, depth, "2");
        AssertNativeReopenAndCompact(session, format, depth, "2");
        await AssertReaderRowsAsync(session.CompactedPath, depth, "2");
    }

    private static async Task AssertNativeBoundaryAsync(DatabaseFormat format, int depth)
    {
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        CreateNativeChain(session, format, depth);
        string mutation = depth == 50
            ? """
                $db.Execute('UPDATE T0 SET Id=5 WHERE Id=1',128)
                """
            : """
                $refused=$false
                try {$db.Execute('UPDATE T0 SET Id=5 WHERE Id=1',128)}
                catch {
                    foreach($errorRecord in $engine.Errors) {
                        if($errorRecord.Number -eq 3000 -and $errorRecord.Description -match '-1623') {$refused=$true}
                    }
                    if(-not $refused){throw}
                }
                if(-not $refused){throw 'Expected native update-depth refusal'}
                """;
        string expected = depth == 50 ? "2,5" : "1,2";
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string script = $$"""
            $db=$engine.OpenDatabase({{source}})
            try {
                $workspace=$engine.Workspaces.Item(0)
                $workspace.BeginTrans()
                try {
                    {{mutation}}
                    {{ReadNativeRows(depth, expected)}}
                } finally {$workspace.Rollback()}
                {{ReadNativeRows(depth, "1,2")}}
                {{mutation}}
                {{ReadNativeRows(depth, expected)}}
            } finally {$db.Close()}
            """;
        RunNative(session, format, script);
        await AssertReaderRowsAsync(session.SourcePath, depth, expected);
        AssertNativeReopenAndCompact(session, format, depth, expected);
        await AssertReaderRowsAsync(session.CompactedPath, depth, expected);
    }

    private static void CreateNativeChain(AccessRoundTripSession session, DatabaseFormat format, int depth)
    {
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        int version = format switch
        {
            DatabaseFormat.Jet3Mdb => 32,
            DatabaseFormat.Jet4Mdb => 64,
            DatabaseFormat.AceAccdb => 128,
            _ => throw new ArgumentOutOfRangeException(nameof(format)),
        };
        string script = $$"""
            $db=$engine.CreateDatabase({{source}},';LANGID=0x0409;CP=1252;COUNTRY=0',{{version}})
            try {
                for($i=0;$i -le {{depth}};$i++) {
                    $name='T'+$i
                    $db.Execute(('CREATE TABLE ['+$name+'] (Id LONG CONSTRAINT [PK_'+$name+'] PRIMARY KEY)'),128)
                    $db.Execute(('INSERT INTO ['+$name+'] VALUES (1)'),128)
                    $db.Execute(('INSERT INTO ['+$name+'] VALUES (2)'),128)
                }
                for($i=0;$i -lt {{depth}};$i++) {
                    $relation=$db.CreateRelation(('FK_'+$i),'T'+$i,'T'+($i+1),4352)
                    $field=$relation.CreateField('Id');$field.ForeignName='Id';$relation.Fields.Append($field)
                    $db.Relations.Append($relation)
                }
            } finally {$db.Close()}
            """;
        RunNative(session, format, script);
    }

    private static void AssertNativeReopenAndCompact(AccessRoundTripSession session, DatabaseFormat format, int depth, string expected)
    {
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string script = $$"""
            $db=$engine.OpenDatabase({{source}})
            try { {{ReadNativeRows(depth, expected)}} } finally {$db.Close()}
            $engine.CompactDatabase({{source}},{{compacted}})
            $db=$engine.OpenDatabase({{compacted}})
            try { {{ReadNativeRows(depth, expected)}} } finally {$db.Close()}
            """;
        RunNative(session, format, script);
    }

    private static string ReadNativeRows(int depth, string expected) => $$"""
        for($table=0;$table -le {{depth}};$table++) {
            $recordset=$db.OpenRecordset(('SELECT Id FROM [T'+$table+'] ORDER BY Id'),4)
            try {
                $values=@(while(-not $recordset.EOF) {$recordset.Fields('Id').Value;$recordset.MoveNext()})
                if(($values -join ',') -ne '{{expected}}'){throw ('Cascade rows differ in T'+$table+': '+($values -join ','))}
            } finally {$recordset.Close()}
        }
        """;

    private static void RunNative(AccessRoundTripSession session, DatabaseFormat format, string script)
    {
        if (format == DatabaseFormat.Jet3Mdb)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, DaoTimeout);
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, result.StandardOutput + result.StandardError);
        }
        else
        {
            AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, DaoTimeout);
            Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
        }
    }

    private static async ValueTask<AccessWriter> OpenWriterAsync(string path, WriteMode mode) =>
        await AccessWriter.OpenAsync(path, new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = mode == WriteMode.AutoCommit }, Ct);

    private static async Task<int> MutateAsync(AccessWriter writer, bool delete) =>
        delete
            ? await writer.DeleteRowsAsync("T0", RowCriteria.Where("Id", 1), Ct)
            : await writer.UpdateRowsAsync("T0", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5 }, Ct);

    private static async Task AssertReaderRowsAsync(string path, int depth, string expected)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, cancellationToken: Ct);
        for (int table = 0; table <= depth; table++)
        {
            var values = new List<int>();
            await foreach (object[] row in reader.Rows("T" + table, cancellationToken: Ct))
            {
                values.Add(Assert.IsType<int>(row[0]));
            }

            values.Sort();
            Assert.Equal(expected, string.Join(",", values));
        }
    }
}
