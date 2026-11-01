namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Native-authored physical rows retain their identity through compatible edits and native compaction.</summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class InPlaceRowUpdateDaoTests
{
    private static readonly TimeSpan NativeTimeout = TimeSpan.FromMinutes(2);

    /// <summary>DAO120 reads and compacts the writer's retained Jet4 and ACE row storage.</summary>
    /// <param name="format">The native database generation.</param>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public Task Dao120_NonKeyEdits_RetainNativeRowsAndCompact(DatabaseFormat format) => VerifyAsync(format);

    /// <summary>DAO3.6 reads and compacts the writer's retained Access 97 row storage.</summary>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    public Task Dao36_NonKeyEdits_RetainNativeRowsAndCompact() => VerifyAsync(DatabaseFormat.Jet3Mdb);

    private static async Task VerifyAsync(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string destination = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string version = format switch { DatabaseFormat.Jet3Mdb => ",32", DatabaseFormat.Jet4Mdb => ",64", DatabaseFormat.AceAccdb => string.Empty, _ => throw new ArgumentOutOfRangeException(nameof(format)) };
        string create = $$"""
            $db = $engine.CreateDatabase({{source}}, ';LANGID=0x0409;CP=1252;COUNTRY=0'{{version}})
            try {
                $db.Execute('CREATE TABLE [Parents] ([Id] LONG PRIMARY KEY)', 128)
                $db.Execute('INSERT INTO [Parents] ([Id]) VALUES (1)', 128)
                $db.Execute('CREATE TABLE [Items] ([Id] LONG PRIMARY KEY, [Parent] LONG, [Score] LONG, [Label] TEXT(40), [Spare] LONG, [Note] MEMO, [Enabled] BIT)', 128)
                $relation = $db.CreateRelation('FK', 'Parents', 'Items', 0)
                $field = $relation.CreateField('Id')
                $field.ForeignName = 'Parent'
                $relation.Fields.Append($field)
                $db.Relations.Append($relation)
                $rs = $db.OpenRecordset('Items', 2)
                try {
                    $rs.AddNew()
                    $rs.Fields.Item('Id').Value = 1
                    $rs.Fields.Item('Parent').Value = 1
                    $rs.Fields.Item('Score').Value = 3
                    $rs.Fields.Item('Spare').Value = 5
                    $rs.Fields.Item('Label').Value = 'first'
                    $rs.Fields.Item('Note').Value = 'm' * 12000
                    $rs.Fields.Item('Enabled').Value = $true
                    $rs.Update()
                } finally { $rs.Close() }
            } finally { $db.Close() }
            """;
        AssertNativeSuccess(format, create, session);

        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            ResolvedTable? table = await writer.Services.Snapshots.ResolveTableAsync("Items", ct);
            Assert.NotNull(table);
            LocatedRow before = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct));
            Assert.Equal(1, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 1), new RowValues { ["Score"] = DBNull.Value }, ct));
            Assert.Equal(before.Location, Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct)).Location);
            Assert.Equal(1, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 1), new RowValues { ["Score"] = 9, ["Label"] = "third", ["Spare"] = 7, ["Enabled"] = false }, ct));
            LocatedRow after = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct));
            Assert.Equal(before.Location, after.Location);
            Assert.NotNull(before.LongValues);
            Assert.NotNull(after.LongValues);
            Assert.Equal(before.LongValues[5], after.LongValues[5]);
        }

        string verify = $$"""
            $db = $engine.OpenDatabase({{source}})
            try {
                $rs = $db.OpenRecordset('Items', 4)
                try {
                    if ($rs.Fields.Item('Score').Value -ne 9 -or $rs.Fields.Item('Label').Value -cne 'third' -or $rs.Fields.Item('Spare').Value -ne 7 -or $rs.Fields.Item('Enabled').Value) { throw 'Native scalar readback mismatch.' }
                    if ($rs.Fields.Item('Note').Value -cne ('m' * 12000)) { throw 'Native Memo storage changed.' }
                    $rs.MoveNext()
                    if (-not $rs.EOF) { throw 'Native row cardinality changed.' }
                } finally { $rs.Close() }
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{destination}})
            """;
        AssertNativeSuccess(format, verify, session);
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, ct);
        object[] row = Assert.Single(await reader.Rows("Items", cancellationToken: ct).ToListAsync(ct));
        Assert.Equal(9, row[2]);
        Assert.Equal("third", row[3]);
        Assert.Equal(7, row[4]);
        Assert.Equal(new string('m', 12_000), row[5]);
        Assert.Equal(false, row[6]);
    }

    private static void AssertNativeSuccess(DatabaseFormat format, string script, AccessRoundTripSession session)
    {
        if (format == DatabaseFormat.Jet3Mdb)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, NativeTimeout);
            Assert.False(result.TimedOut);
            Assert.True(result.OutputComplete);
            Assert.True(result.ExitCode == 0, $"DAO3.6 failed: {result.StandardOutput}\n{result.StandardError}");
        }
        else
        {
            AccessRoundTripEnvironment.CompactResult result = AccessRoundTripEnvironment.RunDaoEngineScript(script, session.WorkDir, NativeTimeout);
            Assert.True(result.ExitCode == 0, $"DAO120 failed: {result.StdOut}\n{result.StdErr}");
        }
    }
}
