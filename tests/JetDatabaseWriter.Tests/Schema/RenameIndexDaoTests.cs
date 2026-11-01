namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Native index renames and managed metadata edits retain keys and relationship identities.</summary>
public sealed class RenameIndexDaoTests
{
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task NativeNamesAndRelationships_SurviveRenameMutationAndCompact(DatabaseFormat format)
        => await VerifyNativeIndexRenameAsync(format);

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Jet3NativeNamesAndRelationships_SurviveRenameMutationAndCompact()
        => await VerifyNativeIndexRenameAsync(DatabaseFormat.Jet3Mdb);

    private static async Task VerifyNativeIndexRenameAsync(DatabaseFormat format)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.RenameIndex", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string controlPath = Path.Combine(session.WorkDir, "control" + Path.GetExtension(session.SourcePath));
        string control = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(controlPath);
        string controlCompact = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(Path.Combine(session.WorkDir, "control-compact" + Path.GetExtension(session.SourcePath)));
        string longName = "A" + new string('x', 39);
        string lowerName = "a" + new string('x', 39);
        int version = format switch { DatabaseFormat.Jet3Mdb => 32, DatabaseFormat.Jet4Mdb => 64, DatabaseFormat.AceAccdb => 128, _ => throw new ArgumentOutOfRangeException(nameof(format)) };
        RunNativeScript(
            format,
            session,
            $$"""
            $db = $engine.CreateDatabase({{source}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            try {
                $extra = ''
                for ($i = 0; $i -lt 75; $i++) { $extra += ', [Column' + $i + ('x' * 32) + '] LONG' }
                $db.Execute(('CREATE TABLE [Items] ([Id] LONG, [Value] LONG, [Notes] MEMO' + $extra + ')'), 128)
                $db.Execute('CREATE UNIQUE INDEX [ItemsPK] ON [Items] ([Id]) WITH PRIMARY', 128)
                $db.Execute('CREATE UNIQUE INDEX [Zulu] ON [Items] ([Value])', 128)
                $db.Execute('CREATE INDEX [Middle] ON [Items] ([Id] DESC, [Value])', 128)
                $db.TableDefs('Items').Indexes('Zulu').Properties.Append($db.CreateProperty('Description', 10, 'Index description to preserve'))
                $db.Execute('INSERT INTO [Items] ([Id],[Value]) VALUES (1,7)', 128)
                $db.Execute('INSERT INTO [Items] ([Id],[Value]) VALUES (2,8)', 128)
                $rows = $db.OpenRecordset('Items', 1)
                try {
                    $rows.Index = 'ItemsPK'
                    $rows.Seek('=', 1)
                    $rows.Edit()
                    $rows.Fields('Notes').Value = ('n' * 5000)
                    $rows.Update()
                } finally { $rows.Close() }
                $db.Execute('CREATE TABLE [Parents] ([Id] LONG, [Label] TEXT(30))', 128)
                $db.Execute('CREATE UNIQUE INDEX [ParentPK] ON [Parents] ([Id]) WITH PRIMARY', 128)
                $db.Execute('CREATE TABLE [Children] ([ParentId] LONG)', 128)
                $db.Execute('ALTER TABLE [Children] ADD CONSTRAINT [FK_Children_Parents] FOREIGN KEY ([ParentId]) REFERENCES [Parents] ([Id])', 128)
                $db.Execute("INSERT INTO [Parents] ([Id],[Label]) VALUES (1,'one')", 128)
                $db.Execute('INSERT INTO [Children] ([ParentId]) VALUES (1)', 128)
            } finally { $db.Close() }
            [System.IO.File]::Copy({{source}}, {{control}})
            $db = $engine.OpenDatabase({{control}}, $false, $false)
            try {
                $db.TableDefs('Items').Indexes('Zulu').Name = '{{longName}}'
                $db.TableDefs('Items').Indexes.Refresh()
                $db.TableDefs('Items').Indexes('{{longName}}').Name = '{{lowerName}}'
                $db.TableDefs('Parents').Indexes('ParentPK').Name = 'ParentIdentity'
                $db.TableDefs('Parents').Indexes.Refresh()
                $db.TableDefs('Parents').Indexes('ParentIdentity').Name = 'parentidentity'
            } finally { $db.Close() }
            Write-Output 'NATIVE_RENAME_CONTROL_OK'
            """,
            "NATIVE_RENAME_CONTROL_OK");

        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            ResolvedTable items = await writer.Services.Catalog.ResolveRequiredTableAsync("Items", TestContext.Current.CancellationToken);
            TDefImage before = Assert.IsType<TDefImage>(await writer.Database.TableDefs.ReadImageAsync(items.Entry.TDefPage, TestContext.Current.CancellationToken));
            Assert.True((await writer.Database.TableDefs.ReadTDefChainAsync(items.Entry.TDefPage, TestContext.Current.CancellationToken)).PageNumbers.Count > 1);
            await writer.RenameIndexAsync("Items", "zulu", longName, TestContext.Current.CancellationToken);
            await writer.RenameIndexAsync("Items", longName, lowerName, TestContext.Current.CancellationToken);
            await writer.RenameIndexAsync("Parents", "ParentPK", "ParentIdentity", TestContext.Current.CancellationToken);
            await writer.RenameIndexAsync("Parents", "ParentIdentity", "parentidentity", TestContext.Current.CancellationToken);
            TDefImage after = Assert.IsType<TDefImage>(await writer.Database.TableDefs.ReadImageAsync(items.Entry.TDefPage, TestContext.Current.CancellationToken));
            Assert.Equal(before.RealIndexes.Select(index => index.RootPage), after.RealIndexes.Select(index => index.RootPage));
        }

        await using (AccessReader subject = await AccessReader.OpenAsync(session.SourcePath, new() { UseLockFile = false }, TestContext.Current.CancellationToken))
        await using (AccessReader oracle = await AccessReader.OpenAsync(controlPath, new() { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            foreach (string table in new[] { "Items", "Parents" })
            {
                ResolvedTable expectedTable = await oracle.Services.TableCatalog.ResolveRequiredTableAsync(table, TestContext.Current.CancellationToken);
                ResolvedTable actualTable = await subject.Services.TableCatalog.ResolveRequiredTableAsync(table, TestContext.Current.CancellationToken);
                TDefImage expected = Assert.IsType<TDefImage>(await oracle.Database.TableDefs.ReadImageAsync(expectedTable.Entry.TDefPage, TestContext.Current.CancellationToken));
                TDefImage actual = Assert.IsType<TDefImage>(await subject.Database.TableDefs.ReadImageAsync(actualTable.Entry.TDefPage, TestContext.Current.CancellationToken));
                Assert.Equal(expected.LogicalIndexes.Select(index => index.Name), actual.LogicalIndexes.Select(index => index.Name));
                foreach (TDefLogicalIndexDescriptor index in expected.LogicalIndexes)
                {
                    Assert.Equal(index.RawDescriptor, Assert.Single(actual.LogicalIndexes, candidate => candidate.Name == index.Name).RawDescriptor);
                }
            }
        }

        await MutateAsync(session.SourcePath);

        RunNativeScript(
            format,
            session,
            $$"""
            function Assert-Names($db) {
                $indexes = $db.TableDefs('Items').Indexes
                if ($indexes.Count -ne 3) { throw 'Wrong Items index count.' }
                if ($indexes.Item('{{lowerName}}').Name -cne '{{lowerName}}' -or !$indexes.Item('{{lowerName}}').Unique) { throw 'Wrong unique index spelling or flags.' }
                if ([string]$indexes.Item('{{lowerName}}').Properties.Item('Description').Value -cne 'Index description to preserve') { throw 'Renamed index lost its description.' }
                $index = $db.TableDefs('Parents').Indexes('parentidentity')
                if ($index.Name -cne 'parentidentity' -or !$index.Primary) { throw 'Wrong primary-key spelling or flags.' }
            }
            $db = $engine.OpenDatabase({{source}}, $false, $false)
            try {
                Assert-Names $db
                $rows = $db.OpenRecordset('Items', 1)
                try {
                    $rows.Index = '{{lowerName}}'
                    $rows.Seek('=', 7)
                    if ($rows.NoMatch -or [int]$rows.Fields('Id').Value -ne 1 -or [string]$rows.Fields('Notes').Value -cne ('n' * 5000)) { throw 'Renamed unique seek lost a row or its Memo.' }
                } finally { $rows.Close() }
                $rows = $db.OpenRecordset('Parents', 1)
                try {
                    $rows.Index = 'parentidentity'
                    $rows.Seek('=', 2)
                    if ($rows.NoMatch -or [string]$rows.Fields('Label').Value -cne 'managed') { throw 'Renamed primary seek lost its row.' }
                } finally { $rows.Close() }
                foreach ($sql in @('INSERT INTO [Items] ([Id],[Value]) VALUES (4,7)', "INSERT INTO [Parents] ([Id],[Label]) VALUES (1,'duplicate')", 'INSERT INTO [Children] ([ParentId]) VALUES (999)', 'DELETE FROM [Parents] WHERE [Id]=1')) {
                    $refused = $false
                    try { $db.Execute($sql, 128) } catch { $refused = $true }
                    if (!$refused) { throw ('Renamed constraint did not refuse: ' + $sql) }
                }
                $db.Execute("INSERT INTO [Items] ([Id],[Value],[Notes]) VALUES (4,10,'native')", 128)
                $db.Execute('UPDATE [Items] SET [Value]=11 WHERE [Id]=3', 128)
                $db.Execute("UPDATE [Parents] SET [Label]='native' WHERE [Id]=2", 128)
                $db.Execute('INSERT INTO [Children] ([ParentId]) VALUES (1)', 128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{compacted}})
            $engine.CompactDatabase({{control}}, {{controlCompact}})
            foreach ($path in @({{compacted}}, {{controlCompact}})) {
                $db = $engine.OpenDatabase($path, $false, $true)
                try { Assert-Names $db } finally { $db.Close() }
            }
            Write-Output 'NATIVE_RENAME_COMPACT_OK'
            """,
            "NATIVE_RENAME_COMPACT_OK");

        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new() { UseLockFile = false }, TestContext.Current.CancellationToken);
        Assert.Equal(4, await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken));
        Assert.Equal(3, await reader.GetRealRowCountAsync("Children", TestContext.Current.CancellationToken));
        Assert.Equal("parentidentity", Assert.Single(await reader.ListIndexesAsync("Parents", TestContext.Current.CancellationToken), index => index.Kind == IndexKind.PrimaryKey).Name);
        using DataTable rows = await reader.ReadTableAsync("Items", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(new string('n', 5000), Assert.Single(rows.Rows.Cast<DataRow>(), row => (int)row["Id"] == 1)["Notes"]);
        Assert.Equal(11, Assert.Single(rows.Rows.Cast<DataRow>(), row => (int)row["Id"] == 3)["Value"]);
    }

    private static async Task MutateAsync(string path)
    {
        await using AccessWriter writer = await AccessWriter.OpenAsync(path, new() { UseLockFile = false }, TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("Parents", [1, "duplicate"], TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("Children", [999], TestContext.Current.CancellationToken));
        await writer.InsertRowAsync("Parents", [2, "managed"], TestContext.Current.CancellationToken);
        await writer.InsertRowAsync("Children", [2], TestContext.Current.CancellationToken);
        object[] values = new object[78];
        Array.Fill(values, DBNull.Value);
        values[0] = 3;
        values[1] = 9;
        values[2] = "managed";
        await writer.InsertRowAsync("Items", values, TestContext.Current.CancellationToken);
    }

    private static void RunNativeScript(DatabaseFormat format, AccessRoundTripSession session, string script, string marker)
    {
        if (format == DatabaseFormat.Jet3Mdb)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0, $"DAO3.6 failed: {result.StandardOutput}\n{result.StandardError}");
            Assert.Contains(marker, result.StandardOutput, StringComparison.Ordinal);
        }
        else
        {
            AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0, $"DAO120 failed: {result.StdOut}\n{result.StdErr}");
            Assert.Contains(marker, result.StdOut, StringComparison.Ordinal);
        }
    }
}
