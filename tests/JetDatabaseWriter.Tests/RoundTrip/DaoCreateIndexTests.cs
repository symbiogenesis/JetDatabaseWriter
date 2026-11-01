namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Native engines accept indexes added to populated native tables.</summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoCreateIndexTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public Task Dao36_AddedIndex_RetainsNativeSeekMutationAndCompact(DatabaseFormat format)
        => VerifyAsync(format, useDao36: true);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public Task Dao120_AddedIndex_RetainsNativeSeekMutationAndCompact(DatabaseFormat format)
        => VerifyAsync(format, useDao36: false);

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    public async Task Dao36_AddingIndexRetainsNativeNameMap()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.IndexNameMap", databaseExtension: ".mdb");
        await File.WriteAllBytesAsync(session.SourcePath, await File.ReadAllBytesAsync(TestDatabases.TestV2000, Ct), Ct);
        byte[] original = await ReadNameMapAsync(session.SourcePath);
        Assert.NotEmpty(original);
        string nativePath = session.CreateDatabasePath("native");
        File.Copy(session.SourcePath, nativePath);
        string native = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(nativePath);
        string key;
        await using (AccessReader reader = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            key = (await reader.GetColumnMetadataAsync("Table1", Ct)).First(column => column.ClrType == typeof(int)).Name;
        }

        string quotedKey = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(key);
        RunNative(
            session,
            useDao36: true,
            $$"""
            $db = $engine.OpenDatabase({{native}})
            try {
                $table = $db.TableDefs('Table1')
                $index = $table.CreateIndex('NameMapEvidence')
                $index.Fields.Append($index.CreateField({{quotedKey}}))
                $table.Indexes.Append($index)
            } finally { $db.Close() }
            """);
        Assert.Equal(original, await ReadNameMapAsync(nativePath));

        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await writer.CreateIndexAsync("Table1", new IndexDefinition("NameMapEvidence", key), Ct);
        }

        Assert.Equal(original, await ReadNameMapAsync(session.SourcePath));
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        RunNative(
            session,
            useDao36: true,
            $$"""
            $db = $engine.OpenDatabase({{source}}, $false, $true)
            try {
                if ($db.TableDefs('Table1').Indexes('NameMapEvidence').Fields.Count -ne 1) { throw 'Native engine could not read the added index beside the original NameMap.' }
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{compacted}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 64)
            $db = $engine.OpenDatabase({{compacted}}, $false, $true)
            try {
                if ($db.TableDefs('Table1').Indexes('NameMapEvidence').Fields.Count -ne 1) { throw 'Native compact lost the added index.' }
            } finally { $db.Close() }
            """);
    }

    private static async Task VerifyAsync(DatabaseFormat format, bool useDao36)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.LateIndex", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        int nativeVersion = format switch { DatabaseFormat.Jet3Mdb => 32, DatabaseFormat.Jet4Mdb => 64, DatabaseFormat.AceAccdb => 128, _ => 128 };
        RunNative(
            session,
            useDao36,
            $$"""
            $db = $engine.CreateDatabase({{source}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{nativeVersion}})
            try {
                $db.Execute('CREATE TABLE [Items] ([Id] LONG CONSTRAINT [NativePK] PRIMARY KEY, [GroupName] TEXT(20), [Score] LONG, [Note] MEMO)', 128)
                $table = $db.TableDefs('Items')
                $table.Properties.Append($table.CreateProperty('Description', 10, 'native table metadata'))
                $db.Execute("INSERT INTO [Items] ([Id],[GroupName],[Score],[Note]) VALUES (1,'alpha',30,'native memo')", 128)
                $db.Execute("INSERT INTO [Items] ([Id],[GroupName],[Score]) VALUES (2,'alpha',10)", 128)
                $db.Execute("INSERT INTO [Items] ([Id],[GroupName],[Score]) VALUES (3,'zulu',20)", 128)
                $db.Execute('INSERT INTO [Items] ([Id]) VALUES (4)', 128)
                $db.Execute('CREATE TABLE [Duplicates] ([Id] LONG)', 128)
                $db.Execute('INSERT INTO [Duplicates] VALUES (1)', 128)
                $db.Execute('INSERT INTO [Duplicates] VALUES (1)', 128)
                $db.Execute('CREATE TABLE [MissingValues] ([Id] LONG)', 128)
                $db.Execute('INSERT INTO [MissingValues] VALUES (Null)', 128)
                $db.Execute('CREATE TABLE [LatePrimary] ([Id] LONG)', 128)
                $db.Execute('INSERT INTO [LatePrimary] VALUES (1)', 128)
                $db.Execute('INSERT INTO [LatePrimary] VALUES (2)', 128)
            } finally { $db.Close() }
            Write-Output 'NATIVE_INDEX_SOURCE_OK'
            """);

        byte[] beforeRefusal = await File.ReadAllBytesAsync(session.SourcePath, Ct);
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.CreateIndexAsync("Duplicates", new IndexDefinition("RefusedUnique", "Id") { IsUnique = true }, Ct));
            await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.CreateIndexAsync("MissingValues", new IndexDefinition("RefusedRequired", "Id") { IsRequired = true }, Ct));
        }

        Assert.Equal(beforeRefusal, await File.ReadAllBytesAsync(session.SourcePath, Ct));
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await writer.CreateIndexAsync("Items", new IndexDefinition("AddedComposite", ["GroupName", "Score"]) { IsUnique = true, IgnoreNulls = true, DescendingColumns = ["Score"] }, Ct);
            await writer.CreateIndexAsync("Duplicates", new IndexDefinition("AddedRequired", "Id") { IsRequired = true }, Ct);
            await writer.CreateIndexAsync("LatePrimary", new IndexDefinition("AddedPrimary", "Id") { IsPrimaryKey = true }, Ct);
        }

        const string inspect = """
            $table = $db.TableDefs('Items')
            if ($table.Properties('Description').Value -ne 'native table metadata') { throw 'Lost native table property.' }
            if (!$table.Indexes('NativePK').Primary) { throw 'Lost native primary key.' }
            $index = $table.Indexes('AddedComposite')
            if (!$index.Unique -or !$index.IgnoreNulls -or $index.Fields.Count -ne 2) { throw 'Wrong added index flags or key width.' }
            if ($index.Fields.Item(0).Name -ne 'GroupName' -or $index.Fields.Item(1).Name -ne 'Score') { throw 'Wrong added index column order.' }
            if (($index.Fields.Item(0).Attributes -band 1) -ne 0 -or ($index.Fields.Item(1).Attributes -band 1) -eq 0) { throw 'Wrong added index sort directions.' }
            if ($db.TableDefs('Duplicates').Indexes.Count -ne 1 -or !$db.TableDefs('Duplicates').Indexes('AddedRequired').Required -or $db.TableDefs('Duplicates').Indexes('AddedRequired').Unique) { throw 'Wrong ordinary required index or a refused unique index was published.' }
            if ($db.TableDefs('MissingValues').Indexes.Count -ne 0) { throw 'Refused required index was published.' }
            if (!$db.TableDefs('LatePrimary').Indexes('AddedPrimary').Primary) { throw 'Wrong added primary-key classification.' }
            """;
        RunNative(
            session,
            useDao36,
            $$"""
            $db = $engine.OpenDatabase({{source}})
            try {
                {{inspect}}
                $rs = $db.OpenRecordset('Items', 1)
                try {
                    $rs.Index = 'AddedComposite'
                    $rs.Seek('=', 'alpha', 30)
                    if ($rs.NoMatch -or $rs.Fields('Id').Value -ne 1 -or $rs.Fields('Note').Value -ne 'native memo') { throw 'Native seek missed an existing row.' }
                    $rs.MoveFirst()
                    $ids = @()
                    while (!$rs.EOF) { $ids += $rs.Fields('Id').Value; $rs.MoveNext() }
                    if (($ids -join ',') -ne '1,2,3') { throw ('Wrong native initial index order: ' + ($ids -join ',')) }
                } finally { $rs.Close() }
                $rejected = $false
                try { $db.Execute("INSERT INTO [Items] ([Id],[GroupName],[Score]) VALUES (5,'alpha',30)", 128) }
                catch {
                    if (@($engine.Errors | ForEach-Object { $_.Number }) -notcontains 3022) { throw }
                    $rejected = $true
                }
                if (!$rejected) { throw 'Native engine accepted duplicate composite keys.' }
                $rejected = $false
                try { $db.Execute('INSERT INTO [Duplicates] VALUES (Null)', 128) }
                catch {
                    if ((@($engine.Errors | ForEach-Object { $_.Number }) -notcontains 3058) -and (@($engine.Errors | ForEach-Object { $_.Number }) -notcontains 3314)) { throw }
                    $rejected = $true
                }
                if (!$rejected) { throw 'Native engine accepted Null in an ordinary required index.' }
                $rejected = $false
                try { $db.Execute('INSERT INTO [LatePrimary] VALUES (Null)', 128) }
                catch {
                    if ((@($engine.Errors | ForEach-Object { $_.Number }) -notcontains 3058) -and (@($engine.Errors | ForEach-Object { $_.Number }) -notcontains 3314)) { throw }
                    $rejected = $true
                }
                if (!$rejected) { throw 'Native engine accepted Null in the added primary key.' }
                $db.Execute("INSERT INTO [Items] ([Id],[GroupName],[Score]) VALUES (5,'alpha',20)", 128)
                $db.Execute('UPDATE [Items] SET [Score]=40 WHERE [Id]=2', 128)
                $db.Execute('DELETE FROM [Items] WHERE [Id]=3', 128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{compacted}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{nativeVersion}})
            $db = $engine.OpenDatabase({{compacted}}, $false, $true)
            try {
                {{inspect}}
                $rs = $db.OpenRecordset('Items', 1)
                try {
                    $rs.Index = 'AddedComposite'
                    $rs.MoveFirst()
                    $ids = @()
                    while (!$rs.EOF) { $ids += $rs.Fields('Id').Value; $rs.MoveNext() }
                    if (($ids -join ',') -ne '2,1,5') { throw ('Wrong compacted index order: ' + ($ids -join ',')) }
                } finally { $rs.Close() }
            } finally { $db.Close() }
            Write-Output 'ADDED_INDEX_NATIVE_COMPACT_OK'
            """);

        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct);
        IndexMetadata added = Assert.Single(await reader.ListIndexesAsync("Items", Ct), index => index.Name == "AddedComposite");
        Assert.True(added.EnforcesUniqueness);
        Assert.True(added.IgnoreNulls);
        Assert.Equal(["GroupName", "Score"], added.Columns.Select(column => column.Name));
        Assert.True(added.Columns[0].IsAscending);
        Assert.False(added.Columns[1].IsAscending);
        int count = 0;
        await foreach (object[] row in reader.Rows("Items", cancellationToken: Ct))
        {
            Assert.True(Assert.IsType<int>(row[0]) is 1 or 2 or 4 or 5);
            count++;
        }

        Assert.Equal(4, count);
    }

    private static void RunNative(AccessRoundTripSession session, bool useDao36, string script)
    {
        if (useDao36)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, result.StandardOutput + result.StandardError);
        }
        else
        {
            AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
        }
    }

    private static async Task<byte[]> ReadNameMapAsync(string path)
    {
        await using AccessWriter writer = await TestDatabase.OpenWriterAsync(path, cancellationToken: Ct);
        long root = (await writer.Services.Catalog.ResolveRequiredTableAsync("Table1", Ct)).Entry.TDefPage;
        ColumnPropertyBlock properties = Assert.IsType<ColumnPropertyBlock>(await writer.Services.Snapshots.ReadLvPropBlockAsync(root, Ct));
        ColumnPropertyEntry nameMap = Assert.IsType<ColumnPropertyEntry>(properties.FindPropertyTarget(string.Empty, "NameMap")?.Find("NameMap"));
        return nameMap.Value;
    }
}
