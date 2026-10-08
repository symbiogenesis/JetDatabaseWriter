namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Compares persisted text rules with Microsoft's Access engine.</summary>
/// <param name="output">Receives native comparison diagnostics.</param>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoTextCollationTests(ITestOutputHelper output)
{
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TableValidationRule_RenameAndSetterRemainEffectiveInDao(bool migrateTextEncoding)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.TableRules");
        string literal = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{literal}}, ';LANGID=0x0409;CP=1252;COUNTRY=0')
            try {
                $db.Execute('CREATE TABLE [Rules] ([Id] LONG, [Other] LONG)')
                $tdf = $db.TableDefs('Rules')
                $tdf.ValidationRule = '[Id]>0'
                $tdf.ValidationText = 'positive'
                $tdf = $null
                $db.Execute('INSERT INTO [Rules] ([Id], [Other]) VALUES (1, 0)')
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, $"DAO failed: {created.StdOut}\n{created.StdErr}");
        if (migrateTextEncoding)
        {
            await using WriterHarness harness = await WriterHarness.OpenAsync(session.SourcePath, cancellationToken: TestContext.Current.CancellationToken);
            CatalogEntry entry = (await harness.Services.Catalog.ResolveRequiredTableAsync("Rules", TestContext.Current.CancellationToken)).Entry;
            ColumnPropertyBlock? stored = await harness.Services.Snapshots.ReadLvPropBlockAsync(entry.TDefPage, TestContext.Current.CancellationToken);
            Assert.NotNull(stored);
            var builder = ColumnPropertyBlockBuilder.FromBlock(stored);
            ColumnPropertyTargetBuilder table = builder.GetOrAddTableTarget();
            ColumnPropertyEntryBuilder rule = Assert.Single(table.Entries, property => property.Name == Constants.ColumnPropertyNames.ValidationRule);
            rule.DataType = ColumnType.TextType;
            await harness.Services.CatalogArtifacts.ExecutePlanAsync(
                new CatalogArtifactPlan([], [])
                {
                    CatalogReplacements = [new UserTableCatalogReplacementArtifact("Rules", "Rules", entry.TDefPage, builder.ToBytes(harness.Database.Format))],
                },
                TestContext.Current.CancellationToken);
        }

        await using (AccessReader reader = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            Assert.Equal(new TableValidationRule("[Id]>0", "positive"), await reader.GetTableValidationRuleAsync("Rules", TestContext.Current.CancellationToken));
        }

        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.InsertRowAsync("Rules", [-1, 0], TestContext.Current.CancellationToken));
            await writer.SetTableValidationRuleAsync("Rules", new TableValidationRule("[Id]>0", "positive identifier"), TestContext.Current.CancellationToken);
            await writer.RenameColumnAsync("Rules", "Id", "Value", TestContext.Current.CancellationToken);
            _ = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.DropColumnAsync("Rules", "Value", TestContext.Current.CancellationToken));
        }

        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScriptThenCompact(
            """
            $tdf = $db.TableDefs('Rules')
            Write-Output "RULE=$($tdf.ValidationRule)"
            Write-Output "TEXT=$($tdf.ValidationText)"
            $tdf = $null
            try { $db.Execute('INSERT INTO [Rules] ([Value], [Other]) VALUES (-1, 0)', 128); Write-Output 'INVALID=ACCEPTED' }
            catch { Write-Output 'INVALID=REFUSED' }
            $db.Execute('INSERT INTO [Rules] ([Value], [Other]) VALUES (2, 0)', 128)
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0 && File.Exists(session.CompactedPath), $"DAO failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains("RULE=[Value]>0", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("TEXT=positive identifier", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("INVALID=REFUSED", result.StdOut, StringComparison.Ordinal);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task TableValidationRule_UnrelatedUpdatesCascadesAndRollbackRemainEffectiveInDao()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.TableRuleMutations");
        string literal = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{literal}}, ';LANGID=0x0409;CP=1252;COUNTRY=0')
            try {
                $db.Execute('CREATE TABLE [Parents] ([Id] LONG CONSTRAINT [PK_Parents] PRIMARY KEY)')
                $db.Execute('CREATE TABLE [Children] ([Id] LONG, [ParentId] LONG, [Other] LONG)')
                $tdf = $db.TableDefs('Children')
                $tdf.ValidationRule = '[ParentId]<5'
                $tdf.ValidationText = 'small parent'
                $tdf = $null
                $relation = $db.CreateRelation('FK_Children', 'Parents', 'Children', 256)
                $field = $relation.CreateField('Id')
                $field.ForeignName = 'ParentId'
                $relation.Fields.Append($field)
                $db.Relations.Append($relation)
                $field = $null
                $relation = $null
                $db.Execute('INSERT INTO [Parents] VALUES (1)')
                $db.Execute('INSERT INTO [Children] VALUES (10, 1, 0)')
                $db.Execute('INSERT INTO [Children] VALUES (11, Null, 0)')
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, $"DAO failed: {created.StdOut}\n{created.StdErr}");

        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            Assert.Equal(2, await writer.UpdateRowsAsync("Children", RowCriteria.Where("Other", 0), new RowValues { ["Other"] = 7 }, TestContext.Current.CancellationToken));
            _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.UpdateRowsAsync("Parents", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 6 }, TestContext.Current.CancellationToken));
            Assert.Equal(1, await writer.UpdateRowsAsync("Parents", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 2 }, TestContext.Current.CancellationToken));
            await using (JetTransaction transaction = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken))
            {
                Assert.Equal(1, await writer.UpdateRowsAsync("Parents", RowCriteria.Where("Id", 2), new RowValues { ["Id"] = 3 }, TestContext.Current.CancellationToken));
                await writer.SetTableValidationRuleAsync("Children", new TableValidationRule("[ParentId]<4", "rolled back"), TestContext.Current.CancellationToken);
                await transaction.RollbackAsync(TestContext.Current.CancellationToken);
            }
        }

        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScriptThenCompact(
            """
            $tdf = $db.TableDefs('Children')
            Write-Output "RULE=$($tdf.ValidationRule)"
            Write-Output "TEXT=$($tdf.ValidationText)"
            $tdf = $null
            $rs = $db.OpenRecordset('SELECT [ParentId], [Other] FROM [Children] WHERE [Id]=10')
            try { Write-Output "ROW=$($rs.Fields('ParentId').Value),$($rs.Fields('Other').Value)" } finally { $rs.Close() }
            $db.Execute('UPDATE [Children] SET [Other]=8 WHERE [Id]=11', 128)
            try { $db.Execute('UPDATE [Parents] SET [Id]=6 WHERE [Id]=2', 128); Write-Output 'CASCADE=ACCEPTED' }
            catch { Write-Output 'CASCADE=REFUSED' }
            $db.Execute('UPDATE [Parents] SET [Id]=4 WHERE [Id]=2', 128)
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0 && File.Exists(session.CompactedPath), $"DAO failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains("RULE=[ParentId]<5", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("TEXT=small parent", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("ROW=2,7", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("CASCADE=REFUSED", result.StdOut, StringComparison.Ordinal);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task TableValidationRule_NativePartitionExpressionIsEvaluated()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.PartitionTableRule");
        string literal = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{literal}}, ';LANGID=0x0409;CP=1252;COUNTRY=0')
            try {
                $db.Execute('CREATE TABLE [Rules] ([Id] LONG, [Other] LONG)')
                $tdf = $db.TableDefs('Rules')
                $tdf.ValidationRule = 'Partition([Id],0,10,2)<>""'
                $tdf.ValidationText = 'partition required'
                $tdf = $null
                $db.Execute('INSERT INTO [Rules] VALUES (1,0)', 128)
                foreach ($expr in @('Partition(-1,0,99,5)', 'Partition(1001,100,1010,20)', 'Partition(1.5,0,10,2)', 'Partition(-1,0,10,1)', 'Partition(2147483647,0,2147483647,2)')) {
                    $rs = $db.OpenRecordset("SELECT $expr AS Result")
                    try { Write-Output "$expr=[$($rs.Fields(0).Value)]" } finally { $rs.Close() }
                }
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, $"DAO failed: {created.StdOut}\n{created.StdErr}");
        Assert.Contains("Partition(-1,0,99,5)=[   : -1]", created.StdOut, StringComparison.Ordinal);
        Assert.Contains("Partition(1001,100,1010,20)=[1000:1010]", created.StdOut, StringComparison.Ordinal);
        Assert.Contains("Partition(1.5,0,10,2)=[ 2: 3]", created.StdOut, StringComparison.Ordinal);
        Assert.Contains("Partition(-1,0,10,1)=[  :-1]", created.StdOut, StringComparison.Ordinal);
        Assert.Contains("Partition(2147483647,0,2147483647,2)=[2147483646:2147483647]", created.StdOut, StringComparison.Ordinal);
        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("Rules", RowCriteria.Where("Id", 1), new RowValues { ["Other"] = 2 }, TestContext.Current.CancellationToken));
        }

        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScriptThenCompact(
            """
            $tdf = $db.TableDefs('Rules')
            Write-Output "RULE=$($tdf.ValidationRule)"
            Write-Output "TEXT=$($tdf.ValidationText)"
            $tdf = $null
            $db.Execute('UPDATE [Rules] SET [Other]=2 WHERE [Id]=1', 128)
            Write-Output 'UPDATE=ACCEPTED'
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0 && File.Exists(session.CompactedPath), $"DAO failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains("RULE=Partition([Id],0,10,2)<>\"\"", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("TEXT=partition required", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("UPDATE=ACCEPTED", result.StdOut, StringComparison.Ordinal);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task TextValidationRule_AccentPunctuationAndTrailingSpace_MatchDao()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.TextCollation");
        string literal = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{literal}}, ';LANGID=0x0409;CP=1252;COUNTRY=0')
            try {
                $values = @([string][char]0x00E9, 'a!', 'a', 'a ')
                $bounds = @('z', 'a?', 'a ', 'a')
                for ($i = 0; $i -lt $values.Count; $i++) {
                    $table = "TextRules$i"
                    $db.Execute("CREATE TABLE [$table] ([Id] LONG, [Value] TEXT(100))")
                    $db.TableDefs.Refresh()
                    $tdf = $db.TableDefs($table)
                    $tdf.Fields('Value').ValidationRule = '< "' + [string]$bounds[$i] + '"'
                    $tdf = $null
                    $rs = $db.OpenRecordset($table, 2)
                    try {
                        $rs.AddNew()
                        $rs.Fields('Id').Value = $i
                        $rs.Fields('Value').Value = [string]$values[$i]
                        try { $rs.Update(); Write-Output "ACCEPT_$i=True" }
                        catch { Write-Output "ACCEPT_$i=False"; $rs.CancelUpdate() }
                    } finally { $rs.Close() }
                    $db.Execute("DELETE FROM [$table]")
                }
                $tdf = $null
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0 && File.Exists(session.SourcePath), $"DAO failed: {result.StdOut}\n{result.StdErr}");

        string[][] pairs = [["é", "z"], ["a!", "a?"], ["a", "a "], ["a ", "a"]];
        await using AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken);
        for (int index = 0; index < pairs.Length; index++)
        {
            bool accepted;
            try
            {
                await writer.InsertRowAsync($"TextRules{index}", [index, pairs[index][0]], TestContext.Current.CancellationToken);
                accepted = true;
            }
            catch (ArgumentException ex)
            {
                output.WriteLine($"Writer rejected TextRules{index}: {ex}");
                accepted = false;
            }

            Assert.Contains($"ACCEPT_{index}={accepted}", result.StdOut, StringComparison.Ordinal);
        }
    }
}
