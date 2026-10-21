namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>DAO-authored same-name column and index maps remain separate through schema edits.</summary>
public sealed class NativePropertyTargetIdentityTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets the focused schema edits in every write mode.</summary>
    /// <returns>The schema edits and write modes.</returns>
    public static TheoryData<string, WriteMode> Operations()
    {
        var data = new TheoryData<string, WriteMode>();
        foreach (string operation in new[] { "add", "drop", "rename" })
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                data.Add(operation, mode);
            }
        }

        return data;
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [MemberData(nameof(Operations))]
    public Task Dao120Jet4_SameNameMapsSurviveRewriteAndCompact(string operation, WriteMode mode)
        => VerifyAsync(DatabaseFormat.Jet4Mdb, operation, mode);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [MemberData(nameof(Operations))]
    public Task Dao120Ace_SameNameMapsSurviveRewriteAndCompact(string operation, WriteMode mode)
        => VerifyAsync(DatabaseFormat.AceAccdb, operation, mode);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [MemberData(nameof(Operations))]
    public Task Dao36Jet3_SameNameMapsSurviveRewriteAndCompact(string operation, WriteMode mode)
        => VerifyAsync(DatabaseFormat.Jet3Mdb, operation, mode);

    private static async Task VerifyAsync(DatabaseFormat format, string operation, WriteMode mode)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativePropertyIdentity", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string source = Literal(session.SourcePath);
        string compacted = Literal(session.CompactedPath);
        int version = format switch { DatabaseFormat.Jet3Mdb => 32, DatabaseFormat.Jet4Mdb => 64, DatabaseFormat.AceAccdb => 128, _ => throw new ArgumentOutOfRangeException(nameof(format)) };
        RunNative(session, format, $$"""
            $db = $engine.CreateDatabase({{source}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            try {
                $db.Execute('CREATE TABLE [T] ([Id] LONG,[Score] LONG,[Note] LONG)', 128)
                $table = $db.TableDefs('T')
                $field = $table.Fields('Score')
                $field.Required = $true
                $field.DefaultValue = '1'
                $field.Properties.Append($field.CreateProperty('Description', 10, 'column description'))
                $field.Properties.Append($field.CreateProperty('SharedCustom', 10, 'column custom'))
                $index = $table.CreateIndex('Score')
                $index.Unique = $true
                $index.Fields.Append($index.CreateField('Id'))
                $table.Indexes.Append($index)
                $index = $table.Indexes('Score')
                $index.Properties.Append($index.CreateProperty('Description', 10, 'index description'))
                $index.Properties.Append($index.CreateProperty('SharedCustom', 10, 'index custom'))
                $removed = $table.CreateIndex('Gone')
                $removed.Fields.Append($removed.CreateField('Score'))
                $table.Indexes.Append($removed)
                $removed = $table.Indexes('Gone')
                $removed.Properties.Append($removed.CreateProperty('SharedCustom', 10, 'dependent index custom'))
                $db.Execute('INSERT INTO [T] ([Id],[Score],[Note]) VALUES (1,2,3)', 128)
            } finally { $db.Close() }
            """);

        await using (AccessReader reader = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            Assert.Equal("column description", Assert.Single(await reader.GetColumnMetadataAsync("T", Ct), column => column.Name == "Score").Description);
        }

        ColumnPropertyTarget originalIndex;
        await using (AccessWriter harness = await TestDatabase.OpenWriterAsync(session.SourcePath, cancellationToken: Ct))
        {
            long page = (await harness.Services.Catalog.ResolveRequiredTableAsync("T", Ct)).Entry.TDefPage;
            ColumnPropertyBlock block = Assert.IsType<ColumnPropertyBlock>(await harness.Services.Snapshots.ReadLvPropBlockAsync(page, Ct));
            originalIndex = Assert.Single(block.Targets, target => target.Name == "Score" && target.ChunkType == ColumnPropertyChunkType.PropertyBlockAlt2);
            Assert.Single(block.Targets, target => target.Name == "Score" && target.ChunkType == ColumnPropertyChunkType.PropertyBlockAlt1);
        }

        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, WriteModes.WriterOptions(mode), Ct))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
                {
                    if (operation == "add")
                    {
                        await writer.AddColumnAsync("T", new ColumnDefinition("Extra", typeof(int)), Ct);
                    }
                    else if (operation == "drop")
                    {
                        await writer.DropColumnAsync("T", "Score", Ct);
                    }
                    else
                    {
                        await writer.RenameColumnAsync("T", "Score", "Amount", Ct);
                    }
                },
                Ct);
        }

        await using (AccessWriter harness = await TestDatabase.OpenWriterAsync(session.SourcePath, cancellationToken: Ct))
        {
            long page = (await harness.Services.Catalog.ResolveRequiredTableAsync("T", Ct)).Entry.TDefPage;
            ColumnPropertyBlock block = Assert.IsType<ColumnPropertyBlock>(await harness.Services.Snapshots.ReadLvPropBlockAsync(page, Ct));
            ColumnPropertyTarget retained = Assert.Single(block.Targets, target => target.Name == "Score" && target.ChunkType == ColumnPropertyChunkType.PropertyBlockAlt2);
            ColumnPropertyTarget? dependent = block.FindTarget("Gone", ColumnPropertyChunkType.PropertyBlockAlt2);
            if (operation == "drop")
            {
                Assert.Null(dependent);
            }
            else
            {
                Assert.Equal("dependent index custom", dependent?.GetTextValue("SharedCustom", harness.Database.Format));
            }

            Assert.Equal(originalIndex.SourceHeader, retained.SourceHeader);
            Assert.Equal(originalIndex.Entries.Count, retained.Entries.Count);
            for (int index = 0; index < originalIndex.Entries.Count; index++)
            {
                ColumnPropertyEntry before = originalIndex.Entries[index];
                ColumnPropertyEntry after = retained.Entries[index];
                Assert.Equal(before.Name, after.Name);
                Assert.Equal(before.DataType, after.DataType);
                Assert.Equal(before.DdlFlag, after.DdlFlag);
                Assert.Equal(before.Value, after.Value);
                Assert.Equal(before.Padding, after.Padding);
                Assert.Equal(before.SourceNameIndex, after.SourceNameIndex);
            }
        }

        string columnName = operation == "rename" ? "Amount" : "Score";
        string dependentIndexCheck = operation == "drop"
            ? "if (@($table.Indexes | Where-Object { $_.Name -eq 'Gone' }).Count -ne 0) { throw 'Retained dropped index.' }"
            : $"if ($table.Indexes('Gone').Fields(0).Name -ne '{columnName}' -or $table.Indexes('Gone').Properties('SharedCustom').Value -ne 'dependent index custom') {{ throw 'Lost dependent index map.' }}";
        string columnCheck = operation == "drop"
            ? string.Empty
            : $$"""
            $field = $table.Fields('{{columnName}}')
            if ($field.Properties('Description').Value -ne 'column description' -or $field.Properties('SharedCustom').Value -ne 'column custom') { throw 'Lost column map.' }
            if (-not $field.Required -or $field.DefaultValue -ne '1') { throw 'Lost field constraint.' }
            """;
        string verify = $$"""
            $table = $db.TableDefs('T')
            {{columnCheck}}
            {{dependentIndexCheck}}
            $index = $table.Indexes('Score')
            if ($index.Properties('Description').Value -ne 'index description' -or $index.Properties('SharedCustom').Value -ne 'index custom') { throw 'Lost index map.' }
            if ($index.Fields(0).Name -ne 'Id' -or -not $index.Unique) { throw 'Lost index identity.' }
            foreach ($candidate in $db.TableDefs) { if ($candidate.Name.StartsWith('~')) { throw 'Left temporary table.' } }
            $rs = $db.OpenRecordset('T', 1)
            try {
                $rs.Index = 'Score'
                $rs.Seek('=', 1)
                if ($rs.NoMatch -or $rs.Fields('Note').Value -ne 3) { throw 'Lost indexed row.' }
            } finally { $rs.Close() }
            """;
        string insertColumns = operation == "drop" ? "[Id],[Note]" : $"[Id],[{columnName}],[Note]";
        string insertValues = operation == "drop" ? "99,4" : "99,5,4";
        RunNative(session, format, $$"""
            $db = $engine.OpenDatabase({{source}})
            try {
                {{verify}}
                $db.Execute('INSERT INTO [T] ({{insertColumns}}) VALUES ({{insertValues}})', 128)
                $db.Execute('UPDATE [T] SET [Note]=5 WHERE [Id]=99', 128)
                $db.Execute('DELETE FROM [T] WHERE [Id]=99', 128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{compacted}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            $db = $engine.OpenDatabase({{compacted}}, $false, $true)
            try { {{verify}} } finally { $db.Close() }
            """);
    }

    private static void RunNative(AccessRoundTripSession session, DatabaseFormat format, string script)
    {
        if (format == DatabaseFormat.Jet3Mdb)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, $"DAO36 failed: {result.StandardOutput}\n{result.StandardError}");
        }
        else
        {
            AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0, $"DAO120 failed: {result.StdOut}\n{result.StdErr}");
        }
    }

    private static string Literal(string value) => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(value);
}
