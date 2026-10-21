namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native acceptance of edited property output carrying preserved synthetic layout variants.</summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoPropertyMutationTests
{
    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(2);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets DAO120-supported formats and every writer mode.</summary>
    /// <returns>The format and mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes()
        => WriteModes.Combine(DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb);

    /// <summary>
    /// DAO creates the schema. Synthetic padding and duplicate/unused names are
    /// planted, accepted by DAO, and preserved through every property edit.
    /// DAO reads metadata, edits data, enforces rules and compacts the edited file.
    /// </summary>
    /// <param name="format">The native database format.</param>
    /// <param name="mode">The writer mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [MemberData(nameof(FormatsAndModes))]
    public async Task EditedProperties_WithPreservedLayoutVariants_OpenEnforceAndCompactInDao(DatabaseFormat format, WriteMode mode)
    {
        await using var session = AccessRoundTripSession.CreateEmpty(
            "JetDatabaseWriter.Tests.PropertyMutation",
            databaseExtension: format == DatabaseFormat.Jet4Mdb ? ".mdb" : ".accdb");
        CreateNativeTable(session, format);
        VerifyWithDao(session, "Score", "[Score] > 0", "positive score", added: false, dropped: false);

        await PlantLayoutVariantsAsync(session.SourcePath, paddedEntries: true, duplicateNames: true, multiplePools: false);
        await PlantEmptyNamePoolsAsync(session.SourcePath);
        ColumnPropertyBlock planted = await ReadPropertiesAsync(session.SourcePath);
        ColumnPropertySourceChunk pool = Assert.Single(planted.SourceChunks, chunk => chunk.Names is { Length: > 0 });
        Assert.Equal(3, planted.SourceChunks.Count(chunk => chunk.Names is { Length: 0 }));
        string[] names = Assert.IsType<string[]>(pool.Names);
        Assert.Contains("UnusedNativeCheck", names);
        Assert.Equal(2, names.Count(name => name == Constants.ColumnPropertyNames.ValidationRule));
        Assert.NotEmpty(Assert.IsType<ColumnPropertyEntry>(planted.FindTableTarget()?.Find(Constants.ColumnPropertyNames.ValidationRule)).Padding);
        VerifyWithDao(session, "Score", "[Score] > 0", "positive score", added: false, dropped: false);

        await MutateAsync(session.SourcePath, mode, writer => writer.SetTableValidationRuleAsync("PropertyVariants", new TableValidationRule("[Score] >= 0", "nonnegative score"), Ct));
        AssertPreservedLayout(planted, await ReadPropertiesAsync(session.SourcePath));
        VerifyWithDao(session, "Score", "[Score] >= 0", "nonnegative score", added: false, dropped: false);

        await MutateAsync(session.SourcePath, mode, writer => writer.RenameColumnAsync("PropertyVariants", "Score", "Amount", Ct));
        AssertPreservedLayout(planted, await ReadPropertiesAsync(session.SourcePath), renamed: true);
        VerifyWithDao(session, "Amount", "[Amount] >= 0", "nonnegative score", added: false, dropped: false);

        await MutateAsync(session.SourcePath, mode, writer => writer.AddColumnAsync("PropertyVariants", new ColumnDefinition("Extra", typeof(int)) { Description = "added property" }, Ct));
        AssertPreservedLayout(planted, await ReadPropertiesAsync(session.SourcePath), renamed: true);
        VerifyWithDao(session, "Amount", "[Amount] >= 0", "nonnegative score", added: true, dropped: false);

        await MutateAsync(session.SourcePath, mode, writer => writer.DropColumnAsync("PropertyVariants", "Note", Ct));
        AssertPreservedLayout(planted, await ReadPropertiesAsync(session.SourcePath), renamed: true, dropped: true);
        VerifyWithDao(session, "Amount", "[Amount] >= 0", "nonnegative score", added: true, dropped: true);

        await MutateAsync(session.SourcePath, mode, writer => writer.SetTableValidationRuleAsync("PropertyVariants", null, Ct));
        AssertPreservedLayout(planted, await ReadPropertiesAsync(session.SourcePath), renamed: true, dropped: true, removedRule: true);
        VerifyWithDao(session, "Amount", string.Empty, string.Empty, added: true, dropped: true);
        session.RunDaoCompact();

        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct);
        Assert.Null(await reader.GetTableValidationRuleAsync("PropertyVariants", Ct));
        IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync("PropertyVariants", Ct);
        Assert.Equal("native score", Assert.Single(metadata, column => column.Name == "Amount").Description);
        Assert.Equal("added property", Assert.Single(metadata, column => column.Name == "Extra").Description);
        DataTable rows = await reader.ReadTableAsync("PropertyVariants", cancellationToken: Ct);
        Assert.Equal(2, Assert.Single(rows.Rows.Cast<DataRow>())["Amount"]);
    }

    /// <summary>Repeated synthetic pools lose native rule semantics and cannot serve as acceptance evidence.</summary>
    /// <param name="format">The native database format.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RepeatedSyntheticNamePools_DoNotEstablishNativeAcceptance(DatabaseFormat format)
    {
        await using var session = AccessRoundTripSession.CreateEmpty(
            "JetDatabaseWriter.Tests.PropertyMutation",
            databaseExtension: format == DatabaseFormat.Jet4Mdb ? ".mdb" : ".accdb");
        CreateNativeTable(session, format);
        VerifyWithDao(session, "Score", "[Score] > 0", "positive score", added: false, dropped: false);
        await PlantLayoutVariantsAsync(session.SourcePath, paddedEntries: false, duplicateNames: false, multiplePools: true);
        ColumnPropertyBlock properties = await ReadPropertiesAsync(session.SourcePath);
        Assert.True(properties.SourceChunks.Count(chunk => chunk.Names is not null) > 1);
        Assert.Equal("[Score] > 0", properties.FindTableTarget()?.GetTextValue(Constants.ColumnPropertyNames.ValidationRule, properties.Format));
        const string script =
            """
            $tdf = $db.TableDefs('PropertyVariants')
            Write-Output "RULE=[$($tdf.ValidationRule)]"
            $tdf = $null
            try {
                $db.Execute('INSERT INTO [PropertyVariants] ([Id], [Score]) VALUES (-1, -1)', 128)
                Write-Output 'MULTIPOOL_INSERT=ACCEPTED'
            } catch {
                Write-Output 'MULTIPOOL_INSERT=REFUSED'
            }
            """;
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScript(session.SourcePath, script, DaoTimeout);
        AssertDaoSucceeded(result);
        Assert.Contains("RULE=[]", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("MULTIPOOL_INSERT=REFUSED", result.StdOut, StringComparison.Ordinal);
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task EmptyOnlyNamePools_AddingRuleKeepsNativeBinding(DatabaseFormat format)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.EmptyPools", databaseExtension: format == DatabaseFormat.Jet4Mdb ? ".mdb" : ".accdb");
        CreateNativeTable(session, format);
        await using (AccessWriter harness = await TestDatabase.OpenWriterAsync(session.SourcePath, cancellationToken: Ct))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.Services.Catalog.GetCatalogEntryAsync("PropertyVariants", Ct));
            ColumnPropertyBlock original = Assert.IsType<ColumnPropertyBlock>(await harness.Services.Snapshots.ReadLvPropBlockAsync(entry.TDefPage, Ct));
            await using var stream = new MemoryStream();
            await using var writer = new BinaryWriter(stream, Encoding.Unicode, leaveOpen: true);
            writer.Write(original.Magic);
            WriteChunk(writer, ColumnPropertyChunkType.NamePool, []);
            WriteChunk(writer, ColumnPropertyChunkType.NamePool, []);
            await harness.Services.CatalogWriter.UpdateTablePropertiesAsync("PropertyVariants", entry.TDefPage, stream.ToArray(), cancellationToken: Ct);
        }

        const string insertScript = """
            $db.Execute('INSERT INTO [PropertyVariants] ([Id], [Score]) VALUES (99, 2)', 128)
            $db.Execute('DELETE FROM [PropertyVariants] WHERE [Id] = 99', 128)
            Write-Output "RULE=[$($db.TableDefs('PropertyVariants').ValidationRule)]"
            """;
        AccessRoundTripEnvironment.CompactResult baseline = session.RunDaoDatabaseScript(session.SourcePath, insertScript, DaoTimeout);
        AssertDaoSucceeded(baseline);
        Assert.Contains("RULE=[]", baseline.StdOut, StringComparison.Ordinal);
        await MutateAsync(session.SourcePath, WriteMode.Direct, writer => writer.SetTableValidationRuleAsync("PropertyVariants", new TableValidationRule("[Score] > 0", "positive"), Ct));
        AccessRoundTripEnvironment.CompactResult edited = session.RunDaoDatabaseScript(session.SourcePath, insertScript, DaoTimeout);
        AssertDaoSucceeded(edited);
        Assert.Contains("RULE=[[Score] > 0]", edited.StdOut, StringComparison.Ordinal);
        session.RunDaoCompact();
    }

    private static void CreateNativeTable(AccessRoundTripSession session, DatabaseFormat format)
    {
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        int version = format == DatabaseFormat.Jet4Mdb ? 64 : 128;
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{source}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            try {
                $db.Execute('CREATE TABLE [PropertyVariants] ([Id] LONG, [Score] LONG, [Note] TEXT(20))')
                $tdf = $db.TableDefs('PropertyVariants')
                $tdf.ValidationRule = '[Score] > 0'
                $tdf.ValidationText = 'positive score'
                $tdf.Properties.Append($tdf.CreateProperty('Description', 10, 'native table'))
                $field = $tdf.Fields('Score')
                $field.Properties.Append($field.CreateProperty('Description', 10, 'native score'))
                $field.Properties.Append($field.CreateProperty('Caption', 10, 'score caption'))
                $field.DefaultValue = '1'
                $field.Required = $true
                $db.Execute("INSERT INTO [PropertyVariants] ([Id], [Score], [Note]) VALUES (1, 1, 'first')", 128)
                Write-Output 'CREATED=OK'
                $field = $null
                $tdf = $null
            } finally { $db.Close() }
            """,
            DaoTimeout);
        AssertDaoSucceeded(created);
    }

    private static void VerifyWithDao(AccessRoundTripSession session, string scoreColumn, string rule, string validationText, bool added, bool dropped)
    {
        string score = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(scoreColumn);
        string expectedRule = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(rule);
        string expectedText = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(validationText);
        int expectedColumns = 3 + (added ? 1 : 0) - (dropped ? 1 : 0);
        string script = $$"""
            $tdf = $db.TableDefs('PropertyVariants')
            if ([string]$tdf.ValidationRule -ne {{expectedRule}}) { throw "Unexpected rule: $($tdf.ValidationRule)" }
            if ([string]$tdf.ValidationText -ne {{expectedText}}) { throw "Unexpected validation text: $($tdf.ValidationText)" }
            if ($tdf.Fields.Count -ne {{expectedColumns}}) { throw 'Unexpected field count' }
            if ($tdf.Properties('Description').Value -ne 'native table') { throw 'Lost table description' }
            $field = $tdf.Fields({{score}})
            if ($field.Properties('Description').Value -ne 'native score') { throw 'Lost score description' }
            if ($field.Properties('Caption').Value -ne 'score caption') { throw 'Lost score caption' }
            if (!$field.Required -or $field.DefaultValue -ne '1') { throw 'Lost required/default properties' }
            $field = $null
            $tdf = $null
            $rs = $db.OpenRecordset('PropertyVariants', 2)
            try {
                if ($rs.Fields({{score}}).Value -ne 1 -and $rs.Fields({{score}}).Value -ne 2) { throw 'Unexpected score' }
                $rs.Edit()
                $rs.Fields({{score}}).Value = 2
                $rs.Update()
            } finally { $rs.Close() }
            $db.Execute('INSERT INTO [PropertyVariants] ([Id], [{{scoreColumn}}]) VALUES (99, 2)', 128)
            $db.Execute('DELETE FROM [PropertyVariants] WHERE [Id] = 99', 128)
            $negativeAccepted = $false
            try {
                $db.Execute('INSERT INTO [PropertyVariants] ([Id], [{{scoreColumn}}]) VALUES (-1, -1)', 128)
                $negativeAccepted = $true
            } catch {
                if ({{expectedRule}} -eq '') { throw }
            }
            if ($negativeAccepted -ne ({{expectedRule}} -eq '')) { throw 'Unexpected validation enforcement' }
            if ($negativeAccepted) { $db.Execute('DELETE FROM [PropertyVariants] WHERE [Id] = -1', 128) }
            Write-Output 'PROPERTIES_DATA_ENFORCEMENT=OK'
            """;
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScript(session.SourcePath, script, DaoTimeout);
        AssertDaoSucceeded(result);
        Assert.Contains("PROPERTIES_DATA_ENFORCEMENT=OK", result.StdOut, StringComparison.Ordinal);
    }

    private static async Task MutateAsync(string path, WriteMode mode, Func<AccessWriter, ValueTask> mutation)
    {
        await using AccessWriter writer = await AccessWriter.OpenAsync(path, WriteModes.WriterOptions(mode), Ct);
        await WriteModes.RunAsync(writer, mode, () => mutation(writer).AsTask(), Ct);
    }

    private static async Task<ColumnPropertyBlock> ReadPropertiesAsync(string path)
    {
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(path, cancellationToken: Ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.Services.Catalog.GetCatalogEntryAsync("PropertyVariants", Ct));
        return Assert.IsType<ColumnPropertyBlock>(await harness.Services.Snapshots.ReadLvPropBlockAsync(entry.TDefPage, Ct));
    }

    private static async Task PlantEmptyNamePoolsAsync(string path)
    {
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(path, cancellationToken: Ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.Services.Catalog.GetCatalogEntryAsync("PropertyVariants", Ct));
        ColumnPropertyBlock original = Assert.IsType<ColumnPropertyBlock>(await harness.Services.Snapshots.ReadLvPropBlockAsync(entry.TDefPage, Ct));
        byte[] bytes = Assert.IsType<byte[]>(original.ToBytes(original.Format));
        await using var stream = new MemoryStream();
        await stream.WriteAsync(bytes.AsMemory(0, sizeof(uint)), Ct);
        int chunkIndex = 0;
        for (int offset = sizeof(uint); offset < bytes.Length; chunkIndex++)
        {
            int length = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset));
            await stream.WriteAsync(bytes.AsMemory(offset, length), Ct);
            offset += length;
            if (chunkIndex < 2 || offset == bytes.Length)
            {
                byte[] emptyPool = [6, 0, 0, 0, 0x80, 0];
                await stream.WriteAsync(emptyPool, Ct);
            }
        }

        await harness.Services.CatalogWriter.UpdateTablePropertiesAsync("PropertyVariants", entry.TDefPage, stream.ToArray(), cancellationToken: Ct);
    }

    private static async Task PlantLayoutVariantsAsync(string path, bool paddedEntries, bool duplicateNames, bool multiplePools)
    {
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(path, cancellationToken: Ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.Services.Catalog.GetCatalogEntryAsync("PropertyVariants", Ct));
        ColumnPropertyBlock original = Assert.IsType<ColumnPropertyBlock>(await harness.Services.Snapshots.ReadLvPropBlockAsync(entry.TDefPage, Ct));
        Assert.Empty(original.UnknownChunks);
        string[] nativeNames = Assert.IsType<string[]>(Assert.Single(original.SourceChunks, chunk => chunk.Names is not null).Names);
        await using var stream = new MemoryStream();
        await using var writer = new BinaryWriter(stream, Encoding.Unicode, leaveOpen: true);
        writer.Write(original.Magic);
        for (int index = 0; index < original.Targets.Count; index++)
        {
            ColumnPropertyTarget target = original.Targets[index];
            string[] names = duplicateNames ? [.. nativeNames, "UnusedNativeCheck", Constants.ColumnPropertyNames.ValidationRule] : nativeNames;
            await using var pool = new MemoryStream();
            await using (var poolWriter = new BinaryWriter(pool, Encoding.Unicode, leaveOpen: true))
            {
                foreach (string name in names)
                {
                    WriteText(poolWriter, name);
                }
            }

            if (index == 0 || multiplePools)
            {
                WriteChunk(writer, ColumnPropertyChunkType.NamePool, pool.ToArray());
            }

            await using var payload = new MemoryStream();
            await using (var propertyWriter = new BinaryWriter(payload, Encoding.Unicode, leaveOpen: true))
            {
                propertyWriter.Write(target.SourceHeader ?? 0u);
                WriteText(propertyWriter, target.Name);
                foreach (ColumnPropertyEntry property in target.Entries)
                {
                    byte[] padding = paddedEntries && property.DataType is ColumnType.TextType or ColumnType.MemoType ? [0xA1, 0xB2, 0xC3] : [];
                    propertyWriter.Write(checked((ushort)(8 + property.Value.Length + padding.Length)));
                    propertyWriter.Write(property.DdlFlag);
                    propertyWriter.Write((byte)property.DataType);
                    propertyWriter.Write(checked((ushort)Array.IndexOf(names, property.Name)));
                    propertyWriter.Write(checked((ushort)property.Value.Length));
                    propertyWriter.Write(property.Value);
                    propertyWriter.Write(padding);
                }
            }

            WriteChunk(writer, target.ChunkType, payload.ToArray());
        }

        await harness.Services.CatalogWriter.UpdateTablePropertiesAsync("PropertyVariants", entry.TDefPage, stream.ToArray(), cancellationToken: Ct);
    }

    private static void AssertPreservedLayout(ColumnPropertyBlock before, ColumnPropertyBlock after, bool renamed = false, bool dropped = false, bool removedRule = false)
    {
        ColumnPropertySourceChunk[] originalPools = [.. before.SourceChunks.Where(chunk => chunk.Names is not null)];
        ColumnPropertySourceChunk[] editedPools = [.. after.SourceChunks.Where(chunk => chunk.Names is not null)];
        Assert.Equal(originalPools.Length, editedPools.Length);
        for (int index = 0; index < originalPools.Length; index++)
        {
            string[] names = Assert.IsType<string[]>(originalPools[index].Names);
            string[] editedNames = Assert.IsType<string[]>(editedPools[index].Names);
            Assert.Equal(names, editedNames.Take(names.Length));
            if (names.Length == 0)
            {
                Assert.Empty(editedNames);
                Assert.Equal(originalPools[index].Payload, editedPools[index].Payload);
            }
        }

        foreach (ColumnPropertyTarget originalTarget in before.Targets)
        {
            if (dropped && originalTarget.Name == "Note")
            {
                Assert.Null(after.FindTarget("Note"));
                continue;
            }

            string name = renamed && originalTarget.Name == "Score" ? "Amount" : originalTarget.Name;
            ColumnPropertyTarget target = Assert.IsType<ColumnPropertyTarget>(after.FindTarget(name));
            Assert.Equal(originalTarget.ChunkType, target.ChunkType);
            Assert.Equal(originalTarget.SourceChunkIndex, target.SourceChunkIndex);
            foreach (ColumnPropertyEntry property in originalTarget.Entries)
            {
                if (removedRule && name.Length == 0 && property.Name is Constants.ColumnPropertyNames.ValidationRule or Constants.ColumnPropertyNames.ValidationText)
                {
                    Assert.Null(target.Find(property.Name));
                    continue;
                }

                ColumnPropertyEntry edited = Assert.IsType<ColumnPropertyEntry>(target.Find(property.Name));
                Assert.Equal(property.Padding, edited.Padding);
                Assert.Equal(property.SourceNameIndex, edited.SourceNameIndex);
                Assert.Equal(property.DataType, edited.DataType);
                Assert.Equal(property.DdlFlag, edited.DdlFlag);
            }
        }
    }

    private static void WriteText(BinaryWriter writer, string text)
    {
        byte[] bytes = Encoding.Unicode.GetBytes(text);
        writer.Write(checked((ushort)bytes.Length));
        writer.Write(bytes);
    }

    private static void WriteChunk(BinaryWriter writer, ColumnPropertyChunkType type, byte[] payload)
    {
        writer.Write(checked((uint)(6 + payload.Length)));
        writer.Write((ushort)type);
        writer.Write(payload);
    }

    private static void AssertDaoSucceeded(AccessRoundTripEnvironment.CompactResult result)
        => Assert.True(result.ExitCode == 0, $"DAO failed (exit={result.ExitCode}).\nstdout: {result.StdOut}\nstderr: {result.StdErr}");
}
