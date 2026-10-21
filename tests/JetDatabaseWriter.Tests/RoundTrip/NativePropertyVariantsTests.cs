namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Buffers.Binary;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Independent native evidence for repeated property layouts and Jet3 property edits.</summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class NativePropertyVariantsTests
{
    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(2);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets original and repeated layouts in every Jet3 writer mode.</summary>
    /// <returns>The layout and writer modes.</returns>
    public static TheoryData<string, WriteMode> Jet3Cases()
    {
        var data = new TheoryData<string, WriteMode>();
        foreach (string layout in new[] { "native", "entries", "split", "targets" })
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                data.Add(layout, mode);
            }
        }

        return data;
    }

    /// <summary>Gets repeated layouts in every supported DAO120 format and writer mode.</summary>
    /// <returns>The database formats, layouts and writer modes.</returns>
    public static TheoryData<DatabaseFormat, string, WriteMode> Jet4AndAceCases()
    {
        var data = new TheoryData<DatabaseFormat, string, WriteMode>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (string layout in new[] { "entries", "split", "targets" })
            {
                foreach (WriteMode mode in Enum.GetValues<WriteMode>())
                {
                    data.Add(format, layout, mode);
                }
            }
        }

        return data;
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [MemberData(nameof(Jet3Cases))]
    public Task Jet3_PropertiesRemainNativeAfterEditsAndCompact(string layout, WriteMode mode)
        => VerifyEditsAsync(DatabaseFormat.Jet3Mdb, layout, mode);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [MemberData(nameof(Jet4AndAceCases))]
    public Task Jet4AndAce_RepeatedPropertiesRemainNativeAfterEditsAndCompact(DatabaseFormat format, string layout, WriteMode mode)
        => VerifyEditsAsync(format, layout, mode);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [InlineData("interleaved")]
    [InlineData("adjacent")]
    public async Task Jet3_PopulatedNamePoolsDoNotEstablishNativeAcceptance(string layout)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativePropertyPools", databaseExtension: ".mdb");
        CreateNativeTable(session, DatabaseFormat.Jet3Mdb);
        VerifyWithDao(session, DatabaseFormat.Jet3Mdb, "Score", "[Score] > 0", "positive score", added: false, dropped: false);
        await PlantPopulatedPoolsAsync(session.SourcePath, layout);
        string script = $$"""
            $db = $engine.OpenDatabase({{Literal(session.SourcePath)}})
            try {
                $tdf = $db.TableDefs('PropertyVariants')
                Write-Output "RULE=[$($tdf.ValidationRule)]"
                $tdf = $null
                try {
                    $db.Execute('INSERT INTO [PropertyVariants] ([Id], [Score]) VALUES (99, 1)', 128)
                    Write-Output 'INSERT=ACCEPTED'
                } catch {
                    Write-Output 'INSERT=REFUSED'
                    Write-Output ('ERRORS=' + (($engine.Errors | ForEach-Object { $_.Number }) -join ','))
                }
            } finally { $db.Close() }
            """;
        string result = RunEngine(session, DatabaseFormat.Jet3Mdb, script);
        Assert.Contains("RULE=[]", result, StringComparison.Ordinal);
        Assert.Contains("INSERT=REFUSED", result, StringComparison.Ordinal);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    public async Task Jet3_EmptyNamePoolDoesNotClearNativeBindings()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativePropertyPools", databaseExtension: ".mdb");
        CreateNativeTable(session, DatabaseFormat.Jet3Mdb);
        await PlantPopulatedPoolsAsync(session.SourcePath, "empty");
        VerifyWithDao(session, DatabaseFormat.Jet3Mdb, "Score", "[Score] > 0", "positive score", added: false, dropped: false);
        Compact(session, DatabaseFormat.Jet3Mdb);
        VerifyWithDao(session, DatabaseFormat.Jet3Mdb, "Score", "[Score] > 0", "positive score", added: false, dropped: false, compacted: true);
    }

    private static async Task VerifyEditsAsync(DatabaseFormat format, string layout, WriteMode mode)
    {
        await using var session = AccessRoundTripSession.CreateEmpty(
            "JetDatabaseWriter.Tests.NativePropertyVariants",
            databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        CreateNativeTable(session, format);
        VerifyWithDao(session, format, "Score", "[Score] > 0", "positive score", added: false, dropped: false);
        if (layout != "native")
        {
            await PlantLayoutAsync(session.SourcePath, layout);

            // Both native engines take table metadata from the first empty-name
            // target; text in a later split target is not surfaced by DAO.
            VerifyWithDao(session, format, "Score", "[Score] > 0", layout == "split" ? string.Empty : "positive score", added: false, dropped: false);
            await using AccessReader reader = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, Ct);
            TableValidationRule? stored = await reader.GetTableValidationRuleAsync("PropertyVariants", Ct);
            Assert.Equal("[Score] > 0", stored?.Expression);
            Assert.Equal(layout == "split" ? string.Empty : "positive score", stored?.ValidationText ?? string.Empty);
        }

        await MutateAsync(session.SourcePath, mode, writer => writer.SetTableValidationRuleAsync("PropertyVariants", new TableValidationRule("[Score] >= 0", "nonnegative score"), Ct));
        AssertRule(await ReadPropertiesAsync(session.SourcePath), "[Score] >= 0", "nonnegative score");
        VerifyWithDao(session, format, "Score", "[Score] >= 0", "nonnegative score", added: false, dropped: false);

        await MutateAsync(session.SourcePath, mode, writer => writer.RenameColumnAsync("PropertyVariants", "Score", "Amount", Ct));
        AssertRule(await ReadPropertiesAsync(session.SourcePath), "[Amount] >= 0", "nonnegative score");
        VerifyWithDao(session, format, "Amount", "[Amount] >= 0", "nonnegative score", added: false, dropped: false);

        await MutateAsync(session.SourcePath, mode, writer => writer.AddColumnAsync("PropertyVariants", new ColumnDefinition("Extra", typeof(int)) { Description = "added property" }, Ct));
        VerifyWithDao(session, format, "Amount", "[Amount] >= 0", "nonnegative score", added: true, dropped: false);

        await MutateAsync(session.SourcePath, mode, writer => writer.DropColumnAsync("PropertyVariants", "Note", Ct));
        VerifyWithDao(session, format, "Amount", "[Amount] >= 0", "nonnegative score", added: true, dropped: true);

        Compact(session, format);
        VerifyWithDao(session, format, "Amount", "[Amount] >= 0", "nonnegative score", added: true, dropped: true, compacted: true);
        await using (AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            Assert.Equal("[Amount] >= 0", (await reader.GetTableValidationRuleAsync("PropertyVariants", Ct))?.Expression);
            using DataTable rows = await reader.ReadTableAsync("PropertyVariants", cancellationToken: Ct);
            Assert.Equal(2, Assert.Single(rows.Rows.Cast<DataRow>())["Amount"]);
        }

        await MutateAsync(session.SourcePath, mode, writer => writer.SetTableValidationRuleAsync("PropertyVariants", null, Ct));
        AssertRule(await ReadPropertiesAsync(session.SourcePath), null, null);
        VerifyWithDao(session, format, "Amount", string.Empty, string.Empty, added: true, dropped: true);
    }

    private static void CreateNativeTable(AccessRoundTripSession session, DatabaseFormat format)
    {
        int version = Version(format);
        string script = $$"""
            $db = $engine.CreateDatabase({{Literal(session.SourcePath)}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            try {
                $db.Execute('CREATE TABLE [PropertyVariants] ([Id] LONG, [Score] LONG, [Note] TEXT(20))', 128)
                $tdf = $db.TableDefs('PropertyVariants')
                $tdf.ValidationRule = '[Score] > 0'
                $tdf.ValidationText = 'positive score'
                $tdf.Properties.Append($tdf.CreateProperty('Description', 10, 'native table'))
                $field = $tdf.Fields('Score')
                $field.Properties.Append($field.CreateProperty('Description', 10, 'native score'))
                $field.Properties.Append($field.CreateProperty('Caption', 10, 'score caption'))
                $field.DefaultValue = '1'
                $field.Required = $true
                $field = $null
                $tdf = $null
                $db.Execute("INSERT INTO [PropertyVariants] ([Id], [Score], [Note]) VALUES (1, 1, 'first')", 128)
            } finally { $db.Close() }
            Write-Output 'CREATED=OK'
            """;
        Assert.Contains("CREATED=OK", RunEngine(session, format, script), StringComparison.Ordinal);
    }

    private static void VerifyWithDao(AccessRoundTripSession session, DatabaseFormat format, string scoreColumn, string rule, string validationText, bool added, bool dropped, bool compacted = false)
    {
        int columnCount = 3 + (added ? 1 : 0) - (dropped ? 1 : 0);
        string path = compacted ? session.CompactedPath : session.SourcePath;
        string script = $$"""
            $db = $engine.OpenDatabase({{Literal(path)}})
            try {
                $tdf = $db.TableDefs('PropertyVariants')
                if ([string]$tdf.ValidationRule -ne {{Literal(rule)}}) { throw "Unexpected native rule: [$($tdf.ValidationRule)]" }
                if ([string]$tdf.ValidationText -ne {{Literal(validationText)}}) { throw "Unexpected native text: [$($tdf.ValidationText)]" }
                if ($tdf.Fields.Count -ne {{columnCount}}) { throw 'Lost schema fields.' }
                if ($tdf.Properties('Description').Value -ne 'native table') { throw 'Lost table description.' }
                if ({{(added ? "$true" : "$false")}} -and $tdf.Fields('Extra').Properties('Description').Value -ne 'added property') { throw 'Lost added description.' }
                $field = $tdf.Fields({{Literal(scoreColumn)}})
                if ($field.Properties('Description').Value -ne 'native score' -or $field.Properties('Caption').Value -ne 'score caption') { throw 'Lost score properties.' }
                if (-not $field.Required -or $field.DefaultValue -ne '1') { throw 'Lost Required/default.' }
                $field = $null
                $tdf = $null
                $rs = $db.OpenRecordset('PropertyVariants', 2)
                try {
                    if ($rs.Fields({{Literal(scoreColumn)}}).Value -ne 1 -and $rs.Fields({{Literal(scoreColumn)}}).Value -ne 2) { throw 'Lost row value.' }
                    $rs.Edit()
                    $rs.Fields({{Literal(scoreColumn)}}).Value = 2
                    $rs.Update()
                } finally { $rs.Close() }
                $negativeAccepted = $false
                try {
                    $db.Execute('INSERT INTO [PropertyVariants] ([Id], [{{scoreColumn}}]) VALUES (-1, -1)', 128)
                    $negativeAccepted = $true
                } catch {
                    if ({{Literal(rule)}} -eq '') { throw }
                    if (-not @($engine.Errors | Where-Object { $_.Number -in @(3316, 3317) }).Count) { throw }
                }
                if ($negativeAccepted -ne ({{Literal(rule)}} -eq '')) { throw 'Wrong native enforcement.' }
                if ($negativeAccepted) { $db.Execute('DELETE FROM [PropertyVariants] WHERE [Id] = -1', 128) }
                $db.Execute('INSERT INTO [PropertyVariants] ([Id], [{{scoreColumn}}]) VALUES (99, 1)', 128)
                $db.Execute('DELETE FROM [PropertyVariants] WHERE [Id] = 99', 128)
            } finally { $db.Close() }
            Write-Output 'PROPERTIES_DATA_ENFORCEMENT=OK'
            """;
        Assert.Contains("PROPERTIES_DATA_ENFORCEMENT=OK", RunEngine(session, format, script), StringComparison.Ordinal);
    }

    private static void Compact(AccessRoundTripSession session, DatabaseFormat format)
    {
        string script = $$"""
            $engine.CompactDatabase({{Literal(session.SourcePath)}}, {{Literal(session.CompactedPath)}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{Version(format)}})
            Write-Output 'COMPACT=OK'
            """;
        Assert.Contains("COMPACT=OK", RunEngine(session, format, script), StringComparison.Ordinal);
    }

    private static string RunEngine(AccessRoundTripSession session, DatabaseFormat format, string script)
    {
        if (format == DatabaseFormat.Jet3Mdb)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, DaoTimeout);
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, $"DAO36 failed: {result.StandardOutput}\n{result.StandardError}");
            return result.StandardOutput;
        }

        AccessRoundTripEnvironment.CompactResult ace = session.RunDaoEngineScript(script, DaoTimeout);
        Assert.True(ace.ExitCode == 0, $"DAO120 failed: {ace.StdOut}\n{ace.StdErr}");
        return ace.StdOut;
    }

    private static int Version(DatabaseFormat format) => format switch
    {
        DatabaseFormat.Jet3Mdb => 32,
        DatabaseFormat.Jet4Mdb => 64,
        DatabaseFormat.AceAccdb => 128,
        _ => throw new ArgumentOutOfRangeException(nameof(format)),
    };

    private static string Literal(string value) => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(value);

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

    private static async Task PlantLayoutAsync(string path, string layout)
    {
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(path, cancellationToken: Ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.Services.Catalog.GetCatalogEntryAsync("PropertyVariants", Ct));
        ColumnPropertyBlock original = Assert.IsType<ColumnPropertyBlock>(await harness.Services.Snapshots.ReadLvPropBlockAsync(entry.TDefPage, Ct));
        var builder = ColumnPropertyBlockBuilder.FromBlock(original);
        ColumnPropertyTargetBuilder table = Assert.Single(builder.Targets, target => target.Name.Length == 0);
        if (layout == "entries")
        {
            foreach (ColumnPropertyEntry property in original.FindTableTarget()!.Entries.Where(IsRuleOrText))
            {
                table.Entries.Add(Copy(property));
            }
        }
        else
        {
            var repeated = new ColumnPropertyTargetBuilder
            {
                Name = string.Empty,
                ChunkType = table.ChunkType,
                SourceHeader = table.SourceHeader,
                SourceHeaderIsNameLength = table.SourceHeaderIsNameLength,
                TextEncoding = table.TextEncoding,
            };
            foreach (ColumnPropertyEntry property in original.FindTableTarget()!.Entries.Where(IsRuleOrText))
            {
                if (layout == "split" && property.Name != Constants.ColumnPropertyNames.ValidationText)
                {
                    continue;
                }

                repeated.Entries.Add(Copy(property));
            }

            if (layout == "split")
            {
                table.Entries.RemoveAll(property => property.Name == Constants.ColumnPropertyNames.ValidationText);
            }

            builder.Targets.Add(repeated);
        }

        await harness.Services.CatalogWriter.UpdateTablePropertiesAsync("PropertyVariants", entry.TDefPage, builder.ToBytes(harness.Database.Format), cancellationToken: Ct);
    }

    private static async Task PlantPopulatedPoolsAsync(string path, string layout)
    {
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(path, cancellationToken: Ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.Services.Catalog.GetCatalogEntryAsync("PropertyVariants", Ct));
        ColumnPropertyBlock block = Assert.IsType<ColumnPropertyBlock>(await harness.Services.Snapshots.ReadLvPropBlockAsync(entry.TDefPage, Ct));
        byte[] original = Assert.IsType<byte[]>(block.ToBytes(harness.Database.Format));
        const int poolStart = 4;
        int poolLength = BinaryPrimitives.ReadInt32LittleEndian(original.AsSpan(poolStart));
        Assert.Equal((ushort)ColumnPropertyChunkType.NamePool, BinaryPrimitives.ReadUInt16LittleEndian(original.AsSpan(poolStart + 4)));
        await using var stream = new MemoryStream();
        stream.Write(original.AsSpan(0, 4));
        if (layout == "adjacent")
        {
            int splitLength = 2 + BinaryPrimitives.ReadUInt16LittleEndian(original.AsSpan(poolStart + 6));
            WritePool(stream, original.AsSpan(poolStart + 6, splitLength));
            WritePool(stream, original.AsSpan(poolStart + 6 + splitLength, poolLength - 6 - splitLength));
            stream.Write(original.AsSpan(poolStart + poolLength));
        }
        else if (layout == "empty")
        {
            stream.Write(original.AsSpan(poolStart, poolLength));
            WritePool(stream, []);
            stream.Write(original.AsSpan(poolStart + poolLength));
        }
        else
        {
            int offset = poolStart + poolLength;
            while (offset < original.Length)
            {
                int length = BinaryPrimitives.ReadInt32LittleEndian(original.AsSpan(offset));
                stream.Write(original.AsSpan(poolStart, poolLength));
                stream.Write(original.AsSpan(offset, length));
                offset += length;
            }
        }

        await harness.Services.CatalogWriter.UpdateTablePropertiesAsync("PropertyVariants", entry.TDefPage, stream.ToArray(), cancellationToken: Ct);
    }

    private static void WritePool(MemoryStream stream, ReadOnlySpan<byte> payload)
    {
        Span<byte> header = stackalloc byte[6];
        BinaryPrimitives.WriteInt32LittleEndian(header, payload.Length + 6);
        BinaryPrimitives.WriteUInt16LittleEndian(header[4..], (ushort)ColumnPropertyChunkType.NamePool);
        stream.Write(header);
        stream.Write(payload);
    }

    private static bool IsRuleOrText(ColumnPropertyEntry property)
        => property.Name is Constants.ColumnPropertyNames.ValidationRule or Constants.ColumnPropertyNames.ValidationText;

    private static ColumnPropertyEntryBuilder Copy(ColumnPropertyEntry entry) => new()
    {
        Name = entry.Name,
        DataType = entry.DataType,
        DdlFlag = entry.DdlFlag,
        Value = [.. entry.Value],
        Padding = [.. entry.Padding],
        SourceNameIndex = entry.SourceNameIndex,
    };

    private static void AssertRule(ColumnPropertyBlock properties, string? expression, string? text)
    {
        ColumnPropertyEntry[] rules = [.. properties.Targets.Where(target => target.Name.Length == 0).SelectMany(target => target.Entries).Where(property => property.Name == Constants.ColumnPropertyNames.ValidationRule)];
        ColumnPropertyEntry[] texts = [.. properties.Targets.Where(target => target.Name.Length == 0).SelectMany(target => target.Entries).Where(property => property.Name == Constants.ColumnPropertyNames.ValidationText)];
        if (expression is null)
        {
            Assert.Empty(rules);
            Assert.Empty(texts);
            return;
        }

        Assert.NotEmpty(rules);
        Assert.NotEmpty(texts);
        Assert.All(rules, rule => Assert.Equal(expression, properties.TextEncoding!.GetString(rule.Value)));
        Assert.All(texts, value => Assert.Equal(text, properties.TextEncoding!.GetString(value.Value)));
    }
}
