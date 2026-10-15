namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Jet3 schema rewrites retain native catalog ownership and table permissions.</summary>
[TestClass(DisableParallelization = true)]
public sealed class NativeJet3SchemaRewriteTests
{
    [Theory]
    [InlineData("add", "NativeFixedTextJet3Cp1252.mdb")]
    [InlineData("drop", "NativeFixedTextJet3Cp1252.mdb")]
    [InlineData("rename", "NativeFixedTextJet3Cp1252.mdb")]
    [InlineData("add", "NativeFixedTextJet3Cp932.mdb")]
    [InlineData("drop", "NativeFixedTextJet3Cp932.mdb")]
    [InlineData("rename", "NativeFixedTextJet3Cp932.mdb")]
    public async Task NativeJet3_RewritePreservesObjectIdentityAndSecurity(string operation, string fixture)
    {
        string path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "FixedText", fixture);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);
        SecuritySnapshot before = await ReadSecurityAsync(stream);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            switch (operation)
            {
                case "add":
                    await writer.AddColumnAsync("FixedText", new ColumnDefinition("Added", typeof(int)), TestContext.Current.CancellationToken);
                    break;
                case "drop":
                    await writer.DropColumnAsync("FixedText", "Spare", TestContext.Current.CancellationToken);
                    break;
                default:
                    await writer.RenameColumnAsync("FixedText", "Spare", "Renamed", TestContext.Current.CancellationToken);
                    break;
            }
        }

        SecuritySnapshot after = await ReadSecurityAsync(stream);
        Assert.Equal(before.ObjectId, after.ObjectId);
        Assert.Equal(before.Owner, after.Owner);
        Assert.Equal(before.Permissions, after.Permissions);
        Assert.Equal(before.CatalogKeys, after.CatalogKeys);
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData("add", false)]
    [InlineData("drop", false)]
    [InlineData("rename", false)]
    [InlineData("add", true)]
    [InlineData("drop", true)]
    [InlineData("rename", true)]
    public async Task Dao36AuthoredLongs_RewriteSurvivesNativeReadWriteAndCompact(string operation, bool wide)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativeJet3Schema", databaseExtension: ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        int originalRows = wide ? 300 : 1;
        string extraColumns = wide ? string.Concat(Enumerable.Range(0, 100).Select(index => $", [C{index}] LONG")) : string.Empty;
        PowerShellRunResult created = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{source}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 32)
            try {
                $db.Execute('CREATE TABLE [Longs] ([Id] LONG, [Spare] LONG{{extraColumns}})', 128)
                for ($id = 1; $id -le {{originalRows}}; $id++) { $db.Execute(('INSERT INTO [Longs] ([Id],[Spare]) VALUES (' + $id + ',7)'), 128) }
                if ({{(wide ? "$true" : "$false")}}) {
                    for ($i = 0; $i -lt 16; $i++) { $db.Execute(('CREATE INDEX [I' + $i + '] ON [Longs] ([C' + $i + '])'), 128) }
                }
                Write-Output ('OWNER=' + $db.Containers('Tables').Documents('Longs').Owner)
            } finally { $db.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(2));
        Assert.True(created.ExitCode == 0, $"DAO36 create failed: {created.StandardOutput}\n{created.StandardError}");
        byte[] originalImage = await File.ReadAllBytesAsync(session.SourcePath, TestContext.Current.CancellationToken);
        await using (AccessWriter refused = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            await Assert.ThrowsAsync<JetObjectExistsException>(async () => await refused.RenameColumnAsync("Longs", "Spare", "Id", TestContext.Current.CancellationToken));
        }

        Assert.Equal(originalImage, await File.ReadAllBytesAsync(session.SourcePath, TestContext.Current.CancellationToken));
        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            switch (operation)
            {
                case "add":
                    await writer.AddColumnAsync("Longs", new ColumnDefinition("Added", typeof(int)), TestContext.Current.CancellationToken);
                    break;
                case "drop":
                    await writer.DropColumnAsync("Longs", "Spare", TestContext.Current.CancellationToken);
                    break;
                default:
                    await writer.RenameColumnAsync("Longs", "Spare", "Renamed", TestContext.Current.CancellationToken);
                    break;
            }
        }

        int scalarColumns = operation switch { "add" => 3, "drop" => 1, _ => 2 };
        int columns = (wide ? 100 : 0) + scalarColumns;
        if (wide)
        {
            await using AccessWriter reopened = await session.OpenWriterAsync(TestContext.Current.CancellationToken);
            for (int id = 1001; id <= 1040; id++)
            {
                await reopened.InsertRowAsync("Longs", new RowValues { ["Id"] = id }, TestContext.Current.CancellationToken);
            }
        }

        int expectedRows = originalRows + (wide ? 40 : 0);
        PowerShellRunResult verified = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db = $engine.OpenDatabase({{source}}, $false, $true)
            try {
                $count = $db.TableDefs('Longs').Fields.Count
                Write-Output ('FIELDS=' + $count)
                if ($count -ne {{columns}}) { throw 'Wrong field count after rewrite.' }
                Write-Output ('OWNER=' + $db.Containers('Tables').Documents('Longs').Owner)
                $rs = $db.OpenRecordset('SELECT * FROM [Longs] WHERE [Id]=1')
                try {
                    if ($rs.EOF -or $rs.Fields('Id').Value -ne 1) { throw 'Lost original row.' }
                } finally { $rs.Close() }
            } finally { $db.Close() }
            $db = $engine.OpenDatabase({{source}})
            try {
                $db.Execute('INSERT INTO [Longs] ([Id]) VALUES (1000000)', 128)
                $db.Execute('UPDATE [Longs] SET [Id]=1000001 WHERE [Id]=1000000', 128)
                $db.Execute('DELETE FROM [Longs] WHERE [Id]=1000001', 128)
                $db.Execute('UPDATE [Longs] SET [Id]=1000002 WHERE [Id]=1', 128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{compacted}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 32)
            $db = $engine.OpenDatabase({{compacted}}, $false, $true)
            try {
                if ($db.TableDefs('Longs').Fields.Count -ne {{columns}}) { throw 'Lost compacted fields.' }
                $rs = $db.OpenRecordset('SELECT * FROM [Longs] WHERE [Id]=1000002')
                try {
                    if ($rs.EOF -or $rs.Fields('Id').Value -ne 1000002) { throw 'Lost compacted row.' }
                } finally { $rs.Close() }
                $rs = $db.OpenRecordset('SELECT Count(*) AS N FROM [Longs]')
                try {
                    if ($rs.Fields('N').Value -ne {{expectedRows}}) { throw 'Wrong compacted row count.' }
                } finally { $rs.Close() }
            } finally { $db.Close() }
            Write-Output 'NATIVE_READ_WRITE_COMPACT_OK'
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(2));
        Assert.True(verified.ExitCode == 0, $"DAO36 failed: {verified.StandardOutput}\n{verified.StandardError}");
        Assert.Contains(created.StandardOutput.Trim(), verified.StandardOutput, StringComparison.Ordinal);
        Assert.Contains("NATIVE_READ_WRITE_COMPACT_OK", verified.StandardOutput, StringComparison.Ordinal);
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        using DataTable rows = await reader.ReadTableAsync("Longs", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(expectedRows, rows.Rows.Count);
        Assert.Single(rows.Select("Id = 1000002"));
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData("NativeFixedTextJet3Cp1252.mdb", "add")]
    [InlineData("NativeFixedTextJet3Cp1252.mdb", "drop")]
    [InlineData("NativeFixedTextJet3Cp1252.mdb", "rename")]
    [InlineData("NativeFixedTextJet3Cp932.mdb", "add")]
    [InlineData("NativeFixedTextJet3Cp932.mdb", "drop")]
    [InlineData("NativeFixedTextJet3Cp932.mdb", "rename")]
    public async Task NativeFixedText_RewriteRetainsDaoValuesThroughCompact(string fixture, string operation)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativeJet3FixedText", databaseExtension: ".mdb");
        await File.WriteAllBytesAsync(session.SourcePath, await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "FixedText", fixture), TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string locale = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(string.Equals(fixture, "NativeFixedTextJet3Cp932.mdb", StringComparison.Ordinal) ? ";LANGID=0x0409;CP=932;COUNTRY=0" : ";LANGID=0x0409;CP=1252;COUNTRY=0");
        const string read = """
            $rs = $db.OpenRecordset('SELECT [Id],[Value] FROM [FixedText] ORDER BY [Id]')
            try {
                while (!$rs.EOF) {
                    $value = $rs.Fields('Value').Value
                    $encoded = if ($value -is [DBNull]) { 'NULL' } else { [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes([string]$value)) }
                    Write-Output ($rs.Fields('Id').Value.ToString() + ':' + $encoded)
                    $rs.MoveNext()
                }
            } finally { $rs.Close() }
            """;
        PowerShellRunResult before = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db = $engine.OpenDatabase({{source}}, $false, $true)
            try {
                {{read}}
            } finally { $db.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(2));
        Assert.True(before.ExitCode == 0, $"DAO36 baseline read failed: {before.StandardOutput}\n{before.StandardError}");
        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            switch (operation)
            {
                case "add":
                    await writer.AddColumnAsync("FixedText", new ColumnDefinition("Added", typeof(int)), TestContext.Current.CancellationToken);
                    break;
                case "drop":
                    await writer.DropColumnAsync("FixedText", "Spare", TestContext.Current.CancellationToken);
                    break;
                default:
                    await writer.RenameColumnAsync("FixedText", "Spare", "Renamed", TestContext.Current.CancellationToken);
                    break;
            }
        }

        PowerShellRunResult after = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db = $engine.OpenDatabase({{source}}, $false, $true)
            try {
                {{read}}
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{compacted}}, {{locale}}, 32)
            $db = $engine.OpenDatabase({{compacted}}, $false, $true)
            try {
                {{read}}
            } finally { $db.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(2));
        Assert.True(after.ExitCode == 0, $"DAO36 rewritten read/compact failed: {after.StandardOutput}\n{after.StandardError}");
        string expected = before.StandardOutput.Trim();
        Assert.NotEmpty(expected);
        Assert.Equal(expected + Environment.NewLine + expected, after.StandardOutput.Trim());
    }

    [Fact]
    public async Task NativeJet3_DuplicateCatalogKeysRefuseRewriteWithoutChangingBytes()
    {
        byte[] image = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "FixedText", "NativeFixedTextJet3Cp1252.mdb"), TestContext.Current.CancellationToken);
        var format = JetFormat.FromHeader(image);
        byte[] definition = image.AsSpan(2 * format.PageSize, format.PageSize).ToArray();
        TDefCounts counts = TDefCodec.ReadCounts(format, definition);
        int start = IndexCatalogReader.LocateRealIdxDescStart(format, definition, counts.ColumnCount, counts.RealIndexCount);
        Assert.True(start >= 0);
        int root = JetTypeInfo.Ri32(definition, format.Index.FirstDpAbsoluteOffset(format.Index.RealIdxPhysOffset(start, 0)));
        byte[] leaf = image.AsSpan(root * format.PageSize, format.PageSize).ToArray();
        List<JetDatabaseWriter.Indexes.Models.IndexEntry> entries = IndexPageCodec.DecodeLeafEntries(format.IndexPage, leaf, format.PageSize);
        Assert.True(entries.Count > 1);
        entries[1] = entries[1] with { Key = entries[0].Key };
        byte[] corrupted = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, entries, enablePrefixCompression: false);
        corrupted.CopyTo(image, root * format.PageSize);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(image, TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await writer.AddColumnAsync("FixedText", new ColumnDefinition("Added", typeof(int)), TestContext.Current.CancellationToken));
        }

        Assert.Equal(image, stream.ToArray());
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao36AuthoredLargeCatalog_ColumnRewriteRetainsMultiLevelCatalogKeys()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativeJet3LargeCatalog", databaseExtension: ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        PowerShellRunResult created = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{source}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 32)
            try {
                $db.Execute('CREATE TABLE [Longs] ([Id] LONG, [Spare] LONG)', 128)
                $db.Execute('INSERT INTO [Longs] ([Id],[Spare]) VALUES (1,7)', 128)
                for ($id=0; $id -lt 150; $id++) {
                    $name = 'Extra' + $id.ToString('D4') + ('X' * 40)
                    $db.Execute(('CREATE TABLE [' + $name + '] ([Id] LONG)'), 128)
                }
            } finally { $db.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(2));
        Assert.True(created.ExitCode == 0, $"DAO36 large catalog create failed: {created.StandardOutput}\n{created.StandardError}");
        await using (AccessReader harness = await TestDatabase.OpenReaderAsync(session.SourcePath, cancellationToken: TestContext.Current.CancellationToken))
        {
            JetFormat format = harness.Database.Format;
            byte[] definition = Assert.IsType<byte[]>(await harness.Database.TableDefs.ReadTDefBytesAsync(2, TestContext.Current.CancellationToken));
            TDefCounts counts = TDefCodec.ReadCounts(format, definition);
            int start = IndexCatalogReader.LocateRealIdxDescStart(format, definition, counts.ColumnCount, counts.RealIndexCount);
            bool multiLevel = false;
            for (int index = 0; index < counts.RealIndexCount; index++)
            {
                int root = JetTypeInfo.Ri32(definition, format.Index.FirstDpAbsoluteOffset(format.Index.RealIdxPhysOffset(start, index)));
                multiLevel |= (await harness.ReadPageCopyAsync(root, TestContext.Current.CancellationToken))[0] == Constants.PageTypes.IndexIntermediate;
            }

            Assert.True(multiLevel);
        }

        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            await writer.RenameColumnAsync("Longs", "Spare", "Renamed", TestContext.Current.CancellationToken);
        }

        PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db = $engine.OpenDatabase({{source}}, $false, $true)
            try {
                if ($db.TableDefs('Longs').Fields.Count -ne 2) { throw 'Lost large-catalog fields.' }
                $rs = $db.OpenRecordset('Longs')
                try { if ($rs.Fields('Renamed').Value -ne 7) { throw 'Lost large-catalog value.' } } finally { $rs.Close() }
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{compacted}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 32)
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, $"DAO36 large catalog rewrite failed: {result.StandardOutput}\n{result.StandardError}");
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        Assert.Equal(151, (await reader.ListTablesAsync(TestContext.Current.CancellationToken)).Count);
        using DataTable rows = await reader.ReadTableAsync("Longs", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(7, Assert.Single(rows.Select())["Renamed"]);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao36AuthoredLargeRows_RewritePromotesOwnedMapAndSurvivesCompact()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativeJet3MapPromotion", databaseExtension: ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        PowerShellRunResult created = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{source}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 32)
            try {
                $db.Execute('CREATE TABLE [Longs] ([Id] LONG, [Spare] LONG, [P0] TEXT(255), [P1] TEXT(255), [P2] TEXT(255), [P3] TEXT(255), [P4] TEXT(255), [P5] TEXT(255), [P6] TEXT(255))', 128)
                $padding = "'" + ('X' * 255) + "'"
                $values = $padding + (',' + $padding) * 6
                $workspace = $engine.Workspaces(0)
                $workspace.BeginTrans()
                for ($id = 1; $id -le 1030; $id++) { $db.Execute(('INSERT INTO [Longs] VALUES (' + $id + ',7,' + $values + ')'), 128) }
                $workspace.CommitTrans()
            } finally { $db.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(2));
        Assert.True(created.ExitCode == 0, $"DAO36 large row create failed: {created.StandardOutput}\n{created.StandardError}");
        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            await writer.AddColumnAsync("Longs", new ColumnDefinition("Added", typeof(int)), TestContext.Current.CancellationToken);
        }

        await using (AccessReader harness = await TestDatabase.OpenReaderAsync(session.SourcePath, cancellationToken: TestContext.Current.CancellationToken))
        {
            JetFormat format = harness.Database.Format;
            long root = Assert.IsType<JetDatabaseWriter.Catalog.Models.CatalogEntry>(await harness.GetCatalogEntryAsync("Longs", TestContext.Current.CancellationToken)).TDefPage;
            byte[] definition = await harness.ReadPageCopyAsync(root, TestContext.Current.CancellationToken);
            int mapPage = UsageMap.ReadUInt24(definition, format.TDef.UsedPagesPage);
            byte[] map = await harness.ReadPageCopyAsync(mapPage, TestContext.Current.CancellationToken);
            Assert.True(UsageMap.TryGetRowBound(map, format.DataPage, format.PageSize, definition[format.TDef.UsedPages], out RowBound bound));
            Assert.Equal(Constants.UsageMap.ReferenceMapType, map[bound.RowStart]);
        }

        PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db = $engine.OpenDatabase({{source}}, $false, $true)
            try {
                $rs = $db.OpenRecordset('SELECT Count(*) AS N FROM [Longs]')
                try { if ($rs.Fields('N').Value -ne 1030) { throw 'Lost promoted-map rows.' } } finally { $rs.Close() }
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{compacted}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 32)
            $db = $engine.OpenDatabase({{compacted}}, $false, $true)
            try {
                $rs = $db.OpenRecordset('SELECT Count(*) AS N FROM [Longs]')
                try { if ($rs.Fields('N').Value -ne 1030) { throw 'Lost compacted promoted-map rows.' } } finally { $rs.Close() }
            } finally { $db.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, $"DAO36 promoted-map rewrite failed: {result.StandardOutput}\n{result.StandardError}");
    }

    private static async Task<SecuritySnapshot> ReadSecurityAsync(MemoryStream stream)
    {
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        using DataTable objects = await reader.ReadTableAsync("MSysObjects", cancellationToken: TestContext.Current.CancellationToken);
        using DataTable aces = await reader.ReadTableAsync("MSysACEs", cancellationToken: TestContext.Current.CancellationToken);
        DataRow table = Assert.Single(objects.AsEnumerable(), row => Equals(row["Name"], "FixedText"));
        int id = Assert.IsType<int>(table["Id"]);
        string[] permissions = aces.AsEnumerable().Where(row => Equals(row["ObjectId"], id))
            .Select(row => Convert.ToHexString(Assert.IsType<byte[]>(row["SID"])) + ":" + row["ACM"] + ":" + row["FInheritable"])
            .Order(StringComparer.Ordinal).ToArray();
        Assert.NotEmpty(permissions);
        return new SecuritySnapshot(id, Assert.IsType<byte[]>(table["Owner"]), permissions, await ReadStoredCatalogKeysAsync(stream));
    }

    private static async Task<string[]> ReadStoredCatalogKeysAsync(MemoryStream stream)
    {
        stream.Position = 0;
        await using AccessReader harness = await TestDatabase.OpenReaderAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        JetFormat format = harness.Database.Format;
        byte[] definition = Assert.IsType<byte[]>(await harness.Database.TableDefs.ReadTDefBytesAsync(2, TestContext.Current.CancellationToken));
        TDefCounts counts = TDefCodec.ReadCounts(format, definition);
        int start = IndexCatalogReader.LocateRealIdxDescStart(format, definition, counts.ColumnCount, counts.RealIndexCount);
        Assert.True(start >= 0);
        var keys = new List<string>();
        for (int index = 0; index < counts.RealIndexCount; index++)
        {
            int root = JetTypeInfo.Ri32(definition, format.Index.FirstDpAbsoluteOffset(format.Index.RealIdxPhysOffset(start, index)));
            byte[] leaf = await harness.ReadPageCopyAsync(root, TestContext.Current.CancellationToken);
            Assert.Equal(Constants.PageTypes.IndexLeaf, leaf[0]);
            keys.AddRange(IndexPageCodec.DecodeLeafEntries(format.IndexPage, leaf, format.PageSize).Select(entry => index + ":" + Convert.ToHexString(entry.Key)));
        }

        return keys.Order(StringComparer.Ordinal).ToArray();
    }

    private sealed record SecuritySnapshot(int ObjectId, byte[] Owner, string[] Permissions, string[] CatalogKeys);
}
