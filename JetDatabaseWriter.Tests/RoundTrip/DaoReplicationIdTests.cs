namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Tests.Schema;
using Xunit;

/// <summary>Verifies native ReplicationID descriptors, defaults and mutations before and after compact.</summary>
/// <param name="output">Captures native engine evidence.</param>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoReplicationIdTests(ITestOutputHelper output)
{
    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(2);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(nameof(TestDatabases.TestV2003), "Table4")]
    [InlineData(nameof(TestDatabases.TestV2010), "Table4")]
    [InlineData(nameof(TestDatabases.TestIndexCodesV2000), "Table13")]
    [InlineData(nameof(TestDatabases.TestIndexCodesV2010), "Table13")]
    public async Task MicrosoftFixture_ReplicationIdMetadata_MatchesDao(string fixture, string tableName)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        string fixturePath = ReplicationIdDescriptorTests.FixturePath(fixture);
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: Path.GetExtension(fixturePath));
        File.Copy(fixturePath, session.SourcePath);
        string table = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(tableName);
        string script = $$$"""
            $field = $db.TableDefs({{{table}}}).Fields('data')
            Write-Output "TYPE=$($field.Type),SIZE=$($field.Size),ATTR=$($field.Attributes),DEFAULT=$($field.DefaultValue)"
            $db.Execute("INSERT INTO [" + {{{table}}} + "] ([name]) VALUES ('native-generated')", 128)
            try {
                $db.Execute("INSERT INTO [" + {{{table}}} + "] ([name], [data]) VALUES ('native-null', Null)", 128)
                Write-Output 'NULL_INSERT=ACCEPTED'
            } catch { Write-Output 'NULL_INSERT=REFUSED' }
            $rs = $db.OpenRecordset("SELECT * FROM [" + {{{table}}} + "] WHERE [name]='native-null'", 2)
            try { if (!$rs.EOF) { Write-Output "EXPLICIT_NULL=$($rs.Fields('data').Value -is [DBNull])" } } finally { $rs.Close() }
            $rs = $db.OpenRecordset("SELECT * FROM [" + {{{table}}} + "] WHERE [name]='native-generated'", 2)
            try {
                Write-Output "DATAUPDATABLE=$($rs.Fields('data').DataUpdatable)"
                $rs.Edit()
                try {
                    $rs.Fields('data').Value = '{guid {11111111-2222-3333-4444-555555555555}}'
                    $rs.Update()
                    Write-Output 'GUID_UPDATE=ACCEPTED'
                } catch { Write-Output 'GUID_UPDATE=REFUSED'; Write-Output $_.Exception.Message; $rs.CancelUpdate() }
            } finally { $rs.Close() }
            """;
        AccessRoundTripEnvironment.CompactResult result = RunDaoDatabaseThenCompact(session, script);
        this.AssertDaoSuccess(result);
        Assert.Contains("TYPE=15,SIZE=16,ATTR=1,DEFAULT=GenGUID()", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("DATAUPDATABLE=True", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("GUID_UPDATE=ACCEPTED", result.StdOut, StringComparison.Ordinal);
        Assert.Contains(tableName == "Table4" ? "NULL_INSERT=REFUSED" : "NULL_INSERT=ACCEPTED", result.StdOut, StringComparison.Ordinal);
        await AssertReplicationDescriptorsAsync(session.SourcePath, tableName);
        await AssertReplicationDescriptorsAsync(session.CompactedPath, tableName);
        File.Copy(session.CompactedPath, session.SourcePath, overwrite: true);
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await writer.AddColumnAsync(tableName, new ColumnDefinition("Spare", typeof(int)), Ct);
            await writer.RenameColumnAsync(tableName, "name", "Label", Ct);
            await writer.DropColumnAsync(tableName, "Spare", Ct);
            await writer.RenameColumnAsync(tableName, "Label", "name", Ct);
        }

        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            Assert.Equal(1, await writer.InsertRowsAsync(tableName, (FixtureRow[])[new() { Name = "writer-poco" }], Ct));
            if (tableName == "Table13")
            {
                Assert.Equal(1, await writer.InsertRowsAsync(tableName, (RowValues[])[new() { ["name"] = "writer-null", ["data"] = null }], Ct));
            }
        }

        if (tableName == "Table4")
        {
            byte[] before = await File.ReadAllBytesAsync(session.SourcePath, Ct);
            await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
            {
                await Assert.ThrowsAsync<JetDatabaseWriter.Exceptions.JetConstraintException>(async () => await writer.InsertRowsAsync(tableName, (RowValues[])[new() { ["name"] = "writer-null", ["data"] = null }], Ct));
            }

            Assert.Equal(before, await File.ReadAllBytesAsync(session.SourcePath, Ct));
        }

        await using (AccessReader reader = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            int seen = 0;
            await foreach (object[] row in reader.Rows(tableName, cancellationToken: Ct))
            {
                if (row[0] is "writer-poco")
                {
                    Assert.NotEqual(Guid.Empty, Assert.IsType<Guid>(row[1]));
                    seen++;
                }
            }

            Assert.Equal(1, seen);
        }

        string verify = $$$"""
            $rs = $db.OpenRecordset("SELECT * FROM [" + {{{table}}} + "] WHERE [name]='writer-poco'", 2)
            try { Write-Output "WRITER_POCO_GUID=$($rs.Fields('data').Value)" } finally { $rs.Close() }
            $db.Execute("INSERT INTO [" + {{{table}}} + "] ([name]) VALUES ('native-after-rewrite')", 128)
            Write-Output 'REWRITE_INSERT=ACCEPTED'
            """;
        AccessRoundTripEnvironment.CompactResult rewritten = RunDaoDatabaseThenCompact(session, verify);
        this.AssertDaoSuccess(rewritten);
        Assert.Contains("REWRITE_INSERT=ACCEPTED", rewritten.StdOut, StringComparison.Ordinal);
        await AssertReplicationDescriptorsAsync(session.CompactedPath, tableName);
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DaoAndWriter_ReplicationIdColumns_SurviveReopenSchemaEditsAndCompact(DatabaseFormat format)
    {
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: format == DatabaseFormat.Jet4Mdb ? ".mdb" : ".accdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        int options = format == DatabaseFormat.Jet4Mdb ? 64 : 128;
        string setup = $$$"""
            $db = $engine.CreateDatabase({{{source}}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{{options}}})
            try {
                $table = $db.CreateTableDef('NativeIds')
                $field = $table.CreateField('Id', 15)
                $field.Attributes = 8193
                $field.DefaultValue = 'GenGUID()'
                $table.Fields.Append($field)
                $field = $table.CreateField('DefaultId', 15)
                $field.DefaultValue = 'GenGUID()'
                $table.Fields.Append($field)
                $table.Fields.Append($table.CreateField('Number', 4))
                $index = $table.CreateIndex('PrimaryKey')
                $index.Primary = $true
                $index.Unique = $true
                $index.Fields.Append($index.CreateField('Id'))
                $table.Indexes.Append($index)
                $db.TableDefs.Append($table)
                $db.Execute('INSERT INTO [NativeIds] ([Number]) VALUES (1)', 128)
                Write-Output "AUTO_DEFAULT=$($db.TableDefs('NativeIds').Fields('Id').DefaultValue)"
                Write-Output "PLAIN_DEFAULT=$($db.TableDefs('NativeIds').Fields('DefaultId').DefaultValue)"
                try {
                    $db.Execute('UPDATE [NativeIds] SET [Id]={guid {11111111-2222-3333-4444-555555555555}} WHERE [Number]=1', 128)
                    Write-Output 'AUTO_UPDATE=ACCEPTED'
                } catch { Write-Output 'AUTO_UPDATE=REFUSED'; Write-Output $_.Exception.Message }
                $db.Execute('UPDATE [NativeIds] SET [DefaultId]={guid {11111111-2222-3333-4444-555555555555}} WHERE [Number]=1', 128)
                Write-Output 'PLAIN_UPDATE=ACCEPTED'
            } finally { $db.Close() }
            """;
        AccessRoundTripEnvironment.CompactResult created = RunDaoScript(session, setup);
        this.AssertDaoSuccess(created);

        Assert.Contains("PLAIN_UPDATE=ACCEPTED", created.StdOut, StringComparison.Ordinal);
        Assert.Contains("AUTO_UPDATE=ACCEPTED", created.StdOut, StringComparison.Ordinal);
        await this.AssertGuidTableAsync(session.SourcePath, "NativeIds", 1, 0x13);
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await writer.CreateTableAsync("LibraryIds", [new ColumnDefinition("Id", typeof(Guid)) { IsPrimaryKey = true, IsAutoIncrement = true }, new ColumnDefinition("DefaultId", typeof(Guid)) { DefaultValueExpression = "GenGUID()" }, new ColumnDefinition("Number", typeof(int))], Ct);
        }

        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            Assert.Equal(1, await writer.InsertRowsAsync("LibraryIds", (ReplicationRow[])[new() { Number = 1 }], Ct));
            await writer.AddColumnAsync("LibraryIds", new ColumnDefinition("Spare", typeof(int)), Ct);
            await writer.RenameColumnAsync("LibraryIds", "Number", "Value", Ct);
            await writer.DropColumnAsync("LibraryIds", "Spare", Ct);
        }

        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            Assert.Equal(1, await writer.InsertRowsAsync("LibraryIds", (RowValues[])[new() { ["Value"] = 2 }], Ct));
            Assert.Equal(1, await writer.UpdateRowsAsync("LibraryIds", RowCriteria.Where("Value", 1), new RowValues { ["Id"] = Guid.Parse("22222222-3333-4444-5555-666666666666") }, Ct));
        }

        await this.AssertGuidTableAsync(session.SourcePath, "LibraryIds", 2, 0x43);
        const string nativeInsert = """
            Write-Output "LIBRARY_DEFAULT=$($db.TableDefs('LibraryIds').Fields('Id').DefaultValue),PLAIN_DEFAULT=$($db.TableDefs('LibraryIds').Fields('DefaultId').DefaultValue)"
            $db.Execute('INSERT INTO [LibraryIds] ([Value]) VALUES (3)', 128)
            $db.Execute('UPDATE [LibraryIds] SET [Id]={guid {33333333-4444-5555-6666-777777777777}} WHERE [Value]=2', 128)
            Write-Output 'LIBRARY_GUID_UPDATE=ACCEPTED'
            """;
        AccessRoundTripEnvironment.CompactResult compacted = RunDaoDatabaseThenCompact(session, nativeInsert);
        this.AssertDaoSuccess(compacted);
        Assert.Contains("LIBRARY_DEFAULT=GenGUID()", compacted.StdOut, StringComparison.Ordinal);
        Assert.Contains("LIBRARY_GUID_UPDATE=ACCEPTED", compacted.StdOut, StringComparison.Ordinal);
        await this.AssertGuidTableAsync(session.CompactedPath, "NativeIds", 1, 0x13);
        await this.AssertGuidTableAsync(session.CompactedPath, "LibraryIds", 3, 0x43);
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData("")]
    [InlineData("{guid {44444444-5555-6666-7777-888888888888}}")]
    public async Task Dao_ChangingGuidDefault_ControlsOmittedAssignments(string defaultExpression)
    {
        await using var session = AccessRoundTripSession.CreateEmpty();
        ArgumentNullException.ThrowIfNull(defaultExpression);
        File.Copy(TestDatabases.TestIndexCodesV2010, session.SourcePath);
        string nativeDefault = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(defaultExpression);
        string script = $$"""
            $db.TableDefs('Table13').Fields('data').DefaultValue = {{nativeDefault}}
            $db.Execute("INSERT INTO [Table13] ([name]) VALUES ('native-no-default')", 128)
            $rs = $db.OpenRecordset("SELECT * FROM [Table13] WHERE [name]='native-no-default'", 2)
            try { Write-Output "OMITTED_NULL=$($rs.Fields('data').Value -is [DBNull])" } finally { $rs.Close() }
            """;
        AccessRoundTripEnvironment.CompactResult result = RunDaoDatabaseThenCompact(session, script);
        this.AssertDaoSuccess(result);
        Assert.Contains(defaultExpression.Length == 0 ? "OMITTED_NULL=True" : "OMITTED_NULL=False", result.StdOut, StringComparison.Ordinal);
        ColumnInfo column = Assert.Single(await ReplicationIdDescriptorTests.ReadColumnsAsync(session.CompactedPath, "Table13"), value => value.Name == "data");
        Assert.Equal((byte)0x43, column.Flags);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.CompactedPath, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            Assert.Equal(1, await writer.DeleteRowsAsync("Table13", RowCriteria.Where("name", "native-no-default"), Ct));
            await writer.AddColumnAsync("Table13", new ColumnDefinition("Spare", typeof(int)), Ct);
            await writer.RenameColumnAsync("Table13", "name", "Label", Ct);
            await writer.DropColumnAsync("Table13", "Spare", Ct);
        }

        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.CompactedPath, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            Assert.Equal(1, await writer.InsertRowsAsync("Table13", (RowValues[])[new() { ["Label"] = "writer-no-default" }], Ct));
        }

        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct);
        int seen = 0;
        await foreach (object[] row in reader.Rows("Table13", cancellationToken: Ct))
        {
            if (row[0] is "writer-no-default")
            {
                seen++;
                if (defaultExpression.Length == 0)
                {
                    Assert.IsType<DBNull>(row[1]);
                }
                else
                {
                    Assert.Equal(Guid.Parse("44444444-5555-6666-7777-888888888888"), Assert.IsType<Guid>(row[1]));
                }
            }
        }

        Assert.Equal(1, seen);
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData("none")]
    [InlineData("add")]
    [InlineData("rename")]
    [InlineData("add-drop")]
    [InlineData("all")]
    public async Task MicrosoftGuidFixture_SchemaOperation_PreservesNativeCompact(string operation)
    {
        await using var session = AccessRoundTripSession.CreateEmpty();
        File.Copy(TestDatabases.TestV2010, session.SourcePath);
        this.AssertDaoSuccess(RunDaoDatabaseThenCompact(session, "$db.Execute(\"INSERT INTO [Table4] ([name]) VALUES ('native-generated')\", 128)"));
        File.Copy(session.CompactedPath, session.SourcePath, overwrite: true);
        Guid[] originalIds = await ReadFixtureGuidsAsync(session.SourcePath);
        Assert.NotEmpty(originalIds);
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            if (operation is "add" or "add-drop" or "all")
            {
                await writer.AddColumnAsync("Table4", new ColumnDefinition("Spare", typeof(int)), Ct);
            }

            if (operation is "rename" or "all")
            {
                await writer.RenameColumnAsync("Table4", "name", "Label", Ct);
            }

            if (operation is "add-drop" or "all")
            {
                await writer.DropColumnAsync("Table4", "Spare", Ct);
            }

            if (operation == "all")
            {
                await writer.RenameColumnAsync("Table4", "Label", "name", Ct);
            }
        }

        const string read = """
            $rs = $db.OpenRecordset('SELECT [data] FROM [Table4]', 2)
            try {
                if ($rs.EOF) { throw 'Rewritten table has no rows.' }
                Write-Output "REWRITTEN_GUID=$($rs.Fields('data').Value)"
            } finally { $rs.Close() }
            """;
        this.AssertDaoSuccess(RunDaoDatabaseThenCompact(session, read));
        await AssertReplicationDescriptorsAsync(session.CompactedPath, "Table4");
        Assert.Equal(originalIds, await ReadFixtureGuidsAsync(session.CompactedPath));
    }

    private static async Task<Guid[]> ReadFixtureGuidsAsync(string path)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, Ct);
        var ids = new List<Guid>();
        await foreach (object[] row in reader.Rows("Table4", cancellationToken: Ct))
        {
            ids.Add(Assert.IsType<Guid>(row[1]));
        }

        return ids.Order().ToArray();
    }

    private static AccessRoundTripEnvironment.CompactResult RunDaoDatabaseThenCompact(AccessRoundTripSession session, string script)
    {
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string destination = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string engineScript = $$"""
            $db = $engine.OpenDatabase({{source}})
            try {
                {{script}}
            } finally { $db.Close() }
            if (Test-Path {{destination}}) { Remove-Item -LiteralPath {{destination}} -Force }
            $engine.CompactDatabase({{source}}, {{destination}})
            """;
        return RunDaoScript(session, engineScript);
    }

    private static AccessRoundTripEnvironment.CompactResult RunDaoScript(AccessRoundTripSession session, string script)
        => AccessRoundTripEnvironment.RunDaoScript(
            $$"""
            $ErrorActionPreference = 'Stop'
            $previousDirectory = [Environment]::CurrentDirectory
            Push-Location $PSScriptRoot
            [Environment]::CurrentDirectory = $PSScriptRoot
            try {
                $engine = New-Object -ComObject DAO.DBEngine.120
                try {
                    {{script}}
                } finally {
                    [System.Runtime.InteropServices.Marshal]::ReleaseComObject($engine) | Out-Null
                    [GC]::Collect(); [GC]::WaitForPendingFinalizers()
                }
            } finally {
                [Environment]::CurrentDirectory = $previousDirectory
                Pop-Location
            }
            """,
            session.WorkDir,
            DaoTimeout);

    private static async Task AssertReplicationDescriptorsAsync(string path, string tableName)
    {
        ColumnInfo column = Assert.Single(await ReplicationIdDescriptorTests.ReadColumnsAsync(path, tableName), value => value.Type == ColumnType.GuidType);
        Assert.Equal((byte)0x43, column.Flags);
        Assert.True(column.IsAutoNumber);
        Assert.True(column.IsFixed);
        Assert.Equal(16, column.Size);
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, cancellationToken: Ct);
        ColumnMetadata metadata = Assert.Single(await reader.GetColumnMetadataAsync(tableName, Ct), value => value.Name == column.Name);
        Assert.Equal("GenGUID()", metadata.DefaultValueExpression);
    }

    private async Task AssertGuidTableAsync(string path, string tableName, int expectedCount, byte expectedFlags)
    {
        ColumnInfo[] columns = await ReplicationIdDescriptorTests.ReadColumnsAsync(path, tableName);
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(path, cancellationToken: Ct))
        {
            long page = Assert.Single(await harness.Services.Catalog.GetUserTablesAsync(Ct), table => table.Name == tableName).TDefPage;
            ColumnPropertyBlock properties = Assert.IsType<ColumnPropertyBlock>(await harness.Services.Catalog.ReadLvPropForTableAsync(page, Ct));
            foreach (string name in new[] { "Id", "DefaultId" })
            {
                ColumnPropertyTarget target = Assert.IsType<ColumnPropertyTarget>(properties.FindTarget(name));
                ColumnPropertyEntry entry = Assert.IsType<ColumnPropertyEntry>(target.Find(Constants.ColumnPropertyNames.DefaultValue));
                Assert.Equal(ColumnType.MemoType, entry.DataType);
                Assert.Equal((byte)1, entry.DdlFlag);
                output.WriteLine($"{tableName}.{name} default type={entry.DataType},ddl={entry.DdlFlag}");
            }
        }

        Assert.Equal(expectedFlags, Assert.Single(columns, column => column.Name == "Id").Flags);
        Assert.Equal((byte)0x03, Assert.Single(columns, column => column.Name == "DefaultId").Flags);
        Assert.False(Assert.Single(columns, column => column.Name == "DefaultId").IsAutoNumber);
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, cancellationToken: Ct);
        IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync(tableName, Ct);
        Assert.Equal("GenGUID()", Assert.Single(metadata, column => column.Name == "Id").DefaultValueExpression);
        Assert.Equal("GenGUID()", Assert.Single(metadata, column => column.Name == "DefaultId").DefaultValueExpression);
        var ids = new List<Guid>();
        await foreach (object[] row in reader.Rows(tableName, cancellationToken: Ct))
        {
            ids.Add(Assert.IsType<Guid>(row[0]));
            Assert.NotEqual(Guid.Empty, Assert.IsType<Guid>(row[1]));
        }

        Assert.Equal(expectedCount, ids.Count);
        Assert.Equal(expectedCount, ids.Distinct().Count());
        Assert.DoesNotContain(Guid.Empty, ids);
    }

    private sealed class FixtureRow
    {
        public string Name { get; set; } = string.Empty;

        public Guid Data { get; set; }
    }

    private sealed class ReplicationRow
    {
        public Guid Id { get; set; }

        public int Number { get; set; }
    }

    private void AssertDaoSuccess(AccessRoundTripEnvironment.CompactResult result)
    {
        output.WriteLine(result.StdOut);
        Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
    }
}
