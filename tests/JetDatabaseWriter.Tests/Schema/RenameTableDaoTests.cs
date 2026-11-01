namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Native controls qualify table rename metadata, relationships and CompactDatabase.</summary>
public sealed class RenameTableDaoTests
{
    /// <summary>Gets every format and statement mode for native frozen query metadata.</summary>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes => WriteModes.Combine(DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task FrozenNativeQueries_TableRenameRetainsDefinitionsAndPermissions(DatabaseFormat format, WriteMode mode)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.FrozenTableRename", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string fixture = format switch
        {
            DatabaseFormat.Jet3Mdb => "NativeSqlSpecificQueriesV1997.mdb",
            DatabaseFormat.Jet4Mdb => "NativeSqlSpecificQueriesV2000.mdb",
            DatabaseFormat.AceAccdb => "NativeSqlSpecificQueriesV2007.accdb",
            _ => throw new ArgumentOutOfRangeException(nameof(format)),
        };
        File.Copy(Path.Combine(AppContext.BaseDirectory, "Databases", "SqlSpecificQueries", fixture), session.SourcePath);
        string[] queries = await ReadMetadataAsync(session.SourcePath, "MSysQueries");
        string[] permissions = await ReadMetadataAsync(session.SourcePath, "MSysACEs");
        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, WriteModes.WriterOptions(mode), Ct))
        {
            await WriteModes.RunAsync(writer, mode, async () => await writer.RenameTableAsync("T", "Renamed T", Ct), Ct);
        }

        Assert.Equal(queries, await ReadMetadataAsync(session.SourcePath, "MSysQueries"));
        Assert.Equal(permissions, await ReadMetadataAsync(session.SourcePath, "MSysACEs"));
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(session.SourcePath, cancellationToken: Ct);
        Assert.DoesNotContain("NeverCreated", await reader.ListTablesAsync(Ct));
        using DataTable rows = await reader.ReadTableAsync("Renamed T", cancellationToken: Ct);
        DataRow row = Assert.Single(rows.AsEnumerable());
        Assert.Equal(1, row["Id"]);
        Assert.Equal(2, row["Note"]);
        Assert.Equal(9, row["Keep"]);
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(DatabaseFormat.Jet3Mdb, "Renamed T")]
    [InlineData(DatabaseFormat.Jet3Mdb, "t")]
    [InlineData(DatabaseFormat.Jet4Mdb, "Renamed T")]
    [InlineData(DatabaseFormat.Jet4Mdb, "t")]
    public Task Dao36_RenamePreservesNativeDependenciesAndConstraints(DatabaseFormat format, string newName)
        => RunRoundTripAsync(format, newName, useDao36: true);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(DatabaseFormat.Jet4Mdb, "Renamed T")]
    [InlineData(DatabaseFormat.Jet4Mdb, "t")]
    [InlineData(DatabaseFormat.AceAccdb, "Renamed T")]
    [InlineData(DatabaseFormat.AceAccdb, "t")]
    public Task Dao120_RenamePreservesNativeDependenciesAndConstraints(DatabaseFormat format, string newName)
        => RunRoundTripAsync(format, newName, useDao36: false);

    private static async Task RunRoundTripAsync(DatabaseFormat format, string newName, bool useDao36)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativeTableRename", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        int version = format switch
        {
            DatabaseFormat.Jet3Mdb => 32,
            DatabaseFormat.Jet4Mdb => 64,
            DatabaseFormat.AceAccdb => 128,
            _ => throw new ArgumentOutOfRangeException(nameof(format)),
        };
        _ = RunEngine(
            $$"""
            $db = $engine.CreateDatabase({{Literal(session.SourcePath)}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            try {
                $db.Execute('CREATE TABLE [T] ([Id] LONG CONSTRAINT [PrimaryKey] PRIMARY KEY, [Value] LONG)', 128)
                $db.Execute('CREATE TABLE [C] ([ParentId] LONG)', 128)
                $db.Execute('INSERT INTO [T] ([Id],[Value]) VALUES (1,7)', 128)
                $db.Execute('INSERT INTO [C] ([ParentId]) VALUES (1)', 128)
                $relation = $db.CreateRelation('FK', 'T', 'C', 4352)
                $field = $relation.CreateField('Id')
                $field.ForeignName = 'ParentId'
                $relation.Fields.Append($field)
                $db.Relations.Append($relation)
                $table = $db.TableDefs('T')
                $table.Properties.Append($table.CreateProperty('Filter', 12, '[T].[Value]>0'))
                $table.Fields('Value').Properties('DefaultValue').Value = '9'
                $query = $db.CreateQueryDef('SavedLookup', 'SELECT X.Id, X.Value FROM T AS X INNER JOIN C AS Y ON X.Id=Y.ParentId WHERE EXISTS (SELECT Z.Id FROM T AS Z WHERE Z.Id=X.Id)')
                $query.Close()
                $query = $db.CreateQueryDef('NeverRun', 'UPDATE [T] SET [Value]=[Value]+1')
                $query.Close()
            } finally { $db.Close() }
            """,
            session.WorkDir,
            useDao36);
        string[] queries = await ReadMetadataAsync(session.SourcePath, "MSysQueries");
        string[] permissions = await ReadMetadataAsync(session.SourcePath, "MSysACEs");
        string control = session.CreateDatabasePath("native-control");
        string controlCompact = session.CreateDatabasePath("native-control-compact");
        File.Copy(session.SourcePath, control);
        _ = RunEngine(
            $$"""
            $db = $engine.OpenDatabase({{Literal(control)}})
            try { $db.TableDefs('T').Name = {{Literal(newName)}} } finally { $db.Close() }
            """,
            session.WorkDir,
            useDao36);
        string native = RunEngine(ReadMetadataScript(control, newName), session.WorkDir, useDao36);
        Assert.Equal(queries, await ReadMetadataAsync(control, "MSysQueries"));
        _ = RunEngine(ValidateAndCompactScript(control, controlCompact, version, newName), session.WorkDir, useDao36);

        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await writer.RenameTableAsync("T", newName, Ct);
        }

        Assert.Equal(queries, await ReadMetadataAsync(session.SourcePath, "MSysQueries"));
        Assert.Equal(permissions, await ReadMetadataAsync(session.SourcePath, "MSysACEs"));
        Assert.Equal(native, RunEngine(ReadMetadataScript(session.SourcePath, newName), session.WorkDir, useDao36));
        _ = RunEngine(ValidateAndCompactScript(session.SourcePath, session.CompactedPath, version, newName), session.WorkDir, useDao36);
    }

    private static string ReadMetadataScript(string path, string newName) => $$"""
        $db = $engine.OpenDatabase({{Literal(path)}})
        try {
            $table = $db.TableDefs({{Literal(newName)}})
            Write-Output ('NAME=' + $table.Name)
            Write-Output ('FILTER=' + $table.Properties('Filter').Value)
            Write-Output ('DEFAULT=' + $table.Fields('Value').Properties('DefaultValue').Value)
            Write-Output ('RELATION=' + $db.Relations('FK').Table + '|' + $db.Relations('FK').ForeignTable)
            foreach ($name in @('SavedLookup','NeverRun')) { Write-Output ($name + '=' + $db.QueryDefs($name).SQL) }
        } finally { $db.Close() }
        """;

    private static string ValidateAndCompactScript(string source, string compacted, int version, string newName) => $$"""
        $db = $engine.OpenDatabase({{Literal(source)}})
        try {
            $table = $db.TableDefs({{Literal(newName)}})
            $sqlTable = '[' + $table.Name + ']'
            $rs = $db.OpenRecordset('SELECT [Value] FROM ' + $sqlTable + ' WHERE [Id]=1')
            try { if ($rs.EOF -or $rs.Fields('Value').Value -ne 7) { throw 'Lost seed data or executed a saved action query.' } } finally { $rs.Close() }
            $db.Execute('INSERT INTO ' + $sqlTable + ' ([Id]) VALUES (2)', 128)
            $db.Execute('INSERT INTO [C] ([ParentId]) VALUES (2)', 128)
            $rejected = $false
            try { $db.Execute('INSERT INTO [C] ([ParentId]) VALUES (999)', 128) } catch { $rejected = $true }
            if (!$rejected) { throw 'Renamed relationship failed to enforce referential integrity.' }
            $db.Execute('UPDATE ' + $sqlTable + ' SET [Id]=3 WHERE [Id]=2', 128)
            $rs = $db.OpenRecordset('SELECT [ParentId] FROM [C] WHERE [ParentId]=3')
            try { if ($rs.EOF) { throw 'Cascade update lost the renamed parent.' } } finally { $rs.Close() }
            $db.Execute('DELETE FROM ' + $sqlTable + ' WHERE [Id]=3', 128)
            $rs = $db.OpenRecordset('SELECT [ParentId] FROM [C] WHERE [ParentId]=3')
            try { if (!$rs.EOF) { throw 'Cascade delete lost the renamed parent.' } } finally { $rs.Close() }
        } finally { $db.Close() }
        $engine.CompactDatabase({{Literal(source)}}, {{Literal(compacted)}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
        $db = $engine.OpenDatabase({{Literal(compacted)}})
        try {
            $table = $db.TableDefs({{Literal(newName)}})
            if ($table.Name -cne {{Literal(newName)}} -or $db.Relations('FK').Table -cne {{Literal(newName)}}) { throw 'Compaction lost renamed identities.' }
            $rs = $db.OpenRecordset('SELECT [Value] FROM [' + $table.Name + '] WHERE [Id]=1')
            try { if ($rs.EOF -or $rs.Fields('Value').Value -ne 7) { throw 'Lost compacted seed data.' } } finally { $rs.Close() }
        } finally { $db.Close() }
        """;

    private static async Task<string[]> ReadMetadataAsync(string path, string tableName)
    {
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(path, cancellationToken: Ct);
        using DataTable table = await reader.ReadTableAsync(tableName, cancellationToken: Ct);
        return table.AsEnumerable().Select(row => string.Join("|", row.ItemArray.Select(value => value switch
        {
            null or DBNull => "null",
            byte[] bytes => "bytes:" + Convert.ToHexString(bytes),
            DateTime date => "date:" + date.ToBinary().ToString(CultureInfo.InvariantCulture),
            _ => value.GetType().Name + ":" + Convert.ToString(value, CultureInfo.InvariantCulture),
        }))).Order(StringComparer.Ordinal).ToArray();
    }

    private static string Literal(string value) => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(value);

    private static string RunEngine(string script, string directory, bool useDao36)
    {
        if (useDao36)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, directory, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, $"DAO36 failed: {result.StandardOutput}\n{result.StandardError}");
            return result.StandardOutput;
        }

        AccessRoundTripEnvironment.CompactResult ace = AccessRoundTripEnvironment.RunDaoScript(
            $$"""
            $ErrorActionPreference = 'Stop'
            $engine = New-Object -ComObject DAO.DBEngine.120
            try {
                {{script}}
            } finally { [System.Runtime.InteropServices.Marshal]::ReleaseComObject($engine) | Out-Null }
            """,
            directory,
            TimeSpan.FromMinutes(2));
        Assert.True(ace.ExitCode == 0, $"DAO120 failed: {ace.StdOut}\n{ace.StdErr}");
        return ace.StdOut;
    }
}
