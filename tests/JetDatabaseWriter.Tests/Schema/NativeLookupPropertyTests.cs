namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Lookup sources and saved-query text follow native DAO metadata preservation.</summary>
public sealed class NativeLookupPropertyTests
{
    private static readonly string[] QueryNames =
    [
        "BaseLookup", "SavedLookup", "ParameterLookup", "UnionLookup", "CrossTabLookup",
        "UpdateDefinition", "AppendDefinition", "DeleteDefinition", "MakeTableDefinition",
    ];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets frozen native schemas, write modes and dependency-changing edits.</summary>
    public static TheoryData<DatabaseFormat, WriteMode, string> FrozenCases()
    {
        var cases = new TheoryData<DatabaseFormat, WriteMode, string>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb })
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                foreach (string operation in new[] { "rename", "drop", "table" })
                {
                    cases.Add(format, mode, operation);
                }
            }
        }

        return cases;
    }

    [Theory]
    [MemberData(nameof(FrozenCases))]
    public async Task FrozenNativeQueryDefinitions_SchemaEditPreservesRawMetadata(DatabaseFormat format, WriteMode mode, string operation)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.FrozenQueries", databaseExtension: ".mdb");
        string version = format == DatabaseFormat.Jet3Mdb ? "V1997" : "V2000";
        string hash = format == DatabaseFormat.Jet3Mdb
            ? "562B9826FF109EE92D062D6E38AA0134A9E8DA49432AC29D0FFE810252623AF2"
            : "08EF602CDB30A80C01E73068E646FB8EB39B641FEEDA82A375E6ABFADDD071BC";
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Databases", "QueryDependencies", $"NativeQueryDependencies{version}.mdb"), Ct);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(bytes)));
        await File.WriteAllBytesAsync(session.SourcePath, bytes, Ct);
        string[] properties = await ReadLookupPropertiesAsync(session.SourcePath);
        string[] queries = await ReadQueryRowsAsync(session.SourcePath);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, WriteModes.WriterOptions(mode), Ct))
        {
            await WriteModes.RunAsync(writer, mode, () => ApplyEditAsync(writer, operation), Ct);
        }

        Assert.Equal(properties, await ReadLookupPropertiesAsync(session.SourcePath));
        Assert.Equal(queries, await ReadQueryRowsAsync(session.SourcePath));
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(session.SourcePath, cancellationToken: Ct);
        Assert.DoesNotContain(await reader.ListTablesAsync(Ct), name => name == "NeverCreated");
        using DataTable lookup = await reader.ReadTableAsync("Lookup", cancellationToken: Ct);
        Assert.Equal(1, Assert.Single(lookup.AsEnumerable())["Direct"]);
        if (operation != "table")
        {
            using DataTable target = await reader.ReadTableAsync("T", cancellationToken: Ct);
            DataRow row = Assert.Single(target.AsEnumerable());
            Assert.Equal(1, row["Id"]);
            Assert.Equal(9, row["Keep"]);
            if (operation == "rename")
            {
                Assert.Equal(2, row["Renamed"]);
            }
        }
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(DatabaseFormat.Jet3Mdb, "rename")]
    [InlineData(DatabaseFormat.Jet3Mdb, "case")]
    [InlineData(DatabaseFormat.Jet3Mdb, "drop")]
    [InlineData(DatabaseFormat.Jet3Mdb, "table")]
    [InlineData(DatabaseFormat.Jet4Mdb, "rename")]
    [InlineData(DatabaseFormat.Jet4Mdb, "case")]
    [InlineData(DatabaseFormat.Jet4Mdb, "drop")]
    [InlineData(DatabaseFormat.Jet4Mdb, "table")]
    public Task Dao36SchemaEdit_PreservesLookupAndSavedQueryText(DatabaseFormat format, string operation)
        => RunRoundTripAsync(format, operation, useDao36: true);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData("rename")]
    [InlineData("case")]
    [InlineData("drop")]
    [InlineData("table")]
    public Task Dao120SchemaEdit_PreservesLookupAndSavedQueryText(string operation)
        => RunRoundTripAsync(DatabaseFormat.AceAccdb, operation, useDao36: false);

    private static async Task RunRoundTripAsync(DatabaseFormat format, string operation, bool useDao36)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativeLookup", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string control = session.CreateDatabasePath("native-control");
        string controlCompact = session.CreateDatabasePath("native-control-compact");
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
                $db.Execute('CREATE TABLE [T] ([Id] LONG, [Note] LONG, [Keep] LONG)', 128)
                $db.Execute('INSERT INTO [T] ([Id],[Note],[Keep]) VALUES (1,2,9)', 128)
                $db.Execute('CREATE TABLE [Lookup] ([Direct] LONG, [Sql] LONG, [Saved] LONG, [Chain] LONG, [Parameters] LONG, [Union] LONG, [CrossTab] LONG)', 128)
                $db.Execute('INSERT INTO [Lookup] ([Direct],[Sql],[Saved],[Chain],[Parameters],[Union],[CrossTab]) VALUES (1,1,1,1,1,1,1)', 128)
                $query = $db.CreateQueryDef('BaseLookup', 'SELECT X.Id, X.Note FROM T AS X')
                $query.Close()
                $query = $db.CreateQueryDef('SavedLookup', 'SELECT B.Id, B.Note FROM BaseLookup AS B')
                $query.Close()
                $definitions = [ordered]@{
                    ParameterLookup = 'PARAMETERS [Minimum] Long; SELECT T.Id, T.Note FROM T WHERE T.Note > [Minimum]'
                    UnionLookup = 'SELECT Id, Note FROM T UNION ALL SELECT Id, Note FROM T'
                    CrossTabLookup = 'TRANSFORM Count(T.Id) AS Total SELECT T.Keep FROM T GROUP BY T.Keep PIVOT T.Note'
                    UpdateDefinition = 'UPDATE [T] SET [Note]=[Note]+1 WHERE [Id]=1'
                    AppendDefinition = 'INSERT INTO [T] ([Id],[Note],[Keep]) SELECT [Id]+100,[Note],[Keep] FROM [T]'
                    DeleteDefinition = 'DELETE FROM [T] WHERE [Note]<0'
                    MakeTableDefinition = 'SELECT [Id],[Note] INTO [NeverCreated] FROM [T]'
                }
                foreach ($definition in $definitions.GetEnumerator()) {
                    $query = $db.CreateQueryDef($definition.Key, $definition.Value)
                    $query.Close()
                }
                $sources = @('T', 'SELECT X.Id, X.Note FROM T AS X', 'BaseLookup', 'SavedLookup', 'ParameterLookup', 'UnionLookup', 'CrossTabLookup')
                $names = @('Direct', 'Sql', 'Saved', 'Chain', 'Parameters', 'Union', 'CrossTab')
                for ($i = 0; $i -lt $names.Length; $i++) {
                    $field = $db.TableDefs('Lookup').Fields($names[$i])
                    $field.Properties.Append($field.CreateProperty('RowSourceType', 10, 'Table/Query'))
                    $field.Properties.Append($field.CreateProperty('RowSource', 12, $sources[$i]))
                }
            } finally { $db.Close() }
            """,
            session.WorkDir,
            useDao36);
        string[] original = await ReadLookupPropertiesAsync(session.SourcePath);
        string[] queryRows = await ReadQueryRowsAsync(session.SourcePath);
        string originalQueries = RunEngine(ReadQueriesScript(session.SourcePath), session.WorkDir, useDao36);
        await File.WriteAllBytesAsync(control, await File.ReadAllBytesAsync(session.SourcePath, Ct), Ct);
        string nativeEdit = operation switch
        {
            "rename" => "$db.TableDefs('T').Fields('Note').Name = 'Renamed'",
            "case" => "$db.TableDefs('T').Fields('Note').Name = 'note'",
            "drop" => "$db.TableDefs('T').Fields.Delete('Note')",
            _ => "$db.TableDefs.Delete('T')",
        };
        _ = RunEngine(
            $$"""
            $db = $engine.OpenDatabase({{Literal(control)}})
            try { {{nativeEdit}} } finally { $db.Close() }
            """,
            session.WorkDir,
            useDao36);
        Assert.Equal(original, await ReadLookupPropertiesAsync(control));
        Assert.Equal(originalQueries, RunEngine(ReadQueriesScript(control), session.WorkDir, useDao36));
        _ = RunEngine(ValidateAndCompactScript(control, controlCompact, version, operation), session.WorkDir, useDao36);
        Assert.Equal(original, await ReadLookupPropertiesAsync(controlCompact));
        Assert.Equal(originalQueries, RunEngine(ReadQueriesScript(controlCompact), session.WorkDir, useDao36));

        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await ApplyEditAsync(writer, operation);
        }

        Assert.Equal(original, await ReadLookupPropertiesAsync(session.SourcePath));
        Assert.Equal(queryRows, await ReadQueryRowsAsync(session.SourcePath));
        Assert.Equal(originalQueries, RunEngine(ReadQueriesScript(session.SourcePath), session.WorkDir, useDao36));
        _ = RunEngine(ValidateAndCompactScript(session.SourcePath, session.CompactedPath, version, operation), session.WorkDir, useDao36);
        Assert.Equal(original, await ReadLookupPropertiesAsync(session.CompactedPath));
        Assert.Equal(originalQueries, RunEngine(ReadQueriesScript(session.CompactedPath), session.WorkDir, useDao36));
    }

    private static Task ApplyEditAsync(AccessWriter writer, string operation) => operation switch
    {
        "rename" => writer.RenameColumnAsync("T", "Note", "Renamed", Ct).AsTask(),
        "case" => writer.RenameColumnAsync("T", "Note", "note", Ct).AsTask(),
        "drop" => writer.DropColumnAsync("T", "Note", Ct).AsTask(),
        "table" => writer.DropTableAsync("T", Ct).AsTask(),
        _ => throw new ArgumentOutOfRangeException(nameof(operation)),
    };

    private static string ValidateAndCompactScript(string source, string compacted, int version, string operation)
    {
        string targetCheck = operation == "table"
            ? "if (@($db.TableDefs | Where-Object { $_.Name -eq 'T' }).Count -ne 0) { throw 'Table was not dropped.' }"
            : "$rs = $db.OpenRecordset('SELECT [Keep] FROM [T]'); try { if ($rs.EOF -or $rs.Fields('Keep').Value -ne 9) { throw 'Lost target data.' } } finally { $rs.Close() }";
        return $$"""
            $db = $engine.OpenDatabase({{Literal(source)}})
            try {
                {{targetCheck}}
                $rs = $db.OpenRecordset('SELECT [Direct] FROM [Lookup]')
                try { if ($rs.EOF -or $rs.Fields('Direct').Value -ne 1) { throw 'Lost lookup data.' } } finally { $rs.Close() }
                $db.Execute('INSERT INTO [Lookup] ([Direct]) VALUES (8)', 128)
                $db.Execute('UPDATE [Lookup] SET [Direct]=7 WHERE [Direct]=8', 128)
                $db.Execute('DELETE FROM [Lookup] WHERE [Direct]=7', 128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{Literal(source)}}, {{Literal(compacted)}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            $db = $engine.OpenDatabase({{Literal(compacted)}})
            try {
                {{targetCheck}}
                $rs = $db.OpenRecordset('SELECT [Direct] FROM [Lookup]')
                try { if ($rs.EOF -or $rs.Fields('Direct').Value -ne 1) { throw 'Lost compacted lookup data.' } } finally { $rs.Close() }
            } finally { $db.Close() }
            """;
    }

    private static string ReadQueriesScript(string path) => $$"""
        $db = $engine.OpenDatabase({{Literal(path)}})
        try {
            $types = [ordered]@{ BaseLookup=0; SavedLookup=0; ParameterLookup=0; UnionLookup=128; CrossTabLookup=16; UpdateDefinition=48; AppendDefinition=64; DeleteDefinition=32; MakeTableDefinition=80 }
            foreach ($entry in $types.GetEnumerator()) {
                $query = $db.QueryDefs($entry.Key)
                if ($query.Type -ne $entry.Value) { throw ('Wrong query type: ' + $entry.Key + '=' + $query.Type) }
                Write-Output ($entry.Key + '=' + $query.SQL)
            }
        } finally { $db.Close() }
        """;

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

    private static async Task<string[]> ReadQueryRowsAsync(string path)
    {
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(path, cancellationToken: Ct);
        using DataTable rows = await reader.ReadTableAsync("MSysQueries", cancellationToken: Ct);
        using DataTable catalog = await reader.ReadTableAsync("MSysObjects", cancellationToken: Ct);
        DataRow[] queries = catalog.AsEnumerable().Where(row => QueryNames.Contains((string)row["Name"], StringComparer.Ordinal)).ToArray();
        Assert.Equal(QueryNames.Length, queries.Length);
        int[] identities = queries.Select(row => (int)row["Id"]).ToArray();
        using DataTable permissions = await reader.ReadTableAsync("MSysACEs", cancellationToken: Ct);
        return rows.AsEnumerable().Select(row => "definition:" + EncodeRow(row))
            .Concat(queries.Select(row => "catalog:" + EncodeRow(row)))
            .Concat(permissions.AsEnumerable().Where(row => identities.Contains((int)row["ObjectId"])).Select(row => "permission:" + EncodeRow(row)))
            .Order(StringComparer.Ordinal).ToArray();
    }

    private static string EncodeRow(DataRow row) => string.Join("|", row.ItemArray.Select(value => value switch
    {
        null or DBNull => "null",
        byte[] bytes => "bytes:" + Convert.ToHexString(bytes),
        DateTime date => "date:" + date.ToBinary().ToString(CultureInfo.InvariantCulture),
        _ => value.GetType().Name + ":" + Convert.ToHexString(System.Text.Encoding.UTF8.GetBytes(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty)),
    }));

    private static async Task<string[]> ReadLookupPropertiesAsync(string path)
    {
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(path, cancellationToken: Ct);
        long page = Assert.Single(await reader.Services.Catalog.GetUserTablesAsync(Ct), table => table.Name == "Lookup").TDefPage;
        ColumnPropertyBlock properties = Assert.IsType<ColumnPropertyBlock>(await reader.Services.Catalog.ReadLvPropForTableAsync(page, Ct));
        string[] result = properties.Targets.SelectMany(target => target.Entries
            .Where(entry => entry.Name is "RowSource" or "RowSourceType")
            .Select(entry => $"{target.Name}|{entry.Name}|{entry.DataType}|{entry.DdlFlag}|{Convert.ToHexString(entry.Value)}")).ToArray();
        Assert.Equal(14, result.Length);
        return result;
    }
}
