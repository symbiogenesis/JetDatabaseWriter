namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Preserves native SQL-specific query definitions as inert schema metadata.</summary>
public sealed class NativeSqlSpecificQueryPreservationTests
{
    private static readonly string[] QueryNames = ["CreateDefinition", "PassThroughSelect", "PassThroughAction"];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every native format, write mode and dependency-changing edit.</summary>
    public static TheoryData<DatabaseFormat, WriteMode, string> FrozenCases()
    {
        var cases = new TheoryData<DatabaseFormat, WriteMode, string>();
        foreach (DatabaseFormat format in Enum.GetValues<DatabaseFormat>())
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                foreach (string operation in new[] { "rename", "case", "drop", "table" })
                {
                    cases.Add(format, mode, operation);
                }
            }
        }

        return cases;
    }

    [Theory]
    [MemberData(nameof(FrozenCases))]
    public async Task FrozenNativeSqlSpecificQueries_PreserveDefinitionsWithoutExecution(DatabaseFormat format, WriteMode mode, string operation)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.SqlSpecificQueries", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        await SeedAsync(session.SourcePath, format);
        string[] expected = await ReadQueryRowsAsync(session.SourcePath);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, WriteModes.WriterOptions(mode), Ct))
        {
            await WriteModes.RunAsync(writer, mode, () => ApplyEditAsync(writer, operation), Ct);
        }

        Assert.Equal(expected, await ReadQueryRowsAsync(session.SourcePath));
        await AssertUserDataAsync(session.SourcePath, operation);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task FrozenSnapshot_DetectsChangedPassThroughConnection(DatabaseFormat format)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.SqlSpecificQueries.Control", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        await SeedAsync(session.SourcePath, format);
        string[] expected = await ReadQueryRowsAsync(session.SourcePath);
        byte[] bytes = await File.ReadAllBytesAsync(session.SourcePath, Ct);
        Encoding encoding = format == DatabaseFormat.Jet3Mdb ? Encoding.ASCII : Encoding.Unicode;
        byte[] marker = encoding.GetBytes("JDW_Inert_NotInstalled");
        int position = bytes.AsSpan().IndexOf(marker);
        Assert.True(position >= 0, "The native fixture must retain its inert DSN in stored query metadata.");
        bytes[position] = (byte)'X';
        await File.WriteAllBytesAsync(session.SourcePath, bytes, Ct);
        Assert.NotEqual(expected, await ReadQueryRowsAsync(session.SourcePath));
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
    public Task Dao36_SqlSpecificQueriesPreserveNativeMetadataThroughEditAndCompact(DatabaseFormat format, string operation)
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
    public Task Dao120_SqlSpecificQueriesPreserveNativeMetadataThroughEditAndCompact(string operation)
        => RunRoundTripAsync(DatabaseFormat.AceAccdb, operation, useDao36: false);

    private static async Task RunRoundTripAsync(DatabaseFormat format, string operation, bool useDao36)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.SqlSpecificQueries.Native", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        if (useDao36)
        {
            await SeedAsync(session.SourcePath, format);
        }
        else
        {
            _ = RunEngine(CreateAceScript(session.SourcePath), session.WorkDir, useDao36);
        }

        string control = session.CreateDatabasePath("native-control");
        string controlCompact = session.CreateDatabasePath("native-control-compact");
        File.Copy(session.SourcePath, control);
        string original = RunEngine(ReadQueriesScript(session.SourcePath), session.WorkDir, useDao36);
        string[] originalRows = await ReadQueryRowsAsync(session.SourcePath);
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
        Assert.Equal(original, RunEngine(ReadQueriesScript(control), session.WorkDir, useDao36));
        Assert.Equal(originalRows, await ReadQueryRowsAsync(control));
        int version = format switch
        {
            DatabaseFormat.Jet3Mdb => 32,
            DatabaseFormat.Jet4Mdb => 64,
            DatabaseFormat.AceAccdb => 128,
            _ => throw new ArgumentOutOfRangeException(nameof(format)),
        };
        _ = RunEngine(CompactScript(control, controlCompact, version), session.WorkDir, useDao36);
        Assert.Equal(original, RunEngine(ReadQueriesScript(controlCompact), session.WorkDir, useDao36));
        await AssertUserDataAsync(controlCompact, operation);

        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await ApplyEditAsync(writer, operation);
        }

        Assert.Equal(original, RunEngine(ReadQueriesScript(session.SourcePath), session.WorkDir, useDao36));
        Assert.Equal(originalRows, await ReadQueryRowsAsync(session.SourcePath));
        _ = RunEngine(CompactScript(session.SourcePath, session.CompactedPath, version), session.WorkDir, useDao36);
        Assert.Equal(original, RunEngine(ReadQueriesScript(session.CompactedPath), session.WorkDir, useDao36));
        await AssertUserDataAsync(session.CompactedPath, operation);
    }

    private static async Task SeedAsync(string path, DatabaseFormat format)
    {
        (string name, string hash) = format switch
        {
            DatabaseFormat.Jet3Mdb => ("NativeSqlSpecificQueriesV1997.mdb", "46013C4F886BB678CFA6C223F9C64354743863454D931619CE887758BF6983E2"),
            DatabaseFormat.Jet4Mdb => ("NativeSqlSpecificQueriesV2000.mdb", "2E560387DCA67386442272FC275D1EC5F75FECE495FAB400A9C0770D9AF21158"),
            DatabaseFormat.AceAccdb => ("NativeSqlSpecificQueriesV2007.accdb", "81258CCD39BCC42162354C8E186642E1754F24DA9085100E035DBB87556FAEA1"),
            _ => throw new ArgumentOutOfRangeException(nameof(format)),
        };
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Databases", "SqlSpecificQueries", name), Ct);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(bytes)));
        await File.WriteAllBytesAsync(path, bytes, Ct);
    }

    private static Task ApplyEditAsync(AccessWriter writer, string operation) => operation switch
    {
        "rename" => writer.RenameColumnAsync("T", "Note", "Renamed", Ct).AsTask(),
        "case" => writer.RenameColumnAsync("T", "Note", "note", Ct).AsTask(),
        "drop" => writer.DropColumnAsync("T", "Note", Ct).AsTask(),
        "table" => writer.DropTableAsync("T", Ct).AsTask(),
        _ => throw new ArgumentOutOfRangeException(nameof(operation)),
    };

    private static async Task AssertUserDataAsync(string path, string operation)
    {
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(path, cancellationToken: Ct);
        string[] names = (await reader.ListTablesAsync(Ct)).ToArray();
        Assert.DoesNotContain("NeverCreated", names);
        if (operation == "table")
        {
            Assert.DoesNotContain("T", names);
            return;
        }

        using DataTable table = await reader.ReadTableAsync("T", cancellationToken: Ct);
        DataRow row = Assert.Single(table.AsEnumerable());
        Assert.Equal(1, row["Id"]);
        Assert.Equal(9, row["Keep"]);
        if (operation != "drop")
        {
            Assert.Equal(2, row[operation == "rename" ? "Renamed" : "note"]);
        }
    }

    private static async Task<string[]> ReadQueryRowsAsync(string path)
    {
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(path, cancellationToken: Ct);
        using DataTable definitions = await reader.ReadTableAsync("MSysQueries", cancellationToken: Ct);
        using DataTable catalog = await reader.ReadTableAsync("MSysObjects", cancellationToken: Ct);
        DataRow[] queries = catalog.AsEnumerable().Where(row => QueryNames.Contains((string)row["Name"], StringComparer.Ordinal)).ToArray();
        Assert.Equal(QueryNames.Length, queries.Length);
        Assert.All(queries, row => Assert.Equal((short)5, row["Type"]));
        int[] identities = queries.Select(row => (int)row["Id"]).ToArray();
        using DataTable permissions = await reader.ReadTableAsync("MSysACEs", cancellationToken: Ct);
        DataRow[] queryPermissions = permissions.AsEnumerable().Where(row => identities.Contains((int)row["ObjectId"])).ToArray();
        Assert.NotEmpty(queryPermissions);
        foreach (int identity in identities)
        {
            Assert.Contains(definitions.AsEnumerable(), row => (int)row["ObjectId"] == identity);
            Assert.Contains(queryPermissions, row => (int)row["ObjectId"] == identity);
        }

        return definitions.AsEnumerable().Select(row => "definition:" + EncodeRow(row))
            .Concat(queries.Select(row => "catalog:" + EncodeRow(row)))
            .Concat(queryPermissions.Select(row => "permission:" + EncodeRow(row)))
            .Order(StringComparer.Ordinal).ToArray();
    }

    private static string ReadQueriesScript(string path) => $$"""
        $db = $engine.OpenDatabase({{Literal(path)}}, $false, $true)
        try {
            $types = [ordered]@{ CreateDefinition=96; PassThroughSelect=112; PassThroughAction=144 }
            foreach ($entry in $types.GetEnumerator()) {
                $query = $db.QueryDefs($entry.Key)
                try {
                    if ($query.Type -ne $entry.Value) { throw ('Wrong query type: ' + $entry.Key + '=' + $query.Type) }
                    if ($entry.Key -eq 'CreateDefinition') {
                        if ($query.Connect -cne '') { throw 'Unexpected DDL connection' }
                        if ($query.Properties('Description').Value -cne ('Inert stored DDL; ' * 160)) { throw 'Changed DDL description' }
                    } else {
                        if ($query.Connect -cne 'ODBC;DSN=JDW_Inert_NotInstalled;DATABASE=JDW_MetadataOnly;') { throw 'Changed inert connection metadata' }
                        if ($query.ODBCTimeout -ne 17) { throw 'Changed ODBC timeout' }
                        if ($query.ReturnsRecords -ne ($entry.Key -eq 'PassThroughSelect')) { throw 'Changed ReturnsRecords' }
                        if ($entry.Key -eq 'PassThroughSelect' -and $query.MaxRecords -ne 23) { throw 'Changed MaxRecords' }
                        if ($query.Properties('Description').Value -cne ('Inert pass-through metadata; ' * 120)) { throw 'Changed pass-through description' }
                    }
                    $expectedSql = switch ($entry.Key) {
                        'CreateDefinition' { 'CREATE TABLE [NeverCreated] ([Id] LONG, [Note] LONG)' }
                        'PassThroughSelect' { 'SELECT [Id], [Note] FROM [T] WHERE [Id] = 1' }
                        'PassThroughAction' { 'DELETE FROM [T] WHERE [Id] = 1' }
                    }
                    if ($query.SQL -cne $expectedSql) { throw ('Changed SQL: ' + $entry.Key) }
                    Write-Output ($entry.Key + '=' + $query.SQL)
                } finally { $query.Close() }
            }
            if (@($db.TableDefs | Where-Object { $_.Name -eq 'NeverCreated' }).Count -ne 0) { throw 'Stored DDL was executed' }
        } finally { $db.Close() }
        """;

    private static string CompactScript(string source, string destination, int version) => $$"""
        $engine.CompactDatabase({{Literal(source)}}, {{Literal(destination)}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
        """;

    private static string CreateAceScript(string path) => $$"""
        $db = $engine.CreateDatabase({{Literal(path)}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 128)
        try {
            $db.Execute('CREATE TABLE [T] ([Id] LONG, [Note] LONG, [Keep] LONG)', 128)
            $db.Execute('INSERT INTO [T] ([Id],[Note],[Keep]) VALUES (1,2,9)', 128)
            $query = $db.CreateQueryDef('CreateDefinition', 'CREATE TABLE [NeverCreated] ([Id] LONG, [Note] LONG)')
            $query.Properties.Append($query.CreateProperty('Description', 12, ('Inert stored DDL; ' * 160)))
            $query.Close()
            foreach ($returnsRecords in @($true, $false)) {
                $name = if ($returnsRecords) { 'PassThroughSelect' } else { 'PassThroughAction' }
                $query = $db.CreateQueryDef($name)
                $query.Connect = 'ODBC;DSN=JDW_Inert_NotInstalled;DATABASE=JDW_MetadataOnly;'
                $query.ReturnsRecords = $returnsRecords
                $query.ODBCTimeout = 17
                if ($returnsRecords) {
                    $query.MaxRecords = 23
                    $query.SQL = 'SELECT [Id], [Note] FROM [T] WHERE [Id] = 1'
                } else {
                    $query.SQL = 'DELETE FROM [T] WHERE [Id] = 1'
                }
                $query.Properties.Append($query.CreateProperty('Description', 12, ('Inert pass-through metadata; ' * 120)))
                $query.Close()
            }
        } finally { $db.Close() }
        """;

    private static string RunEngine(string script, string directory, bool useDao36)
    {
        if (!useDao36)
        {
            AccessRoundTripEnvironment.CompactResult ace = AccessRoundTripEnvironment.RunDaoEngineScript(script, directory, TimeSpan.FromMinutes(2));
            Assert.True(ace.ExitCode == 0, $"DAO120 failed: {ace.StdOut}\n{ace.StdErr}");
            return ace.StdOut;
        }

        PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, directory, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, $"DAO36 failed: {result.StandardOutput}\n{result.StandardError}");
        return result.StandardOutput;
    }

    private static string EncodeRow(DataRow row) => string.Join("|", row.ItemArray.Select(value => value switch
    {
        null or DBNull => "null",
        byte[] bytes => "bytes:" + Convert.ToHexString(bytes),
        DateTime date => "date:" + date.ToBinary().ToString(CultureInfo.InvariantCulture),
        _ => value.GetType().Name + ":" + Convert.ToHexString(Encoding.UTF8.GetBytes(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty)),
    }));

    private static string Literal(string value) => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(value);
}
