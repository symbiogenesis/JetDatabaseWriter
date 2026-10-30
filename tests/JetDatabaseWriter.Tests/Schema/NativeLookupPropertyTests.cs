namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
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
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

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
                $db.Execute('CREATE TABLE [Lookup] ([Direct] LONG, [Sql] LONG, [Saved] LONG, [Chain] LONG)', 128)
                $db.Execute('INSERT INTO [Lookup] ([Direct],[Sql],[Saved],[Chain]) VALUES (1,1,1,1)', 128)
                $query = $db.CreateQueryDef('BaseLookup', 'SELECT X.Id, X.Note FROM T AS X')
                $query.Close()
                $query = $db.CreateQueryDef('SavedLookup', 'SELECT B.Id, B.Note FROM BaseLookup AS B')
                $query.Close()
                $sources = @('T', 'SELECT X.Id, X.Note FROM T AS X', 'BaseLookup', 'SavedLookup')
                $names = @('Direct', 'Sql', 'Saved', 'Chain')
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
            switch (operation)
            {
                case "rename":
                    await writer.RenameColumnAsync("T", "Note", "Renamed", Ct);
                    break;
                case "case":
                    await writer.RenameColumnAsync("T", "Note", "note", Ct);
                    break;
                case "drop":
                    await writer.DropColumnAsync("T", "Note", Ct);
                    break;
                default:
                    await writer.DropTableAsync("T", Ct);
                    break;
            }
        }

        Assert.Equal(original, await ReadLookupPropertiesAsync(session.SourcePath));
        Assert.Equal(queryRows, await ReadQueryRowsAsync(session.SourcePath));
        Assert.Equal(originalQueries, RunEngine(ReadQueriesScript(session.SourcePath), session.WorkDir, useDao36));
        _ = RunEngine(ValidateAndCompactScript(session.SourcePath, session.CompactedPath, version, operation), session.WorkDir, useDao36);
        Assert.Equal(original, await ReadLookupPropertiesAsync(session.CompactedPath));
        Assert.Equal(originalQueries, RunEngine(ReadQueriesScript(session.CompactedPath), session.WorkDir, useDao36));
    }

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
            foreach ($name in @('BaseLookup', 'SavedLookup')) {
                Write-Output ($name + '=' + $db.QueryDefs($name).SQL)
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
        return rows.AsEnumerable().Select(row => "definition:" + EncodeRow(row))
            .Concat(catalog.AsEnumerable().Where(row => row["Name"] is "BaseLookup" or "SavedLookup").Select(row => "catalog:" + EncodeRow(row)))
            .Order(StringComparer.Ordinal).ToArray();
    }

    private static string EncodeRow(DataRow row) => string.Join("|", row.ItemArray.Select(value => value switch
    {
        null or DBNull => "null",
        byte[] bytes => Convert.ToHexString(bytes),
        _ => Convert.ToString(value, CultureInfo.InvariantCulture),
    }));

    private static async Task<string[]> ReadLookupPropertiesAsync(string path)
    {
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(path, cancellationToken: Ct);
        long page = Assert.Single(await reader.Services.Catalog.GetUserTablesAsync(Ct), table => table.Name == "Lookup").TDefPage;
        ColumnPropertyBlock properties = Assert.IsType<ColumnPropertyBlock>(await reader.Services.Catalog.ReadLvPropForTableAsync(page, Ct));
        string[] result = properties.Targets.SelectMany(target => target.Entries
            .Where(entry => entry.Name is "RowSource" or "RowSourceType")
            .Select(entry => $"{target.Name}|{entry.Name}|{entry.DataType}|{entry.DdlFlag}|{Convert.ToHexString(entry.Value)}")).ToArray();
        Assert.Equal(8, result.Length);
        return result;
    }
}
