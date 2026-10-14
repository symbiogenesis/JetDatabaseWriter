namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Native relationships can be dropped and recreated without stale catalog security.</summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoRelationshipCatalogIdentityTests
{
    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(2);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public Task DaoRelationshipDrop_RecreateRollbackAndCompact(DatabaseFormat format) => VerifyAsync(format);

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    public Task Dao36RelationshipDrop_RecreateRollbackAndCompact() => VerifyAsync(DatabaseFormat.Jet3Mdb);

    private static async Task VerifyAsync(DatabaseFormat format)
    {
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string version = format switch { DatabaseFormat.Jet3Mdb => ", 32", DatabaseFormat.Jet4Mdb => ", 64", DatabaseFormat.AceAccdb => string.Empty, _ => throw new ArgumentOutOfRangeException(nameof(format), format, null) };
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string setup = $$"""
            $db = $engine.CreateDatabase({{source}}, ';LANGID=0x0409;CP=1252;COUNTRY=0'{{version}})
            try {
                $db.Execute('CREATE TABLE P (Id LONG CONSTRAINT PK_P PRIMARY KEY)', 128)
                $db.Execute('CREATE TABLE C (Id LONG CONSTRAINT PK_C PRIMARY KEY, ParentId LONG)', 128)
                $db.Execute('CREATE INDEX IX_ParentId ON C (ParentId)', 128)
                $db.Execute('CREATE TABLE FK_C_P (Id LONG CONSTRAINT PK_Unrelated PRIMARY KEY)', 128)
                $db.Execute('INSERT INTO P VALUES (1)', 128)
                $db.Execute('INSERT INTO C VALUES (1, 1)', 128)
                foreach ($name in @('FK_C_P', 'FK_Unrelated')) {
                    $rel = $db.CreateRelation($name, 'P', 'C', 0)
                    $field = $rel.CreateField('Id'); $field.ForeignName = if ($name -eq 'FK_Unrelated') { 'Id' } else { 'ParentId' }
                    $rel.Fields.Append($field); $db.Relations.Append($rel)
                }
            } finally { $db.Close() }
            """;
        RunNative(session, format, setup);
        DataTable originalObjects = await ReadSystemAsync(session.SourcePath, "MSysObjects", Ct);
        DataTable originalAces = await ReadSystemAsync(session.SourcePath, "MSysACEs", Ct);
        int originalId = (int)Assert.Single(originalObjects.AsEnumerable(), row => (string)row["Name"] == "FK_C_P" && Convert.ToInt32(row["Type"], CultureInfo.InvariantCulture) == 8)["Id"];
        byte[] original = await File.ReadAllBytesAsync(session.SourcePath, Ct);
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
            await writer.DropRelationshipAsync("FK_C_P", Ct);
            await writer.CreateRelationshipAsync(Definition(), Ct);
            await transaction.RollbackAsync(Ct);
        }

        Assert.Equal(original, await File.ReadAllBytesAsync(session.SourcePath, Ct));
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
            await writer.DropRelationshipAsync("FK_C_P", Ct);
            await writer.CreateRelationshipAsync(Definition(), Ct);
            await writer.RenameRelationshipAsync("FK_C_P", "FK_Renamed", Ct);
            await writer.DropRelationshipAsync("FK_Renamed", Ct);
            await writer.CreateRelationshipAsync(Definition(), Ct);
            await writer.RenameRelationshipAsync("FK_C_P", "FK_Renamed", Ct);
            await writer.DropRelationshipAsync("FK_Renamed", Ct);
            await writer.CreateRelationshipAsync(Definition(), Ct);
            await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("C", [2, 999], Ct));
            await transaction.CommitAsync(Ct);
        }

        const string verify = """
            if ($db.Relations('FK_C_P').Fields('Id').ForeignName -ne 'ParentId') { throw 'Recreated relation is not readable' }
            if ($db.Relations('FK_Unrelated').Fields('Id').ForeignName -ne 'Id') { throw 'Unrelated relation changed' }
            $null = $db.TableDefs('FK_C_P').Indexes('PK_Unrelated')
            $null = $db.TableDefs('C').Indexes('IX_ParentId')
            foreach ($table in @('P', 'C')) {
                $rs = $db.OpenRecordset($table, 4)
                try { if ($rs.Fields('Id').Value -ne 1) { throw 'Data changed' } } finally { $rs.Close() }
            }
            """;
        string nativeScript = $$"""
            $db = $engine.OpenDatabase({{source}})
            try { {{verify}} } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath)}})
            $db = $engine.OpenDatabase({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath)}})
            try { {{verify}} } finally { $db.Close() }
            """;
        RunNative(session, format, nativeScript);

        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await writer.DropRelationshipAsync("FK_C_P", Ct);
        }

        await using (AccessReader reader = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            DataTable objects = await reader.ReadTableAsync("MSysObjects", cancellationToken: Ct);
            Assert.DoesNotContain(objects.AsEnumerable(), row => (string)row["Name"] == "FK_C_P" && Convert.ToInt32(row["Type"], CultureInfo.InvariantCulture) == 8);
            Assert.Equal(Snapshot(originalObjects, row => (int)row["Id"] != originalId), Snapshot(objects, _ => true));
            DataTable aces = await reader.ReadTableAsync("MSysACEs", cancellationToken: Ct);
            Assert.Equal(Snapshot(originalAces, row => (int)row["ObjectId"] != originalId), Snapshot(aces, _ => true));
        }

        nativeScript = $$"""
            $db = $engine.OpenDatabase({{source}})
            try {
                if (@($db.Relations | Where-Object Name -eq 'FK_C_P').Count -ne 0) { throw 'Dropped relation remains' }
                $null = $db.Relations('FK_Unrelated'); $null = $db.TableDefs('FK_C_P')
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CreateDatabasePath("dropped_compacted"))}})
            """;
        RunNative(session, format, nativeScript);
    }

    private static string[] Snapshot(DataTable table, Func<DataRow, bool> include) =>
        table.AsEnumerable().Where(include).Select(row => string.Join("|", row.ItemArray.Select(value => value is byte[] bytes ? Convert.ToHexString(bytes) : Convert.ToString(value, CultureInfo.InvariantCulture)))).Order(StringComparer.Ordinal).ToArray();

    private static async Task<DataTable> ReadSystemAsync(string path, string name, CancellationToken cancellationToken)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, cancellationToken);
        return await reader.ReadTableAsync(name, cancellationToken: cancellationToken);
    }

    private static RelationshipDefinition Definition() => new("FK_C_P", "P", "Id", "C", "ParentId");

    private static void RunNative(AccessRoundTripSession session, DatabaseFormat format, string script)
    {
        if (format == DatabaseFormat.Jet3Mdb)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, DaoTimeout);
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, result.StandardOutput + result.StandardError);
        }
        else
        {
            AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, DaoTimeout);
            Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
        }
    }
}
