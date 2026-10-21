namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Fresh Jet4 databases include native catalog ownership and inherited permissions.</summary>
public sealed class Jet4NativeBootstrapTests
{
    [Fact]
    public async Task FreshJet4_HeaderUserSlots_MatchNativeAvailabilityFlags()
    {
        byte[] native = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Schema.mdb"), TestContext.Current.CancellationToken);
        byte[] created = TDefPageBuilder.BuildEmptyDatabase(DatabaseFormat.Jet4Mdb);
        for (int offset = 0xE01; offset < 4096; offset += 2)
        {
            Assert.Equal(1, native[offset]);
            Assert.Equal(native[offset], created[offset]);
        }
    }

    [Fact]
    public async Task FreshJet4_CreatesNativeSecurityAndCoreSystemTables()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.Jet4Mdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Added", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Added", [1], TestContext.Current.CancellationToken);
        }

        byte[] header = stream.ToArray().AsSpan(0, 4096).ToArray();
        var format = JetFormat.FromHeader(header);
        byte[] owner = Jet4SecuritySid.Encode(format, header, [0x03, 0x01]);
        byte[] users = Jet4SecuritySid.Encode(format, header, [0x02, 0x01]);
        byte[] systemOwner = Jet4SecuritySid.Encode(format, header, [0x02, 0x03]);
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        using DataTable objects = await reader.ReadTableAsync("MSysObjects", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(owner, Assert.IsType<byte[]>(Assert.Single(objects.Select("Name = 'MSysDb'"))["Owner"]));
        foreach (string name in new[] { "MSysObjects", "MSysACEs", "MSysQueries", "MSysRelationships", "Tables", "Relationships", "Databases" })
        {
            Assert.Equal(systemOwner, Assert.IsType<byte[]>(Assert.Single(objects.Select($"Name = '{name}'"))["Owner"]));
        }

        DataRow added = Assert.Single(objects.Select("Name = 'Added'"));
        Assert.Equal(owner, Assert.IsType<byte[]>(added["Owner"]));
        using DataTable permissions = await reader.ReadTableAsync("MSysACEs", cancellationToken: TestContext.Current.CancellationToken);
        DataRow[] addedPermissions = permissions.Select($"ObjectId = {added["Id"]}");
        Assert.Equal(2, addedPermissions.Length);
        Assert.Contains(addedPermissions, row => ((byte[])row["SID"]).SequenceEqual(owner) && (int)row["ACM"] == 0xF00FE && !(bool)row["FInheritable"]);
        Assert.Contains(addedPermissions, row => ((byte[])row["SID"]).SequenceEqual(users) && (int)row["ACM"] == 0xFFEFF && !(bool)row["FInheritable"]);
        Assert.Equal(1, await reader.GetRealRowCountAsync("Added", TestContext.Current.CancellationToken));
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task FreshJet4_DaoOpensWritesAndCompacts()
    {
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: ".mdb");
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            session.SourcePath,
            DatabaseFormat.Jet4Mdb,
            new AccessWriterOptions { UseLockFile = false },
            TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Added", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(
                "Child",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int))],
                TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("AddedChild", "Added", "Id", "Child", "ParentId"), TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Added", [1], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Child", [10, 1], TestContext.Current.CancellationToken);
        }

        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string destination = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string script = $$"""
            $db = $engine.OpenDatabase({{source}})
            try {
                $rs = $db.OpenRecordset('Added')
                try { Write-Output "ID=$($rs.Fields('Id').Value)" } finally { $rs.Close() }
                $db.Execute('INSERT INTO Added (Id) VALUES (2)', 128)
                $db.Execute('INSERT INTO Child (Id, ParentId) VALUES (20, 2)', 128)
                $rejected = $false
                try { $db.Execute('INSERT INTO Child (Id, ParentId) VALUES (30, 999)', 128) }
                catch {
                    $numbers = @($engine.Errors | ForEach-Object { $_.Number })
                    if (3201 -notin $numbers) { throw }
                    $rejected = $true
                    Write-Output 'ORPHAN_REJECTED=3201'
                }
                if (-not $rejected) { throw 'DAO accepted an unmatched foreign key.' }
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{destination}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 64)
            $db = $engine.OpenDatabase({{destination}})
            try {
                $rel = $db.Relations.Item('AddedChild')
                if ($rel.Table -ne 'Added' -or $rel.ForeignTable -ne 'Child' -or ($rel.Attributes -band 2) -ne 0) { throw 'Compacted relationship metadata differs.' }
                if ($rel.Fields.Item(0).Name -ne 'Id' -or $rel.Fields.Item(0).ForeignName -ne 'ParentId') { throw 'Compacted relationship columns differ.' }
                Write-Output 'RELATION=AddedChild:Added:Child'
                $rs = $db.OpenRecordset('SELECT Id, ParentId FROM Child ORDER BY Id')
                try {
                    while (-not $rs.EOF) {
                        Write-Output "CHILD=$($rs.Fields('Id').Value):$($rs.Fields('ParentId').Value)"
                        $rs.MoveNext()
                    }
                } finally { $rs.Close() }
            } finally { $db.Close() }
            """;
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, $"DAO failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains("ID=1", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("ORPHAN_REJECTED=3201", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("RELATION=AddedChild:Added:Child", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("CHILD=10:1", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("CHILD=20:2", result.StdOut, StringComparison.Ordinal);
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        Assert.Equal(2, await reader.GetRealRowCountAsync("Added", TestContext.Current.CancellationToken));
        Assert.Equal(2, await reader.GetRealRowCountAsync("Child", TestContext.Current.CancellationToken));
        using DataTable parents = await reader.ReadTableAsync("Added", cancellationToken: TestContext.Current.CancellationToken);
        using DataTable children = await reader.ReadTableAsync("Child", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal([1, 2], parents.AsEnumerable().Select(row => (int)row["Id"]).OrderBy(id => id));
        Assert.Equal([(10, 1), (20, 2)], children.AsEnumerable().Select(row => ((int)row["Id"], (int)row["ParentId"])).OrderBy(row => row.Item1));
        RelationshipMetadata relationship = Assert.Single(await reader.ListRelationshipsAsync(TestContext.Current.CancellationToken));
        Assert.Equal("AddedChild", relationship.Name);
        Assert.Equal("Added", relationship.PrimaryTable);
        Assert.Equal("Child", relationship.ForeignTable);
        Assert.Equal("Id", Assert.Single(relationship.PrimaryColumns));
        Assert.Equal("ParentId", Assert.Single(relationship.ForeignColumns));
        Assert.True(relationship.EnforcesReferentialIntegrity);
    }
}
