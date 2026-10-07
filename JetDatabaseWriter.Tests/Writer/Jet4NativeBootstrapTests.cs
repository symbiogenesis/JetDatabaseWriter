namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Fresh Jet4 databases include native catalog ownership and inherited permissions.</summary>
public sealed class Jet4NativeBootstrapTests
{
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
        JetFormat format = JetFormat.FromHeader(header);
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
            await writer.InsertRowAsync("Added", [1], TestContext.Current.CancellationToken);
        }

        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string destination = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string script = $$"""
            $db = $engine.OpenDatabase({{source}})
            try {
                $rs = $db.OpenRecordset('Added')
                try { Write-Output "ID=$($rs.Fields('Id').Value)" } finally { $rs.Close() }
                $db.Execute('INSERT INTO Added (Id) VALUES (2)', 128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{destination}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 64)
            """;
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, $"DAO failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains("ID=1", result.StdOut, StringComparison.Ordinal);
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        Assert.Equal(2, await reader.GetRealRowCountAsync("Added", TestContext.Current.CancellationToken));
    }
}
