namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Fresh Jet3 bootstrap output must be usable by an Access97-compatible engine.</summary>
/// <param name="output">Captures native metadata comparisons.</param>
public sealed class Jet3NativeBootstrapTests(ITestOutputHelper output)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>The frozen DAO3.6 catalog establishes the standard bootstrap without requiring Access.</summary>
    [Fact]
    public async Task FreshDatabase_CoreCatalogAndPermissionsMatchFrozenDao36()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.Jet3Bootstrap", databaseExtension: ".mdb");
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(session.SourcePath, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            Assert.True(writer.Database.Format.IsJet3);
        }

        string nativePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "Jet3", "NativeDescriptorMetadata.mdb");
        CatalogSnapshot native = await ReadCatalogSnapshotAsync(nativePath);
        CatalogSnapshot written = await ReadCatalogSnapshotAsync(session.SourcePath);
        this.AssertCatalogEquivalent(native, written);
    }

    /// <summary>Core catalog definitions, object metadata and permissions agree with native Jet3 creation.</summary>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task FreshDatabase_CoreCatalogAndPermissionsMatchDao36()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.Jet3Bootstrap", databaseExtension: ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string script = $$"""
            $db=$engine.CreateDatabase({{source}},';LANGID=0x0409;CP=1252;COUNTRY=0',32)
            $db.Close()
            """;
        RunNative(session, script);
        string writerPath = session.CreateDatabasePath("writer");
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(writerPath, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            Assert.True(writer.Database.Format.IsJet3);
        }

        CatalogSnapshot native = await ReadCatalogSnapshotAsync(session.SourcePath);
        CatalogSnapshot written = await ReadCatalogSnapshotAsync(writerPath);
        this.AssertCatalogEquivalent(native, written);
    }

    /// <summary>DAO3.6 opens, reads and mutates fresh writer-created Jet3 output before compacting it.</summary>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task FreshDatabase_Dao36OpensMutatesAndCompacts()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.Jet3Bootstrap", databaseExtension: ".mdb");
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(session.SourcePath, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Label", typeof(string), maxLength: 40)], Ct);
            await writer.InsertRowAsync("Items", new RowValues { ["Id"] = 1, ["Label"] = "writer" }, Ct);
        }

        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string script = $$"""
            $db=$engine.OpenDatabase({{source}})
            try {
                $rs=$db.OpenRecordset('SELECT Id,Label FROM Items',4)
                try {
                    if($rs.EOF -or $rs.Fields.Item('Id').Value -ne 1 -or $rs.Fields.Item('Label').Value -ne 'writer') { throw 'DAO did not read the writer row' }
                } finally { $rs.Close() }
                $db.Execute("INSERT INTO Items VALUES (2,'native-insert')",128)
                $db.Execute("UPDATE Items SET Label='native-update' WHERE Id=1",128)
                $db.Execute('DELETE FROM Items WHERE Id=2',128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}},{{compacted}})
            $db=$engine.OpenDatabase({{compacted}})
            try {
                $rs=$db.OpenRecordset('SELECT Id,Label FROM Items',4)
                try {
                    if($rs.EOF -or $rs.Fields.Item('Id').Value -ne 1 -or $rs.Fields.Item('Label').Value -ne 'native-update') { throw 'DAO compact lost the surviving row' }
                    $rs.MoveNext()
                    if(!$rs.EOF) { throw 'DAO delete did not survive compact' }
                } finally { $rs.Close() }
            } finally { $db.Close() }
            Write-Output 'JET3_BOOTSTRAP_DML_COMPACT_OK'
            """;
        RunNative(session, script);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(session.CompactedPath, cancellationToken: Ct);
        Assert.Equal("Items", Assert.Single(await reader.ListTablesAsync(Ct)));
        using DataTable rows = await reader.ReadTableAsync("Items", cancellationToken: Ct);
        DataRow row = Assert.Single(rows.AsEnumerable());
        Assert.Equal(1, row["Id"]);
        Assert.Equal("native-update", row["Label"]);
    }

    /// <summary>A first relationship on fresh Jet3 output is enforced natively and retained through compact.</summary>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task FreshDatabase_Dao36EnforcesRelationshipAndCascadesThroughCompact()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.Jet3Bootstrap", databaseExtension: ".mdb");
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(session.SourcePath, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await writer.CreateTableAsync("P", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Label", typeof(string), maxLength: 40)], Ct);
            await writer.CreateTableAsync("C", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int))], Ct);
            await writer.InsertRowAsync("P", new RowValues { ["Id"] = 1, ["Label"] = "writer" }, Ct);
            await writer.InsertRowAsync("C", new RowValues { ["Id"] = 10, ["ParentId"] = 1 }, Ct);
        }

        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await writer.CreateRelationshipAsync(new RelationshipDefinition("Rel", "P", "Id", "C", "ParentId") { CascadeUpdates = true, CascadeDeletes = true }, Ct);
        }

        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string script = $$"""
            function VerifyRelation($db) {
                $relation=$db.Relations.Item('Rel')
                if($relation.Table -ne 'P' -or $relation.ForeignTable -ne 'C' -or $relation.Attributes -ne 4352) { throw 'Wrong native relationship metadata' }
                if($relation.Fields.Count -ne 1 -or $relation.Fields.Item(0).Name -ne 'Id' -or $relation.Fields.Item(0).ForeignName -ne 'ParentId') { throw 'Wrong native relationship fields' }
                $rejected=$false
                try { $db.Execute('INSERT INTO C VALUES (99,999)',128) } catch {
                    $codes=@($engine.Errors | ForEach-Object { $_.Number })
                    if($codes.Count -ne 1 -or $codes[0] -ne 3201) { throw }
                    $rejected=$true
                }
                if(!$rejected) { throw 'DAO accepted an orphan row' }
            }
            $db=$engine.OpenDatabase({{source}})
            try {
                Write-Output 'VERIFY_RELATION_BEFORE_COMPACT'
                VerifyRelation $db
                $db.Execute('UPDATE P SET Id=2 WHERE Id=1',128)
                $db.Execute("INSERT INTO P VALUES (3,'delete-parent')",128)
                $db.Execute('INSERT INTO C VALUES (11,3)',128)
                $db.Execute('DELETE FROM P WHERE Id=3',128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}},{{compacted}})
            $db=$engine.OpenDatabase({{compacted}})
            try {
                Write-Output 'VERIFY_RELATION_AFTER_COMPACT'
                VerifyRelation $db
                $rs=$db.OpenRecordset('SELECT Id,ParentId FROM C',4)
                try {
                    if($rs.EOF -or $rs.Fields.Item('Id').Value -ne 10 -or $rs.Fields.Item('ParentId').Value -ne 2) { throw 'DAO update cascade did not survive compact' }
                    $rs.MoveNext()
                    if(!$rs.EOF) { throw 'DAO delete cascade did not survive compact' }
                } finally { $rs.Close() }
            } finally { $db.Close() }
            Write-Output 'JET3_BOOTSTRAP_RELATION_COMPACT_OK'
            """;
        RunNative(session, script);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(session.CompactedPath, cancellationToken: Ct);
        using DataTable parents = await reader.ReadTableAsync("P", cancellationToken: Ct);
        Assert.Equal(2, Assert.Single(parents.AsEnumerable())["Id"]);
        using DataTable children = await reader.ReadTableAsync("C", cancellationToken: Ct);
        DataRow child = Assert.Single(children.AsEnumerable());
        Assert.Equal(10, child["Id"]);
        Assert.Equal(2, child["ParentId"]);
        using DataTable relationships = await reader.ReadTableAsync("MSysRelationships", cancellationToken: Ct);
        Assert.Equal("Rel", Assert.Single(relationships.AsEnumerable())["szRelationship"]);
    }

    private static void RunNative(AccessRoundTripSession session, string script)
    {
        PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, $"DAO3.6 bootstrap verification failed:\n{result.StandardOutput}\n{result.StandardError}");
    }

    private static async Task<CatalogSnapshot> ReadCatalogSnapshotAsync(string path)
    {
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(path, cancellationToken: Ct);
        using DataTable objects = await reader.ReadTableAsync("MSysObjects", cancellationToken: Ct);
        var names = objects.AsEnumerable().ToDictionary(row => Assert.IsType<int>(row["Id"]), row => Assert.IsType<string>(row["Name"]));
        string ObjectName(object value)
        {
            int id = Assert.IsType<int>(value);
            return names.TryGetValue(id, out string? name) ? name : id.ToString(CultureInfo.InvariantCulture);
        }

        string[] coreObjects = ["MSysObjects", "MSysACEs", "MSysQueries", "MSysRelationships", "Tables", "Databases", "Relationships", "MSysDb"];
        string[] objectRows = objects.AsEnumerable().Where(row => coreObjects.Contains(Assert.IsType<string>(row["Name"]))).Select(row => string.Join(
            "|",
            row["Name"],
            ObjectName(row["ParentId"]),
            row["Type"],
            row["Flags"],
            Convert.ToHexString(Assert.IsType<byte[]>(row["Owner"]))))
            .Order(StringComparer.Ordinal).ToArray();
        var columns = new List<string>();
        foreach (string table in new[] { "MSysObjects", "MSysACEs", "MSysQueries", "MSysRelationships" })
        {
            int root = Assert.Single(names, pair => pair.Value == table).Key;
            TableDef definition = await reader.ReadTableDefAsync(root, Ct) ?? throw new InvalidOperationException($"Unreadable {table}");
            columns.AddRange(definition.Columns.Select(column => string.Join("|", table, column.Name, column.Type, column.Size, column.Flags)));
        }

        using DataTable permissions = await reader.ReadTableAsync("MSysACEs", cancellationToken: Ct);
        string[] permissionRows = permissions.AsEnumerable().Where(row => coreObjects.Contains(ObjectName(row["ObjectId"]))).Select(row => string.Join(
            "|",
            ObjectName(row["ObjectId"]),
            Convert.ToHexString(Assert.IsType<byte[]>(row["SID"])),
            row["ACM"],
            row["FInheritable"]))
            .Order(StringComparer.Ordinal).ToArray();
        return new CatalogSnapshot(columns.ToArray(), objectRows, permissionRows);
    }

    private void AssertCatalogEquivalent(CatalogSnapshot native, CatalogSnapshot written)
    {
        output.WriteLine("Native catalog columns:\n" + string.Join("\n", native.Columns));
        output.WriteLine("Native catalog objects:\n" + string.Join("\n", native.Objects));
        output.WriteLine("Native permissions:\n" + string.Join("\n", native.Permissions));
        Assert.Equal(native.Columns, written.Columns);
        Assert.Equal(native.Objects, written.Objects);
        Assert.Equal(native.Permissions, written.Permissions);
    }

    private sealed record CatalogSnapshot(string[] Columns, string[] Objects, string[] Permissions);
}
