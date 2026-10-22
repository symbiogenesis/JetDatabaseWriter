namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Inert native Jet4 replicas retain their raw metadata and identities through private assembly.</summary>
/// <param name="output">Records the exact fixture and scoped preservation evidence.</param>
public sealed class CompactPinnedReplicationTests(ITestOutputHelper output)
{
    private const string DesignMasterId = "{guid {7B8B2136-2680-4FCF-AB0F-1940CE0FB38E}}";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("Jet4-DesignMaster.mdb")]
    [InlineData("Jet4-FullReplica.mdb")]
    public async Task FrozenReplicas_PreserveCompleteCatalogSystemRowsAndPinnedStorage(string fixture)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.CompactReplication", databaseExtension: ".mdb");
        byte[] frozen = await SeedAsync(session, fixture);
        string staged = await this.BuildAsync(session);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(session.SourcePath, cancellationToken: Ct);
        await using AccessReader destination = await TestDatabase.OpenReaderAsync(staged, cancellationToken: Ct);
        using DataTable catalog = await source.ReadTableAsync("MSysObjects", cancellationToken: Ct);
        Assert.Contains(catalog.AsEnumerable(), static row => (string)row["Name"] == "MSysRepInfo");
        Assert.Contains(catalog.AsEnumerable(), static row => (string)row["Name"] == "MSysReplicas");
        Assert.Contains(catalog.AsEnumerable(), static row => (string)row["Name"] == "MSysTombstone");
        foreach (DataRow entry in catalog.AsEnumerable().Where(static row => (short)row["Type"] == Constants.SystemObjects.UserTableType))
        {
            string name = (string)entry["Name"];
            using DataTable expected = await source.ReadTableAsync(name, cancellationToken: Ct);
            using DataTable actual = await destination.ReadTableAsync(name, cancellationToken: Ct);
            Assert.Equal(expected.Columns.Cast<DataColumn>().Select(static column => column.ColumnName), actual.Columns.Cast<DataColumn>().Select(static column => column.ColumnName));
            Assert.Equal(expected.Rows.Count, actual.Rows.Count);
            for (int row = 0; row < expected.Rows.Count; row++)
            {
                Assert.Equal(expected.Rows[row].ItemArray, actual.Rows[row].ItemArray);
            }
        }

        using DataTable items = await destination.ReadTableAsync("Items", cancellationToken: Ct);
        Assert.Equal(8, items.Columns.Count);
        Assert.Equal("inert short memo", Assert.Single(items.Select("Id = 1"))["Notes"]);
        Assert.Equal("memo-" + new string('x', 5000), Assert.Single(items.Select("Id = 2"))["Notes"]);
        Assert.Equal(frozen, await File.ReadAllBytesAsync(session.SourcePath, Ct));
        Assert.Equal(frozen, await File.ReadAllBytesAsync(FixturePath(fixture), Ct));
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [InlineData("Jet4-DesignMaster.mdb")]
    [InlineData("Jet4-FullReplica.mdb")]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task FrozenReplicas_Dao36PreservesIdentityAndReplicatedMetadataThroughCompact(string fixture)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.CompactReplication.Native", databaseExtension: ".mdb");
        byte[] frozen = await SeedAsync(session, fixture);
        string staged = await this.BuildAsync(session);
        string script = NativeComparisonScript(session, staged, fixture);
        PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, $"Native replication preservation failed:\n{result.StandardOutput}\n{result.StandardError}");
        Assert.Contains("INERT_REPLICATION_IDENTITIES_AND_METADATA_PRESERVED=OK", result.StandardOutput, StringComparison.Ordinal);
        Assert.Equal(frozen, await File.ReadAllBytesAsync(session.SourcePath, Ct));
        Assert.Equal(frozen, await File.ReadAllBytesAsync(FixturePath(fixture), Ct));
        output.WriteLine(result.StandardOutput);
    }

    private static string FixturePath(string name) => Path.Combine(AppContext.BaseDirectory, "Databases", "Replication", name);

    private static async Task<byte[]> SeedAsync(AccessRoundTripSession session, string fixture)
    {
        byte[] bytes = await File.ReadAllBytesAsync(FixturePath(fixture), Ct);
        string expected = fixture == "Jet4-DesignMaster.mdb"
            ? "AB3390B4DA7C87C65BCFC5376AF2A3E989676E9B4FC1D51C86E05D993CC0AA0E"
            : "A4C286EDABCD06FD1E2BDFB2815244A918CF020D96D4C0923EBCF6760C9D2F95";
        Assert.Equal(expected, Convert.ToHexString(SHA256.HashData(bytes)));
        await File.WriteAllBytesAsync(session.SourcePath, bytes, Ct);
        return bytes;
    }

    private static string NativeComparisonScript(AccessRoundTripSession session, string staged, string fixture)
    {
        string replicaId = fixture == "Jet4-DesignMaster.mdb" ? DesignMasterId : "{guid {BD3E443F-0B2C-4028-98AE-3955DAEA2BF6}}";
        return $$"""
            function Check-Replica([string]$path) {
                $db=$engine.OpenDatabase($path,$false,$true)
                try {
                    if($db.Version -ne '4.0' -or $db.Properties.Item('Replicable').Value -ne 'T') { throw 'Native replicated format changed' }
                    if([string]$db.ReplicaID -cne {{Literal(replicaId)}} -or [string]$db.DesignMasterID -cne {{Literal(DesignMasterId)}}) { throw 'Native replication identity changed' }
                    $table=$db.TableDefs.Item('Items')
                    $fields=@($table.Fields | ForEach-Object { $_.Name+':'+$_.Type+':'+$_.Attributes })
                    $expected='Id:4:1|Label:10:2|Notes:12:2|Gen_Notes:4:8194|s_ColLineage:11:8194|s_Generation:4:8194|s_GUID:15:8194|s_Lineage:11:8194'
                    if(($fields -join '|') -cne $expected) { throw 'Native replicated column metadata changed' }
                    $indexes=@($table.Indexes | ForEach-Object { $_.Name+':'+$_.Unique+':'+$_.Primary })
                    if(($indexes -join '|') -cne 'PrimaryKey:True:True|s_Generation:False:False|s_GUID:True:False') { throw 'Native replicated index metadata changed' }
                    foreach($name in @('MSysRepInfo','MSysReplicas','MSysTombstone','MSysGenHistory','MSysTableGuids','MSysSideTables')) {
                        if($db.TableDefs.Item($name).Name -cne $name) { throw 'Native replication system table lost' }
                    }
                    $rows=$db.OpenRecordset('SELECT [Id],[Label],[Notes] FROM [Items] ORDER BY [Id]',4)
                    try {
                        $count=0
                        while(!$rows.EOF) {
                            $id=[int]$rows.Fields.Item('Id').Value
                            if($id -eq 1) { $label='first';$memo='inert short memo' } elseif($id -eq 2) { $label='second';$memo='memo-'+('x'*5000) } else { throw 'Unexpected native row Id' }
                            if($rows.Fields.Item('Label').Value -cne $label -or $rows.Fields.Item('Notes').Value -cne $memo) { throw 'Native stored literal values changed' }
                            $count++;$rows.MoveNext()
                        }
                        if($count -ne 2) { throw 'Native row count changed' }
                    } finally { $rows.Close() }
                    Write-Output ('REPLICA_ID='+$db.ReplicaID+' DESIGN_MASTER_ID='+$db.DesignMasterID+' FILE='+[IO.Path]::GetFileName($path))
                } finally { $db.Close() }
            }
            Check-Replica {{Literal(session.SourcePath)}}
            Check-Replica {{Literal(staged)}}
            $engine.CompactDatabase({{Literal(session.SourcePath)}},{{Literal(Path.Combine(session.WorkDir, "control-compacted.mdb"))}})
            $engine.CompactDatabase({{Literal(staged)}},{{Literal(session.CompactedPath)}})
            Check-Replica {{Literal(Path.Combine(session.WorkDir, "control-compacted.mdb"))}}
            Check-Replica {{Literal(session.CompactedPath)}}
            Write-Output 'INERT_REPLICATION_IDENTITIES_AND_METADATA_PRESERVED=OK'
            """;
    }

    private static string Literal(string value) => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(value);

    private static async IAsyncEnumerable<IndexEntry> EntriesAsync(IEnumerable<IndexEntry> entries, [EnumeratorCancellation] CancellationToken token)
    {
        await Task.CompletedTask;
        foreach (IndexEntry entry in entries)
        {
            token.ThrowIfCancellationRequested();
            yield return entry;
        }
    }

    private static string PathFor(string directory, CompactPinnedDatabaseFile role, long owner) => Path.Combine(directory, $"{role}-{owner}.bin");

#pragma warning disable CA2000 // The coordinator owns and closes every stream returned by these factories.
    private static ValueTask<Stream> CreateAsync(string directory, CompactPinnedDatabaseFile role, long owner)
        => ValueTask.FromResult<Stream>(new FileStream(PathFor(directory, role, owner), FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None));

    private static ValueTask<Stream> OpenAsync(string directory, CompactPinnedDatabaseFile role, long owner)
        => ValueTask.FromResult<Stream>(new FileStream(PathFor(directory, role, owner), FileMode.Open, FileAccess.Read, FileShare.Read));
#pragma warning restore CA2000

    private async Task<string> BuildAsync(AccessRoundTripSession session)
    {
        string directory = Path.Combine(session.WorkDir, "private");
        Directory.CreateDirectory(directory);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(session.SourcePath, cancellationToken: Ct);
        TDefImage catalog = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(2, Ct));
        var decoder = new CatalogRowReader(source.Database.Format, source.Database.TableDefs, source.Database.OwnedPages);
        List<CatalogRow> entries = await decoder.GetCatalogRowsAsync(catalog.Definition, Ct);
        var tables = new List<CompactPinnedDatabaseBuildTable>();
        foreach (CatalogRow entry in entries.Where(static entry => entry.ObjectType == Constants.SystemObjects.UserTableType).OrderBy(static entry => entry.TDefPage))
        {
            TDefImage image = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(entry.TDefPage, Ct));
            var indexes = new CompactPinnedTableIndexSource[image.RealIndexes.Count];
            for (int slot = 0; slot < indexes.Length; slot++)
            {
                TDefIndexDescriptor descriptor = image.RealIndexes[slot];
                Assert.True(UsageMap.TryReadPointer(image.CopyBytes(), descriptor.Slot.FirstDpOffset - 4, out UsageMapPointer map));
                List<IndexEntry> stored = await IndexLeafChain.ReadEntriesAsync(source.Database, entry.TDefPage, descriptor.RootPage, Ct);
                indexes[slot] = new(descriptor.RootPage, map, EntriesAsync(stored, Ct));
            }

            tables.Add(new(entry.TDefPage, image, indexes));
        }

        var workspace = new CompactPinnedDatabaseWorkspace(
            (role, owner, _) => CreateAsync(directory, role, owner),
            (role, owner, _) => OpenAsync(directory, role, owner));
        CompactPinnedDatabaseBuilderResult result = await CompactPinnedDatabaseBuilder.BuildAndValidateAsync(source.Database.Pages, source.Database.Format, tables, workspace, new(), Ct);
        Assert.Equal(18, result.Validation.Tables);
        Assert.Equal(tables.Count, result.Validation.Prefix.Definitions);
        Assert.Equal(entries.Count, result.Validation.CatalogObjects);
        Assert.Equal(source.Database.Pages.PageCount, result.Validation.Prefix.PinnedPages);
        Assert.True(result.Validation.DestinationInventoryCompared);
        Assert.False(result.Validation.Prefix.OwnershipProven);
        Assert.False(result.DatabaseValidated);
        Assert.False(result.ReclamationAuthorized);
        Assert.False(result.PublicationAuthorized);
        string staged = session.CreateDatabasePath("staged");
        File.Copy(PathFor(directory, CompactPinnedDatabaseFile.Destination, 0), staged);
        output.WriteLine($"Compared {result.Validation.Tables} replicated/system tables, {result.Validation.CatalogObjects} catalog identities and {result.Validation.Prefix.PinnedPages} complete pinned-prefix pages; no synchronization or application code execution.");
        return staged;
    }
}
