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
using Xunit;

/// <summary>Controlled inert Access objects establish private assembly preservation without executing application code.</summary>
/// <param name="output">Records complete comparison and native metadata evidence.</param>
public sealed class CompactPinnedApplicationObjectsTests(ITestOutputHelper output)
{
    private const string FixtureHash = "C809AA9371DF43F0901F6AA27C5ADDEEAAA7BEDA07C37578F2863F3CCF77364F";
    private const string CodeFixtureHash = "EA1899DD97A8492C30B2F720201DD3A13046BEC23915C21A7DD938A12041FEE4";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task FrozenApplicationObjects_CompleteAssemblyPreservesCatalogAndOpaqueStorage()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.CompactApplicationObjects");
        byte[] original = await SeedAsync(session);
        string staged = await this.BuildAsync(session);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(session.SourcePath, cancellationToken: Ct);
        await using AccessReader destination = await TestDatabase.OpenReaderAsync(staged, cancellationToken: Ct);
        using DataTable expectedCatalog = await source.ReadTableAsync("MSysObjects", cancellationToken: Ct);
        using DataTable actualCatalog = await destination.ReadTableAsync("MSysObjects", cancellationToken: Ct);
        AssertIdentity(expectedCatalog, "EvidenceQuery", -2147483638, 5);
        AssertIdentity(expectedCatalog, "EvidenceForm", -2147483637, -32768);
        AssertIdentity(expectedCatalog, "EvidenceReport", -2147483636, -32764);
        CompareRows(expectedCatalog, actualCatalog, "Id");
        foreach (string name in new[] { "MSysAccessStorage", "MSysQueries", "f_C54F32B8B3CB4285928325593DE12252_Data" })
        {
            using DataTable expected = await source.ReadTableAsync(name, cancellationToken: Ct);
            using DataTable actual = await destination.ReadTableAsync(name, cancellationToken: Ct);
            CompareRows(expected, actual, expected.Columns[0].ColumnName);
        }

        Assert.Equal(original, await File.ReadAllBytesAsync(session.SourcePath, Ct));
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task FrozenApplicationObjects_Dao120PreservesDefinitionsAndNamesThroughCompact()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.CompactApplicationObjects.Native");
        byte[] original = await SeedAsync(session);
        string staged = await this.BuildAsync(session);
        string sourcePath = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string stagedPath = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(staged);
        string compactedPath = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string script = $$"""
            function Check-Definitions([string]$path) {
                $db=$engine.OpenDatabase($path,$false,$true)
                try {
                    $query=$db.QueryDefs.Item('EvidenceQuery')
                    $sql=($query.SQL -replace '\s+',' ').Trim()
                    if($sql -cne 'SELECT [Id], [Label] FROM Items WHERE [Id] > 0 ORDER BY [Id];') { throw 'Saved query SQL changed' }
                    if($db.Containers.Item('Forms').Documents.Item('EvidenceForm').Name -cne 'EvidenceForm') { throw 'Form identity lost' }
                    if($db.Containers.Item('Reports').Documents.Item('EvidenceReport').Name -cne 'EvidenceReport') { throw 'Report identity lost' }
                    $rs=$db.OpenRecordset('SELECT [Id],[Label] FROM [Items]',4)
                    try {
                        if($rs.EOF -or $rs.Fields.Item('Id').Value -ne 1 -or $rs.Fields.Item('Label').Value -cne 'retained') { throw 'Controlled row changed' }
                        $rs.MoveNext()
                        if(!$rs.EOF) { throw 'Unexpected controlled row' }
                    } finally { $rs.Close() }
                } finally { $db.Close() }
            }
            Check-Definitions {{sourcePath}}
            Check-Definitions {{stagedPath}}
            $engine.CompactDatabase({{stagedPath}},{{compactedPath}})
            Check-Definitions {{compactedPath}}
            Write-Output 'INERT_APPLICATION_QUERY_METADATA_AND_OBJECT_NAMES_PRESERVED=OK'
            """;
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, $"DAO inert application comparison failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains("INERT_APPLICATION_QUERY_METADATA_AND_OBJECT_NAMES_PRESERVED=OK", result.StdOut, StringComparison.Ordinal);
        Assert.Equal(original, await File.ReadAllBytesAsync(session.SourcePath, Ct));
        output.WriteLine(result.StdOut);
    }

    [Fact]
    public async Task FrozenCodeObjects_CompleteAssemblyPreservesCatalogAndOpaqueStorage()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.CompactCodeObjects");
        byte[] original = await SeedAsync(session, "NativeInertCodeObjects.accdb", CodeFixtureHash);
        string staged = await this.BuildAsync(session);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(session.SourcePath, cancellationToken: Ct);
        await using AccessReader destination = await TestDatabase.OpenReaderAsync(staged, cancellationToken: Ct);
        using DataTable expectedCatalog = await source.ReadTableAsync("MSysObjects", cancellationToken: Ct);
        using DataTable actualCatalog = await destination.ReadTableAsync("MSysObjects", cancellationToken: Ct);
        AssertIdentity(expectedCatalog, "EvidenceMacro", -2147483634, -32766);
        AssertIdentity(expectedCatalog, "EvidenceModule", -2147483635, -32761);
        CompareRows(expectedCatalog, actualCatalog, "Id");
        foreach (string name in new[] { "MSysAccessStorage", "MSysQueries", "f_C54F32B8B3CB4285928325593DE12252_Data" })
        {
            using DataTable expected = await source.ReadTableAsync(name, cancellationToken: Ct);
            using DataTable actual = await destination.ReadTableAsync(name, cancellationToken: Ct);
            CompareRows(expected, actual, expected.Columns[0].ColumnName);
        }

        Assert.Equal(original, await File.ReadAllBytesAsync(session.SourcePath, Ct));
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task FrozenCodeObjects_Dao120PreservesInertMetadataThroughCompact()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.CompactCodeObjects.Native");
        byte[] original = await SeedAsync(session, "NativeInertCodeObjects.accdb", CodeFixtureHash);
        string staged = await this.BuildAsync(session);
        string sourcePath = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string stagedPath = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(staged);
        string compactedPath = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string script = $$"""
            function Check-InertObjects([string]$path) {
                $db=$engine.OpenDatabase($path,$false,$true)
                try {
                    if($db.Containers.Item('Scripts').Documents.Item('EvidenceMacro').Name -cne 'EvidenceMacro') { throw 'Macro identity lost' }
                    if($db.Containers.Item('Modules').Documents.Item('EvidenceModule').Name -cne 'EvidenceModule') { throw 'Module identity lost' }
                } finally { $db.Close() }
            }
            Check-InertObjects {{sourcePath}}
            Check-InertObjects {{stagedPath}}
            $engine.CompactDatabase({{stagedPath}},{{compactedPath}})
            Check-InertObjects {{compactedPath}}
            Write-Output 'INERT_MACRO_MODULE_METADATA_PRESERVED=OK'
            """;
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, $"DAO inert macro/module comparison failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains("INERT_MACRO_MODULE_METADATA_PRESERVED=OK", result.StdOut, StringComparison.Ordinal);
        Assert.Equal(original, await File.ReadAllBytesAsync(session.SourcePath, Ct));
        output.WriteLine(result.StdOut);
    }

    private static async Task<byte[]> SeedAsync(AccessRoundTripSession session, string name = "NativeInertApplicationObjects.accdb", string hash = FixtureHash)
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Databases", "ApplicationObjects", name), Ct);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(original)));
        await File.WriteAllBytesAsync(session.SourcePath, original, Ct);
        return original;
    }

    private static void AssertIdentity(DataTable catalog, string name, int id, int type)
    {
        DataRow row = Assert.Single(catalog.AsEnumerable(), row => (string)row["Name"] == name);
        Assert.Equal(id, row["Id"]);
        Assert.Equal((short)type, row["Type"]);
    }

    private static void CompareRows(DataTable expected, DataTable actual, string key)
    {
        Assert.Equal(expected.Columns.Cast<DataColumn>().Select(column => column.ColumnName), actual.Columns.Cast<DataColumn>().Select(column => column.ColumnName));
        DataRow[] expectedRows = expected.AsEnumerable().OrderBy(row => row[key]).ToArray();
        DataRow[] actualRows = actual.AsEnumerable().OrderBy(row => row[key]).ToArray();
        Assert.Equal(expectedRows.Length, actualRows.Length);
        for (int index = 0; index < expectedRows.Length; index++)
        {
            Assert.Equal(expectedRows[index].ItemArray, actualRows[index].ItemArray);
        }
    }

    private static async IAsyncEnumerable<IndexEntry> EntriesAsync(IEnumerable<IndexEntry> entries, [EnumeratorCancellation] CancellationToken token)
    {
        await Task.CompletedTask;
        foreach (IndexEntry entry in entries)
        {
            token.ThrowIfCancellationRequested();
            yield return entry;
        }
    }

    private static string PathFor(string directory, CompactPinnedDatabaseFile role, long owner)
        => Path.Combine(directory, $"{role}-{owner}.bin");

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
        Assert.Equal(23, result.Validation.Tables);
        Assert.Equal(tables.Count, result.Validation.Prefix.Definitions);
        Assert.Equal(source.Database.Pages.PageCount, result.Validation.Prefix.PinnedPages);
        Assert.True(result.Validation.DestinationInventoryCompared);
        Assert.Equal(entries.Count, result.Validation.CatalogObjects);
        Assert.False(result.DatabaseValidated);
        Assert.False(result.ReclamationAuthorized);
        Assert.False(result.PublicationAuthorized);
        output.WriteLine($"Controlled application objects: {result.Validation.Tables} complete tables, {result.Validation.CatalogObjects} catalog identities, {result.Validation.Prefix.PinnedPages} exact pinned pages and reconstructed TDEF references.");
        string staged = session.CreateDatabasePath("staged");
        File.Copy(PathFor(directory, CompactPinnedDatabaseFile.Destination, 0), staged);
        return staged;
    }
}
