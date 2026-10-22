namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
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

/// <summary>Complete encrypted native fixture assemblies preserve their exact provider and reopen through native engines.</summary>
/// <param name="output">Records codec and native evidence.</param>
public sealed class CompactEncodedDatabaseNativeTests(ITestOutputHelper output)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("NativeJet3Rc4.mdb", "Pássword")]
    [InlineData("NativeJet4Rc4.mdb", "Native123")]
    [InlineData("NativeAceAgile.accdb", "Native123")]
    [InlineData("NativeAceStandard.accdb", "Password1234_")]
    public async Task NativeFixtures_CompleteAssemblyEncodesAndReopensExactOriginalProvider(string fixture, string password)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.CompactEncoded", databaseExtension: Path.GetExtension(fixture));
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, fixture), Ct);
        string encoded = await this.BuildAndEncodeAsync(session, fixture, password);
        await using AccessReader expected = await AccessReader.OpenAsync(Path.Combine(TestDatabases.EncryptedRoot, fixture), new AccessReaderOptions(password) { UseLockFile = false }, Ct);
        await using AccessReader actual = await AccessReader.OpenAsync(encoded, new AccessReaderOptions(password) { UseLockFile = false }, Ct);
        IReadOnlyList<string> names = await expected.ListTablesAsync(Ct);
        Assert.Equal(names, await actual.ListTablesAsync(Ct));
        foreach (string name in names)
        {
            using DataTable expectedRows = await expected.ReadTableAsync(name, cancellationToken: Ct);
            using DataTable actualRows = await actual.ReadTableAsync(name, cancellationToken: Ct);
            Assert.Equal(expectedRows.AsEnumerable().Select(row => row.ItemArray), actualRows.AsEnumerable().Select(row => row.ItemArray));
        }

        Assert.Equal(original, await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, fixture), Ct));
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Jet3EncodedAssembly_Dao36ReadsWritesEnforcesAndCompacts()
        => await this.NativeRoundTripAsync("NativeJet3Rc4.mdb", "Pássword", jet3: true);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData("NativeJet4Rc4.mdb", "Native123")]
    [InlineData("NativeAceAgile.accdb", "Native123")]
    [InlineData("NativeAceStandard.accdb", "Password1234_")]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task EncodedAssembly_Dao120ReadsWritesEnforcesAndCompacts(string fixture, string password)
        => await this.NativeRoundTripAsync(fixture, password, jet3: false);

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

#pragma warning disable CA2000 // Factories transfer ownership to the coordinator, which closes every returned stream.
    private static ValueTask<Stream> CreateAsync(string directory, CompactPinnedDatabaseFile role, long owner)
        => ValueTask.FromResult<Stream>(new FileStream(PathFor(directory, role, owner), FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None));

    private static ValueTask<Stream> OpenAsync(string directory, CompactPinnedDatabaseFile role, long owner)
        => ValueTask.FromResult<Stream>(File.OpenRead(PathFor(directory, role, owner)));
#pragma warning restore CA2000

    private async Task<string> BuildAndEncodeAsync(AccessRoundTripSession session, string fixture, string password)
    {
        string directory = Path.Combine(session.WorkDir, "assembly");
        Directory.CreateDirectory(directory);
        string originalPath = Path.Combine(TestDatabases.EncryptedRoot, fixture);
        byte[] original = await File.ReadAllBytesAsync(originalPath, Ct);
        int pageSize;
        await using (AccessReader source = await AccessReader.OpenAsync(originalPath, new AccessReaderOptions(password) { UseLockFile = false }, Ct))
        {
            pageSize = source.PageSize;
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

            var assembly = new CompactPinnedDatabaseWorkspace(
                (role, owner, _) => CreateAsync(directory, role, owner),
                (role, owner, _) => OpenAsync(directory, role, owner));
            CompactPinnedDatabaseBuilderResult assembled = await CompactPinnedDatabaseBuilder.BuildAndValidateAsync(source.Database.Pages, source.Database.Format, tables, assembly, new(), Ct);
            Assert.False(assembled.DatabaseValidated);
        }

        string staging = PathFor(directory, CompactPinnedDatabaseFile.Destination, 0);
        byte[] stagedBefore = await File.ReadAllBytesAsync(staging, Ct);
        string encoded = session.CreateDatabasePath("encoded");
#pragma warning disable CA2000 // The codec coordinator owns every returned stream.
        var workspace = new CompactEncodedDatabaseWorkspace(
            _ => ValueTask.FromResult<Stream>(File.OpenRead(staging)),
            _ => ValueTask.FromResult<Stream>(new FileStream(encoded, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None)),
            _ => ValueTask.FromResult<Stream>(File.OpenRead(encoded)));
#pragma warning restore CA2000
        CompactEncodedDatabaseResult result = await CompactEncodedDatabaseCopy.EncodeAndValidateAsync(original.AsMemory(0, pageSize), password.AsMemory(), workspace, new(), Ct);
        Assert.Equal(stagedBefore.Length / pageSize, result.Pages);
        Assert.Equal(stagedBefore.Length, result.WrittenBytes);
        Assert.Equal(3L * stagedBefore.Length, result.ReadBytes);
        Assert.False(result.DatabaseValidated);
        Assert.False(result.ReclamationAuthorized);
        Assert.False(result.PublicationAuthorized);
        byte[] encodedBytes = await File.ReadAllBytesAsync(encoded, Ct);
        Assert.Equal(original.AsSpan(0, pageSize).ToArray(), encodedBytes.AsSpan(0, pageSize).ToArray());
        Assert.Equal(stagedBefore, await File.ReadAllBytesAsync(staging, Ct));
        Assert.Equal(original, await File.ReadAllBytesAsync(originalPath, Ct));
        output.WriteLine($"{fixture}: {result.Provider}, {result.Pages} encoded and independently reopened pages, exact original page zero, {result.ReadBytes} requested read bytes, {result.WrittenBytes} write bytes.");
        return encoded;
    }

    private async Task NativeRoundTripAsync(string fixture, string password, bool jet3)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.CompactEncoded.Native", databaseExtension: Path.GetExtension(fixture));
        string encoded = await this.BuildAndEncodeAsync(session, fixture, password);
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(encoded);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string password64 = Convert.ToBase64String(Encoding.Unicode.GetBytes(password));
        string script = $$"""
            $password=[Text.Encoding]::Unicode.GetString([Convert]::FromBase64String('{{password64}}'))
            $connect=';PWD='+$password
            $db=$engine.OpenDatabase({{source}},$false,$false,$connect)
            try {
                $rs=$db.OpenRecordset('SELECT [Label] FROM [T] WHERE [Id]=7',4)
                try { if($rs.EOF -or $rs.Fields.Item('Label').Value -cne 'Native encrypted row') { throw 'Original encrypted row was lost' } } finally { $rs.Close() }
                $db.Execute("UPDATE [T] SET [Label]='native-encoded-update' WHERE [Id]=7",128)
                $db.Execute('CREATE TABLE CodecConstraint (Id LONG CONSTRAINT PK_CodecConstraint PRIMARY KEY)',128)
                $db.Execute('INSERT INTO CodecConstraint VALUES (1)',128)
                $rejected=$false
                try { $db.Execute('INSERT INTO CodecConstraint VALUES (1)',128) } catch {
                    $codes=@($engine.Errors | ForEach-Object { $_.Number })
                    if($codes.Count -ne 1 -or $codes[0] -ne 3022) { throw }
                    $rejected=$true
                }
                if(!$rejected) { throw 'Native primary key accepted a duplicate' }
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}},{{compacted}},';LANGID=0x0409;CP=1252;COUNTRY=0;PWD='+$password,{{(jet3 ? 32 : 0)}},$connect)
            $db=$engine.OpenDatabase({{compacted}},$false,$true,$connect)
            try {
                $rs=$db.OpenRecordset('SELECT [Label] FROM [T] WHERE [Id]=7',4)
                try { if($rs.EOF -or $rs.Fields.Item('Label').Value -cne 'native-encoded-update') { throw 'Native compact lost the update' } } finally { $rs.Close() }
            } finally { $db.Close() }
            Write-Output 'ENCODED_ASSEMBLY_NATIVE_MUTATE_CONSTRAINT_COMPACT=OK'
            """;
        if (jet3)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, $"DAO36 encoded assembly failed: {result.StandardOutput}\n{result.StandardError}");
            output.WriteLine(result.StandardOutput);
        }
        else
        {
            AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0, $"DAO120 encoded assembly failed: {result.StdOut}\n{result.StdErr}");
            output.WriteLine(result.StdOut);
        }

        await using AccessReader reopened = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions(password) { UseLockFile = false }, Ct);
        using DataTable rows = await reopened.ReadTableAsync("T", cancellationToken: Ct);
        Assert.Equal("native-encoded-update", Assert.Single(rows.AsEnumerable(), row => (int)row["Id"] == 7)["Label"]);
    }
}
