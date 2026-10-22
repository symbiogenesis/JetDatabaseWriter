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
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Complete private assembly preserves native custom owners and grants without claiming user authentication.</summary>
/// <param name="output">Retains bounded native security and preservation observations.</param>
public sealed class CompactPinnedSecurityTests(ITestOutputHelper output)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("NativeJet4Workgroup.mdb", "Native123")]
    [InlineData("NativeJet4WorkgroupChanged.mdb", "Changed123")]
    public async Task WorkgroupFixtures_KeepRawSecurityCatalogAndHeaderBytes(string fixture, string password)
    {
        await using var native = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CompactSecurity", databaseExtension: ".mdb");
        string control = native.CreateDatabasePath("control");
        File.Copy(Path.Combine(TestDatabases.EncryptedRoot, fixture), control);
        await this.AssembleAsync(native, control, password);
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData("NativeJet4Workgroup.mdb", "Native123", false)]
    [InlineData("NativeJet4Workgroup.mdb", "Native123", true)]
    [InlineData("NativeJet4WorkgroupChanged.mdb", "Changed123", false)]
    [InlineData("NativeJet4WorkgroupChanged.mdb", "Changed123", true)]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task WorkgroupPermissions_DaoAuthenticatesDistinguishesDenialsAndCompacts(string fixture, string password, bool restricted)
    {
        await using var native = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CompactSecurity", databaseExtension: ".mdb");
        string fixturePath = Path.Combine(TestDatabases.EncryptedRoot, fixture);
        string workgroupFixture = Path.Combine(TestDatabases.EncryptedRoot, "NativeJetWorkgroup.mdw");
        byte[] frozen = await File.ReadAllBytesAsync(fixturePath, Ct);
        byte[] frozenWorkgroup = await File.ReadAllBytesAsync(workgroupFixture, Ct);
        string control = native.CreateDatabasePath("control");
        string workgroup = Path.Combine(native.WorkDir, "workgroup.mdw");
        await File.WriteAllBytesAsync(control, frozen, Ct);
        await File.WriteAllBytesAsync(workgroup, frozenWorkgroup, Ct);
        try
        {
            if (restricted)
            {
                string authoring = RestrictedProfileScript(control, workgroup, password);
                await File.WriteAllTextAsync(Path.Combine(native.WorkDir, "authoring.ps1"), authoring, Ct);
                AccessRoundTripEnvironment.CompactResult authored = native.RunDaoEngineScript(authoring, TimeSpan.FromMinutes(1));
                Assert.True(authored.ExitCode == 0, $"Native restricted profile authoring failed:\n{authored.StdOut}\n{authored.StdErr}");
            }

            await this.AssembleAsync(native, control, password);
            string script = ComparisonScript(native, control, workgroup, password, restricted);
            await File.WriteAllTextAsync(Path.Combine(native.WorkDir, "verification.ps1"), script, Ct);
            AccessRoundTripEnvironment.CompactResult result = native.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0, $"Native security preservation failed:\n{result.StdOut}\n{result.StdErr}");
            Assert.Contains("NATIVE_COMPACT_SECURITY_PRESERVED=OK", result.StdOut, StringComparison.Ordinal);
            Assert.Contains("AUTHENTICATION_DENIED=3029", result.StdOut, StringComparison.Ordinal);
            Assert.Contains("DATABASE_PASSWORD_DENIED=3031", result.StdOut, StringComparison.Ordinal);
            if (restricted)
            {
                Assert.Contains("READ_ALLOWED_WRITE_DENIED=3108", result.StdOut, StringComparison.Ordinal);
            }

            output.WriteLine(result.StdOut);
            await using AccessReader compacted = await AccessReader.OpenAsync(native.CompactedPath, new AccessReaderOptions(password) { UseLockFile = false }, Ct);
            using DataTable rows = await compacted.ReadTableAsync("T", cancellationToken: Ct);
            Assert.Equal("Native encrypted row", Assert.Single(rows.AsEnumerable())["Label"]);
        }
        finally
        {
            Assert.Equal(frozen, await File.ReadAllBytesAsync(fixturePath, Ct));
            Assert.Equal(frozenWorkgroup, await File.ReadAllBytesAsync(workgroupFixture, Ct));
        }
    }

    private static async Task EncodeNativeSubjectAsync(string privateImage, string nativeSubject, ReadOnlyMemory<byte> originalHeader, string password)
    {
        CompactEncodedDatabaseResult result = await CompactEncodedDatabaseCopy.EncodeAndValidateAsync(
            originalHeader,
            password.AsMemory(),
            new(_ => OpenNativeAsync(privateImage), _ => CreateNativeAsync(nativeSubject), _ => OpenNativeAsync(nativeSubject)),
            new(),
            Ct);
        Assert.Equal(new FileInfo(privateImage).Length / originalHeader.Length, result.Pages);
        Assert.False(result.DatabaseValidated);
        Assert.False(result.PublicationAuthorized);
    }

    private static string RestrictedProfileScript(string control, string workgroup, string password)
        => $$"""
            $engine.SystemDB={{Literal(workgroup)}}
            $workspace=$engine.CreateWorkspace('NativeAuthor','NativeOwner','Owner123',2)
            try {
                $workspace.Users.Append($workspace.CreateUser('CompactReader','CompactReaderPID',''))
                $db=$workspace.OpenDatabase({{Literal(control)}},$false,$false,{{Literal(";PWD=" + password)}})
                try {
                    $document=$db.Containers('Tables').Documents('T')
                    if($document.Owner -ne 'NativeOwner') { throw 'Unexpected authoritative fixture owner.' }
                    $document.UserName='Users'
                    $document.Permissions=0
                    $document.UserName='CompactReader'
                    $document.Permissions=20
                    if($document.Permissions -ne 20) { throw 'Native reader permission setup differs.' }
                } finally { $db.Close() }
            } finally { $workspace.Close() }
            Write-Output 'NATIVE_RESTRICTED_PROFILE_AUTHORED=OK'
            """;

    private static string ComparisonScript(AccessRoundTripSession native, string control, string workgroup, string password, bool restricted)
        => $$"""
            $engine.SystemDB={{Literal(workgroup)}}
            $engine.DefaultUser='NativeOwner'
            $engine.DefaultPassword='Owner123'
            $restricted=${{restricted}}
            function Assert-Denied([scriptblock]$operation,[int]$expected) {
                $refused=$false
                try { & $operation } catch {
                    $codes=@($engine.Errors | ForEach-Object { $_.Number })
                    if($codes.Count -ne 1 -or $codes[0] -ne $expected) { throw }
                    $refused=$true
                }
                if(!$refused) { throw "Native permission/authentication operation unexpectedly succeeded; expected $expected." }
            }
            Assert-Denied { $wrong=$engine.CreateWorkspace('WrongAccount','NativeOwner','wrong',2);$wrong.Close() } 3029
            Write-Output 'AUTHENTICATION_DENIED=3029'
            function Assert-Contents($db) {
                $rs=$db.OpenRecordset('T',1)
                try {
                    if($rs.EOF -or $rs.Fields('Id').Value -ne 7 -or $rs.Fields('Label').Value -cne 'Native encrypted row') { throw 'Native secured content differs.' }
                    $rs.MoveNext()
                    if(!$rs.EOF) { throw 'Native secured content has extra rows.' }
                } finally { $rs.Close() }
            }
            function Security-Snapshot([string]$path) {
                $workspace=$engine.CreateWorkspace('OwnerCheck','NativeOwner','Owner123',2)
                try {
                    Assert-Denied { $wrong=$workspace.OpenDatabase($path,$false,$false,';PWD=wrong');$wrong.Close() } 3031
                    $db=$workspace.OpenDatabase($path,$false,$false,{{Literal(";PWD=" + password)}})
                    try {
                        Assert-Contents $db
                        $document=$db.Containers('Tables').Documents('T')
                        if($document.Owner -ne 'NativeOwner') { throw 'Native custom owner changed.' }
                        $permissions=@('OWNER='+$document.Owner)
                        $principals=if($restricted){@('NativeOwner','Users','admin','CompactReader')}else{@('NativeOwner','Users','admin')}
                        foreach($principal in $principals) {
                            $document.UserName=$principal
                            $permissions+=($principal+'='+$document.Permissions)
                        }
                        $workspace.BeginTrans()
                        try { $db.Execute("UPDATE T SET Label='authorized-write' WHERE Id=7",128) } finally { $workspace.Rollback() }
                        Assert-Contents $db
                    } finally { $db.Close() }
                } finally { $workspace.Close() }
                if($restricted) {
                    $workspace=$engine.CreateWorkspace('ReaderCheck','CompactReader','',2)
                    try {
                        $db=$workspace.OpenDatabase($path,$false,$false,{{Literal(";PWD=" + password)}})
                        try {
                            Assert-Contents $db
                            Assert-Denied { $db.Execute("UPDATE T SET Label='denied-write' WHERE Id=7",128) } 3108
                            Assert-Contents $db
                        } finally { $db.Close() }
                    } finally { $workspace.Close() }
                }
                return ($permissions -join '|')
            }
            $expected=Security-Snapshot {{Literal(control)}}
            $actual=Security-Snapshot {{Literal(native.SourcePath)}}
            if($actual -cne $expected) { throw "Native assembled security differs: $expected / $actual" }
            $engine.CompactDatabase({{Literal(control)}},{{Literal(Path.Combine(native.WorkDir, "control-compacted.mdb"))}},{{Literal(";LANGID=0x0409;CP=1252;COUNTRY=0;PWD=" + password)}},64,{{Literal(";PWD=" + password)}})
            $engine.CompactDatabase({{Literal(native.SourcePath)}},{{Literal(native.CompactedPath)}},{{Literal(";LANGID=0x0409;CP=1252;COUNTRY=0;PWD=" + password)}},64,{{Literal(";PWD=" + password)}})
            if((Security-Snapshot {{Literal(Path.Combine(native.WorkDir, "control-compacted.mdb"))}}) -cne $expected) { throw 'Native control compact changed security.' }
            if((Security-Snapshot {{Literal(native.CompactedPath)}}) -cne $expected) { throw 'Native assembled compact changed security.' }
            Write-Output $expected
            Write-Output 'DATABASE_PASSWORD_DENIED=3031'
            if($restricted) { Write-Output 'READ_ALLOWED_WRITE_DENIED=3108' }
            Write-Output 'NATIVE_COMPACT_SECURITY_PRESERVED=OK'
            """;

    private static string Literal(string value) => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(value);

    private static async Task<List<CompactPinnedDatabaseBuildTable>> PlanAsync(AccessReader source)
    {
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

        return tables;
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

    private static string PathFor(string directory, CompactPinnedDatabaseFile role, long owner) => Path.Combine(directory, $"{role}-{owner}.bin");

#pragma warning disable CA2000 // Factories transfer ownership to the coordinator, which closes every returned stream.
    private static ValueTask<Stream> OpenNativeAsync(string path)
        => ValueTask.FromResult<Stream>(File.OpenRead(path));

    private static ValueTask<Stream> CreateNativeAsync(string path)
        => ValueTask.FromResult<Stream>(new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None));

    private static ValueTask<Stream> CreateAsync(string directory, CompactPinnedDatabaseFile role, long owner)
        => ValueTask.FromResult<Stream>(new FileStream(PathFor(directory, role, owner), FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None));

    private static ValueTask<Stream> OpenAsync(string directory, CompactPinnedDatabaseFile role, long owner)
        => ValueTask.FromResult<Stream>(new FileStream(PathFor(directory, role, owner), FileMode.Open, FileAccess.Read, FileShare.Read));
#pragma warning restore CA2000

    private async Task AssembleAsync(AccessRoundTripSession native, string control, string password)
    {
        byte[] before = await File.ReadAllBytesAsync(control, Ct);
        string directory = Path.Combine(native.WorkDir, "private");
        Directory.CreateDirectory(directory);
        await using (AccessReader source = await AccessReader.OpenAsync(control, new AccessReaderOptions(password) { UseLockFile = false }, Ct))
        {
            List<CompactPinnedDatabaseBuildTable> tables = await PlanAsync(source);
            var workspace = new CompactPinnedDatabaseWorkspace(
                (role, owner, _) => CreateAsync(directory, role, owner),
                (role, owner, _) => OpenAsync(directory, role, owner));
            CompactPinnedDatabaseBuilderResult result = await CompactPinnedDatabaseBuilder.BuildAndValidateAsync(source.Database.Pages, source.Database.Format, tables, workspace, new(), Ct);
            Assert.Equal(tables.Count, result.Validation.Tables);
            Assert.Equal(source.Database.Pages.PageCount, result.Validation.Prefix.PinnedPages);
            Assert.True(result.Validation.DestinationInventoryCompared);
            Assert.False(result.DatabaseValidated);
            Assert.False(result.PublicationAuthorized);
            Assert.False(result.ReclamationAuthorized);

            await EncodeNativeSubjectAsync(PathFor(directory, CompactPinnedDatabaseFile.Destination, 0), native.SourcePath, before.AsMemory(0, source.PageSize), password);
            await using AccessReader reopened = await AccessReader.OpenAsync(native.SourcePath, new AccessReaderOptions(password) { UseLockFile = false }, Ct);
            foreach (string table in new[] { "MSysObjects", "MSysACEs", "T" })
            {
                using DataTable expected = await source.ReadTableAsync(table, cancellationToken: Ct);
                using DataTable actual = await reopened.ReadTableAsync(table, cancellationToken: Ct);
                Assert.Equal(expected.Columns.Cast<DataColumn>().Select(static column => column.ColumnName), actual.Columns.Cast<DataColumn>().Select(static column => column.ColumnName));
                Assert.Equal(expected.Rows.Count, actual.Rows.Count);
                for (int row = 0; row < expected.Rows.Count; row++)
                {
                    Assert.Equal(expected.Rows[row].ItemArray, actual.Rows[row].ItemArray);
                }
            }

            using DataTable objects = await reopened.ReadTableAsync("MSysObjects", cancellationToken: Ct);
            byte[] owner = Assert.IsType<byte[]>(Assert.Single(objects.Select("Name = 'T'"))["Owner"]);
            Assert.True(owner.Length > 2);
            byte[] encodedHeader = await reopened.Database.Pages.ReadPageCopyAsync(0, Ct);
            Assert.Equal(before.AsMemory(0, source.PageSize).ToArray(), encodedHeader);
            output.WriteLine($"Compared {tables.Count} local tables, exact catalog/security rows and complete pinned-prefix bytes; source SHA256={Convert.ToHexString(SHA256.HashData(before))}; original header/provider retained.");
        }

        Assert.Equal(before, await File.ReadAllBytesAsync(control, Ct));
    }
}
