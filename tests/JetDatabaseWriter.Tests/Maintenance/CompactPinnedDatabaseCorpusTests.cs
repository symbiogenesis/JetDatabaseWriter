namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Independent native fixtures exercise complete private assembly beyond generated simple tables.</summary>
/// <param name="output">Retains fixture geometry and component comparison evidence.</param>
public sealed class CompactPinnedDatabaseCorpusTests(ITestOutputHelper output)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("FixedText/NativeFixedTextJet3Cp1252.mdb")]
    [InlineData("FixedText/NativeFixedTextJet4.mdb")]
    [InlineData("ComplexFields.accdb")]
    [InlineData("Jackcess/V2010/complexDataTestV2010.accdb")]
    [InlineData("Jackcess/V2010/calcFieldTestV2010.accdb")]
    [InlineData("NorthwindTraders.accdb")]
    public async Task NativeFixtures_CompareCompleteAssemblyAndOrdinaryFeatureReads(string fixture)
    {
        string directory = Path.Combine(Path.GetTempPath(), "JetDatabaseWriter.CompactCorpus." + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Databases", fixture), Ct);
            await using var original = new MemoryStream(bytes, writable: false);
            await using AccessReader source = await TestDatabase.OpenReaderAsync(original, cancellationToken: Ct);
            List<CompactPinnedDatabaseBuildTable> tables = await PlanAsync(source);
            var workspace = new CompactPinnedDatabaseWorkspace(
                (role, owner, _) => CreateAsync(directory, role, owner),
                (role, owner, _) => OpenAsync(directory, role, owner));
            CompactPinnedDatabaseBuilderResult result = await CompactPinnedDatabaseBuilder.BuildAndValidateAsync(
                source.Database.Pages, source.Database.Format, tables, workspace, new(), Ct);
            Assert.Equal(tables.Count, result.Validation.Tables);
            Assert.False(result.DatabaseValidated);
            Assert.False(result.ReclamationAuthorized);
            await using AccessReader reopened = await TestDatabase.OpenReaderAsync(PathFor(directory, CompactPinnedDatabaseFile.Destination, 0), cancellationToken: Ct);
            IReadOnlyList<string> names = await source.ListTablesAsync(Ct);
            Assert.Equal(names, await reopened.ListTablesAsync(Ct));
            int rows = 0;
            foreach (string name in names)
            {
                Assert.Equal(await source.GetColumnMetadataAsync(name, Ct), await reopened.GetColumnMetadataAsync(name, Ct));
                await using IAsyncEnumerator<object[]> expected = source.Rows(name, cancellationToken: Ct).GetAsyncEnumerator(Ct);
                await using IAsyncEnumerator<object[]> actual = reopened.Rows(name, cancellationToken: Ct).GetAsyncEnumerator(Ct);
                while (await expected.MoveNextAsync())
                {
                    Assert.True(await actual.MoveNextAsync());
                    Assert.Equal(expected.Current, actual.Current);
                    rows++;
                }

                Assert.False(await actual.MoveNextAsync());
                await CompareComplexAsync(source, reopened, name);
            }

            Assert.Equal((await source.ListRelationshipsAsync(Ct)).Select(Relationship), (await reopened.ListRelationshipsAsync(Ct)).Select(Relationship));
            Assert.Equal(bytes, original.ToArray());
            output.WriteLine($"{fixture}: {tables.Count} local tables, {rows} ordinary rows, {result.Plan.SourcePages} pinned pages, {result.Validation.Allocations.DestinationPages} complete compared destination pages.");
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Theory]
    [InlineData("FixedText/NativeFixedTextJet3Cp1252.mdb")]
    [InlineData("FixedText/NativeFixedTextJet4.mdb")]
    [InlineData("FixedText/NativeFixedTextAce.accdb")]
    public async Task NativeMalformedRowDirectory_RefusesWithoutChangingTheDamagedSource(string fixture)
    {
        string directory = Path.Combine(Path.GetTempPath(), "JetDatabaseWriter.CompactDamage." + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Databases", fixture), Ct);
            List<CompactPinnedDatabaseBuildTable> tables;
            RowLocation? first = null;
            int directoryOffset;
            await using (var healthy = new MemoryStream(bytes, writable: false))
            await using (AccessReader reader = await TestDatabase.OpenReaderAsync(healthy, cancellationToken: Ct))
            {
                tables = await PlanAsync(reader);
                CatalogEntry entry = Assert.IsType<CatalogEntry>(await reader.GetCatalogEntryAsync("FixedText", Ct));
                await CompactRawRowReader.ScanAsync(
                    reader.Database.Pages,
                    reader.Database.Format,
                    entry.TDefPage,
                    new(),
                    (location, _, _) =>
                    {
                        first ??= location;
                        return ValueTask.CompletedTask;
                    },
                    Ct);
                Assert.NotNull(first);
                directoryOffset = checked((int)(first.Value.DataPageNumber * reader.PageSize) + reader.Database.Format.DataPage.RowsStart + (first.Value.RowIndex * 2));
            }

            ushort before = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(directoryOffset));

            // An independently specified offset of one points before every valid native row directory.
            BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(directoryOffset), 1);
            byte[] damaged = bytes.ToArray();
            await using var original = new MemoryStream(bytes, writable: false);
            await using AccessReader source = await TestDatabase.OpenReaderAsync(original, cancellationToken: Ct);
            var workspace = new CompactPinnedDatabaseWorkspace(
                (role, owner, _) => CreateAsync(directory, role, owner),
                (role, owner, _) => OpenAsync(directory, role, owner));
            JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactPinnedDatabaseBuilder.BuildAndValidateAsync(
                source.Database.Pages, source.Database.Format, tables, workspace, new(), Ct));
            Assert.Equal(damaged, original.ToArray());
            output.WriteLine($"{fixture}: independent directory damage page={first!.Value.DataPageNumber}, row={first.Value.RowIndex}, byte={directoryOffset}, word=0x{before:X4}->0x0001; complete assembly refused: {error.ErrorCode} {error.Message}");
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Theory]
    [InlineData("NativeJet3Rc4.mdb", "Pássword")]
    [InlineData("NativeJet4Rc4.mdb", "Native123")]
    [InlineData("NativeAceAgile.accdb", "Native123")]
    [InlineData("NativeAceStandard.accdb", "Password1234_")]
    public async Task NativeEncryptedSources_CompareOnlyDecryptedPrivateStagingWithoutPublication(string fixture, string password)
    {
        string directory = Path.Combine(Path.GetTempPath(), "JetDatabaseWriter.CompactCodecBoundary." + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, fixture), Ct);
            await using var original = new MemoryStream(bytes, writable: false);
            await using AccessReader source = await AccessReader.OpenAsync(original, new AccessReaderOptions(password) { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
            List<CompactPinnedDatabaseBuildTable> tables = await PlanAsync(source);
            var workspace = new CompactPinnedDatabaseWorkspace(
                (role, owner, _) => CreateAsync(directory, role, owner),
                (role, owner, _) => OpenAsync(directory, role, owner));
            CompactPinnedDatabaseBuilderResult result = await CompactPinnedDatabaseBuilder.BuildAndValidateAsync(
                source.Database.Pages, source.Database.Format, tables, workspace, new(), Ct);
            Assert.Equal(tables.Count, result.Validation.Tables);
            Assert.False(result.DatabaseValidated);
            Assert.False(result.PublicationAuthorized);
            Assert.False(result.ReclamationAuthorized);
            Assert.Equal(bytes, original.ToArray());

            // The internal page source is decrypted; retaining native codec header metadata
            // does not turn the private image into a codec-encoded database output.
            byte[] expectedHeader = await source.Database.Pages.ReadPageCopyAsync(0, Ct);
            byte[] actualHeader = new byte[source.PageSize];
            await using FileStream privateImage = File.OpenRead(PathFor(directory, CompactPinnedDatabaseFile.Destination, 0));
            await privateImage.ReadExactlyAsync(actualHeader, Ct);
            Assert.Equal(expectedHeader, actualHeader);
            output.WriteLine($"{fixture}: {tables.Count} local tables compared as decrypted private pages; original encrypted bytes retained, codec encoding and native/publication acceptance remain separate.");
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task NativeCalculatedByteErrors_RetainRawEnvelopesAndNativeNullDistinction()
    {
        await using AccessRoundTripSession native = await AccessRoundTripSession.CreateDaoAccdbAsync(Ct, "JetDatabaseWriter.Tests.CompactByteEnvelope");
        const string authoring = """
            $db.Execute('CREATE TABLE [T] ([Id] LONG, [Flag] BIT, [Amount] DOUBLE)',128)
            $db.TableDefs.Refresh()
            $tdf=$db.TableDefs('T')
            foreach($name in @('ByteFlag','ByteAmount')) {
                $field=$tdf.CreateField($name,2)
                $field.Expression=if($name -eq 'ByteFlag'){'[Flag]'}else{'[Amount]'}
                $tdf.Fields.Append($field)
            }
            $db.Execute('INSERT INTO [T] ([Id],[Flag],[Amount]) VALUES (1,True,-1)',128)
            $db.Execute('INSERT INTO [T] ([Id],[Flag],[Amount]) VALUES (2,False,Null)',128)
            """;
        await File.WriteAllTextAsync(Path.Combine(native.WorkDir, "authoring.ps1"), authoring, Ct);
        AccessRoundTripEnvironment.CompactResult authored = native.RunDaoDatabaseScript(native.SourcePath, authoring, TimeSpan.FromMinutes(1));
        Assert.True(authored.ExitCode == 0, $"Native calculated error authoring failed: {authored.StdOut}\n{authored.StdErr}");
        byte[] original = await File.ReadAllBytesAsync(native.SourcePath, Ct);
        string directory = Path.Combine(native.WorkDir, "private");
        Directory.CreateDirectory(directory);
        string staged = native.CreateDatabasePath("staged");
        await using (AccessReader source = await TestDatabase.OpenReaderAsync(native.SourcePath, cancellationToken: Ct))
        {
            List<CompactPinnedDatabaseBuildTable> tables = await PlanAsync(source);
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await source.GetCatalogEntryAsync("T", Ct));
            List<byte[]> expected = await ByteEnvelopesAsync(source, entry.TDefPage);
            Assert.Collection(expected, error => Assert.Equal(39, error.Length), nullValue => Assert.Equal(23, nullValue.Length));
            var workspace = new CompactPinnedDatabaseWorkspace(
                (role, owner, _) => CreateAsync(directory, role, owner),
                (role, owner, _) => OpenAsync(directory, role, owner));
            CompactPinnedDatabaseBuilderResult result = await CompactPinnedDatabaseBuilder.BuildAndValidateAsync(source.Database.Pages, source.Database.Format, tables, workspace, new(), Ct);
            Assert.False(result.DatabaseValidated);
            await using (AccessReader reopened = await TestDatabase.OpenReaderAsync(PathFor(directory, CompactPinnedDatabaseFile.Destination, 0), cancellationToken: Ct))
            {
                List<byte[]> actual = await ByteEnvelopesAsync(reopened, entry.TDefPage);
                Assert.Equal(expected.Count, actual.Count);
                for (int index = 0; index < expected.Count; index++)
                {
                    Assert.Equal(expected[index], actual[index]);
                    output.WriteLine($"ByteAmount raw envelope row {index}: {Convert.ToHexString(expected[index])}");
                }
            }

            File.Copy(PathFor(directory, CompactPinnedDatabaseFile.Destination, 0), staged);
        }

        string sourcePath = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.SourcePath);
        string stagedPath = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(staged);
        string compactedPath = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.CompactedPath);
        string check = $$"""
            function Check-ErrorAndNull([string]$path) {
                $db=$engine.OpenDatabase($path)
                try {
                    $rs=$db.OpenRecordset('SELECT [Id],[ByteFlag],[ByteAmount],IsNull([ByteAmount]) AS [NullTest] FROM [T] ORDER BY [Id]')
                    try {
                        if($rs.Fields('Id').Value -ne 1 -or $null -ne $rs.Fields('ByteFlag').Value -or $null -ne $rs.Fields('ByteAmount').Value -or $null -ne $rs.Fields('NullTest').Value) { throw 'Overflow no longer exposes its missing COM result.' }
                        $rs.MoveNext()
                        if($rs.Fields('Id').Value -ne 2 -or $rs.Fields('ByteFlag').Value -ne 0 -or $rs.Fields('ByteAmount').Value -isnot [DBNull] -or $rs.Fields('NullTest').Value -ne -1) { throw 'Ordinary Null no longer differs from overflow.' }
                        $rs.MoveNext()
                        if(!$rs.EOF) { throw 'Unexpected native calculated rows.' }
                    } finally { $rs.Close() }
                } finally { $db.Close() }
            }
            Check-ErrorAndNull {{sourcePath}}
            Check-ErrorAndNull {{stagedPath}}
            $engine.CompactDatabase({{stagedPath}},{{compactedPath}})
            Check-ErrorAndNull {{compactedPath}}
            Write-Output 'RAW_CALCULATED_ERROR_AND_NULL_PRESERVED=OK'
            """;
        await File.WriteAllTextAsync(Path.Combine(native.WorkDir, "verification.ps1"), check, Ct);
        AccessRoundTripEnvironment.CompactResult compared = native.RunDaoEngineScript(check, TimeSpan.FromMinutes(2));
        Assert.True(compared.ExitCode == 0, $"Native raw calculated error comparison failed: {compared.StdOut}\n{compared.StdErr}");
        Assert.Contains("RAW_CALCULATED_ERROR_AND_NULL_PRESERVED=OK", compared.StdOut, StringComparison.Ordinal);
        Assert.Equal(original, await File.ReadAllBytesAsync(native.SourcePath, Ct));
        output.WriteLine(compared.StdOut);
    }

    private static async Task<List<byte[]>> ByteEnvelopesAsync(AccessReader reader, long owner)
    {
        TDefImage image = Assert.IsType<TDefImage>(await reader.Database.TableDefs.ReadImageAsync(owner, Ct));
        var result = new List<byte[]>();
        foreach (long pageNumber in await reader.Database.OwnedPages.GetOwnedDataPagesAsync(owner, Ct))
        {
            byte[] page = await reader.Database.Pages.ReadPageCopyAsync(pageNumber, Ct);
            int rows = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(reader.Database.Format.DataPage.NumRows));
            for (int row = 0; row < rows; row++)
            {
                Assert.True(UsageMap.TryGetRowBound(page, reader.Database.Format.DataPage, reader.PageSize, row, out RowBound bound));
                await CompactRawFieldReader.ScanAsync(
                    reader.Database.Format,
                    image,
                    new(pageNumber, row, bound.RowStart, bound.RowSize),
                    page.AsMemory(bound.RowStart, bound.RowSize),
                    (field, _) =>
                    {
                        if (field.Column.Name == "ByteAmount")
                        {
                            Assert.Equal(CompactRawFieldKind.Stored, field.Kind);
                            result.Add(field.RawBytes.ToArray());
                        }

                        return ValueTask.CompletedTask;
                    },
                    Ct);
            }
        }

        return result;
    }

    [Fact]
    public async Task LegacyUmsaetze_CorroboratedActiveRowsPreserveAndReportHistoricalRows()
    {
        string directory = Path.Combine(Path.GetTempPath(), "JetDatabaseWriter.CompactLegacyRows." + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Databases", "mdbtools", "nwind.mdb"), Ct);
            await using var original = new MemoryStream(bytes, writable: false);
            await using AccessReader source = await TestDatabase.OpenReaderAsync(original, cancellationToken: Ct);
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await source.GetCatalogEntryAsync("Umsätze", Ct));
            Assert.Equal(753, entry.TDefPage);
            List<CompactPinnedDatabaseBuildTable> tables = await PlanAsync(source);
            int slot = tables.FindIndex(table => table.Owner == entry.TDefPage);
            Assert.Equal(830u, tables[slot].Definition.Header.Counters.RowCount);
            int ordinal = tables[slot].Definition.LogicalIndexes.Single(static index => index.Name == "PrimaryKey").Entry.IndexNum2;
            tables[slot] = tables[slot] with { ActiveIndexOrdinal = ordinal };
            var reported = new HashSet<RowLocation>();
            var workspace = new CompactPinnedDatabaseWorkspace(
                (role, owner, _) => CreateAsync(directory, role, owner),
                (role, owner, _) => OpenAsync(directory, role, owner),
                (record, token) =>
                {
                    token.ThrowIfCancellationRequested();
                    if (record.Kind == "unassigned-row")
                    {
                        Assert.True(record.IsPinned);
                        Assert.Equal(entry.TDefPage, record.OwnerTDefPage);
                        Assert.False(record.RawBytes.IsEmpty);
                        reported.Add(Assert.IsType<RowLocation>(record.Location));
                    }

                    return ValueTask.CompletedTask;
                });
            CompactPinnedDatabaseBuilderResult result = await CompactPinnedDatabaseBuilder.BuildAndValidateAsync(
                source.Database.Pages, source.Database.Format, tables, workspace, new(), Ct);
            Assert.Equal(1, result.Validation.ActiveTables);
            Assert.Equal(2049, result.Validation.UnassignedRows);
            Assert.Equal(2049, reported.Count);
            Assert.False(result.DatabaseValidated);
            Assert.False(result.ReclamationAuthorized);
            Assert.Equal(bytes, original.ToArray());
            await using AccessReader reopened = await TestDatabase.OpenReaderAsync(PathFor(directory, CompactPinnedDatabaseFile.Destination, 0), cancellationToken: Ct);
            await using IAsyncEnumerator<object[]> expected = source.Rows(entry.Name, cancellationToken: Ct).GetAsyncEnumerator(Ct);
            await using IAsyncEnumerator<object[]> actual = reopened.Rows(entry.Name, cancellationToken: Ct).GetAsyncEnumerator(Ct);
            int count = 0;
            while (await expected.MoveNextAsync())
            {
                Assert.True(await actual.MoveNextAsync());
                Assert.Equal(expected.Current, actual.Current);
                count++;
            }

            Assert.False(await actual.MoveNextAsync());
            Assert.Equal(830, count);
            output.WriteLine("Legacy Umsätze:830 active rows corroborated by exact native map/index/definition evidence;2049 unassigned physical rows reported and retained in the independently compared pinned prefix.");
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static async Task CompareComplexAsync(AccessReader source, AccessReader reopened, string table)
    {
        IReadOnlyList<ComplexColumnInfo> columns = await source.GetComplexColumnsAsync(table, Ct);
        Assert.Equal(columns, await reopened.GetComplexColumnsAsync(table, Ct));
        foreach (ComplexColumnInfo column in columns)
        {
            if (column.Kind == ComplexColumnKind.Attachment)
            {
                IReadOnlyList<AttachmentRecord> expected = await source.GetAttachmentsAsync(table, column.ColumnName, Ct);
                IReadOnlyList<AttachmentRecord> actual = await reopened.GetAttachmentsAsync(table, column.ColumnName, Ct);
                Assert.Equal(expected.Count, actual.Count);
                for (int index = 0; index < expected.Count; index++)
                {
                    Assert.Equal(expected[index].Id, actual[index].Id);
                    Assert.Equal(expected[index].ConceptualTableId, actual[index].ConceptualTableId);
                    Assert.Equal(expected[index].FileName, actual[index].FileName);
                    Assert.Equal(expected[index].FileData, actual[index].FileData);
                }
            }
            else
            {
                Assert.Equal(await source.GetMultiValueItemsAsync(table, column.ColumnName, Ct), await reopened.GetMultiValueItemsAsync(table, column.ColumnName, Ct));
            }
        }
    }

    private static string Relationship(RelationshipMetadata relationship)
        => $"{relationship.Name}|{relationship.PrimaryTable}|{string.Join(',', relationship.PrimaryColumns)}|{relationship.ForeignTable}|{string.Join(',', relationship.ForeignColumns)}|{relationship.EnforcesReferentialIntegrity}|{relationship.CascadeUpdates}|{relationship.CascadeDeletes}";

    private static async Task<List<CompactPinnedDatabaseBuildTable>> PlanAsync(AccessReader source)
    {
        TDefImage catalog = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(2, Ct));
        var reader = new CatalogRowReader(source.Database.Format, source.Database.TableDefs, source.Database.OwnedPages);
        List<CatalogRow> entries = await reader.GetCatalogRowsAsync(catalog.Definition, Ct);
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

            IReadOnlyList<ColumnInfo>? columns = null;
            if (image.Columns.Any(static column => column.IsCalculated))
            {
                IReadOnlyList<ColumnMetadata> metadata = await source.GetColumnMetadataAsync(entry.Name, Ct);
                columns = image.Columns.Select(column => column.IsCalculated
                    ? column.WithCalculatedResultType((ColumnType)Assert.Single(metadata, item => item.Name == column.Name).CalculatedResultType)
                    : column).ToArray();
            }

            tables.Add(new(entry.TDefPage, image, indexes, columns));
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

    private static string PathFor(string directory, CompactPinnedDatabaseFile role, long owner)
        => Path.Combine(directory, $"{role}-{owner}.bin");

#pragma warning disable CA2000 // Factory ownership transfers to the coordinator, which closes every returned stream.
    private static ValueTask<Stream> CreateAsync(string directory, CompactPinnedDatabaseFile role, long owner)
        => ValueTask.FromResult<Stream>(new FileStream(PathFor(directory, role, owner), FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None));
#pragma warning restore CA2000

#pragma warning disable CA2000 // Factory ownership transfers to the coordinator, which closes every returned stream.
    private static ValueTask<Stream> OpenAsync(string directory, CompactPinnedDatabaseFile role, long owner)
        => ValueTask.FromResult<Stream>(new FileStream(PathFor(directory, role, owner), FileMode.Open, FileAccess.Read, FileShare.Read));
#pragma warning restore CA2000
}
