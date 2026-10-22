namespace JetDatabaseWriter.Tests.Maintenance;

using System;
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
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Catalog, hidden/system tables and related user tables retain their identities during private assembly.</summary>
public sealed class CompactPinnedCatalogCompositionTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ReopenedCatalogAndAllLocalTables_PreserveStoredIdentitiesAndCrossObjectReferences(DatabaseFormat kind)
    {
        byte[] original = await CreateAsync(kind);
        await using var immutable = new MemoryStream(original, writable: false);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(immutable, cancellationToken: Ct);
        JetFormat format = source.Database.Format;
        List<CatalogRow> catalog = await ReadCatalogAsync(source);
        Assert.Contains(catalog, static row => row.Name == "HiddenLocal" && row.Flags != 0);
        Assert.Contains(catalog, static row => row.Name == "External" && row.ObjectType == Constants.SystemObjects.LinkedTableType);
        await using var destination = new MemoryStream();
        await destination.WriteAsync(original, Ct);
        var tables = new List<Table>();
        var buildSession = new CompactPinnedTableAssemblySession(source.Database.Pages, format, new());
        foreach (CatalogRow entry in catalog.Where(static row => row.ObjectType == Constants.SystemObjects.UserTableType).OrderBy(static row => row.TDefPage))
        {
            long owner = entry.TDefPage;
            TDefImage image = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(owner, Ct));
            CompactPinnedTableIndexSource[] indexes = await ObserveAsync(source, owner, image);
            await using var rows = new MemoryStream();
            await using var values = new MemoryStream();
            await using var firstFence = new MemoryStream();
            await using var secondFence = new MemoryStream();
            CompactPinnedTableAssemblyAllocation allocation = await CompactPinnedTableAssembly.BuildAsync(
                buildSession, owner, image, indexes, destination, rows, values, firstFence, secondFence, Ct);
            tables.Add(new(entry.Name, image, allocation, rows.ToArray(), values.ToArray()));
        }

        Assert.Equal(0, buildSession.Discovery.Accounting.ActiveWorkingBytes);
        Table objects = Assert.Single(tables, static table => table.Allocation.Owner == 2);
        Assert.True(objects.Allocation.Table.Data.RowCount >= catalog.Count);
        Assert.True(objects.Image.RealIndexes.Count >= 2);
        Assert.True(objects.Allocation.Table.LongValues.PageCount > 0);
        Assert.Contains(tables, static table => table.Name == Constants.SystemTableNames.Relationships);
        Assert.Contains(tables, static table => table.Name == "HiddenLocal");
        long appendPoint = destination.Length / format.PageSize;
        await CompactGlobalMapInstaller.InstallAsync(source.Database.Pages, format, appendPoint, destination, new(), Ct);
        await using var reopened = new MemoryStream(destination.ToArray(), writable: false);
        CompactGlobalMapResult globalMap = await CompactGlobalMapValidator.ValidateAsync(source.Database.Pages, format, appendPoint, reopened, new(), Ct);
        var expectedDefinitions = new List<CompactPinnedTDefReferences>();
        var validationSession = new CompactPinnedTableAssemblySession(source.Database.Pages, format, new());
        foreach (Table table in tables)
        {
            CompactPinnedTableAssemblyAllocation allocation = table.Allocation;
            CompactPinnedTableIndexSource[] indexes = await ObserveAsync(source, allocation.Owner, table.Image);
            await using var rows = new MemoryStream(table.Rows, writable: false);
            await using var values = new MemoryStream(table.Values, writable: false);
            CompactPinnedTableAssemblyResult verified = await CompactPinnedTableAssembly.ValidateAsync(
                validationSession, allocation.Owner, table.Image, indexes, reopened, rows, values, allocation, Ct);
            expectedDefinitions.Add(new(
                allocation.Owner,
                table.Image,
                allocation.Table.OwnedPages.Pointer,
                allocation.Table.FreePages.Pointer,
                verified.Indexes.Select(static index => index.References).ToArray(),
                allocation.Table.LongValueMaps.Select(static column => new CompactTDefLongValueReferences(column.ColumnNumber, column.OwnedPages.Pointer, column.FreePages.Pointer)).ToArray()));
            Assert.False(verified.DatabaseValidated);
        }

        Assert.Equal(0, validationSession.Discovery.Accounting.ActiveWorkingBytes);
        CompactPinnedPrefixValidatorResult preserved = await CompactPinnedPrefixValidator.ValidateAsync(
            source.Database.Pages, format, expectedDefinitions, reopened, new(), Ct, new(appendPoint, new()));
        Assert.Equal(source.Database.Pages.PageCount, preserved.PinnedPages);
        Assert.False(preserved.OwnershipProven);
        await using var ledger = new MemoryStream();
        CompactGlobalAllocationBuilderResult allocationLedger = await CompactGlobalAllocationBuilder.BuildAsync(
            format, source.Database.Pages.PageCount, AllocationsAsync(tables, appendPoint, globalMap.BitmapPages, Ct), reopened, ledger, new(), Ct);
        await using var reopenedLedger = new MemoryStream(ledger.ToArray(), writable: false);
        CompactGlobalAllocationValidatorResult covered = await CompactGlobalAllocationValidator.ValidateAsync(
            format, source.Database.Pages.PageCount, AllocationsAsync(tables, appendPoint, globalMap.BitmapPages, Ct), reopened, reopenedLedger, allocationLedger, new(), Ct);
        Assert.Equal(globalMap.DestinationPages, covered.DestinationPages);
        Assert.False(covered.DatabaseValidated);
        Assert.False(covered.OwnershipProven);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(reopened, cancellationToken: Ct);
        List<CatalogRow> installedCatalog = await ReadCatalogAsync(reader);
        Assert.Equal(catalog.Select(Identity).Order(), installedCatalog.Select(Identity).Order());
        Assert.All(installedCatalog, row => Assert.InRange(row.PageNumber, objects.Allocation.Table.Data.FirstPageNumber, objects.Allocation.Table.Data.FirstPageNumber + objects.Allocation.Table.Data.PageCount - 1));
        Assert.Equal(await source.ListTablesAsync(Ct), await reader.ListTablesAsync(Ct));
        Assert.DoesNotContain("HiddenLocal", await reader.ListTablesAsync(Ct));
        LinkedTableInfo link = Assert.Single(await reader.ListLinkedTablesAsync(Ct));
        Assert.Equal("External", link.Name);
        Assert.Equal("SourceTable", link.SourceObjectName);
        Assert.Equal("Z:\\never-open-this-source.mdb", link.SourcePath);
        foreach (Table table in tables)
        {
            TDefImage installed = Assert.IsType<TDefImage>(await reader.Database.TableDefs.ReadImageAsync(table.Allocation.Owner, Ct));
            Assert.Equal(table.Image.RealIndexes.Count, installed.RealIndexes.Count);
            await using var rows = new MemoryStream(table.Rows, writable: false);
            CompactRowRelocationMap relocation = await CompactRowRelocationMap.OpenAsync(rows, table.Allocation.Table.Data.RowCount, 10_000_000, Ct);
            for (int slot = 0; slot < installed.RealIndexes.Count; slot++)
            {
                List<IndexEntry> originalEntries = await IndexLeafChain.ReadEntriesAsync(source.Database, table.Allocation.Owner, table.Image.RealIndexes[slot].RootPage, Ct);
                List<IndexEntry> installedEntries = await IndexLeafChain.ReadEntriesAsync(reader.Database, table.Allocation.Owner, installed.RealIndexes[slot].RootPage, Ct);
                Assert.Equal(originalEntries.Count, installedEntries.Count);
                for (int entry = 0; entry < originalEntries.Count; entry++)
                {
                    IndexEntry expected = await relocation.RelocateAsync(originalEntries[entry], Ct);
                    Assert.Equal(expected.Key, installedEntries[entry].Key);
                    Assert.Equal(expected.DataPage, installedEntries[entry].DataPage);
                    Assert.Equal(expected.DataRow, installedEntries[entry].DataRow);
                }
            }
        }

        await AssertUserContentAsync(source, reader);
        await AssertCatalogTamperingRefusedAsync(source, reopened, objects, installedCatalog, expectedDefinitions, appendPoint);

        // Recovery still sees both physically live catalog copies; component proofs cannot declare database validity.
        var physicalInventory = new CompactDiscoverySession(reader.Database.Pages, format, new());
        JetCorruptDataException ambiguity = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await physicalInventory.ScanInventoryAsync(
            static (_, token) =>
            {
                token.ThrowIfCancellationRequested();
                return ValueTask.CompletedTask;
            },
            Ct));
        Assert.Equal(JetErrorCode.CorruptCatalog, ambiguity.ErrorCode);
        Assert.Contains("stored Id", ambiguity.Message, StringComparison.Ordinal);
        Assert.Equal(0, physicalInventory.Accounting.ActiveWorkingBytes);
        Assert.Equal(original, immutable.ToArray());
    }

    private static async Task AssertCatalogTamperingRefusedAsync(AccessReader source, MemoryStream reopened, Table objects, List<CatalogRow> catalog, List<CompactPinnedTDefReferences> definitions, long appendPoint)
    {
        JetFormat format = source.Database.Format;
        CatalogRow target = Assert.Single(catalog, static row => row.Name == "Parents");
        foreach (string column in new[] { "Id", "ParentId", "Name" })
        {
            byte[] bytes = reopened.ToArray();
            int pageStart = checked((int)(target.PageNumber * format.PageSize));
            byte[] page = bytes.AsSpan(pageStart, format.PageSize).ToArray();
            Assert.True(UsageMap.TryGetRowBound(page, format.DataPage, format.PageSize, target.RowIndex, out RowBound bound));
            var location = new RowLocation(target.PageNumber, target.RowIndex, bound.RowStart, bound.RowSize);
            int fieldOffset = -1;
            await CompactRawFieldReader.ScanAsync(
                format,
                objects.Image,
                location,
                page.AsMemory(bound.RowStart, bound.RowSize),
                (field, _) =>
                {
                    if (field.Column.Name == column)
                    {
                        Assert.Equal(CompactRawFieldKind.Stored, field.Kind);
                        Assert.NotEmpty(field.RawBytes.ToArray());
                        fieldOffset = field.RowOffset;
                    }

                    return ValueTask.CompletedTask;
                },
                Ct);
            Assert.True(fieldOffset >= 0);
            bytes[pageStart + bound.RowStart + fieldOffset] ^= 1;
            await using var damaged = new MemoryStream(bytes, writable: false);

            // Appended catalog identities are outside the pinned-prefix proof; raw reopened comparison must reject them.
            await CompactPinnedPrefixValidator.ValidateAsync(source.Database.Pages, format, definitions, damaged, new(), Ct, new(appendPoint, new()));
            CompactPinnedTableIndexSource[] indexes = await ObserveAsync(source, 2, objects.Image);
            await using var rows = new MemoryStream(objects.Rows, writable: false);
            await using var values = new MemoryStream(objects.Values, writable: false);
            var validationSession = new CompactPinnedTableAssemblySession(source.Database.Pages, format, new());
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactPinnedTableAssembly.ValidateAsync(
                validationSession, 2, objects.Image, indexes, damaged, rows, values, objects.Allocation, Ct));
            Assert.Equal(0, validationSession.Discovery.Accounting.ActiveWorkingBytes);
        }
    }

    private static (long Id, long ParentId, string Name, int Type, long Flags, long TDef) Identity(CatalogRow row)
        => (row.Id, row.ParentId, row.Name, row.ObjectType, row.Flags, row.TDefPage);

    private static async Task<List<CatalogRow>> ReadCatalogAsync(AccessReader reader)
    {
        TDefImage image = Assert.IsType<TDefImage>(await reader.Database.TableDefs.ReadImageAsync(2, Ct));
        var decoder = new CatalogRowReader(reader.Database.Format, reader.Database.TableDefs, reader.Database.OwnedPages);
        return await decoder.GetCatalogRowsAsync(image.Definition, Ct);
    }

    private static async Task AssertUserContentAsync(AccessReader source, AccessReader reader)
    {
        foreach (string name in new[] { "Parents", "Children" })
        {
            Assert.Equal(Assert.IsType<CatalogEntry>(await source.GetCatalogEntryAsync(name, Ct)).TDefPage, Assert.IsType<CatalogEntry>(await reader.GetCatalogEntryAsync(name, Ct)).TDefPage);
            Assert.Equal(await source.GetColumnMetadataAsync(name, Ct), await reader.GetColumnMetadataAsync(name, Ct));
            int count = 0;
            await foreach (object[] row in reader.Rows(name, cancellationToken: Ct))
            {
                Assert.Equal(count, Assert.IsType<int>(row[0]));
                Assert.Equal(Payload(name, count), Assert.IsType<string>(row[1]));
                count++;
            }

            Assert.Equal(6, count);
        }

        RelationshipMetadata relationship = Assert.Single(await reader.ListRelationshipsAsync(Ct));
        Assert.Equal("FK_Children_Parents", relationship.Name);
        Assert.Equal("Parents", relationship.PrimaryTable);
        Assert.Equal("Children", relationship.ForeignTable);
        Assert.Equal("Id", Assert.Single(relationship.PrimaryColumns));
        Assert.Equal("ParentId", Assert.Single(relationship.ForeignColumns));
        Assert.True(relationship.EnforcesReferentialIntegrity);
        Assert.True(relationship.CascadeUpdates);
        Assert.True(relationship.CascadeDeletes);
        IndexMetadata parent = Assert.Single(await reader.ListIndexesAsync("Parents", Ct), static index => index.Kind == IndexKind.ForeignKey);
        IndexMetadata child = Assert.Single(await reader.ListIndexesAsync("Children", Ct), static index => index.Kind == IndexKind.ForeignKey);
        Assert.Equal(Assert.IsType<CatalogEntry>(await reader.GetCatalogEntryAsync("Children", Ct)).TDefPage, parent.RelatedTablePage);
        Assert.Equal(Assert.IsType<CatalogEntry>(await reader.GetCatalogEntryAsync("Parents", Ct)).TDefPage, child.RelatedTablePage);
        Assert.Equal(parent.IndexNumber, child.RelatedIndexNumber);
        Assert.Equal(child.IndexNumber, parent.RelatedIndexNumber);
    }

    private static string Payload(string table, int id) => table + new string((char)('a' + id), 3000);

    private static async Task<byte[]> CreateAsync(DatabaseFormat kind)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            foreach (string name in new[] { "Parents", "Children" })
            {
                await writer.CreateTableAsync(name, [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Memo", typeof(string), 20_000) { Description = "Persisted catalog property metadata must survive raw catalog LVAL relocation.", DefaultValueExpression = "\"catalog default\"" }, new("ParentId", typeof(int))], Ct);
                await writer.InsertRowsAsync(name, Enumerable.Range(0, 6).Select(id => new object?[] { id, Payload(name, id), id }), Ct);
            }

            await writer.CreateRelationshipAsync(new("FK_Children_Parents", "Parents", "Id", "Children", "ParentId") { CascadeUpdates = true, CascadeDeletes = true }, Ct);
            await writer.Services.CatalogArtifacts.CreateTableAsync(new("HiddenLocal", [new("Token", typeof(int))], [], Constants.SystemObjects.SystemObjectFlag), Ct);
            await writer.CreateLinkedTableAsync("External", "Z:\\never-open-this-source.mdb", "SourceTable", Ct);
        }

        return stream.ToArray();
    }

    private static async Task<CompactPinnedTableIndexSource[]> ObserveAsync(AccessReader reader, long owner, TDefImage image)
    {
        var sources = new CompactPinnedTableIndexSource[image.RealIndexes.Count];
        for (int slot = 0; slot < sources.Length; slot++)
        {
            TDefIndexDescriptor descriptor = image.RealIndexes[slot];
            Assert.True(UsageMap.TryReadPointer(image.CopyBytes(), descriptor.Slot.FirstDpOffset - 4, out UsageMapPointer map));
            List<IndexEntry> entries = await IndexLeafChain.ReadEntriesAsync(reader.Database, owner, descriptor.RootPage, Ct);
            sources[slot] = new(descriptor.RootPage, map, EntriesAsync(entries, Ct));
        }

        return sources;
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

    private static async IAsyncEnumerable<CompactGlobalAllocationRecord> AllocationsAsync(IEnumerable<Table> tables, long appendPoint, long bitmapPages, [EnumeratorCancellation] CancellationToken token)
    {
        foreach (Table table in tables)
        {
            await foreach (CompactGlobalAllocationRecord allocation in table.Allocation.AllocationsAsync(token))
            {
                yield return allocation;
            }
        }

        token.ThrowIfCancellationRequested();
        if (bitmapPages > 0)
        {
            yield return new(CompactGlobalAllocationKind.UsageMap, 1, new(appendPoint, bitmapPages));
        }
    }

    private sealed record Table(string Name, TDefImage Image, CompactPinnedTableAssemblyAllocation Allocation, byte[] Rows, byte[] Values);
}
