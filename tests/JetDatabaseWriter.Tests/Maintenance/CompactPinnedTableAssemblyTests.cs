namespace JetDatabaseWriter.Tests.Maintenance;

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Private raw tables, stored indexes, pinned definitions and complete allocation evidence compose.</summary>
public sealed class CompactPinnedTableAssemblyTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ReopenedAssembly_PreservesCatalogIdentityRowsAndRelocatedStoredKeys(DatabaseFormat kind)
    {
        (byte[] original, long owner) = await CreateAsync(kind);
        await using var immutable = new MemoryStream(original, writable: false);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(immutable, cancellationToken: Ct);
        JetFormat format = source.Database.Format;
        TDefImage definition = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(owner, Ct));
        TDefIndexDescriptor descriptor = Assert.Single(definition.RealIndexes);
        Assert.True(UsageMap.TryReadPointer(definition.CopyBytes(), descriptor.Slot.FirstDpOffset - 4, out UsageMapPointer originalIndexMap));
        List<IndexEntry> originalEntries = await IndexLeafChain.ReadEntriesAsync(source.Database, owner, descriptor.RootPage, Ct);
        Assert.Equal(60, originalEntries.Count);
        var discovery = new CompactDiscoverySession(source.Database.Pages, format, new());
        await using var destination = new MemoryStream();
        await destination.WriteAsync(original, Ct);
        await using var rowLedger = new MemoryStream();
        await using var valueLedger = new MemoryStream();
        CompactRawTableBuilderResult table = await CompactRawTableBuilder.BuildAsync(discovery, format, owner, definition, destination, rowLedger, valueLedger, [new(descriptor.RootPage, originalIndexMap)], new(), Ct);
        Assert.True(table.LongValues.PageCount > 0);
        CompactRowRelocationMap relocation = await CompactRowRelocationMap.OpenAsync(rowLedger, table.Data.RowCount, 1_000_000, Ct);
        await using var fenceA = new MemoryStream();
        await using var fenceB = new MemoryStream();
        CompactRawIndexAssemblyAllocation index = await CompactRawIndexAssembly.BuildAsync(format, owner, EntriesAsync(originalEntries, Ct), relocation, destination, fenceA, fenceB, new(), Ct);

        // Validate the index on a closed snapshot before accepting its replacement references.
        await using var indexSnapshot = new MemoryStream(destination.ToArray(), writable: false);
        await using var indexRows = new MemoryStream(rowLedger.ToArray(), writable: false);
        CompactRowRelocationMap indexRelocation = await CompactRowRelocationMap.OpenAsync(indexRows, table.Data.RowCount, 1_000_000, Ct);
        CompactRawIndexAssemblyResult verifiedIndex = await CompactRawIndexAssembly.ValidateAsync(format, owner, indexSnapshot, EntriesAsync(originalEntries, Ct), indexRelocation, index, new(), Ct);
        CompactTDefIndexReferences[] indexReferences = [verifiedIndex.References];
        CompactTDefLongValueReferences[] valueReferences = table.LongValueMaps.Select(static column => new CompactTDefLongValueReferences(column.ColumnNumber, column.OwnedPages.Pointer, column.FreePages.Pointer)).ToArray();
        table = table with { PatchedDefinition = CompactTDefReferencePatcher.Patch(format, definition, table.OwnedPages.Pointer, table.FreePages.Pointer, indexReferences, valueReferences, 1_000_000, Ct) };
        CompactTDefChainInstallation[] installations = [new(owner, definition, table.PatchedDefinition)];
        await CompactTDefChainInstaller.InstallAsync(source.Database.Pages, format, installations, destination, new(), Ct);

        await using var reopened = new MemoryStream(destination.ToArray(), writable: false);
        await using var reopenedRows = new MemoryStream(rowLedger.ToArray(), writable: false);
        await using var reopenedValues = new MemoryStream(valueLedger.ToArray(), writable: false);
        CompactRawTableValidatorResult verifiedTable = await CompactRawTableValidator.ValidateAsync(discovery, owner, definition, reopened, reopenedRows, reopenedValues, table, indexReferences, new(), Ct);
        await CompactTDefChainValidator.ValidateAsync(source.Database.Pages, format, installations, reopened, new(), Ct);
        Assert.False(verifiedTable.DatabaseValidated);
        await using var allocationLedger = new MemoryStream();
        CompactGlobalAllocationBuilderResult allocations = await CompactGlobalAllocationBuilder.BuildAsync(format, original.Length / format.PageSize, AllocationsAsync(owner, table, index, Ct), reopened, allocationLedger, new(), Ct);
        await using var reopenedAllocations = new MemoryStream(allocationLedger.ToArray(), writable: false);
        CompactGlobalAllocationValidatorResult global = await CompactGlobalAllocationValidator.ValidateAsync(format, original.Length / format.PageSize, AllocationsAsync(owner, table, index, Ct), reopened, reopenedAllocations, allocations, new(), Ct);
        Assert.Equal(reopened.Length / format.PageSize, global.DestinationPages);
        Assert.False(global.DatabaseValidated);
        Assert.False(global.OwnershipProven);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(reopened, cancellationToken: Ct);
        Assert.Equal(owner, Assert.IsType<CatalogEntry>(await reader.GetCatalogEntryAsync("Items", Ct)).TDefPage);
        List<IndexEntry> installedEntries = await IndexLeafChain.ReadEntriesAsync(reader.Database, owner, verifiedIndex.References.RootPage, Ct);
        Assert.Equal(originalEntries.Count, installedEntries.Count);
        for (int indexNumber = 0; indexNumber < installedEntries.Count; indexNumber++)
        {
            Assert.Equal(originalEntries[indexNumber].Key, installedEntries[indexNumber].Key);
            Assert.InRange(installedEntries[indexNumber].DataPage, table.Data.FirstPageNumber, table.Data.FirstPageNumber + table.Data.PageCount - 1);
        }

        int count = 0;
        await foreach (object[] row in reader.Rows("Items", cancellationToken: Ct))
        {
            Assert.Equal(count, Assert.IsType<int>(row[0]));
            Assert.Equal(Payload(count), Assert.IsType<string>(row[1]));
            count++;
        }

        Assert.Equal(60, count);
        Assert.Equal(original, immutable.ToArray());
        Assert.Equal(0, discovery.Accounting.ActiveWorkingBytes);
    }

    private static string Payload(int id) => new((char)('a' + (id % 26)), 3000);

    private static async Task<(byte[] Bytes, long Owner)> CreateAsync(DatabaseFormat kind)
    {
        await using var stream = new MemoryStream();
        long owner;
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new("Memo", typeof(string), 20_000)], Ct);
            await writer.InsertRowsAsync("Items", Enumerable.Range(0, 60).Select(static id => new object?[] { id, Payload(id) }), Ct);
            owner = Assert.IsType<CatalogEntry>(await writer.Services.Catalog.GetCatalogEntryAsync("Items", Ct)).TDefPage;
        }

        return (stream.ToArray(), owner);
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

    private static async IAsyncEnumerable<CompactGlobalAllocationRecord> AllocationsAsync(long owner, CompactRawTableBuilderResult table, CompactRawIndexAssemblyAllocation index, [EnumeratorCancellation] CancellationToken token)
    {
        await Task.CompletedTask;
        if (table.Data.PageCount > 0)
        {
            yield return new(CompactGlobalAllocationKind.Data, owner, new(table.Data.FirstPageNumber, table.Data.PageCount));
        }

        if (table.LongValues.PageCount > 0)
        {
            yield return new(CompactGlobalAllocationKind.LongValue, owner, new(table.LongValues.FirstPageNumber, table.LongValues.PageCount));
        }

        yield return Map(owner, table.OwnedPages);
        yield return Map(owner, table.FreePages);
        foreach (CompactRawTableLongValueMaps column in table.LongValueMaps)
        {
            token.ThrowIfCancellationRequested();
            yield return Map(owner, column.OwnedPages);
            yield return Map(owner, column.FreePages);
        }

        token.ThrowIfCancellationRequested();
        yield return new(CompactGlobalAllocationKind.Index, owner, new(index.Tree.FirstPageNumber, index.Tree.PageCount));
        yield return Map(owner, index.OwnedPages);
    }

    private static CompactGlobalAllocationRecord Map(long owner, CompactUsageMapBuilderResult map)
        => new(CompactGlobalAllocationKind.UsageMap, owner, new(map.FirstPageNumber, map.PageCount));
}
