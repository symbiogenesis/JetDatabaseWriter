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
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Independent table, global-map, allocation and opaque-prefix proofs compose across multiple tables.</summary>
public sealed class CompactPinnedDatabaseCompositionTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ReopenedMultipleTables_GlobalMapsAndPinnedBytesCompareIndependently(DatabaseFormat kind)
    {
        byte[] original = await CreateAsync(kind);
        await using var immutable = new MemoryStream(original, writable: false);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(immutable, cancellationToken: Ct);
        JetFormat format = source.Database.Format;
        await using var destination = new MemoryStream();
        await destination.WriteAsync(original, Ct);
        var tables = new List<Table>();
        await using var firstFence = new MemoryStream();
        await using var secondFence = new MemoryStream();
        foreach (string name in new[] { "Indexed", "Plain" })
        {
            long owner = Assert.IsType<CatalogEntry>(await source.GetCatalogEntryAsync(name, Ct)).TDefPage;
            TDefImage image = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(owner, Ct));
            CompactPinnedTableIndexSource[] indexes = await ObserveAsync(source, owner, image);
            await using var rows = new MemoryStream();
            await using var values = new MemoryStream();
            firstFence.SetLength(0);
            secondFence.SetLength(0);
            CompactPinnedTableAssemblyAllocation allocation = await CompactPinnedTableAssembly.BuildAsync(
                source.Database.Pages, format, owner, image, indexes, destination, rows, values, firstFence, secondFence, new(), Ct);
            tables.Add(new(name, image, allocation, rows.ToArray(), values.ToArray()));
        }

        long appendPoint = destination.Length / format.PageSize;
        await CompactGlobalMapInstaller.InstallAsync(source.Database.Pages, format, appendPoint, destination, new(), Ct);
        await using var reopened = new MemoryStream(destination.ToArray(), writable: false);
        CompactGlobalMapResult globalMap = await CompactGlobalMapValidator.ValidateAsync(source.Database.Pages, format, appendPoint, reopened, new(), Ct);
        var expectedDefinitions = new List<CompactPinnedTDefReferences>();
        foreach (Table table in tables)
        {
            CompactPinnedTableAssemblyAllocation allocation = table.Allocation;
            CompactPinnedTableIndexSource[] indexes = await ObserveAsync(source, allocation.Owner, table.Image);
            await using var rows = new MemoryStream(table.Rows, writable: false);
            await using var values = new MemoryStream(table.Values, writable: false);
            CompactPinnedTableAssemblyResult verified = await CompactPinnedTableAssembly.ValidateAsync(
                source.Database.Pages, format, allocation.Owner, table.Image, indexes, reopened, rows, values, allocation, new(), Ct);
            expectedDefinitions.Add(new(
                allocation.Owner,
                table.Image,
                allocation.Table.OwnedPages.Pointer,
                allocation.Table.FreePages.Pointer,
                verified.Indexes.Select(static index => index.References).ToArray(),
                allocation.Table.LongValueMaps.Select(static column => new CompactTDefLongValueReferences(column.ColumnNumber, column.OwnedPages.Pointer, column.FreePages.Pointer)).ToArray()));
            Assert.False(verified.DatabaseValidated);
        }

        CompactPinnedPrefixValidatorResult preserved = await CompactPinnedPrefixValidator.ValidateAsync(
            source.Database.Pages, format, expectedDefinitions, reopened, new(), Ct, new(appendPoint, new()));
        Assert.Equal(original.Length / format.PageSize, preserved.PinnedPages);
        Assert.False(preserved.OwnershipProven);
        await using var ledger = new MemoryStream();
        CompactGlobalAllocationBuilderResult allocationLedger = await CompactGlobalAllocationBuilder.BuildAsync(
            format, source.Database.Pages.PageCount, AllocationsAsync(tables, appendPoint, globalMap.BitmapPages, Ct), reopened, ledger, new(), Ct);
        await using var reopenedLedger = new MemoryStream(ledger.ToArray(), writable: false);
        CompactGlobalAllocationValidatorResult covered = await CompactGlobalAllocationValidator.ValidateAsync(
            format, source.Database.Pages.PageCount, AllocationsAsync(tables, appendPoint, globalMap.BitmapPages, Ct), reopened, reopenedLedger, allocationLedger, new(), Ct);
        Assert.Equal(globalMap.DestinationPages, covered.DestinationPages);
        Assert.False(covered.DatabaseValidated);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(reopened, cancellationToken: Ct);
        foreach (Table table in tables)
        {
            int count = 0;
            await foreach (object[] row in reader.Rows(table.Name, cancellationToken: Ct))
            {
                Assert.Equal(count, Assert.IsType<int>(row[0]));
                Assert.Equal(Payload(table.Name, count), Assert.IsType<string>(row[1]));
                count++;
            }

            Assert.Equal(6, count);
        }

        Assert.Equal(original, immutable.ToArray());
    }

    private static string Payload(string table, int id) => table + new string((char)('a' + id), 3000);

    private static async Task<byte[]> CreateAsync(DatabaseFormat kind)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            foreach (string name in new[] { "Indexed", "Plain" })
            {
                await writer.CreateTableAsync(name, [new("Id", typeof(int)) { IsPrimaryKey = name == "Indexed" }, new("Memo", typeof(string), 20_000)], Ct);
                await writer.InsertRowsAsync(name, Enumerable.Range(0, 6).Select(id => new object?[] { id, Payload(name, id) }), Ct);
            }
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
