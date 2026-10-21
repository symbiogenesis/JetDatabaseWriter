namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Private data, native maps and raw definition patches compose while original storage remains pinned.</summary>
public sealed class CompactStagedTableCompositionTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ReopenedTable_UsesOnlyStagedDataAndRetainsEveryOtherSourceByte(DatabaseFormat kind)
    {
        (byte[] original, long owner) = await CreateAsync(kind);
        await using var immutableSource = new MemoryStream(original, writable: false);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(immutableSource, cancellationToken: Ct);
        JetFormat format = source.Database.Format;
        TDefImage image = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(owner, Ct));
        LogicalTDefChain chain = await source.Database.TableDefs.ReadTDefChainAsync(owner, Ct);
        Assert.Single(chain.PageNumbers);
        Assert.Empty(image.RealIndexes);
        Assert.Equal(format.PageSize, image.LogicalByteLength);
        byte[] originalDefinition = image.CopyBytes();
        Assert.Equal(0xD7, originalDefinition[^1]);
        IReadOnlyList<long> originalPages = await source.Database.OwnedPages.GetOwnedDataPagesAsync(owner, Ct);
        Assert.NotEmpty(originalPages);
        var discovery = new CompactDiscoverySession(source.Database.Pages, format, new());
        await using var destination = new MemoryStream();
        await destination.WriteAsync(original, Ct);
        await using var ledger = new MemoryStream();
        CompactRawDataBuilderResult data = await CompactRawDataBuilder.BuildAsync(discovery, format, owner, owner, destination, ledger, new(), Ct);
        await CompactRawDataValidator.ValidateAsync(discovery, owner, owner, destination, ledger, data, 16_000_000, Ct);
        CompactUsageMapBuilderResult owned = await CompactUsageMapBuilder.BuildAsync(format, RunsAsync([new(data.FirstPageNumber, data.PageCount)], Ct), destination, new(), Ct);
        CompactUsageMapBuilderResult free = await CompactUsageMapBuilder.BuildAsync(format, RunsAsync([], Ct), destination, new(), Ct);
        byte[] patched = CompactTDefReferencePatcher.Patch(format, image, owned.Pointer, free.Pointer, [], [], 1_000_000, Ct);
        byte[] expectedDefinition = originalDefinition.AsSpan().ToArray();
        UsageMap.WritePointer(expectedDefinition, format.TDef.UsedPages, owned.Pointer.RowIndex, owned.Pointer.PageNumber);
        UsageMap.WritePointer(expectedDefinition, format.TDef.FreePages, free.Pointer.RowIndex, free.Pointer.PageNumber);
        Assert.Equal(expectedDefinition, patched);
        Assert.Equal(originalDefinition, image.CopyBytes());
        destination.Position = owner * format.PageSize;
        await destination.WriteAsync(patched, Ct);
        await destination.FlushAsync(Ct);
        byte[] staged = destination.ToArray();
        byte[] expectedPrefix = original.AsSpan().ToArray();
        patched.CopyTo(expectedPrefix.AsSpan(checked((int)(owner * format.PageSize)), format.PageSize));
        Assert.Equal(expectedPrefix, staged.AsSpan(0, original.Length).ToArray());
        Assert.Equal(original, immutableSource.ToArray());

        await using var reopenedStream = new MemoryStream(staged, writable: false);
        await using AccessReader reopened = await TestDatabase.OpenReaderAsync(reopenedStream, cancellationToken: Ct);
        CatalogEntry catalog = Assert.IsType<CatalogEntry>(await reopened.GetCatalogEntryAsync("Items", Ct));
        Assert.Equal(owner, catalog.TDefPage);
        IReadOnlyList<long> selectedPages = await reopened.Database.OwnedPages.GetOwnedDataPagesAsync(owner, Ct);
        Assert.Equal(Enumerable.Range(0, checked((int)data.PageCount)).Select(index => data.FirstPageNumber + index), selectedPages);
        Assert.DoesNotContain(selectedPages, originalPages.Contains);
        int rows = 0;
        await foreach (object[] row in reopened.Rows("Items", cancellationToken: Ct))
        {
            Assert.Equal(rows, Assert.IsType<int>(row[0]));
            Assert.Equal(Payload(rows), Assert.IsType<string>(row[1]));
            rows++;
        }

        Assert.Equal(100, rows);
        Assert.Equal(rows, data.RowCount);
        Assert.False(data.IsValidated);
        Assert.False(owned.IsValidated);
        Assert.False(free.IsValidated);
        Assert.Equal(0, discovery.Accounting.ActiveWorkingBytes);
    }

    private static string Payload(int id) => new((char)('a' + (id % 26)), 64);

    private static async Task<(byte[] Bytes, long Owner)> CreateAsync(DatabaseFormat kind)
    {
        await using var stream = new MemoryStream();
        long owner;
        int pageSize;
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Value", typeof(string), 128)], Ct);
            await writer.InsertRowsAsync("Items", Enumerable.Range(0, 100).Select(static id => new object?[] { id, Payload(id) }), Ct);
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await writer.Services.Catalog.GetCatalogEntryAsync("Items", Ct));
            owner = entry.TDefPage;
            pageSize = writer.Database.Format.PageSize;
        }

        byte[] bytes = stream.ToArray();
        bytes[checked((int)(((owner + 1) * pageSize) - 1))] = 0xD7;
        return (bytes, owner);
    }

    private static async IAsyncEnumerable<CompactPageRun> RunsAsync(IEnumerable<CompactPageRun> runs, [EnumeratorCancellation] CancellationToken token)
    {
        await Task.CompletedTask;
        foreach (CompactPageRun run in runs)
        {
            token.ThrowIfCancellationRequested();
            yield return run;
        }
    }
}
