namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Complete discovered-local-table plans are independently bound to the immutable source.</summary>
public sealed class CompactDatabasePlanTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompletePlan_IncludesCatalogAndEveryDiscoveredLocalDefinition(DatabaseFormat kind)
    {
        byte[] bytes = await CreateAsync(kind);
        await using DatabaseFile file = Open(bytes);
        List<CompactDatabasePlanTable> tables = await DiscoverAsync(file);
        var accounting = new CompactDiscoveryAccounting(new());
        CompactDatabasePlanResult result = await CompactDatabasePlanValidator.ValidateAsync(file.Pages, file.Format, tables, new(), Ct, accounting);
        Assert.True(tables.Count > 2);
        Assert.Contains(tables, table => table.OwnerTDefPage == 2);
        var identities = new List<CompactStorageRecord>();
        await CompactStorageInventory.ScanAsync(
            file.Pages,
            file.Format,
            file.TableDefs,
            new(),
            (record, _) =>
            {
                identities.Add(record);
                return ValueTask.CompletedTask;
            },
            Ct);
        CompactStorageRecord hidden = Assert.Single(identities, static record => record.Kind == "catalog-object" && record.Name == "HiddenLocal");
        Assert.Contains(tables, table => table.OwnerTDefPage == hidden.OwnerTDefPage);
        CompactStorageRecord link = Assert.Single(identities, static record => record.Kind == "catalog-object" && record.Name == "External");
        Assert.Equal("opaque-catalog-object", link.Role);
        Assert.Null(link.OwnerTDefPage);
        Assert.Equal(tables.Count, result.LocalTables);
        Assert.Equal(file.Pages.PageCount, result.SourcePages);
        Assert.False(result.DiscoveryComplete);
        Assert.False(result.OwnershipProven);
        Assert.False(result.DatabaseValidated);
        Assert.True(accounting.PageReads > 0);
        Assert.Equal(0, accounting.ActiveWorkingBytes);
        Assert.Equal(bytes, await ReadAllAsync(file));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task MissingDuplicateAndExtraOwners_AreRefused(DatabaseFormat kind)
    {
        byte[] bytes = await CreateAsync(kind);
        await using DatabaseFile file = Open(bytes);
        List<CompactDatabasePlanTable> tables = await DiscoverAsync(file);
        foreach (long omitted in tables.Select(static table => table.OwnerTDefPage))
        {
            await RefuseAsync(tables.Where(table => table.OwnerTDefPage != omitted).ToArray());
        }

        await RefuseAsync([.. tables, tables[0]]);
        await RefuseAsync([.. tables, new(file.Pages.PageCount + 10, tables[0].Definition)]);
        long extraOwner = Enumerable.Range(3, checked((int)file.Pages.PageCount - 3)).First(page => tables.All(table => table.OwnerTDefPage != page));
        await RefuseAsync([.. tables, new(extraOwner, tables[0].Definition)]);
        Assert.Equal(bytes, await ReadAllAsync(file));

        async Task RefuseAsync(IReadOnlyList<CompactDatabasePlanTable> proposed)
        {
            var accounting = new CompactDiscoveryAccounting(new());
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactDatabasePlanValidator.ValidateAsync(file.Pages, file.Format, proposed, new(), Ct, accounting));
            Assert.Equal(0, accounting.ActiveWorkingBytes);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task SameOwnerWithDifferentOriginalImage_IsRefused(DatabaseFormat kind)
    {
        byte[] bytes = await CreateAsync(kind);
        await using DatabaseFile file = Open(bytes);
        List<CompactDatabasePlanTable> tables = await DiscoverAsync(file);
        CompactDatabasePlanTable original = tables[0];
        TDefImage image = original.Definition;
        byte[] changed = image.CopyBytes();
        changed[^1] ^= 1;
        var wrong = new TDefImage(changed, image.Header, [.. image.Columns], image.HasDeletedColumns, image.Issues, image.IndexSection, [.. image.RealIndexes], [.. image.LogicalIndexes])
        {
            TDefPageNumber = original.OwnerTDefPage,
        };
        tables[0] = new(original.OwnerTDefPage, wrong);
        var accounting = new CompactDiscoveryAccounting(new());
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactDatabasePlanValidator.ValidateAsync(file.Pages, file.Format, tables, new(), Ct, accounting));
        Assert.Equal(original.OwnerTDefPage, error.ErrorInfo.PageNumber);
        Assert.Equal(0, accounting.ActiveWorkingBytes);
        Assert.Equal(bytes, await ReadAllAsync(file));
    }

    [Theory]
    [InlineData("header")]
    [InlineData("column")]
    [InlineData("index")]
    [InlineData("logical-index")]
    public async Task IdenticalBytesWithFabricatedParsedLayout_IsRefused(string change)
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        List<CompactDatabasePlanTable> tables = await DiscoverAsync(file);
        int index = tables.FindIndex(static table => table.OwnerTDefPage == 2);
        CompactDatabasePlanTable original = tables[index];
        TDefImage image = original.Definition;
        List<ColumnInfo> columns = [.. image.Columns];
        List<TDefIndexDescriptor> indexes = [.. image.RealIndexes];
        List<TDefLogicalIndexDescriptor> logicalIndexes = [.. image.LogicalIndexes];
        TDefHeader header = image.Header;
        switch (change)
        {
            case "header":
                header = header with { Counters = header.Counters with { RowCount = header.Counters.RowCount + 1 } };
                break;
            case "column":
                columns[0] = columns[0] with { FixedOff = columns[0].FixedOff + 1 };
                break;
            case "index":
                TDefIndexDescriptor storedIndex = indexes[0];
                indexes[0] = new(storedIndex.Slot, storedIndex.RootPage + 1, [.. storedIndex.Columns], storedIndex.RawDescriptor.ToArray());
                break;
            default:
                TDefLogicalIndexDescriptor logicalIndex = logicalIndexes[0];
                logicalIndexes[0] = new(logicalIndex.Entry, logicalIndex.Name + "changed", logicalIndex.RawDescriptor.ToArray());
                break;
        }

        tables[index] = new(original.OwnerTDefPage, new(image.CopyBytes(), header, columns, image.HasDeletedColumns, image.Issues, image.IndexSection, indexes, logicalIndexes));
        var accounting = new CompactDiscoveryAccounting(new());
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactDatabasePlanValidator.ValidateAsync(file.Pages, file.Format, tables, new(), Ct, accounting));
        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task CancellationAndBudgetFailures_ReleaseAdmissionAndKeepCharges()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        List<CompactDatabasePlanTable> tables = await DiscoverAsync(file);
        var measured = new CompactDiscoveryAccounting(new());
        await CompactDatabasePlanValidator.ValidateAsync(file.Pages, file.Format, tables, new(), Ct, measured);
        var budget = new CompactDiscoveryBudget { MaxPageReads = measured.PageReads };
        var accounting = new CompactDiscoveryAccounting(budget);
        await CompactDatabasePlanValidator.ValidateAsync(file.Pages, file.Format, tables, budget, Ct, accounting);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactDatabasePlanValidator.ValidateAsync(file.Pages, file.Format, tables, budget, Ct, accounting));
        Assert.Equal(measured.PageReads, accounting.PageReads);
        Assert.Equal(0, accounting.ActiveWorkingBytes);

        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var interrupted = new CancelingPageSource(file.Pages, cancellation);
        var canceledAccounting = new CompactDiscoveryAccounting(new());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactDatabasePlanValidator.ValidateAsync(interrupted, file.Format, tables, new(), cancellation.Token, canceledAccounting));
        Assert.True(canceledAccounting.PageReads > 0);
        Assert.Equal(0, canceledAccounting.ActiveWorkingBytes);

        var tiny = new CompactDiscoveryBudget { MaxWorkingBytes = 1 };
        var tinyAccounting = new CompactDiscoveryAccounting(tiny);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactDatabasePlanValidator.ValidateAsync(file.Pages, file.Format, tables, tiny, Ct, tinyAccounting));
        Assert.Equal(0, tinyAccounting.PageReads);
        Assert.Equal(0, tinyAccounting.ActiveWorkingBytes);
    }

    private static async Task<byte[]> CreateAsync(DatabaseFormat kind)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Items", [new("Id", typeof(int))], Ct);
            await writer.InsertRowAsync("Items", [17], Ct);
            await writer.Services.CatalogArtifacts.CreateTableAsync(new("HiddenLocal", [new("Token", typeof(int))], [], Constants.SystemObjects.SystemObjectFlag), Ct);
            await writer.CreateLinkedTableAsync("External", "Z:\\never-open-this-source.mdb", "SourceTable", Ct);
        }

        return stream.ToArray();
    }

    private static DatabaseFile Open(byte[] bytes)
        => DatabaseFile.ForReader(new MemoryStream(bytes, writable: false), bytes.AsSpan(0, Math.Min(4096, bytes.Length)).ToArray(), ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: false);

    private static async Task<List<CompactDatabasePlanTable>> DiscoverAsync(DatabaseFile file)
    {
        var tables = new List<CompactDatabasePlanTable>();
        await CompactStorageInventory.ScanAsync(file.Pages, file.Format, file.TableDefs, new(), (_, _) => ValueTask.CompletedTask, Ct, (owner, image, _) =>
        {
            tables.Add(new(owner, image));
            return ValueTask.CompletedTask;
        });
        return tables;
    }

    private static async Task<byte[]> ReadAllAsync(DatabaseFile file)
    {
        await using var stream = new MemoryStream();
        for (long page = 0; page < file.Pages.PageCount; page++)
        {
            byte[] buffer = await file.Pages.ReadPageAsync(page, Ct);
            try
            {
                await stream.WriteAsync(buffer.AsMemory(0, file.Format.PageSize), Ct);
            }
            finally
            {
                PageBuffers.Return(buffer);
            }
        }

        return stream.ToArray();
    }

    private sealed class CancelingPageSource(IPageSource inner, CancellationTokenSource cancellation) : IPageSource
    {
        public int PageSize => inner.PageSize;

        public long PageCount => inner.PageCount;

        public bool IsDisposed => inner.IsDisposed;

        public async ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            byte[] page = await inner.ReadPageAsync(pageNumber, cancellationToken);
            await cancellation.CancelAsync();
            return page;
        }

        public void ThrowIfDisposed() => inner.ThrowIfDisposed();

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => inner.ThrowIfDisposedOrCancelled(cancellationToken);
    }
}
