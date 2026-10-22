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
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>Database discovery retains every discovered physical descriptor's root and map evidence.</summary>
public sealed class CompactIndexDescriptorOwnershipTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("FixedText/NativeFixedTextJet3Cp1252.mdb")]
    [InlineData("FixedText/NativeFixedTextJet4.mdb")]
    [InlineData("NorthwindTraders.accdb")]
    [InlineData("ComplexFields.accdb")]
    [InlineData("mdbtools/nwind.mdb")]
    public async Task NativeDatabaseDiscovery_IncludesCatalogAndHiddenTableMapDeclarations(string fixture)
    {
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", fixture), Ct);
        byte[] original = bytes.ToArray();
        await using DatabaseFile file = Open(bytes);
        var session = new CompactDiscoverySession(file.Pages, file.Format, new());
        List<CompactStorageRecord> storage = [];
        List<CompactIndexOwnershipRecord> ownership = [];
        await ScanAsync(session, storage, ownership);

        Assert.Equal(
            storage.Where(static record => record.Role == "index-owned-map").Select(static record => (record.OwnerTDefPage, record.TargetPageNumber, record.TargetRowIndex)).Order(),
            ownership.Where(static record => record.Kind is "map-declaration" or "shared-map-row").Select(static record => ((long?)record.OwnerTDefPage, (long?)record.PageNumber, record.MapRowIndex)).Order());
        Assert.Contains(ownership, static record => record.Kind == "root-declaration" && record.OwnerTDefPage == 2);
        Assert.True(Assert.Single(ownership, static record => record.Kind == "ownership-terminal").DeclaredRootsObserved);
        Assert.All(ownership, static record => Assert.True(record.IsPinned));
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
        Assert.Equal(original, bytes);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DatabaseDiscovery_DeclaresAllDiscoveredPhysicalMaps(DatabaseFormat kind)
    {
        byte[] bytes = await CreateAsync(kind);
        byte[] original = bytes.ToArray();
        await using DatabaseFile file = Open(bytes);
        var session = new CompactDiscoverySession(file.Pages, file.Format, new());
        List<CompactStorageRecord> storage = [];
        List<CompactIndexOwnershipRecord> ownership = [];
        await ScanAsync(session, storage, ownership);

        Assert.Equal(
            storage.Where(static record => record.Role == "index-owned-map").Select(static record => (record.OwnerTDefPage, record.TargetPageNumber, record.TargetRowIndex)).Order(),
            ownership.Where(static record => record.Kind is "map-declaration" or "shared-map-row").Select(static record => ((long?)record.OwnerTDefPage, (long?)record.PageNumber, record.MapRowIndex)).Order());
        Assert.Contains(ownership, static record => record.Kind == "map-declaration" && record.OwnerTDefPage == 2);
        Assert.True(ownership.Where(static record => record.Kind == "root-declaration").Select(static record => record.OwnerTDefPage).Distinct().Count() >= 3);
        CompactIndexOwnershipRecord terminal = Assert.Single(ownership, static record => record.Kind == "ownership-terminal");
        Assert.Equal(ownership.Count(static record => record.Kind is "root-declaration" or "shared-root" or "missing-root"), terminal.DeclaredDescriptorCount);
        Assert.Equal(0, terminal.MissingRootCount);
        Assert.Equal(0, terminal.MissingMapCount);
        Assert.False(terminal.ExclusiveOwnershipProven);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
        Assert.Equal(original, bytes);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DatabaseDiscovery_ExactIndexPageAllowanceAlsoAdmitsMapCoordinates(DatabaseFormat kind)
    {
        byte[] bytes = await CreateAsync(kind);
        await using DatabaseFile file = Open(bytes);
        var measured = new CompactDiscoverySession(file.Pages, file.Format, new());
        await ScanAsync(measured, [], []);
        long indexPages = measured.Accounting.ReadCount(CompactDiscoveryCharge.IndexPages);
        Assert.True(indexPages > 1);

        var exact = new CompactDiscoverySession(file.Pages, file.Format, new() { MaxIndexPages = indexPages });
        List<CompactIndexOwnershipRecord> ownership = [];
        await ScanAsync(exact, [], ownership);
        Assert.Equal(indexPages, exact.Accounting.ReadCount(CompactDiscoveryCharge.IndexPages));
        Assert.True(Assert.Single(ownership, static record => record.Kind == "ownership-terminal").DeclaredRootsObserved);
        Assert.Equal(0, exact.Accounting.ActiveWorkingBytes);

        var exhausted = new CompactDiscoverySession(file.Pages, file.Format, new() { MaxIndexPages = indexPages - 1 });
        await Assert.ThrowsAsync<JetLimitationException>(async () => await ScanAsync(exhausted, [], []));
        Assert.Equal(0, exhausted.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DatabaseDiscovery_CrossTableMapAliasRetainsBothOwners(DatabaseFormat kind)
    {
        byte[] bytes = await CreateAsync(kind);
        List<CompactStorageRecord> inventory = await InventoryAsync(bytes);
        long items = Assert.Single(inventory, static record => record.Kind == "catalog-object" && record.Name == "Items").OwnerTDefPage!.Value;
        long other = Assert.Single(inventory, static record => record.Kind == "catalog-object" && record.Name == "Other").OwnerTDefPage!.Value;
        CompactStorageRecord source = Assert.Single(inventory, record => record.Role == "index-owned-map" && record.OwnerTDefPage == items);
        CompactStorageRecord target = Assert.Single(inventory, record => record.Role == "index-owned-map" && record.OwnerTDefPage == other);
        int pageSize = JetFormat.ForNewDatabase(kind).PageSize;
        bytes.AsSpan(checked((int)(source.PageNumber * pageSize)) + source.ByteOffset, 4).CopyTo(bytes.AsSpan(checked((int)(target.PageNumber * pageSize)) + target.ByteOffset, 4));
        byte[] original = bytes.ToArray();
        await using DatabaseFile file = Open(bytes);
        var session = new CompactDiscoverySession(file.Pages, file.Format, new());
        List<CompactIndexOwnershipRecord> ownership = [];
        await ScanAsync(session, [], ownership);

        CompactIndexOwnershipRecord alias = Assert.Single(ownership, record => record.Kind == "shared-map-row" && record.OwnerTDefPage == other);
        Assert.Equal(items, alias.RelatedOwnerTDefPage);
        Assert.Equal(source.TargetPageNumber, alias.PageNumber);
        Assert.Equal(source.TargetRowIndex, alias.MapRowIndex);
        Assert.True(alias.IsPinned);
        Assert.False(Assert.Single(ownership, static record => record.Kind == "ownership-terminal").ExclusiveOwnershipProven);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
        Assert.Equal(original, bytes);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DatabaseDiscovery_ZeroRootAndMapRemainExplicit(DatabaseFormat kind)
    {
        byte[] bytes = await CreateAsync(kind);
        List<CompactStorageRecord> inventory = await InventoryAsync(bytes);
        long items = Assert.Single(inventory, static record => record.Kind == "catalog-object" && record.Name == "Items").OwnerTDefPage!.Value;
        int pageSize = JetFormat.ForNewDatabase(kind).PageSize;
        foreach (CompactStorageRecord reference in inventory.Where(record => record.OwnerTDefPage == items && record.Role is "index-root" or "index-owned-map"))
        {
            bytes.AsSpan(checked((int)(reference.PageNumber * pageSize)) + reference.ByteOffset, 4).Clear();
        }

        await using DatabaseFile file = Open(bytes);
        var session = new CompactDiscoverySession(file.Pages, file.Format, new());
        List<CompactIndexOwnershipRecord> ownership = [];
        await ScanAsync(session, [], ownership);

        Assert.Single(ownership, record => record.Kind == "missing-root" && record.OwnerTDefPage == items);
        Assert.Single(ownership, record => record.Kind == "missing-ownership-map" && record.OwnerTDefPage == items);
        CompactIndexOwnershipRecord terminal = Assert.Single(ownership, static record => record.Kind == "ownership-terminal");
        Assert.Equal(1, terminal.MissingRootCount);
        Assert.Equal(1, terminal.MissingMapCount);
        Assert.False(terminal.DeclaredRootsObserved);
        Assert.False(terminal.ExclusiveOwnershipProven);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MapVisitorFailureOrCancellation_ReleasesDatabaseAdmission(bool cancel)
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        byte[] original = bytes.ToArray();
        await using DatabaseFile file = Open(bytes);
        var session = new CompactDiscoverySession(file.Pages, file.Format, new());
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        bool mapObserved = false;
        bool terminal = false;
        async Task Scan()
            => await session.ScanDatabaseAsync(
                static (_, _) => ValueTask.CompletedTask,
                static (_, _, _, _) => ValueTask.CompletedTask,
                static (_, _) => ValueTask.CompletedTask,
                cancellation.Token,
                async (record, _) =>
                {
                    terminal |= record.Kind == "ownership-terminal";
                    if (record.Kind == "map-declaration")
                    {
                        mapObserved = true;
                        Assert.True(session.Accounting.ActiveWorkingBytes > 0);
                        if (cancel)
                        {
                            await cancellation.CancelAsync();
                        }
                        else
                        {
                            throw new IOException("map visitor");
                        }
                    }
                });

        if (cancel)
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(Scan);
        }
        else
        {
            await Assert.ThrowsAsync<IOException>(Scan);
        }

        Assert.True(mapObserved);
        Assert.False(terminal);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
        Assert.Equal(original, bytes);
    }

    private static ValueTask ScanAsync(CompactDiscoverySession session, List<CompactStorageRecord> storage, List<CompactIndexOwnershipRecord> ownership)
        => session.ScanDatabaseAsync(
            (record, _) =>
            {
                storage.Add(record);
                return ValueTask.CompletedTask;
            },
            static (_, _, _, _) => ValueTask.CompletedTask,
            static (_, _) => ValueTask.CompletedTask,
            Ct,
            (record, _) =>
            {
                ownership.Add(record);
                return ValueTask.CompletedTask;
            });

    private static async Task<List<CompactStorageRecord>> InventoryAsync(byte[] bytes)
    {
        await using DatabaseFile file = Open(bytes);
        List<CompactStorageRecord> records = [];
        var session = new CompactDiscoverySession(file.Pages, file.Format, new());
        await session.ScanInventoryAsync(
            (record, _) =>
            {
                records.Add(record);
                return ValueTask.CompletedTask;
            },
            Ct);
        return records;
    }

    private static async Task<byte[]> CreateAsync(DatabaseFormat kind)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            foreach (string name in new[] { "Items", "Other" })
            {
                await writer.CreateTableAsync(name, [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
                await writer.InsertRowAsync(name, [17], Ct);
            }
        }

        return stream.ToArray();
    }

    private static DatabaseFile Open(byte[] bytes)
        => DatabaseFile.ForReader(new MemoryStream(bytes, writable: false), bytes.AsSpan(0, Math.Min(4096, bytes.Length)).ToArray(), ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: false);
}
