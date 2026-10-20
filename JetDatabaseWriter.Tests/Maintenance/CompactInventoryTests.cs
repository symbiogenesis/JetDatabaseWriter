namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using Xunit;

public sealed class CompactInventoryTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DiscoverySession_EnumeratesDiscoveredTablesAndIndexRoots(DatabaseFormat format)
    {
        byte[] bytes = await CreateAsync(format);
        await using DatabaseFile file = Open(bytes);
        var session = new CompactDiscoverySession(file.Pages, file.Format, new());
        var storage = new List<CompactStorageRecord>();
        var rows = new Dictionary<long, int>();
        var roots = new List<(long Owner, long Root)>();
        await session.ScanDatabaseAsync(
            (record, _) =>
            {
                storage.Add(record);
                Assert.True(session.Accounting.ActiveWorkingBytes > 0);
                return ValueTask.CompletedTask;
            },
            (owner, _, _, _) =>
            {
                rows[owner] = rows.GetValueOrDefault(owner) + 1;
                return ValueTask.CompletedTask;
            },
            (record, _) =>
            {
                if (record.Kind == "index-terminal")
                {
                    Assert.True(record.BookmarkTargetsValidated);
                    roots.Add((record.OwnerTDefPage, record.RootPageNumber));
                }

                return ValueTask.CompletedTask;
            },
            Ct);
        CompactStorageRecord item = Assert.Single(storage, record => record.Kind == "catalog-object" && record.Name == "Items");
        Assert.Equal(1, rows[item.OwnerTDefPage!.Value]);
        Assert.True(rows[2] > 0);
        Assert.Equal(
            storage.Where(record => record.Role == "index-root").Select(record => (record.OwnerTDefPage!.Value, record.TargetPageNumber!.Value)).Order(),
            roots.Order());
        Assert.Equal(rows.Values.Sum(), session.Accounting.Rows);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DiscoverySession_DatabaseScanRefusesAnIndexBookmarkIntoIndexStorage(DatabaseFormat kind)
    {
        byte[] bytes = await CreateAsync(kind);
        List<CompactStorageRecord> inventory = await ScanAsync(bytes);
        long owner = Assert.Single(inventory, record => record.Kind == "catalog-object" && record.Name == "Items").OwnerTDefPage!.Value;
        long root = Assert.Single(inventory, record => record.Role == "index-root" && record.OwnerTDefPage == owner).TargetPageNumber!.Value;
        var format = JetFormat.ForNewDatabase(kind);
        byte[] leaf = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, owner, [new([1], root, 0)], enablePrefixCompression: false);
        leaf.CopyTo(bytes, checked((int)(root * format.PageSize)));
        byte[] original = bytes.ToArray();
        await using DatabaseFile file = Open(bytes);
        var session = new CompactDiscoverySession(file.Pages, file.Format, new());
        bool invalidTerminal = false;
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await session.ScanDatabaseAsync(
            (_, _) => ValueTask.CompletedTask,
            (_, _, _, _) => ValueTask.CompletedTask,
            (record, _) =>
            {
                invalidTerminal |= record.Kind == "index-terminal" && record.RootPageNumber == root;
                return ValueTask.CompletedTask;
            },
            Ct));
        Assert.Equal(JetErrorCode.CorruptIndex, error.ErrorCode);
        Assert.Equal(root, error.ErrorInfo.PageNumber);
        Assert.False(invalidTerminal);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
        Assert.Equal(original, bytes);
    }

    [Theory]
    [InlineData("catalog")]
    [InlineData("schema")]
    [InlineData("maps")]
    [InlineData("diagnostics")]
    [InlineData("references")]
    [InlineData("records")]
    [InlineData("reads")]
    [InlineData("slots")]
    public async Task DiscoverySession_RepeatedInventoryConsumesOneAllowance(string kind)
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        Array.Resize(ref bytes, bytes.Length + 4096);
        bytes[^4096] = 0x7F;
        await using DatabaseFile file = Open(bytes);
        var measured = new CompactDiscoverySession(file.Pages, file.Format, new());
        await measured.ScanInventoryAsync((_, _) => ValueTask.CompletedTask, Ct);
        CompactDiscoveryCharge charge = kind switch
        {
            "catalog" => CompactDiscoveryCharge.CatalogObjects,
            "schema" => CompactDiscoveryCharge.SchemaBytes,
            "maps" => CompactDiscoveryCharge.MapSteps,
            "diagnostics" => CompactDiscoveryCharge.Diagnostics,
            "references" => CompactDiscoveryCharge.References,
            "records" => CompactDiscoveryCharge.Records,
            "reads" => CompactDiscoveryCharge.PageReads,
            _ => CompactDiscoveryCharge.DirectorySlots,
        };
        long limit = measured.Accounting.ReadCount(charge);
        Assert.True(limit > 0);
        CompactDiscoveryBudget budget = kind switch
        {
            "catalog" => new() { MaxCatalogObjects = limit },
            "schema" => new() { MaxSchemaBytes = limit },
            "maps" => new() { MaxMapSteps = limit },
            "diagnostics" => new() { MaxDiagnostics = limit },
            "references" => new() { MaxReferences = limit },
            "records" => new() { MaxRecords = limit },
            "reads" => new() { MaxPageReads = limit },
            _ => new() { MaxDirectorySlots = limit },
        };
        var session = new CompactDiscoverySession(file.Pages, file.Format, budget);
        await session.ScanInventoryAsync((_, _) => ValueTask.CompletedTask, Ct);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await session.ScanInventoryAsync((_, _) => ValueTask.CompletedTask, Ct));
        Assert.Equal(limit, session.Accounting.ReadCount(charge));
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task DiscoverySession_InventoryAdmissionIncludesAwaitedValueOperation()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        var source = new ObservedPageSource(file.Pages);
        var budget = new CompactDiscoveryBudget
        {
            MaxDefinitionBytes = 4096,
            MaxCatalogObjects = 16,
            MaxWorkingBytes = (4 * 1024 * 1024) + (16 * 1024) + (4096 * 16) + 100_000,
            MaxChainRows = 1,
        };
        byte[] column = LongValueStore.WrapInlineLongValue([9, 8, 7])!;
        await using var destination = new MemoryStream();
        var standalone = new CompactDiscoverySession(source, file.Format, budget);
        await standalone.CopyLongValueAsync(column, destination, Ct);
        var session = new CompactDiscoverySession(source, file.Format, budget);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await session.ScanInventoryAsync(
            async (_, token) => await session.CopyLongValueAsync(column, destination, token),
            Ct));
        Assert.Equal(new byte[] { 9, 8, 7 }, destination.ToArray());
        Assert.Equal(0, source.Reads);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task DiscoverySession_DatabaseRowVisitorRetainsInventoryAndRowAdmission()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        var budget = new CompactDiscoveryBudget
        {
            MaxDefinitionBytes = 4096,
            MaxCatalogObjects = 16,
            MaxWorkingBytes = (4 * 1024 * 1024) + (16 * 1024) + (4096 * 16) + 250_000,
            MaxChainRows = 1,
        };
        byte[] column = LongValueStore.WrapInlineLongValue([9, 8, 7])!;
        await using var destination = new MemoryStream();
        var session = new CompactDiscoverySession(file.Pages, file.Format, budget);
        int visits = 0;
        await Assert.ThrowsAsync<JetLimitationException>(async () => await session.ScanDatabaseAsync(
            (_, _) => ValueTask.CompletedTask,
            async (_, _, _, token) =>
            {
                visits++;
                await session.CopyLongValueAsync(column, destination, token);
            },
            (_, _) => ValueTask.CompletedTask,
            Ct));
        Assert.Equal(1, visits);
        Assert.Empty(destination.ToArray());
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
        await session.CopyLongValueAsync(column, destination, Ct);
        Assert.Equal(new byte[] { 9, 8, 7 }, destination.ToArray());
    }

    [Fact]
    public async Task DiscoverySession_FailedInventoryRetainsReadAndRecordCharges()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        var failure = new IOException("inventory visitor failed");
        var session = new CompactDiscoverySession(file.Pages, file.Format, new() { MaxRecords = 2 });
        IOException actual = await Assert.ThrowsAsync<IOException>(async () => await session.ScanInventoryAsync(
            (record, _) => record.Kind == "page" ? ValueTask.FromException(failure) : ValueTask.CompletedTask,
            Ct));
        Assert.Same(failure, actual);
        Assert.Equal(1, session.Accounting.PageReads);
        Assert.Equal(2, session.Accounting.ReadCount(CompactDiscoveryCharge.Records));
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await session.ScanInventoryAsync((_, _) => ValueTask.CompletedTask, Ct));
        Assert.Equal(1, session.Accounting.PageReads);
    }

    [Fact]
    public async Task DiscoverySession_MalformedCatalogRowKeepsItsAdmissionCharge()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        List<CompactStorageRecord> records = await ScanAsync(bytes);
        CompactStorageRecord first = records.First(record => record.Kind == "catalog-object");
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        int pageOffset = checked((int)first.PageNumber * format.PageSize);
        Assert.True(UsageMap.TryGetRowBound(bytes.AsSpan(pageOffset, format.PageSize), format.DataPage, format.PageSize, first.TargetRowIndex!.Value, out JetDatabaseWriter.Pages.Models.RowBound row));
        Assert.False(row.IsOverflowPointer);
        bytes.AsSpan(pageOffset + row.RowStart, row.RowSize).Clear();
        await using DatabaseFile file = Open(bytes);
        var session = new CompactDiscoverySession(file.Pages, file.Format, new() { MaxCatalogObjects = 1 });
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await session.ScanInventoryAsync((_, _) => ValueTask.CompletedTask, Ct));
        Assert.Equal(1, session.Accounting.ReadCount(CompactDiscoveryCharge.CatalogObjects));
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await session.ScanInventoryAsync((_, _) => ValueTask.CompletedTask, Ct));
        Assert.Equal(1, session.Accounting.ReadCount(CompactDiscoveryCharge.CatalogObjects));
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task DiscoverySession_InventoryAndRawScansSharePhysicalReadAllowance()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        var measured = new CompactDiscoverySession(file.Pages, file.Format, new());
        await measured.ScanInventoryAsync((_, _) => ValueTask.CompletedTask, Ct);
        var source = new ObservedPageSource(file.Pages);
        var session = new CompactDiscoverySession(source, file.Format, new() { MaxPageReads = measured.Accounting.PageReads });
        await session.ScanInventoryAsync((_, _) => ValueTask.CompletedTask, Ct);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await session.ScanRowsAsync(2, (_, _, _) => ValueTask.CompletedTask, Ct));
        Assert.Equal(measured.Accounting.PageReads, source.Reads);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Inventory_HashesEveryPageAndPinsKnownAndOpaqueStorage(DatabaseFormat format)
    {
        byte[] bytes = await CreateAsync(format);
        int pageSize = JetFormat.ForNewDatabase(format).PageSize;
        Array.Resize(ref bytes, bytes.Length + pageSize);
        bytes[^pageSize] = 0x7F;
        byte[] original = bytes.ToArray();
        List<CompactStorageRecord> records = await ScanAsync(bytes);

        CompactStorageRecord coverage = Assert.Single(records, record => record.Kind == "coverage");
        Assert.Contains("not relocation", coverage.Detail, StringComparison.Ordinal);
        Assert.All(records, record => Assert.True(record.IsPinned));
        CompactStorageRecord[] pageRecords = records.Where(record => record.Kind == "page").ToArray();
        Assert.Equal(bytes.Length / pageSize, pageRecords.Length);
        for (int page = 0; page < pageRecords.Length; page++)
        {
            Assert.Equal(page, pageRecords[page].PageNumber);
            Assert.Equal(Convert.ToHexString(SHA256.HashData(bytes.AsSpan(page * pageSize, pageSize))), pageRecords[page].Sha256);
        }

        Assert.Contains(records, record => record.Kind == "catalog-object" && record.Name == "Items");
        Assert.Contains(records, record => record.Kind == "table-definition" && record.OwnerTDefPage == 2);
        Assert.Contains(records, record => record.Role == "table-owned-map" && record.TargetRowIndex.HasValue);
        Assert.Contains(records, record => record.Role == "index-root" && record.TargetPageNumber.HasValue);
        Assert.Contains(records, record => record.Role == "lval-owned-map" && record.TargetRowIndex.HasValue);
        Assert.Contains(records, record => record.Kind == "diagnostic" && record.PageNumber == pageRecords.Length - 1);
        Assert.Equal(original, bytes);
    }

    [Fact]
    public async Task Inventory_PageLimitRefusesBeforeAnyReadOrRecord()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        var source = new ObservedPageSource(file.Pages);
        using var definitions = new TableDefReader(source, file.Format, cacheResults: false);
        int records = 0;
        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactStorageInventory.ScanAsync(
            source,
            file.Format,
            definitions,
            new CompactTraversalBudget { MaxPages = source.PageCount - 1 },
            (_, _) =>
            {
                records++;
                return ValueTask.CompletedTask;
            },
            Ct));
        Assert.Equal(JetErrorCode.ValueTooLarge, error.ErrorCode);
        Assert.Equal(0, source.Reads);
        Assert.Equal(0, records);
    }

    [Theory]
    [InlineData("objects")]
    [InlineData("references")]
    [InlineData("schema")]
    public async Task Inventory_LimitsRefuseWithoutChangingSource(string kind)
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        byte[] original = bytes.ToArray();
        CompactTraversalBudget budget = kind switch
        {
            "objects" => new() { MaxObjects = 1 },
            "references" => new() { MaxReferences = 1 },
            _ => new() { MaxSchemaBytes = 1 },
        };
        await Assert.ThrowsAsync<JetLimitationException>(async () => await ScanAsync(bytes, budget));
        Assert.Equal(original, bytes);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Inventory_InvalidMapPointerRefusesWithStructuredLocation(DatabaseFormat format)
    {
        byte[] bytes = await CreateAsync(format);
        List<CompactStorageRecord> records = await ScanAsync(bytes);
        CompactStorageRecord table = Assert.Single(records, record => record.Kind == "catalog-object" && record.Name == "Items");
        var profile = JetFormat.ForNewDatabase(format);
        int offset = checked((int)table.OwnerTDefPage!.Value * profile.PageSize) + profile.TDef.UsedPages;
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(offset, 4), checked((uint)(((bytes.Length / profile.PageSize) + 1) << 8)));
        byte[] original = bytes.ToArray();
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(bytes));
        Assert.Equal(JetErrorCode.CorruptTableDefinition, error.ErrorCode);
        Assert.Equal(table.OwnerTDefPage, error.ErrorInfo.PageNumber);
        Assert.NotNull(error.ErrorInfo.Reason);
        Assert.Equal(original, bytes);
    }

    [Fact]
    public async Task Inventory_VisitorCancellationStopsAndIoFailurePropagates()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactStorageInventory.ScanAsync(
            file.Pages,
            file.Format,
            file.TableDefs,
            new(),
            async (_, _) => await cancellation.CancelAsync(),
            cancellation.Token));

        var expected = new IOException("inventory read failed");
        var source = new ObservedPageSource(file.Pages) { Failure = expected };
        using var definitions = new TableDefReader(source, file.Format, cacheResults: false);
        IOException actual = await Assert.ThrowsAsync<IOException>(async () => await CompactStorageInventory.ScanAsync(
            source, file.Format, definitions, new(), (_, _) => ValueTask.CompletedTask, Ct));
        Assert.Same(expected, actual);

        var invalidData = new InvalidDataException("caller stream failure");
        var lateSource = new ObservedPageSource(file.Pages) { Failure = invalidData, FailureAfterReads = file.Pages.PageCount + 1 };
        using var lateDefinitions = new TableDefReader(lateSource, file.Format, cacheResults: false);
        JetCorruptDataException lateActual = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactStorageInventory.ScanAsync(
            lateSource, file.Format, lateDefinitions, new(), (_, _) => ValueTask.CompletedTask, Ct));
        Assert.Same(invalidData, lateActual);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Inventory_CyclicDefinitionRefusesWithStructuredLocation(DatabaseFormat format)
    {
        byte[] bytes = await CreateAsync(format);
        int pageSize = JetFormat.ForNewDatabase(format).PageSize;
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan((2 * pageSize) + 4, 4), 2);
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(bytes));
        Assert.Equal(JetErrorCode.CorruptTableDefinition, error.ErrorCode);
        Assert.Equal(2, error.ErrorInfo.PageNumber);
    }

    [Fact]
    public async Task Inventory_SharedMapsRetainBothOwnerEdges()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        List<CompactStorageRecord> initial = await ScanAsync(bytes);
        CompactStorageRecord table = Assert.Single(initial, record => record.Kind == "catalog-object" && record.Name == "Items");
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        int rootOffset = checked((int)table.OwnerTDefPage!.Value * format.PageSize);
        bytes.AsSpan(rootOffset + format.TDef.UsedPages, 4).CopyTo(bytes.AsSpan(rootOffset + format.TDef.FreePages, 4));

        List<CompactStorageRecord> records = await ScanAsync(bytes);
        CompactStorageRecord owned = Assert.Single(records, record => record.Role == "table-owned-map" && record.OwnerTDefPage == table.OwnerTDefPage);
        CompactStorageRecord free = Assert.Single(records, record => record.Role == "table-free-map" && record.OwnerTDefPage == table.OwnerTDefPage);
        Assert.Equal(owned.TargetPageNumber, free.TargetPageNumber);
        Assert.Equal(owned.TargetRowIndex, free.TargetRowIndex);
        Assert.All(records, record => Assert.True(record.IsPinned));
    }

    [Fact]
    public async Task Inventory_ReferenceMapEmitsBitmapAndExactMembershipLocations()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        List<CompactStorageRecord> initial = await ScanAsync(bytes);
        CompactStorageRecord table = Assert.Single(initial, record => record.Kind == "catalog-object" && record.Name == "Items");
        CompactStorageRecord pointer = Assert.Single(initial, record => record.Role == "table-owned-map" && record.OwnerTDefPage == table.OwnerTDefPage);
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        int mapPage = checked((int)pointer.TargetPageNumber!.Value);
        int mapOffset = mapPage * format.PageSize;
        int rowIndex = pointer.TargetRowIndex!.Value;
        Assert.True(UsageMap.TryGetRowBound(bytes.AsSpan(mapOffset, format.PageSize), format.DataPage, format.PageSize, rowIndex, out JetDatabaseWriter.Pages.Models.RowBound row));
        int rowStart = mapOffset + row.RowStart;
        Assert.Equal(0, bytes[rowStart]);
        int firstPage = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(rowStart + 1, 4));
        int bitmapPage = bytes.Length / format.PageSize;
        Array.Resize(ref bytes, bytes.Length + format.PageSize);
        int bitmapOffset = bitmapPage * format.PageSize;
        bytes[bitmapOffset] = 5;
        bytes[bitmapOffset + 1] = 1;
        for (int bit = 0; bit < (row.RowSize - 5) * 8; bit++)
        {
            if ((bytes[rowStart + 5 + (bit / 8)] & (1 << (bit % 8))) != 0)
            {
                int target = firstPage + bit;
                bytes[bitmapOffset + 4 + (target / 8)] |= checked((byte)(1 << (target % 8)));
            }
        }

        bytes.AsSpan(rowStart, row.RowSize).Clear();
        bytes[rowStart] = 1;
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(rowStart + 1, 4), bitmapPage);
        List<CompactStorageRecord> records = await ScanAsync(bytes);
        Assert.Contains(records, record => record.Role == "table-owned-bitmap" && record.TargetPageNumber == bitmapPage && record.ByteOffset == row.RowStart + 1);
        CompactStorageRecord[] members = records.Where(record => record.Role == "table-owned-member" && record.OwnerTDefPage == table.OwnerTDefPage).ToArray();
        Assert.NotEmpty(members);
        Assert.All(members, record => Assert.Equal(bitmapPage, record.PageNumber));
        Assert.Equal(initial.Count(record => record.Role == "table-owned-member" && record.OwnerTDefPage == table.OwnerTDefPage), members.Length);

        bytes[bitmapOffset] = 0x7F;
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(bytes));
        Assert.Equal(table.OwnerTDefPage, error.ErrorInfo.PageNumber);
    }

    [Fact]
    public async Task Inventory_DiagnosticLimitBoundsOpaquePageObservations()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        const int pageSize = 4096;
        Array.Resize(ref bytes, bytes.Length + (2 * pageSize));
        bytes[^pageSize] = 0x7F;
        bytes[^(2 * pageSize)] = 0x7F;
        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await ScanAsync(bytes, new CompactTraversalBudget { MaxDiagnostics = 1 }));
        Assert.Equal(JetErrorCode.ValueTooLarge, error.ErrorCode);
        Assert.NotNull(error.ErrorInfo.PageNumber);
    }

    [Fact]
    public async Task Inventory_ShortPhysicalBufferRefusesWithStructuredLocation()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        var source = new ObservedPageSource(file.Pages) { ShortPage = 0 };
        using var definitions = new TableDefReader(source, file.Format, cacheResults: false);
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactStorageInventory.ScanAsync(
            source, file.Format, definitions, new(), (_, _) => ValueTask.CompletedTask, Ct));
        Assert.Equal(0, error.ErrorInfo.PageNumber);
        Assert.Equal(1, source.Reads);
    }

    [Fact]
    public async Task Inventory_DoesNotPopulateCallerDefinitionCache()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        var source = new ObservedPageSource(file.Pages);
        using var definitions = new TableDefReader(source, file.Format, cacheResults: true) { VerifyOnHit = false };
        await CompactStorageInventory.ScanAsync(
            source, file.Format, definitions, new(), (_, _) => ValueTask.CompletedTask, Ct);
        int reads = source.Reads;
        Assert.NotNull(await definitions.ReadImageAsync(2, Ct));
        Assert.True(source.Reads > reads);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task Inventory_ChangingPhysicalGeometryRefusesBeforeHashing(bool changePageSize, bool changePageCount)
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        var source = new ObservedPageSource(file.Pages) { ChangePageSizeAfterRead = changePageSize, ChangePageCountAfterRead = changePageCount };
        using var definitions = new TableDefReader(source, file.Format, cacheResults: false);
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactStorageInventory.ScanAsync(
            source, file.Format, definitions, new(), (_, _) => ValueTask.CompletedTask, Ct));
        Assert.Equal(0, error.ErrorInfo.PageNumber);
        Assert.Equal(1, source.Reads);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task Inventory_TotalReadBudgetIncludesMetadataReads(int extraReads)
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        var source = new ObservedPageSource(file.Pages);
        using var definitions = new TableDefReader(source, file.Format, cacheResults: false);
        long limit = file.Pages.PageCount + extraReads;
        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactStorageInventory.ScanAsync(
            source, file.Format, definitions, new() { MaxPageReads = limit }, (_, _) => ValueTask.CompletedTask, Ct));
        Assert.Equal(limit, source.Reads);
        Assert.Contains("MaxPageReads", error.ErrorInfo.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Inventory_ReadBudgetInsideDefinitionRetainsItsCodeAndLocation()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        const int schemaBytes = 64 * 1024;
        using var definitions = new TableDefReader(file.Pages, file.Format, cacheResults: false, maxLogicalBytes: schemaBytes, maxSchemaTraversalBytes: schemaBytes);
        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactStorageInventory.ScanAsync(
            file.Pages,
            file.Format,
            definitions,
            new() { MaxPageReads = file.Pages.PageCount + 2, MaxSchemaBytes = schemaBytes },
            (_, _) => ValueTask.CompletedTask,
            Ct));
        Assert.Equal(JetErrorCode.ValueTooLarge, error.ErrorCode);
        Assert.Equal(2, error.ErrorInfo.PageNumber);
        Assert.Contains("MaxPageReads", error.ErrorInfo.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Inventory_WorkingStorageAdmissionRefusesBeforeReading()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        var source = new ObservedPageSource(file.Pages);
        using var definitions = new TableDefReader(source, file.Format, cacheResults: false);
        int records = 0;
        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactStorageInventory.ScanAsync(
            source,
            file.Format,
            definitions,
            new() { MaxWorkingBytes = 1 },
            (_, _) =>
            {
                records++;
                return ValueTask.CompletedTask;
            },
            Ct));
        Assert.Contains("MaxWorkingBytes", error.ErrorInfo.Reason, StringComparison.Ordinal);
        Assert.Equal(0, source.Reads);
        Assert.Equal(0, records);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Inventory_StandaloneCatalogRecoveryComposesStorageAdmission(bool exactAllowance)
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        var source = new ObservedPageSource(file.Pages);
        using var definitions = new TableDefReader(source, file.Format, cacheResults: false, maxLogicalBytes: 4096, maxSchemaTraversalBytes: 65536);
        const long inventoryBytes = (4 * 1024 * 1024) + (16 * 1024) + (4096 * 16);
        long pageAllowance = Math.Min(16, source.PageCount - 1);
        long indexBytes = (576 * pageAllowance) + (16 * (32 + pageAllowance)) + (18 * 16) + (16 * 4096) + 65536;
        var budget = new CompactTraversalBudget
        {
            MaxObjects = 16,
            MaxSchemaBytes = 65536,
            MaxWorkingBytes = inventoryBytes + indexBytes - (exactAllowance ? 0 : 1),
        };
        if (exactAllowance)
        {
            await CompactStorageInventory.ScanAsync(source, file.Format, definitions, budget, (_, _) => ValueTask.CompletedTask, Ct);
        }
        else
        {
            JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactStorageInventory.ScanAsync(source, file.Format, definitions, budget, (_, _) => ValueTask.CompletedTask, Ct));
            Assert.Contains("index storage", error.ErrorInfo.Reason, StringComparison.Ordinal);
        }

        Assert.True(source.Reads > 0);
    }

    [Fact]
    public async Task Inventory_RecordBudgetStopsBeforeDeliveringAnotherObservation()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        int records = 0;
        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactStorageInventory.ScanAsync(
            file.Pages,
            file.Format,
            file.TableDefs,
            new() { MaxRecords = 1 },
            (_, _) =>
            {
                records++;
                return ValueTask.CompletedTask;
            },
            Ct));
        Assert.Contains("MaxRecords", error.ErrorInfo.Reason, StringComparison.Ordinal);
        Assert.Equal(1, records);
    }

    [Fact]
    public async Task Inventory_EmptyBitmapStillConsumesTraversalBudget()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        Assert.True(UsageMap.TryGetRowBound(bytes.AsSpan(format.PageSize, format.PageSize), format.DataPage, format.PageSize, 0, out JetDatabaseWriter.Pages.Models.RowBound row));
        Assert.Equal(0, bytes[format.PageSize + row.RowStart]);
        bytes.AsSpan(format.PageSize + row.RowStart + 5, row.RowSize - 5).Clear();
        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await ScanAsync(bytes, new() { MaxMapSteps = 1 }));
        Assert.Contains("MaxMapSteps", error.ErrorInfo.Reason, StringComparison.Ordinal);
        Assert.Equal(1, error.ErrorInfo.PageNumber);
    }

    [Fact]
    public async Task Inventory_CatalogDiscoveryDoesNotDependOnOwnedMapBits()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        List<CompactStorageRecord> initial = await ScanAsync(bytes);
        CompactStorageRecord pointer = Assert.Single(initial, record => record.Role == "table-owned-map" && record.OwnerTDefPage == 2);
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        int offset = checked((int)pointer.TargetPageNumber!.Value * format.PageSize);
        Assert.True(UsageMap.TryGetRowBound(bytes.AsSpan(offset, format.PageSize), format.DataPage, format.PageSize, pointer.TargetRowIndex!.Value, out JetDatabaseWriter.Pages.Models.RowBound row));
        Assert.Equal(0, bytes[offset + row.RowStart]);
        bytes.AsSpan(offset + row.RowStart + 5, row.RowSize - 5).Clear();
        List<CompactStorageRecord> records = await ScanAsync(bytes);
        Assert.Equal(initial.Where(record => record.Kind == "catalog-object"), records.Where(record => record.Kind == "catalog-object"));
        Assert.Contains(records, record => record.Kind == "catalog-object" && record.Name == "Items");
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task Inventory_CatalogDamagedPhysicalOwnerRecoversIndexedIdentities(DatabaseFormat kind, bool anotherTableOwner)
    {
        byte[] bytes = await CreateAsync(kind);
        List<CompactStorageRecord> initial = await ScanAsync(bytes);
        CompactStorageRecord item = Assert.Single(initial, record => record.Kind == "catalog-object" && record.Name == "Items");
        var format = JetFormat.ForNewDatabase(kind);
        int ownerOffset = checked(((int)item.PageNumber * format.PageSize) + format.DataPage.TDefOff);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(ownerOffset, 4), anotherTableOwner ? checked((uint)item.OwnerTDefPage!.Value) : 0);
        byte[] original = SHA256.HashData(bytes);

        List<CompactStorageRecord> records = await ScanAsync(bytes);
        Assert.Equal(initial.Where(record => record.Kind == "catalog-object"), records.Where(record => record.Kind == "catalog-object"));
        Assert.Contains(records, record => record.Kind == "diagnostic" && record.PageNumber == item.PageNumber && record.Detail!.Contains("Id index", StringComparison.Ordinal));
        Assert.Contains(records, record => record.Role == "catalog-recovered-bookmark" && record.TargetPageNumber == item.PageNumber && record.TargetRowIndex == item.TargetRowIndex);
        Assert.All(records, record => Assert.True(record.IsPinned));
        Assert.Equal(original, SHA256.HashData(bytes));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task Inventory_CatalogIndexedRecoveryDoesNotRequireUsableAllocationHints(DatabaseFormat kind, bool corruptHint)
    {
        byte[] bytes = await CreateAsync(kind);
        List<CompactStorageRecord> initial = await ScanAsync(bytes);
        CompactStorageRecord item = Assert.Single(initial, record => record.Kind == "catalog-object" && record.Name == "Items");
        CompactStorageRecord pointer = Assert.Single(initial, record => record.Role == "table-owned-map" && record.OwnerTDefPage == 2);
        var format = JetFormat.ForNewDatabase(kind);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(checked(((int)item.PageNumber * format.PageSize) + format.DataPage.TDefOff), 4), 0);
        if (corruptHint)
        {
            int offset = checked((int)pointer.TargetPageNumber!.Value * format.PageSize);
            Assert.True(UsageMap.TryGetRowBound(bytes.AsSpan(offset, format.PageSize), format.DataPage, format.PageSize, pointer.TargetRowIndex!.Value, out JetDatabaseWriter.Pages.Models.RowBound row));
            bytes[offset + row.RowStart] = 0x7F;
        }
        else
        {
            bytes.AsSpan((2 * format.PageSize) + format.TDef.UsedPages, 4).Clear();
        }

        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan((2 * format.PageSize) + format.TDef.NumRows, 4), 0);
        byte[] original = SHA256.HashData(bytes);
        List<CompactStorageRecord> records = await ScanAsync(bytes);
        Assert.Equal(initial.Where(record => record.Kind == "catalog-object"), records.Where(record => record.Kind == "catalog-object"));
        Assert.Contains(records, record => record.Role == "catalog-recovered-bookmark" && record.TargetPageNumber == item.PageNumber);
        Assert.Equal(original, SHA256.HashData(bytes));
    }

    [Theory]
    [InlineData(0xFFFFFF00u)]
    [InlineData(1u)]
    public async Task Inventory_CatalogRecoveryRetainsOutOfFileAllocationHintsAsUnresolved(uint rawHint)
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        List<CompactStorageRecord> initial = await ScanAsync(bytes);
        CompactStorageRecord item = Assert.Single(initial, record => record.Kind == "catalog-object" && record.Name == "Items");
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan((2 * format.PageSize) + format.TDef.UsedPages, 4), rawHint);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(checked(((int)item.PageNumber * format.PageSize) + format.DataPage.TDefOff), 4), 0);
        List<CompactStorageRecord> records = await ScanAsync(bytes);
        Assert.Equal(initial.Where(record => record.Kind == "catalog-object"), records.Where(record => record.Kind == "catalog-object"));
        Assert.Contains(records, record => record.Kind == "unresolved-reference" && record.Role == "table-owned-map" && record.TargetPageNumber == rawHint >> 8 && record.TargetRowIndex == (rawHint & 0xFF));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Inventory_CatalogRecoveryRetainsOutOfFilePhysicalOwnerAsUnresolved(DatabaseFormat kind)
    {
        byte[] bytes = await CreateAsync(kind);
        List<CompactStorageRecord> initial = await ScanAsync(bytes);
        CompactStorageRecord item = Assert.Single(initial, record => record.Kind == "catalog-object" && record.Name == "Items");
        var format = JetFormat.ForNewDatabase(kind);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(checked(((int)item.PageNumber * format.PageSize) + format.DataPage.TDefOff), 4), uint.MaxValue);
        List<CompactStorageRecord> records = await ScanAsync(bytes);
        Assert.Equal(initial.Where(record => record.Kind == "catalog-object"), records.Where(record => record.Kind == "catalog-object"));
        Assert.Contains(records, record => record.Kind == "unresolved-reference" && record.Role == "page-owner-tdef" && record.PageNumber == item.PageNumber && record.TargetPageNumber == uint.MaxValue);
    }

    [Fact]
    public async Task Inventory_CatalogRecoveryDirectoryWorkSharesMapStepAllowance()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        List<CompactStorageRecord> initial = await ScanAsync(bytes);
        CompactStorageRecord item = Assert.Single(initial, record => record.Kind == "catalog-object" && record.Name == "Items");
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(checked(((int)item.PageNumber * 4096) + 4), 4), 0);
        await using DatabaseFile file = Open(bytes);
        var session = new CompactDiscoverySession(file.Pages, file.Format, new());
        await session.ScanInventoryAsync((_, _) => ValueTask.CompletedTask, Ct);
        long steps = session.Accounting.ReadCount(CompactDiscoveryCharge.MapSteps);
        await ScanAsync(bytes, new() { MaxMapSteps = steps });
        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await ScanAsync(bytes, new() { MaxMapSteps = steps - 1 }));
        Assert.Contains("MaxMapSteps", error.ErrorInfo.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Inventory_DamagedCatalogIdIndexRetainsPhysicalOwnerDiscovery()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        List<CompactStorageRecord> initial = await ScanAsync(bytes);
        CompactStorageRecord root = initial.First(record => record.Role == "index-root" && record.OwnerTDefPage == 2);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(checked(((int)root.TargetPageNumber!.Value * 4096) + 4), 4), 0);
        List<CompactStorageRecord> records = await ScanAsync(bytes);
        Assert.Equal(initial.Where(record => record.Kind == "catalog-object"), records.Where(record => record.Kind == "catalog-object"));
        Assert.Contains(records, record => record.Kind == "diagnostic" && record.Detail!.Contains("Id index cannot be completely audited", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Inventory_CatalogRecoveryRetainsSecondaryEvidenceButRefusesDamagedDefinitionIdentity()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        List<CompactStorageRecord> initial = await ScanAsync(bytes);
        CompactStorageRecord item = Assert.Single(initial, record => record.Kind == "catalog-object" && record.Name == "Items");
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        int pageOffset = checked((int)item.PageNumber * format.PageSize);
        int idOffset;
        await using (DatabaseFile intact = Open(bytes))
        {
            TDefImage? image = await intact.TableDefs.ReadImageAsync(2, Ct);
            Assert.NotNull(image);
            ColumnInfo? id = image.Definition.FindColumn("Id");
            Assert.NotNull(id);
            Assert.True(UsageMap.TryGetRowBound(bytes.AsSpan(pageOffset, format.PageSize), format.DataPage, format.PageSize, item.TargetRowIndex!.Value, out JetDatabaseWriter.Pages.Models.RowBound row));
            idOffset = pageOffset + row.RowStart + format.RowFields.NumCols + id.FixedOff;
        }

        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(idOffset, 4), checked((int)item.ObjectId!.Value + 1));
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(pageOffset + format.DataPage.TDefOff, 4), 0);
        byte[] original = bytes.ToArray();
        await using DatabaseFile file = Open(bytes);
        var records = new List<CompactStorageRecord>();
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactStorageInventory.ScanAsync(
            file.Pages,
            file.Format,
            file.TableDefs,
            new(),
            (record, _) =>
            {
                records.Add(record);
                return ValueTask.CompletedTask;
            },
            Ct));
        Assert.Equal(JetErrorCode.CorruptTableDefinition, error.ErrorCode);
        Assert.Equal(item.ObjectId + 1, error.ErrorInfo.PageNumber);
        Assert.Contains(records, record => record.Kind == "catalog-object" && record.Name == "Items" && record.ObjectId == item.ObjectId + 1);
        Assert.Contains(records, record => record.Role == "catalog-recovered-bookmark" && record.Detail!.Contains("ParentIdName", StringComparison.Ordinal));
        Assert.Contains(records, record => record.Kind == "catalog-object" && record.PageNumber == item.PageNumber);
        Assert.Contains(records, record => record.Kind == "diagnostic" && record.Detail!.Contains("key disagrees", StringComparison.Ordinal));
        Assert.Equal(original, bytes);
    }

    [Fact]
    public async Task Inventory_UnknownCatalogOwnedPageRefusesCompleteDiscovery()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        List<CompactStorageRecord> initial = await ScanAsync(bytes);
        CompactStorageRecord item = Assert.Single(initial, record => record.Kind == "catalog-object" && record.Name == "Items");
        bytes[checked((int)item.PageNumber * 4096)] = 0x7F;
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(bytes));
        Assert.Equal(JetErrorCode.CorruptCatalog, error.ErrorCode);
        Assert.Equal(item.PageNumber, error.ErrorInfo.PageNumber);
    }

    [Fact]
    public async Task Inventory_ContradictoryCatalogRowCountIsAdvisoryAndDiagnosed()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        int offset = (2 * format.PageSize) + format.TDef.NumRows;
        uint declared = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(offset, 4));
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(offset, 4), declared + 1);
        List<CompactStorageRecord> records = await ScanAsync(bytes);
        Assert.Contains(records, record => record.Kind == "diagnostic" && record.PageNumber == 2 && record.Detail!.Contains("advisory", StringComparison.Ordinal));
        Assert.Contains(records, record => record.Kind == "catalog-object" && record.Name == "Items");
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static async Task<byte[]> CreateAsync(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Memo", typeof(string)) { ColumnTypeOverride = ColumnType.MemoType }], Ct);
            await writer.InsertRowAsync("Items", [17, new string('x', 10000)], Ct);
        }

        return stream.ToArray();
    }

    private static DatabaseFile Open(byte[] bytes)
        => DatabaseFile.ForReader(new MemoryStream(bytes, writable: false), bytes.AsSpan(0, Math.Min(4096, bytes.Length)).ToArray(), ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: false);

    private static async Task<List<CompactStorageRecord>> ScanAsync(byte[] bytes, CompactTraversalBudget? budget = null)
    {
        await using DatabaseFile file = Open(bytes);
        var records = new List<CompactStorageRecord>();
        await CompactStorageInventory.ScanAsync(
            file.Pages,
            file.Format,
            file.TableDefs,
            budget ?? new(),
            (record, _) =>
            {
                records.Add(record);
                return ValueTask.CompletedTask;
            },
            Ct);
        return records;
    }

    private sealed class ObservedPageSource(IPageSource inner) : IPageSource
    {
        public int PageSize => this.ChangePageSizeAfterRead && this.Reads > 0 ? inner.PageSize / 2 : inner.PageSize;

        public long PageCount => this.ChangePageCountAfterRead && this.Reads > 0 ? inner.PageCount + 1 : inner.PageCount;

        public bool IsDisposed => inner.IsDisposed;

        internal int Reads { get; private set; }

        internal Exception? Failure { get; init; }

        internal long FailureAfterReads { get; init; }

        internal long? ShortPage { get; init; }

        internal bool ChangePageSizeAfterRead { get; init; }

        internal bool ChangePageCountAfterRead { get; init; }

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            this.Reads++;
            if (pageNumber == this.ShortPage)
            {
                return ValueTask.FromResult(System.Buffers.ArrayPool<byte>.Shared.Rent(256));
            }

            return this.Failure is { } failure && this.Reads > this.FailureAfterReads
                ? ValueTask.FromException<byte[]>(failure)
                : inner.ReadPageAsync(pageNumber, cancellationToken);
        }

        public void ThrowIfDisposed() => inner.ThrowIfDisposed();

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => inner.ThrowIfDisposedOrCancelled(cancellationToken);
    }
}
