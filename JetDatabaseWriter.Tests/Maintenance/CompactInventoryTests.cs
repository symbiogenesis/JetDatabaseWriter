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
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using Xunit;

public sealed class CompactInventoryTests
{
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
        InvalidDataException lateActual = await Assert.ThrowsAsync<InvalidDataException>(async () => await CompactStorageInventory.ScanAsync(
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
