namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Independent active membership distinguishes retained catalog copies without trusting maps.</summary>
public sealed class CompactActiveCatalogRowsTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ValidatedMembership_ExcludesOnlyComparedRetainedRows(DatabaseFormat kind)
        => await ExerciseAsync(kind);

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task ValidatedMembership_IncludesIndependentlyRecoveredSourceRows(DatabaseFormat kind, bool moved)
        => await ExerciseAsync(kind, moved ? "recovered-moved-owner" : "recovered-owner");

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ValidatedMembership_ReplacesAnIntactHeaderWithIndependentlyRecoveredMovedPayload(DatabaseFormat kind)
        => await ExerciseAsync(kind, "recovered-moved-intact-header");

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ValidatedMembership_ExcludesRetainedCatalogRowsWhoseDamagedOwnerMimicsAnotherTable(DatabaseFormat kind)
        => await ExerciseAsync(kind, "recovered-conflicting-owner");

    [Theory]
    [InlineData("active-row")]
    [InlineData("retained-row")]
    [InlineData("ledger")]
    [InlineData("map-membership")]
    [InlineData("unknown-copy")]
    [InlineData("recovered-retained-row")]
    [InlineData("recovered-moved-retained-row")]
    public async Task AlteredRowsOrUnverifiedCopies_Refuse(string damage)
        => await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ExerciseAsync(DatabaseFormat.Jet4Mdb, damage));

    [Theory]
    [InlineData("wrong-source")]
    [InlineData("stale-geometry")]
    [InlineData("disposed")]
    public async Task Evidence_IsBoundToExactDestination(string damage)
        => await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ExerciseAsync(DatabaseFormat.Jet4Mdb, damage));

    [Theory]
    [InlineData("read-budget")]
    [InlineData("row-budget")]
    [InlineData("storage-budget")]
    public async Task IndependentComparison_AdmitsReadsRowsAndRetainedStorage(string damage)
        => await Assert.ThrowsAsync<JetLimitationException>(async () => await ExerciseAsync(DatabaseFormat.Jet4Mdb, damage));

    [Fact]
    public async Task IndependentComparison_ReadAdmissionIsCumulativeAcrossPhases()
    {
        long compared = 0;
        await ExerciseAsync(DatabaseFormat.Jet4Mdb, readObserver: value => compared = value);
        Assert.True(compared > 1);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await ExerciseAsync(DatabaseFormat.Jet4Mdb, maxReadBytes: compared - 1));
    }

    [Fact]
    public async Task CancelledComparison_ReleasesAdmission()
        => await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await ExerciseAsync(DatabaseFormat.Jet4Mdb, "cancelled"));

    [Fact]
    public async Task CancelledInventory_ReleasesScanAdmissionAndRetainsEvidenceUntilDisposal()
        => await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await ExerciseAsync(DatabaseFormat.Jet4Mdb, "inventory-cancelled"));

    [Fact]
    public async Task DamagedSourceDuplicateRows_StillRefuseWithoutActiveEvidence()
    {
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ExerciseAsync(DatabaseFormat.Jet4Mdb, "source-duplicate"));
        Assert.Equal(JetErrorCode.CorruptCatalog, error.ErrorCode);
        Assert.Contains("stored Id", error.Message, StringComparison.Ordinal);
    }

    private static async Task ExerciseAsync(DatabaseFormat kind, string? damage = null, long maxReadBytes = 1_000_000_000, Action<long>? readObserver = null)
    {
        await using var original = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(original, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("Items", [new("Id", typeof(int)), new("Text", typeof(string)) { Description = new string('p', 2000) }], Ct);
        }

        await using var immutable = new MemoryStream(original.ToArray(), writable: false);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(immutable, cancellationToken: Ct);
        JetFormat format = source.Database.Format;
        if (damage == "source-duplicate")
        {
            byte[] damaged = original.ToArray();
            int catalogPage = Enumerable.Range(3, (damaged.Length / format.PageSize) - 3).First(page =>
                damaged[page * format.PageSize] == Constants.PageTypes.Data
                && BinaryPrimitives.ReadUInt32LittleEndian(damaged.AsSpan((page * format.PageSize) + format.DataPage.TDefOff)) == 2);
            int end = damaged.Length;
            Array.Resize(ref damaged, end + format.PageSize);
            damaged.AsSpan(catalogPage * format.PageSize, format.PageSize).CopyTo(damaged.AsSpan(end));
            damaged.AsSpan((2 * format.PageSize) + format.TDef.UsedPages, 4).Clear();
            await new CompactDiscoverySession(new Source(damaged, format.PageSize), format, new()).ScanInventoryAsync(IgnoreAsync, Ct);
            return;
        }

        TDefImage image = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(2, Ct));
        var catalogRows = new CatalogRowReader(format, source.Database.TableDefs, source.Database.OwnedPages);
        long itemsRoot = Assert.Single(await catalogRows.GetCatalogRowsAsync(image.Definition, Ct, scanPhysicalPages: true), static row => row.Name == "Items").TDefPage;
        var indexes = new CompactPinnedTableIndexSource[image.RealIndexes.Count];
        for (int slot = 0; slot < indexes.Length; slot++)
        {
            TDefIndexDescriptor descriptor = image.RealIndexes[slot];
            Assert.True(UsageMap.TryReadPointer(image.CopyBytes(), descriptor.Slot.FirstDpOffset - 4, out UsageMapPointer map));
            indexes[slot] = new(descriptor.RootPage, map, EntriesAsync(await IndexLeafChain.ReadEntriesAsync(source.Database, 2, descriptor.RootPage, Ct), Ct));
        }

        byte[] sourceBytes = original.ToArray();
        if (damage?.StartsWith("recovered-", StringComparison.Ordinal) == true)
        {
            int catalogPage = Enumerable.Range(3, (sourceBytes.Length / format.PageSize) - 3).First(page =>
                sourceBytes[page * format.PageSize] == Constants.PageTypes.Data
                && BinaryPrimitives.ReadUInt32LittleEndian(sourceBytes.AsSpan((page * format.PageSize) + format.DataPage.TDefOff)) == 2);
            int offset = catalogPage * format.PageSize;
            if (damage.Contains("-moved-", StringComparison.Ordinal))
            {
                int start = BinaryPrimitives.ReadUInt16LittleEndian(sourceBytes.AsSpan(offset + format.DataPage.RowsStart)) & Constants.DataPage.RowOffsetMask;
                int length = format.PageSize - start;
                int end = sourceBytes.Length;
                Array.Resize(ref sourceBytes, end + format.PageSize);
                int payloadPage = end / format.PageSize;
                sourceBytes[end] = Constants.PageTypes.Data;
                BinaryPrimitives.WriteUInt16LittleEndian(sourceBytes.AsSpan(end + format.DataPage.NumRows), 1);
                BinaryPrimitives.WriteUInt16LittleEndian(sourceBytes.AsSpan(end + format.DataPage.RowsStart), checked((ushort)(start | Constants.DataPage.DeletedRowFlag)));
                sourceBytes.AsSpan(offset + start, length).CopyTo(sourceBytes.AsSpan(end + start));
                sourceBytes[offset + start] = 0;
                sourceBytes[offset + start + 1] = unchecked((byte)payloadPage);
                sourceBytes[offset + start + 2] = unchecked((byte)(payloadPage >> 8));
                sourceBytes[offset + start + 3] = unchecked((byte)(payloadPage >> 16));
                BinaryPrimitives.WriteUInt16LittleEndian(sourceBytes.AsSpan(offset + format.DataPage.RowsStart), checked((ushort)(start | Constants.DataPage.OverflowRowFlag)));
            }

            if (damage != "recovered-moved-intact-header")
            {
                BinaryPrimitives.WriteUInt32LittleEndian(sourceBytes.AsSpan(offset + format.DataPage.TDefOff), damage == "recovered-conflicting-owner" ? checked((uint)itemsRoot) : 0);
            }
        }

        var sourcePages = new Source(sourceBytes, format.PageSize);

        await using var destination = new MemoryStream();
        await destination.WriteAsync(sourceBytes, Ct);
        await using var rows = new MemoryStream();
        await using var values = new MemoryStream();
        await using var firstFence = new MemoryStream();
        await using var secondFence = new MemoryStream();
        var assembly = new CompactPinnedTableAssemblySession(sourcePages, format, new());
        CompactPinnedTableAssemblyAllocation allocation = await assembly.Discovery.WithCatalogRecoveryAsync(
            async token =>
            {
                int itemRows = 0;
                await assembly.Discovery.ScanRowsAsync(
                    itemsRoot,
                    (_, _, _) =>
                    {
                        itemRows++;
                        return default;
                    },
                    token);
                Assert.Equal(0, itemRows);
                return await CompactPinnedTableAssembly.BuildAsync(assembly, 2, image, indexes, destination, rows, values, firstFence, secondFence, token);
            },
            Ct);
        await CompactGlobalMapInstaller.InstallAsync(sourcePages, format, destination.Length / format.PageSize, destination, new(), Ct);
        byte[] bytes = destination.ToArray();
        byte[] ledger = rows.ToArray();
        int active = checked((int)(allocation.Table.Data.FirstPageNumber * format.PageSize));
        int retained = checked((int)(BinaryPrimitives.ReadUInt32LittleEndian(ledger) >> 8) * format.PageSize);
        if (damage is "active-row" or "retained-row" or "recovered-retained-row" or "recovered-moved-retained-row")
        {
            int offset = damage == "active-row" ? active : retained;
            bytes[offset + format.PageSize - 1] ^= 1;
        }
        else if (damage == "ledger")
        {
            ledger[4] ^= 1;
        }
        else if (damage == "map-membership")
        {
            int map = checked((int)(allocation.Table.OwnedPages.FirstPageNumber * format.PageSize));
            bytes[map + format.PageSize - format.UsageMapRowSize + 5] ^= 1;
        }
        else if (damage == "unknown-copy")
        {
            int end = bytes.Length;
            Array.Resize(ref bytes, end + format.PageSize);
            bytes.AsSpan(active, format.PageSize).CopyTo(bytes.AsSpan(end));
        }

        var pages = new Source(bytes, format.PageSize);
        var comparison = new CompactDiscoverySession(sourcePages, format, new());
        await using var reopenedRows = new MemoryStream(ledger, writable: false);
        await using var reopenedValues = new MemoryStream(values.ToArray(), writable: false);
        try
        {
            if (damage is null)
            {
                var ordinary = new CompactDiscoverySession(pages, format, new());
                JetCorruptDataException ambiguity = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ordinary.ScanInventoryAsync(IgnoreAsync, Ct));
                Assert.Equal(JetErrorCode.CorruptCatalog, ambiguity.ErrorCode);
            }

            using (CompactActiveCatalogRows membership = await CompactActiveCatalogRows.ValidateAsync(
                comparison,
                image,
                pages,
                reopenedRows,
                reopenedValues,
                allocation.Table,
                allocation.Indexes.Select(static index => new CompactTDefIndexReferences(checked((uint)index.Tree.FirstPageNumber), index.OwnedPages.Pointer)).ToArray(),
                new()
                {
                    MaxReadBytes = damage == "read-budget" ? 1 : maxReadBytes,
                    MaxRows = damage == "row-budget" ? 1 : 100_000,
                    MaxWorkingBytes = damage == "storage-budget" ? 1 : 64 * 1024 * 1024,
                },
                damage == "cancelled" ? new CancellationToken(canceled: true) : Ct))
            {
                Assert.Equal(allocation.Table.Data.RowCount, membership.ActiveRows);
                Assert.True(membership.ReadBytes > 0);
                readObserver?.Invoke(membership.ReadBytes);
                Assert.False(membership.DatabaseValidated);
                Assert.False(membership.OwnershipProven);
                Assert.True(comparison.Accounting.ActiveWorkingBytes > 0);
                if (damage == "stale-geometry")
                {
                    pages.ExtraPages = 1;
                }

                var inventory = new CompactDiscoverySession(damage == "wrong-source" ? new Source(bytes, format.PageSize) : pages, format, new());
                var objects = new List<CompactStorageRecord>();
                long held = comparison.Accounting.ActiveWorkingBytes;
                long readBefore = pages.Reads;
                using var cancelInventory = CancellationTokenSource.CreateLinkedTokenSource(Ct);
                if (damage == "disposed")
                {
                    membership.Dispose();
                }

                try
                {
                    await inventory.ScanInventoryAsync(
                        async (record, token) =>
                        {
                            if (damage == "inventory-cancelled" && record.Kind == "page")
                            {
                                await cancelInventory.CancelAsync();
                                token.ThrowIfCancellationRequested();
                            }

                            if (record.Kind == "catalog-object")
                            {
                                objects.Add(record);
                            }
                        },
                        cancelInventory.Token,
                        membership);
                }
                finally
                {
                    Assert.Equal(0, inventory.Accounting.ActiveWorkingBytes);
                    if (damage == "disposed")
                    {
                        Assert.Equal(readBefore, pages.Reads);
                    }
                    else
                    {
                        Assert.Equal(held, comparison.Accounting.ActiveWorkingBytes);
                    }
                }

                Assert.Equal(membership.ActiveRows, objects.Count);
                Assert.All(objects, record => Assert.InRange(record.PageNumber, allocation.Table.Data.FirstPageNumber, allocation.Table.Data.FirstPageNumber + allocation.Table.Data.PageCount - 1));
                Assert.Equal(0, inventory.Accounting.ActiveWorkingBytes);
                var rawRows = new HashSet<(long Page, int Row)>();
                await inventory.ScanDatabaseAsync(
                    IgnoreAsync,
                    (owner, location, _, token) =>
                    {
                        token.ThrowIfCancellationRequested();
                        Assert.NotEqual(itemsRoot, owner);
                        if (owner == 2)
                        {
                            Assert.True(rawRows.Add((location.PageNumber, location.RowIndex)));
                            Assert.True(membership.Include(location.PageNumber, location.RowIndex));
                        }

                        return default;
                    },
                    static (_, _) => default,
                    Ct,
                    activeCatalog: membership);
                Assert.Equal(membership.ActiveRows, rawRows.Count);
                var fieldRows = new HashSet<(long Page, int Row)>();
                await inventory.ScanDatabaseFieldsAsync(
                    IgnoreAsync,
                    (owner, field, token) =>
                    {
                        token.ThrowIfCancellationRequested();
                        Assert.NotEqual(itemsRoot, owner);
                        if (owner == 2)
                        {
                            Assert.Contains((field.Location.PageNumber, field.Location.RowIndex), rawRows);
                            _ = fieldRows.Add((field.Location.PageNumber, field.Location.RowIndex));
                        }

                        return default;
                    },
                    static (_, _, _, _) => default,
                    static (_, _) => default,
                    Ct,
                    activeCatalog: membership);
                Assert.Equal(rawRows.Count, fieldRows.Count);
                Assert.Equal(0, inventory.Accounting.ActiveWorkingBytes);
            }
        }
        finally
        {
            Assert.Equal(0, comparison.Accounting.ActiveWorkingBytes);
            Assert.Equal(original.ToArray(), immutable.ToArray());
        }
    }

    private static ValueTask IgnoreAsync(CompactStorageRecord record, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        return default;
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

    private sealed class Source(byte[] bytes, int pageSize) : IPageSource
    {
        public int PageSize { get; } = pageSize;

        public long PageCount => (bytes.Length / this.PageSize) + this.ExtraPages;

        public bool IsDisposed => false;

        internal long ExtraPages { get; set; }

        internal long Reads { get; private set; }

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            this.Reads++;
            byte[] page = ArrayPool<byte>.Shared.Rent(this.PageSize);
            bytes.AsSpan(checked((int)(pageNumber * this.PageSize)), this.PageSize).CopyTo(page);
            return new(page);
        }

        public void ThrowIfDisposed()
        {
        }

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => cancellationToken.ThrowIfCancellationRequested();
    }
}
