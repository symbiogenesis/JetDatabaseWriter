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
    [InlineData("active-row")]
    [InlineData("retained-row")]
    [InlineData("ledger")]
    [InlineData("map-membership")]
    [InlineData("unknown-copy")]
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
        var indexes = new CompactPinnedTableIndexSource[image.RealIndexes.Count];
        for (int slot = 0; slot < indexes.Length; slot++)
        {
            TDefIndexDescriptor descriptor = image.RealIndexes[slot];
            Assert.True(UsageMap.TryReadPointer(image.CopyBytes(), descriptor.Slot.FirstDpOffset - 4, out UsageMapPointer map));
            indexes[slot] = new(descriptor.RootPage, map, EntriesAsync(await IndexLeafChain.ReadEntriesAsync(source.Database, 2, descriptor.RootPage, Ct), Ct));
        }

        await using var destination = new MemoryStream();
        await destination.WriteAsync(original.ToArray(), Ct);
        await using var rows = new MemoryStream();
        await using var values = new MemoryStream();
        await using var firstFence = new MemoryStream();
        await using var secondFence = new MemoryStream();
        CompactPinnedTableAssemblyAllocation allocation = await CompactPinnedTableAssembly.BuildAsync(
            new(source.Database.Pages, format, new()), 2, image, indexes, destination, rows, values, firstFence, secondFence, Ct);
        await CompactGlobalMapInstaller.InstallAsync(source.Database.Pages, format, destination.Length / format.PageSize, destination, new(), Ct);
        byte[] bytes = destination.ToArray();
        byte[] ledger = rows.ToArray();
        int active = checked((int)(allocation.Table.Data.FirstPageNumber * format.PageSize));
        int retained = checked((int)(BinaryPrimitives.ReadUInt32LittleEndian(ledger) >> 8) * format.PageSize);
        if (damage is "active-row" or "retained-row")
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
        var comparison = new CompactDiscoverySession(source.Database.Pages, format, new());
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
