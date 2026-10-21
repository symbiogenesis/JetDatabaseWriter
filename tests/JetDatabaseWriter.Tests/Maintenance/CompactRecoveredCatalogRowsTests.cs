namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueEncoding;
using Xunit;

public sealed class CompactRecoveredCatalogRowsTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AcceptedPayload_PreservesOriginalIdentityAndRechecksCompleteSignedId(bool moved)
    {
        var pages = new MemoryPages(unchecked((int)0x80000007), moved);
        var accounting = new CompactDiscoveryAccounting(new());
        using (var recovered = new CompactRecoveredCatalogRows(pages, pages.Format, new(), accounting))
        {
            recovered.Accept(unchecked((int)0x80000007), pages.Location, Ct);
            recovered.SetCatalog(pages.Catalog);
            Assert.True(recovered.Contains(pages.Location));
            Assert.Null(await recovered.ResolveAsync(8, 0, Ct));
            Assert.Equal(0, pages.Reads);
            RowLocation? location = await recovered.ResolveAsync(9, 0, Ct);
            Assert.Equal(pages.Location, location);
            int delivered = 0;
            await recovered.ScanAsync(
                (actual, payload, _) =>
                {
                    delivered++;
                    Assert.Equal(pages.Location, actual);
                    Assert.Equal(pages.Payload, payload.ToArray());
                    return ValueTask.CompletedTask;
                },
                Ct);
            Assert.Equal(1, delivered);
            Assert.Equal(2, pages.Reads);
            Assert.Equal(1, accounting.Rows);
        }

        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task AcceptedPayload_LowBitsCannotReplaceTheAcceptedSignedId()
    {
        var pages = new MemoryPages(unchecked((int)0x80000007));
        var accounting = new CompactDiscoveryAccounting(new());
        using (var recovered = new CompactRecoveredCatalogRows(pages, pages.Format, new(), accounting))
        {
            recovered.Accept(7, pages.Location, Ct);
            recovered.SetCatalog(pages.Catalog);
            int delivered = 0;
            JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await recovered.ScanAsync(
                (_, _, _) =>
                {
                    delivered++;
                    return ValueTask.CompletedTask;
                },
                Ct));
            Assert.Equal(JetErrorCode.CorruptCatalog, error.ErrorCode);
            Assert.Equal(0, delivered);
        }

        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ConflictingAcceptedBookmarks_RefuseBeforeReadingPayload(bool sameBookmark)
    {
        var pages = new MemoryPages(7);
        var accounting = new CompactDiscoveryAccounting(new());
        using (var recovered = new CompactRecoveredCatalogRows(pages, pages.Format, new(), accounting))
        {
            recovered.Accept(7, pages.Location, Ct);
            recovered.Accept(7, pages.Location, Ct);
            RowLocation other = sameBookmark ? pages.Location : pages.Location with { PageNumber = 8, DataPageNumber = 8 };
            Assert.Throws<JetCorruptDataException>(() => recovered.Accept(sameBookmark ? 8 : 7, other, Ct));
            Assert.Equal(0, pages.Reads);
        }

        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData("storage")]
    [InlineData("objects")]
    [InlineData("directory")]
    public async Task RecoveryAllowances_RemainBoundedAndReleaseAfterFailure(string limit)
    {
        var pages = new MemoryPages(7);
        CompactDiscoveryBudget budget = limit switch
        {
            "storage" => new() { MaxWorkingBytes = 511 },
            "objects" => new() { MaxCatalogObjects = 1 },
            _ => new() { MaxDirectorySlots = 1 },
        };
        var accounting = new CompactDiscoveryAccounting(budget);
        using (var recovered = new CompactRecoveredCatalogRows(pages, pages.Format, budget, accounting))
        {
            if (limit == "storage")
            {
                Assert.Throws<JetLimitationException>(() => recovered.Accept(7, pages.Location, Ct));
                Assert.Equal(0, pages.Reads);
            }
            else
            {
                recovered.Accept(7, pages.Location, Ct);
                recovered.SetCatalog(pages.Catalog);
                if (limit == "objects")
                {
                    Assert.Throws<JetLimitationException>(() => recovered.Accept(8, pages.Location with { PageNumber = 8, DataPageNumber = 8 }, Ct));
                    Assert.Equal(0, pages.Reads);
                }
                else
                {
                    Assert.NotNull(await recovered.ResolveAsync(9, 0, Ct));
                    await Assert.ThrowsAsync<JetLimitationException>(async () => await recovered.ResolveAsync(9, 0, Ct));
                    Assert.Equal(1, accounting.ReadCount(CompactDiscoveryCharge.DirectorySlots));
                }
            }
        }

        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task RecoveryVisitorFailureAndCancellation_PreserveFailureAndReleaseBorrowedStorage()
    {
        var pages = new MemoryPages(7);
        var accounting = new CompactDiscoveryAccounting(new());
        using (var recovered = new CompactRecoveredCatalogRows(pages, pages.Format, new(), accounting))
        {
            recovered.Accept(7, pages.Location, Ct);
            recovered.SetCatalog(pages.Catalog);
            long retained = accounting.ActiveWorkingBytes;
            var failure = new IOException("recovered row consumer failed");
            IOException actual = await Assert.ThrowsAsync<IOException>(async () => await recovered.ScanAsync((_, _, _) => ValueTask.FromException(failure), Ct));
            Assert.Same(failure, actual);
            Assert.Equal(retained, accounting.ActiveWorkingBytes);
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            await cancellation.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await recovered.ScanAsync((_, _, _) => ValueTask.CompletedTask, cancellation.Token));
            Assert.Equal(1, pages.Reads);
        }

        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData("slot")]
    [InlineData("owner")]
    [InlineData("live-payload")]
    [InlineData("size")]
    public async Task ChangedAcceptedPayload_RefusesBeforeDelivery(string damage)
    {
        var pages = new MemoryPages(7, moved: true);
        var accounting = new CompactDiscoveryAccounting(new());
        using (var recovered = new CompactRecoveredCatalogRows(pages, pages.Format, new(), accounting))
        {
            RowLocation location = damage == "size" ? pages.Location with { RowSize = pages.Location.RowSize - 1 } : pages.Location;
            recovered.Accept(7, location, Ct);
            recovered.SetCatalog(pages.Catalog);
            if (damage == "slot")
            {
                BinaryPrimitives.WriteUInt16LittleEndian(pages.Image.AsSpan(pages.Format.DataPage.NumRows), 0);
            }
            else if (damage == "owner")
            {
                pages.Image[pages.Format.DataPage.TDefOff] = 8;
            }
            else if (damage == "live-payload")
            {
                BinaryPrimitives.WriteUInt16LittleEndian(pages.Image.AsSpan(pages.Format.DataPage.RowsStart), checked((ushort)pages.Location.RowStart));
            }

            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await recovered.ResolveAsync(9, 0, Ct));
        }

        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    private sealed class MemoryPages : IPageSource
    {
        internal MemoryPages(int id, bool moved = false)
        {
            this.Payload = new RowEncoder(this.Format).SerializeRow(this.Catalog.Definition, [id]);
            this.Image = new byte[this.PageSize];
            this.Image[0] = Constants.PageTypes.Data;
            BinaryPrimitives.WriteUInt32LittleEndian(this.Image.AsSpan(this.Format.DataPage.TDefOff), moved ? 2u : 0u);
            BinaryPrimitives.WriteUInt16LittleEndian(this.Image.AsSpan(this.Format.DataPage.NumRows), 1);
            int start = this.PageSize - this.Payload.Length;
            this.Payload.CopyTo(this.Image, start);
            BinaryPrimitives.WriteUInt16LittleEndian(this.Image.AsSpan(this.Format.DataPage.RowsStart), checked((ushort)(start | (moved ? Constants.DataPage.DeletedRowFlag : 0))));
            this.Location = new(9, 0, start, this.Payload.Length) { DataPageNumber = moved ? 10 : 9 };
        }

        public int PageSize => this.Format.PageSize;

        public long PageCount => 20;

        public bool IsDisposed => false;

        internal JetFormat Format { get; } = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);

        internal TDefImage Catalog { get; } = new([], new(1, 0, 0, default), [new ColumnInfo { Name = "Id", Type = ColumnType.LongIntegerType, ColNum = 0, Flags = Constants.ColumnDescriptorFlags.Fixed }], false, TDefParseIssues.None, null, [], []);

        internal byte[] Payload { get; }

        internal byte[] Image { get; }

        internal RowLocation Location { get; }

        internal int Reads { get; private set; }

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Assert.Equal(this.Location.DataPageNumber, pageNumber);
            this.Reads++;
            byte[] result = ArrayPool<byte>.Shared.Rent(this.PageSize);
            this.Image.CopyTo(result, 0);
            return ValueTask.FromResult(result);
        }

        public void ThrowIfDisposed()
        {
        }

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => cancellationToken.ThrowIfCancellationRequested();
    }
}
