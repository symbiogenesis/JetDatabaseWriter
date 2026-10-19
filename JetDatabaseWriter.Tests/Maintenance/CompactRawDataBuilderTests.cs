namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages.Paging;
using Xunit;

public sealed class CompactRawDataBuilderTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Build_PacksOpaqueRowsAndMapsOriginalOverflowBookmarks(DatabaseFormat kind)
    {
        var source = new ImagePages(kind, 9);
        byte[] first = new byte[300];
        first.AsSpan().Fill(0xC7);
        byte[] moved = [0xFF, 0xFE, 0, 0x80, 17];
        source.SetRows(3, (0, first), (0x4000, [0, 4, 0, 0]));
        source.SetRows(4, (0x8000, moved));
        source.SetRows(7, (0, new byte[] { 3, 4, 5 }));
        byte[][] original = Array.ConvertAll(source.Pages, static page => (byte[])page.Clone());
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(3L * source.PageSize);
        await using var relocations = new MemoryStream();
        CompactRawDataBuilderResult result = await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, relocations, new(), Ct);
        Assert.False(result.IsValidated);
        Assert.Equal(3, result.RowCount);
        Assert.Equal(1, result.PageCount);
        Assert.Equal(24, result.RelocationBytes);
        Assert.Equal(3, result.FirstPageNumber);
        byte[] map = relocations.ToArray();
        Assert.Equal<uint>([0x300, 0x301, 0x700], [ReadMap(map, 0), ReadMap(map, 8), ReadMap(map, 16)]);
        Assert.Equal<uint>([0x300, 0x301, 0x302], [ReadMap(map, 4), ReadMap(map, 12), ReadMap(map, 20)]);
        var packed = new ImagePages(kind, destination.ToArray());
        var rows = new List<byte[]>();
        await CompactRawRowReader.ScanAsync(
            packed,
            packed.Format,
            2,
            new(),
            (location, bytes, _) =>
            {
                Assert.False(location.IsOverflow);
                rows.Add(bytes.ToArray());
                return ValueTask.CompletedTask;
            },
            Ct);
        Assert.Equal(3, rows.Count);
        Assert.Equal(first, rows[0]);
        Assert.Equal(moved, rows[1]);
        Assert.Equal<byte>([3, 4, 5], rows[2]);
        for (int i = 0; i < original.Length; i++)
        {
            Assert.Equal(original[i], source.Pages[i]);
        }

        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);

        CompactRowRelocationMap relocation = await CompactRowRelocationMap.OpenAsync(relocations, result.RowCount, 4096, Ct);
        await using var firstFence = new MemoryStream();
        await using var secondFence = new MemoryStream();
        CompactRawIndexBuilderResult index = await CompactRawIndexBuilder.BuildAsync(source.Format, 2, RelocateAsync(relocation, Ct), destination, firstFence, secondFence, new(), Ct);
        var indexed = new ImagePages(kind, destination.ToArray());
        int checkedEntries = 0;
        await CompactIndexBookmarkAudit.ScanAsync(
            indexed,
            indexed.Format,
            2,
            index.RootPageNumber,
            new(),
            new(),
            (record, _) =>
            {
                if (record.Kind == "leaf-entry")
                {
                    Assert.True(record.BookmarkTargetValidated);
                    Assert.Equal(3, record.BookmarkDataPageNumber);
                    checkedEntries++;
                }

                return ValueTask.CompletedTask;
            },
            Ct);
        Assert.Equal(3, checkedEntries);
    }

    [Fact]
    public async Task Build_RowSlotsRespectTheNativeWriterLimit()
    {
        var source = new ImagePages(DatabaseFormat.Jet4Mdb, 5);
        var rows = new (int Flags, byte[] Bytes)[256];
        Array.Fill(rows, (0, [1]));
        source.SetRows(3, rows);
        source.SetRows(4, (0, new byte[] { 2 }));
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(3L * source.PageSize);
        await using var relocations = new MemoryStream();
        CompactRawDataBuilderResult result = await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, relocations, new(), Ct);
        Assert.Equal(257, result.RowCount);
        Assert.Equal(2, result.PageCount);
        Assert.Equal(0x401U, ReadMap(relocations.ToArray(), 2052));
        Assert.Equal(Constants.DataPage.MaxRowsPerPage, BinaryPrimitives.ReadUInt16LittleEndian(destination.ToArray().AsSpan((3 * source.PageSize) + source.Format.DataPage.NumRows)));
    }

    [Fact]
    public async Task Build_OutputAndLedgerBudgetsRefuseWithoutCompletionAndReleaseStorage()
    {
        foreach (CompactRawDataBuilderBudget budget in (CompactRawDataBuilderBudget[])[new() { MaxRows = 1 }, new() { MaxRelocationBytes = 8 }, new() { MaxOutputBytes = 1 }, new() { MaxWorkingBytes = 1 }])
        {
            var source = new ImagePages(DatabaseFormat.Jet4Mdb, 4);
            source.SetRows(3, (0, new byte[] { 1 }), (0, new byte[] { 2 }));
            var session = new CompactDiscoverySession(source, source.Format, new());
            await using var destination = new MemoryStream();
            destination.SetLength(3L * source.PageSize);
            await using var relocations = new MemoryStream();
            await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, relocations, budget, Ct));
            Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
            Assert.Equal(new byte[3 * source.PageSize], destination.ToArray().AsSpan(0, 3 * source.PageSize).ToArray());
        }
    }

    [Fact]
    public async Task Build_RefusesAliasedOrNonemptyLedgerBeforeReadingSource()
    {
        var source = new ImagePages(DatabaseFormat.Jet4Mdb, 4);
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(3L * source.PageSize);
        await Assert.ThrowsAsync<ArgumentException>(async () => await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, destination, new(), Ct));
        await using var ledger = new MemoryStream(new byte[8]);
        await Assert.ThrowsAsync<ArgumentException>(async () => await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, ledger, new(), Ct));
        Assert.Equal(0, source.Reads);
    }

    [Fact]
    public async Task Build_FlushFailurePropagatesAndReleasesStorage()
    {
        var source = new ImagePages(DatabaseFormat.Jet4Mdb, 4);
        source.SetRows(3, (0, new byte[] { 1 }));
        var session = new CompactDiscoverySession(source, source.Format, new());
        var failure = new IOException("private staging flush failed");
        await using var destination = new FlushFailureStream(failure);
        destination.SetLength(3L * source.PageSize);
        await using var ledger = new MemoryStream();
        IOException actual = await Assert.ThrowsAsync<IOException>(async () => await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, ledger, new(), Ct));
        Assert.Same(failure, actual);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task Build_EmptyTableAppendsNothingAndCancellationCannotReturnAResult()
    {
        var source = new ImagePages(DatabaseFormat.Jet4Mdb, 4);
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(3L * source.PageSize);
        await using var ledger = new MemoryStream();
        CompactRawDataBuilderResult result = await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, ledger, new(), Ct);
        Assert.Equal(0, result.RowCount);
        Assert.Equal(0, result.PageCount);
        Assert.Equal(0, ledger.Length);
        Assert.Equal(3L * source.PageSize, destination.Length);
        source.SetRows(3, (0, new byte[] { 1 }));
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await using var cancelling = new CancellingStream(cancellation);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactRawDataBuilder.BuildAsync(session, source.Format, 2, 2, destination, cancelling, new(), cancellation.Token));
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task Build_DifferentFormatProfileRefusesBeforeReadingSource()
    {
        var source = new ImagePages(DatabaseFormat.Jet4Mdb, 4);
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(3L * source.PageSize);
        await using var ledger = new MemoryStream();
        await Assert.ThrowsAsync<ArgumentException>(async () => await CompactRawDataBuilder.BuildAsync(session, JetFormat.ForNewDatabase(DatabaseFormat.Jet3Mdb), 2, 2, destination, ledger, new(), Ct));
        Assert.Equal(0, source.Reads);
    }

    private static uint ReadMap(byte[] bytes, int offset) => BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(offset, 4));

    private static async IAsyncEnumerable<IndexEntry> RelocateAsync(CompactRowRelocationMap relocation, [EnumeratorCancellation] CancellationToken token)
    {
        foreach (IndexEntry entry in (IndexEntry[])[new([1], 3, 0), new([1], 3, 1), new([2], 7, 0)])
        {
            yield return await relocation.RelocateAsync(entry, token);
        }
    }

    private sealed class FlushFailureStream(IOException failure) : MemoryStream
    {
        public override Task FlushAsync(CancellationToken cancellationToken) => Task.FromException(failure);
    }

    private sealed class CancellingStream(CancellationTokenSource cancellation) : MemoryStream
    {
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await base.WriteAsync(buffer, cancellationToken);
            await cancellation.CancelAsync();
        }
    }

    private sealed class ImagePages : IPageSource
    {
        internal ImagePages(DatabaseFormat kind, int count)
        {
            this.Format = JetFormat.ForNewDatabase(kind);
            this.Pages = new byte[count][];
            for (int i = 0; i < count; i++)
            {
                this.Pages[i] = new byte[this.PageSize];
            }
        }

        internal ImagePages(DatabaseFormat kind, byte[] bytes)
            : this(kind, bytes.Length / JetFormat.ForNewDatabase(kind).PageSize)
        {
            for (int i = 0; i < this.Pages.Length; i++)
            {
                bytes.AsSpan(i * this.PageSize, this.PageSize).CopyTo(this.Pages[i]);
            }
        }

        public int PageSize => this.Format.PageSize;

        public long PageCount => this.Pages.Length;

        public bool IsDisposed => false;

        internal JetFormat Format { get; }

        internal byte[][] Pages { get; }

        internal int Reads { get; private set; }

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            this.Reads++;
            byte[] rented = ArrayPool<byte>.Shared.Rent(this.PageSize);
            this.Pages[checked((int)pageNumber)].CopyTo(rented, 0);
            return ValueTask.FromResult(rented);
        }

        public void ThrowIfDisposed()
        {
        }

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => cancellationToken.ThrowIfCancellationRequested();

        internal void SetRows(int number, params (int Flags, byte[] Bytes)[] rows)
        {
            byte[] page = this.Pages[number];
            page[0] = 1;
            BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(this.Format.DataPage.TDefOff), 2);
            BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(this.Format.DataPage.NumRows), checked((ushort)rows.Length));
            int start = this.PageSize;
            for (int i = 0; i < rows.Length; i++)
            {
                start -= rows[i].Bytes.Length;
                rows[i].Bytes.CopyTo(page, start);
                BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(this.Format.DataPage.RowsStart + (i * 2)), checked((ushort)(start | rows[i].Flags)));
            }
        }
    }
}
