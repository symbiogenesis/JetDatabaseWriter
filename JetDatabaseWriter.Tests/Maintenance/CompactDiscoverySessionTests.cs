namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using Xunit;

public sealed class CompactDiscoverySessionTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("rows")]
    [InlineData("slots")]
    [InlineData("reads")]
    [InlineData("objects")]
    public async Task RepeatedTables_ConsumeOneAllowance(string limit)
    {
        var source = new MemoryPages();
        source.AddRow(3, 2, [1, 2, 3]);
        source.AddRow(4, 1, [4, 5]);
        CompactDiscoveryBudget budget = limit switch
        {
            "rows" => new() { MaxRows = 1 },
            "slots" => new() { MaxDirectorySlots = 1 },
            "reads" => new() { MaxPageReads = 2 },
            _ => new() { MaxObjects = 1 },
        };
        var session = new CompactDiscoverySession(source, source.Format, budget);
        await session.ScanRowsAsync(2, IgnoreRow, Ct);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await session.ScanRowsAsync(1, IgnoreRow, Ct));
        Assert.Equal(1, session.Accounting.Rows);
    }

    [Theory]
    [InlineData("pages")]
    [InlineData("entries")]
    [InlineData("canonical")]
    [InlineData("references")]
    [InlineData("graph")]
    [InlineData("records")]
    public async Task RepeatedRoots_ConsumeOneIndexAllowance(string limit)
    {
        var source = new MemoryPages();
        source.Images[3] = IndexPageCodec.BuildLeafPage(source.Format.IndexPage, source.PageSize, 2, [new([1], 4, 0)], enablePrefixCompression: false);
        source.Images[4] = IndexPageCodec.BuildLeafPage(source.Format.IndexPage, source.PageSize, 2, [new([2], 3, 0)], enablePrefixCompression: false);
        CompactDiscoveryBudget budget = limit switch
        {
            "pages" => new() { MaxIndexPages = 1 },
            "entries" => new() { MaxIndexEntries = 1 },
            "canonical" => new() { MaxIndexCanonicalBytes = 5 },
            "references" => new() { MaxReferences = 1 },
            "graph" => new() { MaxGraphSteps = 4 },
            _ => new() { MaxRecords = 3 },
        };
        var session = new CompactDiscoverySession(source, source.Format, budget);
        await session.ScanIndexAsync(2, 3, IgnoreIndex, Ct);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await session.ScanIndexAsync(2, 4, IgnoreIndex, Ct));
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RepeatedRawCopies_ConsumeOnePayloadAndRowAllowance(bool rows)
    {
        var source = new MemoryPages();
        source.AddLongValue(3, [0xFF, 0x80, 0]);
        byte[] descriptor = LongValueDescriptor.SinglePage(3, LongValueStore.MakeRowPointer(3, 0), 17).ToHeaderBytes();
        CompactDiscoveryBudget budget = rows ? new() { MaxLongValueRows = 1 } : new() { MaxLongValuePayloadBytes = 5 };
        var session = new CompactDiscoverySession(source, source.Format, budget);
        await using var destination = new MemoryStream();
        await session.CopyLongValueAsync(descriptor, destination, Ct);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await session.CopyLongValueAsync(descriptor, destination, Ct));
        Assert.Equal(new byte[] { 0xFF, 0x80, 0 }, destination.ToArray());
        Assert.Equal(1, source.Reads);
        Assert.Equal(1, session.Accounting.LongValueRows);
    }

    [Fact]
    public async Task RowVisitorCopyAndIndex_SharePageReadAllowance()
    {
        var source = new MemoryPages();
        source.AddLongValue(3, [3, 2, 1]);
        byte[] descriptor = LongValueDescriptor.SinglePage(3, LongValueStore.MakeRowPointer(3, 0), 17).ToHeaderBytes();
        source.AddRow(4, 2, descriptor);
        var session = new CompactDiscoverySession(source, source.Format, new() { MaxPageReads = 3 });
        await using var destination = new MemoryStream();
        await session.ScanRowsAsync(2, async (_, bytes, token) => await session.CopyLongValueAsync(bytes, destination, token), Ct);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await session.ScanIndexAsync(2, 4, IgnoreIndex, Ct));
        Assert.Equal(3, source.Reads);
        Assert.Equal(new byte[] { 3, 2, 1 }, destination.ToArray());
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task CancellationAndSourceIo_KeepTheirOriginalExceptions()
    {
        var source = new MemoryPages();
        source.AddRow(3, 2, [1]);
        var session = new CompactDiscoverySession(source, source.Format, new());
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await session.ScanRowsAsync(2, async (_, _, _) => await cancellation.CancelAsync(), cancellation.Token));
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
        var failure = new IOException("physical read failed");
        source.Failure = failure;
        IOException actual = await Assert.ThrowsAsync<IOException>(async () => await session.ScanRowsAsync(2, IgnoreRow, Ct));
        Assert.Same(failure, actual);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData("bytes")]
    [InlineData("rows")]
    [InlineData("records")]
    public async Task RepeatedLongValueAudits_ConsumeOneAllowance(string limit)
    {
        var source = new MemoryPages();
        source.AddLongValue(3, [1, 2, 3]);
        byte[] column = LongValueDescriptor.SinglePage(3, LongValueStore.MakeRowPointer(3, 0), 17).ToHeaderBytes();
        CompactDiscoveryBudget budget = limit switch
        {
            "bytes" => new() { MaxLongValueBytes = 15 },
            "rows" => new() { MaxLongValueRows = 1 },
            _ => new() { MaxRecords = 4 },
        };
        var session = new CompactDiscoverySession(source, source.Format, budget);
        await session.AuditLongValueAsync(column, IgnoreLongValue, Ct);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await session.AuditLongValueAsync(column, IgnoreLongValue, Ct));
        Assert.Equal(1, source.Reads);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NestedValueAdmission_AccountsForBorrowedRowStorage(bool audit)
    {
        var source = new MemoryPages();
        byte[] column = LongValueStore.WrapInlineLongValue([9, 8, 7])!;
        source.AddRow(3, 2, column);
        var budget = new CompactDiscoveryBudget { MaxWorkingBytes = 150_000, MaxChainRows = 1 };
        var standalone = new CompactDiscoverySession(source, source.Format, budget);
        await using var destination = new MemoryStream();
        if (audit)
        {
            await standalone.AuditLongValueAsync(column, IgnoreLongValue, Ct);
        }
        else
        {
            await standalone.CopyLongValueAsync(column, destination, Ct);
        }

        int observations = 0;
        var session = new CompactDiscoverySession(source, source.Format, budget);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await session.ScanRowsAsync(
            2,
            async (_, bytes, token) =>
            {
                if (audit)
                {
                    await session.AuditLongValueAsync(
                        bytes,
                        (_, _) =>
                        {
                            observations++;
                            return ValueTask.CompletedTask;
                        },
                        token);
                }
                else
                {
                    await session.CopyLongValueAsync(bytes, destination, token);
                }
            },
            Ct));
        Assert.Equal(1, source.Reads);
        Assert.Equal(0, observations);
        byte[] expected = audit ? [] : [9, 8, 7];
        Assert.Equal(expected, destination.ToArray());
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PhysicalAndStorageAdmission_RefuseBeforeIo(bool physical)
    {
        var source = new MemoryPages();
        if (physical)
        {
            Assert.Throws<JetLimitationException>(() => new CompactDiscoverySession(source, source.Format, new() { MaxPages = 4 }));
        }
        else
        {
            var session = new CompactDiscoverySession(source, source.Format, new() { MaxWorkingBytes = 1 });
            await Assert.ThrowsAsync<JetLimitationException>(async () => await session.ScanRowsAsync(2, IgnoreRow, Ct));
            Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
        }

        Assert.Equal(0, source.Reads);
    }

    [Fact]
    public void ChangingGeometryDuringAdmission_IsRefusedBeforeIo()
    {
        var source = new MemoryPages { ChangeCountOnPageSizeRead = true };
        Assert.Throws<JetCorruptDataException>(() => new CompactDiscoverySession(source, source.Format, new() { MaxPages = 5 }));
        Assert.Equal(0, source.Reads);
    }

    [Fact]
    public async Task AuditThenCopy_ConsumeOneReferenceAllowance()
    {
        var source = new MemoryPages();
        source.AddLongValue(3, [1]);
        byte[] column = LongValueDescriptor.SinglePage(1, LongValueStore.MakeRowPointer(3, 0), 17).ToHeaderBytes();
        var session = new CompactDiscoverySession(source, source.Format, new() { MaxReferences = 1 });
        await session.AuditLongValueAsync(column, IgnoreLongValue, Ct);
        await using var destination = new MemoryStream();
        await Assert.ThrowsAsync<JetLimitationException>(async () => await session.CopyLongValueAsync(column, destination, Ct));
        Assert.Empty(destination.ToArray());
        Assert.Equal(1, source.Reads);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OverflowEdges_ConsumeSharedGraphAndReferenceAllowances(bool references)
    {
        var source = new MemoryPages();
        source.AddSlots(3, 2, (0x4000, new byte[] { 0, 4, 0, 0 }), (0x8000, new byte[] { 7 }));
        source.AddSlots(4, 2, (0xC000, new byte[] { 1, 3, 0, 0 }));
        CompactDiscoveryBudget budget = references ? new() { MaxReferences = 1 } : new() { MaxGraphSteps = 1 };
        var session = new CompactDiscoverySession(source, source.Format, budget);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await session.ScanRowsAsync(2, IgnoreRow, Ct));
        Assert.Equal(2, source.Reads);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task DestinationIoFailure_IsPreservedAndReleasesAdmission()
    {
        var source = new MemoryPages();
        source.AddLongValue(3, [1]);
        byte[] column = LongValueDescriptor.SinglePage(1, LongValueStore.MakeRowPointer(3, 0), 17).ToHeaderBytes();
        var session = new CompactDiscoverySession(source, source.Format, new());
        var failure = new IOException("destination write failed");
        await using var destination = new FailingDestination(failure);
        IOException actual = await Assert.ThrowsAsync<IOException>(async () => await session.CopyLongValueAsync(column, destination, Ct));
        Assert.Same(failure, actual);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
        await using var retry = new MemoryStream();
        await session.CopyLongValueAsync(column, retry, Ct);
        Assert.Equal(new byte[] { 1 }, retry.ToArray());
    }

    [Fact]
    public async Task RawCopyRefusesOrdinaryTablePayloadBeforeWriting()
    {
        var source = new MemoryPages();
        source.AddRow(3, 2, [0xFE]);
        byte[] column = LongValueDescriptor.SinglePage(1, LongValueStore.MakeRowPointer(3, 0), 17).ToHeaderBytes();
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await session.CopyLongValueAsync(column, destination, Ct));
        Assert.Empty(destination.ToArray());
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    private static ValueTask IgnoreRow(RowLocation location, ReadOnlyMemory<byte> bytes, CancellationToken cancellationToken)
        => ValueTask.CompletedTask;

    private static ValueTask IgnoreIndex(CompactIndexRecord record, CancellationToken cancellationToken)
        => ValueTask.CompletedTask;

    private static ValueTask IgnoreLongValue(CompactLongValueRecord record, CancellationToken cancellationToken)
        => ValueTask.CompletedTask;

    private sealed class MemoryPages : IPageSource
    {
        private int PageSizeReads { get; set; }

        internal JetFormat Format { get; } = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);

        internal Dictionary<long, byte[]> Images { get; } = [];

        internal int Reads { get; private set; }

        internal Exception? Failure { get; set; }

        internal bool ChangeCountOnPageSizeRead { get; init; }

        public int PageSize
        {
            get
            {
                if (this.ChangeCountOnPageSizeRead && ++this.PageSizeReads == 2)
                {
                    this.PageCount++;
                }

                return this.Format.PageSize;
            }
        }

        public long PageCount { get; private set; } = 5;

        public bool IsDisposed => false;

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            this.Reads++;
            if (this.Failure is not null)
            {
                throw this.Failure;
            }

            byte[] result = ArrayPool<byte>.Shared.Rent(this.PageSize);
            Array.Clear(result, 0, this.PageSize);
            if (this.Images.TryGetValue(pageNumber, out byte[]? image))
            {
                image.AsSpan(0, this.PageSize).CopyTo(result);
            }

            return ValueTask.FromResult(result);
        }

        public void ThrowIfDisposed()
        {
        }

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => cancellationToken.ThrowIfCancellationRequested();

        internal void AddRow(long page, int owner, byte[] bytes) => this.AddSlots(page, owner, (0, bytes));

        internal void AddLongValue(long page, byte[] bytes)
        {
            byte[] buffer = LongValueStore.BuildSinglePageBuffer(bytes, 17, this.PageSize, this.Format.LvalPage);
            try
            {
                this.Images[page] = buffer.AsSpan(0, this.PageSize).ToArray();
            }
            finally
            {
                PageBuffers.Return(buffer);
            }
        }

        internal void AddSlots(long page, int owner, params (int Flags, byte[] Bytes)[] slots)
        {
            byte[] image = new byte[this.PageSize];
            image[0] = Constants.PageTypes.Data;
            BinaryPrimitives.WriteInt32LittleEndian(image.AsSpan(this.Format.DataPage.TDefOff), owner);
            BinaryPrimitives.WriteUInt16LittleEndian(image.AsSpan(this.Format.DataPage.NumRows), checked((ushort)slots.Length));
            int start = this.PageSize;
            for (int index = 0; index < slots.Length; index++)
            {
                (int flags, byte[] bytes) = slots[index];
                start -= bytes.Length;
                BinaryPrimitives.WriteUInt16LittleEndian(image.AsSpan(this.Format.DataPage.RowsStart + (index * 2)), checked((ushort)(start | flags)));
                bytes.CopyTo(image, start);
            }

            this.Images[page] = image;
        }
    }

    private sealed class FailingDestination(Exception failure) : MemoryStream
    {
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            => ValueTask.FromException(failure);
    }
}
