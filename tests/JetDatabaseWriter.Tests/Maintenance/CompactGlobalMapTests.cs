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
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using Xunit;

/// <summary>Global maps pin original storage and account for every appended bitmap through reopened comparison.</summary>
public sealed class CompactGlobalMapTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, 8)]
    [InlineData(DatabaseFormat.Jet4Mdb, 8)]
    [InlineData(DatabaseFormat.AceAccdb, 8)]
    [InlineData(DatabaseFormat.Jet3Mdb, 1000)]
    [InlineData(DatabaseFormat.Jet4Mdb, 520)]
    [InlineData(DatabaseFormat.AceAccdb, 520)]
    public async Task ReopenedMap_PinsEveryPagePreservesSurroundAndAllowsAppend(DatabaseFormat kind, int appendPoint)
    {
        var format = JetFormat.ForNewDatabase(kind);
        byte[] original = Image(format);
        var source = new Source(original, format.PageSize);
        await using var destination = new MemoryStream();
        await destination.WriteAsync(original, Ct);
        destination.SetLength((long)appendPoint * format.PageSize);
        CompactGlobalMapResult installed = await CompactGlobalMapInstaller.InstallAsync(source, format, appendPoint, destination, new(), Ct);
        byte[] bytes = destination.ToArray();
        await using var reopened = new MemoryStream(bytes, writable: false);
        CompactGlobalMapResult verified = await CompactGlobalMapValidator.ValidateAsync(source, format, appendPoint, reopened, new(), Ct);
        Assert.Equal(installed.DestinationPages, verified.DestinationPages);
        Assert.Equal(appendPoint <= (format.UsageMapRowSize - 5) * 8 ? 0 : 1, verified.BitmapPages);
        Assert.False(verified.DatabaseValidated);
        Assert.False(verified.ReclamationAuthorized);
        Assert.Equal(0, verified.WrittenBytes);
        Assert.Equal(original.AsSpan(0, format.PageSize).ToArray(), bytes.AsSpan(0, format.PageSize).ToArray());
        Assert.Equal(original.AsSpan(2 * format.PageSize).ToArray(), bytes.AsSpan(2 * format.PageSize, 6 * format.PageSize).ToArray());

        // An ordinary allocator must treat every page as used, including former source free pages and its own bitmaps.
        await using var allocationStream = new MemoryStream();
        await allocationStream.WriteAsync(bytes, Ct);
#pragma warning disable CA2000 // The awaited pager owns its codec.
        await using var pager = new Pager(allocationStream, format.PageSize, new NoPageCodec(), true, typeof(AccessWriter), 0);
#pragma warning restore CA2000
        var allocator = new PageAllocator(format, pager, new AccessWriterOptions());
        Assert.Equal(verified.DestinationPages, await allocator.ReserveContiguousPagesAsync(1, Ct));
        Assert.Equal(original, source.Bytes);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task BitmapCoverage_IncludesItsOwnBoundaryCrossingAllocation(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        int appendPoint = UsageMap.PagesPerReferenceMapPage(format.PageSize);
        var source = new Source(Image(format), format.PageSize);
        await using var destination = new MemoryStream();
        await destination.WriteAsync(source.Bytes, Ct);
        destination.SetLength((long)appendPoint * format.PageSize);
        CompactGlobalMapResult installed = await CompactGlobalMapInstaller.InstallAsync(source, format, appendPoint, destination, new(), Ct);
        Assert.Equal(2, installed.BitmapPages);
        Assert.Equal(appendPoint + 2, installed.DestinationPages);
        await using var reopened = new MemoryStream(destination.ToArray(), writable: false);
        CompactGlobalMapResult verified = await CompactGlobalMapValidator.ValidateAsync(source, format, appendPoint, reopened, new(), Ct);
        await using var ledger = new MemoryStream();
        CompactGlobalAllocationBuilderResult allocation = await CompactGlobalAllocationBuilder.BuildAsync(format, source.PageCount, RecordsAsync(appendPoint, installed.BitmapPages, Ct), reopened, ledger, new(), Ct);
        await using var reopenedLedger = new MemoryStream(ledger.ToArray(), writable: false);
        CompactGlobalAllocationValidatorResult accounting = await CompactGlobalAllocationValidator.ValidateAsync(format, source.PageCount, RecordsAsync(appendPoint, installed.BitmapPages, Ct), reopened, reopenedLedger, allocation, new(), Ct);
        Assert.Equal(verified.DestinationPages, accounting.DestinationPages);
    }

    [Theory]
    [InlineData("live-free")]
    [InlineData("future-used")]
    [InlineData("surround")]
    [InlineData("row-geometry")]
    [InlineData("pointer")]
    [InlineData("unused-pointer")]
    [InlineData("bitmap-header")]
    [InlineData("bitmap-padding")]
    [InlineData("bitmap-free")]
    [InlineData("extra-page")]
    [InlineData("wrong-append")]
    public async Task ReopenedForgery_Refuses(string fault)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new Source(Image(format), format.PageSize);
        await using var destination = new MemoryStream();
        await destination.WriteAsync(source.Bytes, Ct);
        destination.SetLength(520L * format.PageSize);
        CompactGlobalMapResult result = await CompactGlobalMapInstaller.InstallAsync(source, format, 520, destination, new(), Ct);
        byte[] bytes = destination.ToArray();
        int row = format.PageSize + result.MapRowStart;
        int bitmap = 520 * format.PageSize;
        switch (fault)
        {
            case "live-free":
                bytes[bitmap + Constants.UsageMap.ReferenceMapBitmapOffset] = 4;
                break;
            case "future-used":
                bytes[bitmap + Constants.UsageMap.ReferenceMapBitmapOffset + 70] = 0;
                break;
            case "surround":
                bytes[format.PageSize + 8] ^= 1;
                break;
            case "row-geometry":
                bytes[format.PageSize + format.DataPage.RowsStart] ^= 1;
                break;
            case "pointer":
                BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(row + 1), 2);
                break;
            case "unused-pointer":
                BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(row + 5), 2);
                break;
            case "bitmap-header":
                bytes[bitmap + 1] = 0;
                break;
            case "bitmap-padding":
                bytes[bitmap + 2] = 1;
                break;
            case "bitmap-free":
                bytes[bitmap + Constants.UsageMap.ReferenceMapBitmapOffset + (520 / 8)] |= 1;
                break;
            case "extra-page":
                Array.Resize(ref bytes, bytes.Length + format.PageSize);
                break;
            case "wrong-append":
                break;
            default:
                throw new InvalidOperationException();
        }

        await using var reopened = new MemoryStream(bytes, writable: false);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactGlobalMapValidator.ValidateAsync(source, format, fault == "wrong-append" ? 519 : 520, reopened, new(), Ct));
    }

    [Theory]
    [InlineData("header")]
    [InlineData("short")]
    [InlineData("type")]
    [InlineData("inline-base")]
    [InlineData("private-differs")]
    [InlineData("truncated-directory")]
    [InlineData("aliased-row")]
    [InlineData("other-outside")]
    [InlineData("other-in-directory")]
    [InlineData("nonlive-row0")]
    public async Task InvalidSourceOrPrivatePageOne_RefusesBeforeWriting(string fault)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        byte[] original = Image(format);
        int row = format.PageSize + format.PageSize - format.UsageMapRowSize;
        switch (fault)
        {
            case "header":
                original[format.PageSize] = 0;
                break;
            case "short":
                BinaryPrimitives.WriteUInt16LittleEndian(original.AsSpan(format.PageSize + format.DataPage.RowsStart), checked((ushort)(format.PageSize - 4)));
                break;
            case "type":
                original[row] = 2;
                break;
            case "inline-base":
                original[row + 1] = 1;
                break;
            case "private-differs":
                break;
            case "truncated-directory":
                BinaryPrimitives.WriteUInt16LittleEndian(original.AsSpan(format.PageSize + format.DataPage.NumRows), ushort.MaxValue);
                break;
            case "aliased-row":
            case "other-outside":
            case "other-in-directory":
                BinaryPrimitives.WriteUInt16LittleEndian(original.AsSpan(format.PageSize + format.DataPage.NumRows), 2);
                ushort second = fault switch
                {
                    "aliased-row" => checked((ushort)(row - format.PageSize)),
                    "other-outside" => ushort.MaxValue,
                    _ => 2,
                };
                BinaryPrimitives.WriteUInt16LittleEndian(original.AsSpan(format.PageSize + format.DataPage.RowsStart + 2), second);
                break;
            case "nonlive-row0":
                original[format.PageSize + format.DataPage.RowsStart + 1] |= 0x80;
                break;
            default:
                throw new InvalidOperationException();
        }

        var source = new Source(original, format.PageSize);
        await using var destination = new MemoryStream();
        await destination.WriteAsync(original, Ct);
        if (fault == "private-differs")
        {
            destination.Position = format.PageSize + 8;
            destination.WriteByte(1);
        }

        byte[] before = destination.ToArray();
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactGlobalMapInstaller.InstallAsync(source, format, 8, destination, new(), Ct));
        Assert.Equal(before, destination.ToArray());
    }

    [Theory]
    [InlineData("pages")]
    [InlineData("read")]
    [InlineData("write")]
    [InlineData("working")]
    [InlineData("bitmap")]
    public async Task InstallerAdmission_RefusesBeforeWriting(string limit)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new Source(Image(format), format.PageSize);
        await using var destination = new MemoryStream();
        await destination.WriteAsync(source.Bytes, Ct);
        destination.SetLength(520L * format.PageSize);
        byte[] before = destination.ToArray();
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactGlobalMapInstaller.InstallAsync(source, format, 520, destination, Budget(limit), Ct));
        Assert.Equal(before, destination.ToArray());
    }

    [Fact]
    public async Task PartialReads_AreAdmittedBeforeEveryAttemptAndRealFailuresPropagate()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new Source(Image(format), format.PageSize);
        await using var destination = new MemoryStream();
        await destination.WriteAsync(source.Bytes, Ct);
        await CompactGlobalMapInstaller.InstallAsync(source, format, 8, destination, new(), Ct);
        await using var reopened = new ReadProbe(destination.ToArray(), 97);
        CompactGlobalMapResult result = await CompactGlobalMapValidator.ValidateAsync(source, format, 8, reopened, new(), Ct);
        Assert.Equal(format.PageSize + reopened.RequestedBytes, result.ReadBytes);
        await using var limited = new ReadProbe(destination.ToArray(), 97);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactGlobalMapValidator.ValidateAsync(source, format, 8, limited, new() { MaxReadBytes = 2L * format.PageSize }, Ct));
        Assert.True(format.PageSize + limited.RequestedBytes <= 2L * format.PageSize);
        await using var failed = new ReadProbe(destination.ToArray(), 0);
        await Assert.ThrowsAsync<IOException>(async () => await CompactGlobalMapValidator.ValidateAsync(source, format, 8, failed, new(), Ct));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WriteOrFlushFailure_ReturnsNoCompletion(bool flush)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new Source(Image(format), format.PageSize);
        await using var destination = new WriteFailure(source.Bytes, flush);
        await Assert.ThrowsAsync<IOException>(async () => await CompactGlobalMapInstaller.InstallAsync(source, format, 8, destination, new(), Ct));
    }

    [Fact]
    public async Task Cancellation_RefusesBeforeWriting()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new Source(Image(format), format.PageSize);
        await using var destination = new MemoryStream(source.Bytes.ToArray());
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactGlobalMapInstaller.InstallAsync(source, format, 8, destination, new(), cancellation.Token));
        Assert.Equal(source.Bytes, destination.ToArray());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OtherPageOneRowsAndOldReferenceStorage_RemainExact(bool reference)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        byte[] bytes = Image(format);
        int first = (2 * format.PageSize) - format.UsageMapRowSize;
        int second = first - 32;
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(format.PageSize + format.DataPage.NumRows), 2);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(format.PageSize + format.DataPage.RowsStart + 2), checked((ushort)(second - format.PageSize)));
        bytes.AsSpan(second, 32).Fill(0xA5);
        if (reference)
        {
            bytes.AsSpan(first, format.UsageMapRowSize).Clear();
            bytes[first] = Constants.UsageMap.ReferenceMapType;
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(first + 1), 2);
            bytes[2 * format.PageSize] = Constants.PageTypes.UsageMap;
            bytes[(2 * format.PageSize) + 1] = 1;
        }

        var source = new Source(bytes, format.PageSize);
        await using var destination = new MemoryStream();
        await destination.WriteAsync(bytes, Ct);
        destination.SetLength(1000L * format.PageSize);
        await CompactGlobalMapInstaller.InstallAsync(source, format, 1000, destination, new(), Ct);
        await using var reopened = new MemoryStream(destination.ToArray(), writable: false);
        await CompactGlobalMapValidator.ValidateAsync(source, format, 1000, reopened, new() { MaxWriteBytes = 0 }, Ct);
        Assert.Equal(bytes.AsSpan(second, 32).ToArray(), destination.ToArray().AsSpan(second, 32).ToArray());
        Assert.Equal(bytes.AsSpan(2 * format.PageSize).ToArray(), destination.ToArray().AsSpan(2 * format.PageSize, 6 * format.PageSize).ToArray());
    }

    private static CompactGlobalMapBudget Budget(string limit) => limit switch
    {
        "pages" => new() { MaxPages = 520 },
        "read" => new() { MaxReadBytes = 1 },
        "write" => new() { MaxWriteBytes = 1 },
        "working" => new() { MaxWorkingBytes = 1 },
        "bitmap" => new() { MaxBitmapPages = 0 },
        _ => throw new InvalidOperationException(),
    };

    private static byte[] Image(JetFormat format)
    {
        byte[] bytes = new byte[8 * format.PageSize];
        int page = format.PageSize;
        int start = format.PageSize - format.UsageMapRowSize;
        bytes[page] = Constants.PageTypes.Data;
        bytes[page + 1] = 1;
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(page + 2), checked((ushort)(start - format.DataPage.RowsStart - 2)));
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(page + format.DataPage.NumRows), 1);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(page + format.DataPage.RowsStart), checked((ushort)start));
        bytes.AsSpan(page + start + 5, format.UsageMapRowSize - 5).Fill(byte.MaxValue);
        return bytes;
    }

    private static async IAsyncEnumerable<CompactGlobalAllocationRecord> RecordsAsync(int appendPoint, long bitmapPages, [EnumeratorCancellation] CancellationToken token)
    {
        await Task.CompletedTask;
        token.ThrowIfCancellationRequested();
        yield return new(CompactGlobalAllocationKind.Data, 2, new(8, appendPoint - 8));
        yield return new(CompactGlobalAllocationKind.UsageMap, 1, new(appendPoint, bitmapPages));
    }

    private sealed class Source(byte[] bytes, int pageSize) : IPageSource
    {
        internal byte[] Bytes => bytes;

        public int PageSize => pageSize;

        public long PageCount => bytes.Length / pageSize;

        public bool IsDisposed => false;

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] buffer = ArrayPool<byte>.Shared.Rent(pageSize);
            bytes.AsSpan(checked((int)pageNumber * pageSize), pageSize).CopyTo(buffer);
            return new(buffer);
        }

        public void ThrowIfDisposed()
        {
        }

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => cancellationToken.ThrowIfCancellationRequested();
    }

    private sealed class ReadProbe(byte[] bytes, int maximum) : MemoryStream(bytes, writable: false)
    {
        internal long RequestedBytes { get; private set; }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            this.RequestedBytes += buffer.Length;
            return maximum == 0 ? throw new IOException("Map read failure.") : base.ReadAsync(buffer[..Math.Min(maximum, buffer.Length)], cancellationToken);
        }
    }

    private sealed class WriteFailure(byte[] bytes, bool flush) : MemoryStream(bytes.ToArray())
    {
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            => flush ? base.WriteAsync(buffer, cancellationToken) : throw new IOException("Map write failure.");

        public override Task FlushAsync(CancellationToken cancellationToken)
            => flush ? throw new IOException("Map flush failure.") : base.FlushAsync(cancellationToken);
    }
}
