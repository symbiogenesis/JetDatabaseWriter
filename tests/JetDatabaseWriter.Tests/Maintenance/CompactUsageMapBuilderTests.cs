namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Private allocation maps retain exact page membership with bounded native bitmap storage.</summary>
public sealed class CompactUsageMapBuilderTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task InlineRuns_RoundTripAndRetainAnIndependentNativePointer(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        await using var destination = new AppendDestination(format, 1300);
        CompactPageRun[] runs = [new(1200, 3), new(1205, 2)];
        CompactUsageMapBuilderResult result = await BuildAsync(format, runs, destination);
        Assert.Equal(new UsageMapPointer(0, 1300), result.Pointer);
        Assert.Equal(1, result.PageCount);
        Assert.Equal(5, result.ListedPages);
        Assert.Equal(2, result.InputRuns);
        Assert.False(result.IsValidated);
        byte[] allocation = destination.Bytes;
        Assert.Equal(0, allocation[format.PageSize - format.UsageMapRowSize]);
        Assert.Equal(1200, BinaryPrimitives.ReadInt32LittleEndian(allocation.AsSpan(format.PageSize - format.UsageMapRowSize + 1)));
        Assert.Equal<long>([1200, 1201, 1202, 1205, 1206], await EnumerateAsync(format, destination, result));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ScatteredRuns_PromoteAndCrossReferenceWindowsWithoutListingMapStorage(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        int window = (format.PageSize - 4) * 8;
        await using var destination = new AppendDestination(format, (window * 3) + 20);
        CompactPageRun[] runs = [new(3, 2), new(window - 1, 3), new((window * 3) + 4, 2)];
        CompactUsageMapBuilderResult result = await BuildAsync(format, runs, destination);
        Assert.Equal(4, result.PageCount);
        Assert.Equal(7, result.ListedPages);
        Assert.Equal(1, destination.Bytes[format.PageSize - format.UsageMapRowSize]);
        Assert.Equal<long>([3, 4, window - 1, window, window + 1, (window * 3) + 4, (window * 3) + 5], await EnumerateAsync(format, destination, result));
        Assert.Equal(result.PageCount * format.PageSize, destination.Bytes.LongLength);
    }

    [Fact]
    public async Task EmptyInput_EmitsAnEmptyInlineMap()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        await using var destination = new AppendDestination(format, 3);
        CompactUsageMapBuilderResult result = await BuildAsync(format, [], destination);
        Assert.Equal(1, result.PageCount);
        Assert.Equal(0, result.ListedPages);
        Assert.Empty(await EnumerateAsync(format, destination, result));
    }

    [Theory]
    [InlineData("runs")]
    [InlineData("listed")]
    [InlineData("pages")]
    [InlineData("output")]
    [InlineData("working")]
    public async Task Limits_RefuseWithoutReturningCompletedMaps(string limit)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        CompactUsageMapBuilderBudget budget = limit switch
        {
            "runs" => new() { MaxRuns = 1 },
            "listed" => new() { MaxListedPages = 1 },
            "pages" => new() { MaxPages = 1 },
            "output" => new() { MaxOutputBytes = 1 },
            "working" => new() { MaxWorkingBytes = 1 },
            _ => throw new InvalidOperationException(),
        };
        await using var destination = new AppendDestination(format, 1000);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await BuildAsync(format, [new(3, 2), new(900, 1)], destination, budget));
    }

    [Theory]
    [InlineData(0, 1, 3, 1)]
    [InlineData(3, 0, 4, 1)]
    [InlineData(3, -1, 4, 1)]
    [InlineData(3, 2, 4, 1)]
    [InlineData(4, 1, 3, 1)]
    [InlineData(999, 2, 3, 1)]
    [InlineData(long.MaxValue, long.MaxValue, 3, 1)]
    public async Task InvalidOrOverlappingRanges_RefuseWithoutTouchingPrefix(long first, long count, long second, long secondCount)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        await using var destination = new AppendDestination(format, 1000);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await BuildAsync(format, [new(first, count), new(second, secondCount)], destination));
    }

    [Fact]
    public async Task ReferenceCapacity_RefusesRatherThanTruncatingHighPages()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        long capacity = UsageMap.ReferencePointerCount(format.UsageMapRowSize) * (long)UsageMap.PagesPerReferenceMapPage(format.PageSize);
        await using var destination = new AppendDestination(format, capacity + 1);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await BuildAsync(format, [new(3, 1), new(capacity, 1)], destination));
    }

    [Fact]
    public async Task CancelledBeforeAdmission_LeavesDestinationEmpty()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        await using var destination = new AppendDestination(format, 3);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactUsageMapBuilder.BuildAsync(format, InputAsync([], Ct), destination, new(), cancellation.Token));
        Assert.Empty(destination.Bytes);
    }

    [Fact]
    public async Task InlinePromotionAcrossTwoReferenceWindows_PreservesEveryPreviouslyBufferedBit()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        int window = UsageMap.PagesPerReferenceMapPage(format.PageSize);
        await using var destination = new AppendDestination(format, window + 700);
        CompactUsageMapBuilderResult result = await BuildAsync(format, [new(window - 8, 16), new(window + 600, 1)], destination);
        Assert.Equal(3, result.PageCount);
        var expected = new List<long>();
        for (int page = window - 8; page < window + 8; page++)
        {
            expected.Add(page);
        }

        expected.Add(window + 600);
        Assert.Equal(expected, await EnumerateAsync(format, destination, result));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CancellationDuringFinalWriteOrFlush_PreventsCompletion(bool duringFlush)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        using var cancellation = new CancellationTokenSource();
        await using var destination = new AppendDestination(format, 3, cancellation, duringFlush);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactUsageMapBuilder.BuildAsync(format, InputAsync([], Ct), destination, new(), cancellation.Token));
        Assert.Equal(format.PageSize, destination.Bytes.Length);
    }

    [Theory]
    [InlineData("first-write")]
    [InlineData("final-write")]
    [InlineData("partial-final-write")]
    [InlineData("flush")]
    public async Task DestinationFailures_PropagateAndRetainTheExistingPrefix(string fault)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        await using var destination = new WriteFaultStream();
        destination.SetLength(3L * format.PageSize);
        switch (fault)
        {
            case "first-write":
                destination.FailOnWrite(1);
                break;
            case "final-write":
                destination.FailOnWrite(2);
                break;
            case "partial-final-write":
                destination.FailDuringWrite(2);
                break;
            case "flush":
                destination.FailOnFlush(1);
                break;
            default:
                throw new InvalidOperationException();
        }

        await Assert.ThrowsAsync<IOException>(async () => await BuildAsync(format, [], destination));
        Assert.True(destination.Faulted);
        Assert.Equal(new byte[format.PageSize * 3], destination.ToArray().AsSpan(0, format.PageSize * 3).ToArray());
    }

    private static ValueTask<CompactUsageMapBuilderResult> BuildAsync(JetFormat format, IEnumerable<CompactPageRun> runs, Stream destination, CompactUsageMapBuilderBudget? budget = null)
        => CompactUsageMapBuilder.BuildAsync(format, InputAsync(runs, Ct), destination, budget ?? new(), Ct);

    private static async IAsyncEnumerable<CompactPageRun> InputAsync(IEnumerable<CompactPageRun> runs, [EnumeratorCancellation] CancellationToken token)
    {
        await Task.CompletedTask;
        foreach (CompactPageRun run in runs)
        {
            token.ThrowIfCancellationRequested();
            yield return run;
        }
    }

    private static async Task<List<long>> EnumerateAsync(JetFormat format, AppendDestination destination, CompactUsageMapBuilderResult result)
    {
        byte[] image = destination.Bytes;
        byte[] page = image.AsSpan(0, format.PageSize).ToArray();
        Assert.True(UsageMap.TryGetRowBound(page, format.DataPage, format.PageSize, result.Pointer.RowIndex, out RowBound bound));
        Assert.Equal(0, BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(4)));
        var actual = new List<long>();
        Assert.True(await UsageMap.TryEnumeratePagesAsync(
            page,
            bound,
            format.PageSize,
            destination.Length / format.PageSize,
            1,
            true,
            (number, token) =>
            {
                token.ThrowIfCancellationRequested();
                return new(image.AsSpan(checked((int)((number - result.FirstPageNumber) * format.PageSize)), format.PageSize).ToArray());
            },
            static _ => { },
            actual,
            Ct));
        return actual;
    }

    private sealed class AppendDestination(JetFormat format, long firstPage, CancellationTokenSource? cancellation = null, bool duringFlush = false) : Stream
    {
        private readonly MemoryStream appended = new();
        private readonly long prefix = firstPage * format.PageSize;
        private long position = firstPage * format.PageSize;
        private int writes;

        internal byte[] Bytes => this.appended.ToArray();

        public override bool CanRead => false;

        public override bool CanSeek => true;

        public override bool CanWrite => true;

        public override long Length => this.prefix + this.appended.Length;

        public override long Position
        {
            get =>
                this.position;

            set
            {
                Assert.True(value >= this.prefix, "A private map write must never touch the pinned prefix.");
                this.position = value;
            }
        }

        public override void Flush() => this.appended.Flush();

        public override async Task FlushAsync(CancellationToken cancellationToken)
        {
            await this.appended.FlushAsync(cancellationToken);
            if (duringFlush && cancellation is not null)
            {
                await cancellation.CancelAsync();
            }
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin)
        {
            this.Position = origin switch
            {
                SeekOrigin.Begin => offset,
                SeekOrigin.Current => this.Position + offset,
                SeekOrigin.End => this.Length + offset,
                _ => throw new ArgumentOutOfRangeException(nameof(origin)),
            };
            return this.Position;
        }

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count)
        {
            this.appended.Position = this.Position - this.prefix;
            this.appended.Write(buffer, offset, count);
            this.Position += count;
        }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            this.appended.Position = this.Position - this.prefix;
            await this.appended.WriteAsync(buffer, cancellationToken);
            this.Position += buffer.Length;
            this.writes++;
            if (this.writes == 2 && !duringFlush && cancellation is not null)
            {
                await cancellation.CancelAsync();
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.appended.Dispose();
            }

            base.Dispose(disposing);
        }
    }
}
