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
using Xunit;

/// <summary>Reopened private maps must cover exactly their independently supplied allocation runs.</summary>
public sealed class CompactUsageMapValidatorTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task ReopenedMaps_CompareExactMembershipAndCompletePrivateAllocation(DatabaseFormat kind, bool reference)
    {
        var format = JetFormat.ForNewDatabase(kind);
        int first = reference ? UsageMap.PagesPerReferenceMapPage(format.PageSize) + 100 : 100;
        CompactPageRun[] runs = reference ? [new(3, 2), new(first - 2, 2)] : [new(3, 2), new(8, 1)];
        (byte[] bytes, CompactUsageMapBuilderResult allocation) = await CreateAsync(format, first, runs);
        await using var reopened = new MemoryStream(bytes, writable: false);
        CompactUsageMapValidatorResult result = await ValidateAsync(format, reopened, allocation, runs);
        Assert.Equal(allocation.PageCount, result.Pages);
        Assert.Equal(allocation.ListedPages, result.ListedPages);
        Assert.Equal(allocation.InputRuns, result.InputRuns);
        Assert.False(result.DatabaseValidated);
    }

    [Theory]
    [InlineData("missing")]
    [InlineData("extra")]
    [InlineData("owner")]
    [InlineData("directory")]
    [InlineData("count")]
    [InlineData("uncovered")]
    public async Task ChangedMembershipGeometryOrEvidence_Refuses(string fault)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        CompactPageRun[] runs = [new(3, 2)];
        (byte[] bytes, CompactUsageMapBuilderResult allocation) = await CreateAsync(format, 100, runs);
        int start = checked((int)(allocation.FirstPageNumber * format.PageSize));
        int bitmap = start + format.PageSize - format.UsageMapRowSize + 5;
        switch (fault)
        {
            case "missing":
                bytes[bitmap] &= 0xF7;
                break;
            case "extra":
                bytes[bitmap] |= 0x20;
                break;
            case "owner":
                bytes[start + 4] = 1;
                break;
            case "directory":
                bytes[start + format.DataPage.RowsStart] = 0;
                break;
            case "count":
                allocation = allocation with { ListedPages = 3 };
                break;
            case "uncovered":
                Array.Resize(ref bytes, bytes.Length + format.PageSize);
                allocation = allocation with { PageCount = 2 };
                break;
            default:
                throw new InvalidOperationException();
        }

        await using var reopened = new MemoryStream(bytes, writable: false);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ValidateAsync(format, reopened, allocation, runs));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReferenceAliasOrOutsideAllocation_Refuses(bool alias)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        int window = UsageMap.PagesPerReferenceMapPage(format.PageSize);
        CompactPageRun[] runs = [new(3, 1), new(window + 3, 1)];
        (byte[] bytes, CompactUsageMapBuilderResult allocation) = await CreateAsync(format, window + 100, runs);
        int row = checked((int)((allocation.FirstPageNumber + 1) * format.PageSize)) - format.UsageMapRowSize;
        int firstPointer = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(row + 1));
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(row + 5), alias ? firstPointer : 2);
        await using var reopened = new MemoryStream(bytes, writable: false);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ValidateAsync(format, reopened, allocation, runs));
    }

    [Fact]
    public async Task ExtraEmptyReferencePage_RefusesEvenWithConsistentAllocationCounts()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        int window = UsageMap.PagesPerReferenceMapPage(format.PageSize);
        CompactPageRun[] runs = [new(3, 1), new(window + 3, 1)];
        (byte[] bytes, CompactUsageMapBuilderResult allocation) = await CreateAsync(format, window + 100, runs);
        int row = checked((int)((allocation.FirstPageNumber + 1) * format.PageSize)) - format.UsageMapRowSize;
        int extraPage = checked((int)(allocation.FirstPageNumber + allocation.PageCount));
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(row + 9), extraPage);
        Array.Resize(ref bytes, bytes.Length + format.PageSize);
        bytes[extraPage * format.PageSize] = Constants.PageTypes.UsageMap;
        bytes[(extraPage * format.PageSize) + 1] = 1;
        allocation = allocation with { PageCount = allocation.PageCount + 1 };
        await using var reopened = new MemoryStream(bytes, writable: false);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ValidateAsync(format, reopened, allocation, runs));
    }

    [Theory]
    [InlineData(0, 1, 4, 1)]
    [InlineData(3, 2, 4, 1)]
    [InlineData(4, 1, 3, 1)]
    [InlineData(3, 0, 4, 1)]
    [InlineData(3, long.MaxValue, 4, 1)]
    public async Task InvalidIndependentAllocationRuns_Refuse(long first, long count, long second, long secondCount)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        (byte[] bytes, CompactUsageMapBuilderResult allocation) = await CreateAsync(format, 100, [new(3, 2)]);
        await using var reopened = new MemoryStream(bytes, writable: false);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ValidateAsync(format, reopened, allocation, [new(first, count), new(second, secondCount)]));
    }

    [Fact]
    public async Task EmptyMap_AndShortReads_AreValidated()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        (byte[] bytes, CompactUsageMapBuilderResult allocation) = await CreateAsync(format, 3, []);
        await using var reopened = new ShortReadStream(bytes);
        CompactUsageMapValidatorResult result = await ValidateAsync(format, reopened, allocation, []);
        Assert.Equal(0, result.ListedPages);
        Assert.True(result.ReadBytes > format.PageSize);
    }

    [Theory]
    [InlineData("read")]
    [InlineData("steps")]
    [InlineData("working")]
    [InlineData("runs")]
    [InlineData("members")]
    [InlineData("pages")]
    public async Task AdmissionLimits_Refuse(string limit)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        CompactPageRun[] runs = [new(3, 2)];
        (byte[] bytes, CompactUsageMapBuilderResult allocation) = await CreateAsync(format, 100, runs);
        CompactUsageMapValidatorBudget budget = limit switch
        {
            "read" => new() { MaxReadBytes = 1 },
            "steps" => new() { MaxBitmapBytes = 1 },
            "working" => new() { MaxWorkingBytes = 1 },
            "runs" => new() { MaxRuns = 0 },
            "members" => new() { MaxListedPages = 1 },
            "pages" => new() { MaxPages = 0 },
            _ => throw new InvalidOperationException(),
        };
        await using var reopened = new MemoryStream(bytes, writable: false);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactUsageMapValidator.ValidateAsync(format, RunsAsync(runs, Ct), reopened, allocation, budget, Ct));
    }

    [Fact]
    public async Task CancellationAndRealIoFailures_Propagate()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        (byte[] bytes, CompactUsageMapBuilderResult allocation) = await CreateAsync(format, 3, []);
        await using var reopened = new MemoryStream(bytes, writable: false);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactUsageMapValidator.ValidateAsync(format, RunsAsync([], Ct), reopened, allocation, new(), cancellation.Token));
        await using var failure = new ReadFailureStream(bytes);
        await Assert.ThrowsAsync<IOException>(async () => await ValidateAsync(format, failure, allocation, []));
    }

    private static async Task<(byte[] Bytes, CompactUsageMapBuilderResult Allocation)> CreateAsync(JetFormat format, int firstPage, CompactPageRun[] runs)
    {
        await using var destination = new MemoryStream();
        destination.SetLength((long)firstPage * format.PageSize);
        CompactUsageMapBuilderResult allocation = await CompactUsageMapBuilder.BuildAsync(format, RunsAsync(runs, Ct), destination, new(), Ct);
        return (destination.ToArray(), allocation);
    }

    private static ValueTask<CompactUsageMapValidatorResult> ValidateAsync(JetFormat format, Stream reopened, CompactUsageMapBuilderResult allocation, CompactPageRun[] runs)
        => CompactUsageMapValidator.ValidateAsync(format, RunsAsync(runs, Ct), reopened, allocation, new(), Ct);

    private static async IAsyncEnumerable<CompactPageRun> RunsAsync(IEnumerable<CompactPageRun> runs, [EnumeratorCancellation] CancellationToken token)
    {
        await Task.CompletedTask;
        foreach (CompactPageRun run in runs)
        {
            token.ThrowIfCancellationRequested();
            yield return run;
        }
    }

    private sealed class ShortReadStream(byte[] bytes) : MemoryStream(bytes, writable: false)
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => base.ReadAsync(buffer[..Math.Min(buffer.Length, 97)], cancellationToken);
    }

    private sealed class ReadFailureStream(byte[] bytes) : MemoryStream(bytes, writable: false)
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => throw new IOException("Independent map read failure.");
    }
}
