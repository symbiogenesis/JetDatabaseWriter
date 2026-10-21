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
using Xunit;

/// <summary>Global allocation evidence covers every retained and appended page without authorizing reclamation.</summary>
public sealed class CompactGlobalAllocationTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ReopenedLedger_AccountsForPinnedPrefixAndEveryAllocationIncludingMaps(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        CompactGlobalAllocationRecord[] records = Records();
        await using var destination = new MemoryStream(new byte[16 * format.PageSize], writable: false);
        await using var ledger = new MemoryStream();
        CompactGlobalAllocationBuilderResult allocation = await BuildAsync(format, destination, ledger, records);
        await using var reopened = new MemoryStream(ledger.ToArray(), writable: false);
        CompactGlobalAllocationValidatorResult result = await ValidateAsync(format, destination, reopened, allocation, records);
        Assert.Equal(8, result.PinnedPages);
        Assert.Equal(16, result.DestinationPages);
        Assert.Equal(records.Length + 1, result.Records);
        Assert.Equal(ledger.Length, result.ReadBytes);
        Assert.False(allocation.DatabaseValidated);
        Assert.False(result.DatabaseValidated);
        Assert.False(result.OwnershipProven);
        Assert.All(destination.ToArray(), value => Assert.Equal(0, value));
    }

    [Fact]
    public async Task NoAppend_StillAccountsForEveryPinnedSourcePage()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        await using var destination = new MemoryStream(new byte[8 * format.PageSize]);
        await using var ledger = new MemoryStream();
        CompactGlobalAllocationBuilderResult allocation = await BuildAsync(format, destination, ledger, []);
        await using var reopened = new MemoryStream(ledger.ToArray(), writable: false);
        CompactGlobalAllocationValidatorResult result = await ValidateAsync(format, destination, reopened, allocation, []);
        Assert.Equal(1, result.Records);
        Assert.Equal(8, result.PinnedPages);
    }

    [Theory]
    [InlineData("gap")]
    [InlineData("overlap")]
    [InlineData("omitted-map")]
    [InlineData("outside")]
    [InlineData("zero")]
    [InlineData("overflow")]
    [InlineData("pinned")]
    [InlineData("owner")]
    [InlineData("kind")]
    public async Task IncompleteOrInvalidAllocationEvidence_Refuses(string fault)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        CompactGlobalAllocationRecord[] records = fault switch
        {
            "gap" => [Record(9, 7)],
            "overlap" => [Record(8, 4), Record(11, 5)],
            "omitted-map" => [Record(8, 7)],
            "outside" => [Record(8, 9)],
            "zero" => [Record(8, 0), Record(8, 8)],
            "overflow" => [Record(8, long.MaxValue)],
            "pinned" => [new(CompactGlobalAllocationKind.PinnedSource, 0, new(8, 8))],
            "owner" => [new(CompactGlobalAllocationKind.Data, 8, new(8, 8))],
            "kind" => [new((CompactGlobalAllocationKind)99, 2, new(8, 8))],
            _ => throw new InvalidOperationException(),
        };
        await using var destination = new MemoryStream(new byte[16 * format.PageSize]);
        await using var ledger = new MemoryStream();
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await BuildAsync(format, destination, ledger, records));
    }

    [Theory]
    [InlineData("owner")]
    [InlineData("kind")]
    [InlineData("pinned")]
    [InlineData("header")]
    [InlineData("reserved")]
    [InlineData("count")]
    [InlineData("length")]
    [InlineData("destination")]
    public async Task ForgedReopenedLedgerOrDeclaredCounts_Refuse(string fault)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        CompactGlobalAllocationRecord[] records = Records();
        await using var destination = new MemoryStream();
        destination.SetLength(16L * format.PageSize);
        await using var ledger = new MemoryStream();
        CompactGlobalAllocationBuilderResult allocation = await BuildAsync(format, destination, ledger, records);
        byte[] bytes = ledger.ToArray();
        switch (fault)
        {
            case "owner":
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(80), 3);
                break;
            case "kind":
                BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(72), (int)CompactGlobalAllocationKind.Index);
                break;
            case "pinned":
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(64), 7);
                break;
            case "header":
                bytes[0] ^= 1;
                break;
            case "reserved":
                bytes[76] = 1;
                break;
            case "count":
                allocation = allocation with { Records = allocation.Records - 1 };
                break;
            case "length":
                Array.Resize(ref bytes, bytes.Length + 1);
                break;
            case "destination":
                destination.SetLength(destination.Length + format.PageSize);
                break;
            default:
                throw new InvalidOperationException();
        }

        await using var reopened = new MemoryStream(bytes, writable: false);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ValidateAsync(format, destination, reopened, allocation, records));
    }

    [Fact]
    public async Task SelfConsistentForgedOwner_StillDisagreesWithIndependentEvidence()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        CompactGlobalAllocationRecord[] records = Records();
        CompactGlobalAllocationRecord[] forged = Records();
        forged[0] = forged[0] with { OwnerPageNumber = 3 };
        await using var destination = new MemoryStream(new byte[16 * format.PageSize]);
        await using var ledger = new MemoryStream();
        CompactGlobalAllocationBuilderResult allocation = await BuildAsync(format, destination, ledger, forged);
        await using var reopened = new MemoryStream(ledger.ToArray(), writable: false);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ValidateAsync(format, destination, reopened, allocation, records));
    }

    [Theory]
    [InlineData("gap")]
    [InlineData("overlap")]
    [InlineData("omitted")]
    [InlineData("extra")]
    [InlineData("owner")]
    public async Task InvalidIndependentExpectedEvidence_Refuses(string fault)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        CompactGlobalAllocationRecord[] expected = fault switch
        {
            "gap" => [Record(9, 7)],
            "overlap" => [Record(8, 2), Record(9, 7)],
            "omitted" => Records()[..^1],
            "extra" => [.. Records(), Record(16, 1)],
            "owner" => [new(CompactGlobalAllocationKind.Data, 0, new(8, 8))],
            _ => throw new InvalidOperationException(),
        };
        await using var destination = new MemoryStream(new byte[16 * format.PageSize]);
        await using var ledger = new MemoryStream();
        CompactGlobalAllocationBuilderResult allocation = await BuildAsync(format, destination, ledger, Records());
        await using var reopened = new MemoryStream(ledger.ToArray(), writable: false);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ValidateAsync(format, destination, reopened, allocation, expected));
    }

    [Theory]
    [InlineData("records")]
    [InlineData("pages")]
    [InlineData("bytes")]
    [InlineData("working")]
    public async Task BuilderAdmission_RefusesBeforeExceedingLimits(string limit)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        CompactGlobalAllocationBudget budget = Budget(limit);
        await using var destination = new MemoryStream(new byte[16 * format.PageSize]);
        await using var ledger = new MemoryStream();
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactGlobalAllocationBuilder.BuildAsync(format, 8, RecordsAsync(Records(), Ct), destination, ledger, budget, Ct));
    }

    [Theory]
    [InlineData("records")]
    [InlineData("pages")]
    [InlineData("bytes")]
    [InlineData("working")]
    [InlineData("read")]
    public async Task ValidatorAdmission_RefusesBeforeExceedingLimits(string limit)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        await using var destination = new MemoryStream(new byte[16 * format.PageSize]);
        await using var ledger = new MemoryStream();
        CompactGlobalAllocationBuilderResult allocation = await BuildAsync(format, destination, ledger, Records());
        await using var reopened = new MemoryStream(ledger.ToArray(), writable: false);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactGlobalAllocationValidator.ValidateAsync(format, 8, RecordsAsync(Records(), Ct), destination, reopened, allocation, Budget(limit), Ct));
    }

    [Fact]
    public async Task PartialReads_AreChargedBeforeEveryReadAttempt()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        await using var destination = new MemoryStream(new byte[16 * format.PageSize]);
        await using var ledger = new MemoryStream();
        CompactGlobalAllocationBuilderResult allocation = await BuildAsync(format, destination, ledger, Records());
        await using var reopened = new ReadProbeStream(ledger.ToArray(), shortReads: true);
        CompactGlobalAllocationValidatorResult result = await ValidateAsync(format, destination, reopened, allocation, Records());
        Assert.True(result.ReadBytes > ledger.Length);
        Assert.Equal(reopened.RequestedBytes, result.ReadBytes);
        await using var limited = new ReadProbeStream(ledger.ToArray(), shortReads: true);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactGlobalAllocationValidator.ValidateAsync(format, 8, RecordsAsync(Records(), Ct), destination, limited, allocation, new() { MaxReadBytes = ledger.Length }, Ct));
        Assert.True(limited.RequestedBytes <= ledger.Length);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WriteAndFlushFailures_PropagateWithoutCompletedEvidence(bool flush)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        await using var destination = new MemoryStream(new byte[16 * format.PageSize]);
        await using var ledger = new WriteFailureStream(flush);
        await Assert.ThrowsAsync<IOException>(async () => await BuildAsync(format, destination, ledger, Records()));
    }

    [Fact]
    public async Task ReadFailureAndCancellation_Propagate()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        await using var destination = new MemoryStream(new byte[16 * format.PageSize]);
        await using var ledger = new MemoryStream();
        CompactGlobalAllocationBuilderResult allocation = await BuildAsync(format, destination, ledger, Records());
        await using var reopened = new ReadProbeStream(ledger.ToArray(), fail: true);
        await Assert.ThrowsAsync<IOException>(async () => await ValidateAsync(format, destination, reopened, allocation, Records()));
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        await using var cancelledLedger = new MemoryStream();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactGlobalAllocationBuilder.BuildAsync(format, 8, RecordsAsync(Records(), Ct), destination, cancelledLedger, new(), cancellation.Token));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactGlobalAllocationValidator.ValidateAsync(format, 8, RecordsAsync(Records(), Ct), destination, ledger, allocation, new(), cancellation.Token));
        Assert.Equal(0, cancelledLedger.Length);
    }

    [Fact]
    public async Task CancellationAtEnumerationCompletion_PropagatesBeforeCoverageRefusal()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        await using var destination = new MemoryStream(new byte[16 * format.PageSize]);
        await using var ledger = new MemoryStream();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactGlobalAllocationBuilder.BuildAsync(format, 8, CancelAtEndAsync(cancellation), destination, ledger, new(), cancellation.Token));
    }

    [Fact]
    public async Task NonemptyOrAliasedLedger_RefusesWithoutMutation()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        await using var destination = new MemoryStream(new byte[16 * format.PageSize]);
        await Assert.ThrowsAsync<ArgumentException>(async () => await BuildAsync(format, destination, destination, Records()));
        await using var ledger = new MemoryStream(new byte[1]);
        await Assert.ThrowsAsync<ArgumentException>(async () => await BuildAsync(format, destination, ledger, Records()));
        Assert.Equal(1, ledger.Length);
        Assert.Equal(16L * format.PageSize, destination.Length);
    }

    [Fact]
    public async Task DiskBackedLedger_ReopensWithoutRetainingAllocationRecordsInMemory()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        string directory = Directory.CreateTempSubdirectory("jdw-global-allocation-").FullName;
        string path = Path.Combine(directory, "allocations.bin");
        try
        {
            await using var destination = new MemoryStream(new byte[16 * format.PageSize]);
            CompactGlobalAllocationBuilderResult allocation;
            await using (var ledger = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous))
            {
                allocation = await BuildAsync(format, destination, ledger, Records());
            }

            await using var reopened = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous);
            CompactGlobalAllocationValidatorResult result = await ValidateAsync(format, destination, reopened, allocation, Records());
            Assert.Equal(allocation.Records, result.Records);
        }
        finally
        {
            File.Delete(path);
            Directory.Delete(directory);
        }
    }

    private static CompactGlobalAllocationRecord[] Records()
        => [Record(8, 2), new(CompactGlobalAllocationKind.LongValue, 2, new(10, 1)), new(CompactGlobalAllocationKind.Index, 2, new(11, 1)), new(CompactGlobalAllocationKind.UsageMap, 2, new(12, 3)), new(CompactGlobalAllocationKind.TableDefinition, 2, new(15, 1))];

    private static CompactGlobalAllocationRecord Record(long first, long count)
        => new(CompactGlobalAllocationKind.Data, 2, new(first, count));

    private static CompactGlobalAllocationBudget Budget(string limit) => limit switch
    {
        "records" => new() { MaxRecords = 1 },
        "pages" => new() { MaxPages = 15 },
        "bytes" => new() { MaxLedgerBytes = 1 },
        "working" => new() { MaxWorkingBytes = 1 },
        "read" => new() { MaxReadBytes = 1 },
        _ => throw new InvalidOperationException(),
    };

    private static ValueTask<CompactGlobalAllocationBuilderResult> BuildAsync(JetFormat format, Stream destination, Stream ledger, CompactGlobalAllocationRecord[] records)
        => CompactGlobalAllocationBuilder.BuildAsync(format, 8, RecordsAsync(records, Ct), destination, ledger, new(), Ct);

    private static ValueTask<CompactGlobalAllocationValidatorResult> ValidateAsync(JetFormat format, Stream destination, Stream ledger, CompactGlobalAllocationBuilderResult allocation, CompactGlobalAllocationRecord[] records)
        => CompactGlobalAllocationValidator.ValidateAsync(format, 8, RecordsAsync(records, Ct), destination, ledger, allocation, new(), Ct);

    private static async IAsyncEnumerable<CompactGlobalAllocationRecord> RecordsAsync(IEnumerable<CompactGlobalAllocationRecord> records, [EnumeratorCancellation] CancellationToken token)
    {
        await Task.CompletedTask;
        foreach (CompactGlobalAllocationRecord record in records)
        {
            token.ThrowIfCancellationRequested();
            yield return record;
        }
    }

    private static async IAsyncEnumerable<CompactGlobalAllocationRecord> CancelAtEndAsync(CancellationTokenSource cancellation)
    {
        yield return Record(8, 1);
        await cancellation.CancelAsync();
    }

    private sealed class ReadProbeStream(byte[] bytes, bool shortReads = false, bool fail = false) : MemoryStream(bytes, writable: false)
    {
        internal long RequestedBytes { get; private set; }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            this.RequestedBytes += buffer.Length;
            return fail ? throw new IOException("Global allocation ledger read failure.")
                : base.ReadAsync(shortReads ? buffer[..Math.Min(buffer.Length, 7)] : buffer, cancellationToken);
        }
    }

    private sealed class WriteFailureStream(bool flush) : MemoryStream
    {
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            => flush ? base.WriteAsync(buffer, cancellationToken) : throw new IOException("Global allocation ledger write failure.");

        public override Task FlushAsync(CancellationToken cancellationToken)
            => flush ? throw new IOException("Global allocation ledger flush failure.") : base.FlushAsync(cancellationToken);
    }
}
