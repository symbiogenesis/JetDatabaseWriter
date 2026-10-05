namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Checks page-store traffic through the public writer on each supported format.</summary>
/// <param name="output">The test output.</param>
public sealed class WriterPageIoTests(ITestOutputHelper output)
{
    /// <summary>Bulk writes reuse plaintext frames in direct and transactional modes.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, WriteMode.Direct)]
    [InlineData(DatabaseFormat.Jet3Mdb, WriteMode.AutoCommit)]
    [InlineData(DatabaseFormat.Jet3Mdb, WriteMode.ExplicitCommit)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.Direct)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.AutoCommit)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.ExplicitCommit)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.Direct)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.AutoCommit)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.ExplicitCommit)]
    public async Task Bulk999_ReadsDropWithFrameCache(DatabaseFormat format, WriteMode mode)
    {
        using var baseline = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(baseline, format, Options(0, false), leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("PageIo", [new("Id", typeof(int)), new("Name", typeof(string), 100)], TestContext.Current.CancellationToken);
        }

        byte[] image = baseline.ToArray();
        long uncached = await InsertAsync(image, 0, mode, format == DatabaseFormat.Jet3Mdb ? 2048 : 4096);
        long cached = await InsertAsync(image, 256, mode, format == DatabaseFormat.Jet3Mdb ? 2048 : 4096);
        output.WriteLine($"{format}, mode={mode}: uncached={uncached} page reads, cached={cached} page reads");
        Assert.True(cached <= uncached, $"Caching increased physical reads: {cached} versus {uncached}.");
        if (mode == WriteMode.Direct)
        {
            Assert.True(cached < uncached, $"Expected fewer physical reads with caching: {cached} versus {uncached}.");
        }
    }

    /// <summary>Each fitting unchanged page is read only once through the writer pager.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, WriteMode.Direct)]
    [InlineData(DatabaseFormat.Jet3Mdb, WriteMode.AutoCommit)]
    [InlineData(DatabaseFormat.Jet3Mdb, WriteMode.ExplicitCommit)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.Direct)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.AutoCommit)]
    [InlineData(DatabaseFormat.Jet4Mdb, WriteMode.ExplicitCommit)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.Direct)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.AutoCommit)]
    [InlineData(DatabaseFormat.AceAccdb, WriteMode.ExplicitCommit)]
    public async Task RepeatedReads_EachEligiblePageHitsStoreOnce(DatabaseFormat format, WriteMode mode)
    {
        using var stream = new MemoryStream();
        await using (AccessWriter created = await AccessWriter.CreateDatabaseAsync(stream, format, Options(0, false), leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await created.CreateTableAsync("PageIo", [new("Id", typeof(int))], TestContext.Current.CancellationToken);
        }

        int pageSize = format == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        using var trace = new PageTraceStream(stream, pageSize);
        await using WriterHarness writer = await WriterHarness.OpenAsync(trace, Options(256, mode == WriteMode.AutoCommit), cancellationToken: TestContext.Current.CancellationToken);
        await writer.InsertRowAsync("PageIo", [1], TestContext.Current.CancellationToken);
        await using JetTransaction? transaction = mode == WriteMode.ExplicitCommit
            ? await writer.BeginTransactionAsync(TestContext.Current.CancellationToken) : null;
        writer.Pager.InvalidateAll();
        trace.Reset();
        for (int iteration = 0; iteration < 3; iteration++)
        {
            PageBuffers.Return(await writer.Pager.ReadPageAsync(1, TestContext.Current.CancellationToken));
            PageBuffers.Return(await writer.Pager.ReadPageAsync(2, TestContext.Current.CancellationToken));
        }

        Assert.Equal(2, trace.ReadCounts(pageSize).Count);
        Assert.All(trace.ReadCounts(pageSize).Values, count => Assert.Equal(1, count));
        if (transaction is not null)
        {
            await transaction.CommitAsync(TestContext.Current.CancellationToken);
        }
    }
    /// <summary>Negative cache capacities fail before touching the caller stream.</summary>
    [Fact]
    public async Task NegativeCapacity_FailsBeforeReadingStream()
    {
        using var image = new MemoryStream(new byte[4096]);
        using var trace = new PageTraceStream(image);
        _ = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await AccessWriter.OpenAsync(trace, Options(-1, false), leaveOpen: true, TestContext.Current.CancellationToken));
        Assert.Equal(0, trace.BytesRead);
        Assert.Equal(0, trace.BytesWritten);
        Assert.False(trace.IsDisposed);
    }

    private static AccessWriterOptions Options(int capacity, bool transactional) => new()
    {
        PageCacheSize = capacity,
        UseTransactionalWrites = transactional,
        UseLockFile = false,
        UseByteRangeLocks = false,
    };

    private static async Task<long> InsertAsync(byte[] image, int capacity, WriteMode mode, int pageSize)
    {
        using var stream = new MemoryStream();
        await stream.WriteAsync(image, TestContext.Current.CancellationToken);
        using var trace = new PageTraceStream(stream, pageSize);
        await using AccessWriter writer = await AccessWriter.OpenAsync(trace, Options(capacity, mode == WriteMode.AutoCommit), leaveOpen: true, TestContext.Current.CancellationToken);
        trace.Reset();
        await WriteModes.RunAsync(writer, mode, async () =>
        {
            _ = await writer.InsertRowsAsync("PageIo", Enumerable.Range(1, 999).Select(i => new object?[] { i, $"row-{i}" }), TestContext.Current.CancellationToken);
        }, TestContext.Current.CancellationToken);
        return trace.ReadCounts(pageSize).Values.Sum();
    }
}