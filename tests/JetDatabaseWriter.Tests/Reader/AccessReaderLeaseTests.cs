namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Real scans release pinned data and speculative reads on every enumeration exit.</summary>
public sealed class AccessReaderLeaseTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public async Task AbandonCancelAndConcurrentScans_ReleaseEveryPageLease(int cacheSize)
    {
        CancellationToken token = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, token))
        {
            await writer.CreateTableAsync("Values", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Text", typeof(string), 1000)], [new IndexDefinition("ById", "Id")], token);
            for (int index = 0; index < 80; index++)
            {
                await writer.InsertRowAsync("Values", [index, new string('x', 1000)], token);
            }
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, PageCacheSize = cacheSize, PageReadOptimizationMode = PageReadOptimizationMode.Enabled }, leaveOpen: true, token);
        await using (IAsyncEnumerator<object[]> abandoned = reader.Rows("Values", cancellationToken: token).GetAsyncEnumerator(token))
        {
            Assert.True(await abandoned.MoveNextAsync());
            Assert.True(reader.Services.PageCache.OutstandingLeases > 0);
        }

        Assert.Equal(0, reader.Services.PageCache.OutstandingLeases);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
        await using (IAsyncEnumerator<object[]> cancelled = reader.Rows("Values", cancellationToken: cancellation.Token).GetAsyncEnumerator(cancellation.Token))
        {
            Assert.True(await cancelled.MoveNextAsync());
            await cancellation.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => _ = await cancelled.MoveNextAsync());
        }

        Assert.Equal(0, reader.Services.PageCache.OutstandingLeases);
        int[] counts = await Task.WhenAll(CountAsync(), CountAsync(), CountAsync());
        Assert.All(counts, static count => Assert.Equal(80, count));
        Assert.Equal(0, reader.Services.PageCache.OutstandingLeases);

        await using (IAsyncEnumerator<object[]> abandonedSeek = reader.SeekRowsAsync("Values", "ById", [20], token).GetAsyncEnumerator(token))
        {
            Assert.True(await abandonedSeek.MoveNextAsync());
            Assert.Equal(20, abandonedSeek.Current[0]);
        }

        Assert.Equal(0, reader.Services.PageCache.OutstandingLeases);
        async Task<int> CountAsync()
        {
            int count = 0;
            await foreach (object[] row in reader.Rows("Values", cancellationToken: token))
            {
                Assert.Equal(1000, Assert.IsType<string>(row[1]).Length);
                count++;
            }

            return count;
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task OverflowRows_AbandonedAndCancelledAcrossMaterializers_ReleaseTargetLeases(DatabaseFormat format)
    {
        CancellationToken token = TestContext.Current.CancellationToken;
        byte[] image = await SyntheticOverflowRows.CreateTableAsync(format, "Values", 20, primaryKey: false, token);
        _ = await SyntheticOverflowRows.MoveRowAsync(image, "Values", OverflowRowLayout.CrossPage, token);
        await using var stream = new MemoryStream(image, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false, PageCacheSize = 0, PageReadOptimizationMode = PageReadOptimizationMode.Enabled },
            leaveOpen: true,
            token);

        await CheckAsync(ct => reader.Rows("Values", cancellationToken: ct), static row => Assert.Equal(1, row[0]));
        await CheckAsync(ct => reader.Rows<OverflowLeaseRow>("Values", cancellationToken: ct), static row => Assert.Equal(1, row.Id));
        await CheckAsync(
            ct => reader.Services.Tables.RowsWithDecoder<OverflowLeaseRow>("Values", forceProjection: true, progress: null, cancellationToken: ct),
            static row => Assert.Equal(1, row.Id));

        async Task CheckAsync<T>(Func<CancellationToken, IAsyncEnumerable<T>> create, Action<T> assertFirst)
        {
            await using (IAsyncEnumerator<T> abandoned = create(token).GetAsyncEnumerator(token))
            {
                Assert.True(await abandoned.MoveNextAsync());
                assertFirst(abandoned.Current);
                Assert.True(reader.Services.PageCache.OutstandingLeases >= 2);
            }

            Assert.Equal(0, reader.Services.PageCache.OutstandingLeases);
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
            await using (IAsyncEnumerator<T> cancelled = create(cancellation.Token).GetAsyncEnumerator(cancellation.Token))
            {
                Assert.True(await cancelled.MoveNextAsync());
                assertFirst(cancelled.Current);
                await cancellation.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => _ = await cancelled.MoveNextAsync());
            }

            Assert.Equal(0, reader.Services.PageCache.OutstandingLeases);
        }
    }

#pragma warning disable CA1812 // Generic row decoders construct this POCO.
    private sealed class OverflowLeaseRow
    {
        public int Id { get; set; }

        public string? Name { get; set; }
    }
#pragma warning restore CA1812
}
