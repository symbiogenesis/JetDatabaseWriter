namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetSharedStreamConcurrencyTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PeerReader_WaitsForSharedStreamRead(bool warmPeer)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using PausingStream stream = new();
        DbContextOptions<StreamContext> options = CreateOptions(stream);
        await SeedAsync(options, ct);
        await using StreamContext first = new(options);
        await using StreamContext second = new(options);
        Assert.Equal(100, await first.Set<StreamRow>().CountAsync(ct));
        if (warmPeer)
        {
            Assert.Equal(100, await second.Set<StreamRow>().CountAsync(ct));
        }

        stream.PauseNextRead = true;
        Task<List<StreamRow>> pending = first.Set<StreamRow>().AsNoTracking().ToListAsync(ct);
        await stream.Paused.Task.WaitAsync(ct);
        Task<int> peer = second.Set<StreamRow>().CountAsync(ct);
        try
        {
            Assert.False(peer.IsCompleted);
        }
        finally
        {
            stream.Resume.TrySetResult();
            await Task.WhenAll(pending, peer);
        }

        Assert.Equal(100, (await pending).Count);
        Assert.Equal(100, await peer);
        Assert.Equal(Enumerable.Range(1, 100), (await pending).Select(static row => row.Id).Order());
    }

    [Fact]
    public async Task CancelingPeerAndDisposingCursor_ReleasesSharedStreamLease()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using PausingStream stream = new();
        DbContextOptions<StreamContext> options = CreateOptions(stream);
        await SeedAsync(options, ct);
        await using StreamContext first = new(options);
        await using StreamContext second = new(options);
        await using (IAsyncEnumerator<StreamRow> cursor = first.Set<StreamRow>().AsAsyncEnumerable().GetAsyncEnumerator(ct))
        {
            Assert.True(await cursor.MoveNextAsync());
            using var cancel = CancellationTokenSource.CreateLinkedTokenSource(ct);
            Task<int> blocked = second.Set<StreamRow>().CountAsync(cancel.Token);
            Assert.False(blocked.IsCompleted);
            await cancel.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await blocked);
        }

        Assert.Equal(100, await second.Set<StreamRow>().CountAsync(ct));
        Assert.Equal(100, await first.Set<StreamRow>().CountAsync(ct));
    }

    private static DbContextOptions<StreamContext> CreateOptions(Stream stream)
        => new DbContextOptionsBuilder<StreamContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true, jetOptionsAction: static options =>
                options.WithReaderOptions(new AccessReaderOptions { PageCacheSize = 0 })).Options;

    private static async Task SeedAsync(DbContextOptions<StreamContext> options, CancellationToken ct)
    {
        await using StreamContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        context.AddRange(Enumerable.Range(1, 100).Select(static id => new StreamRow { Id = id, Name = new string('a', 100) }));
        Assert.Equal(100, await context.SaveChangesAsync(ct));
    }

    private sealed class StreamContext(DbContextOptions<StreamContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<StreamRow>().Property(static row => row.Id).ValueGeneratedNever();
    }

    private sealed class StreamRow
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private sealed class PausingStream : MemoryStream
    {
        public bool PauseNextRead { get; set; }

        public TaskCompletionSource Paused { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Resume { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (this.PauseNextRead)
            {
                this.PauseNextRead = false;
                this.Paused.TrySetResult();
                await this.Resume.Task.WaitAsync(cancellationToken);
            }

            return await base.ReadAsync(buffer, cancellationToken);
        }
    }
}
