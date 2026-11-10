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

public sealed class JetSharedStreamSchemaReadTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EnsureCreated_CatalogRead_BlocksPeerQuery(bool warmPeer)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using PausingCatalogStream stream = new();
        DbContextOptions<SchemaContext> options = CreateOptions(stream);
        await SeedAsync(options, ct);
        await using SchemaContext schema = new(options);
        await using SchemaContext peer = new(options);
        if (warmPeer)
        {
            Assert.Equal(100, await peer.Set<SchemaRow>().CountAsync(ct));
        }

        stream.PauseNextCatalogRead = true;
        Task<bool> pending = schema.Database.EnsureCreatedAsync(ct);
        await stream.Paused.Task.WaitAsync(ct);
        Task<int> query = peer.Set<SchemaRow>().CountAsync(ct);
        try
        {
            Assert.False(query.IsCompleted);
        }
        finally
        {
            stream.Resume.TrySetResult();
            await Task.WhenAll(pending, query);
        }

        Assert.False(await pending);
        Assert.Equal(100, await query);
    }

    [Fact]
    public async Task CancelingCatalogRead_ReleasesSchemaLease()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using PausingCatalogStream stream = new();
        DbContextOptions<SchemaContext> options = CreateOptions(stream);
        await SeedAsync(options, ct);
        await using SchemaContext schema = new(options);
        await using SchemaContext peer = new(options);
        using CancellationTokenSource cancel = CancellationTokenSource.CreateLinkedTokenSource(ct);
        stream.PauseNextCatalogRead = true;
        Task<bool> pending = schema.Database.EnsureCreatedAsync(cancel.Token);
        await stream.Paused.Task.WaitAsync(ct);
        await cancel.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pending);

        Assert.Equal(100, await peer.Set<SchemaRow>().CountAsync(ct));
        Assert.False(await schema.Database.EnsureCreatedAsync(ct));
    }

    [Fact]
    public async Task CancelingPeerSchemaCheck_PreservesCurrentReaderLease()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using PausingCatalogStream stream = new();
        DbContextOptions<SchemaContext> options = CreateOptions(stream);
        await SeedAsync(options, ct);
        await using SchemaContext reader = new(options);
        await using SchemaContext peer = new(options);
        await using (IAsyncEnumerator<SchemaRow> cursor = reader.Set<SchemaRow>().AsAsyncEnumerable().GetAsyncEnumerator(ct))
        {
            Assert.True(await cursor.MoveNextAsync());
            using CancellationTokenSource cancel = CancellationTokenSource.CreateLinkedTokenSource(ct);
            Task<bool> pending = peer.Database.EnsureCreatedAsync(cancel.Token);
            Assert.False(pending.IsCompleted);
            await cancel.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pending);
        }

        Assert.False(await peer.Database.EnsureCreatedAsync(ct));
        Assert.Equal(100, await reader.Set<SchemaRow>().CountAsync(ct));
    }

    private static DbContextOptions<SchemaContext> CreateOptions(Stream stream)
        => new DbContextOptionsBuilder<SchemaContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true, jetOptionsAction: static options =>
                options.WithReaderOptions(new AccessReaderOptions { PageCacheSize = 0 })).Options;

    private static async Task SeedAsync(DbContextOptions<SchemaContext> options, CancellationToken ct)
    {
        await using SchemaContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        context.AddRange(Enumerable.Range(1, 100).Select(static id => new SchemaRow { Id = id, Name = new string('a', 100) }));
        Assert.Equal(100, await context.SaveChangesAsync(ct));
    }

    private sealed class SchemaContext(DbContextOptions<SchemaContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<SchemaRow>().Property(static row => row.Id).ValueGeneratedNever();
    }

    private sealed class SchemaRow
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private sealed class PausingCatalogStream : MemoryStream
    {
        public bool PauseNextCatalogRead { get; set; }

        public TaskCompletionSource Paused { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Resume { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            // Header reads during CanConnect and reader construction precede the schema operation.
            if (this.PauseNextCatalogRead && this.Position >= 4096)
            {
                this.PauseNextCatalogRead = false;
                this.Paused.TrySetResult();
                await this.Resume.Task.WaitAsync(cancellationToken);
            }

            return await base.ReadAsync(buffer, cancellationToken);
        }
    }
}
