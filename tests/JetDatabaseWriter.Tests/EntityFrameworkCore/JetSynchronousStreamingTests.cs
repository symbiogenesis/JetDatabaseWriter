namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetSynchronousStreamingTests
{
    private static readonly AsyncLocal<Action?> OnConstruct = new();

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Synchronous_Enumeration_Materializes_Only_Requested_Rows_And_Releases_The_Lease(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using AsyncReadStream stream = new();
        DbContextOptions<StreamingContext> options = CreateOptions(stream, format);
        await using (StreamingContext seed = new(options))
        {
            await SeedAsync(seed, ct);
        }

        await using StreamingContext context = new(options);
        int constructed = 0;
        OnConstruct.Value = () => constructed++;
        try
        {
#pragma warning disable CA1849 // The regression deliberately exercises synchronous EF query enumeration.
            using (IEnumerator<Item> rows = context.Items.AsNoTracking().AsEnumerable().GetEnumerator())
            {
                Assert.True(rows.MoveNext());
                Assert.Equal(1, rows.Current.Id);
                Assert.Equal(1, constructed);
            }
#pragma warning restore CA1849
        }
        finally
        {
            OnConstruct.Value = null;
        }

        context.Items.Add(new Item { Id = 4 });
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        Assert.Equal(4, await context.Items.CountAsync(ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Synchronous_Projection_Does_Not_Evaluate_Unrequested_Rows(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using AsyncReadStream stream = new();
        await using StreamingContext context = new(CreateOptions(stream, format));
        await SeedAsync(context, ct);

#pragma warning disable CA1849 // The regression deliberately exercises synchronous EF query enumeration and faults.
        using (IEnumerator<int> rows = context.Items.Select(item => ProjectId(item.Id)).AsEnumerable().GetEnumerator())
        {
            Assert.True(rows.MoveNext());
            Assert.Equal(1, rows.Current);
            Assert.Throws<InvalidOperationException>(() => rows.MoveNext());
        }
#pragma warning restore CA1849

        context.Items.Add(new Item { Id = 4 });
        Assert.Equal(1, await context.SaveChangesAsync(ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Synchronous_Consumer_Exception_Releases_The_Lease(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using AsyncReadStream stream = new();
        await using StreamingContext context = new(CreateOptions(stream, format));
        await SeedAsync(context, ct);

#pragma warning disable CA1849 // The regression deliberately throws from a synchronous query consumer.
        Assert.Throws<InvalidOperationException>(() =>
        {
            foreach (Item item in context.Items.AsNoTracking())
            {
                throw new InvalidOperationException($"Consumer rejected row {item.Id}.");
            }
        });
#pragma warning restore CA1849

        context.Items.Add(new Item { Id = 4 });
        Assert.Equal(1, await context.SaveChangesAsync(ct));
    }

    private static DbContextOptions<StreamingContext> CreateOptions(Stream stream, DatabaseFormat format)
        => new DbContextOptionsBuilder<StreamingContext>().UseJetDatabaseWriter(stream, format, leaveOpen: true).Options;

    private static async Task SeedAsync(StreamingContext context, CancellationToken cancellationToken)
    {
        await context.Database.EnsureCreatedAsync(cancellationToken);
        context.Items.AddRange(new Item { Id = 1 }, new Item { Id = 2 }, new Item { Id = 3 });
        await context.SaveChangesAsync(cancellationToken);
        context.ChangeTracker.Clear();
    }

    private static int ProjectId(int id)
        => id == 2 ? throw new InvalidOperationException("The second row was requested.") : id;

    private sealed class StreamingContext(DbContextOptions<StreamingContext> options) : DbContext(options)
    {
        public DbSet<Item> Items => this.Set<Item>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Item>().Property(item => item.Id).ValueGeneratedNever();
    }

    private sealed class Item
    {
        public Item() => OnConstruct.Value?.Invoke();

        public int Id { get; set; }
    }

    private sealed class AsyncReadStream : MemoryStream
    {
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            return await base.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        }
    }
}
