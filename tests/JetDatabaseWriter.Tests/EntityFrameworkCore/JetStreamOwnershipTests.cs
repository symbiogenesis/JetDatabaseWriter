namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetStreamOwnershipTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Context_Disposal_Respects_Direct_Stream_Ownership(bool leaveOpen, bool synchronously)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<StreamContext> options = new DbContextOptionsBuilder<StreamContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: leaveOpen)
            .Options;
        StreamContext context = new(options);
        try
        {
            Assert.True(await context.Database.EnsureCreatedAsync(ct));
            context.Add(new Item { Id = 1 });
            Assert.Equal(1, await context.SaveChangesAsync(ct));
            Assert.Equal(1, await context.Set<Item>().AsNoTracking().CountAsync(ct));
        }
        finally
        {
            if (synchronously)
            {
#pragma warning disable CA1849 // Exercise the synchronous context disposal boundary.
                context.Dispose();
#pragma warning restore CA1849
            }
            else
            {
                await context.DisposeAsync();
            }
        }

        Assert.Equal(leaveOpen, stream.CanRead);
        Assert.Equal(leaveOpen, stream.CanWrite);
    }

    private sealed class StreamContext(DbContextOptions<StreamContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Item>().Property(item => item.Id).ValueGeneratedNever();
    }

    private sealed class Item
    {
        public int Id { get; set; }
    }
}
