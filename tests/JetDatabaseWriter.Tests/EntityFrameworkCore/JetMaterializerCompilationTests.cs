namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Xunit;

public sealed class JetMaterializerCompilationTests
{
    private static readonly AsyncLocal<Action?> OnCreateShaper = new();

    [Fact]
    public async Task Scalar_Queries_Skip_Entity_Shapers_And_Entity_Queries_Compile_Them_Before_Reading()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<CompilationContext> options = new DbContextOptionsBuilder<CompilationContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true)
            .ReplaceService<IShapedQueryCompilingExpressionVisitorFactory, CountingShaperFactory>()
            .Options;
        await using CompilationContext context = new(options);
        _ = await context.Database.EnsureCreatedAsync(cancellationToken);
        context.Items.AddRange(new Item { Id = 1, Amount = 2 }, new Item { Id = 2, Amount = 4 });
        _ = await context.SaveChangesAsync(cancellationToken);
        context.ChangeTracker.Clear();
        int shapers = 0;
        OnCreateShaper.Value = () => shapers++;
        try
        {
            string tag = Guid.NewGuid().ToString();
            Assert.Equal(2, await context.Items.TagWith(tag).CountAsync(cancellationToken));
            Assert.Equal(6, await context.Items.TagWith(tag).SumAsync(item => item.Amount, cancellationToken));
            int[] amounts = await context.Items.TagWith(tag).Select(item => item.Amount).ToArrayAsync(cancellationToken);
            Assert.Equal([2, 4], amounts);
            int updated = await context.Items.TagWith(tag).Where(item => item.Id == 1)
                .ExecuteUpdateAsync(setters => setters.SetProperty(item => item.Amount, 2), cancellationToken);
            Assert.Equal(1, updated);
            Assert.Equal(0, await context.Items.TagWith(tag).Where(item => item.Id < 0).ExecuteDeleteAsync(cancellationToken));
            Assert.Equal(0, shapers);

            await using IAsyncEnumerator<Item> rows = context.Items.TagWith(tag).AsNoTracking().AsAsyncEnumerable().GetAsyncEnumerator(cancellationToken);
            Assert.True(shapers > 0);
            int beforeRead = shapers;
            Assert.True(await rows.MoveNextAsync());
            Assert.Equal(1, rows.Current.Id);
            Assert.Equal(2, rows.Current.Amount);
            Assert.Equal(beforeRead, shapers);
        }
        finally
        {
            OnCreateShaper.Value = null;
        }
    }

    private sealed class CompilationContext(DbContextOptions<CompilationContext> options) : DbContext(options)
    {
        public DbSet<Item> Items => this.Set<Item>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Item>().Property(item => item.Id).ValueGeneratedNever();
    }

    private sealed class Item
    {
        public int Id { get; set; }

        public int Amount { get; set; }
    }

#pragma warning disable EF1001, CA1812 // Probe provider shaper creation through EF service injection.
    private sealed class CountingShaperFactory(ShapedQueryCompilingExpressionVisitorDependencies dependencies)
        : IShapedQueryCompilingExpressionVisitorFactory
    {
        public ShapedQueryCompilingExpressionVisitor Create(QueryCompilationContext queryCompilationContext)
        {
            OnCreateShaper.Value?.Invoke();
            return new JetShapedQueryCompilingExpressionVisitor(dependencies, queryCompilationContext);
        }
    }
#pragma warning restore EF1001, CA1812
}
