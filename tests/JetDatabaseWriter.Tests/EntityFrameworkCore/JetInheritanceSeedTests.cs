namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetInheritanceSeedTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task CreationAndMigration_SeedConvertedDiscriminators(DatabaseFormat format, bool migrate)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<SeedContext> options = new DbContextOptionsBuilder<SeedContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options;
        await using SeedContext context = new(options);
        if (migrate)
        {
            await context.Database.MigrateAsync(ct);
        }
        else
        {
            Assert.True(await context.Database.EnsureCreatedAsync(ct));
        }

        SeedChild child = await context.Set<SeedChild>().SingleAsync(ct);
        Assert.Equal(2, child.Id);
        Assert.Equal("child", child.Name);
        Assert.Equal("detail", child.Detail);
        Assert.Equal(SeedKind.Child, context.Entry(child).Property<SeedKind>("Kind").CurrentValue);
        SeedRoot root = await context.Set<SeedRoot>().SingleAsync(x => x.Id == 1, ct);
        Assert.IsType<SeedRoot>(root);
        Assert.Equal(SeedKind.Root, context.Entry(root).Property<SeedKind>("Kind").CurrentValue);
        await context.Database.MigrateAsync(ct);
        Assert.Equal(2, await context.Set<SeedRoot>().CountAsync(ct));
    }

    private enum SeedKind
    {
        Root,
        Child,
    }

    private sealed class SeedContext(DbContextOptions<SeedContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<SeedRoot>(entity =>
            {
                _ = entity.ToTable("SeededHierarchy");
                _ = entity.Property(x => x.Name).HasMaxLength(40);
                _ = entity.HasDiscriminator<SeedKind>("Kind")
                    .HasValue<SeedRoot>(SeedKind.Root)
                    .HasValue<SeedChild>(SeedKind.Child);
                _ = entity.Property<SeedKind>("Kind").HasConversion<string>().HasMaxLength(16);
                _ = entity.HasData(new SeedRoot { Id = 1, Name = "root" });
            });
            _ = modelBuilder.Entity<SeedChild>(entity =>
            {
                _ = entity.Property(x => x.Detail).HasMaxLength(40);
                _ = entity.HasData(new SeedChild { Id = 2, Name = "child", Detail = "detail" });
            });
        }
    }

    private class SeedRoot
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private sealed class SeedChild : SeedRoot
    {
        public string Detail { get; set; } = string.Empty;
    }
}