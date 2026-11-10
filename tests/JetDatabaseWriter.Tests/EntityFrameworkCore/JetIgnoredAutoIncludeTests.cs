namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetIgnoredAutoIncludeTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task AutoIncludeModifiersPreserveRecursiveOwnedGraphs(bool tracking, bool ignoreAutoIncludes)
    {
        await using MemoryStream stream = new();
        await using OwnedContext context = new(new DbContextOptionsBuilder<OwnedContext>().UseJetDatabaseWriter(stream, leaveOpen: true).Options);
        _ = await context.Database.EnsureCreatedAsync(TestContext.Current.CancellationToken);
        RelatedRow related = new() { Id = 10, Name = "Related" };
        context.Owners.Add(new Owner
        {
            Id = 1,
            Details = new Details { Name = "Details", Address = new Address { City = "Austin" } },
            Parts = [new Part { Id = 20, Info = new PartInfo { Label = "Part" } }],
            Related = related,
            Tags = [new Tag { Id = 30, Name = "Tag" }],
        });
        _ = await context.SaveChangesAsync(TestContext.Current.CancellationToken);
        context.ChangeTracker.Clear();
        IQueryable<Owner> query = context.Owners;
        if (ignoreAutoIncludes)
        {
            query = query.IgnoreAutoIncludes();
        }

        query = tracking ? query.AsTracking() : query.AsNoTracking();
        Owner actual = await query.SingleAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(actual.Details);
        Assert.Equal("Details", actual.Details.Name);
        Assert.NotNull(actual.Details.Address);
        Assert.Equal("Austin", actual.Details.Address.City);
        Assert.Equal("Part", Assert.Single(actual.Parts).Info.Label);
        if (ignoreAutoIncludes)
        {
            Assert.Null(actual.Related);
            Assert.Empty(actual.Tags);
        }
        else
        {
            Assert.NotNull(actual.Related);
            Assert.Single(actual.Tags);
        }
    }

    private sealed class OwnedContext(DbContextOptions<OwnedContext> options) : DbContext(options)
    {
        public DbSet<Owner> Owners => this.Set<Owner>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<Owner>().Property(owner => owner.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<Owner>().OwnsOne(owner => owner.Details, details =>
            {
                _ = details.Property(value => value.Name).HasColumnName("DetailName");
                _ = details.OwnsOne(value => value.Address, address => address.Property(value => value.City).HasColumnName("City"));
            });
            _ = modelBuilder.Entity<Owner>().OwnsMany(owner => owner.Parts, parts =>
            {
                _ = parts.ToTable("Parts");
                _ = parts.WithOwner().HasForeignKey(part => part.OwnerId);
                _ = parts.HasKey(part => part.Id);
                _ = parts.Property(part => part.Id).ValueGeneratedNever();
                _ = parts.OwnsOne(part => part.Info, info => info.Property(value => value.Label).HasColumnName("PartLabel"));
            });
            _ = modelBuilder.Entity<Owner>().HasOne(owner => owner.Related).WithMany().HasForeignKey(owner => owner.RelatedId);
            _ = modelBuilder.Entity<Owner>().Navigation(owner => owner.Related).AutoInclude();
            _ = modelBuilder.Entity<Owner>().HasMany(owner => owner.Tags).WithMany();
            _ = modelBuilder.Entity<Owner>().Navigation(owner => owner.Tags).AutoInclude();
            _ = modelBuilder.Entity<RelatedRow>().Property(row => row.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<Tag>().Property(tag => tag.Id).ValueGeneratedNever();
        }
    }

    private sealed class Owner
    {
        public int Id { get; set; }

        public Details Details { get; set; } = null!;

        public List<Part> Parts { get; set; } = [];

        public int RelatedId { get; set; }

        public RelatedRow? Related { get; set; }

        public List<Tag> Tags { get; set; } = [];
    }

    private sealed class Details
    {
        public string Name { get; set; } = string.Empty;

        public Address Address { get; set; } = null!;
    }

    private sealed class Address
    {
        public string City { get; set; } = string.Empty;
    }

    private sealed class Part
    {
        public int Id { get; set; }

        public int OwnerId { get; set; }

        public PartInfo Info { get; set; } = null!;
    }

    private sealed class PartInfo
    {
        public string Label { get; set; } = string.Empty;
    }

    private sealed class RelatedRow
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private sealed class Tag
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }
}
