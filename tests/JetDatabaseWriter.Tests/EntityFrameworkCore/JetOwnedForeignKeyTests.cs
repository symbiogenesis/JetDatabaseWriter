namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetOwnedForeignKeyTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task Unrelated_Owned_ForeignKeys_RoundTrip_Through_Inserts_Updates_And_Removal(DatabaseFormat format, bool modifyOwner)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<RelationshipContext> options = new DbContextOptionsBuilder<RelationshipContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options;
        await using RelationshipContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        context.AddRange(new Country { Id = 10 }, new Country { Id = 20 });
        Assert.Equal(2, await context.SaveChangesAsync(ct));
        _ = context.Add(new Person { Id = 1, Name = "owner", Detail = new PersonDetail { Text = "detail", CountryId = 10 } });
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();

        Person person = await context.Set<Person>().SingleAsync(ct);
        Assert.NotNull(person.Detail);
        Assert.Equal(10, person.Detail.CountryId);
        person.Detail.CountryId = 20;
        if (modifyOwner)
        {
            person.Name = "changed owner";
        }

        Assert.Equal(1, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        person = await context.Set<Person>().SingleAsync(ct);
        Assert.NotNull(person.Detail);
        Assert.Equal(20, person.Detail.CountryId);
        person.Detail = new PersonDetail { Text = "replacement", CountryId = 10 };
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        person = await context.Set<Person>().SingleAsync(ct);
        Assert.NotNull(person.Detail);
        Assert.Equal(10, person.Detail.CountryId);
        person.Detail = null;
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        Assert.Null((await context.Set<Person>().SingleAsync(ct)).Detail);
        _ = context.Remove(new Country { Id = 10 });
        Assert.Equal(1, await context.SaveChangesAsync(ct));
    }

    [Fact]
    public async Task Required_Owned_ForeignKeys_Are_Persisted_With_The_Owner()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<RequiredRelationshipContext> options = new DbContextOptionsBuilder<RequiredRelationshipContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using RequiredRelationshipContext context = new(options);
        Assert.True(await context.Database.EnsureCreatedAsync(ct));
        _ = context.Add(new Country { Id = 10 });
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        _ = context.Add(new Person { Id = 1, Name = "required owner", Detail = new PersonDetail { Text = "required", CountryId = 10 } });

        Assert.Equal(1, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        Person person = await context.Set<Person>().SingleAsync(ct);
        Assert.NotNull(person.Detail);
        Assert.Equal(10, person.Detail.CountryId);
    }

    private class RelationshipContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<Country>().Property(country => country.Id).ValueGeneratedNever();
            _ = modelBuilder.Entity<Person>(entity =>
            {
                _ = entity.Property(person => person.Id).ValueGeneratedNever();
                _ = entity.Property(person => person.Name).HasMaxLength(40);
                _ = entity.OwnsOne(person => person.Detail, owned =>
                {
                    _ = owned.Property(detail => detail.Text).HasMaxLength(40);
                    _ = owned.HasOne<Country>().WithMany().HasForeignKey(detail => detail.CountryId)
                        .OnDelete(DeleteBehavior.ClientNoAction);
                });
            });
        }
    }

    private sealed class RequiredRelationshipContext(DbContextOptions<RequiredRelationshipContext> options) : RelationshipContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            _ = modelBuilder.Entity<Person>().Navigation(person => person.Detail).IsRequired();
            _ = modelBuilder.Entity<Person>().OwnsOne(person => person.Detail).Property(detail => detail.CountryId).IsRequired();
        }
    }

    private sealed class Person
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public PersonDetail? Detail { get; set; }
    }

    private sealed class PersonDetail
    {
        public string Text { get; set; } = string.Empty;

        public int? CountryId { get; set; }
    }

    private sealed class Country
    {
        public int Id { get; set; }
    }

}
