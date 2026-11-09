namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetDatabaseCreationSafetyTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task WrongPassword_PreservesExistingStream(bool migrate, bool streamFactory)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using (CreationContext seed = new(CreateOptions(stream, streamFactory, "correct-password")))
        {
            Assert.True(await seed.Database.EnsureCreatedAsync(ct));
            seed.Items.Add(new CreationItem { Name = "Preserved row" });
            Assert.Equal(1, await seed.SaveChangesAsync(ct));
        }

        byte[] original = stream.ToArray();
        await using (CreationContext wrongPassword = new(CreateOptions(stream, streamFactory, "wrong-password")))
        {
            Assert.False(await wrongPassword.Database.CanConnectAsync(ct));
            ArgumentException exception = await Assert.ThrowsAsync<ArgumentException>(
                () => CreateOrMigrateAsync(wrongPassword, migrate, ct));
            Assert.Contains("empty", exception.Message, StringComparison.Ordinal);
            Assert.Equal(original, stream.ToArray());
        }

        await using CreationContext reopened = new(CreateOptions(stream, streamFactory, "correct-password"));
        Assert.Equal("Preserved row", (await reopened.Items.SingleAsync(ct)).Name);
        Assert.Equal(original, stream.ToArray());
    }

    [Theory]
    [InlineData(false, 17)]
    [InlineData(false, 2048)]
    [InlineData(true, 17)]
    [InlineData(true, 2048)]
    public async Task InvalidNonemptyStream_IsNeverReplaced(bool migrate, int length)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        byte[] original = new byte[length];
        original.AsSpan().Fill(0x5a);
        await stream.WriteAsync(original, ct);
        await using CreationContext context = new(CreateOptions(stream, streamFactory: false, password: null));

        Assert.False(await context.Database.CanConnectAsync(ct));
        await Assert.ThrowsAsync<ArgumentException>(() => CreateOrMigrateAsync(context, migrate, ct));
        Assert.Equal(original, stream.ToArray());
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task EmptyStream_CanStillBeCreated(bool migrate, bool streamFactory)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        stream.Position = 23;
        await using CreationContext context = new(CreateOptions(stream, streamFactory, password: null));

        await CreateOrMigrateAsync(context, migrate, ct);
        context.Items.Add(new CreationItem { Name = "New row" });
        Assert.Equal(1, await context.SaveChangesAsync(ct));
        context.ChangeTracker.Clear();
        Assert.Equal("New row", (await context.Items.SingleAsync(ct)).Name);
    }

    private static DbContextOptions<CreationContext> CreateOptions(MemoryStream stream, bool streamFactory, string? password)
    {
        DbContextOptionsBuilder<CreationContext> builder = new();
        if (streamFactory)
        {
            _ = builder.UseJetDatabaseWriter(() => stream, DatabaseFormat.Jet4Mdb, leaveOpen: true, jetOptionsAction: jet => jet.WithPassword(password));
        }
        else
        {
            _ = builder.UseJetDatabaseWriter(stream, DatabaseFormat.Jet4Mdb, leaveOpen: true, jetOptionsAction: jet => jet.WithPassword(password));
        }

        return builder.Options;
    }

    private static async Task CreateOrMigrateAsync(CreationContext context, bool migrate, CancellationToken cancellationToken)
    {
        if (migrate)
        {
            await context.Database.MigrateAsync(cancellationToken);
        }
        else
        {
            _ = await context.Database.EnsureCreatedAsync(cancellationToken);
        }
    }

    private sealed class CreationContext(DbContextOptions<CreationContext> options) : DbContext(options)
    {
        public DbSet<CreationItem> Items => this.Set<CreationItem>();
    }

    private sealed class CreationItem
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }
}
