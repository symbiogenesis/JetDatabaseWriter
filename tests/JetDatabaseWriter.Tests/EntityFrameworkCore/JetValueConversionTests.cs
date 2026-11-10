namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Microsoft.EntityFrameworkCore;
using Xunit;

#pragma warning disable EF1001 // These regressions also exercise the conversion adapter independently of query execution.
public sealed class JetValueConversionTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task NativeHyperlinks_MaterializeAsTextAndConverterProviderValues(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        Hyperlink link = new("Documentation", "https://example.com/docs", "Introduction", "Open documentation");
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, leaveOpen: true, cancellationToken: ct))
        {
            await writer.CreateTableAsync(
                "Links",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Text", typeof(Hyperlink)), new ColumnDefinition("Converted", typeof(Hyperlink))],
                ct);
            await writer.InsertRowsAsync("Links", [[1, link, link]], ct);
        }

        DbContextOptions<LinkContext> options = new DbContextOptionsBuilder<LinkContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true).Options;
        await using LinkContext context = new(options);
        LinkRow row = await context.Set<LinkRow>().AsNoTracking().SingleAsync(ct);
        Assert.Equal(link.ToString(), row.Text);
        Assert.Equal(new LinkText(link.ToString()), row.Converted);
        Assert.Equal(link.ToString(), await context.Set<LinkRow>().Select(x => x.Text).SingleAsync(ct));
    }

    [Fact]
    public void CalculationErrors_MaterializeAsExplicitText()
    {
        Assert.Equal("#Error(5)", JetEntityTypeMetadata.CoerceToType(new CalculatedError(5), typeof(string)));
        Assert.Equal("#Error(Uncached)", JetEntityTypeMetadata.CoerceToType(CalculatedError.Uncached, typeof(string)));
    }

    [Fact]
    public void InvalidScalarConversions_RetainTheirFailureKinds()
    {
        Assert.Throws<FormatException>(() => JetEntityTypeMetadata.CoerceToType("not-a-guid", typeof(Guid)));
        Assert.Throws<ArgumentException>(() => JetEntityTypeMetadata.CoerceToType(new byte[15], typeof(Guid)));
        Assert.Throws<OverflowException>(() => JetEntityTypeMetadata.CoerceToType(300, typeof(byte)));
        Assert.Throws<InvalidCastException>(() => JetEntityTypeMetadata.CoerceToType(new object(), typeof(int)));
    }

    [Fact]
    public void ProviderSpecificConversions_RetainTheirRepresentations()
    {
        Assert.Equal(DayOfWeek.Tuesday, JetEntityTypeMetadata.CoerceToType(2.5, typeof(DayOfWeek)));
        Assert.Equal('6', JetEntityTypeMetadata.CoerceToType(65, typeof(char)));
        Assert.Equal('a', JetEntityTypeMetadata.CoerceToType("abc", typeof(char)));
        Assert.Equal('\0', JetEntityTypeMetadata.CoerceToType(string.Empty, typeof(char)));
        Assert.Equal(123m, JetEntityTypeMetadata.CoerceToType(TimeSpan.FromTicks(123), typeof(decimal)));
        Assert.Equal(TimeSpan.FromTicks(123), JetEntityTypeMetadata.CoerceToType(123.9m, typeof(TimeSpan)));
        Assert.Equal(new DateTime(1899, 12, 30, 8, 30, 0), JetEntityTypeMetadata.CoerceToType(new TimeOnly(8, 30), typeof(DateTime)));
    }

    private readonly record struct LinkText(string Value);

    private sealed class LinkContext(DbContextOptions<LinkContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => _ = modelBuilder.Entity<LinkRow>(entity =>
            {
                _ = entity.ToTable("Links");
                _ = entity.Property(x => x.Id).ValueGeneratedNever();
                _ = entity.Property(x => x.Converted).HasConversion(x => x.Value, x => new LinkText(x));
            });
    }

#pragma warning disable CA1812 // EF constructs the row through its materializer.
    private sealed class LinkRow
    {
        public int Id { get; set; }

        public string Text { get; set; } = string.Empty;

        public LinkText Converted { get; set; }
    }
#pragma warning restore CA1812
}
#pragma warning restore EF1001
