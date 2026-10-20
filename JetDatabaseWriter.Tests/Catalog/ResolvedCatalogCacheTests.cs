namespace JetDatabaseWriter.Tests.Catalog;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

/// <summary>Catalog lookups retain verified identities without retaining stale writer schemas.</summary>
public sealed class ResolvedCatalogCacheTests
{
    [Fact]
    public async Task WarmSystemAndMissingNames_AvoidPhysicalReadsWithoutPageCache()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream file = await CreateDatabaseAsync(ct);
        await using var trace = new CountingStream(file);
        await using AccessReader reader = await AccessReader.OpenAsync(trace, new AccessReaderOptions { PageCacheSize = 0, UseLockFile = false }, leaveOpen: true, ct);
        reader.Database.TableDefs.VerifyOnHit = false;
        ResolvedTable system = Assert.IsType<ResolvedTable>(await reader.Services.Catalog.ResolveTableAsync("MSysObjects", ct));
        Assert.Null(await reader.Services.Catalog.ResolveTableAsync("Missing", ct));
        trace.Reset();

        for (int i = 0; i < 3; i++)
        {
            ResolvedTable repeated = Assert.IsType<ResolvedTable>(await reader.Services.Catalog.ResolveTableAsync("msysobjects", ct));
            Assert.Same(system.Schema, repeated.Schema);
            Assert.Null(await reader.Services.Catalog.ResolveTableAsync("MISSING", ct));
        }

        Assert.Equal(0, trace.BytesRead);
    }

    [Fact]
    public async Task EvictedMissingName_IsVerifiedAgainAndStillMissing()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream file = await CreateDatabaseAsync(ct);
        await using var trace = new CountingStream(file);
        await using AccessReader reader = await AccessReader.OpenAsync(trace, new AccessReaderOptions { PageCacheSize = 0, UseLockFile = false }, leaveOpen: true, ct);
        reader.Database.TableDefs.VerifyOnHit = false;
        Assert.Null(await reader.Services.Catalog.ResolveTableAsync("OldMissing", ct));
        for (int i = 0; i < 300; i++)
        {
            Assert.Null(await reader.Services.Catalog.ResolveTableAsync("Missing" + i.ToString(System.Globalization.CultureInfo.InvariantCulture), ct));
        }

        trace.Reset();
        Assert.Null(await reader.Services.Catalog.ResolveTableAsync("Missing299", ct));
        Assert.Equal(0, trace.BytesRead);
        reader.Database.TableDefs.VerifyOnHit = false;
        Assert.Null(await reader.Services.Catalog.ResolveTableAsync("OldMissing", ct));
        Assert.True(trace.BytesRead > 0);
    }

    [Fact]
    public async Task CanceledWarmLookup_ThrowsBeforeReturningCachedIdentity()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream file = await CreateDatabaseAsync(ct);
        await using AccessReader reader = await AccessReader.OpenAsync(file, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        _ = await reader.Services.Catalog.ResolveTableAsync("MSysObjects", ct);
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.Services.Catalog.ResolveTableAsync("MSysObjects", canceled.Token));
    }

    [Fact]
    public async Task WriterCatalogGeneration_RefreshesResolvedAndMissingNames()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream file = await CreateDatabaseAsync(ct);
        await using AccessWriter writer = await TestDatabase.OpenWriterAsync(file, cancellationToken: ct);
        using var pages = new ReaderPageCache(writer.Database.Format, writer.Database.Pages, capacity: 0);
        var rows = new RowDecoder(writer.Database.Format, writer.Database.OwnedPages, pages, new LongValueDecoder(writer.Database.Format, pages), strictParsing: true);
        var catalog = new CatalogReader(writer.Database.Format, writer.Database.TableDefs, writer.Services.Catalog, writer.Services.CatalogRows, rows);
        Assert.Null(await catalog.ResolveTableAsync("Added", ct));
        ResolvedTable original = Assert.IsType<ResolvedTable>(await catalog.ResolveTableAsync("T", ct));

        await writer.CreateTableAsync("Added", [new ColumnDefinition("Value", typeof(int))], ct);
        Assert.NotNull(await catalog.ResolveTableAsync("Added", ct));
        await writer.RenameColumnAsync("T", "Id", "Renamed", ct);
        ResolvedTable renamed = Assert.IsType<ResolvedTable>(await catalog.ResolveTableAsync("T", ct));
        Assert.Equal("Renamed", Assert.Single(renamed.Definition.Columns).Name);
        Assert.Equal("Id", Assert.Single(original.Definition.Columns).Name);
    }

    private static async ValueTask<MemoryStream> CreateDatabaseAsync(CancellationToken cancellationToken)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int))], cancellationToken);
        }

        stream.Position = 0;
        return stream;
    }
}
