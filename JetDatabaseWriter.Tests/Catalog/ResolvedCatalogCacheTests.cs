namespace JetDatabaseWriter.Tests.Catalog;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
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
    public async Task ConcurrentWarmLookups_ReuseVerifiedIdentitiesWithoutPhysicalReads()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream file = await CreateDatabaseAsync(ct);
        await using var trace = new CountingStream(file);
        await using AccessReader reader = await AccessReader.OpenAsync(trace, new AccessReaderOptions { PageCacheSize = 0, UseLockFile = false }, leaveOpen: true, ct);
        reader.Database.TableDefs.VerifyOnHit = false;
        _ = await reader.Services.Catalog.ResolveTableAsync("MSysObjects", ct);
        Assert.Null(await reader.Services.Catalog.ResolveTableAsync("Missing", ct));
        trace.Reset();

        await Task.WhenAll(Enumerable.Range(0, 16).Select(async _ =>
        {
            Assert.NotNull(await reader.Services.Catalog.ResolveTableAsync("MSysObjects", ct));
            Assert.Null(await reader.Services.Catalog.ResolveTableAsync("Missing", ct));
        }));

        Assert.Equal(0, trace.BytesRead);
    }

    [Fact]
    public async Task FailedLookup_DoesNotCacheAnUnverifiedMissingResult()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream seed = await CreateDatabaseAsync(ct);
        await using var file = new FailingReadStream(seed.ToArray());
        await using AccessReader reader = await AccessReader.OpenAsync(file, new AccessReaderOptions { PageCacheSize = 0, UseLockFile = false }, leaveOpen: true, ct);
        _ = await reader.ListTablesAsync(ct);
        file.FailReads = true;

        IOException failure = await Assert.ThrowsAsync<IOException>(async () => await reader.Services.Catalog.ResolveTableAsync("Missing", ct));

        Assert.Same(file.Failure, failure);
        file.FailReads = false;
        Assert.Null(await reader.Services.Catalog.ResolveTableAsync("Missing", ct));
        Assert.NotNull(await reader.Services.Catalog.ResolveTableAsync("MSysObjects", ct));
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

    [Fact]
    public async Task WriterRollback_DiscardsTheRolledBackResolvedIdentity()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream file = await CreateDatabaseAsync(ct);
        await using AccessWriter writer = await TestDatabase.OpenWriterAsync(file, WriteModes.WriterOptions(WriteMode.ExplicitCommit), cancellationToken: ct);
        using var pages = new ReaderPageCache(writer.Database.Format, writer.Database.Pages, capacity: 0);
        var rows = new RowDecoder(writer.Database.Format, writer.Database.OwnedPages, pages, new LongValueDecoder(writer.Database.Format, pages), strictParsing: true);
        var catalog = new CatalogReader(writer.Database.Format, writer.Database.TableDefs, writer.Services.Catalog, writer.Services.CatalogRows, rows);
        Assert.Null(await catalog.ResolveTableAsync("Added", ct));
        await using JetTransaction transaction = await writer.BeginTransactionAsync(ct);
        await writer.CreateTableAsync("Added", [new ColumnDefinition("Value", typeof(int))], ct);
        Assert.NotNull(await catalog.ResolveTableAsync("Added", ct));

        await transaction.RollbackAsync(ct);

        Assert.Null(await catalog.ResolveTableAsync("Added", ct));
        Assert.NotNull(await catalog.ResolveTableAsync("T", ct));
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

    private sealed class FailingReadStream(byte[] bytes) : MemoryStream(bytes)
    {
        internal bool FailReads { get; set; }

        internal IOException Failure { get; } = new("Injected catalog read failure.");

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => this.FailReads ? ValueTask.FromException<int>(this.Failure) : base.ReadAsync(buffer, cancellationToken);
    }
}
