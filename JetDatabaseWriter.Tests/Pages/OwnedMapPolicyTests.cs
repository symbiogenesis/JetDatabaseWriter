namespace JetDatabaseWriter.Tests.Pages;

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Pins <see cref="CatalogOwnedMapPolicy"/>, which decides whose owned-page
/// usage maps the writer may extend when it appends a data page: never on
/// Jet3; never for an Access-authored <c>MSys*</c> table, an answer it keeps by
/// TDEF page so a later append does not rescan <c>MSysObjects</c>; and a table
/// that a rolled-back transaction created is forgotten.
/// </summary>
public sealed class OwnedMapPolicyTests
{
    private const string TableName = "Items";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>
    /// On Jet3 the writer extends no table's owned-page map, neither one it
    /// created, even once registered, nor one Access wrote, and it decides so
    /// without reading a page.
    /// </summary>
    [Fact]
    public async Task Jet3_NeverMaintainsAnOwnedMap()
    {
        await using (MemoryStream created = await CreateDatabaseAsync(DatabaseFormat.Jet3Mdb))
        await using (var counting = new CountingStream(created))
        await using (WriterHarness harness = await WriterHarness.OpenAsync(counting, cancellationToken: Ct))
        {
            CatalogOwnedMapPolicy policy = harness.Services.OwnedMaps;
            long tdefPage = await TDefPageOfAsync(harness, TableName);

            counting.Reset();
            Assert.False(await policy.CanMaintainAsync(tdefPage, Ct));
            policy.RegisterWritable(tdefPage);
            Assert.False(await policy.CanMaintainAsync(tdefPage, Ct));
            Assert.True(counting.BytesRead == 0, "The Jet3 answer read pages.");
        }

        await using var fixture = new MemoryStream(await File.ReadAllBytesAsync(TestDatabases.Jet3Test, Ct));
        await using WriterHarness accessAuthored = await WriterHarness.OpenAsync(fixture, cancellationToken: Ct);
        List<CatalogEntry> tables = await accessAuthored.Services.Catalog.GetUserTablesAsync(Ct);
        Assert.NotEmpty(tables);
        foreach (CatalogEntry table in tables)
        {
            Assert.False(await accessAuthored.Services.OwnedMaps.CanMaintainAsync(table.TDefPage, Ct));
        }
    }

    /// <summary>
    /// The writer never extends the owned-page map of an Access-authored
    /// <c>MSys*</c> table, and keeps that answer by TDEF page, so asking again
    /// for the same table, as every page appended to it does, reads nothing
    /// instead of rescanning <c>MSysObjects</c>. A user table's answer is kept
    /// too, and <c>MSysObjects</c>' own map is refused without a scan.
    /// </summary>
    [Fact]
    public async Task AccessAuthoredSystemTables_AreRefusedOnce_WithoutRescanningTheCatalog()
    {
        await using var file = new MemoryStream(await File.ReadAllBytesAsync(TestDatabases.NorthwindTraders, Ct));
        await using var counting = new CountingStream(file);
        // Measure the policy's memoization through physical reads of the catalog.
        var options = new AccessWriterOptions { PageCacheSize = 0, UseLockFile = false, UseByteRangeLocks = false };
        await using WriterHarness harness = await WriterHarness.OpenAsync(counting, options, cancellationToken: Ct);
        CatalogOwnedMapPolicy policy = harness.Services.OwnedMaps;
        long[] systemTables =
        [
            await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysACEs", Ct),
            await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysQueries", Ct),
            await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysRelationships", Ct),
        ];
        long userTable = await TDefPageOfAsync(harness, "Orders");
        Assert.All(systemTables, tdefPage => Assert.True(tdefPage > 2, "A system table was not found."));

        foreach (long tdefPage in systemTables)
        {
            counting.Reset();
            Assert.False(await policy.CanMaintainAsync(tdefPage, Ct));
            Assert.True(counting.BytesRead > 0, "The first answer for a system table did not scan MSysObjects.");

            counting.Reset();
            Assert.False(await policy.CanMaintainAsync(tdefPage, Ct));
            Assert.True(
                counting.BytesRead == 0,
                $"The repeated answer for TDEF page {tdefPage} read pages {string.Join(", ", counting.PagesRead(harness.Database.Format.PageSize).Order())}.");
        }

        Assert.True(await policy.CanMaintainAsync(userTable, Ct));
        counting.Reset();
        Assert.True(await policy.CanMaintainAsync(userTable, Ct));
        Assert.False(await policy.CanMaintainAsync(2, Ct));
        Assert.False(await policy.CanMaintainAsync(0, Ct));
        Assert.True(counting.BytesRead == 0, "A kept answer, MSysObjects' own map or page 0 read pages.");

        OwnedMapPolicyState state = policy.Capture();
        Assert.Equal(systemTables.Order(), state.RefusedTdefs.Order());
        Assert.Equal(userTable, Assert.Single(state.WritableTdefs));
    }

    /// <summary>
    /// A table the writer creates where it refused before, such as a TDEF page
    /// reused after a drop, is writable: registering it overrides the refusal.
    /// </summary>
    [Fact]
    public async Task RegisterWritable_OverridesARefusal()
    {
        await using MemoryStream stream = await CreateDatabaseAsync(DatabaseFormat.AceAccdb);
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct);
        CatalogOwnedMapPolicy policy = harness.Services.OwnedMaps;
        long unnamedPage = harness.Database.Pages.PageCount + 10;

        Assert.False(await policy.CanMaintainAsync(unnamedPage, Ct));
        Assert.Contains(unnamedPage, policy.Capture().RefusedTdefs);

        policy.RegisterWritable(unnamedPage);
        Assert.True(await policy.CanMaintainAsync(unnamedPage, Ct));
        OwnedMapPolicyState state = policy.Capture();
        Assert.Contains(unnamedPage, state.WritableTdefs);
        Assert.DoesNotContain(unnamedPage, state.RefusedTdefs);
    }

    /// <summary>
    /// A table that a rolled-back transaction created is forgotten: the
    /// policy's sets go back to what they held when the transaction began, the
    /// table's TDEF page is decided afresh from the catalog, and a table the
    /// writer creates after the rollback is writable again.
    /// </summary>
    [Fact]
    public async Task RolledBackTable_IsForgotten()
    {
        await using MemoryStream stream = await CreateDatabaseAsync(DatabaseFormat.AceAccdb);
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct);
        CatalogOwnedMapPolicy policy = harness.Services.OwnedMaps;
        Assert.True(await policy.CanMaintainAsync(await TDefPageOfAsync(harness, TableName), Ct));
        OwnedMapPolicyState before = policy.Capture();

        long rolledBackPage;
        await using (JetTransaction transaction = await harness.BeginTransactionAsync(Ct))
        {
            await harness.CreateTableAsync("Temp", [new ColumnDefinition("Id", typeof(int))], [], Ct);
            rolledBackPage = await TDefPageOfAsync(harness, "Temp");
            Assert.Contains(rolledBackPage, policy.Capture().WritableTdefs);
            Assert.True(await policy.CanMaintainAsync(rolledBackPage, Ct));
            await transaction.RollbackAsync(Ct);
        }

        OwnedMapPolicyState after = policy.Capture();
        Assert.Equal(before.WritableTdefs.Order(), after.WritableTdefs.Order());
        Assert.Equal(before.RefusedTdefs.Order(), after.RefusedTdefs.Order());
        Assert.Null(await harness.Services.Catalog.GetCatalogEntryAsync("Temp", Ct));
        Assert.False(await policy.CanMaintainAsync(rolledBackPage, Ct));

        await harness.CreateTableAsync("Temp", [new ColumnDefinition("Id", typeof(int))], [], Ct);
        long recreatedPage = await TDefPageOfAsync(harness, "Temp");
        Assert.True(await policy.CanMaintainAsync(recreatedPage, Ct));
        Assert.DoesNotContain(recreatedPage, policy.Capture().RefusedTdefs);
    }

    private static async Task<long> TDefPageOfAsync(WriterHarness harness, string tableName)
    {
        CatalogEntry? entry = await harness.Services.Catalog.GetCatalogEntryAsync(tableName, Ct);
        Assert.NotNull(entry);
        return entry.TDefPage;
    }

    private static async Task<MemoryStream> CreateDatabaseAsync(DatabaseFormat format)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false },
            leaveOpen: true,
            Ct))
        {
            await writer.CreateTableAsync(TableName, [new ColumnDefinition("Id", typeof(int))], Ct);
        }

        stream.Position = 0;
        return stream;
    }
}
