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
/// usage maps the writer may extend when it appends a data page: local user tables,
/// MSysObjects and Jet4/ACE system tables, with decisions cached by TDEF page.
/// The fixed MSysObjects root also proves map ownership, and a table
/// that a rolled-back transaction created is forgotten.
/// </summary>
public sealed class OwnedMapPolicyTests
{
    private const string TableName = "Items";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>
    /// Native and writer-created Jet3 user maps remain writable after reopen,
    /// while other imported system maps remain outside this policy.
    /// </summary>
    [Fact]
    public async Task Jet3_UserAndObjectCatalogMapsAreWritableAfterReopen()
    {
        await using (MemoryStream created = await CreateDatabaseAsync(DatabaseFormat.Jet3Mdb))
        await using (var counting = new CountingStream(created))
        await using (AccessWriter harness = await TestDatabase.OpenWriterAsync(counting, cancellationToken: Ct))
        {
            CatalogOwnedMapPolicy policy = harness.Services.OwnedMaps;
            long tdefPage = await TDefPageOfAsync(harness, TableName);

            counting.Reset();
            Assert.True(await policy.CanMaintainAsync(tdefPage, Ct));
            counting.Reset();
            policy.RegisterWritable(tdefPage);
            Assert.True(await policy.CanMaintainAsync(tdefPage, Ct));
            Assert.True(await policy.CanMaintainAsync(2, Ct));
            Assert.True(counting.BytesRead == 0, "A cached Jet3 answer read pages.");
        }

        await using var fixture = new MemoryStream();
        await fixture.WriteAsync(await File.ReadAllBytesAsync(TestDatabases.Jet3Test, Ct), Ct);
        await using AccessWriter accessAuthored = await TestDatabase.OpenWriterAsync(fixture, cancellationToken: Ct);
        List<CatalogEntry> tables = await accessAuthored.Services.Catalog.GetUserTablesAsync(Ct);
        Assert.NotEmpty(tables);
        foreach (CatalogEntry table in tables)
        {
            Assert.True(await accessAuthored.Services.OwnedMaps.CanMaintainAsync(table.TDefPage, Ct));
        }
    }

    /// <summary>
    /// Imported local Jet4/ACE system tables have writable owned maps. Their
    /// decisions are cached after one scan, as a user table's decision is.
    /// Retargeting shared maps invalidates earlier decisions; unchanged metadata
    /// permits cached answers, including the fixed MSysObjects root.
    /// </summary>
    /// <param name="jet4">Whether to use the Jet4 fixture rather than ACE.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AccessAuthoredLocalTables_AreWritableOnce_WithoutRescanningTheCatalog(bool jet4)
    {
        await using var file = new MemoryStream();
        await file.WriteAsync(await File.ReadAllBytesAsync(jet4 ? TestDatabases.TestV2003 : TestDatabases.NorthwindTraders, Ct), Ct);
        await using var counting = new CountingStream(file);

        // Measure the policy's memoization through physical reads of the catalog.
        var options = new AccessWriterOptions { PageCacheSize = 0, UseLockFile = false, UseByteRangeLocks = false };
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(counting, options, cancellationToken: Ct);
        CatalogOwnedMapPolicy policy = harness.Services.OwnedMaps;
        long[] systemTables =
        [
            await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysACEs", Ct),
            await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysQueries", Ct),
            await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysRelationships", Ct),
        ];
        long userTable = await TDefPageOfAsync(harness, jet4 ? "Table4" : "Orders");
        Assert.All(systemTables, tdefPage => Assert.True(tdefPage > 2, "A system table was not found."));

        foreach (long tdefPage in systemTables)
        {
            counting.Reset();
            Assert.True(await policy.CanMaintainAsync(tdefPage, Ct));
            Assert.True(counting.BytesRead > 0, "The first answer for a system table did not scan MSysObjects.");

            counting.Reset();
            Assert.True(await policy.CanMaintainAsync(tdefPage, Ct));
            Assert.True(
                counting.BytesRead == 0,
                $"The repeated answer for TDEF page {tdefPage} read pages {string.Join(", ", counting.PagesRead(harness.Database.Format.PageSize).Order())}.");
        }

        Assert.True(await policy.CanMaintainAsync(userTable, Ct));
        Assert.True(await policy.CanMaintainAsync(2, Ct));
        Assert.True(await policy.CanMaintainAsync(userTable, Ct));
        counting.Reset();
        Assert.True(await policy.CanMaintainAsync(userTable, Ct));
        Assert.True(await policy.CanMaintainAsync(2, Ct));
        Assert.False(await policy.CanMaintainAsync(0, Ct));
        Assert.True(counting.BytesRead == 0, "A kept answer, MSysObjects' own map or page 0 read pages.");

        OwnedMapPolicyState state = policy.Capture();
        Assert.Empty(state.RefusedTdefs);
        Assert.Contains(userTable, state.WritableTdefs);
        Assert.Contains(2L, state.WritableTdefs);
        Assert.All(state.WritableTdefs, root => Assert.Contains(root, systemTables.Append(userTable).Append(2L)));
    }

    /// <summary>
    /// A table the writer creates where it refused before, such as a TDEF page
    /// reused after a drop, is writable: registering it overrides the refusal.
    /// </summary>
    [Fact]
    public async Task RegisterWritable_OverridesARefusal()
    {
        await using MemoryStream stream = await CreateDatabaseAsync(DatabaseFormat.AceAccdb);
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
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
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        CatalogOwnedMapPolicy policy = harness.Services.OwnedMaps;
        Assert.True(await policy.CanMaintainAsync(await TDefPageOfAsync(harness, TableName), Ct));

        long rolledBackPage;
        await using (JetTransaction transaction = await harness.BeginTransactionAsync(Ct))
        {
            await harness.CreateTableAsync("Temp", [new ColumnDefinition("Id", typeof(int))], [], Ct);
            rolledBackPage = await TDefPageOfAsync(harness, "Temp");
            Assert.True(await policy.CanMaintainAsync(rolledBackPage, Ct));
            Assert.Contains(rolledBackPage, policy.Capture().WritableTdefs);
            await transaction.RollbackAsync(Ct);
        }

        OwnedMapPolicyState after = policy.Capture();
        Assert.DoesNotContain(rolledBackPage, after.WritableTdefs);
        Assert.Null(await harness.Services.Catalog.GetCatalogEntryAsync("Temp", Ct));
        Assert.False(await policy.CanMaintainAsync(rolledBackPage, Ct));
        Assert.True(await policy.CanMaintainAsync(await TDefPageOfAsync(harness, TableName), Ct));

        await harness.CreateTableAsync("Temp", [new ColumnDefinition("Id", typeof(int))], [], Ct);
        long recreatedPage = await TDefPageOfAsync(harness, "Temp");
        Assert.True(await policy.CanMaintainAsync(recreatedPage, Ct));
        Assert.DoesNotContain(recreatedPage, policy.Capture().RefusedTdefs);
    }

    private static async Task<long> TDefPageOfAsync(AccessWriter harness, string tableName)
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
