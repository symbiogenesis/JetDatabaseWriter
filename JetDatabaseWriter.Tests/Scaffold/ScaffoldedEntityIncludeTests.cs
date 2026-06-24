namespace JetDatabaseWriter.Tests.Scaffold;

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// End-to-end tests that exercise <c>reader.Query&lt;T&gt;(...).Include(...)</c> against
/// entities shaped exactly like the scaffolder emits them: collection navigations typed
/// as <see cref="ICollection{T}"/> (initialized to a <c>List&lt;T&gt;</c>) and reference
/// navigations named after the foreign-key column (EF-style, e.g. <c>SupplierId</c> →
/// <c>Supplier</c>), including the case of multiple foreign keys to the same parent.
/// </summary>
/// <param name="db">The shared database cache.</param>
public sealed class ScaffoldedEntityIncludeTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Fact]
    public async Task Include_Collection_TypedAsICollection_Populates()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildSingleFkAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<Supplier> suppliers = await reader.Query<Supplier>("Supplier")
            .Include(s => s.Items)
            .ToListAsync(ct);

        suppliers.Sort((a, b) => a.SupplierId.CompareTo(b.SupplierId));
        Assert.Equal(2, suppliers.Count);
        Assert.Equal(2, suppliers[0].Items.Count);
        Assert.Single(suppliers[1].Items);
        Assert.All(suppliers[0].Items, i => Assert.Equal(1, i.SupplierId));
    }

    [Fact]
    public async Task Include_Reference_NamedAfterForeignKey_Populates()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildSingleFkAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<Item> items = await reader.Query<Item>("Item")
            .Include(i => i.Supplier)
            .ToListAsync(ct);

        Assert.Equal(3, items.Count);
        Assert.All(items, i => Assert.NotNull(i.Supplier));
        Assert.Equal("Acme", items.Single(i => i.ItemId == 10).Supplier!.SupplierName);
        Assert.Equal("Globex", items.Single(i => i.ItemId == 12).Supplier!.SupplierName);
    }

    [Fact]
    public async Task Include_TwoForeignKeysToSameParent_ResolveDistinctNavigations()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildMultiFkAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<Deal> deals = await reader.Query<Deal>("Deal")
            .Include(d => d.Buyer)
            .Include(d => d.Seller)
            .ToListAsync(ct);

        Deal deal100 = deals.Single(d => d.DealId == 100);
        Deal deal101 = deals.Single(d => d.DealId == 101);

        // Each navigation must follow its own foreign-key column, not collapse to one.
        Assert.Equal("Acme", deal100.Buyer!.PartyName);
        Assert.Equal("Globex", deal100.Seller!.PartyName);
        Assert.Equal("Initech", deal101.Buyer!.PartyName);
        Assert.Equal("Acme", deal101.Seller!.PartyName);
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream, CancellationToken ct)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream, CancellationToken ct)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
    }

    private async Task<MemoryStream> BuildSingleFkAsync(CancellationToken ct)
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, ct);
        await using AccessWriter writer = await OpenWriterAsync(temp, ct);

        await writer.CreateTableAsync(
            "Supplier",
            [new("SupplierId", typeof(int)) { IsPrimaryKey = true }, new("SupplierName", typeof(string), maxLength: 50)],
            ct);
        await writer.CreateTableAsync(
            "Item",
            [new("ItemId", typeof(int)) { IsPrimaryKey = true }, new("SupplierId", typeof(int)), new("ItemName", typeof(string), maxLength: 50)],
            ct);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_Item_Supplier", "Supplier", "SupplierId", "Item", "SupplierId"),
            ct);

        await writer.InsertRowsAsync(
            "Supplier",
            [[1, "Acme"], [2, "Globex"]],
            ct);
        await writer.InsertRowsAsync(
            "Item",
            [[10, 1, "a1"], [11, 1, "a2"], [12, 2, "g1"]],
            ct);

        return temp;
    }

    private async Task<MemoryStream> BuildMultiFkAsync(CancellationToken ct)
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, ct);
        await using AccessWriter writer = await OpenWriterAsync(temp, ct);

        await writer.CreateTableAsync(
            "Party",
            [new("PartyId", typeof(int)) { IsPrimaryKey = true }, new("PartyName", typeof(string), maxLength: 50)],
            ct);
        await writer.CreateTableAsync(
            "Deal",
            [
                new("DealId", typeof(int)) { IsPrimaryKey = true },
                new("BuyerId", typeof(int)),
                new("SellerId", typeof(int)),
                new("DealName", typeof(string), maxLength: 50),
            ],
            ct);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_Deal_Buyer", "Party", "PartyId", "Deal", "BuyerId"),
            ct);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_Deal_Seller", "Party", "PartyId", "Deal", "SellerId"),
            ct);

        await writer.InsertRowsAsync(
            "Party",
            [[1, "Acme"], [2, "Globex"], [3, "Initech"]],
            ct);
        await writer.InsertRowsAsync(
            "Deal",
            [[100, 1, 2, "D1"], [101, 3, 1, "D2"]],
            ct);

        return temp;
    }

    internal sealed class Supplier
    {
        public int SupplierId { get; set; }

        public string SupplierName { get; set; } = string.Empty;

        public ICollection<Item> Items { get; set; } = [];
    }

    internal sealed class Item
    {
        public int ItemId { get; set; }

        public int SupplierId { get; set; }

        public string ItemName { get; set; } = string.Empty;

        public Supplier? Supplier { get; set; }
    }

    internal sealed class Party
    {
        public int PartyId { get; set; }

        public string PartyName { get; set; } = string.Empty;
    }

    internal sealed class Deal
    {
        public int DealId { get; set; }

        public int BuyerId { get; set; }

        public int SellerId { get; set; }

        public string DealName { get; set; } = string.Empty;

        public Party? Buyer { get; set; }

        public Party? Seller { get; set; }
    }
}
