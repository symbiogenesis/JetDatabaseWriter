namespace JetDatabaseWriter.Tests.Queries;

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
/// End-to-end tests for nested eager loading (<c>Include(...).ThenInclude(...)</c>) over a
/// three-level relationship chain: <c>JdwRegion</c> (parent) → <c>JdwCustomer</c> →
/// <c>JdwOrder</c> (child). Covers reference-then-reference, collection-then-collection,
/// collection-then-reference, shared-prefix merging (one <c>Include</c> loaded once for two
/// <c>ThenInclude</c> branches), and a leading <c>Where</c> filter ahead of the chain. Table
/// names match the POCO type names so the query's name convention resolves each relationship.
/// </summary>
public sealed class EntityQueryThenIncludeTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Fact]
    public async Task ThenInclude_ReferenceThenReference_LoadsNestedParent()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<JdwOrder> orders = await reader.Query<JdwOrder>("JdwOrder")
            .Include(o => o.Customer)
            .ThenInclude(c => c!.Region)
            .ToListAsync(ct);

        Assert.Equal(3, orders.Count);
        JdwOrder first = orders.Single(o => o.Id == 1);
        Assert.NotNull(first.Customer);
        Assert.Equal("Alice", first.Customer!.Name);
        Assert.NotNull(first.Customer.Region);
        Assert.Equal("North", first.Customer.Region!.Name);

        JdwOrder third = orders.Single(o => o.Id == 3);
        Assert.Equal("Bob", third.Customer!.Name);
        Assert.Equal("South", third.Customer.Region!.Name);
    }

    [Fact]
    public async Task ThenInclude_SharesCustomerInstanceAcrossOrders()
    {
        // Orders 1 and 2 reference customer 10: the reference loader hands both the same
        // customer instance, so the nested Region loads once and is visible through both.
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<JdwOrder> orders = await reader.Query<JdwOrder>("JdwOrder")
            .Include(o => o.Customer)
            .ThenInclude(c => c!.Region)
            .ToListAsync(ct);

        JdwOrder first = orders.Single(o => o.Id == 1);
        JdwOrder second = orders.Single(o => o.Id == 2);
        Assert.Same(first.Customer, second.Customer);
        Assert.Same(first.Customer!.Region, second.Customer!.Region);
        Assert.Equal("North", second.Customer.Region!.Name);
    }

    [Fact]
    public async Task ThenInclude_CollectionThenCollection_LoadsNestedChildren()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<JdwRegion> regions = await reader.Query<JdwRegion>("JdwRegion")
            .Include(r => r.Customers)
            .ThenInclude(c => c.Orders)
            .ToListAsync(ct);

        regions.Sort((a, b) => a.Id.CompareTo(b.Id));
        Assert.Equal(2, regions.Count);

        JdwRegion north = regions.Single(r => r.Name == "North");
        JdwCustomer alice = Assert.Single(north.Customers);
        Assert.Equal("Alice", alice.Name);
        Assert.Equal(2, alice.Orders.Count);
        Assert.All(alice.Orders, o => Assert.Equal(alice.Id, o.CustomerId));

        JdwRegion south = regions.Single(r => r.Name == "South");
        JdwCustomer bob = Assert.Single(south.Customers);
        Assert.Single(bob.Orders);
    }

    [Fact]
    public async Task ThenInclude_CollectionThenReference_LoadsBackReference()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<JdwRegion> regions = await reader.Query<JdwRegion>("JdwRegion")
            .Include(r => r.Customers)
            .ThenInclude(c => c.Region)
            .ToListAsync(ct);

        JdwRegion north = regions.Single(r => r.Name == "North");
        JdwCustomer alice = Assert.Single(north.Customers);
        Assert.NotNull(alice.Region);
        Assert.Equal("North", alice.Region!.Name);
        Assert.Equal(north.Id, alice.Region.Id);
    }

    [Fact]
    public async Task Include_SharedPrefix_LoadsBothBranchesOnce()
    {
        // Two Include(o => o.Customer) chains with different ThenIncludes must merge: the
        // customer loads once and carries BOTH its Region (reference) and its Orders
        // (collection). Without prefix merging, the second chain would overwrite the first.
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<JdwOrder> orders = await reader.Query<JdwOrder>("JdwOrder")
            .Include(o => o.Customer).ThenInclude(c => c!.Region)
            .Include(o => o.Customer).ThenInclude(c => c!.Orders)
            .ToListAsync(ct);

        JdwOrder first = orders.Single(o => o.Id == 1);
        Assert.NotNull(first.Customer);
        Assert.NotNull(first.Customer!.Region);
        Assert.Equal("North", first.Customer.Region!.Name);
        Assert.Equal(2, first.Customer.Orders.Count);
    }

    [Fact]
    public async Task Where_FiltersRoots_ThenLoadsChain()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<JdwOrder> orders = await reader.Query<JdwOrder>("JdwOrder")
            .Where(o => o.Id == 3)
            .Include(o => o.Customer)
            .ThenInclude(c => c!.Region)
            .ToListAsync(ct);

        JdwOrder only = Assert.Single(orders);
        Assert.Equal(3, only.Id);
        Assert.Equal("Bob", only.Customer!.Name);
        Assert.Equal("South", only.Customer.Region!.Name);
    }

    private async Task<MemoryStream> BuildAsync(CancellationToken ct)
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, ct);
        await using AccessWriter writer = await OpenWriterAsync(temp, ct);

        await writer.CreateTableAsync(
            "JdwRegion",
            [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Name", typeof(string), maxLength: 50)],
            ct);
        await writer.CreateTableAsync(
            "JdwCustomer",
            [new("Id", typeof(int)) { IsPrimaryKey = true }, new("RegionId", typeof(int)), new("Name", typeof(string), maxLength: 50)],
            ct);
        await writer.CreateTableAsync(
            "JdwOrder",
            [new("Id", typeof(int)) { IsPrimaryKey = true }, new("CustomerId", typeof(int)), new("Label", typeof(string), maxLength: 50)],
            ct);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_JdwCustomer_JdwRegion", "JdwRegion", "Id", "JdwCustomer", "RegionId"),
            ct);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_JdwOrder_JdwCustomer", "JdwCustomer", "Id", "JdwOrder", "CustomerId"),
            ct);

        await writer.InsertRowsAsync(
            "JdwRegion",
            new[] { new object[] { 100, "North" }, new object[] { 101, "South" } },
            ct);
        await writer.InsertRowsAsync(
            "JdwCustomer",
            new[] { new object[] { 10, 100, "Alice" }, new object[] { 11, 101, "Bob" } },
            ct);
        await writer.InsertRowsAsync(
            "JdwOrder",
            new[] { new object[] { 1, 10, "o1" }, new object[] { 2, 10, "o2" }, new object[] { 3, 11, "o3" } },
            ct);

        return temp;
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

    public sealed class JdwRegion
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public List<JdwCustomer> Customers { get; set; } = [];
    }

    public sealed class JdwCustomer
    {
        public int Id { get; set; }

        public int RegionId { get; set; }

        public string Name { get; set; } = string.Empty;

        public JdwRegion? Region { get; set; }

        public List<JdwOrder> Orders { get; set; } = [];
    }

    public sealed class JdwOrder
    {
        public int Id { get; set; }

        public int CustomerId { get; set; }

        public string Label { get; set; } = string.Empty;

        public JdwCustomer? Customer { get; set; }
    }
}
