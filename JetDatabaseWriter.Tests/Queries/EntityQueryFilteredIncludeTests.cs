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
/// End-to-end tests for EF-style filtered / ordered / paged collection includes
/// (<c>Include(c =&gt; c.Orders.Where(...).OrderBy(...).Skip(n).Take(m))</c>). The inline
/// operators apply per parent in memory, so each parent's children are filtered, ordered,
/// and paged independently, and a following <c>ThenInclude</c> descends only into the kept
/// rows. Table names match the POCO type names so the query's name convention resolves the
/// relationship.
/// </summary>
public sealed class EntityQueryFilteredIncludeTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Fact]
    public async Task FilteredInclude_Where_KeepsMatchingChildrenPerParent()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<FiCustomer> customers = await reader.Query<FiCustomer>("FiCustomer")
            .Include(c => c.Orders.Where(o => o.Amount >= 100))
            .ToListAsync(ct);

        FiCustomer alice = customers.Single(c => c.Id == 1);
        FiCustomer bob = customers.Single(c => c.Id == 2);

        // Alice keeps 200, 100, 150, 125, 100 (amounts >= 100); Bob keeps only 300.
        Assert.Equal(5, alice.Orders.Count);
        Assert.All(alice.Orders, o => Assert.True(o.Amount >= 100));
        FiOrder onlyBob = Assert.Single(bob.Orders);
        Assert.Equal(20, onlyBob.Id);
    }

    [Fact]
    public async Task OrderedInclude_Take_KeepsTopChildrenPerParent()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<FiCustomer> customers = await reader.Query<FiCustomer>("FiCustomer")
            .Include(c => c.Orders.OrderByDescending(o => o.Amount).Take(2))
            .ToListAsync(ct);

        FiCustomer alice = customers.Single(c => c.Id == 1);
        FiCustomer bob = customers.Single(c => c.Id == 2);

        // Take is per parent: Alice's top two by amount and Bob's top two are both kept
        // (a global Take(2) would have returned two rows in total).
        Assert.Equal([11, 13], alice.Orders.Select(o => o.Id));
        Assert.Equal([20, 21], bob.Orders.Select(o => o.Id));
    }

    [Fact]
    public async Task FilteredOrderedPagedInclude_AppliesWhereOrderSkipTakeInOrder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<FiCustomer> customers = await reader.Query<FiCustomer>("FiCustomer")
            .Include(c => c.Orders
                .Where(o => o.Amount >= 120)
                .OrderBy(o => o.Amount)
                .Skip(1)
                .Take(2))
            .ToListAsync(ct);

        FiCustomer alice = customers.Single(c => c.Id == 1);
        FiCustomer bob = customers.Single(c => c.Id == 2);

        // Alice >= 120 ascending: 125, 150, 200; Skip(1).Take(2) keeps 150, 200.
        Assert.Equal([13, 11], alice.Orders.Select(o => o.Id));

        // Bob >= 120 ascending: just 300; Skip(1) drops it, so the collection is empty.
        Assert.Empty(bob.Orders);
    }

    [Fact]
    public async Task OrderedInclude_OrderByThenBy_AppliesCompositeOrder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<FiCustomer> customers = await reader.Query<FiCustomer>("FiCustomer")
            .Include(c => c.Orders.OrderBy(o => o.Amount).ThenBy(o => o.Id))
            .ToListAsync(ct);

        FiCustomer alice = customers.Single(c => c.Id == 1);

        // By amount ascending, breaking the 100 tie (orders 12 and 16) by id ascending.
        Assert.Equal([10, 14, 12, 16, 15, 13, 11], alice.Orders.Select(o => o.Id));
    }

    [Fact]
    public async Task FilteredInclude_ThenInclude_DescendsOnlyIntoKeptRows()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<FiCustomer> customers = await reader.Query<FiCustomer>("FiCustomer")
            .Include(c => c.Orders.OrderByDescending(o => o.Amount).Take(2))
            .ThenInclude(o => o.Customer)
            .ToListAsync(ct);

        FiCustomer alice = customers.Single(c => c.Id == 1);

        // The kept orders (and only those) get their back-reference loaded, resolving to the
        // parent by key — proving the ThenInclude descended into the filtered child set.
        Assert.Equal([11, 13], alice.Orders.Select(o => o.Id));
        Assert.All(alice.Orders, o =>
        {
            Assert.NotNull(o.Customer);
            Assert.Equal(alice.Id, o.Customer!.Id);
        });
    }

    [Fact]
    public async Task FilteredInclude_AfterRootWhere_AppliesBothFilters()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<FiCustomer> customers = await reader.Query<FiCustomer>("FiCustomer")
            .Where(c => c.Id == 1)
            .Include(c => c.Orders.Where(o => o.Amount >= 150))
            .ToListAsync(ct);

        FiCustomer alice = Assert.Single(customers);
        Assert.Equal(1, alice.Id);

        // Only Alice's orders with amount >= 150 (200 and 150) are kept.
        Assert.Equal(2, alice.Orders.Count);
        Assert.All(alice.Orders, o => Assert.True(o.Amount >= 150));
        Assert.Equal([11, 13], alice.Orders.Select(o => o.Id).OrderBy(id => id));
    }

    [Fact]
    public async Task FilteredThenInclude_AppliesOperatorsAtNestedLevel()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<FiRegion> regions = await reader.Query<FiRegion>("FiRegion")
            .Include(r => r.Customers)
            .ThenInclude(c => c.Orders.Where(o => o.Amount >= 150).OrderByDescending(o => o.Amount))
            .ToListAsync(ct);

        FiRegion north = Assert.Single(regions);
        Assert.Equal(2, north.Customers.Count);

        FiCustomer alice = north.Customers.Single(c => c.Id == 1);
        FiCustomer bob = north.Customers.Single(c => c.Id == 2);

        // The filter/order on the nested ThenInclude collection apply per customer: Alice's
        // orders >= 150 in descending amount, Bob's the single qualifying order.
        Assert.Equal([11, 13], alice.Orders.Select(o => o.Id));
        FiOrder onlyBob = Assert.Single(bob.Orders);
        Assert.Equal(20, onlyBob.Id);
    }

    private async Task<MemoryStream> BuildAsync(CancellationToken ct)
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, ct);
        await using AccessWriter writer = await OpenWriterAsync(temp, ct);

        await writer.CreateTableAsync(
            "FiRegion",
            [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Name", typeof(string), maxLength: 50)],
            ct);
        await writer.CreateTableAsync(
            "FiCustomer",
            [new("Id", typeof(int)) { IsPrimaryKey = true }, new("RegionId", typeof(int)), new("Name", typeof(string), maxLength: 50)],
            ct);
        await writer.CreateTableAsync(
            "FiOrder",
            [new("Id", typeof(int)) { IsPrimaryKey = true }, new("CustomerId", typeof(int)), new("Amount", typeof(int))],
            ct);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_FiCustomer_FiRegion", "FiRegion", "Id", "FiCustomer", "RegionId"),
            ct);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_FiOrder_FiCustomer", "FiCustomer", "Id", "FiOrder", "CustomerId"),
            ct);

        await writer.InsertRowsAsync(
            "FiRegion",
            new[] { new object[] { 100, "North" } },
            ct);
        await writer.InsertRowsAsync(
            "FiCustomer",
            new[] { new object[] { 1, 100, "Alice" }, new object[] { 2, 100, "Bob" } },
            ct);
        await writer.InsertRowsAsync(
            "FiOrder",
            new[]
            {
                new object[] { 10, 1, 50 },
                new object[] { 11, 1, 200 },
                new object[] { 12, 1, 100 },
                new object[] { 13, 1, 150 },
                new object[] { 14, 1, 75 },
                new object[] { 15, 1, 125 },
                new object[] { 16, 1, 100 },
                new object[] { 20, 2, 300 },
                new object[] { 21, 2, 25 },
            },
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

    public sealed class FiRegion
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public List<FiCustomer> Customers { get; set; } = [];
    }

    public sealed class FiCustomer
    {
        public int Id { get; set; }

        public int RegionId { get; set; }

        public string Name { get; set; } = string.Empty;

        public FiRegion? Region { get; set; }

        public List<FiOrder> Orders { get; set; } = [];
    }

    public sealed class FiOrder
    {
        public int Id { get; set; }

        public int CustomerId { get; set; }

        public int Amount { get; set; }

        public FiCustomer? Customer { get; set; }
    }
}
