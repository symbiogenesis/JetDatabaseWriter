namespace JetDatabaseWriter.Tests.Queries;

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Linq;
using JetDatabaseWriter.Mapping;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Queries;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Tests that every POCO path (typed inserts, <c>Rows&lt;T&gt;</c>, <c>ReadTableAsync&lt;T&gt;</c>,
/// index reads, and the LINQ <c>Where</c> / <c>OrderBy</c> / <c>Include</c> translation) binds a
/// property to its column through one mapping model: <c>[Column("...")]</c> renames the column a
/// property maps to, <c>[NotMapped]</c> excludes a property, and <c>[Table("...")]</c> names the
/// table of an <c>Include</c> target. The tables use column names with spaces (<c>Last Name</c>,
/// <c>Person ID</c>), which a C# property name cannot spell, so the attribute is the only way the
/// property can bind.
/// </summary>
/// <param name="db">The shared database cache.</param>
public sealed class EntityColumnMappingTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private const string Table = "People";

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task InsertRowAsync_ColumnAttribute_WritesRenamedColumn_AndSkipsNotMapped(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreatePeopleAsync(format, ct);

        await using (AccessWriter writer = await OpenWriterAsync(ms, ct))
        {
            await writer.InsertRowAsync(Table, new Person { PersonId = 2, LastName = "Jones", Note = "not stored" }, ct);
            await writer.InsertRowsAsync(Table, [new Person { PersonId = 3, LastName = "Brown", Note = "not stored" }], ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        Dictionary<int, object[]> rows = [];
        await foreach (object[] row in reader.Rows(Table, cancellationToken: ct))
        {
            rows[(int)row[0]] = row;
        }

        Assert.Equal("Jones", rows[2][1]);
        Assert.Equal("Brown", rows[3][1]);

        // [NotMapped] Note is never written, so the Note column stays null.
        Assert.IsType<DBNull>(rows[2][2]);
        Assert.IsType<DBNull>(rows[3][2]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Rows_ColumnAttribute_ReadsRenamedColumn_AndLeavesNotMappedAtDefault(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateSeededPeopleAsync(format, ct);
        await using AccessReader reader = await OpenReaderAsync(ms, ct);

        List<Person> people = await reader.Rows<Person>(Table, cancellationToken: ct).ToListAsync(ct);
        Person smith = Assert.Single(people, p => p.PersonId == 1);
        Assert.Equal("Smith", smith.LastName);
        Assert.Null(smith.Note);

        // A POCO that maps every column (one through an explicit rename of a plain
        // name) goes through the same rule.
        List<PersonWithRemark> remarks = await reader.Rows<PersonWithRemark>(Table, cancellationToken: ct).ToListAsync(ct);
        PersonWithRemark first = Assert.Single(remarks, p => p.PersonId == 1);
        Assert.Equal("Smith", first.LastName);
        Assert.Equal("n1", first.Remark);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ReadTableAsync_ColumnAttribute_ReadsRenamedColumn(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateSeededPeopleAsync(format, ct);
        await using AccessReader reader = await OpenReaderAsync(ms, ct);

        // Person maps two of the three columns (projected read path).
        IReadOnlyList<Person> projected = await reader.ReadTableAsync<Person>(Table, cancellationToken: ct);
        Assert.Equal(3, projected.Count);
        Assert.Equal("Smith", projected.Single(p => p.PersonId == 1).LastName);
        Assert.All(projected, p => Assert.Null(p.Note));

        // PersonWithRemark maps all three columns (full-row read path).
        IReadOnlyList<PersonWithRemark> full = await reader.ReadTableAsync<PersonWithRemark>(Table, cancellationToken: ct);
        Assert.Equal(3, full.Count);
        PersonWithRemark jones = full.Single(p => p.PersonId == 2);
        Assert.Equal("Jones", jones.LastName);
        Assert.Equal("n2", jones.Remark);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Query_Where_OnColumnAttributeProperty_MatchesRow(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateSeededPeopleAsync(format, ct);
        await using AccessReader reader = await OpenReaderAsync(ms, ct);

        Assert.Equal(1, reader.Query<Person>(Table).Where(p => p.LastName == "Smith").Count());

        List<Person> viaQuery = await reader.Query<Person>(Table).Where(p => p.LastName == "Jones").ToListAsync(ct);
        Assert.Equal(2, Assert.Single(viaQuery).PersonId);

        List<Person> viaPredicate = await reader.Rows<Person>(Table, p => p.LastName == "Adams", cancellationToken: ct).ToListAsync(ct);
        Assert.Equal(3, Assert.Single(viaPredicate).PersonId);

        List<Person> byKey = await reader.Query<Person>(Table).Where(p => p.PersonId == 2).ToListAsync(ct);
        Assert.Equal("Jones", Assert.Single(byKey).LastName);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task FromIndex_ColumnAttribute_MapsRenamedColumn(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateSeededPeopleAsync(format, ct);
        await using AccessReader reader = await OpenReaderAsync(ms, ct);

        List<Person> hits = await reader.FromIndex<Person>(Table, "IX_LastName").WhereEquals("Smith").ToRowsAsync(ct).ToListAsync(ct);

        Person smith = Assert.Single(hits);
        Assert.Equal(1, smith.PersonId);
        Assert.Equal("Smith", smith.LastName);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Query_OrderBy_OnColumnAttributeProperty_SortsRows(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateSeededPeopleAsync(format, ct);
        await using AccessReader reader = await OpenReaderAsync(ms, ct);

        List<Person> byId = await reader.Query<Person>(Table).OrderByDescending(p => p.PersonId).ToListAsync(ct);
        Assert.Equal([3, 2, 1], byId.Select(p => p.PersonId));
        Assert.Equal(["Adams", "Jones", "Smith"], byId.Select(p => p.LastName));

        List<Person> byName = await reader.Query<Person>(Table).OrderBy(p => p.LastName).ToListAsync(ct);
        Assert.Equal(["Adams", "Jones", "Smith"], byName.Select(p => p.LastName));
    }

    [Fact]
    public void OrderStage_FindCoveringIndex_ResolvesColumnAttribute()
    {
        var stage = new OrderStage();
        Expression<Func<Person, int>> key = p => p.PersonId;
        stage.Keys.Add(new OrderingKey(key, Descending: false));

        IndexMetadata primaryKey = new()
        {
            Name = "PrimaryKey",
            Kind = IndexKind.PrimaryKey,
            FirstDp = 1,
            Columns = [new IndexColumnReference { Name = "Person ID", IsAscending = true }],
        };

        Assert.Same(primaryKey, stage.FindCoveringIndex([primaryKey]));
    }

    [Fact]
    public void IndexPredicateTranslator_ResolvesColumnAttribute_AndSkipsNotMapped()
    {
        RowCriteria renamed = IndexPredicateTranslator.ExtractPushableCriteria((Expression<Func<Person, bool>>)(p => p.LastName == "Smith"));
        ColumnPredicate predicate = Assert.Single(renamed.Predicates);
        Assert.Equal("Last Name", predicate.ColumnName);

        // A [NotMapped] property is not a column, so it is never pushed to an index.
        RowCriteria notMapped = IndexPredicateTranslator.ExtractPushableCriteria((Expression<Func<Person, bool>>)(p => p.Note == "x"));
        Assert.Empty(notMapped.Predicates);
    }

    [Fact]
    public void EntityMap_ExplicitColumn_BeatsPropertyThatOnlyMatchesByName()
    {
        var map = EntityMap.For(typeof(ShadowedName));

        Assert.Equal(nameof(ShadowedName.DisplayName), map.FindByColumn("Name")!.Property.Name);
        Assert.Null(map.FindByMember(typeof(ShadowedName).GetProperty(nameof(ShadowedName.Name))!));
        Assert.Equal("ShadowTable", map.TableName);
    }

    [Fact]
    public async Task Rows_TwoPropertiesClaimOneColumn_ThrowsInvalidOperation()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateSeededPeopleAsync(DatabaseFormat.AceAccdb, ct);
        await using AccessReader reader = await OpenReaderAsync(ms, ct);

        // The error names both properties and is not wrapped in a TypeInitializationException,
        // so a second attempt reports the same error.
        for (int attempt = 0; attempt < 2; attempt++)
        {
            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
                async () => await reader.Rows<DuplicateColumn>(Table, cancellationToken: ct).ToListAsync(ct));
            Assert.Contains(nameof(DuplicateColumn.First), ex.Message, StringComparison.Ordinal);
            Assert.Contains(nameof(DuplicateColumn.Second), ex.Message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task Include_AcrossRenamedKeyColumns_LoadsReferenceAndCollection()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await this.BuildRelationshipAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<MapOrder> orders = await reader.Query<MapOrder>("JdwMapOrder").Include(o => o.Customer).ToListAsync(ct);
        Assert.Equal(9, orders.Count);
        Assert.All(orders, o => Assert.Equal(o.CustomerId, o.Customer!.CustomerId));
        Assert.Equal("Bob", orders.Single(o => o.OrderId == 108).Customer!.FullName);

        List<MapCustomer> customers = await reader.Query<MapCustomer>("JdwMapCustomer").Include(c => c.Orders).ToListAsync(ct);
        customers.Sort((a, b) => a.CustomerId.CompareTo(b.CustomerId));
        Assert.Equal(8, customers[0].Orders.Count);
        Assert.Single(customers[1].Orders);

        // One root key against nine child rows takes the foreign-key index seek path.
        List<MapCustomer> alice = await reader.Query<MapCustomer>("JdwMapCustomer")
            .Where(c => c.CustomerId == 1)
            .Include(c => c.Orders)
            .ToListAsync(ct);
        Assert.Equal("Alice", Assert.Single(alice).FullName);
        Assert.Equal(8, alice[0].Orders.Count);
        Assert.All(alice[0].Orders, o => Assert.Equal(1, o.CustomerId));
    }

    private static async Task<MemoryStream> CreatePeopleAsync(DatabaseFormat format, CancellationToken ct)
    {
        var ms = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        await writer.CreateTableAsync(
            Table,
            [
                new("Person ID", typeof(int)) { IsPrimaryKey = true },
                new("Last Name", typeof(string), maxLength: 50),
                new("Note", typeof(string), maxLength: 50),
            ],
            [new IndexDefinition("IX_LastName", "Last Name")],
            ct);
        return ms;
    }

    private static async Task<MemoryStream> CreateSeededPeopleAsync(DatabaseFormat format, CancellationToken ct)
    {
        MemoryStream ms = await CreatePeopleAsync(format, ct);
        await using AccessWriter writer = await OpenWriterAsync(ms, ct);
        await writer.InsertRowsAsync(Table, [[2, "Jones", "n2"], [1, "Smith", "n1"], [3, "Adams", "n3"]], ct);
        return ms;
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

    private async Task<MemoryStream> BuildRelationshipAsync(CancellationToken ct)
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, ct);
        await using AccessWriter writer = await OpenWriterAsync(temp, ct);

        await writer.CreateTableAsync(
            "JdwMapCustomer",
            [new("Customer ID", typeof(int)) { IsPrimaryKey = true }, new("Full Name", typeof(string), maxLength: 50)],
            ct);
        await writer.CreateTableAsync(
            "JdwMapOrder",
            [new("Order ID", typeof(int)) { IsPrimaryKey = true }, new("Customer ID", typeof(int)), new("Amount", typeof(int))],
            ct);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_JdwMapOrder_JdwMapCustomer", "JdwMapCustomer", "Customer ID", "JdwMapOrder", "Customer ID"),
            ct);

        await writer.InsertRowsAsync("JdwMapCustomer", [[1, "Alice"], [2, "Bob"]], ct);
        var orders = new List<object[]>();
        for (int i = 0; i < 8; i++)
        {
            orders.Add([100 + i, 1, i * 10]);
        }

        orders.Add([108, 2, 5]);
        await writer.InsertRowsAsync("JdwMapOrder", orders, ct);
        return temp;
    }

    internal sealed class Person
    {
        [Column("Person ID")]
        public int PersonId { get; set; }

        [Column("Last Name")]
        public string? LastName { get; set; }

        [NotMapped]
        public string? Note { get; set; }
    }

    internal sealed class PersonWithRemark
    {
        [Column("Person ID")]
        public int PersonId { get; set; }

        [Column("Last Name")]
        public string? LastName { get; set; }

        [Column("Note")]
        public string? Remark { get; set; }
    }

#pragma warning disable CA1812 // Only inspected by EntityMap, never instantiated.
    [Table("ShadowTable")]
    internal sealed class ShadowedName
#pragma warning restore CA1812
    {
        public string? Name { get; set; }

        [Column("Name")]
        public string? DisplayName { get; set; }
    }

    internal sealed class DuplicateColumn
    {
        [Column("Last Name")]
        public string? First { get; set; }

        [Column("last name")]
        public string? Second { get; set; }
    }

    [Table("JdwMapCustomer")]
    internal sealed class MapCustomer
    {
        [Column("Customer ID")]
        public int CustomerId { get; set; }

        [Column("Full Name")]
        public string FullName { get; set; } = string.Empty;

        public List<MapOrder> Orders { get; set; } = [];
    }

    [Table("JdwMapOrder")]
    internal sealed class MapOrder
    {
        [Column("Order ID")]
        public int OrderId { get; set; }

        [Column("Customer ID")]
        public int CustomerId { get; set; }

        public int Amount { get; set; }

        public MapCustomer? Customer { get; set; }
    }
}
