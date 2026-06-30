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
/// Regression tests for relationship-inferred eager loading (<c>Include</c>) when the
/// POCO join-key property uses a different numeric CLR type than the underlying Access
/// column. The join keys here are declared as <c>double</c> while the database stores
/// them as Long Integer, so the in-memory key normalization must compare by value, not
/// CLR type; otherwise the scan-path group/lookup maps never line up and the navigation
/// silently comes back empty (the §1b defect). Table names match the POCO type names so
/// the query's name convention resolves the relationship.
/// </summary>
public sealed class EntityQueryKeyTypeTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Fact]
    public async Task Include_Reference_MatchesWhenChildKeyIsDoubleAndParentColumnIsInteger()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<JdwChild> children = await reader.Query<JdwChild>("JdwChild")
            .Include(c => c.Parent)
            .ToListAsync(ct);

        Assert.Equal(3, children.Count);
        Assert.All(children, c => Assert.NotNull(c.Parent));

        JdwChild first = children.Single(c => c.Id == 10);
        Assert.NotNull(first.Parent);
        Assert.Equal(1d, first.Parent.Id);
        Assert.Equal("Alice", first.Parent.Name);
    }

    [Fact]
    public async Task Include_Collection_MatchesWhenParentKeyIsDoubleAndChildColumnIsInteger()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<JdwParent> parents = await reader.Query<JdwParent>("JdwParent")
            .Include(p => p.Children)
            .ToListAsync(ct);

        parents.Sort((a, b) => a.Id.CompareTo(b.Id));
        Assert.Equal(2, parents.Count);
        Assert.Equal(2, parents[0].Children.Count);
        Assert.Single(parents[1].Children);
        Assert.All(parents[0].Children, c => Assert.Equal(parents[0].Id, c.ParentId));
    }

    private async Task<MemoryStream> BuildAsync(CancellationToken ct)
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, ct);
        await using AccessWriter writer = await OpenWriterAsync(temp, ct);

        await writer.CreateTableAsync(
            "JdwParent",
            [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Name", typeof(string), maxLength: 50)],
            ct);
        await writer.CreateTableAsync(
            "JdwChild",
            [new("Id", typeof(int)) { IsPrimaryKey = true }, new("ParentId", typeof(int)), new("Label", typeof(string), maxLength: 50)],
            ct);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_JdwChild_JdwParent", "JdwParent", "Id", "JdwChild", "ParentId"),
            ct);

        await writer.InsertRowsAsync("JdwParent", new[] { new object[] { 1, "Alice" }, new object[] { 2, "Bob" } }, ct);
        await writer.InsertRowsAsync(
            "JdwChild",
            new[] { new object[] { 10, 1, "a1" }, new object[] { 11, 1, "a2" }, new object[] { 12, 2, "b1" } },
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

    public sealed class JdwParent
    {
        public double Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public List<JdwChild> Children { get; set; } = [];
    }

    public sealed class JdwChild
    {
        public int Id { get; set; }

        public double ParentId { get; set; }

        public string Label { get; set; } = string.Empty;

        public JdwParent? Parent { get; set; }
    }
}
