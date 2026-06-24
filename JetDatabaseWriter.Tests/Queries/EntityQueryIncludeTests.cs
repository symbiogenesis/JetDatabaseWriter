namespace JetDatabaseWriter.Tests.Queries;

using System;
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
/// End-to-end tests for the EF-style entity query (<c>reader.Query&lt;T&gt;(...)</c>):
/// confirms relationship-inferred eager loading via <c>Include</c> for both child
/// collections and parent references, that <c>Where</c> filters roots before loading,
/// plain streaming without includes, and the failure mode when no relationship can be
/// inferred. Tables use distinctive names so their POCO type names map back to the
/// related table by the query's name convention.
/// </summary>
public sealed class EntityQueryIncludeTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Fact]
    public async Task Include_Collection_LoadsChildrenGroupedByForeignKey()
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

    [Fact]
    public async Task Include_Reference_LoadsParentByKey()
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
        Assert.Equal(1, first.Parent.Id);
        Assert.Equal("Alice", first.Parent.Name);
    }

    [Fact]
    public async Task Where_FiltersRoots_BeforeInclude()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<JdwParent> parents = await reader.Query<JdwParent>("JdwParent")
            .Where(p => p.Id == 1)
            .Include(p => p.Children)
            .ToListAsync(ct);

        JdwParent only = Assert.Single(parents);
        Assert.Equal(1, only.Id);
        Assert.Equal(2, only.Children.Count);
    }

    [Fact]
    public async Task Query_WithoutInclude_StreamsRows()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        var ids = new List<int>();
        await foreach (JdwParent parent in reader.Query<JdwParent>("JdwParent").AsAsyncEnumerable().WithCancellation(ct))
        {
            ids.Add(parent.Id);
        }

        Assert.Equal(2, ids.Count);
        Assert.Contains(1, ids);
        Assert.Contains(2, ids);
    }

    [Fact]
    public async Task Include_UnrelatedType_Throws()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await reader.Query<JdwChild>("JdwChild").Include(c => c.Stray).ToListAsync(ct));
    }

    [Fact]
    public async Task Include_Reference_UsesIndexSeek_WhenKeysAreFewRelativeToTable()
    {
        // 25 parents, 2 children: the distinct foreign keys are a small share of the
        // parent table, so the cost guard resolves parents via index seeks.
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildSizedAsync(parentCount: 25, childCount: 2, ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<JdwChild> children = await reader.Query<JdwChild>("JdwChild").Include(c => c.Parent).ToListAsync(ct);

        Assert.Equal(2, children.Count);
        Assert.All(children, c => Assert.NotNull(c.Parent));
        Assert.Equal("P1", children.Single(c => c.Id == 100).Parent!.Name);
        Assert.Equal("P2", children.Single(c => c.Id == 101).Parent!.Name);
    }

    [Fact]
    public async Task Include_Collection_UsesIndexSeek_WhenKeysAreFewRelativeToTable()
    {
        // 2 parents, 12 children: the distinct parent keys are a small share of the
        // child table, so the cost guard groups children via foreign-key index seeks.
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildSizedAsync(parentCount: 2, childCount: 12, ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<JdwParent> parents = await reader.Query<JdwParent>("JdwParent").Include(p => p.Children).ToListAsync(ct);

        parents.Sort((a, b) => a.Id.CompareTo(b.Id));
        Assert.Equal(2, parents.Count);
        Assert.Equal(6, parents[0].Children.Count);
        Assert.Equal(6, parents[1].Children.Count);
        Assert.All(parents[0].Children, c => Assert.Equal(1, c.ParentId));
    }

    private async Task<MemoryStream> BuildSizedAsync(int parentCount, int childCount, CancellationToken ct)
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

        var parents = new List<object[]>(parentCount);
        for (int i = 1; i <= parentCount; i++)
        {
            parents.Add(new object[] { i, $"P{i}" });
        }

        await writer.InsertRowsAsync("JdwParent", parents, ct);

        var children = new List<object[]>(childCount);
        for (int i = 0; i < childCount; i++)
        {
            children.Add(new object[] { 100 + i, (i % 2) + 1, $"c{i}" });
        }

        await writer.InsertRowsAsync("JdwChild", children, ct);
        return temp;
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
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public List<JdwChild> Children { get; set; } = [];
    }

    public sealed class JdwChild
    {
        public int Id { get; set; }

        public int ParentId { get; set; }

        public string Label { get; set; } = string.Empty;

        public JdwParent? Parent { get; set; }

        public Nonexistent? Stray { get; set; }
    }

    public sealed class Nonexistent
    {
        public int Id { get; set; }
    }
}
