namespace JetDatabaseWriter.Tests.Queries;

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Tests the eager-loading escape hatch for the type-name → table-name convention: a
/// navigation's target POCO whose type name does not match its Access table still binds
/// when the type carries <c>[Table("...")]</c>
/// (<see cref="System.ComponentModel.DataAnnotations.Schema.TableAttribute"/>). The tables
/// here are named <c>JdwAttrParent</c> / <c>JdwAttrChild</c>, but the entity types are
/// <c>AttrParent</c> / <c>AttrChild</c>, so the relationship only resolves through the
/// attribute. A parallel unmapped pair proves the constraint the attribute relieves: a
/// mismatched type name with no attribute fails to infer the relationship.
/// </summary>
public sealed class EntityQueryTableAttributeTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Fact]
    public async Task Include_Reference_ResolvesRelatedTableFromTableAttribute()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<AttrChild> children = await reader.Query<AttrChild>("JdwAttrChild")
            .Include(c => c.Parent)
            .ToListAsync(ct);

        Assert.Equal(3, children.Count);
        Assert.All(children, c => Assert.NotNull(c.Parent));

        AttrChild first = children.Single(c => c.Id == 10);
        Assert.Equal(1, first.Parent!.Id);
        Assert.Equal("Alice", first.Parent.Name);
    }

    [Fact]
    public async Task Include_Collection_ResolvesRelatedTableFromTableAttribute()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        List<AttrParent> parents = await reader.Query<AttrParent>("JdwAttrParent")
            .Include(p => p.Children)
            .ToListAsync(ct);

        parents.Sort((a, b) => a.Id.CompareTo(b.Id));
        Assert.Equal(2, parents.Count);
        Assert.Equal(2, parents[0].Children.Count);
        Assert.Single(parents[1].Children);
        Assert.All(parents[0].Children, c => Assert.Equal(parents[0].Id, c.ParentId));
    }

    [Fact]
    public async Task Include_WithoutTableAttribute_TypeNameMismatch_ThrowsWithGuidance()
    {
        // The root table is passed explicitly, so the root type binds regardless of its
        // name; only the navigation's target type (UnmappedChild) is resolved by name, and
        // "UnmappedChild" does not match the "JdwAttrChild" table, so without a [Table]
        // attribute the relationship cannot be inferred.
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream temp = await BuildAsync(ct);
        await using AccessReader reader = await OpenReaderAsync(temp, ct);

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await reader.Query<UnmappedParent>("JdwAttrParent")
                .Include(p => p.Children)
                .ToListAsync(ct));

        Assert.Contains("[Table(", ex.Message, StringComparison.Ordinal);
    }

    private async Task<MemoryStream> BuildAsync(CancellationToken ct)
    {
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, ct);
        await using AccessWriter writer = await OpenWriterAsync(temp, ct);

        await writer.CreateTableAsync(
            "JdwAttrParent",
            [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Name", typeof(string), maxLength: 50)],
            ct);
        await writer.CreateTableAsync(
            "JdwAttrChild",
            [new("Id", typeof(int)) { IsPrimaryKey = true }, new("ParentId", typeof(int)), new("Label", typeof(string), maxLength: 50)],
            ct);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_JdwAttrChild_JdwAttrParent", "JdwAttrParent", "Id", "JdwAttrChild", "ParentId"),
            ct);

        await writer.InsertRowsAsync("JdwAttrParent", new[] { new object[] { 1, "Alice" }, new object[] { 2, "Bob" } }, ct);
        await writer.InsertRowsAsync(
            "JdwAttrChild",
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

    [Table("JdwAttrParent")]
    public sealed class AttrParent
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public List<AttrChild> Children { get; set; } = [];
    }

    [Table("JdwAttrChild")]
    public sealed class AttrChild
    {
        public int Id { get; set; }

        public int ParentId { get; set; }

        public string Label { get; set; } = string.Empty;

        public AttrParent? Parent { get; set; }
    }

    public sealed class UnmappedParent
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public List<UnmappedChild> Children { get; set; } = [];
    }

    public sealed class UnmappedChild
    {
        public int Id { get; set; }

        public int ParentId { get; set; }

        public string Label { get; set; } = string.Empty;
    }
}
