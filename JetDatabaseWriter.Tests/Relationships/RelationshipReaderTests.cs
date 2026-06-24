namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Tests for <see cref="AccessReader.ListRelationshipsAsync"/>: confirms that writer-created
/// relationships round-trip back through the reader as aggregated
/// <see cref="RelationshipMetadata"/>, including composite keys and the grbit flags.
/// Each test starts from a copy of <c>NorthwindTraders.accdb</c> (which already has the
/// <c>MSysRelationships</c> catalog table).
/// </summary>
public sealed class RelationshipReaderTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Fact]
    public async Task ListRelationshipsAsync_SingleColumn_ReturnsAggregatedRelationship()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, ct);
        string parent = Unique("RRP");
        string child = Unique("RRC");
        string relName = $"FK_{child}_{parent}";

        await using (AccessWriter writer = await OpenWriterAsync(temp, ct))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int)) { IsPrimaryKey = true }], ct);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)) { IsPrimaryKey = true }, new("ParentId", typeof(int))], ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition(relName, parent, "Id", child, "ParentId"), ct);
        }

        await using AccessReader reader = await OpenReaderAsync(temp, ct);
        RelationshipMetadata match = (await reader.ListRelationshipsAsync(ct)).Single(r => r.Name == relName);

        Assert.Equal(parent, match.PrimaryTable);
        Assert.Equal("Id", Assert.Single(match.PrimaryColumns));
        Assert.Equal(child, match.ForeignTable);
        Assert.Equal("ParentId", Assert.Single(match.ForeignColumns));
        Assert.True(match.EnforcesReferentialIntegrity);
        Assert.False(match.CascadeUpdates);
        Assert.False(match.CascadeDeletes);
    }

    [Fact]
    public async Task ListRelationshipsAsync_MultiColumn_PreservesColumnOrder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, ct);
        string parent = Unique("RMP");
        string child = Unique("RMC");
        string relName = $"FK_{child}_{parent}";

        await using (AccessWriter writer = await OpenWriterAsync(temp, ct))
        {
            await writer.CreateTableAsync(parent, [new("KeyA", typeof(int)), new("KeyB", typeof(int))], ct);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)), new("RefA", typeof(int)), new("RefB", typeof(int))], ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition(relName, parent, ["KeyA", "KeyB"], child, ["RefA", "RefB"]), ct);
        }

        await using AccessReader reader = await OpenReaderAsync(temp, ct);
        RelationshipMetadata match = (await reader.ListRelationshipsAsync(ct)).Single(r => r.Name == relName);

        Assert.Equal(2, match.PrimaryColumns.Count);
        Assert.Equal("KeyA", match.PrimaryColumns[0]);
        Assert.Equal("KeyB", match.PrimaryColumns[1]);
        Assert.Equal("RefA", match.ForeignColumns[0]);
        Assert.Equal("RefB", match.ForeignColumns[1]);
    }

    [Fact]
    public async Task ListRelationshipsAsync_CascadeAndNoEnforce_ReportsFlags()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        MemoryStream temp = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, ct);
        string parent = Unique("RCP");
        string child = Unique("RCC");
        string relName = $"FK_{child}_{parent}";

        await using (AccessWriter writer = await OpenWriterAsync(temp, ct))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int)) { IsPrimaryKey = true }], ct);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)) { IsPrimaryKey = true }, new("ParentId", typeof(int))], ct);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(relName, parent, "Id", child, "ParentId")
                {
                    EnforceReferentialIntegrity = false,
                    CascadeUpdates = true,
                    CascadeDeletes = true,
                },
                ct);
        }

        await using AccessReader reader = await OpenReaderAsync(temp, ct);
        RelationshipMetadata match = (await reader.ListRelationshipsAsync(ct)).Single(r => r.Name == relName);

        Assert.False(match.EnforcesReferentialIntegrity);
        Assert.True(match.CascadeUpdates);
        Assert.True(match.CascadeDeletes);
    }

    private static string Unique(string prefix) => prefix + Guid.NewGuid().ToString("N")[..8];

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
}
