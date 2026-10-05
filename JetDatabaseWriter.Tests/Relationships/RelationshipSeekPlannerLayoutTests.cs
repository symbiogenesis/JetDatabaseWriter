namespace JetDatabaseWriter.Tests.Relationships;

using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Relationship seeks select Access-authored real-index roots on both TDEF layouts.</summary>
/// <param name="cache">Caches fixture bytes.</param>
public sealed class RelationshipSeekPlannerLayoutTests(DatabaseCache cache) : IClassFixture<DatabaseCache>
{
    public static TheoryData<string> Fixtures => [TestDatabases.IndexTestV1997, TestDatabases.IndexTestV2003];

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task ParentAndChildRoots_MatchAccessIndexMetadata(string fixture)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await cache.CopyToStreamAsync(fixture, ct);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        stream.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: ct);
        var planner = new RelationshipSeekPlanner(harness.Database.Format, harness.Database.TableDefs, harness.Services.TableCatalog);
        int checkedRelationships = 0;
        foreach (RelationshipMetadata metadata in await reader.ListRelationshipsAsync(ct))
        {
            if (!metadata.EnforcesReferentialIntegrity)
            {
                continue;
            }

            var relationship = new FkRelationship(metadata.Name, metadata.PrimaryTable, metadata.PrimaryColumns, metadata.ForeignTable, metadata.ForeignColumns, metadata.CascadeUpdates, metadata.CascadeDeletes);
            var context = new FkContext([relationship]);
            ParentSeekIndex parent = Assert.IsType<ParentSeekIndex>(await planner.ResolveParentSeekIndexAsync(relationship, context, ct));
            ChildSeekIndex child = Assert.IsType<ChildSeekIndex>(await planner.ResolveChildSeekIndexAsync(relationship, context, ct));
            IndexMetadata parentIndex = (await reader.ListIndexesAsync(metadata.PrimaryTable, ct))
                .Where(index => index.Columns.Select(column => column.Name).SequenceEqual(metadata.PrimaryColumns))
                .OrderBy(index => index.RealIndexNumber).First();
            IndexMetadata childIndex = (await reader.ListIndexesAsync(metadata.ForeignTable, ct))
                .Where(index => index.Columns.Select(column => column.Name).SequenceEqual(metadata.ForeignColumns))
                .OrderBy(index => index.RealIndexNumber).First();
            Assert.True(parentIndex.FirstDp > 0);
            Assert.True(childIndex.FirstDp > 0);
            Assert.Equal(parentIndex.FirstDp, parent.RootPage);
            Assert.Equal(childIndex.FirstDp, child.RootPage);
            Assert.Same(parent, await planner.ResolveParentSeekIndexAsync(relationship, context, ct));
            Assert.Same(child, await planner.ResolveChildSeekIndexAsync(relationship, context, ct));
            checkedRelationships++;
        }

        Assert.True(checkedRelationships >= 2, "The fixture must exercise both Access-authored relationships.");
    }
}
