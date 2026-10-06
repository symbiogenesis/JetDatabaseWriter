namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Tests.Relationships;
using Xunit;

/// <summary>Generated index names fit Access's limit even after a collision or self-relationship suffix.</summary>
/// <param name="db">Caches the Access-authored relationship fixtures.</param>
public sealed class GeneratedIndexNameTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Fact]
    public void Collisions_ReserveRoomForTheNumericSuffix()
    {
        string prefix = new('x', 64);
        var existing = new List<string> { prefix };
        for (int i = 1; i < 10; i++)
        {
            existing.Add(new string('x', 62) + "_" + i.ToString(CultureInfo.InvariantCulture));
        }

        Assert.Equal(new string('x', 61) + "_10", IndexHelpers.MakeUniqueLogicalIdxName(prefix, existing));
    }

    [Fact]
    public void Truncation_ChecksCollisionsIgnoringCase()
    {
        string candidate = IndexHelpers.MakeUniqueLogicalIdxName(new string('x', 64) + "_FK", [new string('X', 64)]);
        Assert.Equal(new string('x', 62) + "_1", candidate);
    }

    [Fact]
    public void Truncation_DoesNotSplitASurrogatePair()
    {
        string candidate = IndexHelpers.MakeUniqueLogicalIdxName(new string('x', 63) + "\U0001F600_FK", []);
        Assert.Equal(new string('x', 63), candidate);
        string collided = IndexHelpers.MakeUniqueLogicalIdxName(new string('x', 61) + "\U0001F600x", [new string('x', 61) + "\U0001F600x"]);
        Assert.Equal(new string('x', 61) + "_1", collided);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task SelfRelationship_CreateAndRename_KeepIndexNamesWithinLimit(DatabaseFormat format)
    {
        var ct = TestContext.Current.CancellationToken;
        using MemoryStream stream = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        string originalName = new('r', 64);
        string renamedName = new('s', 64);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("Tree", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("ParentId", typeof(int))], ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition(originalName, "Tree", "Id", "Tree", "ParentId"), ct);
        }

        await AssertNamesAsync(stream);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, ct))
        {
            await writer.RenameRelationshipAsync(originalName, renamedName, ct);
        }

        await AssertNamesAsync(stream);
    }

    private static async Task AssertNamesAsync(MemoryStream stream)
    {
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Tree", TestContext.Current.CancellationToken);
        Assert.Equal(2, indexes.Count(index => index.Kind == IndexKind.ForeignKey));
        Assert.All(indexes, index => Assert.InRange(index.Name.Length, 1, 64));
        Assert.Equal(indexes.Count, indexes.Select(index => index.Name).Distinct(StringComparer.OrdinalIgnoreCase).Count());
    }
}
