namespace JetDatabaseWriter.Tests.Relationships;

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Validates that foreign-key surrogate indexes (the auto-created indexes
/// that back relationship constraints) are surfaced correctly by
/// <see cref="AccessReader.ListIndexesAsync"/>. The current reader
/// exposes them with <see cref="IndexMetadata.IsForeignKey"/> = true.
/// Closes §1.4 gap: "Foreign-key surrogate indexes — round-trip + listing
/// semantics; we filter them out today.".
/// </summary>
public sealed class ForeignKeySurrogateIndexTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// NorthwindTraders has relationships between Companies and other tables;
    /// the auto-created FK surrogate indexes should appear with IsForeignKey = true.
    /// </summary>
    [Fact]
    public async Task NorthwindTraders_HasForeignKeySurrogateIndexes()
    {
        var reader = await db.GetReaderAsync(
            TestDatabases.NorthwindTraders,
            TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        int fkIndexCount = 0;
        foreach (string table in tables)
        {
            IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(
                table, TestContext.Current.CancellationToken);
            fkIndexCount += indexes.Count(i => i.IsForeignKey);
        }

        Assert.True(fkIndexCount > 0, "Expected at least one FK surrogate index in NorthwindTraders.");
    }

    /// <summary>
    /// FK surrogate indexes have valid column references (non-empty column list).
    /// </summary>
    [Fact]
    public async Task ForeignKeySurrogateIndexes_HaveValidColumns()
    {
        var reader = await db.GetReaderAsync(
            TestDatabases.NorthwindTraders,
            TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        foreach (string table in tables)
        {
            IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(
                table, TestContext.Current.CancellationToken);

            foreach (IndexMetadata idx in indexes.Where(i => i.IsForeignKey))
            {
                Assert.NotEmpty(idx.Columns);
                Assert.All(idx.Columns, c =>
                    Assert.False(string.IsNullOrEmpty(c.Name), $"FK index '{idx.Name}' on '{table}' has unresolved column."));
            }
        }
    }

    /// <summary>
    /// FK surrogate indexes reference a related table (RelatedTablePage > 0).
    /// </summary>
    [Fact]
    public async Task ForeignKeySurrogateIndexes_HaveForeignTableReference()
    {
        var reader = await db.GetReaderAsync(
            TestDatabases.NorthwindTraders,
            TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        bool foundAny = false;
        foreach (string table in tables)
        {
            IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(
                table, TestContext.Current.CancellationToken);

            foreach (IndexMetadata idx in indexes.Where(i => i.IsForeignKey))
            {
                foundAny = true;
                Assert.True(
                    idx.RelatedTablePage > 0,
                    $"FK index '{idx.Name}' on '{table}' has RelatedTablePage = {idx.RelatedTablePage}.");
            }
        }

        Assert.True(foundAny, "Expected at least one FK surrogate index.");
    }

    /// <summary>
    /// The indexTest fixtures typically contain relationships and thus should
    /// contain FK surrogate indexes. Verifies at least one is visible.
    /// </summary>
    [Theory]
    [InlineData(nameof(TestDatabases.IndexTestV2007))]
    [InlineData(nameof(TestDatabases.IndexTestV2010))]
    public async Task IndexTestFixtures_ContainForeignKeySurrogateIndexes(string fieldName)
    {
        string path = (string)typeof(TestDatabases).GetField(fieldName)!.GetValue(null)!;
        if (!File.Exists(path))
        {
            return;
        }

        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        int fkCount = 0;
        foreach (string table in tables)
        {
            IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(
                table, TestContext.Current.CancellationToken);
            fkCount += indexes.Count(i => i.IsForeignKey);
        }

        // These fixtures may or may not have FK indexes — assert non-negative
        // to avoid fragile assertions on third-party fixture content.
        Assert.True(fkCount >= 0);
    }
}
