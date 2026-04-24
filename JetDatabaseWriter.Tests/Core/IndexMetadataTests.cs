namespace JetDatabaseWriter.Tests;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Tests for <see cref="IAccessReader.ListIndexesAsync"/> against the
/// <c>NorthwindTraders.accdb</c> and <c>ComplexFields.accdb</c> fixtures.
/// Layout assertions are grounded in
/// <c>docs/design/format-probe-appendix-index.md</c>.
/// </summary>
public sealed class IndexMetadataTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Fact]
    public async Task ListIndexes_NorthwindCompanies_ReturnsBothPkAndForeignKeys()
    {
        var reader = await db.GetReaderAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Companies", TestContext.Current.CancellationToken);

        Assert.NotEmpty(indexes);

        // The probe appendix shows Companies has 16 logical indexes backed by 5 real indexes,
        // including a single primary key.
        Assert.Single(indexes, i => i.Kind == IndexKind.PrimaryKey);

        // Multiple logical indexes share the same RealIndexNumber for FK relationships.
        var byReal = indexes.GroupBy(i => i.RealIndexNumber).ToList();
        Assert.True(byReal.Count < indexes.Count, "Expected logical-index sharing across real indexes.");

        // At least one foreign-key entry must be present (Companies references CompanyTypes/States/etc.).
        Assert.Contains(indexes, i => i.IsForeignKey);
    }

    [Fact]
    public async Task ListIndexes_AllIndexesHaveNonEmptyKeyColumns()
    {
        var reader = await db.GetReaderAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Companies", TestContext.Current.CancellationToken);

        foreach (IndexMetadata idx in indexes)
        {
            Assert.NotEmpty(idx.Columns);
            Assert.All(idx.Columns, c => Assert.False(string.IsNullOrEmpty(c.Name), $"Index '{idx.Name}' has unresolved column number {c.ColumnNumber}."));
            Assert.All(idx.Columns, c => Assert.True(c.IsAscending, $"Unexpected descending key column in '{idx.Name}'."));
        }
    }

    [Fact]
    public async Task ListIndexes_PrimaryKey_HasSingleColumnCompanies()
    {
        var reader = await db.GetReaderAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Companies", TestContext.Current.CancellationToken);

        IndexMetadata pk = indexes.Single(i => i.Kind == IndexKind.PrimaryKey);
        Assert.Single(pk.Columns); // Companies.ID is a single-column PK.

        // Note: Access does not always set the IsUnique flag bit on PK entries because
        // primary keys are implicitly unique. The Kind == PrimaryKey discriminator is
        // the authoritative signal; IsUnique reflects only the raw real-idx flags byte.
    }

    [Fact]
    public async Task ListIndexes_UnknownTable_ReturnsEmpty()
    {
        var reader = await db.GetReaderAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("NoSuchTable", TestContext.Current.CancellationToken);
        Assert.Empty(indexes);
    }

    [Fact]
    public async Task ListIndexes_ComplexFields_DocumentsHasSystemManagedAttachmentIndex()
    {
        // The Documents table in ComplexFields.accdb has no user-defined PK,
        // but Access creates a system-managed cascading index on the
        // hidden Attachments complex column.
        var reader = await db.GetReaderAsync(TestDatabases.ComplexFields, TestContext.Current.CancellationToken);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Documents", TestContext.Current.CancellationToken);

        IndexMetadata attachmentIdx = Assert.Single(indexes);
        Assert.StartsWith("Attachments_", attachmentIdx.Name, StringComparison.OrdinalIgnoreCase);
        Assert.True(attachmentIdx.CascadeUpdates);
        Assert.True(attachmentIdx.CascadeDeletes);
    }
}
