namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Tests for <see cref="IAccessReader.ListIndexesAsync"/> against the
/// <c>NorthwindTraders.accdb</c> and <c>ComplexFields.accdb</c> fixtures.
/// Layout assertions are grounded in
/// <see href="docs/design/format-probe-appendix-index.md" />.
/// </summary>
/// <param name="db">The database input.</param>
public sealed class IndexMetadataTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Fact]
    public async Task ListIndexes_NorthwindCompanies_ReturnsBothPkAndForeignKeys()
    {
        var reader = await db.GetReaderAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        var indexes = await reader.ListIndexesAsync("Companies", TestContext.Current.CancellationToken);

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
        var indexes = await reader.ListIndexesAsync("Companies", TestContext.Current.CancellationToken);

        foreach (var idx in indexes)
        {
            Assert.NotEmpty(idx.Columns);
            Assert.All(idx.Columns, c =>
            {
                Assert.False(string.IsNullOrEmpty(c.Name), $"Index '{idx.Name}' has unresolved column number {c.ColumnNumber}.");
                Assert.True(c.IsAscending, $"Unexpected descending key column in '{idx.Name}'.");
            });
        }
    }

    [Fact]
    public async Task ListIndexes_PrimaryKey_HasSingleColumnCompanies()
    {
        var reader = await db.GetReaderAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        var indexes = await reader.ListIndexesAsync("Companies", TestContext.Current.CancellationToken);

        var pk = indexes.Single(i => i.Kind == IndexKind.PrimaryKey);
        Assert.Single(pk.Columns); // Companies.ID is a single-column PK.

        Assert.True(pk.EnforcesUniqueness);

        // Access does not consistently encode PK uniqueness in the physical flag bit;
        // the Kind discriminator is the authoritative semantic signal.
    }

    [Fact]
    public async Task ListIndexes_UnknownTable_ReturnsEmpty()
    {
        var reader = await db.GetReaderAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        var indexes = await reader.ListIndexesAsync("NoSuchTable", TestContext.Current.CancellationToken);
        Assert.Empty(indexes);
    }

    [Fact]
    public async Task ListIndexes_ComplexFields_DocumentsHasSystemManagedAttachmentIndex()
    {
        // The Documents table in ComplexFields.accdb has no user-defined PK,
        // but Access creates a system-managed index on the hidden Attachments
        // complex column. DAO/Access stamps the cascade_ups / cascade_dels bytes
        // with the placeholder value 0x04 (Jackcess CASCADE_SET_DEFAULT_FLAG) for
        // every non-FK index — including this system index — rather than the
        // user-facing cascade bit 0x01. Since the index has no FK linkage
        // (relIdxNum == -1), neither IsForeignKey nor the cascade flags should be set.
        var reader = await db.GetReaderAsync(TestDatabases.ComplexFields, TestContext.Current.CancellationToken);
        var indexes = await reader.ListIndexesAsync("Documents", TestContext.Current.CancellationToken);

        var attachmentIdx = Assert.Single(indexes);
        Assert.StartsWith("Attachments_", attachmentIdx.Name, StringComparison.OrdinalIgnoreCase);
        Assert.False(attachmentIdx.IsForeignKey);
        Assert.False(attachmentIdx.CascadeUpdates);
        Assert.False(attachmentIdx.CascadeDeletes);
    }
}
