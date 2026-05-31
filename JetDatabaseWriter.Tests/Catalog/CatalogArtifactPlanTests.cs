namespace JetDatabaseWriter.Tests.Catalog;

using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

public sealed class CatalogArtifactPlanTests
{
    [Fact]
    public async Task ExecuteCatalogArtifactPlanAsync_TableArtifacts_CreateReadableTablesAndReturnPages()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.Jet4Mdb,
            leaveOpen: true,
            cancellationToken: cancellationToken))
        {
            var plan = new CatalogArtifactPlan(
                [
                    new CatalogTableArtifact(
                        "PlanA",
                        [new ColumnDefinition("Id", typeof(int))],
                        [],
                        0),
                    new CatalogTableArtifact(
                        "PlanB",
                        [new ColumnDefinition("Name", typeof(string), maxLength: 25)],
                        [new IndexDefinition("IX_Name", "Name")],
                        0),
                ],
                []);

            long[] tablePages = await writer.ExecuteCatalogArtifactPlanAsync(plan, cancellationToken);

            Assert.Equal(2, tablePages.Length);
            Assert.True(tablePages[0] > 0);
            Assert.True(tablePages[1] > 0);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken);

        List<string> tables = await reader.ListTablesAsync(cancellationToken);
        Assert.Contains("PlanA", tables);
        Assert.Contains("PlanB", tables);

        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("PlanB", cancellationToken);
        Assert.Contains(indexes, index => string.Equals(index.Name, "IX_Name", System.StringComparison.Ordinal));
    }

    [Fact]
    public async Task CreateDatabaseAsync_ComplexTypeTemplates_KeepZeroUsageMapPointers()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: cancellationToken);

        long templatePage = await writer.Relationships.FindSystemTableTdefPageAsync(
            Constants.ComplexTypeNames.Attachment,
            cancellationToken);
        Assert.True(templatePage > 0);

        byte[] page = await writer.ReadPageAsync(templatePage, cancellationToken);
        try
        {
            for (int pointerOffset = Constants.TableDefinition.OwnedPagesRowOffset;
                pointerOffset <= Constants.TableDefinition.FreePagesPageOffset + 2;
                pointerOffset++)
            {
                Assert.Equal(0, page[pointerOffset]);
            }
        }
        finally
        {
            AccessBase.ReturnPage(page);
        }
    }
}
