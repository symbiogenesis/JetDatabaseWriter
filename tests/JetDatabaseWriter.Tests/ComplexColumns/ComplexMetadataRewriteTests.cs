namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Checks preservation and index coverage during complex-catalog rewrites.</summary>
public sealed class ComplexMetadataRewriteTests
{
    [Theory]
    [InlineData("", false)]
    [InlineData(null, false)]
    [InlineData("Files", false)]
    [InlineData("Files", true)]
    [InlineData("files", true)]
    public async Task RewriteMetadata_PreservesUnassignedValuesAndIndexes(string? columnName, bool rename)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: ct);
        long page = await writer.Services.CatalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, ct);
        TableDef definition = await writer.Database.TableDefs.ReadRequiredTableDefAsync(page, Constants.SystemTableNames.ComplexColumns, ct);
        object[] original = definition.CreateNullValueRow();
        definition.SetValueByName(original, "ColumnName", columnName ?? (object)DBNull.Value);
        definition.SetValueByName(original, "ComplexID", 70_001);
        definition.SetValueByName(original, "ComplexTypeObjectID", -17);
        definition.SetValueByName(original, "ConceptualTableID", 70_002);
        definition.SetValueByName(original, "FlatTableID", DBNull.Value);
        await writer.Services.Indexes.InsertSystemRowAndMaintainAsync(page, definition, Constants.SystemTableNames.ComplexColumns, original, cancellationToken: ct);
        object[] expected = (object[])original.Clone();
        if (rename)
        {
            await writer.Services.ComplexColumns.RenameComplexColumnArtifactsAsync("Files", "FILES", 70_001, ct);
            definition.SetValueByName(expected, "ColumnName", "FILES");
        }
        else
        {
            await writer.Services.ComplexColumns.UpdateComplexColumnParentTableIdAsync(70_001, 70_003, ct);
            definition.SetValueByName(expected, "ConceptualTableID", 70_003);
        }

        LocatedRow actual = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(page, ct));
        Assert.Equal(expected, actual.Values);
        foreach (long root in await IndexLeafChain.ReadRealIndexRootsAsync(writer.Database, page, ct))
        {
            await IndexLeafChain.AssertCoversLiveRowsAsync(writer.Database, page, root, ct);
        }
    }
}
