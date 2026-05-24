namespace JetDatabaseWriter.Tests.Indexes;

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using Xunit;

public sealed class SystemTableIndexMaintenanceTests
{
    [Fact]
    public async Task InsertSystemRowAndMaintainAsync_MSysACEs_UsesIncrementalMaintenance()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using AccessWriter writer = await CreateFreshAceWriterAsync(ct);

        long tdefPage = await writer.Relationships.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Aces, ct);
        TableDef tableDef = await writer.ReadRequiredTableDefAsync(tdefPage, Constants.SystemTableNames.Aces, ct);
        object[] row = tableDef.CreateNullValueRow();
        tableDef.SetValueByName(row, "ObjectId", -70_001);
        tableDef.SetValueByName(row, "SID", Constants.Aces.UsersSid);
        tableDef.SetValueByName(row, "ACM", Constants.Aces.DefaultAcm);
        tableDef.SetValueByName(row, "FInheritable", false);

        await writer.InsertSystemRowAndMaintainAsync(
            tdefPage,
            tableDef,
            Constants.SystemTableNames.Aces,
            row,
            cancellationToken: ct);

        Assert.Equal(SystemTableIndexMaintenancePath.Incremental, writer.LastSystemTableIndexMaintenancePath);
    }

    [Fact]
    public async Task InsertSystemRowAndMaintainAsync_MSysComplexColumns_UsesIncrementalMaintenance()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using AccessWriter writer = await CreateFreshAceWriterAsync(ct);

        long tdefPage = await writer.Relationships.FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, ct);
        TableDef tableDef = await writer.ReadRequiredTableDefAsync(tdefPage, Constants.SystemTableNames.ComplexColumns, ct);
        object[] row = tableDef.CreateNullValueRow();
        tableDef.SetValueByName(row, "ColumnName", "SyntheticComplexColumn");
        tableDef.SetValueByName(row, "ComplexID", 70_001);
        tableDef.SetValueByName(row, "ComplexTypeObjectID", 70_002);
        tableDef.SetValueByName(row, "ConceptualTableID", 70_003);
        tableDef.SetValueByName(row, "FlatTableID", 70_004);

        await writer.InsertSystemRowAndMaintainAsync(
            tdefPage,
            tableDef,
            Constants.SystemTableNames.ComplexColumns,
            row,
            cancellationToken: ct);

        Assert.Equal(SystemTableIndexMaintenancePath.Incremental, writer.LastSystemTableIndexMaintenancePath);
    }

    private static async ValueTask<AccessWriter> CreateFreshAceWriterAsync(CancellationToken cancellationToken)
    {
        var stream = new MemoryStream();
        return await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            leaveOpen: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }
}
