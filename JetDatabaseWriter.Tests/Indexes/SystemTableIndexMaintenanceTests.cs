namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

public sealed class SystemTableIndexMaintenanceTests
{
    [Fact]
    public async Task InsertSystemRowAndMaintainAsync_MSysACEs_UsesIncrementalMaintenance()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var writer = await CreateFreshAceWriterAsync(ct);

        long tdefPage = await writer.Relationships.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Aces, ct);
        var tableDef = await writer.ReadRequiredTableDefAsync(tdefPage, Constants.SystemTableNames.Aces, ct);
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
        var ct = TestContext.Current.CancellationToken;
        await using var writer = await CreateFreshAceWriterAsync(ct);

        long tdefPage = await writer.Relationships.FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, ct);
        var tableDef = await writer.ReadRequiredTableDefAsync(tdefPage, Constants.SystemTableNames.ComplexColumns, ct);
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

    [Fact]
    public async Task InsertSystemRowAndMaintainAsync_Throws_WhenSystemTableIncrementalMaintenanceBails()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var writer = await CreateFreshAceWriterAsync(ct);

        long tdefPage = await writer.Relationships.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Aces, ct);
        var tableDef = await writer.ReadRequiredTableDefAsync(tdefPage, Constants.SystemTableNames.Aces, ct);
        await CorruptFirstIndexRootPageTypeAsync(writer, tdefPage, ct);

        object[] row = tableDef.CreateNullValueRow();
        tableDef.SetValueByName(row, "ObjectId", -70_101);
        tableDef.SetValueByName(row, "SID", Constants.Aces.UsersSid);
        tableDef.SetValueByName(row, "ACM", Constants.Aces.DefaultAcm);
        tableDef.SetValueByName(row, "FInheritable", false);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.InsertSystemRowAndMaintainAsync(
                tdefPage,
                tableDef,
                Constants.SystemTableNames.Aces,
                row,
                cancellationToken: ct).AsTask());

        Assert.Contains("Could not maintain MSysACEs system-table indexes incrementally", ex.Message, StringComparison.Ordinal);
        Assert.Contains("full rebuild fallback is disabled", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DropTableAsync_Throws_WhenMsysAcesDeleteIncrementalMaintenanceBails()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var writer = await CreateFreshAceWriterAsync(ct);

        await writer.CreateTableAsync(
            "Victim",
            [new ColumnDefinition("Id", typeof(int))],
            ct);

        long acesTdefPage = await writer.Relationships.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Aces, ct);
        await CorruptFirstIndexRootPageTypeAsync(writer, acesTdefPage, ct);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.DropTableAsync("Victim", ct).AsTask());

        Assert.Contains("Could not maintain MSysACEs system-table indexes incrementally", ex.Message, StringComparison.Ordinal);
        Assert.Contains("full rebuild fallback is disabled", ex.Message, StringComparison.Ordinal);
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

    private static async ValueTask CorruptFirstIndexRootPageTypeAsync(AccessWriter writer, long tdefPage, CancellationToken cancellationToken)
    {
        byte[] tdef = await writer.ReadPageAsync(tdefPage, cancellationToken);
        try
        {
            int numCols = Ru16(tdef, writer._tdef.NumCols);
            int numRealIdx = Ri32(tdef, writer._tdef.NumRealIdx);
            Assert.True(numRealIdx > 0, $"Expected TDEF page {tdefPage} to declare at least one real index.");

            int colStart = writer._tdef.BlockEnd + (numRealIdx * writer._tdef.RealIdxEntrySz);
            int namePos = colStart + (numCols * writer._colDesc.Size);
            for (int i = 0; i < numCols; i++)
            {
                int nameLength = writer.ReadColumnName(tdef, ref namePos, out _);
                Assert.True(nameLength >= 0, $"Failed to walk TDEF page {tdefPage} column name {i}.");
            }

            int realIdxDescStart = namePos;
            int physStart = writer._indexLayout.RealIdxPhysOffset(realIdxDescStart, 0);
            int firstDp = Ri32(tdef, writer._indexLayout.FirstDpAbsoluteOffset(physStart));
            Assert.True(firstDp > 0, $"Expected TDEF page {tdefPage} first real-index root page to be allocated.");

            byte[] root = await writer.ReadPageAsync(firstDp, cancellationToken);
            try
            {
                Assert.True(
                    root[0] == Constants.IndexLeafPage.PageTypeLeaf || root[0] == Constants.IndexLeafPage.PageTypeIntermediate,
                    $"Expected index root page {firstDp} to be an index page, got 0x{root[0]:X2}.");
                root[0] = 0x01;
                await writer.WritePageAsync(firstDp, root, cancellationToken);
            }
            finally
            {
                AccessBase.ReturnPage(root);
            }
        }
        finally
        {
            AccessBase.ReturnPage(tdef);
        }
    }
}
