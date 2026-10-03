namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

public sealed class SystemTableIndexMaintenanceTests
{
    [Fact]
    public async Task InsertSystemRowAndMaintainAsync_MSysACEs_UsesIncrementalMaintenance()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateFreshAceDatabaseAsync(ct);
        await using WriterHarness writer = await WriterHarness.OpenAsync(stream, cancellationToken: ct);

        long tdefPage = await writer.Services.CatalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Aces, ct);
        TableDef tableDef = await writer.Database.ReadRequiredTableDefAsync(tdefPage, Constants.SystemTableNames.Aces, ct);
        object[] row = tableDef.CreateNullValueRow();
        tableDef.SetValueByName(row, "ObjectId", -70_001);
        tableDef.SetValueByName(row, "SID", Constants.Aces.UsersSid);
        tableDef.SetValueByName(row, "ACM", Constants.Aces.DefaultAcm);
        tableDef.SetValueByName(row, "FInheritable", false);

        await writer.Services.Indexes.InsertSystemRowAndMaintainAsync(
            tdefPage,
            tableDef,
            Constants.SystemTableNames.Aces,
            row,
            cancellationToken: ct);

        Assert.Equal(SystemTableIndexMaintenancePath.Incremental, writer.Services.Indexes.LastSystemTableIndexMaintenancePath);
    }

    [Fact]
    public async Task InsertSystemRowAndMaintainAsync_MSysAces_RootLeafOverflow_GrowsTreeIncrementally()
    {
        // A fresh ACCDB's MSysACEs indexes are single root leaves. System
        // tables have no full-rebuild fallback, so the insert that overflows
        // a root leaf used to throw (bail C13) instead of growing the index.
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateFreshAceDatabaseAsync(ct);
        await using WriterHarness writer = await WriterHarness.OpenAsync(stream, cancellationToken: ct);

        long tdefPage = await writer.Services.CatalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Aces, ct);
        for (int i = 0; i < 700; i++)
        {
            TableDef tableDef = await writer.Database.ReadRequiredTableDefAsync(tdefPage, Constants.SystemTableNames.Aces, ct);
            object[] row = tableDef.CreateNullValueRow();
            tableDef.SetValueByName(row, "ObjectId", -70_001 - i);
            tableDef.SetValueByName(row, "SID", Constants.Aces.UsersSid);
            tableDef.SetValueByName(row, "ACM", Constants.Aces.DefaultAcm);
            tableDef.SetValueByName(row, "FInheritable", false);

            await writer.Services.Indexes.InsertSystemRowAndMaintainAsync(
                tdefPage,
                tableDef,
                Constants.SystemTableNames.Aces,
                row,
                cancellationToken: ct);

            Assert.Equal(SystemTableIndexMaintenancePath.Incremental, writer.Services.Indexes.LastSystemTableIndexMaintenancePath);
        }

        List<long> roots = await IndexLeafChain.ReadRealIndexRootsAsync(writer.Database, tdefPage, ct);
        Assert.NotEmpty(roots);
        bool anyGrown = false;
        foreach (long root in roots)
        {
            anyGrown |= (await writer.Database.ReadPageCopyAsync(root, ct))[0] == Constants.IndexLeafPage.PageTypeIntermediate;
            await IndexLeafChain.AssertCoversLiveRowsAsync(writer.Database, tdefPage, root, ct);
        }

        Assert.True(anyGrown, "An MSysACEs index should have grown past its root leaf.");
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task CreateTableAsync_MoreTablesThanOneAcesLeafHolds_AllSucceed(bool transactionalWrites, bool explicitTransaction)
    {
        // Each table adds two MSysACEs rows; the root leaf held 602 entries,
        // so the 302nd table used to throw, and without a transaction it
        // left that table half-created.
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int tableCount = 320;
        await using MemoryStream stream = await CreateFreshAceDatabaseAsync(ct);

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false, UseTransactionalWrites = transactionalWrites },
            leaveOpen: true,
            ct))
        {
            await using JetTransaction? tx = explicitTransaction ? await writer.BeginTransactionAsync(ct) : null;
            for (int i = 1; i <= tableCount; i++)
            {
                await writer.CreateTableAsync(FormattableString.Invariant($"Tbl{i:D4}"), [new ColumnDefinition("Id", typeof(int))], ct);
            }

            if (tx is not null)
            {
                await tx.CommitAsync(ct);
            }
        }

        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            IReadOnlyList<string> tables = await reader.ListTablesAsync(ct);
            Assert.Equal(tableCount, tables.Count(name => name.StartsWith("Tbl", StringComparison.Ordinal)));
        }

        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: ct);
        long acesTdefPage = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Aces, ct);
        foreach (long root in await IndexLeafChain.ReadRealIndexRootsAsync(harness.Database, acesTdefPage, ct))
        {
            await IndexLeafChain.AssertCoversLiveRowsAsync(harness.Database, acesTdefPage, root, ct);
        }
    }

    [Fact]
    public async Task InsertSystemRowAndMaintainAsync_MSysComplexColumns_UsesIncrementalMaintenance()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateFreshAceDatabaseAsync(ct);
        await using WriterHarness writer = await WriterHarness.OpenAsync(stream, cancellationToken: ct);

        long tdefPage = await writer.Services.CatalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.ComplexColumns, ct);
        TableDef tableDef = await writer.Database.ReadRequiredTableDefAsync(tdefPage, Constants.SystemTableNames.ComplexColumns, ct);
        object[] row = tableDef.CreateNullValueRow();
        tableDef.SetValueByName(row, "ColumnName", "SyntheticComplexColumn");
        tableDef.SetValueByName(row, "ComplexID", 70_001);
        tableDef.SetValueByName(row, "ComplexTypeObjectID", 70_002);
        tableDef.SetValueByName(row, "ConceptualTableID", 70_003);
        tableDef.SetValueByName(row, "FlatTableID", 70_004);

        await writer.Services.Indexes.InsertSystemRowAndMaintainAsync(
            tdefPage,
            tableDef,
            Constants.SystemTableNames.ComplexColumns,
            row,
            cancellationToken: ct);

        Assert.Equal(SystemTableIndexMaintenancePath.Incremental, writer.Services.Indexes.LastSystemTableIndexMaintenancePath);
    }

    [Fact]
    public async Task InsertSystemRowAndMaintainAsync_Throws_WhenSystemTableIncrementalMaintenanceBails()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateFreshAceDatabaseAsync(ct);
        await using WriterHarness writer = await WriterHarness.OpenAsync(stream, cancellationToken: ct);

        long tdefPage = await writer.Services.CatalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Aces, ct);
        TableDef tableDef = await writer.Database.ReadRequiredTableDefAsync(tdefPage, Constants.SystemTableNames.Aces, ct);
        await CorruptFirstIndexRootPageTypeAsync(writer.Database, tdefPage, ct);

        object[] row = tableDef.CreateNullValueRow();
        tableDef.SetValueByName(row, "ObjectId", -70_101);
        tableDef.SetValueByName(row, "SID", Constants.Aces.UsersSid);
        tableDef.SetValueByName(row, "ACM", Constants.Aces.DefaultAcm);
        tableDef.SetValueByName(row, "FInheritable", false);

        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.Services.Indexes.InsertSystemRowAndMaintainAsync(
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
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateFreshAceDatabaseAsync(ct);

        await using (AccessWriter setup = await AccessWriter.OpenAsync(stream, leaveOpen: true, cancellationToken: ct))
        {
            await setup.CreateTableAsync(
                "Victim",
                [new ColumnDefinition("Id", typeof(int))],
                ct);
        }

        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: ct))
        {
            long acesTdefPage = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Aces, ct);
            await CorruptFirstIndexRootPageTypeAsync(harness.Database, acesTdefPage, ct);
        }

        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, leaveOpen: true, cancellationToken: ct);
        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.DropTableAsync("Victim", ct).AsTask());

        Assert.Contains("Could not maintain MSysACEs system-table indexes incrementally", ex.Message, StringComparison.Ordinal);
        Assert.Contains("full rebuild fallback is disabled", ex.Message, StringComparison.Ordinal);
    }

    private static async ValueTask<MemoryStream> CreateFreshAceDatabaseAsync(CancellationToken cancellationToken)
    {
        var stream = new MemoryStream();
        AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: cancellationToken).ConfigureAwait(false);
        await writer.DisposeAsync().ConfigureAwait(false);
        return stream;
    }

    private static async ValueTask CorruptFirstIndexRootPageTypeAsync(DatabaseFile db, long tdefPage, CancellationToken cancellationToken)
    {
        byte[] tdef = await db.ReadPageAsync(tdefPage, cancellationToken);
        try
        {
            int numCols = Ru16(tdef, db.TDef.NumCols);
            int numRealIdx = Ri32(tdef, db.TDef.NumRealIdx);
            Assert.True(numRealIdx > 0, $"Expected TDEF page {tdefPage} to declare at least one real index.");

            int colStart = db.TDef.BlockEnd + (numRealIdx * db.TDef.RealIdxEntrySz);
            int namePos = colStart + (numCols * db.ColumnDescriptor.Size);
            for (int i = 0; i < numCols; i++)
            {
                int nameLength = db.ReadColumnName(tdef, ref namePos, out _);
                Assert.True(nameLength >= 0, $"Failed to walk TDEF page {tdefPage} column name {i}.");
            }

            int realIdxDescStart = namePos;
            int physStart = db.IndexLayoutInfo.RealIdxPhysOffset(realIdxDescStart, 0);
            int firstDp = Ri32(tdef, db.IndexLayoutInfo.FirstDpAbsoluteOffset(physStart));
            Assert.True(firstDp > 0, $"Expected TDEF page {tdefPage} first real-index root page to be allocated.");

            byte[] root = await db.ReadPageAsync(firstDp, cancellationToken);
            try
            {
                Assert.True(
                    root[0] is Constants.IndexLeafPage.PageTypeLeaf or Constants.IndexLeafPage.PageTypeIntermediate,
                    $"Expected index root page {firstDp} to be an index page, got 0x{root[0]:X2}.");
                root[0] = 0x01;
                await db.WritePageAsync(firstDp, root, cancellationToken);
            }
            finally
            {
                DatabaseFile.ReturnPage(root);
            }
        }
        finally
        {
            DatabaseFile.ReturnPage(tdef);
        }
    }
}
