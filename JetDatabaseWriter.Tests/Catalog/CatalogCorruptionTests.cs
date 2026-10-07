namespace JetDatabaseWriter.Tests.Catalog;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

public sealed class CatalogCorruptionTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CatalogReferences_RejectsDuplicateNamesWithDistinctRoots(bool systemAlias)
    {
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(TestContext.Current.CancellationToken);
        await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Victim", [new ColumnDefinition("Value", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync("Other", [new ColumnDefinition("Value", typeof(int))], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using ReaderHarness reader = await ReaderHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        var catalog = new CatalogRowReader(reader.Database.Format, reader.Database.TableDefs, reader.Database.OwnedPages);
        TableDef definition = Assert.IsType<TableDef>(await reader.Database.TableDefs.ReadTableDefAsync(2, TestContext.Current.CancellationToken));
        System.Collections.Generic.List<CatalogRow> rows = await catalog.GetCatalogRowsAsync(definition, TestContext.Current.CancellationToken);
        int victimIndex = rows.FindIndex(static row => row.Name == "Victim");
        int otherIndex = rows.FindIndex(static row => row.Name == "Other");
        Assert.True(victimIndex >= 0 && otherIndex >= 0);
        Assert.NotEqual(rows[victimIndex].TDefPage, rows[otherIndex].TDefPage);
        rows[otherIndex] = rows[otherIndex] with
        {
            Name = systemAlias ? "vIcTiM" : "Victim",
            Flags = systemAlias ? Constants.SystemObjects.SystemTableMask : 0,
        };

        JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await catalog.ValidateTableReferencesAsync(rows, TestContext.Current.CancellationToken));
        Assert.Equal(JetErrorCode.CorruptCatalog, failure.ErrorCode);
    }

    [Fact]
    public async Task CatalogReferences_RejectsCatalogOnlyOdbcAliasForReservedSelfName()
    {
        var catalog = new CatalogRowReader(JetFormat.ForNewDatabase(DatabaseFormat.Jet3Mdb), null!, null!);
        var row = new CatalogRow(3, 0, "mSySoBjEcTs", Constants.SystemObjects.LinkedOdbcType, 0, 1, -1, 0, true);
        JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await catalog.ValidateTableReferencesAsync([row], TestContext.Current.CancellationToken));
        Assert.Equal(JetErrorCode.CorruptCatalog, failure.ErrorCode);
    }

    [Theory]
    [InlineData("Id")]
    [InlineData("Name")]
    [InlineData("Type")]
    [InlineData("Flags")]
    public async Task CatalogRows_RejectsMissingRequiredColumnBeforeScanning(string missing)
    {
        string[] names = ["Id", "Name", "Type", "Flags"];
        var columns = new System.Collections.Generic.List<ColumnInfo>();
        foreach (string name in names)
        {
            if (name != missing)
            {
                columns.Add(new ColumnInfo { Name = name, Type = ColumnType.LongIntegerType });
            }
        }

        var catalog = new CatalogRowReader(JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb), null!, null!);
        JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await catalog.GetCatalogRowsAsync(new TableDef { Columns = columns }, TestContext.Current.CancellationToken));

        Assert.Equal(JetErrorCode.CorruptCatalog, failure.ErrorCode);
    }

    [Fact]
    public async Task PublicListTables_RejectsMissingCatalogInsteadOfEmptyDatabase()
    {
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(TestContext.Current.CancellationToken);
        byte[] bytes = stream.ToArray();
        bytes[2 * 4096] = 0;
        await using var corrupted = new MemoryStream(bytes);
        await using AccessReader reader = await AccessReader.OpenAsync(
            corrupted, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await reader.ListTablesAsync(TestContext.Current.CancellationToken));

        Assert.Equal(JetErrorCode.CorruptCatalog, failure.ErrorCode);

        // A failed scan must not cache an empty list: the same read fails again.
        await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await reader.ListTablesAsync(TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(true, "Layout")]
    [InlineData(false, "Layout")]
    [InlineData(true, "Id")]
    [InlineData(false, "Id")]
    [InlineData(true, "Reference")]
    [InlineData(false, "Reference")]
    [InlineData(true, "Duplicate")]
    [InlineData(false, "Duplicate")]
    [InlineData(true, "SystemReference")]
    [InlineData(false, "SystemReference")]
    [InlineData(true, "SystemDuplicate")]
    [InlineData(false, "SystemDuplicate")]
    [InlineData(true, "SystemAlias")]
    [InlineData(false, "SystemAlias")]
    [InlineData(true, "SystemOutOfRange")]
    [InlineData(false, "SystemOutOfRange")]
    [InlineData(true, "SystemTdef")]
    [InlineData(false, "SystemTdef")]
    [InlineData(true, "NullId")]
    [InlineData(false, "NullId")]
    [InlineData(true, "NullType")]
    [InlineData(false, "NullType")]
    [InlineData(true, "NullFlags")]
    [InlineData(false, "NullFlags")]
    [InlineData(true, "NullName")]
    [InlineData(false, "NullName")]
    public async Task PublicListTables_RejectsMalformedRowWithoutCachingPartialCatalog(bool strict, string damage)
    {
        ArgumentNullException.ThrowIfNull(damage);
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(TestContext.Current.CancellationToken);
        await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Victim", [new ColumnDefinition("Value", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync("Other", [new ColumnDefinition("Value", typeof(int))], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken))
        {
            TableDef definition = Assert.IsType<TableDef>(await harness.Database.TableDefs.ReadTableDefAsync(2, TestContext.Current.CancellationToken));
            ColumnInfo name = Assert.IsType<ColumnInfo>(definition.FindColumn("Name"));
            ColumnInfo id = Assert.IsType<ColumnInfo>(definition.FindColumn("Id"));
            long otherPage = (await harness.Services.Catalog.ResolveRequiredTableAsync("Other", TestContext.Current.CancellationToken)).Entry.TDefPage;
            long victimPage = (await harness.Services.Catalog.ResolveRequiredTableAsync("Victim", TestContext.Current.CancellationToken)).Entry.TDefPage;
            bool damaged = false;
            foreach (RowLocation location in await harness.Database.GetLiveRowLocationsAsync(2, TestContext.Current.CancellationToken))
            {
                byte[] page = await harness.Database.Pages.ReadPageAsync(location.PageNumber, TestContext.Current.CancellationToken);
                if (ScalarColumnReader.DecodeSimpleColumnValue(harness.Database.Format, page, location.RowStart, location.RowSize, name) != "Victim")
                {
                    continue;
                }

                if (damage == "Layout")
                {
                    page.AsSpan(location.RowStart, harness.Database.Format.RowFields.NumCols).Clear();
                }
                else if (damage.StartsWith("Null", StringComparison.Ordinal))
                {
                    ColumnInfo column = Assert.IsType<ColumnInfo>(definition.FindColumn(damage[4..]));
                    int columnCount = harness.Database.Format.ReadRowColumnCount(page, location.RowStart);
                    int maskStart = location.RowStart + location.RowSize - ((columnCount + 7) / 8);
                    int maskIndex = maskStart + (column.ColNum / 8);
                    page[maskIndex] &= unchecked((byte)~(1 << (column.ColNum % 8)));
                }
                else
                {
                    int targetPage = damage switch
                    {
                        "Duplicate" or "SystemDuplicate" => checked((int)otherPage),
                        "SystemAlias" => 2,
                        "SystemOutOfRange" => checked((int)harness.Database.Pages.PageCount),
                        "SystemTdef" => checked((int)victimPage),
                        "Id" => 0,
                        _ => 1,
                    };
                    if (damage.StartsWith("System", StringComparison.Ordinal) && damage != "SystemAlias")
                    {
                        ColumnInfo flags = Assert.IsType<ColumnInfo>(definition.FindColumn("Flags"));
                        BinaryPrimitives.WriteInt32LittleEndian(
                            page.AsSpan(location.RowStart + harness.Database.Format.RowFields.NumCols + flags.FixedOff, 4),
                            unchecked((int)Constants.SystemObjects.SystemTableMask));
                    }

                    BinaryPrimitives.WriteInt32LittleEndian(
                        page.AsSpan(location.RowStart + harness.Database.Format.RowFields.NumCols + id.FixedOff, 4),
                        targetPage);
                }

                await harness.Pager.WritePageAsync(location.PageNumber, page, TestContext.Current.CancellationToken);
                if (damage == "SystemTdef")
                {
                    byte[] root = await harness.Database.Pages.ReadPageAsync(victimPage, TestContext.Current.CancellationToken);
                    root[0] = 0;
                    await harness.Pager.WritePageAsync(victimPage, root, TestContext.Current.CancellationToken);
                }

                damaged = true;
                break;
            }

            Assert.True(damaged);
        }

        stream.Position = 0;
        await using (var lookupStream = new MemoryStream(stream.ToArray(), writable: false))
        await using (ReaderHarness lookup = await ReaderHarness.OpenAsync(lookupStream, cancellationToken: TestContext.Current.CancellationToken))
        {
            for (int attempt = 0; attempt < 2; attempt++)
            {
                JetCorruptDataException lookupFailure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                    await lookup.Services.Catalog.FindSystemTablePageAsync("Victim", TestContext.Current.CancellationToken));
                Assert.Equal(JetErrorCode.CorruptCatalog, lookupFailure.ErrorCode);
            }
        }

        await using AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        for (int attempt = 0; attempt < 2; attempt++)
        {
            JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                await reader.ListTablesAsync(TestContext.Current.CancellationToken));
            Assert.Equal(JetErrorCode.CorruptCatalog, failure.ErrorCode);
            Assert.Equal("MSysObjects", failure.ErrorInfo.TableName);
            Assert.NotNull(failure.ErrorInfo.PageNumber);
        }
    }
}
