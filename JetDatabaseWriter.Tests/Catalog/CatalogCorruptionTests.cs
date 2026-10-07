namespace JetDatabaseWriter.Tests.Catalog;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

public sealed class CatalogCorruptionTests
{
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
            bool damaged = false;
            foreach (RowLocation location in await harness.Database.GetLiveRowLocationsAsync(2, TestContext.Current.CancellationToken))
            {
                byte[] page = await harness.Database.Pages.ReadPageCopyAsync(location.PageNumber, TestContext.Current.CancellationToken);
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
                    BinaryPrimitives.WriteInt32LittleEndian(
                        page.AsSpan(location.RowStart + harness.Database.Format.RowFields.NumCols + id.FixedOff, 4),
                        damage == "Duplicate" ? checked((int)otherPage) : damage == "Id" ? 0 : 1);
                }

                await harness.Pager.WritePageAsync(location.PageNumber, page, TestContext.Current.CancellationToken);
                damaged = true;
                break;
            }

            Assert.True(damaged);
        }

        stream.Position = 0;
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
