namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Tests.ComplexColumns.ComplexColumnTestSupport;

/// <summary>Refuses complex-column creation when required native template metadata is missing.</summary>
public sealed class ComplexTemplateIntegrityTests
{
    /// <summary>Gets write modes and malformed native template cases.</summary>
    public static TheoryData<WriteMode, string> TemplateDamageCases()
    {
        var cases = new TheoryData<WriteMode, string>();
        foreach (WriteMode mode in new[] { WriteMode.Direct, WriteMode.AutoCommit, WriteMode.ExplicitCommit })
        {
            foreach (string damage in new[] { "Unreadable", "OutOfFile", "WrongShape" })
            {
                cases.Add(mode, damage);
            }
        }

        return cases;
    }

    /// <summary>A missing template never becomes a zero catalog reference or changes the database.</summary>
    /// <param name="mode">The write mode.</param>
    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task CreateAttachment_MissingTemplate_RefusesWithoutWriting(WriteMode mode)
    {
        await using var created = new MemoryStream();
        await using (await CreateWriterAsync(created))
        {
        }

        byte[] bytes = created.ToArray();
        await DeleteTemplateCatalogRowAsync(bytes);
        await using (var missingStream = new MemoryStream(bytes, writable: false))
        await using (ReaderHarness reader = await ReaderHarness.OpenAsync(missingStream, cancellationToken: Ct))
        {
            TableDef catalog = Assert.IsType<TableDef>(await reader.ReadTableDefAsync(2, Ct));
            var rows = new CatalogRowReader(reader.Database.Format, reader.Database.TableDefs, reader.Database.OwnedPages);
            Assert.DoesNotContain(await rows.GetCatalogRowsAsync(catalog, Ct), row => row.Name == "MSysComplexType_Attachment");
        }

        await using var stream = new MemoryStream();
        await stream.WriteAsync(bytes, Ct);
        await using (AccessWriter writer = await OpenWriterAsync(stream, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                    await writer.CreateTableAsync("Docs", [new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }], Ct));
                Assert.Equal(JetErrorCode.CorruptComplexColumn, error.ErrorCode);
            });
        }

        Assert.Equal(bytes, stream.ToArray());
    }

    [Theory]
    [MemberData(nameof(TemplateDamageCases))]
    public async Task CreateAttachment_DamagedTemplate_RefusesWithoutWriting(WriteMode mode, string damage)
    {
        ArgumentNullException.ThrowIfNull(damage);
        await using var created = new MemoryStream();
        await using (await CreateWriterAsync(created))
        {
        }

        byte[] bytes = created.ToArray();
        await DamageTemplateAsync(bytes, damage);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(bytes, Ct);
        await using (AccessWriter writer = await OpenWriterAsync(stream, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                    await writer.CreateTableAsync("Docs", [new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }], Ct));
                if (damage == "WrongShape")
                {
                    Assert.Equal(JetErrorCode.CorruptComplexColumn, error.ErrorCode);
                    Assert.Equal("Files", error.ErrorInfo.ColumnName);
                    Assert.Equal("MSysComplexType_Attachment", error.ErrorInfo.ObjectName);
                }
                else
                {
                    Assert.Equal(JetErrorCode.CorruptCatalog, error.ErrorCode);
                    Assert.Equal("MSysObjects", error.ErrorInfo.TableName);
                }

                Assert.True(error.ErrorInfo.PageNumber > 0);
            });
        }

        Assert.Equal(bytes, stream.ToArray());
    }

    private static async Task DamageTemplateAsync(byte[] bytes, string damage)
    {
        await using var stream = new MemoryStream(bytes, writable: false);
        await using ReaderHarness reader = await ReaderHarness.OpenAsync(stream, cancellationToken: Ct);
        DatabaseFile database = reader.Database;
        TableDef catalog = Assert.IsType<TableDef>(await reader.ReadTableDefAsync(2, Ct));
        var catalogRows = new CatalogRowReader(database.Format, database.TableDefs, database.OwnedPages);
        CatalogRow template = Assert.Single(await catalogRows.GetCatalogRowsAsync(catalog, Ct), row => row.Name == "MSysComplexType_Attachment");
        int templateOffset = checked((int)(template.TDefPage * database.Format.PageSize));
        if (damage == "Unreadable")
        {
            bytes[templateOffset] = 0;
        }
        else if (damage == "WrongShape")
        {
            int realIndexCount = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(templateOffset + database.Format.TDef.NumRealIdx, 4));
            int firstColumnOffset = templateOffset + database.Format.TDef.BlockEnd + (realIndexCount * database.Format.TDef.RealIdxEntrySz);
            bytes[firstColumnOffset + database.Format.ColumnDescriptor.TypeOff] = (byte)ColumnType.LongIntegerType;
        }
        else
        {
            ColumnInfo idColumn = Assert.IsType<ColumnInfo>(catalog.FindColumn("Id"));
            byte[] page = await reader.ReadPageCopyAsync(template.PageNumber, Ct);
            RowBound row = DataPageRows.EnumerateLiveRowBounds(database.Format, page).Single(bound => bound.RowIndex == template.RowIndex);
            int idOffset = checked((int)(template.PageNumber * database.Format.PageSize)) + row.RowStart + database.Format.RowFields.NumCols + idColumn.FixedOff;
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(idOffset, 4), (bytes.Length / database.Format.PageSize) + 10);
        }
    }

    private static async Task DeleteTemplateCatalogRowAsync(byte[] bytes)
    {
        await using var stream = new MemoryStream(bytes, writable: false);
        await using ReaderHarness reader = await ReaderHarness.OpenAsync(stream, cancellationToken: Ct);
        DatabaseFile database = reader.Database;
        TableDef? catalog = await reader.ReadTableDefAsync(2, Ct);
        Assert.NotNull(catalog);
        var catalogRows = new CatalogRowReader(database.Format, database.TableDefs, database.OwnedPages);
        CatalogRow template = Assert.Single(await catalogRows.GetCatalogRowsAsync(catalog, Ct), row => row.Name == "MSysComplexType_Attachment");
        int slotOffset = checked((int)(template.PageNumber * database.Format.PageSize))
            + database.Format.DataPage.RowsStart + (template.RowIndex * sizeof(ushort));
        ushort slot = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(slotOffset, sizeof(ushort)));
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(slotOffset, sizeof(ushort)), (ushort)(slot | Constants.DataPage.DeletedRowFlag));
    }
}
