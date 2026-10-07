namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Tests.ComplexColumns.ComplexColumnTestSupport;

/// <summary>Malformed reference metadata must be refused before counter allocation or row mutation.</summary>
public sealed class ComplexColumnsCounterCorruptionTests
{
    /// <summary>Gets all write modes paired with missing and nonpositive references.</summary>
    public static TheoryData<WriteMode, int?> InvalidReferences()
    {
        var data = new TheoryData<WriteMode, int?>();
        foreach (WriteMode mode in new[] { WriteMode.Direct, WriteMode.AutoCommit, WriteMode.ExplicitCommit })
        {
            foreach (int? reference in new int?[] { null, 0, -1 })
            {
                data.Add(mode, reference);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(InvalidReferences))]
    public async Task InsertRow_InvalidExistingParentReference_RefusesWithoutRepair(WriteMode mode, int? reference)
    {
        await using var ms = new MemoryStream();
        await CreatePopulatedAsync(ms);
        await SetComplexSlotsAsync(ms, "Docs", _ => true, (_, _) => reference);
        byte[] baseline = ms.ToArray();

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                    await writer.InsertRowAsync("Docs", [2, DBNull.Value], Ct));
                Assert.Equal(JetErrorCode.CorruptComplexColumn, error.ErrorCode);
                Assert.Equal("Files", error.ErrorInfo.ColumnName);
                Assert.True(error.ErrorInfo.PageNumber > 0);
            });
        }

        Assert.Equal(baseline, ms.ToArray());
    }

    [Theory]
    [MemberData(nameof(InvalidReferences))]
    public async Task InsertRow_InvalidFlatForeignKey_RefusesWithoutRepair(WriteMode mode, int? reference)
    {
        await using var ms = new MemoryStream();
        await CreatePopulatedAsync(ms);
        long flatPage = await ReadFlatPageAsync(ms);
        ms.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: Ct))
        {
            TableDef flatDef = await harness.Database.TableDefs.ReadRequiredTableDefAsync(flatPage, "<flat>", Ct);
            ColumnInfo foreignKey = Assert.IsType<ColumnInfo>(flatDef.FindColumn("_Files"));
            RowLocation row = Assert.Single(await harness.Database.GetLiveRowLocationsAsync(flatPage, Ct));
            byte[] page = await harness.Pager.ReadPageCopyAsync(row.DataPageNumber, Ct);
            int nullMaskSize = JetTypeInfo.GetNullMaskSizeBytes(harness.Database.Format.ReadRowColumnCount(page, row.RowStart));
            JetTypeInfo.SetNullMaskBit(page.AsSpan(row.RowStart + row.RowSize - nullMaskSize, nullMaskSize), foreignKey.ColNum, reference.HasValue);
            BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(row.RowStart + harness.Database.Format.RowFields.NumCols + foreignKey.FixedOff, 4), reference ?? 0);
            await harness.Pager.WritePageAsync(row.DataPageNumber, page, Ct);
        }

        byte[] baseline = ms.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                    await writer.InsertRowAsync("Docs", [2, DBNull.Value], Ct));
                Assert.Equal(JetErrorCode.CorruptComplexColumn, error.ErrorCode);
                Assert.Equal("Files", error.ErrorInfo.ColumnName);
                Assert.Contains("flat", error.Message, StringComparison.OrdinalIgnoreCase);
            });
        }

        Assert.Equal(baseline, ms.ToArray());
    }

    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task InsertRow_MissingFlatForeignKeyDescriptor_RefusesWithoutRepair(WriteMode mode)
    {
        await using var ms = new MemoryStream();
        await CreatePopulatedAsync(ms);
        await RenameStoredColumnAsync(ms, await ReadFlatPageAsync(ms), "_Files", "BadFKx");
        byte[] baseline = ms.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                    await writer.InsertRowAsync("Docs", [2, DBNull.Value], Ct));
                Assert.Equal(JetErrorCode.CorruptComplexColumn, error.ErrorCode);
                Assert.Equal("Files", error.ErrorInfo.ColumnName);
            });
        }

        Assert.Equal(baseline, ms.ToArray());
    }

    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task CreateTable_MissingComplexIdDescriptor_RefusesWithoutRepair(WriteMode mode)
    {
        await using var ms = new MemoryStream();
        await CreatePopulatedAsync(ms);
        ms.Position = 0;
        long complexPage;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: Ct))
        {
            complexPage = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysComplexColumns", Ct);
        }

        await RenameStoredColumnAsync(ms, complexPage, "ComplexID", "MissingID");
        byte[] baseline = ms.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                    await writer.CreateTableAsync("Extra", [new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }], Ct));
                Assert.Equal(JetErrorCode.CorruptComplexColumn, error.ErrorCode);
                Assert.Equal("MSysComplexColumns", error.ErrorInfo.TableName);
                Assert.Equal("ComplexID", error.ErrorInfo.ColumnName);
            });
        }

        Assert.Equal(baseline, ms.ToArray());
    }

    [Theory]
    [MemberData(nameof(InvalidReferences))]
    public async Task CreateTable_InvalidStoredComplexId_RefusesWithoutRepair(WriteMode mode, int? reference)
    {
        await using var ms = new MemoryStream();
        await CreatePopulatedAsync(ms);
        ms.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: Ct))
        {
            long complexPage = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysComplexColumns", Ct);
            TableDef definition = await harness.Database.TableDefs.ReadRequiredTableDefAsync(complexPage, "MSysComplexColumns", Ct);
            ColumnInfo idColumn = Assert.IsType<ColumnInfo>(definition.FindColumn("ComplexID"));
            RowLocation row = Assert.Single(await harness.Database.GetLiveRowLocationsAsync(complexPage, Ct));
            byte[] page = await harness.Pager.ReadPageCopyAsync(row.DataPageNumber, Ct);
            int nullMaskSize = JetTypeInfo.GetNullMaskSizeBytes(harness.Database.Format.ReadRowColumnCount(page, row.RowStart));
            JetTypeInfo.SetNullMaskBit(page.AsSpan(row.RowStart + row.RowSize - nullMaskSize, nullMaskSize), idColumn.ColNum, reference.HasValue);
            BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(row.RowStart + harness.Database.Format.RowFields.NumCols + idColumn.FixedOff, 4), reference ?? 0);
            await harness.Pager.WritePageAsync(row.DataPageNumber, page, Ct);
        }

        byte[] baseline = ms.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                    await writer.CreateTableAsync("Extra", [new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }], Ct));
                Assert.Equal(JetErrorCode.CorruptComplexColumn, error.ErrorCode);
                Assert.Equal("MSysComplexColumns", error.ErrorInfo.TableName);
                Assert.Equal("ComplexID", error.ErrorInfo.ColumnName);
            });
        }

        Assert.Equal(baseline, ms.ToArray());
    }

    [Theory]
    [InlineData(-1, 7, 0)]
    [InlineData(16, 7, 0)]
    [InlineData(0, -1, 0)]
    [InlineData(0, 2, 0)]
    [InlineData(0, 17, 0)]
    [InlineData(0, 7, -1)]
    [InlineData(0, 7, int.MaxValue)]
    public void TryReadSlot_OutOfBounds_ReturnsFalse(int rowStart, int rowSize, int fixedOffset)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        byte[] page = new byte[16];
        BinaryPrimitives.WriteUInt16LittleEndian(page, 1);
        page[6] = 1;
        var column = new ColumnInfo { Type = ColumnType.ComplexType, ColNum = 0, FixedOff = fixedOffset, Name = "Files" };
        Assert.False(ComplexReferenceSeedReader.TryReadSlot(format, page, rowStart, rowSize, column, out int reference));
        Assert.Equal(0, reference);
    }

    private static async Task CreatePopulatedAsync(MemoryStream ms)
    {
        await using AccessWriter writer = await CreateWriterAsync(ms);
        await writer.CreateTableAsync("Docs", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }], Ct);
        await writer.InsertRowAsync("Docs", [1, DBNull.Value], Ct);
        await writer.AddAttachmentAsync("Docs", "Files", new Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("one.txt", [1]), Ct);
    }

    private static async Task<long> ReadFlatPageAsync(MemoryStream ms)
    {
        await using AccessReader reader = await OpenReaderAsync(ms);
        return CatalogValueReader.TdefPageFromId(Assert.Single(await reader.GetComplexColumnsAsync("Docs", Ct)).FlatTableId);
    }

    private static async Task RenameStoredColumnAsync(MemoryStream ms, long tdefPage, string oldName, string newName)
    {
        ms.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: Ct);
        byte[] page = await harness.Pager.ReadPageCopyAsync(tdefPage, Ct);
        byte[] oldBytes = Encoding.Unicode.GetBytes(oldName);
        byte[] newBytes = Encoding.Unicode.GetBytes(newName);
        Assert.Equal(oldBytes.Length, newBytes.Length);
        int offset = page.AsSpan().IndexOf(oldBytes);
        Assert.True(offset >= 0);
        newBytes.CopyTo(page, offset);
        await harness.Pager.WritePageAsync(tdefPage, page, Ct);
    }
}
