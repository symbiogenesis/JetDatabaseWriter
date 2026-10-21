namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;
using static JetDatabaseWriter.Tests.ComplexColumns.ComplexColumnTestSupport;

public sealed class ComplexItemIdentityTests
{
    [Theory]
    [MemberData(nameof(TestDatabases.ComplexData), MemberType = typeof(TestDatabases))]
    public async Task NativeItemsAndCells_CarryTheStoredAutoNumberIdAcrossReopen(string path)
    {
        await using MemoryStream stream = await CopyFixtureAsync(path);
        for (int reopen = 0; reopen < 2; reopen++)
        {
            await using AccessReader reader = await OpenReaderAsync(stream);
            foreach (ComplexColumnInfo column in await reader.GetComplexColumnsAsync("Table1", Ct))
            {
                RawTable flat = await ReadFlatTableAsync(stream, column);
                int identity = flat.Definition.FindColumnIndex(c => c.IsAutoNumber);
                Assert.True(identity >= 0);
                int[] expected = flat.Rows.Select(row => Assert.IsType<int>(row[identity])).ToArray();
                Assert.Equal(expected.Length, expected.Distinct().Count());
                List<object[]> parents = await reader.Rows("Table1", cancellationToken: Ct).ToListAsync(Ct);
                int ordinal = (await reader.GetColumnMetadataAsync("Table1", Ct)).Single(c => c.Name == column.ColumnName).Ordinal;
                if (column.Kind == ComplexColumnKind.Attachment)
                {
                    IReadOnlyList<AttachmentRecord> attachments = await reader.GetAttachmentsAsync("Table1", column.ColumnName, Ct);
                    Assert.Equal(expected, attachments.Select(item => item.Id));
                    Assert.Equal(expected.Order(), parents.Where(row => row[ordinal] is byte[]).SelectMany(row => ComplexCellValue.ReadAttachments((byte[])row[ordinal])).Select(item => item.Id).Order());
                }
                else
                {
                    IReadOnlyList<MultiValueItem> items = await reader.GetMultiValueItemsAsync("Table1", column.ColumnName, Ct);
                    Assert.Equal(expected, items.Select(item => item.Id));
                    Assert.Equal(expected.Order(), parents.Where(row => row[ordinal] is byte[]).SelectMany(row => ComplexCellValue.ReadMultiValueItems((byte[])row[ordinal])).Select(item => item.Id).Order());
                }
            }
        }
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task NativeItems_KeepTheirIdentitiesThroughDaoCompact()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.ComplexItemIdentity");
        File.Copy(TestDatabases.ComplexDataTestV2007, session.SourcePath);
        File.SetAttributes(session.SourcePath, FileAttributes.Normal);
        Dictionary<(string Column, int Id), string> before = await ReadIdentitiesAsync(session.SourcePath);

        session.RunDaoCompact();

        Dictionary<(string Column, int Id), string> compacted = await ReadIdentitiesAsync(session.CompactedPath);
        Assert.Equal(before.OrderBy(pair => pair.Key), compacted.OrderBy(pair => pair.Key));
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task SharedNativeHistory_IdentifiesEachMemoAndOmitsDroppedPayloadRows()
    {
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateDaoAccdbAsync(Ct, "JetDatabaseWriter.Tests.SharedHistoryIdentity");
        const string create = """
            $db.Execute('CREATE TABLE HistoryControl (Id LONG, Notes MEMO, More MEMO)',128)
            $db.TableDefs('HistoryControl').Fields('Notes').AppendOnly = $true
            $db.TableDefs('HistoryControl').Fields('More').AppendOnly = $true
            $db.Execute("INSERT INTO HistoryControl (Id,Notes,More) VALUES (1,'first','other')",128)
            $db.Execute("UPDATE HistoryControl SET Notes='revised',More='other revised' WHERE Id=1",128)
            """;
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoDatabaseScript(session.SourcePath, create, TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, $"DAO failed: {created.StdOut}\n{created.StdErr}");
        MultiValueItem[] all = await ReadHistoryAsync(session.SourcePath);
        Assert.Equal(4, all.Length);
        Assert.Equal(["first", "revised"], all.Where(item => item.MemoColumnName == "Notes").Select(item => item.Value));
        Assert.Equal(["other", "other revised"], all.Where(item => item.MemoColumnName == "More").Select(item => item.Value));

        AccessRoundTripEnvironment.CompactResult dropped = session.RunDaoDatabaseScript(session.SourcePath, " $db.Execute('ALTER TABLE HistoryControl DROP COLUMN More',128)", TimeSpan.FromMinutes(1));
        Assert.True(dropped.ExitCode == 0, $"DAO failed: {dropped.StdOut}\n{dropped.StdErr}");
        MultiValueItem[] retained = await ReadHistoryAsync(session.SourcePath);
        Assert.Equal(all.Where(item => item.MemoColumnName == "Notes"), retained);
        session.RunDaoCompact();
        Assert.Equal(retained, await ReadHistoryAsync(session.CompactedPath));

        static async Task<MultiValueItem[]> ReadHistoryAsync(string path)
        {
            await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, Ct);
            ComplexColumnInfo history = Assert.Single(await reader.GetComplexColumnsAsync("HistoryControl", Ct), column => column.Kind == ComplexColumnKind.VersionHistory);
            MultiValueItem[] items = [.. await reader.GetMultiValueItemsAsync("HistoryControl", history.ColumnName, Ct)];
            int ordinal = (await reader.GetColumnMetadataAsync("HistoryControl", Ct)).Single(column => column.Name == history.ColumnName).Ordinal;
            object[] parent = await reader.Rows("HistoryControl", cancellationToken: Ct).SingleAsync(Ct);
            Assert.Equal(items, ComplexCellValue.ReadMultiValueItems(Assert.IsType<byte[]>(parent[ordinal])));
            return items;
        }
    }

    private static async Task<RawTable> ReadFlatTableAsync(MemoryStream stream, ComplexColumnInfo descriptor)
    {
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        long page = CatalogValueReader.TdefPageFromId(descriptor.FlatTableId);
        TableDef definition = await harness.Database.TableDefs.ReadRequiredTableDefAsync(page, descriptor.FlatTableName, Ct);
        List<LocatedRow> rows = await harness.Services.Snapshots.ReadRowsAsync(page, Ct);
        byte[] tdef = await harness.Database.Pages.ReadPageCopyAsync(page, Ct);
        return new RawTable(definition, [.. rows.Select(row => row.Values)], tdef);
    }

    private static async Task<Dictionary<(string Column, int Id), string>> ReadIdentitiesAsync(string path)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, Ct);
        var result = new Dictionary<(string Column, int Id), string>();
        foreach (ComplexColumnInfo column in await reader.GetComplexColumnsAsync("Table1", Ct))
        {
            if (column.Kind == ComplexColumnKind.Attachment)
            {
                foreach (AttachmentRecord attachment in await reader.GetAttachmentsAsync("Table1", column.ColumnName, Ct))
                {
                    result.Add((column.ColumnName, attachment.Id), attachment.FileName + ":" + Convert.ToHexString(attachment.FileData));
                }
            }
            else
            {
                foreach (MultiValueItem item in await reader.GetMultiValueItemsAsync("Table1", column.ColumnName, Ct))
                {
                    result.Add((column.ColumnName, item.Id), Assert.IsType<string>(item.Value));
                }
            }
        }

        Assert.NotEmpty(result);
        return result;
    }

    [Theory]
    [InlineData("A", true)]
    [InlineData("A", false)]
    [InlineData("M", true)]
    [InlineData("M", false)]
    [InlineData("V", true)]
    [InlineData("V", false)]
    public void EncodedCell_InvalidOrDuplicateIdentity_IsStructuredCorruption(string kind, bool duplicate)
    {
        MultiValueItem[] values = [new() { Id = 1, Value = "first" }, new() { Id = duplicate ? 1 : 0, Value = "second" }];
        byte[] cell = kind switch
        {
            "A" => ComplexCellValue.EncodeAttachments(7, [new() { Id = 1, FileName = "first.txt" }, new() { Id = duplicate ? 1 : 0, FileName = "second.txt" }]),
            "V" => ComplexCellValue.EncodeVersionHistoryItems(7, values),
            _ => ComplexCellValue.EncodeMultiValueItems(7, values),
        };
        Action decode = kind == "A" ? () => ComplexCellValue.ReadAttachments(cell) : () => ComplexCellValue.ReadMultiValueItems(cell);

        JetCorruptDataException failure = Assert.Throws<JetCorruptDataException>(decode);

        Assert.Equal(JetErrorCode.CorruptComplexColumn, failure.ErrorCode);
        Assert.Equal("Id", failure.ErrorInfo.ColumnName);
    }

    [Theory]
    [InlineData(true, true, true)]
    [InlineData(true, false, true)]
    [InlineData(false, true, true)]
    [InlineData(false, false, true)]
    [InlineData(true, true, false)]
    [InlineData(true, false, false)]
    [InlineData(false, true, false)]
    [InlineData(false, false, false)]
    public async Task InvalidAssignedIdentity_IsRejectedOrOmittedWithoutInventingIds(bool strict, bool attachment, bool duplicate)
    {
        await using var stream = new MemoryStream();
        string columnName = attachment ? "Files" : "Tags";
        await using (AccessWriter writer = await CreateWriterAsync(stream))
        {
            ColumnDefinition column = attachment
                ? new(columnName, typeof(byte[])) { IsAttachment = true }
                : new(columnName, typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(int) };
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)), column], Ct);
            await writer.InsertRowAsync("Items", [1, DBNull.Value], Ct);
            var key = new Dictionary<string, object?> { ["Id"] = 1 };
            for (int item = 1; item <= 3; item++)
            {
                if (attachment)
                {
                    await writer.AddAttachmentAsync("Items", columnName, key, new AttachmentInput(item + ".txt", [(byte)item]), Ct);
                }
                else
                {
                    await writer.AddMultiValueItemAsync("Items", columnName, key, item, Ct);
                }
            }
        }

        ComplexColumnInfo descriptor;
        await using (AccessReader inspection = await OpenReaderAsync(stream))
        {
            descriptor = Assert.Single(await inspection.GetComplexColumnsAsync("Items", Ct));
        }

        await using (AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            long flatPage = CatalogValueReader.TdefPageFromId(descriptor.FlatTableId);
            TableDef flat = await harness.Database.TableDefs.ReadRequiredTableDefAsync(flatPage, descriptor.FlatTableName, Ct);
            ColumnInfo identity = Assert.Single(flat.Columns, c => c.IsAutoNumber);
            LocatedRow first = (await harness.Services.Snapshots.ReadRowsAsync(flatPage, Ct))[0];
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(first.Location.DataPageNumber, Ct);
            Assert.True(RowDecodePlan.TryParseRowLayout(harness.Database.Format.RowFields, page, first.Location.RowStart, first.Location.RowSize, flat.HasVarColumns, out RowLayout layout));
            ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(harness.Database.Format.RowFields, page, first.Location.RowStart, first.Location.RowSize, layout, identity);
            Assert.Equal(4, slice.DataLen);
            int offset = first.Location.RowStart + slice.DataStart;
            Assert.Equal(1, BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(offset, 4)));
            BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(offset, 4), duplicate ? 2 : 0);
            await harness.Pager.WritePageAsync(first.Location.DataPageNumber, page, Ct);
            List<LocatedRow> changed = await harness.Services.Snapshots.ReadRowsAsync(flatPage, Ct);
            Assert.Equal(duplicate ? 2 : 0, Assert.IsType<int>(changed[0].Values[flat.FindColumnIndex(c => c.IsAutoNumber)]));
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: Ct);
        if (strict)
        {
            JetCorruptDataException failure = attachment
                ? await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.GetAttachmentsAsync("Items", columnName, Ct))
                : await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.GetMultiValueItemsAsync("Items", columnName, Ct));
            Assert.Equal(JetErrorCode.CorruptComplexColumn, failure.ErrorCode);
            Assert.Equal(descriptor.FlatTableName, failure.ErrorInfo.TableName);
            Assert.NotNull(failure.ErrorInfo.ColumnName);
            Assert.NotNull(failure.ErrorInfo.PageNumber);
        }
        else
        {
            int[] remaining = attachment
                ? (await reader.GetAttachmentsAsync("Items", columnName, Ct)).Select(item => item.Id).ToArray()
                : (await reader.GetMultiValueItemsAsync("Items", columnName, Ct)).Select(item => item.Id).ToArray();
            Assert.Equal(duplicate ? Enumerable.Repeat(3, 1) : Enumerable.Range(2, 2), remaining);
            object[] parent = await reader.Rows("Items", cancellationToken: Ct).SingleAsync(Ct);
            byte[] cell = Assert.IsType<byte[]>(parent[1]);
            Assert.Equal(remaining, attachment ? ComplexCellValue.ReadAttachments(cell).Select(item => item.Id) : ComplexCellValue.ReadMultiValueItems(cell).Select(item => item.Id));
        }
    }
}
