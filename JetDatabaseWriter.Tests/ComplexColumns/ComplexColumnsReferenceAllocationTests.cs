namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Tests.ComplexColumns.ComplexColumnTestSupport;

/// <summary>
/// Per-row complex references (the 4-byte value in a parent row's complex
/// slot that joins it to its flat rows) come from the ACE TDEF complex
/// AutoNumber at offset 28, the last reference handed out, as Access and
/// Jackcess allocate them. Each row gets one reference shared by all its
/// complex columns, and the counter is raised on use and carried across
/// schema rewrites, so a deleted row's reference is never handed out again
/// and a writer-added row never takes the reference an Access row owns.
/// </summary>
public sealed class ComplexColumnsReferenceAllocationTests
{
    private static readonly Dictionary<string, object?> Row1 = new() { ["Id"] = 1 };

    private static readonly Dictionary<string, object?> Row2 = new() { ["Id"] = 2 };

    private static readonly Dictionary<string, object?> Row3 = new() { ["Id"] = 3 };

    private static readonly string[] DocsComplexColumns = ["Files", "Tags"];

    private static readonly ComplexDataColumn[] ComplexDataColumns =
    [
        new("VersionHistory_F5F8918F-0A3F-4DA9-AE71-184EE5012880", "VersionHistory_F5F8918F-0A3F-4D_6E54CCBB170741DD8FD837271ED8B90C"),
        new("multi-value-data", "multi-value-data_F4C67B0F60124C1989D5583C00CF76E2"),
        new("attach-data", "attach-data_071D71EDD53D45A1A9089929F06857D9"),
    ];

    /// <summary>Gets the complexDataTest fixtures with their TDEF complex AutoNumber, in every write mode.</summary>
    public static TheoryData<string, int, WriteMode> AccessFixturesAndModes()
    {
        var data = new TheoryData<string, int, WriteMode>();
        foreach (WriteMode mode in new[] { WriteMode.Direct, WriteMode.AutoCommit, WriteMode.ExplicitCommit })
        {
            data.Add(TestDatabases.ComplexDataTestV2007, 7, mode);
            data.Add(TestDatabases.ComplexDataTestV2010, 8, mode);
        }

        return data;
    }

    /// <summary>Gets malformed persisted slots in every write mode.</summary>
    public static TheoryData<int?, WriteMode> InvalidReferencesAndModes()
    {
        var data = new TheoryData<int?, WriteMode>();
        foreach (WriteMode mode in new[] { WriteMode.Direct, WriteMode.AutoCommit, WriteMode.ExplicitCommit })
        {
            data.Add(null, mode);
            data.Add(0, mode);
            data.Add(-1, mode);
            data.Add(7, mode);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task AddAttachment_AfterDeletingTopParent_DoesNotReuseReference(WriteMode mode)
    {
        await using MemoryStream ms = await CreateDeletedTopParentScenarioAsync(mode);

        RawTable docs = await ReadRawTableAsync(ms, "Docs");
        Assert.Equal([1, 3], docs.Rows.Select(r => (int)r[0]));
        object[] row3 = docs.Rows[1];
        Assert.Equal(3, Slot(docs, row3, "Files"));
        Assert.Equal(3, Slot(docs, row3, "Tags"));
        Assert.Equal(3, docs.ComplexAutoNumber);

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal(["three.txt"], (await reader.GetAttachmentsAsync("Docs", "Files", Ct)).Where(a => a.ConceptualTableId == 3).Select(a => a.FileName));
        Assert.Equal([7], (await reader.GetMultiValueItemsAsync("Docs", "Tags", Ct)).Where(i => i.ConceptualTableId == 3).Select(i => i.Value));
    }

    [Fact]
    public async Task AddItem_ExistingNullSlots_RefusesWithoutChangingReferences()
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms))
        {
            await CreateDocsAsync(writer);
            await writer.InsertRowsAsync("Docs", [[1, DBNull.Value, DBNull.Value], [2, DBNull.Value, DBNull.Value]], Ct);
        }

        await ClearComplexReferencesAsync(ms, "Docs");

        byte[] baseline = ms.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(ms))
        {
            JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                await writer.AddMultiValueItemAsync("Docs", "Tags", Row2, 42, Ct));
            Assert.Equal(JetErrorCode.CorruptComplexColumn, error.ErrorCode);
        }

        Assert.Equal(baseline, ms.ToArray());
        RawTable docs = await ReadRawTableAsync(ms, "Docs");
        Assert.Null(Slot(docs, docs.Rows[0], "Files"));
        Assert.Null(Slot(docs, docs.Rows[0], "Tags"));
        Assert.Null(Slot(docs, docs.Rows[1], "Files"));
        Assert.Null(Slot(docs, docs.Rows[1], "Tags"));
        Assert.Equal(0, docs.ComplexAutoNumber);
    }

    [Theory]
    [MemberData(nameof(AccessFixturesAndModes))]
    public async Task AddMultiValue_AccessFixtureRowInsertedByWriter_DoesNotTakeAnotherRowsReference(string fixture, int counter, WriteMode mode)
    {
        await using MemoryStream ms = await CopyFixtureAsync(fixture);
        Assert.Equal(counter, (await ReadRawTableAsync(ms, "Table1")).ComplexAutoNumber);

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                await writer.InsertRowAsync("Table1", new RowValues { ["id"] = "row5" }, Ct);
                var key = new Dictionary<string, object?> { ["id"] = "row5" };
                await writer.AddMultiValueItemAsync("Table1", "multi-value-data", key, "writer-value", Ct);
                await writer.AddAttachmentAsync("Table1", "attach-data", key, new AttachmentInput("w.txt", Encoding.UTF8.GetBytes("writer attachment")), Ct);
            });
        }

        RawTable table = await ReadRawTableAsync(ms, "Table1");
        object[] row5 = Assert.Single(table.Rows, r => (string)r[0] == "row5");
        foreach (ComplexDataColumn column in ComplexDataColumns)
        {
            Assert.Equal(counter + 1, Slot(table, row5, column.Name));
        }

        Assert.Equal(counter + 1, table.ComplexAutoNumber);

        await using AccessReader reader = await OpenReaderAsync(ms);
        var rows = (await reader.Rows("Table1", cancellationToken: Ct).ToListAsync(Ct)).ToDictionary(r => (string)r[0]);
        Assert.IsType<DBNull>(rows["row4"][4]);
        Assert.Equal(["writer-value"], ComplexCellValue.ReadMultiValueItems(Assert.IsType<byte[]>(rows["row5"][4])).Select(i => i.Value));
        Assert.Equal(["w.txt"], ComplexCellValue.ReadAttachments(Assert.IsType<byte[]>(rows["row5"][5])).Select(a => a.FileName));
        Assert.Equal(["value1", "value4"], ComplexCellValue.ReadMultiValueItems(Assert.IsType<byte[]>(rows["row2"][4])).Select(i => i.Value));

        // The writer keeps Access's unique complex indexes current.
        foreach (ComplexDataColumn column in ComplexDataColumns)
        {
            List<byte[]> keys = await ReadIndexLeafKeysAsync(ms, "Table1", column.Index);
            Assert.Equal(table.Rows.Count, keys.Count);
            Assert.Equal([0x7F, 0x80, 0x00, 0x00, (byte)(counter + 1)], keys[^1]);
        }
    }

    [Theory]
    [MemberData(nameof(AccessFixturesAndModes))]
    public async Task AddMultiValue_AccessFixtureRowWithNullSlots_PreservesIndexesOnRefusal(string fixture, int counter, WriteMode mode)
    {
        await using MemoryStream ms = await CopyFixtureAsync(fixture);
        await using (AccessWriter writer = await OpenWriterAsync(ms))
        {
            await writer.InsertRowAsync("Table1", new RowValues { ["id"] = "row5" }, Ct);
        }

        // Adversarial metadata: the row slots disagree with the existing index keys.
        await ClearComplexReferencesAsync(ms, "Table1", r => (string)r[0] == "row5", clearCounter: false);

        byte[] baseline = ms.ToArray();
        var keysBefore = new Dictionary<string, List<string>>();
        foreach (ComplexDataColumn column in ComplexDataColumns)
        {
            keysBefore[column.Name] = [.. (await ReadIndexLeafKeysAsync(ms, "Table1", column.Index)).Select(Convert.ToHexString)];
        }

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
                _ = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                    await writer.AddMultiValueItemAsync("Table1", "multi-value-data", new Dictionary<string, object?> { ["id"] = "row5" }, "writer-value", Ct)));
        }

        Assert.Equal(baseline, ms.ToArray());
        RawTable table = await ReadRawTableAsync(ms, "Table1");
        object[] row5 = Assert.Single(table.Rows, r => (string)r[0] == "row5");
        Assert.Equal(counter + 1, table.ComplexAutoNumber);
        foreach (ComplexDataColumn column in ComplexDataColumns)
        {
            Assert.Null(Slot(table, row5, column.Name));
            Assert.Equal(keysBefore[column.Name], (await ReadIndexLeafKeysAsync(ms, "Table1", column.Index)).Select(Convert.ToHexString));
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.DoesNotContain(await reader.GetMultiValueItemsAsync("Table1", "multi-value-data", Ct), item => Equals(item.Value, "writer-value"));
    }

    [Theory]
    [InlineData(WriteMode.Direct, 0)]
    [InlineData(WriteMode.AutoCommit, 0)]
    [InlineData(WriteMode.ExplicitCommit, 0)]
    [InlineData(WriteMode.Direct, 1)]
    [InlineData(WriteMode.AutoCommit, 1)]
    [InlineData(WriteMode.ExplicitCommit, 1)]
    public async Task InsertRow_StaleComplexCounter_RefusesWithoutRepair(WriteMode mode, int counter)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms))
        {
            await CreateDocsAsync(writer);
            await writer.InsertRowsAsync("Docs", [[1, DBNull.Value, DBNull.Value], [2, DBNull.Value, DBNull.Value]], Ct);
            await writer.AddAttachmentAsync("Docs", "Files", Row1, new AttachmentInput("one.txt", [1]), Ct);
            await writer.AddAttachmentAsync("Docs", "Files", Row2, new AttachmentInput("two.txt", [2]), Ct);
        }

        await SetComplexAutoNumberAsync(ms, "Docs", counter);
        byte[] baseline = ms.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                    await writer.InsertRowAsync("Docs", [3, DBNull.Value, DBNull.Value], Ct));
                Assert.Equal(JetErrorCode.CorruptComplexColumn, error.ErrorCode);
                Assert.Equal("Files", error.ErrorInfo.ColumnName);
                Assert.True(error.ErrorInfo.PageNumber > 0);
                Assert.Contains("counter", error.Message, StringComparison.OrdinalIgnoreCase);
            });
        }

        Assert.Equal(baseline, ms.ToArray());
        RawTable docs = await ReadRawTableAsync(ms, "Docs");
        Assert.Equal([1, 2], docs.Rows.Select(r => Slot(docs, r, "Files")));
        Assert.Equal(counter, docs.ComplexAutoNumber);
        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal(["one.txt", "two.txt"], (await reader.GetAttachmentsAsync("Docs", "Files", Ct)).Select(a => a.FileName));
    }

    [Theory]
    [InlineData(WriteMode.Direct, 0)]
    [InlineData(WriteMode.AutoCommit, 0)]
    [InlineData(WriteMode.ExplicitCommit, 0)]
    [InlineData(WriteMode.Direct, 1)]
    [InlineData(WriteMode.AutoCommit, 1)]
    [InlineData(WriteMode.ExplicitCommit, 1)]
    public async Task AddItems_ReferenceExceedsComplexCounter_RefusesWithoutRepair(WriteMode mode, int counter)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms))
        {
            await CreateDocsAsync(writer);
            await writer.InsertRowsAsync("Docs", [[1, DBNull.Value, DBNull.Value], [2, DBNull.Value, DBNull.Value]], Ct);
        }

        await SetComplexAutoNumberAsync(ms, "Docs", counter);
        byte[] baseline = ms.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                JetCorruptDataException attachmentError = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                    await writer.AddAttachmentAsync("Docs", "Files", Row2, new AttachmentInput("two.txt", [2]), Ct));
                Assert.Equal(JetErrorCode.CorruptComplexColumn, attachmentError.ErrorCode);
                Assert.Equal("Docs", attachmentError.ErrorInfo.TableName);
                Assert.Equal("Files", attachmentError.ErrorInfo.ColumnName);
                Assert.True(attachmentError.ErrorInfo.PageNumber > 0);
                Assert.Contains("counter", attachmentError.Message, StringComparison.OrdinalIgnoreCase);

                JetCorruptDataException valueError = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                    await writer.AddMultiValueItemAsync("Docs", "Tags", Row2, 42, Ct));
                Assert.Equal(JetErrorCode.CorruptComplexColumn, valueError.ErrorCode);
                Assert.Equal("Docs", valueError.ErrorInfo.TableName);
                Assert.Equal("Tags", valueError.ErrorInfo.ColumnName);
            });
        }

        Assert.Equal(baseline, ms.ToArray());
        Assert.Equal(counter, (await ReadRawTableAsync(ms, "Docs")).ComplexAutoNumber);
        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Empty(await reader.GetAttachmentsAsync("Docs", "Files", Ct));
        Assert.Empty(await reader.GetMultiValueItemsAsync("Docs", "Tags", Ct));
    }

    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task InsertRow_FlatReferenceExceedsComplexCounter_RefusesWithoutRepair(WriteMode mode)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms))
        {
            await CreateDocsAsync(writer);
            await writer.InsertRowsAsync("Docs", [[1, DBNull.Value, DBNull.Value], [2, DBNull.Value, DBNull.Value]], Ct);
            await writer.AddAttachmentAsync("Docs", "Files", Row2, new AttachmentInput("two.txt", [2]), Ct);
        }

        // Leave reference 2 in the flat table while every parent slot and the
        // persisted counter hold 1. Scanning parents alone cannot find it.
        await SetComplexSlotsAsync(ms, "Docs", _ => true, (_, _) => 1);
        await SetComplexAutoNumberAsync(ms, "Docs", 1);
        byte[] baseline = ms.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                    await writer.InsertRowAsync("Docs", [3, DBNull.Value, DBNull.Value], Ct));
                Assert.Equal(JetErrorCode.CorruptComplexColumn, error.ErrorCode);
                Assert.Equal("Files", error.ErrorInfo.ColumnName);
                Assert.True(error.ErrorInfo.PageNumber > 0);
                Assert.Contains("flat", error.Message, StringComparison.OrdinalIgnoreCase);
            });
        }

        Assert.Equal(baseline, ms.ToArray());
    }

    [Fact]
    public async Task AddAttachment_RolledBackTransaction_RestoresCounter()
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms))
        {
            await CreateDocsAsync(writer);
            await writer.InsertRowAsync("Docs", [1, DBNull.Value, DBNull.Value], Ct);
            await writer.AddAttachmentAsync("Docs", "Files", Row1, new AttachmentInput("one.txt", [1]), Ct);

            await using (JetTransaction tx = await writer.BeginTransactionAsync(Ct))
            {
                await writer.InsertRowAsync("Docs", [2, DBNull.Value, DBNull.Value], Ct);
                await writer.AddAttachmentAsync("Docs", "Files", Row2, new AttachmentInput("two.txt", [2]), Ct);
                await tx.RollbackAsync(Ct);
            }

            // The rolled-back reference 2 is handed out again, as the TDEF
            // counter the rollback discarded says it was never used.
            await writer.InsertRowAsync("Docs", [3, DBNull.Value, DBNull.Value], Ct);
            await writer.AddAttachmentAsync("Docs", "Files", Row3, new AttachmentInput("three.txt", [3]), Ct);
        }

        RawTable docs = await ReadRawTableAsync(ms, "Docs");
        Assert.Equal([1, 3], docs.Rows.Select(r => (int)r[0]));
        Assert.Equal(2, Slot(docs, docs.Rows[1], "Files"));
        Assert.Equal(2, docs.ComplexAutoNumber);

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal(["one.txt", "three.txt"], (await reader.GetAttachmentsAsync("Docs", "Files", Ct)).Select(a => a.FileName));
    }

    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task SchemaRewrite_AfterDeletingTopRow_KeepsComplexAutoNumber(WriteMode mode)
    {
        await using MemoryStream ms = await CreateDeletedTopParentScenarioAsync(WriteMode.Direct);

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                // Row 1 (reference 1) is left, under a counter of 3, so only the
                // carried counter keeps references 2 and 3 from being reused.
                Assert.Equal(1, await writer.DeleteRowsAsync("Docs", "Id", 3, Ct));

                // A table with a surviving complex column is transplanted onto its TDEF page.
                await writer.AddColumnAsync("Docs", new ColumnDefinition("Note", typeof(string), maxLength: 20), Ct);
            });
        }

        RawTable afterTransplant = await ReadRawTableAsync(ms, "Docs");
        Assert.Equal([1], afterTransplant.Rows.Select(r => Slot(afterTransplant, r, "Files")));
        Assert.Equal(3, afterTransplant.ComplexAutoNumber);

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            // Dropping a complex column takes the copy-and-swap path onto a new TDEF page.
            await RunAsync(writer, mode, async () => await writer.DropColumnAsync("Docs", "Tags", Ct));
        }

        Assert.Equal(3, (await ReadRawTableAsync(ms, "Docs")).ComplexAutoNumber);

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                await writer.InsertRowAsync("Docs", [4, DBNull.Value, DBNull.Value], Ct);
                await writer.AddAttachmentAsync("Docs", "Files", new Dictionary<string, object?> { ["Id"] = 4 }, new AttachmentInput("four.txt", [4]), Ct);
            });
        }

        RawTable docs = await ReadRawTableAsync(ms, "Docs");
        Assert.Equal(4, Slot(docs, Assert.Single(docs.Rows, r => (int)r[0] == 4), "Files"));
        Assert.Equal(4, docs.ComplexAutoNumber);

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal(["1:one.txt", "4:four.txt"], (await reader.GetAttachmentsAsync("Docs", "Files", Ct)).Select(a => $"{a.ConceptualTableId}:{a.FileName}").Order(StringComparer.Ordinal));
    }

    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task InsertRows_TableWithComplexColumns_AssignsOneReferencePerRow(WriteMode mode)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms, mode))
        {
            await CreateDocsAsync(writer);
            await RunAsync(writer, mode, async () =>
            {
                Assert.Equal(2, await writer.InsertRowsAsync("Docs", [[1, DBNull.Value, null], [2, null, DBNull.Value]], Ct));
                await writer.InsertRowAsync("Docs", new RowValues { ["Id"] = 3 }, Ct);
            });
        }

        RawTable docs = await ReadRawTableAsync(ms, "Docs");
        Assert.Equal(["1|1|1", "2|2|2", "3|3|3"], docs.Rows.Select(r => $"{r[0]}|{Slot(docs, r, "Files")}|{Slot(docs, r, "Tags")}"));
        Assert.Equal(3, docs.ComplexAutoNumber);

        // A reference with no items still reads as an empty cell.
        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.All(await reader.Rows("Docs", cancellationToken: Ct).ToListAsync(Ct), r => Assert.Equal([DBNull.Value, DBNull.Value], r[1..]));
        Assert.All(await reader.RowsAsStrings("Docs", cancellationToken: Ct).ToListAsync(Ct), r => Assert.Equal([string.Empty, string.Empty], r[1..]));
    }

    [Theory]
    [MemberData(nameof(AccessFixturesAndModes))]
    public async Task InsertRow_AccessFixture_ContinuesFromTdefCounter(string fixture, int counter, WriteMode mode)
    {
        await using MemoryStream ms = await CopyFixtureAsync(fixture);
        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                await writer.InsertRowAsync("Table1", new RowValues { ["id"] = "row5" }, Ct);
                await writer.InsertRowAsync("Table1", new RowValues { ["id"] = "row6" }, Ct);
            });
        }

        RawTable table = await ReadRawTableAsync(ms, "Table1");
        foreach (ComplexDataColumn column in ComplexDataColumns)
        {
            Assert.Equal(counter + 1, Slot(table, Assert.Single(table.Rows, r => (string)r[0] == "row5"), column.Name));
            Assert.Equal(counter + 2, Slot(table, Assert.Single(table.Rows, r => (string)r[0] == "row6"), column.Name));

            List<byte[]> keys = await ReadIndexLeafKeysAsync(ms, "Table1", column.Index);
            Assert.Equal(
                [IndexKeyEncoderEntry(counter + 1), IndexKeyEncoderEntry(counter + 2)],
                keys[^2..]);
        }

        Assert.Equal(counter + 2, table.ComplexAutoNumber);
    }

    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task InsertRows_FailedBatch_RewindsComplexCounter(WriteMode mode)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms, mode))
        {
            await writer.CreateTableAsync(
                "Docs",
                [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }],
                [new IndexDefinition("UX_Id", "Id") { IsUnique = true }],
                Ct);
            await writer.InsertRowAsync("Docs", [1, DBNull.Value], Ct);

            await RunAsync(writer, mode, async () =>
            {
                _ = await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowsAsync("Docs", [[4, DBNull.Value], [4, DBNull.Value]], Ct));
                await writer.InsertRowAsync("Docs", [5, DBNull.Value], Ct);
            });
        }

        RawTable docs = await ReadRawTableAsync(ms, "Docs");
        Assert.Equal(["1|1", "5|2"], docs.Rows.Select(r => $"{r[0]}|{Slot(docs, r, "Files")}"));
        Assert.Equal(2, docs.ComplexAutoNumber);
    }

    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task AddColumn_AttachmentOnTableWithRows_GivesExistingRowsReferences(WriteMode mode)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms, mode))
        {
            await writer.CreateTableAsync("Docs", [new ColumnDefinition("Id", typeof(int))], Ct);
            await writer.InsertRowsAsync("Docs", [[1], [2]], Ct);
            await RunAsync(writer, mode, async () =>
            {
                await writer.AddColumnAsync("Docs", new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }, Ct);
                await writer.InsertRowAsync("Docs", [3, DBNull.Value], Ct);
            });
        }

        RawTable docs = await ReadRawTableAsync(ms, "Docs");
        Assert.Equal(["1|1", "2|2", "3|3"], docs.Rows.Select(r => $"{r[0]}|{Slot(docs, r, "Files")}"));
        Assert.Equal(3, docs.ComplexAutoNumber);
    }

    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task AddColumn_ComplexColumnBesideAnother_SharesEachRowsReference(WriteMode mode)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms, mode))
        {
            await writer.CreateTableAsync("Docs", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }], Ct);
            await writer.InsertRowsAsync("Docs", [[1, DBNull.Value], [2, DBNull.Value]], Ct);
            await writer.AddAttachmentAsync("Docs", "Files", Row2, new AttachmentInput("two.txt", [2]), Ct);
            await RunAsync(writer, mode, async () =>
            {
                await writer.AddColumnAsync("Docs", new ColumnDefinition("Tags", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(int) }, Ct);
                await writer.AddMultiValueItemAsync("Docs", "Tags", Row2, 42, Ct);
                await writer.InsertRowAsync("Docs", [3, DBNull.Value, DBNull.Value], Ct);
            });
        }

        // Each row keeps one reference for all its complex columns, as Access does.
        RawTable docs = await ReadRawTableAsync(ms, "Docs");
        Assert.Equal(["1|1|1", "2|2|2", "3|3|3"], docs.Rows.Select(r => $"{r[0]}|{Slot(docs, r, "Files")}|{Slot(docs, r, "Tags")}"));
        Assert.Equal(3, docs.ComplexAutoNumber);

        ms.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: Ct))
        {
            long parentPage = (await harness.Services.Catalog.ResolveRequiredTableAsync("Docs", Ct)).Entry.TDefPage;
            long complexPage = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysComplexColumns", Ct);
            TableDef complexDef = await harness.Database.TableDefs.ReadRequiredTableDefAsync(complexPage, "MSysComplexColumns", Ct);
            List<LocatedRow> metadata = await harness.Services.Snapshots.ReadRowsAsync(complexPage, Ct);
            Assert.Equal(2, metadata.Count);
            Assert.All(metadata, row => Assert.Equal(parentPage, Convert.ToInt64(row.Values[complexDef.FindColumnIndex("ConceptualTableID")], System.Globalization.CultureInfo.InvariantCulture)));
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal(["2:two.txt"], (await reader.GetAttachmentsAsync("Docs", "Files", Ct)).Select(a => $"{a.ConceptualTableId}:{a.FileName}"));
        Assert.Equal(["2:42"], (await reader.GetMultiValueItemsAsync("Docs", "Tags", Ct)).Select(i => FormattableString.Invariant($"{i.ConceptualTableId}:{i.Value}")));
    }

    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task AddColumn_ExistingDuplicateComplexReferences_RefusesWithoutRepair(WriteMode mode)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms))
        {
            await CreateDocsAsync(writer);
            await writer.InsertRowsAsync("Docs", [.. Enumerable.Range(1, 5).Select(id => new object?[] { id, DBNull.Value, DBNull.Value })], Ct);
        }

        // Rows 3 and 4 share reference 3 in both complex columns. Every
        // slot is otherwise valid and stays below the persisted counter.
        // A schema change must refuse the duplicate without reassigning it.
        await SetComplexSlotsAsync(ms, "Docs", r => (int)r[0] == 4, (_, _) => 3);

        byte[] baseline = ms.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                    await writer.AddColumnAsync("Docs", new ColumnDefinition("Labels", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(int) }, Ct));
                Assert.Equal(JetErrorCode.CorruptComplexColumn, error.ErrorCode);
                Assert.Equal("Files", error.ErrorInfo.ColumnName);
                Assert.True(error.ErrorInfo.PageNumber > 0);
            });
        }

        Assert.Equal(baseline, ms.ToArray());
        RawTable docs = await ReadRawTableAsync(ms, "Docs");
        Assert.Equal(
            ["1|1|1", "2|2|2", "3|3|3", "4|3|3", "5|5|5"],
            docs.Rows.Select(r => $"{r[0]}|{Slot(docs, r, "Files")}|{Slot(docs, r, "Tags")}"));
    }

    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task AddColumn_CachedCounterWithDuplicateInOneExistingColumn_RefusesWithoutWrite(WriteMode mode) =>
        await AssertCachedAddColumnRefusesAsync(mode, (row, column) => (int)row[0] == 4 && column == "Files" ? 3 : (int)row[0], "Files");

    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task AddColumn_CachedCounterWithMismatchedExistingReferences_RefusesWithoutWrite(WriteMode mode) =>
        await AssertCachedAddColumnRefusesAsync(
            mode,
            (row, column) => column == "Tags" && (int)row[0] is 3 or 4 ? 7 - (int)row[0] : (int)row[0],
            "Tags");

    [Theory]
    [MemberData(nameof(InvalidReferencesAndModes))]
    public async Task AddColumn_CachedCounterWithInvalidExistingReference_RefusesWithoutWrite(int? reference, WriteMode mode) =>
        await AssertCachedAddColumnRefusesAsync(mode, (row, column) => (int)row[0] == 4 && column == "Tags" ? reference : (int)row[0], "Tags");

    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task InsertRow_SuppliedReference_IsSharedByTheRowsComplexColumns(WriteMode mode)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms, mode))
        {
            await CreateDocsAsync(writer);
            await RunAsync(writer, mode, async () =>
            {
                await writer.InsertRowAsync("Docs", [1, 7, DBNull.Value], Ct);
                await writer.InsertRowAsync("Docs", [2, DBNull.Value, DBNull.Value], Ct);
                await writer.InsertRowAsync("Docs", new RowValues { ["Id"] = 3, ["Tags"] = 20 }, Ct);
                await writer.InsertRowAsync("Docs", [4, 30, 30], Ct);

                // A row's complex columns share one reference, so two
                // different ones are rejected, as Jackcess rejects them.
                ArgumentException differ = await Assert.ThrowsAsync<ArgumentException>(async () => await writer.InsertRowAsync("Docs", [5, 40, 41], Ct));
                Assert.Contains("'Files'", differ.Message, StringComparison.Ordinal);
                Assert.Contains("'Tags'", differ.Message, StringComparison.Ordinal);
                _ = await Assert.ThrowsAsync<ArgumentException>(async () => await writer.InsertRowAsync("Docs", [5, 0, DBNull.Value], Ct));
                _ = await Assert.ThrowsAsync<ArgumentException>(async () => await writer.InsertRowAsync("Docs", [5, DBNull.Value, -1], Ct));

                await writer.InsertRowAsync("Docs", [5, DBNull.Value, DBNull.Value], Ct);
            });
        }

        RawTable docs = await ReadRawTableAsync(ms, "Docs");
        Assert.Equal(
            ["1|7|7", "2|8|8", "3|20|20", "4|30|30", "5|31|31"],
            docs.Rows.Select(r => $"{r[0]}|{Slot(docs, r, "Files")}|{Slot(docs, r, "Tags")}"));
        Assert.Equal(31, docs.ComplexAutoNumber);
    }

    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task UpdateRows_AssigningComplexColumn_IsRejected(WriteMode mode)
    {
        // Row 1 holds reference 1 (one.txt); row 3 holds 3 (three.txt and the tag 7).
        await using MemoryStream ms = await CreateDeletedTopParentScenarioAsync(WriteMode.Direct);

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                foreach (string column in DocsComplexColumns)
                {
                    foreach (object? value in new object?[] { null, DBNull.Value, 3 })
                    {
                        ArgumentException rejected = await Assert.ThrowsAsync<ArgumentException>(async () =>
                            await writer.UpdateRowsAsync("Docs", "Id", 1, new Dictionary<string, object?> { [column] = value }, Ct));
                        Assert.Equal("updatedValues", rejected.ParamName);
                        Assert.Contains($"'{column}'", rejected.Message, StringComparison.Ordinal);
                    }
                }

                // Refused before any row is read, even when no row matches.
                _ = await Assert.ThrowsAsync<ArgumentException>(async () =>
                    await writer.UpdateRowsAsync("Docs", "Id", 99, new Dictionary<string, object?> { ["Files"] = null }, Ct));

                // Updating another column keeps the row's reference.
                Assert.Equal(1, await writer.UpdateRowsAsync("Docs", "Id", 3, new Dictionary<string, object?> { ["Id"] = 4 }, Ct));
            });
        }

        RawTable docs = await ReadRawTableAsync(ms, "Docs");
        Assert.Equal(["1|1|1", "4|3|3"], docs.Rows.Select(r => $"{r[0]}|{Slot(docs, r, "Files")}|{Slot(docs, r, "Tags")}").Order(StringComparer.Ordinal));

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal(["1:one.txt", "3:three.txt"], (await reader.GetAttachmentsAsync("Docs", "Files", Ct)).Select(a => $"{a.ConceptualTableId}:{a.FileName}").Order(StringComparer.Ordinal));
        Assert.Equal(["3:7"], (await reader.GetMultiValueItemsAsync("Docs", "Tags", Ct)).Select(i => FormattableString.Invariant($"{i.ConceptualTableId}:{i.Value}")));
    }

    [Fact]
    public void TDefHeaderLayout_ComplexAutoNumber_IsAceOnly()
    {
        Assert.Equal(-1, JetFormat.ForNewDatabase(DatabaseFormat.Jet3Mdb).TDef.ComplexAutoNumber);
        Assert.Equal(-1, JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb).TDef.ComplexAutoNumber);
        Assert.Equal(28, JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb).TDef.ComplexAutoNumber);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public async Task ComplexHighWater_JetMdb_IsNeverWritten(DatabaseFormat format)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, format, WriterOptions(WriteMode.Direct), leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int))], Ct);
        }

        ms.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: Ct);
        long tdefPage = (await harness.Services.Catalog.ResolveRequiredTableAsync("T", Ct)).Entry.TDefPage;
        byte[] before = await harness.Database.Pages.ReadPageCopyAsync(tdefPage, Ct);
        var autoNumbers = new AutoNumberMaintainer(harness.Database.Format, harness.Database.TableDefs, harness.Database.OwnedPages, harness.Pager);

        await autoNumbers.RaiseComplexHighWaterAsync(tdefPage, 99, Ct);

        Assert.Equal(before, await harness.Database.Pages.ReadPageCopyAsync(tdefPage, Ct));
        Assert.Equal(0, await autoNumbers.ReadComplexHighWaterAsync(tdefPage, Ct));
    }

    /// <summary>
    /// Docs(Id, Files attachment, Tags multi-value int): rows 1 and 2 get
    /// attachments, row 2 a tag; row 2 is deleted, then row 3 is inserted and
    /// given an attachment and a tag.
    /// </summary>
    /// <param name="mode">The write mode.</param>
    private static async Task<MemoryStream> CreateDeletedTopParentScenarioAsync(WriteMode mode)
    {
        var ms = new MemoryStream();
        await using AccessWriter writer = await CreateWriterAsync(ms, mode);
        await CreateDocsAsync(writer);
        await RunAsync(writer, mode, async () =>
        {
            await writer.InsertRowsAsync("Docs", [[1, DBNull.Value, DBNull.Value], [2, DBNull.Value, DBNull.Value]], Ct);
            await writer.AddAttachmentAsync("Docs", "Files", Row1, new AttachmentInput("one.txt", Encoding.UTF8.GetBytes("payload one")), Ct);
            await writer.AddAttachmentAsync("Docs", "Files", Row2, new AttachmentInput("two.txt", Encoding.UTF8.GetBytes("payload two")), Ct);
            await writer.AddMultiValueItemAsync("Docs", "Tags", Row2, 42, Ct);
        });

        await RunAsync(writer, mode, async () =>
        {
            Assert.Equal(1, await writer.DeleteRowsAsync("Docs", "Id", 2, Ct));
            await writer.InsertRowAsync("Docs", [3, DBNull.Value, DBNull.Value], Ct);
            await writer.AddAttachmentAsync("Docs", "Files", Row3, new AttachmentInput("three.txt", Encoding.UTF8.GetBytes("payload three")), Ct);
            await writer.AddMultiValueItemAsync("Docs", "Tags", Row3, 7, Ct);
        });

        return ms;
    }

    private static async Task AssertCachedAddColumnRefusesAsync(WriteMode mode, Func<object[], string, int?> slotFor, string offendingColumn)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms))
        {
            await CreateDocsAsync(writer);
            await writer.InsertRowsAsync("Docs", [.. Enumerable.Range(1, 5).Select(id => new object?[] { id, DBNull.Value, DBNull.Value })], Ct);
        }

        ms.Position = 0;
        byte[] baseline;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(ms, WriterOptions(mode), cancellationToken: Ct))
        {
            // Prime the same writer's next-reference cache before corrupting
            // persisted slots, so allocation cannot rely on a cold seed scan.
            await harness.InsertRowAsync("Docs", [6, DBNull.Value, DBNull.Value], Ct);
            ResolvedTable table = await harness.Services.Catalog.ResolveRequiredTableAsync("Docs", Ct);
            DatabaseFile db = harness.Database;
            foreach (LocatedRow row in await harness.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, Ct))
            {
                RowLocation location = row.Location;
                byte[] page = await harness.Pager.ReadPageCopyAsync(location.DataPageNumber, Ct);
                int nullMaskSize = JetTypeInfo.GetNullMaskSizeBytes(db.Format.ReadRowColumnCount(page, location.RowStart));
                foreach (ColumnInfo column in table.Definition.Columns.Where(c => c.Type is ColumnType.ComplexType))
                {
                    int? reference = slotFor(row.Values, column.Name);
                    JetTypeInfo.SetNullMaskBit(page.AsSpan(location.RowStart + location.RowSize - nullMaskSize, nullMaskSize), column.ColNum, reference.HasValue);
                    BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(location.RowStart + db.Format.RowFields.NumCols + column.FixedOff, 4), reference ?? 0);
                }

                await harness.Pager.WritePageAsync(location.DataPageNumber, page, Ct);
            }

            baseline = ms.ToArray();
            await using JetTransaction? transaction = mode == WriteMode.ExplicitCommit ? await harness.BeginTransactionAsync(Ct) : null;
            JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                await harness.Services.Transactions.RunAutoCommitAsync(
                    _ => harness.Services.Schema.AddColumnAsync("Docs", new ColumnDefinition("Labels", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(int) }, Ct),
                    Ct));
            Assert.Equal(JetErrorCode.CorruptComplexColumn, error.ErrorCode);
            Assert.Equal("Docs", error.ErrorInfo.TableName);
            Assert.Equal(offendingColumn, error.ErrorInfo.ColumnName);
            Assert.True(error.ErrorInfo.PageNumber > 0);
            if (transaction is not null)
            {
                await transaction.CommitAsync(Ct);
            }
        }

        Assert.Equal(baseline, ms.ToArray());
        RawTable docs = await ReadRawTableAsync(ms, "Docs");
        Assert.Equal(6, docs.Rows.Count);
        Assert.Equal(6, docs.ComplexAutoNumber);
        Assert.DoesNotContain(docs.Definition.Columns, column => column.Name == "Labels");
        Assert.All(docs.Rows, row =>
        {
            Assert.Equal(slotFor(row, "Files"), Slot(docs, row, "Files"));
            Assert.Equal(slotFor(row, "Tags"), Slot(docs, row, "Tags"));
        });
    }

    private static async Task CreateDocsAsync(AccessWriter writer) =>
        await writer.CreateTableAsync(
            "Docs",
            [
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                new ColumnDefinition("Tags", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(int) },
            ],
            Ct);

    private static byte[] IndexKeyEncoderEntry(int reference) => IndexKeyEncoder.EncodeEntry(ColumnType.ComplexType, reference);

    private static async Task SetComplexAutoNumberAsync(MemoryStream ms, string tableName, int counter)
    {
        ms.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: Ct);
        long tdefPage = (await harness.Services.Catalog.ResolveRequiredTableAsync(tableName, Ct)).Entry.TDefPage;
        byte[] tdef = await harness.Database.Pages.ReadPageCopyAsync(tdefPage, Ct);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(tdef.AsSpan(ComplexAutoNumberOffset, 4), counter);
        await harness.Pager.WritePageAsync(tdefPage, tdef, Ct);
    }

    /// <summary>A complexDataTest Table1 complex column and the unique index Access keeps on it.</summary>
    /// <param name="Name">The column name.</param>
    /// <param name="Index">The index name.</param>
    private sealed record ComplexDataColumn(string Name, string Index);
}
