namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
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
    public static TheoryData<string, int, ComplexWriteMode> AccessFixturesAndModes()
    {
        var data = new TheoryData<string, int, ComplexWriteMode>();
        foreach (ComplexWriteMode mode in Enum.GetValues<ComplexWriteMode>())
        {
            data.Add(TestDatabases.ComplexDataTestV2007, 7, mode);
            data.Add(TestDatabases.ComplexDataTestV2010, 8, mode);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task AddAttachment_AfterDeletingTopParent_DoesNotReuseReference(ComplexWriteMode mode)
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
    public async Task AddItem_EarlierBuildRowWithNullSlots_PatchesEveryNullComplexSlotOfTheRow()
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms))
        {
            await CreateDocsAsync(writer);
            await writer.InsertRowsAsync("Docs", [[1, DBNull.Value, DBNull.Value], [2, DBNull.Value, DBNull.Value]], Ct);
        }

        await ClearComplexReferencesAsync(ms, "Docs");

        await using (AccessWriter writer = await OpenWriterAsync(ms))
        {
            await writer.AddMultiValueItemAsync("Docs", "Tags", Row2, 42, Ct);
        }

        RawTable docs = await ReadRawTableAsync(ms, "Docs");
        Assert.Null(Slot(docs, docs.Rows[0], "Files"));
        Assert.Null(Slot(docs, docs.Rows[0], "Tags"));
        Assert.Equal(1, Slot(docs, docs.Rows[1], "Files"));
        Assert.Equal(1, Slot(docs, docs.Rows[1], "Tags"));
        Assert.Equal(1, docs.ComplexAutoNumber);
    }

    [Theory]
    [MemberData(nameof(AccessFixturesAndModes))]
    public async Task AddMultiValue_AccessFixtureRowInsertedByWriter_DoesNotTakeAnotherRowsReference(string fixture, int counter, ComplexWriteMode mode)
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
    public async Task AddMultiValue_AccessFixtureRowWithNullSlots_RebuildsComplexIndexes(string fixture, int counter, ComplexWriteMode mode)
    {
        await using MemoryStream ms = await CopyFixtureAsync(fixture);
        await using (AccessWriter writer = await OpenWriterAsync(ms))
        {
            await writer.InsertRowAsync("Table1", new RowValues { ["id"] = "row5" }, Ct);
        }

        // An earlier build left row5's slots null; Access's complex indexes
        // still hold the reference the insert gave it.
        await ClearComplexReferencesAsync(ms, "Table1", r => (string)r[0] == "row5", clearCounter: false);

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
                await writer.AddMultiValueItemAsync("Table1", "multi-value-data", new Dictionary<string, object?> { ["id"] = "row5" }, "writer-value", Ct));
        }

        RawTable table = await ReadRawTableAsync(ms, "Table1");
        object[] row5 = Assert.Single(table.Rows, r => (string)r[0] == "row5");
        Assert.Equal(counter + 2, table.ComplexAutoNumber);
        foreach (ComplexDataColumn column in ComplexDataColumns)
        {
            Assert.Equal(counter + 2, Slot(table, row5, column.Name));

            // The patched slots replaced the old keys in every complex index.
            IEnumerable<string> expected = table.Rows.Select(r => Convert.ToHexString(IndexKeyEncoder.EncodeEntry(ColumnType.ComplexType, Slot(table, r, column.Name))));
            IEnumerable<string> onDisk = (await ReadIndexLeafKeysAsync(ms, "Table1", column.Index)).Select(Convert.ToHexString);
            Assert.Equal(expected.Order(StringComparer.Ordinal), onDisk);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal(["writer-value"], (await reader.GetMultiValueItemsAsync("Table1", "multi-value-data", Ct)).Where(i => i.ConceptualTableId == counter + 2).Select(i => i.Value));
    }

    [Fact]
    public async Task AddAttachment_ZeroCounterFile_SeedsAboveExistingReferences()
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms))
        {
            await CreateDocsAsync(writer);
            await writer.InsertRowsAsync("Docs", [[1, DBNull.Value, DBNull.Value], [2, DBNull.Value, DBNull.Value]], Ct);
            await writer.AddAttachmentAsync("Docs", "Files", Row1, new AttachmentInput("one.txt", [1]), Ct);
            await writer.AddAttachmentAsync("Docs", "Files", Row2, new AttachmentInput("two.txt", [2]), Ct);
        }

        Assert.Equal([1, 2], (await ReadRawTableAsync(ms, "Docs")).Rows.Select(r => ((ComplexIdRef)r[1]).Id));
        await ZeroComplexAutoNumberAsync(ms, "Docs");

        await using (AccessWriter writer = await OpenWriterAsync(ms))
        {
            await writer.InsertRowAsync("Docs", [3, DBNull.Value, DBNull.Value], Ct);
            await writer.AddAttachmentAsync("Docs", "Files", Row3, new AttachmentInput("three.txt", [3]), Ct);
        }

        RawTable docs = await ReadRawTableAsync(ms, "Docs");
        Assert.Equal(3, Slot(docs, docs.Rows[2], "Files"));
        Assert.Equal(3, docs.ComplexAutoNumber);
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
    public async Task SchemaRewrite_AfterDeletingTopRow_KeepsComplexAutoNumber(ComplexWriteMode mode)
    {
        await using MemoryStream ms = await CreateDeletedTopParentScenarioAsync(ComplexWriteMode.Direct);

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
    public async Task InsertRows_TableWithComplexColumns_AssignsOneReferencePerRow(ComplexWriteMode mode)
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
    public async Task InsertRow_AccessFixture_ContinuesFromTdefCounter(string fixture, int counter, ComplexWriteMode mode)
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
    public async Task InsertRows_FailedBatch_RewindsComplexCounter(ComplexWriteMode mode)
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
                _ = await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.InsertRowsAsync("Docs", [[4, DBNull.Value], [4, DBNull.Value]], Ct));
                await writer.InsertRowAsync("Docs", [5, DBNull.Value], Ct);
            });
        }

        RawTable docs = await ReadRawTableAsync(ms, "Docs");
        Assert.Equal(["1|1", "5|2"], docs.Rows.Select(r => $"{r[0]}|{Slot(docs, r, "Files")}"));
        Assert.Equal(2, docs.ComplexAutoNumber);
    }

    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task AddColumn_AttachmentOnTableWithRows_GivesExistingRowsReferences(ComplexWriteMode mode)
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
    public async Task AddColumn_ComplexColumnBesideAnother_SharesEachRowsReference(ComplexWriteMode mode)
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

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal(["2:two.txt"], (await reader.GetAttachmentsAsync("Docs", "Files", Ct)).Select(a => $"{a.ConceptualTableId}:{a.FileName}"));
        Assert.Equal(["2:42"], (await reader.GetMultiValueItemsAsync("Docs", "Tags", Ct)).Select(i => FormattableString.Invariant($"{i.ConceptualTableId}:{i.Value}")));
    }

    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task AddColumn_ComplexColumn_RowsWithoutOneUniqueReference_GetFreshReferences(ComplexWriteMode mode)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms))
        {
            await CreateDocsAsync(writer);
            await writer.InsertRowsAsync("Docs", [.. Enumerable.Range(1, 5).Select(id => new object?[] { id, DBNull.Value, DBNull.Value })], Ct);
        }

        // Shapes an earlier build could leave: a null slot (row 1), complex
        // columns holding different references (row 2) and two rows holding
        // the same one (rows 3 and 4). An existing column's flat table may
        // hold rows for any of those references, so only row 5's reference
        // is safe for the new column to share.
        await SetComplexSlotsAsync(
            ms,
            "Docs",
            r => (int)r[0] is 1 or 2 or 4,
            (r, column) => ((int)r[0], column) switch
            {
                (1, "Tags") => null,
                (2, "Tags") => 6,
                (4, _) => 3,
                _ => (int)r[0],
            });

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
                await writer.AddColumnAsync("Docs", new ColumnDefinition("Labels", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(int) }, Ct));
        }

        RawTable docs = await ReadRawTableAsync(ms, "Docs");
        Assert.Equal(
            ["1|1|7|7", "2|2|6|8", "3|3|3|9", "4|3|3|10", "5|5|5|5"],
            docs.Rows.Select(r => $"{r[0]}|{Slot(docs, r, "Files")}|{Slot(docs, r, "Tags")}|{Slot(docs, r, "Labels")}"));
        Assert.Equal(10, docs.ComplexAutoNumber);
    }

    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task InsertRow_SuppliedReference_IsSharedByTheRowsComplexColumns(ComplexWriteMode mode)
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
    public async Task UpdateRows_AssigningComplexColumn_IsRejected(ComplexWriteMode mode)
    {
        // Row 1 holds reference 1 (one.txt); row 3 holds 3 (three.txt and the tag 7).
        await using MemoryStream ms = await CreateDeletedTopParentScenarioAsync(ComplexWriteMode.Direct);

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
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, format, WriterOptions(ComplexWriteMode.Direct), leaveOpen: true, cancellationToken: Ct))
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
    private static async Task<MemoryStream> CreateDeletedTopParentScenarioAsync(ComplexWriteMode mode)
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

    private static async Task ZeroComplexAutoNumberAsync(MemoryStream ms, string tableName)
    {
        ms.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: Ct);
        long tdefPage = (await harness.Services.Catalog.ResolveRequiredTableAsync(tableName, Ct)).Entry.TDefPage;
        byte[] tdef = await harness.Database.Pages.ReadPageCopyAsync(tdefPage, Ct);
        tdef.AsSpan(ComplexAutoNumberOffset, 4).Clear();
        await harness.Pager.WritePageAsync(tdefPage, tdef, Ct);
    }

    /// <summary>A complexDataTest Table1 complex column and the unique index Access keeps on it.</summary>
    /// <param name="Name">The column name.</param>
    /// <param name="Index">The index name.</param>
    private sealed record ComplexDataColumn(string Name, string Index);
}
