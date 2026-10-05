namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tables;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueEncoding;
using Xunit;

/// <summary>
/// Writer workflows on overflow rows, the rows Access moved to another slot when
/// they grew. The row keeps its identity at the header slot, which index entries
/// name, so a delete flags the header deleted (<c>0xC000</c>, Jackcess
/// <c>TableImpl.deleteRow</c>) and an update rewrites the row from the moved bytes.
/// Tables whose <c>MSysObjects</c> row is an overflow row (Orders, Products and
/// Welcome in NorthwindTraders.accdb, Categories in nwind.mdb) were invisible to
/// the writer's catalog, so DDL and foreign-key checks failed on them.
/// </summary>
public sealed class OverflowRowWriteTests
{
    private const int OverflowFlags = 0xC000;
    private const int OverflowHeader = 0x4000;
    private const string SyntheticTable = "Items";

    /// <summary>The <c>ParentId</c> of the row the cascade tests move to an overflow slot (<c>Id</c> 1).</summary>
    private const int MovedChildParentId = 2;

    /// <summary>Gets the tables with overflow rows, each in every write mode.</summary>
    public static TheoryData<string, WriteMode, bool> OverflowSourcesAndModes => Combine(
        nameof(TestDatabases.ExtDateTestV2019),
        nameof(TestDatabases.OverflowTestV2000),
        "SyntheticJet3");

    /// <summary>Gets the tables with overflow rows that have a text column to update, each in every write mode.</summary>
    public static TheoryData<string, WriteMode, bool> UpdateSourcesAndModes => Combine(
        nameof(TestDatabases.ExtDateTestV2019),
        nameof(TestDatabases.OverflowTestV2000),
        "SyntheticJet3");

    /// <summary>Gets every format with each write mode that keeps its change.</summary>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndCommittedModes
    {
        get
        {
            var data = new TheoryData<DatabaseFormat, WriteMode>();
            foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
            {
                foreach (WriteMode mode in new[] { WriteMode.Direct, WriteMode.AutoCommit, WriteMode.ExplicitCommit })
                {
                    data.Add(format, mode);
                }
            }

            return data;
        }
    }

    /// <summary>Gets each shared write mode and a local rollback flag.</summary>
    public static TheoryData<WriteMode, bool> Modes => new() { { WriteMode.Direct, false }, { WriteMode.AutoCommit, false }, { WriteMode.ExplicitCommit, false }, { WriteMode.ExplicitCommit, true } };

    /// <summary>Gets every overflow layout and format in each committed write mode.</summary>
    public static TheoryData<DatabaseFormat, WriteMode, OverflowRowLayout> EraseLayouts
    {
        get
        {
            var data = new TheoryData<DatabaseFormat, WriteMode, OverflowRowLayout>();
            foreach (DatabaseFormat format in Enum.GetValues<DatabaseFormat>())
            {
                if (format is not (DatabaseFormat.Jet3Mdb or DatabaseFormat.Jet4Mdb or DatabaseFormat.AceAccdb))
                {
                    continue;
                }

                foreach (WriteMode mode in Enum.GetValues<WriteMode>())
                {
                    data.Add(format, mode, OverflowRowLayout.CrossPage);
                    data.Add(format, mode, OverflowRowLayout.SamePage);
                    data.Add(format, mode, OverflowRowLayout.TwoHop);
                }
            }

            return data;
        }
    }

    /// <summary>Gets clearing modes for valid chains and pointers into another table.</summary>
    public static TheoryData<WriteMode, bool, OverflowRowLayout, bool> ClearingModes
    {
        get
        {
            var data = new TheoryData<WriteMode, bool, OverflowRowLayout, bool>();
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                data.Add(mode, false, OverflowRowLayout.TwoHop, false);
                data.Add(mode, false, OverflowRowLayout.PointerToOtherTable, false);
                data.Add(mode, false, OverflowRowLayout.PointerToOtherTable, true);
            }

            data.Add(WriteMode.ExplicitCommit, true, OverflowRowLayout.TwoHop, false);
            return data;
        }
    }

    public static TheoryData<DatabaseFormat> Formats =>
    [
        DatabaseFormat.Jet3Mdb,
        DatabaseFormat.Jet4Mdb,
        DatabaseFormat.AceAccdb,
    ];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>
    /// Deleting every row deletes the overflow rows too: their header slots end up
    /// flagged <c>0xC000</c>, and no primary-key entry is left behind.
    /// </summary>
    /// <param name="source">The table with overflow rows.</param>
    /// <param name="mode">How the writer runs the delete.</param>
    /// <param name="rollback">Whether to roll back the explicit transaction.</param>
    [Theory]
    [MemberData(nameof(OverflowSourcesAndModes))]
    public async Task DeleteRows_All_OnOverflowRows_RemovesThemAndTheirIndexEntries(string source, WriteMode mode, bool rollback)
    {
        (byte[] bytes, string table) = await LoadAsync(source);
        List<(long Page, int Row)> headers = await FindOverflowHeadersAsync(bytes, table);
        Assert.NotEmpty(headers);
        int rowCount = await CountRowsAsync(bytes, table);

        await using var ms = new MemoryStream();
        await ms.WriteAsync(bytes, Ct);
        int deleted = 0;
        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, rollback, async () => deleted = await writer.DeleteRowsAsync(table, RowCriteria.All(), Ct));
        }

        byte[] after = ms.ToArray();
        bool rolledBack = rollback;
        Assert.Equal(rowCount, deleted);
        Assert.Equal(rolledBack ? rowCount : 0, await CountRowsAsync(after, table));
        (int pageSize, int rowsStart) = await PageLayoutAsync(after);
        Assert.All(headers, h => Assert.Equal(rolledBack ? OverflowHeader : OverflowFlags, SyntheticOverflowRows.ReadSlot(after, pageSize, rowsStart, h.Page, h.Row) & OverflowFlags));
        if (await CountPrimaryKeyEntriesAsync(after, table) is int entries)
        {
            Assert.Equal(rolledBack ? rowCount : 0, entries);
        }
    }

    /// <summary>
    /// Deleting one overflow row by key removes exactly that row and its index entry.
    /// </summary>
    /// <param name="mode">How the writer runs the delete.</param>
    /// <param name="rollback">Whether to roll back the explicit transaction.</param>
    [Theory]
    [MemberData(nameof(Modes))]
    public async Task DeleteRows_ByKey_OverflowRow(WriteMode mode, bool rollback)
    {
        byte[] bytes = await File.ReadAllBytesAsync(TestDatabases.ExtDateTestV2019, Ct);
        Assert.True(await IsOverflowRowAsync(bytes, "Table1", "ID", 7), "ID 7 of extDateTestV2019 should be an overflow row.");

        await using var ms = new MemoryStream();
        await ms.WriteAsync(bytes, Ct);
        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, rollback, async () => Assert.Equal(1, await writer.DeleteRowsAsync("Table1", "ID", 7, Ct)));
        }

        byte[] after = ms.ToArray();
        int expected = rollback ? 9 : 8;
        Assert.Equal(expected, await CountRowsAsync(after, "Table1"));
        Assert.Equal(expected, await CountPrimaryKeyEntriesAsync(after, "Table1"));
        await AssertEverySeekHitsOnceAsync(after, "Table1", "ID");
    }

    /// <summary>
    /// Updating every row rewrites the overflow rows too, and the indexes stay in
    /// step with the rows.
    /// </summary>
    /// <param name="source">The table with overflow rows.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <param name="rollback">Whether to roll back the explicit transaction.</param>
    [Theory]
    [MemberData(nameof(UpdateSourcesAndModes))]
    public async Task UpdateRows_All_OnOverflowRows_RewritesEveryRow(string source, WriteMode mode, bool rollback)
    {
        (byte[] bytes, string table) = await LoadAsync(source);
        List<(long Page, int Row)> headers = await FindOverflowHeadersAsync(bytes, table);
        Assert.NotEmpty(headers);
        int rowCount = await CountRowsAsync(bytes, table);
        string column = await FirstTextColumnAsync(bytes, table);

        await using var ms = new MemoryStream();
        await ms.WriteAsync(bytes, Ct);
        int updated = 0;
        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, rollback, async () => updated = await writer.UpdateRowsAsync(table, RowCriteria.All(), RowValues.Create().Set(column, "updated"), Ct));
        }

        // An update rewrites each row in a new slot and deletes the old one, so
        // every overflow header ends up flagged deleted.
        byte[] after = ms.ToArray();
        (int pageSize, int rowsStart) = await PageLayoutAsync(after);
        Assert.All(headers, h => Assert.Equal(rollback ? OverflowHeader : OverflowFlags, SyntheticOverflowRows.ReadSlot(after, pageSize, rowsStart, h.Page, h.Row) & OverflowFlags));
        Assert.Equal(rowCount, updated);
        await using AccessReader reader = await OpenReaderAsync(after);
        using DataTable data = await reader.ReadDataTableAsync(table, cancellationToken: Ct);
        Assert.Equal(rowCount, data.Rows.Count);
        int changed = data.Rows.Cast<DataRow>().Count(row => Equals(row[column], "updated"));
        Assert.Equal(rollback ? 0 : rowCount, changed);
        if (await CountPrimaryKeyEntriesAsync(after, table) is int entries)
        {
            Assert.Equal(rowCount, entries);
            await AssertEverySeekHitsOnceAsync(after, table, "ID");
        }
    }

    /// <summary>
    /// With <see cref="SecureEraseMode.DeletedRowsAndFreedPages"/>, deleting an
    /// overflow row zeroes the moved row's bytes and the header's pointer, not just
    /// the header slot.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The transaction mode.</param>
    /// <param name="layout">The overflow layout.</param>
    [Theory]
    [MemberData(nameof(EraseLayouts))]
    public async Task SecureErase_DeleteOverflowRow_ClearsMovedRowBytes(DatabaseFormat format, WriteMode mode, OverflowRowLayout layout)
    {
        byte[] bytes = await SyntheticOverflowRows.CreateTableAsync(format, SyntheticTable, 300, primaryKey: true, Ct);
        SyntheticOverflowRow moved = await SyntheticOverflowRows.MoveRowAsync(bytes, SyntheticTable, layout, Ct);
        int movedId = await ReadOverflowIdAsync(bytes);
        (int pageSize, int rowsStart) = await PageLayoutAsync(bytes);
        int headerStart = SyntheticOverflowRows.ReadSlot(bytes, pageSize, rowsStart, moved.HeaderPage, moved.HeaderRow) & 0x1FFF;
        Assert.False(IsZero(bytes, moved.DataPage, pageSize, moved.DataStart, moved.DataSize));

        await using var ms = new MemoryStream();
        await ms.WriteAsync(bytes, Ct);
        ms.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            ms,
            new AccessWriterOptions { UseLockFile = false, SecureEraseMode = SecureEraseMode.DeletedRowsAndFreedPages, UseTransactionalWrites = mode == WriteMode.AutoCommit },
            leaveOpen: true,
            Ct))
        {
            await RunAsync(writer, mode, false, async () => Assert.Equal(1, await writer.DeleteRowsAsync(SyntheticTable, "Id", movedId, Ct)));
        }

        byte[] after = ms.ToArray();
        if (layout == OverflowRowLayout.TwoHop)
        {
            int middleStart = SyntheticOverflowRows.ReadSlot(bytes, pageSize, rowsStart, moved.DataPage, moved.DataRow + 1) & 0x1FFF;
            Assert.True(IsZero(after, moved.DataPage, pageSize, middleStart, 4), "The middle pointer was not erased.");
        }

        Assert.True(IsZero(after, moved.DataPage, pageSize, moved.DataStart, moved.DataSize), "The moved row's bytes were not erased.");
        Assert.True(IsZero(after, moved.HeaderPage, pageSize, headerStart, 4), "The header's pointer was not erased.");
        Assert.Equal(OverflowFlags, SyntheticOverflowRows.ReadSlot(after, pageSize, rowsStart, moved.HeaderPage, moved.HeaderRow) & OverflowFlags);
        Assert.Equal(await CountRowsAsync(bytes, SyntheticTable) - 1, await CountRowsAsync(after, SyntheticTable));
    }

    /// <summary>Explicit row clearing scrubs a two-hop chain and rolls back with the transaction.</summary>
    /// <param name="mode">The transaction mode.</param>
    /// <param name="rollback">Whether to roll back.</param>
    /// <param name="layout">The pointer layout.</param>
    /// <param name="secureErase">Whether secure erase is enabled.</param>
    [Theory]
    [MemberData(nameof(ClearingModes))]
    public async Task Clear_DeleteTwoHopOverflowRow_ClearsAllSlots(WriteMode mode, bool rollback, OverflowRowLayout layout, bool secureErase)
    {
        byte[] bytes = await SyntheticOverflowRows.CreateTableAsync(DatabaseFormat.Jet4Mdb, SyntheticTable, 300, primaryKey: true, Ct);
        SyntheticOverflowRow moved = await SyntheticOverflowRows.MoveRowAsync(bytes, SyntheticTable, layout, Ct);
        (int pageSize, int rowsStart) = await PageLayoutAsync(bytes);
        int headerStart = SyntheticOverflowRows.ReadSlot(bytes, pageSize, rowsStart, moved.HeaderPage, moved.HeaderRow) & 0x1FFF;
        int middleStart = layout == OverflowRowLayout.TwoHop
            ? SyntheticOverflowRows.ReadSlot(bytes, pageSize, rowsStart, moved.DataPage, moved.DataRow + 1) & 0x1FFF
            : 0;
        await using var stream = new MemoryStream();
        await stream.WriteAsync(bytes, Ct);
        stream.Position = 0;
        var options = new AccessWriterOptions
        {
            UseLockFile = false,
            UseByteRangeLocks = false,
            UseTransactionalWrites = mode == WriteMode.AutoCommit,
            SecureEraseMode = secureErase ? SecureEraseMode.DeletedRowsAndFreedPages : SecureEraseMode.None,
        };
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, options, cancellationToken: Ct))
        {
            DatabaseFile db = harness.Database;
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.Services.Catalog.GetCatalogEntryAsync(SyntheticTable, Ct));
            TableDef tableDef = await db.TableDefs.ReadRequiredTableDefAsync(entry.TDefPage, SyntheticTable, Ct);
            var store = new TableRowStore(
                db.Format,
                db.OwnedPages,
                harness.Pager,
                options,
                new LongValueEncoder(db.Format, harness.Pager, harness.Services.PageAllocator, options),
                new RowEncoder(db.Format),
                new DataPageInserter(db.Format, db.OwnedPages, harness.Pager, harness.Services.PageAllocator, harness.Services.OwnedMaps, new UsageMapEditor(db.Format, harness.Pager, harness.Services.PageAllocator)),
                new TDefPageBuilder(db.Format, harness.Pager));
            if (mode == WriteMode.ExplicitCommit)
            {
                await using JetTransaction transaction = await harness.BeginTransactionAsync(Ct);
                await store.MarkRowDeletedAsync(moved.HeaderPage, moved.HeaderRow, tableDef, DeletedRowDataMode.Clear, Ct);
                if (rollback)
                {
                    await transaction.RollbackAsync(Ct);
                }
                else
                {
                    await transaction.CommitAsync(Ct);
                }
            }
            else
            {
                await harness.Services.Transactions.RunAutoCommitAsync(
                    _ => store.MarkRowDeletedAsync(moved.HeaderPage, moved.HeaderRow, tableDef, DeletedRowDataMode.Clear, Ct), Ct);
            }
        }

        byte[] after = stream.ToArray();
        if (rollback)
        {
            Assert.Equal(bytes, after);
        }
        else
        {
            Assert.True(IsZero(after, moved.HeaderPage, pageSize, headerStart, 4));
            if (layout == OverflowRowLayout.TwoHop)
            {
                Assert.True(IsZero(after, moved.DataPage, pageSize, middleStart, 4));
                Assert.True(IsZero(after, moved.DataPage, pageSize, moved.DataStart, moved.DataSize));
            }
            else
            {
                for (int pageNumber = 0; pageNumber < bytes.Length / pageSize; pageNumber++)
                {
                    if (pageNumber != moved.HeaderPage)
                    {
                        Assert.Equal(bytes.AsSpan(pageNumber * pageSize, pageSize).ToArray(), after.AsSpan(pageNumber * pageSize, pageSize).ToArray());
                    }
                }
            }
        }
    }

    /// <summary>
    /// A cascade delete from a parent row reaches a child row stored as an overflow
    /// row: the foreign-key index names the child's header slot, and the child is
    /// deleted through it.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the delete.</param>
    [Theory]
    [MemberData(nameof(FormatsAndCommittedModes))]
    public async Task CascadeDelete_ToOverflowChildRow_DeletesIt(DatabaseFormat format, WriteMode mode)
    {
        (byte[] bytes, SyntheticOverflowRow moved) = await CreateParentsAndOverflowChildAsync(format);
        int[] before = await ReadParentIdsAsync(bytes);
        Assert.Equal(MovedChildParentId, before[0]);

        await using var ms = new MemoryStream();
        await ms.WriteAsync(bytes, Ct);
        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, false, async () => Assert.Equal(1, await writer.DeleteRowsAsync("Parents", "Id", MovedChildParentId, Ct)));
        }

        byte[] after = ms.ToArray();
        int[] remaining = await ReadParentIdsAsync(after);
        Assert.DoesNotContain(MovedChildParentId, remaining);
        Assert.Equal(before.Count(id => id != MovedChildParentId), remaining.Length);
        Assert.Equal(OverflowFlags, await ReadSlotFlagsAsync(after, moved.HeaderPage, moved.HeaderRow));
    }

    /// <summary>
    /// A cascade update from a parent row rewrites a child row stored as an
    /// overflow row with the new key.
    /// </summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [MemberData(nameof(Formats))]
    public async Task CascadeUpdate_ToOverflowChildRow_RewritesIt(DatabaseFormat format)
    {
        const int newParentId = 20;
        (byte[] bytes, SyntheticOverflowRow moved) = await CreateParentsAndOverflowChildAsync(format);
        int[] before = await ReadParentIdsAsync(bytes);

        await using var ms = new MemoryStream();
        await ms.WriteAsync(bytes, Ct);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("Parents", "Id", MovedChildParentId, new Dictionary<string, object?> { ["Id"] = newParentId }, Ct));
        }

        byte[] after = ms.ToArray();
        int[] parentIds = await ReadParentIdsAsync(after);
        Assert.DoesNotContain(MovedChildParentId, parentIds);
        Assert.Equal(before.Count(id => id == MovedChildParentId), parentIds.Count(id => id == newParentId));
        Assert.Equal(before.Length, parentIds.Length);
        Assert.Equal(OverflowFlags, await ReadSlotFlagsAsync(after, moved.HeaderPage, moved.HeaderRow));
    }

    /// <summary>
    /// Deleting a row stored as an overflow row deletes its attachments too: the
    /// complex reference is read from the row's moved bytes.
    /// </summary>
    [Fact]
    public async Task DeleteRows_OverflowRowWithAttachment_DeletesItsAttachments()
    {
        byte[] bytes = await CreateAttachmentTableAsync([(1, "a.txt"), (2, "b.txt")]);
        await using (AccessReader original = await OpenReaderAsync(bytes))
        {
            Assert.Equal(2, (await original.GetAttachmentsAsync(SyntheticTable, "Files", Ct)).Count);
        }

        await using var ms = new MemoryStream();
        await ms.WriteAsync(bytes, Ct);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            Assert.Equal(1, await writer.DeleteRowsAsync(SyntheticTable, "Id", 1, Ct));
        }

        await using AccessReader reader = await OpenReaderAsync(ms.ToArray());
        AttachmentRecord remaining = Assert.Single(await reader.GetAttachmentsAsync(SyntheticTable, "Files", Ct));
        Assert.Equal("b.txt", remaining.FileName);
    }

    /// <summary>
    /// An attachment added to a row stored as an overflow row joins that row's
    /// attachments: the row's complex reference is read from the row's moved
    /// bytes, not from the same offset on its header slot's page.
    /// </summary>
    /// <param name="attachedBeforeMove">Whether the row already had an attachment when it moved.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AddAttachment_ToOverflowRow_JoinsTheRowsAttachments(bool attachedBeforeMove)
    {
        byte[] bytes = await CreateAttachmentTableAsync(attachedBeforeMove ? [(1, "a.txt"), (2, "b.txt")] : [(2, "b.txt")]);
        int rows = await CountRowsAsync(bytes, SyntheticTable);

        await using var ms = new MemoryStream();
        await ms.WriteAsync(bytes, Ct);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.AddAttachmentAsync(SyntheticTable, "Files", new Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("c.txt", [3]), Ct);
        }

        byte[] after = ms.ToArray();
        Dictionary<int, string[]> attachments = await ReadAttachmentsByIdAsync(after);
        Assert.Equal(attachedBeforeMove ? ["a.txt", "c.txt"] : ["c.txt"], attachments[1]);
        Assert.Equal(["b.txt"], attachments[2]);
        Assert.Equal(2, attachments.Count);
        Assert.Equal(rows, await CountRowsAsync(after, SyntheticTable));
    }

    /// <summary>
    /// The Orders table of NorthwindTraders.accdb has an overflow catalog row, so the
    /// writer used to accept a second table named Orders.
    /// </summary>
    [Fact]
    public async Task CreateTable_NameOfTableWithOverflowCatalogRow_Throws()
    {
        await using MemoryStream ms = await CopyAsync(TestDatabases.NorthwindTraders);
        await using AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct);

        JetObjectExistsException ex = await Assert.ThrowsAsync<JetObjectExistsException>(
            async () => await writer.CreateTableAsync("Orders", [new ColumnDefinition("Id", typeof(int))], Ct));
        Assert.Contains("already exists", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Dropping a table whose catalog row is an overflow row deletes that row
    /// through its header slot.
    /// </summary>
    /// <param name="mode">How the writer runs the drop.</param>
    /// <param name="rollback">Whether to roll back the explicit transaction.</param>
    [Theory]
    [MemberData(nameof(Modes))]
    public async Task DropTable_TableWithOverflowCatalogRow_RemovesIt(WriteMode mode, bool rollback)
    {
        await using MemoryStream ms = await CopyAsync(TestDatabases.NorthwindTraders);
        byte[] before = ms.ToArray();
        CatalogRow welcome = await FindCatalogRowAsync(before, "Welcome");
        Assert.Equal(OverflowHeader, await ReadSlotFlagsAsync(before, welcome.PageNumber, welcome.RowIndex));
        int catalogRows = await CountRowsAsync(before, "MSysObjects");

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, rollback, async () => await writer.DropTableAsync("Welcome", Ct));
        }

        byte[] after = ms.ToArray();
        bool rolledBack = rollback;
        await using AccessReader reader = await OpenReaderAsync(after);
        Assert.Equal(rolledBack, (await reader.ListTablesAsync(Ct)).Contains("Welcome"));
        Assert.Equal(rolledBack ? OverflowHeader : OverflowFlags, await ReadSlotFlagsAsync(after, welcome.PageNumber, welcome.RowIndex));
        Assert.Equal(rolledBack ? catalogRows : catalogRows - 1, await CountRowsAsync(after, "MSysObjects"));
    }

    /// <summary>
    /// Adding a column to a table whose catalog row is an overflow row rewrites the
    /// table under the same single catalog row, with every row and index.
    /// </summary>
    [Fact]
    public async Task AddColumn_TableWithOverflowCatalogRow_KeepsOneCatalogRow()
    {
        await using MemoryStream ms = await CopyAsync(TestDatabases.NorthwindTraders);
        int rows = await CountRowsAsync(ms.ToArray(), "Orders");
        int indexes = await CountIndexesAsync(ms.ToArray(), "Orders");
        Assert.Equal(52, rows);

        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.AddColumnAsync("Orders", new ColumnDefinition("Extra", typeof(int)), Ct);
        }

        byte[] after = ms.ToArray();
        await using AccessReader reader = await OpenReaderAsync(after);
        Assert.Single(await reader.ListTablesAsync(Ct), t => t == "Orders");
        Assert.Contains(await reader.GetColumnMetadataAsync("Orders", Ct), c => c.Name == "Extra");
        Assert.Equal(rows, await CountRowsAsync(after, "Orders"));
        Assert.Equal(indexes, await CountIndexesAsync(after, "Orders"));
    }

    /// <summary>
    /// Renaming a column of a table whose catalog row is an overflow row works and
    /// keeps every row.
    /// </summary>
    [Fact]
    public async Task RenameColumn_TableWithOverflowCatalogRow_Renames()
    {
        await using MemoryStream ms = await CopyAsync(TestDatabases.NorthwindTraders);
        int rows = await CountRowsAsync(ms.ToArray(), "Products");

        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.RenameColumnAsync("Products", "ProductName", "ProductTitle", Ct);
        }

        byte[] after = ms.ToArray();
        await using AccessReader reader = await OpenReaderAsync(after);
        IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync("Products", Ct);
        Assert.Contains(columns, c => c.Name == "ProductTitle");
        Assert.DoesNotContain(columns, c => c.Name == "ProductName");
        Assert.Equal(rows, await CountRowsAsync(after, "Products"));
    }

    /// <summary>
    /// The foreign-key check against Orders sees Orders' rows: an update of an
    /// OrderDetails row succeeds, an insert naming a missing order is rejected, and
    /// one naming an existing order is accepted.
    /// </summary>
    [Fact]
    public async Task UpdateRows_ChildOfTableWithOverflowCatalogRow_ChecksForeignKey()
    {
        await using MemoryStream ms = await CopyAsync(TestDatabases.NorthwindTraders);
        (RowValues missingOrder, RowValues existingOrder) = await BuildOrderDetailRowsAsync(ms.ToArray());

        await using AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct);
        Assert.Equal(1, await writer.UpdateRowsAsync("OrderDetails", "OrderDetailID", 1, new Dictionary<string, object?> { ["Quantity"] = 3 }, Ct));

        JetConstraintException ex = await Assert.ThrowsAsync<JetConstraintException>(
            async () => await writer.InsertRowAsync("OrderDetails", missingOrder, Ct));
        Assert.Contains("foreign-key", ex.Message, StringComparison.OrdinalIgnoreCase);

        await writer.InsertRowAsync("OrderDetails", existingOrder, Ct);
    }

    /// <summary>
    /// Categories in the Jet3 nwind.mdb has an overflow catalog row; adding a column
    /// keeps it listed once with all eight rows.
    /// </summary>
    [Fact]
    public async Task AddColumn_Jet3TableWithOverflowCatalogRow_KeepsOneCatalogRow()
    {
        await using MemoryStream ms = await CopyAsync(TestDatabases.MdbtoolsNwind);
        await using (AccessReader original = await OpenReaderAsync(ms.ToArray()))
        {
            Assert.Equal(DatabaseFormat.Jet3Mdb, original.DatabaseFormat);
        }

        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.AddColumnAsync("Categories", new ColumnDefinition("Extra", typeof(int)), Ct);
        }

        byte[] after = ms.ToArray();
        await using AccessReader reader = await OpenReaderAsync(after);
        Assert.Single(await reader.ListTablesAsync(Ct), t => t == "Categories");
        Assert.Equal(8, await CountRowsAsync(after, "Categories"));
    }

    /// <summary>
    /// Table3_desc in testIndexCodesV2000.mdb has an overflow catalog row; dropping
    /// it removes it from the catalog.
    /// </summary>
    [Fact]
    public async Task DropTable_Jet4TableWithOverflowCatalogRow_RemovesIt()
    {
        await using MemoryStream ms = await CopyAsync(TestDatabases.TestIndexCodesV2000);
        CatalogRow row = await FindCatalogRowAsync(ms.ToArray(), "Table3_desc");
        Assert.Equal(OverflowHeader, await ReadSlotFlagsAsync(ms.ToArray(), row.PageNumber, row.RowIndex));

        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.DropTableAsync("Table3_desc", Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms.ToArray());
        IReadOnlyList<string> tables = await reader.ListTablesAsync(Ct);
        Assert.DoesNotContain("Table3_desc", tables);
        Assert.Contains("Table3", tables);
    }

    /// <summary>
    /// With the overflow catalog rows counted, the usage map of NorthwindTraders'
    /// <c>MSysObjects</c> validates, so an update that matches nothing no longer
    /// reads the whole 12 MB file to find the catalog's pages.
    /// </summary>
    [Fact]
    public async Task Writer_NoMatchUpdate_OnNorthwind_ReadsFewPages()
    {
        await using MemoryStream backing = await CopyAsync(TestDatabases.NorthwindTraders);
        await using var counting = new CountingStream(backing);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(counting, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            Assert.Equal(0, await writer.UpdateRowsAsync("OrderDetails", "OrderDetailID", -1, new Dictionary<string, object?> { ["Quantity"] = 3 }, Ct));
        }

        long pages = counting.BytesRead / 4096;
        Assert.True(pages < 256, $"A no-match update read {pages} pages of a {backing.Length / 4096}-page file.");
    }

    /// <summary>
    /// Adds <see cref="SyntheticTable"/> with a <c>ParentId</c> column (<c>Id</c> mod 5,
    /// plus 1) to a copy of an Access-authored indexTest fixture, which has the
    /// <c>MSysRelationships</c> table a writer-created Jet3 or Jet4 file lacks; moves
    /// the row with <c>Id</c> 1 (<c>ParentId</c> <see cref="MovedChildParentId"/>) to
    /// an overflow slot on another page; and then adds a <c>Parents</c> table
    /// (<c>Id</c> 1 to 5) and an enforced relationship to it that cascades deletes
    /// and updates.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="format"/> is not a defined format.</exception>
    private static async ValueTask<(byte[] Bytes, SyntheticOverflowRow Moved)> CreateParentsAndOverflowChildAsync(DatabaseFormat format)
    {
        string fixture = format switch
        {
            DatabaseFormat.Jet3Mdb => TestDatabases.IndexTestV1997,
            DatabaseFormat.Jet4Mdb => TestDatabases.IndexTestV2000,
            DatabaseFormat.AceAccdb => TestDatabases.IndexTestV2007,
            _ => throw new ArgumentOutOfRangeException(nameof(format), format, null),
        };
        byte[] bytes = await SyntheticOverflowRows.AddTableAsync(
            await File.ReadAllBytesAsync(fixture, Ct),
            SyntheticTable,
            300,
            primaryKey: true,
            [new ColumnDefinition("ParentId", typeof(int))],
            id => [(id % 5) + 1],
            Ct);
        SyntheticOverflowRow moved = await SyntheticOverflowRows.MoveRowAsync(bytes, SyntheticTable, OverflowRowLayout.CrossPage, Ct);
        Assert.True(await IsOverflowRowAsync(bytes, SyntheticTable, "Id", 1), "Id 1 should be the overflow row.");

        await using var ms = new MemoryStream();
        await ms.WriteAsync(bytes, Ct);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync(
                "Parents",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("PrimaryKey", "Id") { IsPrimaryKey = true }],
                Ct);
            await writer.InsertRowsAsync("Parents", Enumerable.Range(1, 5).Select(id => new object[] { id }), Ct);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("ParentsItems", "Parents", "Id", SyntheticTable, "ParentId") { CascadeDeletes = true, CascadeUpdates = true },
                Ct);
        }

        return (ms.ToArray(), moved);
    }

    /// <summary>Returns the <c>ParentId</c> of every row of <see cref="SyntheticTable"/>, in <c>Id</c> order.</summary>
    /// <param name="bytes">The database image.</param>
    private static async ValueTask<int[]> ReadParentIdsAsync(byte[] bytes)
    {
        await using AccessReader reader = await OpenReaderAsync(bytes);
        using DataTable data = await reader.ReadDataTableAsync(SyntheticTable, cancellationToken: Ct);
        return [.. data.Rows.Cast<DataRow>().OrderBy(row => (int)row["Id"]).Select(row => (int)row["ParentId"])];
    }

    /// <summary>
    /// Creates <see cref="SyntheticTable"/> on ACCDB with an Attachment column
    /// <c>Files</c>, adds <paramref name="attachments"/>, and then moves the row with
    /// <c>Id</c> 1 to an overflow slot on another page.
    /// </summary>
    /// <param name="attachments">The attachments to add before the move, by the parent row's <c>Id</c>.</param>
    private static async ValueTask<byte[]> CreateAttachmentTableAsync((int Id, string FileName)[] attachments)
    {
        byte[] bytes = await SyntheticOverflowRows.CreateTableAsync(
            DatabaseFormat.AceAccdb,
            SyntheticTable,
            300,
            primaryKey: true,
            [new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }],
            extraValues: null,
            Ct);

        await using (var ms = new MemoryStream())
        {
            await ms.WriteAsync(bytes, Ct);
            await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
            {
                foreach ((int id, string fileName) in attachments)
                {
                    await writer.AddAttachmentAsync(SyntheticTable, "Files", new Dictionary<string, object?> { ["Id"] = id }, new AttachmentInput(fileName, [(byte)id]), Ct);
                }
            }

            bytes = ms.ToArray();
        }

        _ = await SyntheticOverflowRows.MoveRowAsync(bytes, SyntheticTable, OverflowRowLayout.CrossPage, Ct);
        Assert.True(await IsOverflowRowAsync(bytes, SyntheticTable, "Id", 1), "Id 1 should be the overflow row.");
        return bytes;
    }

    /// <summary>Returns the attachment file names of each row of <see cref="SyntheticTable"/> that has any, by <c>Id</c>.</summary>
    /// <param name="bytes">The database image.</param>
    private static async ValueTask<Dictionary<int, string[]>> ReadAttachmentsByIdAsync(byte[] bytes)
    {
        await using AccessReader reader = await OpenReaderAsync(bytes);
        using DataTable data = await reader.ReadDataTableAsync(SyntheticTable, cancellationToken: Ct);
        var result = new Dictionary<int, string[]>();
        foreach (DataRow row in data.Rows)
        {
            if (row["Files"] is byte[] cell && ComplexCellValue.ReadAttachments(cell) is { Count: > 0 } files)
            {
                result.Add((int)row["Id"], [.. files.Select(file => file.FileName).Order(StringComparer.Ordinal)]);
            }
        }

        return result;
    }

    private static TheoryData<string, WriteMode, bool> Combine(params string[] sources)
    {
        var data = new TheoryData<string, WriteMode, bool>();
        foreach (string source in sources)
        {
            foreach ((WriteMode mode, bool rollback) in new[] { (WriteMode.Direct, false), (WriteMode.AutoCommit, false), (WriteMode.ExplicitCommit, false), (WriteMode.ExplicitCommit, true) })
            {
                data.Add(source, mode, rollback);
            }
        }

        return data;
    }

    private static async ValueTask<(byte[] Bytes, string Table)> LoadAsync(string source)
    {
        if (source == "SyntheticJet3")
        {
            byte[] bytes = await SyntheticOverflowRows.CreateTableAsync(DatabaseFormat.Jet3Mdb, SyntheticTable, 300, primaryKey: true, Ct);
            _ = await SyntheticOverflowRows.MoveRowAsync(bytes, SyntheticTable, OverflowRowLayout.CrossPage, Ct);
            return (bytes, SyntheticTable);
        }

        string path = (string)typeof(TestDatabases).GetField(source)!.GetValue(null)!;
        return (await File.ReadAllBytesAsync(path, Ct), "Table1");
    }

    private static async ValueTask<MemoryStream> CopyAsync(string path)
    {
        var ms = new MemoryStream();
        byte[] bytes = await File.ReadAllBytesAsync(path, Ct);
        await ms.WriteAsync(bytes, Ct);
        ms.Position = 0;
        return ms;
    }

    private static async ValueTask<AccessWriter> OpenWriterAsync(MemoryStream ms, WriteMode mode)
    {
        ms.Position = 0;
        return await AccessWriter.OpenAsync(
            ms,
            WriteModes.WriterOptions(mode),
            leaveOpen: true,
            Ct);
    }

    private static async ValueTask<AccessReader> OpenReaderAsync(byte[] bytes)
        => await AccessReader.OpenAsync(new MemoryStream(bytes, writable: false), new AccessReaderOptions { UseLockFile = false }, leaveOpen: false, Ct);

    private static async Task RunAsync(AccessWriter writer, WriteMode mode, bool rollback, Func<Task> work)
    {
        if (mode is WriteMode.Direct or WriteMode.AutoCommit)
        {
            await work();
            return;
        }

        await using JetTransaction tx = await writer.BeginTransactionAsync(Ct);
        await work();
        if (!rollback)
        {
            await tx.CommitAsync(Ct);
        }
        else
        {
            await tx.RollbackAsync(Ct);
        }
    }

    private static async ValueTask<int> CountRowsAsync(byte[] bytes, string table)
    {
        await using AccessReader reader = await OpenReaderAsync(bytes);
        using DataTable data = await reader.ReadDataTableAsync(table, cancellationToken: Ct);
        return data.Rows.Count;
    }

    private static async ValueTask<int> CountIndexesAsync(byte[] bytes, string table)
    {
        await using AccessReader reader = await OpenReaderAsync(bytes);
        return (await reader.ListIndexesAsync(table, Ct)).Count;
    }

    private static async ValueTask<string> FirstTextColumnAsync(byte[] bytes, string table)
    {
        await using AccessReader reader = await OpenReaderAsync(bytes);
        return (await reader.GetColumnMetadataAsync(table, Ct)).First(c => c.ClrType == typeof(string) && !c.IsCalculated).Name;
    }

    /// <summary>Seeks every row's key through the primary key and expects one hit each; index seeks are Jet4/ACE only, so a Jet3 file is not checked.</summary>
    /// <param name="bytes">The database image.</param>
    /// <param name="table">The table name.</param>
    /// <param name="keyColumn">The primary key's column.</param>
    private static async Task AssertEverySeekHitsOnceAsync(byte[] bytes, string table, string keyColumn)
    {
        await using AccessReader reader = await OpenReaderAsync(bytes);
        if (reader.DatabaseFormat == DatabaseFormat.Jet3Mdb)
        {
            return;
        }

        IndexMetadata primaryKey = (await reader.ListIndexesAsync(table, Ct)).Single(i => i.Kind == IndexKind.PrimaryKey);
        using DataTable data = await reader.ReadDataTableAsync(table, cancellationToken: Ct);
        foreach (DataRow row in data.Rows)
        {
            int hits = 0;
            await foreach (object[] hit in reader.SeekRowsAsync(table, primaryKey.Name, [row[keyColumn]], Ct))
            {
                _ = hit;
                hits++;
            }

            Assert.True(hits == 1, $"Seeking {keyColumn} = {row[keyColumn]} found {hits} rows.");
        }
    }

    /// <summary>
    /// Returns the number of primary-key leaf entries, or null when the table has no
    /// primary key, after asserting that the entries name exactly the table's live
    /// rows, an overflow row by its header slot.
    /// </summary>
    /// <param name="bytes">The database image.</param>
    /// <param name="table">The table name.</param>
    private static async ValueTask<int?> CountPrimaryKeyEntriesAsync(byte[] bytes, string table)
    {
        await using var ms = new MemoryStream(bytes, writable: false);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(ms, cancellationToken: Ct);
        DatabaseFile db = harness.Database;
        CatalogEntry? entry = await harness.GetCatalogEntryAsync(table, Ct);
        Assert.NotNull(entry);
        TableDef? tableDef = await harness.ReadTableDefAsync(entry.TDefPage, Ct);
        byte[]? tdef = await db.TableDefs.ReadTDefBytesAsync(entry.TDefPage, Ct);
        Assert.NotNull(tableDef);
        Assert.NotNull(tdef);
        IndexMetadata? primaryKey = IndexCatalogReader.ReadMetadata(db.Format, tdef, tableDef.Columns).Find(i => i.Kind == IndexKind.PrimaryKey);
        if (primaryKey is null)
        {
            return null;
        }

        await IndexLeafChain.AssertCoversLiveRowsAsync(db, entry.TDefPage, primaryKey.FirstDp, Ct);
        return (await IndexLeafChain.ReadEntriesAsync(db, entry.TDefPage, primaryKey.FirstDp, Ct)).Count;
    }

    /// <summary>Reads the key of the synthetic table's only overflow row.</summary>
    /// <param name="bytes">The database image.</param>
    /// <returns>The moved row's Id.</returns>
    private static async ValueTask<int> ReadOverflowIdAsync(byte[] bytes)
    {
        await using var stream = new MemoryStream(bytes, writable: false);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: Ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync(SyntheticTable, Ct));
        TableDef tableDef = await harness.Database.TableDefs.ReadRequiredTableDefAsync(entry.TDefPage, SyntheticTable, Ct);
        int keyOrdinal = tableDef.FindColumnIndex("Id");
        int movedId = 0;
        await harness.Database.OwnedPages.ForEachLiveTableRowAsync(
            entry.TDefPage,
            async (row, token) =>
            {
                if (!row.Location.IsOverflow)
                {
                    return true;
                }

                object?[]? values = await PartialColumnReader.TryReadColumnValuesTypedAsync(harness.Database.Format, harness.Database.Pages, row.Location, tableDef, [keyOrdinal], token);
                movedId = Assert.IsType<int>(Assert.Single(Assert.IsType<object?[]>(values)));
                return false;
            },
            Ct);
        Assert.True(movedId > 0);
        return movedId;
    }

    /// <summary>Returns every row-offset slot of <paramref name="table"/> flagged as an overflow header.</summary>
    /// <param name="bytes">The database image.</param>
    /// <param name="table">The table name.</param>
    private static async ValueTask<List<(long Page, int Row)>> FindOverflowHeadersAsync(byte[] bytes, string table)
    {
        await using var ms = new MemoryStream(bytes, writable: false);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(ms, cancellationToken: Ct);
        DatabaseFile db = harness.Database;
        CatalogEntry? entry = await harness.GetCatalogEntryAsync(table, Ct);
        Assert.NotNull(entry);

        var headers = new List<(long Page, int Row)>();
        foreach (long pageNumber in await db.OwnedPages.GetOwnedDataPagesAsync(entry.TDefPage, Ct))
        {
            int numRows = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(checked((int)(pageNumber * db.Format.PageSize)) + db.Format.DataPage.NumRows));
            for (int r = 0; r < numRows; r++)
            {
                if ((SyntheticOverflowRows.ReadSlot(bytes, db.Format.PageSize, db.Format.DataPage.RowsStart, pageNumber, r) & OverflowFlags) == OverflowHeader)
                {
                    headers.Add((pageNumber, r));
                }
            }
        }

        return headers;
    }

    private static async ValueTask<int> ReadSlotFlagsAsync(byte[] bytes, long pageNumber, int rowIndex)
    {
        (int pageSize, int rowsStart) = await PageLayoutAsync(bytes);
        return SyntheticOverflowRows.ReadSlot(bytes, pageSize, rowsStart, pageNumber, rowIndex) & OverflowFlags;
    }

    private static async ValueTask<(int PageSize, int RowsStart)> PageLayoutAsync(byte[] bytes)
    {
        await using var ms = new MemoryStream(bytes, writable: false);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(ms, cancellationToken: Ct);
        return (harness.Database.Format.PageSize, harness.Database.Format.DataPage.RowsStart);
    }

    private static bool IsZero(byte[] bytes, long pageNumber, int pageSize, int start, int length)
        => !bytes.AsSpan(checked((int)(pageNumber * pageSize)) + start, length).ContainsAnyExcept((byte)0);

    private static async ValueTask<CatalogRow> FindCatalogRowAsync(byte[] bytes, string name)
    {
        await using var ms = new MemoryStream(bytes, writable: false);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(ms, cancellationToken: Ct);
        TableDef? msys = await harness.ReadTableDefAsync(2, Ct);
        Assert.NotNull(msys);
        List<CatalogRow> rows = await new CatalogRowReader(harness.Database.Format, harness.Database.TableDefs, harness.Database.OwnedPages).GetCatalogRowsAsync(msys, Ct);
        return Assert.Single(rows, r => r.ObjectType == 1 && r.Name == name);
    }

    /// <summary>Returns whether the row whose <paramref name="keyColumn"/> is <paramref name="key"/> is stored as an overflow row.</summary>
    /// <param name="bytes">The database image.</param>
    /// <param name="table">The table name.</param>
    /// <param name="keyColumn">The key column.</param>
    /// <param name="key">The key value.</param>
    private static async ValueTask<bool> IsOverflowRowAsync(byte[] bytes, string table, string keyColumn, int key)
    {
        await using var ms = new MemoryStream(bytes, writable: false);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(ms, cancellationToken: Ct);
        CatalogEntry? entry = await harness.GetCatalogEntryAsync(table, Ct);
        Assert.NotNull(entry);
        TableDef? tableDef = await harness.ReadTableDefAsync(entry.TDefPage, Ct);
        Assert.NotNull(tableDef);
        int keyOrdinal = tableDef.FindColumnIndex(keyColumn);

        bool isOverflow = false;
        await harness.Database.OwnedPages.ForEachLiveTableRowAsync(
            entry.TDefPage,
            async (row, token) =>
            {
                object?[]? values = await PartialColumnReader.TryReadColumnValuesTypedAsync(harness.Database.Format, harness.Database.Pages, row.Location, tableDef, [keyOrdinal], token);
                if (values is [int id] && id == key)
                {
                    isOverflow = row.Location.IsOverflow;
                    return false;
                }

                return true;
            },
            Ct);

        return isOverflow;
    }

    /// <summary>
    /// Builds two OrderDetails rows copied from an existing one: one naming an order
    /// that does not exist, and one naming an existing order with a product that
    /// order does not have yet.
    /// </summary>
    /// <param name="bytes">The NorthwindTraders.accdb image.</param>
    private static async ValueTask<(RowValues MissingOrder, RowValues ExistingOrder)> BuildOrderDetailRowsAsync(byte[] bytes)
    {
        await using AccessReader reader = await OpenReaderAsync(bytes);
        using DataTable details = await reader.ReadDataTableAsync("OrderDetails", cancellationToken: Ct);
        using DataTable products = await reader.ReadDataTableAsync("Products", cancellationToken: Ct);
        DataRow template = details.Rows[0];
        object orderId = template["OrderID"];
        var usedProducts = details.Rows.Cast<DataRow>().Where(r => Equals(r["OrderID"], orderId)).Select(r => r["ProductID"]).ToHashSet();
        string productKey = (await reader.ListIndexesAsync("Products", Ct)).Single(i => i.Kind == IndexKind.PrimaryKey).Columns[0].Name;
        object freeProduct = products.Rows.Cast<DataRow>().Select(r => r[productKey]).First(id => !usedProducts.Contains(id));

        RowValues Copy(object order, object product)
        {
            var values = RowValues.Create();
            foreach (DataColumn column in details.Columns)
            {
                if (column.ColumnName == "OrderDetailID" || template[column] is DBNull)
                {
                    continue;
                }

                object value = column.ColumnName switch
                {
                    "OrderID" => order,
                    "ProductID" => product,
                    _ => template[column],
                };
                values = values.Set(column.ColumnName, value);
            }

            return values;
        }

        return (Copy(999999, freeProduct), Copy(orderId, freeProduct));
    }
}
