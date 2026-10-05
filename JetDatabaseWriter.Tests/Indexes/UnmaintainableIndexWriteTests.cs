namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Writes to a table one of whose indexes names a key column the table does
/// not have, or lies past the end of a TDEF chain cut short, as in the wide
/// tables of JetDatabaseWriter builds before 4.0.0
/// (<see cref="LegacyDamageInjector"/>). The writer cannot maintain such an
/// index, so every insert, update, delete and schema rewrite must refuse with
/// <see cref="JetLimitationException"/> before it changes anything, in every
/// write mode: the file must be byte for byte what it was.
/// </summary>
public sealed class UnmaintainableIndexWriteTests
{
    private const string TableName = "Wide";
    private const int RowCount = 40;

    /// <summary>The damaged table a test writes to.</summary>
    [SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "Theory parameters of public xUnit test methods must be public.")]
    public enum DamagedTable
    {
        /// <summary>
        /// Jet4 or ACE: 200 columns and 30 non-unique indexes, with the stray
        /// <c>used_pages</c> byte, which gives real indexes 0 to 14 a phantom
        /// key column. No unique index, so no unique check runs first.
        /// </summary>
        NonUnique200 = 0,

        /// <summary>
        /// Jet4 or ACE: 100 columns, the unique index UX_00 and 29 non-unique
        /// indexes, with the stray byte. Real indexes 0 to 2 sit on the first
        /// TDEF page and keep their key column; 3 to 29 get the phantom.
        /// </summary>
        UniqueOnFirstPage100 = 1,

        /// <summary>
        /// Jet3: 50 columns, the unique index UX_00 and 19 non-unique indexes,
        /// with col_num 0x0300 hand-set in <c>col_map</c> slot 1 of real index 3.
        /// </summary>
        Jet3HandSet50 = 2,

        /// <summary>
        /// Jet4 or ACE: 101 columns, the unique index UX_00 and 29 non-unique
        /// indexes, with the stray byte. Real index 2's <c>used_pages</c>
        /// offset is exactly the page size, so its stray byte, 0x04, lands on
        /// the page-type byte of the first TDEF continuation page and the
        /// chain ends after the first page. Real indexes 0 and 1 stay
        /// readable; the descriptors of 2 to 29, every logical index entry and
        /// every index name lie past the end.
        /// </summary>
        ChainHeaderHit101 = 3,
    }

    /// <summary>A write to the damaged table.</summary>
    [SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "Theory parameters of public xUnit test methods must be public.")]
    public enum TableWrite
    {
        /// <summary><c>InsertRowsAsync</c> of one row.</summary>
        Insert = 0,

        /// <summary><c>UpdateRowsAsync</c> of one row.</summary>
        Update = 1,

        /// <summary><c>DeleteRowsAsync</c> of one row.</summary>
        Delete = 2,

        /// <summary><c>AddColumnAsync</c> of a new column.</summary>
        AddColumn = 3,

        /// <summary><c>RenameColumnAsync</c> of the last column, which no index uses.</summary>
        RenameColumn = 4,

        /// <summary><c>DropColumnAsync</c> of the last column, which no index uses.</summary>
        DropColumn = 5,
    }

    /// <summary>The schema rewrite a test runs.</summary>
    [SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "Theory parameters of public xUnit test methods must be public.")]
    public enum SchemaOperation
    {
        /// <summary><c>AddColumnAsync</c> of a new column.</summary>
        AddColumn = 0,

        /// <summary><c>RenameColumnAsync</c> of the last column, which no index uses.</summary>
        RenameColumn = 1,

        /// <summary><c>DropColumnAsync</c> of the last column, which no index uses.</summary>
        DropColumn = 2,
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every damaged layout of every format, in every write mode.</summary>
    /// <returns>The format, layout and write-mode triples.</returns>
    public static TheoryData<DatabaseFormat, DamagedTable, WriteMode> RowWriteCases()
    {
        var data = new TheoryData<DatabaseFormat, DamagedTable, WriteMode>();
        foreach (WriteMode mode in Enum.GetValues<WriteMode>())
        {
            data.Add(DatabaseFormat.Jet3Mdb, DamagedTable.Jet3HandSet50, mode);
            foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
            {
                data.Add(format, DamagedTable.NonUnique200, mode);
                data.Add(format, DamagedTable.UniqueOnFirstPage100, mode);
            }
        }

        return data;
    }

    /// <summary>Gets the layouts whose first real indexes keep their key column, in every write mode.</summary>
    /// <returns>The format, layout and write-mode triples.</returns>
    public static TheoryData<DatabaseFormat, DamagedTable, WriteMode> GoodIndexesFirstCases()
    {
        var data = new TheoryData<DatabaseFormat, DamagedTable, WriteMode>();
        foreach (WriteMode mode in Enum.GetValues<WriteMode>())
        {
            data.Add(DatabaseFormat.Jet3Mdb, DamagedTable.Jet3HandSet50, mode);
            data.Add(DatabaseFormat.Jet4Mdb, DamagedTable.UniqueOnFirstPage100, mode);
            data.Add(DatabaseFormat.AceAccdb, DamagedTable.UniqueOnFirstPage100, mode);
        }

        return data;
    }

    /// <summary>Gets every schema rewrite on a damaged table of every format, in every write mode.</summary>
    /// <returns>The format, layout, operation and write-mode tuples.</returns>
    public static TheoryData<DatabaseFormat, DamagedTable, SchemaOperation, WriteMode> SchemaRewriteCases()
    {
        var data = new TheoryData<DatabaseFormat, DamagedTable, SchemaOperation, WriteMode>();
        foreach (WriteMode mode in Enum.GetValues<WriteMode>())
        {
            foreach (SchemaOperation operation in Enum.GetValues<SchemaOperation>())
            {
                data.Add(DatabaseFormat.Jet3Mdb, DamagedTable.Jet3HandSet50, operation, mode);
                data.Add(DatabaseFormat.Jet4Mdb, DamagedTable.NonUnique200, operation, mode);
                data.Add(DatabaseFormat.AceAccdb, DamagedTable.NonUnique200, operation, mode);
            }
        }

        return data;
    }

    /// <summary>Gets every write to the table whose TDEF chain a stray byte cut short, on Jet4 and ACE, in every write mode.</summary>
    /// <returns>The format, write and write-mode triples.</returns>
    public static TheoryData<DatabaseFormat, TableWrite, WriteMode> ChainHeaderHitCases()
    {
        var data = new TheoryData<DatabaseFormat, TableWrite, WriteMode>();
        foreach (WriteMode mode in Enum.GetValues<WriteMode>())
        {
            foreach (TableWrite write in Enum.GetValues<TableWrite>())
            {
                data.Add(DatabaseFormat.Jet4Mdb, write, mode);
                data.Add(DatabaseFormat.AceAccdb, write, mode);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(RowWriteCases))]
    public async Task Update_IndexNamesMissingColumn_ThrowsBeforeChangingAnyRow(DatabaseFormat format, DamagedTable layout, WriteMode mode)
    {
        await using MemoryStream stream = await CreateDamagedTableAsync(format, layout);
        byte[] before = stream.ToArray();

        JetLimitationException ex = await WriteExpectingRefusalAsync(
            stream,
            mode,
            async writer => await writer.UpdateRowsAsync(TableName, "C000", 7, new Dictionary<string, object?> { ["C001"] = 999 }, Ct));

        Assert.Equal(before, stream.ToArray());
        AssertNamesFirstPhantomIndex(ex, layout);
    }

    [Theory]
    [MemberData(nameof(RowWriteCases))]
    public async Task Delete_IndexNamesMissingColumn_ThrowsBeforeDeletingAnyRow(DatabaseFormat format, DamagedTable layout, WriteMode mode)
    {
        await using MemoryStream stream = await CreateDamagedTableAsync(format, layout);
        byte[] before = stream.ToArray();

        JetLimitationException ex = await WriteExpectingRefusalAsync(
            stream,
            mode,
            async writer => await writer.DeleteRowsAsync(TableName, "C000", 13, Ct));

        Assert.Equal(before, stream.ToArray());
        AssertNamesFirstPhantomIndex(ex, layout);
    }

    /// <summary>
    /// The incremental index path used to splice the new row into the good
    /// indexes before it reached the first phantom one, and rolling the row
    /// back left those entries dangling. No index may hold an entry for the
    /// refused row.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="layout">The damaged table.</param>
    /// <param name="mode">How the writer runs the insert.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Theory]
    [MemberData(nameof(GoodIndexesFirstCases))]
    public async Task Insert_IndexNamesMissingColumnAfterGoodIndexes_LeavesNoIndexEntryBehind(DatabaseFormat format, DamagedTable layout, WriteMode mode)
    {
        await using MemoryStream stream = await CreateDamagedTableAsync(format, layout);
        byte[] before = stream.ToArray();

        JetLimitationException ex = await WriteExpectingRefusalAsync(
            stream,
            mode,
            async writer => await writer.InsertRowsAsync(TableName, [WideRow(layout, 100)], Ct));

        // Real indexes 0 to 2 (UX_00, IX_01 and IX_02) come before the first
        // phantom one: each must hold exactly one entry per live row.
        stream.Position = 0;
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: Ct))
        {
            CatalogEntry? entry = await harness.GetCatalogEntryAsync(TableName, Ct);
            Assert.NotNull(entry);
            List<long> roots = await IndexLeafChain.ReadRealIndexRootsAsync(harness.Database, entry.TDefPage, Ct);
            Assert.Equal(RowCount, (await harness.Database.GetLiveRowLocationsAsync(entry.TDefPage, Ct)).Count);
            for (int realIndex = 0; realIndex < 3; realIndex++)
            {
                await IndexLeafChain.AssertCoversLiveRowsAsync(harness.Database, entry.TDefPage, roots[realIndex], Ct);
            }
        }

        Assert.Equal(before, stream.ToArray());
        AssertNamesFirstPhantomIndex(ex, layout);
    }

    /// <summary>
    /// A schema rewrite used to forward only the indexes whose key columns it
    /// could name, so AddColumn and RenameColumn on a damaged table succeeded
    /// and silently dropped every damaged index, unique ones included.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="layout">The damaged table.</param>
    /// <param name="operation">The schema rewrite.</param>
    /// <param name="mode">How the writer runs the rewrite.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Theory]
    [MemberData(nameof(SchemaRewriteCases))]
    public async Task SchemaRewrite_IndexNamesMissingColumn_ThrowsAndKeepsEveryIndex(DatabaseFormat format, DamagedTable layout, SchemaOperation operation, WriteMode mode)
    {
        await using MemoryStream stream = await CreateDamagedTableAsync(format, layout);
        byte[] before = stream.ToArray();
        string lastColumn = ColumnName(ColumnCountOf(layout) - 1);

        JetLimitationException ex = await WriteExpectingRefusalAsync(
            stream,
            mode,
            async writer =>
            {
                switch (operation)
                {
                    case SchemaOperation.AddColumn:
                        await writer.AddColumnAsync(TableName, new ColumnDefinition("Extra", typeof(int)), Ct);
                        break;
                    case SchemaOperation.RenameColumn:
                        await writer.RenameColumnAsync(TableName, lastColumn, "Renamed", Ct);
                        break;
                    case SchemaOperation.DropColumn:
                        await writer.DropColumnAsync(TableName, lastColumn, Ct);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(operation));
                }
            });

        Assert.Equal(before, stream.ToArray());
        AssertNamesFirstPhantomIndex(ex, layout);

        Assert.Equal(IndexCountOf(layout), await CountIndexesAsync(stream));
    }

    /// <summary>
    /// A stray byte on the page-type byte of a TDEF continuation page ends the
    /// chain before that page, so the descriptors, entries and names of most
    /// indexes lie past the end of what can be read. The preflight checked
    /// only the indexes it could read, so every write succeeded and left the
    /// others stale, and a schema rewrite rebuilt the table without any index.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="write">The write.</param>
    /// <param name="mode">How the writer runs the write.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Theory]
    [MemberData(nameof(ChainHeaderHitCases))]
    public async Task Write_IndexSectionPastEndOfTableDefinition_ThrowsBeforeChangingAnything(DatabaseFormat format, TableWrite write, WriteMode mode)
    {
        const DamagedTable layout = DamagedTable.ChainHeaderHit101;
        await using MemoryStream stream = await CreateDamagedTableAsync(format, layout);
        byte[] before = stream.ToArray();
        int indexesBefore = await CountIndexesAsync(stream);
        string lastColumn = ColumnName(ColumnCountOf(layout) - 1);

        JetLimitationException ex = await WriteExpectingRefusalAsync(
            stream,
            mode,
            async writer =>
            {
                switch (write)
                {
                    case TableWrite.Insert:
                        _ = await writer.InsertRowsAsync(TableName, [WideRow(layout, 100)], Ct);
                        break;
                    case TableWrite.Update:
                        _ = await writer.UpdateRowsAsync(TableName, "C000", 7, new Dictionary<string, object?> { ["C001"] = 999 }, Ct);
                        break;
                    case TableWrite.Delete:
                        _ = await writer.DeleteRowsAsync(TableName, "C000", 13, Ct);
                        break;
                    case TableWrite.AddColumn:
                        await writer.AddColumnAsync(TableName, new ColumnDefinition("Extra", typeof(int)), Ct);
                        break;
                    case TableWrite.RenameColumn:
                        await writer.RenameColumnAsync(TableName, lastColumn, "Renamed", Ct);
                        break;
                    case TableWrite.DropColumn:
                        await writer.DropColumnAsync(TableName, lastColumn, Ct);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(write));
                }
            });

        Assert.Equal(before, stream.ToArray());
        Assert.Equal(indexesBefore, await CountIndexesAsync(stream));
        Assert.Contains($"'{TableName}'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("ends before the descriptor of real index 2", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A delete that cascades into the damaged table deletes that table's
    /// dependent rows before its index rebuild refuses (docs/todo.md). The rows
    /// the delete has not deleted by then, its own row and the rows of a
    /// cascade applied after the damaged table's, must keep their attachments:
    /// each set of rows loses its attachment rows just before it is deleted.
    /// </summary>
    /// <returns>A task that completes when the test has run.</returns>
    [Fact]
    public async Task Delete_CascadeIntoDamagedTable_KeepsTheAttachmentsOfRowsNotYetDeleted()
    {
        // Owner (Id primary key, Files) <- Kid (Id primary key, OwnerId, Files)
        // <- Wide.C001, both cascading deletes. Deleting Owner 2 cascades to
        // Kid 10001 and on to the 8 Wide rows whose C001 is 10001. The deepest
        // cascade, Wide's, is applied first, and its index rebuild throws.
        const int owners = 5;
        await using MemoryStream stream = await CreateDamagedTableAsync(
            DatabaseFormat.AceAccdb,
            DamagedTable.NonUnique200,
            async writer =>
            {
                await writer.CreateTableAsync(
                    "Owner",
                    [
                        new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                        new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                    ],
                    Ct);
                await writer.CreateTableAsync(
                    "Kid",
                    [
                        new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                        new ColumnDefinition("OwnerId", typeof(int)),
                        new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                    ],
                    Ct);
                _ = await writer.InsertRowsAsync("Owner", [.. Enumerable.Range(1, owners).Select(id => new object[] { id, DBNull.Value })], Ct);
                _ = await writer.InsertRowsAsync("Kid", [.. Enumerable.Range(1, owners).Select(id => new object[] { KidOf(id), id, DBNull.Value })], Ct);
                for (int id = 1; id <= owners; id++)
                {
                    await writer.AddAttachmentAsync("Owner", "Files", new Dictionary<string, object?> { ["Id"] = id }, new AttachmentInput(OwnerFile(id), [(byte)id]), Ct);
                    await writer.AddAttachmentAsync("Kid", "Files", new Dictionary<string, object?> { ["Id"] = KidOf(id) }, new AttachmentInput(KidFile(id), [(byte)id]), Ct);
                }

                await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_Kid_Owner", "Owner", "Id", "Kid", "OwnerId") { CascadeDeletes = true }, Ct);
                await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_Wide_Kid", "Kid", "Id", TableName, "C001") { CascadeDeletes = true }, Ct);
            });

        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            JetLimitationException ex = await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.DeleteRowsAsync("Owner", "Id", 2, Ct));
            Assert.Contains($"'{TableName}'", ex.Message, StringComparison.Ordinal);
        }

        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            IReadOnlyList<TableStat> stats = await reader.GetTableStatsAsync(Ct);
            Assert.Equal(owners, stats.Single(stat => stat.Name == "Owner").RowCount);
            Assert.Equal(owners, stats.Single(stat => stat.Name == "Kid").RowCount);
            IReadOnlyList<AttachmentRecord> ownerFiles = await reader.GetAttachmentsAsync("Owner", "Files", Ct);
            Assert.Equal(Enumerable.Range(1, owners).Select(OwnerFile), ownerFiles.Select(file => file.FileName).Order(StringComparer.Ordinal));
            IReadOnlyList<AttachmentRecord> kidFiles = await reader.GetAttachmentsAsync("Kid", "Files", Ct);
            Assert.Equal(Enumerable.Range(1, owners).Select(KidFile), kidFiles.Select(file => file.FileName).Order(StringComparer.Ordinal));
        }

        Assert.Equal((owners, owners), await FlatTableRowCounts.ReadAsync(stream, "Owner", "Files", Ct));
        Assert.Equal((owners, owners), await FlatTableRowCounts.ReadAsync(stream, "Kid", "Files", Ct));

        // Kid keys 10000 to 10004 are the C001 values of the Wide rows.
        static int KidOf(int ownerId) => 9_999 + ownerId;

        static string OwnerFile(int ownerId) => string.Create(CultureInfo.InvariantCulture, $"owner-{ownerId}.txt");

        static string KidFile(int ownerId) => string.Create(CultureInfo.InvariantCulture, $"kid-{KidOf(ownerId)}.txt");
    }

    private static async Task<int> CountIndexesAsync(MemoryStream stream)
    {
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct);
        return (await reader.ListIndexesAsync(TableName, Ct)).Count;
    }

    private static int ColumnCountOf(DamagedTable layout) => layout switch
    {
        DamagedTable.NonUnique200 => 200,
        DamagedTable.UniqueOnFirstPage100 => 100,
        DamagedTable.Jet3HandSet50 => 50,
        DamagedTable.ChainHeaderHit101 => 101,
        _ => throw new ArgumentOutOfRangeException(nameof(layout)),
    };

    private static int IndexCountOf(DamagedTable layout) => layout == DamagedTable.Jet3HandSet50 ? 20 : 30;

    private static string ColumnName(int column) => string.Create(CultureInfo.InvariantCulture, $"C{column:D3}");

    /// <summary>
    /// Returns the name of real index <paramref name="realIndex"/>: index
    /// <c>n</c> is on column <c>n</c>, and a layout with a unique index names
    /// its first one UX_00.
    /// </summary>
    /// <param name="layout">The layout.</param>
    /// <param name="realIndex">The real (and logical) index number.</param>
    /// <returns>The index name.</returns>
    private static string IndexName(DamagedTable layout, int realIndex) =>
        realIndex == 0 && layout != DamagedTable.NonUnique200
            ? "UX_00"
            : string.Create(CultureInfo.InvariantCulture, $"IX_{realIndex:D2}");

    private static int[] ExpectedPhantomIndexes(DamagedTable layout) => layout switch
    {
        DamagedTable.NonUnique200 => [.. Enumerable.Range(0, 15)],
        DamagedTable.UniqueOnFirstPage100 => [.. Enumerable.Range(3, 27)],
        DamagedTable.Jet3HandSet50 => [3],

        // Real indexes 0 and 1 keep their key columns; the rest cannot be read.
        DamagedTable.ChainHeaderHit101 => [],
        _ => throw new ArgumentOutOfRangeException(nameof(layout)),
    };

    private static void AssertNamesFirstPhantomIndex(JetLimitationException ex, DamagedTable layout)
    {
        Assert.Contains($"'{TableName}'", ex.Message, StringComparison.Ordinal);
        Assert.Contains($"'{IndexName(layout, ExpectedPhantomIndexes(layout)[0])}'", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Builds a row whose <c>C000</c> is <paramref name="key"/>. Even columns
    /// derive a per-row value from the key; odd columns repeat every five keys
    /// so the non-unique indexes hold duplicates.
    /// </summary>
    /// <param name="layout">The layout, which sets the column count.</param>
    /// <param name="key">The unique key stored in <c>C000</c>.</param>
    /// <returns>The row, in column order.</returns>
    private static object[] WideRow(DamagedTable layout, int key)
    {
        object[] row = new object[ColumnCountOf(layout)];
        row[0] = key;
        for (int c = 1; c < row.Length; c++)
        {
            row[c] = (c % 2 == 0 ? key : key % 5) + (c * 10_000);
        }

        return row;
    }

    /// <summary>
    /// Creates the table of <paramref name="layout"/> with <see cref="RowCount"/>
    /// rows, then damages its TDEF as builds before 4.0.0 did and checks which
    /// indexes now name a column the table does not have.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="layout">The layout.</param>
    /// <param name="addRelatedTables">Run with the writer once the table's rows are written and before the damage, to add other tables and relationships to it; <see langword="null"/> for none.</param>
    /// <returns>The database, positioned at its start.</returns>
    private static async Task<MemoryStream> CreateDamagedTableAsync(DatabaseFormat format, DamagedTable layout, Func<AccessWriter, Task>? addRelatedTables = null)
    {
        int columnCount = ColumnCountOf(layout);
        var columns = new List<ColumnDefinition>(columnCount);
        for (int c = 0; c < columnCount; c++)
        {
            columns.Add(new ColumnDefinition(ColumnName(c), typeof(int)));
        }

        var indexes = new List<IndexDefinition>(IndexCountOf(layout));
        for (int i = 0; i < IndexCountOf(layout); i++)
        {
            string name = IndexName(layout, i);
            indexes.Add(new IndexDefinition(name, ColumnName(i)) { IsUnique = name == "UX_00" });
        }

        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync(TableName, columns, indexes, Ct);
            _ = await writer.InsertRowsAsync(TableName, [.. Enumerable.Range(1, RowCount).Select(key => WideRow(layout, key))], Ct);
            if (addRelatedTables != null)
            {
                await addRelatedTables(writer);
            }
        }

        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct))
        {
            ResolvedTable table = await harness.Services.Catalog.ResolveRequiredTableAsync(TableName, Ct);
            IReadOnlyList<int> phantoms;
            if (layout == DamagedTable.Jet3HandSet50)
            {
                await LegacyDamageInjector.SetPhantomKeyColumnAsync(harness, table.Entry.TDefPage, realIndexNumber: 3, Ct);
                phantoms = await LegacyDamageInjector.FindPhantomIndexesAsync(harness.Database, table.Entry.TDefPage, Ct);
            }
            else
            {
                LogicalTDefChain intact = await harness.Database.TableDefs.ReadTDefChainAsync(table.Entry.TDefPage, Ct);
                phantoms = await LegacyDamageInjector.InjectStrayUsedPagesByteAsync(harness, table.Entry.TDefPage, Ct);
                if (layout == DamagedTable.ChainHeaderHit101)
                {
                    // Real index 2's stray byte is now the first continuation
                    // page's type, so the chain ends after its first page.
                    byte[] continuation = await harness.Database.Pages.ReadPageCopyAsync(intact.PageNumbers[1], Ct);
                    Assert.Equal((byte)0x04, continuation[0]);
                    Assert.Single((await harness.Database.TableDefs.ReadTDefChainAsync(table.Entry.TDefPage, Ct)).PageNumbers);
                }
            }

            Assert.Equal(ExpectedPhantomIndexes(layout), phantoms);
        }

        stream.Position = 0;
        return stream;
    }

    /// <summary>
    /// Runs <paramref name="write"/> in <paramref name="mode"/> and returns the
    /// <see cref="JetLimitationException"/> it must throw. In an explicit
    /// transaction the refusal leaves the transaction usable, and it is then
    /// committed, so anything the write changed before it threw reaches the file.
    /// </summary>
    /// <param name="stream">The database.</param>
    /// <param name="mode">How the writer runs the write.</param>
    /// <param name="write">The write.</param>
    /// <returns>The exception the write threw.</returns>
    private static async Task<JetLimitationException> WriteExpectingRefusalAsync(MemoryStream stream, WriteMode mode, Func<AccessWriter, Task> write)
    {
        stream.Position = 0;
        var options = new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = mode == WriteMode.AutoCommit };
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, options, leaveOpen: true, Ct);
        if (mode != WriteMode.ExplicitCommit)
        {
            return await Assert.ThrowsAsync<JetLimitationException>(() => write(writer));
        }

        await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
        JetLimitationException ex = await Assert.ThrowsAsync<JetLimitationException>(() => write(writer));
        await transaction.CommitAsync(Ct);
        return ex;
    }
}
