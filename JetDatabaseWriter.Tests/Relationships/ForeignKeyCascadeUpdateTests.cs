namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// A key update plans the cascades of every relationship whose referenced key
/// it moves, and checks the table's own unique indexes, before it writes any
/// row: a refusal from any relationship, an unreadable MEMO in any dependent
/// row or a duplicate key changes nothing, and a relationship that does not
/// cascade refuses from its child index seek without reading the dependent
/// rows. A child row that several relationships reach is rewritten once with
/// every key change, and a row the update itself rewrites takes its own
/// cascade through a self-relationship in that one rewrite.
/// </summary>
/// <param name="db">Caches the fixture files.</param>
public sealed class ForeignKeyCascadeUpdateTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private const string UnreadableMemoMarker = "CASCADEUNREADABLEMEMOMARKER";

    private static readonly AccessReaderOptions ReaderOptions = new() { UseLockFile = false };

    private static readonly DatabaseFormat[] Formats = [DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every format in every write mode.</summary>
    /// <returns>The format and write-mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes() => ForeignKeyTestDatabase.FormatsAndModes();

    /// <summary>
    /// Gets every format in every write mode, with the child table read through
    /// its foreign-key indexes and through a scan. A MEMO column makes Jet4 and
    /// ACCDB scan the table, because the single-row reader behind the seek
    /// cannot decode one; Jet3 always scans.
    /// </summary>
    /// <returns>The format, write-mode and scan triples.</returns>
    public static TheoryData<DatabaseFormat, WriteMode, bool> FormatsModesAndScans()
    {
        var data = new TheoryData<DatabaseFormat, WriteMode, bool>();
        foreach (DatabaseFormat format in Formats)
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                data.Add(format, mode, false);
                data.Add(format, mode, true);
            }
        }

        return data;
    }

    /// <summary>
    /// Gets the formats whose relationships are found through the child's
    /// foreign-key index (Jet4 and ACCDB; Jet3 always scans), in every write
    /// mode.
    /// </summary>
    /// <returns>The format and write-mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> SeekFormatsAndModes()
    {
        var data = new TheoryData<DatabaseFormat, WriteMode>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                data.Add(format, mode);
            }
        }

        return data;
    }

    /// <summary>
    /// Gets every format in every write mode, with the value a self-referencing
    /// row's update assigns to its own foreign key besides moving its key.
    /// </summary>
    /// <returns>The format, write-mode and assigned-key triples.</returns>
    public static TheoryData<DatabaseFormat, WriteMode, int> FormatsModesAndAssignedParents()
    {
        var data = new TheoryData<DatabaseFormat, WriteMode, int>();
        foreach (DatabaseFormat format in Formats)
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                data.Add(format, mode, 1);
                data.Add(format, mode, 3);
            }
        }

        return data;
    }

    /// <summary>Primary-key changes reach grandchildren before any row is rewritten.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_RecursiveCascade_RewritesEveryGeneration(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            foreach (string name in new[] { "A", "B", "C" })
            {
                await writer.CreateTableAsync(name, [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
                Assert.Equal(1, await writer.InsertRowsAsync(name, [[1]], Ct));
            }

            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_B_A", "A", "Id", "B", "Id") { CascadeUpdates = true }, Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_B", "B", "Id", "C", "Id") { CascadeUpdates = true }, Ct);
        }

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                Assert.Equal(1, await writer.UpdateRowsAsync("A", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5 }, Ct)));
        }

        foreach (string name in new[] { "A", "B", "C" })
        {
            Assert.Equal(["5"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, name));
        }

        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "A", "B", "C");
    }
    /// <summary>Primary-key changes reach grandchildren before any row is rewritten.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_RecursiveCascadeWithGrandchildRule_ChangesNothing(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            foreach (string name in new[] { "A", "B", "C" })
            {
                await writer.CreateTableAsync(name, [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true, ValidationRuleExpression = name == "C" ? "< 5" : null }], Ct);
                Assert.Equal(1, await writer.InsertRowsAsync(name, [[1]], Ct));
            }

            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_B_A", "A", "Id", "B", "Id") { CascadeUpdates = true }, Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_B", "B", "Id", "C", "Id") { CascadeUpdates = true }, Ct);
        }

        byte[] before = ms.ToArray();

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.UpdateRowsAsync("A", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5 }, Ct)));
        }

        Assert.Equal(before, ms.ToArray());
        foreach (string name in new[] { "A", "B", "C" })
        {
            Assert.Equal(["1"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, name));
        }

        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "A", "B", "C");
    }
    /// <summary>A non-null cascading key refreshes the child's stored calculated result.</summary>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [InlineData(WriteMode.Direct)]
    [InlineData(WriteMode.AutoCommit)]
    [InlineData(WriteMode.ExplicitCommit)]
    public async Task Update_CascadedNonNullValue_RefreshesChildCalculatedColumn(WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, DatabaseFormat.AceAccdb);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync("P", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            await writer.CreateTableAsync(
                "C",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("ParentId", typeof(int)),
                    new ColumnDefinition("TwiceParentId", typeof(int)) { IsCalculated = true, CalculationExpression = "[ParentId] * 2" },
                ],
                Ct);
            Assert.Equal(1, await writer.InsertRowsAsync("P", [[1]], Ct));
            Assert.Equal(1, await writer.InsertRowsAsync("C", [[1, 1, null]], Ct));
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", "Id", "C", "ParentId") { CascadeUpdates = true }, Ct);
        }

        Assert.Equal(["1|1|2"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5 }, Ct)));
        }

        Assert.Equal(["5"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        Assert.Equal(["1|5|10"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "P", "C");
    }

    /// <summary>A non-null cascaded value obeys the child's persisted column rule before writing.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_CascadedNonNullValueViolatesChildRule_ChangesNothing(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync("P", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            await writer.CreateTableAsync("C", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int)) { ValidationRuleExpression = "< 5" }], Ct);
            Assert.Equal(1, await writer.InsertRowsAsync("P", [[1]], Ct));
            Assert.Equal(1, await writer.InsertRowsAsync("C", [[1, 1]], Ct));
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", "Id", "C", "ParentId") { CascadeUpdates = true }, Ct);
        }

        byte[] before = ms.ToArray();

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () =>
                    await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5 }, Ct)));
        }

        Assert.Equal(before, ms.ToArray());
        Assert.Equal(["1"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        Assert.Equal(["1|1"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "P", "C");
    }

    /// <summary>An orphan holding a duplicate child key causes a refusal before any row is changed.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The write mode.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_CascadedValueDuplicatesChildUniqueIndex_ChangesNothing(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync("P", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            await writer.CreateTableAsync("C", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int))], [new IndexDefinition("UX_ParentId", "ParentId") { IsUnique = true }], Ct);
            Assert.Equal(1, await writer.InsertRowsAsync("P", [[1]], Ct));
            Assert.Equal(2, await writer.InsertRowsAsync("C", [[1, 1], [2, 5]], Ct));
        }

        await PlantCascadeRelationshipAsync(ms);
        byte[] before = ms.ToArray();

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                _ = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5 }, Ct)));
        }

        Assert.Equal(before, ms.ToArray());
        Assert.Equal(["1"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        Assert.Equal(["1|1", "2|5"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "P", "C");
    }

    /// <summary>
    /// A key update that a relationship without cascading updates refuses
    /// rewrites no row, even though a relationship read before it cascades
    /// updates: <c>C</c> keeps its row under <c>P.Id</c> 1, and every row
    /// count stays as it was.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_CascadingRelationshipBeforeRefusingOne_RewritesNoRow(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateCascadingAsync(
            db,
            format,
            secondChild: new RelationshipDefinition("FK_D_P", "P", "Id", "D", "ParentId"));

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                JetConstraintException ex = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 3 }, Ct));
                Assert.Equal(JetErrorCode.ForeignKeyRestrictUpdate, ex.ErrorCode);
                Assert.Equal("P", ex.ErrorInfo.TableName);
                Assert.Equal("FK_D_P", ex.ErrorInfo.RelationshipName);
                Assert.Equal(
                    "UPDATE on 'P' violates foreign-key constraint 'FK_D_P': 1 dependent row(s) in 'D' reference the old key(s) and cascade-update is not enabled.",
                    ex.Message);
            });
        }

        Assert.Equal(["1|one"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        Assert.Equal(["1|1|a"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
        Assert.Equal(["1|1"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "D"));
        long[] rowCounts = await ReadRowCountsAsync(ms, "P", "C", "D");
        Assert.Equal([1L, 1L, 1L], rowCounts);
    }

    /// <summary>
    /// A key update that two relationships cascade into two child tables
    /// rewrites the dependent rows of both, each in its own table, and
    /// rebuilds both tables' indexes.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_CascadingIntoTwoChildTables_RewritesBoth(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateCascadingAsync(
            db,
            format,
            secondChild: new RelationshipDefinition("FK_D_P", "P", "Id", "D", "ParentId") { CascadeUpdates = true });

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 3 }, Ct)));
        }

        Assert.Equal(["3|one"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        Assert.Equal(["1|3|a"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
        Assert.Equal(["1|3"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "D"));
        long[] rowCounts = await ReadRowCountsAsync(ms, "P", "C", "D");
        Assert.Equal([1L, 1L, 1L], rowCounts);
        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "P", "C", "D");
    }

    /// <summary>
    /// A relationship without cascading updates refuses a key update from the
    /// row locations its child index seek finds, without reading the
    /// dependent rows: of <c>M</c>'s data pages, the update reads only the one
    /// that holds the dependent row, which the seek checks, although a MEMO
    /// column stops the single-row reader from decoding <c>M</c>'s rows.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(SeekFormatsAndModes))]
    public async Task Update_RefusedBySeekableRelationshipWithoutCascade_ReadsNoOtherChildPage(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateAsync(db, format, [[1, "one"], [2, "two"]], []);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync(
                "M",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("ParentId", typeof(int)),
                    new ColumnDefinition("Notes", typeof(string)),
                ],
                Ct);
            object[][] rows = [.. Enumerable.Range(1, 200).Select(id => new object[] { id, id == 1 ? 1 : 2, new string('n', 100) })];
            Assert.Equal(200, await writer.InsertRowsAsync("M", rows, Ct));
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_M_P", "P", "Id", "M", "ParentId"), Ct);
        }

        HashSet<long> childPages = await ReadDataPagesAsync(ms, "M");
        Assert.True(childPages.Count >= 4, $"M fills {childPages.Count} data pages; the test needs at least 4.");

        var counting = new CountingStream(ms);
        HashSet<long> pagesRead = [];
        ms.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(counting, new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false, UseTransactionalWrites = mode == WriteMode.AutoCommit, PageCacheSize = 0 }, leaveOpen: true, Ct))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                // The writer's first load of the relationships can read every
                // page of the file (the owned-page index); an update that
                // moves no key loads them first.
                Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 2), new RowValues { ["Name"] = "deux" }, Ct));
                counting.Reset();
                JetConstraintException ex = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 3 }, Ct));
                pagesRead = counting.PagesRead(4096);
                Assert.Equal(
                    "UPDATE on 'P' violates foreign-key constraint 'FK_M_P': 1 dependent row(s) in 'M' reference the old key(s) and cascade-update is not enabled.",
                    ex.Message);
            });
        }

        Assert.Single(pagesRead.Intersect(childPages));
        Assert.Equal(["1|one", "2|deux"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
    }

    /// <summary>
    /// A relationship without cascading updates refuses a key update from the
    /// row locations its child index seek finds, without reading the
    /// dependent rows: of <c>M</c>'s data pages, the update reads only the one
    /// that holds the dependent row, which the seek checks, although a MEMO
    /// column stops the single-row reader from decoding <c>M</c>'s rows.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(SeekFormatsAndModes))]
    public async Task Delete_RefusedBySeekableRelationshipWithoutCascade_ReadsNoOtherChildPage(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateAsync(db, format, [[1, "one"], [2, "two"]], []);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync(
                "M",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("ParentId", typeof(int)),
                    new ColumnDefinition("Notes", typeof(string)),
                ],
                Ct);
            object[][] rows = [.. Enumerable.Range(1, 200).Select(id => new object[] { id, id == 1 ? 1 : 2, new string('n', 100) })];
            Assert.Equal(200, await writer.InsertRowsAsync("M", rows, Ct));
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_M_P", "P", "Id", "M", "ParentId"), Ct);
        }

        HashSet<long> childPages = await ReadDataPagesAsync(ms, "M");
        Assert.True(childPages.Count >= 4, $"M fills {childPages.Count} data pages; the test needs at least 4.");

        var counting = new CountingStream(ms);
        HashSet<long> pagesRead = [];
        ms.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(counting, new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false, UseTransactionalWrites = mode == WriteMode.AutoCommit, PageCacheSize = 0 }, leaveOpen: true, Ct))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                // The writer's first load of the relationships can read every
                // page of the file (the owned-page index); an update that
                // moves no key loads them first.
                Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 2), new RowValues { ["Name"] = "deux" }, Ct));
                counting.Reset();
                JetConstraintException ex = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.DeleteRowsAsync("P", RowCriteria.Where("Id", 1), Ct));
                pagesRead = counting.PagesRead(4096);
                Assert.Equal(
                    "DELETE on 'P' violates foreign-key constraint 'FK_M_P': 1 dependent row(s) in 'M' reference the deleted key(s) and cascade-delete is not enabled.",
                    ex.Message);
            });
        }

        Assert.Single(pagesRead.Intersect(childPages));
        Assert.Equal(["1|one", "2|deux"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
    }

    /// <summary>
    /// A key update whose later cascade reaches a row with an unreadable MEMO
    /// rewrites no row: the earlier cascade into <c>C</c> is planned, but the
    /// refusal comes before it is applied.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_UnreadableMemoInLaterCascade_RewritesNoRow(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateCascadingAsync(db, format, secondChild: null);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync(
                "M",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("ParentId", typeof(int)),
                    new ColumnDefinition("Body", typeof(string)),
                ],
                Ct);
            await writer.InsertRowAsync("M", [1, 1, UnreadableMemoMarker + new string('q', 1500)], Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_M_P", "P", "Id", "M", "ParentId") { CascadeUpdates = true }, Ct);
        }

        BreakLongValuePage(ms, format);
        List<string> memoRows = await ForeignKeyTestDatabase.ReadRowsAsync(ms, "M");

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                InvalidDataException ex = await Assert.ThrowsAsync<InvalidDataException>(async () =>
                    await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 3 }, Ct));
                Assert.Contains("column 'Body' of table 'M'", ex.Message, StringComparison.Ordinal);
            });
        }

        Assert.Equal(["1|one"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        Assert.Equal(["1|1|a"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
        Assert.Equal(memoRows, await ForeignKeyTestDatabase.ReadRowsAsync(ms, "M"));
        long[] rowCounts = await ReadRowCountsAsync(ms, "P", "C", "M");
        Assert.Equal([1L, 1L, 1L], rowCounts);
    }

    /// <summary>
    /// A key update to a value another parent row holds is refused by the
    /// parent's unique check before the cascade rewrites <c>C</c>.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_ParentKeyToDuplicate_RewritesNoChildRow(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateAsync(db, format, [[1, "one"], [2, "two"]], [[1, 1, "a"], [2, 2, "b"]]);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", "Id", "C", "ParentId") { CascadeUpdates = true }, Ct);
        }

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                JetConstraintException ex = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 2 }, Ct));
                Assert.StartsWith("Unique index violation on table 'P'", ex.Message, StringComparison.Ordinal);
            });
        }

        Assert.Equal(["1|one", "2|two"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        Assert.Equal(["1|1|a", "2|2|b"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
        long[] rowCounts = await ReadRowCountsAsync(ms, "P", "C");
        Assert.Equal([2L, 2L], rowCounts);
    }

    /// <summary>
    /// Two cascading relationships from <c>M.A</c> and <c>M.B</c> to <c>P.Id</c>
    /// both reach row 10, which references key 1 in both columns. The row is
    /// rewritten once with both new keys, so <c>M</c> keeps three live rows
    /// and a row count of three, and every index of <c>M</c> names the
    /// rewritten rows, whether its rows are found by the foreign-key indexes
    /// or by a scan.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <param name="scan">Whether the child table has a MEMO column, which makes Jet4 and ACCDB scan it.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsModesAndScans))]
    public async Task Update_RowReachedByTwoCascades_IsRewrittenOnceWithBothKeys(DatabaseFormat format, WriteMode mode, bool scan)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateAsync(db, format, [[1, "one"], [2, "two"]], []);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            var columns = new List<ColumnDefinition>
            {
                new("Id", typeof(int)) { IsPrimaryKey = true },
                new("A", typeof(int)),
                new("B", typeof(int)),
            };
            if (scan)
            {
                columns.Add(new ColumnDefinition("Notes", typeof(string)));
            }

            await writer.CreateTableAsync("M", columns, Ct);
            object[][] rows = scan
                ? [[10, 1, 1, "n10"], [11, 1, 2, "n11"], [12, 2, 1, "n12"]]
                : [[10, 1, 1], [11, 1, 2], [12, 2, 1]];
            Assert.Equal(3, await writer.InsertRowsAsync("M", rows, Ct));
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_M_A", "P", "Id", "M", "A") { CascadeUpdates = true }, Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_M_B", "P", "Id", "M", "B") { CascadeUpdates = true }, Ct);
        }

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5 }, Ct)));
        }

        List<string> expected = scan
            ? ["10|5|5|n10", "11|5|2|n11", "12|2|5|n12"]
            : ["10|5|5", "11|5|2", "12|2|5"];
        Assert.Equal(["2|two", "5|one"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        Assert.Equal(expected, await ForeignKeyTestDatabase.ReadRowsAsync(ms, "M"));
        long[] rowCounts = await ReadRowCountsAsync(ms, "P", "M");
        Assert.Equal([2L, 3L], rowCounts);
        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "P", "M");
    }

    /// <summary>
    /// Through a cascading self-relationship, row [1, 1] is both the row the
    /// update moves to key 5 and a dependent row of key 1. It is rewritten
    /// once, as [5, 5]; row [2, 1] follows to [2, 5], and the table keeps
    /// three live rows and a row count of three.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_SelfReferencingRowMovingItsOwnKey_IsRewrittenOnce(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateTreeAsync(format, cascadeUpdates: true);

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                Assert.Equal(1, await writer.UpdateRowsAsync("Tree", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5 }, Ct)));
        }

        Assert.Equal(["2|5", "3|", "5|5"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "Tree"));
        long[] rowCounts = await ReadRowCountsAsync(ms, "Tree");
        Assert.Equal([3L], rowCounts);
    }

    /// <summary>
    /// One key update cascades through a self-relationship and into another
    /// child table: <c>Tree</c> row [1, 1] becomes [5, 5] and [2, 1] follows
    /// to [2, 5], <c>Leaf</c> row [1, 1] follows to [1, 5], and both tables
    /// keep their row counts and indexes that name every live row.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_SelfRelationshipAndAnotherChildTable_RewritesBoth(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateTreeAsync(format, cascadeUpdates: true);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync(
                "Leaf",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("TreeId", typeof(int))],
                Ct);
            Assert.Equal(2, await writer.InsertRowsAsync("Leaf", [[1, 1], [2, 2]], Ct));
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_Leaf_Tree", "Tree", "Id", "Leaf", "TreeId") { CascadeUpdates = true },
                Ct);
        }

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                Assert.Equal(1, await writer.UpdateRowsAsync("Tree", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5 }, Ct)));
        }

        Assert.Equal(["2|5", "3|", "5|5"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "Tree"));
        Assert.Equal(["1|5", "2|2"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "Leaf"));
        long[] rowCounts = await ReadRowCountsAsync(ms, "Tree", "Leaf");
        Assert.Equal([3L, 2L], rowCounts);
        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "Tree", "Leaf");
    }

    /// <summary>
    /// When the update of self-referencing row [1, 1] also assigns its foreign
    /// key, the row keeps the assigned value unless that value is the key the
    /// update moves: assigning 3 gives [5, 3], and assigning 1, the old key,
    /// gives [5, 5]. Either way row [2, 1] follows to [2, 5] and the row is
    /// rewritten once.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <param name="assignedParentId">The foreign key the update assigns.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsModesAndAssignedParents))]
    public async Task Update_SelfReferencingRowAlsoAssigningItsForeignKey_KeepsAnAssignedKeyThatDoesNotMove(DatabaseFormat format, WriteMode mode, int assignedParentId)
    {
        await using MemoryStream ms = await this.CreateTreeAsync(format, cascadeUpdates: true);

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                Assert.Equal(1, await writer.UpdateRowsAsync("Tree", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5, ["ParentId"] = assignedParentId }, Ct)));
        }

        string movedRow = assignedParentId == 1 ? "5|5" : "5|3";
        Assert.Equal(["2|5", "3|", movedRow], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "Tree"));
        long[] rowCounts = await ReadRowCountsAsync(ms, "Tree");
        Assert.Equal([3L], rowCounts);
    }

    /// <summary>
    /// Without cascading updates, a self-referencing row that the update moves
    /// counts among the dependent rows of its old key, and the update is
    /// refused without changing any row.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_SelfReferencingRowWithoutCascade_IsRefusedAndCountsItself(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateTreeAsync(format, cascadeUpdates: false);

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                JetConstraintException ex = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.UpdateRowsAsync("Tree", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5 }, Ct));
                Assert.Equal(
                    "UPDATE on 'Tree' violates foreign-key constraint 'FK_Tree_Self': 2 dependent row(s) in 'Tree' reference the old key(s) and cascade-update is not enabled.",
                    ex.Message);
            });
        }

        Assert.Equal(["1|1", "2|1", "3|"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "Tree"));
        long[] rowCounts = await ReadRowCountsAsync(ms, "Tree");
        Assert.Equal([3L], rowCounts);
    }

    /// <summary>
    /// Without cascading updates, a self-referencing row whose update moves
    /// its key and assigns its foreign key to another parent is not a
    /// dependent row of its old key, though the stored row still names it:
    /// only row [2, 1] is counted, and the update is refused without changing
    /// any row.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_SelfReferencingRowWithoutCascadeAssigningAnotherParent_CountsOnlyTheOtherRows(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateTreeAsync(format, cascadeUpdates: false);

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                JetConstraintException ex = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.UpdateRowsAsync("Tree", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5, ["ParentId"] = 3 }, Ct));
                Assert.Equal(
                    "UPDATE on 'Tree' violates foreign-key constraint 'FK_Tree_Self': 1 dependent row(s) in 'Tree' reference the old key(s) and cascade-update is not enabled.",
                    ex.Message);
            });
        }

        Assert.Equal(["1|1", "2|1", "3|"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "Tree"));
        long[] rowCounts = await ReadRowCountsAsync(ms, "Tree");
        Assert.Equal([3L], rowCounts);
    }

    /// <summary>
    /// Through a cascading self-relationship on the two-column key
    /// (<c>A</c>, <c>B</c>), one update moves rows (1, 1) and (1, 2) to
    /// <c>A</c> 5. Row (1, 2), whose foreign key names (1, 1), is a dependent
    /// row of the key the other moved row gives up, so it takes that row's new
    /// key (5, 1) in its own rewrite; row (2, 1), which the update does not
    /// match, follows to (5, 1) as a stored dependent row. The table keeps
    /// three live rows, a row count of three and indexes that name every row.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_SelfReferencingRowNamingAnotherMovedRow_TakesThatRowsNewKey(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateCompositeTreeAsync(format, cascadeUpdates: true);

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                Assert.Equal(2, await writer.UpdateRowsAsync("Tree", RowCriteria.Where("A", 1), new RowValues { ["A"] = 5 }, Ct)));
        }

        Assert.Equal(["2|1|5|1", "5|1||", "5|2|5|1"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "Tree"));
        long[] rowCounts = await ReadRowCountsAsync(ms, "Tree");
        Assert.Equal([3L], rowCounts);
        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "Tree");
    }

    /// <summary>
    /// Without cascading updates, the same update is refused. Row (1, 2),
    /// which the update moves and whose foreign key names the key row (1, 1)
    /// gives up, counts among the dependent rows with row (2, 1), which the
    /// update does not match, and no row changes.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_SelfReferencingRowNamingAnotherMovedRowWithoutCascade_IsCounted(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await this.CreateCompositeTreeAsync(format, cascadeUpdates: false);

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                JetConstraintException ex = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.UpdateRowsAsync("Tree", RowCriteria.Where("A", 1), new RowValues { ["A"] = 5 }, Ct));
                Assert.Equal(
                    "UPDATE on 'Tree' violates foreign-key constraint 'FK_Tree_Self': 2 dependent row(s) in 'Tree' reference the old key(s) and cascade-update is not enabled.",
                    ex.Message);
            });
        }

        Assert.Equal(["1|1||", "1|2|1|1", "2|1|1|1"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "Tree"));
        long[] rowCounts = await ReadRowCountsAsync(ms, "Tree");
        Assert.Equal([3L], rowCounts);
    }

    /// <summary>Plants a cascading relationship without checking an existing orphan.</summary>
    /// <param name="ms">The database.</param>
    /// <returns>The asynchronous operation.</returns>
    private static async Task PlantCascadeRelationshipAsync(MemoryStream ms)
    {
        ms.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: Ct);
        long page = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, Ct);
        TableDef definition = await harness.Database.TableDefs.ReadRequiredTableDefAsync(page, Constants.SystemTableNames.Relationships, Ct);
        object[] row = definition.CreateNullValueRow();
        definition.SetValueByName(row, "ccolumn", 1);
        definition.SetValueByName(row, "grbit", 0x100);
        definition.SetValueByName(row, "icolumn", 0);
        definition.SetValueByName(row, "szColumn", "ParentId");
        definition.SetValueByName(row, "szObject", "C");
        definition.SetValueByName(row, "szReferencedColumn", "Id");
        definition.SetValueByName(row, "szReferencedObject", "P");
        definition.SetValueByName(row, "szRelationship", "FK_C_P");
        await harness.Services.Indexes.InsertSystemRowAndMaintainAsync(page, definition, Constants.SystemTableNames.Relationships, row, cancellationToken: Ct);
    }

    /// <summary>
    /// Makes the MEMO that starts with <see cref="UnreadableMemoMarker"/>
    /// unreadable by changing the page-type byte of its LVAL page.
    /// </summary>
    /// <param name="ms">The database.</param>
    /// <param name="format">The database format.</param>
    private static void BreakLongValuePage(MemoryStream ms, DatabaseFormat format)
    {
        byte[] file = ms.ToArray();
        int offset = file.AsSpan().IndexOf(Encoding.ASCII.GetBytes(UnreadableMemoMarker));
        if (offset < 0)
        {
            offset = file.AsSpan().IndexOf(Encoding.Unicode.GetBytes(UnreadableMemoMarker));
        }

        Assert.True(offset >= 0, "The MEMO's LVAL page was not found.");
        int pageSize = format == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        int pageStart = offset / pageSize * pageSize;
        Assert.Equal(0x01, file[pageStart]);
        ms.Position = pageStart;
        ms.WriteByte(0x7F);
        ms.Position = 0;
    }

    /// <summary>Returns the data pages that hold the live rows of <paramref name="tableName"/>.</summary>
    /// <param name="ms">The database.</param>
    /// <param name="tableName">The table.</param>
    /// <returns>The page numbers.</returns>
    private static async Task<HashSet<long>> ReadDataPagesAsync(MemoryStream ms, string tableName)
    {
        ms.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(ms, ReaderOptions, cancellationToken: Ct);
        CatalogEntry? entry = await harness.GetCatalogEntryAsync(tableName, Ct);
        Assert.NotNull(entry);
        List<RowLocation> rows = await harness.Database.GetLiveRowLocationsAsync(entry.TDefPage, Ct);
        return [.. rows.Select(row => row.PageNumber)];
    }

    /// <summary>Reads the stored row count of each of <paramref name="tableNames"/>.</summary>
    /// <param name="ms">The database.</param>
    /// <param name="tableNames">The tables.</param>
    /// <returns>The row counts, in the order of <paramref name="tableNames"/>.</returns>
    private static async Task<long[]> ReadRowCountsAsync(MemoryStream ms, params string[] tableNames)
    {
        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, Ct);
        IReadOnlyList<TableStat> stats = await reader.GetTableStatsAsync(Ct);
        return [.. tableNames.Select(name => stats.Single(stat => stat.Name == name).RowCount)];
    }

    /// <summary>
    /// Returns <c>Tree</c> (<c>Id</c> primary key, <c>ParentId</c>) with rows
    /// [1, 1], [2, 1] and [3, null], related to itself by <c>FK_Tree_Self</c>
    /// from <c>ParentId</c> to <c>Id</c>.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="cascadeUpdates">Whether the relationship cascades updates.</param>
    /// <returns>The database, positioned at 0.</returns>
    private async Task<MemoryStream> CreateTreeAsync(DatabaseFormat format, bool cascadeUpdates)
    {
        MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync(
                "Tree",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int))],
                Ct);
            Assert.Equal(3, await writer.InsertRowsAsync("Tree", [[1, 1], [2, 1], [3, null]], Ct));
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_Tree_Self", "Tree", "Id", "Tree", "ParentId") { CascadeUpdates = cascadeUpdates },
                Ct);
        }

        ms.Position = 0;
        return ms;
    }

    /// <summary>
    /// Returns <c>Tree</c> (<c>A</c> and <c>B</c> the primary key, <c>PA</c>,
    /// <c>PB</c>) with rows [1, 1, null, null], [1, 2, 1, 1] and [2, 1, 1, 1],
    /// related to itself by <c>FK_Tree_Self</c> from (<c>PA</c>, <c>PB</c>) to
    /// (<c>A</c>, <c>B</c>).
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="cascadeUpdates">Whether the relationship cascades updates.</param>
    /// <returns>The database, positioned at 0.</returns>
    private async Task<MemoryStream> CreateCompositeTreeAsync(DatabaseFormat format, bool cascadeUpdates)
    {
        MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync(
                "Tree",
                [
                    new ColumnDefinition("A", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("B", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("PA", typeof(int)),
                    new ColumnDefinition("PB", typeof(int)),
                ],
                Ct);
            Assert.Equal(3, await writer.InsertRowsAsync("Tree", [[1, 1, null, null], [1, 2, 1, 1], [2, 1, 1, 1]], Ct));
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_Tree_Self", "Tree", ["A", "B"], "Tree", ["PA", "PB"]) { CascadeUpdates = cascadeUpdates },
                Ct);
        }

        ms.Position = 0;
        return ms;
    }
}
