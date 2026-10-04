namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using WriteMode = JetDatabaseWriter.Tests.Writer.TransactionReadVisibilityTests.WriteMode;

/// <summary>
/// A cascade update rewrites each dependent row by deleting it and inserting
/// it with the new key, and it used to encode the new version only after the
/// delete, so a child row the new key grew past a data page was lost. Every
/// rewritten child row, with the key changes of every relationship that
/// reaches it, is now encoded and measured before any row is deleted, on the
/// index-seek path and on the snapshot path. Jet3 always takes the snapshot
/// path; Jet4 and ACCDB seek the child's foreign-key index, and take the
/// snapshot path when the seek cannot decode a child row (one with a MEMO
/// value). The update's own new rows are encoded once the plan has written
/// any self-relationship's cascaded key into them and before any cascade is
/// applied, so each is measured as it will be written, and a parent row that
/// cannot be encoded moves no child row.
/// </summary>
/// <param name="db">Caches the fixture files.</param>
public sealed class ForeignKeyCascadeUpdateRowSizeTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private static readonly AccessReaderOptions ReaderOptions = new() { UseLockFile = false };

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every format in every write mode, with and without a MEMO column in the child table.</summary>
    /// <returns>The format, write mode and MEMO-column triples.</returns>
    public static TheoryData<DatabaseFormat, WriteMode, bool> FormatsModesAndPaths()
    {
        var data = new TheoryData<DatabaseFormat, WriteMode, bool>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                data.Add(format, mode, false);
                data.Add(format, mode, true);
            }
        }

        return data;
    }

    /// <summary>Gets every format in every write mode.</summary>
    /// <returns>The format and write-mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes()
    {
        var data = new TheoryData<DatabaseFormat, WriteMode>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                data.Add(format, mode);
            }
        }

        return data;
    }

    /// <summary>Gets every write mode.</summary>
    /// <returns>The write modes.</returns>
    public static TheoryData<WriteMode> Modes() => [.. Enum.GetValues<WriteMode>()];

    /// <summary>
    /// <c>C.PCode</c> references the Text key <c>P.Code</c> and cascades
    /// updates. Child row 1 is short; child row 2 is padded with Binary values
    /// to about 100 bytes under the maximum row length, so changing the key
    /// from 'k' to 200 characters grows it past a data page. The update throws
    /// the row-size <see cref="JetLimitationException"/> and the file is
    /// byte-for-byte what it was: the parent keeps its key and both child rows
    /// stay (an explicit transaction is committed after the failure).
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <param name="childHasMemo">Whether the child table has a MEMO column, which sends a Jet4 or ACCDB cascade down the snapshot path.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsModesAndPaths))]
    public async Task CascadeUpdate_RewrittenChildRowPastAPage_IsRefusedAndChangesNothing(DatabaseFormat format, WriteMode mode, bool childHasMemo)
    {
        // Jet3 rows hold at most 2,036 bytes and Jet4/ACE rows 4,080; the
        // padded child row is 1,936 (1,950 with the MEMO) or 3,980 (3,996).
        int[] padLengths = PadLengths(format);
        int maxRowLength = MaxRowLength(format);

        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync("P", [new ColumnDefinition("Code", typeof(string), maxLength: 200) { IsPrimaryKey = true }], Ct);

            var childColumns = new List<ColumnDefinition>
            {
                new("Id", typeof(int)) { IsPrimaryKey = true },
                new("PCode", typeof(string), maxLength: 200),
            };
            childColumns.AddRange(padLengths.Select((_, i) => new ColumnDefinition($"Pad{i}", typeof(byte[]), maxLength: 255)));
            if (childHasMemo)
            {
                childColumns.Add(new ColumnDefinition("Notes", typeof(string)));
            }

            await writer.CreateTableAsync("C", childColumns, Ct);
            await writer.InsertRowAsync("P", ["k"], Ct);
            await writer.InsertRowsAsync("C", [ChildRow(1, ["k"], padLengths.Select(_ => 10), childHasMemo), ChildRow(2, ["k"], padLengths, childHasMemo)], Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", "Code", "C", "PCode") { CascadeUpdates = true }, Ct);
        }

        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                JetLimitationException ex = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                    await writer.UpdateRowsAsync("P", RowCriteria.Where("Code", "k"), new RowValues { ["Code"] = new string('n', 200) }, Ct));
                Assert.Contains($"{maxRowLength}-byte maximum", ex.Message, StringComparison.Ordinal);
            });
        }

        Assert.Equal(before, ms.ToArray());
        Assert.Equal(["k"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, Ct);
        var children = new List<string>();
        await foreach (object[] row in reader.Rows("C", cancellationToken: Ct))
        {
            children.Add(string.Create(CultureInfo.InvariantCulture, $"{row[0]}|{row[1]}"));
        }

        children.Sort(StringComparer.Ordinal);
        Assert.Equal(["1|k", "2|k"], children);
        IReadOnlyList<TableStat> stats = await reader.GetTableStatsAsync(Ct);
        Assert.Equal(1, stats.Single(stat => stat.Name == "P").RowCount);
        Assert.Equal(2, stats.Single(stat => stat.Name == "C").RowCount);
    }

    /// <summary>
    /// <c>C.A</c> and <c>C.B</c> both reference the Text key <c>P.Code</c> and
    /// cascade updates, and both child rows hold the key 'k' in both columns.
    /// Child row 2 is padded with Binary values to about 300 bytes under the
    /// maximum row length, so changing the key to 200 characters in one of its
    /// columns fits, but the one rewrite that takes the key change of both
    /// relationships is past a data page. The update throws the row-size
    /// <see cref="JetLimitationException"/> and the file is byte-for-byte what
    /// it was. Once row 2's <c>B</c> is set to Null, the same update succeeds
    /// and rewrites row 2 through <c>C.A</c> alone.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <param name="childHasMemo">Whether the child table has a MEMO column, which sends a Jet4 or ACCDB cascade down the snapshot path.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsModesAndPaths))]
    public async Task CascadeUpdate_RowReachedByTwoRelationshipsPastAPage_IsRefusedAndChangesNothing(DatabaseFormat format, WriteMode mode, bool childHasMemo)
    {
        int[] padLengths = PadLengthsWithRoomForOneKey(format);
        int maxRowLength = MaxRowLength(format);
        string newCode = new('n', 200);

        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync("P", [new ColumnDefinition("Code", typeof(string), maxLength: 200) { IsPrimaryKey = true }], Ct);

            var childColumns = new List<ColumnDefinition>
            {
                new("Id", typeof(int)) { IsPrimaryKey = true },
                new("A", typeof(string), maxLength: 200),
                new("B", typeof(string), maxLength: 200),
            };
            childColumns.AddRange(padLengths.Select((_, i) => new ColumnDefinition($"Pad{i}", typeof(byte[]), maxLength: 255)));
            if (childHasMemo)
            {
                childColumns.Add(new ColumnDefinition("Notes", typeof(string)));
            }

            await writer.CreateTableAsync("C", childColumns, Ct);
            await writer.InsertRowAsync("P", ["k"], Ct);
            await writer.InsertRowsAsync(
                "C",
                [ChildRow(1, ["k", "k"], padLengths.Select(_ => 10), childHasMemo), ChildRow(2, ["k", "k"], padLengths, childHasMemo)],
                Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_A", "P", "Code", "C", "A") { CascadeUpdates = true }, Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_B", "P", "Code", "C", "B") { CascadeUpdates = true }, Ct);
        }

        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                JetLimitationException ex = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                    await writer.UpdateRowsAsync("P", RowCriteria.Where("Code", "k"), new RowValues { ["Code"] = newCode }, Ct));
                Assert.Contains($"{maxRowLength}-byte maximum", ex.Message, StringComparison.Ordinal);
            });
        }

        Assert.Equal(before, ms.ToArray());

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("C", RowCriteria.Where("Id", 2), new RowValues { ["B"] = null }, Ct));
            Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Code", "k"), new RowValues { ["Code"] = newCode }, Ct));
        }

        Assert.Equal([newCode], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        Assert.Equal([$"1|{newCode}|{newCode}", $"2|{newCode}|"], await ReadLeadingColumnsAsync(ms, "C", 3));
    }

    /// <summary>
    /// Through the cascading self-relationship <c>Tree.ParentCode</c> to
    /// <c>Tree.Code</c>, the update's own row ['k', 'k'] is a dependent row of
    /// the key it moves, so the plan writes the cascaded key into the row's own
    /// new version, which the update then encodes. The row is padded with
    /// Binary values to about 300 bytes under the maximum row length: the new
    /// 200-character <c>Code</c> alone fits, but with the cascaded
    /// <c>ParentCode</c> as well the row is past a data page. The update throws
    /// the row-size <see cref="JetLimitationException"/> and the file is
    /// byte-for-byte what it was. The same update with <c>ParentCode</c>
    /// assigned Null, which keeps the key from cascading into the row, then
    /// succeeds.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task CascadeUpdate_SelfReferencingRowPastAPageWithItsCascadedKey_IsRefusedAndChangesNothing(DatabaseFormat format, WriteMode mode)
    {
        int[] padLengths = PadLengthsWithRoomForOneKey(format);
        int maxRowLength = MaxRowLength(format);
        string newCode = new('n', 200);

        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            var columns = new List<ColumnDefinition>
            {
                new("Code", typeof(string), maxLength: 200) { IsPrimaryKey = true },
                new("ParentCode", typeof(string), maxLength: 200),
            };
            columns.AddRange(padLengths.Select((_, i) => new ColumnDefinition($"Pad{i}", typeof(byte[]), maxLength: 255)));
            await writer.CreateTableAsync("Tree", columns, Ct);
            await writer.InsertRowAsync("Tree", ["k", "k", .. padLengths.Select(length => (object)new byte[length])], Ct);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_Tree_Self", "Tree", "Code", "Tree", "ParentCode") { CascadeUpdates = true },
                Ct);
        }

        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                JetLimitationException ex = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                    await writer.UpdateRowsAsync("Tree", RowCriteria.Where("Code", "k"), new RowValues { ["Code"] = newCode }, Ct));
                Assert.Contains($"{maxRowLength}-byte maximum", ex.Message, StringComparison.Ordinal);
            });
        }

        Assert.Equal(before, ms.ToArray());

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("Tree", RowCriteria.Where("Code", "k"), new RowValues { ["Code"] = newCode, ["ParentCode"] = null }, Ct));
        }

        Assert.Equal([$"{newCode}|"], await ReadLeadingColumnsAsync(ms, "Tree", 2));
    }

    /// <summary>
    /// The update's own row is measured as it will be written, with the key a
    /// self-relationship cascades into it. Through the cascading
    /// self-relationship <c>Tree.ParentCode</c> to <c>Tree.Code</c>, the row
    /// [K, K], where K is 41 characters, moves its key to 'x' and so takes 'x'
    /// as its <c>ParentCode</c> too. The row is padded with Binary values to
    /// about 10 bytes under the maximum row length, and the update also sets
    /// <c>Extra</c> to 60 characters: with <c>ParentCode</c> still K the row
    /// would be past a data page, but the version the update writes,
    /// ['x', 'x'], fits. The update succeeds, and the row reads back as
    /// written, with its row count and indexes in step.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task CascadeUpdate_SelfReferencingRowThatFitsWithItsCascadedKey_IsUpdated(DatabaseFormat format, WriteMode mode)
    {
        int[] padLengths = PadLengthsWithRoomForTenBytes(format);
        string oldCode = new('k', 41);
        string extra = new('e', 60);

        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            var columns = new List<ColumnDefinition>
            {
                new("Code", typeof(string), maxLength: 200) { IsPrimaryKey = true },
                new("ParentCode", typeof(string), maxLength: 200),
            };
            columns.AddRange(padLengths.Select((_, i) => new ColumnDefinition($"Pad{i}", typeof(byte[]), maxLength: 255)));
            columns.Add(new ColumnDefinition("Extra", typeof(string), maxLength: 200));
            await writer.CreateTableAsync("Tree", columns, Ct);
            await writer.InsertRowAsync("Tree", [oldCode, oldCode, .. padLengths.Select(length => (object)new byte[length]), null], Ct);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_Tree_Self", "Tree", "Code", "Tree", "ParentCode") { CascadeUpdates = true },
                Ct);
        }

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                Assert.Equal(1, await writer.UpdateRowsAsync("Tree", RowCriteria.Where("Code", oldCode), new RowValues { ["Code"] = "x", ["Extra"] = extra }, Ct)));
        }

        ms.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, Ct))
        {
            object[] row = Assert.Single(await reader.Rows("Tree", cancellationToken: Ct).ToListAsync(Ct));
            Assert.Equal("x", row[0]);
            Assert.Equal("x", row[1]);
            Assert.Equal(padLengths, row.Skip(2).Take(padLengths.Length).Select(value => ((byte[])value).Length));
            Assert.Equal(extra, row[^1]);
            IReadOnlyList<TableStat> stats = await reader.GetTableStatsAsync(Ct);
            Assert.Equal(1, stats.Single(stat => stat.Name == "Tree").RowCount);
        }

        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(ms, "Tree");
    }

    /// <summary>
    /// The update's own row is encoded and measured before any cascade is
    /// applied. Here the parent row is padded with Binary values to about 100 bytes
    /// under the maximum row length, so changing its key from 'k' to 200
    /// characters grows it past a data page, while the short child rows would
    /// take the new key. The update throws the row-size
    /// <see cref="JetLimitationException"/> and the file is byte-for-byte what
    /// it was: no child row was moved to a key no parent row holds. Only the
    /// direct and explicit-commit modes would show such a cascade; the
    /// implicit rollback of <see cref="AccessWriterOptions.UseTransactionalWrites"/>
    /// undoes it.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the writer runs the update.</param>
    /// <param name="childHasMemo">Whether the child table has a MEMO column, which sends a Jet4 or ACCDB cascade down the snapshot path.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsModesAndPaths))]
    public async Task CascadeUpdate_ParentRowPastAPage_IsRefusedBeforeAnyChildRowIsRewritten(DatabaseFormat format, WriteMode mode, bool childHasMemo)
    {
        int[] padLengths = PadLengths(format);
        int maxRowLength = MaxRowLength(format);

        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            var parentColumns = new List<ColumnDefinition> { new("Code", typeof(string), maxLength: 200) { IsPrimaryKey = true } };
            parentColumns.AddRange(padLengths.Select((_, i) => new ColumnDefinition($"Pad{i}", typeof(byte[]), maxLength: 255)));
            await writer.CreateTableAsync("P", parentColumns, Ct);

            var childColumns = new List<ColumnDefinition>
            {
                new("Id", typeof(int)) { IsPrimaryKey = true },
                new("PCode", typeof(string), maxLength: 200),
            };
            if (childHasMemo)
            {
                childColumns.Add(new ColumnDefinition("Notes", typeof(string)));
            }

            await writer.CreateTableAsync("C", childColumns, Ct);
            await writer.InsertRowAsync("P", ["k", .. padLengths.Select(length => (object)new byte[length])], Ct);
            await writer.InsertRowsAsync("C", [ChildRow(1, ["k"], [], childHasMemo), ChildRow(2, ["k"], [], childHasMemo)], Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", "Code", "C", "PCode") { CascadeUpdates = true }, Ct);
        }

        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                JetLimitationException ex = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                    await writer.UpdateRowsAsync("P", RowCriteria.Where("Code", "k"), new RowValues { ["Code"] = new string('n', 200) }, Ct));
                Assert.Contains($"{maxRowLength}-byte maximum", ex.Message, StringComparison.Ordinal);
            });
        }

        Assert.Equal(before, ms.ToArray());
        string childSuffix = childHasMemo ? "|m" : string.Empty;
        Assert.Equal([$"1|k{childSuffix}", $"2|k{childSuffix}"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, Ct);
        object[] parent = Assert.Single(await reader.Rows("P", cancellationToken: Ct).ToListAsync(Ct));
        Assert.Equal("k", parent[0]);
        IReadOnlyList<TableStat> stats = await reader.GetTableStatsAsync(Ct);
        Assert.Equal(1, stats.Single(stat => stat.Name == "P").RowCount);
        Assert.Equal(2, stats.Single(stat => stat.Name == "C").RowCount);
    }

    /// <summary>
    /// The same ordering for a value the encoder refuses: on Jet3, where a
    /// decimal column is Currency, an update that moves the cascading key
    /// <c>P.Id</c> from 1 to 3 and sets <c>P.Amt</c> past
    /// ±922,337,203,685,477.5807 throws <see cref="OverflowException"/> with
    /// the file unchanged, so the child row keeps <c>ParentId</c> 1.
    /// </summary>
    /// <param name="mode">How the writer runs the update.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(Modes))]
    public async Task CascadeUpdate_ParentCurrencyOutOfRange_IsRefusedBeforeAnyChildRowIsRewritten(WriteMode mode)
    {
        await using MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, DatabaseFormat.Jet3Mdb);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync(
                "P",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Amt", typeof(decimal)) { NumericPrecision = 14 }],
                Ct);
            await writer.CreateTableAsync(
                "C",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int))],
                Ct);
            await writer.InsertRowAsync("P", [1, 12m], Ct);
            await writer.InsertRowAsync("C", [1, 1], Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", "Id", "C", "ParentId") { CascadeUpdates = true }, Ct);
        }

        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(ms, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
                await Assert.ThrowsAsync<OverflowException>(async () =>
                    await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 3, ["Amt"] = 999_999_999_999_999m }, Ct)));
        }

        Assert.Equal(before, ms.ToArray());
        Assert.Equal(["1|12"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "P"));
        Assert.Equal(["1|1"], await ForeignKeyTestDatabase.ReadRowsAsync(ms, "C"));
    }

    /// <summary>
    /// Gets the lengths of Binary pad values that bring a row of an <c>Id</c>,
    /// a one-character Text key and the pads to about 100 bytes under the
    /// maximum row length.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>The pad lengths.</returns>
    private static int[] PadLengths(DatabaseFormat format) => format == DatabaseFormat.Jet3Mdb
        ? [.. Enumerable.Repeat(255, 7), 125]
        : [.. Enumerable.Repeat(255, 15), 106];

    /// <summary>
    /// Gets the lengths of Binary pad values that bring a row of two one-character
    /// Text keys and the pads, with or without an <c>Id</c>, to about 300 bytes
    /// under the maximum row length: room for one of the keys to grow to 200
    /// characters, but not both.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>The pad lengths.</returns>
    private static int[] PadLengthsWithRoomForOneKey(DatabaseFormat format) => format == DatabaseFormat.Jet3Mdb
        ? [.. Enumerable.Repeat(255, 6), 180]
        : [.. Enumerable.Repeat(255, 14), 161];

    /// <summary>
    /// Gets the lengths of Binary pad values that bring a row of two
    /// 41-character Text keys, the pads and a Null Text value to about 10
    /// bytes under the maximum row length.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>The pad lengths.</returns>
    private static int[] PadLengthsWithRoomForTenBytes(DatabaseFormat format) => format == DatabaseFormat.Jet3Mdb
        ? [.. Enumerable.Repeat(255, 7), 136]
        : [.. Enumerable.Repeat(255, 15), 112];

    /// <summary>Gets the maximum row length: 2,036 bytes on Jet3 and 4,080 on Jet4 and ACE.</summary>
    /// <param name="format">The database format.</param>
    /// <returns>The maximum row length in bytes.</returns>
    private static int MaxRowLength(DatabaseFormat format) => format == DatabaseFormat.Jet3Mdb ? 2036 : 4080;

    /// <summary>
    /// Reads the first <paramref name="count"/> columns of every row of a
    /// table, each row's values joined with '|' and Null as an empty string,
    /// sorted.
    /// </summary>
    /// <param name="ms">The database.</param>
    /// <param name="tableName">The table.</param>
    /// <param name="count">The number of leading columns to read.</param>
    /// <returns>The rows.</returns>
    private static async Task<List<string>> ReadLeadingColumnsAsync(MemoryStream ms, string tableName, int count)
    {
        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, Ct);
        var rows = new List<string>();
        await foreach (object[] row in reader.Rows(tableName, cancellationToken: Ct))
        {
            rows.Add(string.Join("|", row.Take(count).Select(value => value is DBNull ? string.Empty : Convert.ToString(value, CultureInfo.InvariantCulture))));
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    /// <summary>Builds a child row with the keys given and Binary pad values of <paramref name="padLengths"/> bytes.</summary>
    /// <param name="id">The row's <c>Id</c>.</param>
    /// <param name="keys">The row's key values, after its <c>Id</c>.</param>
    /// <param name="padLengths">The length of each pad value.</param>
    /// <param name="withMemo">Whether the row ends with a short MEMO value.</param>
    /// <returns>The row, in table-column order.</returns>
    private static object[] ChildRow(int id, string[] keys, IEnumerable<int> padLengths, bool withMemo)
    {
        var row = new List<object> { id };
        row.AddRange(keys);
        row.AddRange(padLengths.Select(length => (object)Enumerable.Repeat((byte)id, length).ToArray()));
        if (withMemo)
        {
            row.Add("m");
        }

        return [.. row];
    }
}
