namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1812 // Test class instantiated by xUnit

/// <summary>
/// Behavioural coverage for <see cref="ColumnDefinition.IsAutoIncrement"/>
/// across the integral CLR types Jet supports for autonumber columns
/// (<see cref="byte"/>, <see cref="short"/>, <see cref="int"/>,
/// <see cref="long"/>) plus explicit-value override and seed-after-delete
/// behaviour.
///
/// <para>Jackcess analogue: <c>impl/AutoNumberTest.java</c>.
/// </para>
/// </summary>
/// <param name="db">The database input.</param>
public sealed class AutoNumberTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// Auto-increment values start at 1 and increase monotonically when null
    /// is supplied for each FLAG_AUTO_LONG CLR type supported by the writer.
    /// </summary>
    /// <param name="clrType">The CLR type of the column being tested.</param>
    /// <param name="expected1">The expected value for the first row.</param>
    /// <param name="expected2">The expected value for the second row.</param>
    /// <param name="expected3">The expected value for the third row.</param>
    [Theory]
    [InlineData(typeof(int), 1, 2, 3)]
    [InlineData(typeof(short), (short)1, (short)2, (short)3)]
    public async Task AutoIncrement_NullValues_AssignsMonotonicSequenceFromOne(Type clrType, object expected1, object expected2, object expected3)
    {
        await using MemoryStream? ms = await this.CopyNorthwindAsync();
        if (ms is null)
        {
            return;
        }

        string tableName = $"AI_{Guid.NewGuid():N}"[..18];

        await using (AccessWriter writer = await OpenWriterAsync(ms, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new("Id", clrType) { IsAutoIncrement = true, IsNullable = false },
                    new("Label", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(tableName, [DBNull.Value, "first"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [DBNull.Value, "second"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [DBNull.Value, "third"], TestContext.Current.CancellationToken);
        }

        await using (AccessReader reader = await OpenReaderAsync(ms, TestContext.Current.CancellationToken))
        {
            List<object> ids = [];
            await foreach (object[] row in reader.Rows(tableName, cancellationToken: TestContext.Current.CancellationToken))
            {
                ids.Add(row[0]);
            }

            Assert.Equal(3, ids.Count);
            Assert.Equal(expected1, ids[0]);
            Assert.Equal(expected2, ids[1]);
            Assert.Equal(expected3, ids[2]);
        }
    }

    /// <summary>
    /// FLAG_AUTO_LONG (0x04) is persisted in the TDEF column flags and surfaced
    /// through ColumnMetadata on reopen for the supported writer mappings.
    /// </summary>
    /// <param name="clrType">The CLR type of the column being tested.</param>
    [Theory]
    [InlineData(typeof(short))]
    [InlineData(typeof(int))]
    public async Task AutoIncrement_FlagPersists_AcrossWriterClose(Type clrType)
    {
        await using MemoryStream? ms = await this.CopyNorthwindAsync();
        if (ms is null)
        {
            return;
        }

        string tableName = $"AIF_{Guid.NewGuid():N}"[..18];

        await using (AccessWriter writer = await OpenWriterAsync(ms, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                tableName,
                [new("Id", clrType) { IsAutoIncrement = true, IsNullable = false }],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(ms, TestContext.Current.CancellationToken);
        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);

        ColumnMetadata id = Assert.Single(meta);
        Assert.Equal(clrType, id.ClrType);
        Assert.False(id.IsNullable);
    }

    /// <summary>
    /// After deleting all rows from an autonumber table, the next inserted
    /// row continues from the high-water mark — Access never re-uses counter
    /// values within a single writer session.
    /// </summary>
    [Fact]
    public async Task AutoIncrement_AfterDeleteAllRows_DoesNotReuseValues()
    {
        await using MemoryStream? ms = await this.CopyNorthwindAsync();
        if (ms is null)
        {
            return;
        }

        string tableName = $"AID_{Guid.NewGuid():N}"[..18];

        await using (AccessWriter writer = await OpenWriterAsync(ms, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new("Id", typeof(int)) { IsAutoIncrement = true, IsNullable = false },
                    new("Label", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(tableName, [DBNull.Value, "a"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [DBNull.Value, "b"], TestContext.Current.CancellationToken);
            int deleted = await writer.DeleteRowsAsync(tableName, "Label", "a", TestContext.Current.CancellationToken);
            Assert.Equal(1, deleted);
            deleted = await writer.DeleteRowsAsync(tableName, "Label", "b", TestContext.Current.CancellationToken);
            Assert.Equal(1, deleted);

            await writer.InsertRowAsync(tableName, [DBNull.Value, "c"], TestContext.Current.CancellationToken);
        }

        await using (AccessReader reader = await OpenReaderAsync(ms, TestContext.Current.CancellationToken))
        {
            List<int> ids = [];
            await foreach (object[] row in reader.Rows(tableName, cancellationToken: TestContext.Current.CancellationToken))
            {
                ids.Add((int)row[0]);
            }

            Assert.Single(ids);
            Assert.Equal(3, ids[0]);
        }
    }

    /// <summary>
    /// A writer session that deletes the top AutoNumber rows does not hand
    /// those values out again in a later session. The high-water mark lives in
    /// the TDEF counter, so a fresh writer seeds from it rather than from the
    /// largest surviving row. Covers plain writes, transactional writes and an
    /// explicit transaction in the second session.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="deleteAll">Whether the first session deletes every row rather than only the top one.</param>
    /// <param name="mode">How the second session writes: <c>plain</c>, <c>transactional</c> or <c>explicit</c>.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false, "plain")]
    [InlineData(DatabaseFormat.Jet4Mdb, false, "plain")]
    [InlineData(DatabaseFormat.AceAccdb, false, "plain")]
    [InlineData(DatabaseFormat.Jet3Mdb, true, "plain")]
    [InlineData(DatabaseFormat.Jet4Mdb, true, "plain")]
    [InlineData(DatabaseFormat.AceAccdb, true, "plain")]
    [InlineData(DatabaseFormat.Jet3Mdb, false, "transactional")]
    [InlineData(DatabaseFormat.AceAccdb, false, "transactional")]
    [InlineData(DatabaseFormat.Jet4Mdb, false, "explicit")]
    [InlineData(DatabaseFormat.AceAccdb, true, "explicit")]
    public async Task AutoIncrement_AfterDeletingTopRowsInEarlierSession_DoesNotReuseValues(DatabaseFormat format, bool deleteAll, string mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync(
                "Items",
                [
                    new("Id", typeof(int)) { IsAutoIncrement = true, IsNullable = false },
                    new("Label", typeof(string), maxLength: 50),
                ],
                [new IndexDefinition("PK_Items", "Id") { IsPrimaryKey = true }],
                ct);

            await writer.InsertRowAsync("Items", [DBNull.Value, "a"], ct);
            await writer.InsertRowAsync("Items", [DBNull.Value, "b"], ct);
            await writer.InsertRowAsync("Items", [DBNull.Value, "c"], ct);
            Assert.Equal(1, await writer.DeleteRowsAsync("Items", "Id", 3, ct));
            if (deleteAll)
            {
                Assert.Equal(2, await writer.DeleteRowsAsync("Items", RowCriteria.All(), ct));
            }
        }

        ms.Position = 0;
        var options = new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = mode == "transactional" };
        await using (AccessWriter writer = await AccessWriter.OpenAsync(ms, options, leaveOpen: true, ct))
        {
            if (mode == "explicit")
            {
                await using JetTransaction tx = await writer.BeginTransactionAsync(ct);
                await writer.InsertRowAsync("Items", [DBNull.Value, "d"], ct);
                await tx.CommitAsync(ct);
            }
            else
            {
                await writer.InsertRowAsync("Items", [DBNull.Value, "d"], ct);
            }
        }

        await using (AccessWriter writer = await OpenWriterAsync(ms, ct))
        {
            await writer.InsertRowAsync("Items", [DBNull.Value, "e"], ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        var ids = new Dictionary<string, int>(StringComparer.Ordinal);
        await foreach (object[] row in reader.Rows("Items", cancellationToken: ct))
        {
            ids[(string)row[1]] = (int)row[0];
        }

        Assert.Equal(4, ids["d"]);
        Assert.Equal(5, ids["e"]);
        Assert.Equal(deleteAll ? 2 : 4, ids.Count);
    }

    /// <summary>
    /// AddColumn, DropColumn and RenameColumn rebuild the table through a
    /// temporary copy. The rebuilt TDEF must carry the original AutoNumber
    /// high-water value forward, so values freed by deleting the top rows are
    /// not handed out again after the rewrite, whether the next insert runs in
    /// the same session or a later one. The attachment variant takes the
    /// rewrite path that transplants the temporary TDEF onto the original page.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="operation">The schema change: <c>add</c>, <c>drop</c> or <c>rename</c>.</param>
    /// <param name="sameSession">Whether the insert runs in the session that changed the schema.</param>
    /// <param name="withAttachment">Whether the table also has an attachment column.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "add", false, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, "add", false, false)]
    [InlineData(DatabaseFormat.AceAccdb, "add", false, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, "add", true, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, "add", true, false)]
    [InlineData(DatabaseFormat.AceAccdb, "add", true, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, "drop", false, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, "drop", true, false)]
    [InlineData(DatabaseFormat.AceAccdb, "rename", false, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, "rename", true, false)]
    [InlineData(DatabaseFormat.AceAccdb, "add", false, true)]
    [InlineData(DatabaseFormat.AceAccdb, "add", true, true)]
    [InlineData(DatabaseFormat.AceAccdb, "rename", true, true)]
    public async Task AutoIncrement_AfterSchemaRewrite_DoesNotReuseDeletedTopValues(DatabaseFormat format, string operation, bool sameSession, bool withAttachment)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        List<ColumnDefinition> columns =
        [
            new("Id", typeof(int)) { IsAutoIncrement = true, IsNullable = false },
            new("Label", typeof(string), maxLength: 50),
            new("Note", typeof(string), maxLength: 50),
        ];
        if (withAttachment)
        {
            columns.Add(new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true });
        }

        int rewrittenCount = columns.Count + operation switch
        {
            "add" => 1,
            "drop" => -1,
            _ => 0,
        };

        // Label stays at index 1 through every rewrite; every other cell is null.
        static object[] Row(string label, int count)
        {
            object[] row = new object[count];
            Array.Fill(row, DBNull.Value);
            row[1] = label;
            return row;
        }

        object[] NewRow(string label) => Row(label, rewrittenCount);

        async ValueTask ChangeSchemaAsync(AccessWriter writer)
        {
            switch (operation)
            {
                case "add":
                    await writer.AddColumnAsync("Items", new ColumnDefinition("Extra", typeof(int)), ct);
                    break;
                case "drop":
                    await writer.DropColumnAsync("Items", "Note", ct);
                    break;
                default:
                    await writer.RenameColumnAsync("Items", "Note", "Remark", ct);
                    break;
            }
        }

        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync(
                "Items",
                columns,
                [new IndexDefinition("PK_Items", "Id") { IsPrimaryKey = true }],
                ct);

            await writer.InsertRowAsync("Items", Row("a", columns.Count), ct);
            await writer.InsertRowAsync("Items", Row("b", columns.Count), ct);
            await writer.InsertRowAsync("Items", Row("c", columns.Count), ct);
            Assert.Equal(1, await writer.DeleteRowsAsync("Items", "Id", 3, ct));

            if (sameSession)
            {
                await ChangeSchemaAsync(writer);
                await writer.InsertRowAsync("Items", NewRow("d"), ct);
            }
        }

        if (!sameSession)
        {
            await using (AccessWriter writer = await OpenWriterAsync(ms, ct))
            {
                await ChangeSchemaAsync(writer);
            }

            await using (AccessWriter writer = await OpenWriterAsync(ms, ct))
            {
                await writer.InsertRowAsync("Items", NewRow("d"), ct);
            }
        }

        await using (AccessWriter writer = await OpenWriterAsync(ms, ct))
        {
            await writer.InsertRowAsync("Items", NewRow("e"), ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        var ids = new Dictionary<string, int>(StringComparer.Ordinal);
        await foreach (object[] row in reader.Rows("Items", cancellationToken: ct))
        {
            ids[(string)row[1]] = (int)row[0];
        }

        Assert.Equal(1, ids["a"]);
        Assert.Equal(2, ids["b"]);
        Assert.Equal(4, ids["d"]);
        Assert.Equal(5, ids["e"]);
        Assert.Equal(4, ids.Count);
    }

    /// <summary>
    /// A schema rewrite moves the rebuilt constraint list onto the table name,
    /// on both the drop-and-rename path and the transplant path a table with
    /// an attachment column takes. Rolling the AddColumn back must restore the
    /// pre-rewrite list, or its column count no longer matches the table and
    /// the next insert stores a NULL AutoNumber.
    /// </summary>
    /// <param name="withAttachment">Whether the table also has an attachment column.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AutoIncrement_AfterRolledBackAddColumn_AssignsNextValue(bool withAttachment)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        List<ColumnDefinition> columns =
        [
            new("Id", typeof(int)) { IsAutoIncrement = true, IsNullable = false },
            new("Label", typeof(string), maxLength: 50),
        ];
        if (withAttachment)
        {
            columns.Add(new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true });
        }

        object[] Row(string label)
        {
            object[] row = new object[columns.Count];
            Array.Fill(row, DBNull.Value);
            row[1] = label;
            return row;
        }

        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync(
                "Items",
                columns,
                [new IndexDefinition("PK_Items", "Id") { IsPrimaryKey = true }],
                ct);
            await writer.InsertRowAsync("Items", Row("a"), ct);

            JetTransaction tx = await writer.BeginTransactionAsync(ct);
            await writer.AddColumnAsync("Items", new ColumnDefinition("Extra", typeof(int)), ct);
            await tx.RollbackAsync(ct);

            await writer.InsertRowAsync("Items", Row("b"), ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        var ids = new Dictionary<string, object>(StringComparer.Ordinal);
        await foreach (object[] row in reader.Rows("Items", cancellationToken: ct))
        {
            ids[(string)row[1]] = row[0];
        }

        Assert.Equal(1, ids["a"]);
        Assert.Equal(2, ids["b"]);
        Assert.Equal(2, ids.Count);
    }

    /// <summary>
    /// When an explicit, non-null integer value is supplied for an autoincrement
    /// column, the writer honours the override and the value round-trips on read.
    /// </summary>
    [Fact]
    public async Task AutoIncrement_ExplicitValue_OverridesCounterAndRoundTrips()
    {
        await using MemoryStream? ms = await this.CopyNorthwindAsync();
        if (ms is null)
        {
            return;
        }

        string tableName = $"AIE_{Guid.NewGuid():N}"[..18];

        await using (AccessWriter writer = await OpenWriterAsync(ms, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new("Id", typeof(int)) { IsAutoIncrement = true, IsNullable = false },
                    new("Label", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(tableName, [42, "explicit"], TestContext.Current.CancellationToken);
        }

        await using (AccessReader reader = await OpenReaderAsync(ms, TestContext.Current.CancellationToken))
        {
            object[]? row = null;
            await foreach (object[] r in reader.Rows(tableName, cancellationToken: TestContext.Current.CancellationToken))
            {
                row = r;
                break;
            }

            Assert.NotNull(row);
            Assert.Equal(42, row[0]);
            Assert.Equal("explicit", row[1]);
        }
    }

    /// <summary>
    /// Documents the gap relative to Jackcess: byte and long auto-increment
    /// (Jet "BigInt"/Large Number autonumber and tiny-int autonumber) remain
    /// outside the writer's supported auto-number schema. The Jackcess analogue
    /// is AutoNumberTest#testInsertLongAutoNumber.
    /// </summary>
    /// <param name="clrType">The CLR type of the unsupported integral type.</param>
    [Theory]
    [InlineData(typeof(byte))]
    [InlineData(typeof(long))]
    public async Task AutoIncrement_OnUnsupportedIntegralType_ThrowsNotSupported(Type clrType)
    {
        await using MemoryStream? ms = await this.CopyNorthwindAsync();
        if (ms is null)
        {
            return;
        }

        string tableName = $"AIU_{Guid.NewGuid():N}"[..18];

        await using AccessWriter writer = await OpenWriterAsync(ms, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await writer.CreateTableAsync(
                tableName,
                [new("Id", clrType) { IsAutoIncrement = true, IsNullable = false }],
                TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// Access gives an AutoNumber column no default, so a <c>DefaultValue</c>
    /// property another tool stored on one stays in the file but is never
    /// applied. A schema rewrite carries the property over to the rebuilt
    /// column, and re-registering the rebuilt table used to turn it into the
    /// column's default, so after an AddColumn every insert stored 0 instead of
    /// the next AutoNumber. An explicit null never takes a default, so the rows
    /// after the rewrite ask for it, with <see cref="DbDefault.Value"/> or by
    /// leaving the column out. Runs with no transaction, with
    /// <see cref="AccessWriterOptions.UseTransactionalWrites"/> and in an
    /// explicit committed transaction.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode"><c>plain</c>, <c>transactional</c> or <c>explicit</c>.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "plain")]
    [InlineData(DatabaseFormat.Jet4Mdb, "plain")]
    [InlineData(DatabaseFormat.AceAccdb, "plain")]
    [InlineData(DatabaseFormat.Jet3Mdb, "transactional")]
    [InlineData(DatabaseFormat.Jet4Mdb, "transactional")]
    [InlineData(DatabaseFormat.AceAccdb, "transactional")]
    [InlineData(DatabaseFormat.Jet3Mdb, "explicit")]
    [InlineData(DatabaseFormat.Jet4Mdb, "explicit")]
    [InlineData(DatabaseFormat.AceAccdb, "explicit")]
    public async Task AutoIncrement_StrayDefaultValueProperty_IsIgnoredAfterSchemaRewrite(DatabaseFormat format, string mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            // FLAG_FIXED | 0x02 | FLAG_AUTO_LONG: an AutoNumber descriptor that also
            // carries a DefaultValue property, as another tool could write it.
            await writer.CreateTableAsync(
                "Items",
                [
                    new("Id", typeof(int)) { DefaultValueExpression = "0", DescriptorFlagsOverride = 0x07 },
                    new("Label", typeof(string), maxLength: 50),
                ],
                ct);
        }

        ms.Position = 0;
        var options = new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = mode == "transactional" };
        await using (AccessWriter writer = await AccessWriter.OpenAsync(ms, options, leaveOpen: true, ct))
        {
            JetTransaction? tx = mode == "explicit" ? await writer.BeginTransactionAsync(ct) : null;
            await writer.InsertRowAsync("Items", [DBNull.Value, "a"], ct);
            await writer.AddColumnAsync("Items", new ColumnDefinition("Extra", typeof(int)), ct);
            await writer.InsertRowAsync("Items", [DbDefault.Value, "b", DBNull.Value], ct);
            await writer.InsertRowAsync("Items", new RowValues { ["Label"] = "c" }, ct);
            if (tx is not null)
            {
                await tx.CommitAsync(ct);
                await tx.DisposeAsync();
            }
        }

        await using (AccessWriter writer = await OpenWriterAsync(ms, ct))
        {
            await writer.InsertRowAsync("Items", [DbDefault.Value, "d", DBNull.Value], ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        var ids = new Dictionary<string, object>(StringComparer.Ordinal);
        await foreach (object[] row in reader.Rows("Items", cancellationToken: ct))
        {
            ids[(string)row[1]] = row[0];
        }

        Assert.Equal(1, ids["a"]);
        Assert.Equal(2, ids["b"]);
        Assert.Equal(3, ids["c"]);
        Assert.Equal(4, ids["d"]);
        Assert.Equal(4, ids.Count);

        // The property itself is kept.
        IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync("Items", ct);
        Assert.Equal("0", metadata[0].DefaultValueExpression);
    }

    // ── Helpers ────────────────────────────────────────────────────────

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream, CancellationToken cancellationToken)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream, CancellationToken cancellationToken)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken);
    }

    private async ValueTask<MemoryStream?> CopyNorthwindAsync()
    {
        if (!File.Exists(TestDatabases.NorthwindTraders))
        {
            return null;
        }

        byte[] bytes = await db.GetFileAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        var ms = new MemoryStream();
        await ms.WriteAsync(bytes, TestContext.Current.CancellationToken);
        ms.Position = 0;
        return ms;
    }
}
