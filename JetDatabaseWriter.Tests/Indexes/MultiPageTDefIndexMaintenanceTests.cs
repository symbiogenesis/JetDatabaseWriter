namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Unique-index enforcement and index maintenance on a table whose definition
/// spans several TDEF pages, so its real-index descriptors (with their
/// <c>first_dp</c> root and <c>used_pages</c> pointers) sit on a continuation
/// page of the TDEF chain. Jet4 and ACE use 200 columns and 30 single-column
/// indexes; Jet3 uses 50 columns and 20 indexes, which already overflow its
/// 2 KB page. Jet3 rows longer than 255 bytes are covered by
/// <see cref="Writer.Jet3LongRowTests"/>.
/// The round-trip of such a schema is covered by
/// <see cref="IndexWriterTests.CreateTable_TDefChainSpansMultiplePages_RoundTrips"/>;
/// these tests cover writes to it.
/// <para>
/// Every test checks the indexes structurally: each index B-tree, walked from
/// the root recorded in the logical TDEF, must hold exactly one entry per live
/// row, pointing at that row and carrying that row's encoded key. Jet4 and ACE
/// tests also seek through the public <see cref="AccessReader.SeekRowsAsync"/>.
/// </para>
/// </summary>
public sealed class MultiPageTDefIndexMaintenanceTests : IDisposable
{
    private const string TableName = "Wide";

    private readonly CancellationToken ct = TestContext.Current.CancellationToken;
    private readonly List<string> tempFiles = [];

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DuplicateInsert_IntoUniqueIndex_IsRejectedBeforeWrite(DatabaseFormat format)
    {
        await using MemoryStream stream = await this.CreateWideTableStreamAsync(format);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.InsertRowAsync(TableName, WideRow(format, 1), this.ct);

            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.InsertRowAsync(TableName, WideRow(format, 1, salt: 7), this.ct));
            Assert.Contains("UX_00", ex.Message, StringComparison.Ordinal);
            Assert.Contains("before any row was written", ex.Message, StringComparison.Ordinal);

            // A duplicate inside one batch is caught too.
            _ = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.InsertRowsAsync(TableName, [WideRow(format, 2), WideRow(format, 2, salt: 7)], this.ct));
        }

        await this.AssertIndexesMatchRowsAsync(stream, format, expectedKeys: [1]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DuplicateUpdate_IntoUniqueIndex_IsRejected(DatabaseFormat format)
    {
        await using MemoryStream stream = await this.CreateWideTableStreamAsync(format);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            _ = await writer.InsertRowsAsync(TableName, [WideRow(format, 1), WideRow(format, 2)], this.ct);

            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.UpdateRowsAsync(TableName, "C000", 2, new Dictionary<string, object?> { ["C000"] = 1 }, this.ct));
            Assert.Contains("UX_00", ex.Message, StringComparison.Ordinal);
        }

        await this.AssertIndexesMatchRowsAsync(stream, format, expectedKeys: [1, 2]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task InsertUpdateDelete_KeepsEveryIndexInStep(DatabaseFormat format)
    {
        await using MemoryStream stream = await this.CreateWideTableStreamAsync(format);
        string lastIndex = string.Create(CultureInfo.InvariantCulture, $"IX_{IndexCountOf(format) - 1:D2}");
        string lastIndexColumn = string.Create(CultureInfo.InvariantCulture, $"C{IndexCountOf(format) - 1:D3}");

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            // 40 rows span several data pages.
            _ = await writer.InsertRowsAsync(TableName, Enumerable.Range(1, 40).Select(k => WideRow(format, k)).ToList(), this.ct);
            await writer.InsertRowAsync(TableName, WideRow(format, 41), this.ct);

            var changes = new Dictionary<string, object?> { ["C000"] = 1007, [lastIndexColumn] = -29 };
            Assert.Equal(1, await writer.UpdateRowsAsync(TableName, "C000", 7, changes, this.ct));
            Assert.Equal(1, await writer.DeleteRowsAsync(TableName, "C000", 13, this.ct));
            Assert.Equal(1, await writer.DeleteRowsAsync(TableName, "C000", 14, this.ct));
        }

        int[] expectedKeys = [.. Enumerable.Range(1, 41).Where(k => k is not (7 or 13 or 14)), 1007];
        await this.AssertIndexesMatchRowsAsync(stream, format, expectedKeys);

        if (format != DatabaseFormat.Jet3Mdb)
        {
            await using AccessReader reader = await OpenReaderAsync(stream);
            Assert.Empty(await SeekAsync(reader, "UX_00", 7));
            Assert.Empty(await SeekAsync(reader, "UX_00", 13));
            Assert.Equal(1007, Assert.Single(await SeekAsync(reader, "UX_00", 1007))[0]);
            Assert.Equal(41, Assert.Single(await SeekAsync(reader, "UX_00", 41))[0]);
            Assert.Equal(1007, Assert.Single(await SeekAsync(reader, lastIndex, -29))[0]);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ManyRows_MultiLevelIndexes_StayInStep(DatabaseFormat format)
    {
        await using MemoryStream stream = await this.CreateWideTableStreamAsync(format);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            // Even keys first: one batch large enough to need multi-level trees.
            _ = await writer.InsertRowsAsync(TableName, Enumerable.Range(1, 600).Select(k => WideRow(format, k * 2)).ToList(), this.ct);

            // Odd keys land between existing entries across many leaves.
            _ = await writer.InsertRowsAsync(TableName, Enumerable.Range(0, 150).Select(k => WideRow(format, (k * 8) + 1)).ToList(), this.ct);

            // Appends past the current maximum key.
            await writer.InsertRowAsync(TableName, WideRow(format, 5000), this.ct);
            await writer.InsertRowAsync(TableName, WideRow(format, 5001), this.ct);

            Assert.Equal(1, await writer.DeleteRowsAsync(TableName, "C000", 2, this.ct));
            Assert.Equal(1, await writer.DeleteRowsAsync(TableName, "C000", 601, this.ct));
        }

        int[] expectedKeys =
        [
            .. Enumerable.Range(1, 600).Select(k => k * 2).Where(k => k != 2),
            .. Enumerable.Range(0, 150).Select(k => (k * 8) + 1).Where(k => k != 601),
            5000,
            5001,
        ];
        await this.AssertIndexesMatchRowsAsync(stream, format, expectedKeys);

        if (format != DatabaseFormat.Jet3Mdb)
        {
            await using AccessReader reader = await OpenReaderAsync(stream);
            Assert.Empty(await SeekAsync(reader, "UX_00", 601));
            Assert.Equal(1001, Assert.Single(await SeekAsync(reader, "UX_00", 1001))[0]);
            Assert.Equal(5001, Assert.Single(await SeekAsync(reader, "UX_00", 5001))[0]);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ExplicitTransaction_Commit_EnforcesUniquenessAndMaintainsIndexes(DatabaseFormat format)
    {
        await using MemoryStream stream = await this.CreateWideTableStreamAsync(format);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            _ = await writer.InsertRowsAsync(TableName, [WideRow(format, 1), WideRow(format, 2), WideRow(format, 3)], this.ct);

            await using JetTransaction tx = await writer.BeginTransactionAsync(this.ct);
            _ = await writer.InsertRowsAsync(TableName, [WideRow(format, 4), WideRow(format, 5)], this.ct);
            await writer.InsertRowAsync(TableName, WideRow(format, 6), this.ct);

            // Duplicates of a committed key and of a key written inside the transaction.
            _ = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.InsertRowAsync(TableName, WideRow(format, 2, salt: 7), this.ct));
            _ = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.InsertRowAsync(TableName, WideRow(format, 5, salt: 7), this.ct));

            await tx.CommitAsync(this.ct);
        }

        await this.AssertIndexesMatchRowsAsync(stream, format, expectedKeys: [1, 2, 3, 4, 5, 6]);

        if (format != DatabaseFormat.Jet3Mdb)
        {
            await using AccessReader reader = await OpenReaderAsync(stream);
            Assert.Equal(5, Assert.Single(await SeekAsync(reader, "UX_00", 5))[0]);
            Assert.Equal(6, Assert.Single(await SeekAsync(reader, "IX_02", WideRow(format, 6)[2]))[0]);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ExplicitTransaction_Rollback_LeavesIndexesAtCommittedState(DatabaseFormat format)
    {
        await using MemoryStream stream = await this.CreateWideTableStreamAsync(format);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            _ = await writer.InsertRowsAsync(TableName, [WideRow(format, 1), WideRow(format, 2)], this.ct);

            await using JetTransaction tx = await writer.BeginTransactionAsync(this.ct);
            _ = await writer.InsertRowsAsync(TableName, [WideRow(format, 3), WideRow(format, 4)], this.ct);
            await tx.RollbackAsync(this.ct);
        }

        await this.AssertIndexesMatchRowsAsync(stream, format, expectedKeys: [1, 2]);

        await using AccessReader reader = await OpenReaderAsync(stream);
        Assert.Empty(await SeekAsync(reader, "UX_00", 3));
        Assert.Equal(2, Assert.Single(await SeekAsync(reader, "UX_00", 2))[0]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task TransactionalWrites_InsertUpdateDelete_KeepIndexesInStep(DatabaseFormat format)
    {
        await using MemoryStream stream = await this.CreateWideTableStreamAsync(format);

        await using (AccessWriter writer = await OpenWriterAsync(stream, new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = true }))
        {
            _ = await writer.InsertRowsAsync(TableName, Enumerable.Range(1, 12).Select(k => WideRow(format, k)).ToList(), this.ct);

            _ = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.InsertRowAsync(TableName, WideRow(format, 4, salt: 7), this.ct));
            _ = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.UpdateRowsAsync(TableName, "C000", 5, new Dictionary<string, object?> { ["C000"] = 6 }, this.ct));

            Assert.Equal(1, await writer.UpdateRowsAsync(TableName, "C000", 5, new Dictionary<string, object?> { ["C000"] = 500 }, this.ct));
            Assert.Equal(1, await writer.DeleteRowsAsync(TableName, "C000", 9, this.ct));
        }

        int[] expectedKeys = [1, 2, 3, 4, 6, 7, 8, 10, 11, 12, 500];
        await this.AssertIndexesMatchRowsAsync(stream, format, expectedKeys);

        await using AccessReader reader = await OpenReaderAsync(stream);
        Assert.Empty(await SeekAsync(reader, "UX_00", 5));
        Assert.Empty(await SeekAsync(reader, "UX_00", 9));
        Assert.Equal(500, Assert.Single(await SeekAsync(reader, "UX_00", 500))[0]);
    }

    /// <summary>
    /// AddColumn rebuilds the table through a fresh multi-page TDEF and
    /// re-indexes the copied rows; the rebuilt indexes must still enforce
    /// uniqueness and track later writes.
    /// </summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task AddColumn_RebuiltWideTable_KeepsIndexesInStep(DatabaseFormat format)
    {
        await using MemoryStream stream = await this.CreateWideTableStreamAsync(format);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            _ = await writer.InsertRowsAsync(TableName, Enumerable.Range(1, 10).Select(k => WideRow(format, k)).ToList(), this.ct);
            await writer.AddColumnAsync(TableName, new ColumnDefinition("Extra", typeof(int)), this.ct);

            object[] duplicate = [.. WideRow(format, 4, salt: 7), 1];
            _ = await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.InsertRowAsync(TableName, duplicate, this.ct));

            await writer.InsertRowAsync(TableName, [.. WideRow(format, 11), 2], this.ct);
            Assert.Equal(1, await writer.DeleteRowsAsync(TableName, "C000", 5, this.ct));
        }

        await this.AssertIndexesMatchRowsAsync(stream, format, [1, 2, 3, 4, 6, 7, 8, 9, 10, 11]);
    }

    /// <summary>
    /// The wide table is both the child of one relationship, whose FK column
    /// has no index of its own (so the relationship adds a real index to the
    /// wide TDEF), and the parent of another. Foreign-key logical indexes
    /// keep the wide table on the bulk rebuild path, and the relationship
    /// seeks read the wide TDEF to find the covering index. ACCDB only: a
    /// writer-created .mdb has no <c>MSysRelationships</c> table.
    /// </summary>
    [Fact]
    public async Task Relationships_OnBothSides_KeepIndexesInStepAndEnforceKeys()
    {
        const DatabaseFormat format = DatabaseFormat.AceAccdb;
        await using MemoryStream stream = await this.CreateWideTableStreamAsync(format);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "Parent",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }],
                this.ct);
            _ = await writer.InsertRowsAsync("Parent", Enumerable.Range(0, 5).Select(k => new object[] { 1_010_000 + k }).ToList(), this.ct);

            await writer.CreateTableAsync(
                "Child",
                [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("WideKey", typeof(int))],
                this.ct);

            _ = await writer.InsertRowsAsync(TableName, Enumerable.Range(1, 10).Select(k => WideRow(format, k)).ToList(), this.ct);

            // C101 holds key % 5 + 1,010,000, so every wide row has a parent.
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_Wide_Parent", "Parent", "Id", TableName, "C101"), this.ct);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_Child_Wide", TableName, "C000", "Child", "WideKey") { CascadeDeletes = true },
                this.ct);

            _ = await writer.InsertRowsAsync(TableName, Enumerable.Range(11, 10).Select(k => WideRow(format, k)).ToList(), this.ct);
            _ = await writer.InsertRowsAsync("Child", [[1, 3], [2, 3], [3, 4], [4, 15]], this.ct);

            object[] orphan = WideRow(format, 21);
            orphan[101] = 42;
            _ = await Assert.ThrowsAnyAsync<InvalidOperationException>(async () => await writer.InsertRowAsync(TableName, orphan, this.ct));
            _ = await Assert.ThrowsAnyAsync<InvalidOperationException>(async () => await writer.InsertRowAsync("Child", [5, 999], this.ct));
            _ = await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.InsertRowAsync(TableName, WideRow(format, 15, salt: 7), this.ct));

            Assert.Equal(1, await writer.DeleteRowsAsync(TableName, "C000", 3, this.ct));
        }

        await this.AssertIndexesMatchRowsAsync(stream, format, [.. Enumerable.Range(1, 20).Where(k => k != 3)], expectedForeignKeys: 2);

        await using AccessReader reader = await OpenReaderAsync(stream);
        using DataTable children = await reader.ReadDataTableAsync("Child", cancellationToken: this.ct);
        Assert.Equal([3, 4], children.Rows.Cast<DataRow>().Select(r => (int)r["Id"]).Order());
        Assert.Equal(15, Assert.Single(await SeekAsync(reader, "UX_00", 15))[0]);
        Assert.Empty(await SeekAsync(reader, "UX_00", 3));
    }

    [Fact]
    public async Task AesEncryptedAccdb_EnforcesUniquenessAndMaintainsIndexes()
    {
        const string password = "wide-tdef";
        const DatabaseFormat format = DatabaseFormat.AceAccdb;
        string path = Path.Combine(Path.GetTempPath(), $"MultiPageTDef_{Guid.NewGuid():N}.accdb");
        this.tempFiles.Add(path);

        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(path, format, new AccessWriterOptions { UseLockFile = false }, this.ct))
        {
            await writer.CreateTableAsync(TableName, WideColumns(format), WideIndexes(format), this.ct);
        }

        await AccessWriter.EncryptAsync(path, password.AsMemory(), AccessEncryptionFormat.AccdbAesCfbWrapped, new AccessWriterOptions { UseLockFile = false }, this.ct);

        var writerOptions = new AccessWriterOptions { UseLockFile = false, Password = password.AsMemory() };
        await using (AccessWriter writer = await AccessWriter.OpenAsync(path, writerOptions, this.ct))
        {
            _ = await writer.InsertRowsAsync(TableName, [WideRow(format, 1), WideRow(format, 2), WideRow(format, 3)], this.ct);
            _ = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.InsertRowAsync(TableName, WideRow(format, 3, salt: 7), this.ct));
            Assert.Equal(1, await writer.DeleteRowsAsync(TableName, "C000", 2, this.ct));
        }

        await using FileStream stream = File.OpenRead(path);
        await this.AssertIndexesMatchRowsAsync(stream, format, expectedKeys: [1, 3], password);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        foreach (string path in this.tempFiles)
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
                // Best-effort cleanup.
            }
        }
    }

    private static int ColumnCountOf(DatabaseFormat format) => format == DatabaseFormat.Jet3Mdb ? 50 : 200;

    private static int IndexCountOf(DatabaseFormat format) => format == DatabaseFormat.Jet3Mdb ? 20 : 30;

    private static List<ColumnDefinition> WideColumns(DatabaseFormat format)
    {
        int columnCount = ColumnCountOf(format);
        var columns = new List<ColumnDefinition>(columnCount);
        for (int i = 0; i < columnCount; i++)
        {
            columns.Add(new ColumnDefinition(string.Create(CultureInfo.InvariantCulture, $"C{i:D3}"), typeof(int)));
        }

        return columns;
    }

    private static List<IndexDefinition> WideIndexes(DatabaseFormat format)
    {
        int indexCount = IndexCountOf(format);
        var indexes = new List<IndexDefinition>(indexCount) { new("UX_00", "C000") { IsUnique = true } };
        for (int i = 1; i < indexCount; i++)
        {
            indexes.Add(new IndexDefinition(
                string.Create(CultureInfo.InvariantCulture, $"IX_{i:D2}"),
                string.Create(CultureInfo.InvariantCulture, $"C{i:D3}")));
        }

        return indexes;
    }

    /// <summary>
    /// Builds a row whose <c>C000</c> is <paramref name="key"/>. Even columns
    /// derive a per-row value from the key; odd columns repeat every five keys
    /// so the non-unique indexes hold duplicates.
    /// </summary>
    /// <param name="format">The database format, which sets the column count.</param>
    /// <param name="key">The unique key stored in <c>C000</c>.</param>
    /// <param name="salt">An offset added to every other column, so a duplicate key can differ elsewhere.</param>
    private static object[] WideRow(DatabaseFormat format, int key, int salt = 0)
    {
        object[] row = new object[ColumnCountOf(format)];
        row[0] = key;
        for (int c = 1; c < row.Length; c++)
        {
            row[c] = (c % 2 == 0 ? key : key % 5) + (c * 10_000) + salt;
        }

        return row;
    }

    private static async ValueTask<List<object[]>> SeekAsync(AccessReader reader, string indexName, object key)
    {
        var rows = new List<object[]>();
        await foreach (object[] row in reader.SeekRowsAsync(TableName, indexName, [key], TestContext.Current.CancellationToken)
            .WithCancellation(TestContext.Current.CancellationToken))
        {
            rows.Add(row);
        }

        return rows;
    }

    private static async ValueTask<List<IndexEntry>> ReadAllLeafEntriesAsync(DatabaseFile db, IndexPageLayout layout, long rootPage, CancellationToken cancellationToken)
    {
        long pageNumber = rootPage;
        byte[] page = await db.ReadPageCopyAsync(pageNumber, cancellationToken);
        for (int depth = 0; page[0] == Constants.IndexLeafPage.PageTypeIntermediate; depth++)
        {
            Assert.True(depth < 32, $"Index tree rooted at page {rootPage} is deeper than 32 levels.");
            List<DecodedIntermediateEntry> children = IndexPageCodec.DecodeIntermediateEntries(layout, page, db.PageSizeBytes);
            Assert.NotEmpty(children);
            pageNumber = children[0].ChildPage;
            page = await db.ReadPageCopyAsync(pageNumber, cancellationToken);
        }

        var entries = new List<IndexEntry>();
        var visited = new HashSet<long>();
        while (true)
        {
            Assert.Equal(Constants.IndexLeafPage.PageTypeLeaf, page[0]);
            Assert.True(visited.Add(pageNumber), $"Leaf chain of the index rooted at page {rootPage} revisits page {pageNumber}.");
            entries.AddRange(IndexPageCodec.DecodeLeafEntries(layout, page, db.PageSizeBytes));

            pageNumber = IndexPageCodec.ReadNextPage(layout, page);
            if (pageNumber == 0)
            {
                return entries;
            }

            page = await db.ReadPageCopyAsync(pageNumber, cancellationToken);
        }
    }

    private static string EntryKey(byte[] key, long dataPage, int dataRow)
        => string.Create(CultureInfo.InvariantCulture, $"{Convert.ToHexString(key)}@{dataPage}:{dataRow}");

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream, AccessWriterOptions? options = null)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            options ?? new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(Stream stream, string? password = null)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false, Password = password.AsMemory() },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }

    private async ValueTask<MemoryStream> CreateWideTableStreamAsync(DatabaseFormat format)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            this.ct))
        {
            await writer.CreateTableAsync(TableName, WideColumns(format), WideIndexes(format), this.ct);
        }

        stream.Position = 0;
        return stream;
    }

    /// <summary>
    /// Asserts that the table holds exactly the rows keyed by
    /// <paramref name="expectedKeys"/>, that its TDEF really spans several
    /// pages, that every index keeps the key columns it was created with, and
    /// that every index B-tree (foreign-key ones included) holds one entry per
    /// live row, pointing at that row and carrying its encoded key.
    /// </summary>
    /// <param name="stream">The database stream or file.</param>
    /// <param name="format">The database format.</param>
    /// <param name="expectedKeys">The <c>C000</c> keys of the rows the table must hold.</param>
    /// <param name="password">The database password, when encrypted.</param>
    /// <param name="expectedForeignKeys">The number of foreign-key logical indexes the table carries.</param>
    private async ValueTask AssertIndexesMatchRowsAsync(
        Stream stream,
        DatabaseFormat format,
        int[] expectedKeys,
        string? password = null,
        int expectedForeignKeys = 0)
    {
        DataTable rows;
        await using (AccessReader reader = await OpenReaderAsync(stream, password))
        {
            rows = await reader.ReadDataTableAsync(TableName, cancellationToken: this.ct);
        }

        using (rows)
        {
            int[] actualKeys = [.. rows.Rows.Cast<DataRow>().Select(r => (int)r["C000"]).Order()];
            Assert.Equal(expectedKeys.Order(), actualKeys);

            stream.Position = 0;
            await using ReaderHarness harness = await ReaderHarness.OpenAsync(
                stream,
                new AccessReaderOptions { UseLockFile = false, Password = password.AsMemory() },
                leaveOpen: true,
                this.ct);
            DatabaseFile db = harness.Database;
            CatalogEntry? entry = await harness.GetCatalogEntryAsync(TableName, this.ct);
            Assert.NotNull(entry);

            byte[] firstTdefPage = await db.ReadPageCopyAsync(entry.TDefPage, this.ct);
            Assert.NotEqual(0, BitConverter.ToInt32(firstTdefPage, 4));

            TableDef tableDef = await db.ReadRequiredTableDefAsync(entry.TDefPage, TableName, this.ct);
            byte[]? td = await db.ReadTDefBytesAsync(entry.TDefPage, this.ct);
            Assert.NotNull(td);
            List<IndexMetadata> indexes = IndexCatalogReader.ReadMetadata(db.Profile, td, tableDef.Columns);
            Assert.Equal(IndexCountOf(format), indexes.Count(i => !i.IsForeignKey));
            Assert.Equal(expectedForeignKeys, indexes.Count(i => i.IsForeignKey));

            // Jet4 / ACE real indexes carry a used_pages pointer (row = real
            // index + 2 of the table's usage map) just before first_dp; it
            // must land in the descriptor, not in a neighbouring col_map.
            if (format != DatabaseFormat.Jet3Mdb)
            {
                int numCols = BinaryPrimitives.ReadUInt16LittleEndian(td.AsSpan(db.TDef.NumCols));
                int numRealIdx = BinaryPrimitives.ReadInt32LittleEndian(td.AsSpan(db.TDef.NumRealIdx));
                int realIdxDescStart = IndexCatalogReader.LocateRealIdxDescStart(db.Profile, td, numCols, numRealIdx);
                int usageMapPage = td[db.TDef.UsedPagesPage]
                    | (td[db.TDef.UsedPagesPage + 1] << 8)
                    | (td[db.TDef.UsedPagesPage + 2] << 16);
                foreach (int realIdxNum in indexes.Select(i => i.RealIndexNumber).Distinct())
                {
                    Assert.True(db.IndexLayoutInfo.TryReadRealIdxSlot(td, realIdxDescStart, realIdxNum, out RealIdxSlot slot));
                    int usedPagesOffset = slot.FirstDpOffset - 4;
                    Assert.Equal(realIdxNum + 2, td[usedPagesOffset]);
                    Assert.Equal(usageMapPage, td[usedPagesOffset + 1] | (td[usedPagesOffset + 2] << 8) | (td[usedPagesOffset + 3] << 16));
                }
            }

            List<RowLocation> locations = await db.GetLiveRowLocationsAsync(entry.TDefPage, this.ct);
            Assert.Equal(rows.Rows.Count, locations.Count);

            IndexPageLayout layout = JetFormat.ForNewDatabase(format).IndexPage;
            foreach (IndexMetadata index in indexes)
            {
                IndexColumnReference keyColumn = Assert.Single(index.Columns);
                if (!index.IsForeignKey)
                {
                    string expectedColumn = index.Name == "UX_00" ? "C000" : $"C0{index.Name[3..]}";
                    Assert.Equal(expectedColumn, keyColumn.Name);
                }

                var expected = new List<string>(locations.Count);
                for (int r = 0; r < locations.Count; r++)
                {
                    byte[] key = IndexKeyEncoder.EncodeEntry(ColumnType.LongIntegerType, rows.Rows[r][keyColumn.Name], keyColumn.IsAscending);
                    expected.Add(EntryKey(key, locations[r].PageNumber, locations[r].RowIndex));
                }

                Assert.True(index.FirstDp > 0, $"Index '{index.Name}' has no root page.");
                List<IndexEntry> leafEntries = await ReadAllLeafEntriesAsync(db, layout, index.FirstDp, this.ct);
                List<string> actual = [.. leafEntries.Select(e => EntryKey(e.Key, e.DataPage, e.DataRow))];

                expected.Sort(StringComparer.Ordinal);
                actual.Sort(StringComparer.Ordinal);
                Assert.True(
                    expected.SequenceEqual(actual),
                    $"Index '{index.Name}' holds {actual.Count} entries that do not match the {expected.Count} live rows.");
            }
        }
    }
}
