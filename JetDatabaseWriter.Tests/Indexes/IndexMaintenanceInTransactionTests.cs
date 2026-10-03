namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
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
using JetDatabaseWriter.ValueDecoding;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Index maintenance after a transaction has appended pages. Pages appended
/// inside a transaction live in the journal, past the physical end of the
/// file, until commit. Index splits and rebuilds that number their new pages,
/// and bounds checks that validate page numbers, must use the journal-aware
/// end of file (<see cref="DatabaseFile.PageCount"/>); the physical one names
/// pages the transaction has already used. The full rebuild must also see the
/// rows a transaction inserted into a table it created. Each scenario runs
/// with no transaction, inside an explicit transaction, and with
/// <see cref="AccessWriterOptions.UseTransactionalWrites"/>, and must leave
/// the same table and indexes in every mode, both through the writer's own
/// pages before commit and after reopening.
/// </summary>
/// <param name="cache">Caches the Access-authored fixtures the foreign-key cases copy.</param>
public sealed class IndexMaintenanceInTransactionTests(DatabaseCache cache) : IClassFixture<DatabaseCache>
{
    private const string TableName = "T";
    private const string PrimaryKeyName = "PK";
    private const string IdColumn = "Id";
    private const string ChildTable = "C";
    private const string ParentIdColumn = "ParentId";
    private const string Password = "Secret1!";

    private static readonly AccessReaderOptions ReaderOptions = new() { UseLockFile = false };

    /// <summary>
    /// A capacity-0 page cache, so reads through a writer's DatabaseFile see
    /// the pages its transaction has pending rather than a stale cached copy.
    /// </summary>
    private static readonly AccessReaderOptions UncachedReaderOptions = new() { UseLockFile = false, PageCacheSize = 0 };

    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, false, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, true)]
    [InlineData(DatabaseFormat.AceAccdb, false, false)]
    [InlineData(DatabaseFormat.AceAccdb, true, false)]
    [InlineData(DatabaseFormat.AceAccdb, false, true)]
    public async Task SingleInserts_IntoOneKeyRangeAfterAppendedPages_KeepPrimaryKeyConsistent(
        DatabaseFormat format,
        bool explicitTransaction,
        bool transactionalWrites)
    {
        var expectedIds = new List<int>();
        await using MemoryStream stream = await CreateSeededDatabaseAsync(format, baseRows: 4000, keyStep: 1000, expectedIds);

        await using (AccessWriter writer = await OpenWriterAsync(stream, transactionalWrites))
        {
            await using JetTransaction? tx = explicitTransaction ? await writer.BeginTransactionAsync(this.ct) : null;
            await this.InsertTailThenSingleKeysAsync(writer, expectedIds);
            await AssertTableAndPrimaryKeyMatchAsync(FacadeInternals.Database(writer), expectedIds, this.ct);

            if (tx is not null)
            {
                await tx.CommitAsync(this.ct);
            }
        }

        await AssertReopenedTableAndPrimaryKeyMatchAsync(stream, expectedIds, this.ct);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task SingleInserts_IntoOneKeyRangeAfterAppendedPages_RolledBack_LeaveSeededTable(DatabaseFormat format)
    {
        var seededIds = new List<int>();
        await using MemoryStream stream = await CreateSeededDatabaseAsync(format, baseRows: 4000, keyStep: 1000, seededIds);

        await using (AccessWriter writer = await OpenWriterAsync(stream, transactionalWrites: false))
        {
            await using JetTransaction tx = await writer.BeginTransactionAsync(this.ct);
            var pendingIds = new List<int>(seededIds);
            await this.InsertTailThenSingleKeysAsync(writer, pendingIds);
            await AssertTableAndPrimaryKeyMatchAsync(FacadeInternals.Database(writer), pendingIds, this.ct);

            await tx.RollbackAsync(this.ct);
            await AssertTableAndPrimaryKeyMatchAsync(FacadeInternals.Database(writer), seededIds, this.ct);
        }

        await AssertReopenedTableAndPrimaryKeyMatchAsync(stream, seededIds, this.ct);
    }

    [Fact]
    public async Task SingleInserts_IntoOneKeyRangeAfterAppendedPages_InTransactionOnEncryptedFile_KeepPrimaryKeyConsistent()
    {
        // The journal holds plaintext; commit encrypts each page as it is
        // written, including the pages the index split reserved past the
        // physical end of file.
        string path = Path.Combine(Path.GetTempPath(), $"IndexMaintenanceInTransaction_{Guid.NewGuid():N}.accdb");
        try
        {
            var expectedIds = new List<int>();
            await using (MemoryStream seeded = await CreateSeededDatabaseAsync(DatabaseFormat.AceAccdb, baseRows: 4000, keyStep: 1000, expectedIds))
            {
                await File.WriteAllBytesAsync(path, seeded.ToArray(), this.ct);
            }

            await AccessWriter.EncryptAsync(
                path,
                Password.AsMemory(),
                AccessEncryptionFormat.AccdbAesCfbWrapped,
                new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false },
                this.ct);

            await using (AccessWriter writer = await AccessWriter.OpenAsync(
                path,
                new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false, Password = Password.AsMemory() },
                this.ct))
            {
                await using JetTransaction tx = await writer.BeginTransactionAsync(this.ct);
                await this.InsertTailThenSingleKeysAsync(writer, expectedIds);
                await AssertTableAndPrimaryKeyMatchAsync(FacadeInternals.Database(writer), expectedIds, this.ct);
                await tx.CommitAsync(this.ct);
            }

            await using ReaderHarness reopened = await ReaderHarness.OpenAsync(
                path,
                new AccessReaderOptions { UseLockFile = false, Password = Password.AsMemory() },
                this.ct);
            await AssertTableAndPrimaryKeyMatchAsync(reopened.Database, expectedIds, this.ct);
        }
        finally
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
            }
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, false, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, true)]
    [InlineData(DatabaseFormat.AceAccdb, false, false)]
    [InlineData(DatabaseFormat.AceAccdb, true, false)]
    [InlineData(DatabaseFormat.AceAccdb, false, true)]
    public async Task BatchInsert_BetweenEveryExistingKey_KeepsPrimaryKeyConsistent(
        DatabaseFormat format,
        bool explicitTransaction,
        bool transactionalWrites)
    {
        // One batch with a key between every pair of existing keys, so
        // every leaf of the tree splits. Its rows are written, appending
        // data pages, before the index is maintained.
        const int baseRows = 4000;
        var expectedIds = new List<int>();
        await using MemoryStream stream = await CreateSeededDatabaseAsync(format, baseRows, keyStep: 10, expectedIds);

        await using (AccessWriter writer = await OpenWriterAsync(stream, transactionalWrites))
        {
            await using JetTransaction? tx = explicitTransaction ? await writer.BeginTransactionAsync(this.ct) : null;

            var middle = new List<object?[]>(baseRows);
            for (int i = 0; i < baseRows; i++)
            {
                int id = (i * 10) + 5;
                middle.Add([id, new string('m', 150)]);
                expectedIds.Add(id);
            }

            _ = await writer.InsertRowsAsync(TableName, middle, this.ct);
            await AssertTableAndPrimaryKeyMatchAsync(FacadeInternals.Database(writer), expectedIds, this.ct);

            if (tx is not null)
            {
                await tx.CommitAsync(this.ct);
            }
        }

        await AssertReopenedTableAndPrimaryKeyMatchAsync(stream, expectedIds, this.ct);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, true, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, false, true)]
    [InlineData(DatabaseFormat.Jet3Mdb, false, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false)]
    [InlineData(DatabaseFormat.AceAccdb, true, false)]
    [InlineData(DatabaseFormat.AceAccdb, false, true)]
    [InlineData(DatabaseFormat.AceAccdb, false, false)]
    public async Task RandomSingleInserts_IntoTableCreatedInTransaction_KeepPrimaryKeyConsistent(
        DatabaseFormat format,
        bool explicitTransaction,
        bool transactionalWrites)
    {
        // The review's scenario: a table created in the transaction, then
        // single inserts in random key order. A full rebuild that cannot
        // see the transaction's rows used to drop most of the primary key.
        await using MemoryStream stream = await CreateEmptyDatabaseAsync(format);
        List<int> expectedIds = DistinctRandomIds(1500);

        await using (AccessWriter writer = await OpenWriterAsync(stream, transactionalWrites))
        {
            await using JetTransaction? tx = explicitTransaction ? await writer.BeginTransactionAsync(this.ct) : null;
            await CreateKeyedTableAsync(writer, this.ct);
            foreach (int id in expectedIds)
            {
                await writer.InsertRowAsync(TableName, [id, "r"], this.ct);
            }

            await AssertTableAndPrimaryKeyMatchAsync(FacadeInternals.Database(writer), expectedIds, this.ct);

            if (tx is not null)
            {
                await tx.CommitAsync(this.ct);
            }
        }

        await AssertReopenedTableAndPrimaryKeyMatchAsync(stream, expectedIds, this.ct);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, true, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, false, true)]
    [InlineData(DatabaseFormat.Jet3Mdb, false, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false)]
    [InlineData(DatabaseFormat.AceAccdb, true, false)]
    [InlineData(DatabaseFormat.AceAccdb, false, true)]
    [InlineData(DatabaseFormat.AceAccdb, false, false)]
    public async Task SingleInserts_IntoChildWithForeignKeyIndex_TakeFullRebuildAndKeepIndexesConsistent(
        DatabaseFormat format,
        bool explicitTransaction,
        bool transactionalWrites)
    {
        // A foreign-key logical index keeps the table on the full rebuild
        // for every insert (bail C1c), so each insert rebuilds the primary
        // key and the foreign-key index from a snapshot of the table the
        // transaction created.
        (MemoryStream stream, string parentTable, string parentKey, List<int> parentIds) = await this.CreateParentDatabaseAsync(format);
        await using MemoryStream disposeStream = stream;
        List<int> expectedIds = DistinctRandomIds(400);
        var expectedParentIds = new Dictionary<int, int>(expectedIds.Count);

        await using (AccessWriter writer = await OpenWriterAsync(stream, transactionalWrites))
        {
            await using JetTransaction? tx = explicitTransaction ? await writer.BeginTransactionAsync(this.ct) : null;
            await writer.CreateTableAsync(
                ChildTable,
                [
                    new ColumnDefinition(IdColumn, typeof(int)),
                    new ColumnDefinition(ParentIdColumn, typeof(int)),
                    new ColumnDefinition("Pad", typeof(string), maxLength: 100),
                ],
                [new IndexDefinition(PrimaryKeyName, IdColumn) { IsPrimaryKey = true }],
                this.ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("ParentChild", parentTable, parentKey, ChildTable, ParentIdColumn), this.ct);

            var services = (WriterServices)FacadeInternals.ReadPrivateField(writer, "services")!;
            foreach (int id in expectedIds)
            {
                int parentId = parentIds[id % parentIds.Count];
                await writer.InsertRowAsync(ChildTable, [id, parentId, new string('p', 60)], this.ct);
                expectedParentIds.Add(id, parentId);
                if (expectedParentIds.Count == 1)
                {
                    Assert.StartsWith("C1c", services.Indexes.LastIncrementalBail, StringComparison.Ordinal);
                }
            }

            await AssertChildIndexesMatchAsync(FacadeInternals.Database(writer), expectedParentIds, this.ct);

            if (tx is not null)
            {
                await tx.CommitAsync(this.ct);
            }
        }

        stream.Position = 0;
        await using ReaderHarness reopened = await ReaderHarness.OpenAsync(stream, ReaderOptions, cancellationToken: this.ct);
        await AssertChildIndexesMatchAsync(reopened.Database, expectedParentIds, this.ct);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task FullRebuild_InExplicitTransaction_SeesRowsInsertedIntoTableCreatedInIt(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateEmptyDatabaseAsync(format);
        List<int> expectedIds = DistinctRandomIds(1500);

        await using (AccessWriter writer = await OpenWriterAsync(stream, transactionalWrites: false))
        {
            await using JetTransaction tx = await writer.BeginTransactionAsync(this.ct);
            await CreateKeyedTableAsync(writer, this.ct);
            _ = await writer.InsertRowsAsync(TableName, [.. expectedIds.Select(id => new object?[] { id, "r" })], this.ct);

            var services = (WriterServices)FacadeInternals.ReadPrivateField(writer, "services")!;
            ResolvedTable resolved = await services.Catalog.ResolveRequiredTableAsync(TableName, this.ct);
            await services.Indexes.MaintainIndexesAsync(resolved.Entry.TDefPage, resolved.Definition, TableName, this.ct);
            await AssertTableAndPrimaryKeyMatchAsync(FacadeInternals.Database(writer), expectedIds, this.ct);

            await tx.CommitAsync(this.ct);
        }

        await AssertReopenedTableAndPrimaryKeyMatchAsync(stream, expectedIds, this.ct);
    }

    private static async Task<MemoryStream> CreateEmptyDatabaseAsync(DatabaseFormat format)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
        }

        return stream;
    }

    private static ValueTask CreateKeyedTableAsync(AccessWriter writer, CancellationToken cancellationToken)
        => writer.CreateTableAsync(
            TableName,
            [
                new ColumnDefinition(IdColumn, typeof(int)),
                new ColumnDefinition("Pad", typeof(string), maxLength: 200),
            ],
            [new IndexDefinition(PrimaryKeyName, IdColumn) { IsPrimaryKey = true }],
            cancellationToken);

    /// <summary>
    /// Returns <paramref name="count"/> distinct ids in [1, 10,000,000) in a
    /// fixed pseudo-random order, so single inserts land all over the key
    /// space and every run sees the same sequence.
    /// </summary>
    /// <param name="count">The number of ids.</param>
    private static List<int> DistinctRandomIds(int count)
    {
#pragma warning disable CA5394 // Deterministic test keys; nothing here needs a secure generator.
        var random = new Random(20261003);
        var seen = new HashSet<int>(count);
        var ids = new List<int>(count);
        while (ids.Count < count)
        {
            int id = random.Next(1, 10_000_000);
            if (seen.Add(id))
            {
                ids.Add(id);
            }
        }
#pragma warning restore CA5394

        return ids;
    }

    private static async Task<MemoryStream> CreateSeededDatabaseAsync(DatabaseFormat format, int baseRows, int keyStep, List<int> expectedIds)
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false },
            leaveOpen: true,
            cancellationToken))
        {
            await CreateKeyedTableAsync(writer, cancellationToken);

            var rows = new List<object?[]>(baseRows);
            for (int i = 0; i < baseRows; i++)
            {
                int id = i * keyStep;
                rows.Add([id, new string('p', 150)]);
                expectedIds.Add(id);
            }

            _ = await writer.InsertRowsAsync(TableName, rows, cancellationToken);
        }

        return stream;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream, bool transactionalWrites)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false, UseTransactionalWrites = transactionalWrites },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }

    private static async Task AssertReopenedTableAndPrimaryKeyMatchAsync(MemoryStream stream, List<int> expectedIds, CancellationToken cancellationToken)
    {
        await using ReaderHarness reopened = await ReaderHarness.OpenAsync(stream, ReaderOptions, cancellationToken: cancellationToken);
        await AssertTableAndPrimaryKeyMatchAsync(reopened.Database, expectedIds, cancellationToken);
    }

    private static Task AssertTableAndPrimaryKeyMatchAsync(DatabaseFile db, List<int> expectedIds, CancellationToken cancellationToken)
        => AssertTableAndUniqueIndexMatchAsync(db, TableName, PrimaryKeyName, IdColumn, expectedIds, cancellationToken);

    /// <summary>
    /// Checks the child table of the foreign-key cases: its primary key as
    /// <see cref="AssertTableAndUniqueIndexMatchAsync"/> does, and its
    /// foreign-key index entry by entry against the live rows.
    /// </summary>
    /// <param name="db">The database file to read, a writer's or a reopened reader's.</param>
    /// <param name="expectedParentIds">Every child id the table must hold, with its parent id.</param>
    /// <param name="cancellationToken">A token used to cancel the reads.</param>
    private static async Task AssertChildIndexesMatchAsync(DatabaseFile db, Dictionary<int, int> expectedParentIds, CancellationToken cancellationToken)
    {
        await AssertTableAndUniqueIndexMatchAsync(db, ChildTable, PrimaryKeyName, IdColumn, [.. expectedParentIds.Keys], cancellationToken);

        using var services = new ReaderServices(db, UncachedReaderOptions);
        CatalogEntry? entry = await services.TableCatalog.GetCatalogEntryAsync(ChildTable, cancellationToken);
        Assert.NotNull(entry);
        TableDef? tableDef = await db.ReadTableDefAsync(entry.TDefPage, cancellationToken);
        Assert.NotNull(tableDef);
        IndexMetadata foreignKey = Assert.Single(
            await services.Indexes.ListIndexesAsync(ChildTable, cancellationToken),
            candidate => candidate.Kind == IndexKind.ForeignKey);

        List<(int Key, long DataPage, byte DataRow)> entries = await ReadIndexLeafEntriesAsync(
            db, entry.TDefPage, tableDef, foreignKey.FirstDp, ParentIdColumn, unique: false, cancellationToken);
        Assert.Equal(expectedParentIds.Values.Order(), entries.Select(e => e.Key));

        List<RowLocation> liveRows = await db.GetLiveRowLocationsAsync(entry.TDefPage, cancellationToken);
        Assert.Equal(
            liveRows.Select(row => (row.PageNumber, row.RowIndex)).Order(),
            entries.Select(e => (e.DataPage, (int)e.DataRow)).Order());
    }

    /// <summary>
    /// Scans <paramref name="tableName"/> and walks the leaf chain of its
    /// unique index <paramref name="indexName"/> through <paramref name="db"/>,
    /// which reads a writer's pending transaction pages when one is active.
    /// Every expected key must appear once in the scan and once in the index,
    /// and every leaf entry must point at a live row of the table holding the
    /// entry's key. Formats that support seeks also seek every key through
    /// the intermediate pages.
    /// </summary>
    /// <param name="db">The database file to read, a writer's or a reopened reader's.</param>
    /// <param name="tableName">The table.</param>
    /// <param name="indexName">The unique index on <paramref name="keyColumn"/>.</param>
    /// <param name="keyColumn">The Long Integer key column.</param>
    /// <param name="expectedKeys">Every key the table must hold.</param>
    /// <param name="cancellationToken">A token used to cancel the reads.</param>
    private static async Task AssertTableAndUniqueIndexMatchAsync(
        DatabaseFile db,
        string tableName,
        string indexName,
        string keyColumn,
        List<int> expectedKeys,
        CancellationToken cancellationToken)
    {
        int[] expected = [.. expectedKeys.Order()];
        using var services = new ReaderServices(db, UncachedReaderOptions);

        CatalogEntry? entry = await services.TableCatalog.GetCatalogEntryAsync(tableName, cancellationToken);
        Assert.NotNull(entry);
        TableDef? tableDef = await db.ReadTableDefAsync(entry.TDefPage, cancellationToken);
        Assert.NotNull(tableDef);
        int keyOrdinal = tableDef.FindColumnIndex(keyColumn);

        var scannedKeys = new List<int>(expected.Length);
        await foreach (object[] row in services.Tables.Rows(tableName, progress: null, cancellationToken))
        {
            scannedKeys.Add((int)row[keyOrdinal]);
        }

        Assert.Equal(expected, scannedKeys.Order());

        IndexMetadata index = Assert.Single(
            await services.Indexes.ListIndexesAsync(tableName, cancellationToken),
            candidate => candidate.Name == indexName);

        List<(int Key, long DataPage, byte DataRow)> entries = await ReadIndexLeafEntriesAsync(
            db, entry.TDefPage, tableDef, index.FirstDp, keyColumn, unique: true, cancellationToken);
        Assert.Equal(expected, entries.Select(e => e.Key));

        if (db.Format == DatabaseFormat.Jet3Mdb)
        {
            return;
        }

        int seekMisses = 0;
        foreach (int key in expected)
        {
            int hits = 0;
            await foreach (object[] row in services.Indexes.SeekRowsAsync(tableName, indexName, [key], cancellationToken))
            {
                Assert.Equal(key, (int)row[keyOrdinal]);
                hits++;
            }

            if (hits != 1)
            {
                seekMisses++;
            }
        }

        Assert.Equal(0, seekMisses);
    }

    /// <summary>
    /// Walks the leaf chain of a single-column Long Integer index from its
    /// root and returns each entry's decoded key and row pointer, in chain
    /// order. Asserts that every page belongs to the table, that every entry
    /// points at a live row of the table whose key column encodes to the
    /// entry's key, and that keys are in order (strictly, for a unique index).
    /// </summary>
    /// <param name="db">The database file to read.</param>
    /// <param name="tdefPage">The table's TDEF page.</param>
    /// <param name="tableDef">The table definition.</param>
    /// <param name="rootPage">The index root page.</param>
    /// <param name="keyColumn">The indexed column.</param>
    /// <param name="unique">Whether keys must be strictly increasing.</param>
    /// <param name="cancellationToken">A token used to cancel the reads.</param>
    private static async Task<List<(int Key, long DataPage, byte DataRow)>> ReadIndexLeafEntriesAsync(
        DatabaseFile db,
        long tdefPage,
        TableDef tableDef,
        long rootPage,
        string keyColumn,
        bool unique,
        CancellationToken cancellationToken)
    {
        var layout = IndexPageLayout.ForFormat(db.Format);
        int keyOrdinal = tableDef.FindColumnIndex(keyColumn);
        ColumnType keyType = tableDef.Columns[keyOrdinal].Type;
        var decodePlan = RowDecodePlan.CreatePartial(tableDef, [keyOrdinal]);

        long current = rootPage;
        for (int depth = 0; ; depth++)
        {
            Assert.True(depth < 16, $"Index descent from root {rootPage} did not reach a leaf.");
            byte[] page = await db.ReadPageCopyAsync(current, cancellationToken);
            Assert.True(Ri32(page, 4) == tdefPage, $"Index page {current} is owned by page {Ri32(page, 4)}, not the table's TDEF {tdefPage}.");
            if (page[0] == Constants.IndexLeafPage.PageTypeLeaf)
            {
                break;
            }

            Assert.True(page[0] == Constants.IndexLeafPage.PageTypeIntermediate, $"Page {current} has type 0x{page[0]:X2}, not an index page.");
            List<DecodedIntermediateEntry> children = IndexPageCodec.DecodeIntermediateEntries(layout, page, db.PageSizeBytes);
            Assert.NotEmpty(children);
            current = children[0].ChildPage;
        }

        var dataPages = new Dictionary<long, (byte[] Page, RowBound[] Rows)>();
        var entries = new List<(int Key, long DataPage, byte DataRow)>();
        byte[]? previousKey = null;
        int leafBudget = 10_000;
        while (current != 0)
        {
            Assert.True(--leafBudget > 0, "Index leaf chain does not end.");
            byte[] leaf = await db.ReadPageCopyAsync(current, cancellationToken);
            Assert.True(leaf[0] == Constants.IndexLeafPage.PageTypeLeaf, $"Leaf chain reached page {current} of type 0x{leaf[0]:X2}.");
            Assert.True(Ri32(leaf, 4) == tdefPage, $"Leaf page {current} is owned by page {Ri32(leaf, 4)}, not the table's TDEF {tdefPage}.");

            foreach (IndexEntry indexEntry in IndexPageCodec.DecodeLeafEntries(layout, leaf, db.PageSizeBytes))
            {
                if (!dataPages.TryGetValue(indexEntry.DataPage, out (byte[] Page, RowBound[] Rows) data))
                {
                    byte[] dataPage = await db.ReadPageCopyAsync(indexEntry.DataPage, cancellationToken);
                    Assert.True(
                        dataPage[0] == Constants.PageTypes.Data && Ri32(dataPage, db.DataPage.TDefOff) == tdefPage,
                        $"Index entry points at page {indexEntry.DataPage}, which is not a data page of the table.");
                    data = (dataPage, db.ComputeRowDirectory(dataPage));
                    dataPages.Add(indexEntry.DataPage, data);
                }

                RowBound rowBound = Array.Find(data.Rows, row => row.RowIndex == indexEntry.DataRow);
                Assert.True(rowBound.RowSize > 0, $"Index entry points at row {indexEntry.DataRow} of page {indexEntry.DataPage}, which is not a live row.");

                object?[] values = new object?[1];
                Assert.True(decodePlan.TryDecodePartialColumns(db, data.Page, rowBound.RowStart, rowBound.RowSize, values));
                int key = Assert.IsType<int>(values[0]);
                Assert.Equal(IndexKeyEncoder.EncodeEntry(keyType, key), indexEntry.Key);
                int order = previousKey is null ? -1 : IndexPageCodec.CompareKeyBytes(previousKey, indexEntry.Key);
                Assert.True(unique ? order < 0 : order <= 0, $"Leaf page {current} holds key {key} out of order.");
                previousKey = indexEntry.Key;
                entries.Add((key, indexEntry.DataPage, indexEntry.DataRow));
            }

            current = IndexPageCodec.ReadNextPage(layout, leaf);
        }

        return entries;
    }

    /// <summary>
    /// Returns a database holding a parent table for the foreign-key cases:
    /// on Jet3 and Jet4, a copy of the Access-authored indexTestV1997.mdb or
    /// indexTestV2000.mdb, whose Table2 is keyed on <c>id</c> (writer-created
    /// .mdb files have no <c>MSysRelationships</c>); on ACCDB, a fresh file
    /// with a ten-row parent keyed on <c>Id</c>.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>The stream, the parent table and key column, and the parent's key values.</returns>
    private async Task<(MemoryStream Stream, string ParentTable, string ParentKey, List<int> ParentIds)> CreateParentDatabaseAsync(DatabaseFormat format)
    {
        if (format != DatabaseFormat.AceAccdb)
        {
            string path = format == DatabaseFormat.Jet3Mdb ? TestDatabases.IndexTestV1997 : TestDatabases.IndexTestV2000;
            MemoryStream fixture = await cache.CopyToStreamAsync(path, this.ct);
            var fixtureIds = new List<int>();
            await using (AccessReader reader = await AccessReader.OpenAsync(fixture, ReaderOptions, leaveOpen: true, this.ct))
            {
                await foreach (object[] row in reader.Rows("Table2", cancellationToken: this.ct))
                {
                    fixtureIds.Add(Convert.ToInt32(row[0], System.Globalization.CultureInfo.InvariantCulture));
                }
            }

            Assert.NotEmpty(fixtureIds);
            return (fixture, "Table2", "id", fixtureIds);
        }

        MemoryStream stream = await CreateEmptyDatabaseAsync(format);
        var parentIds = new List<int>();
        await using (AccessWriter writer = await OpenWriterAsync(stream, transactionalWrites: false))
        {
            await writer.CreateTableAsync(
                "P",
                [new ColumnDefinition(IdColumn, typeof(int))],
                [new IndexDefinition(PrimaryKeyName, IdColumn) { IsPrimaryKey = true }],
                this.ct);
            for (int id = 1; id <= 10; id++)
            {
                await writer.InsertRowAsync("P", [id], this.ct);
                parentIds.Add(id);
            }
        }

        return (stream, "P", IdColumn, parentIds);
    }

    private async Task InsertTailThenSingleKeysAsync(AccessWriter writer, List<int> expectedIds)
    {
        // A batch whose rows need new data pages, then single inserts
        // packed into one key range of the multi-level primary key, so the
        // target leaf splits several times after the data-page appends.
        var tail = new List<object?[]>();
        for (int i = 0; i < 200; i++)
        {
            int id = 10_000_000 + i;
            tail.Add([id, new string('t', 190)]);
            expectedIds.Add(id);
        }

        _ = await writer.InsertRowsAsync(TableName, tail, this.ct);

        for (int i = 1; i <= 600; i++)
        {
            int id = 2_000_000 + i;
            await writer.InsertRowAsync(TableName, [id, "m"], this.ct);
            expectedIds.Add(id);
        }
    }
}
