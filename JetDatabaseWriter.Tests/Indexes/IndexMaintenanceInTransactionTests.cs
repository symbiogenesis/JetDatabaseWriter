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
/// pages the transaction has already used. Each scenario runs with no
/// transaction, inside an explicit transaction, and with
/// <see cref="AccessWriterOptions.UseTransactionalWrites"/>, and must leave
/// the same table and primary-key index in every mode, both through the
/// writer's own pages before commit and after reopening.
/// </summary>
public sealed class IndexMaintenanceInTransactionTests
{
    private const string TableName = "T";
    private const string PrimaryKeyName = "PK";
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
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Pad", typeof(string), maxLength: 200),
                ],
                [new IndexDefinition(PrimaryKeyName, "Id") { IsPrimaryKey = true }],
                cancellationToken);

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

    /// <summary>
    /// Scans the table and walks the primary key's leaf chain through
    /// <paramref name="db"/>, which reads a writer's pending transaction
    /// pages when one is active. Every expected id must appear once in the
    /// scan and once in the index, and every leaf entry must point at a live
    /// row of the table holding the entry's key. Formats that support seeks
    /// also seek every id through the intermediate pages.
    /// </summary>
    /// <param name="db">The database file to read, a writer's or a reopened reader's.</param>
    /// <param name="expectedIds">Every id the table must hold.</param>
    /// <param name="cancellationToken">A token used to cancel the reads.</param>
    private static async Task AssertTableAndPrimaryKeyMatchAsync(DatabaseFile db, List<int> expectedIds, CancellationToken cancellationToken)
    {
        int[] expected = [.. expectedIds.Order()];
        using var services = new ReaderServices(db, UncachedReaderOptions);

        var scannedIds = new List<int>(expected.Length);
        await foreach (object[] row in services.Tables.Rows(TableName, progress: null, cancellationToken))
        {
            scannedIds.Add((int)row[0]);
        }

        Assert.Equal(expected, scannedIds.Order());

        CatalogEntry? entry = await services.TableCatalog.GetCatalogEntryAsync(TableName, cancellationToken);
        Assert.NotNull(entry);
        TableDef? tableDef = await db.ReadTableDefAsync(entry.TDefPage, cancellationToken);
        Assert.NotNull(tableDef);
        IndexMetadata primaryKey = Assert.Single(
            await services.Indexes.ListIndexesAsync(TableName, cancellationToken),
            index => index.Name == PrimaryKeyName);

        List<int> indexedIds = await ReadPrimaryKeyLeafIdsAsync(db, entry.TDefPage, tableDef, primaryKey.FirstDp, cancellationToken);
        Assert.Equal(expected, indexedIds);

        if (db.Format == DatabaseFormat.Jet3Mdb)
        {
            return;
        }

        int seekMisses = 0;
        foreach (int id in expected)
        {
            int hits = 0;
            await foreach (object[] row in services.Indexes.SeekRowsAsync(TableName, PrimaryKeyName, [id], cancellationToken))
            {
                Assert.Equal(id, (int)row[0]);
                hits++;
            }

            if (hits != 1)
            {
                seekMisses++;
            }
        }

        Assert.Equal(0, seekMisses);
    }

    private static async Task<List<int>> ReadPrimaryKeyLeafIdsAsync(DatabaseFile db, long tdefPage, TableDef tableDef, long rootPage, CancellationToken cancellationToken)
    {
        var layout = IndexPageLayout.ForFormat(db.Format);
        int idOrdinal = tableDef.FindColumnIndex("Id");
        ColumnType idType = tableDef.Columns[idOrdinal].Type;
        var decodePlan = RowDecodePlan.CreatePartial(tableDef, [idOrdinal]);

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
        var ids = new List<int>();
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
                    data = (dataPage, db.ComputeLiveRowBoundsArray(dataPage));
                    dataPages.Add(indexEntry.DataPage, data);
                }

                RowBound rowBound = Array.Find(data.Rows, row => row.RowIndex == indexEntry.DataRow);
                Assert.True(rowBound.RowSize > 0, $"Index entry points at row {indexEntry.DataRow} of page {indexEntry.DataPage}, which is not a live row.");

                object?[] values = new object?[1];
                Assert.True(decodePlan.TryDecodePartialColumns(db, data.Page, rowBound.RowStart, rowBound.RowSize, values));
                int id = Assert.IsType<int>(values[0]);
                Assert.Equal(IndexKeyEncoder.EncodeEntry(idType, id), indexEntry.Key);
                Assert.True(previousKey is null || IndexPageCodec.CompareKeyBytes(previousKey, indexEntry.Key) < 0, $"Leaf page {current} holds key {id} out of order.");
                previousKey = indexEntry.Key;
                ids.Add(id);
            }

            current = IndexPageCodec.ReadNextPage(layout, leaf);
        }

        return ids;
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
