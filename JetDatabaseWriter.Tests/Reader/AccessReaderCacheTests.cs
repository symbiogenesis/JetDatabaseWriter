namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

public sealed class AccessReaderCacheTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private const string AlphaRowsTable = "AlphaRows";
    private const string BetaRowsTable = "BetaRows";
    private const string PageCacheFieldName = "pageCache";
    private const string RowBoundsCacheFieldName = "rowBoundsCache";
    private const string CatalogCacheFieldName = "userTables";
    private const string OwnedDataPageIndexFieldName = "ownedDataPageIndex";
    private const string AsyncLazyValueFieldName = "value";

    [Fact]
    public async Task OpenAsync_WithZeroPageCacheSize_DoesNotAllocateCache()
    {
        byte[] bytes = await db.GetFileAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        var options = new AccessReaderOptions
        {
            PageCacheSize = 0,
            UseLockFile = false,
        };

        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            options,
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        Assert.Equal(0, reader.PageCacheSize);
        Assert.Null(ReadPrivateField(PageCacheOf(reader), PageCacheFieldName));
        Assert.Null(ReadPrivateField(PageCacheOf(reader), RowBoundsCacheFieldName));
        Assert.NotEmpty(await reader.ListTablesAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Rows_WithTinyPageCache_EvictsDuringLargeTableScan()
    {
        const string tableName = "LargeRows";
        const int rowCount = 320;
        await using MemoryStream stream = await CreateCacheExerciseDatabaseAsync(
            new List<(string Name, int RowCount, string Prefix)>
            {
                (tableName, rowCount, "L"),
            },
            TestContext.Current.CancellationToken);
        var options = new AccessReaderOptions
        {
            PageCacheSize = 3,
            UseLockFile = false,
        };

        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            options,
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        int actualRows = await CountRowsAsync(reader, tableName, TestContext.Current.CancellationToken);

        LruCache<long, byte[]> pageCache = ReadRequiredPrivateField<LruCache<long, byte[]>>(PageCacheOf(reader), PageCacheFieldName);
        LruCache<long, RowBound[]> rowBoundsCache = ReadRequiredPrivateField<LruCache<long, RowBound[]>>(PageCacheOf(reader), RowBoundsCacheFieldName);
        Assert.Equal(rowCount, actualRows);
        Assert.Equal(options.PageCacheSize, pageCache.Count);
        Assert.True(pageCache.Misses > pageCache.Count);
        Assert.Equal(options.PageCacheSize, rowBoundsCache.Count);
        Assert.True(rowBoundsCache.Misses > rowBoundsCache.Count);
    }

    [Fact]
    public async Task Rows_WhenLongValueChainEvictsCurrentDataPage_ReturnsEveryRow()
    {
        const string tableName = "MemoRows";
        const int smallRowCount = 29;

        // CJK text defeats Unicode compression, so the chain spans ~50 LVAL
        // pages: far more than the cache holds, which evicts the data page the
        // scan is still iterating while the first row's MEMO is decoded.
        var large = new StringBuilder(100_000);
        for (int i = 0; i < 100_000; i++)
        {
            _ = large.Append((char)(0x4E00 + (i % 0x5000)));
        }

        string largeBody = large.ToString();
        var rows = new List<object[]> { new object[] { 1, largeBody } };
        for (int id = 2; id <= smallRowCount + 1; id++)
        {
            rows.Add([id, "small-" + id.ToString(CultureInfo.InvariantCulture)]);
        }

        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                tableName,
                [new("Id", typeof(int)), new("Body", typeof(string))],
                TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync(tableName, rows, TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { PageCacheSize = 8, UseLockFile = false },
            leaveOpen: false,
            TestContext.Current.CancellationToken);

        var actual = new List<object[]>();
        await foreach (object[] row in reader.Rows(tableName, cancellationToken: TestContext.Current.CancellationToken))
        {
            actual.Add(row);
        }

        Assert.Equal(rows.Count, actual.Count);
        for (int i = 0; i < rows.Count; i++)
        {
            Assert.Equal(rows[i][0], actual[i][0]);
            Assert.Equal(rows[i][1], actual[i][1]);
        }
    }

    [Fact]
    public async Task InterleavedReads_ReuseCatalogAndRowBoundsCaches()
    {
        const int alphaRows = 48;
        const int betaRows = 52;
        await using MemoryStream stream = await CreateCacheExerciseDatabaseAsync(
            new List<(string Name, int RowCount, string Prefix)>
            {
                (AlphaRowsTable, alphaRows, "A"),
                (BetaRowsTable, betaRows, "B"),
            },
            TestContext.Current.CancellationToken);
        var options = new AccessReaderOptions
        {
            PageCacheSize = 64,
            UseLockFile = false,
        };

        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            options,
            leaveOpen: true,
            TestContext.Current.CancellationToken);
        LruCache<long, byte[]> pageCache = ReadRequiredPrivateField<LruCache<long, byte[]>>(PageCacheOf(reader), PageCacheFieldName);
        LruCache<long, RowBound[]> rowBoundsCache = ReadRequiredPrivateField<LruCache<long, RowBound[]>>(PageCacheOf(reader), RowBoundsCacheFieldName);

        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Contains(AlphaRowsTable, tables);
        Assert.Contains(BetaRowsTable, tables);
        Assert.NotNull(ReadPrivateField(FacadeInternals.Services(reader).TableCatalog, CatalogCacheFieldName));

        long catalogMisses = pageCache.Misses;
        IReadOnlyList<string> repeatedTables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Equal(tables, repeatedTables);
        Assert.Equal(catalogMisses, pageCache.Misses);

        Assert.Equal(alphaRows, await CountRowsAsync(reader, AlphaRowsTable, TestContext.Current.CancellationToken));
        Assert.Equal(betaRows, await CountRowsAsync(reader, BetaRowsTable, TestContext.Current.CancellationToken));
        long rowBoundHitsAfterFirstInterleave = rowBoundsCache.Hits;
        long rowBoundMissesAfterFirstInterleave = rowBoundsCache.Misses;
        long pageHitsAfterFirstInterleave = pageCache.Hits;

        Assert.Equal(betaRows, await CountRowsAsync(reader, BetaRowsTable, TestContext.Current.CancellationToken));
        Assert.Equal(alphaRows, await CountRowsAsync(reader, AlphaRowsTable, TestContext.Current.CancellationToken));
        Assert.True(rowBoundsCache.Hits > rowBoundHitsAfterFirstInterleave);
        Assert.Equal(rowBoundMissesAfterFirstInterleave, rowBoundsCache.Misses);
        Assert.True(pageCache.Hits > pageHitsAfterFirstInterleave);
    }

    [Fact]
    public async Task Rows_WithInlineOwnedUsageMap_DoesNotBuildWholeFileOwnerIndex()
    {
        const string tableName = "MappedRows";
        const int rowCount = 96;
        await using MemoryStream stream = await CreateCacheExerciseDatabaseAsync(
            new List<(string Name, int RowCount, string Prefix)>
            {
                (tableName, rowCount, "M"),
            },
            TestContext.Current.CancellationToken);
        var options = new AccessReaderOptions
        {
            PageCacheSize = 16,
            UseLockFile = false,
        };

        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            options,
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        int actualRows = await CountRowsAsync(reader, tableName, TestContext.Current.CancellationToken);

        object? ownedDataPageIndex = ReadPrivateField(FacadeInternals.Database(reader), OwnedDataPageIndexFieldName);
        Assert.Equal(rowCount, actualRows);
        Assert.NotNull(ownedDataPageIndex);
        Assert.Null(ReadPrivateField(ownedDataPageIndex, AsyncLazyValueFieldName));
    }

    [Fact]
    public async Task Rows_WithReferenceOwnedUsageMap_DoesNotBuildWholeFileOwnerIndex()
    {
        const string tableName = "ReferenceMappedRows";
        const int rowCount = 96;
        await using MemoryStream stream = await CreateCacheExerciseDatabaseAsync(
            new List<(string Name, int RowCount, string Prefix)>
            {
                (tableName, rowCount, "R"),
            },
            TestContext.Current.CancellationToken);
        await ConvertOwnedUsageMapToReferenceAsync(stream, tableName, TestContext.Current.CancellationToken);
        var options = new AccessReaderOptions
        {
            PageCacheSize = 16,
            UseLockFile = false,
        };

        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            options,
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        int actualRows = await CountRowsAsync(reader, tableName, TestContext.Current.CancellationToken);

        object? ownedDataPageIndex = ReadPrivateField(FacadeInternals.Database(reader), OwnedDataPageIndexFieldName);
        Assert.Equal(rowCount, actualRows);
        Assert.NotNull(ownedDataPageIndex);
        Assert.Null(ReadPrivateField(ownedDataPageIndex, AsyncLazyValueFieldName));
    }

    [Fact]
    public async Task OpenAsync_WithZeroPageCacheSize_ReturnsRowsEquivalentToCachedReader()
    {
        await using MemoryStream source = await CreateCacheExerciseDatabaseAsync(
            new List<(string Name, int RowCount, string Prefix)>
            {
                (AlphaRowsTable, 72, "A"),
                (BetaRowsTable, 68, "B"),
            },
            TestContext.Current.CancellationToken);
        byte[] bytes = source.ToArray();

        await using var cachedStream = new MemoryStream(bytes, writable: false);
        await using var uncachedStream = new MemoryStream(bytes, writable: false);
        await using AccessReader cachedReader = await AccessReader.OpenAsync(
            cachedStream,
            new AccessReaderOptions { PageCacheSize = 16, UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
        await using AccessReader uncachedReader = await AccessReader.OpenAsync(
            uncachedStream,
            new AccessReaderOptions { PageCacheSize = 0, UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        Assert.NotNull(ReadPrivateField(PageCacheOf(cachedReader), PageCacheFieldName));
        Assert.NotNull(ReadPrivateField(PageCacheOf(cachedReader), RowBoundsCacheFieldName));
        Assert.Null(ReadPrivateField(PageCacheOf(uncachedReader), PageCacheFieldName));
        Assert.Null(ReadPrivateField(PageCacheOf(uncachedReader), RowBoundsCacheFieldName));

        foreach (string tableName in (string[])[AlphaRowsTable, BetaRowsTable])
        {
            List<string> cachedRows = await ReadRowSignaturesAsync(cachedReader, tableName, TestContext.Current.CancellationToken);
            List<string> uncachedRows = await ReadRowSignaturesAsync(uncachedReader, tableName, TestContext.Current.CancellationToken);
            Assert.Equal(cachedRows, uncachedRows);
        }
    }

    /// <summary>
    /// The writer's own reads go through a capacity-0 page cache over its
    /// <see cref="JetDatabaseWriter.Pages.Paging.Pager"/>, so they see the pages its transaction
    /// has pending, and the file's bytes again once it rolls back. (A reader
    /// can no longer have a journal attached: its page file cannot write.)
    /// </summary>
    [Fact]
    public async Task ReaderPageCache_OverWriterPager_ReadsThroughTheJournal()
    {
        await using MemoryStream stream = await CreateCacheExerciseDatabaseAsync(
            new List<(string Name, int RowCount, string Prefix)>
            {
                ("JournalRows", 4, "J"),
            },
            TestContext.Current.CancellationToken);
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        using var cache = new ReaderPageCache(harness.Database, capacity: 0);
        int pageSize = harness.Database.PageSizeBytes;

        byte[] original = await harness.Database.ReadPageCopyAsync(1, TestContext.Current.CancellationToken);
        byte[] changed = (byte[])original.Clone();
        changed[pageSize - 1] ^= 0xFF;

        JetTransaction tx = await harness.Services.Transactions.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await harness.Database.WritePageAsync(1, changed, TestContext.Current.CancellationToken);

        byte[] pending = await cache.ReadPageAsync(1, TestContext.Current.CancellationToken);
        try
        {
            Assert.Equal(changed, pending.AsSpan(0, pageSize).ToArray());
        }
        finally
        {
            DatabaseFile.ReturnPage(pending);
        }

        await tx.RollbackAsync(TestContext.Current.CancellationToken);

        byte[] afterRollback = await cache.ReadPageAsync(1, TestContext.Current.CancellationToken);
        try
        {
            Assert.Equal(original, afterRollback.AsSpan(0, pageSize).ToArray());
        }
        finally
        {
            DatabaseFile.ReturnPage(afterRollback);
        }
    }

    private static async ValueTask<MemoryStream> CreateCacheExerciseDatabaseAsync(
        IReadOnlyList<(string Name, int RowCount, string Prefix)> tableSeeds,
        CancellationToken cancellationToken)
    {
        var stream = new MemoryStream();
        var writerOptions = new AccessWriterOptions
        {
            UseLockFile = false,
            UseByteRangeLocks = false,
        };

        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            writerOptions,
            leaveOpen: true,
            cancellationToken: cancellationToken))
        {
            foreach ((string tableName, int rowCount, string prefix) in tableSeeds)
            {
                await writer.CreateTableAsync(tableName, CacheExerciseSchema(), cancellationToken);
                await writer.InsertRowsAsync(tableName, CreateRows(rowCount, prefix), cancellationToken);
            }
        }

        stream.Position = 0;
        return stream;
    }

    private static List<ColumnDefinition> CacheExerciseSchema() =>
    [
        new("Id", typeof(int)),
        new("Payload", typeof(string), maxLength: 220),
    ];

    private static List<object[]> CreateRows(int rowCount, string prefix)
    {
        var rows = new List<object[]>(rowCount);
        for (int rowNumber = 1; rowNumber <= rowCount; rowNumber++)
        {
            rows.Add([rowNumber, CreatePayload(prefix, rowNumber)]);
        }

        return rows;
    }

    private static string CreatePayload(string prefix, int rowNumber) =>
        prefix + "-" + rowNumber.ToString("D4", CultureInfo.InvariantCulture) + "-" + new string('x', 180);

    private static async ValueTask<int> CountRowsAsync(AccessReader reader, string tableName, CancellationToken cancellationToken)
    {
        int rowCount = 0;
        await foreach (object[] row in reader.Rows(tableName, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            _ = row;
            rowCount++;
        }

        return rowCount;
    }

    private static async ValueTask<List<string>> ReadRowSignaturesAsync(AccessReader reader, string tableName, CancellationToken cancellationToken)
    {
        var rows = new List<string>();
        await foreach (object[] row in reader.Rows(tableName, cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            rows.Add(CreateRowSignature(row));
        }

        return rows;
    }

    private static string CreateRowSignature(object[] row)
    {
        string[] values = new string[row.Length];
        for (int columnIndex = 0; columnIndex < row.Length; columnIndex++)
        {
            values[columnIndex] = Convert.ToString(row[columnIndex], CultureInfo.InvariantCulture) ?? string.Empty;
        }

        return string.Join("|", values);
    }

    private static async ValueTask ConvertOwnedUsageMapToReferenceAsync(MemoryStream stream, string tableName, CancellationToken cancellationToken)
    {
        long tdefPage;
        int pageSize;
        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken))
        {
            pageSize = reader.PageSize;
            tdefPage = await ResolveTdefPageAsync(reader, tableName, cancellationToken);
        }

        byte[] patched = ConvertOwnedUsageMapToReference(stream.ToArray(), pageSize, tdefPage);
        stream.SetLength(0);
        stream.Position = 0;
        await stream.WriteAsync(patched, cancellationToken);
        stream.Position = 0;
    }

    private static async ValueTask<long> ResolveTdefPageAsync(AccessReader reader, string tableName, CancellationToken cancellationToken)
    {
        IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync("MSysObjects", cancellationToken);
        int idIndex = FindColumnIndex(metadata, static column => string.Equals(column.Name, "Id", StringComparison.OrdinalIgnoreCase));
        int nameIndex = FindColumnIndex(metadata, static column => string.Equals(column.Name, "Name", StringComparison.OrdinalIgnoreCase));
        Assert.True(idIndex >= 0);
        Assert.True(nameIndex >= 0);

        await foreach (object[] row in reader.Rows("MSysObjects", cancellationToken: cancellationToken))
        {
            if (row[nameIndex] is string name && string.Equals(name, tableName, StringComparison.OrdinalIgnoreCase))
            {
                return Convert.ToInt64(row[idIndex], CultureInfo.InvariantCulture);
            }
        }

        throw new InvalidOperationException($"Could not resolve TDEF page for table '{tableName}'.");
    }

    private static byte[] ConvertOwnedUsageMapToReference(byte[] fileBytes, int pageSize, long tdefPage)
    {
        const int dataPageRowsStart = 14;
        var tdefLayout = TDefHeaderLayout.For(DatabaseFormat.Jet4Mdb);

        int tdefOffset = checked((int)(tdefPage * pageSize));
        int usageMapRow = fileBytes[tdefOffset + tdefLayout.UsedPages];
        int usageMapPage = ReadUInt24(fileBytes, tdefOffset + tdefLayout.UsedPagesPage);
        int usageMapOffset = checked(usageMapPage * pageSize);
        int rowOffsetPosition = usageMapOffset + dataPageRowsStart + (usageMapRow * 2);
        int rowStart = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(rowOffsetPosition, 2)) & 0x1FFF;
        int rowAbsoluteStart = usageMapOffset + rowStart;
        Assert.Equal(Constants.UsageMap.InlineMapType, fileBytes[rowAbsoluteStart]);

        int basePage = BinaryPrimitives.ReadInt32LittleEndian(fileBytes.AsSpan(rowAbsoluteStart + 1, 4));
        var dataPages = new List<int>();
        for (int bitIndex = 0; bitIndex < 512; bitIndex++)
        {
            int byteOffset = rowAbsoluteStart + Constants.UsageMap.InlineBitmapOffset + (bitIndex / 8);
            byte bitMask = (byte)(1 << (bitIndex % 8));
            if ((fileBytes[byteOffset] & bitMask) != 0)
            {
                dataPages.Add(basePage + bitIndex);
            }
        }

        Assert.NotEmpty(dataPages);

        int referencePageNumber = fileBytes.Length / pageSize;
        Array.Resize(ref fileBytes, fileBytes.Length + pageSize);
        int referencePageOffset = referencePageNumber * pageSize;
        fileBytes[referencePageOffset] = Constants.PageTypes.UsageMap;

        int pagesPerReferenceMapPage = (pageSize - Constants.UsageMap.ReferenceMapBitmapOffset) * 8;
        foreach (int dataPage in dataPages)
        {
            Assert.InRange(dataPage, 1, pagesPerReferenceMapPage - 1);
            int bitIndex = dataPage % pagesPerReferenceMapPage;
            fileBytes[referencePageOffset + Constants.UsageMap.ReferenceMapBitmapOffset + (bitIndex / 8)] |= (byte)(1 << (bitIndex % 8));
        }

        Array.Clear(fileBytes, rowAbsoluteStart, Constants.UsageMap.RowSize);
        fileBytes[rowAbsoluteStart] = Constants.UsageMap.ReferenceMapType;
        BinaryPrimitives.WriteInt32LittleEndian(
            fileBytes.AsSpan(rowAbsoluteStart + Constants.UsageMap.ReferenceMapPointerOffset, 4),
            referencePageNumber);

        return fileBytes;
    }

    private static int FindColumnIndex(IReadOnlyList<ColumnMetadata> columns, Predicate<ColumnMetadata> match)
    {
        for (int i = 0; i < columns.Count; i++)
        {
            if (match(columns[i]))
            {
                return i;
            }
        }

        return -1;
    }

    private static int ReadUInt24(byte[] buffer, int offset)
        => buffer[offset] | (buffer[offset + 1] << 8) | (buffer[offset + 2] << 16);

    private static ReaderPageCache PageCacheOf(AccessReader reader) => FacadeInternals.Services(reader).PageCache;

    private static T ReadRequiredPrivateField<T>(object instance, string fieldName)
        where T : class =>
        Assert.IsType<T>(ReadPrivateField(instance, fieldName));

    private static object? ReadPrivateField(object instance, string fieldName) =>
        FacadeInternals.ReadPrivateField(instance, fieldName);
}
