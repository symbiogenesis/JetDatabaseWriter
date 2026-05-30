namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

public sealed class AccessReaderCacheTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private const string AlphaRowsTable = "AlphaRows";
    private const string BetaRowsTable = "BetaRows";
    private const string PageCacheFieldName = "pageCache";
    private const string RowBoundsCacheFieldName = "rowBoundsCache";
    private const string CatalogCacheFieldName = "catalogCache";
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
        Assert.Null(ReadPrivateField(reader, PageCacheFieldName));
        Assert.Null(ReadPrivateField(reader, RowBoundsCacheFieldName));
        Assert.NotEmpty(await reader.ListTablesAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task OpenUncachedAsync_WithPositivePageCacheSize_SuppressesCacheAllocation()
    {
        byte[] bytes = await db.GetFileAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        var options = new AccessReaderOptions
        {
            PageCacheSize = 256,
            UseLockFile = false,
        };

        await using var cachedStream = new MemoryStream(bytes, writable: false);
        await using (AccessReader cachedReader = await AccessReader.OpenAsync(
            cachedStream,
            options,
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
            Assert.NotNull(ReadPrivateField(cachedReader, PageCacheFieldName));
            Assert.NotNull(ReadPrivateField(cachedReader, RowBoundsCacheFieldName));
        }

        await using var uncachedStream = new MemoryStream(bytes, writable: false);
        await using AccessReader uncachedReader = await AccessReader.OpenUncachedAsync(
            uncachedStream,
            options,
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        Assert.Equal(256, uncachedReader.PageCacheSize);
        Assert.Null(ReadPrivateField(uncachedReader, PageCacheFieldName));
        Assert.Null(ReadPrivateField(uncachedReader, RowBoundsCacheFieldName));
        Assert.NotEmpty(await uncachedReader.ListTablesAsync(TestContext.Current.CancellationToken));
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

        LruCache<long, byte[]> pageCache = ReadRequiredPrivateField<LruCache<long, byte[]>>(reader, PageCacheFieldName);
        LruCache<long, AccessBase.RowBound[]> rowBoundsCache = ReadRequiredPrivateField<LruCache<long, AccessBase.RowBound[]>>(reader, RowBoundsCacheFieldName);
        Assert.Equal(rowCount, actualRows);
        Assert.Equal(options.PageCacheSize, pageCache.Count);
        Assert.True(pageCache.Misses > pageCache.Count);
        Assert.Equal(options.PageCacheSize, rowBoundsCache.Count);
        Assert.True(rowBoundsCache.Misses > rowBoundsCache.Count);
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
        LruCache<long, byte[]> pageCache = ReadRequiredPrivateField<LruCache<long, byte[]>>(reader, PageCacheFieldName);
        LruCache<long, AccessBase.RowBound[]> rowBoundsCache = ReadRequiredPrivateField<LruCache<long, AccessBase.RowBound[]>>(reader, RowBoundsCacheFieldName);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Contains(AlphaRowsTable, tables);
        Assert.Contains(BetaRowsTable, tables);
        Assert.NotNull(ReadPrivateField(reader, CatalogCacheFieldName));

        long catalogMisses = pageCache.Misses;
        List<string> repeatedTables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
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

        object? ownedDataPageIndex = ReadPrivateField(reader, OwnedDataPageIndexFieldName);
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

        object? ownedDataPageIndex = ReadPrivateField(reader, OwnedDataPageIndexFieldName);
        Assert.Equal(rowCount, actualRows);
        Assert.NotNull(ownedDataPageIndex);
        Assert.Null(ReadPrivateField(ownedDataPageIndex, AsyncLazyValueFieldName));
    }

    [Fact]
    public async Task OpenUncachedAsync_ReturnsEquivalentRowsWithoutAllocatingPageCaches()
    {
        await using MemoryStream source = await CreateCacheExerciseDatabaseAsync(
            new List<(string Name, int RowCount, string Prefix)>
            {
                (AlphaRowsTable, 72, "A"),
                (BetaRowsTable, 68, "B"),
            },
            TestContext.Current.CancellationToken);
        byte[] bytes = source.ToArray();
        var options = new AccessReaderOptions
        {
            PageCacheSize = 16,
            UseLockFile = false,
        };

        await using var cachedStream = new MemoryStream(bytes, writable: false);
        await using var uncachedStream = new MemoryStream(bytes, writable: false);
        await using AccessReader cachedReader = await AccessReader.OpenAsync(
            cachedStream,
            options,
            leaveOpen: true,
            TestContext.Current.CancellationToken);
        await using AccessReader uncachedReader = await AccessReader.OpenUncachedAsync(
            uncachedStream,
            options,
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        Assert.NotNull(ReadPrivateField(cachedReader, PageCacheFieldName));
        Assert.NotNull(ReadPrivateField(cachedReader, RowBoundsCacheFieldName));
        Assert.Null(ReadPrivateField(uncachedReader, PageCacheFieldName));
        Assert.Null(ReadPrivateField(uncachedReader, RowBoundsCacheFieldName));

        foreach (string tableName in (string[])[AlphaRowsTable, BetaRowsTable])
        {
            List<string> cachedRows = await ReadRowSignaturesAsync(cachedReader, tableName, TestContext.Current.CancellationToken);
            List<string> uncachedRows = await ReadRowSignaturesAsync(uncachedReader, tableName, TestContext.Current.CancellationToken);
            Assert.Equal(cachedRows, uncachedRows);
        }
    }

    [Fact]
    public async Task ReadPageCachedAsync_WithActiveJournal_BypassesCachedPageBytes()
    {
        await using MemoryStream stream = await CreateCacheExerciseDatabaseAsync(
            new List<(string Name, int RowCount, string Prefix)>
            {
                ("JournalRows", 4, "J"),
            },
            TestContext.Current.CancellationToken);
        var options = new AccessReaderOptions
        {
            PageCacheSize = 8,
            UseLockFile = false,
        };

        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            options,
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        byte[] cachedPage = await reader.ReadPageCachedAsync(0, TestContext.Current.CancellationToken);
        LruCache<long, byte[]> pageCache = ReadRequiredPrivateField<LruCache<long, byte[]>>(reader, PageCacheFieldName);
        Assert.Equal(1, pageCache.Count);

        byte[] journaledPage = new byte[reader.PageSize];
        Buffer.BlockCopy(cachedPage, 0, journaledPage, 0, reader.PageSize);
        journaledPage[0x14] = unchecked((byte)(journaledPage[0x14] + 1));

        var journal = new PageJournal(stream.Length, reader.PageSize, maxPages: 4);
        journal.Write(0, journaledPage);
        reader.ActiveJournal = journal;

        byte[] rereadPage = await reader.ReadPageCachedAsync(0, TestContext.Current.CancellationToken);
        try
        {
            Assert.NotSame(cachedPage, rereadPage);
            Assert.Equal(journaledPage[0x14], rereadPage[0x14]);
        }
        finally
        {
            reader.ActiveJournal = null;
            AccessBase.ReturnPage(rereadPage);
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
        List<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync("MSysObjects", cancellationToken);
        int idIndex = metadata.FindIndex(static column => string.Equals(column.Name, "Id", StringComparison.OrdinalIgnoreCase));
        int nameIndex = metadata.FindIndex(static column => string.Equals(column.Name, "Name", StringComparison.OrdinalIgnoreCase));
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

        int tdefOffset = checked((int)(tdefPage * pageSize));
        int usageMapRow = fileBytes[tdefOffset + Constants.TableDefinition.OwnedPagesRowOffset];
        int usageMapPage = ReadUInt24(fileBytes, tdefOffset + Constants.TableDefinition.OwnedPagesRowOffset + 1);
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

    private static int ReadUInt24(byte[] buffer, int offset)
        => buffer[offset] | (buffer[offset + 1] << 8) | (buffer[offset + 2] << 16);

    private static T ReadRequiredPrivateField<T>(AccessBase instance, string fieldName)
        where T : class =>
        Assert.IsType<T>(ReadPrivateField(instance, fieldName));

    private static object? ReadPrivateField(object instance, string fieldName)
    {
        Type? currentType = instance.GetType();
        while (currentType is not null)
        {
            FieldInfo? field = currentType.GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
            if (field is not null)
            {
                return field.GetValue(instance);
            }

            currentType = currentType.BaseType;
        }

        throw new MissingFieldException(instance.GetType().FullName, fieldName);
    }
}
