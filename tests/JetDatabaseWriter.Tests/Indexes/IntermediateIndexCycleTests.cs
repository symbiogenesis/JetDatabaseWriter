namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>Cyclic intermediate branches are refused before their pages are read again.</summary>
public sealed class IntermediateIndexCycleTests
{
    private const long FirstPageNumber = 50;

    public static TheoryData<DatabaseFormat, string, bool, int> CycleCases
    {
        get
        {
            var cases = new TheoryData<DatabaseFormat, string, bool, int>();
            foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.AceAccdb })
            {
                foreach (string operation in new[] { "contains", "exact", "range", "streaming", "last" })
                {
                    foreach (bool useTail in new[] { false, true })
                    {
                        foreach (int pageCount in new[] { 1, 2 })
                        {
                            cases.Add(format, operation, useTail, pageCount);
                        }
                    }
                }
            }

            return cases;
        }
    }

    [Theory]
    [MemberData(nameof(CycleCases))]
    public async Task Cursor_CyclicIntermediateBranch_ReportsCorruptionBeforeRevisit(DatabaseFormat format, string operation, bool useTail, int pageCount)
    {
        var profile = JetFormat.ForNewDatabase(format);
        byte[] key = EncodeKey(42);
        var range = new EncodedIndexRange(new EncodedIndexBound(key, true, false), EncodedIndexBound.None);
        var pages = new Dictionary<long, byte[]>();
        for (int index = 0; index < pageCount; index++)
        {
            long next = FirstPageNumber + ((index + 1) % pageCount);
            var summary = new DecodedIntermediateEntry(new IndexEntry(EncodeKey(useTail ? 1 : 42), DataPage: 7, DataRow: 0), useTail ? FirstPageNumber + pageCount : next);
            byte[] page;
            if (operation == "last" && useTail)
            {
                page = IndexPageCodec.BuildLeafPage(profile.IndexPage, profile.PageSize, parentTdefPage: 7, [], prevPage: 0, nextPage: 0, tailPage: next, enablePrefixCompression: false);
                page[0] = Constants.IndexLeafPage.PageTypeIntermediate;
            }
            else
            {
                page = IndexPageCodec.BuildIntermediatePage(profile.IndexPage, profile.PageSize, parentTdefPage: 7, [summary], prevPage: 0, nextPage: 0, tailPage: useTail ? next : 0);
            }

            pages.Add(FirstPageNumber + index, page);
        }

        int reads = 0;
        var cursor = new IndexCursor(
            profile.IndexPage,
            (page, _) =>
            {
                reads++;
                return new ValueTask<byte[]>(pages[page]);
            },
            profile.PageSize);
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
        {
            switch (operation)
            {
                case "contains":
                    _ = await cursor.ContainsKeyAsync(FirstPageNumber, key, TestContext.Current.CancellationToken);
                    break;
                case "exact":
                    _ = await cursor.FindRowLocationsAsync(FirstPageNumber, key, TestContext.Current.CancellationToken);
                    break;
                case "range":
                    _ = await cursor.FindRowLocationsInRangeAsync(FirstPageNumber, range, TestContext.Current.CancellationToken);
                    break;
                case "streaming":
                    await foreach ((long _, int _) in cursor.EnumerateRowLocationsInRangeAsync(FirstPageNumber, range, TestContext.Current.CancellationToken))
                    {
                        Assert.Fail("A cyclic intermediate branch emitted a row.");
                    }

                    break;
                case "last":
                    _ = await cursor.TryReadLastEntryAsync(FirstPageNumber, TestContext.Current.CancellationToken);
                    break;
                default:
                    Assert.Fail("Unknown index operation.");
                    break;
            }
        });

        Assert.Equal(JetErrorCode.CorruptIndex, error.ErrorCode);
        Assert.Equal(FirstPageNumber, error.ErrorInfo.PageNumber);
        Assert.Equal(pageCount, reads);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task PublicIndexReads_CyclicIntermediateBranch_ReportsCorruption(DatabaseFormat format, bool exact)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [1], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        IndexMetadata index;
        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            index = Assert.Single(await reader.ListIndexesAsync("Items", TestContext.Current.CancellationToken));
        }

        var profile = JetFormat.ForNewDatabase(format);
        byte[] bytes = stream.ToArray();
        int rootOffset = checked(index.FirstDp * profile.PageSize);
        long parentTdef = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(rootOffset + 4, 4));
        var summary = new DecodedIntermediateEntry(new IndexEntry(EncodeKey(1), DataPage: 7, DataRow: 0), index.FirstDp);
        byte[] root = IndexPageCodec.BuildIntermediatePage(profile.IndexPage, profile.PageSize, parentTdef, [summary], prevPage: 0, nextPage: 0, tailPage: index.FirstDp);
        stream.Position = rootOffset;
        await stream.WriteAsync(root, TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using AccessReader damagedReader = await OpenReaderAsync(stream);

        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
        {
            IAccessIndexQuery<object[]> query = damagedReader.FromIndex("Items", index.Name);
            if (exact)
            {
                query = query.WhereEquals(1);
            }

            await foreach (object[] row in query.ToRowsAsync(TestContext.Current.CancellationToken))
            {
                Assert.Fail($"A cyclic index emitted a row with {row.Length} cells.");
            }
        });

        Assert.Equal(JetErrorCode.CorruptIndex, error.ErrorCode);
        Assert.Equal(index.FirstDp, error.ErrorInfo.PageNumber);
    }

    private static byte[] EncodeKey(int value)
        => IndexKeyEncoder.EncodeEntry(ColumnType.LongIntegerType, value, ascending: true);

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, PageCacheSize = 0 }, leaveOpen: true, TestContext.Current.CancellationToken);
    }
}
