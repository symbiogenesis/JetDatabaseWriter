namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueEncoding;
using Xunit;

/// <summary>Stored scalar keys are compared with independent raw-row decoding, including missing entries.</summary>
public sealed class CompactIndexRowAuditTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task IntegerRows_HaveOneMatchingStoredEntryEach(DatabaseFormat kind)
    {
        (MemoryPages pages, JetFormat format, TDefImage image) = Create(kind);
        CompactIndexRowAuditResult result = await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct);
        Assert.Equal(3, result.SourceRows);
        Assert.Equal(3, result.IndexedRows);
        Assert.Equal(0, result.ExcludedNullRows);
        Assert.True(result.StoredKeysMatchRows);
        Assert.True(result.DiscoveredRowsCovered);
        Assert.False(result.CompleteSourceOwnershipProven);
        Assert.True(result.IsPinned);
    }

    [Theory]
    [InlineData("wrong-key")]
    [InlineData("duplicate")]
    [InlineData("missing")]
    [InlineData("unknown-row")]
    public async Task OrderedValidTree_WithIncorrectRowCorrespondenceRefuses(string damage)
    {
        (MemoryPages pages, JetFormat format, TDefImage image) = Create(DatabaseFormat.Jet4Mdb);
        List<IndexEntry> entries = [Entry(10, 0), Entry(20, 1), Entry(30, 2)];
        switch (damage)
        {
            case "wrong-key":
                entries[1] = Entry(21, 1);
                break;
            case "duplicate":
                entries[1] = entries[0];
                break;
            case "missing":
                entries.RemoveAt(1);
                break;
            default:
                entries.Add(new(Entry(40, 0).Key, 5, 0));
                break;
        }

        pages.Images[3] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, entries, enablePrefixCompression: false);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct));
    }

    [Theory]
    [InlineData("separator-key")]
    [InlineData("separator-bookmark")]
    [InlineData("disconnected-leaves")]
    public async Task MatchingLeafRows_RequireComposedRoutingSeparatorAndChainEvidence(string damage)
    {
        (MemoryPages pages, JetFormat format, TDefImage image) = Create(DatabaseFormat.Jet4Mdb);
        IndexEntry separator = damage == "separator-key" ? Entry(40, 2) : Entry(30, 1);
        pages.Images[3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(separator, 6)], 0, 0, 0);
        pages.Images[6] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [Entry(10, 0), Entry(20, 1), Entry(30, 2)], enablePrefixCompression: false);
        if (damage == "disconnected-leaves")
        {
            pages.Images[3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(Entry(20, 1), 6)], 0, 0, 7);
            pages.Images[6] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [Entry(10, 0), Entry(20, 1)], enablePrefixCompression: false);
            pages.Images[7] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [Entry(30, 2)], enablePrefixCompression: false);
        }

        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task IgnoreNulls_ExcludesOnlyAllNullCompositeRows(bool ignoreNulls)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        ColumnInfo[] columns = [new() { Name = "A", ColNum = 0, Type = ColumnType.LongIntegerType, Flags = 1, Size = 4 }, new() { Name = "B", ColNum = 1, Type = ColumnType.LongIntegerType, Flags = 1, FixedOff = 4, Size = 4 }];
        TDefImage image = Image(columns, [new(0, true), new(1, false)], ignoreNulls ? Constants.TableDefinition.IgnoreNullsIndexFlag : (byte)0);
        List<IndexEntry> entries = [];
        object?[][] rows = [[null, null], [null, 20], [10, null]];
        for (int row = 0; row < rows.Length; row++)
        {
            if (ignoreNulls && row == 0)
            {
                continue;
            }

            byte[] a = IndexKeyEncoder.EncodeColumnEntry(format, columns[0], rows[row][0]);
            byte[] b = IndexKeyEncoder.EncodeColumnEntry(format, columns[1], rows[row][1], ascending: false);
            entries.Add(new([.. a, .. b], 4, checked((byte)row)));
        }

        entries.Sort(static (left, right) => left.Key.AsSpan().SequenceCompareTo(right.Key));
        var pages = new MemoryPages(format.PageSize, new() { [3] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, entries, enablePrefixCompression: true), [4] = Data(format, image, rows) });
        CompactIndexRowAuditResult result = await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct);
        Assert.Equal(ignoreNulls ? 1 : 0, result.ExcludedNullRows);
        Assert.Equal(ignoreNulls ? 2 : 3, result.IndexedRows);
    }

    [Theory]
    [InlineData(ColumnType.BooleanType)]
    [InlineData(ColumnType.MemoType)]
    [InlineData(ColumnType.OleType)]
    public async Task UnsupportedKeyTypes_RefuseBeforeReading(ColumnType type)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        TDefImage image = Image([new() { Name = "Opaque", ColNum = 0, Type = type }], [new(0, true)], 0);
        var pages = new MemoryPages(format.PageSize, []);
        Assert.False(CompactIndexRowAudit.SupportsDescriptor(image, 0));
        await Assert.ThrowsAsync<JetNotSupportedException>(async () => await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct));
    }

    [Fact]
    public async Task ComplexReferenceKeys_PreserveRawParentIdentitiesWithoutFollowingItems()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        TDefImage image = Image([new() { Name = "Items", ColNum = 0, Type = ColumnType.ComplexType, Flags = 1, Size = 4 }], [new(0, true)], 0);
        var pages = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [Entry(10, 0), Entry(20, 1), Entry(30, 2)], enablePrefixCompression: false),
            [4] = Data(format, image, [[10], [20], [30]]),
        });
        Assert.True(CompactIndexRowAudit.SupportsDescriptor(image, 0));
        CompactIndexRowAuditResult result = await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct);
        Assert.Equal(3, result.IndexedRows);
        Assert.True(result.StoredKeysMatchRows);
        Assert.False(result.CompleteSourceOwnershipProven);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task ComplexReferenceKeys_RejectMatchingNonpositiveAssignedIdentities(int reference)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        TDefImage image = Image([new() { Name = "Items", ColNum = 0, Type = ColumnType.ComplexType, Flags = 1, Size = 4 }], [new(0, true)], 0);
        var pages = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [Entry(reference, 0)], enablePrefixCompression: false),
            [4] = Data(format, image, [[reference]]),
        });
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct));
    }

    [Fact]
    public async Task DateKeys_RetainSubMillisecondStoredDoublePrecision()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        TDefImage image = Image([new() { Name = "When", ColNum = 0, Type = ColumnType.DateTimeType, Flags = 1, Size = 8 }], [new(0, true)], 0);
        byte[] data = Data(format, image, [[new DateTime(2026, 1, 1)]]);
        int rowStart = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(format.DataPage.RowsStart));
        const double stored = 45_000.123456789;
        BinaryPrimitives.WriteDoubleLittleEndian(data.AsSpan(rowStart + format.RowFields.NumCols), stored);
        byte[] key = IndexKeyEncoder.EncodeEntry(ColumnType.DoubleType, stored);
        var pages = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new(key, 4, 0)], enablePrefixCompression: false),
            [4] = data,
        });
        CompactIndexRowAuditResult result = await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct);
        Assert.True(result.StoredKeysMatchRows);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TextKeys_RefuseIncompleteCodeUnitsInsteadOfComparingLossyEmptyText(bool compressed)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        TDefImage image = Image([new() { Name = "Text", ColNum = 0, Type = ColumnType.TextType, Size = 16, ExtraFlags = compressed ? Constants.CompressedUnicodeExtFlagMask : (byte)0 }], [new(0, true)], 0);
        TDefImage binary = Image([new() { Name = "Text", ColNum = 0, Type = ColumnType.BinaryType, Size = 16 }], [new(0, true)], 0);
        byte[] key = IndexKeyEncoder.EncodeTextEntry(image.Columns[0].TextSortOrder, string.Empty);
        byte[] storedText = compressed ? [0xFF, 0xFE, 0, 0x41] : "A"u8.ToArray();
        var pages = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new(key, 4, 0)], enablePrefixCompression: false),
            [4] = Data(format, binary, [[storedText]]),
        });
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task UncompressedText_LeadingByteOrderMarkRemainsAStoredCodeUnit(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        TDefImage image = Image([new() { Name = "Text", ColNum = 0, Type = ColumnType.TextType, Size = 16 }], [new(0, true)], 0);
        TDefImage binary = Image([new() { Name = "Text", ColNum = 0, Type = ColumnType.BinaryType, Size = 16 }], [new(0, true)], 0);
        byte[] key = IndexKeyEncoder.EncodeTextEntry(image.Columns[0].TextSortOrder, "\uFEFFA");
        var pages = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new(key, 4, 0)], enablePrefixCompression: false),
            [4] = Data(format, binary, [[new byte[] { 0xFF, 0xFE, 0x41, 0 }]]),
        });
        CompactIndexRowAuditResult result = await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct);
        Assert.True(result.StoredKeysMatchRows);
        Assert.True(result.ObservedTreeAndBookmarksValidated);
    }

    [Theory]
    [InlineData("rows")]
    [InlineData("key-bytes")]
    [InlineData("working")]
    public async Task EvidenceBudget_RefusesAndReleasesAggregateAdmission(string limit)
    {
        (MemoryPages pages, JetFormat format, TDefImage image) = Create(DatabaseFormat.Jet4Mdb);
        var accounting = new CompactDiscoveryAccounting(new());
        CompactIndexRowBudget budget = limit switch
        {
            "rows" => new() { Accounting = accounting, MaxRows = 1 },
            "key-bytes" => new() { Accounting = accounting, MaxKeyBytes = 1 },
            _ => new() { Accounting = accounting, MaxWorkingBytes = 1 },
        };
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), budget, Ct));
        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData("FixedText/NativeFixedTextJet3Cp1252.mdb")]
    [InlineData("FixedText/NativeFixedTextJet4.mdb")]
    [InlineData("NorthwindTraders.accdb")]
    [InlineData("ComplexFields.accdb")]
    [InlineData("mdbtools/nwind.mdb")]
    public async Task NativeSupportedScalarIndexes_MatchTheirIndependentlyDecodedRows(string fixture)
    {
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", fixture), Ct);
        await using var stream = new MemoryStream(bytes, writable: false);
        await using var file = DatabaseFile.ForReader(stream, bytes, ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: true, cancellationToken: Ct);
        int verified = 0;
        int ambiguousPhysicalScopes = 0;
        await CompactStorageInventory.ScanAsync(file.Pages, file.Format, file.TableDefs, new(), static (_, _) => default, Ct, async (owner, image, token) =>
        {
            for (int ordinal = 0; ordinal < image.RealIndexes.Count; ordinal++)
            {
                if (!CompactIndexRowAudit.SupportsDescriptor(image, ordinal) || image.RealIndexes[ordinal].RootPage == 0)
                {
                    continue;
                }

                CompactIndexRowAuditResult result;
                try
                {
                    result = await CompactIndexRowAudit.ValidateAsync(file.Pages, file.Format, owner, image, ordinal, new(), new(), new(), token);
                }
                catch (JetCorruptDataException exception) when (fixture == "mdbtools/nwind.mdb" && owner == 753 && exception.Message.StartsWith("An independently discovered included row is missing", StringComparison.Ordinal))
                {
                    // Umsätze has historical same-owner DATA outside its active
                    // map. Physical headers alone cannot choose which rows to drop.
                    Assert.Equal(830u, image.Header.Counters.RowCount);
                    Assert.Contains("rows 2879/830", exception.Message, StringComparison.Ordinal);
                    IReadOnlyList<long> mapped = await file.OwnedPages.GetOwnedDataPagesAsync(owner, token);
                    Assert.DoesNotContain(843, mapped);
                    ambiguousPhysicalScopes++;
                    continue;
                }
                catch (JetCorruptDataException exception)
                {
                    string keys = string.Join(", ", image.RealIndexes[ordinal].Columns.Select(key => image.Columns.Single(column => column.ColNum == key.ColNum)).Select(column => $"{column.Name}/{column.Type}/{column.TextSortOrder}"));
                    throw new InvalidDataException($"Native owner {owner}, descriptor {ordinal}: {keys}", exception);
                }

                Assert.True(result.StoredKeysMatchRows);
                Assert.True(result.DiscoveredRowsCovered);
                verified++;
            }
        });
        Assert.True(verified > 0);
        Assert.Equal(fixture == "mdbtools/nwind.mdb", ambiguousPhysicalScopes > 0);
    }

    private static (MemoryPages Pages, JetFormat Format, TDefImage Image) Create(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        TDefImage image = Image([new() { Name = "Id", ColNum = 0, Type = ColumnType.LongIntegerType, Flags = 1, Size = 4 }], [new(0, true)], 0);
        var pages = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [Entry(10, 0), Entry(20, 1), Entry(30, 2)], enablePrefixCompression: true),
            [4] = Data(format, image, [[10], [20], [30]]),
        });
        return (pages, format, image);
    }

    private static TDefImage Image(ColumnInfo[] columns, List<KeyColumn> keys, byte flags)
        => new([], new() { ColumnCount = columns.Length, VariableColumnCount = columns.Count(static column => !column.IsFixed) }, [.. columns], false, TDefParseIssues.None, null, [new(new(0, 0, flags), 3, keys, [])], []) { TDefPageNumber = 2 };

    private static IndexEntry Entry(int value, byte row) => new(IndexKeyEncoder.EncodeEntry(ColumnType.LongIntegerType, value), 4, row);

    private static byte[] Data(JetFormat format, TDefImage image, object?[][] rows)
    {
        byte[] page = new byte[format.PageSize];
        page[0] = Constants.PageTypes.Data;
        BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(format.DataPage.TDefOff), 2);
        BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(format.DataPage.NumRows), checked((ushort)rows.Length));
        int end = format.PageSize;
        for (int row = 0; row < rows.Length; row++)
        {
            object[] values = rows[row].Select(static value => value ?? 0).ToArray();
            byte[] payload = new RowEncoder(format).SerializeRow(image.Definition, values);
            for (int column = 0; column < rows[row].Length; column++)
            {
                if (rows[row][column] is null)
                {
                    int bit = image.Columns[column].ColNum;
                    int nullMaskStart = payload.Length - JetFormat.GetNullMaskSizeBytes(image.Columns.Count);
                    payload[nullMaskStart + (bit / 8)] &= unchecked((byte)~(1 << (bit % 8)));
                }
            }

            end -= payload.Length;
            payload.CopyTo(page, end);
            BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(format.DataPage.RowsStart + (row * 2)), checked((ushort)end));
        }

        return page;
    }

    private sealed class MemoryPages(int pageSize, Dictionary<long, byte[]> images) : IPageSource
    {
        public int PageSize => pageSize;

        public long PageCount => 20;

        public bool IsDisposed => false;

        internal Dictionary<long, byte[]> Images => images;

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] buffer = ArrayPool<byte>.Shared.Rent(pageSize);
            Array.Clear(buffer, 0, buffer.Length);
            if (images.TryGetValue(pageNumber, out byte[]? image))
            {
                image.CopyTo(buffer, 0);
            }

            return ValueTask.FromResult(buffer);
        }

        public void ThrowIfDisposed()
        {
        }

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => cancellationToken.ThrowIfCancellationRequested();
    }
}
