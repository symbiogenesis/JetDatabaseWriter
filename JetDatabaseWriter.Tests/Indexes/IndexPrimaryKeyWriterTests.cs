namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Round-trip tests for primary-key emission.
/// <para>
/// PK is emitted as a logical-index entry with <c>index_type = 0x01</c> and
/// is implicitly unique. Multi-column PKs participate in the bulk B-tree
/// rebuild via the composite-key concatenation path, provided every key
/// column's type is supported by <c>IndexKeyEncoder</c>.
/// </para>
/// </summary>
public sealed class IndexPrimaryKeyWriterTests
{
    private static readonly string[] CompositeOrderLine = ["OrderId", "LineNo"];
    private static readonly string[] CompositeAB = ["A", "B"];
    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    [Fact]
    public async Task CreateTable_WithSingleColumnPrimaryKey_ViaIndexDefinition_RoundTrips()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();
        const string TableName = "Pk_Single";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Name", typeof(string), maxLength: 50),
                ],
                [new IndexDefinition("PK_Pk_Single", "Id") { IsPrimaryKey = true }],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(TableName, TestContext.Current.CancellationToken);

        IndexMetadata pk = Assert.Single(indexes);
        Assert.Equal("PK_Pk_Single", pk.Name);
        Assert.Equal(IndexKind.PrimaryKey, pk.Kind);
        IndexColumnReference col = Assert.Single(pk.Columns);
        Assert.Equal("Id", col.Name);
        Assert.True(col.IsAscending);
    }

    [Fact]
    public async Task CreateTable_WithSingleColumnPrimaryKey_ViaColumnFlag_SynthesizesNamedPrimaryKey()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();
        const string TableName = "Pk_ColFlag";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Name", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(TableName, TestContext.Current.CancellationToken);

        IndexMetadata pk = Assert.Single(indexes);
        Assert.Equal("PrimaryKey", pk.Name);
        Assert.Equal(IndexKind.PrimaryKey, pk.Kind);
        IndexColumnReference col = Assert.Single(pk.Columns);
        Assert.Equal("Id", col.Name);
    }

    [Fact]
    public async Task CreateTable_WithCompositePrimaryKey_RoundTripsAllKeyColumnsInOrder()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();
        const string TableName = "Pk_Composite";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("OrderId", typeof(int)),
                    new ColumnDefinition("LineNo", typeof(int)),
                    new ColumnDefinition("Sku", typeof(string), maxLength: 20),
                ],
                [
                    new IndexDefinition("PK_Order", CompositeOrderLine) { IsPrimaryKey = true },
                ],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(TableName, TestContext.Current.CancellationToken);

        IndexMetadata pk = Assert.Single(indexes);
        Assert.Equal(IndexKind.PrimaryKey, pk.Kind);
        Assert.Equal(2, pk.Columns.Count);
        Assert.Equal("OrderId", pk.Columns[0].Name);
        Assert.Equal("LineNo", pk.Columns[1].Name);
        Assert.True(pk.Columns[0].IsAscending);
        Assert.True(pk.Columns[1].IsAscending);
    }

    [Fact]
    public async Task CreateTable_WithCompositePrimaryKey_ViaColumnFlags_PreservesDeclarationOrder()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();
        const string TableName = "Pk_CompositeFlag";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("OrderId", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("LineNo", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Sku", typeof(string), maxLength: 20),
                ],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(TableName, TestContext.Current.CancellationToken);

        IndexMetadata pk = Assert.Single(indexes);
        Assert.Equal("PrimaryKey", pk.Name);
        Assert.Equal(IndexKind.PrimaryKey, pk.Kind);
        Assert.Equal(CompositeOrderLine, pk.Columns.Select(c => c.Name).ToArray());
    }

    [Fact]
    public async Task CreateTable_PrimaryKeyColumns_AreForcedNonNullable()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();
        const string TableName = "Pk_NonNull";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            // Default IsNullable=true on the Id column; the PK shortcut must override it to false.
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Name", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(TableName, TestContext.Current.CancellationToken);
        ColumnMetadata id = meta.Single(c => c.Name == "Id");
        ColumnMetadata name = meta.Single(c => c.Name == "Name");
        Assert.False(id.IsNullable);
        Assert.True(name.IsNullable);
    }

    [Fact]
    public async Task CreateTable_PrimaryKeyAlongsideRegularIndex_BothEmitted()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();
        const string TableName = "Pk_PlusIx";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Score", typeof(int)),
                ],
                [
                    new("IX_Score", "Score"),
                    new("PK_Id", "Id") { IsPrimaryKey = true },
                ],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(TableName, TestContext.Current.CancellationToken);

        Assert.Equal(2, indexes.Count);
        IndexMetadata pk = Assert.Single(indexes, i => i.Kind == IndexKind.PrimaryKey);
        IndexMetadata normal = Assert.Single(indexes, i => i.Kind == IndexKind.Normal);
        Assert.Equal("PK_Id", pk.Name);
        Assert.Equal("IX_Score", normal.Name);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task SinglePrimaryKey_OnInteger_ParticipatesInBulkRebuild(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }],
                ct);

            await writer.InsertRowsAsync(
                "T",
                [
                    [5],
                    [1],
                    [3],
                ],
                ct);
        }

        // PK leaf was rebuilt in bulk → most-recent leaf reports 3 entries
        // (maintenance applies to single-column PKs the same as normal IXes).
        Assert.Equal(3, await FindMaxLeafEntryCountAsync(stream, format, "T"));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task CompositePrimaryKey_OnInsert_ParticipatesInBulkRebuild(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("OrderId", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("LineNo", typeof(int)) { IsPrimaryKey = true },
                ],
                ct);

            await writer.InsertRowsAsync(
                "T",
                [
                    [5, 1],
                    [1, 2],
                    [3, 1],
                ],
                ct);
        }

        // Multi-column PK leaf is now maintained on bulk insert.
        Assert.Equal(3, await FindMaxLeafEntryCountAsync(stream, format, "T"));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task CompositePrimaryKey_OnUpdateAndDelete_LeafReflectsLatestState(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("OrderId", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("LineNo", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Note", typeof(string), maxLength: 50),
                ],
                ct);

            await writer.InsertRowsAsync(
                "T",
                [
                    [1, 1, "a"],
                    [1, 2, "b"],
                    [2, 1, "c"],
                ],
                ct);

            _ = await writer.UpdateRowsAsync(
                "T",
                "OrderId",
                1,
                new Dictionary<string, object?> { ["Note"] = "updated" },
                ct);

            _ = await writer.DeleteRowsAsync("T", "LineNo", 1, ct);
        }

        // After delete the latest (highest-page-number) leaf is the current
        // root and reports a single remaining entry; older leaves are
        // orphaned for Compact & Repair.
        Assert.Equal(1, await FindLatestLeafEntryCountAsync(stream, format, "T"));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task CompositePrimaryKey_SurvivesAddColumn_LeafRebuilt(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("A", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("B", typeof(int)) { IsPrimaryKey = true },
                ],
                ct);

            await writer.InsertRowsAsync(
                "T",
                [
                    [1, 1],
                    [2, 2],
                ],
                ct);

            await writer.AddColumnAsync(
                "T",
                new ColumnDefinition("C", typeof(string), maxLength: 10),
                ct);
        }

        // RewriteTableAsync forwards the composite PK and rebuilds the leaf
        // for the rewritten table.
        Assert.Equal(2, await FindLatestLeafEntryCountAsync(stream, format, "T"));
    }

    [Fact]
    public async Task PrimaryKey_SurvivesAddColumn()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Name", typeof(string), maxLength: 50),
                ],
                ct);

            await writer.InsertRowsAsync("T", [[1, "a"], [2, "b"]], ct);
            await writer.AddColumnAsync("T", new ColumnDefinition("Note", typeof(string), maxLength: 50), ct);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("T", TestContext.Current.CancellationToken);
        IndexMetadata pk = Assert.Single(indexes);
        Assert.Equal(IndexKind.PrimaryKey, pk.Kind);
        Assert.Equal("Id", Assert.Single(pk.Columns).Name);
    }

    [Fact]
    public async Task PrimaryKey_DroppedWhenAnyKeyColumnIsDropped()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("OrderId", typeof(int)),
                    new ColumnDefinition("LineNo", typeof(int)),
                    new ColumnDefinition("Sku", typeof(string), maxLength: 20),
                ],
                [
                    new IndexDefinition("PK_Order", CompositeOrderLine) { IsPrimaryKey = true },
                ],
                ct);

            await writer.DropColumnAsync("T", "LineNo", ct);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("T", TestContext.Current.CancellationToken);
        Assert.Empty(indexes);
    }

    [Fact]
    public async Task PrimaryKey_RemapsRenamedKeyColumn()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }],
                ct);

            await writer.RenameColumnAsync("T", "Id", "Identifier", ct);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("T", TestContext.Current.CancellationToken);
        IndexMetadata pk = Assert.Single(indexes);
        Assert.Equal(IndexKind.PrimaryKey, pk.Kind);
        Assert.Equal("Identifier", Assert.Single(pk.Columns).Name);
    }

    [Fact]
    public async Task IndexDefinition_AcceptsMultiColumn_WhenNotPrimaryKey()
    {
        // Multi-column non-PK indexes are accepted and emitted (live B-tree
        // maintenance applies when every key column type is supported by
        // IndexKeyEncoder).
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("A", typeof(int)),
                    new ColumnDefinition("B", typeof(int)),
                ],
                [new IndexDefinition("IX_AB", CompositeAB)],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("T", TestContext.Current.CancellationToken);
        IndexMetadata ix = Assert.Single(indexes);
        Assert.Equal(IndexKind.Normal, ix.Kind);
        Assert.Equal(CompositeAB, ix.Columns.Select(c => c.Name).ToArray());
    }

    [Fact]
    public async Task IndexDefinition_RejectsTwoPrimaryKeys()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();
        await using AccessWriter writer = await OpenWriterAsync(stream);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("A", typeof(int)),
                    new ColumnDefinition("B", typeof(int)),
                ],
                [
                    new IndexDefinition("PK_A", "A") { IsPrimaryKey = true },
                    new IndexDefinition("PK_B", "B") { IsPrimaryKey = true },
                ],
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ColumnFlag_AndExplicitPrimaryKeyIndex_AreMutuallyExclusive()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();
        await using AccessWriter writer = await OpenWriterAsync(stream);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("B", typeof(int)),
                ],
                [new IndexDefinition("PK_B", "B") { IsPrimaryKey = true }],
                TestContext.Current.CancellationToken));
    }

    // ── helpers (page scanning) ─────────────────────────────────

    private static int CountLeafEntries(byte[] fileBytes, int leafOffset, DatabaseFormat format)
    {
        // Subtract 1 for the sentinel bit at the position one past the last entry.
        int count = 1;
        for (int i = BitmaskOffset(format); i < FirstEntryOffset(format); i++)
        {
            byte b = fileBytes[leafOffset + i];
            for (int bit = 0; bit < 8; bit++)
            {
                if ((b & (1 << bit)) != 0)
                {
                    count++;
                }
            }
        }

        return count < 1 ? 0 : count - 1;
    }

    private static async ValueTask<int> FindMaxLeafEntryCountAsync(MemoryStream stream, DatabaseFormat format, string tableName)
    {
        long tdefPage = await GetTDefPageNumberAsync(stream, tableName);
        return FindMaxLeafEntryCount(stream.ToArray(), format, tdefPage);
    }

    private static int FindMaxLeafEntryCount(byte[] fileBytes, DatabaseFormat format, long parentTdefPage)
    {
        int pageSize = PageSizeOf(format);
        int max = 0;
        for (int p = 0; p < fileBytes.Length / pageSize; p++)
        {
            int o = p * pageSize;
            int parentTdef = fileBytes[o + 4] | (fileBytes[o + 5] << 8) | (fileBytes[o + 6] << 16) | (fileBytes[o + 7] << 24);
            if (fileBytes[o] == 0x04 && fileBytes[o + 1] == 0x01 && parentTdef == parentTdefPage)
            {
                int n = CountLeafEntries(fileBytes, o, format);
                if (n > max)
                {
                    max = n;
                }
            }
        }

        return max;
    }

    private static async ValueTask<int> FindLatestLeafEntryCountAsync(MemoryStream stream, DatabaseFormat format, string tableName)
    {
        long tdefPage = await GetTDefPageNumberAsync(stream, tableName);
        return FindLatestLeafEntryCount(stream.ToArray(), format, tdefPage);
    }

    private static int FindLatestLeafEntryCount(byte[] fileBytes, DatabaseFormat format, long parentTdefPage)
    {
        int pageSize = PageSizeOf(format);
        int latest = -1;
        for (int p = 0; p < fileBytes.Length / pageSize; p++)
        {
            int o = p * pageSize;
            int parentTdef = fileBytes[o + 4] | (fileBytes[o + 5] << 8) | (fileBytes[o + 6] << 16) | (fileBytes[o + 7] << 24);
            if (fileBytes[o] == 0x04 && fileBytes[o + 1] == 0x01 && parentTdef == parentTdefPage)
            {
                latest = p;
            }
        }

        return latest < 0 ? 0 : CountLeafEntries(fileBytes, latest * pageSize, format);
    }

    private static int PageSizeOf(DatabaseFormat fmt) =>
        fmt == DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet3 : Constants.PageSizes.Jet4;

    private static int BitmaskOffset(DatabaseFormat fmt) =>
        fmt == DatabaseFormat.Jet3Mdb ? Constants.IndexLeafPage.Jet3.BitmaskOffset : Constants.IndexLeafPage.Jet4.BitmaskOffset;

    private static int FirstEntryOffset(DatabaseFormat fmt) =>
        fmt == DatabaseFormat.Jet3Mdb ? Constants.IndexLeafPage.Jet3.FirstEntryOffset : Constants.IndexLeafPage.Jet4.FirstEntryOffset;

    private static async ValueTask<long> GetTDefPageNumberAsync(MemoryStream stream, string tableName)
    {
        await using AccessReader reader = await OpenReaderAsync(stream);
        CatalogEntry? entry = await reader.GetCatalogEntryAsync(tableName, TestContext.Current.CancellationToken);
        if (entry is null)
        {
            throw new InvalidOperationException($"Table '{tableName}' not found in catalog.");
        }

        return entry.TDefPage;
    }

    private static async ValueTask<MemoryStream> CreateFreshStreamAsync(DatabaseFormat format)
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        return ms;
    }

    private static async ValueTask<MemoryStream> CreateFreshAccdbStreamAsync()
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        return ms;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }
}
