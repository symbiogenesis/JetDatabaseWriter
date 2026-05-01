namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Round-trip tests for primary-key emission.
/// <para>
/// PK is emitted as a logical-index entry with <c>index_type = 0x01</c> and
/// is implicitly unique. Multi-column PKs participate in the bulk B-tree
/// rebuild via the composite-key concatenation path, provided every key
/// column's type is supported by <c>IndexKeyEncoder</c>.
/// </para>
/// </summary>
public sealed class PrimaryKeyWriterTests
{
    private static readonly string[] CompositeOrderLine = ["OrderId", "LineNo"];
    private static readonly string[] CompositeAB = ["A", "B"];
    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    [Fact]
    public async Task CreateTable_WithSingleColumnPrimaryKey_ViaIndexDefinition_RoundTrips()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string TableName = "Pk_Single";

        await using (var writer = await OpenWriterAsync(stream))
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

        await using var reader = await OpenReaderAsync(stream);
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
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string TableName = "Pk_ColFlag";

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Name", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
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
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string TableName = "Pk_Composite";

        await using (var writer = await OpenWriterAsync(stream))
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

        await using var reader = await OpenReaderAsync(stream);
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
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string TableName = "Pk_CompositeFlag";

        await using (var writer = await OpenWriterAsync(stream))
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

        await using var reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(TableName, TestContext.Current.CancellationToken);

        IndexMetadata pk = Assert.Single(indexes);
        Assert.Equal("PrimaryKey", pk.Name);
        Assert.Equal(IndexKind.PrimaryKey, pk.Kind);
        Assert.Equal(CompositeOrderLine, pk.Columns.Select(c => c.Name).ToArray());
    }

    [Fact]
    public async Task CreateTable_PrimaryKeyColumns_AreForcedNonNullable()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string TableName = "Pk_NonNull";

        await using (var writer = await OpenWriterAsync(stream))
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

        await using var reader = await OpenReaderAsync(stream);
        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(TableName, TestContext.Current.CancellationToken);
        ColumnMetadata id = meta.Single(c => c.Name == "Id");
        ColumnMetadata name = meta.Single(c => c.Name == "Name");
        Assert.False(id.IsNullable);
        Assert.True(name.IsNullable);
    }

    [Fact]
    public async Task CreateTable_PrimaryKeyAlongsideRegularIndex_BothEmitted()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string TableName = "Pk_PlusIx";

        await using (var writer = await OpenWriterAsync(stream))
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

        await using var reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(TableName, TestContext.Current.CancellationToken);

        Assert.Equal(2, indexes.Count);
        IndexMetadata pk = Assert.Single(indexes, i => i.Kind == IndexKind.PrimaryKey);
        IndexMetadata normal = Assert.Single(indexes, i => i.Kind == IndexKind.Normal);
        Assert.Equal("PK_Id", pk.Name);
        Assert.Equal("IX_Score", normal.Name);
    }

    [Fact]
    public async Task SinglePrimaryKey_OnInteger_ParticipatesInBulkRebuild()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();

        await using (var writer = await OpenWriterAsync(stream))
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
        Assert.Equal(3, FindMaxLeafEntryCount(stream.ToArray()));
    }

    [Fact]
    public async Task CompositePrimaryKey_OnInsert_ParticipatesInBulkRebuild()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();

        await using (var writer = await OpenWriterAsync(stream))
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
        Assert.Equal(3, FindMaxLeafEntryCount(stream.ToArray()));
    }

    [Fact]
    public async Task CompositePrimaryKey_OnUpdateAndDelete_LeafReflectsLatestState()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();

        await using (var writer = await OpenWriterAsync(stream))
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
                new Dictionary<string, object> { ["Note"] = "updated" },
                ct);

            _ = await writer.DeleteRowsAsync("T", "LineNo", 1, ct);
        }

        // After delete the latest (highest-page-number) leaf is the current
        // root and reports a single remaining entry; older leaves are
        // orphaned for Compact & Repair.
        Assert.Equal(1, FindLatestLeafEntryCount(stream.ToArray()));
    }

    [Fact]
    public async Task CompositePrimaryKey_SurvivesAddColumn_LeafRebuilt()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();

        await using (var writer = await OpenWriterAsync(stream))
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
        Assert.Equal(2, FindLatestLeafEntryCount(stream.ToArray()));
    }

    [Fact]
    public async Task PrimaryKey_SurvivesAddColumn()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();

        await using (var writer = await OpenWriterAsync(stream))
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

        await using var reader = await OpenReaderAsync(stream);
        var indexes = await reader.ListIndexesAsync("T", TestContext.Current.CancellationToken);
        IndexMetadata pk = Assert.Single(indexes);
        Assert.Equal(IndexKind.PrimaryKey, pk.Kind);
        Assert.Equal("Id", Assert.Single(pk.Columns).Name);
    }

    [Fact]
    public async Task PrimaryKey_DroppedWhenAnyKeyColumnIsDropped()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();

        await using (var writer = await OpenWriterAsync(stream))
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

        await using var reader = await OpenReaderAsync(stream);
        var indexes = await reader.ListIndexesAsync("T", TestContext.Current.CancellationToken);
        Assert.Empty(indexes);
    }

    [Fact]
    public async Task PrimaryKey_RemapsRenamedKeyColumn()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }],
                ct);

            await writer.RenameColumnAsync("T", "Id", "Identifier", ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        var indexes = await reader.ListIndexesAsync("T", TestContext.Current.CancellationToken);
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
        await using var stream = await CreateFreshAccdbStreamAsync();

        await using (var writer = await OpenWriterAsync(stream))
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

        await using var reader = await OpenReaderAsync(stream);
        var indexes = await reader.ListIndexesAsync("T", TestContext.Current.CancellationToken);
        IndexMetadata ix = Assert.Single(indexes);
        Assert.Equal(IndexKind.Normal, ix.Kind);
        Assert.Equal(CompositeAB, ix.Columns.Select(c => c.Name).ToArray());
    }

    [Fact]
    public async Task IndexDefinition_RejectsTwoPrimaryKeys()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        await using var writer = await OpenWriterAsync(stream);

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
        await using var stream = await CreateFreshAccdbStreamAsync();
        await using var writer = await OpenWriterAsync(stream);

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

    // --- helpers (page scanning) ---------------------------------------------

    private static int CountLeafEntries(byte[] fileBytes, int leafOffset)
    {
        int count = 1;
        for (int i = Constants.IndexLeafPage.Jet4BitmaskOffset; i < Constants.IndexLeafPage.Jet4FirstEntryOffset; i++)
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

        return count;
    }

    private static int FindMaxLeafEntryCount(byte[] fileBytes)
    {
        int max = 0;
        for (int p = 0; p < fileBytes.Length / Constants.PageSizes.Jet4; p++)
        {
            int o = p * Constants.PageSizes.Jet4;
            if (fileBytes[o] == 0x04 && fileBytes[o + 1] == 0x01)
            {
                int n = CountLeafEntries(fileBytes, o);
                if (n > max)
                {
                    max = n;
                }
            }
        }

        return max;
    }

    private static int FindLatestLeafEntryCount(byte[] fileBytes)
    {
        int latest = -1;
        for (int p = 0; p < fileBytes.Length / Constants.PageSizes.Jet4; p++)
        {
            int o = p * Constants.PageSizes.Jet4;
            if (fileBytes[o] == 0x04 && fileBytes[o + 1] == 0x01)
            {
                latest = p;
            }
        }

        return latest < 0 ? 0 : CountLeafEntries(fileBytes, latest * Constants.PageSizes.Jet4);
    }

    private static async ValueTask<MemoryStream> CreateFreshAccdbStreamAsync()
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
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
