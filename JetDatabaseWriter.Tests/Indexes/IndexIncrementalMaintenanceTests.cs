namespace JetDatabaseWriter.Tests.Indexes;

using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Round-trip tests for the single-leaf incremental B-tree maintenance
/// fast path. The fast path is engaged on insert/update/delete when the
/// index B-tree fits on a single leaf page; otherwise the writer falls back
/// to the bulk <c>MaintainIndexesAsync</c> rebuild. Tests cover:
/// <list type="bullet">
///   <item>Insert hits fast path → the single leaf page is rewritten in place.</item>
///   <item>Delete hits fast path → the single leaf is rewritten without the deleted row pointer.</item>
///   <item>Read-back of inserted/deleted rows is correct.</item>
///   <item>Index leaf entry count tracks the table row count after each
///   incremental mutation.</item>
/// </list>
/// </summary>
public sealed class IndexIncrementalMaintenanceTests
{
    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task SingleInsert_ReusesSingleLeafPage(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);
        }

        int leafCountBefore = CountLeafPages(stream.ToArray(), format);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.InsertRowAsync("T", [42], ct);
        }

        int leafCountAfter = CountLeafPages(stream.ToArray(), format);

        Assert.Equal(leafCountBefore, leafCountAfter);
        Assert.Equal(1, GetLatestLeafEntryCount(stream.ToArray(), format));

        await using var reader = await OpenReaderAsync(stream);
        var dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Single(dt!.Rows);
        Assert.Equal(42, dt.Rows[0]["Id"]);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task RepeatedSingleInserts_ReuseSingleLeaf_AllRowsReadable(DatabaseFormat format)
    {
        // Demonstrates the fast path advantage: 5 sequential single-row inserts
        // rewrite the existing single leaf without re-scanning the whole table.
        await using var stream = await CreateFreshStreamAsync(format);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);
        }

        int leafCountBefore = CountLeafPages(stream.ToArray(), format);

        await using (var writer = await OpenWriterAsync(stream))
        {
            for (int i = 1; i <= 5; i++)
            {
                await writer.InsertRowAsync("T", [i * 10], ct);
            }
        }

        int leafCountAfter = CountLeafPages(stream.ToArray(), format);
        Assert.Equal(leafCountBefore, leafCountAfter);

        // Latest leaf must hold all 5 entries.
        Assert.Equal(5, GetLatestLeafEntryCount(stream.ToArray(), format));

        await using var reader = await OpenReaderAsync(stream);
        var dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(5, dt!.Rows.Count);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task SingleDelete_ReusesSingleLeaf_WithReducedEntryCount(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);
            await writer.InsertRowsAsync(
                "T",
                [
                    [1],
                    [2],
                    [3],
                    [4],
                ],
                ct);
        }

        int leafCountBefore = CountLeafPages(stream.ToArray(), format);

        await using (var writer = await OpenWriterAsync(stream))
        {
            int deleted = await writer.DeleteRowsAsync("T", "Id", 2, ct);
            Assert.Equal(1, deleted);
        }

        int leafCountAfter = CountLeafPages(stream.ToArray(), format);
        Assert.Equal(leafCountBefore, leafCountAfter);
        Assert.Equal(3, GetLatestLeafEntryCount(stream.ToArray(), format));

        await using var reader = await OpenReaderAsync(stream);
        var dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(3, dt!.Rows.Count);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task UpdateRow_FastPath_RowReadableWithNewValue(DatabaseFormat format)
    {
        // Update is delete+insert on the same call; the fast path receives
        // both in a single hint and emits one new leaf per index.
        await using var stream = await CreateFreshStreamAsync(format);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Score", typeof(int)),
                ],
                [new IndexDefinition("IX_Score", "Score")],
                ct);
            await writer.InsertRowsAsync(
                "T",
                [
                    [1, 10],
                    [2, 20],
                    [3, 30],
                ],
                ct);

            int updated = await writer.UpdateRowsAsync(
                "T",
                "Id",
                2,
                new Dictionary<string, object?> { ["Score"] = 99 },
                ct);
            Assert.Equal(1, updated);
        }

        Assert.Equal(3, GetLatestLeafEntryCount(stream.ToArray(), format));

        await using var reader = await OpenReaderAsync(stream);
        var dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(3, dt!.Rows.Count);
        bool foundUpdated = false;
        foreach (DataRow row in dt.Rows)
        {
            if ((int)row["Id"] == 2)
            {
                Assert.Equal(99, row["Score"]);
                foundUpdated = true;
            }
        }

        Assert.True(foundUpdated);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task FastPath_FallsBackToBulk_WhenLeafOverflows(DatabaseFormat format)
    {
        // Small page-fitting table → fast path. Then push enough rows in a
        // single batch to spill the leaf → bulk path takes over (multi-page
        // tree). The end result must still be correct.
        await using var stream = await CreateFreshStreamAsync(format);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            // ~9 bytes per int entry; ~3616 byte payload area / 9 ≈ 400 entries
            // per leaf. Insert 800 to force a multi-leaf bulk rebuild.
            var rows = new object[800][];
            for (int i = 0; i < 800; i++)
            {
                rows[i] = [i + 1];
            }

            await writer.InsertRowsAsync("T", rows, ct);

            // Now insert one more row — fast path won't fit in the single
            // leaf (because the tree is already multi-level), so the bulk
            // path runs. Must succeed without corrupting the file.
            await writer.InsertRowAsync("T", [99999], ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        var dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(801, dt!.Rows.Count);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task FastPath_TextIndex_InsertReadableAfterIncrementalMaintenance(DatabaseFormat format)
    {
        // Text indexes are supported by the General Legacy encoder, so
        // single-row inserts should hit the fast path.
        await using var stream = await CreateFreshStreamAsync(format);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Code", typeof(string), maxLength: 32)],
                [new IndexDefinition("IX_Code", "Code")],
                ct);
            await writer.InsertRowAsync("T", ["alpha"], ct);
            await writer.InsertRowAsync("T", ["beta"], ct);
            await writer.InsertRowAsync("T", ["gamma"], ct);
        }

        Assert.Equal(3, GetLatestLeafEntryCount(stream.ToArray(), format));

        await using var reader = await OpenReaderAsync(stream);
        var dt = await reader.ReadDataTableAsync("T", cancellationToken: ct);
        Assert.NotNull(dt);
        Assert.Equal(3, dt!.Rows.Count);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task FastPath_UniqueIndex_PreCheckStillFires(DatabaseFormat format)
    {
        // The pre-write unique check pre-write unique-index check must still reject duplicates
        // even when the post-mutation index maintenance is incremental.
        await using var stream = await CreateFreshStreamAsync(format);

        await using var writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            "T",
            [new ColumnDefinition("Id", typeof(int))],
            [new IndexDefinition("UQ_Id", "Id") { IsUnique = true }],
            ct);

        await writer.InsertRowAsync("T", [1], ct);
        await writer.InsertRowAsync("T", [2], ct);

        await Assert.ThrowsAsync<System.InvalidOperationException>(async () =>
            await writer.InsertRowAsync("T", [1], ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task FastPath_Bails_WhenIndexedTdefDecodesNoRealIndexKeyColumns(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);
        }

        long tdefPage = await GetTDefPageNumberAsync(stream, "T");

        await using var reopened = await OpenWriterAsync(stream);
        var tableDef = await reopened.ReadRequiredTableDefAsync(tdefPage, "T", ct);
        await ClearRealIdxColMapsAsync(reopened, tdefPage, ct);

        var insertedRows = new List<(RowLocation Loc, object[] Row)>
        {
            (new RowLocation(10, 0, 0, 0), [1]),
        };

        var indexMaintainer = new IndexMaintainer(reopened, new PageAllocator(reopened));
        bool incremental = await indexMaintainer.TryMaintainIndexesIncrementalAsync(
            tdefPage,
            tableDef,
            insertedRows,
            deletedRows: null,
            ct);

        Assert.False(incremental);
    }

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

    private static int CountLeafPages(byte[] fileBytes, DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        int n = 0;
        for (int p = 0; p < fileBytes.Length / pageSize; p++)
        {
            int o = p * pageSize;
            if (fileBytes[o] == 0x04 && fileBytes[o + 1] == 0x01)
            {
                n++;
            }
        }

        return n;
    }

    private static int GetLatestLeafEntryCount(byte[] fileBytes, DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        int latest = -1;
        for (int p = 0; p < fileBytes.Length / pageSize; p++)
        {
            int o = p * pageSize;
            if (fileBytes[o] == 0x04 && fileBytes[o + 1] == 0x01)
            {
                latest = p;
            }
        }

        Assert.True(latest >= 0, "Expected at least one index leaf page in the file.");
        return CountLeafEntries(fileBytes, latest * pageSize, format);
    }

    private static int PageSizeOf(DatabaseFormat fmt) =>
        fmt == DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet3 : Constants.PageSizes.Jet4;

    private static int BitmaskOffset(DatabaseFormat fmt) =>
        fmt == DatabaseFormat.Jet3Mdb ? Constants.IndexLeafPage.Jet3.BitmaskOffset : Constants.IndexLeafPage.Jet4.BitmaskOffset;

    private static int FirstEntryOffset(DatabaseFormat fmt) =>
        fmt == DatabaseFormat.Jet3Mdb ? Constants.IndexLeafPage.Jet3.FirstEntryOffset : Constants.IndexLeafPage.Jet4.FirstEntryOffset;

    private static async ValueTask<long> GetTDefPageNumberAsync(MemoryStream stream, string tableName)
    {
        await using var reader = await OpenReaderAsync(stream);
        var entry = await reader.GetCatalogEntryAsync(tableName, TestContext.Current.CancellationToken);
        if (entry is null)
        {
            throw new System.InvalidOperationException($"Table '{tableName}' not found in catalog.");
        }

        return entry.TDefPage;
    }

    private static async ValueTask ClearRealIdxColMapsAsync(
        AccessWriter writer,
        long tdefPage,
        CancellationToken cancellationToken)
    {
        byte[] tdef = await writer.ReadPageAsync(tdefPage, cancellationToken);
        try
        {
            int numCols = Ru16(tdef, writer.tdef.NumCols);
            int numRealIdx = Ri32(tdef, writer.tdef.NumRealIdx);
            Assert.True(numRealIdx > 0, "Expected the test fixture to declare at least one real index.");

            int colStart = writer.tdef.BlockEnd + (numRealIdx * writer.tdef.RealIdxEntrySz);
            int namePos = colStart + (numCols * writer.colDesc.Size);
            for (int i = 0; i < numCols; i++)
            {
                int nameLength = writer.ReadColumnName(tdef, ref namePos, out _);
                Assert.True(nameLength >= 0, $"Failed to walk TDEF column name {i}.");
            }

            int realIdxDescStart = namePos;
            var layout = writer.indexLayout;
            for (int ri = 0; ri < numRealIdx; ri++)
            {
                bool decoded = layout.TryReadRealIdxSlotWithKeyColumns(
                    tdef,
                    realIdxDescStart,
                    ri,
                    out var slot,
                    out var keyCols);
                Assert.True(decoded, $"Failed to decode real-idx slot {ri}.");
                Assert.NotEmpty(keyCols);

                for (int colMapSlot = 0; colMapSlot < Constants.TableDefinition.ColMapSlotCount; colMapSlot++)
                {
                    int colMapOffset = layout.ColMapSlotOffset(slot.PhysStart, colMapSlot);
                    Wu16(tdef, colMapOffset, Constants.TableDefinition.ColMapPaddingSlot);
                    tdef[colMapOffset + 2] = 0;
                }
            }

            await writer.WritePageAsync(tdefPage, tdef, cancellationToken);
        }
        finally
        {
            AccessBase.ReturnPage(tdef);
        }
    }

    private static async ValueTask<MemoryStream> CreateFreshStreamAsync(DatabaseFormat format)
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
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
