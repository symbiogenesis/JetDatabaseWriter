namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>How a complex-column test drives the writer.</summary>
[SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "Theory parameters of public xUnit test methods must be public.")]
public enum ComplexWriteMode
{
    /// <summary>No transaction; every page write goes straight to the stream.</summary>
    Direct = 0,

    /// <summary><see cref="AccessWriterOptions.UseTransactionalWrites"/> wraps each call in its own transaction.</summary>
    AutoCommit = 1,

    /// <summary>The mutations run inside one explicit transaction that is committed.</summary>
    ExplicitCommit = 2,
}

/// <summary>
/// Shared helpers for the complex-column writer tests: fixture copies, write
/// modes, raw parent slots (<see cref="ComplexIdRef"/>), TDEF header fields
/// and index leaf keys, all read through the library's internal layers.
/// </summary>
internal static class ComplexColumnTestSupport
{
    /// <summary>Offset of the ACE TDEF complex AutoNumber (the last per-row complex reference handed out).</summary>
    public const int ComplexAutoNumberOffset = 28;

    /// <summary>Offset of the Jet4/ACE TDEF AutoNumber counter.</summary>
    public const int AutoNumberOffset = 20;

    public static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every write mode.</summary>
    public static TheoryData<ComplexWriteMode> AllModes =>
    [
        ComplexWriteMode.Direct,
        ComplexWriteMode.AutoCommit,
        ComplexWriteMode.ExplicitCommit,
    ];

    public static AccessWriterOptions WriterOptions(ComplexWriteMode mode) => new()
    {
        UseLockFile = false,
        UseTransactionalWrites = mode == ComplexWriteMode.AutoCommit,
    };

    /// <summary>Returns a writable in-memory copy of an Access-authored fixture.</summary>
    /// <param name="path">The fixture path.</param>
    public static async Task<MemoryStream> CopyFixtureAsync(string path)
    {
        byte[] bytes = await File.ReadAllBytesAsync(path, Ct);
        var ms = new MemoryStream();
        await ms.WriteAsync(bytes, Ct);
        ms.Position = 0;
        return ms;
    }

    public static async Task<AccessWriter> CreateWriterAsync(MemoryStream ms, ComplexWriteMode mode = ComplexWriteMode.Direct) =>
        await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, WriterOptions(mode), leaveOpen: true, cancellationToken: Ct);

    public static async Task<AccessWriter> OpenWriterAsync(MemoryStream ms, ComplexWriteMode mode = ComplexWriteMode.Direct)
    {
        ms.Position = 0;
        return await AccessWriter.OpenAsync(ms, WriterOptions(mode), leaveOpen: true, cancellationToken: Ct);
    }

    public static async Task<AccessReader> OpenReaderAsync(MemoryStream ms)
    {
        ms.Position = 0;
        return await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
    }

    /// <summary>Runs <paramref name="work"/> directly, or inside one committed transaction for <see cref="ComplexWriteMode.ExplicitCommit"/>.</summary>
    /// <param name="writer">The writer.</param>
    /// <param name="mode">The write mode.</param>
    /// <param name="work">The mutations.</param>
    public static async Task RunAsync(AccessWriter writer, ComplexWriteMode mode, Func<Task> work)
    {
        if (mode != ComplexWriteMode.ExplicitCommit)
        {
            await work();
            return;
        }

        await using JetTransaction tx = await writer.BeginTransactionAsync(Ct);
        await work();
        await tx.CommitAsync(Ct);
    }

    /// <summary>
    /// Reads a user table's TDEF page and its live rows as the writer's
    /// snapshot decodes them, so complex slots come back as
    /// <see cref="ComplexIdRef"/> (or DBNull when the slot is null).
    /// </summary>
    /// <param name="ms">The database stream.</param>
    /// <param name="tableName">The user table name.</param>
    public static async Task<RawTable> ReadRawTableAsync(MemoryStream ms, string tableName)
    {
        ms.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: Ct);
        ResolvedTable table = await harness.Services.Catalog.ResolveRequiredTableAsync(tableName, Ct);
        List<LocatedRow> rows = await harness.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, Ct);
        byte[] tdef = await harness.Database.ReadPageCopyAsync(table.Entry.TDefPage, Ct);
        return new RawTable(table.Definition, [.. rows.Select(r => r.Values)], tdef);
    }

    /// <summary>
    /// Rewrites a table the way builds before per-row reference allocation
    /// left it: every complex slot of every row null and the TDEF complex
    /// AutoNumber 0. Flat rows are left alone.
    /// </summary>
    /// <param name="ms">The database stream.</param>
    /// <param name="tableName">The user table name.</param>
    public static async Task ClearComplexReferencesAsync(MemoryStream ms, string tableName)
    {
        ms.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: Ct);
        DatabaseFile db = harness.Database;
        ResolvedTable table = await harness.Services.Catalog.ResolveRequiredTableAsync(tableName, Ct);
        foreach (RowLocation location in await db.GetLiveRowLocationsAsync(table.Entry.TDefPage, Ct))
        {
            byte[] page = await db.ReadPageCopyAsync(location.PageNumber, Ct);
            int nullMaskSize = JetTypeInfo.GetNullMaskSizeBytes(db.ReadRowColumnCount(page, location.RowStart));
            Span<byte> nullMask = page.AsSpan(location.RowStart + location.RowSize - nullMaskSize, nullMaskSize);
            foreach (ColumnInfo column in table.Definition.Columns.Where(c => c.Type is ColumnType.ComplexType or ColumnType.AttachmentType))
            {
                JetTypeInfo.SetNullMaskBit(nullMask, column.ColNum, false);
                page.AsSpan(location.RowStart + db.RowFields.NumCols + column.FixedOff, 4).Clear();
            }

            await db.WritePageAsync(location.PageNumber, page, Ct);
        }

        byte[] tdef = await db.ReadPageCopyAsync(table.Entry.TDefPage, Ct);
        tdef.AsSpan(ComplexAutoNumberOffset, 4).Clear();
        await db.WritePageAsync(table.Entry.TDefPage, tdef, Ct);
    }

    /// <summary>Reads the TDEF page of a system table such as <c>MSysComplexColumns</c>.</summary>
    /// <param name="ms">The database stream.</param>
    /// <param name="systemTableName">The system table name.</param>
    public static async Task<byte[]> ReadSystemTdefAsync(MemoryStream ms, string systemTableName)
    {
        ms.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: Ct);
        long page = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync(systemTableName, Ct);
        Assert.True(page > 0, $"System table '{systemTableName}' was not found.");
        return await harness.Database.ReadPageCopyAsync(page, Ct);
    }

    /// <summary>Returns the per-row complex reference held in <paramref name="row"/>'s slot for <paramref name="column"/>, or null.</summary>
    /// <param name="table">The raw table.</param>
    /// <param name="row">The raw row.</param>
    /// <param name="column">The complex column name.</param>
    public static int? Slot(RawTable table, object[] row, string column)
        => row[table.Definition.FindColumnIndex(column)] is ComplexIdRef reference ? reference.Id : null;

    /// <summary>Returns every key in the leaf chain of the index named <paramref name="indexName"/>, in leaf order.</summary>
    /// <param name="ms">The database stream.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="indexName">The index name.</param>
    public static async Task<List<byte[]>> ReadIndexLeafKeysAsync(MemoryStream ms, string tableName, string indexName)
    {
        IndexMetadata index;
        DatabaseFormat format;
        int pageSize;
        await using (AccessReader reader = await OpenReaderAsync(ms))
        {
            index = Assert.Single(await reader.ListIndexesAsync(tableName, Ct), i => i.Name == indexName);
            format = reader.DatabaseFormat;
            pageSize = reader.PageSize;
        }

        ms.Position = 0;
        await using ReaderHarness pages = await ReaderHarness.OpenAsync(ms, cancellationToken: Ct);
        return await CollectLeafKeysAsync(pages, IndexPageLayout.ForFormat(format), pageSize, index.FirstDp, Ct);
    }

    /// <summary>Walks from <paramref name="rootPage"/> to the leftmost leaf, then returns every key along the leaf sibling chain.</summary>
    /// <param name="pages">The page reader.</param>
    /// <param name="layout">The index page layout.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="rootPage">The index root page.</param>
    /// <param name="ct">The cancellation token.</param>
    public static async Task<List<byte[]>> CollectLeafKeysAsync(
        ReaderHarness pages,
        IndexPageLayout layout,
        int pageSize,
        long rootPage,
        CancellationToken ct)
    {
        long current = rootPage;
        for (int depth = 0; depth < 32; depth++)
        {
            byte[] page = await pages.ReadPageCopyAsync(current, ct);
            if (page[0] == Constants.IndexLeafPage.PageTypeLeaf)
            {
                break;
            }

            Assert.Equal(Constants.IndexLeafPage.PageTypeIntermediate, page[0]);
            List<DecodedIntermediateEntry> entries = IndexPageCodec.DecodeIntermediateEntries(layout, page, pageSize);
            Assert.NotEmpty(entries);
            current = entries[0].ChildPage;
        }

        var result = new List<byte[]>();
        int visits = 0;
        while (current != 0)
        {
            Assert.True(++visits < 100_000, "Leaf chain exceeds the visit guard.");
            byte[] page = await pages.ReadPageCopyAsync(current, ct);
            Assert.Equal(Constants.IndexLeafPage.PageTypeLeaf, page[0]);
            foreach (IndexEntry entry in IndexPageCodec.DecodeLeafEntries(layout, page, pageSize))
            {
                result.Add(entry.Key);
            }

            (long _, long next, long _) = IndexPageCodec.ReadSiblingPointers(layout, page);
            current = next;
        }

        return result;
    }

    /// <summary>
    /// Builds the attachment content stream Access writes (and Jackcess
    /// <c>encodeData</c>): <c>headerLen</c>, the flag 1, the extension length in
    /// characters including its NUL, the UTF-16LE extension and NUL, then the file.
    /// </summary>
    /// <param name="extension">The extension, as stored.</param>
    /// <param name="payload">The file bytes.</param>
    public static byte[] AccessContent(string extension, byte[] payload)
    {
        byte[] ext = Encoding.Unicode.GetBytes(extension + "\0");
        byte[] content = new byte[12 + ext.Length + payload.Length];
        BinaryPrimitives.WriteInt32LittleEndian(content, 12 + ext.Length);
        BinaryPrimitives.WriteInt32LittleEndian(content.AsSpan(4), 1);
        BinaryPrimitives.WriteInt32LittleEndian(content.AsSpan(8), extension.Length + 1);
        ext.CopyTo(content, 12);
        payload.CopyTo(content, 12 + ext.Length);
        return content;
    }

    /// <summary>Computes the zlib Adler-32 checksum of <paramref name="data"/> (RFC 1950).</summary>
    /// <param name="data">The data.</param>
    public static uint Adler32(byte[] data)
    {
        long a = 1;
        long b = 0;
        foreach (byte value in data)
        {
            a = (a + value) % 65521;
            b = (b + a) % 65521;
        }

        return (uint)((b << 16) | a);
    }

    /// <summary>Inflates a zlib stream (header, deflate blocks and Adler-32 trailer).</summary>
    /// <param name="zlib">The zlib stream bytes.</param>
    public static byte[] InflateZlib(byte[] zlib)
    {
        using var input = new MemoryStream(zlib);
        using var inflater = new ZLibStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        inflater.CopyTo(output);
        return output.ToArray();
    }

    /// <summary>Reads a little-endian int32 from a TDEF page copy.</summary>
    /// <param name="tdef">The TDEF page.</param>
    /// <param name="offset">The byte offset.</param>
    public static int ReadInt32(byte[] tdef, int offset) => BinaryPrimitives.ReadInt32LittleEndian(tdef.AsSpan(offset, 4));

    /// <summary>A table's definition, live rows (snapshot-decoded) and TDEF page.</summary>
    /// <param name="Definition">The table definition.</param>
    /// <param name="Rows">The live rows in page order.</param>
    /// <param name="Tdef">A copy of the first TDEF page.</param>
    internal sealed record RawTable(TableDef Definition, List<object[]> Rows, byte[] Tdef)
    {
        /// <summary>Gets the TDEF complex AutoNumber (ACE offset 28).</summary>
        public int ComplexAutoNumber => ReadInt32(this.Tdef, ComplexAutoNumberOffset);
    }
}
