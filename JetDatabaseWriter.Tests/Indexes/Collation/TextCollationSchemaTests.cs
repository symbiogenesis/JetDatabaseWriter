namespace JetDatabaseWriter.Tests.Indexes.Collation;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Schema rewrites retain a stored text sort order and give new text the header default.</summary>
/// <param name="cache">Caches the fixture bytes.</param>
public sealed class TextCollationSchemaTests(DatabaseCache cache) : IClassFixture<DatabaseCache>
{
    public static TheoryData<string, WriteMode> Cases => new()
    {
        { TestDatabases.TestV1997, WriteMode.Direct },
        { TestDatabases.TestV1997, WriteMode.AutoCommit },
        { TestDatabases.TestV1997, WriteMode.ExplicitCommit },
        { TestDatabases.TestV2003, WriteMode.Direct },
        { TestDatabases.TestV2003, WriteMode.AutoCommit },
        { TestDatabases.TestV2003, WriteMode.ExplicitCommit },
        { TestDatabases.TestV2010, WriteMode.Direct },
        { TestDatabases.TestV2010, WriteMode.AutoCommit },
        { TestDatabases.TestV2010, WriteMode.ExplicitCommit },
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task AddDropAndRename_PreserveColumnSort_NewTextUsesHeader(string fixture, WriteMode mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await cache.CopyToStreamAsync(fixture, ct);
        TextSortOrder header;
        DatabaseFormat format;
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: ct))
        {
            header = harness.Database.Format.DefaultTextSortOrder;
            format = harness.Database.Format.Kind;
        }

        TextSortOrder stored = format == DatabaseFormat.Jet3Mdb
            ? new TextSortOrder(0, 0, false)
            : new TextSortOrder(0x0409, header.Version == 0 ? (byte)1 : (byte)0, true);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, ct))
        {
            await WriteModes.RunAsync(writer, mode, async () =>
            {
                await writer.CreateTableAsync(
                    "CollationSchema",
                    [
                        new ColumnDefinition("Id", typeof(int)),
                        new ColumnDefinition("Text", typeof(string), 40) { TextSortOrderOverride = stored },
                        new ColumnDefinition("Discard", typeof(int)),
                    ],
                    ct);
                await writer.InsertRowAsync("CollationSchema", [1, "caf\u00e9", 9], ct);
                await writer.AddColumnAsync("CollationSchema", new ColumnDefinition("AddedText", typeof(string), 40), ct);
                await writer.DropColumnAsync("CollationSchema", "Discard", ct);
                await writer.RenameColumnAsync("CollationSchema", "Text", "RenamedText", ct);
            }, ct);
        }

        Assert.Equal(stored, (await CollationTestSupport.ReadColumnAsync(stream, "CollationSchema", "RenamedText", ct)).TextSortOrder);
        Assert.Equal(header, (await CollationTestSupport.ReadColumnAsync(stream, "CollationSchema", "AddedText", ct)).TextSortOrder);
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        using System.Data.DataTable rows = await reader.ReadDataTableAsync("CollationSchema", cancellationToken: ct);
        Assert.Equal("caf\u00e9", Assert.Single(rows.Rows.Cast<System.Data.DataRow>())["RenamedText"]);
    }

    [Fact]
    public async Task NewJet3Descriptors_StampSortCodePageAndColumnNumberCopy()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("Stamped", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Text", typeof(string), 40)], ct);
        }

        stream.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: ct);
        Assert.Equal(new TextSortOrder(0x0409, 0, false), harness.Database.Format.DefaultTextSortOrder);
        foreach (string table in new[] { "MSysObjects", "Stamped" })
        {
            long pageNumber = table == "MSysObjects"
                ? await harness.Services.Catalog.FindSystemTablePageAsync(table, ct)
                : Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync(table, ct)).TDefPage;
            TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(pageNumber, ct));
            byte[] page = await harness.ReadPageCopyAsync(pageNumber, ct);
            JetFormat format = harness.Database.Format;
            int realIndexes = BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(format.TDef.NumRealIdx, 4));
            int descriptors = format.TDef.BlockEnd + (realIndexes * format.TDef.RealIdxEntrySz);
            for (int ordinal = 0; ordinal < definition.Columns.Count; ordinal++)
            {
                int start = descriptors + (ordinal * format.ColumnDescriptor.Size);
                Assert.Equal(definition.Columns[ordinal].ColNum, BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(start + 5, 2)));
                Assert.Equal((ushort)1252, BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(start + 11, 2)));
                if (definition.Columns[ordinal].Type == ColumnType.TextType)
                {
                    Assert.Equal((ushort)0x0409, BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(start + 9, 2)));
                }
            }
        }
    }
}

/// <summary>Raw descriptor helpers shared by the collation acceptance tests.</summary>
internal static class CollationTestSupport
{
    public static async Task<ColumnInfo> ReadColumnAsync(MemoryStream stream, string table, string column, CancellationToken ct)
    {
        stream.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: ct);
        long tablePage = table == "MSysObjects" ? await harness.Services.Catalog.FindSystemTablePageAsync(table, ct) : Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync(table, ct)).TDefPage;
        TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(tablePage, ct));
        return Assert.Single(definition.Columns, value => value.Name == column);
    }

    public static async Task PatchSortOrderAsync(MemoryStream stream, string table, string column, ushort sortOrder, CancellationToken ct)
    {
        stream.Position = 0;
        long offset;
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: ct))
        {
            long tablePage = table == "MSysObjects" ? await harness.Services.Catalog.FindSystemTablePageAsync(table, ct) : Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync(table, ct)).TDefPage;
            TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(tablePage, ct));
            JetFormat format = harness.Database.Format;
            byte[] page = await harness.ReadPageCopyAsync(tablePage, ct);
            int realIndexes = BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(format.TDef.NumRealIdx, 4));
            int descriptors = format.TDef.BlockEnd + (realIndexes * format.TDef.RealIdxEntrySz);
            int index = definition.FindColumnIndex(column);
            Assert.True(index >= 0);
            int sortOffset = format.Kind == DatabaseFormat.Jet3Mdb ? 9 : 11;
            offset = (tablePage * format.PageSize) + descriptors + (index * format.ColumnDescriptor.Size) + sortOffset;
        }

        byte[] bytes = new byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(bytes, sortOrder);
        stream.Position = offset;
        await stream.WriteAsync(bytes, ct);
        stream.Position = 0;
        Assert.Equal(sortOrder, (await ReadColumnAsync(stream, table, column, ct)).TextSortOrder.Value);
    }
}
