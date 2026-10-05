namespace JetDatabaseWriter.Tests.Indexes.Collation;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

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
            ColumnInfo target = Assert.Single(definition.Columns, value => value.Name == column);
            int index = -1;
            for (int descriptor = 0; descriptor < definition.Columns.Count; descriptor++)
            {
                int numberOffset = descriptors + (descriptor * format.ColumnDescriptor.Size) + format.ColumnDescriptor.NumOff;
                if (BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(numberOffset, 2)) == target.ColNum)
                {
                    index = descriptor;
                    break;
                }
            }

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
