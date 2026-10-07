namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Tests.ComplexColumns.ComplexColumnTestSupport;

/// <summary>Checks physical index entries and row counts after complex-column deletes.</summary>
public sealed class ComplexColumnDeleteIndexIntegrityTests
{
    private static readonly int[] ParentIds = [1, 2];

    /// <summary>Gets every writer mode.</summary>
    public static TheoryData<WriteMode> Modes => AllModes;

    [Fact]
    public async Task SecureParentDelete_ScrubsAttachmentLongValue()
    {
        await using var stream = new MemoryStream();
        byte[] payload = new byte[20_000];
        for (int index = 0; index < payload.Length; index++)
        {
            payload[index] = (byte)(((index * 73) + (index / 251)) & 0xFF);
        }

        var options = new AccessWriterOptions { UseLockFile = false, SecureEraseMode = JetDatabaseWriter.Enums.SecureEraseMode.DeletedRowsAndFreedPages };
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, JetDatabaseWriter.Enums.DatabaseFormat.AceAccdb, options, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("Docs", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }], Ct);
            await writer.InsertRowAsync("Docs", [1, DBNull.Value], Ct);
            await writer.AddAttachmentAsync("Docs", "Files", new Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("private.zip", payload), Ct);
            Assert.True(ContainsPayload(stream, payload));
            Assert.Equal(1, await writer.DeleteRowsAsync("Docs", "Id", 1, Ct));
        }

        Assert.False(ContainsPayload(stream, payload));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task DropComplexTable_KeepsCatalogCountsAndIndexesCurrent(WriteMode mode)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(stream, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                await writer.CreateTableAsync("Docs", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }], Ct);
                await writer.CreateTableAsync("Keep", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }], Ct);
                await writer.DropTableAsync("Docs", Ct);
            });
        }

        foreach (string table in new[] { "MSysObjects", "MSysComplexColumns" })
        {
            long page;
            int liveCount;
            stream.Position = 0;
            await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct))
            {
                page = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync(table, Ct);
                liveCount = (await harness.Database.GetLiveRowLocationsAsync(page, Ct)).Count;
                byte[] tdef = await harness.Database.Pages.ReadPageCopyAsync(page, Ct);
                Assert.Equal((uint)liveCount, System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(tdef.AsSpan(harness.Database.Format.TDef.NumRows)));
            }

            IReadOnlyList<IndexMetadata> indexes;
            await using (AccessReader reader = await OpenReaderAsync(stream))
            {
                indexes = await reader.ListIndexesAsync(table, Ct);
            }

            Assert.NotEmpty(indexes);
            foreach (IndexMetadata index in indexes)
            {
                Assert.Equal(liveCount, (await ReadIndexLeafKeysAsync(stream, table, index.Name)).Count);
            }
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task ParentDelete_RemovesFlatIndexEntries(WriteMode mode)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(stream, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                await writer.CreateTableAsync("Docs", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }, new ColumnDefinition("Tags", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(int) }], Ct);
                await writer.InsertRowsAsync("Docs", [[1, DBNull.Value, DBNull.Value], [2, DBNull.Value, DBNull.Value]], Ct);
                foreach (int id in ParentIds)
                {
                    var key = new Dictionary<string, object?> { ["Id"] = id };
                    await writer.AddAttachmentAsync("Docs", "Files", key, new AttachmentInput($"{id}.txt", [1, 2, 3]), Ct);
                    await writer.AddMultiValueItemAsync("Docs", "Tags", key, id, Ct);
                }

                Assert.Equal(1, await writer.DeleteRowsAsync("Docs", "Id", 1, Ct));
            });
        }

        IReadOnlyList<ComplexColumnInfo> columns;
        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            columns = await reader.GetComplexColumnsAsync("Docs", Ct);
        }

        Assert.Equal((1L, 1), await FlatTableRowCounts.ReadAsync(stream, "Docs", "Files", Ct));
        Assert.Equal((1L, 1), await FlatTableRowCounts.ReadAsync(stream, "Docs", "Tags", Ct));

        foreach (ComplexColumnInfo column in columns)
        {
            IReadOnlyList<IndexMetadata> indexes;
            await using (AccessReader reader = await OpenReaderAsync(stream))
            {
                indexes = await reader.ListIndexesAsync(column.FlatTableName, Ct);
                Assert.Equal(1, (await reader.ReadTableAsync(column.FlatTableName, cancellationToken: Ct))!.Rows.Count);
            }

            foreach (IndexMetadata index in indexes)
            {
                Assert.Single(await ReadIndexLeafKeysAsync(stream, column.FlatTableName, index.Name));
            }
        }
    }

    private static bool ContainsPayload(MemoryStream stream, byte[] payload)
        => stream.ToArray().AsSpan().IndexOf(payload.AsSpan(1_000, 100)) >= 0;
}
