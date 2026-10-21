namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Tests.ComplexColumns.ComplexColumnTestSupport;

/// <summary>Individual complex items retain their native identities and change atomically.</summary>
public sealed class ComplexItemMutationTests
{
    /// <summary>Gets the supported statement and transaction modes.</summary>
    public static TheoryData<WriteMode> Modes => AllModes;

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task ReplaceAndRemove_KeepSiblingItemsIdentitiesCountsAndIndexes(WriteMode mode)
    {
        await using var stream = new MemoryStream();
        await SeedAsync(stream);
        await using (AccessWriter writer = await OpenWriterAsync(stream, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                await writer.ReplaceAttachmentAsync("Parents", "Files", Key(1), 1, new AttachmentInput("changed.zip", new byte[20_000]), Ct);
                await writer.RemoveAttachmentAsync("Parents", "Files", Key(1), 2, Ct);
                await writer.ReplaceMultiValueItemAsync("Parents", "Items", Key(1), 1, "changed", Ct);
                await writer.RemoveMultiValueItemAsync("Parents", "Items", Key(1), 2, Ct);
                await writer.AddMultiValueItemAsync("Parents", "Items", Key(1), "later", Ct);
            });
        }

        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            IReadOnlyList<AttachmentRecord> files = await reader.GetAttachmentsAsync("Parents", "Files", Ct);
            Assert.Equal([1, 3], files.Select(file => file.Id).Order());
            Assert.Equal(["changed.zip", "other.txt"], files.Select(file => file.FileName).Order(StringComparer.Ordinal));
            Assert.Equal(20_000, Assert.Single(files, file => file.FileName == "changed.zip").FileData.Length);
            IReadOnlyList<MultiValueItem> items = await reader.GetMultiValueItemsAsync("Parents", "Items", Ct);
            Assert.Equal([1, 3, 4], items.Select(item => item.Id).Order());
            Assert.Equal(["changed", "later", "other"], items.Select(item => Assert.IsType<string>(item.Value)).Order(StringComparer.Ordinal));
            ComplexColumnInfo info = Assert.Single(await reader.GetComplexColumnsAsync("Parents", Ct), column => column.ColumnName == "Items");
            using DataTable flat = await reader.ReadTableAsync(info.FlatTableName, cancellationToken: Ct);
            Assert.Equal([1, 3, 4], flat.Rows.Cast<DataRow>().Select(row => Assert.IsType<int>(row["Parents_Items"])).Order());
        }

        foreach (string column in new[] { "Files", "Items" })
        {
            int expected = column == "Files" ? 2 : 3;
            Assert.Equal((expected, expected), await FlatTableRowCounts.ReadAsync(stream, "Parents", column, Ct));
            IReadOnlyList<IndexMetadata> indexes;
            string flatName;
            await using (AccessReader reader = await OpenReaderAsync(stream))
            {
                flatName = Assert.Single(await reader.GetComplexColumnsAsync("Parents", Ct), info => info.ColumnName == column).FlatTableName;
                indexes = await reader.ListIndexesAsync(flatName, Ct);
            }

            foreach (IndexMetadata index in indexes)
            {
                Assert.Equal(expected, (await ReadIndexLeafKeysAsync(stream, flatName, index.Name)).Count);
            }
        }
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task InvalidTargetAndDuplicateReplacement_LeaveBytesAndCounterUnchanged(WriteMode mode)
    {
        await using var stream = new MemoryStream();
        await SeedAsync(stream);
        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(stream, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                JetConstraintException duplicate = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                    await writer.ReplaceMultiValueItemAsync("Parents", "Items", Key(1), 1, "two", Ct));
                Assert.Equal(JetErrorCode.UniqueViolation, duplicate.ErrorCode);
                JetOperationException wrongParent = await Assert.ThrowsAsync<JetOperationException>(async () =>
                    await writer.RemoveAttachmentAsync("Parents", "Files", Key(2), 1, Ct));
                Assert.Equal(JetErrorCode.RowNotFound, wrongParent.ErrorCode);
                JetOperationException missing = await Assert.ThrowsAsync<JetOperationException>(async () =>
                    await writer.ReplaceMultiValueItemAsync("Parents", "Items", Key(1), 99, "missing", Ct));
                Assert.Equal(JetErrorCode.RowNotFound, missing.ErrorCode);
                await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
                    await writer.RemoveMultiValueItemAsync("Parents", "Items", Key(1), 0, Ct));
                Assert.Equal(before, stream.ToArray());
            });
        }

        Assert.Equal(before, stream.ToArray());
    }

    [Fact]
    public async Task ExplicitRollback_RestoresRemovedAndReplacedItems()
    {
        await using var stream = new MemoryStream();
        await SeedAsync(stream);
        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
            await writer.ReplaceAttachmentAsync("Parents", "Files", Key(1), 1, new AttachmentInput("changed.txt", [9]), Ct);
            await writer.RemoveMultiValueItemAsync("Parents", "Items", Key(1), 1, Ct);
            await transaction.RollbackAsync(Ct);
        }

        Assert.Equal(before, stream.ToArray());
    }

    [Fact]
    public async Task UnderscoreParent_DistinguishesItemIdentityFromParentReference()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(stream))
        {
            await writer.CreateTableAsync("_Parents", [new("Id", typeof(int)), new("Items", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(string) }], Ct);
            await writer.InsertRowsAsync("_Parents", [[1, DBNull.Value], [2, DBNull.Value]], Ct);
            await writer.AddMultiValueItemAsync("_Parents", "Items", Key(2), "two", Ct);
            await writer.AddMultiValueItemAsync("_Parents", "Items", Key(1), "one", Ct);
            await writer.ReplaceMultiValueItemAsync("_Parents", "Items", Key(2), 1, "changed", Ct);
            await writer.RemoveMultiValueItemAsync("_Parents", "Items", Key(1), 2, Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        MultiValueItem item = Assert.Single(await reader.GetMultiValueItemsAsync("_Parents", "Items", Ct));
        Assert.Equal(2, item.ConceptualTableId);
        Assert.Equal("changed", item.Value);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task WriteAndFlushFaults_RestoreCompleteItemMutation(bool remove, bool automatic)
    {
        await using var seed = new MemoryStream();
        await SeedAsync(seed);
        byte[] before = seed.ToArray();
        for (int fault = 0; ; fault++)
        {
            await using var stream = new WriteFaultStream();
            await stream.WriteAsync(before, Ct);
            stream.Position = 0;
            await using AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = automatic }, leaveOpen: true, cancellationToken: Ct);
            if (fault == 0)
            {
                stream.FailOnFlush(1);
            }
            else
            {
                stream.FailDuringWrite(fault);
            }

            try
            {
                if (remove)
                {
                    await writer.RemoveAttachmentAsync("Parents", "Files", Key(1), 1, Ct);
                }
                else
                {
                    await writer.ReplaceAttachmentAsync("Parents", "Files", Key(1), 1, new AttachmentInput("changed.zip", new byte[20_000]), Ct);
                }
            }
            catch (IOException)
            {
                Assert.True(stream.Faulted);
                Assert.Equal(before, stream.ToArray());
                await writer.ReplaceMultiValueItemAsync("Parents", "Items", Key(1), 1, "after-fault", Ct);
                continue;
            }

            Assert.False(stream.Faulted);
            break;
        }
    }

    private static Dictionary<string, object?> Key(int id) => new() { ["Id"] = id };

    private static async Task SeedAsync(MemoryStream stream)
    {
        await using AccessWriter writer = await CreateWriterAsync(stream);
        await writer.CreateTableAsync("Parents", [new("Id", typeof(int)), new("Files", typeof(byte[])) { IsAttachment = true }, new("Items", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(string) }], Ct);
        await writer.InsertRowsAsync("Parents", [[1, DBNull.Value, DBNull.Value], [2, DBNull.Value, DBNull.Value]], Ct);
        await writer.AddAttachmentAsync("Parents", "Files", Key(1), new AttachmentInput("one.txt", [1]), Ct);
        await writer.AddAttachmentAsync("Parents", "Files", Key(1), new AttachmentInput("two.txt", [2]), Ct);
        await writer.AddAttachmentAsync("Parents", "Files", Key(2), new AttachmentInput("other.txt", [3]), Ct);
        await writer.AddMultiValueItemAsync("Parents", "Items", Key(1), "one", Ct);
        await writer.AddMultiValueItemAsync("Parents", "Items", Key(1), "two", Ct);
        await writer.AddMultiValueItemAsync("Parents", "Items", Key(2), "other", Ct);
    }
}
