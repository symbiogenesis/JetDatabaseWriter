namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native column identities and continuation chains survive schema rewrites.</summary>
public sealed class NativeSchemaPreservationTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets schema operations in every write mode.</summary>
    public static TheoryData<string, WriteMode> Operations()
    {
        var data = new TheoryData<string, WriteMode>();
        foreach (string operation in new[] { "add", "drop", "rename" })
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                data.Add(operation, mode);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Operations))]
    public async Task NativeTable4_RewritePreservesDistinctColumnIdentities(string operation, WriteMode mode)
    {
        await using var stream = new MemoryStream();
        await stream.WriteAsync(await File.ReadAllBytesAsync(TestDatabases.TestV2010, Ct), Ct);
        TableDef original = await ReadDefinitionAsync(stream, "Table4");
        Assert.Equal([0, 1], original.Columns.Select(column => column.ColNum));
        Assert.Equal([1, 2], original.Columns.Select(Identity));
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, Ct))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
                {
                    if (operation == "add")
                    {
                        await writer.AddColumnAsync("Table4", new ColumnDefinition("Added", typeof(int)), Ct);
                    }
                    else if (operation == "drop")
                    {
                        await writer.DropColumnAsync("Table4", original.Columns[0].Name, Ct);
                    }
                    else
                    {
                        await writer.RenameColumnAsync("Table4", original.Columns[0].Name, "Renamed", Ct);
                    }
                },
                Ct);
        }

        TableDef rewritten = await ReadDefinitionAsync(stream, "Table4");
        foreach (ColumnInfo column in original.Columns)
        {
            if (operation == "drop" && column == original.Columns[0])
            {
                continue;
            }

            string name = operation == "rename" && column == original.Columns[0] ? "Renamed" : column.Name;
            Assert.Equal(Identity(column), Identity(Assert.Single(rewritten.Columns, value => value.Name == name)));
        }

        Assert.Equal(rewritten.Columns.Count, rewritten.Columns.Select(Identity).Distinct().Count());
    }

    [Fact]
    public async Task NativeTable4_DropAddSequenceAllocatesNoncollidingIdentities()
    {
        await using var stream = new MemoryStream();
        await stream.WriteAsync(await File.ReadAllBytesAsync(TestDatabases.TestV2010, Ct), Ct);
        TableDef original = await ReadDefinitionAsync(stream, "Table4");
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.AddColumnAsync("Table4", new ColumnDefinition("First", typeof(int)), Ct);
            await writer.DropColumnAsync("Table4", original.Columns[0].Name, Ct);
            await writer.RenameColumnAsync("Table4", "First", "Kept", Ct);
            await writer.AddColumnAsync("Table4", new ColumnDefinition("Second", typeof(int)), Ct);
        }

        TableDef rewritten = await ReadDefinitionAsync(stream, "Table4");
        Assert.Equal(2, Identity(Assert.Single(rewritten.Columns, value => value.Name == original.Columns[1].Name)));
        Assert.Equal(3, rewritten.Columns.Count);
        Assert.Equal(3, rewritten.Columns.Select(Identity).Distinct().Count());
    }

    [Theory]
    [MemberData(nameof(Operations))]
    public async Task AttachmentTable_MultiPageRewritePreservesStorageAndIndexes(string operation, WriteMode mode)
    {
        await using var stream = new MemoryStream();
        var columns = new List<ColumnDefinition> { new("Id", typeof(int)) { IsPrimaryKey = true }, new("Files", typeof(byte[])) { IsAttachment = true } };
        columns.AddRange(Enumerable.Range(0, 200).Select(index => new ColumnDefinition($"C{index}", typeof(int))));
        object?[] row = [1, DBNull.Value, .. Enumerable.Range(0, 200).Select(index => (object)index)];
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, WriteModes.WriterOptions(mode), leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Documents", columns, Ct);
            await writer.InsertRowAsync("Documents", row, Ct);
            await writer.AddAttachmentAsync("Documents", "Files", new Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("keep.txt", [1, 2, 3]), Ct);
        }

        long originalRoot = await ReadRootAsync(stream, "Documents");
        await AssertContinuationAsync(stream, originalRoot);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, Ct))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
                {
                    if (operation == "add")
                    {
                        await writer.AddColumnAsync("Documents", new ColumnDefinition("Added", typeof(int)), Ct);
                    }
                    else if (operation == "drop")
                    {
                        await writer.DropColumnAsync("Documents", "C0", Ct);
                    }
                    else
                    {
                        await writer.RenameColumnAsync("Documents", "C0", "Renamed", Ct);
                    }
                },
                Ct);
        }

        Assert.Equal(originalRoot, await ReadRootAsync(stream, "Documents"));
        await AssertContinuationAsync(stream, originalRoot);
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct);
        Assert.Equal("Documents", Assert.Single(await reader.ListTablesAsync(Ct)));
        AttachmentRecord attachment = Assert.Single(await reader.GetAttachmentsAsync("Documents", "Files", Ct));
        Assert.Equal("keep.txt", attachment.FileName);
        Assert.Equal([1, 2, 3], attachment.FileData);
        IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync("Documents", Ct);
        using DataTable rows = await reader.ReadTableAsync("Documents", cancellationToken: Ct);
        DataRow stored = Assert.Single(rows.Rows.Cast<DataRow>());
        for (int index = operation == "drop" ? 1 : 0; index < 200; index++)
        {
            string name = operation == "rename" && index == 0 ? "Renamed" : $"C{index}";
            Assert.Equal(index, stored[name]);
        }

        Assert.Equal(operation switch { "add" => 203, "drop" => 201, _ => 202 }, metadata.Count);
        IndexMetadata primary = Assert.Single(await reader.ListIndexesAsync("Documents", Ct), index => index.Kind == IndexKind.PrimaryKey);
        Assert.Single(await reader.SeekRowsAsync("Documents", primary.Name, [1], Ct).ToListAsync(Ct));
        Assert.Single(await reader.ListIndexesAsync("Documents", Ct), index => index.Columns.Count == 1 && index.Columns[0].Name == "Files");
    }

    private static int Identity(ColumnInfo column) => column.RawDescriptor[9] | (column.RawDescriptor[10] << 8);

    private static async Task<TableDef> ReadDefinitionAsync(MemoryStream stream, string table)
    {
        stream.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct);
        long root = Assert.Single(await harness.Services.Catalog.GetUserTablesAsync(Ct), entry => entry.Name == table).TDefPage;
        return Assert.IsType<TableDef>(await harness.Database.TableDefs.ReadTableDefAsync(root, Ct));
    }

    private static async Task<long> ReadRootAsync(MemoryStream stream, string table)
    {
        stream.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct);
        return Assert.Single(await harness.Services.Catalog.GetUserTablesAsync(Ct), entry => entry.Name == table).TDefPage;
    }

    private static async Task AssertContinuationAsync(MemoryStream stream, long root)
    {
        stream.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct);
        LogicalTDefChain chain = await LogicalTDefChain.ReadRequiredAsync(root, harness.Database.Format.PageSize, harness.Pager.ReadPageAsync, JetDatabaseWriter.Pages.Paging.PageBuffers.Return, true, Ct);
        Assert.True(chain.PageNumbers.Count > 1);
    }
}
