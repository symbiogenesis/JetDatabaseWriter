namespace JetDatabaseWriter.Tests.Pages;

using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

public sealed class EmittedPageInvariantTests
{
    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateTable_WithIndex_EmitsWellFormedPages(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Code", typeof(string), maxLength: 32),
                ],
                [new IndexDefinition("IX_Id", "Id")],
                ct);
        }

        EmittedPageInvariantAssert.AllPagesAreWellFormed(stream.ToArray(), format);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task InsertUpdateDelete_WithMaintainedIndex_LeavesWellFormedPages(DatabaseFormat format)
    {
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
                    [4, 40],
                ],
                ct);

            int updated = await writer.UpdateRowsAsync(
                "T",
                "Id",
                2,
                new Dictionary<string, object?> { ["Score"] = 99 },
                ct);
            Assert.Equal(1, updated);

            int deleted = await writer.DeleteRowsAsync("T", "Id", 3, ct);
            Assert.Equal(1, deleted);
        }

        EmittedPageInvariantAssert.AllPagesAreWellFormed(stream.ToArray(), format);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task MultiLevelIndexAndChainedLval_EmitWellFormedPages(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "IndexedRows",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                ct);

            var rows = new List<object[]>(700);
            for (int rowNumber = 0; rowNumber < 700; rowNumber++)
            {
                rows.Add([rowNumber]);
            }

            await writer.InsertRowsAsync("IndexedRows", rows, ct);

            await writer.CreateTableAsync(
                "LongTextRows",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Body", typeof(string)),
                ],
                ct);

            string longText = new('X', 9000);
            await writer.InsertRowAsync("LongTextRows", [1, longText], ct);
        }

        EmittedPageInvariantAssert.AllPagesAreWellFormed(stream.ToArray(), format);
    }

    private static async ValueTask<MemoryStream> CreateFreshStreamAsync(DatabaseFormat format)
    {
        var stream = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
        }

        stream.Position = 0;
        return stream;
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
}
