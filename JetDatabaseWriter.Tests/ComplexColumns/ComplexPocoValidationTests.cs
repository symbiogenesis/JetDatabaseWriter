namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Tests.ComplexColumns.ComplexColumnTestSupport;

#pragma warning disable CA1812 // POCO properties are accessed by the compiled row mapper.

/// <summary>Invalid POCO complex references are refused before any insert is applied.</summary>
public sealed class ComplexPocoValidationTests
{
    /// <summary>Gets unsupported complex property payloads.</summary>
    public static TheoryData<object> InvalidValues =>
    [
        "discarded",
        1.5,
        1.5m,
        true,
        uint.MaxValue,
        0,
        -1L,
        (long)int.MaxValue + 1,
        new AttachmentInput("file.txt", []),
        new AttachmentInput?[] { null },
        new MultiValueItem?[] { null },
        ComplexCellValue.EncodeAttachments(1, []),
        ComplexCellValue.EncodeMultiValueItems(1, []),
    ];

    [Theory]
    [MemberData(nameof(InvalidValues))]
    public async Task Insert_InvalidComplexProperty_RefusesNamedArgumentWithoutMutation(object value)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await CreateWriterAsync(stream);
        await CreateTableAsync(writer);
        byte[] before = stream.ToArray();

        foreach (string column in new[] { "Files", "Tags" })
        {
            var item = new ComplexPoco
            {
                Files = column == "Files" ? value : null,
                Tags = column == "Tags" ? value : null,
            };
            ArgumentException error = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.InsertRowAsync("Docs", item, Ct));

            Assert.Equal("item", error.ParamName);
            Assert.Contains(column, error.Message, StringComparison.Ordinal);
            Assert.Equal(before, stream.ToArray());
        }
    }

    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task InsertBatch_InvalidSecondProperty_DoesNotApplyFirstRow(WriteMode mode)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(stream, mode))
        {
            await CreateTableAsync(writer);
            byte[] before = stream.ToArray();
            await RunAsync(writer, mode, async () =>
            {
                ArgumentException error = await Assert.ThrowsAsync<ArgumentException>(async () =>
                    await writer.InsertRowsAsync("Docs", new[]
                    {
                        new ComplexPoco(),
                        new ComplexPoco { Files = new object() },
                    }, Ct));
                Assert.Equal("item", error.ParamName);
            });
            Assert.Equal(before, stream.ToArray());
            await writer.InsertRowAsync("Docs", new ComplexPoco(), Ct);
        }

        RawTable table = await ReadRawTableAsync(stream, "Docs");
        Assert.Single(table.Rows);
        Assert.Equal(1, table.Rows[0][0]);
        Assert.Equal(1, table.ComplexAutoNumber);
    }

    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task Insert_ContradictoryReferences_RefusesNamedArgumentAndRestoresCounters(WriteMode mode)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(stream, mode))
        {
            await CreateTableAsync(writer);
            byte[] before = stream.ToArray();
            await RunAsync(writer, mode, async () =>
            {
                ArgumentException error = await Assert.ThrowsAsync<ArgumentException>(async () =>
                    await writer.InsertRowsAsync("Docs", new[]
                    {
                        new ComplexPoco(),
                        new ComplexPoco { Files = 1, Tags = 2 },
                    }, Ct));
                Assert.Equal("item", error.ParamName);
                Assert.Contains("Files", error.Message, StringComparison.Ordinal);
                Assert.Contains("Tags", error.Message, StringComparison.Ordinal);
            });
            Assert.Equal(before, stream.ToArray());
            await writer.InsertRowAsync("Docs", new ComplexPoco(), Ct);
        }

        RawTable table = await ReadRawTableAsync(stream, "Docs");
        Assert.Single(table.Rows);
        Assert.Equal(1, table.Rows[0][0]);
        Assert.Equal(1, table.ComplexAutoNumber);
    }

    [Fact]
    public async Task Insert_IntegralComplexReference_PreservesSuppliedReference()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(stream))
        {
            await CreateTableAsync(writer);
            await writer.InsertRowAsync("Docs", new ComplexPoco { Files = 9L }, Ct);
        }

        RawTable table = await ReadRawTableAsync(stream, "Docs");
        Assert.Single(table.Rows);
        Assert.Equal(9, table.ComplexAutoNumber);
        Assert.Equal(new ComplexIdRef(9), table.Rows[0][1]);
        Assert.Equal(new ComplexIdRef(9), table.Rows[0][2]);
    }

    private static async Task CreateTableAsync(AccessWriter writer)
        => await writer.CreateTableAsync("Docs", new[]
        {
            new ColumnDefinition("Id", typeof(int)) { IsAutoIncrement = true },
            new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
            new ColumnDefinition("Tags", typeof(string)) { IsMultiValue = true },
        }, Ct);

    private sealed class ComplexPoco
    {
        public int Id { get; set; }

        public object? Files { get; set; }

        public object? Tags { get; set; }
    }
}
