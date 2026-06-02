namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Focused tests for NUMERIC row payload encoding.
/// </summary>
public sealed class NumericRowEncodingTests
{
    [Fact]
    public async Task InsertRow_NumericValue_RoundsHalfToEvenAtDeclaredScale()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using (AccessWriter writer = await OpenWriterAsync(stream, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("N", typeof(decimal)) { NumericPrecision = 5, NumericScale = 2 }],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync("T", [1.245m], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [1.255m], TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream, TestContext.Current.CancellationToken);
        DataTable table = await reader.ReadDataTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, table.Rows.Count);
        Assert.Equal(1.24m, table.Rows[0].Field<decimal>("N"));
        Assert.Equal(1.26m, table.Rows[1].Field<decimal>("N"));
    }

    [Fact]
    public async Task InsertRow_NumericValueExceedsPrecisionAfterRounding_ThrowsJetLimitationException()
    {
        await using MemoryStream stream = await CreateFreshAccdbStreamAsync();

        await using AccessWriter writer = await OpenWriterAsync(stream, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(
            "T",
            [new ColumnDefinition("N", typeof(decimal)) { NumericPrecision = 3, NumericScale = 2 }],
            TestContext.Current.CancellationToken);

        JetLimitationException ex = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await writer.InsertRowAsync("T", [99.995m], TestContext.Current.CancellationToken));

        Assert.Contains("NUMERIC(3,2)", ex.Message, StringComparison.Ordinal);
    }

    private static async ValueTask<MemoryStream> CreateFreshAccdbStreamAsync()
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
        }

        stream.Position = 0;
        return stream;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream, CancellationToken cancellationToken = default)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream, CancellationToken cancellationToken = default)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken);
    }
}
