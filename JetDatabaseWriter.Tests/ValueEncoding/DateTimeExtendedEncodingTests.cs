namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;
using static JetDatabaseWriter.Enums.ColumnType;

public sealed class DateTimeExtendedEncodingTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InsertRow_DateTimeExtendedRawPayload_WritesPayload(bool forceVariableLengthStorage)
    {
        await using var stream = new MemoryStream();
        const string tableName = "Events";
        byte[] payload = CreatePayload();
        var column = new ColumnDefinition("ExtendedAt", typeof(string))
        {
            ColumnTypeOverride = DateTimeExtendedType,
            ForceVariableLengthStorage = forceVariableLengthStorage,
        };

        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, [column], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [payload], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);
        using DataTable table = await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(1, table.Rows.Count);
        DataRow row = table.Rows[0];
        string value = Assert.IsType<string>(row["ExtendedAt"]);
        Assert.Equal("0102030405060708", value);
    }

    [Fact]
    public async Task InsertRow_DateTimeExtendedWrongLength_ThrowsArgumentException()
    {
        await using var stream = new MemoryStream();
        const string tableName = "Events";
        var column = new ColumnDefinition("ExtendedAt", typeof(string))
        {
            ColumnTypeOverride = DateTimeExtendedType,
        };

        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await writer.CreateTableAsync(tableName, [column], TestContext.Current.CancellationToken);

        ArgumentException exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.InsertRowAsync(tableName, [new byte[41]], TestContext.Current.CancellationToken));

        Assert.Contains("exactly 42 bytes", exception.Message, StringComparison.Ordinal);
    }

    private static byte[] CreatePayload()
    {
        var payload = new byte[42];
        for (int index = 0; index < payload.Length; index++)
        {
            payload[index] = (byte)(index + 1);
        }

        return payload;
    }
}