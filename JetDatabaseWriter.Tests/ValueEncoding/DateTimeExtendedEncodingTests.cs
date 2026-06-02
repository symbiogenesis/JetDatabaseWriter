namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

public sealed class DateTimeExtendedEncodingTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InsertRow_DateTimeExtendedDateTime_WritesAndReadsDateTime(bool forceVariableLengthStorage)
    {
        await using var stream = new MemoryStream();
        const string tableName = "Events";
        DateTime expected = CreateValue();
        var column = new ColumnDefinition("ExtendedAt", typeof(DateTime))
        {
            IsDateTimeExtended = true,
            ForceVariableLengthStorage = forceVariableLengthStorage,
        };

        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, [column], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [expected], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);
        using DataTable table = await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(1, table.Rows.Count);
        Assert.Equal(typeof(DateTime), table.Columns["ExtendedAt"]!.DataType);
        DataRow row = table.Rows[0];
        DateTime value = Assert.IsType<DateTime>(row["ExtendedAt"]);
        Assert.Equal(expected, value);

        IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);
        ColumnMetadata extended = Assert.Single(metadata);
        Assert.Equal("Date/Time Extended", extended.TypeName);
        Assert.Equal(typeof(DateTime), extended.ClrType);
    }

    [Fact]
    public async Task SchemaEvolution_DateTimeExtendedColumn_PreservesTypeAndValue()
    {
        await using var stream = new MemoryStream();
        const string tableName = "Events";
        DateTime expected = CreateValue();

        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("ExtendedAt", typeof(DateTime)) { IsDateTimeExtended = true },
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [1, expected], TestContext.Current.CancellationToken);
            await writer.AddColumnAsync(tableName, new ColumnDefinition("Label", typeof(string), maxLength: 32), TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);
        ColumnMetadata extended = metadata.Single(column => column.Name == "ExtendedAt");
        Assert.Equal("Date/Time Extended", extended.TypeName);
        Assert.Equal(typeof(DateTime), extended.ClrType);

        using DataTable table = await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken);
        DateTime actual = Assert.IsType<DateTime>(table.Rows[0]["ExtendedAt"]);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task InsertRow_DateTimeExtendedWrongLength_ThrowsArgumentException()
    {
        await using var stream = new MemoryStream();
        const string tableName = "Events";
        var column = new ColumnDefinition("ExtendedAt", typeof(DateTime))
        {
            IsDateTimeExtended = true,
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

    private static DateTime CreateValue() => new DateTime(2021, 6, 14, 22, 45, 12, 345, DateTimeKind.Unspecified).AddTicks(6789);
}
