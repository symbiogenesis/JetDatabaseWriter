namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.Data;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1812 // Test POCO is instantiated by Rows<T>.

public sealed class FixedWidthVariableColumnTests
{
    [Fact]
    public async Task ForcedVariableFixedPayloadScalars_DecodeThroughTypedAndStringReaders()
    {
        await using var stream = new MemoryStream();
        const string tableName = "VarScalars";
        const long expectedBig = 9_223_372_036_854_770_000L;
        const decimal expectedAmount = 123.45m;
        var expectedGuid = Guid.Parse("12345678-9abc-def0-1234-56789abcdef0");
        var expectedExtended = new DateTime(2021, 6, 14, 22, 45, 12, 345, DateTimeKind.Unspecified).AddTicks(6789);

        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new ColumnDefinition("Big", typeof(long)) { ForceVariableLengthStorage = true },
                    new ColumnDefinition("Amount", typeof(decimal))
                    {
                        ForceVariableLengthStorage = true,
                        NumericPrecision = 18,
                        NumericScale = 2,
                    },
                    new ColumnDefinition("RowGuid", typeof(Guid)) { ForceVariableLengthStorage = true },
                    new ColumnDefinition("ExtendedAt", typeof(DateTime))
                    {
                        ForceVariableLengthStorage = true,
                        IsDateTimeExtended = true,
                    },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                tableName,
                [expectedBig, expectedAmount, expectedGuid, expectedExtended],
                TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        using DataTable typedTable = await reader.ReadDataTableAsync(
            tableName,
            cancellationToken: TestContext.Current.CancellationToken);

        DataRow typedRow = typedTable.Rows[0];
        Assert.Equal(expectedBig, Assert.IsType<long>(typedRow["Big"]));
        Assert.Equal(expectedAmount, Assert.IsType<decimal>(typedRow["Amount"]));
        Assert.Equal(expectedGuid, Assert.IsType<Guid>(typedRow["RowGuid"]));
        Assert.Equal(expectedExtended, Assert.IsType<DateTime>(typedRow["ExtendedAt"]));

        string[]? stringRow = null;
        await foreach (string[] row in reader.RowsAsStrings(tableName, cancellationToken: TestContext.Current.CancellationToken))
        {
            stringRow = row;
            break;
        }

        Assert.NotNull(stringRow);
        Assert.Equal(expectedBig.ToString(CultureInfo.InvariantCulture), stringRow[0]);
        Assert.Equal("123.45", stringRow[1]);
        Assert.Equal(expectedGuid.ToString("B"), stringRow[2]);
        Assert.Equal(expectedExtended.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture), stringRow[3]);
    }

    [Fact]
    public async Task ForcedVariableNumeric_DecodesThroughTypedAndStringReaders()
    {
        await using var stream = new MemoryStream();
        const string tableName = "VarNumeric";

        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new ColumnDefinition("Amount", typeof(decimal))
                    {
                        ForceVariableLengthStorage = true,
                        NumericPrecision = 18,
                        NumericScale = 2,
                    },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(tableName, [123.45m], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        using DataTable typedTable = await reader.ReadDataTableAsync(
            tableName,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(123.45m, Assert.IsType<decimal>(typedTable.Rows[0]["Amount"]));

        string[]? stringRow = null;
        await foreach (string[] row in reader.RowsAsStrings(tableName, cancellationToken: TestContext.Current.CancellationToken))
        {
            stringRow = row;
            break;
        }

        Assert.NotNull(stringRow);
        Assert.Equal("123.45", stringRow[0]);

        VarNumericRow? typedRow = null;
        await foreach (VarNumericRow row in reader.Rows<VarNumericRow>(tableName, cancellationToken: TestContext.Current.CancellationToken))
        {
            typedRow = row;
            break;
        }

        Assert.NotNull(typedRow);
        Assert.Equal(123.45m, typedRow.Amount);
    }

    private sealed class VarNumericRow
    {
        public decimal Amount { get; set; }
    }
}
