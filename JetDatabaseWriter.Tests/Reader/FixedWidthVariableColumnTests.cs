namespace JetDatabaseWriter.Tests.Reader;

using System.Data;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

public sealed class FixedWidthVariableColumnTests
{
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
    }
}