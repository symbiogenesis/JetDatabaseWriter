namespace JetDatabaseWriter.Tests.Schema;

using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Existing descriptor bytes survive schema rewrites without normalizing native storage.</summary>
public sealed class ColumnDescriptorPreservationTests
{
    [Theory]
    [InlineData("add")]
    [InlineData("drop")]
    [InlineData("rename")]
    public async Task CalculatedFixture_SurvivingDescriptorsKeepNativeBytes(string operation)
    {
        await using var stream = new MemoryStream();
        await stream.WriteAsync(await File.ReadAllBytesAsync(TestDatabases.CalcFieldTestV2010, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);
        ColumnInfo[] before = await ReadColumnsAsync(stream, "Table1");
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            if (operation == "add")
            {
                await writer.AddColumnAsync("Table1", new ColumnDefinition("Spare", typeof(int)), TestContext.Current.CancellationToken);
            }
            else if (operation == "drop")
            {
                await writer.DropColumnAsync("Table1", "City", TestContext.Current.CancellationToken);
            }
            else
            {
                await writer.RenameColumnAsync("Table1", "Salary", "Pay", TestContext.Current.CancellationToken);
            }
        }

        ColumnInfo[] after = await ReadColumnsAsync(stream, "Table1");
        foreach (ColumnInfo source in before)
        {
            if (operation == "drop" && source.Name == "City")
            {
                continue;
            }

            string name = operation == "rename" && source.Name == "Salary" ? "Pay" : source.Name;
            ColumnInfo rewritten = Assert.Single(after, column => column.Name == name);
            Assert.Equal(source.Type, rewritten.Type);
            Assert.Equal(source.IsFixed, rewritten.IsFixed);
            Assert.Equal(source.Size, rewritten.Size);
            byte[] expected = source.RawDescriptor.ToArray();
            byte[] actual = rewritten.RawDescriptor.ToArray();
            foreach (int offset in new[] { 5, 7, 9, 21 })
            {
                expected[offset] = actual[offset];
                expected[offset + 1] = actual[offset + 1];
            }

            Assert.Equal(expected, actual);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        using DataTable rows = await reader.ReadDataTableAsync("Table1", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(4, rows.Rows.Count);
    }

    [Theory]
    [InlineData("add")]
    [InlineData("rename")]
    public async Task FixedTextFixture_RewriteKeepsFixedStorage(string operation)
    {
        await using var stream = new MemoryStream();
        await stream.WriteAsync(await File.ReadAllBytesAsync(TestDatabases.FixedTextTestV2000, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            if (operation == "add")
            {
                await writer.AddColumnAsync("users", new ColumnDefinition("Spare", typeof(int)), TestContext.Current.CancellationToken);
            }
            else
            {
                await writer.RenameColumnAsync("users", "c_flag_", "Flag", TestContext.Current.CancellationToken);
            }
        }

        ColumnInfo column = Assert.Single(await ReadColumnsAsync(stream, "users"), value => value.Name == (operation == "add" ? "c_flag_" : "Flag"));
        Assert.True(column.IsFixed);
        Assert.Equal(ColumnType.TextType, column.Type);
        Assert.Equal(2, column.Size);
    }

    private static async Task<ColumnInfo[]> ReadColumnsAsync(MemoryStream stream, string table)
    {
        stream.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        long page = Assert.Single(await harness.Services.Catalog.GetUserTablesAsync(TestContext.Current.CancellationToken), entry => entry.Name == table).TDefPage;
        TableDef definition = Assert.IsType<TableDef>(await harness.Database.TableDefs.ReadTableDefAsync(page, TestContext.Current.CancellationToken));
        return definition.Columns.ToArray();
    }
}
