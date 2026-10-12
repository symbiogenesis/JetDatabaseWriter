namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Microsoft-authored GUID descriptors distinguish ordinary GUIDs from ReplicationID AutoNumbers.</summary>
/// <param name="output">Captures fixture descriptor evidence.</param>
public sealed class ReplicationIdDescriptorTests(ITestOutputHelper output)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(nameof(TestDatabases.TestV2003), "Table4")]
    [InlineData(nameof(TestDatabases.TestV2010), "Table4")]
    [InlineData(nameof(TestDatabases.TestIndexCodesV2000), "Table13")]
    [InlineData(nameof(TestDatabases.TestIndexCodesV2010), "Table13")]
    public async Task MicrosoftFixture_GuidDescriptors_RetainTheirFlags(string fixture, string tableName)
    {
        string path = FixturePath(fixture);
        ColumnInfo[] columns = await ReadColumnsAsync(path, tableName);
        Assert.Contains(columns, column => column.Type == ColumnType.GuidType);
        foreach (ColumnInfo column in columns)
        {
            output.WriteLine($"{fixture} {tableName}.{column.Name} type={column.Type} flags=0x{column.Flags:X2} descriptor={Convert.ToHexString(column.RawDescriptor.ToArray())}");
            if (column.Type == ColumnType.GuidType)
            {
                Assert.Equal((byte)0x43, column.Flags);
                Assert.True(column.IsAutoNumber);
                Assert.True(column.IsFixed);
                Assert.Equal(16, column.Size);
            }
        }
    }

    [Theory]
    [InlineData(ColumnType.GuidType, (byte)0x43, true)]
    [InlineData(ColumnType.GuidType, (byte)0x03, false)]
    [InlineData(ColumnType.GuidType, (byte)0x07, false)]
    [InlineData(ColumnType.LongIntegerType, (byte)0x43, false)]
    [InlineData(ColumnType.LongIntegerType, (byte)0x07, true)]
    [InlineData(ColumnType.ComplexType, (byte)0x07, false)]
    [InlineData(ColumnType.MemoType, (byte)0x42, false)]
    public void AutoNumberFlag_IsInterpretedForItsColumnType(ColumnType type, byte flags, bool expected)
    {
        var column = new ColumnInfo { Name = "Id", Type = type, Flags = flags };
        Assert.Equal(expected, column.IsAutoNumber);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task GuidAutoNumber_ExplicitNullAndEmpty_RemainAssignmentsAfterReopenAndRewrite(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("NullableIds", [new ColumnDefinition("Id", typeof(Guid)) { IsAutoIncrement = true }, new ColumnDefinition("Number", typeof(int))], Ct);
            await writer.CreateTableAsync("RequiredIds", [new ColumnDefinition("Id", typeof(Guid)) { IsAutoIncrement = true, IsNullable = false }, new ColumnDefinition("Number", typeof(int))], Ct);
            await writer.CreateTableAsync("KeyIds", [new ColumnDefinition("Id", typeof(Guid)) { IsAutoIncrement = true, IsPrimaryKey = true }, new ColumnDefinition("Number", typeof(int))], Ct);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            Assert.Equal(1, await writer.InsertRowsAsync("NullableIds", (RowValues[])[new() { ["Id"] = null, ["Number"] = 1 }], Ct));
            Assert.Equal(1, await writer.InsertRowsAsync("NullableIds", (RowValues[])[new() { ["Id"] = Guid.Empty, ["Number"] = 2 }], Ct));
            Assert.Equal(1, await writer.InsertRowsAsync("NullableIds", (GuidRow[])[new() { Number = 3 }], Ct));
            await writer.AddColumnAsync("NullableIds", new ColumnDefinition("Spare", typeof(int)), Ct);
            await writer.RenameColumnAsync("NullableIds", "Number", "Value", Ct);
            await writer.DropColumnAsync("NullableIds", "Spare", Ct);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            Assert.Equal(1, await writer.InsertRowsAsync("NullableIds", (RowValues[])[new() { ["Value"] = 4 }], Ct));
            Assert.Equal(1, await writer.InsertRowsAsync("NullableIds", (object?[][])[[DBNull.Value, 5]], Ct));
            Assert.Equal(1, await writer.UpdateRowsAsync("NullableIds", RowCriteria.Where("Value", 4), new RowValues { ["Id"] = null }, Ct));
            byte[] before = stream.ToArray();
            await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowsAsync("RequiredIds", (RowValues[])[new() { ["Id"] = null, ["Number"] = 1 }], Ct));
            await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowsAsync("KeyIds", (RowValues[])[new() { ["Id"] = null, ["Number"] = 1 }], Ct));
            Assert.Equal(before, stream.ToArray());
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct);
        ColumnMetadata metadata = Assert.Single(await reader.GetColumnMetadataAsync("NullableIds", Ct), column => column.Name == "Id");
        Assert.True(metadata.IsNullable);
        Assert.Equal("GenGUID()", metadata.DefaultValueExpression);
        Assert.False(Assert.Single(await reader.GetColumnMetadataAsync("RequiredIds", Ct), column => column.Name == "Id").IsNullable);
        int count = 0;
        await foreach (object[] row in reader.Rows("NullableIds", cancellationToken: Ct))
        {
            int number = Assert.IsType<int>(row[1]);
            if (number is 1 or 4 or 5)
            {
                Assert.IsType<DBNull>(row[0]);
            }
            else if (number == 2)
            {
                Assert.Equal(Guid.Empty, Assert.IsType<Guid>(row[0]));
            }
            else
            {
                Assert.NotEqual(Guid.Empty, Assert.IsType<Guid>(row[0]));
            }

            count++;
        }

        Assert.Equal(5, count);
    }

    private sealed class GuidRow
    {
        public Guid Id { get; set; }

        public int Number { get; set; }
    }

    internal static string FixturePath(string fixture) => fixture switch
    {
        nameof(TestDatabases.TestV2003) => TestDatabases.TestV2003,
        nameof(TestDatabases.TestV2010) => TestDatabases.TestV2010,
        nameof(TestDatabases.TestIndexCodesV2000) => TestDatabases.TestIndexCodesV2000,
        nameof(TestDatabases.TestIndexCodesV2010) => TestDatabases.TestIndexCodesV2010,
        _ => throw new ArgumentOutOfRangeException(nameof(fixture)),
    };

    internal static async Task<ColumnInfo[]> ReadColumnsAsync(string path, string tableName)
    {
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(path, cancellationToken: Ct);
        long page = Assert.Single(await harness.Services.Catalog.GetUserTablesAsync(Ct), table => table.Name == tableName).TDefPage;
        TableDef definition = Assert.IsType<TableDef>(await harness.Database.TableDefs.ReadTableDefAsync(page, Ct));
        return [.. definition.Columns];
    }
}
