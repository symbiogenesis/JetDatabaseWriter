namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.ComplexColumns.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using Xunit;
using static JetDatabaseWriter.Tests.ComplexColumns.ComplexColumnTestSupport;

public sealed class ComplexParentKeyTests
{
    [Theory]
    [InlineData("binary")]
    [InlineData("memo")]
    [InlineData("null-empty")]
    [InlineData("guid")]
    [InlineData("decimal")]
    [InlineData("date")]
    public async Task AddMultiValueItem_TargetsTypedParentIdentity(string kind)
    {
        (ColumnDefinition column, object first, object second) = kind switch
        {
            "binary" => (new("Key", typeof(byte[])), (byte[])[1], (byte[])[2]),
            "memo" => (new("Key", typeof(string)), new string('x', 600) + "one", new string('x', 600) + "two"),
            "null-empty" => (new("Key", typeof(string), 32), DBNull.Value, string.Empty),
            "guid" => (new("Key", typeof(Guid)), Guid.Parse("00000000-0000-0000-0000-000000000001"), Guid.Parse("00000000-0000-0000-0000-000000000002")),
            "decimal" => (new("Key", typeof(decimal)), 1.25m, 1.5m),
            "date" => (new("Key", typeof(DateTime)), new DateTime(2026, 1, 1, 0, 0, 1), new DateTime(2026, 1, 1, 0, 0, 2)),
            _ => throw new ArgumentException("Unknown test case.", nameof(kind)),
        };
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("Parents", [new("Id", typeof(int)), column, new("Items", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(string) }], Ct);
            await writer.InsertRowsAsync("Parents", [[1, first, DBNull.Value], [2, second, DBNull.Value]], Ct);
            await writer.AddMultiValueItemAsync("Parents", "Items", new Dictionary<string, object?> { ["Key"] = second }, "selected", Ct);
        }

        RawTable parents = await ReadRawTableAsync(stream, "Parents");
        int expectedReference = Slot(parents, Assert.Single(parents.Rows, row => Equals(row[0], 2)), "Items")!.Value;
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: Ct);
        MultiValueItem item = Assert.Single(await reader.GetMultiValueItemsAsync("Parents", "Items", Ct));
        Assert.Equal(expectedReference, item.ConceptualTableId);
    }

    [Fact]
    public void ParentBinaryComparison_PadsOnlyNativeFixedColumns()
    {
        var fixedColumn = new ColumnInfo { Name = "Key", Type = ColumnType.BinaryType, Flags = 1, Size = 4 };
        var variableColumn = fixedColumn with { Flags = 0 };
        JetFormat format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        Assert.Equal(
            ComplexParentKeyComparer.Encode(format, fixedColumn, (byte[])[1]),
            ComplexParentKeyComparer.Encode(format, fixedColumn, (byte[])[1, 0, 0, 0]));
        Assert.NotEqual(
            ComplexParentKeyComparer.Encode(format, variableColumn, (byte[])[1]),
            ComplexParentKeyComparer.Encode(format, variableColumn, (byte[])[1, 0, 0, 0]));
    }

    [Fact]
    public async Task AddMultiValueItem_InvalidBinaryKey_RefusesWithoutChangingBytes()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: Ct);
        await writer.CreateTableAsync("Parents", [new("Key", typeof(byte[])), new("Items", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(string) }], Ct);
        await writer.InsertRowAsync("Parents", [(byte[])[1], DBNull.Value], Ct);
        byte[] baseline = stream.ToArray();
        ArgumentException error = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.AddMultiValueItemAsync("Parents", "Items", new Dictionary<string, object?> { ["Key"] = "invalid" }, "selected", Ct));
        Assert.Equal("parentRowKey", error.ParamName);
        Assert.Equal(baseline, stream.ToArray());
    }
}
