namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
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
        (ColumnDefinition column, object first, object second) = ParentCase(kind);
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
    public void ParentBinaryComparison_PreservesFixedAndVariableOperandLengths()
    {
        ColumnInfo fixedColumn = new() { Name = "Key", Type = ColumnType.BinaryType, Flags = 1, Size = 4 };
        ColumnInfo variableColumn = fixedColumn with { Flags = 0 };
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        Assert.NotEqual(
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
        await writer.CreateTableAsync("Parents", [new("Key", typeof(byte[]), 4), new("Items", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(string) }], Ct);
        await writer.InsertRowAsync("Parents", [(byte[])[1], DBNull.Value], Ct);
        byte[] baseline = stream.ToArray();
        ArgumentException error = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.AddMultiValueItemAsync("Parents", "Items", new Dictionary<string, object?> { ["Key"] = "invalid" }, "selected", Ct));
        Assert.Equal("parentRowKey", error.ParamName);
        Assert.Equal(baseline, stream.ToArray());
    }

    [Fact]
    public async Task AddMultiValueItem_ShortFixedBinaryKeyDoesNotMatchPaddedParent()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: Ct);
        await writer.CreateTableAsync("Parents", [new("Key", typeof(byte[]), 4), new("Items", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(string) }], Ct);
        await writer.InsertRowAsync("Parents", [(byte[])[1], DBNull.Value], Ct);
        byte[] baseline = stream.ToArray();
        JetOperationException error = await Assert.ThrowsAsync<JetOperationException>(async () =>
            await writer.AddMultiValueItemAsync("Parents", "Items", new Dictionary<string, object?> { ["Key"] = (byte[])[1] }, "selected", Ct));
        Assert.Equal(JetErrorCode.RowNotFound, error.ErrorCode);
        Assert.Equal(baseline, stream.ToArray());
    }

    [Theory]
    [InlineData("decimal")]
    [InlineData("integer")]
    [InlineData("currency")]
    [InlineData("single")]
    [InlineData("integer-wide-double")]
    [InlineData("integer-wide-single")]
    [InlineData("integer-tiny-double")]
    [InlineData("currency-wide-double")]
    [InlineData("currency-wide-single")]
    [InlineData("currency-tiny-double")]
    [InlineData("decimal-wide-double")]
    [InlineData("decimal-wide-single")]
    [InlineData("decimal-tiny-double")]
    [InlineData("currency-large-double")]
    [InlineData("decimal-large-double")]
    public void ParentNumericComparison_DoesNotRoundOperandToStoredPrecision(string kind)
    {
        (ColumnType Type, object Stored, object Operand) values = kind switch
        {
            "decimal" => (ColumnType.NumericType, 123.4567m, 123.45671m),
            "integer" => (ColumnType.LongIntegerType, 123, 123.1m),
            "currency" => (ColumnType.MoneyType, 123.4567m, 123.45671m),
            "single" => (ColumnType.FloatType, 1.5f, 1.50000001d),
            "integer-wide-double" => (ColumnType.LongIntegerType, 1, 1.0000000000000002d),
            "integer-wide-single" => (ColumnType.LongIntegerType, 1, 1.0000001f),
            "integer-tiny-double" => (ColumnType.LongIntegerType, 0, double.Epsilon),
            "currency-wide-double" => (ColumnType.MoneyType, 123.4567m, 123.45670000000001d),
            "currency-wide-single" => (ColumnType.MoneyType, 123.4567m, 123.45671f),
            "currency-tiny-double" => (ColumnType.MoneyType, 0m, 1e-100d),
            "decimal-wide-double" => (ColumnType.NumericType, 1m, 1.0000000000000002d),
            "decimal-wide-single" => (ColumnType.NumericType, 1m, 1.0000001f),
            "decimal-tiny-double" => (ColumnType.NumericType, 0m, 1e-100d),
            "currency-large-double" => (ColumnType.MoneyType, 123456789012346m, 123456789012345.67d),
            "decimal-large-double" => (ColumnType.NumericType, 123456789012346m, 123456789012345.67d),
            _ => throw new ArgumentException("Unknown numeric case.", nameof(kind)),
        };
        ColumnInfo column = new() { Name = "Key", Type = values.Type, NumericPrecision = 28, NumericScale = 4 };
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        Assert.NotEqual(
            ComplexParentKeyComparer.Encode(format, column, values.Stored),
            ComplexParentKeyComparer.Encode(format, column, values.Operand));
    }

    [Fact]
    public void ParentSingleComparison_PreservesExactDecimalAndLargeIntegerOperands()
    {
        ColumnInfo column = new() { Name = "Key", Type = ColumnType.FloatType };
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        Assert.Equal(ComplexParentKeyComparer.Encode(format, column, 1.5f), ComplexParentKeyComparer.Encode(format, column, 1.5m));
        Assert.NotEqual(ComplexParentKeyComparer.Encode(format, column, 1.1f), ComplexParentKeyComparer.Encode(format, column, 1.1m));
        const long exact = 1L << 60;
        Assert.Equal(ComplexParentKeyComparer.Encode(format, column, (float)exact), ComplexParentKeyComparer.Encode(format, column, exact));
        Assert.NotEqual(ComplexParentKeyComparer.Encode(format, column, (float)exact), ComplexParentKeyComparer.Encode(format, column, exact + 1));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SignedZero_IdentifiesOneParentAndRefusesAmbiguousParents(bool wide)
    {
        object negative = wide ? (object)BitConverter.Int64BitsToDouble(long.MinValue) : BitConverter.Int32BitsToSingle(int.MinValue);
        object positive = wide ? (object)0d : 0f;
        var key = new Dictionary<string, object?> { ["Key"] = positive };
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: Ct);
        await writer.CreateTableAsync("Parents", [new("Key", wide ? typeof(double) : typeof(float)), new("Items", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(string) }], Ct);
        await writer.InsertRowAsync("Parents", [negative, DBNull.Value], Ct);
        await writer.AddMultiValueItemAsync("Parents", "Items", key, "first", Ct);
        await writer.ReplaceMultiValueItemAsync("Parents", "Items", key, 1, "changed", Ct);
        await writer.RemoveMultiValueItemAsync("Parents", "Items", key, 1, Ct);
        await writer.AddMultiValueItemAsync("Parents", "Items", key, "retained", Ct);
        await writer.InsertRowAsync("Parents", [positive, DBNull.Value], Ct);
        byte[] before = stream.ToArray();
        JetOperationException add = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.AddMultiValueItemAsync("Parents", "Items", key, "ambiguous", Ct));
        JetOperationException replace = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.ReplaceMultiValueItemAsync("Parents", "Items", key, 2, "ambiguous", Ct));
        JetOperationException remove = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.RemoveMultiValueItemAsync("Parents", "Items", key, 2, Ct));
        Assert.Equal(JetErrorCode.RowNotUnique, add.ErrorCode);
        Assert.Equal(JetErrorCode.RowNotUnique, replace.ErrorCode);
        Assert.Equal(JetErrorCode.RowNotUnique, remove.ErrorCode);
        Assert.Equal(before, stream.ToArray());
    }

    private static (ColumnDefinition Column, object First, object Second) ParentCase(string kind)
        => kind switch
        {
            "binary" => (new("Key", typeof(byte[]), 4), (byte[])[1, 0, 0, 0], (byte[])[2, 0, 0, 0]),
            "memo" => (new("Key", typeof(string)), new string('x', 600) + "one", new string('x', 600) + "two"),
            "null-empty" => (new("Key", typeof(string), 32) { AllowZeroLength = true }, DBNull.Value, string.Empty),
            "guid" => (new("Key", typeof(Guid)), Guid.Parse("00000000-0000-0000-0000-000000000001"), Guid.Parse("00000000-0000-0000-0000-000000000002")),
            "decimal" => (new("Key", typeof(decimal)) { NumericPrecision = 18, NumericScale = 2 }, 1.25m, 1.5m),
            "date" => (new("Key", typeof(DateTime)), new DateTime(2026, 1, 1, 0, 0, 1), new DateTime(2026, 1, 1, 0, 0, 2)),
            _ => throw new ArgumentException("Unknown test case.", nameof(kind)),
        };
}
