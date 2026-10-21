namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Tests.ComplexColumns.ComplexColumnTestSupport;

/// <summary>
/// Multi-value columns whose <see cref="ColumnDefinition.MultiValueElementType"/>
/// is <see cref="decimal"/>. <c>ComplexColumnManager.BuildFlatTableSchema</c>
/// built the flat table's <c>Value</c> column without the parent definition's
/// <see cref="ColumnDefinition.NumericPrecision"/> and
/// <see cref="ColumnDefinition.NumericScale"/>, so it was always Decimal(18,0),
/// and <c>RowEncoder</c> rounded every item to a whole number: 12.34 was stored
/// as 12. The declared precision and scale now apply to the item values, and an
/// invalid pair is refused before anything is written.
/// </summary>
public sealed class MultiValueDecimalTests
{
    private const string TableName = "Ledger";

    /// <summary>Gets the schema rewrites that copy a table with a multi-value decimal column.</summary>
    public static TheoryData<string> SchemaRewrites => ["AddSibling", "DropSibling", "RenameSibling", "RenameMultiValue"];

    /// <summary>
    /// Items added in each write mode keep the declared scale, read through
    /// <c>GetMultiValueItemsAsync</c>, the <c>Rows()</c> cell,
    /// <c>ReadTableAsync</c> and <c>Rows&lt;T&gt;</c>.
    /// </summary>
    /// <param name="mode">The write mode.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task AddMultiValueItem_DecimalElement_KeepsDeclaredScale(WriteMode mode)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                await writer.CreateTableAsync(TableName, [new ColumnDefinition("Id", typeof(int)), Amounts("Amounts", 10, 2)], Ct);
                await writer.InsertRowAsync(TableName, [1, DBNull.Value], Ct);
                foreach (decimal value in new[] { 12.34m, 0.75m, -1.5m })
                {
                    await writer.AddMultiValueItemAsync(TableName, "Amounts", Key(1), value, Ct);
                }
            });
        }

        decimal[] expected = [12.34m, 0.75m, -1.50m];
        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal(expected, Values(await reader.GetMultiValueItemsAsync(TableName, "Amounts", Ct)));

        object[] row = Assert.Single(await reader.Rows(TableName, cancellationToken: Ct).ToListAsync(Ct));
        Assert.Equal(expected, Values(ComplexCellValue.ReadMultiValueItems(Assert.IsType<byte[]>(row[1]))));

        using DataTable table = await reader.ReadTableAsync(TableName, cancellationToken: Ct);
        Assert.Equal(expected, Values(ComplexCellValue.ReadMultiValueItems(Assert.IsType<byte[]>(Assert.Single(table.Rows.Cast<DataRow>())["Amounts"]))));

        LedgerRow mapped = Assert.Single(await reader.Rows<LedgerRow>(TableName, cancellationToken: Ct).ToListAsync(Ct));
        Assert.Equal(expected, Values(ComplexCellValue.ReadMultiValueItems(mapped.Amounts!)));
    }

    /// <summary>
    /// The flat table's <c>Value</c> column carries the declared precision and
    /// scale, and Access's Decimal(18,0) default, which the
    /// <c>MSysComplexType_Decimal</c> template also has, when none is declared.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task CreateTable_MultiValueDecimal_FlatValueColumnCarriesPrecisionAndScale()
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    Amounts("Amounts", 10, 2),
                    new ColumnDefinition("Plain", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(decimal) },
                ],
                Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal((10, 2), await ReadValuePrecisionAndScaleAsync(reader, "Amounts"));
        Assert.Equal((18, 0), await ReadValuePrecisionAndScaleAsync(reader, "Plain"));
    }

    /// <summary>
    /// Items are rounded half to even at the declared scale, and an item the
    /// declared precision cannot hold throws and is not stored.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task AddMultiValueItem_DecimalElement_RoundsHalfToEvenAtDeclaredScale()
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms))
        {
            await writer.CreateTableAsync(
                TableName,
                [new ColumnDefinition("Id", typeof(int)), Amounts("Amounts", 10, 2), Amounts("Small", 3, 2)],
                Ct);
            await writer.InsertRowAsync(TableName, [1, DBNull.Value, DBNull.Value], Ct);
            await writer.AddMultiValueItemAsync(TableName, "Amounts", Key(1), 0.755m, Ct);
            await writer.AddMultiValueItemAsync(TableName, "Amounts", Key(1), 0.745m, Ct);

            JetLimitationException ex = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                await writer.AddMultiValueItemAsync(TableName, "Small", Key(1), 99.995m, Ct));
            Assert.Contains("NUMERIC(3,2)", ex.Message, StringComparison.Ordinal);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal([0.76m, 0.74m], Values(await reader.GetMultiValueItemsAsync(TableName, "Amounts", Ct)));
        Assert.Empty(await reader.GetMultiValueItemsAsync(TableName, "Small", Ct));
    }

    /// <summary>
    /// A precision or scale out of range throws
    /// <see cref="ArgumentOutOfRangeException"/> from <c>CreateTableAsync</c>
    /// and <c>AddColumnAsync</c> before anything is written, so neither the
    /// parent nor a flat table is left behind.
    /// </summary>
    /// <param name="precision">The declared precision.</param>
    /// <param name="scale">The declared scale.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [InlineData(5, 6)]
    [InlineData(29, 0)]
    public async Task CreateTable_MultiValueDecimal_InvalidPrecisionOrScale_ThrowsBeforeWriting(int precision, int scale)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms))
        {
            await writer.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int))], Ct);
        }

        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(ms))
        {
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
                await writer.CreateTableAsync(TableName, [new ColumnDefinition("Id", typeof(int)), Amounts("Amounts", precision, scale)], Ct));
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
                await writer.AddColumnAsync("Other", Amounts("Amounts", precision, scale), Ct));
        }

        Assert.Equal(before, ms.ToArray());

        await using (AccessWriter writer = await OpenWriterAsync(ms))
        {
            await writer.CreateTableAsync(TableName, [new ColumnDefinition("Id", typeof(int)), Amounts("Amounts", 10, 2)], Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal(["Ledger", "Other"], (await reader.ListTablesAsync(Ct)).Order(StringComparer.Ordinal));
        Assert.Single(await reader.GetComplexColumnsAsync(TableName, Ct));
    }

    /// <summary>
    /// <c>AddColumnAsync</c> creates a multi-value decimal column whose items
    /// keep the declared scale.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task AddColumn_MultiValueDecimal_KeepsDeclaredScale()
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms))
        {
            await writer.CreateTableAsync(TableName, [new ColumnDefinition("Id", typeof(int))], Ct);
            await writer.InsertRowAsync(TableName, [1], Ct);
            await writer.InsertRowAsync(TableName, [2], Ct);
            await writer.AddColumnAsync(TableName, Amounts("Amounts", 10, 2), Ct);
            await writer.AddMultiValueItemAsync(TableName, "Amounts", Key(2), 12.34m, Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal((10, 2), await ReadValuePrecisionAndScaleAsync(reader, "Amounts"));
        Assert.Equal([12.34m], Values(await reader.GetMultiValueItemsAsync(TableName, "Amounts", Ct)));
    }

    /// <summary>
    /// A schema rewrite keeps the flat table, so its items and its
    /// <c>Value</c> column's precision and scale are unchanged, and an item
    /// added afterwards keeps the declared scale.
    /// </summary>
    /// <param name="rewrite">The schema change.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [MemberData(nameof(SchemaRewrites))]
    public async Task SchemaRewrite_OnTableWithMultiValueDecimal_KeepsScale(string rewrite)
    {
        string column = rewrite == "RenameMultiValue" ? "Totals" : "Amounts";
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await CreateWriterAsync(ms))
        {
            await writer.CreateTableAsync(
                TableName,
                [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Note", typeof(string), maxLength: 20), Amounts("Amounts", 10, 2)],
                Ct);
            await writer.InsertRowAsync(TableName, [1, "a", DBNull.Value], Ct);
            await writer.AddMultiValueItemAsync(TableName, "Amounts", Key(1), 12.34m, Ct);

            switch (rewrite)
            {
                case "AddSibling":
                    await writer.AddColumnAsync(TableName, new ColumnDefinition("Extra", typeof(int)), Ct);
                    break;
                case "DropSibling":
                    await writer.DropColumnAsync(TableName, "Note", Ct);
                    break;
                case "RenameSibling":
                    await writer.RenameColumnAsync(TableName, "Note", "Remark", Ct);
                    break;
                default:
                    await writer.RenameColumnAsync(TableName, "Amounts", column, Ct);
                    break;
            }

            await writer.AddMultiValueItemAsync(TableName, column, Key(1), 0.75m, Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.Equal((10, 2), await ReadValuePrecisionAndScaleAsync(reader, column));
        Assert.Equal([12.34m, 0.75m], Values(await reader.GetMultiValueItemsAsync(TableName, column, Ct)));
    }

    private static ColumnDefinition Amounts(string name, int precision, int scale) =>
        new(name, typeof(object))
        {
            IsMultiValue = true,
            MultiValueElementType = typeof(decimal),
            NumericPrecision = (byte)precision,
            NumericScale = (byte)scale,
        };

    private static Dictionary<string, object?> Key(int id) => new() { ["Id"] = id };

    private static decimal[] Values(IEnumerable<MultiValueItem> items) => [.. items.Select(item => Assert.IsType<decimal>(item.Value))];

    private static async Task<(int Precision, int Scale)> ReadValuePrecisionAndScaleAsync(AccessReader reader, string column)
    {
        ComplexColumnInfo info = Assert.Single(await reader.GetComplexColumnsAsync(TableName, Ct), c => c.ColumnName == column);
        ColumnMetadata value = Assert.Single(await reader.GetColumnMetadataAsync(info.FlatTableName, Ct), c => c.Name == "Value");
        Assert.Equal("Decimal", value.TypeName);
        return (value.NumericPrecision, value.NumericScale);
    }

    private sealed class LedgerRow
    {
        public int Id { get; set; }

        public byte[]? Amounts { get; set; }
    }
}
