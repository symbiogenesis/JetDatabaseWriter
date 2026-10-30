namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Calculated expressions see the Text values that the native field slots store.</summary>
public sealed class CalculatedTextStorageProjectionTests
{
    [Fact]
    public async Task SourceText_DeclaredWidthAppliesBeforeCalculation()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("T", [new("Content", typeof(string), 3), new("Cached", typeof(string), 243) { IsCalculated = true, CalculationExpression = "[Content]" }, new("Length", typeof(int)) { IsCalculated = true, CalculationExpression = "Len([Content])" }], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", ["abcdef", null, null], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        object[] row = Assert.Single(await reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken));
        Assert.Equal("abc", row[0]);
        Assert.Equal("abc", row[1]);
        Assert.Equal(3, row[2]);
    }

    [Fact]
    public void NoncanonicalCalculatedTextWidth_BindsDependentReferences()
    {
        var text = new ColumnInfo { Name = "Cached", Type = ColumnType.TextType, Size = 29, ExtraFlags = Constants.CalculatedColumn.ExtFlagMask };
        var definition = new TableDef { Columns = [text, new ColumnInfo { Name = "Length" }] };
        ColumnConstraint[] constraints =
        [
            new() { Name = "Cached", ClrType = typeof(string), IsCalculated = true, StorageType = ColumnType.TextType, CalculatedResultType = ColumnType.TextType, PhysicalColumn = text, CalculationExpression = "'abcdef'" },
            new() { Name = "Length", ClrType = typeof(int), IsCalculated = true, CalculationExpression = "Len([Cached])" },
        ];
        object[] values = [DBNull.Value, DBNull.Value];
        CalculatedExpressionEvaluator.Apply(definition, constraints, values);
        Assert.Equal("abcdef", values[0]);
        Assert.Equal(3, values[1]);
    }

    [Theory]
    [InlineData(0xD800, -1)]
    [InlineData(0xDC00, -1)]
    [InlineData(0xD83D, -1)]
    [InlineData(0xDE01, -1)]
    [InlineData(0xD83D, 0xDE00)]
    [InlineData(0xD83D, 0xDE01)]
    public async Task SurrogateCodeUnits_SourceAndCalculatedTextPreserveNativeUtf16(int first, int second)
    {
        string value = second < 0 ? new string(checked((char)first), 1) : new string([checked((char)first), checked((char)second)]);
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("T", [new("Content", typeof(string), 243) { IsCompressedUnicode = false }, new("Cached", typeof(string), 243) { IsCalculated = true, CalculationExpression = "[Content]" }], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [value, null], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        object[] row = Assert.Single(await reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken));
        Assert.Equal(value, row[0]);
        Assert.Equal(value, row[1]);
    }
}
