namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using Xunit;

/// <summary>Calculated fields admit different conversions from defaults and validation rules.</summary>
public sealed class CalculatedConversionContextTests
{
    [Theory]
    [InlineData("CByte([Flag])")]
    [InlineData("CInt([Amount])")]
    [InlineData("CDate(\"6:00 PM\")")]
    [InlineData("TimeValue(\"6:00 PM\")")]
    [InlineData("DateValue(\"6:00 PM\")")]
    [InlineData("CBool([Amount])")]
    [InlineData("CLng([Amount])")]
    [InlineData("CSng([Amount])")]
    [InlineData("CCur([Amount])")]
    [InlineData("CDec([Amount])")]
    [InlineData("CStr([Amount])")]
    [InlineData("IIf([Flag],cint ([Amount]),0)")]
    [InlineData("cstr$ ([Amount])")]
    public void Conversion_RefusedOnlyInCalculatedField(string expression)
    {
        JetNotSupportedException exception = Assert.Throws<JetNotSupportedException>(() => CalculatedExpressionParser.Parse(expression));
        Assert.Contains("calculated columns", exception.Message, StringComparison.Ordinal);
        Assert.NotNull(CalculatedExpressionParser.Parse(expression, isCalculatedColumn: false));
    }

    [Theory]
    [InlineData("CDbl([Amount])")]
    [InlineData("#6:00 PM#")]
    [InlineData("DateSerial(2020,1,31)")]
    [InlineData("TimeSerial(18,0,0)")]
    [InlineData("[CInt] & \"CDate(1)\" & 'CByte(2)'")]
    public void CalculatedField_AdmitsAlternativesAndLiteralText(string expression)
        => Assert.NotNull(CalculatedExpressionParser.Parse(expression));

    [Theory]
    [InlineData("CByte([Flag])")]
    [InlineData("CInt([Amount])")]
    [InlineData("CDate(\"6:00 PM\")")]
    [InlineData("TimeValue(\"6:00 PM\")")]
    [InlineData("DateValue(\"6:00 PM\")")]
    public async Task CreateAndAddConversionCalculation_RefuseWithoutChangingImage(string expression)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();
        await Assert.ThrowsAsync<JetNotSupportedException>(async () => await writer.CreateTableAsync(
            "Rejected",
            [new ColumnDefinition("Flag", typeof(bool)), new ColumnDefinition("Amount", typeof(double)), CalculatedColumn(expression)],
            TestContext.Current.CancellationToken));
        Assert.Equal(before, stream.ToArray());
        await writer.CreateTableAsync("T", [new ColumnDefinition("Flag", typeof(bool)), new ColumnDefinition("Amount", typeof(double))], TestContext.Current.CancellationToken);
        before = stream.ToArray();
        await Assert.ThrowsAsync<JetNotSupportedException>(async () => await writer.AddColumnAsync("T", CalculatedColumn(expression), TestContext.Current.CancellationToken));
        Assert.Equal(before, stream.ToArray());
        await writer.InsertRowAsync("T", [false, 1d], TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task StoredRefusedConversion_CachedReadSurvivesAndRecalculationRefusesAtomically()
    {
        const string placeholder = "\"1\"     ";
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)), CalculatedColumn(placeholder)], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [1, "1"], TestContext.Current.CancellationToken);
        }

        byte[] image = stream.ToArray();
        int offset = image.AsSpan().IndexOf(Encoding.Unicode.GetBytes(placeholder));
        Assert.True(offset >= 0);
        Encoding.Unicode.GetBytes("CByte(1)").CopyTo(image, offset);
        stream.Position = 0;
        await stream.WriteAsync(image, TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            Assert.Equal("CByte(1)", Assert.Single(await reader.GetColumnMetadataAsync("T", TestContext.Current.CancellationToken), column => column.IsCalculated).CalculationExpression);
            int count = 0;
            await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
            {
                Assert.Equal("1", row[1]);
                count++;
            }

            Assert.Equal(1, count);
        }

        stream.Position = 0;
        await using AccessWriter reopened = await AccessWriter.OpenAsync(
            stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<JetNotSupportedException>(async () => await reopened.InsertRowAsync("T", [2, DBNull.Value], TestContext.Current.CancellationToken));
        Assert.Equal(image, stream.ToArray());
        await Assert.ThrowsAsync<JetNotSupportedException>(async () => await reopened.UpdateRowsAsync(
            "T", "Id", 1, new Dictionary<string, object?> { ["Id"] = 2 }, TestContext.Current.CancellationToken));
        Assert.Equal(image, stream.ToArray());
    }

    [Fact]
    public async Task StoredDefaultsAndValidation_KeepConversionFunctions()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("ByteValue", typeof(byte)) { DefaultValueExpression = "CByte(True)" },
                    new ColumnDefinition("IntegerValue", typeof(short)) { DefaultValueExpression = "CInt(2.5)", ValidationRuleExpression = "CInt([IntegerValue]) > 0" },
                    new ColumnDefinition("DateValue", typeof(DateTime)) { DefaultValueExpression = "CDate(\"6:00 PM\")" },
                    new ColumnDefinition("TimeValue", typeof(DateTime)) { DefaultValueExpression = "TimeValue(\"6:00 PM\")" },
                ],
                TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync("T", [DbDefault.Value, DbDefault.Value, DbDefault.Value, DbDefault.Value], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        int count = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal((byte)255, row[0]);
            Assert.Equal((short)2, row[1]);
            Assert.Equal(new DateTime(1899, 12, 30, 18, 0, 0), row[2]);
            Assert.Equal(row[2], row[3]);
            count++;
        }

        Assert.Equal(1, count);
    }

    private static ColumnDefinition CalculatedColumn(string expression)
        => new("Calculated", typeof(string)) { IsCalculated = true, CalculationExpression = expression };
}
