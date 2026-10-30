namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using Xunit;

/// <summary>Freezes independent DAO120 table observations for numeric context differences.</summary>
public sealed class CalculatedNativeNumericValueTests
{
    /// <summary>Gets native source bits and text, including noncanonical and signaling NaNs.</summary>
    public static TheoryData<bool, long, string> NativeNaNTextCases { get; } = new()
    {
        { false, 9221120237041090560L, "nan" },
        { false, -2251799813685248L, "-nan(ind)" },
        { false, 9221120237041090561L, "nan" },
        { false, -2251799813685247L, "-nan" },
        { false, 9218868437227405313L, "nan(snan)" },
        { false, -4503599627370495L, "-nan(snan)" },
        { true, 2143289344L, "nan" },
        { true, -4194304L, "-nan(ind)" },
        { true, 2143289345L, "nan" },
        { true, -4194303L, "-nan" },
        { true, 2139095041L, "nan" },
        { true, -8388607L, "-nan" },
    };

    [Fact]
    public async Task CalculatedNegativeZero_TextFunctionsAndRoundRetainNativeContext()
    {
        // DAO120 16.0.20527.20112: source Double/Single retain their sign bits;
        // table text conversion preserves -0, while Round produces positive zero.
        (string Expression, string Expected)[] cases =
        [
            ("[D]", "-0"), ("+[D]", "-0"), ("-[D]", "0"), ("-CDbl(0)", "-0"), ("CDbl(0)", "0"), ("-0", "0"),
            ("Str([D])", "-0"), ("Str(-CDbl(0))", "-0"), ("Str$([D])", "-0"), ("[D]&\"\"", "-0"),
            ("Left([D],2)", "-0"), ("Right([D],1)", "0"), ("Mid([D],1,2)", "-0"), ("Len([D])", "2"), ("Asc([D])", "45"),
            ("String(2,[D])", "\0\0"), ("PV(0,1,0)", "-0"), ("PV(0,1,0)&\"\"", "-0"), ("Str(PV(0,1,0))", "-0"),
            ("Round([D],2)", "0"), ("Int([D])", "-0"), ("Fix([D])", "-0"), ("Abs([D])", "0"),
            ("[S]&\"\"", "-0"), ("Str([S])", "-0"), ("Left([S],2)", "-0"), ("[D]=\"-0\"", "-1"), ("[D] Like \"-0\"", "-1"),
            ("UCase([D])", "-0"), ("Trim([D])", "-0"), ("InStr(1,[D],\"-\")", "1"),
            ("Str(\"-0\")", "-0"), ("Str(\"1E100\")", " 1E+100"), ("Str(\"1.2345678901234567\")", " 1.23456789012346"), ("Str(\"1,5\")", " 15"),
            ("[D] In(\"-0\")", "-1"), ("[D] In(\"0\")", "0"), ("[D] In(Null,\"-0\")", "-1"),
            ("([D] Like \"-0\") & \"\"", "True"), ("IsNull(Null) & \"\"", "True"), ("IsEmpty([D]) & \"\"", "False"),
            ("(1 And 2) & \"\"", "-1"), ("True & \"\"", "-1"), ("IIf(True,IsNull(Null),False) & \"\"", "True"),
            ("IsNull(Null)", "-1"), ("IsEmpty([D])", "0"),
        ];
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            ColumnDefinition[] columns = new ColumnDefinition[] { new("D", typeof(double)), new("S", typeof(float)), new("Missing", typeof(string)) }
                .Concat(cases.Select((item, index) => new ColumnDefinition("T" + index, typeof(string)) { IsCalculated = true, CalculationExpression = item.Expression }))
                .Concat(
                [
                    new("Rounded", typeof(double)) { IsCalculated = true, CalculationExpression = "Round([D],2)" },
                    new("NullComparison", typeof(bool)) { IsCalculated = true, CalculationExpression = "[D]=[Missing]" },
                    new("AscBinary", typeof(byte[]), 255) { IsCalculated = true, CalculatedResultType = (byte)ColumnType.BinaryType, CalculationExpression = "Asc([D])" },
                    new("StoredBoolean", typeof(bool)) { IsCalculated = true, CalculationExpression = "IsNull(Null)" },
                    new("StoredBooleanText", typeof(string)) { IsCalculated = true, CalculationExpression = "[StoredBoolean] & \"\"" },
                ]).ToArray();
            await writer.CreateTableAsync("T", columns, TestContext.Current.CancellationToken);
            object?[] values = new object?[] { BitConverter.Int64BitsToDouble(long.MinValue), BitConverter.Int32BitsToSingle(int.MinValue), DBNull.Value }
                .Concat(Enumerable.Repeat<object?>(null, cases.Length + 5)).ToArray();
            await writer.InsertRowAsync("T", values, TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        object[] row = Assert.Single(await reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
        Assert.Equal(long.MinValue, BitConverter.DoubleToInt64Bits(Assert.IsType<double>(row[0])));
        Assert.Equal(int.MinValue, BitConverter.SingleToInt32Bits(Assert.IsType<float>(row[1])));
        for (int index = 0; index < cases.Length; index++)
        {
            Assert.True(Equals(cases[index].Expected, row[index + 3]), cases[index].Expression + " differs from native table text.");
        }

        Assert.Equal(0L, BitConverter.DoubleToInt64Bits(Assert.IsType<double>(row[^5])));
        Assert.Same(DBNull.Value, row[^4]);
        Assert.Equal(new byte[] { 45, 0 }, Assert.IsType<byte[]>(row[^3]));
        Assert.True(Assert.IsType<bool>(row[^2]));
        Assert.Equal("-1", row[^1]);
        Assert.Equal("0", CalculatedExpressionCoercion.ToText(BitConverter.Int64BitsToDouble(long.MinValue)));
    }

    [Theory]
    [InlineData("[D]+[N]", "3.5")]
    [InlineData("[N]+[D]", "3.5")]
    [InlineData("[D]-[N]", "-0.5")]
    [InlineData("[N]-[D]", "0.5")]
    [InlineData("[D]*[N]", "3")]
    [InlineData("[N]*[D]", "3")]
    [InlineData("[D]/[N]", "0.75")]
    [InlineData("[N]/[D]", "1.3333333333333333333333333333")]
    public async Task CalculatedDateAndDecimal_PromoteToNativeDecimalVariant(string expression, string expected)
    {
        // Native table Binary caches contain Decimal text, not eight Date/Double
        // bytes. Fresh ACE query TypeName independently reports Decimal too.
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("T", [new("D", typeof(DateTime)), new("N", typeof(decimal)) { NumericPrecision = 28, NumericScale = 0 }, new("B", typeof(byte[]), 255) { IsCalculated = true, CalculatedResultType = (byte)ColumnType.BinaryType, CalculationExpression = expression }, new("Text", typeof(string)) { IsCalculated = true, CalculationExpression = expression }], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [new DateTime(1899, 12, 31, 12, 0, 0), 2m, null, null], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        object[] row = Assert.Single(await reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
        Assert.Equal(new DateTime(1899, 12, 31, 12, 0, 0), row[0]);
        Assert.Equal(2m, row[1]);
        Assert.Equal(Encoding.Unicode.GetBytes(expected), Assert.IsType<byte[]>(row[2]));
        Assert.Equal(expected, row[3]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void SharedDateAndDecimal_RetainIndependentlyQueriedNativePromotion(bool native)
    {
        object date = native ? NativeDate.FromSerial(1.5d) : new DateTime(1899, 12, 31, 12, 0, 0);
        Assert.Equal(3.5m, Assert.IsType<decimal>(AccessVariantOperators.Add(date, 2m)));
        Assert.Equal(-0.5m, Assert.IsType<decimal>(AccessVariantOperators.Subtract(date, 2m)));
        Assert.Equal(3m, Assert.IsType<decimal>(AccessVariantOperators.Multiply(date, 2m)));
        Assert.Equal(0.75m, Assert.IsType<decimal>(AccessVariantOperators.Divide(date, 2m)));
        Assert.Same(DBNull.Value, AccessVariantOperators.Add(date, DBNull.Value));
        Assert.Same(DBNull.Value, AccessVariantOperators.Subtract(DBNull.Value, 2m));
    }

    [Theory]
    [InlineData("PV(-1,10,-100,1000)", "-inf", -4503599627370496L)]
    [InlineData("PMT(-1,10,1000,0,1)", "-nan(ind)", -2251799813685248L)]
    [InlineData("SYD(1E308,0,1000,1)", "inf", 9218868437227405312L)]
    [InlineData("-(PMT(-1,10,1000,0,1))", "nan", 9221120237041090560L)]
    [InlineData("Abs(PMT(-1,10,1000,0,1))", "nan", 9221120237041090560L)]
    [InlineData("Round(PMT(-1,10,1000,0,1),2)", "-nan(ind)", -2251799813685248L)]
    public async Task CalculatedFinancialNonfiniteText_PreservesNativeSpellingAndVariantBits(string expression, string expected, long bits)
    {
        ArgumentNullException.ThrowIfNull(expected);
        await AssertNonfiniteTextAsync(expression, expected, BitConverter.Int64BitsToDouble(bits), calculatedSource: true);
    }

    [Theory]
    [MemberData(nameof(NativeNaNTextCases))]
    public async Task CalculatedNonfiniteSourceText_PreservesNativeNaNPayloadAndSign(bool narrow, long bits, string expected)
    {
        ArgumentNullException.ThrowIfNull(expected);
        object value = narrow ? BitConverter.Int32BitsToSingle(checked((int)bits)) : (object)BitConverter.Int64BitsToDouble(bits);
        await AssertNonfiniteTextAsync("[D]", expected, value, calculatedSource: false);
    }

    private static async Task AssertNonfiniteTextAsync(string expression, string expected, object source, bool calculatedSource)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            ColumnDefinition sourceColumn = calculatedSource
                ? new("D", typeof(double)) { IsCalculated = true, CalculationExpression = expression }
                : new("D", source.GetType());
            await writer.CreateTableAsync("T", [sourceColumn, new("B", typeof(byte[]), 255) { IsCalculated = true, CalculatedResultType = (byte)ColumnType.BinaryType, CalculationExpression = "[D]" }, new("Direct", typeof(string)) { IsCalculated = true, CalculationExpression = "[D]" }, new("Concat", typeof(string)) { IsCalculated = true, CalculationExpression = "[D] & \"\"" }, new("StrText", typeof(string)) { IsCalculated = true, CalculationExpression = "Str([D])" }], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [calculatedSource ? null : source, null, null, null, null], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        object[] row = Assert.Single(await reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
        byte[] bytes;
        if (source is float number)
        {
            Assert.Equal(BitConverter.SingleToInt32Bits(number), BitConverter.SingleToInt32Bits(Assert.IsType<float>(row[0])));
            bytes = new byte[sizeof(float)];
            BinaryPrimitives.WriteInt32LittleEndian(bytes, BitConverter.SingleToInt32Bits(number));
        }
        else
        {
            double value = Assert.IsType<double>(source);
            Assert.Equal(BitConverter.DoubleToInt64Bits(value), BitConverter.DoubleToInt64Bits(Assert.IsType<double>(row[0])));
            bytes = new byte[sizeof(double)];
            BinaryPrimitives.WriteInt64LittleEndian(bytes, BitConverter.DoubleToInt64Bits(value));
        }

        Assert.Equal(bytes, Assert.IsType<byte[]>(row[1]));
        Assert.Equal(expected, row[2]);
        Assert.Equal(expected, row[3]);
        Assert.Equal(expected.StartsWith('-') ? expected : " " + expected, row[4]);
    }
}
