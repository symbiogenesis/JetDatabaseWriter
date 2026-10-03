namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Globalization;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using Xunit;

public sealed class ConstraintRegistryTests
{
    /// <summary>
    /// Date-column rule cases. <c>Date()</c> reads the local clock; dates two days either
    /// side of today's UTC date are on the same side of the local date.
    /// </summary>
    /// <returns>The rule, the value, and whether the rule accepts it.</returns>
    public static TheoryData<string, DateTime, bool> DateValidationRuleCases()
    {
        DateTime today = DateTime.UtcNow.Date;
        return new TheoryData<string, DateTime, bool>
        {
            { "<=Date()", today.AddDays(-2), true },
            { "<=Date()", today.AddDays(2), false },
            { ">=#2000-01-01#", new DateTime(1999, 12, 31), false },
            { ">=#2000-01-01#", new DateTime(2000, 1, 1), true },
            { ">=Date()-30", today.AddDays(-2), true },
            { ">=Date()-30", today.AddDays(-40), false },
            { "<Date()+1", today.AddDays(-2), true },
            { "<Date()+1", today.AddDays(3), false },
            { "Between Date()-7 And Date()+7", today.AddDays(-9), false },
            { "Between Date()-7 And Date()+7", today, true },
        };
    }

    [Fact]
    public async Task ApplyAsync_HydratedCalculatedColumn_UsesPersistedResultTypeClrProjection()
    {
        var tableDef = new TableDef
        {
            Columns =
            [
                new ColumnInfo { Name = "Score", Type = ColumnType.LongIntegerType },
                new ColumnInfo
                {
                    Name = "IsHigh",
                    Type = ColumnType.LongIntegerType,
                    ExtraFlags = Constants.CalculatedColumn.ExtFlagMask,
                },
            ],
        };

        ColumnPropertyBlock properties = BuildCalculatedColumnProperties(
            "IsHigh",
            ColumnType.BooleanType,
            "[Score] >= 10");
        var registry = new ConstraintRegistry((_, _) => ValueTask.FromResult<ColumnPropertyBlock?>(properties));
        object[] values = [12, DBNull.Value];

        _ = await registry.ApplyAsync("Calc", tableDef, values, TestContext.Current.CancellationToken);

        bool isHigh = Assert.IsType<bool>(values[1]);
        Assert.True(isHigh);
    }

    [Theory]
    [InlineData(">=0 And <=100", 50, true)]
    [InlineData(">=0 And <=100", -1, false)]
    [InlineData(">=0 And <=100", 101, false)]
    [InlineData(">=0 And <=100", null, true)]
    [InlineData("Is Not Null", null, false)]
    [InlineData("Is Not Null", 5, true)]
    [InlineData("Is Null Or >5", null, true)]
    [InlineData("Is Null Or >5", 3, false)]
    [InlineData("Between 1 And 10", 10, true)]
    [InlineData("Between 1 And 10", 11, false)]
    [InlineData("Between 1 And 10 Or 99", 99, true)]
    [InlineData("Not Between 1 And 10", 5, false)]
    [InlineData("In (1,2,3)", 2, true)]
    [InlineData("In (1,2,3)", 4, false)]
    [InlineData("Not In (1,2)", 1, false)]
    [InlineData("<>0", 0, false)]
    [InlineData("<>0", 7, true)]
    [InlineData("0 Or >100", 0, true)]
    [InlineData("0 Or >100", 50, false)]
    [InlineData("0 Or >100", 101, true)]
    [InlineData("Not 5", 5, false)]
    [InlineData("Not 5", 6, true)]
    [InlineData("(>=0 And <=10) Or 99", 99, true)]
    [InlineData("(>=0 And <=10) Or 99", 50, false)]
    [InlineData(">=0 and <=100", 101, false)]
    [InlineData("[Score] >= 0", -1, false)]
    [InlineData("([Score] And 4) <> 0", 12, true)]
    [InlineData("([Score] And 4) <> 0", 8, false)]
    [InlineData("<=&HFF", 255, true)]
    [InlineData("<=&HFF", 256, false)]
    [InlineData("In (&H10, &O20)", 16, true)]
    [InlineData("In (&H10, &O20)", 20, false)]
    public async Task ApplyAsync_HydratedValidationRule_NumberColumn_SuppliesTheImplicitOperand(string rule, int? value, bool accepted)
        => await AssertRuleAsync(rule, ColumnType.LongIntegerType, value, accepted);

    [Theory]
    [InlineData("Like \"A*\"", "Apple", true)]
    [InlineData("Like \"A*\"", "Banana", false)]
    [InlineData("Not Like \"A*\"", "Apple", false)]
    [InlineData("Like 'A*'", "Banana", false)]
    [InlineData("Like 'A*'", "Apple", true)]
    [InlineData("'M' Or 'F'", "f", true)]
    [InlineData("<>'it''s'", "it's", false)]
    [InlineData("\"M\" Or \"F\"", "f", true)]
    [InlineData("\"M\" Or \"F\"", "X", false)]
    [InlineData("<>\"\"", "", false)]
    [InlineData("Len([Score]) = 3", "abcd", false)]
    [InlineData("Len([Score]) = 3", "abc", true)]
    public async Task ApplyAsync_HydratedValidationRule_TextColumn_SuppliesTheImplicitOperand(string rule, string? value, bool accepted)
        => await AssertRuleAsync(rule, ColumnType.TextType, value, accepted);

    [Theory]
    [MemberData(nameof(DateValidationRuleCases))]
    public async Task ApplyAsync_HydratedValidationRule_DateColumn_SuppliesTheImplicitOperand(string rule, DateTime value, bool accepted)
        => await AssertRuleAsync(rule, ColumnType.DateTimeType, value, accepted);

    [Theory]
    [InlineData("MyFunction([Score]) > 0")]
    [InlineData("DLookUp(\"Id\",\"Other\") > 0")]
    [InlineData(">= And")]
    [InlineData("((>0)")]
    public async Task ApplyAsync_ValidationRuleThisLibraryCannotEvaluate_IsNotEnforced(string rule)
    {
        TableDef tableDef = SingleColumnTable(ColumnType.LongIntegerType);
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties("Score", (Constants.ColumnPropertyNames.ValidationRule, rule)));
        object[] values = [-1];

        _ = await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken);

        Assert.Equal(-1, values[0]);
    }

    /// <summary>
    /// A persisted default is evaluated and converted to the column's CLR type. The
    /// expected value is given as its type and invariant text so the data serializes.
    /// </summary>
    /// <param name="expression">The persisted DefaultValue expression.</param>
    /// <param name="type">The column type.</param>
    /// <param name="expectedType">The CLR type the default must have.</param>
    /// <param name="expectedText">The default's invariant text.</param>
    [Theory]
    [InlineData("7", ColumnType.LongIntegerType, typeof(int), "7")]
    [InlineData("=7", ColumnType.LongIntegerType, typeof(int), "7")]
    [InlineData("-3", ColumnType.IntegerType, typeof(short), "-3")]
    [InlineData("1+2", ColumnType.LongIntegerType, typeof(int), "3")]
    [InlineData("12.5", ColumnType.DoubleType, typeof(double), "12.5")]
    [InlineData("1234567890.123456789", ColumnType.NumericType, typeof(decimal), "1234567890.123456789")]
    [InlineData("0", ColumnType.MoneyType, typeof(decimal), "0")]
    [InlineData("\"hi\"", ColumnType.TextType, typeof(string), "hi")]
    [InlineData("\"say \"\"hi\"\"\"", ColumnType.TextType, typeof(string), "say \"hi\"")]
    [InlineData("'N/A'", ColumnType.TextType, typeof(string), "N/A")]
    [InlineData("='it''s'", ColumnType.TextType, typeof(string), "it's")]
    [InlineData("=&HFF", ColumnType.LongIntegerType, typeof(int), "255")]
    [InlineData("&HFFFF&", ColumnType.LongIntegerType, typeof(int), "65535")]
    [InlineData("True", ColumnType.BooleanType, typeof(bool), "True")]
    [InlineData("No", ColumnType.BooleanType, typeof(bool), "False")]
    [InlineData("=No", ColumnType.BooleanType, typeof(bool), "False")]
    [InlineData("#2020-01-31#", ColumnType.DateTimeType, typeof(DateTime), "2020-01-31 00:00:00")]
    [InlineData("#2026-04-24 13:05:30#", ColumnType.DateTimeType, typeof(DateTime), "2026-04-24 13:05:30")]
    [InlineData("#2020-01-31#+1", ColumnType.DateTimeType, typeof(DateTime), "2020-02-01 00:00:00")]
    [InlineData("=#2020-01-31 06:00# - 0.25", ColumnType.DateTimeType, typeof(DateTime), "2020-01-31 00:00:00")]
    [InlineData("#2020-01-31 18:30:05#", ColumnType.TextType, typeof(string), "1/31/2020 6:30:05 PM")]
    [InlineData("=#2020-01-31# & \"\"", ColumnType.TextType, typeof(string), "1/31/2020")]
    [InlineData("#6:00#", ColumnType.DateTimeType, typeof(DateTime), "1899-12-30 06:00:00")]
    [InlineData("=CDate(\"6:00:00 PM\")", ColumnType.DateTimeType, typeof(DateTime), "1899-12-30 18:00:00")]
    [InlineData("=TimeValue(\"6:00 PM\")", ColumnType.DateTimeType, typeof(DateTime), "1899-12-30 18:00:00")]
    [InlineData("=TimeSerial(18, 30, 5)", ColumnType.DateTimeType, typeof(DateTime), "1899-12-30 18:30:05")]
    [InlineData("=CStr(TimeSerial(18, 30, 5))", ColumnType.TextType, typeof(string), "6:30:05 PM")]
    [InlineData("{guid 12345678-1234-1234-1234-1234567890ab}", ColumnType.GuidType, typeof(Guid), "12345678-1234-1234-1234-1234567890ab")]
    [InlineData("{guid {12345678-1234-1234-1234-1234567890AB}}", ColumnType.GuidType, typeof(Guid), "12345678-1234-1234-1234-1234567890ab")]
    public async Task ApplyAsync_HydratedDefaultValue_FillsDbDefault(string expression, ColumnType type, Type expectedType, string expectedText)
    {
        TableDef tableDef = SingleColumnTable(type);
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties("Score", (Constants.ColumnPropertyNames.DefaultValue, expression)));
        object[] values = [DbDefault.Value];

        _ = await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken);

        Assert.IsType(expectedType, values[0]);
        string actualText = values[0] switch
        {
            DateTime dateTime => dateTime.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
            Guid guid => guid.ToString("D", CultureInfo.InvariantCulture),
            _ => Convert.ToString(values[0], CultureInfo.InvariantCulture)!,
        };
        Assert.Equal(expectedText, actualText);
    }

    /// <summary>
    /// A persisted Double default is parsed as a double, so it is exact. It used to be
    /// parsed as a decimal first, which turned anything below about 1e-28 into 0 and
    /// rounded the rest through <see cref="Convert.ToDouble(decimal)"/>, which is not
    /// correctly rounded.
    /// </summary>
    /// <param name="expression">The persisted DefaultValue expression.</param>
    /// <param name="expected">The double it denotes.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData("1E-30", 1e-30)]
    [InlineData("1.2345678901234567E-20", 1.2345678901234567E-20)]
    [InlineData("0.30000000000000004", 0.30000000000000004)]
    [InlineData("1E+300", 1e300)]
    [InlineData("4.9406564584124654E-324", double.Epsilon)]
    [InlineData("1.7976931348623157E+308", double.MaxValue)]
    [InlineData("-2.5E-25", -2.5e-25)]
    public async Task ApplyAsync_HydratedDoubleDefault_IsExact(string expression, double expected)
    {
        TableDef tableDef = SingleColumnTable(ColumnType.DoubleType);
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties("Score", (Constants.ColumnPropertyNames.DefaultValue, expression)));
        object[] values = [DbDefault.Value];

        _ = await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken);

        Assert.Equal(BitConverter.DoubleToInt64Bits(expected), BitConverter.DoubleToInt64Bits(Assert.IsType<double>(values[0])));
    }

    /// <summary>
    /// A persisted Single default is parsed as a float, so it is exact, including a
    /// subnormal value that a decimal parse turned into 0.
    /// </summary>
    /// <param name="expression">The persisted DefaultValue expression.</param>
    /// <param name="expected">The float it denotes.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData("1E-45", float.Epsilon)]
    [InlineData("3.4028235E+38", float.MaxValue)]
    [InlineData("0.1", 0.1f)]
    [InlineData("1.17549435E-38", 1.17549435E-38f)]
    public async Task ApplyAsync_HydratedSingleDefault_IsExact(string expression, float expected)
    {
        TableDef tableDef = SingleColumnTable(ColumnType.FloatType);
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties("Score", (Constants.ColumnPropertyNames.DefaultValue, expression)));
        object[] values = [DbDefault.Value];

        _ = await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken);

        Assert.Equal(BitConverter.SingleToInt32Bits(expected), BitConverter.SingleToInt32Bits(Assert.IsType<float>(values[0])));
    }

    /// <summary>
    /// A Single default too large for a float produces no default. It used to store
    /// positive infinity.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task ApplyAsync_SingleDefaultOutOfRange_LeavesNull()
    {
        TableDef tableDef = SingleColumnTable(ColumnType.FloatType);
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties("Score", (Constants.ColumnPropertyNames.DefaultValue, "1E+39")));
        object[] values = [DbDefault.Value];

        _ = await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken);

        Assert.Equal(DBNull.Value, values[0]);
    }

    /// <summary>
    /// Every finite double and float survives the round trip through the literal a CLR
    /// default is persisted as (its round-trip <c>"R"</c> text). Before, about one
    /// mid-range double in ten and one float in six came back different.
    /// </summary>
    [Fact]
    public void ColumnDefaultValue_FloatingLiteral_RoundTripsEveryRFormattedValue()
    {
        var random = new Random(20261003);

#pragma warning disable CA5394 // Deterministic test values; nothing here needs a secure generator.
        for (int i = 0; i < 5000; i++)
        {
            double d = BitConverter.Int64BitsToDouble(random.NextInt64() & 0x7FEFFFFFFFFFFFFF);
            d = random.Next(2) == 0 ? d : -d;
            object doubleValue = EvaluateLiteral(d.ToString("R", CultureInfo.InvariantCulture), typeof(double));
            Assert.Equal(BitConverter.DoubleToInt64Bits(d), BitConverter.DoubleToInt64Bits((double)doubleValue));

            float f = BitConverter.Int32BitsToSingle(random.Next() & 0x7F7FFFFF);
            f = random.Next(2) == 0 ? f : -f;
            object singleValue = EvaluateLiteral(f.ToString("R", CultureInfo.InvariantCulture), typeof(float));
            Assert.Equal(BitConverter.SingleToInt32Bits(f), BitConverter.SingleToInt32Bits((float)singleValue));
        }
#pragma warning restore CA5394
    }

    [Fact]
    public async Task ApplyAsync_HydratedDefaultValue_DateFunctionsUseTheClock()
    {
        var tableDef = new TableDef
        {
            Columns =
            [
                new ColumnInfo { Name = "Today", Type = ColumnType.DateTimeType },
                new ColumnInfo { Name = "Stamp", Type = ColumnType.DateTimeType },
            ],
        };
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties(
            ("Today", Constants.ColumnPropertyNames.DefaultValue, "Date()"),
            ("Stamp", Constants.ColumnPropertyNames.DefaultValue, "=Now()")));
        object[] values = [DbDefault.Value, DbDefault.Value];

        // Access Date() and Now() read the local clock.
        DateTime before = DateTimeOffset.Now.DateTime;
        _ = await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken);
        DateTime after = DateTimeOffset.Now.DateTime;

        Assert.Equal(before.Date, Assert.IsType<DateTime>(values[0]).Date);
        DateTime stamp = Assert.IsType<DateTime>(values[1]);
        Assert.InRange(stamp, before.AddSeconds(-1), after.AddSeconds(1));
    }

    /// <summary>
    /// A <c>=Time()</c> default, a common time-stamp default in Access, stores the
    /// current time on day 0 (1899-12-30), as Access does. A <c>=CStr(Time())</c>
    /// default in a Text column stores the time without a date. Both used to carry
    /// today's date.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task ApplyAsync_HydratedDefaultValue_TimeIsOnDayZero()
    {
        var tableDef = new TableDef
        {
            Columns =
            [
                new ColumnInfo { Name = "Stamp", Type = ColumnType.DateTimeType },
                new ColumnInfo { Name = "Label", Type = ColumnType.TextType },
            ],
        };
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties(
            ("Stamp", Constants.ColumnPropertyNames.DefaultValue, "=Time()"),
            ("Label", Constants.ColumnPropertyNames.DefaultValue, "=CStr(Time())")));
        object[] values = [DbDefault.Value, DbDefault.Value];

        _ = await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken);

        Assert.Equal(new DateTime(1899, 12, 30), Assert.IsType<DateTime>(values[0]).Date);
        Assert.DoesNotContain("/", Assert.IsType<string>(values[1]), StringComparison.Ordinal);
    }

    [Fact]
    public async Task ApplyAsync_HydratedDefaultValue_DateArithmeticUsesTheClock()
    {
        TableDef tableDef = SingleColumnTable(ColumnType.DateTimeType);
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties("Score", (Constants.ColumnPropertyNames.DefaultValue, "=Date()+7")));
        object[] values = [DbDefault.Value];

        // Access Date() reads the local clock.
        DateTime before = DateTimeOffset.Now.DateTime.Date;
        _ = await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken);
        DateTime after = DateTimeOffset.Now.DateTime.Date;

        Assert.InRange(Assert.IsType<DateTime>(values[0]), before.AddDays(7), after.AddDays(7));
    }

    [Fact]
    public async Task ApplyAsync_HydratedDefaultValue_DoesNotReplaceSuppliedValue()
    {
        TableDef tableDef = SingleColumnTable(ColumnType.LongIntegerType);
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties("Score", (Constants.ColumnPropertyNames.DefaultValue, "7")));
        object[] values = [3];

        _ = await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken);

        Assert.Equal(3, values[0]);
    }

    /// <summary>
    /// An explicit <see cref="DBNull"/> is stored as null, as an explicit Null is in an
    /// Access SQL INSERT; only <see cref="DbDefault"/> takes the persisted or CLR default.
    /// </summary>
    /// <param name="clrDefault">Whether the default is registered as a CLR default rather than read from the file.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ApplyAsync_ExplicitDbNull_IsNotReplacedByDefault(bool clrDefault)
    {
        TableDef tableDef = SingleColumnTable(ColumnType.LongIntegerType);
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties("Score", (Constants.ColumnPropertyNames.DefaultValue, "7")));
        if (clrDefault)
        {
            registry.Register("T", [new ColumnDefinition("Score", typeof(int)) { DefaultValue = 7 }]);
        }

        object[] explicitNull = [DBNull.Value];
        _ = await registry.ApplyAsync("T", tableDef, explicitNull, TestContext.Current.CancellationToken);
        Assert.Equal(DBNull.Value, explicitNull[0]);

        object[] requested = [DbDefault.Value];
        _ = await registry.ApplyAsync("T", tableDef, requested, TestContext.Current.CancellationToken);
        Assert.Equal(7, requested[0]);
    }

    /// <summary>
    /// <see cref="DbDefault"/> never reaches the row encoder: it becomes
    /// <see cref="DBNull"/> even when the registry skips the table because its
    /// constraint list does not line up with the table definition.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task ApplyAsync_DbDefault_IsReplacedWhenConstraintsAreSkipped()
    {
        TableDef tableDef = SingleColumnTable(ColumnType.LongIntegerType);
        var registry = new ConstraintRegistry();
        registry.Register("T", [new ColumnDefinition("Score", typeof(int)) { DefaultValue = 7 }, new ColumnDefinition("Other", typeof(int))]);
        object[] values = [DbDefault.Value];

        Assert.Null(await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken));

        Assert.Equal(DBNull.Value, values[0]);
    }

    [Theory]
    [InlineData("=CurrentUser()")]
    [InlineData("GenUniqueID()")]
    [InlineData("\"unterminated")]
    [InlineData("\"text\"")]
    public async Task ApplyAsync_DefaultValueThisLibraryCannotEvaluate_LeavesNull(string expression)
    {
        // "text" cannot become a Long Integer, so it is skipped like an unknown function.
        TableDef tableDef = SingleColumnTable(ColumnType.LongIntegerType);
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties("Score", (Constants.ColumnPropertyNames.DefaultValue, expression)));
        object[] values = [DbDefault.Value];

        _ = await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken);

        Assert.Equal(DBNull.Value, values[0]);
    }

    [Fact]
    public async Task ApplyAsync_HydratedDefaultValue_IsCheckedByTheValidationRule()
    {
        TableDef tableDef = SingleColumnTable(ColumnType.LongIntegerType);
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties(
            ("Score", Constants.ColumnPropertyNames.DefaultValue, "-1"),
            ("Score", Constants.ColumnPropertyNames.ValidationRule, ">=0")));

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await registry.ApplyAsync("T", tableDef, [DbDefault.Value], TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// A default registered on an AutoNumber column is dropped, so a null value
    /// still gets the next AutoNumber rather than the default.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task Register_DefaultOnAutoNumberColumn_IsNotApplied()
    {
        TableDef tableDef = SingleColumnTable(ColumnType.LongIntegerType);
        var registry = new ConstraintRegistry();
        registry.Register("T", [new ColumnDefinition("Score", typeof(int)) { IsAutoIncrement = true, DefaultValueExpression = "0", DefaultValue = 5 }]);
        object[] values = [DbDefault.Value];

        _ = await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken);

        Assert.Equal(1, values[0]);
    }

    /// <summary>
    /// A numeric CLR default is registered as the value its persisted literal reads back
    /// as in the column's type, whatever the CLR type of the value, and a number the
    /// column's type cannot hold gives no default, as in a writer that reads the literal
    /// from the file. A decimal on a Double or Single column used to be converted, not
    /// parsed, and 1e39 on a Single column stored infinity.
    /// </summary>
    /// <param name="kind"><c>DecimalOnDouble</c>, <c>DecimalOnSingle</c>, <c>DoubleTooLargeForSingle</c> or <c>IntegerTooLargeForByte</c>.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData("DecimalOnDouble")]
    [InlineData("DecimalOnSingle")]
    [InlineData("DoubleTooLargeForSingle")]
    [InlineData("IntegerTooLargeForByte")]
    public async Task Register_NumericClrDefault_IsAppliedAsItsLiteralReadsBack(string kind)
    {
        (ColumnType Type, ColumnDefinition Column, object Expected) testCase = kind switch
        {
            "DecimalOnDouble" => (ColumnType.DoubleType, new ColumnDefinition("Score", typeof(double)) { DefaultValue = 0.0000000000000000000123456789m }, 1.23456789e-20),
            "DecimalOnSingle" => (ColumnType.FloatType, new ColumnDefinition("Score", typeof(float)) { DefaultValue = 1.00000005960464477539062501m }, 1.0000001f),
            "DoubleTooLargeForSingle" => (ColumnType.FloatType, new ColumnDefinition("Score", typeof(float)) { DefaultValue = 1e39 }, DBNull.Value),
            _ => (ColumnType.ByteType, new ColumnDefinition("Score", typeof(byte)) { DefaultValue = 300 }, DBNull.Value),
        };
        TableDef tableDef = SingleColumnTable(testCase.Type);
        var registry = new ConstraintRegistry();
        registry.Register("T", [testCase.Column]);
        object[] values = [DbDefault.Value];

        _ = await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken);

        Assert.Equal(testCase.Expected, values[0]);
    }

    [Fact]
    public async Task ApplyUpdateAsync_HydratedValidationRule_ChecksOnlyAssignedColumns()
    {
        var tableDef = new TableDef
        {
            Columns =
            [
                new ColumnInfo { Name = "A", Type = ColumnType.LongIntegerType },
                new ColumnInfo { Name = "B", Type = ColumnType.LongIntegerType },
            ],
        };
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties(
            ("A", Constants.ColumnPropertyNames.ValidationRule, ">=0"),
            ("B", Constants.ColumnPropertyNames.ValidationRule, ">=0")));

        // B already violates its rule; an update that assigns only A is still checked only on A.
        await registry.ApplyUpdateAsync("T", tableDef, [5, -1], [0], TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await registry.ApplyUpdateAsync("T", tableDef, [-5, 1], [0], TestContext.Current.CancellationToken));
    }

    private static async Task AssertRuleAsync(string rule, ColumnType type, object? value, bool accepted)
    {
        TableDef tableDef = SingleColumnTable(type);
        ConstraintRegistry registry = RegistryWithProperties(BuildColumnProperties("Score", (Constants.ColumnPropertyNames.ValidationRule, rule)));
        object[] values = [value ?? DBNull.Value];

        if (accepted)
        {
            _ = await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken);
        }
        else
        {
            ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await registry.ApplyAsync("T", tableDef, values, TestContext.Current.CancellationToken));
            Assert.Contains(rule, ex.Message, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// Evaluates a default that needs no row, such as a numeric literal, and asserts
    /// that it produces a value.
    /// </summary>
    /// <param name="expression">The DefaultValue expression.</param>
    /// <param name="clrType">The column's CLR type.</param>
    /// <returns>The default value.</returns>
    private static object EvaluateLiteral(string expression, Type clrType)
    {
        Assert.True(ColumnDefaultValue.Compile(expression).TryEvaluate(
            clrType,
            static () => throw new InvalidOperationException("A literal needs no evaluation context."),
            out object value));
        return value;
    }

    private static TableDef SingleColumnTable(ColumnType type) => new()
    {
        Columns = [new ColumnInfo { Name = "Score", Type = type }],
    };

    private static ConstraintRegistry RegistryWithProperties(ColumnPropertyBlock properties) => new(
        (_, _) => ValueTask.FromResult<ColumnPropertyBlock?>(properties));

    private static ColumnPropertyBlock BuildColumnProperties(string columnName, (string Name, string Value) property)
        => BuildColumnProperties((columnName, property.Name, property.Value));

    private static ColumnPropertyBlock BuildColumnProperties(params (string Column, string Name, string Value)[] properties)
    {
        var builder = new ColumnPropertyBlockBuilder();
        foreach ((string column, string name, string value) in properties)
        {
            builder.GetOrAddTarget(column).AddText(name, value, DatabaseFormat.AceAccdb);
        }

        return ColumnPropertyBlock.Parse(builder.ToBytes(DatabaseFormat.AceAccdb), DatabaseFormat.AceAccdb)!;
    }

    private static ColumnPropertyBlock BuildCalculatedColumnProperties(
        string columnName,
        ColumnType resultType,
        string expression)
    {
        var builder = new ColumnPropertyBlockBuilder();
        ColumnPropertyTargetBuilder target = builder.GetOrAddTarget(columnName);
        target.AddMemoText(Constants.ColumnPropertyNames.Expression, expression, DatabaseFormat.AceAccdb);
        target.AddByte(Constants.ColumnPropertyNames.ResultType, (byte)resultType);

        return ColumnPropertyBlock.Parse(builder.ToBytes(DatabaseFormat.AceAccdb), DatabaseFormat.AceAccdb)!;
    }
}
