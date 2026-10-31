namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Calculated operators preserve native argument evaluation, subtypes and cached errors.</summary>
public sealed class CalculatedOperatorStatusTests
{
    /// <summary>Gets native IN literal, runtime and selected-branch discriminator controls.</summary>
    public static TheoryData<string, int> NativeLiteralProvenance { get; } = new()
    {
        { "1 In (\"1\")", 13 },
        { "\"1\" In (1)", 13 },
        { "1.5 In (\"1.5\")", 13 },
        { "1E100 In (\"1E+100\")", 13 },
        { "-1 In (\"-1\")", 13 },
        { "(-1) In (\"-1\")", 13 },
        { "-(1) In (\"-1\")", -1 },
        { "-((1)) In (\"-1\")", -1 },
        { "- 1 In (\"-1\")", -1 },
        { "- (1) In (\"-1\")", -1 },
        { "--1 In (\"1\")", -1 },
        { "+1 In (\"1\")", -1 },
        { "(1+0) In (\"1\")", -1 },
        { "Abs(1) In (\"1\")", -1 },
        { "CDbl(1) In (\"1\")", -1 },
        { "1 In (1&\"\")", -1 },
        { "1 In (+\"1\")", -1 },
        { "1 In (Choose(1,\"1\",\"2\"))", -1 },
        { "IIf(True,1,CDbl(2)) In (\"1\")", 13 },
        { "IIf(True,CDbl(1),2) In (\"1\")", -1 },
        { "1 In (IIf(True,\"1\",UCase(\"2\")))", 13 },
        { "1 In (IIf(True,UCase(\"1\"),\"2\"))", -1 },
        { "-IIf(True,1,2) In (\"-1\")", -1 },
        { "IIf(True,1,2) In (UCase(\"1\"))", -1 },
        { "-CDbl(0) In (\"-0\")", -1 },
        { "-CDbl(0) In (\"0\")", 0 },
        { "IsNull(Null) In (\"True\")", 0 },
        { "\"True\" In (IsNull(Null))", 0 },
        { "IsNull(Null) In (\"-1\")", -1 },
        { "\"-1\" In (IsNull(Null))", -1 },
        { "1 In(UCase(\"bad\"))", 13 },
        { "UCase(\"bad\") In(1)", 13 },
        { "1 In(UCase(\"01\"))", -1 },
        { "1 In(UCase(\"1.0\"))", -1 },
        { "1 In(UCase(\"1.5\"))", 0 },
        { "1 In(UCase(\"1E100\"))", 0 },
        { "1 In(UCase(\"1E999\"))", 6 },
        { "CDbl(1) In(UCase(\"01\"))", 0 },
        { "UCase(\"01\") In(CDbl(1))", 0 },
        { "9007199254740993 In(UCase(\"9007199254740993\"))", -1 },
        { "9007199254740993 In(UCase(\"9007199254740992\"))", 0 },
        { "9007199254740992 In(UCase(\"9007199254740993\"))", 0 },
        { "1.0000000000000002 In(UCase(\"1.0000000000000002\"))", -1 },
        { "1.5 In(UCase(\"1.5000000000000000000000000001\"))", 0 },
        { "1 In(UCase(\"1.0000000000000000000000000001\"))", -1 },
    };

    [Theory]
    [MemberData(nameof(NativeLiteralProvenance))]
    public void Membership_PreservesOnlySelectedNativeLiteralDiscriminators(string expression, int expected)
    {
        var table = new TableDef { Columns = [new ColumnInfo { Name = "V", Type = ColumnType.DoubleType, ExtraFlags = 0xC0, CalculatedResultType = ColumnType.DoubleType }] };
        ColumnConstraint[] constraints = [new() { Name = "V", IsCalculated = true, ClrType = typeof(double), CalculatedResultType = ColumnType.DoubleType, CalculationExpression = expression }];
        object actual = new CalculatedExpressionEvaluationContext(table, constraints, [DBNull.Value]).EvaluateColumn(0);
        if (expected is 6 or 13)
        {
            Assert.Equal(expected, Assert.IsType<CalculatedError>(actual).NativeCode);
        }
        else
        {
            Assert.Equal(expected, Assert.IsType<double>(actual));
        }
    }

    [Theory]
    [InlineData("native-membership-conversion.accdb", "30FFFE459B8257EA385C8EE451B474AEF8724BA0872712E4E71C6639023A8A42")]
    [InlineData("native-membership-conversion-compacted.accdb", "D26FE689050995AE0545DF6D77357A666E05F52F59FCC034D012CA94F223E4DA")]
    public async Task NativeMembershipConversion_TextMemoAndDecimalInputsRetainEveryCache(string fixture, string hash)
    {
        string path = Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedMembershipConversion", fixture);
        byte[] bytes = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(bytes)));
        await using var stream = new MemoryStream();
        await stream.WriteAsync(bytes, TestContext.Current.CancellationToken);
        object[][] native;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            native = (await reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken)).OrderBy(row => row[0]).ToArray();
        }

        Assert.Equal(9, native.Length);
        Assert.Equal("iIıİäÄéÉßœ漢", native[8][1]);
        foreach (int index in new[] { 1, 2 })
        {
            Assert.Equal(-1d, native[index][5]);
            Assert.Equal(-1d, native[index][6]);
            Assert.Equal(0d, native[index][7]);
            Assert.Equal(0d, native[index][8]);
        }

        foreach (int index in new[] { 3, 4, 6, 8 })
        {
            int code = index == 4 ? 6 : 13;
            foreach (int ordinal in new[] { 5, 6, 11, 12 })
            {
                Assert.Equal(code, Assert.IsType<CalculatedError>(native[index][ordinal]).NativeCode);
            }
        }

        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false, CalculatedExpressionCulture = CultureInfo.GetCultureInfo("en-US") }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            foreach (object[] row in native)
            {
                Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", row[0], new Dictionary<string, object?> { ["S"] = row[1] }, TestContext.Current.CancellationToken));
                await writer.InsertRowAsync("T", new RowValues { ["Id"] = (int)row[0] + 100, ["S"] = row[1], ["M"] = row[2], ["D"] = row[3], ["I"] = row[4] }, TestContext.Current.CancellationToken);
            }
        }

        await using AccessReader reopened = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        object[][] rows = (await reopened.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken)).OrderBy(row => row[0]).ToArray();
        Assert.Equal(native.Length * 2, rows.Length);
        foreach (object[] row in rows)
        {
            int index = ((int)row[0] % 100) - 1;
            Assert.Equal(native[index][1..], row[1..]);
        }
    }

    [Theory]
    [InlineData("native-membership-provenance.accdb", "8CF1CB72889DC7EE0AE31B1055419D9CEDF51788CA9BB70106DEFB0545027759")]
    [InlineData("native-membership-provenance-compacted.accdb", "BBD0C7D1EC4FBC7207EB1814C5DA61174D39231BECA293505C8FA34E67267870")]
    public async Task NativeMembershipProvenance_FieldReadsAndSelectedBranchesRetainNativeCaches(string fixture, string hash)
    {
        string path = Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedMembershipProvenance", fixture);
        byte[] bytes = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(bytes)));
        await using var stream = new MemoryStream();
        await stream.WriteAsync(bytes, TestContext.Current.CancellationToken);
        object[] native;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            native = Assert.Single(await reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
        }

        Assert.Equal(13, Assert.IsType<CalculatedError>(native[7]).NativeCode);
        Assert.Equal(13, Assert.IsType<CalculatedError>(native[8]).NativeCode);
        Assert.Equal(-1d, native[9]);
        Assert.Equal(-1d, native[10]);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false, CalculatedExpressionCulture = CultureInfo.GetCultureInfo("en-US") }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", 1, new Dictionary<string, object?> { ["S"] = "1" }, TestContext.Current.CancellationToken));
            await writer.InsertRowAsync("T", new RowValues { ["Id"] = 1, ["S"] = "1", ["NegS"] = "-1" }, TestContext.Current.CancellationToken);
        }

        await using AccessReader reopened = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        object[][] rows = await reopened.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Length);
        Assert.All(rows, row => Assert.Equal(native, row));
    }

    [Theory]
    [InlineData("native-membership.accdb", "31E420C894D0117E75B346B117043F3F1B9A752E9A8E3D06AF1AB141B3D866F8")]
    [InlineData("native-membership-compacted.accdb", "DEA2BF5E77F0F94BF707F621CCECA32ADCB06B99EE0181CEBCD2FAF993FDAE4B")]
    public async Task NativeMembershipFixtures_RetainNativeTypeMismatchAndFormattedComparison(string fixture, string hash)
    {
        string path = Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedMembership", fixture);
        byte[] bytes = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(bytes)));
        await using var stream = new MemoryStream();
        await stream.WriteAsync(bytes, TestContext.Current.CancellationToken);
        object[] native;
        Dictionary<string, object> named;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            native = Assert.Single(await reader.Rows("Membership", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
            IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync("Membership", TestContext.Current.CancellationToken);
            named = columns.Select((column, index) => (column.Name, Value: native[index])).ToDictionary(pair => pair.Name, pair => pair.Value, StringComparer.Ordinal);
        }

        foreach (string name in new[] { "NumberText", "NumberBadText", "TextNumber", "TextBadNumber", "NumberGroupedText", "NumberExponentText", "BooleanText", "NullCandidate", "GuidNumber", "NumberGuid" })
        {
            Assert.Equal(13, Assert.IsType<CalculatedError>(named[name]).NativeCode);
        }

        foreach (string name in new[] { "EarlyNumber", "EarlyText", "DateCanonical", "DateAfterBadText", "TextDate", "DateTimeCanonical", "DateNumber", "NumberDate", "GuidText", "TextGuid", "InfinityText", "TextInfinity", "NegativeInfinityText", "NaNCanonical", "NaNNumber", "NumberNaN", "InfinitySelf" })
        {
            Assert.Equal(-1d, named[name]);
        }

        foreach (string name in new[] { "DatePadded", "DateBadText", "BadTextDate", "DateMidnightText", "GuidBadText", "BadTextGuid", "InfinityBadText", "NegativeInfinityOther", "NaNOtherText", "InfinityNumber" })
        {
            Assert.Equal(0d, named[name]);
        }

        Assert.Equal(DBNull.Value, named["NullNeedle"]);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("Membership", "Id", 1, new Dictionary<string, object?> { ["Id"] = 2 }, TestContext.Current.CancellationToken));
            await writer.InsertRowAsync("Membership", new RowValues { ["Id"] = 3 }, TestContext.Current.CancellationToken);
        }

        await using AccessReader reopened = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        object[][] written = await reopened.Rows("Membership", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, written.Length);
        Assert.All(written, row => Assert.Equal(native[1..], row[1..]));
    }

    [Theory]
    [InlineData("native-operators.accdb", "079445EF0BC57C9E41949D1E9A0AFE6EF357163F2F6F3C0165C1BAC11C984434")]
    [InlineData("native-operators-compacted.accdb", "73593FF49BBE2C01B93FA2A8368F069D5F14C3F7625BBE41161CC3B8054820C3")]
    public async Task NativeOperatorFixtures_PreserveStatusesAndUnaryTextIdentity(string fixture, string hash)
    {
        string path = Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedOperators", fixture);
        byte[] bytes = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(bytes)));
        await using var stream = new MemoryStream();
        await stream.WriteAsync(bytes, TestContext.Current.CancellationToken);
        object[][] unary;
        object[] membership;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            unary = (await reader.Rows("Unary", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken)).OrderBy(row => row[0]).ToArray();
            membership = Assert.Single(await reader.Rows("Membership", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
        }

        Assert.Equal(8, unary.Length);
        for (int index = 0; index < unary.Length; index++)
        {
            object[] row = unary[index];
            if (row[1] is string text)
            {
                Assert.Equal(Encoding.Unicode.GetBytes(text), Assert.IsType<byte[]>(row[4]));
                Assert.Equal(text, row[6]);
            }

            if (index is 0 or 1)
            {
                int code = index == 0 ? 13 : 6;
                foreach (int ordinal in new[] { 2, 3, 5 })
                {
                    Assert.Equal(code, Assert.IsType<CalculatedError>(row[ordinal]).NativeCode);
                }
            }
        }

        Assert.Equal(1E100d, unary[2][2]);
        Assert.Equal(-1E100d, unary[2][3]);
        Assert.Equal(Convert.FromHexString("7DC39425AD49B2D4"), Assert.IsType<byte[]>(unary[2][5]));
        Assert.All(unary[5][2..], value => Assert.Equal(DBNull.Value, value));
        Assert.Equal(11, Assert.IsType<CalculatedError>(membership[2]).NativeCode);
        Assert.Equal(DBNull.Value, membership[3]);
        Assert.Equal(-1d, membership[1]);

        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            foreach (object[] row in unary)
            {
                Assert.Equal(1, await writer.UpdateRowsAsync("Unary", "Id", row[0], new Dictionary<string, object?> { ["S"] = row[1] }, TestContext.Current.CancellationToken));
            }

            Assert.Equal(1, await writer.UpdateRowsAsync("Membership", "Id", 1, new Dictionary<string, object?> { ["Id"] = 1 }, TestContext.Current.CancellationToken));
        }

        await using AccessReader reopened = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(unary, (await reopened.Rows("Unary", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken)).OrderBy(row => row[0]).ToArray());
        Assert.Equal(membership, Assert.Single(await reopened.Rows("Membership", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken)));
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task Dao120_OperatorsMatchNativeCachedValues()
    {
        string[] expressions = [
            "1 In (1,1/0)", "1 In (2,1/0)", "Null In (1/0,1)",
            "1 In (1/0,1)", "1 In (1/0,2)", "Null In (1,1/0)",
            "1 In (1,Null)", "1 In (2,Null)", "Null In (1,2)",
            "1 In (1,Sqr(-1))", "1 In (Sqr(-1),1)", "(1/0) In (1,2)",
            "+\"bad\"", "-\"bad\"", "+\"1E999\"", "-\"1E999\"",
            "+\"1E100\"", "-\"1E100\"", "+\"1.2345678901234567\"", "-\"1.2345678901234567\"",
            "1 In (\"1\")", "1 In (\"bad\",1)", "\"1\" In (1)",
            "#2020-01-01# In (\"bad\")", "#2020-01-01# In (\"bad\",#2020-01-01#)",
            "#2020-01-01# In (\"01/01/2020\")", "#2020-01-01# In (\"1/1/2020\")",
            "\"1/1/2020\" In (#2020-01-01#)", "#2020-01-01# In (43831)", "43831 In (#2020-01-01#)",
            "SYD(1E308,0,1000,1) In (\"inf\")", "\"inf\" In (SYD(1E308,0,1000,1))",
            "PMT(-1,10,1000,0,1) In (1)", "1 In (PMT(-1,10,1000,0,1))",
        ];
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CalculatedInErrors");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string cases = string.Join(",", expressions.Select(AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral));
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db=$engine.CreateDatabase({{path}},';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            try {
                $cases=@({{cases}})
                for($index=0;$index -lt $cases.Count;$index++) {
                    $table=$db.CreateTableDef('T'+$index)
                    $table.Fields.Append($table.CreateField('Id',4))
                    $db.TableDefs.Append($table)
                    $field=$table.CreateField('V',7)
                    $field.Expression=$cases[$index]
                    $table.Fields.Append($field)
                    $db.Execute('INSERT INTO T'+$index+' (Id) VALUES (1)',128)
                }
            } finally {$db.Close()}
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, created.StdOut + created.StdErr);
        var expected = new List<object>();
        await using (AccessReader reader = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            for (int index = 0; index < expressions.Length; index++)
            {
                object[] row = Assert.Single(await reader.Rows("T" + index, cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
                expected.Add(row[1]);
            }
        }

        Assert.Equal(13, Assert.IsType<CalculatedError>(expected[12]).NativeCode);
        Assert.Equal(13, Assert.IsType<CalculatedError>(expected[13]).NativeCode);
        Assert.Equal(6, Assert.IsType<CalculatedError>(expected[14]).NativeCode);
        Assert.Equal(6, Assert.IsType<CalculatedError>(expected[15]).NativeCode);

        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            for (int index = 0; index < expressions.Length; index++)
            {
                Assert.Equal(1, await writer.UpdateRowsAsync("T" + index, "Id", 1, new Dictionary<string, object?> { ["Id"] = 2 }, TestContext.Current.CancellationToken));
            }
        }

        await using AccessReader reopened = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        for (int index = 0; index < expressions.Length; index++)
        {
            object[] row = Assert.Single(await reopened.Rows("T" + index, cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
            Assert.True(Equals(expected[index], row[1]), expressions[index] + " native=" + expected[index] + " managed=" + row[1]);
        }
    }
}
