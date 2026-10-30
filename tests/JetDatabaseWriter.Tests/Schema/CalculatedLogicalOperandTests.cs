namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Table logical operators normalize operands independently from generic VBA bitwise operators.</summary>
public sealed class CalculatedLogicalOperandTests
{
    public static TheoryData<string, double> NativeValues { get; } = new()
    {
        { "1 And 2", -1 },
        { "2 Or 4", -1 },
        { "Not 1", 0 },
        { "0.1 And 1", 0 },
        { "0.5 And 1", 0 },
        { "0.6 And 1", -1 },
        { "1E-100 And 1", 0 },
        { "2147483647 And 1", -1 },
        { "2147483648 And 1", -1 },
        { "-2147483648 And 1", -1 },
        { "-2147483649 Or 1", -1 },
        { "1E100 And 1", -1 },
        { "1E100 Or 1", -1 },
        { "Not 1E100", 0 },
        { "\"bad\" And 1", -1 },
        { "\"bad\" Or 1", -1 },
        { "Not \"bad\"", 0 },
        { "\"\" And 1", -1 },
        { "\"0\" And 1", 0 },
        { "\"False\" Or 0", -1 },
        { "\"1E2\" And 1", -1 },
        { "\"1E999\" And 1", -1 },
        { "\"2147483648\" And 1", -1 },
        { "PV(-1,10,-100,1000) And 1", -1 },
        { "PMT(-1,10,1000,0,1) And 1", -1 },
        { "Not PV(-1,10,-100,1000)", 0 },
        { "Not PMT(-1,10,1000,0,1)", 0 },
    };

    [Theory]
    [InlineData("[N] And [T]", -1d)]
    [InlineData("[T] Or [N]", -1d)]
    [InlineData("[N] And [H]", -1d)]
    [InlineData("[H] Or [N]", -1d)]
    [MemberData(nameof(NativeValues))]
    public void LogicalOperands_UseNativeRoundedBooleanValues(string expression, double expected)
    {
        var definition = new TableDef
        {
            Columns =
            [
                new ColumnInfo { Name = "N", Type = ColumnType.DoubleType },
                new ColumnInfo { Name = "T", Type = ColumnType.TextType },
                new ColumnInfo { Name = "H", Type = ColumnType.DoubleType },
                new ColumnInfo { Name = "V", Type = ColumnType.DoubleType, CalculatedResultType = ColumnType.DoubleType, ExtraFlags = 0xC0 },
            ],
        };
        ColumnConstraint[] constraints =
        [
            new() { Name = "N", ClrType = typeof(double), StorageType = ColumnType.DoubleType },
            new() { Name = "T", ClrType = typeof(string), StorageType = ColumnType.TextType },
            new() { Name = "H", ClrType = typeof(double), StorageType = ColumnType.DoubleType },
            new() { Name = "V", ClrType = typeof(double), IsCalculated = true, CalculatedResultType = ColumnType.DoubleType, CalculationExpression = expression },
        ];
        var context = new CalculatedExpressionEvaluationContext(definition, constraints, [1d, "bad", 1E100, DBNull.Value]);
        Assert.Equal(expected, Assert.IsType<double>(context.EvaluateColumn(3)));
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120_LogicalConversionsMatchNativeRoundedBooleanOperands()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.LogicalErrors");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string nativeCases = string.Join(",", NativeValues.Select(static item => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(item.Data.Item1)));
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            try {
                $db.Execute('CREATE TABLE T (N DOUBLE,[Text] TEXT(32),H DOUBLE)')
                $tdf=$db.TableDefs('T')
                foreach ($case in @(@('A','[N] And [Text]'),@('B','[Text] Or [N]'),@('C','[N] And [H]'),@('D','[H] Or [N]'))) {
                    $field=$tdf.CreateField($case[0],7)
                    $field.Expression=$case[1]
                    $tdf.Fields.Append($field)
                }
                $cases=@({{nativeCases}})
                for ($index=0;$index -lt $cases.Count;$index++) {
                    $field=$tdf.CreateField('V'+$index,7)
                    $field.Expression=$cases[$index]
                    $tdf.Fields.Append($field)
                }
                $db.Execute("INSERT INTO T (N,[Text],H) VALUES (1,'bad',1E100)",128)
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, created.StdOut + created.StdErr);
        await AssertRowAsync(session.SourcePath);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            session.SourcePath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("T", "N", 1d, new Dictionary<string, object?> { ["N"] = 2d }, TestContext.Current.CancellationToken));
        }

        await AssertRowAsync(session.SourcePath);
    }

    private static async Task AssertRowAsync(string path)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(
            path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal(-1d, Assert.IsType<double>(row[3]));
            Assert.Equal(-1d, Assert.IsType<double>(row[4]));
            Assert.Equal(-1d, Assert.IsType<double>(row[5]));
            Assert.Equal(-1d, Assert.IsType<double>(row[6]));
            int index = 7;
            foreach (TheoryDataRow<string, double> expected in NativeValues)
            {
                Assert.Equal(expected.Data.Item2, Assert.IsType<double>(row[index]));
                index++;
            }
        }
    }
}
