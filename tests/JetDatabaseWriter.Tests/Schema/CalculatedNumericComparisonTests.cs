namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native numeric comparisons use the operands' Variant subtypes before rounding.</summary>
public sealed class CalculatedNumericComparisonTests
{
    public static TheoryData<object, object, int> NativeValues { get; } = new()
    {
        { 1E-100, 0, 1 },
        { 1E100, 1E99, 1 },
        { 1d, 1.0000000000000002d, -1 },
        { 0.1f, 0.1000000001d, 0 },
        { 16777216f, 16777217d, 0 },
        { 16777216f, 16777217, -1 },
        { 1.0000000000000002m, 1.0000000000000002d, 1 },
        { 1m, 1.0000000000000002d, 0 },
        { 0m, 1E-100, 0 },
        { 1m, 1E100, -1 },
        { 1.0000001m, 1.0000001f, 1 },
        { AccessCurrency.FromDecimal(0.1m), 0.10001d, 0 },
        { AccessCurrency.FromDecimal(0.1m), 0.10006d, -1 },
        { AccessCurrency.FromDecimal(0.1m), 0.10001f, 0 },
        { AccessCurrency.FromDecimal(1m), 1E100, -1 },
        { AccessCurrency.FromDecimal(922337203685477.5807m), 922337203685477.5807d, -1 },
        { AccessCurrency.FromDecimal(0.1m), 0.10001m, -1 },
        { double.NaN, 1d, 0 },
        { double.NaN, double.NaN, 0 },
        { double.NegativeInfinity, 0d, -1 },
        { double.PositiveInfinity, double.NegativeInfinity, 1 },
        { "2", 10, 1 },
        { "0.1", 0.1d, 0 },
    };

    [Theory]
#pragma warning disable xUnit1045 // Currency comparison cases must retain their internal, nonserializable Variant subtype.
    [MemberData(nameof(NativeValues))]
#pragma warning restore xUnit1045
    public void MixedComparisons_UseNativeSubtypeCoercions(object left, object right, int expected)
    {
        Assert.Equal(expected == 0, CalculatedExpressionCoercion.CompareValues(left, right, static result => result == 0));
        Assert.Equal(expected < 0, CalculatedExpressionCoercion.CompareValues(left, right, static result => result < 0));
        Assert.Equal(expected > 0, CalculatedExpressionCoercion.CompareValues(left, right, static result => result > 0));
        Assert.Equal(expected != 0, CalculatedExpressionCoercion.CompareValues(left, right, static result => result != 0));
        Assert.Equal(expected <= 0, CalculatedExpressionCoercion.CompareValues(left, right, static result => result <= 0));
        Assert.Equal(expected >= 0, CalculatedExpressionCoercion.CompareValues(left, right, static result => result >= 0));
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120_MixedFieldComparisonsMatchNativeCachedValuesAfterWriterUpdate()
    {
        (string LeftType, string Left, string RightType, string Right, int Order)[] cases =
        [
            ("DOUBLE", "1E-100", "LONG", "0", 1),
            ("DOUBLE", "1E100", "DOUBLE", "1E99", 1),
            ("DOUBLE", "1", "DOUBLE", "1.0000000000000002", -1),
            ("SINGLE", "0.1", "DOUBLE", "0.1000000001", 0),
            ("SINGLE", "16777216", "DOUBLE", "16777217", 0),
            ("SINGLE", "16777216", "LONG", "16777217", -1),
            ("DECIMAL(28,16)", "1.0000000000000002", "DOUBLE", "1.0000000000000002", 1),
            ("DECIMAL(28,16)", "1", "DOUBLE", "1.0000000000000002", 0),
            ("DECIMAL(28,28)", "0", "DOUBLE", "1E-100", 0),
            ("DECIMAL(28,16)", "1", "DOUBLE", "1E100", -1),
            ("DECIMAL(28,8)", "1.0000001", "SINGLE", "1.0000001", 1),
            ("CURRENCY", "0.1", "DOUBLE", "0.10001", 0),
            ("CURRENCY", "0.1", "DOUBLE", "0.10006", -1),
            ("CURRENCY", "0.1", "SINGLE", "0.10001", 0),
            ("CURRENCY", "1", "DOUBLE", "1E100", -1),
            ("CURRENCY", "922337203685477.5807", "DOUBLE", "922337203685477.5807", -1),
            ("CURRENCY", "0.1", "DECIMAL(28,8)", "0.10001", -1),
            ("TEXT(32)", "'2'", "LONG", "10", 1),
            ("TEXT(32)", "'0.1'", "DOUBLE", "0.1", 0),
        ];
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NumericComparisons");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string nativeCases = string.Join(",", cases.Select(static item => $"@('{item.LeftType}',{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(item.Left)},'{item.RightType}',{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(item.Right)},{item.Order})"));
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            $db.Close()
            $cases = @({{nativeCases}})
            $connection = New-Object -ComObject ADODB.Connection
            try {
                $connection.Open('Provider=Microsoft.ACE.OLEDB.12.0;Data Source='+{{path}})
                for ($index=0;$index -lt $cases.Count;$index++) {
                    $case=$cases[$index]
                    $connection.Execute('CREATE TABLE T'+$index+' (Id LONG,[L] '+$case[0]+',[R] '+$case[2]+')') | Out-Null
                }
            } finally { $connection.Close(); [Runtime.InteropServices.Marshal]::ReleaseComObject($connection) | Out-Null }
            $db = $engine.OpenDatabase({{path}})
            try {
                for ($index=0;$index -lt $cases.Count;$index++) {
                    $case=$cases[$index]
                    $tdf=$db.TableDefs('T'+$index)
                    $ops=@('=','<','>','<>','<=','>=')
                    for ($slot=0;$slot -lt $ops.Length;$slot++) {
                        $field=$tdf.CreateField('C'+$slot,1)
                        $field.Expression='[L]'+$ops[$slot]+'[R]'
                        $tdf.Fields.Append($field)
                    }
                    $db.Execute('INSERT INTO T'+$index+' (Id,[L],[R]) VALUES (1,'+$case[1]+','+$case[3]+')',128)
                    $expected=@(($case[4] -eq 0),($case[4] -lt 0),($case[4] -gt 0),($case[4] -ne 0),($case[4] -le 0),($case[4] -ge 0))
                    $rs=$db.OpenRecordset('T'+$index)
                    try { for ($slot=0;$slot -lt $ops.Length;$slot++) { if ($rs.Fields('C'+$slot).Value -ne $expected[$slot]) { throw ('Native comparison changed: '+$index+'/'+$ops[$slot]) } } }
                    finally { $rs.Close() }
                }
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, created.StdOut + created.StdErr);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            session.SourcePath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            for (int index = 0; index < cases.Length; index++)
            {
                Assert.Equal(1, await writer.UpdateRowsAsync("T" + index, "Id", 1, new Dictionary<string, object?> { ["Id"] = 2 }, TestContext.Current.CancellationToken));
            }
        }

        await using AccessReader reader = await AccessReader.OpenAsync(
            session.SourcePath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        for (int index = 0; index < cases.Length; index++)
        {
            int order = cases[index].Order;
            bool[] expected = [order == 0, order < 0, order > 0, order != 0, order <= 0, order >= 0];
            await foreach (object[] row in reader.Rows("T" + index, cancellationToken: TestContext.Current.CancellationToken))
            {
                Assert.Equal(expected, row.Skip(3).Cast<bool>());
            }
        }
    }
}
