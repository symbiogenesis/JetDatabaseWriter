namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Independent native SPACE controls and bounded managed generation.</summary>
public sealed class CalculatedSpaceFunctionTests
{
    [Theory]
    [InlineData("0", 0)]
    [InlineData("0.5", 0)]
    [InlineData("0.6", 1)]
    [InlineData("1.5", 2)]
    [InlineData("2.5", 2)]
    [InlineData("-0.5", 0)]
    [InlineData("False", 0)]
    [InlineData("'2.5'", 2)]
    public void NativeCounts_RoundToEven(string argument, int expectedLength)
    {
        foreach (string name in new[] { "Space", "Space$" })
        {
            Assert.Equal(new string(' ', expectedLength), Evaluate($"{name}({argument})"));
        }
    }

    [Theory]
    [InlineData("-0.6")]
    [InlineData("-1")]
    [InlineData("True")]
    [InlineData("1073741824")]
    [InlineData("2147483647")]
    public void InvalidCounts_RefuseRatherThanClamp(string argument)
    {
        foreach (string name in new[] { "Space", "Space$" })
        {
            Assert.ThrowsAny<ArgumentException>(() => Evaluate($"{name}({argument})"));
        }
    }

    [Theory]
    [InlineData("Null")]
    [InlineData("'bad'")]
    [InlineData("''")]
    public void InvalidTypes_RefuseRatherThanBecomingZero(string argument)
    {
        foreach (string name in new[] { "Space", "Space$" })
        {
            Assert.ThrowsAny<InvalidCastException>(() => Evaluate($"{name}({argument})"));
        }
    }

    [Theory]
    [InlineData("2147483648")]
    [InlineData("1E100")]
    public void CountsOutsideLongRange_Refuse(string argument)
    {
        foreach (string name in new[] { "Space", "Space$" })
        {
            Assert.Throws<OverflowException>(() => Evaluate($"{name}({argument})"));
        }
    }

    [Theory]
    [InlineData("Space")]
    [InlineData("Space$")]
    public void GenerationBudget_RejectsBeforeAllocatingPastLimit(string name)
    {
        Assert.Equal(32768, Assert.IsType<string>(Evaluate($"{name}(32768)")).Length);
        JetLimitationException exception = Assert.Throws<JetLimitationException>(() => Evaluate($"{name}(32769)"));
        Assert.Equal(JetErrorCode.ValueTooLarge, exception.ErrorCode);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120_CountsAndInvalidRowRefusalMatchWriter()
    {
        await using AccessRoundTripSession native = await AccessRoundTripSession.CreateDaoAccdbAsync(TestContext.Current.CancellationToken, "JetDatabaseWriter.CalculatedSpace");
        string subjectPath = Path.Combine(native.WorkDir, "writer.accdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.SourcePath);
        string subject = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(subjectPath);
        AccessRoundTripEnvironment.CompactResult authored = native.RunDaoEngineScript(
            $$"""
            $db=$engine.OpenDatabase({{source}})
            try {
                foreach($name in @('Space','Space$')) {
                    $table=if($name -eq 'Space'){'T'}else{'Dollar'}
                    $td=$db.CreateTableDef($table);$td.Fields.Append($td.CreateField('N',7));$db.TableDefs.Append($td)
                    $field=$td.CreateField('V',10,243);$field.Expression=$name+'([N])';$td.Fields.Append($field)
                }
            } finally {$db.Close()}
            Copy-Item -LiteralPath {{source}} -Destination {{subject}}
            $db=$engine.OpenDatabase({{source}})
            try {
                foreach($table in @('T','Dollar')) {
                    foreach($literal in @('0','0.5','0.6','1.5','2.5','-0.5')) {
                        $db.Execute('INSERT INTO '+$table+' (N) VALUES ('+$literal+')',128)
                    }
                    foreach($literal in @('-0.6','-1','Null','1073741824','2147483648','1E100')) {
                        $refused=$false
                        try {$db.Execute('INSERT INTO '+$table+' (N) VALUES ('+$literal+')',128)}
                        catch {if(@($engine.Errors|ForEach-Object Number) -notcontains 3802){throw};$refused=$true}
                        if(-not $refused){throw 'Invalid native SPACE row was accepted'}
                    }
                }
            } finally {$db.Close()}
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(authored.ExitCode == 0, authored.StdOut + authored.StdErr);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(subjectPath, WriteModes.WriterOptions(WriteMode.Direct), TestContext.Current.CancellationToken))
        {
            foreach (string table in new[] { "T", "Dollar" })
            {
                foreach (double count in new[] { 0d, 0.5d, 0.6d, 1.5d, 2.5d, -0.5d })
                {
                    await writer.InsertRowAsync(table, [count, null], TestContext.Current.CancellationToken);
                }

                JetOperationException negative = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.InsertRowAsync(table, [-1d, null], TestContext.Current.CancellationToken));
                Assert.Equal(JetErrorCode.InvalidExpression, negative.ErrorCode);
                JetOperationException nullCount = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.InsertRowAsync(table, [null, null], TestContext.Current.CancellationToken));
                Assert.Equal(JetErrorCode.InvalidExpression, nullCount.ErrorCode);
                JetLimitationException overflow = await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.InsertRowAsync(table, [2147483648d, null], TestContext.Current.CancellationToken));
                Assert.Equal(JetErrorCode.NumericOverflow, overflow.ErrorCode);
            }
        }

        foreach (string table in new[] { "T", "Dollar" })
        {
            await using AccessReader expected = await AccessReader.OpenAsync(native.SourcePath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
            await using AccessReader actual = await AccessReader.OpenAsync(subjectPath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
            Assert.Equal(
                await expected.Rows(table, cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken),
                await actual.Rows(table, cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
        }
    }

    private static object Evaluate(string expression)
        => CalculatedExpressionParser.Parse(expression, isCalculatedColumn: false).Evaluate(new CalculatedExpressionEvaluationContext(new TableDef(), [], []));
}
