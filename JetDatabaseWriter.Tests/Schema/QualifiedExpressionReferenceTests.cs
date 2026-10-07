namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Qualified expression references resolve only within their explicit row context.</summary>
public sealed class QualifiedExpressionReferenceTests
{
    [Theory]
    [InlineData("[T].[Price]")]
    [InlineData("T.Price")]
    [InlineData("[t]![PRICE]")]
    [InlineData("T![Price]")]
    [InlineData("[T].Price")]
    public void SameTableReference_EvaluatesForCalculation(string reference)
    {
        CalculatedExpressionEvaluationContext context = Context("T");
        CalculatedExpressionPlan plan = CalculatedExpressionPlan.Parse(reference + " * 2");
        Assert.Equal(8d, plan.Root.Evaluate(context, plan));
    }

    [Theory]
    [InlineData("[Other].[Price]", "T")]
    [InlineData("Other.Price", "T")]
    [InlineData("[Other]![Price]", "T")]
    [InlineData("[T].[Price]", null)]
    [InlineData("T.Price", null)]
    public void CrossTableOrMissingContext_RefusesInsteadOfUsingCurrentRow(string expression, string? tableName)
    {
        CalculatedExpressionPlan plan = CalculatedExpressionPlan.Parse(expression);
        CalculatedExpressionEvaluationContext context = Context(tableName);
        Assert.Throws<NotSupportedException>(() => plan.Root.Evaluate(context, plan));
        Assert.False(ColumnDefaultValue.Compile(expression).TryEvaluate(typeof(double), () => context, out _));
        Assert.False(ColumnValidationRule.Compile(expression + " > 0", "Price").IsSupported);
    }

    [Theory]
    [InlineData("[T].[Price]", "[T].[Cost]")]
    [InlineData("T.Price", "T.[Cost]")]
    [InlineData("[T]![Price]", "[T]![Cost]")]
    public void Rename_PreservesQualifierAndEvaluation(string expression, string expected)
    {
        string renamed = Assert.IsType<string>(ExpressionFieldReferences.Rename(expression, "Price", "Cost", "T"));
        Assert.Equal(expected, renamed);
        Assert.False(ExpressionFieldReferences.References(renamed, "Price", "T"));
        Assert.True(ExpressionFieldReferences.References(renamed, "Cost", "T"));
        CalculatedExpressionPlan plan = CalculatedExpressionPlan.Parse(renamed);
        Assert.Equal(4, plan.Root.Evaluate(Context("T", "Cost"), plan));
    }

    [Fact]
    public void QualifiedNamesInsideLiterals_RemainText()
    {
        CalculatedExpressionPlan plan = CalculatedExpressionPlan.Parse("'[Other]![Price]' & \"T.Price\"");
        Assert.Equal("[Other]![Price]T.Price", plan.Root.Evaluate(Context("T"), plan));
    }

    [Fact]
    public void GeneratedPlaceholder_CannotAliasBareField()
    {
        CalculatedExpressionPlan plan = CalculatedExpressionPlan.Parse("[Price] + __JdwCalcCol0");
        Assert.Throws<InvalidOperationException>(() => plan.Root.Evaluate(Context("T"), plan));
    }

    [Theory]
    [InlineData("Forms![T]![Price]")]
    [InlineData("[T].[Price].[Value]")]
    public void MultiSegmentObjectReference_Refuses(string expression)
        => Assert.Throws<ArgumentException>(() => CalculatedExpressionPlan.Parse(expression));

    [Theory]
    [InlineData("[T].[Price]")]
    [InlineData("T.Price")]
    [InlineData("[T]![Price]")]
    public void QualifiedRuleAndDefault_RefuseAsNativeDaoDoes(string reference)
    {
        Assert.False(ColumnDefaultValue.Compile(reference + " * 3").TryEvaluate(typeof(int), () => Context("T"), out _));
        Assert.False(ColumnValidationRule.Compile(reference + " > 0", "Price").IsSupported);
        Assert.Throws<NotSupportedException>(() => CalculatedExpressionPlan.Parse(reference + " > 0", allowQualifiedReferences: false));
    }

    [Theory]
    [InlineData("[T].[Price]")]
    [InlineData("T.Price")]
    [InlineData("[T]![Price]")]
    public async Task PersistedQualifiedCalculation_ReopenAndRenamePreserveEvaluation(string reference)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Price", typeof(int)) { ValidationRuleExpression = ">0" },
                    new ColumnDefinition("Calculated", typeof(int)) { IsCalculated = true, CalculationExpression = reference + " * 2" },
                ],
                TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync("T", [4, DBNull.Value], TestContext.Current.CancellationToken);
            byte[] before = stream.ToArray();
            await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.InsertRowAsync("T", [-1, DBNull.Value], TestContext.Current.CancellationToken));
            Assert.Equal(before, stream.ToArray());
            await writer.RenameColumnAsync("T", "Price", "Cost", TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [2, DBNull.Value], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        int count = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            int price = Assert.IsType<int>(row[0]);
            Assert.Equal(price * 2, row[1]);
            count++;
        }

        Assert.Equal(2, count);
    }

    [Theory]
    [InlineData("[T].[Price]")]
    [InlineData("T.Price")]
    [InlineData("[T]![Price]")]
    public async Task EmptyTable_QualifiedTableRuleSetterRefusesWithoutChangingImage(string reference)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Price", typeof(int))], TestContext.Current.CancellationToken);
        }

        byte[] before = stream.ToArray();
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await Assert.ThrowsAsync<NotSupportedException>(async () => await writer.SetTableValidationRuleAsync(
                "T", new TableValidationRule(reference + " > 0"), TestContext.Current.CancellationToken));
            Assert.Equal(before, stream.ToArray());
        }

        Assert.Equal(before, stream.ToArray());
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        Assert.Null(await reader.GetTableValidationRuleAsync("T", TestContext.Current.CancellationToken));
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Fail($"The empty table unexpectedly contains {row.Length} values.");
        }
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData("[T].[Price]")]
    [InlineData("T.Price")]
    [InlineData("[T]![Price]")]
    public async Task NativeQualifiedCalculation_ReevaluatesRenamesAndCompacts(string reference)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.QualifiedCalculation");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string expression = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(reference + " * 2");
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0')
            try {
                $db.Execute('CREATE TABLE [T] ([Price] LONG)')
                $db.TableDefs.Refresh()
                $tdf = $db.TableDefs('T')
                $field = $tdf.CreateField('Calculated', 4)
                $field.Properties('Expression').Value = {{expression}}
                $tdf.Fields.Append($field)
                $field = $null
                $tdf = $null
                $db.Execute('INSERT INTO [T] ([Price]) VALUES (4)', 128)
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, $"DAO failed: {created.StdOut}\n{created.StdErr}");
        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync("T", [3, DBNull.Value], TestContext.Current.CancellationToken);
            await writer.RenameColumnAsync("T", "Price", "Cost", TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [2, DBNull.Value], TestContext.Current.CancellationToken);
        }

        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScriptThenCompact(
            """
            $rs = $db.OpenRecordset('SELECT [Cost],[Calculated] FROM [T] ORDER BY [Cost]')
            try {
                while (!$rs.EOF) {
                    Write-Output ("ROW=" + [string]$rs.Fields('Cost').Value + ":" + [string]$rs.Fields('Calculated').Value)
                    $rs.MoveNext()
                }
            } finally { $rs.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0 && File.Exists(session.CompactedPath), $"DAO failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains("ROW=2:4", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("ROW=3:6", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("ROW=4:8", result.StdOut, StringComparison.Ordinal);
        await using AccessReader reader = await AccessReader.OpenAsync(
            session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync("T", TestContext.Current.CancellationToken);
        Assert.Collection(
            columns,
            column =>
            {
                Assert.Equal("Cost", column.Name);
                Assert.Equal(typeof(int), column.ClrType);
                Assert.False(column.IsCalculated);
            },
            column =>
            {
                Assert.Equal("Calculated", column.Name);
                Assert.Equal(typeof(int), column.ClrType);
                Assert.True(column.IsCalculated);
                Assert.True(ExpressionFieldReferences.References(column.CalculationExpression, "Cost", "T"));
                Assert.False(ExpressionFieldReferences.References(column.CalculationExpression, "Price", "T"));
            });
        var values = new Dictionary<int, int>();
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            values.Add(Assert.IsType<int>(row[0]), Assert.IsType<int>(row[1]));
        }

        Assert.Equal(3, values.Count);
        Assert.Equal(4, values[2]);
        Assert.Equal(6, values[3]);
        Assert.Equal(8, values[4]);
    }

    private static CalculatedExpressionEvaluationContext Context(string? tableName, string column = "Price")
        => new(
            new TableDef { Columns = [new ColumnInfo { Name = column }] },
            [new ColumnConstraint { Name = column, ClrType = typeof(int) }],
            [4],
            force: false,
            tableName);
}
