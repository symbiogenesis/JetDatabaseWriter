namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Globalization;
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

/// <summary>Calculated qualifiers preserve DAO syntax while resolving within the current row.</summary>
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
        CalculatedExpressionNode plan = CalculatedExpressionParser.Parse(reference + " * 2");
        Assert.Equal(8, Assert.IsType<int>(plan.Evaluate(context)));
    }

    [Theory]
    [InlineData("Other.Price")]
    [InlineData("Other![Price]")]
    [InlineData("T.Price")]
    [InlineData("T![Price]")]
    public void BareQualifier_BindsToCurrentRowEvenWhenNameDiffers(string reference)
    {
        CalculatedExpressionNode plan = CalculatedExpressionParser.Parse(reference + " * 2");
        Assert.Equal(8, Assert.IsType<int>(plan.Evaluate(Context("Renamed T"))));
        Assert.False(ColumnDefaultValue.Compile(reference).TryEvaluate(typeof(int), () => Context("Renamed T"), out _));
        Assert.Throws<JetNotSupportedException>(() => CalculatedExpressionParser.Parse(reference, isCalculatedColumn: false));
    }

    [Theory]
    [InlineData("Other.Price", "Other.[Cost]")]
    [InlineData("Other![Price]", "Other![Cost]")]
    public void BareQualifier_RenameAndDependencyFollowCurrentRow(string expression, string expected)
    {
        Assert.True(ExpressionFieldReferences.References(expression, "Price", "Renamed T"));
        string renamed = Assert.IsType<string>(ExpressionFieldReferences.Rename(expression, "Price", "Cost", "Renamed T"));
        Assert.Equal(expected, renamed);
        CalculatedExpressionNode plan = CalculatedExpressionParser.Parse(renamed);
        Assert.Equal(4, plan.Evaluate(Context("Renamed T", "Cost")));
    }

    [Theory]
    [InlineData("T", "vbTrue")]
    [InlineData("Other", "vbTrue")]
    [InlineData("[T]", "vbTrue")]
    [InlineData("T", "Yes")]
    [InlineData("Other", "Yes")]
    [InlineData("[T]", "Yes")]
    [InlineData("T", "Null")]
    [InlineData("Other", "Null")]
    [InlineData("[T]", "Null")]
    [InlineData("T", "And")]
    [InlineData("Other", "And")]
    [InlineData("[T]", "And")]
    [InlineData("T", "Mod")]
    [InlineData("Other", "Mod")]
    [InlineData("[T]", "Mod")]
    [InlineData("T", "Date")]
    [InlineData("Other", "Date")]
    [InlineData("[T]", "Date")]
    public void QualifiedReservedName_RenameAndDependencyFollowActualField(string qualifier, string fieldName)
    {
        string expression = qualifier + "." + fieldName + " * 2";
        CalculatedExpressionNode plan = CalculatedExpressionParser.Parse(expression);
        Assert.Equal(8, Assert.IsType<int>(plan.Evaluate(Context("T", fieldName))));
        Assert.True(ExpressionFieldReferences.References(expression, fieldName, "T"));
        string renamed = Assert.IsType<string>(ExpressionFieldReferences.Rename(expression, fieldName, "Cost", "T"));
        Assert.Equal(qualifier + ".[Cost] * 2", renamed);
        CalculatedExpressionNode renamedPlan = CalculatedExpressionParser.Parse(renamed);
        Assert.Equal(8, Assert.IsType<int>(renamedPlan.Evaluate(Context("T", "Cost"))));
    }

    [Theory]
    [InlineData("vbTrue")]
    [InlineData("Yes")]
    [InlineData("Null")]
    [InlineData("And")]
    [InlineData("Mod")]
    public void UnqualifiedReservedToken_IsNotAFieldDependency(string name)
    {
        Assert.False(ExpressionFieldReferences.References(name, name, "T"));
        Assert.Equal(name, ExpressionFieldReferences.Rename(name, name, "Cost", "T"));
    }

    [Theory]
    [InlineData("[T].[Price] + 1e-3", 4.001d, typeof(decimal))]
    [InlineData("T.Price + 1E+3", 1004d, typeof(int))]
    [InlineData("[T]![Price] + .5E1", 9d, typeof(int))]
    [InlineData("[T].[Price] + &HFFFF", 3d, typeof(int))]
    [InlineData("T.Price + &HFFFF&", 65539d, typeof(int))]
    [InlineData("[T]![Price] + &O17", 19d, typeof(int))]
    [InlineData("T.Price + &17", 19d, typeof(int))]
    public void QualifiedReference_WithScientificOrRadixLiteralRetainsNumericToken(string expression, double expected, Type expectedType)
    {
        CalculatedExpressionNode plan = CalculatedExpressionParser.Parse(expression);
        object actual = plan.Evaluate(Context("T"));
        Assert.IsType(expectedType, actual);

        // Keep this tokenization check at the literals' double precision across runtime conversions.
        Assert.Equal(expected, Convert.ToDouble(actual, CultureInfo.InvariantCulture));
    }

    [Theory]
    [InlineData("[T].[Price]", null)]
    [InlineData("T.Price", null)]
    public void MissingContext_RefusesInsteadOfUsingCurrentRow(string expression, string? tableName)
    {
        CalculatedExpressionNode plan = CalculatedExpressionParser.Parse(expression);
        CalculatedExpressionEvaluationContext context = Context(tableName);
        Assert.Throws<JetNotSupportedException>(() => plan.Evaluate(context));
        Assert.False(ColumnDefaultValue.Compile(expression).TryEvaluate(typeof(double), () => context, out _));
        Assert.False(ColumnValidationRule.Compile(expression + " > 0", "Price").IsSupported);
    }

    [Theory]
    [InlineData("[Other].[Price]")]
    [InlineData("[Other]![Price]")]
    [InlineData("[Other].Price")]
    public void StaleBracketedQualifier_ReturnsNullInsteadOfUsingCurrentRow(string reference)
    {
        CalculatedExpressionEvaluationContext context = Context("T");
        Assert.Same(DBNull.Value, CalculatedExpressionParser.Parse(reference + " * 2").Evaluate(context));
        Assert.True(Assert.IsType<bool>(CalculatedExpressionParser.Parse("IsNull(" + reference + ")").Evaluate(context)));
        Assert.Equal(12, CalculatedExpressionParser.Parse("Nz(" + reference + ", 12)").Evaluate(context));
        Assert.False(ColumnDefaultValue.Compile(reference).TryEvaluate(typeof(double), () => context, out _));
        Assert.False(ColumnValidationRule.Compile(reference + " > 0", "Price").IsSupported);
    }

    [Theory]
    [InlineData("[Other].[Missing]")]
    [InlineData("[Other]![Missing]")]
    public void StaleQualifierWithUnknownField_RefusesUnverifiedResolution(string reference)
    {
        CalculatedExpressionNode plan = CalculatedExpressionParser.Parse(reference);
        JetOperationException failure = Assert.Throws<JetOperationException>(() => plan.Evaluate(Context("T")));
        Assert.Equal(JetErrorCode.InvalidExpression, failure.ErrorCode);
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
        CalculatedExpressionNode plan = CalculatedExpressionParser.Parse(renamed);
        Assert.Equal(4, plan.Evaluate(Context("T", "Cost")));
    }

    [Theory]
    [InlineData("[Other] . [Price]")]
    [InlineData("Other ! Price")]
    [InlineData("Forms ! [T] ! [Price]")]
    public void SpacedForeignReference_IsNotRenamedAsLocalField(string expression)
    {
        Assert.False(ExpressionFieldReferences.References(expression, "Price", "T"));
        Assert.Equal(expression, ExpressionFieldReferences.Rename(expression, "Price", "Cost", "T"));
    }

    [Fact]
    public void QualifiedNamesInsideLiterals_RemainText()
    {
        CalculatedExpressionNode plan = CalculatedExpressionParser.Parse("'[Other]![Price]' & \"T.Price\"");
        Assert.Equal("[Other]![Price]T.Price", plan.Evaluate(Context("T")));
    }

    [Fact]
    public void GeneratedPlaceholder_CannotAliasBareField()
    {
        CalculatedExpressionNode plan = CalculatedExpressionParser.Parse("[Price] + __JdwCalcCol0");
        JetOperationException failure = Assert.Throws<JetOperationException>(() => plan.Evaluate(Context("T")));
        Assert.Equal(JetErrorCode.InvalidExpression, failure.ErrorCode);
    }

    [Theory]
    [InlineData("Forms![T]![Price]")]
    [InlineData("Forms ! [T] ! [Price]")]
    [InlineData("[T] . [Price] . [Value]")]
    [InlineData("[T].[Price].[Value]")]
    public void MultiSegmentObjectReference_Refuses(string expression)
        => Assert.Throws<ArgumentException>(() => CalculatedExpressionParser.Parse(expression));

    [Theory]
    [InlineData("[T].[Price]")]
    [InlineData("T.Price")]
    [InlineData("[T]![Price]")]
    public void QualifiedRuleAndDefault_RefuseAsNativeDaoDoes(string reference)
    {
        Assert.False(ColumnDefaultValue.Compile(reference + " * 3").TryEvaluate(typeof(int), () => Context("T"), out _));
        Assert.False(ColumnValidationRule.Compile(reference + " > 0", "Price").IsSupported);
        Assert.Throws<JetNotSupportedException>(() => CalculatedExpressionParser.Parse(reference + " > 0", isCalculatedColumn: false));
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
            await Assert.ThrowsAsync<JetNotSupportedException>(async () => await writer.SetTableValidationRuleAsync(
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

    [Theory]
    [InlineData("[T] .[Price]")]
    [InlineData("[T]. [Price]")]
    [InlineData("[T] . [Price]")]
    [InlineData("T .Price")]
    [InlineData("T. Price")]
    [InlineData("T . Price")]
    [InlineData("[T] ![Price]")]
    [InlineData("[T]! [Price]")]
    [InlineData("[T] ! [Price]")]
    public void SpacedQualifiedCalculation_RefusesAsNativeDaoDoes(string reference)
        => Assert.Throws<ArgumentException>(() => CalculatedExpressionParser.Parse(reference + " * 2"));

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData("[T] .[Price]")]
    [InlineData("[T]. [Price]")]
    [InlineData("[T] . [Price]")]
    [InlineData("T .Price")]
    [InlineData("T. Price")]
    [InlineData("T . Price")]
    [InlineData("[T] ![Price]")]
    [InlineData("[T]! [Price]")]
    [InlineData("[T] ! [Price]")]
    public async Task NativeSpacedQualifiedCalculation_RefusesWithoutAddingField(string reference)
    {
        ArgumentNullException.ThrowIfNull(reference);
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.SpacedQualification");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string expression = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(reference + " * 2");
        string adjacent = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(reference.Replace(" ", string.Empty, StringComparison.Ordinal) + " * 2");
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0')
            try {
                $db.Execute('CREATE TABLE [T] ([Price] LONG)')
                $db.TableDefs.Refresh()
                $tdf = $db.TableDefs('T')
                $field = $tdf.CreateField('Rejected', 4)
                $field.Properties('Expression').Value = {{expression}}
                $rejected = $false
                try { $tdf.Fields.Append($field) }
                catch [System.Runtime.InteropServices.COMException] { $rejected = $true }
                if (!$rejected) { throw 'DAO accepted a spaced qualified expression.' }
                $tdf.Fields.Refresh()
                if ($tdf.Fields.Count -ne 1) { throw 'DAO retained the rejected calculated field.' }
                $field = $tdf.CreateField('Calculated', 4)
                $field.Properties('Expression').Value = {{adjacent}}
                $tdf.Fields.Append($field)
                $db.Execute('INSERT INTO [T] ([Price]) VALUES (4)', 128)
                $rs = $db.OpenRecordset('SELECT [Calculated] FROM [T]')
                try { Write-Output ('CALCULATED=' + [string]$rs.Fields('Calculated').Value) }
                finally { $rs.Close() }
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0, $"DAO failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains("CALCULATED=8", result.StdOut, StringComparison.Ordinal);
    }

    private static CalculatedExpressionEvaluationContext Context(string? tableName, string column = "Price")
        => new(
            new TableDef { Columns = [new ColumnInfo { Name = column }] },
            [new ColumnConstraint { Name = column, ClrType = typeof(int) }],
            [4],
            force: false,
            tableName);
}
