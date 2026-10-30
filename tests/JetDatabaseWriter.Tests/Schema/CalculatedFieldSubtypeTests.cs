namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Checks the expression service's Byte and Yes/No field subtypes.</summary>
public sealed class CalculatedFieldSubtypeTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ByteField_ProjectsLongForOrdinaryAndCalculatedReferences(bool calculated)
    {
        CalculatedExpressionEvaluationContext context = CreateContext(ColumnType.ByteType, (byte)42, calculated, "42");
        Assert.Equal(42, Assert.IsType<int>(context.GetFieldValue("N")));
    }

    [Theory]
    [InlineData(false, true, (short)-1)]
    [InlineData(false, false, (short)0)]
    [InlineData(true, true, (short)-1)]
    [InlineData(true, false, (short)0)]
    public void BooleanField_ProjectsIntegerForOrdinaryAndCalculatedReferences(bool calculated, bool value, short expected)
    {
        CalculatedExpressionEvaluationContext context = CreateContext(ColumnType.BooleanType, value, calculated, value ? "True" : "False");
        Assert.Equal(expected, Assert.IsType<short>(context.GetFieldValue("N")));
    }

    [Fact]
    public void OrdinaryBooleanNull_ProjectsItsStoredFalseValue()
    {
        CalculatedExpressionEvaluationContext context = CreateContext(ColumnType.BooleanType, DBNull.Value, calculated: false, "False");
        Assert.Equal((short)0, Assert.IsType<short>(context.GetFieldValue("N")));
    }

    [Fact]
    public void ByteFieldAndSingleArithmetic_UsesNativeDoublePromotion()
    {
        var definition = new TableDef
        {
            Columns = [new ColumnInfo { Name = "N", Type = ColumnType.ByteType }, new ColumnInfo { Name = "Rate", Type = ColumnType.FloatType }],
        };
        ColumnConstraint[] constraints =
        [
            new() { Name = "N", StorageType = ColumnType.ByteType, ClrType = typeof(byte) },
            new() { Name = "Rate", StorageType = ColumnType.FloatType, ClrType = typeof(float) },
        ];
        var context = new CalculatedExpressionEvaluationContext(definition, constraints, [(byte)42, 1.225f]);
        object result = CalculatedExpressionParser.Parse("[N]*[Rate]").Evaluate(context);
        Assert.Equal(51.45000100135803d, Assert.IsType<double>(result));
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120_FieldSubtypesAndMixedSingleCachesMatchNativeExpressions()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.FieldSubtypes");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            try {
                $db.Execute('CREATE TABLE T (N BYTE,Flag YESNO,Rate SINGLE)')
                $tdf = $db.TableDefs('T')
                foreach ($case in @(@('CB',2,'[N]'),@('CF',1,'[Flag]'),@('Product',7,'[N]*[Rate]'),@('ProductFromCB',7,'[CB]*[Rate]'))) {
                    $field = $tdf.CreateField($case[0],$case[1])
                    $field.Expression = $case[2]
                    $tdf.Fields.Append($field)
                }
                $db.Execute('INSERT INTO T (N,Flag,Rate) VALUES (42,True,1.225)')
                foreach ($field in @('N','CB')) {
                    $rs = $db.OpenRecordset('SELECT TypeName(['+$field+']) AS T,VarType(['+$field+']) AS V FROM T')
                    try { if ($rs.Fields('T').Value -ne 'Long' -or $rs.Fields('V').Value -ne 3) { throw ('Native Byte projection changed: '+$field) } }
                    finally { $rs.Close() }
                }
                foreach ($field in @('Flag','CF')) {
                    $rs = $db.OpenRecordset('SELECT TypeName(['+$field+']) AS T,VarType(['+$field+']) AS V FROM T')
                    try { if ($rs.Fields('T').Value -ne 'Integer' -or $rs.Fields('V').Value -ne 2) { throw ('Native Yes/No projection changed: '+$field) } }
                    finally { $rs.Close() }
                }
                $rs = $db.OpenRecordset('T')
                try { if ($rs.Fields('Product').Value -ne [double]51.45000100135803) { throw 'Native Byte/Single product changed' } }
                finally { $rs.Close() }
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, created.StdOut + created.StdErr);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            session.SourcePath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("T", "N", (byte)42, new Dictionary<string, object?> { ["N"] = (byte)43, ["Flag"] = DBNull.Value }, TestContext.Current.CancellationToken));
        }

        AccessRoundTripEnvironment.CompactResult checkedCaches = AccessRoundTripEnvironment.RunDaoDatabaseScript(
            session.SourcePath,
            """
            $rs = $db.OpenRecordset('SELECT Product,ProductFromCB,[N]*[Rate] AS Expected,Flag,CF FROM T')
            try {
                if ($rs.Fields('Product').Value -ne $rs.Fields('Expected').Value -or $rs.Fields('ProductFromCB').Value -ne $rs.Fields('Expected').Value) { throw 'Writer Byte/Single promotion differs from native' }
                if ($rs.Fields('Flag').Value -or $rs.Fields('CF').Value) { throw 'Writer Yes/No Null projection differs from stored False' }
            } finally { $rs.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(1));
        Assert.True(checkedCaches.ExitCode == 0, checkedCaches.StdOut + checkedCaches.StdErr);
    }

    private static CalculatedExpressionEvaluationContext CreateContext(ColumnType type, object value, bool calculated, string expression)
    {
        var definition = new TableDef
        {
            Columns = [new ColumnInfo { Name = "N", Type = type, ExtraFlags = calculated ? Constants.CalculatedColumn.ExtFlagMask : (byte)0, CalculatedResultType = calculated ? type : default }],
        };
        ColumnConstraint[] constraints =
        [
            new()
            {
                Name = "N",
                StorageType = type,
                ClrType = type == ColumnType.BooleanType ? typeof(bool) : typeof(byte),
                IsCalculated = calculated,
                CalculatedResultType = calculated ? type : default,
                CalculationExpression = expression,
            },
        ];
        return new CalculatedExpressionEvaluationContext(definition, constraints, [value]);
    }
}
