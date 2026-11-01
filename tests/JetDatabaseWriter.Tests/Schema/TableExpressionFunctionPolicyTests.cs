namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Stored defaults and rules exclude functions requiring the Access application host.</summary>
public sealed class TableExpressionFunctionPolicyTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets native formats and host functions refused in persisted table expressions.</summary>
    public static TheoryData<DatabaseFormat, string> RefusedFunctions()
    {
        var data = new TheoryData<DatabaseFormat, string>();
        foreach (DatabaseFormat format in Enum.GetValues<DatabaseFormat>())
        {
            foreach (string expression in new[] { "CurrentUser()", "Eval(\"1+2\")", "DLookup(\"Id\",\"T\")", "IIf(True,1,DCount(\"*\",\"T\"))" })
            {
                data.Add(format, expression);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(RefusedFunctions))]
    public async Task SchemaDeclarations_RefuseHostFunctionsBeforeMutation(DatabaseFormat format, string expression)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct);
        await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int))], Ct);
        foreach (bool defaultValue in new[] { false, true })
        {
            var column = new ColumnDefinition("Score", typeof(int))
            {
                DefaultValueExpression = defaultValue ? expression : null,
                ValidationRuleExpression = defaultValue ? null : expression + " > 0",
            };
            byte[] before = stream.ToArray();
            JetNotSupportedException create = await Assert.ThrowsAsync<JetNotSupportedException>(async () =>
                await writer.CreateTableAsync("Rejected", [column], Ct));
            Assert.Equal(JetErrorCode.UnsupportedExpression, create.ErrorCode);
            Assert.Equal(before, stream.ToArray());
            await Assert.ThrowsAsync<JetNotSupportedException>(async () => await writer.AddColumnAsync("T", column, Ct));
            Assert.Equal(before, stream.ToArray());
        }

        byte[] original = stream.ToArray();
        await Assert.ThrowsAsync<JetNotSupportedException>(async () =>
            await writer.SetTableValidationRuleAsync("T", new TableValidationRule(expression + " > 0"), Ct));
        Assert.Equal(original, stream.ToArray());
        await writer.InsertRowAsync("T", [7], Ct);
    }

    [Theory]
    [InlineData("CurrentUser()")]
    [InlineData("IIf(True,1,CurrentUser())")]
    [InlineData("DLookup(\"Id\",\"T\")")]
    [InlineData("IIf(True,1,DCount(\"*\",\"T\"))")]
    [InlineData("Eval(\"1+2\")")]
    public void StoredHostFunctions_RefuseEvenInUnusedBranches(string expression)
    {
        var value = ColumnDefaultValue.Compile(expression);
        Assert.Same(ColumnDefaultValue.Unsupported, value);
        Assert.False(ColumnValidationRule.Compile(expression + " > 0", "Score").IsSupported);
    }

    [Theory]
    [InlineData("\"CurrentUser() Eval(1) DCount(*)\"")]
    [InlineData("'CurrentUser() Eval(1) DCount(*)'")]
    public void HostFunctionText_RemainsAnOrdinaryDefault(string expression)
    {
        var value = ColumnDefaultValue.Compile(expression);
        Assert.NotSame(ColumnDefaultValue.Unsupported, value);
    }

    [Theory]
    [InlineData("[CurrentUser]")]
    [InlineData("[DLookup] + 1")]
    [InlineData("\"say \"\"CurrentUser()\"\"\"")]
    public void HostFunctionNamesInFieldsAndLiterals_RemainValid(string expression)
        => TableExpressionFunctionPolicy.Validate(expression);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public Task Dao36_TableDefaultsAndRulesRefuseApplicationFunctions(DatabaseFormat format)
        => AssertNativePolicyAsync(format, useDao36: true);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public Task Dao120_TableDefaultsAndRulesRefuseApplicationFunctions(DatabaseFormat format)
        => AssertNativePolicyAsync(format, useDao36: false);

    private static async Task AssertNativePolicyAsync(DatabaseFormat format, bool useDao36)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.TableExpressionPolicy", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        int version = format switch
        {
            DatabaseFormat.Jet3Mdb => 32,
            DatabaseFormat.Jet4Mdb => 64,
            DatabaseFormat.AceAccdb => 128,
            _ => throw new ArgumentOutOfRangeException(nameof(format)),
        };
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string script = $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            try {
                $expressions = @('CurrentUser()', 'Eval("1+2")', 'DLookup("Id","T")', 'DCount("*","T")',
                    'DSum("Id","T")', 'DAvg("Id","T")', 'DMin("Id","T")', 'DMax("Id","T")',
                    'DFirst("Id","T")', 'DLast("Id","T")', 'DStDev("Id","T")', 'DStDevP("Id","T")',
                    'DVar("Id","T")', 'DVarP("Id","T")', 'IIf(True,1,CurrentUser())')
                $ordinal = 0
                foreach ($expression in $expressions) {
                    foreach ($kind in @('default','field','table')) {
                        $ordinal++
                        $table = $db.CreateTableDef("Rejected$ordinal")
                        $field = $table.CreateField('Id', 4)
                        $rejected = $false
                        try {
                            if ($kind -eq 'default') { $field.DefaultValue = $expression }
                            if ($kind -eq 'field') { $field.ValidationRule = '>=' + $expression }
                            $table.Fields.Append($field)
                            if ($kind -eq 'table') { $table.ValidationRule = '[Id]>=' + $expression }
                            $db.TableDefs.Append($table)
                        } catch {
                            if (@($engine.Errors | Where-Object { $_.Number -eq 3388 }).Count -eq 0) { throw }
                            $rejected = $true
                        }
                        if (!$rejected) { throw "Native table accepted $kind $expression" }
                    }
                }
                $table = $db.CreateTableDef('Accepted')
                $field = $table.CreateField('Id', 4)
                $field.DefaultValue = 'Abs(-3)'
                $field.ValidationRule = '>=Abs(-3)'
                $table.Fields.Append($field)
                $table.ValidationRule = '[Id]>=Abs(-3)'
                $db.TableDefs.Append($table)
                $rows = $db.OpenRecordset('Accepted')
                try {
                    $rows.AddNew()
                    $rows.Update()
                    $rows.MoveFirst()
                    if ($rows.Fields('Id').Value -ne 3) { throw 'Native scalar default or rule failed' }
                } finally { $rows.Close() }
                Write-Output 'NATIVE_TABLE_FUNCTION_POLICY_OK'
            } finally { $db.Close() }
            """;
        string output;
        if (useDao36)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, $"DAO36 failed: {result.StandardOutput}\n{result.StandardError}");
            output = result.StandardOutput;
        }
        else
        {
            AccessRoundTripEnvironment.CompactResult result = AccessRoundTripEnvironment.RunDaoScript(
                $$"""
                $ErrorActionPreference = 'Stop'
                $engine = New-Object -ComObject DAO.DBEngine.120
                try { {{script}} } finally { [System.Runtime.InteropServices.Marshal]::ReleaseComObject($engine) | Out-Null }
                """,
                session.WorkDir,
                TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0, $"DAO120 failed: {result.StdOut}\n{result.StdErr}");
            output = result.StdOut;
        }

        Assert.Contains("NATIVE_TABLE_FUNCTION_POLICY_OK", output, StringComparison.Ordinal);
    }
}
