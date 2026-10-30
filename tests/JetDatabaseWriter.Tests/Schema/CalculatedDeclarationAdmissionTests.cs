namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>New declarations reject native untyped Null results and malformed year literals.</summary>
public sealed class CalculatedDeclarationAdmissionTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("Null")]
    [InlineData("((Null))")]
    [InlineData("+Null")]
    [InlineData("-Null")]
    [InlineData("Null+0")]
    [InlineData("[N]+Null")]
    [InlineData("Null*[N]")]
    [InlineData("Null/0")]
    [InlineData("Null+(1/0)")]
    [InlineData("Null^0")]
    [InlineData("IIf(True,Null,Null)")]
    [InlineData("IIf(True,Null+1,Null)")]
    [InlineData("Choose(1,Null,1)")]
    [InlineData("Choose(2,1,Null)")]
    [InlineData("Choose([N],1,Null)")]
    public async Task UntypedNullResult_CreateAndAddRefuseBeforeMutation(string expression)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        byte[] before = stream.ToArray();
        ArgumentException create = await Assert.ThrowsAsync<ArgumentException>(async () => await writer.CreateTableAsync("Bad", [new("N", typeof(int)), Calculated("V", expression)], Ct));
        Assert.Equal("columns", create.ParamName);
        Assert.Equal(before, stream.ToArray());
        await writer.CreateTableAsync("T", [new("N", typeof(int))], Ct);
        before = stream.ToArray();
        ArgumentException add = await Assert.ThrowsAsync<ArgumentException>(async () => await writer.AddColumnAsync("T", Calculated("V", expression), Ct));
        Assert.Equal("column", add.ParamName);
        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [InlineData("IIf(True,Null,[N])")]
    [InlineData("IIf(False,1,Null)")]
    [InlineData("IIf(True,Abs(Null),Null)")]
    [InlineData("IIf(True,Choose(1,Null),1)")]
    [InlineData("Null=1")]
    [InlineData("Null And False")]
    [InlineData("Null Or False")]
    [InlineData("Not Null")]
    [InlineData("Null Like \"[\"")]
    [InlineData("Null&Null")]
    [InlineData("Abs(Null)")]
    [InlineData("Len(Null)")]
    [InlineData("Left(Null,1)")]
    [InlineData("Choose(Null,1,2)")]
    public void TypedNullableExpression_RemainsAdmissible(string expression)
        => Assert.NotNull(CalculatedExpressionParser.Parse(expression));

    [Theory]
    [InlineData("0")]
    [InlineData("29")]
    [InlineData("999")]
    [InlineData("0100")]
    [InlineData("0000")]
    [InlineData("10000")]
    [InlineData("-1")]
    [InlineData("2020.0")]
    [InlineData("2020.5")]
    [InlineData("1000.")]
    [InlineData("2020.00E0")]
    [InlineData("1.2345E3")]
    [InlineData("100.0E1")]
    [InlineData("2020E-1")]
    [InlineData("999.49")]
    [InlineData("0.2020")]
    [InlineData("0.0001")]
    public void DateSerial_UnparenthesizedYearLiteralUsesNativeCoefficient(string year)
        => Assert.Throws<ArgumentException>(() => CalculatedExpressionParser.Parse($"DateSerial({year},1,1)"));

    [Theory]
    [InlineData("1000")]
    [InlineData("9999")]
    [InlineData("-1000")]
    [InlineData("+29")]
    [InlineData("+2020.0")]
    [InlineData("(29)")]
    [InlineData("((0000))")]
    [InlineData("2020+0")]
    [InlineData("2020.0+0")]
    [InlineData("CDbl(2020)")]
    [InlineData("[N]")]
    [InlineData("\"2020\"")]
    [InlineData("1e3")]
    [InlineData("1.0000E3")]
    [InlineData("10000E-1")]
    [InlineData("999.50")]
    [InlineData("123.4")]
    [InlineData("1.234E-10")]
    [InlineData("9999.0")]
    public void DateSerial_ExpressionsAndValidNativeLiteralsRemainAdmissible(string year)
        => Assert.NotNull(CalculatedExpressionParser.Parse($"DateSerial({year},1,1)"));

    [Fact]
    public void GenericExpressions_KeepNullAndOrdinaryYearLiterals()
    {
        Assert.NotNull(CalculatedExpressionParser.Parse("Null", isCalculatedColumn: false));
        Assert.NotNull(CalculatedExpressionParser.Parse("DateSerial(29,1,1)", isCalculatedColumn: false));
    }

    [Fact(Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason, SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable), SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task NativeDeclarations_RefuseUntypedNullForEveryResultTypeButAcceptTypedNull()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NullDeclaration");
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(
            $$$"""
            $db=$engine.CreateDatabase({{{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath)}}},';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            try {
                foreach($type in @(1,2,3,4,5,6,7,8,9,10,11,12,15,16,20,26)) {
                    $name='T'+$type;$db.Execute('CREATE TABLE ['+$name+']([N] DOUBLE)',128);$db.TableDefs.Refresh();$td=$db.TableDefs($name)
                    $bad=if($type -in @(9,10)){$td.CreateField('Bad',$type,100)}else{$td.CreateField('Bad',$type)}
                    $bad.Expression='Null';$refused=$false
                    try{$td.Fields.Append($bad)}catch{if(@($engine.Errors|ForEach-Object Number) -notcontains 3948){throw};$refused=$true}
                    if(-not $refused){throw ('Untyped Null accepted for result'+$type)}
                    $good=if($type -in @(9,10)){$td.CreateField('Good',$type,100)}else{$td.CreateField('Good',$type)}
                    $good.Expression='IIf(True,Null,[N])';$td.Fields.Append($good)
                }
            } finally {$db.Close()}
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
    }

    [Fact(Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason, SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable), SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task NativeDateSerial_YearLiteralGrammarMatchesPinnedSourceForms()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.YearDeclaration");
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(
            $$$"""
            $db=$engine.CreateDatabase({{{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath)}}},';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            try {
                $cases=@(@('0',$false),@('29',$false),@('999',$false),@('0100',$false),@('0000',$false),@('10000',$false),@('-1',$false),@('2020.0',$false),@('2020.5',$false),@('1000.',$false),@('2020.00E0',$false),@('1.2345E3',$false),@('100.0E1',$false),@('2020E-1',$false),@('999.49',$false),@('0.2020',$false),@('0.0001',$false),@('1.234E-1000',$false),@('1000',$true),@('9999',$true),@('-1000',$true),@('+29',$true),@('+2020.0',$true),@('(29)',$true),@('((0000))',$true),@('2020+0',$true),@('2020.0+0',$true),@('CDbl(2020)',$true),@('[N]',$true),@('"2020"',$true),@('1e3',$true),@('1.0000E3',$true),@('10000E-1',$true),@('999.50',$true),@('123.4',$true),@('1.234E-10',$true),@('1.234E-300',$true),@('9999.0',$true))
                for($index=0;$index -lt $cases.Count;$index++) {
                    $item=$cases[$index];$name='T'+$index;$db.Execute('CREATE TABLE ['+$name+']([N] DOUBLE)',128);$db.TableDefs.Refresh();$td=$db.TableDefs($name)
                    $field=$td.CreateField('V',8);$field.Expression='DateSerial('+$item[0]+',1,1)';$accepted=$true
                    try{$td.Fields.Append($field)}catch{if(@($engine.Errors|ForEach-Object Number) -notcontains 3950){throw};$accepted=$false}
                    if($accepted -ne $item[1]){throw ('Native DateSerial admission changed for '+$item[0])}
                }
            } finally {$db.Close()}
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
    }

    private static ColumnDefinition Calculated(string name, string expression)
        => new(name, typeof(double)) { IsCalculated = true, CalculationExpression = expression };
}
