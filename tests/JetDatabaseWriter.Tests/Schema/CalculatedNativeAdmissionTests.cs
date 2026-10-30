namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Compares table expression admission with independently measured ACE declarations.</summary>
public sealed class CalculatedNativeAdmissionTests
{
    private static readonly (string Expression, bool Accepted, int NativeError)[] Controls =
    [
        ("Mid(Null,1)", false, 3950),
        ("Mid([Id],1)", false, 3950),
        ("IIf([Id]>0,\"yes\",\"no\")", true, 0),
        ("Nz([Text],\"\")", false, 3859),
        ("IsNull([Text])", true, 0),
        ("IsNumeric([Text])", false, 3859),
        ("IsDate([Text])", false, 3859),
        ("Len([Text])", true, 0),
        ("Left([Text],2)", true, 0),
        ("Right([Text],2)", true, 0),
        ("Mid([Text],2,2)", true, 0),
        ("UCase([Text])", true, 0),
        ("LCase([Text])", true, 0),
        ("Trim([Text])", true, 0),
        ("LTrim([Text])", false, 3859),
        ("RTrim([Text])", false, 3859),
        ("Replace([Text],\"a\",\"b\")", false, 3859),
        ("InStr([Text],\"a\")", false, 3950),
        ("InStrRev([Text],\"a\")", false, 3859),
        ("Space(2)", true, 0),
        ("StrComp([Text],\"a\")", false, 3859),
        ("StrConv([Text],1)", false, 3859),
        ("String(2,\"a\")", true, 0),
        ("StrReverse([Text])", false, 3859),
        ("Asc([Text])", true, 0),
        ("AscW([Text])", false, 3859),
        ("Chr(65)", false, 3859),
        ("ChrW(65)", false, 3859),
        ("Str([Amount])", true, 0),
        ("Format([Amount],\"0.00\")", false, 3859),
        ("FormatNumber([Amount])", false, 3859),
        ("FormatPercent([Amount])", false, 3859),
        ("FormatCurrency([Amount])", false, 3859),
        ("FormatDateTime([At])", false, 3859),
        ("Abs([Amount])", true, 0),
        ("Round([Amount],2)", true, 0),
        ("Int([Amount])", true, 0),
        ("Fix([Amount])", true, 0),
        ("Rnd()", false, 3859),
        ("Sgn([Amount])", true, 0),
        ("Sqr(Abs([Amount]))", true, 0),
        ("Sin([Amount])", true, 0),
        ("Cos([Amount])", true, 0),
        ("Tan([Amount])", true, 0),
        ("Atn([Amount])", true, 0),
        ("Exp([Amount])", true, 0),
        ("Log(Abs([Amount])+1)", true, 0),
        ("Date()", false, 3859),
        ("Now()", false, 3859),
        ("Time()", false, 3859),
        ("Timer()", false, 3859),
        ("DateSerial(2020,1,31)", true, 0),
        ("TimeSerial(18,0,0)", true, 0),
        ("DateAdd(\"d\",1,[At])", false, 3859),
        ("DateDiff(\"d\",[At],#2020-02-01#)", false, 3859),
        ("DatePart(\"yyyy\",[At])", false, 3859),
        ("Year([At])", true, 0),
        ("Month([At])", true, 0),
        ("Day([At])", true, 0),
        ("Hour([At])", true, 0),
        ("Minute([At])", true, 0),
        ("Second([At])", true, 0),
        ("MonthName(1)", true, 0),
        ("Weekday([At])", true, 0),
        ("WeekdayName(1)", true, 0),
        ("CDbl([Amount])", true, 0),
        ("CVar([Amount])", false, 3859),
        ("VarType([Amount])", false, 3859),
        ("TypeName([Amount])", false, 3859),
        ("Hex([Id])", false, 3859),
        ("Oct([Id])", false, 3859),
        ("Val([Text])", false, 3859),
        ("FV(0.1,2,1)", true, 0),
        ("PV(0.1,2,1)", true, 0),
        ("Pmt(0.1,2,1)", true, 0),
        ("NPer(0.1,-1,2)", true, 0),
        ("IPmt(0.1,1,2,1)", true, 0),
        ("PPmt(0.1,1,2,1)", true, 0),
        ("DDB(10,1,2,1)", true, 0),
        ("SLN(10,1,2)", true, 0),
        ("SYD(10,1,2,1)", true, 0),
        ("Rate(2,-1,1)", true, 0),
        ("Choose(1,\"a\",\"b\")", true, 0),
        ("Switch(True,\"a\")", false, 3859),
        ("CurrentUser()", false, 3859),
        ("GenGUID()", false, 3859),
        ("[At]+[CurrencyValue]", true, 0),
        ("[Id]/0", true, 0),
        ("&H10", false, 3950),
        ("True And Null", true, 0),
        ("InStr(1,[Text],\"a\")", true, 0),
        ("InStr(1,[Text],\"a\",0)", true, 0),
        ("InStr(1,[Text],\"a\",1)", true, 0),
        ("Left$([Text],1)", true, 0),
        ("Mid$([Text],1,1)", true, 0),
        ("Str$([Amount])", true, 0),
        ("MonthName(1,True)", true, 0),
        ("WeekdayName(1,True,2)", true, 0),
        ("Upper([Text])", false, 3859),
        ("Lower([Text])", false, 3859),
        ("Sqrt([Amount])", false, 3859),
        ("Atan([Amount])", false, 3859),
        ("IF(True,1,2)", false, 3859),
        ("IsBlank([Text])", false, 3859),
        ("Len$([Text])", false, 3859),
        ("Abs$([Id])", false, 3859),
        ("CDbl$([Id])", false, 3859),
        ("True Eqv False", false, 3858),
        ("True Imp False", false, 3858),
        ("True Xor False", false, 3859),
        ("1\\2", false, 3859),
        ("5 Mod 2", false, 3859),
        ("[Id] Between 1 And 2", false, 3858),
        ("[Id] In (1,2)", true, 0),
        ("[Text] Like \"a*\"", true, 0),
        ("[Id] Is Null", true, 0),
        ("2^3^2", true, 0),
        ("1%", false, 3950),
        ("Right$([Text],1)", true, 0),
        ("UCase$([Text])", true, 0),
        ("LCase$([Text])", true, 0),
        ("Trim$([Text])", true, 0),
        ("Space$(2)", true, 0),
        ("String$(2,\"a\")", true, 0),
        ("MonthName$(1)", false, 3859),
        ("WeekdayName$(1)", false, 3859),
        ("IIf$(True,\"a\",\"b\")", false, 3859),
        ("Choose$(1,\"a\",\"b\")", false, 3859),
        ("InStr$(1,[Text],\"a\")", false, 3859),
        ("5 And 3", true, 0),
        ("5 Or 3", true, 0),
        ("Not 1", true, 0),
        ("[Id] Not In (1,2)", true, 0),
        ("[Text] Not Like \"a*\"", true, 0),
        ("[Id] Not Between 1 And 2", false, 3858),
        ("=1", false, 3950),
        ("Sqr(-1)", true, 0),
        ("Log(0)", true, 0),
        ("Sin(Null)", true, 0),
        ("CDbl(Null)", true, 0),
        ("InStr(1,Null,\"a\")", true, 0),
    ];

    /// <summary>Gets frozen declarations measured with Microsoft Access Runtime 16.0.20527.20112.</summary>
    public static TheoryData<string, bool, int> AdmissionCases
    {
        get
        {
            var data = new TheoryData<string, bool, int>();
            foreach ((string expression, bool accepted, int error) in Controls)
            {
                data.Add(expression, accepted, error);
            }

            return data;
        }
    }

    [Theory]
    [MemberData(nameof(AdmissionCases))]
    public void CalculatedDeclaration_MatchesNativeContext(string expression, bool accepted, int nativeError)
    {
        Exception? failure = Record.Exception(() => CalculatedExpressionParser.Parse(expression));
        if (accepted)
        {
            Assert.Null(failure);
        }
        else
        {
            Assert.True(failure is ArgumentException or JetNotSupportedException, $"Native error {nativeError} refuses '{expression}'; managed failure: {failure}");
        }
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task DaoCalculatedDeclarations_MatchFrozenAdmissionControls()
    {
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateDaoAccdbAsync(
            TestContext.Current.CancellationToken, "JetDatabaseWriter.Tests.CalculatedAdmission");
        string cases = string.Join(",\n", Controls.Select(static control => $"@({AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(control.Expression)},{(control.Accepted ? "$true" : "$false")},{control.NativeError})"));
        AccessRoundTripEnvironment.CompactResult native = session.RunDaoDatabaseScript(
            session.SourcePath,
            $$"""
            $cases = @({{cases}})
            foreach ($case in $cases) {
                $db.Execute('CREATE TABLE [T] ([Id] LONG, [Amount] DOUBLE, [Text] TEXT(100), [At] DATETIME, [CurrencyValue] CURRENCY)',128)
                $db.Execute('INSERT INTO [T] ([Id],[Amount],[Text],[At],[CurrencyValue]) VALUES (1,1.25,"alpha",#2020-01-01#,1.25)',128)
                $db.TableDefs.Refresh()
                $table = $db.TableDefs('T')
                $field = $table.CreateField('Calculated',10,255)
                $field.Expression = $case[0]
                $accepted = $false
                $errorCode = 0
                try { $table.Fields.Append($field); $accepted = $true }
                catch {
                    foreach ($nativeError in $engine.Errors) { $errorCode = $nativeError.Number }
                }
                if ($accepted -ne $case[1] -or $errorCode -ne $case[2]) {
                    throw ('Unexpected native admission: ' + $case[0] + '; accepted=' + $accepted + '; error=' + $errorCode)
                }
                if ($accepted) { $table.Fields.Delete('Calculated') }
                if ($table.Fields.Count -ne 5) { throw 'Declaration changed the surviving source fields.' }
                $db.TableDefs.Delete('T')
            }
            Write-Output ('NATIVE_ADMISSION_CONTROLS=' + $cases.Count)
            """,
            TimeSpan.FromMinutes(2));
        Assert.True(native.ExitCode == 0, $"DAO admission failed: {native.StdOut}\n{native.StdErr}");
        Assert.Contains($"NATIVE_ADMISSION_CONTROLS={Controls.Length}", native.StdOut, StringComparison.Ordinal);
    }
}
