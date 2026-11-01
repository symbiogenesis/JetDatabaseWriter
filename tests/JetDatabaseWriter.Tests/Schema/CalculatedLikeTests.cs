namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native ACE wildcard grammar, character boundaries and calculation errors.</summary>
public sealed class CalculatedLikeTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("native-like-legacy.accdb", "EA3F78F92F7819165EB4F9402F4BB02DBC6538916528963E89A8CD2D11B14C1A")]
    [InlineData("native-like-legacy-compacted.accdb", "EF7564E97CFF6157C73599EEAF200E27978C67D11199D1BC4E8A24497287686E")]
    [InlineData("native-like-general.accdb", "89A767CA41C48AD1B1F8605B9A9EE6A6392E6E3D808FB6D7D5AD342E1FE512B8")]
    [InlineData("native-like-general-compacted.accdb", "3BC97832C8FD79519DF211980CE3DED22D3EA2327E153BEB7042AAF53EA794B9")]
    public async Task FrozenNativeResults_KeepErrorsAndMatchExpressionEvaluation(string name, string hash)
    {
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(await File.ReadAllBytesAsync(Fixture(name), Ct))));
        await using AccessReader reader = await AccessReader.OpenAsync(Fixture(name), new AccessReaderOptions { UseLockFile = false }, Ct);
        object[][] rows = (await reader.Rows("LikeCases", cancellationToken: Ct).ToArrayAsync(Ct)).OrderBy(row => row[0]).ToArray();
        using var manifest = JsonDocument.Parse(await File.ReadAllTextAsync(Fixture("cases.json"), Ct));
        Assert.Equal(manifest.RootElement.GetArrayLength(), rows.Length);
        foreach (JsonElement entry in manifest.RootElement.EnumerateArray())
        {
            int id = entry.GetProperty("Id").GetInt32();
            object[] row = rows[id];
            Assert.Equal(ReadText(entry, "Source"), row[1]);
            Assert.Equal(ReadText(entry, "Pattern"), row[2]);
            string expected = entry.GetProperty("Expected").GetString()!;
            AssertValue(expected, row[3], numeric: true, id);
            AssertValue(expected, row[4], numeric: false, id);
            Assert.Equal(42d, row[5]);
            Assert.Equal(false, row[6]);
            Assert.Equal(true, row[7]);
            AssertValue(expected, Evaluate(ReadText(entry, "Source"), ReadText(entry, "Pattern")), numeric: false, id);
        }
    }

    [Fact]
    public void MatchingBudgets_RefuseExcessWorkWithoutReturningFalse()
    {
        Assert.Throws<JetLimitationException>(() => AccessLikeMatcher.Match(new string('a', 1024 * 1024), "*"));
        Assert.Throws<JetLimitationException>(() => AccessLikeMatcher.Match(new string('a', 16384), "*" + new string('a', 4096) + "b"));
        Assert.True(AccessLikeMatcher.Match(new string('a', 32768), "a*"));
    }

    [Fact]
    public void NullOperand_DoesNotHideAnErrorInTheOtherOperand()
    {
        var error = new CalculatedError(5);
        var plan = new CalculatedExpressionFunctionNode("LIKE", [new CalculatedExpressionValueNode(DBNull.Value), new CalculatedExpressionValueNode(error)]);
        Assert.Equal(error, plan.Evaluate(new CalculatedExpressionEvaluationContext(new TableDef(), [], [])));
    }

    [Fact(Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason, SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable), SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task WriterCaches_NativeReadRepairAndCompactAcceptLikeErrors()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CalculatedLike");
        using var manifest = JsonDocument.Parse(await File.ReadAllTextAsync(Fixture("cases.json"), Ct));
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(session.SourcePath, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("S", typeof(string)) { AllowZeroLength = true }, new("P", typeof(string)) { AllowZeroLength = true }, new("V", typeof(bool)) { IsCalculated = true, CalculationExpression = "[S] Like [P]" }], Ct);
            foreach (JsonElement entry in manifest.RootElement.EnumerateArray())
            {
                await writer.InsertRowAsync("T", new RowValues { ["Id"] = entry.GetProperty("Id").GetInt32(), ["S"] = ReadText(entry, "Source"), ["P"] = ReadText(entry, "Pattern") }, Ct);
            }
        }

        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScriptThenCompact(
            $$$"""
            $expected=[IO.File]::ReadAllText({{{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(Fixture("cases.json"))}}})|ConvertFrom-Json
            $rs=$db.OpenRecordset('SELECT * FROM [T] ORDER BY [Id]')
            try {
                foreach($item in $expected) {
                    if($rs.EOF -or $rs.Fields('Id').Value -ne $item.Id){throw 'Native row identity mismatch'}
                    $value=$rs.Fields('V').Value
                    if($item.Expected -eq 'error'){if($null -ne $value){throw ('Expected native error93 at '+$item.Id)}}
                    elseif($item.Expected -eq 'null'){if($value -isnot [DBNull]){throw ('Expected ordinary Null at '+$item.Id)}}
                    elseif($value -ne ($item.Expected -eq 'true')){throw ('Native LIKE cache differs at '+$item.Id)}
                    $rs.MoveNext()
                }
                if(-not $rs.EOF){throw 'Unexpected native rows'}
            } finally {$rs.Close()}
            $db.Execute('UPDATE [T] SET [P]="*" WHERE [Id]=11',128)
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct);
        object[][] rows = await reader.Rows("T", cancellationToken: Ct).ToArrayAsync(Ct);
        Assert.Equal(true, Assert.Single(rows, row => (int)row[0] == 11)[3]);
        Assert.Equal(93, Assert.IsType<CalculatedError>(Assert.Single(rows, row => (int)row[0] == 13)[3]).NativeCode);
    }

    [Fact]
    public async Task WriterCaches_MatchNativeCorpus()
    {
        await using var stream = new MemoryStream();
        using var manifest = JsonDocument.Parse(await File.ReadAllTextAsync(Fixture("cases.json"), Ct));
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("S", typeof(string)) { AllowZeroLength = true }, new("P", typeof(string)) { AllowZeroLength = true }, new("V", typeof(bool)) { IsCalculated = true, CalculationExpression = "[S] Like [P]" }], Ct);
            foreach (JsonElement entry in manifest.RootElement.EnumerateArray())
            {
                await writer.InsertRowAsync("T", new RowValues { ["Id"] = entry.GetProperty("Id").GetInt32(), ["S"] = ReadText(entry, "Source"), ["P"] = ReadText(entry, "Pattern") }, Ct);
            }
        }

        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        object[][] rows = (await reader.Rows("T", cancellationToken: Ct).ToArrayAsync(Ct)).OrderBy(row => row[0]).ToArray();
        foreach (JsonElement entry in manifest.RootElement.EnumerateArray())
        {
            int id = entry.GetProperty("Id").GetInt32();
            AssertValue(entry.GetProperty("Expected").GetString()!, rows[id][3], numeric: false, id);
        }
    }

    private static object Evaluate(object source, object pattern)
    {
        var table = new TableDef { Columns = [new ColumnInfo { Name = "S" }, new ColumnInfo { Name = "P" }, new ColumnInfo { Name = "V" }] };
        var context = new CalculatedExpressionEvaluationContext(table, [new() { Name = "S", ClrType = typeof(string) }, new() { Name = "P", ClrType = typeof(string) }, new() { Name = "V", IsCalculated = true, CalculatedResultType = ColumnType.BooleanType, ClrType = typeof(bool), CalculationExpression = "[S] Like [P]" }], [source, pattern, DBNull.Value]);
        return context.EvaluateColumn(2);
    }

    private static object ReadText(JsonElement entry, string name)
        => (object?)entry.GetProperty(name).GetString() ?? DBNull.Value;

    private static string Fixture(string name)
        => Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedLike", name);

    private static void AssertValue(string expected, object actual, bool numeric, int id)
    {
        if (expected == "error")
        {
            Assert.True(actual is CalculatedError { NativeCode: 93 }, $"Case {id}: expected native LIKE error93, actual={actual}");
        }
        else if (expected == "null")
        {
            Assert.Equal(DBNull.Value, actual);
        }
        else
        {
            bool truth = expected == "true";
            object value = truth;
            if (numeric)
            {
                value = truth ? -1d : 0d;
            }

            Assert.True(value.Equals(actual), $"Case {id}: expected={value}, actual={actual}");
        }
    }
}
