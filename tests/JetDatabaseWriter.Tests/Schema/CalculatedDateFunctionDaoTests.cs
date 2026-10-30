namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native date function caches, including error codes, survive writer updates and native compact.</summary>
public sealed class CalculatedDateFunctionDaoTests
{
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task NativeDateFunctions_WriterAndNativeCompactAgree()
    {
        double?[] values = [null, 0, 1.5, 2.5, 2025.5, 45657.999995, 45657.999994, 0.0000058, 0.0000057, -0.0000058, 1E100, -25, 49, 50, -100, 100, 999];
        string[] expressions = ["Year([N])", "Month([N])", "Day([N])", "Hour([N])", "Minute([N])", "Second([N])", "Weekday([N])", "Weekday(Null,[N])", "DateSerial([N],2,3)", "TimeSerial([N],0,0)", "Len(MonthName([N],True))", "Len(WeekdayName([N],True,2))", "Year(TimeSerial([N],0,0))"];
        await using AccessRoundTripSession native = await AccessRoundTripSession.CreateDaoAccdbAsync(TestContext.Current.CancellationToken, "JetDatabaseWriter.CalculatedDateFunctions");
        string writerPath = Path.Combine(native.WorkDir, "writer.accdb");
        string writerCompactedPath = Path.Combine(native.WorkDir, "writer-compacted.accdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.CompactedPath);
        string subject = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(writerPath);
        string subjectCompacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(writerCompactedPath);
        string nativeExpressions = string.Join(',', expressions.Select(AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral));
        string nativeValues = string.Join(',', values.Select(static value => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(value?.ToString("R", CultureInfo.InvariantCulture) ?? "NULL")));
        AccessRoundTripEnvironment.CompactResult authored = native.RunDaoEngineScript(
            $$"""
            $db=$engine.OpenDatabase({{source}})
            try {
                $db.Execute('CREATE TABLE [T] ([Id] LONG,[N] DOUBLE)',128)
                $db.TableDefs.Refresh()
                $tdf=$db.TableDefs('T')
                $expressions=@({{nativeExpressions}})
                for($index=0;$index -lt $expressions.Count;$index++) {
                    $field=$tdf.CreateField('V'+$index,7)
                    $field.Expression=[string]$expressions[$index]
                    $tdf.Fields.Append($field)
                }
            } finally { $db.Close() }
            Copy-Item -LiteralPath {{source}} -Destination {{subject}}
            $db=$engine.OpenDatabase({{source}})
            try {
                $rs=$db.OpenRecordset('T',2)
                try {
                    $values=@({{nativeValues}})
                    for($id=0;$id -lt $values.Count;$id++) {
                        $rs.AddNew()
                        $rs.Fields('Id').Value=[int]$id
                        if($values[$id] -eq 'NULL') { $rs.Fields('N').Value=[DBNull]::Value }
                        else { $rs.Fields('N').Value=[double]([double]::Parse($values[$id],[Globalization.CultureInfo]::InvariantCulture)) }
                        $rs.Update()
                    }
                } finally { $rs.Close() }
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(authored.ExitCode == 0, authored.StdOut + authored.StdErr);
        object[][] expected = await RowsAsync(native.SourcePath);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(writerPath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowsAsync("T", values.Select((value, id) => new object?[] { id, value }.Concat(Enumerable.Repeat<object?>(null, expressions.Length)).ToArray()), TestContext.Current.CancellationToken);
        }

        Assert.Equal(expected, await RowsAsync(writerPath));
        await using (AccessWriter writer = await AccessWriter.OpenAsync(writerPath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", 0, new Dictionary<string, object?> { ["N"] = 2025.5d }, TestContext.Current.CancellationToken));
        }

        AccessRoundTripEnvironment.CompactResult updated = native.RunDaoEngineScript(
            $$"""
            $db=$engine.OpenDatabase({{source}})
            try { $db.Execute('UPDATE [T] SET [N]=2025.5 WHERE [Id]=0',128) } finally { $db.Close() }
            $engine.CompactDatabase({{source}},{{compacted}})
            $engine.CompactDatabase({{subject}},{{subjectCompacted}})
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(updated.ExitCode == 0, updated.StdOut + updated.StdErr);
        expected = await RowsAsync(native.SourcePath);
        Assert.Equal(expected, await RowsAsync(writerPath));
        Assert.Equal(await RowsAsync(native.CompactedPath), await RowsAsync(writerCompactedPath));
    }

    private static async Task<object[][]> RowsAsync(string path)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        return (await reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken)).OrderBy(static row => (int)row[0]).ToArray();
    }
}
