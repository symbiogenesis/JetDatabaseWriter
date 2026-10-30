namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native date variants keep their precise serials across the complete calculation pipeline.</summary>
public sealed class CalculatedNativeDateExpressionDaoTests
{
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task NativeDateExpressions_SourceRecalculationUpdateAndCompactAgree()
    {
        double[] serials = [-0.25d, -1.5d, -1.25d, 0d, 0.4999d / 86400d, 0.5001d / 86400d, 45657.999995d, 0.5d / 86400d, 1.5d / 86400d];
        (int Type, string Expression)[] expressions =
        [
            (7, "CDbl([D])"), (7, "CDbl([D]+[N])"), (7, "CDbl(-[D])"),
            (7, "CDbl(Abs([D]))"), (7, "CDbl(Int([D]))"), (7, "CDbl(Fix([D]))"), (7, "CDbl(Round([D],1))"),
            (7, "[D] < #1899-12-29 6:00#"), (7, "#1899-12-29 12:00# < #1899-12-29 6:00#"),
            (7, "CDbl(TimeSerial(-1,0,0))"), (7, "Second([D])"), (10, "[D] & ''"),
            (8, "TimeSerial(-1,0,0)"), (7, "CDbl([V12])"), (8, "[D]+[N]"), (7, "CDbl([V14])"),
            (7, "CDbl(+[D])"), (7, "[D]-TimeSerial(-1,0,0)"), (10, "Abs([D]) & ''"),
            (7, "[D] < 1E100"),
        ];
        await using AccessRoundTripSession native = await AccessRoundTripSession.CreateDaoAccdbAsync(TestContext.Current.CancellationToken, "JetDatabaseWriter.NativeDateExpressions");
        string writerPath = Path.Combine(native.WorkDir, "writer.accdb");
        string writerCompactedPath = Path.Combine(native.WorkDir, "writer-compacted.accdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.SourcePath);
        string subject = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(writerPath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.CompactedPath);
        string subjectCompacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(writerCompactedPath);
        string nativeExpressions = string.Join(',', expressions.Select(static item => $"@({item.Type},{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(item.Expression)})"));
        string nativeSerials = string.Join(',', serials.Select(static value => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(value.ToString("R", CultureInfo.InvariantCulture))));
        AccessRoundTripEnvironment.CompactResult authored = native.RunDaoEngineScript(
            $$"""
            $db=$engine.OpenDatabase({{source}})
            try {
                $db.Execute('CREATE TABLE [T] ([Id] LONG,[D] DATETIME,[N] DOUBLE)',128)
                $db.TableDefs.Refresh()
                $tdf=$db.TableDefs('T')
                $definitions=@({{nativeExpressions}})
                for($index=0;$index -lt $definitions.Count;$index++) {
                    $field=$tdf.CreateField('V'+$index,[int]$definitions[$index][0])
                    $field.Expression=[string]$definitions[$index][1]
                    $tdf.Fields.Append($field)
                }
                $rs=$db.OpenRecordset('T',2)
                try {
                    $serials=@({{nativeSerials}})
                    for($id=0;$id -lt $serials.Count;$id++) {
                        $rs.AddNew()
                        $rs.Fields('Id').Value=[int]$id
                        $rs.Fields('D').Value=[double]([double]::Parse($serials[$id],[Globalization.CultureInfo]::InvariantCulture))
                        $rs.Fields('N').Value=[double]0
                        $rs.Update()
                    }
                } finally { $rs.Close() }
            } finally { $db.Close() }
            Copy-Item -LiteralPath {{source}} -Destination {{subject}}
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(authored.ExitCode == 0, authored.StdOut + authored.StdErr);
        object[][] expected = await RowsAsync(native.SourcePath);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(writerPath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            for (int id = 0; id < serials.Length; id++)
            {
                Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", id, new Dictionary<string, object?> { ["N"] = 0d }, TestContext.Current.CancellationToken));
            }
        }

        AssertRows(expected, await RowsAsync(writerPath));
        await using (AccessWriter writer = await AccessWriter.OpenAsync(writerPath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", 0, new Dictionary<string, object?> { ["D"] = new DateTime(1899, 12, 29, 12, 0, 0), ["N"] = 0.25d }, TestContext.Current.CancellationToken));
        }

        AccessRoundTripEnvironment.CompactResult updated = native.RunDaoEngineScript(
            $$"""
            $db=$engine.OpenDatabase({{source}})
            try { $db.Execute('UPDATE [T] SET [D]=#12/29/1899 12:00#,[N]=0.25 WHERE [Id]=0',128) } finally { $db.Close() }
            $engine.CompactDatabase({{source}},{{compacted}})
            $engine.CompactDatabase({{subject}},{{subjectCompacted}})
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(updated.ExitCode == 0, updated.StdOut + updated.StdErr);
        AssertRows(await RowsAsync(native.SourcePath), await RowsAsync(writerPath));
        AssertRows(await RowsAsync(native.CompactedPath), await RowsAsync(writerCompactedPath));
    }

    private static void AssertRows(object[][] expected, object[][] actual)
    {
        Assert.Equal(expected.Length, actual.Length);
        for (int row = 0; row < expected.Length; row++)
        {
            Assert.Equal(expected[row].Length, actual[row].Length);
            for (int column = 0; column < expected[row].Length; column++)
            {
                Assert.True(Equals(expected[row][column], actual[row][column]), $"Native date mismatch at row {row}, column {column}: expected {Convert.ToString(expected[row][column], CultureInfo.InvariantCulture)}, actual {Convert.ToString(actual[row][column], CultureInfo.InvariantCulture)}.");
            }
        }
    }

    private static async Task<object[][]> RowsAsync(string path)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        return (await reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken)).OrderBy(static row => (int)row[0]).ToArray();
    }
}
