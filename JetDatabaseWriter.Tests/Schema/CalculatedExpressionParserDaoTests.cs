namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>DAO verifies that cell-shaped names are ordinary Access field references.</summary>
public sealed class CalculatedExpressionParserDaoTests
{
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task BareCellShapedFieldName_ReevaluatesAndCompactsWithDao()
    {
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateDaoAccdbAsync(
            TestContext.Current.CancellationToken, "JetDatabaseWriter.Tests.CalculatedExpressionParser");
        const string authorScript = """
            $db.Execute('CREATE TABLE [T] ([Id] LONG, [A1] LONG)', 128)
            $db.TableDefs.Refresh()
            $tdf = $db.TableDefs('T')
            $field = $tdf.CreateField('Calculated', 4)
            $field.Expression = 'A1 + 1'
            $tdf.Fields.Append($field)
            $db.Execute('INSERT INTO [T] ([Id], [A1]) VALUES (1, 4)', 128)
            $rs = $db.OpenRecordset('SELECT [Calculated] FROM [T]')
            try { Write-Output ('NATIVE_RESULT=' + [string]$rs.Fields('Calculated').Value) }
            finally { $rs.Close() }
            """;
        AccessRoundTripEnvironment.CompactResult authored = session.RunDaoDatabaseScript(session.SourcePath, authorScript, TimeSpan.FromMinutes(1));
        Assert.True(authored.ExitCode == 0, $"DAO authoring failed: {authored.StdOut}\n{authored.StdErr}");
        Assert.Contains("NATIVE_RESULT=5", authored.StdOut, StringComparison.Ordinal);
        await AssertValuesAsync(session.SourcePath, 4, 5);

        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            int updated = await writer.UpdateRowsAsync(
                "T", "Id", 1, new Dictionary<string, object?> { ["A1"] = 8 }, TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
        }

        AccessRoundTripEnvironment.CompactResult compacted = session.RunDaoDatabaseScriptThenCompact(
            """
            $rs = $db.OpenRecordset('SELECT [A1], [Calculated] FROM [T]')
            try {
                if ($rs.Fields('A1').Value -ne 8 -or $rs.Fields('Calculated').Value -ne 9) {
                    throw 'DAO did not read the writer updated calculated value.'
                }
                Write-Output 'WRITER_RESULT=9'
            } finally { $rs.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(compacted.ExitCode == 0, $"DAO compact failed: {compacted.StdOut}\n{compacted.StdErr}");
        Assert.Contains("WRITER_RESULT=9", compacted.StdOut, StringComparison.Ordinal);
        await AssertValuesAsync(session.CompactedPath, 8, 9);
    }

    private static async Task AssertValuesAsync(string path, int source, int calculated)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(
            path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync("T", TestContext.Current.CancellationToken);
        Assert.Equal("A1 + 1", columns[2].CalculationExpression?.TrimEnd('\0'));
        int count = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, row[0]);
            Assert.Equal(source, row[1]);
            Assert.Equal(calculated, row[2]);
            count++;
        }

        Assert.Equal(1, count);
    }
}
