namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Choose converts its index to Single before truncation and preserves native Null/error boundaries.</summary>
public sealed class CalculatedChooseTests
{
    private static readonly string[] TableNames = ["DecimalIndices", "NonfiniteIndices", "SingleIndices", "T"];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(null, "#Error(13)")]
    [InlineData(0.9999999d, "(Null)")]
    [InlineData(0.99999999d, "10")]
    [InlineData(1.5d, "10")]
    [InlineData(1.9999999d, "10")]
    [InlineData(1.99999999d, "20")]
    [InlineData(2.99999999d, "30")]
    [InlineData(2147483648d, "(Null)")]
    [InlineData(1E30, "(Null)")]
    [InlineData(3.40282355E38, "(Null)")]
    [InlineData(3.4028236E38, "#Error(6)")]
    [InlineData(1E100, "#Error(6)")]
    [InlineData(-1E30, "(Null)")]
    [InlineData(-1E100, "#Error(6)")]
    public async Task CalculatedIndex_MatchesIndependentNativeOutcomes(double? value, string expected)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("N", typeof(double)), new ColumnDefinition("Choice", typeof(double)) { IsCalculated = true, CalculationExpression = "Choose([N],10,20,30)" }], Ct);
            await writer.InsertRowAsync("T", [value, DBNull.Value], Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        object[] row = Assert.Single(await reader.Rows("T", cancellationToken: Ct).ToArrayAsync(Ct));
        Assert.Equal(expected, Render(row[1]));
    }

    [Theory]
    [InlineData(0.99999999d, "10")]
    [InlineData(1.5d, "10")]
    [InlineData(1.99999999d, "20")]
    [InlineData(2147483648d, "(Null)")]
    [InlineData(1E30, "(Null)")]
    public void SharedIndex_MatchesFreshAceQueryControls(double value, string expected)
    {
        var table = new TableDef { Columns = [new ColumnInfo { Name = "N" }] };
        var context = new CalculatedExpressionEvaluationContext(table, [new ColumnConstraint { Name = "N", ClrType = typeof(double), StorageType = ColumnType.DoubleType }], [value]);
        object result = CalculatedExpressionParser.Parse("Choose([N],10,20,30)", isCalculatedColumn: false).Evaluate(context);
        Assert.Equal(expected, Render(result));
    }

    [Theory]
    [InlineData("choose.accdb", "E3CBD8DFE40A31D9D5D2D2DE2545369EBAA13352F322AED456BC86ECF572BF52")]
    [InlineData("choose-compacted.accdb", "492D36470C2B9EE135AF5D52124C4D0A8BBFDC52F8D4F81AC65E13A3775ECDC8")]
    public async Task NativeFixtures_PreserveIndicesBranchWidthsAndErrorsThroughPublicWrites(string name, string hash)
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedChoose", name), Ct);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(original)));
        NativeTable[] expected = await ReadTablesAsync(original);
        byte[] rewritten = await RewriteAsync(original, expected);
        AssertTablesEqual(expected, await ReadTablesAsync(rewritten));
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task DaoChoose_FreshSourceWriterNativeRecalculationAndCompactAgree()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CalculatedChoose");
        string output = Path.Combine(session.WorkDir, "native");
        string script = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(Path.Combine(AppContext.BaseDirectory, "Scripts", "new-native-calculated-choose-fixtures.ps1"));
        AccessRoundTripEnvironment.CompactResult authored = session.RunDaoEngineScript($"& {script} -OutputDirectory {AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(output)}", TimeSpan.FromMinutes(1));
        Assert.True(authored.ExitCode == 0, authored.StdOut + authored.StdErr);
        string source = Path.Combine(output, "choose.accdb");
        byte[] original = await File.ReadAllBytesAsync(source, Ct);
        NativeTable[] expected = await ReadTablesAsync(original);
        AssertTablesEqual(expected, await ReadTablesAsync(await File.ReadAllBytesAsync(Path.Combine(output, "choose-compacted.accdb"), Ct)));
        string subject = session.CreateDatabasePath("writer");
        await File.WriteAllBytesAsync(subject, await RewriteAsync(original, expected), Ct);
        File.Copy(subject, session.CreateDatabasePath("writer-pristine"));
        AssertTablesEqual(expected, await ReadTablesAsync(await File.ReadAllBytesAsync(subject, Ct)));
        string sourceCompacted = session.CreateDatabasePath("native-recomputed");
        string subjectCompacted = session.CreateDatabasePath("writer-native-recomputed");
        AccessRoundTripEnvironment.CompactResult recomputed = session.RunDaoEngineScript(
            $$"""
            foreach($path in @({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(source)}},{{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(subject)}})) {
                Write-Output ('OPEN '+$path)
                $db=$engine.OpenDatabase($path)
                try {
                    foreach($table in @('T','SingleIndices','DecimalIndices','NonfiniteIndices')) {
                        Write-Output ('READ '+$table)
                        $record=$db.OpenRecordset('SELECT [Choice],[ChoiceBinary] FROM ['+$table+']')
                        try { while(!$record.EOF) { $null=$record.Fields('Choice').Value; $null=$record.Fields('ChoiceBinary').Value; $record.MoveNext() } }
                        finally { $record.Close(); [void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($record) }
                        Write-Output ('UPDATE '+$table)
                        $db.Execute('UPDATE ['+$table+'] SET Id=Id',128)
                    }
                } finally { $db.Close(); [void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($db) }
            }
            Write-Output 'COMPACT SOURCE'
            $engine.CompactDatabase({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(source)}},{{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(sourceCompacted)}})
            Write-Output 'COMPACT WRITER'
            $engine.CompactDatabase({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(subject)}},{{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(subjectCompacted)}})
            Write-Output 'NATIVE_CHOOSE_RECALCULATION_COMPACT=OK'
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(recomputed.ExitCode == 0, recomputed.StdOut + recomputed.StdErr);
        Assert.Contains("NATIVE_CHOOSE_RECALCULATION_COMPACT=OK", recomputed.StdOut, StringComparison.Ordinal);
        AssertTablesEqual(expected, await ReadTablesAsync(await File.ReadAllBytesAsync(sourceCompacted, Ct)));
        AssertTablesEqual(expected, await ReadTablesAsync(await File.ReadAllBytesAsync(subjectCompacted, Ct)));
    }

    private static async Task<NativeTable[]> ReadTablesAsync(byte[] bytes)
    {
        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        Assert.Equal(TableNames, (await reader.ListTablesAsync(Ct)).Order(StringComparer.Ordinal));
        var tables = new List<NativeTable>();
        foreach (string table in TableNames)
        {
            IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync(table, Ct);
            object[][] rows = (await reader.Rows(table, cancellationToken: Ct).ToArrayAsync(Ct)).OrderBy(row => (int)row[0]).ToArray();
            tables.Add(new NativeTable(table, columns, rows));
        }

        return tables.ToArray();
    }

    private static async Task<byte[]> RewriteAsync(byte[] original, NativeTable[] expected)
    {
        await using var stream = new MemoryStream();
        await stream.WriteAsync(original, Ct);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            foreach (NativeTable table in expected)
            {
                Assert.Equal(table.Rows.Length, await writer.DeleteRowsAsync(table.Name, RowCriteria.All(), Ct));
                foreach (object[] row in table.Rows)
                {
                    object?[] values = row.Select((value, index) => table.Columns[index].IsCalculated ? DBNull.Value : value).ToArray();
                    await writer.InsertRowAsync(table.Name, values, Ct);
                }
            }
        }

        AssertTablesEqual(expected, await ReadTablesAsync(stream.ToArray()));
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            foreach (NativeTable table in expected)
            {
                foreach (object[] row in table.Rows)
                {
                    Assert.Equal(1, await writer.UpdateRowsAsync(table.Name, "Id", row[0], new Dictionary<string, object?> { ["Id"] = row[0] }, Ct));
                }
            }
        }

        return stream.ToArray();
    }

    private static void AssertTablesEqual(NativeTable[] expected, NativeTable[] actual)
    {
        Assert.Equal(expected.Length, actual.Length);
        for (int table = 0; table < expected.Length; table++)
        {
            Assert.Equal(expected[table].Name, actual[table].Name);
            Assert.Equal(expected[table].Rows.Length, actual[table].Rows.Length);
            for (int row = 0; row < expected[table].Rows.Length; row++)
            {
                object[] left = expected[table].Rows[row];
                object[] right = actual[table].Rows[row];
                Assert.Equal(left.Length, right.Length);
                for (int column = 0; column < left.Length; column++)
                {
                    bool equal = (left[column], right[column]) switch
                    {
                        (byte[] a, byte[] b) => a.AsSpan().SequenceEqual(b),
                        (double a, double b) => BitConverter.DoubleToInt64Bits(a) == BitConverter.DoubleToInt64Bits(b),
                        _ => Equals(left[column], right[column]),
                    };
                    Assert.True(equal, $"{expected[table].Name}, row {row}, column {expected[table].Columns[column].Name}: native {Render(left[column])}, actual {Render(right[column])}.");
                }
            }
        }
    }

    private static string Render(object value) => value switch
    {
        DBNull => "(Null)",
        byte[] bytes => Convert.ToHexString(bytes),
        IFormattable formatted => formatted.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? string.Empty,
    };

    private sealed record NativeTable(string Name, IReadOnlyList<ColumnMetadata> Columns, object[][] Rows);
}
