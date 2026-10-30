namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native table condition evaluation and result coercion have different rules.</summary>
public sealed class CalculatedNativeLogicalTests
{
    private static readonly Control[] LogicalControls =
    [
        new("T0", "Choose(1.5,10,20,30)", typeof(double), "10"),
        new("T1", "Choose(2.5,10,20,30)", typeof(double), "20"),
        new("T2", "Choose(0.5,10,20,30)", typeof(double), "(Null)"),
        new("T3", "Choose(Null,10,20)", typeof(double), "#Error(13)"),
        new("T4", "IIf(Null,1,2)", typeof(double), "2"),
        new("T5", "IIf(\"abc\",1,2)", typeof(double), "1"),
        new("T6", "IIf(\"\",1,2)", typeof(double), "1"),
        new("T7", "IIf(\"true\",1,2)", typeof(double), "1"),
        new("T8", "\"abc\"", typeof(bool), "#Error(13)"),
        new("T9", "\"\"", typeof(bool), "#Error(13)"),
        new("T10", "\"yes\"", typeof(bool), "#Error(13)"),
        new("T11", "1 In (Null,2)", typeof(bool), "False"),
        new("T12", "1 Not In (Null,2)", typeof(bool), "True"),
        new("T13", "1 In (Null,1)", typeof(bool), "True"),
        new("T14", "Null Like \"a\"", typeof(bool), "(Null)"),
        new("T16", "IIf(False,1/0,2)", typeof(double), "2"),
        new("T17", "Choose(1,42,1/0)", typeof(double), "#Error(11)"),
        new("T18", "False And \"abc\"", typeof(bool), "False"),
        new("T19", "False And (1/0)", typeof(bool), "False"),
        new("T20", "Not \"abc\"", typeof(double), "0"),
        new("T21", "Null And \"abc\"", typeof(bool), "(Null)"),
        new("T22", "IsEmpty(Null)", typeof(bool), "False"),
        new("T23", "IsEmpty(\"\")", typeof(bool), "False"),
        new("T24", "IsEmpty(1/0)", typeof(bool), "#Error(11)"),
        new("T25", "IsEmpty(0)", typeof(bool), "False"),
    ];

    private static readonly Control[] BooleanControls =
    [
        new("T0", "0.1", typeof(bool), "False"),
        new("T1", "0.5", typeof(bool), "False"),
        new("T2", "0.6", typeof(bool), "True"),
        new("T3", "1.5", typeof(bool), "True"),
        new("T4", "-0.1", typeof(bool), "False"),
        new("T5", "-0.5", typeof(bool), "False"),
        new("T6", "-0.6", typeof(bool), "True"),
        new("T7", "255", typeof(bool), "True"),
        new("T8", "256", typeof(bool), "True"),
        new("T9", "32767", typeof(bool), "True"),
        new("T10", "32768", typeof(bool), "#Error(6)"),
        new("T11", "2147483647", typeof(bool), "#Error(6)"),
        new("T12", "2147483648", typeof(bool), "#Error(13)"),
        new("T13", "1e100", typeof(bool), "#Error(6)"),
        new("T14", "1e-100", typeof(bool), "False"),
        new("T15", "\"true\"", typeof(bool), "#Error(13)"),
        new("T16", "\"false\"", typeof(bool), "#Error(13)"),
        new("T17", "\"0\"", typeof(bool), "False"),
        new("T18", "\"1\"", typeof(bool), "True"),
        new("T19", "\"abc\"", typeof(bool), "#Error(13)"),
        new("T20", "\"\"", typeof(bool), "#Error(13)"),
        new("T21", "#12:00 AM#", typeof(bool), "False"),
        new("T22", "#1:00 AM#", typeof(bool), "False"),
        new("T23", "IIf(\"0\",1,2)", typeof(bool), "True"),
        new("T24", "IIf(\"false\",1,2)", typeof(bool), "True"),
        new("T25", "IIf(0.1,1,2)", typeof(bool), "True"),
        new("T26", "IIf(1e-100,1,2)", typeof(bool), "True"),
        new("T27", "\"abc\" And 3", typeof(bool), "True"),
        new("T28", "\"0\" Or 0", typeof(bool), "False"),
        new("T29", "\"\" Or 0", typeof(bool), "True"),
        new("T30", "\"false\" And 1", typeof(bool), "True"),
    ];

    /// <summary>Gets independent native expression results and result-type controls.</summary>
    public static TheoryData<string, Type, string> Cases
    {
        get
        {
            var data = new TheoryData<string, Type, string>();
            foreach (Control control in LogicalControls.Concat(BooleanControls))
            {
                data.Add(control.Expression, control.ResultType, control.Expected);
            }

            return data;
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task CalculatedLogic_MatchesNativeCachedResult(string expression, Type resultType, string expected)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Value", resultType) { IsCalculated = true, CalculationExpression = expression }], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [DBNull.Value], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        object[] row = Assert.Single(await reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken));
        Assert.Equal(expected, Render(row[0]));
    }

    [Theory]
    [InlineData("logical.accdb", "3F5A27ACDC8EEA1F1766F11572A066A172FA01A7686BC1230776EFE36629A25B", false)]
    [InlineData("logical-compacted.accdb", "CF43C55F1D4BBBEE85EF6512F11EB3592DFCEF6D09BBF7F5B8523BEBB6BB757A", false)]
    [InlineData("boolean.accdb", "A923D3D8DFE8A36A5C25156C9A554246736AF96499821DDA43C2C7B13AA656AE", true)]
    [InlineData("boolean-compacted.accdb", "F4FF0782D94CC8A2055EA1E1F50DB88B787B3520627F518D7C1C642123939D3A", true)]
    public async Task NativeFixtures_RetainIndependentCachedResults(string fixture, string hash, bool boolean)
    {
        string path = Path.Combine(AppContext.BaseDirectory, "Databases", "NativeCalculatedLogic", fixture);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken))));
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        foreach (Control control in boolean ? BooleanControls : LogicalControls)
        {
            object[] row = Assert.Single(await reader.Rows(control.Table, cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken));
            Assert.Equal(control.Expected, Render(row[1]));
        }
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task DaoLogicalCaches_AcceptsWriterResultsAndRecalculatesCompacts()
    {
        Control[] controls = [.. LogicalControls, .. BooleanControls];
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateDaoAccdbAsync(
            TestContext.Current.CancellationToken, "JetDatabaseWriter.Tests.NativeCalculatedLogic");
        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            for (int index = 0; index < controls.Length; index++)
            {
                Control control = controls[index];
                await writer.CreateTableAsync(
                    "C" + index.ToString(CultureInfo.InvariantCulture),
                    [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Value", control.ResultType) { IsCalculated = true, CalculationExpression = control.Expression }],
                    TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("C" + index.ToString(CultureInfo.InvariantCulture), [1, DBNull.Value], TestContext.Current.CancellationToken);
            }
        }

        AccessRoundTripEnvironment.CompactResult native = session.RunDaoDatabaseScriptThenCompact(
            """
            foreach ($table in $db.TableDefs) {
                if (!$table.Name.StartsWith('C')) { continue }
                $record = $db.OpenRecordset('SELECT [Value] FROM [' + $table.Name + ']')
                try { if ($record.EOF) { throw 'Writer logical row is missing.' }; $value = $record.Fields(0).Value }
                finally { $record.Close() }
                $db.Execute('UPDATE [' + $table.Name + '] SET [Id]=2',128)
            }
            Write-Output 'NATIVE_LOGICAL_RECALCULATION=OK'
            """,
            TimeSpan.FromMinutes(2));
        Assert.True(native.ExitCode == 0, $"DAO logical validation failed: {native.StdOut}\n{native.StdErr}");
        Assert.Contains("NATIVE_LOGICAL_RECALCULATION=OK", native.StdOut, StringComparison.Ordinal);
        await using AccessReader reader = await AccessReader.OpenAsync(
            session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        for (int index = 0; index < controls.Length; index++)
        {
            object[] row = Assert.Single(await reader.Rows("C" + index.ToString(CultureInfo.InvariantCulture), cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken));
            Assert.Equal(2, row[0]);
            Assert.Equal(controls[index].Expected, Render(row[1]));
        }
    }

    private static string Render(object value) => value switch
    {
        DBNull => "(Null)",
        double number => number.ToString("R", CultureInfo.InvariantCulture),
        _ => value.ToString() ?? string.Empty,
    };

    private sealed record Control(string Table, string Expression, Type ResultType, string Expected);
}
