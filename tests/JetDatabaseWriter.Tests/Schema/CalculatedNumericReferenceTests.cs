namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Checks Numeric source normalization and independent calculated Decimal scales.</summary>
public sealed class CalculatedNumericReferenceTests
{
    [Fact]
    public async Task NumericReferences_UseStoredSourceAndPreserveCalculatedVariantScale()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("N", typeof(decimal)) { NumericPrecision = 8, NumericScale = 2 },
                    new ColumnDefinition("Times", typeof(decimal)) { IsCalculated = true, CalculationExpression = "[N]*10000", NumericPrecision = 8, NumericScale = 2 },
                    new ColumnDefinition("Third", typeof(decimal)) { IsCalculated = true, CalculationExpression = "[N]/3", NumericPrecision = 8, NumericScale = 2 },
                    new ColumnDefinition("Derived", typeof(decimal)) { IsCalculated = true, CalculationExpression = "[Third]*10000", NumericPrecision = 8, NumericScale = 2 },
                    new ColumnDefinition("AsDouble", typeof(double)) { IsCalculated = true, CalculationExpression = "CDbl([N])" },
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [1, 1.2m, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await AssertRowAsync(reader, updated: false);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", 1, new Dictionary<string, object?> { ["N"] = 1.245m }, TestContext.Current.CancellationToken));
            byte[] before = stream.ToArray();
            await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.UpdateRowsAsync(
                "T", "Id", 1, new Dictionary<string, object?> { ["N"] = 999999.995m }, TestContext.Current.CancellationToken));
            Assert.Equal(before, stream.ToArray());
        }

        stream.Position = 0;
        await using AccessReader reopened = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        await AssertRowAsync(reopened, updated: true);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120_NumericReferencesMatchNativeStoredAndCalculatedScales()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NumericReferences");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            $db.Close()
            $connection = New-Object -ComObject ADODB.Connection
            try {
                $connection.Open('Provider=Microsoft.ACE.OLEDB.12.0;Data Source=' + {{path}})
                $connection.Execute('CREATE TABLE T (Id LONG,N DECIMAL(8,2))') | Out-Null
            } finally {
                $connection.Close()
                [System.Runtime.InteropServices.Marshal]::ReleaseComObject($connection) | Out-Null
            }
            $db = $engine.OpenDatabase({{path}})
            try {
                $tdf = $db.TableDefs('T')
                foreach ($case in @(@('Times',20,'[N]*10000'),@('Third',20,'[N]/3'),@('Derived',20,'[Third]*10000'),@('AsDouble',7,'CDbl([N])'))) {
                    $field = $tdf.CreateField($case[0],$case[1])
                    $field.Expression = $case[2]
                    if ($case[1] -eq 20) { $field.Precision = 8; $field.Scale = 2 }
                    $tdf.Fields.Append($field)
                }
                $rs = $db.OpenRecordset('T',2)
                try {
                    $rs.AddNew()
                    $rs.Fields('Id').Value = 1
                    $rs.Fields('N').Value = [decimal]'1.2'
                    $rs.Update()
                } finally { $rs.Close() }
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, created.StdOut + created.StdErr);
        await using (AccessReader reader = await AccessReader.OpenAsync(
            session.SourcePath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            await AssertRowAsync(reader, updated: false);
        }

        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            session.SourcePath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", 1, new Dictionary<string, object?> { ["N"] = 1.245m }, TestContext.Current.CancellationToken));
        }

        await using (AccessReader updated = await AccessReader.OpenAsync(
            session.SourcePath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            await AssertRowAsync(updated, updated: true);
        }

        string compacted = session.CreateDatabasePath("compacted");
        AccessRoundTripEnvironment.CompactResult accepted = AccessRoundTripEnvironment.RunDaoDatabaseScriptThenCompact(
            session.SourcePath,
            compacted,
            """
            $rs = $db.OpenRecordset('T')
            try {
                if ($rs.Fields('N').Value -ne [decimal]'1.24') { throw 'Stored source changed' }
                if ($rs.Fields('Times').Value -ne 12400) { throw 'Calculated reference did not use the stored source' }
                $third = [decimal]::Parse('0.4133333333333333333333333333',[Globalization.CultureInfo]::InvariantCulture)
                if ($rs.Fields('Third').Value -ne $third) { throw 'Calculated Decimal was rounded to descriptor scale' }
                if (([decimal]::GetBits($rs.Fields('Times').Value)[3] -shr 16 -band 255) -ne 2) { throw 'Source Decimal coefficient scale changed' }
                if (([decimal]::GetBits($rs.Fields('Third').Value)[3] -shr 16 -band 255) -ne 28) { throw 'Calculated Decimal Variant scale changed' }
                $rs.Edit()
                $rs.Fields('N').Value = [decimal]'1.24'
                $rs.Update()
            } finally { $rs.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(1));
        Assert.True(accepted.ExitCode == 0, accepted.StdOut + accepted.StdErr);
        await using AccessReader reopened = await AccessReader.OpenAsync(
            compacted, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        await AssertRowAsync(reopened, updated: true);
    }

    private static async Task AssertRowAsync(AccessReader reader, bool updated)
    {
        int count = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            decimal source = Assert.IsType<decimal>(row[1]);
            decimal times = Assert.IsType<decimal>(row[2]);
            decimal third = Assert.IsType<decimal>(row[3]);
            decimal derived = Assert.IsType<decimal>(row[4]);
            Assert.Equal(updated ? 1.24m : 1.2m, source);
            Assert.Equal(2, (decimal.GetBits(source)[3] >> 16) & 255);
            Assert.Equal(updated ? 12400m : 12000m, times);
            Assert.Equal(2, (decimal.GetBits(times)[3] >> 16) & 255);
            Assert.Equal(updated ? 0.4133333333333333333333333333m : 0.4m, third);
            Assert.Equal(updated ? 28 : 1, (decimal.GetBits(third)[3] >> 16) & 255);
            Assert.Equal(updated ? 4133.3333333333333333333333330m : 4000.0m, derived);
            Assert.Equal(updated ? 25 : 1, (decimal.GetBits(derived)[3] >> 16) & 255);
            Assert.Equal(updated ? 1.24d : 1.2d, row[5]);
            count++;
        }

        Assert.Equal(1, count);
    }
}
