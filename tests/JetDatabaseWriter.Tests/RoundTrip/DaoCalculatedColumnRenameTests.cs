namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Checks native calculated-field behavior and the writer's preservation of valid dependencies.
/// </summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoCalculatedColumnRenameTests
{
    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(3);

    /// <summary>
    /// DAO leaves unresolved dependencies after rename and deletion. The writer
    /// rewrites dependencies on rename and refuses a drop that would invalidate them.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task DaoRenameOrDeleteOfFieldUsedByCalculatedColumn_WriterPreservesValidDependencies()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CalculatedColumnRename");
        string dbPath = session.CreateDatabasePath("calc_rename");
        string dbLiteral = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(dbPath);

        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{dbLiteral}}, ';LANGID=0x0409;CP=1252;COUNTRY=0')
            try {
                $db.Execute('CREATE TABLE [CalcRename] ([Id] LONG, [Score] LONG)')
                $tdf = $db.TableDefs('CalcRename')
                $field = $tdf.CreateField('Doubled', 4)
                $field.Expression = '[Score]*2'
                $tdf.Fields.Append($field)
                $field = $null

                $db.Execute('CREATE TABLE [WriterCalc] ([Id] LONG, [Score] LONG)')
                $db.TableDefs.Refresh()
                $writerTdf = $db.TableDefs('WriterCalc')
                $writerField = $writerTdf.CreateField('Doubled', 4)
                $writerField.Expression = '[Score]*2'
                $writerTdf.Fields.Append($writerField)
                $writerField = $null
                $writerTdf = $null

                $sourceName = 'Score'
                try {
                    $tdf.Fields('Score').Name = 'Points'
                    $sourceName = 'Points'
                    $db.TableDefs.Refresh()
                    $tdf = $db.TableDefs('CalcRename')
                    Write-Output "RENAME_OK EXPR=$($tdf.Fields('Doubled').Expression)"
                } catch {
                    Write-Output "RENAME_REJECTED=$($_.Exception.Message)"
                }

                try {
                    $tdf.Fields.Delete($sourceName)
                    Write-Output 'DELETE_OK'
                } catch {
                    Write-Output "DELETE_REJECTED=$($_.Exception.Message)"
                }

                $tdf = $null
            } finally {
                if ($db -ne $null) { try { $db.Close() } catch {} }
            }
            """,
            DaoTimeout);

        Assert.True(result.ExitCode == 0, $"DAO script failed (exit={result.ExitCode}).\nstdout: {result.StdOut}\nstderr: {result.StdErr}");
        Assert.Contains("RENAME_OK EXPR=[Score]*2", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("DELETE_OK", result.StdOut, StringComparison.Ordinal);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(dbPath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            await writer.RenameColumnAsync("WriterCalc", "Score", "Points", TestContext.Current.CancellationToken);
        }

        byte[] before = await File.ReadAllBytesAsync(dbPath, TestContext.Current.CancellationToken);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(dbPath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            JetOperationException failure = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.DropColumnAsync("WriterCalc", "Points", TestContext.Current.CancellationToken));
            Assert.Equal(JetErrorCode.ColumnReferencedByExpression, failure.ErrorCode);
        }

        Assert.Equal(before, await File.ReadAllBytesAsync(dbPath, TestContext.Current.CancellationToken));
        await using AccessReader reader = await AccessReader.OpenAsync(dbPath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        Assert.Equal("[Points]*2", Assert.Single(await reader.GetColumnMetadataAsync("WriterCalc", TestContext.Current.CancellationToken), column => column.Name == "Doubled").CalculationExpression);
    }

    /// <summary>
    /// The writer renames a field of the Access-authored calcFieldTestV2010
    /// table that MonthlySalary uses. DAO then opens the table, reads the
    /// rewritten expression, edits the renamed field so Access recomputes
    /// MonthlySalary through it, and compacts the file, which the reader opens.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task WriterRenamedCalculatedColumn_OpensEvaluatesAndCompactsInDao()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CalculatedColumnRename");
        await using (FileStream source = File.OpenRead(TestDatabases.CalcFieldTestV2010))
        await using (FileStream destination = File.Create(session.SourcePath))
        {
            await source.CopyToAsync(destination, TestContext.Current.CancellationToken);
        }

        File.SetAttributes(session.SourcePath, File.GetAttributes(session.SourcePath) & ~FileAttributes.ReadOnly);
        const string originalScript =
            """
            $rs = $db.OpenRecordset('Table1')
            try {
                $iterated = 0
                while (!$rs.EOF) { $iterated++; $rs.MoveNext() }
                Write-Output "ITERATED=$iterated"
                Write-Output "DECLARED=$($rs.RecordCount)"
            } finally { $rs.Close() }
            """;
        AccessRoundTripEnvironment.CompactResult original = session.RunDaoDatabaseScript(session.SourcePath, originalScript, DaoTimeout);
        Assert.True(original.ExitCode == 0, $"DAO original read failed: {original.StdOut}\n{original.StdErr}");
        Assert.Contains("ITERATED=4", original.StdOut, StringComparison.Ordinal);
        Assert.Contains("DECLARED=3", original.StdOut, StringComparison.Ordinal);

        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            await writer.RenameColumnAsync("Table1", "Salary", "Pay", TestContext.Current.CancellationToken);
        }

        const string script =
            """
            $tdf = $db.TableDefs('Table1')
            Write-Output "EXPR=$($tdf.Fields('MonthlySalary').Expression)"
            $tdf = $null
            $rs = $db.OpenRecordset('SELECT * FROM [Table1] WHERE [ID] = 1', 2)
            try {
                $rs.Edit()
                $rs.Fields('Pay').Value = 1200
                $rs.Update()
                $rs.Requery()
                Write-Output "EDITED_MONTHLY=$($rs.Fields('MonthlySalary').Value)"
            } finally {
                $rs.Close()
            }
            """;

        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScriptThenCompact(script, DaoTimeout);
        Assert.True(
            result.ExitCode == 0 && File.Exists(session.CompactedPath),
            $"DAO edit and CompactDatabase failed (exit={result.ExitCode}).\nstdout: {result.StdOut}\nstderr: {result.StdErr}");
        Assert.Contains("EXPR=[Pay]/12", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("EDITED_MONTHLY=100", result.StdOut, StringComparison.Ordinal);

        await using AccessReader reader = await AccessReader.OpenAsync(
            session.CompactedPath,
            new AccessReaderOptions { UseLockFile = false },
            TestContext.Current.CancellationToken);
        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("Table1", TestContext.Current.CancellationToken);
        Assert.Equal("[Pay]/12", Assert.Single(meta, c => c.Name == "MonthlySalary").CalculationExpression);
        Assert.Equal("[Pay]>100000", Assert.Single(meta, c => c.Name == "IsRich").CalculationExpression);

        DataTable rows = await reader.ReadTableAsync("Table1", cancellationToken: TestContext.Current.CancellationToken);
        DataRow edited = Assert.Single(rows.AsEnumerable(), r => Convert.ToInt32(r["ID"], CultureInfo.InvariantCulture) == 1);
        Assert.Equal(100m, Convert.ToDecimal(edited["MonthlySalary"], CultureInfo.InvariantCulture));
        Assert.Equal(4, rows.Rows.Count);
        Assert.All(rows.AsEnumerable().Where(r => r["Pay"] is not DBNull), r => Assert.Equal(
            Math.Round(Convert.ToDecimal(r["Pay"], CultureInfo.InvariantCulture) / 12m, 4),
            Math.Round(Convert.ToDecimal(r["MonthlySalary"], CultureInfo.InvariantCulture), 4)));
    }
}
