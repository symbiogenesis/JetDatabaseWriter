namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Pins the writer's handling of a field that a calculated column uses to
/// what Microsoft Access does: <c>RenameColumnAsync</c> rewrites the
/// expression to the new name and <c>DropColumnAsync</c> refuses the drop.
/// Neither choice has been checked against Access on this project's build
/// hosts, which have no Access install, so these tests skip there.
/// </summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoCalculatedColumnRenameTests
{
    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(3);

    /// <summary>
    /// DAO renames, then deletes, a field a calculated column uses. The writer
    /// assumes Access rewrites the expression on the rename and refuses the
    /// delete; if Access does otherwise, the rename-expressions design in
    /// <c>TableSchemaEditor.ProjectExpressionReferences</c> needs revisiting.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task DaoRenameOrDeleteOfFieldUsedByCalculatedColumn_MatchesWriterBehaviour()
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
        Assert.True(
            result.StdOut.Contains("RENAME_OK EXPR=[Points]*2", StringComparison.Ordinal),
            $"Access did not rename the field inside the calculated expression the way RenameColumnAsync does; revisit the rename-expressions design.\nstdout: {result.StdOut}");
        Assert.True(
            result.StdOut.Contains("DELETE_REJECTED=", StringComparison.Ordinal),
            $"Access deleted a field a calculated column uses, which DropColumnAsync refuses; revisit the rename-expressions design.\nstdout: {result.StdOut}");
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

        DataTable rows = await reader.ReadDataTableAsync("Table1", cancellationToken: TestContext.Current.CancellationToken);
        DataRow edited = Assert.Single(rows.AsEnumerable(), r => Convert.ToInt32(r["ID"], CultureInfo.InvariantCulture) == 1);
        Assert.Equal(100m, Convert.ToDecimal(edited["MonthlySalary"], CultureInfo.InvariantCulture));
        Assert.Equal(4, rows.Rows.Count);
        Assert.All(rows.AsEnumerable().Where(r => r["Pay"] is not DBNull), r => Assert.Equal(
            Math.Round(Convert.ToDecimal(r["Pay"], CultureInfo.InvariantCulture) / 12m, 4),
            Math.Round(Convert.ToDecimal(r["MonthlySalary"], CultureInfo.InvariantCulture), 4)));
    }
}
