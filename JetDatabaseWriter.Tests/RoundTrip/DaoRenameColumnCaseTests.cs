namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Pins <c>RenameColumnAsync</c>'s handling of a rename that changes only the
/// letter case to what Microsoft Access does: the rename is allowed, a rename
/// to the name the field already has changes nothing, and a rename onto another
/// field's name in another case is refused. None of this has been checked
/// against Access on this project's build hosts, which have no Access install,
/// so these tests skip there.
/// </summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoRenameColumnCaseTests
{
    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(3);

    /// <summary>
    /// DAO renames a field Name to NAME, then to NAME again, then onto the
    /// Id field's name in lower case. The writer assumes Access allows the
    /// first, treats the second as no change, and refuses the third; if Access
    /// does otherwise, the duplicate-name check in
    /// <c>TableSchemaEditor.RenameColumnAsync</c> needs revisiting.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task DaoCaseOnlyFieldRename_MatchesWriterBehaviour()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.RenameColumnCase");
        string dbPath = session.CreateDatabasePath("case_rename");
        string dbLiteral = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(dbPath);

        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{dbLiteral}}, ';LANGID=0x0409;CP=1252;COUNTRY=0')
            try {
                $db.Execute('CREATE TABLE [CaseRename] ([Id] LONG, [Name] TEXT(20))')
                $tdf = $db.TableDefs('CaseRename')

                try {
                    $tdf.Fields('Name').Name = 'NAME'
                    $db.TableDefs.Refresh()
                    $tdf = $db.TableDefs('CaseRename')
                    $names = for ($i = 0; $i -lt $tdf.Fields.Count; $i++) { $tdf.Fields($i).Name }
                    Write-Output "RENAME_OK FIELDS=$($names -join ',')"
                } catch {
                    Write-Output "RENAME_REJECTED=$($_.Exception.Message)"
                }

                try {
                    $tdf.Fields('NAME').Name = 'NAME'
                    Write-Output 'SAME_OK'
                } catch {
                    Write-Output "SAME_REJECTED=$($_.Exception.Message)"
                }

                try {
                    $tdf.Fields('NAME').Name = 'id'
                    Write-Output 'DUPLICATE_OK'
                } catch {
                    Write-Output "DUPLICATE_REJECTED=$($_.Exception.Message)"
                }

                $tdf = $null
            } finally {
                if ($db -ne $null) { try { $db.Close() } catch {} }
            }
            """,
            DaoTimeout);

        Assert.True(result.ExitCode == 0, $"DAO script failed (exit={result.ExitCode}).\nstdout: {result.StdOut}\nstderr: {result.StdErr}");
        Assert.True(
            result.StdOut.Contains("RENAME_OK FIELDS=Id,NAME", StringComparison.Ordinal),
            $"Access did not rename Name to NAME the way RenameColumnAsync does; revisit the case-only rename.\nstdout: {result.StdOut}");
        Assert.True(
            result.StdOut.Contains("SAME_OK", StringComparison.Ordinal),
            $"Access refused a rename to the name the field already has, which RenameColumnAsync treats as no change; revisit the case-only rename.\nstdout: {result.StdOut}");
        Assert.True(
            result.StdOut.Contains("DUPLICATE_REJECTED=", StringComparison.Ordinal),
            $"Access renamed a field onto another field's name in another case, which RenameColumnAsync refuses; revisit the duplicate-name check.\nstdout: {result.StdOut}");
    }

    /// <summary>
    /// The writer renames the Title and Attachments columns of the
    /// Access-authored ComplexFields table by case only and attaches a file
    /// through the new spelling. DAO then reads the field names, opens the
    /// attachment field's child recordset under its new name and compacts the
    /// file, and the reader finds the new spelling in the compacted column
    /// descriptors and in <c>MSysComplexColumns.ColumnName</c>.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task WriterCaseOnlyRename_OpensReadsAndCompactsInDao()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.RenameColumnCase");
        await using (FileStream source = File.OpenRead(TestDatabases.ComplexFields))
        await using (FileStream destination = File.Create(session.SourcePath))
        {
            await source.CopyToAsync(destination, TestContext.Current.CancellationToken);
        }

        File.SetAttributes(session.SourcePath, File.GetAttributes(session.SourcePath) & ~FileAttributes.ReadOnly);

        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            await writer.RenameColumnAsync("Documents", "Title", "TITLE", TestContext.Current.CancellationToken);
            await writer.RenameColumnAsync("Documents", "Attachments", "ATTACHMENTS", TestContext.Current.CancellationToken);
            await writer.AddAttachmentAsync(
                "Documents",
                "ATTACHMENTS",
                new Dictionary<string, object?> { ["ID"] = 1 },
                new AttachmentInput("case-rename.jpg", Encoding.ASCII.GetBytes("case-only rename")),
                TestContext.Current.CancellationToken);
        }

        const string script =
            """
            $tdf = $db.TableDefs('Documents')
            $names = for ($i = 0; $i -lt $tdf.Fields.Count; $i++) { $tdf.Fields($i).Name }
            Write-Output "FIELDS=$($names -join ',')"
            $tdf = $null
            $rs = $db.OpenRecordset('SELECT [ID], [TITLE], [ATTACHMENTS] FROM [Documents] WHERE [ID] = 1', 2)
            try {
                $files = $rs.Fields('ATTACHMENTS').Value
                try {
                    $fileNames = while (-not $files.EOF) { $files.Fields('FileName').Value; $files.MoveNext() }
                    Write-Output "FILES=$(($fileNames | Sort-Object) -join ',')"
                } finally {
                    $files.Close()
                }
            } finally {
                $rs.Close()
            }
            """;

        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScriptThenCompact(script, DaoTimeout);
        Assert.True(
            result.ExitCode == 0 && File.Exists(session.CompactedPath),
            $"DAO read and CompactDatabase failed (exit={result.ExitCode}).\nstdout: {result.StdOut}\nstderr: {result.StdErr}");
        Assert.Contains("FIELDS=ID,TITLE,ATTACHMENTS", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("case-rename.jpg", result.StdOut, StringComparison.Ordinal);

        await using AccessReader reader = await AccessReader.OpenAsync(
            session.CompactedPath,
            new AccessReaderOptions { UseLockFile = false },
            TestContext.Current.CancellationToken);
        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("Documents", TestContext.Current.CancellationToken);
        Assert.Equal(["ID", "TITLE", "ATTACHMENTS"], meta.Select(c => c.Name));

        ComplexColumnInfo complex = Assert.Single(await reader.GetComplexColumnsAsync("Documents", TestContext.Current.CancellationToken));
        Assert.Equal("ATTACHMENTS", complex.ColumnName);

        DataTable complexColumns = await reader.ReadTableAsync("MSysComplexColumns", cancellationToken: TestContext.Current.CancellationToken);
        DataRow row = Assert.Single(
            complexColumns.AsEnumerable(),
            r => string.Equals(Convert.ToString(r["ColumnName"], CultureInfo.InvariantCulture), "Attachments", StringComparison.OrdinalIgnoreCase));
        Assert.Equal("ATTACHMENTS", row["ColumnName"]);

        IReadOnlyList<AttachmentRecord> attachments = await reader.GetAttachmentsAsync("Documents", "ATTACHMENTS", TestContext.Current.CancellationToken);
        Assert.Contains(attachments, a => string.Equals(a.FileName, "case-rename.jpg", StringComparison.Ordinal));
    }
}
