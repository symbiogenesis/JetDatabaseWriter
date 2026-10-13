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

/// <summary>Qualified reserved names identify fields for evaluation and schema dependencies.</summary>
public sealed class QualifiedReservedFieldTests
{
    [Theory]
    [InlineData("vbTrue")]
    [InlineData("Yes")]
    [InlineData("Null")]
    [InlineData("And")]
    [InlineData("Mod")]
    [InlineData("Date")]
    public async Task QualifiedReservedField_DropRefusesAndRenamePreservesCalculation(string fieldName)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("T", Columns(fieldName), TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [4, DBNull.Value, DBNull.Value, DBNull.Value], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            byte[] before = stream.ToArray();
            await Assert.ThrowsAsync<JetOperationException>(async () => await writer.DropColumnAsync("T", fieldName, TestContext.Current.CancellationToken));
            Assert.Equal(before, stream.ToArray());
            await writer.RenameColumnAsync("T", fieldName, "Cost", TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [3, DBNull.Value, DBNull.Value, DBNull.Value], TestContext.Current.CancellationToken);
            await writer.UpdateRowsAsync("T", "Cost", 4, new Dictionary<string, object?> { ["Cost"] = 5 }, TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        await AssertRenamedCalculationsAsync(reader);
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData("vbTrue")]
    [InlineData("Yes")]
    [InlineData("Null")]
    [InlineData("And")]
    [InlineData("Mod")]
    [InlineData("Date")]
    public async Task NativeQualifiedReservedField_WriterRenamesAndDaoRecalculatesCompacts(string fieldName)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.QualifiedReservedField");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string quotedField = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral("[" + fieldName + "]");
        string expressions = string.Join(',', Array.ConvertAll<string, string>(
            ["T." + fieldName + " * 2", "Other." + fieldName + " * 2", "[T]." + fieldName + " * 2"],
            AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral));
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0')
            try {
                $db.Execute('CREATE TABLE [T] (' + {{quotedField}} + ' LONG)')
                $db.TableDefs.Refresh()
                $tdf = $db.TableDefs('T')
                $expressions = @({{expressions}})
                for ($i = 0; $i -lt $expressions.Count; $i++) {
                    $field = $tdf.CreateField('Calculated' + $i, 4)
                    $field.Properties('Expression').Value = $expressions[$i]
                    $tdf.Fields.Append($field)
                }
                $db.Execute('INSERT INTO [T] (' + {{quotedField}} + ') VALUES (4)', 128)
                $rs = $db.OpenRecordset('SELECT [Calculated0],[Calculated1],[Calculated2] FROM [T]')
                try {
                    for ($i = 0; $i -lt 3; $i++) { Write-Output ('BEFORE' + $i + '=' + [string]$rs.Fields($i).Value) }
                } finally { $rs.Close() }
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, $"DAO failed: {created.StdOut}\n{created.StdErr}");
        Assert.Contains("BEFORE0=8", created.StdOut, StringComparison.Ordinal);
        Assert.Contains("BEFORE1=8", created.StdOut, StringComparison.Ordinal);
        Assert.Contains("BEFORE2=8", created.StdOut, StringComparison.Ordinal);
        byte[] before = await File.ReadAllBytesAsync(session.SourcePath, TestContext.Current.CancellationToken);
        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            await Assert.ThrowsAsync<JetOperationException>(async () => await writer.DropColumnAsync("T", fieldName, TestContext.Current.CancellationToken));
        }

        Assert.Equal(before, await File.ReadAllBytesAsync(session.SourcePath, TestContext.Current.CancellationToken));
        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            await writer.RenameColumnAsync("T", fieldName, "Cost", TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [3, DBNull.Value, DBNull.Value, DBNull.Value], TestContext.Current.CancellationToken);
            await writer.UpdateRowsAsync("T", "Cost", 4, new Dictionary<string, object?> { ["Cost"] = 5 }, TestContext.Current.CancellationToken);
        }

        AccessRoundTripEnvironment.CompactResult updated = session.RunDaoDatabaseScriptThenCompact(
            """
            $db.Execute('UPDATE [T] SET [Cost]=6 WHERE [Cost]=5', 128)
            $rs = $db.OpenRecordset('SELECT [Cost],[Calculated0],[Calculated1],[Calculated2] FROM [T] ORDER BY [Cost]')
            try {
                while (!$rs.EOF) {
                    for ($i = 1; $i -lt 4; $i++) {
                        if ($rs.Fields($i).Value -ne $rs.Fields(0).Value * 2) { throw 'Qualified reserved-field calculation diverged.' }
                    }
                    Write-Output ('ROW=' + [string]$rs.Fields(0).Value)
                    $rs.MoveNext()
                }
            } finally { $rs.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(updated.ExitCode == 0, $"DAO failed: {updated.StdOut}\n{updated.StdErr}");
        Assert.Contains("ROW=3", updated.StdOut, StringComparison.Ordinal);
        Assert.Contains("ROW=6", updated.StdOut, StringComparison.Ordinal);
        await using AccessReader reader = await AccessReader.OpenAsync(
            session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        await AssertRenamedCalculationsAsync(reader);
    }

    private static ColumnDefinition[] Columns(string fieldName) =>
    [
        new(fieldName, typeof(int)),
        new("Calculated0", typeof(int)) { IsCalculated = true, CalculationExpression = "T." + fieldName + " * 2" },
        new("Calculated1", typeof(int)) { IsCalculated = true, CalculationExpression = "Other." + fieldName + " * 2" },
        new("Calculated2", typeof(int)) { IsCalculated = true, CalculationExpression = "[T]." + fieldName + " * 2" },
    ];

    private static async Task AssertRenamedCalculationsAsync(AccessReader reader)
    {
        IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync("T", TestContext.Current.CancellationToken);
        Assert.Equal("Cost", columns[0].Name);
        Assert.Equal("T.[Cost] * 2", columns[1].CalculationExpression?.TrimEnd('\0'));
        Assert.Equal("Other.[Cost] * 2", columns[2].CalculationExpression?.TrimEnd('\0'));
        Assert.Equal("[T].[Cost] * 2", columns[3].CalculationExpression?.TrimEnd('\0'));
        int count = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            int source = Assert.IsType<int>(row[0]);
            Assert.Equal(source * 2, row[1]);
            Assert.Equal(source * 2, row[2]);
            Assert.Equal(source * 2, row[3]);
            count++;
        }

        Assert.Equal(2, count);
    }
}
