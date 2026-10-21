namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Microsoft Access accepts preserved native schema identities and multi-page complex rewrites.</summary>
public sealed class NativeSchemaDaoTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData("add")]
    [InlineData("drop")]
    [InlineData("rename")]
    public async Task NativeTable4_RewriteSurvivesDaoOpenAndCompact(string operation)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativeSchema");
        await File.WriteAllBytesAsync(session.SourcePath, await File.ReadAllBytesAsync(TestDatabases.TestV2010, Ct), Ct);
        string firstColumn;
        int expectedRows;
        await using (AccessReader reader = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            firstColumn = (await reader.GetColumnMetadataAsync("Table4", Ct))[0].Name;
            using DataTable rows = await reader.ReadTableAsync("Table4", cancellationToken: Ct);
            expectedRows = rows.Rows.Count;
        }

        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await RewriteAsync(writer, "Table4", firstColumn, operation);
        }

        int expectedColumns = operation switch { "add" => 3, "drop" => 1, _ => 2 };
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScriptThenCompact(
            $$"""
            if ($db.TableDefs('Table4').Fields.Count -ne {{expectedColumns}}) { throw 'Wrong column count.' }
            $rs = $db.OpenRecordset('Table4')
            try {
                if (!$rs.EOF) { $rs.MoveLast() }
                if ($rs.RecordCount -ne {{expectedRows}}) { throw 'Wrong row count.' }
            } finally { $rs.Close() }
            Write-Output 'NATIVE_READ_OK'
            """,
            TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, $"DAO failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains("NATIVE_READ_OK", result.StdOut, StringComparison.Ordinal);
        await using AccessReader compacted = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct);
        Assert.Equal(expectedColumns, (await compacted.GetColumnMetadataAsync("Table4", Ct)).Count);
        using DataTable compactedRows = await compacted.ReadTableAsync("Table4", cancellationToken: Ct);
        Assert.Equal(expectedRows, compactedRows.Rows.Count);
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData("add")]
    [InlineData("drop")]
    [InlineData("rename")]
    public async Task DaoAuthoredWideAttachment_RewriteSurvivesNativeMutationAndCompact(string operation)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativeWideSchema");
        string payloadPath = Path.Combine(session.WorkDir, "keep.txt");
        string extractedPath = Path.Combine(session.WorkDir, "extracted.txt");
        await File.WriteAllBytesAsync(payloadPath, [1, 2, 3], Ct);
        string quotedPayload = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(payloadPath);
        string createScript = $$"""
            $table = $db.CreateTableDef('Documents')
            $table.Fields.Append($table.CreateField('Id', 4))
            $table.Fields.Append($table.CreateField('Files', 101))
            for ($i = 0; $i -lt 200; $i++) { $table.Fields.Append($table.CreateField(('C' + $i), 4)) }
            $db.TableDefs.Append($table)
            $index = $table.CreateIndex('PrimaryKey')
            $index.Primary = $true
            $index.Fields.Append($index.CreateField('Id'))
            $table.Indexes.Append($index)
            $db.Execute('INSERT INTO [Documents] ([Id],[C0],[C1]) VALUES (1,10,20)', 128)
            $rs = $db.OpenRecordset('Documents')
            try {
                $rs.Edit()
                $attachments = $rs.Fields('Files').Value
                try {
                    $attachments.AddNew()
                    $attachments.Fields('FileData').LoadFromFile({{quotedPayload}})
                    $attachments.Update()
                } finally { $attachments.Close() }
                $rs.Update()
            } finally { $rs.Close() }
            """;
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoCreateDatabaseScript(
            session.SourcePath,
            ";LANGID=0x0409;CP=1252;COUNTRY=0",
            createScript,
            TimeSpan.FromMinutes(2));
        Assert.True(created.ExitCode == 0, $"DAO create failed: {created.StdOut}\n{created.StdErr}");
        await using (AccessWriter harness = await TestDatabase.OpenWriterAsync(session.SourcePath, cancellationToken: Ct))
        {
            long root = Assert.Single(await harness.Services.Catalog.GetUserTablesAsync(Ct), entry => entry.Name == "Documents").TDefPage;
            JetDatabaseWriter.Schema.LogicalTDefChain chain = await JetDatabaseWriter.Schema.LogicalTDefChain.ReadRequiredAsync(root, harness.Database.Format.PageSize, harness.Pager.ReadPageAsync, JetDatabaseWriter.Pages.Paging.PageBuffers.Return, true, Ct);
            Assert.True(chain.PageNumbers.Count > 1);
        }

        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await RewriteAsync(writer, "Documents", "C0", operation);
        }

        string quotedExtracted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(extractedPath);
        int expectedColumns = operation switch { "add" => 203, "drop" => 201, _ => 202 };
        AccessRoundTripEnvironment.CompactResult checkedResult = session.RunDaoDatabaseScriptThenCompact(
            $$"""
            if ($db.TableDefs('Documents').Fields.Count -ne {{expectedColumns}}) { throw 'Wrong column count.' }
            $rs = $db.OpenRecordset('Documents')
            try {
                if ($rs.Fields('C1').Value -ne 20) { throw 'Lost surviving value.' }
                $attachments = $rs.Fields('Files').Value
                try {
                    if ($attachments.EOF) { throw 'Lost attachment.' }
                    $attachments.Fields('FileData').SaveToFile({{quotedExtracted}})
                } finally { $attachments.Close() }
            } finally { $rs.Close() }
            $db.Execute('INSERT INTO [Documents] ([Id],[C1]) VALUES (2,99)', 128)
            $db.Execute('UPDATE [Documents] SET [C1]=77 WHERE [Id]=2', 128)
            $db.Execute('DELETE FROM [Documents] WHERE [Id]=2', 128)
            Write-Output 'NATIVE_MUTATION_OK'
            """,
            TimeSpan.FromMinutes(2));
        Assert.True(checkedResult.ExitCode == 0, $"DAO failed: {checkedResult.StdOut}\n{checkedResult.StdErr}");
        Assert.Contains("NATIVE_MUTATION_OK", checkedResult.StdOut, StringComparison.Ordinal);
        Assert.Equal([1, 2, 3], await File.ReadAllBytesAsync(extractedPath, Ct));
        await using AccessReader compacted = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct);
        Assert.Equal(expectedColumns, (await compacted.GetColumnMetadataAsync("Documents", Ct)).Count);
        Assert.Equal([1, 2, 3], Assert.Single(await compacted.GetAttachmentsAsync("Documents", "Files", Ct)).FileData);
        using DataTable compactedRows = await compacted.ReadTableAsync("Documents", cancellationToken: Ct);
        Assert.Equal(20, Assert.Single(compactedRows.Rows.Cast<DataRow>())["C1"]);
        Assert.Single(await compacted.ListIndexesAsync("Documents", Ct), index => index.Name == "PrimaryKey");
    }

    private static ValueTask RewriteAsync(AccessWriter writer, string table, string column, string operation) => operation switch
    {
        "add" => writer.AddColumnAsync(table, new ColumnDefinition("Added", typeof(int)), Ct),
        "drop" => writer.DropColumnAsync(table, column, Ct),
        _ => writer.RenameColumnAsync(table, column, "Renamed", Ct),
    };
}
