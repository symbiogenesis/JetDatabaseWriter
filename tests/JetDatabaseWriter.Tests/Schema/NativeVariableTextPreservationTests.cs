namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;

/// <summary>Schema and row edits preserve native variable Text storage without normalizing it.</summary>
/// <param name="output">Records native engine diagnostics.</param>
public sealed class NativeVariableTextPreservationTests(ITestOutputHelper output)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets native specimens, mutations and statement modes.</summary>
    public static TheoryData<string, string, WriteMode> Cases()
    {
        var data = new TheoryData<string, string, WriteMode>();
        foreach (string file in new[] { "native-compressed.mdb", "native-compressed-compacted.mdb" })
        {
            foreach (string operation in new[] { "add", "drop", "rename", "index", "update" })
            {
                foreach (WriteMode mode in Enum.GetValues<WriteMode>())
                {
                    data.Add(file, operation, mode);
                }
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task NativeVariableText_UnassignedSlotsRetainExactBytes(string file, string operation, WriteMode mode)
    {
        await using MemoryStream stream = await OpenAsync(file);
        Dictionary<int, byte[]> original = await ReadStorageAsync(stream, "Content");
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
                {
                    switch (operation)
                    {
                        case "add":
                            await writer.AddColumnAsync("Items", new ColumnDefinition("Spare", typeof(int)), Ct);
                            break;
                        case "drop":
                            await writer.DropColumnAsync("Items", "Id", Ct);
                            break;
                        case "rename":
                            await writer.RenameColumnAsync("Items", "Content", "Label", Ct);
                            break;
                        case "index":
                            await writer.CreateIndexAsync("Items", new IndexDefinition("ByContent", ["Content"]), Ct);
                            break;
                        default:
                            Assert.Equal(6, await writer.UpdateRowsAsync("Items", RowCriteria.All(), new RowValues { ["Id"] = 9 }, Ct));
                            break;
                    }
                },
                Ct);
        }

        Dictionary<int, byte[]> actual = await ReadStorageAsync(stream, operation == "rename" ? "Label" : "Content");
        Assert.Equal(original.Count, actual.Count);
        for (int index = 0; index < original.Count; index++)
        {
            Assert.Equal(original[index], actual[index]);
        }
    }

    [Fact]
    public async Task NativeVariableText_ExplicitEqualAssignmentUsesEncoder()
    {
        await using MemoryStream stream = await OpenAsync("native-compressed.mdb");
        Dictionary<int, byte[]> original = await ReadStorageAsync(stream, "Content");
        Assert.Equal(new byte[] { 0xFF, 0xFE, 0x61, 0 }, original[4]);
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("Items", "Id", 4, new Dictionary<string, object?> { ["Content"] = "a" }, Ct));
        }

        Dictionary<int, byte[]> actual = await ReadStorageAsync(stream, "Content");
        Assert.Contains(actual.Values, bytes => bytes.AsSpan().SequenceEqual(new byte[] { 0x61, 0 }));
    }

    [Fact]
    public async Task NativeVariableText_ExplicitRollbackRestoresBytesAndCanRetry()
    {
        await using MemoryStream stream = await OpenAsync("native-compressed.mdb");
        byte[] original = stream.ToArray();
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await using (JetTransaction transaction = await writer.BeginTransactionAsync(Ct))
            {
                await writer.RenameColumnAsync("Items", "Content", "Label", Ct);
                await transaction.RollbackAsync(Ct);
            }

            Assert.Equal(original, stream.ToArray());
            await writer.RenameColumnAsync("Items", "Content", "Label", Ct);
        }

        Dictionary<int, byte[]> actual = await ReadStorageAsync(stream, "Label");
        Assert.Equal(new byte[] { 0xFF, 0xFE, 0x61, 0 }, actual[4]);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipType = typeof(Jet3RoundTripEnvironment),
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable))]
    public async Task NativeVariableText_Dao36AcceptsPreservedSlotsAndCompact()
        => await this.AssertNativeAsync(dao36: true);

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipType = typeof(AccessRoundTripEnvironment),
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable))]
    public async Task NativeVariableText_Dao120AcceptsPreservedSlotsAndCompact()
        => await this.AssertNativeAsync(dao36: false);

    private static async Task<MemoryStream> OpenAsync(string file)
    {
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "NativeTextMarkers", file), Ct);
        var stream = new MemoryStream();
        await stream.WriteAsync(bytes, Ct);
        stream.Position = 0;
        return stream;
    }

    private static async Task<Dictionary<int, byte[]>> ReadStorageAsync(MemoryStream stream, string columnName)
    {
        stream.Position = 0;
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        ResolvedTable table = Assert.IsType<ResolvedTable>(await harness.Services.Snapshots.ResolveTableAsync("Items", Ct));
        TableDef definition = table.Definition;
        ColumnInfo column = Assert.IsType<ColumnInfo>(definition.FindColumn(columnName));
        Assert.False(column.IsFixed);
        var result = new Dictionary<int, byte[]>();
        foreach (LocatedRow row in await harness.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, Ct))
        {
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(row.Location.DataPageNumber, Ct);
            Assert.True(RowDecodePlan.TryParseRowLayout(harness.Database.Format.RowFields, page, row.Location.RowStart, row.Location.RowSize, definition.HasVarColumns, out RowLayout layout));
            ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(harness.Database.Format.RowFields, page, row.Location.RowStart, row.Location.RowSize, layout, column);
            result.Add(result.Count, page.AsSpan(row.Location.RowStart + slice.DataStart, slice.DataLen).ToArray());
        }

        return result;
    }

    private async Task AssertNativeAsync(bool dao36)
    {
        await using MemoryStream stream = await OpenAsync("native-compressed.mdb");
        var expected = new List<string>();
        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct))
        {
            using DataTable rows = await reader.ReadTableAsync("Items", cancellationToken: Ct);
            foreach (DataRow row in rows.Select(string.Empty, "Id ASC"))
            {
                expected.Add(AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(Convert.ToBase64String(Encoding.Unicode.GetBytes(Assert.IsType<string>(row["Content"])))));
            }
        }

        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await writer.AddColumnAsync("Items", new ColumnDefinition("Spare", typeof(int)), Ct);
            await writer.RenameColumnAsync("Items", "Content", "Label", Ct);
            await writer.CreateIndexAsync("Items", new IndexDefinition("ByLabel", "Label"), Ct);
            await writer.UpdateRowsAsync("Items", "Id", 4, new Dictionary<string, object?> { ["Spare"] = 9 }, Ct);
            await writer.DropColumnAsync("Items", "Spare", Ct);
        }

        await using var native = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.VariableTextPreservation", databaseExtension: ".mdb");
        await File.WriteAllBytesAsync(native.SourcePath, stream.ToArray(), Ct);
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.CompactedPath);
        string script = $$"""
            Write-Output "ENGINE=$($engine.Version)"
            $expected=@({{string.Join(',', expected)}})
            function Assert-Values([string]$path) {
                $db=$engine.OpenDatabase($path,$false,$true)
                try {
                    $rs=$db.OpenRecordset('SELECT Id,Label FROM Items ORDER BY Id',2)
                    try {
                        $index=0
                        while(!$rs.EOF) {
                            $value=[Text.Encoding]::Unicode.GetString([Convert]::FromBase64String($expected[$index]))
                            if($rs.Fields.Item('Id').Value -ne $index -or ![string]::Equals([string]$rs.Fields.Item('Label').Value,$value,[StringComparison]::Ordinal)) { throw "Native preserved Text mismatch at $index" }
                            $index++
                            $rs.MoveNext()
                        }
                        if($index -ne $expected.Count) { throw 'Native preserved Text row count changed' }
                    } finally { $rs.Close() }
                } finally { $db.Close() }
            }
            Assert-Values {{source}}
            $engine.CompactDatabase({{source}},{{compacted}})
            Assert-Values {{compacted}}
            """;
        if (dao36)
        {
            JetDatabaseWriter.TestSupport.PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, native.WorkDir, TimeSpan.FromMinutes(2));
            output.WriteLine(result.StandardOutput);
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, result.StandardOutput + result.StandardError);
        }
        else
        {
            AccessRoundTripEnvironment.CompactResult result = native.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
            output.WriteLine(result.StdOut);
            Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
        }
    }
}
