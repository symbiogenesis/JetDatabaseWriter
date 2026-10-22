namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Buffers.Binary;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Fresh Jet3 allocation maps must permit native long-value mutation and allocation.</summary>
public sealed class Jet3NativeExternalMemoTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Both global-map rows have the geometry of the frozen Microsoft-authored Jet3 database.</summary>
    [Fact]
    public void BuildEmptyDatabase_GlobalMapGeometryMatchesFrozenDao36()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet3Mdb);
        byte[] written = TDefPageBuilder.BuildEmptyDatabase(DatabaseFormat.Jet3Mdb).AsSpan(format.PageSize, format.PageSize).ToArray();
        string nativePath = Path.Combine(AppContext.BaseDirectory, "Databases", "Jet3", "NativeDescriptorMetadata.mdb");
        byte[] native = File.ReadAllBytes(nativePath).AsSpan(format.PageSize, format.PageSize).ToArray();
        Assert.Equal((ushort)2, BinaryPrimitives.ReadUInt16LittleEndian(written.AsSpan(format.DataPage.NumRows)));
        for (int index = 0; index < 2; index++)
        {
            Assert.True(UsageMap.TryGetRowBound(native, format.DataPage, format.PageSize, index, out RowBound nativeRow));
            Assert.True(UsageMap.TryGetRowBound(written, format.DataPage, format.PageSize, index, out RowBound writtenRow));
            Assert.Equal(133, nativeRow.RowSize);
            Assert.Equal(nativeRow, writtenRow);
        }
    }

    /// <summary>DAO mutates external Memo values, allocates a table and compacts a fresh managed Jet3 database.</summary>
    /// <param name="length">The external Memo character count.</param>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [InlineData(2000)]
    [InlineData(3000)]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task FreshDatabase_ExternalMemo_Dao36MutatesAllocatesAndCompacts(int length)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.Jet3ExternalMemo", databaseExtension: ".mdb");
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(session.SourcePath, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Memo", typeof(string))], Ct);
            for (int id = 0; id < 3; id++)
            {
                await writer.InsertRowAsync("Items", [id, new string((char)('a' + id), length)], Ct);
            }
        }

        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string script = $$"""
            $db=$engine.OpenDatabase({{source}})
            try {
                $rs=$db.OpenRecordset('SELECT Id,Memo FROM Items ORDER BY Id',4)
                try {
                    for($id=0;$id -lt 3;$id++) {
                        if($rs.EOF -or $rs.Fields.Item('Id').Value -ne $id -or $rs.Fields.Item('Memo').Value -cne (([char](97+$id)).ToString()*{{length}})) { throw 'DAO read the wrong external Memo' }
                        $rs.MoveNext()
                    }
                    if(!$rs.EOF) { throw 'Unexpected native row' }
                } finally { $rs.Close() }
                $db.Execute("UPDATE Items SET Memo='native-update' WHERE Id=0",128)
                $db.Execute('DELETE FROM Items WHERE Id=1',128)
                $db.Execute('CREATE TABLE NativeAllocated (Id LONG)',128)
                $db.Execute('INSERT INTO NativeAllocated VALUES (1)',128)
                $duplicateRejected=$false
                try { $db.Execute("INSERT INTO Items VALUES (0,'duplicate')",128) } catch {
                    $codes=@($engine.Errors | ForEach-Object { $_.Number })
                    if($codes.Count -ne 1 -or $codes[0] -ne 3022) { throw }
                    $duplicateRejected=$true
                }
                if(!$duplicateRejected) { throw 'DAO accepted a duplicate primary key' }
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}},{{compacted}})
            $db=$engine.OpenDatabase({{compacted}})
            try {
                $rs=$db.OpenRecordset('SELECT Id,Memo FROM Items ORDER BY Id',4)
                try {
                    if($rs.EOF -or $rs.Fields.Item('Id').Value -ne 0 -or $rs.Fields.Item('Memo').Value -cne 'native-update') { throw 'DAO compact lost the updated Memo' }
                    $rs.MoveNext()
                    if($rs.EOF -or $rs.Fields.Item('Id').Value -ne 2 -or $rs.Fields.Item('Memo').Value -cne ('c'*{{length}})) { throw 'DAO compact lost the external Memo' }
                    $rs.MoveNext()
                    if(!$rs.EOF) { throw 'DAO compact retained a deleted row' }
                } finally { $rs.Close() }
            } finally { $db.Close() }
            Write-Output 'JET3_EXTERNAL_MEMO_MUTATE_ALLOCATE_COMPACT_OK'
            """;
        PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, $"DAO3.6 external Memo verification failed:\n{result.StandardOutput}\n{result.StandardError}");

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(session.CompactedPath, cancellationToken: Ct);
        using DataTable rows = await reader.ReadTableAsync("Items", cancellationToken: Ct);
        Assert.Equal(2, rows.Rows.Count);
        Assert.Equal("native-update", Assert.Single(rows.AsEnumerable(), row => (int)row["Id"] == 0)["Memo"]);
        Assert.Equal(new string('c', length), Assert.Single(rows.AsEnumerable(), row => (int)row["Id"] == 2)["Memo"]);
        using DataTable allocated = await reader.ReadTableAsync("NativeAllocated", cancellationToken: Ct);
        Assert.Equal(1, Assert.Single(allocated.AsEnumerable())["Id"]);
    }
}
