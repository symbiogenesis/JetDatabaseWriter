namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Microsoft-created data pages establish free-map hints and deletion/reuse behavior.</summary>
public sealed class NativeDataPageFreeSpaceTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>DAO3.6 establishes native Jet3 deletion packing and accepts managed reuse through compact.</summary>
    [Fact(Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason, SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable), SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public Task Jet3_FreeMapDeletionAndReuse_MatchDao36() => VerifyAsync(DatabaseFormat.Jet3Mdb);

    /// <summary>DAO120 establishes the same lifecycle for Jet4 and ACE.</summary>
    /// <param name="format">The native database format.</param>
    [Theory(Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason, SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable), SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public Task Jet4AndAce_FreeMapDeletionAndReuse_MatchDao120(DatabaseFormat format) => VerifyAsync(format);

    private static async Task VerifyAsync(DatabaseFormat format)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativeFreeSpace", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        int version = format switch { DatabaseFormat.Jet3Mdb => 32, DatabaseFormat.Jet4Mdb => 64, DatabaseFormat.AceAccdb => 128, _ => throw new ArgumentOutOfRangeException(nameof(format)) };
        string createScript = $$"""
            $db=$engine.CreateDatabase({{path}},';LANGID=0x0409;CP=1252;COUNTRY=0',{{version}})
            try {
                $db.Execute('CREATE TABLE Items (Id LONG CONSTRAINT PK_Items PRIMARY KEY, Pad BINARY(200))',128)
                $rs=$db.OpenRecordset('Items',2)
                try {
                    foreach($id in 1..25) {
                        $rs.AddNew()
                        $rs.Fields.Item('Id').Value=$id
                        $rs.Fields.Item('Pad').Value=[byte[]](1..200)
                        $rs.Update()
                    }
                } finally { $rs.Close() }
            } finally { $db.Close() }
            """;
        RunNative(format, session, createScript);
        PageState original = await ReadFirstPageAsync(session.SourcePath);
        Assert.False(original.MarkedFree);
        string deleteScript = $$"""
            $db=$engine.OpenDatabase({{path}})
            try { $db.Execute('DELETE FROM Items WHERE Id=1',128) } finally { $db.Close() }
            """;
        RunNative(format, session, deleteScript);
        PageState deleted = await ReadFirstPageAsync(session.SourcePath);
        Assert.Equal(original.PageNumber, deleted.PageNumber);
        Assert.Equal(original.Slots, deleted.Slots);
        Assert.True(deleted.FreeBytes > original.FreeBytes);
        Assert.True(deleted.MarkedFree);

        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(session.SourcePath, cancellationToken: Ct))
        {
            Assert.Equal(1, await writer.DeleteRowsAsync("Items", "Id", 2, Ct));
            await writer.InsertRowAsync("Items", [26, new byte[200]], Ct);
        }

        PageState reused = await ReadFirstPageAsync(session.SourcePath);
        Assert.Equal(original.PageNumber, reused.PageNumber);
        Assert.Equal(original.Slots + 1, reused.Slots);
        string compact = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string compactScript = $$"""
            $db=$engine.OpenDatabase({{path}})
            try {
                $rs=$db.OpenRecordset('SELECT COUNT(*) AS N FROM Items',4)
                try { if($rs.Fields.Item(0).Value -ne 24) { throw 'Managed reuse lost rows' } } finally { $rs.Close() }
                $rejected=$false
                try { $db.Execute('INSERT INTO Items(Id) VALUES (26)',128) } catch { $rejected=$true }
                if(!$rejected) { throw 'Managed reuse lost primary-key enforcement' }
                $db.Execute('UPDATE Items SET Id=27 WHERE Id=26',128)
                $db.Execute('DELETE FROM Items WHERE Id=3',128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{path}},{{compact}})
            $db=$engine.OpenDatabase({{compact}})
            try {
                $rs=$db.OpenRecordset('SELECT COUNT(*) AS N FROM Items',4)
                try { if($rs.Fields.Item(0).Value -ne 23) { throw 'Compact lost surviving rows' } } finally { $rs.Close() }
            } finally { $db.Close() }
            """;
        RunNative(format, session, compactScript);
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(session.CompactedPath, cancellationToken: Ct);
        var ids = new HashSet<int>();
        await foreach (object[] row in reader.Rows("Items", cancellationToken: Ct))
        {
            Assert.True(ids.Add((int)row[0]));
        }

        Assert.Equal(23, ids.Count);
        Assert.Contains(27, ids);
        Assert.DoesNotContain(1, ids);
        Assert.DoesNotContain(2, ids);
        Assert.DoesNotContain(3, ids);
    }

    private static async Task<PageState> ReadFirstPageAsync(string path)
    {
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(path, cancellationToken: Ct);
        CatalogEntry? entry = await reader.GetCatalogEntryAsync("Items", Ct);
        Assert.NotNull(entry);
        DatabaseFile database = reader.Database;
        long number = (await database.OwnedPages.GetOwnedDataPagesAsync(entry.TDefPage, Ct))[0];
        byte[] page = await database.Pages.ReadPageCopyAsync(number, Ct);
        byte[] tdef = await database.Pages.ReadPageCopyAsync(entry.TDefPage, Ct);
        byte[] map = await database.Pages.ReadPageCopyAsync(UsageMap.ReadUInt24(tdef, database.Format.TDef.FreePagesPage), Ct);
        Assert.True(UsageMap.TryGetRowBound(map, database.Format.DataPage, database.Format.PageSize, tdef[database.Format.TDef.FreePages], out RowBound bound));
        var free = new List<long>();
        Assert.True(await UsageMap.TryEnumeratePagesAsync(map, bound, database.Format.PageSize, database.Pages.PageCount, 1, true, database.Pages.ReadPageAsync, PageBuffers.Return, free, Ct));
        return new PageState(number, Ru16(page, 2), Ru16(page, database.Format.DataPage.NumRows), free.Contains(number));
    }

    private static void RunNative(DatabaseFormat format, AccessRoundTripSession session, string script)
    {
        if (format == DatabaseFormat.Jet3Mdb)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, result.StandardOutput + result.StandardError);
        }
        else
        {
            AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
        }
    }

    private readonly record struct PageState(long PageNumber, int FreeBytes, int Slots, bool MarkedFree);
}
