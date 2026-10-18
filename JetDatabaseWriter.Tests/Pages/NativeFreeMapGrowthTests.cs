namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Collections.Generic;
using System.IO;
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

/// <summary>DAO accepts native free-map promotion when reusable pages span multiple INLINE windows.</summary>
public sealed class NativeFreeMapGrowthTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact(Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason, SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable), SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public Task Jet3_FreeMapReferenceGrowth_PreservesNativeRowsAndConstraints() => VerifyAsync(DatabaseFormat.Jet3Mdb);

    [Theory(Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason, SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable), SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public Task Jet4AndAce_FreeMapReferenceGrowth_PreservesNativeRowsAndConstraints(DatabaseFormat format) => VerifyAsync(format);

    private static async Task VerifyAsync(DatabaseFormat format)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativeFreeMapGrowth", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        int version = format switch
        {
            DatabaseFormat.Jet3Mdb => 32,
            DatabaseFormat.Jet4Mdb => 64,
            DatabaseFormat.AceAccdb => 128,
            _ => throw new ArgumentOutOfRangeException(nameof(format)),
        };
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
        long originalPage;
        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(session.SourcePath, cancellationToken: Ct))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await reader.GetCatalogEntryAsync("Items", Ct));
            originalPage = (await reader.Database.OwnedPages.GetOwnedDataPagesAsync(entry.TDefPage, Ct))[0];
        }

        // This exercises page-map layout and native interoperability. Keep the
        // managed mutation in memory so the filler does not measure durable
        // rollback-journal flushes for every captured page.
        await using var image = new MemoryStream();
        await image.WriteAsync(await File.ReadAllBytesAsync(session.SourcePath, Ct), Ct);
        image.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(image, cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                "Filler",
                [new("B1", typeof(byte[]), 255), new("B2", typeof(byte[]), 255), new("B3", typeof(byte[]), 255), new("B4", typeof(byte[]), 255), new("B5", typeof(byte[]), 255), new("B6", typeof(byte[]), 255)],
                Ct);
            _ = await writer.InsertRowsAsync("Filler", FillerRows(), Ct);
            _ = await writer.InsertRowsAsync("Items", ItemRows(), Ct);
            Assert.Equal(1, await writer.DeleteRowsAsync("Items", "Id", 1, Ct));
            Assert.Equal(1, await writer.DeleteRowsAsync("Items", "Id", 2, Ct));
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await writer.Services.Catalog.GetCatalogEntryAsync("Items", Ct));
            MapState map = await ReadFreeMapAsync(writer.Database, entry.TDefPage);
            Assert.Equal(Constants.UsageMap.ReferenceMapType, map.Type);
            Assert.Contains(originalPage, map.Pages);
            int window = writer.Database.Format.IsJet3 ? 1_024 : 512;
            Assert.Contains(map.Pages, page => page >= originalPage + window);
        }

        await File.WriteAllBytesAsync(session.SourcePath, image.ToArray(), Ct);

        string compact = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string compactScript = $$"""
            $db=$engine.OpenDatabase({{path}})
            try {
                $rs=$db.OpenRecordset('SELECT COUNT(*) AS N FROM Items',4)
                try { if($rs.Fields.Item(0).Value -ne 58) { throw 'Free-map promotion lost native rows' } } finally { $rs.Close() }
                $rejected=$false
                try { $db.Execute('INSERT INTO Items(Id) VALUES (26)',128) } catch { $rejected=$true }
                if(!$rejected) { throw 'Free-map promotion lost primary-key enforcement' }
                $rs=$db.OpenRecordset('Items',2)
                try {
                    $rs.AddNew()
                    $rs.Fields.Item('Id').Value=61
                    $rs.Fields.Item('Pad').Value=[byte[]](1..200)
                    $rs.Update()
                } finally { $rs.Close() }
                $db.Execute('DELETE FROM Items WHERE Id=3',128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{path}},{{compact}})
            $db=$engine.OpenDatabase({{compact}})
            try {
                $rs=$db.OpenRecordset('SELECT COUNT(*) AS N FROM Items',4)
                try { if($rs.Fields.Item(0).Value -ne 58) { throw 'Compact lost rows from the promoted free map' } } finally { $rs.Close() }
                $rs=$db.OpenRecordset('SELECT COUNT(*) AS N FROM Filler',4)
                try { if($rs.Fields.Item(0).Value -ne 1100) { throw 'Compact lost filler rows' } } finally { $rs.Close() }
            } finally { $db.Close() }
            """;
        RunNative(format, session, compactScript);
        await using AccessReader compacted = await TestDatabase.OpenReaderAsync(session.CompactedPath, cancellationToken: Ct);
        var ids = new HashSet<int>();
        await foreach (object[] row in compacted.Rows("Items", cancellationToken: Ct))
        {
            Assert.True(ids.Add(Assert.IsType<int>(row[0])));
            Assert.Equal(200, Assert.IsType<byte[]>(row[1]).Length);
        }

        Assert.Equal(58, ids.Count);
        Assert.Contains(61, ids);
        Assert.DoesNotContain(1, ids);
        Assert.DoesNotContain(2, ids);
        Assert.DoesNotContain(3, ids);
    }

    private static IEnumerable<object?[]> FillerRows()
    {
        byte[] bytes = new byte[255];
        for (int index = 0; index < 1_100; index++)
        {
            yield return [bytes, bytes, bytes, bytes, bytes, bytes];
        }
    }

    private static IEnumerable<object?[]> ItemRows()
    {
        byte[] bytes = new byte[200];
        for (int id = 26; id <= 60; id++)
        {
            yield return [id, bytes];
        }
    }

    private static async Task<MapState> ReadFreeMapAsync(DatabaseFile database, long tdefPage)
    {
        byte[] tdef = await database.Pages.ReadPageCopyAsync(tdefPage, Ct);
        Assert.True(UsageMap.TryReadPointer(tdef, database.Format.TDef.FreePages, out UsageMapPointer pointer));
        byte[] map = await database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
        Assert.True(UsageMap.TryGetRowBound(map, database.Format.DataPage, database.Format.PageSize, pointer.RowIndex, out RowBound bound));
        var pages = new List<long>();
        Assert.True(await UsageMap.TryEnumeratePagesAsync(map, bound, database.Format.PageSize, database.Pages.PageCount, 1, true, database.Pages.ReadPageAsync, PageBuffers.Return, pages, Ct));
        return new MapState(map[bound.RowStart], pages);
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

    private sealed record MapState(byte Type, List<long> Pages);
}
