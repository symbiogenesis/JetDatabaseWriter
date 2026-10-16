namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>DAO3.6 verifies private core-system map storage and reference-map growth through native compact.</summary>
public sealed class Jet3SystemUsageMapDaoTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [InlineData("reference")]
    [InlineData("shared-row")]
    [InlineData("shared-bitmap")]
    [InlineData("foreign-row")]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task ReopenedCoreMaps_Dao36ReadsMutatesEnforcesAndCompacts(string storage)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.Jet3SystemMaps", databaseExtension: ".mdb");
        string fixture = Path.Combine(AppContext.BaseDirectory, "Databases", "Jet3", "NativeDescriptorMetadata.mdb");
        File.Copy(fixture, session.SourcePath);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await writer.CreateTableAsync("P", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            await writer.CreateTableAsync("C", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int))], Ct);
            await writer.InsertRowAsync("P", new RowValues { ["Id"] = 1 }, Ct);
            foreach (string name in new[] { "MSysObjects", "MSysACEs", "MSysQueries", "MSysRelationships" })
            {
                long root = await writer.Services.CatalogRows.FindSystemTableTdefPageAsync(name, Ct);
                await ForceReferenceMapsAsync(writer, root);
            }

            long relationships = await writer.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysRelationships", Ct);
            byte[] definition = await writer.Database.Pages.ReadPageCopyAsync(relationships, Ct);
            Assert.True(UsageMap.TryReadPointer(definition, writer.Database.Format.TDef.UsedPages, out UsageMapPointer owned));
            Assert.True(UsageMap.TryReadPointer(definition, writer.Database.Format.TDef.FreePages, out UsageMapPointer free));
            if (storage == "shared-row")
            {
                UsageMap.WritePointer(definition, writer.Database.Format.TDef.FreePages, owned.RowIndex, owned.PageNumber);
                await writer.Pager.WritePageAsync(relationships, definition, Ct);
            }
            else if (storage == "foreign-row")
            {
                long parentRoot = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("P", Ct)).TDefPage;
                byte[] parent = await writer.Database.Pages.ReadPageCopyAsync(parentRoot, Ct);
                Assert.True(UsageMap.TryReadPointer(parent, writer.Database.Format.TDef.UsedPages, out UsageMapPointer parentMap));
                UsageMap.WritePointer(definition, writer.Database.Format.TDef.UsedPages, parentMap.RowIndex, parentMap.PageNumber);
                UsageMap.WritePointer(definition, writer.Database.Format.TDef.FreePages, parentMap.RowIndex, parentMap.PageNumber);
                await writer.Pager.WritePageAsync(relationships, definition, Ct);
            }
            else if (storage == "shared-bitmap")
            {
                byte[] bitmap = new byte[writer.Database.Format.PageSize];
                bitmap[0] = Constants.PageTypes.UsageMap;
                bitmap[1] = 1;
                long bitmapPage = await writer.Services.PageAllocator.AllocatePageAsync(bitmap, Ct);
                foreach (UsageMapPointer pointer in new[] { owned, free })
                {
                    byte[] map = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
                    Assert.True(UsageMap.TryGetRowBound(map, writer.Database.Format.DataPage, writer.Database.Format.PageSize, pointer.RowIndex, out RowBound row));
                    Wi32(map, row.RowStart + Constants.UsageMap.ReferenceMapPointerOffset, checked((int)bitmapPage));
                    await writer.Pager.WritePageAsync(pointer.PageNumber, map, Ct);
                }
            }
        }

        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await writer.CreateRelationshipAsync(new RelationshipDefinition("P_C", "P", "Id", "C", "ParentId") { CascadeUpdates = true, CascadeDeletes = true }, Ct);
            await writer.InsertRowAsync("C", new RowValues { ["Id"] = 10, ["ParentId"] = 1 }, Ct);
            for (int index = 0; index < 24; index++)
            {
                await writer.CreateTableAsync($"CatalogGrowth_{index:D2}", [new ColumnDefinition("Id", typeof(int))], Ct);
            }
        }

        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            function VerifyRelation($db) {
                $relation=$db.Relations.Item('P_C')
                if($relation.Table -ne 'P' -or $relation.ForeignTable -ne 'C' -or $relation.Attributes -ne 4352) { throw 'Wrong relationship metadata' }
                $rejected=$false
                try { $db.Execute('INSERT INTO C VALUES (99,999)',128) } catch {
                    if(@($engine.Errors | Where-Object Number -eq 3201).Count -eq 0) { throw }
                    $rejected=$true
                }
                if(!$rejected) { throw 'DAO accepted an orphan' }
                $rs=$db.OpenRecordset('SELECT Id,ParentId FROM C',4)
                try {
                    if($rs.EOF -or $rs.Fields('Id').Value -ne 10 -or $rs.Fields('ParentId').Value -ne 2) { throw 'Lost surviving child' }
                    $rs.MoveNext()
                    if(!$rs.EOF) { throw 'Delete cascade failed' }
                } finally { $rs.Close() }
            }
            $db=$engine.OpenDatabase({{source}})
            try {
                $db.Execute('UPDATE P SET Id=2 WHERE Id=1',128)
                $db.Execute('INSERT INTO P VALUES (3)',128)
                $db.Execute('INSERT INTO C VALUES (11,3)',128)
                $db.Execute('DELETE FROM P WHERE Id=3',128)
                $query=$db.CreateQueryDef('CoreMapQuery','SELECT Id FROM P')
                $query.Close()
                VerifyRelation $db
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}},{{compacted}},';LANGID=0x0409;CP=1252;COUNTRY=0',32)
            $db=$engine.OpenDatabase({{compacted}})
            try {
                VerifyRelation $db
                $rs=$db.QueryDefs('CoreMapQuery').OpenRecordset(4)
                try { if($rs.EOF -or $rs.Fields('Id').Value -ne 2) { throw 'Lost query or parent' } } finally { $rs.Close() }
                if($db.TableDefs('CatalogGrowth_23').Fields.Count -ne 1) { throw 'Lost catalog growth' }
            } finally { $db.Close() }
            Write-Output 'JET3_CORE_MAP_COMPACT_OK'
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, $"DAO3.6 core maps failed:\n{result.StandardOutput}\n{result.StandardError}");
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(session.CompactedPath, cancellationToken: Ct);
        using DataTable children = await reader.ReadTableAsync("C", cancellationToken: Ct);
        Assert.Equal(2, Assert.Single(children.Select())["ParentId"]);
        Assert.Equal("P_C", Assert.Single(await reader.ListRelationshipsAsync(Ct)).Name);
    }

    private static async Task ForceReferenceMapsAsync(AccessWriter writer, long root)
    {
        JetFormat format = writer.Database.Format;
        byte[] definition = await writer.Database.Pages.ReadPageCopyAsync(root, Ct);
        var editor = new UsageMapEditor(format, writer.Pager, writer.Services.PageAllocator);
        foreach (int offset in new[] { format.TDef.UsedPages, format.TDef.FreePages })
        {
            Assert.True(UsageMap.TryReadPointer(definition, offset, out UsageMapPointer pointer));
            byte[] map = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
            Assert.True(UsageMap.TryGetRowBound(map, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound row));
            var members = new List<long>();
            Assert.True(await UsageMap.TryEnumeratePagesAsync(map, row, format.PageSize, writer.Database.Pages.PageCount, 1, true, writer.Database.Pages.ReadPageAsync, PageBuffers.Return, members, Ct));
            Array.Clear(map, row.RowStart, row.RowSize);
            map[row.RowStart] = Constants.UsageMap.ReferenceMapType;
            await editor.WriteRowAsync(map, row.RowStart, row.RowSize, members, keepReferencePages: true, Ct);
            await writer.Pager.WritePageAsync(pointer.PageNumber, map, Ct);
        }
    }
}
