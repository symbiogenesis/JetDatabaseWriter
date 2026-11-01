namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Independent native controls for admitting relationship-backed index mutation.</summary>
/// <param name="output">Captures actual native module and incremental-path evidence.</param>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class ForeignKeyIncrementalEvidenceTests(ITestOutputHelper output)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>A single-leaf insert must avoid decoding unrelated long values and retain native constraints.</summary>
    /// <param name="format">The DAO3.6-supported format.</param>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public Task Dao36_SingleLeaf_InsertUsesIncrementalPathAndBoundedReads(DatabaseFormat format) => this.SingleLeafAsync(format, useDao36: true);

    /// <summary>Native multi-level FK trees retain exact maps through growth, changed keys, deletion and cascade.</summary>
    /// <param name="format">The DAO3.6-supported format.</param>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public Task Dao36_MultiLevel_GrowthKeyChangesDeleteAndCompactPreserveMembership(DatabaseFormat format) => this.MultiLevelAsync(format, useDao36: true);

    /// <summary>DAO120 verifies the single-leaf incremental path on Jet4 and ACE.</summary>
    /// <param name="format">The native database format.</param>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public Task Dao120_SingleLeaf_InsertUsesIncrementalPathAndBoundedReads(DatabaseFormat format) => this.SingleLeafAsync(format, useDao36: false);

    /// <summary>DAO120 verifies larger mixed-codec FK trees through mutation and Compact.</summary>
    /// <param name="format">The native database format.</param>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public Task Dao120_MultiLevel_GrowthKeyChangesDeleteAndCompactPreserveMembership(DatabaseFormat format) => this.MultiLevelAsync(format, useDao36: false);

    private static async Task AssertIndexMapsAsync(AccessWriter writer, string name)
    {
        CatalogEntry table = await writer.Services.Catalog.GetRequiredCatalogEntryAsync(name, Ct);
        byte[] definition = (await writer.Database.TableDefs.ReadTDefBytesAsync(table.TDefPage, Ct))!;
        JetFormat format = writer.Database.Format;
        int columns = Ru16(definition, format.TDef.NumCols);
        int count = Ri32(definition, format.TDef.NumRealIdx);
        int start = IndexCatalogReader.LocateRealIdxDescStart(format, definition, columns, count);
        for (int index = 0; index < count; index++)
        {
            Assert.True(format.Index.TryReadRealIdxSlot(definition, start, index, out RealIdxSlot slot));
            long root = Ri32(definition, slot.FirstDpOffset);
            HashSet<long> tree = await IndexLeafChain.ReadTreePagesAsync(writer.Database, table.TDefPage, root, Ct);
            Assert.True(UsageMap.TryReadPointer(definition, slot.FirstDpOffset - 4, out UsageMapPointer pointer));
            byte[] map = await writer.Database.Pages.ReadPageCopyAsync(pointer.PageNumber, Ct);
            Assert.True(UsageMap.TryGetRowBound(map, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound row));
            var members = new List<long>();
            Assert.True(await UsageMap.TryEnumeratePagesAsync(map, row, format.PageSize, writer.Database.Pages.PageCount, 1, true, writer.Database.Pages.ReadPageAsync, PageBuffers.Return, members, Ct));
            Assert.Equal(tree.Order(), members.Order());
            await IndexLeafChain.AssertCoversLiveRowsAsync(writer.Database, table.TDefPage, root, Ct);
        }
    }

    private async Task SingleLeafAsync(DatabaseFormat format, bool useDao36)
    {
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        this.AuthorNative(session, format, 32, longValues: true, useDao36);
        await using var source = new MemoryStream();
        await source.WriteAsync(await File.ReadAllBytesAsync(session.SourcePath, Ct), Ct);
        source.Position = 0;
        await using var trace = new CountingStream(source);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(trace, new AccessWriterOptions { UseLockFile = false, PageCacheSize = 0 }, leaveOpen: true, Ct))
        {
            CatalogEntry child = await writer.Services.Catalog.GetRequiredCatalogEntryAsync("C", Ct);
            List<long> roots = await IndexLeafChain.ReadRealIndexRootsAsync(writer.Database, child.TDefPage, Ct);
            foreach (long root in roots)
            {
                Assert.Equal(Constants.IndexLeafPage.PageTypeLeaf, (await writer.Database.Pages.ReadPageCopyAsync(root, Ct))[0]);
            }

            trace.Reset();
            Assert.Equal(32, (await writer.Services.Snapshots.ReadRowsAsync(child.TDefPage, Ct)).Count);
            long completeSnapshotReads = trace.BytesRead;
            trace.Reset();
            await writer.InsertRowAsync("C", [33, 1, "added", new DateTime(2020, 1, 1).AddMinutes(33), "label-33"], Ct);
            long reads = trace.BytesRead;
            output.WriteLine($"{format}: incremental bail={writer.Services.Indexes.LastIncrementalBail ?? "none"}; read={reads}; complete snapshot={completeSnapshotReads}; length={source.Length}");
            Assert.Null(writer.Services.Indexes.LastIncrementalBail);
            Assert.True(reads < completeSnapshotReads, $"A single FK insert read {reads} bytes; a complete table snapshot reads {completeSnapshotReads}.");
            Assert.Equal(roots, await IndexLeafChain.ReadRealIndexRootsAsync(writer.Database, child.TDefPage, Ct));
            await AssertIndexMapsAsync(writer, "C");
        }

        string subject = session.CreateDatabasePath("incremental");
        await File.WriteAllBytesAsync(subject, source.ToArray(), Ct);
        this.ValidateNativeAndCompact(session, subject, 33, parentKey: 1, firstKey: 1, otherParentRows: 0, useDao36);
    }

    private async Task MultiLevelAsync(DatabaseFormat format, bool useDao36)
    {
        const int initial = 1536;
        const int added = 1024;
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        this.AuthorNative(session, format, initial, longValues: false, useDao36);
        string subject = session.CreateDatabasePath("incremental");
        File.Copy(session.SourcePath, subject);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(subject, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            CatalogEntry child = await writer.Services.Catalog.GetRequiredCatalogEntryAsync("C", Ct);
            List<long> roots = await IndexLeafChain.ReadRealIndexRootsAsync(writer.Database, child.TDefPage, Ct);
            int originalPages = 0;
            int intermediateRoots = 0;
            foreach (long root in roots)
            {
                byte type = (await writer.Database.Pages.ReadPageCopyAsync(root, Ct))[0];
                intermediateRoots += type == Constants.IndexLeafPage.PageTypeIntermediate ? 1 : 0;
                output.WriteLine($"{format}: native root={root};type={type};entries={(await IndexLeafChain.ReadEntriesAsync(writer.Database, child.TDefPage, root, Ct)).Count}");
                originalPages += (await IndexLeafChain.ReadTreePagesAsync(writer.Database, child.TDefPage, root, Ct)).Count;
            }

            Assert.True(intermediateRoots > 0);
            for (int first = initial + 1; first <= initial + added; first += 128)
            {
                await writer.InsertRowsAsync("C", Enumerable.Range(first, 128).Select(static id => new object[] { id, 1, "added", new DateTime(2020, 1, 1).AddMinutes(id), $"label-{id}" }), Ct);
                Assert.Null(writer.Services.Indexes.LastIncrementalBail);
                await AssertIndexMapsAsync(writer, "C");
            }

            int grownPages = 0;
            foreach (long root in await IndexLeafChain.ReadRealIndexRootsAsync(writer.Database, child.TDefPage, Ct))
            {
                grownPages += (await IndexLeafChain.ReadTreePagesAsync(writer.Database, child.TDefPage, root, Ct)).Count;
            }

            Assert.True(grownPages > originalPages);
            Assert.Equal(1, await writer.UpdateRowsAsync("C", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 99_000 }, Ct));
            Assert.Null(writer.Services.Indexes.LastIncrementalBail);
            Assert.Equal(1, await writer.UpdateRowsAsync("C", RowCriteria.Where("Id", 2), new RowValues { ["ParentId"] = 2 }, Ct));
            Assert.Null(writer.Services.Indexes.LastIncrementalBail);
            Assert.Equal(1, await writer.DeleteRowsAsync("C", RowCriteria.Where("Id", 3), Ct));
            Assert.Null(writer.Services.Indexes.LastIncrementalBail);
            Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 10 }, Ct));
            Assert.Null(writer.Services.Indexes.LastIncrementalBail);
            await AssertIndexMapsAsync(writer, "P");
            await AssertIndexMapsAsync(writer, "C");
            output.WriteLine($"{format}: native tree pages grew {originalPages}->{grownPages}; every mutation used incremental maintenance.");
        }

        this.ValidateNativeAndCompact(session, subject, initial + added - 1, parentKey: 10, firstKey: 99_000, otherParentRows: 1, useDao36);
    }

    private void AuthorNative(AccessRoundTripSession session, DatabaseFormat format, int rows, bool longValues, bool useDao36)
    {
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        int version = format switch
        {
            DatabaseFormat.Jet3Mdb => 32,
            DatabaseFormat.Jet4Mdb => 64,
            DatabaseFormat.AceAccdb => 128,
            _ => 128,
        };
        this.RunNative(session, useDao36, $$"""
            $db=$engine.CreateDatabase({{path}},';LANGID=0x0409;CP=1252;COUNTRY=0',{{version}})
            try {
                $db.Execute('CREATE TABLE P (Id LONG CONSTRAINT PK_P PRIMARY KEY)',128)
                $db.Execute('CREATE TABLE C (Id LONG CONSTRAINT PK_C PRIMARY KEY,ParentId LONG,Payload MEMO,Stamp DATETIME,Label TEXT(30))',128)
                if ({{(longValues ? "$false" : "$true")}}) {
                    $db.Execute('CREATE INDEX IX_C_Stamp ON C (Stamp)',128)
                    $db.Execute('CREATE INDEX IX_C_Label ON C (Label)',128)
                }
                $db.Execute('INSERT INTO P VALUES (1)',128);$db.Execute('INSERT INTO P VALUES (2)',128)
                $rel=$db.CreateRelation('FK_C_P','P','C',4352)
                $field=$rel.CreateField('Id');$field.ForeignName='ParentId';$rel.Fields.Append($field);$db.Relations.Append($rel)
                $payload=if({{(longValues ? "$true" : "$false")}}){'m'*12000}else{'native'}
                for($i=1;$i -le {{rows}};$i++){$db.Execute(('INSERT INTO C VALUES ('+$i+',1,'''+$payload+''',DateAdd(''n'','+$i+',#2020-01-01#),''label-'+$i+''')'),128)}
            } finally {$db.Close()}
            """);
    }

    private void ValidateNativeAndCompact(AccessRoundTripSession session, string subject, int rows, int parentKey, int firstKey, int otherParentRows, bool useDao36)
    {
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(subject);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        this.RunNative(session, useDao36, $$"""
            foreach($path in @({{source}},{{compacted}})) {
                if($path -eq {{compacted}}){$engine.CompactDatabase({{source}},{{compacted}})}
                $db=$engine.OpenDatabase($path)
                try {
                    $rs=$db.OpenRecordset('SELECT COUNT(*) AS N FROM C',4)
                    try {if([int]$rs.Fields('N').Value -ne {{rows}}){throw 'Wrong native child count'} } finally {$rs.Close()}
                    $rs=$db.OpenRecordset('SELECT COUNT(*) AS N FROM C WHERE ParentId={{parentKey}}',4)
                    try {if([int]$rs.Fields('N').Value -ne {{rows - otherParentRows}}){throw 'Wrong native foreign-key membership'} } finally {$rs.Close()}
                    $rs=$db.OpenRecordset('C',1)
                    try {$rs.Index='PK_C';$rs.Seek('=',{{firstKey}});if($rs.NoMatch){throw 'Native primary-key seek missed'} } finally {$rs.Close()}
                    if ({{(rows > 100 ? "$true" : "$false")}}) {
                    $rs=$db.OpenRecordset('C',1)
                    try {$rs.Index='IX_C_Label';$rs.Seek('=','label-1');if($rs.NoMatch){throw 'Native text seek missed'} } finally {$rs.Close()}
                    $rs=$db.OpenRecordset('C',1)
                    try {$rs.Index='IX_C_Stamp';$rs.Seek('=',[DateTime]::new(2020,1,1,0,1,0));if($rs.NoMatch){throw 'Native date seek missed'} } finally {$rs.Close()}
                    }
                    $refused=$false
                    try {$db.Execute('INSERT INTO C (Id,ParentId,Payload) VALUES (1000000,777,''orphan'')',128)}catch{$refused=$true}
                    if(!$refused){throw 'Native foreign-key enforcement lost'}
                    $db.Execute('INSERT INTO C (Id,ParentId,Payload) VALUES (1000001,2,''native'')',128)
                    $db.Execute('UPDATE C SET ParentId={{parentKey}} WHERE Id=1000001',128)
                    $db.Execute('DELETE FROM C WHERE Id=1000001',128)
                    Write-Output ('NATIVE_READ_WRITE_COMPACT_OK '+$path)
                } finally {$db.Close()}
            }
            """);
    }

    private void RunNative(AccessRoundTripSession session, bool useDao36, string body)
    {
        body = "[Diagnostics.Process]::GetCurrentProcess().Modules | Where-Object ModuleName -Match 'dao360|acedao' | ForEach-Object { Write-Output ('DAO_MODULE='+$_.FileName+';VERSION='+$_.FileVersionInfo.FileVersion) }\n" + body;
        if (!useDao36)
        {
            AccessRoundTripEnvironment.CompactResult native = session.RunDaoEngineScript(body, TimeSpan.FromMinutes(2));
            output.WriteLine(native.StdOut);
            Assert.True(native.ExitCode == 0, native.StdOut + native.StdErr);
            return;
        }

        PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(
            body,
            session.WorkDir,
            TimeSpan.FromMinutes(2));
        output.WriteLine(result.StandardOutput);
        Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, result.StandardOutput + result.StandardError);
    }
}
