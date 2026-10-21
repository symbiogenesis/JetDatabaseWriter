namespace JetDatabaseWriter.Tests.Schema;

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
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Jet3 indexes retain native flags and opaque optimizer statistics.</summary>
public sealed class Jet3IndexDefinitionTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets the schema and data operations crossed with every write mode.</summary>
    public static TheoryData<string, WriteMode> Changes =>
    [
        ("add", WriteMode.Direct), ("drop", WriteMode.Direct), ("rename", WriteMode.Direct), ("mutate", WriteMode.Direct),
        ("add", WriteMode.AutoCommit), ("drop", WriteMode.AutoCommit), ("rename", WriteMode.AutoCommit), ("mutate", WriteMode.AutoCommit),
        ("add", WriteMode.ExplicitCommit), ("drop", WriteMode.ExplicitCommit), ("rename", WriteMode.ExplicitCommit), ("mutate", WriteMode.ExplicitCommit),
    ];

    /// <summary>Access 97 does not set the Jet4-only high index flag.</summary>
    /// <param name="kind">The index variant.</param>
    /// <param name="expectedFlags">The native physical flags.</param>
    /// <returns>The asynchronous check.</returns>
    [Theory]
    [InlineData(0, 0x09)]
    [InlineData(1, 0x01)]
    [InlineData(2, 0x00)]
    [InlineData(3, 0x02)]
    [InlineData(4, 0x08)]
    [InlineData(5, 0x03)]
    [InlineData(6, 0x00)]
    [InlineData(7, 0x0B)]
    public async Task CreateTable_IndexFlagsMatchAccess97(int kind, int expectedFlags)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet3Mdb, new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Indexed", Columns(), [Definition(kind)], Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        IndexSnapshot index = Assert.Single(await ReadIndexesAsync(reader, "Indexed"));
        Assert.Equal(expectedFlags, index.Flags);
        Assert.Equal(kind != 6, index.Ascending);
        Assert.Equal("0000000000000000", index.Statistics);
    }

    /// <summary>A frozen native fixture protects optimizer bytes even on hosts without DAO.</summary>
    /// <param name="operation">The schema or data operation.</param>
    /// <param name="mode">The transaction mode.</param>
    /// <returns>The asynchronous regression.</returns>
    [Theory]
    [MemberData(nameof(Changes))]
    public async Task NativeFixture_IndexMetadataSurvivesWriterChanges(string operation, WriteMode mode)
    {
        await using MemoryStream stream = await CopyNativeFixtureAsync();
        List<IndexSnapshot> original;
        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct))
        {
            original = await ReadIndexesAsync(reader, "Native");
            Assert.Equal("0300000002000000", Assert.Single(original, index => index.Name == "IX_Amount").Statistics);
            Assert.Equal(0, Assert.Single(original, index => index.Name == "IX_Amount").Flags);
            Assert.False(Assert.Single(original, index => index.Name == "IX_Label").Ascending);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, Ct))
        {
            await WriteModes.RunAsync(writer, mode, () => ApplyFixtureChangeAsync(writer, operation, original), Ct);
        }

        stream.Position = 0;
        await using AccessReader reopened = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        Assert.Equal(original, await ReadIndexesAsync(reopened, "Native"));
    }

    /// <summary>Rollback restores the original native bytes after schema and indexed-row mutations.</summary>
    /// <param name="operation">The operation being rolled back.</param>
    /// <returns>The asynchronous rollback regression.</returns>
    [Theory]
    [InlineData("add")]
    [InlineData("drop")]
    [InlineData("rename")]
    [InlineData("mutate")]
    public async Task NativeFixture_RollbackRestoresIndexMetadataAndDatabaseBytes(string operation)
    {
        await using MemoryStream stream = await CopyNativeFixtureAsync();
        byte[] originalImage = stream.ToArray();
        List<IndexSnapshot> original;
        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct))
        {
            original = await ReadIndexesAsync(reader, "Native");
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.ExplicitCommit), leaveOpen: true, Ct))
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
            await ApplyFixtureChangeAsync(writer, operation, original);
            await transaction.RollbackAsync(Ct);
            Assert.Equal(original, await ReadIndexesAsync(writer, "Native"));
        }

        Assert.Equal(originalImage, stream.ToArray());
    }

    /// <summary>Independent DAO36 indexes establish the native flags and accept initially zero writer statistics.</summary>
    /// <returns>The asynchronous comparison.</returns>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao36_IndexKindsAndWriterZeroStatistics_SurviveReadWriteAndCompact()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.Jet3IndexDefinitions", databaseExtension: ".mdb");
        string source = Literal(session.SourcePath);
        RunNative(
            $$"""
            $db=$engine.CreateDatabase({{source}},';LANGID=0x0409;CP=1252;COUNTRY=0',32)
            try {
                for($kind=0;$kind -lt 8;$kind++) {
                    $name='N'+$kind
                    $db.Execute(('CREATE TABLE ['+$name+'] (Id LONG,K LONG,Spare LONG)'),128)
                    $db.TableDefs.Refresh()
                    $td=$db.TableDefs($name)
                    $ix=$td.CreateIndex('IX')
                    $ix.Primary=($kind -eq 0)
                    $ix.Unique=($kind -eq 0 -or $kind -eq 1 -or $kind -eq 5 -or $kind -eq 7)
                    $ix.IgnoreNulls=($kind -eq 3 -or $kind -eq 5 -or $kind -eq 7)
                    $ix.Required=($kind -eq 0 -or $kind -eq 4 -or $kind -eq 7)
                    $field=$ix.CreateField($(if($kind -eq 0){'Id'}else{'K'}))
                    if($kind -eq 6){$field.Attributes=1}
                    $ix.Fields.Append($field)
                    $td.Indexes.Append($ix)
                }
            } finally {$db.Close()}
            """,
            session);
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            for (int kind = 0; kind < 8; kind++)
            {
                await writer.CreateTableAsync($"W{kind}", Columns(), [Definition(kind)], Ct);
                await writer.InsertRowAsync($"W{kind}", [1, 10, 7], Ct);
                await writer.InsertRowAsync($"W{kind}", [2, 20, 7], Ct);
                await writer.InsertRowAsync($"W{kind}", [3, kind is 1 or 5 or 7 ? 30 : 20, 7], Ct);
                object fourthKey = kind switch { 1 or 7 => 40, 4 => 30, _ => DBNull.Value };
                await writer.InsertRowAsync($"W{kind}", [4, fourthKey, 7], Ct);
            }
        }

        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(session.SourcePath, cancellationToken: Ct))
        {
            for (int kind = 0; kind < 8; kind++)
            {
                IndexSnapshot native = Assert.Single(await ReadIndexesAsync(reader, $"N{kind}"));
                IndexSnapshot written = Assert.Single(await ReadIndexesAsync(reader, $"W{kind}"));
                Assert.Equal(native, written);
                Assert.Equal("0000000000000000", written.Statistics);
            }
        }

        string compacted = Literal(session.CompactedPath);
        RunNative(
            $$"""
            $db=$engine.OpenDatabase({{source}})
            try {
                for($kind=0;$kind -lt 8;$kind++) {
                    $native='N'+$kind
                    $third=if($kind -eq 1 -or $kind -eq 5 -or $kind -eq 7){30}else{20}
                    $fourth=if($kind -eq 1 -or $kind -eq 7){40}elseif($kind -eq 4){30}else{'NULL'}
                    $keys=@(10,20,$third,$fourth)
                    for($id=1;$id -le 4;$id++){$db.Execute(('INSERT INTO ['+$native+'] (Id,K,Spare) VALUES ('+$id+','+$keys[$id-1]+',7)'),128)}
                    foreach($name in @($native,('W'+$kind))) {
                        $rs=$db.OpenRecordset($name,1)
                        try {
                            $rs.Index='IX'
                            $rs.Seek('=',$(if($kind -eq 0){1}else{10}))
                            if($rs.NoMatch -or $rs.Fields('Id').Value -ne 1){throw 'Native indexed read lost a writer row.'}
                            $rs.MoveFirst();$count=0
                            while(!$rs.EOF){$count++;$rs.MoveNext()}
                            $indexedRows=if($kind -eq 3 -or $kind -eq 5){3}else{4}
                            if($count -ne $indexedRows){throw 'Native index count differs.'}
                        } finally {$rs.Close()}
                        $db.Execute(('INSERT INTO ['+$name+'] (Id,K,Spare) VALUES (100,100,7)'),128)
                        $db.Execute(('UPDATE ['+$name+'] SET K=101 WHERE Id=100'),128)
                        $db.Execute(('DELETE FROM ['+$name+'] WHERE Id=100'),128)
                        if($kind -eq 0 -or $kind -eq 1 -or $kind -eq 5 -or $kind -eq 7) {
                            $refused=$false
                            try {$db.Execute(('INSERT INTO ['+$name+'] (Id,K,Spare) VALUES (1,10,7)'),128)}catch{if(($_.Exception.HResult -band 65535) -ne 3022){throw};$refused=$true}
                            if(!$refused){throw 'Native unique enforcement accepted a duplicate.'}
                        }
                        if($kind -eq 0 -or $kind -eq 4 -or $kind -eq 7) {
                            $nullColumn=if($kind -eq 0){'Id'}else{'K'}
                            $refused=$false
                            try {$db.Execute(('INSERT INTO ['+$name+'] (['+$nullColumn+']) VALUES (NULL)'),128)}catch{$code=$_.Exception.HResult -band 65535;if($code -ne 3058 -and $code -ne 3314){throw};$refused=$true}
                            if(!$refused){throw 'Native required-index enforcement accepted Null.'}
                        }
                    }
                }
            } finally {$db.Close()}
            $engine.CompactDatabase({{source}},{{compacted}},';LANGID=0x0409;CP=1252;COUNTRY=0',32)
            $db=$engine.OpenDatabase({{compacted}},$false,$true)
            try {
                for($kind=0;$kind -lt 8;$kind++) {
                    foreach($prefix in @('N','W')) {
                        $rs=$db.OpenRecordset(('SELECT Count(*) AS N FROM ['+$prefix+$kind+']'))
                        try {if($rs.Fields('N').Value -ne 4){throw 'Compaction lost indexed rows.'} }finally{$rs.Close()}
                    }
                }
            } finally {$db.Close()}
            """,
            session);
        await using AccessReader compactReader = await TestDatabase.OpenReaderAsync(session.CompactedPath, cancellationToken: Ct);
        for (int kind = 0; kind < 8; kind++)
        {
            Assert.Equal(await ReadIndexesAsync(compactReader, $"N{kind}"), await ReadIndexesAsync(compactReader, $"W{kind}"));
        }
    }

    /// <summary>Schema rewrites and row mutations must retain the native optimizer statistics verbatim.</summary>
    /// <param name="operation">The writer operation.</param>
    /// <param name="mode">The writer transaction mode.</param>
    /// <returns>The asynchronous native verification.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [MemberData(nameof(Changes))]
    public async Task Dao36_NativeIndexStatisticsAndFlags_SurviveWriterChanges(string operation, WriteMode mode)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.Jet3IndexStatistics", databaseExtension: ".mdb");
        string source = Literal(session.SourcePath);
        RunNative(
            $$"""
            $db=$engine.CreateDatabase({{source}},';LANGID=0x0409;CP=1252;COUNTRY=0',32)
            try {
                $db.Execute('CREATE TABLE T (Id LONG,K LONG,Spare LONG)',128)
                $db.Execute('INSERT INTO T VALUES (1,10,7)',128)
                $db.Execute('INSERT INTO T VALUES (2,20,7)',128)
                $db.Execute('INSERT INTO T VALUES (3,20,7)',128)
                $db.Execute('INSERT INTO T VALUES (4,NULL,7)',128)
                $db.Execute('CREATE UNIQUE INDEX PK ON T (Id) WITH PRIMARY',128)
                $db.Execute('CREATE INDEX IX ON T (K)',128)
                $db.Execute('CREATE INDEX DX ON T (K DESC) WITH IGNORE NULL',128)
            } finally {$db.Close()}
            """,
            session);
        List<IndexSnapshot> before;
        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(session.SourcePath, cancellationToken: Ct))
        {
            before = await ReadIndexesAsync(reader, "T");
            Assert.Contains(before, index => index.Name == "IX" && index.Statistics == "0400000003000000");
            Assert.Contains(before, index => index.Name == "DX" && index.Statistics == "0300000002000000");
        }

        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, WriteModes.WriterOptions(mode), Ct))
        {
            await WriteModes.RunAsync(writer, mode, () => ApplyChangeAsync(writer, operation, before), Ct);
        }

        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(session.SourcePath, cancellationToken: Ct))
        {
            Assert.Equal(before, await ReadIndexesAsync(reader, "T"));
        }

        string key = operation == "rename" ? "Renamed" : "K";
        string compacted = Literal(session.CompactedPath);
        RunNative(
            $$"""
            $db=$engine.OpenDatabase({{source}})
            try {
                foreach($index in @('PK','IX','DX')) {
                    $rs=$db.OpenRecordset('T',1)
                    try {
                        $rs.Index=$index
                        $rs.Seek('=',$(if($index -eq 'PK'){1}else{10}))
                        if($rs.NoMatch -or $rs.Fields('Id').Value -ne 1){throw 'Preserved index lost its key.'}
                    } finally {$rs.Close()}
                }
                $db.Execute('INSERT INTO T (Id,[{{key}}]) VALUES (100,100)',128)
                $db.Execute('UPDATE T SET [{{key}}]=101 WHERE Id=100',128)
                $db.Execute('DELETE FROM T WHERE Id=100',128)
            } finally {$db.Close()}
            $engine.CompactDatabase({{source}},{{compacted}},';LANGID=0x0409;CP=1252;COUNTRY=0',32)
            $db=$engine.OpenDatabase({{compacted}},$false,$true)
            try {
                $rs=$db.OpenRecordset('SELECT Count(*) AS N FROM T')
                try {if($rs.Fields('N').Value -ne 4){throw 'Compaction lost preserved rows.'} }finally{$rs.Close()}
            } finally {$db.Close()}
            """,
            session);
    }

    /// <summary>Native statistics can be stale and differ from both the table and live index row counts.</summary>
    /// <param name="ignoreNulls">Whether Null keys are omitted.</param>
    /// <returns>The asynchronous native characterization.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Dao36_StatisticsRetainInsertionObservationsUntilCompact(bool ignoreNulls)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.Jet3NativeStatistics", databaseExtension: ".mdb");
        string source = Literal(session.SourcePath);
        string directory = Literal(session.WorkDir);
        string compacted = Literal(session.CompactedPath);
        RunNative(
            $$"""
            $db=$engine.CreateDatabase({{source}},';LANGID=0x0409;CP=1252;COUNTRY=0',32)
            try {
                $db.Execute('CREATE TABLE T (Id LONG,K LONG,Spare LONG)',128)
                $db.Execute('CREATE INDEX IX ON T (K){{(ignoreNulls ? " WITH IGNORE NULL" : string.Empty)}}',128)
            } finally {$db.Close()}
            [IO.File]::Copy({{source}},[IO.Path]::Combine({{directory}},'empty.mdb'))
            function Apply($stage,$sql) {
                $db=$engine.OpenDatabase({{source}})
                try {$db.Execute($sql,128)}finally{$db.Close()}
                [IO.File]::Copy({{source}},[IO.Path]::Combine({{directory}},$stage+'.mdb'))
            }
            Apply 'first' 'INSERT INTO T (Id,K) VALUES (1,10)'
            Apply 'duplicate' 'INSERT INTO T (Id,K) VALUES (2,10)'
            Apply 'null' 'INSERT INTO T (Id,K) VALUES (3,NULL)'
            Apply 'new' 'INSERT INTO T (Id,K) VALUES (4,20)'
            Apply 'update' 'UPDATE T SET K=30 WHERE Id=2'
            Apply 'delete' 'DELETE FROM T WHERE Id=4 OR Id=2'
            Apply 'reinsert' 'INSERT INTO T (Id,K) VALUES (5,30)'
            $engine.CompactDatabase({{source}},{{compacted}},';LANGID=0x0409;CP=1252;COUNTRY=0',32)
            """,
            session);
        string[] stages = ["empty", "first", "duplicate", "null", "new", "update", "delete", "reinsert"];
        int[] observations = ignoreNulls ? [0, 1, 1, 1, 2, 2, 2, 3] : [0, 1, 1, 2, 3, 3, 3, 4];
        for (int stage = 0; stage < stages.Length; stage++)
        {
            await using AccessReader reader = await TestDatabase.OpenReaderAsync(Path.Combine(session.WorkDir, stages[stage] + ".mdb"), cancellationToken: Ct);
            IndexSnapshot index = Assert.Single(await ReadIndexesAsync(reader, "T"));
            Assert.Equal($"000000000{observations[stage]}000000", index.Statistics);
        }

        await using AccessReader compactReader = await TestDatabase.OpenReaderAsync(session.CompactedPath, cancellationToken: Ct);
        Assert.Equal(ignoreNulls ? "0200000002000000" : "0300000003000000", Assert.Single(await ReadIndexesAsync(compactReader, "T")).Statistics);
    }

    private static async Task ApplyChangeAsync(AccessWriter writer, string operation, List<IndexSnapshot> expected)
    {
        switch (operation)
        {
            case "add":
                await writer.AddColumnAsync("T", new ColumnDefinition("Added", typeof(int)), Ct);
                break;
            case "drop":
                await writer.DropColumnAsync("T", "Spare", Ct);
                break;
            case "rename":
                await writer.RenameColumnAsync("T", "K", "Renamed", Ct);
                break;
            default:
                await writer.InsertRowAsync("T", [5, 30, 7], Ct);
                Assert.Equal(expected, await ReadIndexesAsync(writer, "T"));
                await writer.UpdateRowsAsync("T", RowCriteria.Where("Id", 2), new RowValues { ["K"] = 40 }, Ct);
                Assert.Equal(expected, await ReadIndexesAsync(writer, "T"));
                await writer.DeleteRowsAsync("T", RowCriteria.Where("Id", 3), Ct);
                break;
        }

        Assert.Equal(expected, await ReadIndexesAsync(writer, "T"));
    }

    private static async Task<MemoryStream> CopyNativeFixtureAsync()
    {
        string path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "Jet3", "NativeDescriptorMetadata.mdb");
        byte[] image = await File.ReadAllBytesAsync(path, Ct);
        var stream = new MemoryStream();
        await stream.WriteAsync(image, Ct);
        stream.Position = 0;
        return stream;
    }

    private static async Task ApplyFixtureChangeAsync(AccessWriter writer, string operation, List<IndexSnapshot> expected)
    {
        switch (operation)
        {
            case "add":
                await writer.AddColumnAsync("Native", new ColumnDefinition("Added", typeof(int)), Ct);
                break;
            case "drop":
                await writer.DropColumnAsync("Native", "Flag", Ct);
                break;
            case "rename":
                await writer.RenameColumnAsync("Native", "Label", "Renamed", Ct);
                break;
            default:
                await writer.InsertRowAsync("Native", new RowValues { ["Id"] = 4, ["Amount"] = 30m, ["Label"] = "new text" }, Ct);
                Assert.Equal(expected, await ReadIndexesAsync(writer, "Native"));
                await writer.UpdateRowsAsync("Native", RowCriteria.Where("Id", 2), new RowValues { ["Amount"] = 40m }, Ct);
                Assert.Equal(expected, await ReadIndexesAsync(writer, "Native"));
                await writer.DeleteRowsAsync("Native", RowCriteria.Where("Id", 3), Ct);
                break;
        }

        Assert.Equal(expected, await ReadIndexesAsync(writer, "Native"));
    }

    private static string Literal(string value) => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(value);

    private static void RunNative(string script, AccessRoundTripSession session)
    {
        PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, result.StandardOutput + result.StandardError);
    }

    private static ColumnDefinition[] Columns() =>
    [
        new("Id", typeof(int)),
        new("K", typeof(int)),
        new("Spare", typeof(int)),
    ];

    private static IndexDefinition Definition(int kind) => new("IX", kind == 0 ? "Id" : "K")
    {
        IsPrimaryKey = kind == 0,
        IsUnique = kind is 1 or 5 or 7,
        IgnoreNulls = kind is 3 or 5 or 7,
        IsRequired = kind is 4 or 7,
        DescendingColumns = kind == 6 ? ["K"] : [],
    };

    private static async Task<List<IndexSnapshot>> ReadIndexesAsync(AccessReader reader, string table)
    {
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await reader.GetCatalogEntryAsync(table, Ct));
        return await ReadIndexesAsync(reader.Database, entry);
    }

    private static async Task<List<IndexSnapshot>> ReadIndexesAsync(AccessWriter writer, string table)
    {
        CatalogEntry entry = await writer.Services.Catalog.GetRequiredCatalogEntryAsync(table, Ct);
        return await ReadIndexesAsync(writer.Database, entry);
    }

    private static async Task<List<IndexSnapshot>> ReadIndexesAsync(DatabaseFile database, CatalogEntry entry)
    {
        byte[] td = (await database.TableDefs.ReadTDefBytesAsync(entry.TDefPage, Ct))!;
        JetFormat format = database.Format;
        int columns = Ru16(td, format.TDef.NumCols);
        int realIndexes = Ri32(td, format.TDef.NumRealIdx);
        int indexes = Ri32(td, format.TDef.NumIdx);
        int start = IndexCatalogReader.LocateRealIdxDescStart(format, td, columns, realIndexes);
        IndexSectionAnchors anchors = format.Index.GetIndexSection(start, realIndexes, indexes);
        List<string> names = IndexCatalogReader.ReadLogicalIdxNames(format, td, anchors.LogIdxNamesStart, indexes);
        var result = new List<IndexSnapshot>();
        for (int ordinal = 0; ordinal < names.Count; ordinal++)
        {
            Assert.True(format.Index.TryReadLogicalEntry(td, anchors.LogIdxStart, ordinal, out LogicalIdxEntry logical));
            Assert.True(format.Index.TryReadRealIdxSlotWithKeyColumns(td, start, logical.IndexNum2, out RealIdxSlot slot, out List<KeyColumn> keys));
            int statistics = format.TDef.BlockEnd + (logical.IndexNum2 * format.TDef.RealIdxEntrySz);
            result.Add(new IndexSnapshot(names[ordinal], slot.Flags, keys[0].Ascending, Convert.ToHexString(td.AsSpan(statistics, format.TDef.RealIdxEntrySz))));
        }

        return result.OrderBy(index => index.Name, StringComparer.Ordinal).ToList();
    }

    private sealed record IndexSnapshot(string Name, byte Flags, bool Ascending, string Statistics);
}
