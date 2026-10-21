namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Native continuation headers and opaque bytes survive schema edits and logical chain write-back.</summary>
public sealed class NativeTDefChainPreservationTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets the DAO120 formats and writer modes.</summary>
    public static TheoryData<DatabaseFormat, WriteMode> Dao120FormatsAndModes()
        => WriteModes.Combine(DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [MemberData(nameof(Dao120FormatsAndModes))]
    public Task Dao120Chain_SchemaEditsPreserveNativeBytesAndSurviveCompact(DatabaseFormat format, WriteMode mode)
        => VerifyAsync(format, mode);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(WriteMode.Direct)]
    [InlineData(WriteMode.AutoCommit)]
    [InlineData(WriteMode.ExplicitCommit)]
    public Task Dao36Chain_SchemaEditsPreserveNativeBytesAndSurviveCompact(WriteMode mode)
        => VerifyAsync(DatabaseFormat.Jet3Mdb, mode);

    private static async Task VerifyAsync(DatabaseFormat format, WriteMode mode)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativeTDefChain", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        int version = format switch { DatabaseFormat.Jet3Mdb => 32, DatabaseFormat.Jet4Mdb => 64, DatabaseFormat.AceAccdb => 128, _ => throw new ArgumentOutOfRangeException(nameof(format)) };
        string createScript = $$"""
            $db = $engine.CreateDatabase({{source}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            try {
                $db.Execute('CREATE TABLE Parent (Id LONG CONSTRAINT PK_Parent PRIMARY KEY)',128)
                $db.Execute('INSERT INTO Parent VALUES (10)',128)
                $table = $db.CreateTableDef('Native')
                $table.Fields.Append($table.CreateField('Id', 4))
                $table.Fields.Append($table.CreateField('Label', 10, 40))
                for ($i = 0; $i -lt 200; $i++) { $table.Fields.Append($table.CreateField(('ContinuationColumn' + $i), 4)) }
                $db.TableDefs.Append($table)
                $db.Execute('CREATE UNIQUE INDEX PK_Native ON Native (Id) WITH PRIMARY',128)
                $db.Execute('CREATE INDEX IX_Label ON Native (Label DESC)',128)
                $db.Execute('ALTER TABLE Native ADD CONSTRAINT NativeParent FOREIGN KEY (ContinuationColumn0) REFERENCES Parent (Id)',128)
                $db.Execute("INSERT INTO [Native] ([Id],[Label],[ContinuationColumn0]) VALUES (1,'native payload',10)", 128)
            } finally { $db.Close() }
            """;
        (int exitCode, string stdout, string stderr) = RunNative(session, format, createScript);
        Assert.True(exitCode == 0, $"Native create failed: {stdout}\n{stderr}");

        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, WriteModes.WriterOptions(mode), Ct))
        {
            long root = (await writer.Services.Catalog.ResolveRequiredTableAsync("Native", Ct)).Entry.TDefPage;
            LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(root, Ct);
            Assert.True(chain.PageNumbers.Count > 1);
            var pages = new Dictionary<long, byte[]>();
            foreach (long page in chain.PageNumbers)
            {
                pages.Add(page, await writer.Database.Pages.ReadPageCopyAsync(page, Ct));
            }

            await chain.WriteAsync(chain.Bytes, checked(Ri32(chain.Bytes, 8) + 8), writer.Services.PageAllocator.AllocatePageAsync, writer.Pager.WritePageAsync, writer.Services.PageAllocator.DeallocatePageAsync, writer.Database.Format.WritesTDefFreeSpace, Ct);
            foreach ((long page, byte[] original) in pages)
            {
                byte[] actual = await writer.Database.Pages.ReadPageCopyAsync(page, Ct);
                if (page == root && writer.Database.Format.WritesTDefFreeSpace)
                {
                    actual[2] = original[2];
                    actual[3] = original[3];
                }

                Assert.Equal(original, actual);
            }

            NativeSnapshot before = await SnapshotAsync(writer, root);
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
                {
                    await writer.AddColumnAsync("Native", new ColumnDefinition("Added", typeof(int)), Ct);
                    AssertPreserved(before, await SnapshotAsync(writer, root));
                    await writer.RenameColumnAsync("Native", "Label", "Renamed", Ct);
                    AssertPreserved(before, await SnapshotAsync(writer, root));
                    await writer.DropColumnAsync("Native", "Added", Ct);
                    AssertPreserved(before, await SnapshotAsync(writer, root));
                },
                Ct);
        }

        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(session.SourcePath, cancellationToken: Ct))
        {
            Assert.Equal(["Native", "Parent"], (await reader.ListTablesAsync(Ct)).Order(StringComparer.Ordinal));
            using DataTable rows = await reader.ReadTableAsync("Native", cancellationToken: Ct);
            DataRow row = Assert.Single(rows.AsEnumerable());
            Assert.Equal("native payload", row["Renamed"]);
            Assert.Equal(10, row["ContinuationColumn0"]);
            Assert.Single(await reader.SeekRowsAsync("Native", "PK_Native", [1], Ct).ToListAsync(Ct));
            Assert.Single(await reader.SeekRowsAsync("Native", "IX_Label", ["native payload"], Ct).ToListAsync(Ct));
            Assert.Single(await reader.ListRelationshipsAsync(Ct), relationship => relationship.Name == "NativeParent");
        }

        const string verify = """
            if ($db.TableDefs('Native').Fields.Count -ne 202) { throw 'Lost native columns.' }
            if ($db.Relations('NativeParent').Table -ne 'Parent') { throw 'Lost native relationship.' }
            foreach($table in $db.TableDefs) {
                if($table.Name -notlike 'MSys*' -and $table.Name -notin @('Native','Parent')) { throw ('Unexpected temporary table: '+$table.Name) }
            }
            $rs = $db.OpenRecordset('Native',1)
            try {
                $rs.Index='PK_Native'; $rs.Seek('=',1)
                if ($rs.NoMatch -or $rs.Fields('Renamed').Value -ne 'native payload') { throw 'Lost native primary-key value.' }
                $rs.Index='IX_Label'; $rs.Seek('=','native payload')
                if ($rs.NoMatch -or $rs.Fields('Id').Value -ne 1) { throw 'Lost native text index.' }
            } finally { $rs.Close() }
            $refused=$false
            try { $db.Execute('INSERT INTO Native (Id) VALUES (1)',128) } catch { if(($_.Exception.HResult -band 65535) -ne 3022){throw}; $refused=$true }
            if(!$refused){throw 'Native uniqueness accepted a duplicate.'}
            $refused=$false
            try { $db.Execute('INSERT INTO Native (Id,ContinuationColumn0) VALUES (3,999)',128) } catch { if(($_.Exception.HResult -band 65535) -ne 3201){throw}; $refused=$true }
            if(!$refused){throw 'Native relationship accepted a missing parent.'}
            """;
        string verificationScript = $$"""
            $db = $engine.OpenDatabase({{source}})
            try {
                {{verify}}
                $db.Execute("INSERT INTO [Native] ([Id],[Renamed],[ContinuationColumn0]) VALUES (2,'after rewrite',10)", 128)
                $db.Execute("UPDATE [Native] SET [Renamed]='updated' WHERE [Id]=2", 128)
                $db.Execute('DELETE FROM [Native] WHERE [Id]=2',128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{compacted}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            $db = $engine.OpenDatabase({{compacted}})
            try {
                {{verify}}
                $db.Execute("INSERT INTO [Native] ([Id],[Renamed],[ContinuationColumn0]) VALUES (2,'after compact',10)", 128)
            } finally { $db.Close() }
            Write-Output 'NATIVE_CHAIN_COMPACT_OK'
            """;
        (exitCode, stdout, stderr) = RunNative(session, format, verificationScript);
        Assert.True(exitCode == 0, $"Native reopen/write/compact failed: {stdout}\n{stderr}");
        Assert.Contains("NATIVE_CHAIN_COMPACT_OK", stdout, StringComparison.Ordinal);
    }

    private static async Task<NativeSnapshot> SnapshotAsync(AccessWriter writer, long root)
    {
        LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(root, Ct);
        int used = checked(Ri32(chain.Bytes, 8) + 8);
        TDefImage image = Assert.IsType<TDefImage>(TDefCodec.Parse(writer.Database.Format, chain.Bytes));
        var headers = new List<uint>();
        foreach (long page in chain.PageNumbers.Skip(1))
        {
            headers.Add(Ru32(await writer.Database.Pages.ReadPageCopyAsync(page, Ct), 0));
        }

        byte[] header = writer.Database.Format.IsJet3
            ? chain.Bytes.AsSpan(1, 3).ToArray()
            : [chain.Bytes[1], .. chain.Bytes.AsSpan(24, 4).ToArray(), .. chain.Bytes.AsSpan(32, 8).ToArray()];
        return new NativeSnapshot([.. chain.PageNumbers], header, chain.Bytes.AsSpan(used - 10, 10).ToArray(), [.. headers], Assert.Single(image.Columns, column => column.Name is "Label" or "Renamed").TextSortOrder, used, chain.Bytes);
    }

    private static void AssertPreserved(NativeSnapshot before, NativeSnapshot after)
    {
        Assert.Equal(before.Pages, after.Pages);
        Assert.Equal(before.Header, after.Header);
        Assert.Equal(before.Trailer, after.Trailer);
        Assert.Equal(before.ContinuationHeaders, after.ContinuationHeaders);
        Assert.Equal(before.SortOrder, after.SortOrder);
        int paddingStart = Math.Max(before.UsedLength, after.UsedLength);
        Assert.Equal(before.Bytes.AsSpan(paddingStart).ToArray(), after.Bytes.AsSpan(paddingStart).ToArray());
    }

    private static (int ExitCode, string Stdout, string Stderr) RunNative(AccessRoundTripSession session, DatabaseFormat format, string script)
    {
        if (format == DatabaseFormat.Jet3Mdb)
        {
            JetDatabaseWriter.TestSupport.PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(2));
            return (result.ExitCode, result.StandardOutput, result.StandardError);
        }

        AccessRoundTripEnvironment.CompactResult native = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
        return (native.ExitCode, native.StdOut, native.StdErr);
    }

    private sealed record NativeSnapshot(long[] Pages, byte[] Header, byte[] Trailer, uint[] ContinuationHeaders, JetDatabaseWriter.Indexes.Collation.TextSortOrder SortOrder, int UsedLength, byte[] Bytes);
}
