namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Microsoft ACE controls for append-only assignment semantics and writer interoperability.</summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class AppendOnlyHistoryDaoTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Explicit identical, empty and Null assignments create versions; unrelated edits and omitted Null values do not.</summary>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task NativeAssignmentControls_LibraryReadsExactVersions()
    {
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateDaoAccdbAsync(Ct, "JetDatabaseWriter.Tests.AppendOnlyControls");
        const string create = """
            $db.Execute('CREATE TABLE HistoryControl (Id LONG, Notes MEMO, Other LONG)',128)
            $field = $db.TableDefs('HistoryControl').Fields('Notes')
            $field.AppendOnly = $true
            if (!$field.AppendOnly) { throw 'AppendOnly not enabled.' }
            """;
        AssertDaoSuccess(session.RunDaoDatabaseScript(session.SourcePath, create, TimeSpan.FromMinutes(1)));
        const string mutate = """
            $db.Execute("INSERT INTO HistoryControl (Id, Notes, Other) VALUES (1,'first',1)",128)
            $db.Execute('UPDATE HistoryControl SET Other=2 WHERE Id=1',128)
            $db.Execute("UPDATE HistoryControl SET Notes='first' WHERE Id=1",128)
            $db.Execute("UPDATE HistoryControl SET Notes='' WHERE Id=1",128)
            $db.Execute('UPDATE HistoryControl SET Notes=Null WHERE Id=1',128)
            $db.Execute("UPDATE HistoryControl SET Notes='later' WHERE Id=1",128)
            $db.Execute('INSERT INTO HistoryControl (Id) VALUES (2)',128)
            $db.Execute("INSERT INTO HistoryControl (Id,Notes) VALUES (3,'')",128)
            $db.Execute('INSERT INTO HistoryControl (Id,Notes) VALUES (4,Null)',128)
            """;
        AssertDaoSuccess(session.RunDaoDatabaseScriptThenCompact(mutate, TimeSpan.FromMinutes(2)));

        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct);
        ComplexColumnInfo history = Assert.Single(await reader.GetComplexColumnsAsync("HistoryControl", Ct), column => column.Kind == ComplexColumnKind.VersionHistory);
        MultiValueItem[] versions = [.. await reader.GetMultiValueItemsAsync("HistoryControl", history.ColumnName, Ct)];
        Assert.Equal(["first", "first", "\0", "\0", "later", "\0", "\0"], versions.Select(item => item.Value));
        Assert.Equal([1, 1, 1, 1, 1, 3, 4], versions.Select(item => item.ConceptualTableId));
        Assert.All(versions, item => Assert.Equal("Notes", item.MemoColumnName));
        Assert.All(versions, item => Assert.NotNull(item.Modified));
    }

    /// <summary>Native ACE reads and updates library-written Memo values, then preserves every version through compact.</summary>
    /// <param name="fixture">The Microsoft-authored source file.</param>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [MemberData(nameof(TestDatabases.ComplexData), MemberType = typeof(TestDatabases))]
    public async Task LibraryMemoWrites_NativeReadUpdateAndCompactPreserveHistory(string fixture)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.AppendOnlyRoundTrip");
        await File.WriteAllBytesAsync(session.SourcePath, await File.ReadAllBytesAsync(fixture, Ct), Ct);
        string longVersion = new('L', 5000);
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await writer.UpdateRowsAsync("Table1", RowCriteria.Where("id", "row3"), new RowValues { ["append-memo-data"] = longVersion }, Ct);
            await writer.InsertRowAsync("Table1", new RowValues { ["id"] = "new-row", ["append-memo-data"] = "library insert" }, Ct);
            await writer.UpdateRowsAsync("Table1", RowCriteria.Where("id", "row2"), new RowValues { ["append-memo-data"] = null }, Ct);
        }

        const string native = """
            $rs = $db.OpenRecordset("SELECT [append-memo-data] FROM Table1 WHERE id='row3'")
            try {
                if ($rs.Fields(0).Value -cne ('L' * 5000)) { throw 'Library Memo changed.' }
                $rs.Edit()
                $rs.Fields(0).Value='native revision'
                $rs.Update()
            } finally { $rs.Close() }
            $rs=$db.OpenRecordset("SELECT [append-memo-data] FROM Table1 WHERE id='new-row'")
            try { if ($rs.Fields(0).Value -cne 'library insert') { throw 'Library insert changed.' } } finally { $rs.Close() }
            $rs=$db.OpenRecordset("SELECT [append-memo-data] FROM Table1 WHERE id='row2'")
            try { if ($rs.Fields(0).Value -isnot [DBNull]) { throw 'Library Null changed.' } } finally { $rs.Close() }
            Write-Output 'APPEND_ONLY_NATIVE_OK'
            """;
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScriptThenCompact(native, TimeSpan.FromMinutes(2));
        AssertDaoSuccess(result);
        Assert.Contains("APPEND_ONLY_NATIVE_OK", result.StdOut, StringComparison.Ordinal);
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct);
        ComplexColumnInfo history = Assert.Single(await reader.GetComplexColumnsAsync("Table1", Ct), column => column.Kind == ComplexColumnKind.VersionHistory);
        MultiValueItem[] versions = [.. await reader.GetMultiValueItemsAsync("Table1", history.ColumnName, Ct)];
        Assert.Equal(["row3-memo", "row3-memo-revised", "row3-memo-again", longVersion, "native revision"], versions.Where(item => item.ConceptualTableId == 3).Select(item => item.Value));
        Assert.Equal(["row2-memo", "\0"], versions.Where(item => item.ConceptualTableId == 2).Select(item => item.Value));
        Assert.Single(versions, item => Equals(item.Value, "library insert"));
        Assert.All(versions, item => Assert.Equal("append-memo-data", item.MemoColumnName));
        Assert.All(versions, item => Assert.NotNull(item.Modified));
    }

    /// <summary>Multiple append-only fields share one flat table and keep one sparse row per assigned Memo.</summary>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task SharedHistory_LibraryAndNativeAssignmentsKeepEachMemoVersion()
    {
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateDaoAccdbAsync(Ct, "JetDatabaseWriter.Tests.SharedHistoryWriter");
        const string create = """
            $db.Execute('CREATE TABLE HistoryControl (Id LONG, Notes MEMO, More MEMO)',128)
            foreach ($name in @('Notes','More')) {
                $field=$db.TableDefs('HistoryControl').Fields($name)
                $field.AppendOnly=$true
            }
            """;
        AssertDaoSuccess(session.RunDaoDatabaseScript(session.SourcePath, create, TimeSpan.FromMinutes(1)));
        AssertDaoSuccess(session.RunDaoDatabaseScript(session.SourcePath, "$db.Execute(\"INSERT INTO HistoryControl (Id,Notes,More) VALUES (1,'initial note','initial more')\",128)", TimeSpan.FromMinutes(1)));
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await writer.InsertRowAsync("HistoryControl", new RowValues { ["Id"] = 2, ["Notes"] = "library note", ["More"] = "library more" }, Ct);
            await writer.UpdateRowsAsync("HistoryControl", RowCriteria.Where("Id", 1), new RowValues { ["Notes"] = null, ["More"] = string.Empty }, Ct);
        }

        const string native = """
            $rs=$db.OpenRecordset('SELECT * FROM HistoryControl WHERE Id=1')
            try {
                if ($rs.Fields('Notes').Value -isnot [DBNull]) { throw 'Null Memo changed.' }
                if ($rs.Fields('More').Value -cne '') { throw 'Empty Memo changed.' }
            } finally { $rs.Close() }
            $db.Execute("UPDATE HistoryControl SET Notes='native note',More='native more' WHERE Id=2",128)
            """;
        AssertDaoSuccess(session.RunDaoDatabaseScriptThenCompact(native, TimeSpan.FromMinutes(2)));
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct);
        ComplexColumnInfo history = Assert.Single(await reader.GetComplexColumnsAsync("HistoryControl", Ct), column => column.Kind == ComplexColumnKind.VersionHistory);
        MultiValueItem[] versions = [.. await reader.GetMultiValueItemsAsync("HistoryControl", history.ColumnName, Ct)];
        Assert.Equal(["initial note", "initial more", "library note", "library more", "\0", "\0", "native note", "native more"], versions.Select(item => item.Value));
        Assert.Equal(["Notes", "More", "Notes", "More", "Notes", "More", "Notes", "More"], versions.Select(item => item.MemoColumnName));
        Assert.All(versions, item => Assert.NotNull(item.Modified));
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SharedHistoryUniqueIndex_ValidatesTheCombinedStatementBeforeMutation(bool insert)
    {
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateDaoAccdbAsync(Ct, "JetDatabaseWriter.Tests.HistoryUnique");
        const string create = """
            $db.Execute('CREATE TABLE HistoryControl (Id LONG, Notes MEMO, More MEMO)',128)
            foreach ($name in @('Notes','More')) {
                $field=$db.TableDefs('HistoryControl').Fields($name)
                $field.AppendOnly=$true
            }
            $db.Execute('INSERT INTO HistoryControl (Id) VALUES (1)',128)
            """;
        AssertDaoSuccess(session.RunDaoDatabaseScript(session.SourcePath, create, TimeSpan.FromMinutes(1)));
        ComplexColumnInfo history;
        IndexMetadata foreignIndex;
        await using (AccessReader reader = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            history = Assert.Single(await reader.GetComplexColumnsAsync("HistoryControl", Ct));
            foreignIndex = Assert.Single(await reader.ListIndexesAsync(history.FlatTableName, Ct), index => !index.EnforcesUniqueness);
        }

        // A synthetic unique flag isolates cross-batch preflight; DAO hides the flat
        // table from its public TableDefs collection.
        await using (AccessWriter harness = await session.OpenWriterAsync(Ct))
        {
            ResolvedTable flat = (await harness.Services.Snapshots.ResolveTableAsync(history.FlatTableName, Ct))!;
            TDefImage image = (await harness.Database.TableDefs.ReadImageAsync(flat.Entry.TDefPage, Ct))!;
            JetFormat format = harness.Database.Format;
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(flat.Entry.TDefPage, Ct);
            int start = TDefCodec.LocateRealIndexDescriptors(format, page, image.Header.ColumnCount, image.Header.RealIndexCount);
            int offset = format.Index.FlagsAbsoluteOffset(format.Index.RealIdxPhysOffset(start, foreignIndex.RealIndexNumber));
            page[offset] |= Constants.TableDefinition.UniqueIndexFlag;
            await harness.Pager.WritePageAsync(flat.Entry.TDefPage, page, Ct);
        }

        byte[] before = await File.ReadAllBytesAsync(session.SourcePath, Ct);
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            JetConstraintException error = await Assert.ThrowsAsync<JetConstraintException>(async () =>
            {
                var values = new RowValues { ["Notes"] = "first", ["More"] = "second" };
                if (insert)
                {
                    values["Id"] = 2;
                    await writer.InsertRowAsync("HistoryControl", values, Ct);
                }
                else
                {
                    await writer.UpdateRowsAsync("HistoryControl", RowCriteria.Where("Id", 1), values, Ct);
                }
            });
            Assert.Equal(JetErrorCode.UniqueViolation, error.ErrorCode);
        }

        Assert.Equal(before, await File.ReadAllBytesAsync(session.SourcePath, Ct));
    }

    private static void AssertDaoSuccess(AccessRoundTripEnvironment.CompactResult result)
        => Assert.True(result.ExitCode == 0, $"DAO failed: {result.StdOut}\n{result.StdErr}");
}
