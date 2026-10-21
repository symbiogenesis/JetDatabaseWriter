namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;
using static JetDatabaseWriter.Tests.ComplexColumns.ComplexColumnTestSupport;

/// <summary>Append-only Memo schema edits preserve native history bindings and identities.</summary>
public sealed class AppendOnlyHistorySchemaTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Rename and nonfinal drop retain surviving versions; dropping the final Memo removes the hidden infrastructure.</summary>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task SharedMemoRenameAndDrop_NativeReadAndCompactPreserveSurvivingHistory()
    {
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateDaoAccdbAsync(Ct, "JetDatabaseWriter.Tests.HistorySchema");
        const string create = """
            $db.Execute('CREATE TABLE HistoryControl (Id LONG, Notes MEMO, More MEMO)',128)
            foreach ($name in @('Notes','More')) {
                $field=$db.TableDefs('HistoryControl').Fields($name)
                $field.AppendOnly=$true
            }
            """;
        AssertDaoSuccess(session.RunDaoDatabaseScript(session.SourcePath, create, TimeSpan.FromMinutes(1)));
        AssertDaoSuccess(session.RunDaoDatabaseScript(session.SourcePath, "$db.Execute(\"INSERT INTO HistoryControl (Id,Notes,More) VALUES (1,'initial note','initial more')\",128)", TimeSpan.FromMinutes(1)));
        ComplexColumnInfo history;
        int retainedId;
        await using (AccessReader reader = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            history = Assert.Single(await reader.GetComplexColumnsAsync("HistoryControl", Ct), column => column.Kind == ComplexColumnKind.VersionHistory);
            retainedId = Assert.Single(await reader.GetMultiValueItemsAsync("HistoryControl", history.ColumnName, Ct), item => item.MemoColumnName == "Notes").Id;
        }

        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await writer.RenameColumnAsync("HistoryControl", "Notes", "Renamed", Ct);
            await writer.UpdateRowsAsync("HistoryControl", RowCriteria.Where("Id", 1), new RowValues { ["Renamed"] = "after rename" }, Ct);
            await writer.DropColumnAsync("HistoryControl", "More", Ct);
            await writer.UpdateRowsAsync("HistoryControl", RowCriteria.Where("Id", 1), new RowValues { ["Renamed"] = "after drop" }, Ct);
        }

        int[] retainedIds;
        await using (AccessReader reader = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            MultiValueItem[] versions = [.. await reader.GetMultiValueItemsAsync("HistoryControl", history.ColumnName, Ct)];
            Assert.Equal(["initial note", "after rename", "after drop"], versions.Select(item => item.Value));
            Assert.All(versions, item => Assert.Equal("Renamed", item.MemoColumnName));
            Assert.Equal(retainedId, versions[0].Id);
            retainedIds = versions.Select(item => item.Id).ToArray();
            Assert.Equal(4, await reader.Rows(history.FlatTableName, cancellationToken: Ct).CountAsync(Ct));
            Assert.DoesNotContain(await reader.GetColumnMetadataAsync(history.FlatTableName, Ct), column => column.Name == "More");
        }

        const string nativeRead = """
            if (!$db.TableDefs('HistoryControl').Fields('Renamed').AppendOnly) { throw 'AppendOnly lost after rename.' }
            $rs=$db.OpenRecordset('SELECT * FROM HistoryControl')
            try {
                if ($rs.Fields('Renamed').Value -cne 'after drop') { throw 'Memo value changed.' }
                if ($rs.Fields.Count -ne 2) { throw 'Dropped Memo still visible.' }
            } finally { $rs.Close() }
            """;
        AssertDaoSuccess(session.RunDaoDatabaseScriptThenCompact(nativeRead, TimeSpan.FromMinutes(2)));
        await using (AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            MultiValueItem[] compacted = [.. await reader.GetMultiValueItemsAsync("HistoryControl", history.ColumnName, Ct)];
            Assert.Equal(retainedIds, compacted.Select(item => item.Id));
            Assert.Equal(["initial note", "after rename", "after drop"], compacted.Select(item => item.Value));
            Assert.All(compacted, item => Assert.Equal("Renamed", item.MemoColumnName));
        }

        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await writer.DropColumnAsync("HistoryControl", "Renamed", Ct);
        }

        await using (AccessReader reader = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            Assert.Empty(await reader.GetComplexColumnsAsync("HistoryControl", Ct));
            Assert.Equal(["Id"], (await reader.GetColumnMetadataAsync("HistoryControl", Ct)).Select(column => column.Name));
            HashSet<string> objects = await ReadObjectNamesAsync(reader);
            Assert.DoesNotContain(history.FlatTableName, objects);
            Assert.DoesNotContain(history.ComplexTypeName, objects);
        }

        const string finalRead = """
            $rs=$db.OpenRecordset('HistoryControl')
            try { if ($rs.Fields.Count -ne 1 -or $rs.Fields('Id').Value -ne 1) { throw 'Final Memo drop damaged parent.' } } finally { $rs.Close() }
            """;
        AssertDaoSuccess(session.RunDaoDatabaseScriptThenCompact(finalRead, TimeSpan.FromMinutes(2)));
        await using AccessReader final = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct);
        Assert.Empty(await final.GetComplexColumnsAsync("HistoryControl", Ct));
        Assert.Equal(["Id"], (await final.GetColumnMetadataAsync("HistoryControl", Ct)).Select(column => column.Name));
    }

    /// <summary>Rolling back paired schema edits restores every original history and catalog byte.</summary>
    [Fact]
    public async Task MemoRenameAndDrop_RollbackRestoresOriginalDatabase()
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.ComplexDataTestV2007);
        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(stream, WriteMode.Direct))
        await using (JetTransaction transaction = await writer.BeginTransactionAsync(Ct))
        {
            await writer.RenameColumnAsync("Table1", "append-memo-data", "Renamed", Ct);
            await writer.DropColumnAsync("Table1", "Renamed", Ct);
            await transaction.RollbackAsync(Ct);
        }

        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UnverifiedTemplateOwnership_RefusesSchemaChangeWithoutMutation(bool drop)
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.ComplexDataTestV2007);
        ComplexColumnInfo history;
        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            history = Assert.Single(await reader.GetComplexColumnsAsync("Table1", Ct), column => column.Kind == ComplexColumnKind.VersionHistory);
        }

        await using (AccessWriter harness = await OpenWriterAsync(stream))
        {
            TableDef objects = await harness.Database.TableDefs.ReadRequiredTableDefAsync(2, "MSysObjects", Ct);
            int idIndex = objects.FindColumnIndex("Id");
            ColumnInfo flags = objects.FindColumn("Flags")!;
            LocatedRow target = Assert.Single(await harness.Services.Snapshots.ReadRowsAsync(2, Ct), row =>
                row.Values[idIndex] is int id && CatalogValueReader.TdefPageFromId(id) == CatalogValueReader.TdefPageFromId(history.ComplexTypeObjectId));
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(target.Location.DataPageNumber, Ct);
            Assert.True(RowDecodePlan.TryParseRowLayout(harness.Database.Format.RowFields, page, target.Location.RowStart, target.Location.RowSize, objects.HasVarColumns, out RowLayout layout));
            ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(harness.Database.Format.RowFields, page, target.Location.RowStart, target.Location.RowSize, layout, flags);
            Assert.Equal(4, slice.DataLen);
            BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(target.Location.RowStart + slice.DataStart, 4), 0);
            await harness.Pager.WritePageAsync(target.Location.DataPageNumber, page, Ct);
        }

        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            {
                if (drop)
                {
                    await writer.DropColumnAsync("Table1", "append-memo-data", Ct);
                }
                else
                {
                    await writer.RenameColumnAsync("Table1", "append-memo-data", "Renamed", Ct);
                }
            });
            Assert.Equal(JetErrorCode.CorruptComplexColumn, error.ErrorCode);
        }

        Assert.Equal(before, stream.ToArray());
    }

    private static async Task<HashSet<string>> ReadObjectNamesAsync(AccessReader reader)
    {
        int nameIndex = (await reader.GetColumnMetadataAsync("MSysObjects", Ct)).Single(column => column.Name == "Name").Ordinal;
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await foreach (object[] row in reader.Rows("MSysObjects", cancellationToken: Ct))
        {
            names.Add(Assert.IsType<string>(row[nameIndex]));
        }

        return names;
    }

    private static void AssertDaoSuccess(AccessRoundTripEnvironment.CompactResult result)
        => Assert.True(result.ExitCode == 0, $"DAO failed: {result.StdOut}\n{result.StdErr}");
}
