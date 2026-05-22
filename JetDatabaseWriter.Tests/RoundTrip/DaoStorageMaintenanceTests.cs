namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// DAO CompactDatabase coverage for writer storage-maintenance paths that
/// reuse or scrub pages instead of leaving all old storage append-only.
/// </summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoStorageMaintenanceTests
{
    private const int MarkerLength = 16;
    private const int IndexRows = 800;
    private static readonly TimeSpan CompactTimeout = TimeSpan.FromMinutes(3);

    [Fact(Skip = "Known fresh ACCDB bootstrap gap: DAO CompactDatabase still rejects writer-created files as an unrecognized database format.")]
    public async Task FreshWriterCreatedDatabase_SurvivesCompactAndRepair()
    {
        await using AccessRoundTripSession session = AccessRoundTripSession.CreateEmpty(compactTimeout: CompactTimeout);

        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            session.SourcePath,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "SM_FreshBootstrap",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true, IsNullable = false },
                    new ColumnDefinition("Label", typeof(string), maxLength: 80),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(
                "SM_FreshBootstrap",
                new[]
                {
                    new object[] { 1, "fresh-one" },
                    new object[] { 2, "fresh-two" },
                },
                TestContext.Current.CancellationToken);
        }

        session.RunDaoCompact();

        await using AccessReader reader = await AccessReader.OpenAsync(
            session.CompactedPath,
            new AccessReaderOptions { UseLockFile = false },
            cancellationToken: TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Contains("SM_FreshBootstrap", tables);

        DataTable table = await reader.ReadDataTableAsync("SM_FreshBootstrap", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, table.Rows.Count);
        Assert.Contains(
            table.AsEnumerable(),
            row => Convert.ToInt32(row["Id"], CultureInfo.InvariantCulture) == 2 && string.Equals(SafeString(row, "Label"), "fresh-two", StringComparison.Ordinal));
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task SecureErase_RowGapAndOldLvalChain_SurviveCompactAndRepair()
    {
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateFromNorthwindAsync(
            TestContext.Current.CancellationToken,
            compactTimeout: CompactTimeout);

        byte[] deletedPayload = BuildPayload(96, 0x21);
        byte[] originalLargePayload = BuildPayload(9000, 0x31);
        byte[] replacementLargePayload = BuildPayload(9000, 0x41);
        byte[] deletedMarker = MarkerOf(deletedPayload);
        byte[] originalLargeMarker = MarkerOf(originalLargePayload);
        byte[] replacementLargeMarker = MarkerOf(replacementLargePayload);

        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            session.SourcePath,
            new AccessWriterOptions
            {
                UseLockFile = false,
                SecureEraseMode = SecureEraseMode.DeletedRowsAndFreedPages,
            },
            TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "SM_SecureErase",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true, IsNullable = false },
                    new ColumnDefinition("Blob", typeof(byte[])),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(
                "SM_SecureErase",
                new[]
                {
                    new object[] { 1, deletedPayload },
                    new object[] { 2, originalLargePayload },
                    new object[] { 3, BuildPayload(96, 0x51) },
                },
                TestContext.Current.CancellationToken);

            int updated = await writer.UpdateRowsAsync(
                "SM_SecureErase",
                "Id",
                2,
                new Dictionary<string, object> { ["Blob"] = replacementLargePayload },
                TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);

            int deleted = await writer.DeleteRowsAsync(
                "SM_SecureErase",
                "Id",
                1,
                TestContext.Current.CancellationToken);
            Assert.Equal(1, deleted);
        }

        byte[] sourceBytes = await File.ReadAllBytesAsync(session.SourcePath, TestContext.Current.CancellationToken);
        Assert.False(ContainsSequence(sourceBytes, deletedMarker));
        Assert.False(ContainsSequence(sourceBytes, originalLargeMarker));
        Assert.True(ContainsSequence(sourceBytes, replacementLargeMarker));

        session.RunDaoCompact();

        byte[] compactedBytes = await File.ReadAllBytesAsync(session.CompactedPath, TestContext.Current.CancellationToken);
        Assert.False(ContainsSequence(compactedBytes, deletedMarker));
        Assert.False(ContainsSequence(compactedBytes, originalLargeMarker));

        await using AccessReader reader = await AccessReader.OpenAsync(
            session.CompactedPath,
            new AccessReaderOptions { UseLockFile = false },
            cancellationToken: TestContext.Current.CancellationToken);
        DataTable table = await reader.ReadDataTableAsync("SM_SecureErase", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, table.Rows.Count);

        DataRow replacementRow = table.AsEnumerable().Single(row => Convert.ToInt32(row["Id"], CultureInfo.InvariantCulture) == 2);
        byte[] actualReplacement = Assert.IsType<byte[]>(replacementRow["Blob"]);
        Assert.Equal(replacementLargePayload, actualReplacement);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task RelationshipRenameOnMultiPageTDef_SurvivesCompactAndRepair()
    {
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateFromNorthwindAsync(
            TestContext.Current.CancellationToken,
            compactTimeout: CompactTimeout);

        const string Parent = "SM_RenameParent";
        const string WideChild = "SM_RenameWideChild";
        const string OldRelationship = "SM_FK_RenameWide_Old";
        const string NewRelationship = "SM_FK_RenameWide_New";

        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            session.SourcePath,
            new AccessWriterOptions { UseLockFile = false },
            TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                Parent,
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true, IsNullable = false }],
                TestContext.Current.CancellationToken);
            await CreateWideTDefTableAsync(
                writer,
                WideChild,
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true, IsNullable = false },
                    new ColumnDefinition("ParentId", typeof(int)) { IsNullable = false },
                ],
                columnCount: 40,
                indexCount: 30,
                cancellationToken: TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(Parent, [1], TestContext.Current.CancellationToken);

            object[] wideChildRow = Enumerable.Range(0, 40).Select(value => (object)value).ToArray();
            wideChildRow[0] = 1;
            wideChildRow[1] = 1;
            await writer.InsertRowAsync(WideChild, wideChildRow, TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(OldRelationship, Parent, "Id", WideChild, "ParentId")
                {
                    EnforceReferentialIntegrity = true,
                },
                TestContext.Current.CancellationToken);

            await writer.RenameRelationshipAsync(OldRelationship, NewRelationship, TestContext.Current.CancellationToken);
        }

        int widePagesAfterRename = await CountTDefChainPagesAsync(WideChild, session.SourcePath, TestContext.Current.CancellationToken);
        Assert.True(widePagesAfterRename > 1, $"Expected {WideChild} to exercise a multi-page TDEF chain before DAO compact.");

        session.RunDaoCompact();

        await using AccessReader reader = await AccessReader.OpenAsync(
            session.CompactedPath,
            new AccessReaderOptions { UseLockFile = false },
            cancellationToken: TestContext.Current.CancellationToken);

        DataTable wideChildRows = await reader.ReadDataTableAsync(WideChild, cancellationToken: TestContext.Current.CancellationToken);
        IReadOnlyList<IndexMetadata> childIndexes = await reader.ListIndexesAsync(WideChild, TestContext.Current.CancellationToken);
        DataTable relationships = await reader.ReadDataTableAsync("MSysRelationships", cancellationToken: TestContext.Current.CancellationToken);
        string indexSummary = string.Join(", ", childIndexes.Select(index => $"{index.Name}:{index.Kind}"));
        string relationshipSummary = string.Join(", ", relationships.AsEnumerable()
            .Select(row => SafeString(row, "szRelationship"))
            .Where(name => string.Equals(name, OldRelationship, StringComparison.Ordinal) || string.Equals(name, NewRelationship, StringComparison.Ordinal)));

        Assert.Equal(1, wideChildRows.Rows.Count);
        Assert.True(
            childIndexes.Any(index => index.Kind == IndexKind.ForeignKey && index.Name == NewRelationship),
            $"Expected compacted {WideChild} to retain renamed FK index {NewRelationship}. Indexes=[{indexSummary}], relationships=[{relationshipSummary}].");
        Assert.DoesNotContain(childIndexes, index => index.Name == OldRelationship);
        Assert.Contains(relationships.AsEnumerable(), row => string.Equals(SafeString(row, "szRelationship"), NewRelationship, StringComparison.Ordinal));
        Assert.DoesNotContain(relationships.AsEnumerable(), row => string.Equals(SafeString(row, "szRelationship"), OldRelationship, StringComparison.Ordinal));
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task IndexRebuildAndShortenedTDefChain_SurviveCompactAndRepair()
    {
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateFromNorthwindAsync(
            TestContext.Current.CancellationToken,
            compactTimeout: CompactTimeout);

        const string IndexedParent = "SM_IndexParent";
        const string IndexedChild = "SM_IndexChild";
        const string IndexedRelationship = "SM_FK_IndexChild_Parent";
        const string WideParent = "SM_WideParent";
        const string WideChild = "SM_WideChild";
        const string WideRelationship = "SM_FK_WideChild_Parent";

        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            session.SourcePath,
            new AccessWriterOptions { UseLockFile = false },
            TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                IndexedParent,
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true, IsNullable = false },
                    new ColumnDefinition("Label", typeof(string), maxLength: 64),
                ],
                TestContext.Current.CancellationToken);

            await writer.CreateTableAsync(
                IndexedChild,
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true, IsNullable = false },
                    new ColumnDefinition("ParentId", typeof(int)) { IsNullable = false },
                    new ColumnDefinition("Label", typeof(string), maxLength: 64),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(IndexedParent, BuildIndexedParentRows(), TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync(IndexedChild, BuildIndexedChildRows(), TestContext.Current.CancellationToken);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(IndexedRelationship, IndexedParent, "Id", IndexedChild, "ParentId")
                {
                    EnforceReferentialIntegrity = true,
                },
                TestContext.Current.CancellationToken);

            await CreateWideTDefTableAsync(
                writer,
                WideParent,
                [new("Id", typeof(int))],
                columnCount: 115,
                indexCount: 0,
                cancellationToken: TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(WideChild, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(WideRelationship, WideParent, "Id", WideChild, "ParentId"),
                TestContext.Current.CancellationToken);
        }

        int widePagesWithRelationship = await CountTDefChainPagesAsync(WideParent, session.SourcePath, TestContext.Current.CancellationToken);

        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            session.SourcePath,
            new AccessWriterOptions { UseLockFile = false },
            TestContext.Current.CancellationToken))
        {
            await writer.DropRelationshipAsync(WideRelationship, TestContext.Current.CancellationToken);
        }

        int widePagesAfterDrop = await CountTDefChainPagesAsync(WideParent, session.SourcePath, TestContext.Current.CancellationToken);
        Assert.True(
            widePagesAfterDrop < widePagesWithRelationship,
            $"Expected {WideParent} TDEF chain to shorten after dropping {WideRelationship}; before={widePagesWithRelationship}, after={widePagesAfterDrop}.");

        session.RunDaoCompact();

        await using AccessReader reader = await AccessReader.OpenAsync(
            session.CompactedPath,
            new AccessReaderOptions { UseLockFile = false },
            cancellationToken: TestContext.Current.CancellationToken);

        DataTable child = await reader.ReadDataTableAsync(IndexedChild, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(IndexRows, child.Rows.Count);

        IReadOnlyList<IndexMetadata> childIndexes = await reader.ListIndexesAsync(IndexedChild, TestContext.Current.CancellationToken);
        Assert.Contains(childIndexes, index => index.Kind == IndexKind.ForeignKey && index.Name == IndexedRelationship);

        IReadOnlyList<IndexMetadata> wideParentIndexes = await reader.ListIndexesAsync(WideParent, TestContext.Current.CancellationToken);
        IReadOnlyList<IndexMetadata> wideChildIndexes = await reader.ListIndexesAsync(WideChild, TestContext.Current.CancellationToken);
        Assert.DoesNotContain(wideParentIndexes, index => index.Kind == IndexKind.ForeignKey || index.Name == WideRelationship);
        Assert.DoesNotContain(wideChildIndexes, index => index.Kind == IndexKind.ForeignKey || index.Name == WideRelationship);

        DataTable relationships = await reader.ReadDataTableAsync("MSysRelationships", cancellationToken: TestContext.Current.CancellationToken);
        Assert.DoesNotContain(relationships.AsEnumerable(), row => string.Equals(SafeString(row, "szRelationship"), WideRelationship, StringComparison.Ordinal));
    }

    private static object[][] BuildIndexedParentRows()
    {
        var rows = new object[IndexRows][];
        for (int row = 0; row < rows.Length; row++)
        {
            rows[row] = [row + 1, $"Parent_{row + 1:D4}"];
        }

        return rows;
    }

    private static object[][] BuildIndexedChildRows()
    {
        var rows = new object[IndexRows][];
        for (int row = 0; row < rows.Length; row++)
        {
            int id = row + 1;
            rows[row] = [id, id, $"Child_{id:D4}"];
        }

        return rows;
    }

    private static async ValueTask CreateWideTDefTableAsync(
        AccessWriter writer,
        string tableName,
        IReadOnlyList<ColumnDefinition> leadingColumns,
        int columnCount,
        int indexCount,
        CancellationToken cancellationToken)
    {
        var columns = new List<ColumnDefinition>(columnCount);
        columns.AddRange(leadingColumns);
        for (int columnOrdinal = columns.Count; columnOrdinal < columnCount; columnOrdinal++)
        {
            columns.Add(new ColumnDefinition($"C{columnOrdinal:D3}", typeof(int)));
        }

        var indexes = new List<IndexDefinition>(indexCount);
        for (int indexOrdinal = 0; indexOrdinal < indexCount; indexOrdinal++)
        {
            int indexedColumnOrdinal = leadingColumns.Count + indexOrdinal;
            indexes.Add(new IndexDefinition($"IX_{indexOrdinal:D2}", $"C{indexedColumnOrdinal:D3}"));
        }

        await writer.CreateTableAsync(tableName, columns, indexes, cancellationToken);
    }

    private static byte[] BuildPayload(int length, byte markerByte)
    {
        var payload = new byte[length];
        for (int byteIndex = 0; byteIndex < payload.Length; byteIndex++)
        {
            unchecked
            {
                payload[byteIndex] = (byte)(markerByte + (byteIndex * 37) + (byteIndex >> 2));
            }
        }

        byte[] marker = [0xCA, 0xFE, markerByte, 0xD0, 0x0D, 0xC0, 0xDE, 0x71, 0x5A, 0xB1, 0x6C, 0x3E, 0x99, 0x24, 0x42, 0x18];
        Buffer.BlockCopy(marker, 0, payload, 0, marker.Length);
        return payload;
    }

    private static byte[] MarkerOf(byte[] payload)
        => payload.AsSpan(0, MarkerLength).ToArray();

    private static bool ContainsSequence(byte[] bytes, byte[] marker)
        => bytes.AsSpan().IndexOf(marker) >= 0;

    private static async ValueTask<int> CountTDefChainPagesAsync(string tableName, string databasePath, CancellationToken cancellationToken)
    {
        const int PageSize = 4096;

        int tdefPageNumber;
        await using (AccessReader reader = await AccessReader.OpenAsync(
            databasePath,
            new AccessReaderOptions { UseLockFile = false },
            cancellationToken: cancellationToken))
        {
            DataTable objects = await reader.ReadDataTableAsync("MSysObjects", cancellationToken: cancellationToken);
            DataRow row = objects.AsEnumerable().Single(r => string.Equals(SafeString(r, "Name"), tableName, StringComparison.Ordinal));
            tdefPageNumber = Convert.ToInt32(row["Id"], CultureInfo.InvariantCulture);
        }

        byte[] bytes = await File.ReadAllBytesAsync(databasePath, cancellationToken);
        var seen = new HashSet<int>();
        int count = 0;
        int pageNumber = tdefPageNumber;
        while (pageNumber > 0 && seen.Add(pageNumber))
        {
            int offset = checked(pageNumber * PageSize);
            if (offset < 0 || offset + PageSize > bytes.Length || bytes[offset] != 0x02)
            {
                break;
            }

            count++;
            pageNumber = BitConverter.ToInt32(bytes, offset + 4);
        }

        return count;
    }

    private static string SafeString(DataRow row, string column)
    {
        if (!row.Table.Columns.Contains(column))
        {
            return string.Empty;
        }

        object value = row[column];
        return value == DBNull.Value ? string.Empty : value.ToString() ?? string.Empty;
    }
}
