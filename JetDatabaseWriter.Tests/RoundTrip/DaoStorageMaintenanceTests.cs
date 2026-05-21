namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Collections.Generic;
using System.Data;
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

        DataRow replacementRow = table.AsEnumerable().Single(row => Convert.ToInt32(row["Id"]) == 2);
        byte[] actualReplacement = Assert.IsType<byte[]>(replacementRow["Blob"]);
        Assert.Equal(replacementLargePayload, actualReplacement);
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

            await CreateWideTDefTableAsync(writer, WideParent, [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync(WideChild, [new("Id", typeof(int)), new("ParentId", typeof(int))], TestContext.Current.CancellationToken);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition(WideRelationship, WideParent, "Id", WideChild, "ParentId"),
                TestContext.Current.CancellationToken);
            await writer.DropRelationshipAsync(WideRelationship, TestContext.Current.CancellationToken);
        }

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
        CancellationToken cancellationToken)
    {
        const int ColumnCount = 200;
        const int IndexCount = 30;

        var columns = new List<ColumnDefinition>(ColumnCount);
        columns.AddRange(leadingColumns);
        for (int columnOrdinal = columns.Count; columnOrdinal < ColumnCount; columnOrdinal++)
        {
            columns.Add(new ColumnDefinition($"C{columnOrdinal:D3}", typeof(int)));
        }

        var indexes = new List<IndexDefinition>(IndexCount);
        for (int indexOrdinal = 0; indexOrdinal < IndexCount; indexOrdinal++)
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
