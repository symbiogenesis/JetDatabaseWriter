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
    private const int AdvancedIndexRows = 300;
    private const int IndexRows = 800;
    private const int Jet3IndexRows = 260;
    private const int MarkerLength = 16;
    private static readonly TimeSpan CompactTimeout = TimeSpan.FromMinutes(3);

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
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
    public async Task Jet3IndexEmissionAndMaintenance_SurviveCompactAndRepair()
    {
        await using AccessRoundTripSession session = AccessRoundTripSession.CreateEmpty(
            compactTimeout: CompactTimeout,
            databaseExtension: ".mdb");

        await CopyDatabaseAsync(TestDatabases.IndexTestV1997, session.SourcePath, TestContext.Current.CancellationToken);

        AccessRoundTripEnvironment.CompactResult jet3OpenProbe = session.RunDaoDatabaseScript(
            session.SourcePath,
            "Write-Output 'JET3_OPEN_OK'",
            CompactTimeout);
        if (IsDaoPreviousVersionFailure(jet3OpenProbe))
        {
            Assert.Skip("Installed DAO/Access cannot open Access 97 Jet3 .mdb files; Jet3 DAO CompactDatabase coverage is unavailable on this host.");
        }

        AssertDaoSuccess(jet3OpenProbe, "DAO Jet3 fixture open probe");

        const string TableName = "SM_Jet3Index";

        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            session.SourcePath,
            new AccessWriterOptions { UseLockFile = false },
            TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true, IsNullable = false },
                    new ColumnDefinition("Code", typeof(string), maxLength: 32) { IsNullable = false },
                    new ColumnDefinition("Score", typeof(int)) { IsNullable = false },
                ],
                [
                    new IndexDefinition("IX_Code", "Code"),
                    new IndexDefinition("IX_Score", "Score"),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(TableName, BuildJet3IndexRows(), TestContext.Current.CancellationToken);

            int updated = await writer.UpdateRowsAsync(
                TableName,
                "Id",
                42,
                new Dictionary<string, object>
                {
                    ["Code"] = "J3_UPDATED",
                    ["Score"] = -420,
                },
                TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);

            int deleted = await writer.DeleteRowsAsync(TableName, "Id", 17, TestContext.Current.CancellationToken);
            Assert.Equal(1, deleted);

            await writer.InsertRowAsync(TableName, [Jet3IndexRows + 1, "J3_INSERTED", 12345], TestContext.Current.CancellationToken);
        }

        AccessRoundTripEnvironment.CompactResult preCompactDao = session.RunDaoDatabaseScript(
            session.SourcePath,
            """
            $rs = $db.OpenRecordset('SELECT COUNT(*) AS Cnt FROM [SM_Jet3Index]', 4)
            try {
                Write-Output "ROWCOUNT=$($rs.Fields('Cnt').Value)"
            } finally {
                $rs.Close()
            }
            """,
            CompactTimeout);
        AssertDaoSuccess(preCompactDao, "DAO pre-compact OpenRecordset");
        Assert.Contains($"ROWCOUNT={Jet3IndexRows}", preCompactDao.StdOut, StringComparison.Ordinal);

        session.RunDaoCompact();

        await using AccessReader reader = await AccessReader.OpenAsync(
            session.CompactedPath,
            new AccessReaderOptions { UseLockFile = false },
            cancellationToken: TestContext.Current.CancellationToken);

        DataTable rows = await reader.ReadDataTableAsync(TableName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(Jet3IndexRows, rows.Rows.Count);
        Assert.DoesNotContain(rows.AsEnumerable(), row => Convert.ToInt32(row["Id"], CultureInfo.InvariantCulture) == 17);
        Assert.Contains(rows.AsEnumerable(), row => Convert.ToInt32(row["Id"], CultureInfo.InvariantCulture) == Jet3IndexRows + 1);

        DataRow updatedRow = rows.AsEnumerable().Single(row => Convert.ToInt32(row["Id"], CultureInfo.InvariantCulture) == 42);
        Assert.Equal("J3_UPDATED", SafeString(updatedRow, "Code"));
        Assert.Equal(-420, Convert.ToInt32(updatedRow["Score"], CultureInfo.InvariantCulture));

        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(TableName, TestContext.Current.CancellationToken);
        Assert.Contains(indexes, index => index.Kind == IndexKind.PrimaryKey && HasSingleColumn(index, "Id"));
        Assert.Contains(indexes, index => index.Kind == IndexKind.Normal && index.Name == "IX_Code" && HasSingleColumn(index, "Code"));
        Assert.Contains(indexes, index => index.Kind == IndexKind.Normal && index.Name == "IX_Score" && HasSingleColumn(index, "Score"));
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task FreshWriterCreatedComplexColumns_SurviveCompactAndRepair()
    {
        await using AccessRoundTripSession session = AccessRoundTripSession.CreateEmpty(compactTimeout: CompactTimeout);

        const string TableName = "SM_FreshComplex";
        byte[] largeAttachmentPayload = BuildPayload(12 * 1024, 0x6A);
        byte[] extraAttachmentPayload = BuildPayload(8 * 1024, 0x2B);
        byte[] secondParentAttachmentPayload = BuildPayload(10 * 1024, 0x3C);

        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            session.SourcePath,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true, IsNullable = false },
                    new ColumnDefinition("Title", typeof(string), maxLength: 80),
                    new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                    new ColumnDefinition("Tags", typeof(object), maxLength: 80)
                    {
                        IsMultiValue = true,
                        MultiValueElementType = typeof(string),
                    },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(
                TableName,
                new[]
                {
                    new object[] { 1, "fresh-complex", DBNull.Value, DBNull.Value },
                    new object[] { 2, "fresh-complex-second", DBNull.Value, DBNull.Value },
                },
                TestContext.Current.CancellationToken);

            var firstParentKey = new Dictionary<string, object> { ["Id"] = 1 };
            var secondParentKey = new Dictionary<string, object> { ["Id"] = 2 };
            await writer.AddAttachmentAsync(
                TableName,
                "Files",
                firstParentKey,
                new AttachmentInput("fresh-complex.jpg", largeAttachmentPayload),
                TestContext.Current.CancellationToken);
            await writer.AddAttachmentAsync(
                TableName,
                "Files",
                firstParentKey,
                new AttachmentInput("fresh-complex-extra.jpg", extraAttachmentPayload),
                TestContext.Current.CancellationToken);
            await writer.AddAttachmentAsync(
                TableName,
                "Files",
                secondParentKey,
                new AttachmentInput("fresh-second.jpg", secondParentAttachmentPayload),
                TestContext.Current.CancellationToken);

            await writer.AddMultiValueItemAsync(TableName, "Tags", firstParentKey, "alpha", TestContext.Current.CancellationToken);
            await writer.AddMultiValueItemAsync(TableName, "Tags", firstParentKey, "beta", TestContext.Current.CancellationToken);
            await writer.AddMultiValueItemAsync(TableName, "Tags", firstParentKey, "gamma", TestContext.Current.CancellationToken);
            await writer.AddMultiValueItemAsync(TableName, "Tags", secondParentKey, "delta", TestContext.Current.CancellationToken);
        }

        session.RunDaoCompact();

        await using AccessReader reader = await AccessReader.OpenAsync(
            session.CompactedPath,
            new AccessReaderOptions { UseLockFile = false },
            cancellationToken: TestContext.Current.CancellationToken);

        DataTable parent = await reader.ReadDataTableAsync(TableName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, parent.Rows.Count);
        Assert.Contains(parent.AsEnumerable(), row => Convert.ToInt32(row["Id"], CultureInfo.InvariantCulture) == 1 && string.Equals(SafeString(row, "Title"), "fresh-complex", StringComparison.Ordinal));
        Assert.Contains(parent.AsEnumerable(), row => Convert.ToInt32(row["Id"], CultureInfo.InvariantCulture) == 2 && string.Equals(SafeString(row, "Title"), "fresh-complex-second", StringComparison.Ordinal));

        IReadOnlyList<ComplexColumnInfo> complexColumns = await reader.GetComplexColumnsAsync(TableName, TestContext.Current.CancellationToken);
        Assert.Equal(2, complexColumns.Count);

        ComplexColumnInfo attachmentInfo = Assert.Single(complexColumns, column => string.Equals(column.ColumnName, "Files", StringComparison.Ordinal));
        Assert.Equal(ComplexColumnKind.Attachment, attachmentInfo.Kind);
        Assert.False(string.IsNullOrEmpty(attachmentInfo.FlatTableName));

        ComplexColumnInfo tagsInfo = Assert.Single(complexColumns, column => string.Equals(column.ColumnName, "Tags", StringComparison.Ordinal));
        Assert.Equal(ComplexColumnKind.MultiValue, tagsInfo.Kind);
        Assert.False(string.IsNullOrEmpty(tagsInfo.FlatTableName));

        IReadOnlyList<AttachmentRecord> attachments = await reader.GetAttachmentsAsync(TableName, "Files", TestContext.Current.CancellationToken);
        Assert.Equal(3, attachments.Count);
        AttachmentRecord largeAttachment = Assert.Single(attachments, attachment => string.Equals(attachment.FileName, "fresh-complex.jpg", StringComparison.Ordinal));
        AttachmentRecord extraAttachment = Assert.Single(attachments, attachment => string.Equals(attachment.FileName, "fresh-complex-extra.jpg", StringComparison.Ordinal));
        AttachmentRecord secondParentAttachment = Assert.Single(attachments, attachment => string.Equals(attachment.FileName, "fresh-second.jpg", StringComparison.Ordinal));
        Assert.Equal(largeAttachmentPayload, largeAttachment.FileData);
        Assert.Equal(extraAttachmentPayload, extraAttachment.FileData);
        Assert.Equal(secondParentAttachmentPayload, secondParentAttachment.FileData);
        Assert.Equal(largeAttachment.ConceptualTableId, extraAttachment.ConceptualTableId);
        Assert.NotEqual(largeAttachment.ConceptualTableId, secondParentAttachment.ConceptualTableId);

        IReadOnlyList<(int ConceptualTableId, object? Value)> tagItems = await reader.GetMultiValueItemsAsync(TableName, "Tags", TestContext.Current.CancellationToken);
        Assert.Equal(4, tagItems.Count);
        string[][] tagGroups = tagItems
            .GroupBy(item => item.ConceptualTableId)
            .Select(group => group.Select(item => Assert.IsType<string>(item.Value)).Order(StringComparer.Ordinal).ToArray())
            .OrderBy(group => group.Length)
            .ToArray();
        Assert.Equal(2, tagGroups.Length);
        Assert.Equal(["delta"], tagGroups[0]);
        Assert.Equal(["alpha", "beta", "gamma"], tagGroups[1]);

        IReadOnlyList<IndexMetadata> attachmentIndexes = await reader.ListIndexesAsync(attachmentInfo.FlatTableName, TestContext.Current.CancellationToken);
        Assert.Equal(3, attachmentIndexes.Count);
        Assert.Contains(attachmentIndexes, index => index.Kind == IndexKind.PrimaryKey && string.Equals(index.Name, "MSysComplexPKIndex", StringComparison.Ordinal));
        Assert.Contains(attachmentIndexes, index => index.Kind == IndexKind.Normal && string.Equals(index.Name, "_Files", StringComparison.Ordinal));
        Assert.Contains(attachmentIndexes, index => index.Kind == IndexKind.Normal && string.Equals(index.Name, "IdxFKPrimaryScalar", StringComparison.Ordinal));

        IReadOnlyList<IndexMetadata> tagsIndexes = await reader.ListIndexesAsync(tagsInfo.FlatTableName, TestContext.Current.CancellationToken);
        Assert.Equal(2, tagsIndexes.Count);
        Assert.Contains(tagsIndexes, index => index.Kind == IndexKind.PrimaryKey && string.Equals(index.Name, "MSysComplexPKIndex", StringComparison.Ordinal));
        Assert.Contains(tagsIndexes, index => index.Kind == IndexKind.Normal && string.Equals(index.Name, "_Tags", StringComparison.Ordinal));
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
    public async Task AdvancedIndexKeysAndBTreeMaintenance_SurviveCompactAndRepair()
    {
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateFromNorthwindAsync(
            TestContext.Current.CancellationToken,
            compactTimeout: CompactTimeout);

        const string TableName = "SM_AdvancedIndex";

        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            session.SourcePath,
            new AccessWriterOptions { UseLockFile = false },
            TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true, IsNullable = false },
                    new ColumnDefinition("Code", typeof(string), maxLength: 80) { IsNullable = false },
                    new ColumnDefinition("GuidKey", typeof(Guid)) { IsNullable = false },
                    new ColumnDefinition("Amount", typeof(decimal)) { IsNullable = false, NumericPrecision = 18, NumericScale = 2 },
                    new ColumnDefinition("BinKey", typeof(byte[]), maxLength: 16) { IsNullable = false },
                    new ColumnDefinition("Score", typeof(int)) { IsNullable = false },
                ],
                [
                    new IndexDefinition("IX_CodeScore", ["Code", "Score"]) { IsUnique = true, DescendingColumns = ["Score"] },
                    new IndexDefinition("IX_GuidKey", "GuidKey"),
                    new IndexDefinition("IX_Amount", "Amount"),
                    new IndexDefinition("IX_BinKey", "BinKey") { DescendingColumns = ["BinKey"] },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(TableName, BuildAdvancedIndexRows(), TestContext.Current.CancellationToken);

            int updated = await writer.UpdateRowsAsync(
                TableName,
                "Id",
                42,
                new Dictionary<string, object>
                {
                    ["Code"] = "Code_42_UPDATED",
                    ["GuidKey"] = BuildAdvancedGuid(4200),
                    ["Amount"] = -42.42m,
                    ["BinKey"] = BuildAdvancedBinaryKey(4200),
                    ["Score"] = -4200,
                },
                TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);

            int deleted = await writer.DeleteRowsAsync(TableName, "Id", 17, TestContext.Current.CancellationToken);
            Assert.Equal(1, deleted);

            await writer.InsertRowsAsync(TableName, [BuildAdvancedIndexRow(301)], TestContext.Current.CancellationToken);
        }

        AccessRoundTripEnvironment.CompactResult preCompactDao = session.RunDaoDatabaseScript(
            session.SourcePath,
            """
            $rs = $db.OpenRecordset('SELECT COUNT(*) AS Cnt FROM [SM_AdvancedIndex]', 4)
            try {
                Write-Output "ROWCOUNT=$($rs.Fields('Cnt').Value)"
            } finally {
                $rs.Close()
            }
            """,
            CompactTimeout);
        Assert.Equal(0, preCompactDao.ExitCode);
        Assert.Contains($"ROWCOUNT={AdvancedIndexRows}", preCompactDao.StdOut, StringComparison.Ordinal);

        session.RunDaoCompact();

        await using AccessReader reader = await AccessReader.OpenAsync(
            session.CompactedPath,
            new AccessReaderOptions { UseLockFile = false },
            cancellationToken: TestContext.Current.CancellationToken);

        DataTable rows = await reader.ReadDataTableAsync(TableName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(AdvancedIndexRows, rows.Rows.Count);
        Assert.DoesNotContain(rows.AsEnumerable(), row => Convert.ToInt32(row["Id"], CultureInfo.InvariantCulture) == 17);
        Assert.Contains(rows.AsEnumerable(), row => Convert.ToInt32(row["Id"], CultureInfo.InvariantCulture) == 301);

        DataRow updatedRow = rows.AsEnumerable().Single(row => Convert.ToInt32(row["Id"], CultureInfo.InvariantCulture) == 42);
        Assert.Equal("Code_42_UPDATED", SafeString(updatedRow, "Code"));
        Assert.Equal(BuildAdvancedGuid(4200), Assert.IsType<Guid>(updatedRow["GuidKey"]));
        Assert.Equal(-42.42m, Assert.IsType<decimal>(updatedRow["Amount"]));
        Assert.Equal(BuildAdvancedBinaryKey(4200), Assert.IsType<byte[]>(updatedRow["BinKey"]));
        Assert.Equal(-4200, Convert.ToInt32(updatedRow["Score"], CultureInfo.InvariantCulture));

        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(TableName, TestContext.Current.CancellationToken);
        Assert.Single(indexes, index => index.Kind == IndexKind.PrimaryKey);

        IndexMetadata codeScoreIndex = Assert.Single(indexes, index => index.Name == "IX_CodeScore");
        Assert.Equal(IndexKind.Normal, codeScoreIndex.Kind);
        Assert.True(codeScoreIndex.IsUnique);
        Assert.Collection(
            codeScoreIndex.Columns,
            column =>
            {
                Assert.Equal("Code", column.Name);
                Assert.True(column.IsAscending);
            },
            column =>
            {
                Assert.Equal("Score", column.Name);
                Assert.False(column.IsAscending);
            });

        IndexMetadata guidIndex = Assert.Single(indexes, index => index.Name == "IX_GuidKey");
        Assert.Equal("GuidKey", Assert.Single(guidIndex.Columns).Name);

        IndexMetadata amountIndex = Assert.Single(indexes, index => index.Name == "IX_Amount");
        Assert.Equal("Amount", Assert.Single(amountIndex.Columns).Name);

        IndexMetadata binaryIndex = Assert.Single(indexes, index => index.Name == "IX_BinKey");
        IndexColumnReference binaryColumn = Assert.Single(binaryIndex.Columns);
        Assert.Equal("BinKey", binaryColumn.Name);
        Assert.False(binaryColumn.IsAscending);
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

    private static object[][] BuildJet3IndexRows()
    {
        var rows = new object[Jet3IndexRows][];
        for (int rowOrdinal = 0; rowOrdinal < rows.Length; rowOrdinal++)
        {
            int id = rowOrdinal + 1;
            rows[rowOrdinal] = [id, $"J3_{id % 37:D2}_{(char)('A' + (id % 26))}", 1000 - id];
        }

        return rows;
    }

    private static object[][] BuildAdvancedIndexRows()
    {
        var rows = new object[AdvancedIndexRows][];
        for (int rowOrdinal = 0; rowOrdinal < rows.Length; rowOrdinal++)
        {
            rows[rowOrdinal] = BuildAdvancedIndexRow(rowOrdinal + 1);
        }

        return rows;
    }

    private static object[] BuildAdvancedIndexRow(int id) =>
        [
            id,
            $"Code_{id % 47:D2}_{(char)('A' + (id % 26))}",
            BuildAdvancedGuid(id),
            BuildAdvancedAmount(id),
            BuildAdvancedBinaryKey(id),
            10000 - id,
        ];

    private static Guid BuildAdvancedGuid(int id)
    {
        byte[] bytes = new byte[16];
        BitConverter.GetBytes(id).CopyTo(bytes, 0);
        BitConverter.GetBytes(id * 17).CopyTo(bytes, 4);
        bytes[8] = unchecked((byte)id);
        bytes[9] = unchecked((byte)(id >> 8));
        bytes[10] = unchecked((byte)(id * 29));
        bytes[11] = unchecked((byte)(id * 31));
        bytes[12] = unchecked((byte)(id * 37));
        bytes[13] = unchecked((byte)(id * 41));
        bytes[14] = unchecked((byte)(id * 43));
        bytes[15] = unchecked((byte)(id * 47));
        return new Guid(bytes);
    }

    private static decimal BuildAdvancedAmount(int id) =>
        ((id * 3713m) / 100m) - 5000m;

    private static byte[] BuildAdvancedBinaryKey(int id)
    {
        var payload = new byte[16];
        for (int byteIndex = 0; byteIndex < payload.Length; byteIndex++)
        {
            payload[byteIndex] = unchecked((byte)((id * 29) + (byteIndex * 17)));
        }

        return payload;
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

    private static void AssertDaoSuccess(AccessRoundTripEnvironment.CompactResult result, string operation)
    {
        Assert.True(
            result.ExitCode == 0,
            $"""
            {operation} failed (exit={result.ExitCode}).
            --- stdout ---
            {result.StdOut}
            --- stderr ---
            {result.StdErr}
            """);
    }

    private static async Task CopyDatabaseAsync(string sourcePath, string destinationPath, CancellationToken cancellationToken)
    {
        await using (FileStream source = File.OpenRead(sourcePath))
        await using (FileStream destination = File.Create(destinationPath))
        {
            await source.CopyToAsync(destination, cancellationToken);
        }

        File.SetAttributes(destinationPath, File.GetAttributes(destinationPath) & ~FileAttributes.ReadOnly);
    }

    private static bool HasSingleColumn(IndexMetadata index, string columnName) =>
        index.Columns.Count == 1 && string.Equals(index.Columns[0].Name, columnName, StringComparison.Ordinal);

    private static bool IsDaoPreviousVersionFailure(AccessRoundTripEnvironment.CompactResult result) =>
        result.ExitCode != 0
        && result.StdErr.Contains("previous version", StringComparison.OrdinalIgnoreCase);

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
