namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// End-to-end tests for optimal Compact &amp; Repair capabilities: interior gap-filling,
/// damaged index regeneration from verified active rows, damaged-header data-page salvage,
/// unrecoverable data-page quarantine sidecar export, <c>TDef</c> counter verification/repair,
/// row-level partial-page salvage, 3-tier owner header reconstruction, orphaned complex/FK child quarantine,
/// Primary-Key clustered row ordering, <c>LVAL</c> chain coalescing, <c>Reference</c> usage-map hole packing,
/// and read-only dry-run health inspection (<see cref="AccessDatabaseCompact.InspectAsync(Stream, AccessCompactOptions?, CancellationToken)"/>).
/// </summary>
public sealed class CompactOptimalCapabilitiesTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactAsync_PackInteriorFreePages_PacksRebuiltTablesIntoInteriorPrefixHolesAndShrinksBelowUnpacked(DatabaseFormat kind)
    {
        await using var sourceStream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            sourceStream,
            kind,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                "EarlyTable",
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Memo", typeof(string), 20_000)],
                [new IndexDefinition("IX_Early_Id", "Id")],
                Ct);
            for (int i = 0; i < 30; i++)
            {
                await writer.InsertRowAsync("EarlyTable", [i, new string((char)('A' + (i % 20)), 1800)], Ct);
            }

            await writer.CreateTableAsync(
                "LateTable",
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Label", typeof(string), 80)],
                cancellationToken: Ct);
            for (int i = 1; i <= 5; i++)
            {
                await writer.InsertRowAsync("LateTable", [i, $"late-{i}"], Ct);
            }

            for (int i = 2; i < 30; i++)
            {
                Assert.Equal(1, await writer.DeleteRowsAsync("EarlyTable", "Id", i, Ct));
            }
        }

        await using var unpackedStream = new MemoryStream();
        AccessCompactReport unpackedReport = await AccessDatabaseCompact.CompactAsync(
            sourceStream,
            unpackedStream,
            new AccessCompactOptions { PackInteriorFreePages = false },
            Ct);

        await using var packedStream = new MemoryStream();
        AccessCompactReport packedReport = await AccessDatabaseCompact.CompactAsync(
            sourceStream,
            packedStream,
            new AccessCompactOptions { PackInteriorFreePages = true },
            Ct);

        Assert.True(packedReport.IsComplete);
        Assert.True(packedReport.DatabaseValidated);
        Assert.True(packedReport.ReclamationAuthorized);
        Assert.True(packedReport.ReplacementAuthorized);
        Assert.True(packedReport.PackedInteriorPages > 0);
        Assert.Equal(unpackedReport.ScrubbedFreePages, packedReport.PackedInteriorPages + packedReport.ScrubbedFreePages);
        Assert.Equal(unpackedReport.DestinationPages - packedReport.PackedInteriorPages, packedReport.DestinationPages);
        Assert.True(packedStream.Length < unpackedStream.Length);

        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(packedStream, cancellationToken: Ct))
        {
            Assert.Equal(2, await reader.GetRealRowCountAsync("EarlyTable", Ct));
            Assert.Equal(5, await reader.GetRealRowCountAsync("LateTable", Ct));
            int earlyCount = 0;
            await foreach (object[] row in reader.Rows("EarlyTable", cancellationToken: Ct))
            {
                int id = Assert.IsType<int>(row[0]);
                Assert.Equal(earlyCount, id);
                Assert.Equal(new string((char)('A' + id), 1800), row[1]);
                earlyCount++;
            }

            Assert.Equal(2, earlyCount);
        }

        await using var packedTwiceStream = new MemoryStream();
        AccessCompactReport secondReport = await AccessDatabaseCompact.CompactAsync(packedStream, packedTwiceStream, cancellationToken: Ct);
        Assert.True(secondReport.IsComplete);
        Assert.True(secondReport.DatabaseValidated);
        Assert.Equal(packedReport.DestinationPages, secondReport.DestinationPages);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactToAsync_DamagedPrimaryKeyAndSecondaryIndex_RegeneratesIndexesFromVerifiedActiveRows(DatabaseFormat kind)
    {
        string tempDir = Path.Combine(Path.GetTempPath(), "JetDatabaseWriter.CompactIdxRegen." + Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(tempDir);
        try
        {
            string ext = kind == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb";
            string sourcePath = Path.Combine(tempDir, "source" + ext);
            string salvagePath = Path.Combine(tempDir, "salvage" + ext);

            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
                sourcePath,
                kind,
                new AccessWriterOptions { UseLockFile = false },
                Ct))
            {
                await writer.CreateTableAsync(
                    "Customers",
                    [
                        new("CustomerID", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true },
                        new("CustomerName", typeof(string), 80),
                        new("Email", typeof(string), 100),
                        new("Notes", typeof(string), 10_000),
                    ],
                    [new IndexDefinition("idx_Email", "Email") { IsUnique = true }],
                    Ct);

                for (int i = 1; i <= 40; i++)
                {
                    await writer.InsertRowAsync(
                        "Customers",
                        [i, $"Customer_{i:D3}", $"user{i:D3}@example.com", $"Notes for customer {i} " + new string('n', 200)],
                        Ct);
                }
            }

            byte[] bytes = await File.ReadAllBytesAsync(sourcePath, Ct);
            int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
            long customersTDef = FindUserTableTDefPage(bytes, pageSize);
            var indexLeafPages = new List<long>();
            for (long p = 3; p < bytes.Length / pageSize; p++)
            {
                int offset = checked((int)(p * pageSize));
                if (bytes[offset] == 0x04 && BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(offset + 4, 4)) == customersTDef)
                {
                    indexLeafPages.Add(p);
                }
            }

            Assert.True(indexLeafPages.Count >= 2);
            foreach (long leafPage in indexLeafPages)
            {
                bytes[checked((int)(leafPage * pageSize))] = 0x7F;
            }

            await File.WriteAllBytesAsync(sourcePath, bytes, Ct);

            _ = await Assert.ThrowsAsync<JetCorruptDataException>(
                async () => await AccessDatabaseCompact.CompactInPlaceAsync(sourcePath, cancellationToken: Ct));
            _ = await Assert.ThrowsAsync<JetCorruptDataException>(
                async () => await AccessDatabaseCompact.CompactToAsync(sourcePath, salvagePath, new AccessCompactOptions { AllowPartialRecovery = false }, Ct));
            _ = await Assert.ThrowsAsync<JetCorruptDataException>(
                async () => await AccessDatabaseCompact.CompactToAsync(sourcePath, salvagePath, new AccessCompactOptions { AllowPartialRecovery = true, RebuildDamagedIndexes = false }, Ct));

            AccessCompactReport report = await AccessDatabaseCompact.CompactToAsync(
                sourcePath,
                salvagePath,
                new AccessCompactOptions { AllowPartialRecovery = true, RebuildDamagedIndexes = true },
                Ct);

            Assert.True(report.DatabaseValidated);
            Assert.False(report.IsComplete);
            Assert.False(report.ReplacementAuthorized);
            Assert.Equal(2, report.RegeneratedIndexCount);
            Assert.Contains(report.Diagnostics, d => d.Category == AccessCompactDiagnosticCategory.RegeneratedIndex && d.TableName == "Customers");

            AccessCompactTableSummary customerSummary = Assert.Single(report.Tables, t => t.TableName == "Customers");
            Assert.Equal(40, customerSummary.ActiveRowCount);
            Assert.Equal(2, customerSummary.RegeneratedIndexCount);

            await using AccessReader reader = await AccessReader.OpenAsync(salvagePath, cancellationToken: Ct);
            Assert.Equal(40, await reader.GetRealRowCountAsync("Customers", Ct));
            IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Customers", Ct);
            Assert.Equal(2, indexes.Count);
            Assert.Contains(indexes, idx => idx.Kind == IndexKind.PrimaryKey);
            Assert.Contains(indexes, idx => idx.Name == "idx_Email" && idx.EnforcesUniqueness);

            int count = 0;
            await foreach (object[] row in reader.Rows("Customers", cancellationToken: Ct))
            {
                count++;
                Assert.Equal(count, row[0]);
                Assert.Equal($"Customer_{count:D3}", row[1]);
                Assert.Equal($"user{count:D3}@example.com", row[2]);
            }

            Assert.Equal(40, count);
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, recursive: true);
            }
        }
    }

    [Fact]
    public async Task CompactAsync_DamagedUniqueIndexWithDuplicateKeysInDataRows_RefusesRegeneration()
    {
        await using var sourceStream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            sourceStream,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                "Accounts",
                [new("Code", typeof(int)), new("Label", typeof(string), 40)],
                [new IndexDefinition("UQ_Code", "Code") { IsUnique = true }],
                Ct);
            await writer.InsertRowAsync("Accounts", [100, "first"], Ct);
            await writer.InsertRowAsync("Accounts", [200, "second"], Ct);
        }

        byte[] bytes = sourceStream.ToArray();
        const int pageSize = 4096;
        long tdefPage = FindUserTableTDefPage(bytes, pageSize);

        // Corrupt the unique index leaf page header AND overwrite the second row's Code (200 -> 100) on the DATA page.
        for (long p = 3; p < bytes.Length / pageSize; p++)
        {
            int offset = checked((int)(p * pageSize));
            uint owner = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(offset + 4, 4));
            if (owner != tdefPage)
            {
                continue;
            }

            if (bytes[offset] == 0x04)
            {
                bytes[offset] = 0x7F;
            }
            else if (bytes[offset] == 0x01)
            {
                Span<byte> pageSpan = bytes.AsSpan(offset, pageSize);
                byte[] target = [200, 0, 0, 0];
                int idx = pageSpan[16..].IndexOf(target);
                Assert.True(idx >= 0);
                BinaryPrimitives.WriteInt32LittleEndian(pageSpan.Slice(16 + idx, 4), 100);
            }
        }

        await using var corruptedStream = new MemoryStream(bytes, writable: false);
        await using var outputStream = new MemoryStream();
        JetCorruptDataException ex = await Assert.ThrowsAsync<JetCorruptDataException>(
            async () => await AccessDatabaseCompact.CompactAsync(
                corruptedStream,
                outputStream,
                new AccessCompactOptions { AllowPartialRecovery = true, RebuildDamagedIndexes = true },
                Ct));
        Assert.Equal(JetErrorCode.UniqueViolation, ex.ErrorCode);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactToAsync_DamagedDataPageTypeHeader_SalvagesAllVerifiedRowsWithoutPlaceholders(DatabaseFormat kind)
    {
        string tempDir = Path.Combine(Path.GetTempPath(), "JetDatabaseWriter.CompactDataSalvage." + Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(tempDir);
        try
        {
            string ext = kind == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb";
            string sourcePath = Path.Combine(tempDir, "source" + ext);
            string salvagePath = Path.Combine(tempDir, "salvage" + ext);

            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
                sourcePath,
                kind,
                new AccessWriterOptions { UseLockFile = false },
                Ct))
            {
                await writer.CreateTableAsync(
                    "Customers",
                    [
                        new("CustomerID", typeof(int)) { IsPrimaryKey = true },
                        new("CustomerName", typeof(string), 80),
                        new("Payload", typeof(string), 200),
                    ],
                    cancellationToken: Ct);

                for (int i = 1; i <= 50; i++)
                {
                    await writer.InsertRowAsync("Customers", [i, $"Name_{i:D3}", new string((char)('a' + (i % 20)), 150)], Ct);
                }
            }

            byte[] bytes = await File.ReadAllBytesAsync(sourcePath, Ct);
            int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
            long customersTDef = FindUserTableTDefPage(bytes, pageSize);
            long damagedDataPage = -1;
            for (long p = 3; p < bytes.Length / pageSize; p++)
            {
                int offset = checked((int)(p * pageSize));
                if (bytes[offset] == 0x01 && BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(offset + 4, 4)) == customersTDef)
                {
                    int numRows = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(offset + (kind == DatabaseFormat.Jet3Mdb ? 8 : 12), 2));
                    if (numRows > 0)
                    {
                        damagedDataPage = p;
                        bytes[offset] = 0x7F;
                        break;
                    }
                }
            }

            Assert.True(damagedDataPage >= 3);
            await File.WriteAllBytesAsync(sourcePath, bytes, Ct);

            _ = await Assert.ThrowsAsync<JetCorruptDataException>(
                async () => await AccessDatabaseCompact.CompactInPlaceAsync(sourcePath, cancellationToken: Ct));
            _ = await Assert.ThrowsAsync<JetCorruptDataException>(
                async () => await AccessDatabaseCompact.CompactToAsync(sourcePath, salvagePath, new AccessCompactOptions { AllowPartialRecovery = false }, Ct));

            AccessCompactReport report = await AccessDatabaseCompact.CompactToAsync(
                sourcePath,
                salvagePath,
                new AccessCompactOptions { AllowPartialRecovery = true },
                Ct);

            Assert.True(report.DatabaseValidated);
            Assert.False(report.IsComplete);
            Assert.True(report.SalvagedRowCount > 0);
            Assert.Contains(
                report.Diagnostics,
                d => d.Category == AccessCompactDiagnosticCategory.SalvagedDataPage
                    && d.PageNumber == damagedDataPage
                    && d.TableName == "Customers");

            await using AccessReader reader = await AccessReader.OpenAsync(salvagePath, cancellationToken: Ct);
            Assert.Equal(50, await reader.GetRealRowCountAsync("Customers", Ct));
            IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Customers", Ct);
            Assert.Contains(indexes, idx => idx.Kind == IndexKind.PrimaryKey);

            int verifiedRows = 0;
            await foreach (object[] row in reader.Rows("Customers", cancellationToken: Ct))
            {
                verifiedRows++;
                int id = Assert.IsType<int>(row[0]);
                Assert.Equal($"Name_{id:D3}", row[1]);
                Assert.Equal(new string((char)('a' + (id % 20)), 150), row[2]);
            }

            Assert.Equal(50, verifiedRows);
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, recursive: true);
            }
        }
    }

    [Fact]
    public async Task CompactToAsync_UnrecoverableDataPage_QuarantinesCorruptPageAndCompactsSurvivingPages()
    {
        string tempDir = Path.Combine(Path.GetTempPath(), "JetDatabaseWriter.CompactQuarantine." + Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(tempDir);
        try
        {
            string sourcePath = Path.Combine(tempDir, "source.accdb");
            string salvagePath = Path.Combine(tempDir, "salvage.accdb");
            string quarantinePath = Path.Combine(tempDir, "quarantine.ndjson");

            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
                sourcePath,
                DatabaseFormat.AceAccdb,
                new AccessWriterOptions { UseLockFile = false },
                Ct))
            {
                await writer.CreateTableAsync(
                    "Events",
                    [
                        new("EventID", typeof(int)) { IsPrimaryKey = true },
                        new("Description", typeof(string), 200),
                    ],
                    cancellationToken: Ct);

                for (int i = 1; i <= 45; i++)
                {
                    await writer.InsertRowAsync("Events", [i, new string('E', 180)], Ct);
                }
            }

            byte[] bytes = await File.ReadAllBytesAsync(sourcePath, Ct);
            const int pageSize = 4096;
            long eventsTDef = FindUserTableTDefPage(bytes, pageSize);
            var dataPages = new List<long>();
            for (long p = 3; p < bytes.Length / pageSize; p++)
            {
                int offset = checked((int)(p * pageSize));
                if (bytes[offset] == 0x01 && BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(offset + 4, 4)) == eventsTDef)
                {
                    int numRows = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(offset + 12, 2));
                    if (numRows > 0)
                    {
                        dataPages.Add(p);
                    }
                }
            }

            Assert.True(dataPages.Count >= 2);
            long corruptedPage = dataPages[^1];
            int corruptOffset = checked((int)(corruptedPage * pageSize));

            // Corrupt the row directory on the last DATA page so it is genuinely unrecoverable.
            BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(corruptOffset + 12, 2), 9999);
            await File.WriteAllBytesAsync(sourcePath, bytes, Ct);

            AccessCompactReport report = await AccessDatabaseCompact.CompactToAsync(
                sourcePath,
                salvagePath,
                new AccessCompactOptions
                {
                    AllowPartialRecovery = true,
                    QuarantinePath = quarantinePath,
                },
                Ct);

            Assert.True(report.DatabaseValidated);
            Assert.False(report.IsComplete);
            Assert.Equal(1, report.QuarantinedRecordCount);
            Assert.True(report.RegeneratedIndexCount >= 1);
            Assert.Contains(
                report.Diagnostics,
                d => d.Category == AccessCompactDiagnosticCategory.QuarantinedDataPage
                    && d.PageNumber == corruptedPage
                    && d.TableName == "Events");

            Assert.True(File.Exists(quarantinePath));
            string[] lines = await File.ReadAllLinesAsync(quarantinePath, Ct);
            string line = Assert.Single(lines, l => !string.IsNullOrWhiteSpace(l));
            using var doc = JsonDocument.Parse(line);
            Assert.Equal("quarantined-data-page", doc.RootElement.GetProperty("kind").GetString());
            Assert.Equal(corruptedPage, doc.RootElement.GetProperty("pageNumber").GetInt64());
            Assert.Equal("Events", doc.RootElement.GetProperty("tableName").GetString());
            byte[] decodedRaw = Convert.FromBase64String(doc.RootElement.GetProperty("rawBytesBase64").GetString()!);
            Assert.Equal(pageSize, decodedRaw.Length);

            await using AccessReader reader = await AccessReader.OpenAsync(salvagePath, cancellationToken: Ct);
            long survivingRows = await reader.GetRealRowCountAsync("Events", Ct);
            Assert.True(survivingRows is > 0 and < 45);
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, recursive: true);
            }
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactAsync_RepairTableCounters_FixesCorruptRowCountLowAutoNumberSeedAndResetsEmptyTableAutoNumber(DatabaseFormat kind)
    {
        await using var sourceStream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            sourceStream,
            kind,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                "CorruptRowCountTable",
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Val", typeof(string), 40)],
                cancellationToken: Ct);
            for (int i = 1; i <= 5; i++)
            {
                await writer.InsertRowAsync("CorruptRowCountTable", [i, $"v{i}"], Ct);
            }

            await writer.CreateTableAsync(
                "LowSeedAutoNumberTable",
                [new("Id", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true }, new("Name", typeof(string), 40)],
                cancellationToken: Ct);
            for (int i = 1; i <= 10; i++)
            {
                await writer.InsertRowAsync("LowSeedAutoNumberTable", [i, $"n{i}"], Ct);
            }

            await writer.CreateTableAsync(
                "EmptyAutoNumberTable",
                [new("Id", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true }, new("Tag", typeof(string), 40)],
                cancellationToken: Ct);
            for (int i = 1; i <= 4; i++)
            {
                await writer.InsertRowAsync("EmptyAutoNumberTable", [i, $"t{i}"], Ct);
            }

            for (int i = 1; i <= 4; i++)
            {
                Assert.Equal(1, await writer.DeleteRowsAsync("EmptyAutoNumberTable", "Id", i, Ct));
            }
        }

        // Inspect first to find the OwnerTDefPage of each table, then corrupt TDef counters on sourceStream.
        AccessCompactReport initialInspect = await AccessDatabaseCompact.InspectAsync(sourceStream, cancellationToken: Ct);
        long rowCountTDef = Assert.Single(initialInspect.Tables, t => t.TableName == "CorruptRowCountTable").OwnerTDefPage;
        long lowSeedTDef = Assert.Single(initialInspect.Tables, t => t.TableName == "LowSeedAutoNumberTable").OwnerTDefPage;

        byte[] bytes = sourceStream.ToArray();
        int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        int numRowsOff = kind == DatabaseFormat.Jet3Mdb ? 12 : 16;
        int autoNumOff = kind == DatabaseFormat.Jet3Mdb ? 16 : 20;

        // Corrupt NumRows on CorruptRowCountTable from 5 -> 999.
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(checked((int)(rowCountTDef * pageSize)) + numRowsOff, 4), 999U);

        // Corrupt AutoNumber high-water mark on LowSeedAutoNumberTable from 10 -> 2 (Access duplicate-PK seed bug).
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(checked((int)(lowSeedTDef * pageSize)) + autoNumOff, 4), 2U);

        await using var corruptedSource = new MemoryStream(bytes, writable: false);
        await using var compactedStream = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(
            corruptedSource,
            compactedStream,
            new AccessCompactOptions { RepairTableCounters = true },
            Ct);

        Assert.True(report.IsComplete);
        Assert.True(report.DatabaseValidated);
        Assert.True(report.ReclamationAuthorized);
        Assert.True(report.ReplacementAuthorized);
        Assert.Equal(3, report.RepairedCounterTableCount);
        Assert.Equal(2, report.Diagnostics.Count(d => d.Category == AccessCompactDiagnosticCategory.RepairedTableCounter));

        AccessCompactTableSummary rowCountSummary = Assert.Single(report.Tables, t => t.TableName == "CorruptRowCountTable");
        Assert.Equal(999U, rowCountSummary.DeclaredRowCount);
        Assert.Equal(5U, rowCountSummary.RepairedRowCount);
        Assert.True(rowCountSummary.CountersRepaired);

        AccessCompactTableSummary lowSeedSummary = Assert.Single(report.Tables, t => t.TableName == "LowSeedAutoNumberTable");
        Assert.Equal(2U, lowSeedSummary.DeclaredAutoNumber);
        Assert.Equal(10U, lowSeedSummary.RepairedAutoNumber);
        Assert.True(lowSeedSummary.CountersRepaired);

        AccessCompactTableSummary emptyAutoSummary = Assert.Single(report.Tables, t => t.TableName == "EmptyAutoNumberTable");
        Assert.Equal(4U, emptyAutoSummary.DeclaredAutoNumber);
        Assert.Equal(0U, emptyAutoSummary.RepairedAutoNumber);
        Assert.True(emptyAutoSummary.CountersRepaired);

        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            compactedStream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: Ct))
        {
            await writer.InsertRowAsync("LowSeedAutoNumberTable", [null, "next-after-10"], Ct);
            await writer.InsertRowAsync("EmptyAutoNumberTable", [null, "reset-to-1"], Ct);
        }

        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(compactedStream, cancellationToken: Ct))
        {
            IReadOnlyList<TableStat> stats = await reader.GetTableStatsAsync(Ct);
            Assert.Equal(5, Assert.Single(stats, s => s.Name == "CorruptRowCountTable").RowCount);
            Assert.Equal(11, Assert.Single(stats, s => s.Name == "LowSeedAutoNumberTable").RowCount);
            Assert.Equal(1, Assert.Single(stats, s => s.Name == "EmptyAutoNumberTable").RowCount);

            var lowSeedIds = new List<int>();
            await foreach (object[] row in reader.Rows("LowSeedAutoNumberTable", cancellationToken: Ct))
            {
                lowSeedIds.Add(Assert.IsType<int>(row[0]));
            }

            Assert.Contains(11, lowSeedIds);

            object[] emptyTableRow = await reader.Rows("EmptyAutoNumberTable", cancellationToken: Ct).SingleAsync(Ct);
            Assert.Equal(1, emptyTableRow[0]);
        }
    }

    [Fact]
    public async Task InspectAsync_DryRun_ReportsHealthReclamationProgressAndTableSummariesWithoutWriting()
    {
        await using var sourceStream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            sourceStream,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                "Metrics",
                [new("Id", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true }, new("Note", typeof(string), 200)],
                cancellationToken: Ct);
            for (int i = 1; i <= 20; i++)
            {
                await writer.InsertRowAsync("Metrics", [i, new string('M', 150)], Ct);
            }

            for (int i = 6; i <= 20; i++)
            {
                Assert.Equal(1, await writer.DeleteRowsAsync("Metrics", "Id", i, Ct));
            }
        }

        var phases = new List<AccessCompactPhase>();
        var progress = new SynchronousProgress<AccessCompactProgress>(p => phases.Add(p.Phase));

        AccessCompactReport report = await AccessDatabaseCompact.InspectAsync(
            sourceStream,
            new AccessCompactOptions { Progress = progress },
            Ct);

        Assert.True(report.IsComplete);
        Assert.False(report.DatabaseValidated);
        Assert.True(report.ReclamationAuthorized);
        Assert.True(report.ReplacementAuthorized);
        Assert.Equal(0, report.WrittenBytes);
        Assert.True(report.ReclaimedPages > 0);
        Assert.Contains(AccessCompactPhase.Planning, phases);
        Assert.Equal(AccessCompactPhase.Completed, phases[^1]);

        AccessCompactTableSummary metricsSummary = Assert.Single(report.Tables, t => t.TableName == "Metrics");
        Assert.Equal(5, metricsSummary.ActiveRowCount);
        Assert.Equal(5U, metricsSummary.DeclaredRowCount);
        Assert.Equal(5U, metricsSummary.RepairedRowCount);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactToAsync_PartiallyCorruptDataPage_SalvagesHealthyRowsOnSamePageAndQuarantinesOnlyCorruptRow(DatabaseFormat kind)
    {
        string ext = kind == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb";
        string tempDir = Path.Combine(Path.GetTempPath(), "jdw-compact-partialrow-" + Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(tempDir);
        string sourcePath = Path.Combine(tempDir, "source" + ext);
        string salvagePath = Path.Combine(tempDir, "salvage" + ext);
        string quarantinePath = Path.Combine(tempDir, "quarantine.jsonl");
        try
        {
            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(sourcePath, kind, cancellationToken: Ct))
            {
                await writer.CreateTableAsync(
                    "Members",
                    [
                        new("Id", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true },
                        new("Name", typeof(string), 40),
                        new("Score", typeof(int)),
                    ],
                    [new IndexDefinition("IX_Members_Name", "Name") { IsUnique = true }],
                    Ct);
                await writer.InsertRowAsync("Members", [1, "Alice", 100], Ct);
                await writer.InsertRowAsync("Members", [2, "BobCorrupt", 200], Ct);
                await writer.InsertRowAsync("Members", [3, "Charlie", 300], Ct);
            }

            AccessCompactReport initialInspect = await AccessDatabaseCompact.InspectAsync(sourcePath, cancellationToken: Ct);
            long membersTDef = Assert.Single(initialInspect.Tables, t => t.TableName == "Members").OwnerTDefPage;

            byte[] bytes = await File.ReadAllBytesAsync(sourcePath, Ct);
            int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
            int rowsStartOff = kind == DatabaseFormat.Jet3Mdb ? 10 : 14;
            long dataPage = 0;
            for (long p = 3; p < bytes.Length / pageSize; p++)
            {
                int pageOff = checked((int)(p * pageSize));
                if (bytes[pageOff] == 0x01 && BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pageOff + 4, 4)) == membersTDef)
                {
                    dataPage = p;

                    // Corrupt only row slot 1 ("BobCorrupt") by overwriting its variable-column offset table with 0xFF,
                    // while leaving row slots 0 ("Alice") and 2 ("Charlie") on the same DATA page completely intact.
                    int slot1End = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(pageOff + rowsStartOff, 2)) & 0x0FFF;
                    for (int pos = slot1End - 4; pos < slot1End - 1; pos++)
                    {
                        bytes[pageOff + pos] = 0xFF;
                    }

                    break;
                }
            }

            Assert.True(dataPage >= 3);
            await File.WriteAllBytesAsync(sourcePath, bytes, Ct);

            AccessCompactReport report = await AccessDatabaseCompact.CompactToAsync(
                sourcePath,
                salvagePath,
                new AccessCompactOptions
                {
                    AllowPartialRecovery = true,
                    RebuildDamagedIndexes = true,
                    RepairTableCounters = true,
                    QuarantinePath = quarantinePath,
                },
                Ct);

            Assert.True(report.DatabaseValidated);
            Assert.False(report.IsComplete);
            Assert.Equal(2, report.SalvagedRowCount);
            Assert.Equal(1, report.QuarantinedRowCount);
            Assert.Equal(1, report.QuarantinedRecordCount);
            Assert.Contains(
                report.Diagnostics,
                d => d.Kind == "salvaged-partial-data-page"
                    && d.Category == AccessCompactDiagnosticCategory.SalvagedDataPage
                    && d.PageNumber == dataPage);
            Assert.Contains(
                report.Diagnostics,
                d => d.Kind == "quarantined-corrupt-row"
                    && d.Category == AccessCompactDiagnosticCategory.QuarantinedRow
                    && d.PageNumber == dataPage
                    && d.RowIndex == 1);

            AccessCompactTableSummary summary = Assert.Single(report.Tables, t => t.TableName == "Members");
            Assert.Equal(2, summary.ActiveRowCount);
            Assert.Equal(2, summary.SalvagedRowCount);
            Assert.Equal(1, summary.QuarantinedRowCount);
            Assert.Equal(0, summary.QuarantinedPageCount);
            Assert.Equal(2U, summary.RepairedRowCount);

            string[] qLines = await File.ReadAllLinesAsync(quarantinePath, Ct);
            string qLine = Assert.Single(qLines, l => !string.IsNullOrWhiteSpace(l));
            using var doc = JsonDocument.Parse(qLine);
            Assert.Equal("quarantined-corrupt-row", doc.RootElement.GetProperty("kind").GetString());
            Assert.Equal(1, doc.RootElement.GetProperty("rowIndex").GetInt32());

            await using AccessReader reader = await AccessReader.OpenAsync(salvagePath, cancellationToken: Ct);
            var survivingNames = new List<string>();
            await foreach (object[] row in reader.Rows("Members", cancellationToken: Ct))
            {
                survivingNames.Add(Assert.IsType<string>(row[1]));
            }

            Assert.Equal(["Alice", "Charlie"], survivingNames);
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, recursive: true);
            }
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactToAsync_DamagedDataPageOwnerHeader_ReconstructsOwnerFromIndexBookmarksAndSchemaSignature(DatabaseFormat kind)
    {
        await using var sourceStream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            sourceStream,
            kind,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                "Orders",
                [
                    new("Id", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true },
                    new("Sku", typeof(string), 30),
                    new("Qty", typeof(int)),
                    new("UnitCost", typeof(int)),
                ],
                cancellationToken: Ct);
            await writer.InsertRowAsync("Orders", [1, "SKU-100", 5, 25], Ct);
            await writer.InsertRowAsync("Orders", [2, "SKU-200", 8, 40], Ct);

            await writer.CreateTableAsync(
                "FlagsOnly",
                [
                    new("Id", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true },
                ],
                cancellationToken: Ct);
            await writer.InsertRowAsync("FlagsOnly", [1], Ct);
        }

        AccessCompactReport inspect = await AccessDatabaseCompact.InspectAsync(sourceStream, cancellationToken: Ct);
        long ordersTDef = Assert.Single(inspect.Tables, t => t.TableName == "Orders").OwnerTDefPage;

        byte[] bytes = sourceStream.ToArray();
        int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        int rowsStartOff = kind == DatabaseFormat.Jet3Mdb ? 10 : 14;
        int usedPagesOff = kind == DatabaseFormat.Jet3Mdb ? 35 : 55;
        int mapPageNumber = (int)(BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(checked((int)(ordersTDef * pageSize)) + usedPagesOff, 4)) >> 8);

        long ordersDataPage = 0;
        for (long p = 3; p < bytes.Length / pageSize; p++)
        {
            int pageOff = checked((int)(p * pageSize));
            if (bytes[pageOff] == 0x01 && BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pageOff + 4, 4)) == ordersTDef)
            {
                ordersDataPage = p;

                // Overwrite the 4-byte TDef owner pointer with 0xDEADCAFE.
                BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(pageOff + 4, 4), 0xDEADCAFEU);
                break;
            }
        }

        Assert.True(ordersDataPage >= 3);

        // Clear the Orders inline usage-map bitmap so Tier 1 (usage-map) cannot identify the page,
        // forcing Tier 2 (index-bookmarks) reconstruction.
        int mapPageOff = checked(mapPageNumber * pageSize);
        int mapSlotStart = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(mapPageOff + rowsStartOff, 2)) & 0x1FFF;
        bytes.AsSpan(mapPageOff + mapSlotStart + 5, pageSize - mapSlotStart - 5).Clear();

        await using var tier2Source = new MemoryStream(bytes, writable: false);
        await using var tier2Dest = new MemoryStream();
        AccessCompactReport tier2Report = await AccessDatabaseCompact.CompactAsync(
            tier2Source,
            tier2Dest,
            new AccessCompactOptions { AllowPartialRecovery = true },
            Ct);

        Assert.True(tier2Report.DatabaseValidated);
        Assert.Equal(2, tier2Report.SalvagedRowCount);
        Assert.Contains(
            tier2Report.Diagnostics,
            d => d.Kind == "salvaged-data-page-owner"
                && d.Category == AccessCompactDiagnosticCategory.SalvagedDataPage
                && d.PageNumber == ordersDataPage
                && d.Detail.Contains("index-bookmarks", StringComparison.Ordinal));

        // Now also corrupt the Orders index leaf page owner so Tier 2 (index-bookmarks) cannot see it,
        // forcing Tier 3 (schema-signature) reconstruction!
        byte[] tier3Bytes = (byte[])bytes.Clone();
        for (long p = 3; p < tier3Bytes.Length / pageSize; p++)
        {
            int pageOff = checked((int)(p * pageSize));
            if (tier3Bytes[pageOff] == 0x04 && BinaryPrimitives.ReadUInt32LittleEndian(tier3Bytes.AsSpan(pageOff + 4, 4)) == ordersTDef)
            {
                tier3Bytes[pageOff] = 0x7F;
                BinaryPrimitives.WriteUInt32LittleEndian(tier3Bytes.AsSpan(pageOff + 4, 4), 0U);
            }
        }

        await using var tier3Source = new MemoryStream(tier3Bytes, writable: false);
        await using var tier3Dest = new MemoryStream();
        AccessCompactReport tier3Report = await AccessDatabaseCompact.CompactAsync(
            tier3Source,
            tier3Dest,
            new AccessCompactOptions { AllowPartialRecovery = true, RebuildDamagedIndexes = true },
            Ct);

        Assert.True(tier3Report.DatabaseValidated);
        Assert.Equal(2, tier3Report.SalvagedRowCount);
        Assert.Contains(
            tier3Report.Diagnostics,
            d => d.Kind == "salvaged-data-page-owner"
                && d.Category == AccessCompactDiagnosticCategory.SalvagedDataPage
                && d.PageNumber == ordersDataPage
                && d.Detail.Contains("schema-signature", StringComparison.Ordinal));

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(tier3Dest, cancellationToken: Ct);
        Assert.Equal(2, await reader.GetRealRowCountAsync("Orders", Ct));
    }

    [Fact]
    public async Task CompactToAsync_OrphanedComplexFlatTableAndForeignKeyChildRows_QuarantinesOrphansAndRepairsTables()
    {
        await using var sourceStream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            sourceStream,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                "Customers",
                [
                    new("Id", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true },
                    new("Name", typeof(string), 40),
                ],
                cancellationToken: Ct);
            await writer.CreateTableAsync(
                "Invoices",
                [
                    new("Id", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true },
                    new("CustomerId", typeof(int)),
                    new("Amount", typeof(int)),
                ],
                cancellationToken: Ct);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_Invoices_Customers", "Customers", "Id", "Invoices", "CustomerId")
                {
                    CascadeDeletes = true,
                },
                Ct);

            await writer.InsertRowAsync("Customers", [1, "Alice"], Ct);
            await writer.InsertRowAsync("Customers", [2, "Bob"], Ct);
            await writer.InsertRowAsync("Invoices", [10, 1, 500], Ct);
            await writer.InsertRowAsync("Invoices", [20, 2, 900], Ct);

            await writer.CreateTableAsync(
                "TaggedDocs",
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("Labels", typeof(object))
                    {
                        IsMultiValue = true,
                        MultiValueElementType = typeof(int),
                    },
                ],
                Ct);
            await writer.InsertRowsAsync("TaggedDocs", [[1, DBNull.Value], [2, DBNull.Value]], Ct);
            await writer.AddMultiValueItemAsync("TaggedDocs", "Labels", new Dictionary<string, object?> { ["Id"] = 1 }, 101, Ct);
            await writer.AddMultiValueItemAsync("TaggedDocs", "Labels", new Dictionary<string, object?> { ["Id"] = 2 }, 202, Ct);
        }

        AccessCompactReport inspect = await AccessDatabaseCompact.InspectAsync(sourceStream, cancellationToken: Ct);
        long customersTDef = Assert.Single(inspect.Tables, t => t.TableName == "Customers").OwnerTDefPage;
        long taggedDocsTDef = Assert.Single(inspect.Tables, t => t.TableName == "TaggedDocs").OwnerTDefPage;

        byte[] bytes = sourceStream.ToArray();
        const int pageSize = 4096;
        const int rowsStartOff = 14;
        for (long p = 3; p < bytes.Length / pageSize; p++)
        {
            int pageOff = checked((int)(p * pageSize));
            if (bytes[pageOff] == 0x01)
            {
                uint owner = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pageOff + 4, 4));
                if (owner == customersTDef)
                {
                    // Mark Customer 2 (last live slot) as deleted (0x8000) on Customers DATA page without cascading to Invoices,
                    // leaving Invoice 20 (CustomerId=2) as an orphaned enforced FK child row!
                    int numRows = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(pageOff + 12, 2));
                    int lastSlotOff = pageOff + rowsStartOff + ((numRows - 1) * 2);
                    ushort lastSlot = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(lastSlotOff, 2));
                    BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(lastSlotOff, 2), (ushort)(lastSlot | 0x8000));
                }
                else if (owner == taggedDocsTDef)
                {
                    // Mark TaggedDocs row 2 (last live slot) as deleted (0x8000) on TaggedDocs DATA page without deleting its flat-table row,
                    // leaving its complex flat-table child row (Labels=202) orphaned!
                    int numRows = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(pageOff + 12, 2));
                    int lastSlotOff = pageOff + rowsStartOff + ((numRows - 1) * 2);
                    ushort lastSlot = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(lastSlotOff, 2));
                    BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(lastSlotOff, 2), (ushort)(lastSlot | 0x8000));
                }
            }
        }

        await using var corruptedSource = new MemoryStream(bytes, writable: false);
        await using var compactedDest = new MemoryStream();
        await using var quarantineStream = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(
            corruptedSource,
            compactedDest,
            new AccessCompactOptions
            {
                AllowPartialRecovery = true,
                RebuildDamagedIndexes = true,
                RepairTableCounters = true,
                QuarantineOrphanedChildRows = true,
                QuarantineStream = quarantineStream,
            },
            Ct);

        Assert.True(report.DatabaseValidated);
        Assert.Equal(2, report.QuarantinedRowCount);
        Assert.Equal(2, report.QuarantinedRecordCount);
        Assert.Contains(
            report.Diagnostics,
            d => d.Kind == "quarantined-orphan-fk-row"
                && d.Category == AccessCompactDiagnosticCategory.QuarantinedRow
                && d.TableName == "Invoices");
        Assert.Contains(
            report.Diagnostics,
            d => d.Kind == "quarantined-orphan-complex-row"
                && d.Category == AccessCompactDiagnosticCategory.QuarantinedRow);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(compactedDest, cancellationToken: Ct);
        Assert.Equal(1, await reader.GetRealRowCountAsync("Customers", Ct));
        Assert.Equal(1, await reader.GetRealRowCountAsync("Invoices", Ct));
        object[] survivingInvoice = await reader.Rows("Invoices", cancellationToken: Ct).SingleAsync(Ct);
        Assert.Equal(10, survivingInvoice[0]);
        Assert.Equal(1, survivingInvoice[1]);
        Assert.Equal(500, survivingInvoice[2]);

        MultiValueItem[] survivingItems = [.. await reader.GetMultiValueItemsAsync("TaggedDocs", "Labels", Ct)];
        MultiValueItem singleItem = Assert.Single(survivingItems);
        Assert.Equal(101, singleItem.Value);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactAsync_ClusterByPrimaryKeyAndCoalesceLongValueChains_ReordersRowsInPrimaryKeyOrderAndCoalescesMultiHopLval(DatabaseFormat kind)
    {
        await using var sourceStream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            sourceStream,
            kind,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                "CatalogItems",
                [
                    new("Code", typeof(string), 20) { IsPrimaryKey = true },
                    new("Rank", typeof(int)),
                    new("Description", typeof(string), 0),
                ],
                [new IndexDefinition("IX_CatalogItems_Rank", "Rank")],
                Ct);

            // Insert in reverse Primary Key order so physical row order ("Zeta", "Mu", "Delta", "Alpha")
            // disagrees with canonical PK order ("Alpha", "Delta", "Mu", "Zeta").
            await writer.InsertRowAsync("CatalogItems", ["Zeta", 10, new string('Z', 900)], Ct);
            await writer.InsertRowAsync("CatalogItems", ["Mu", 20, "short-M"], Ct);
            await writer.InsertRowAsync("CatalogItems", ["Delta", 10, "short-D"], Ct);
            await writer.InsertRowAsync("CatalogItems", ["Alpha", 5, "short-A"], Ct);
        }

        // Rewrite the external SinglePageStorageMode (0x40) LVAL into a 2-hop ChainedStorageMode (0x00) LVAL on the same LVAL page
        // so CoalesceLongValueChains can coalesce it back into a single-page LVAL chunk!
        byte[] bytes = sourceStream.ToArray();
        int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        int rowsStartOff = kind == DatabaseFormat.Jet3Mdb ? 10 : 14;
        AccessCompactReport inspect = await AccessDatabaseCompact.InspectAsync(sourceStream, cancellationToken: Ct);
        long tableOwner = Assert.Single(inspect.Tables, t => t.TableName == "CatalogItems").OwnerTDefPage;

        bool convertedToChained = false;
        for (long p = 3; p < bytes.Length / pageSize && !convertedToChained; p++)
        {
            int dataPageOff = checked((int)(p * pageSize));
            if (bytes[dataPageOff] != 0x01 || BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(dataPageOff + 4, 4)) != tableOwner)
            {
                continue;
            }

            int slot0Start = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(dataPageOff + rowsStartOff, 2)) & 0x1FFF;
            for (int pos = slot0Start; pos <= pageSize - 12; pos++)
            {
                uint rawLen = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(dataPageOff + pos, 4));
                byte mode = (byte)((rawLen >> 24) & 0xC0U);
                int len = (int)(rawLen & 0x3FFFFFFFU);
                if (mode == 0x40 && len is >= 800 and <= 1900)
                {
                    uint firstDp = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(dataPageOff + pos + 4, 4));
                    long lvalPage = firstDp >> 8;
                    int lvalRow = (int)(firstDp & 0xFFU);
                    if (lvalPage >= 3 && lvalPage < bytes.Length / pageSize && lvalRow == 0)
                    {
                        int lvalPageOff = checked((int)(lvalPage * pageSize));
                        int numLvalRows = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(lvalPageOff + (kind == DatabaseFormat.Jet3Mdb ? 8 : 12), 2));
                        if (numLvalRows == 1)
                        {
                            int r0Start = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(lvalPageOff + rowsStartOff, 2)) & 0x1FFF;
                            byte[] fullPayload = bytes.AsSpan(lvalPageOff + r0Start, len).ToArray();
                            int half = len / 2;
                            int chunk0Len = 4 + half;
                            int chunk1Len = 4 + (len - half);
                            int newR0Start = pageSize - chunk0Len;
                            int newR1Start = newR0Start - chunk1Len;
                            if (newR1Start > rowsStartOff + 4)
                            {
                                uint nextBookmark = checked(((uint)lvalPage << 8) | 1U);
                                BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(lvalPageOff + newR0Start, 4), nextBookmark);
                                fullPayload.AsSpan(0, half).CopyTo(bytes.AsSpan(lvalPageOff + newR0Start + 4, half));

                                BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(lvalPageOff + newR1Start, 4), 0U);
                                fullPayload.AsSpan(half).CopyTo(bytes.AsSpan(lvalPageOff + newR1Start + 4, len - half));

                                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(lvalPageOff + (kind == DatabaseFormat.Jet3Mdb ? 8 : 12), 2), 2);
                                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(lvalPageOff + rowsStartOff, 2), checked((ushort)newR0Start));
                                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(lvalPageOff + rowsStartOff + 2, 2), checked((ushort)newR1Start));
                                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(lvalPageOff + 2, 2), checked((ushort)(newR1Start - rowsStartOff - 4)));

                                // Change descriptor storage mode from 0x40 (SinglePage) to 0x00 (Chained).
                                BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(dataPageOff + pos, 4), len);
                                convertedToChained = true;
                                break;
                            }
                        }
                    }
                }
            }
        }

        Assert.True(convertedToChained);

        await using var modifiedSource = new MemoryStream(bytes, writable: false);
        await using var compactedDest = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(
            modifiedSource,
            compactedDest,
            new AccessCompactOptions
            {
                ClusterByPrimaryKey = true,
                CoalesceLongValueChains = true,
            },
            Ct);

        Assert.True(report.IsComplete);
        Assert.True(report.DatabaseValidated);
        Assert.Equal(1, report.ClusteredTableCount);
        Assert.Equal(1, report.CoalescedLongValueCount);

        AccessCompactTableSummary summary = Assert.Single(report.Tables, t => t.TableName == "CatalogItems");
        Assert.True(summary.ClusteredByPrimaryKey);
        Assert.Equal(1, summary.CoalescedLongValueCount);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(compactedDest, cancellationToken: Ct);
        var physicalOrderCodes = new List<string>();
        var descriptionsByCode = new Dictionary<string, string>();
        await foreach (object[] row in reader.Rows("CatalogItems", cancellationToken: Ct))
        {
            string code = Assert.IsType<string>(row[0]);
            string desc = Assert.IsType<string>(row[2]);
            physicalOrderCodes.Add(code);
            descriptionsByCode[code] = desc;
        }

        // Physical DATA page row order is now clustered in canonical Primary Key order!
        Assert.Equal(["Alpha", "Delta", "Mu", "Zeta"], physicalOrderCodes);
        Assert.Equal("short-A", descriptionsByCode["Alpha"]);
        Assert.Equal("short-D", descriptionsByCode["Delta"]);
        Assert.Equal("short-M", descriptionsByCode["Mu"]);
        Assert.Equal(new string('Z', 900), descriptionsByCode["Zeta"]);
    }

    [Fact]
    public async Task CompactAsync_PackInteriorFreePagesWithReferenceUsageMap_PacksHolesAcrossReferenceBitmapWindow()
    {
        await using var sourceStream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            sourceStream,
            DatabaseFormat.Jet4Mdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                "EarlyTable",
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Pad", typeof(string), 255)],
                cancellationToken: Ct);
            for (int i = 1; i <= 40; i++)
            {
                await writer.InsertRowAsync("EarlyTable", [i, new string('E', 240)], Ct);
            }

            // Create a large table whose DATA pages exceed Jet4's 512-page inline usage-map window,
            // so CompactUsageMapBuilder constructs a Reference usage map (0x01 + 0x05 bitmap page).
            await writer.CreateTableAsync(
                "LargeRefMapTable",
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("C1", typeof(string), 255),
                    new("C2", typeof(string), 255),
                    new("C3", typeof(string), 255),
                    new("C4", typeof(string), 255),
                ],
                cancellationToken: Ct);

            string p1 = new('A', 250);
            string p2 = new('B', 250);
            string p3 = new('C', 250);
            string p4 = new('D', 250);
            var batch = new List<object?[]>(1100);
            for (int i = 1; i <= 1100; i++)
            {
                batch.Add([i, p1, p2, p3, p4]);
            }

            await writer.InsertRowsAsync("LargeRefMapTable", batch, Ct);
            for (int i = 3; i <= 40; i++)
            {
                Assert.Equal(1, await writer.DeleteRowsAsync("EarlyTable", "Id", i, Ct));
            }
        }

        await using var compactedStream = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(
            sourceStream,
            compactedStream,
            new AccessCompactOptions { PackInteriorFreePages = true },
            Ct);

        Assert.True(report.IsComplete);
        Assert.True(report.DatabaseValidated);
        Assert.True(report.PackedInteriorPages > 0);
        Assert.Equal(0, report.ScrubbedFreePages);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(compactedStream, cancellationToken: Ct);
        Assert.Equal(2, await reader.GetRealRowCountAsync("EarlyTable", Ct));
        Assert.Equal(1100, await reader.GetRealRowCountAsync("LargeRefMapTable", Ct));
    }

    private static long FindUserTableTDefPage(byte[] bytes, int pageSize)
    {
        for (long p = 3; p < bytes.Length / pageSize; p++)
        {
            int offset = checked((int)(p * pageSize));
            if (bytes[offset] == 0x02)
            {
                int realIdxCount = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset + (pageSize == 2048 ? 28 : 40), 4));
                if (realIdxCount >= 1)
                {
                    // Return the last matching TDEF page (user tables are created after system tables).
                    long candidate = p;
                    for (long later = p + 1; later < bytes.Length / pageSize; later++)
                    {
                        int laterOffset = checked((int)(later * pageSize));
                        if (bytes[laterOffset] == 0x02)
                        {
                            candidate = later;
                        }
                    }

                    return candidate;
                }
            }
        }

        throw new InvalidOperationException("No user table TDEF page found.");
    }

    private sealed class SynchronousProgress<T>(Action<T> callback) : IProgress<T>
    {
        public void Report(T value) => callback(value);
    }
}
