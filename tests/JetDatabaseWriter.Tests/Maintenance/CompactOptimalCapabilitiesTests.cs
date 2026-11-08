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

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactToAsync_DamagedLvalPageHeaderAndMagic_RepairsLvalPageAndRecoversFullMemoPayload(DatabaseFormat kind)
    {
        await using var sourceStream = new MemoryStream();
        string memo1 = new('L', 900);
        string memo2 = new('M', 700);
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            sourceStream,
            kind,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                "Documents",
                [
                    new("Id", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true },
                    new("Title", typeof(string), 40),
                    new("Body", typeof(string), 0),
                ],
                cancellationToken: Ct);
            await writer.InsertRowAsync("Documents", [1, "Doc-One", memo1], Ct);
            await writer.InsertRowAsync("Documents", [2, "Doc-Two", memo2], Ct);
        }

        byte[] bytes = sourceStream.ToArray();
        int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        long lvalPage = 0;
        for (long p = 3; p < bytes.Length / pageSize; p++)
        {
            int pageOff = checked((int)(p * pageSize));
            if (bytes[pageOff] == 0x01 && BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pageOff + 4, 4)) == 0x4C41564CU)
            {
                lvalPage = p;
                bytes[pageOff] = 0x7F;
                BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(pageOff + 4, 4), 0xDEADCAFEU);
                break;
            }
        }

        Assert.True(lvalPage >= 3);

        await using var corruptedSource = new MemoryStream(bytes, writable: false);
        await using var compactedDest = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(
            corruptedSource,
            compactedDest,
            new AccessCompactOptions { AllowPartialRecovery = true },
            Ct);

        Assert.True(report.DatabaseValidated);
        Assert.Contains(
            report.Diagnostics,
            d => d.Kind == "salvaged-lval-page-header"
                && d.Category == AccessCompactDiagnosticCategory.SalvagedDataPage
                && d.PageNumber == lvalPage);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(compactedDest, cancellationToken: Ct);
        var rows = new List<object[]>();
        await foreach (object[] row in reader.Rows("Documents", cancellationToken: Ct))
        {
            rows.Add(row);
        }

        Assert.Equal(2, rows.Count);
        Assert.Equal(memo1, rows[0][2]);
        Assert.Equal(memo2, rows[1][2]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactToAsync_UnrecoverableAndTruncatedExternalLvalChains_PreservesScalarRowColumnsTruncatesPrefixAndNullsDeadLval(DatabaseFormat kind)
    {
        await using var sourceStream = new MemoryStream();
        string multiHopMemo = new string('A', 2500) + new string('B', 2500);
        string deadPageMemo = new('C', 900);
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            sourceStream,
            kind,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                "Articles",
                [
                    new("Id", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true },
                    new("Title", typeof(string), 40),
                    new("Notes", typeof(string), 0),
                    new("Views", typeof(int)),
                ],
                cancellationToken: Ct);
            await writer.InsertRowAsync("Articles", [1, "TruncatedArticle", multiHopMemo, 100], Ct);
            await writer.InsertRowAsync("Articles", [2, "DeadMemoArticle", deadPageMemo, 200], Ct);
            await writer.InsertRowAsync("Articles", [3, "HealthyArticle", "Short inline memo", 300], Ct);
        }

        byte[] bytes = sourceStream.ToArray();
        int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        int rowsStartOff = kind == DatabaseFormat.Jet3Mdb ? 10 : 14;
        long row0FirstPage = 0;
        long row0SecondPage = 0;
        long row1FirstPage = 0;

        for (long p = 3; p < bytes.Length / pageSize; p++)
        {
            int pageOff = checked((int)(p * pageSize));
            if (bytes[pageOff] != 0x01 || BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pageOff + 4, 4)) != 0x4C41564CU)
            {
                continue;
            }

            int r0Start = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(pageOff + rowsStartOff, 2)) & 0x1FFF;
            if (r0Start <= rowsStartOff || r0Start >= pageSize - 8)
            {
                continue;
            }

            if (bytes[pageOff + pageSize - 1] == (byte)'C' || bytes[pageOff + pageSize - 2] == (byte)'C')
            {
                row1FirstPage = p;
            }
            else if ((bytes[pageOff + pageSize - 1] == (byte)'A' || bytes[pageOff + pageSize - 2] == (byte)'A') && row0FirstPage == 0)
            {
                uint nextBookmark = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pageOff + r0Start, 4));
                if ((nextBookmark >> 8) >= 3)
                {
                    row0FirstPage = p;
                    row0SecondPage = nextBookmark >> 8;
                }
            }
        }

        Assert.True(row0FirstPage >= 3);
        Assert.True(row0SecondPage >= 3);
        Assert.True(row1FirstPage >= 3);
        Assert.NotEqual(row0FirstPage, row0SecondPage);
        Assert.NotEqual(row0FirstPage, row1FirstPage);

        // Corrupt row0SecondPage and row1FirstPage completely (0xFF over header and row directory) so hop 1 of row 0 survives
        // while hop 2 of row 0 and all of row 1's Memo LVAL are unrecoverable.
        bytes.AsSpan(checked((int)(row0SecondPage * pageSize)), pageSize).Fill(0xFF);
        if (row1FirstPage != row0SecondPage)
        {
            bytes.AsSpan(checked((int)(row1FirstPage * pageSize)), pageSize).Fill(0xFF);
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
                QuarantineStream = quarantineStream,
            },
            Ct);

        Assert.True(report.DatabaseValidated);
        Assert.Contains(
            report.Diagnostics,
            d => d.Kind == "salvaged-truncated-lval-chain"
                && d.Category == AccessCompactDiagnosticCategory.SalvagedDataPage);
        Assert.Contains(
            report.Diagnostics,
            d => d.Kind == "quarantined-corrupt-lval-field"
                && d.Category == AccessCompactDiagnosticCategory.QuarantinedRow);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(compactedDest, cancellationToken: Ct);
        var rows = new Dictionary<int, object[]>();
        await foreach (object[] row in reader.Rows("Articles", cancellationToken: Ct))
        {
            rows[Assert.IsType<int>(row[0])] = row;
        }

        Assert.Equal(3, rows.Count);
        Assert.Equal("TruncatedArticle", rows[1][1]);
        string truncatedMemo = Assert.IsType<string>(rows[1][2]);
        Assert.True(truncatedMemo.Length > 0 && truncatedMemo.Length < multiHopMemo.Length);
        Assert.StartsWith("AAAA", truncatedMemo, StringComparison.Ordinal);
        Assert.Equal(100, rows[1][3]);

        Assert.Equal("DeadMemoArticle", rows[2][1]);
        Assert.Equal(DBNull.Value, rows[2][2]);
        Assert.Equal(200, rows[2][3]);

        Assert.Equal("HealthyArticle", rows[3][1]);
        Assert.Equal("Short inline memo", rows[3][2]);
        Assert.Equal(300, rows[3][3]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactToAsync_CorruptTableAndLvalUsageMapPages_ReconstructsUsageMapsAndRecoversAllRows(DatabaseFormat kind)
    {
        await using var sourceStream = new MemoryStream();
        string largeMemo = new('D', 900);
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            sourceStream,
            kind,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                "Ledger",
                [
                    new("Id", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true },
                    new("Entry", typeof(string), 40),
                    new("Details", typeof(string), 0),
                ],
                cancellationToken: Ct);
            await writer.InsertRowAsync("Ledger", [1, "First", largeMemo], Ct);
            await writer.InsertRowAsync("Ledger", [2, "Second", "short-2"], Ct);
            await writer.InsertRowAsync("Ledger", [3, "Third", "short-3"], Ct);
        }

        AccessCompactReport inspect = await AccessDatabaseCompact.InspectAsync(sourceStream, cancellationToken: Ct);
        long ledgerTDef = Assert.Single(inspect.Tables, t => t.TableName == "Ledger").OwnerTDefPage;

        byte[] bytes = sourceStream.ToArray();
        int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        int rowsStartOff = kind == DatabaseFormat.Jet3Mdb ? 10 : 14;
        int usedPagesOff = kind == DatabaseFormat.Jet3Mdb ? 35 : 55;
        int freePagesOff = kind == DatabaseFormat.Jet3Mdb ? 39 : 59;
        int tdefOff = checked((int)(ledgerTDef * pageSize));

        uint usedMapPtr = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(tdefOff + usedPagesOff, 4));
        uint freeMapPtr = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(tdefOff + freePagesOff, 4));
        CorruptUsageMapSlot(bytes, pageSize, rowsStartOff, usedMapPtr);
        CorruptUsageMapSlot(bytes, pageSize, rowsStartOff, freeMapPtr);

        await using var corruptedSource = new MemoryStream(bytes, writable: false);
        await using var compactedDest = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(
            corruptedSource,
            compactedDest,
            new AccessCompactOptions { AllowPartialRecovery = true },
            Ct);

        Assert.True(report.DatabaseValidated);
        Assert.Contains(
            report.Diagnostics,
            d => d.Kind == "salvaged-usage-map"
                && d.Category == AccessCompactDiagnosticCategory.SalvagedDataPage
                && d.TableName == "Ledger");

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(compactedDest, cancellationToken: Ct);
        var rows = new List<object[]>();
        await foreach (object[] row in reader.Rows("Ledger", cancellationToken: Ct))
        {
            rows.Add(row);
        }

        Assert.Equal(3, rows.Count);
        Assert.Equal(largeMemo, rows[0][2]);
        Assert.Equal("short-2", rows[1][2]);
        Assert.Equal("short-3", rows[2][2]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactToAsync_QuarantineConstraintViolatingRows_QuarantinesDuplicateAndNullKeyRowsWhilePreservingPrimaryKeyAndUniqueIndexes(DatabaseFormat kind)
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
                "Accounts",
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("Email", typeof(string), 40),
                    new("Balance", typeof(int)),
                ],
                [new IndexDefinition("IX_Accounts_Email", "Email") { IsUnique = true }],
                Ct);
            await writer.InsertRowAsync("Accounts", [1, "alice@example.com", 100], Ct);
            await writer.InsertRowAsync("Accounts", [2, "bob@example.com", 200], Ct);
            await writer.InsertRowAsync("Accounts", [3, "charlie@example.com", 300], Ct);
        }

        AccessCompactReport inspect = await AccessDatabaseCompact.InspectAsync(sourceStream, cancellationToken: Ct);
        long accountsTDef = Assert.Single(inspect.Tables, t => t.TableName == "Accounts").OwnerTDefPage;

        byte[] bytes = sourceStream.ToArray();
        int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        int rowsStartOff = kind == DatabaseFormat.Jet3Mdb ? 10 : 14;

        for (long p = 3; p < bytes.Length / pageSize; p++)
        {
            int pageOff = checked((int)(p * pageSize));
            if (bytes[pageOff] == 0x01 && BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pageOff + 4, 4)) == accountsTDef)
            {
                int slot0Start = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(pageOff + rowsStartOff, 2)) & 0x1FFF;
                int slot1Start = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(pageOff + rowsStartOff + 2, 2)) & 0x1FFF;
                int slot2Start = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(pageOff + rowsStartOff + 4, 2)) & 0x1FFF;

                // Copy row 0 ("alice@example.com") over row 1, keeping Id=2 in fixed column 0 so row 1 has a duplicate Unique Email!
                int row0Len = pageSize - slot0Start;
                int row1Len = slot0Start - slot1Start;
                if (row0Len == row1Len)
                {
                    bytes.AsSpan(pageOff + slot0Start, row0Len).CopyTo(bytes.AsSpan(pageOff + slot1Start, row1Len));
                    int fixedStartOff = kind == DatabaseFormat.Jet3Mdb ? 1 : 2;
                    BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(pageOff + slot1Start + fixedStartOff, 4), 2);
                }
                else
                {
                    // Even simpler: copy row 0's exact bytes into row 1 by pointing slot 1's directory entry or copying row 0 into a new slice,
                    // or make row 1 have Duplicate PK Id = 1!
                    int fixedStartOff = kind == DatabaseFormat.Jet3Mdb ? 1 : 2;
                    BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(pageOff + slot1Start + fixedStartOff, 4), 1);
                }

                // Clear the null-mask bit for column 0 (Id, PrimaryKey) in row 2 (slot 2, whose slice is [slot2Start .. slot1Start)) so row 2 has a NULL Primary Key!
                bytes[pageOff + slot1Start - 1] &= 0xFE;
            }
            else if (bytes[pageOff] == 0x04 && BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pageOff + 4, 4)) == accountsTDef)
            {
                // Corrupt index pages so index regeneration is triggered.
                bytes[pageOff] = 0x7F;
                BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(pageOff + 4, 4), 0U);
            }
        }

        // With QuarantineConstraintViolatingRows = false (default), regeneration is refused on constraint violations.
        await using (var strictSource = new MemoryStream(bytes, writable: false))
        await using (var strictDest = new MemoryStream())
        {
            await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                await AccessDatabaseCompact.CompactAsync(
                    strictSource,
                    strictDest,
                    new AccessCompactOptions
                    {
                        AllowPartialRecovery = true,
                        RebuildDamagedIndexes = true,
                        QuarantineConstraintViolatingRows = false,
                    },
                    Ct));
        }

        // With QuarantineConstraintViolatingRows = true, offending rows are quarantined and indexes regenerate cleanly.
        await using var salvageSource = new MemoryStream(bytes, writable: false);
        await using var salvageDest = new MemoryStream();
        await using var quarantineStream = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(
            salvageSource,
            salvageDest,
            new AccessCompactOptions
            {
                AllowPartialRecovery = true,
                RebuildDamagedIndexes = true,
                RepairTableCounters = true,
                QuarantineConstraintViolatingRows = true,
                QuarantineStream = quarantineStream,
            },
            Ct);

        Assert.True(report.DatabaseValidated);
        Assert.Equal(2, report.QuarantinedRowCount);
        Assert.Equal(2, report.Diagnostics.Count(d => d.Kind == "quarantined-constraint-row" && d.Category == AccessCompactDiagnosticCategory.QuarantinedRow));

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(salvageDest, cancellationToken: Ct);
        object[] survivingRow = await reader.Rows("Accounts", cancellationToken: Ct).SingleAsync(Ct);
        Assert.Equal(1, survivingRow[0]);
        Assert.Equal("alice@example.com", survivingRow[1]);
        Assert.Equal(100, survivingRow[2]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactToAsync_OrphanedUserTableTDefMissingFromMSysObjects_ReattachesTableToCatalogAndRecoversRows(DatabaseFormat kind)
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
                "VisibleTable",
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Label", typeof(string), 30)],
                cancellationToken: Ct);
            await writer.InsertRowAsync("VisibleTable", [1, "Vis-1"], Ct);

            await writer.CreateTableAsync(
                "OrphanedVault",
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Secret", typeof(string), 40)],
                cancellationToken: Ct);
            await writer.InsertRowAsync("OrphanedVault", [10, "Secret-Alpha"], Ct);
            await writer.InsertRowAsync("OrphanedVault", [20, "Secret-Beta"], Ct);
            await writer.InsertRowAsync("OrphanedVault", [30, "Secret-Gamma"], Ct);
        }

        AccessCompactReport inspect = await AccessDatabaseCompact.InspectAsync(sourceStream, cancellationToken: Ct);
        long vaultTDef = Assert.Single(inspect.Tables, t => t.TableName == "OrphanedVault").OwnerTDefPage;

        byte[] bytes = sourceStream.ToArray();
        int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        int rowsStartOff = kind == DatabaseFormat.Jet3Mdb ? 10 : 14;
        int numRowsHeaderOff = kind == DatabaseFormat.Jet3Mdb ? 8 : 12;
        int fixedIdOff = kind == DatabaseFormat.Jet3Mdb ? 1 : 2;

        // Find the MSysObjects DATA page row for OrphanedVault (Id == vaultTDef) and mark that row slot as deleted (0x8000).
        bool deletedCatalogRow = false;
        for (long p = 3; p < bytes.Length / pageSize && !deletedCatalogRow; p++)
        {
            int pageOff = checked((int)(p * pageSize));
            if (bytes[pageOff] != 0x01 || BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pageOff + 4, 4)) != 2U)
            {
                continue;
            }

            int numRows = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(pageOff + numRowsHeaderOff, 2));
            for (int r = 0; r < numRows; r++)
            {
                int slotOff = pageOff + rowsStartOff + (r * 2);
                ushort rawSlot = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(slotOff, 2));
                if ((rawSlot & 0xC000) != 0)
                {
                    continue;
                }

                int rowStart = rawSlot & 0x1FFF;
                int id = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(pageOff + rowStart + fixedIdOff, 4));
                if (id == (int)vaultTDef)
                {
                    BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(slotOff, 2), (ushort)(rawSlot | 0x8000));
                    deletedCatalogRow = true;
                    break;
                }
            }
        }

        Assert.True(deletedCatalogRow);

        await using var corruptedSource = new MemoryStream(bytes, writable: false);
        await using var compactedDest = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(
            corruptedSource,
            compactedDest,
            new AccessCompactOptions
            {
                AllowPartialRecovery = true,
                RebuildDamagedIndexes = true,
                RepairTableCounters = true,
            },
            Ct);

        string expectedRecoveredName = $"RecoveredTable_{vaultTDef}";
        Assert.True(report.DatabaseValidated);
        Assert.Contains(
            report.Diagnostics,
            d => d.Kind == "salvaged-orphan-table"
                && d.Category == AccessCompactDiagnosticCategory.CatalogUncertainty
                && d.TableName == expectedRecoveredName);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(compactedDest, cancellationToken: Ct);
        var secrets = new List<string>();
        await foreach (object[] row in reader.Rows(expectedRecoveredName, cancellationToken: Ct))
        {
            secrets.Add(Assert.IsType<string>(row[1]));
        }

        Assert.Equal(["Secret-Alpha", "Secret-Beta", "Secret-Gamma"], secrets);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactAsync_CoalesceLongValueChainsAndInlinePromotion_PhysicallyShrinksLvalPagesAndPromotesSmallExternalMemosInline(DatabaseFormat kind)
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
                "CompactMemos",
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("MemoText", typeof(string), 0),
                ],
                cancellationToken: Ct);

            // Insert a large Memo first so an LVAL page is allocated for CompactMemos.
            await writer.InsertRowAsync("CompactMemos", [1, new string('X', 800)], Ct);

            await writer.CreateTableAsync(
                "TrailingTable",
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("Tag", typeof(string), 30),
                ],
                cancellationToken: Ct);
            await writer.InsertRowAsync("TrailingTable", [100, "Trailing-Row"], Ct);
        }

        // Shrink the external LVAL payload on CompactMemos from 800 chars down to 16 bytes on disk,
        // leaving it stored as a SinglePageStorageMode (0x40) external LVAL on its LVAL page.
        // When CoalesceLongValueChains = true, this <= 64-byte external LVAL is promoted inline (0x80) onto the DATA page,
        // freeing the entire LVAL page and physically shrinking DestinationPages!
        byte[] bytes = sourceStream.ToArray();
        int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        int rowsStartOff = kind == DatabaseFormat.Jet3Mdb ? 10 : 14;
        AccessCompactReport inspect = await AccessDatabaseCompact.InspectAsync(sourceStream, cancellationToken: Ct);
        long memosOwner = Assert.Single(inspect.Tables, t => t.TableName == "CompactMemos").OwnerTDefPage;

        byte[] smallEncoded = kind == DatabaseFormat.Jet3Mdb
            ? System.Text.Encoding.ASCII.GetBytes("InlinePromoted!")
            : System.Text.Encoding.Unicode.GetBytes("InlinePromoted!");

        bool patched = false;
        for (long p = 3; p < bytes.Length / pageSize && !patched; p++)
        {
            int dataPageOff = checked((int)(p * pageSize));
            if (bytes[dataPageOff] != 0x01 || BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(dataPageOff + 4, 4)) != memosOwner)
            {
                continue;
            }

            int slot0Start = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(dataPageOff + rowsStartOff, 2)) & 0x1FFF;
            for (int pos = slot0Start; pos <= pageSize - 12; pos++)
            {
                uint rawLen = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(dataPageOff + pos, 4));
                byte mode = (byte)((rawLen >> 24) & 0xC0U);
                int len = (int)(rawLen & 0x3FFFFFFFU);
                if (mode == 0x40 && len >= 700)
                {
                    uint firstDp = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(dataPageOff + pos + 4, 4));
                    long lvalPage = firstDp >> 8;
                    int lvalPageOff = checked((int)(lvalPage * pageSize));
                    int r0Start = pageSize - smallEncoded.Length;
                    smallEncoded.CopyTo(bytes.AsSpan(lvalPageOff + r0Start, smallEncoded.Length));
                    BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(lvalPageOff + rowsStartOff, 2), checked((ushort)r0Start));
                    BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(lvalPageOff + 2, 2), checked((ushort)(r0Start - rowsStartOff - 2)));
                    BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(dataPageOff + pos, 4), 0x40000000U | (uint)smallEncoded.Length);
                    patched = true;
                    break;
                }
            }
        }

        Assert.True(patched);

        await using var uncoalescedSource = new MemoryStream(bytes, writable: false);
        await using var uncoalescedDest = new MemoryStream();
        AccessCompactReport uncoalescedReport = await AccessDatabaseCompact.CompactAsync(
            uncoalescedSource,
            uncoalescedDest,
            new AccessCompactOptions
            {
                CoalesceLongValueChains = false,
                PackInteriorFreePages = false,
            },
            Ct);

        await using var coalescedSource = new MemoryStream(bytes, writable: false);
        await using var coalescedDest = new MemoryStream();
        AccessCompactReport coalescedReport = await AccessDatabaseCompact.CompactAsync(
            coalescedSource,
            coalescedDest,
            new AccessCompactOptions
            {
                CoalesceLongValueChains = true,
                PackInteriorFreePages = false,
            },
            Ct);

        Assert.True(coalescedReport.IsComplete);
        Assert.True(coalescedReport.DatabaseValidated);
        Assert.Equal(1, coalescedReport.CoalescedLongValueCount);
        Assert.True(coalescedReport.DestinationPages < uncoalescedReport.DestinationPages);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(coalescedDest, cancellationToken: Ct);
        object[] memoRow = await reader.Rows("CompactMemos", cancellationToken: Ct).SingleAsync(Ct);
        Assert.Equal(1, memoRow[0]);
        Assert.Equal("InlinePromoted!", memoRow[1]);

        object[] trailingRow = await reader.Rows("TrailingTable", cancellationToken: Ct).SingleAsync(Ct);
        Assert.Equal(100, trailingRow[0]);
        Assert.Equal("Trailing-Row", trailingRow[1]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactAsync_MultiPageTDefContinuationPackingAndDeadTDefReclamation_PacksContinuationPagesAndReclaimsDeadTDefs(DatabaseFormat kind)
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
                "EarlySpacer",
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Pad", typeof(string), 250)],
                cancellationToken: Ct);
            for (int i = 1; i <= 25; i++)
            {
                await writer.InsertRowAsync("EarlySpacer", [i, new string('S', 220)], Ct);
            }

            var wideColumns = new List<ColumnDefinition>(120)
            {
                new("Id", typeof(int)) { IsPrimaryKey = true },
            };
            for (int c = 1; c < 120; c++)
            {
                wideColumns.Add(new($"WideField_{c:D3}", typeof(int)));
            }

            await writer.CreateTableAsync("WideTable", wideColumns, cancellationToken: Ct);
            object?[] rowValues = new object?[120];
            for (int c = 0; c < 120; c++)
            {
                rowValues[c] = c + 1;
            }

            await writer.InsertRowAsync("WideTable", rowValues, Ct);

            for (int i = 2; i <= 25; i++)
            {
                Assert.Equal(1, await writer.DeleteRowsAsync("EarlySpacer", "Id", i, Ct));
            }
        }

        // Append an orphaned/dead 0x02 TDef page at the end of the file and mark it allocated in the global usage map (page 1, row 0).
        byte[] bytes = sourceStream.ToArray();
        int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        long deadTDefPage = bytes.Length / pageSize;
        byte[] extended = new byte[bytes.Length + pageSize];
        bytes.CopyTo(extended, 0);
        int deadOff = checked((int)(deadTDefPage * pageSize));
        extended[deadOff] = 0x02;
        extended[deadOff + 1] = 0x01;
        BinaryPrimitives.WriteUInt32LittleEndian(extended.AsSpan(deadOff + 8, 4), 63U);
        MarkGlobalUsageMapAllocated(extended, pageSize, deadTDefPage);

        await using var unpackedSource = new MemoryStream(extended, writable: false);
        await using var unpackedDest = new MemoryStream();
        AccessCompactReport unpackedReport = await AccessDatabaseCompact.CompactAsync(
            unpackedSource,
            unpackedDest,
            new AccessCompactOptions
            {
                AllowPartialRecovery = true,
                PackInteriorFreePages = false,
            },
            Ct);

        await using var packedSource = new MemoryStream(extended, writable: false);
        await using var packedDest = new MemoryStream();
        AccessCompactReport packedReport = await AccessDatabaseCompact.CompactAsync(
            packedSource,
            packedDest,
            new AccessCompactOptions
            {
                AllowPartialRecovery = true,
                PackInteriorFreePages = true,
            },
            Ct);

        Assert.True(packedReport.DatabaseValidated);
        Assert.Contains(
            packedReport.Diagnostics,
            d => d.Kind == "salvaged-dead-tdef-page"
                && d.Category == AccessCompactDiagnosticCategory.SalvagedDataPage
                && d.PageNumber == deadTDefPage);
        Assert.True(packedReport.PackedInteriorPages > 0);
        Assert.True(packedReport.DestinationPages < unpackedReport.DestinationPages);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(packedDest, cancellationToken: Ct);
        Assert.Equal(1, await reader.GetRealRowCountAsync("WideTable", Ct));
        object[] readRow = await reader.Rows("WideTable", cancellationToken: Ct).SingleAsync(Ct);
        Assert.Equal(1, readRow[0]);
        Assert.Equal(120, readRow[119]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactAsync_DamagedMultiPageTDefContinuationHeader_SalvagesTDefChainAndRecoversTableRows(DatabaseFormat kind)
    {
        await using var sourceStream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            sourceStream,
            kind,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: Ct))
        {
            var wideColumns = new List<ColumnDefinition>(120)
            {
                new("Id", typeof(int)) { IsPrimaryKey = true },
            };
            for (int c = 1; c < 120; c++)
            {
                wideColumns.Add(new($"ColName_{c:D3}", typeof(int)));
            }

            await writer.CreateTableAsync("MultiPageTDefTable", wideColumns, cancellationToken: Ct);
            for (int r = 1; r <= 3; r++)
            {
                object?[] vals = new object?[120];
                vals[0] = r;
                for (int c = 1; c < 120; c++)
                {
                    vals[c] = (r * 1000) + c;
                }

                await writer.InsertRowAsync("MultiPageTDefTable", vals, Ct);
            }
        }

        AccessCompactReport inspect = await AccessDatabaseCompact.InspectAsync(sourceStream, cancellationToken: Ct);
        long wideTDef = Assert.Single(inspect.Tables, t => t.TableName == "MultiPageTDefTable").OwnerTDefPage;

        byte[] bytes = sourceStream.ToArray();
        int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        int headOff = checked((int)(wideTDef * pageSize));
        uint contPage = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(headOff + 4, 4));
        Assert.True(contPage >= 3);

        // Corrupt the TDef continuation page's page-type byte (0x02 -> 0x7F).
        bytes[checked((int)(contPage * pageSize))] = 0x7F;

        await using (var strictSource = new MemoryStream(bytes, writable: false))
        await using (var strictDest = new MemoryStream())
        {
            await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                await AccessDatabaseCompact.CompactAsync(
                    strictSource,
                    strictDest,
                    new AccessCompactOptions { AllowPartialRecovery = false },
                    Ct));
        }

        await using var salvageSource = new MemoryStream(bytes, writable: false);
        await using var salvageDest = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(
            salvageSource,
            salvageDest,
            new AccessCompactOptions { AllowPartialRecovery = true },
            Ct);

        Assert.True(report.DatabaseValidated);
        Assert.Contains(
            report.Diagnostics,
            d => d.Kind == "salvaged-tdef-continuation"
                && d.Category == AccessCompactDiagnosticCategory.SalvagedDataPage
                && d.PageNumber == contPage);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(salvageDest, cancellationToken: Ct);
        Assert.Equal(3, await reader.GetRealRowCountAsync("MultiPageTDefTable", Ct));
        var rows = new List<object[]>();
        await foreach (object[] row in reader.Rows("MultiPageTDefTable", cancellationToken: Ct))
        {
            rows.Add(row);
        }

        Assert.Equal(3, rows.Count);
        Assert.Equal(1119, rows[0][119]);
        Assert.Equal(3119, rows[2][119]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactAsync_FragmentedMultiHopLvalChain_PhysicallyShrinksLvalPages(DatabaseFormat kind)
    {
        string expectedMemo = new('Z', 300);
        await using var sourceStream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            sourceStream,
            kind,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                "MultiHopMemos",
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("MemoBody", typeof(string), 0),
                ],
                cancellationToken: Ct);
            await writer.InsertRowAsync("MultiHopMemos", [1, expectedMemo], Ct);

            await writer.CreateTableAsync(
                "AnchorAfterMemos",
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Name", typeof(string), 30)],
                cancellationToken: Ct);
            await writer.InsertRowAsync("AnchorAfterMemos", [1, "Anchor"], Ct);
        }

        AccessCompactReport inspect = await AccessDatabaseCompact.InspectAsync(sourceStream, cancellationToken: Ct);
        long memosOwner = Assert.Single(inspect.Tables, t => t.TableName == "MultiHopMemos").OwnerTDefPage;

        byte[] bytes = sourceStream.ToArray();
        int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        int rowsStartOff = kind == DatabaseFormat.Jet3Mdb ? 10 : 14;
        int numRowsHeaderOff = kind == DatabaseFormat.Jet3Mdb ? 8 : 12;
        int lvMapPtrOff = kind == DatabaseFormat.Jet3Mdb ? 43 : 63;

        // Locate the SinglePageStorageMode (0x40) LVAL descriptor on MultiHopMemos's DATA page,
        // and split its payload across 3 ChainedStorageMode (0x00) LVAL pages!
        long lvalPage2 = bytes.Length / pageSize;
        long lvalPage3 = lvalPage2 + 1;
        byte[] extended = new byte[bytes.Length + (2 * pageSize)];
        bytes.CopyTo(extended, 0);

        int tdefOff = checked((int)(memosOwner * pageSize));
        uint lvMapPtr = BinaryPrimitives.ReadUInt32LittleEndian(extended.AsSpan(tdefOff + lvMapPtrOff, 4));
        MarkInlineUsageMapBit(extended, pageSize, rowsStartOff, lvMapPtr, lvalPage2);
        MarkInlineUsageMapBit(extended, pageSize, rowsStartOff, lvMapPtr, lvalPage3);
        MarkGlobalUsageMapAllocated(extended, pageSize, lvalPage2);
        MarkGlobalUsageMapAllocated(extended, pageSize, lvalPage3);

        bool split = false;
        for (long p = 3; p < bytes.Length / pageSize && !split; p++)
        {
            int dataPageOff = checked((int)(p * pageSize));
            if (extended[dataPageOff] != 0x01 || BinaryPrimitives.ReadUInt32LittleEndian(extended.AsSpan(dataPageOff + 4, 4)) != memosOwner)
            {
                continue;
            }

            int slot0Start = BinaryPrimitives.ReadUInt16LittleEndian(extended.AsSpan(dataPageOff + rowsStartOff, 2)) & 0x1FFF;
            for (int pos = slot0Start; pos <= pageSize - 12; pos++)
            {
                uint rawLen = BinaryPrimitives.ReadUInt32LittleEndian(extended.AsSpan(dataPageOff + pos, 4));
                byte mode = (byte)((rawLen >> 24) & 0xC0U);
                int totalLen = (int)(rawLen & 0x3FFFFFFFU);
                if (mode == 0x40 && totalLen >= 250)
                {
                    uint firstDp = BinaryPrimitives.ReadUInt32LittleEndian(extended.AsSpan(dataPageOff + pos + 4, 4));
                    long lvalPage1 = firstDp >> 8;
                    int lval1Off = checked((int)(lvalPage1 * pageSize));
                    int r0Start = BinaryPrimitives.ReadUInt16LittleEndian(extended.AsSpan(lval1Off + rowsStartOff, 2)) & 0x1FFF;
                    byte[] fullPayload = extended.AsSpan(lval1Off + r0Start, totalLen).ToArray();

                    int part1Len = totalLen / 3;
                    int part2Len = totalLen / 3;
                    int part3Len = totalLen - part1Len - part2Len;

                    WriteChainedLvalPage(extended, pageSize, rowsStartOff, numRowsHeaderOff, lvalPage1, fullPayload.AsSpan(0, part1Len), (uint)((lvalPage2 << 8) | 0));
                    WriteChainedLvalPage(extended, pageSize, rowsStartOff, numRowsHeaderOff, lvalPage2, fullPayload.AsSpan(part1Len, part2Len), (uint)((lvalPage3 << 8) | 0));
                    WriteChainedLvalPage(extended, pageSize, rowsStartOff, numRowsHeaderOff, lvalPage3, fullPayload.AsSpan(part1Len + part2Len, part3Len), 0U);

                    // Change the 12-byte LVAL descriptor mode from 0x40 (SinglePage) to 0x00 (Chained).
                    BinaryPrimitives.WriteUInt32LittleEndian(extended.AsSpan(dataPageOff + pos, 4), (uint)totalLen);
                    BinaryPrimitives.WriteUInt32LittleEndian(extended.AsSpan(dataPageOff + pos + 4, 4), (uint)((lvalPage1 << 8) | 0));
                    split = true;
                    break;
                }
            }
        }

        Assert.True(split);

        await using var uncoalescedSource = new MemoryStream(extended, writable: false);
        await using var uncoalescedDest = new MemoryStream();
        AccessCompactReport uncoalescedReport = await AccessDatabaseCompact.CompactAsync(
            uncoalescedSource,
            uncoalescedDest,
            new AccessCompactOptions
            {
                CoalesceLongValueChains = false,
                PackInteriorFreePages = false,
            },
            Ct);

        await using var coalescedSource = new MemoryStream(extended, writable: false);
        await using var coalescedDest = new MemoryStream();
        AccessCompactReport coalescedReport = await AccessDatabaseCompact.CompactAsync(
            coalescedSource,
            coalescedDest,
            new AccessCompactOptions
            {
                CoalesceLongValueChains = true,
                PackInteriorFreePages = false,
            },
            Ct);

        Assert.True(coalescedReport.IsComplete);
        Assert.True(coalescedReport.DatabaseValidated);
        Assert.Equal(1, coalescedReport.CoalescedLongValueCount);
        Assert.True(coalescedReport.DestinationPages <= uncoalescedReport.DestinationPages - 2);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(coalescedDest, cancellationToken: Ct);
        object[] row = await reader.Rows("MultiHopMemos", cancellationToken: Ct).SingleAsync(Ct);
        Assert.Equal(1, row[0]);
        Assert.Equal(expectedMemo, row[1]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactAsync_BrokenRowOverflowChain_SalvagesDamagedTargetPageAndPromotesOrphanedMovedRow(DatabaseFormat kind)
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
                "MovedRowsTable",
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("Note", typeof(string), 80),
                ],
                cancellationToken: Ct);
            await writer.InsertRowAsync("MovedRowsTable", [1, "FirstInlineRow"], Ct);
            await writer.InsertRowAsync("MovedRowsTable", [2, "SecondMovedRowPayload"], Ct);
        }

        AccessCompactReport inspect = await AccessDatabaseCompact.InspectAsync(sourceStream, cancellationToken: Ct);
        long tableOwner = Assert.Single(inspect.Tables, t => t.TableName == "MovedRowsTable").OwnerTDefPage;

        byte[] baseBytes = sourceStream.ToArray();
        int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        int rowsStartOff = kind == DatabaseFormat.Jet3Mdb ? 10 : 14;
        int numRowsHeaderOff = kind == DatabaseFormat.Jet3Mdb ? 8 : 12;
        int usedPagesOff = kind == DatabaseFormat.Jet3Mdb ? 35 : 55;

        // Allocate a second DATA page (targetPage) for MovedRowsTable and turn row 1 on sourceDataPage into
        // an OverflowRowFlag (0x4000) pointer targeting slot 0 (DeletedRowFlag 0x8000) on targetPage!
        long targetPage = baseBytes.Length / pageSize;
        byte[] overflowBytes = new byte[baseBytes.Length + pageSize];
        baseBytes.CopyTo(overflowBytes, 0);

        int tdefOff = checked((int)(tableOwner * pageSize));
        uint usedMapPtr = BinaryPrimitives.ReadUInt32LittleEndian(overflowBytes.AsSpan(tdefOff + usedPagesOff, 4));
        MarkInlineUsageMapBit(overflowBytes, pageSize, rowsStartOff, usedMapPtr, targetPage);
        MarkGlobalUsageMapAllocated(overflowBytes, pageSize, targetPage);

        long sourceDataPage = 0;
        for (long p = 3; p < baseBytes.Length / pageSize; p++)
        {
            int pageOff = checked((int)(p * pageSize));
            if (overflowBytes[pageOff] == 0x01 && BinaryPrimitives.ReadUInt32LittleEndian(overflowBytes.AsSpan(pageOff + 4, 4)) == tableOwner)
            {
                sourceDataPage = p;
                int slot0Start = BinaryPrimitives.ReadUInt16LittleEndian(overflowBytes.AsSpan(pageOff + rowsStartOff, 2)) & 0x1FFF;
                int slot1Start = BinaryPrimitives.ReadUInt16LittleEndian(overflowBytes.AsSpan(pageOff + rowsStartOff + 2, 2)) & 0x1FFF;
                int row1Len = slot0Start - slot1Start;
                byte[] row1Payload = overflowBytes.AsSpan(pageOff + slot1Start, row1Len).ToArray();

                // Write targetPage with 0 direct live rows and 1 moved-row target slot (0x8000 | targetRowStart) at slot 0.
                int targetOff = checked((int)(targetPage * pageSize));
                overflowBytes[targetOff] = 0x01;
                overflowBytes[targetOff + 1] = 0x01;
                BinaryPrimitives.WriteUInt32LittleEndian(overflowBytes.AsSpan(targetOff + 4, 4), checked((uint)tableOwner));
                BinaryPrimitives.WriteUInt16LittleEndian(overflowBytes.AsSpan(targetOff + numRowsHeaderOff, 2), 1);
                int targetRowStart = pageSize - row1Len;
                row1Payload.CopyTo(overflowBytes.AsSpan(targetOff + targetRowStart, row1Len));
                BinaryPrimitives.WriteUInt16LittleEndian(overflowBytes.AsSpan(targetOff + rowsStartOff, 2), checked((ushort)(0x8000 | targetRowStart)));
                BinaryPrimitives.WriteUInt16LittleEndian(overflowBytes.AsSpan(targetOff + 2, 2), checked((ushort)(targetRowStart - rowsStartOff - 2)));

                // Replace row 1 on sourceDataPage with a 4-byte overflow pointer (0x4000 | ptrStart) pointing to (targetPage, row 0).
                int ptrStart = slot0Start - 4;
                BinaryPrimitives.WriteUInt32LittleEndian(overflowBytes.AsSpan(pageOff + ptrStart, 4), checked((uint)((targetPage << 8) | 0)));
                BinaryPrimitives.WriteUInt16LittleEndian(overflowBytes.AsSpan(pageOff + rowsStartOff + 2, 2), checked((ushort)(0x4000 | ptrStart)));
                BinaryPrimitives.WriteUInt16LittleEndian(overflowBytes.AsSpan(pageOff + 2, 2), checked((ushort)(ptrStart - rowsStartOff - 4)));
                break;
            }
        }

        Assert.True(sourceDataPage >= 3);

        // Part A: Corrupt targetPage's page-type byte (0x7F) and owner header (0U).
        // Even though targetPage has 0 direct live rows (only a 0x8000 moved-row target slot),
        // SalvageDamagedOverflowTargetPagesAsync repairs targetPage ("salvaged-overflow-target-page")!
        byte[] targetDamagedBytes = (byte[])overflowBytes.Clone();
        int targetPageOff = checked((int)(targetPage * pageSize));
        targetDamagedBytes[targetPageOff] = 0x7F;
        BinaryPrimitives.WriteUInt32LittleEndian(targetDamagedBytes.AsSpan(targetPageOff + 4, 4), 0U);

        await using (var targetSource = new MemoryStream(targetDamagedBytes, writable: false))
        await using (var targetDest = new MemoryStream())
        {
            AccessCompactReport targetReport = await AccessDatabaseCompact.CompactAsync(
                targetSource,
                targetDest,
                new AccessCompactOptions { AllowPartialRecovery = true },
                Ct);

            Assert.True(targetReport.DatabaseValidated);
            Assert.Contains(
                targetReport.Diagnostics,
                d => d.Kind == "salvaged-overflow-target-page"
                    && d.Category == AccessCompactDiagnosticCategory.SalvagedDataPage
                    && d.PageNumber == targetPage);

            await using AccessReader targetReader = await TestDatabase.OpenReaderAsync(targetDest, cancellationToken: Ct);
            var notes = new List<string>();
            await foreach (object[] row in targetReader.Rows("MovedRowsTable", cancellationToken: Ct))
            {
                notes.Add(Assert.IsType<string>(row[1]));
            }

            Assert.Equal(["FirstInlineRow", "SecondMovedRowPayload"], notes);
        }

        // Part B: Corrupt sourceDataPage's row directory (numRows = 9999) so sourceDataPage is quarantined ("quarantined-data-page"),
        // orphaning the 0x8000 moved-row payload on targetPage. PromoteOrphanedMovedRowPayloadsAsync promotes it ("salvaged-overflow-row")!
        byte[] pointerQuarantinedBytes = (byte[])overflowBytes.Clone();
        int sourcePageOff = checked((int)(sourceDataPage * pageSize));
        BinaryPrimitives.WriteUInt16LittleEndian(pointerQuarantinedBytes.AsSpan(sourcePageOff + numRowsHeaderOff, 2), 9999);

        await using (var orphanSource = new MemoryStream(pointerQuarantinedBytes, writable: false))
        await using (var orphanDest = new MemoryStream())
        {
            AccessCompactReport orphanReport = await AccessDatabaseCompact.CompactAsync(
                orphanSource,
                orphanDest,
                new AccessCompactOptions
                {
                    AllowPartialRecovery = true,
                    RebuildDamagedIndexes = true,
                    RepairTableCounters = true,
                },
                Ct);

            Assert.True(orphanReport.DatabaseValidated);
            Assert.Contains(
                orphanReport.Diagnostics,
                d => d.Kind == "salvaged-overflow-row"
                    && d.Category == AccessCompactDiagnosticCategory.SalvagedDataPage
                    && d.PageNumber == targetPage);

            await using AccessReader orphanReader = await TestDatabase.OpenReaderAsync(orphanDest, cancellationToken: Ct);
            object[] rescuedRow = await orphanReader.Rows("MovedRowsTable", cancellationToken: Ct).SingleAsync(Ct);
            Assert.Equal(2, rescuedRow[0]);
            Assert.Equal("SecondMovedRowPayload", rescuedRow[1]);
        }
    }

    [Fact]
    public async Task CompactAsync_CalculatedColumnIndex_VerifiesStoredKeysAndRegeneratesDamagedOrStaleCalculatedIndex()
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
                "CalcOrders",
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("Qty", typeof(int)),
                    new("Price", typeof(int)),
                    new("LineTotal", typeof(int))
                    {
                        IsCalculated = true,
                        CalculationExpression = "[Qty] * [Price]",
                    },
                    new("Code", typeof(string), 40),
                    new("UpperCode", typeof(string), 50)
                    {
                        IsCalculated = true,
                        CalculationExpression = "[Code] & \"-CALC\"",
                    },
                ],
                [
                    new IndexDefinition("IX_CalcOrders_LineTotal", "LineTotal"),
                    new IndexDefinition("IX_CalcOrders_UpperCode", "UpperCode"),
                ],
                Ct);

            for (int i = 1; i <= 10; i++)
            {
                await writer.InsertRowAsync("CalcOrders", [i, i * 2, 10, null, $"ORD{i:D2}", null], Ct);
            }
        }

        // 1. Clean compact validates calculated-column indexes against cached row envelopes.
        await using (var cleanDest = new MemoryStream())
        {
            AccessCompactReport cleanReport = await AccessDatabaseCompact.CompactAsync(sourceStream, cleanDest, cancellationToken: Ct);
            Assert.True(cleanReport.IsComplete);
            Assert.True(cleanReport.DatabaseValidated);
        }

        // 2. Corrupt the calculated-column index leaf pages and verify RebuildDamagedIndexes regenerates them from cached envelopes.
        AccessCompactReport inspect = await AccessDatabaseCompact.InspectAsync(sourceStream, cancellationToken: Ct);
        long calcOwner = Assert.Single(inspect.Tables, t => t.TableName == "CalcOrders").OwnerTDefPage;

        byte[] bytes = sourceStream.ToArray();
        const int pageSize = 4096;
        int corruptedIndexPages = 0;
        for (long p = 3; p < bytes.Length / pageSize; p++)
        {
            int off = checked((int)(p * pageSize));
            if (bytes[off] == 0x04 && BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(off + 4, 4)) == calcOwner)
            {
                bytes[off] = 0x7F;
                corruptedIndexPages++;
            }
        }

        Assert.True(corruptedIndexPages >= 2);

        await using (var strictSource = new MemoryStream(bytes, writable: false))
        await using (var strictDest = new MemoryStream())
        {
            await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                await AccessDatabaseCompact.CompactAsync(
                    strictSource,
                    strictDest,
                    new AccessCompactOptions { AllowPartialRecovery = false },
                    Ct));
        }

        await using var regenSource = new MemoryStream(bytes, writable: false);
        await using var regenDest = new MemoryStream();
        AccessCompactReport regenReport = await AccessDatabaseCompact.CompactAsync(
            regenSource,
            regenDest,
            new AccessCompactOptions
            {
                AllowPartialRecovery = true,
                RebuildDamagedIndexes = true,
            },
            Ct);

        Assert.True(regenReport.DatabaseValidated);
        Assert.Equal(3, regenReport.RegeneratedIndexCount);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(regenDest, cancellationToken: Ct);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("CalcOrders", Ct);
        Assert.Equal(3, indexes.Count);
        Assert.Contains(indexes, idx => idx.Name == "IX_CalcOrders_LineTotal");
        Assert.Contains(indexes, idx => idx.Name == "IX_CalcOrders_UpperCode");

        int rowCount = 0;
        await foreach (object[] row in reader.Rows("CalcOrders", cancellationToken: Ct))
        {
            rowCount++;
            int id = Assert.IsType<int>(row[0]);
            Assert.Equal(id * 20, row[3]);
            Assert.Equal($"ORD{id:D2}-CALC", row[5]);
        }

        Assert.Equal(10, rowCount);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactAsync_QuarantinedRows_PopulatesStructuredDecodedFieldsAndFixedColumnFallbackInReportAndNdjson(DatabaseFormat kind)
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
                "AuditMembers",
                [
                    new("Id", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true },
                    new("Name", typeof(string), 40),
                    new("Score", typeof(int)),
                ],
                cancellationToken: Ct);
            await writer.InsertRowAsync("AuditMembers", [1, "Alice", 100], Ct);
            await writer.InsertRowAsync("AuditMembers", [2, "BobCorruptVarOffsets", 250], Ct);
            await writer.InsertRowAsync("AuditMembers", [3, "Charlie", 300], Ct);
        }

        AccessCompactReport inspect = await AccessDatabaseCompact.InspectAsync(sourceStream, cancellationToken: Ct);
        long owner = Assert.Single(inspect.Tables, t => t.TableName == "AuditMembers").OwnerTDefPage;

        byte[] bytes = sourceStream.ToArray();
        int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        int rowsStartOff = kind == DatabaseFormat.Jet3Mdb ? 10 : 14;
        for (long p = 3; p < bytes.Length / pageSize; p++)
        {
            int pageOff = checked((int)(p * pageSize));
            if (bytes[pageOff] == 0x01 && BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pageOff + 4, 4)) == owner)
            {
                // Corrupt row 1's variable-offset table at the end of row 1 (just before the null mask),
                // leaving its fixed columns (Id=2, Score=250) intact so TryDecodeFixedColumnsFallback rescues them!
                int slot0Start = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(pageOff + rowsStartOff, 2)) & 0x0FFF;
                for (int pos = slot0Start - 4; pos < slot0Start - 1; pos++)
                {
                    bytes[pageOff + pos] = 0xFF;
                }

                break;
            }
        }

        await using var corruptedSource = new MemoryStream(bytes, writable: false);
        await using var salvagedDest = new MemoryStream();
        await using var quarantineStream = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(
            corruptedSource,
            salvagedDest,
            new AccessCompactOptions
            {
                AllowPartialRecovery = true,
                RebuildDamagedIndexes = true,
                RepairTableCounters = true,
                QuarantineStream = quarantineStream,
            },
            Ct);

        Assert.True(report.DatabaseValidated);
        AccessCompactQuarantineRecord qRec = Assert.Single(report.QuarantinedRecords);
        Assert.Equal("quarantined-corrupt-row", qRec.Kind);
        Assert.NotNull(qRec.DecodedFields);
        Assert.Equal(2, qRec.DecodedFields!["Id"]);
        Assert.Equal(250, qRec.DecodedFields["Score"]);

        quarantineStream.Position = 0;
        using var sr = new StreamReader(quarantineStream);
        string ndjsonLine = (await sr.ReadToEndAsync(Ct)).Trim();
        using var jsonDoc = JsonDocument.Parse(ndjsonLine);
        JsonElement decodedFieldsElem = jsonDoc.RootElement.GetProperty("decodedFields");
        Assert.Equal(2, decodedFieldsElem.GetProperty("Id").GetInt32());
        Assert.Equal(250, decodedFieldsElem.GetProperty("Score").GetInt32());
    }

    private static void MarkGlobalUsageMapAllocated(byte[] bytes, int pageSize, long pageNumber)
    {
        int rowsStartOff = pageSize == 2048 ? 10 : 14;
        int page1Off = pageSize;
        int row0Start = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(page1Off + rowsStartOff, 2)) & 0x1FFF;
        int bitmapStart = page1Off + row0Start + 5;
        int byteIndex = checked((int)(pageNumber >> 3));
        int bitMask = 1 << (int)(pageNumber & 7);
        bytes[bitmapStart + byteIndex] = (byte)(bytes[bitmapStart + byteIndex] & ~bitMask);
    }

    private static void MarkInlineUsageMapBit(byte[] bytes, int pageSize, int rowsStartOff, uint mapPtr, long pageNumber)
    {
        long mapPage = mapPtr >> 8;
        int mapRow = (int)(mapPtr & 0xFFU);
        int pageOff = checked((int)(mapPage * pageSize));
        int slotOff = pageOff + rowsStartOff + (mapRow * 2);
        int rowStart = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(slotOff, 2)) & 0x1FFF;
        int startPage = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(pageOff + rowStart + 1, 4));
        int relativeBit = checked((int)(pageNumber - startPage));
        int bitmapStart = pageOff + rowStart + 5;
        bytes[bitmapStart + (relativeBit >> 3)] |= (byte)(1 << (relativeBit & 7));
    }

    private static void WriteChainedLvalPage(
        byte[] bytes,
        int pageSize,
        int rowsStartOff,
        int numRowsHeaderOff,
        long lvalPage,
        ReadOnlySpan<byte> chunkPayload,
        uint nextPointer)
    {
        int pageOff = checked((int)(lvalPage * pageSize));
        Array.Clear(bytes, pageOff, pageSize);
        bytes[pageOff] = 0x01;
        bytes[pageOff + 1] = 0x01;
        "LVAL"u8.CopyTo(bytes.AsSpan(pageOff + 4, 4));
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(pageOff + numRowsHeaderOff, 2), 1);
        int rowLen = 4 + chunkPayload.Length;
        int rowStart = pageSize - rowLen;
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(pageOff + rowStart, 4), nextPointer);
        chunkPayload.CopyTo(bytes.AsSpan(pageOff + rowStart + 4, chunkPayload.Length));
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(pageOff + rowsStartOff, 2), checked((ushort)rowStart));
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(pageOff + 2, 2), checked((ushort)(rowStart - rowsStartOff - 2)));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactAsync_ClusterByPrimaryKey_MultiPageSecondaryIndexWithDuplicates_ClustersAndValidates(DatabaseFormat kind)
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
                "Events",
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("CategoryKey", typeof(string), 120) { IsNullable = false },
                ],
                [new IndexDefinition("IX_CategoryKey", "CategoryKey")],
                Ct);

            // Insert 90 rows in reverse PK order with duplicate CategoryKey values (3 rows per category)
            // long enough to force IX_CategoryKey across multiple leaf pages and an intermediate root page.
            for (int id = 90; id >= 1; id--)
            {
                int bucket = (id - 1) / 3;
                string category = $"Category_{bucket:D3}_" + new string((char)('A' + (bucket % 26)), 85);
                await writer.InsertRowAsync("Events", [id, category], Ct);
            }
        }

        sourceStream.Position = 0;
        await using var destStream = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(
            sourceStream,
            destStream,
            new AccessCompactOptions
            {
                ClusterByPrimaryKey = true,
            },
            Ct);

        Assert.True(report.ReplacementAuthorized);
        Assert.Equal(1, report.ClusteredTableCount);
        AccessCompactTableSummary summary = Assert.Single(report.Tables, static t => t.TableName == "Events");
        Assert.True(summary.ClusteredByPrimaryKey);

        destStream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(destStream, cancellationToken: Ct);
        var ids = new List<int>();
        await foreach (object[] row in reader.Rows("Events", cancellationToken: Ct))
        {
            ids.Add(Assert.IsType<int>(row[0]));
        }

        Assert.Equal(90, ids.Count);
        for (int i = 0; i < 90; i++)
        {
            Assert.Equal(i + 1, ids[i]);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactAsync_CoalesceLongValueChains_GreedyInlinePromotionOnNearlyFullDataPage_PromotesFittingRows(DatabaseFormat kind)
    {
        await using var sourceStream = new MemoryStream();
        int rowCount = kind == DatabaseFormat.Jet3Mdb ? 12 : 23;
        string pad = new('P', kind == DatabaseFormat.Jet3Mdb ? 130 : 165);
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            sourceStream,
            kind,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                "DenseMemos",
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("Padding", typeof(string), 200) { IsNullable = false },
                    new("MemoText", typeof(string), 0),
                ],
                cancellationToken: Ct);

            for (int i = 1; i <= rowCount; i++)
            {
                await writer.InsertRowAsync("DenseMemos", [i, pad, new string('M', 700)], Ct);
            }
        }

        byte[] bytes = sourceStream.ToArray();
        int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        int rowsStartOff = kind == DatabaseFormat.Jet3Mdb ? 10 : 14;
        AccessCompactReport inspect = await AccessDatabaseCompact.InspectAsync(sourceStream, cancellationToken: Ct);
        long memosOwner = Assert.Single(inspect.Tables, static t => t.TableName == "DenseMemos").OwnerTDefPage;

        byte[] smallEncoded = kind == DatabaseFormat.Jet3Mdb
            ? System.Text.Encoding.ASCII.GetBytes(new string('S', 60))
            : System.Text.Encoding.Unicode.GetBytes(new string('S', 30));

        // Shrink every external 0x40 LVAL descriptor on DenseMemos's first DATA page down to 60 bytes.
        // Because the page holds multiple 200+ byte rows, promoting ALL rows on the page (+60 bytes * rowCount)
        // would overflow pageSize, whereas greedy per-row promotion promotes the subset of rows that fit!
        int patchedOnFirstDataPage = 0;
        for (long p = 3; p < bytes.Length / pageSize; p++)
        {
            int dataPageOff = checked((int)(p * pageSize));
            if (bytes[dataPageOff] != 0x01 || BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(dataPageOff + 4, 4)) != memosOwner)
            {
                continue;
            }

            int numRows = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(dataPageOff + (kind == DatabaseFormat.Jet3Mdb ? 8 : 12), 2));
            for (int r = 0; r < numRows; r++)
            {
                int rowStart = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(dataPageOff + rowsStartOff + (r * 2), 2)) & 0x1FFF;
                for (int pos = rowStart; pos <= pageSize - 12; pos++)
                {
                    uint rawLen = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(dataPageOff + pos, 4));
                    byte mode = (byte)((rawLen >> 24) & 0xC0U);
                    int len = (int)(rawLen & 0x3FFFFFFFU);
                    if (mode == 0x40 && len >= 600)
                    {
                        uint firstDp = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(dataPageOff + pos + 4, 4));
                        long lvalPage = firstDp >> 8;
                        int lvalRow = (int)(firstDp & 0xFFU);
                        if (lvalPage >= 3 && lvalPage < bytes.Length / pageSize && lvalRow == 0)
                        {
                            int lvalPageOff = checked((int)(lvalPage * pageSize));
                            int r0Start = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(lvalPageOff + rowsStartOff, 2)) & 0x1FFF;
                            smallEncoded.CopyTo(bytes.AsSpan(lvalPageOff + r0Start, smallEncoded.Length));
                            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(dataPageOff + pos, 4), 0x40000000U | (uint)smallEncoded.Length);
                            patchedOnFirstDataPage++;
                            break;
                        }
                    }
                }
            }

            if (patchedOnFirstDataPage > 0)
            {
                break;
            }
        }

        Assert.True(patchedOnFirstDataPage >= 3);

        await using var coalescedSource = new MemoryStream(bytes, writable: false);
        await using var coalescedDest = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(
            coalescedSource,
            coalescedDest,
            new AccessCompactOptions
            {
                CoalesceLongValueChains = true,
            },
            Ct);

        Assert.True(report.IsComplete);
        Assert.True(report.DatabaseValidated);
        Assert.True(report.CoalescedLongValueCount > 0);
        Assert.True(report.CoalescedLongValueCount < patchedOnFirstDataPage);
    }

    private static void CorruptUsageMapSlot(byte[] bytes, int pageSize, int rowsStartOff, uint mapPtr)
    {
        long mapPage = mapPtr >> 8;
        int mapRow = (int)(mapPtr & 0xFFU);
        if (mapPage < 3 || mapPage >= bytes.Length / pageSize)
        {
            return;
        }

        int pageOff = checked((int)(mapPage * pageSize));
        int slotOff = pageOff + rowsStartOff + (mapRow * 2);
        int rowStart = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(slotOff, 2)) & 0x1FFF;
        bytes[pageOff + rowStart] = 0xFF;
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(pageOff + rowStart + 1, 4), -999);
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
