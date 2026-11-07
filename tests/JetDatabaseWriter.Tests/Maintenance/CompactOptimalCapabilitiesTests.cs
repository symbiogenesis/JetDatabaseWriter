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
