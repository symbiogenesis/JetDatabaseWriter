namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>
/// End-to-end tests for <see cref="AccessDatabaseCompact"/>, <see cref="CompactDatabasePlanner"/>,
/// and <see cref="CompactReclamationPlan"/>.
/// </summary>
public sealed class CompactReclaimingDatabaseTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactAsync_AfterDeletingRowsAndDroppingTable_ReclaimsPagesShrinksAndIsIdempotent(DatabaseFormat kind)
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
                "Items",
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Memo", typeof(string), 20_000)],
                [new IndexDefinition("ById", "Id")],
                Ct);
            await writer.CreateTableAsync(
                "DroppedBulk",
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Payload", typeof(string), 20_000)],
                cancellationToken: Ct);
            for (int i = 0; i < 35; i++)
            {
                await writer.InsertRowAsync("DroppedBulk", [i, new string('x', 2800)], Ct);
            }

            for (int i = 0; i < 30; i++)
            {
                await writer.InsertRowAsync("Items", [i, new string((char)('a' + (i % 20)), 2400)], Ct);
            }

            for (int i = 5; i < 30; i++)
            {
                Assert.Equal(1, await writer.DeleteRowsAsync("Items", "Id", i, Ct));
            }

            await writer.DropTableAsync("DroppedBulk", Ct);
        }

        byte[] sourceBytes = sourceStream.ToArray();
        await using var compactedStream = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(sourceStream, compactedStream, cancellationToken: Ct);

        Assert.Equal(kind, report.Format);
        Assert.True(report.IsComplete);
        Assert.True(report.DatabaseValidated);
        Assert.True(report.ReclamationAuthorized);
        Assert.True(report.ReplacementAuthorized);
        Assert.Empty(report.Diagnostics);
        Assert.True(report.ReclaimedPages > 0);
        Assert.True(report.DestinationPages < report.SourcePages);
        Assert.True(compactedStream.Length < sourceBytes.Length);
        Assert.Equal(sourceBytes, sourceStream.ToArray());

        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(compactedStream, cancellationToken: Ct))
        {
            IReadOnlyList<string> tables = await reader.ListTablesAsync(Ct);
            Assert.Equal(["Items"], tables);
            int count = 0;
            await foreach (object[] row in reader.Rows("Items", cancellationToken: Ct))
            {
                int id = Assert.IsType<int>(row[0]);
                Assert.Equal(count, id);
                Assert.Equal(new string((char)('a' + id), 2400), row[1]);
                count++;
            }

            Assert.Equal(5, count);
        }

        await using var compactedTwiceStream = new MemoryStream();
        AccessCompactReport secondReport = await AccessDatabaseCompact.CompactAsync(compactedStream, compactedTwiceStream, cancellationToken: Ct);
        Assert.True(secondReport.IsComplete);
        Assert.True(secondReport.DatabaseValidated);
        Assert.True(secondReport.ReplacementAuthorized);
        Assert.Equal(report.DestinationPages, secondReport.DestinationPages);
        Assert.Equal(compactedStream.Length, compactedTwiceStream.Length);

        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            compactedStream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: Ct))
        {
            await writer.InsertRowAsync("Items", [99, new string('z', 2400)], Ct);
        }

        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(compactedStream, cancellationToken: Ct))
        {
            Assert.Equal(6, await reader.GetRealRowCountAsync("Items", Ct));
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactAsync_InteriorReclaimedPrefixPages_AreScrubbedMarkedFreeAndReusedByWriter(DatabaseFormat kind)
    {
        string secretMarker = "SECRET_DELETED_PAYLOAD_" + new string('S', 1500);
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
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Secret", typeof(string), 20_000)],
                cancellationToken: Ct);
            for (int i = 0; i < 160; i++)
            {
                await writer.InsertRowAsync("EarlyTable", [i, secretMarker], Ct);
            }

            await writer.CreateTableAsync(
                "LateTable",
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Label", typeof(string), 80)],
                cancellationToken: Ct);
            await writer.InsertRowAsync("LateTable", [1, "keep"], Ct);

            for (int i = 1; i < 160; i++)
            {
                Assert.Equal(1, await writer.DeleteRowsAsync("EarlyTable", "Id", i, Ct));
            }
        }

        await using var compactedStream = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(sourceStream, compactedStream, cancellationToken: Ct);

        Assert.True(report.IsComplete);
        Assert.True(report.DatabaseValidated);
        Assert.True(report.ReclamationAuthorized);
        Assert.True(report.ReplacementAuthorized);
        Assert.True(report.PackedInteriorPages > 0);
        Assert.True(report.ScrubbedFreePages > 0);

        byte[] compactedBytes = compactedStream.ToArray();
        int pageSize = report.PageSize;
        int actualScrubbedCount = 0;
        for (long page = 3; page < report.RetainedPrefixPages; page++)
        {
            ReadOnlySpan<byte> pageSpan = compactedBytes.AsSpan(checked((int)(page * pageSize)), pageSize);
            if (CompactReclamationPlan.IsScrubbedFreePage(pageSpan, pageSize))
            {
                actualScrubbedCount++;
            }
        }

        Assert.Equal(report.ScrubbedFreePages, actualScrubbedCount);

        long lengthBeforeReuse = compactedStream.Length;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            compactedStream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                "ReusedSpaceTable",
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Note", typeof(string), 80)],
                cancellationToken: Ct);
            await writer.InsertRowAsync("ReusedSpaceTable", [1, "reused interior free page"], Ct);
        }

        Assert.Equal(lengthBeforeReuse, compactedStream.Length);
    }

    [Fact]
    public async Task CompactAsync_PreservesRelationshipsCalculatedAndComplexColumns()
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
                "Departments",
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Name", typeof(string), 80)],
                cancellationToken: Ct);
            await writer.CreateTableAsync(
                "Orders",
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("DeptId", typeof(int)),
                    new("Price", typeof(int)),
                    new("Qty", typeof(int)),
                    new("Total", typeof(int)) { IsCalculated = true, CalculationExpression = "[Price]*[Qty]" },
                    new("Tags", typeof(int)) { IsMultiValue = true, MultiValueElementType = typeof(int) },
                ],
                [new IndexDefinition("IX_Orders_DeptId", "DeptId")],
                Ct);

            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_Orders_Departments", "Departments", "Id", "Orders", "DeptId")
                {
                    EnforceReferentialIntegrity = true,
                },
                Ct);

            await writer.InsertRowAsync("Departments", [1, "Engineering"], Ct);
            await writer.InsertRowAsync("Departments", [2, "Research"], Ct);
            for (int i = 1; i <= 10; i++)
            {
                await writer.InsertRowAsync("Orders", [i, (i % 2) + 1, i * 10, 3, null, null], Ct);
                await writer.AddMultiValueItemAsync("Orders", "Tags", new Dictionary<string, object?> { ["Id"] = i }, i * 100, Ct);
            }

            for (int i = 4; i <= 10; i++)
            {
                Assert.Equal(1, await writer.DeleteRowsAsync("Orders", "Id", i, Ct));
            }
        }

        await using var compactedStream = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(sourceStream, compactedStream, cancellationToken: Ct);

        Assert.True(report.IsComplete);
        Assert.True(report.DatabaseValidated);
        Assert.True(report.ReclamationAuthorized);
        Assert.True(report.ReplacementAuthorized);
        Assert.Empty(report.Diagnostics);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(compactedStream, cancellationToken: Ct);
        IReadOnlyList<RelationshipMetadata> relationships = await reader.ListRelationshipsAsync(Ct);
        Assert.Contains(relationships, r => r.Name == "FK_Orders_Departments" && r.PrimaryTable == "Departments" && r.ForeignTable == "Orders");

        IReadOnlyList<ColumnMetadata> ordersCols = await reader.GetColumnMetadataAsync("Orders", Ct);
        ColumnMetadata totalCol = Assert.Single(ordersCols, c => c.Name == "Total");
        Assert.True(totalCol.IsCalculated);

        IReadOnlyList<MultiValueItem> items = await reader.GetMultiValueItemsAsync("Orders", "Tags", Ct);
        Assert.Equal(3, items.Count);

        int orderCount = 0;
        await foreach (object[] row in reader.Rows("Orders", cancellationToken: Ct))
        {
            int id = Assert.IsType<int>(row[0]);
            Assert.Equal(id * 30, row[4]);
            orderCount++;
        }

        Assert.Equal(3, orderCount);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactToAsync_And_CompactInPlaceAsync_EncryptedDatabases_RoundTripAndReclaim(DatabaseFormat kind)
    {
        string tempDir = Path.Combine(Path.GetTempPath(), "JetDatabaseWriter.CompactEncrypted." + Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(tempDir);
        try
        {
            string ext = kind == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb";
            string sourcePath = Path.Combine(tempDir, "source" + ext);
            string copyPath = Path.Combine(tempDir, "copy" + ext);
            const string password = "SecretPassword123";

            await using var sourceStream = new MemoryStream();
            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(sourceStream, kind, leaveOpen: true, cancellationToken: Ct))
            {
                await writer.CreateTableAsync(
                    "Items",
                    [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Memo", typeof(string), 20_000)],
                    cancellationToken: Ct);
                await writer.CreateTableAsync(
                    "DroppedTemp",
                    [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Payload", typeof(string), 20_000)],
                    cancellationToken: Ct);
                for (int i = 0; i < 35; i++)
                {
                    await writer.InsertRowAsync("DroppedTemp", [i, new string('d', 2800)], Ct);
                }

                for (int i = 0; i < 25; i++)
                {
                    await writer.InsertRowAsync("Items", [i, new string('e', 2600)], Ct);
                }

                for (int i = 3; i < 25; i++)
                {
                    Assert.Equal(1, await writer.DeleteRowsAsync("Items", "Id", i, Ct));
                }

                await writer.DropTableAsync("DroppedTemp", Ct);
            }

            await File.WriteAllBytesAsync(sourcePath, sourceStream.ToArray(), Ct);
            await AccessDatabaseEncryption.EncryptAsync(sourcePath, password.AsMemory(), cancellationToken: Ct);
            long encryptedSizeBefore = new FileInfo(sourcePath).Length;

            var compactOptions = new AccessCompactOptions { Password = password.AsMemory() };
            AccessCompactReport toReport = await AccessDatabaseCompact.CompactToAsync(sourcePath, copyPath, compactOptions, Ct);
            Assert.True(toReport.IsEncrypted);
            Assert.True(toReport.IsComplete);
            Assert.True(toReport.DatabaseValidated);
            Assert.True(toReport.ReclamationAuthorized);
            Assert.True(toReport.ReplacementAuthorized);
            Assert.True(toReport.DestinationPages < toReport.SourcePages);
            Assert.True(new FileInfo(copyPath).Length < encryptedSizeBefore);

            AccessCompactReport inPlaceReport = await AccessDatabaseCompact.CompactInPlaceAsync(sourcePath, compactOptions, Ct);
            Assert.True(inPlaceReport.IsEncrypted);
            Assert.True(inPlaceReport.DatabaseValidated);
            Assert.True(inPlaceReport.ReplacementAuthorized);
            Assert.Equal(toReport.DestinationPages, inPlaceReport.DestinationPages);
            Assert.True(new FileInfo(sourcePath).Length < encryptedSizeBefore);

            await using AccessReader reader = await AccessReader.OpenAsync(
                sourcePath,
                new AccessReaderOptions { Password = password.AsMemory() },
                Ct);
            Assert.Equal(3, await reader.GetRealRowCountAsync("Items", Ct));
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DamagedCatalogOwnerHeader_SalvagesWithAllowPartialRecovery_AndRefusesInPlace(DatabaseFormat kind)
    {
        string tempDir = Path.Combine(Path.GetTempPath(), "JetDatabaseWriter.CompactDamagedCatalog." + Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(tempDir);
        try
        {
            string ext = kind == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb";
            string sourcePath = Path.Combine(tempDir, "damaged" + ext);
            string salvagePath = Path.Combine(tempDir, "salvage" + ext);

            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(sourcePath, kind, cancellationToken: Ct))
            {
                await writer.CreateTableAsync(
                    "Items",
                    [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Name", typeof(string), 80)],
                    cancellationToken: Ct);
                await writer.InsertRowAsync("Items", [1, "alpha"], Ct);
                await writer.InsertRowAsync("Items", [2, "beta"], Ct);
            }

            long userTDefPage;
            int pageSize;
            int tdefOff;
            await using (AccessReader inspect = await AccessReader.OpenAsync(sourcePath, cancellationToken: Ct))
            {
                userTDefPage = Assert.IsType<CatalogEntry>(await inspect.GetCatalogEntryAsync("Items", Ct)).TDefPage;
                pageSize = inspect.Database.Format.PageSize;
                tdefOff = inspect.Database.Format.DataPage.TDefOff;
            }

            byte[] damagedBytes = await File.ReadAllBytesAsync(sourcePath, Ct);
            int catalogDataPage = Enumerable.Range(3, (damagedBytes.Length / pageSize) - 3).First(page =>
                damagedBytes[page * pageSize] == Constants.PageTypes.Data
                && BinaryPrimitives.ReadInt32LittleEndian(damagedBytes.AsSpan((page * pageSize) + tdefOff)) == 2);
            BinaryPrimitives.WriteInt32LittleEndian(
                damagedBytes.AsSpan((catalogDataPage * pageSize) + tdefOff),
                checked((int)userTDefPage));
            await File.WriteAllBytesAsync(sourcePath, damagedBytes, Ct);

            await Assert.ThrowsAsync<JetCorruptDataException>(
                async () => await AccessDatabaseCompact.CompactInPlaceAsync(sourcePath, cancellationToken: Ct));
            Assert.Equal(damagedBytes, await File.ReadAllBytesAsync(sourcePath, Ct));

            await Assert.ThrowsAsync<JetCorruptDataException>(
                async () => await AccessDatabaseCompact.CompactToAsync(sourcePath, salvagePath, new AccessCompactOptions { AllowPartialRecovery = false }, Ct));
            Assert.False(File.Exists(salvagePath));

            AccessCompactReport report = await AccessDatabaseCompact.CompactToAsync(
                sourcePath,
                salvagePath,
                new AccessCompactOptions { AllowPartialRecovery = true },
                Ct);
            Assert.False(report.IsComplete);
            Assert.True(report.DatabaseValidated);
            Assert.False(report.ReclamationAuthorized);
            Assert.False(report.ReplacementAuthorized);
            Assert.True(report.RecoveredCatalogRowCount > 0);
            Assert.Contains(report.Diagnostics, d => d.Kind == "catalog-recovered-row");

            await using AccessReader salvagedReader = await AccessReader.OpenAsync(salvagePath, cancellationToken: Ct);
            Assert.Equal(2, await salvagedReader.GetRealRowCountAsync("Items", Ct));
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task UnassignedPhysicalTableRow_CorroboratesActiveRows_PinsSourcePage_AndRefusesInPlace(DatabaseFormat kind)
    {
        string tempDir = Path.Combine(Path.GetTempPath(), "JetDatabaseWriter.CompactUnassignedRow." + Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(tempDir);
        try
        {
            string ext = kind == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb";
            string sourcePath = Path.Combine(tempDir, "unassigned" + ext);
            string salvagePath = Path.Combine(tempDir, "salvage" + ext);

            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(sourcePath, kind, cancellationToken: Ct))
            {
                await writer.CreateTableAsync(
                    "Items",
                    [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Name", typeof(string), 40)],
                    cancellationToken: Ct);
                for (int i = 1; i <= 3; i++)
                {
                    await writer.InsertRowAsync("Items", [i, "row" + i], Ct);
                }
            }

            long itemsTDefPage;
            int pageSize;
            int tdefOff;
            await using (AccessReader inspect = await AccessReader.OpenAsync(sourcePath, cancellationToken: Ct))
            {
                itemsTDefPage = Assert.IsType<CatalogEntry>(await inspect.GetCatalogEntryAsync("Items", Ct)).TDefPage;
                pageSize = inspect.Database.Format.PageSize;
                tdefOff = inspect.Database.Format.DataPage.TDefOff;
            }

            byte[] originalBytes = await File.ReadAllBytesAsync(sourcePath, Ct);
            int itemsDataPage = Enumerable.Range(3, (originalBytes.Length / pageSize) - 3).First(page =>
                originalBytes[page * pageSize] == Constants.PageTypes.Data
                && BinaryPrimitives.ReadInt32LittleEndian(originalBytes.AsSpan((page * pageSize) + tdefOff)) == itemsTDefPage);
            byte[] withSurplusDataPage = new byte[originalBytes.Length + pageSize];
            Buffer.BlockCopy(originalBytes, 0, withSurplusDataPage, 0, originalBytes.Length);
            Buffer.BlockCopy(originalBytes, itemsDataPage * pageSize, withSurplusDataPage, originalBytes.Length, pageSize);
            await File.WriteAllBytesAsync(sourcePath, withSurplusDataPage, Ct);

            await Assert.ThrowsAsync<JetCorruptDataException>(
                async () => await AccessDatabaseCompact.CompactInPlaceAsync(sourcePath, cancellationToken: Ct));
            Assert.Equal(withSurplusDataPage, await File.ReadAllBytesAsync(sourcePath, Ct));

            await Assert.ThrowsAsync<JetCorruptDataException>(
                async () => await AccessDatabaseCompact.CompactToAsync(sourcePath, salvagePath, new AccessCompactOptions { AllowPartialRecovery = false }, Ct));

            AccessCompactReport report = await AccessDatabaseCompact.CompactToAsync(
                sourcePath,
                salvagePath,
                new AccessCompactOptions { AllowPartialRecovery = true },
                Ct);
            Assert.False(report.IsComplete);
            Assert.True(report.DatabaseValidated);
            Assert.False(report.ReclamationAuthorized);
            Assert.False(report.ReplacementAuthorized);
            Assert.Equal(3, report.UnassignedRowCount);
            Assert.Contains(report.Diagnostics, d => d.Kind == "unassigned-table-row");

            await using AccessReader salvagedReader = await AccessReader.OpenAsync(salvagePath, cancellationToken: Ct);
            Assert.Equal(3, await salvagedReader.GetRealRowCountAsync("Items", Ct));
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task CompactArgumentsAndOptions_ValidateBeforeMutatingFiles()
    {
        await using var stream = new MemoryStream();
        await Assert.ThrowsAsync<ArgumentException>(
            async () => await AccessDatabaseCompact.CompactAsync(stream, stream, cancellationToken: Ct));

        string tempDir = Path.Combine(Path.GetTempPath(), "JetDatabaseWriter.CompactValidation." + Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(tempDir);
        try
        {
            string sourcePath = Path.Combine(tempDir, "source.accdb");
            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(sourcePath, DatabaseFormat.AceAccdb, cancellationToken: Ct))
            {
                await writer.CreateTableAsync(
                    "Items",
                    [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Name", typeof(string), 40)],
                    cancellationToken: Ct);
            }

            await Assert.ThrowsAsync<ArgumentException>(
                async () => await AccessDatabaseCompact.CompactToAsync(sourcePath, sourcePath, cancellationToken: Ct));
            await Assert.ThrowsAsync<ArgumentException>(
                async () => await AccessDatabaseCompact.CompactInPlaceAsync(sourcePath, new AccessCompactOptions { AllowPartialRecovery = true }, Ct));
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public async Task FrozenNativeFixtures_CompactAsync_PreservesApplicationCodeAndSecurityObjects()
    {
        await VerifyFrozenNativeFixtureAsync("ApplicationObjects", "NativeInertApplicationObjects.accdb", null);
        await VerifyFrozenNativeFixtureAsync("ApplicationObjects", "NativeInertCodeObjects.accdb", null);
        await VerifyFrozenNativeFixtureAsync("Encrypted", "NativeJet4Workgroup.mdb", "Native123");
        await VerifyFrozenNativeFixtureAsync("Encrypted", "NativeJet4WorkgroupChanged.mdb", "Changed123");
    }

    private static async Task VerifyFrozenNativeFixtureAsync(string folder, string fileName, string? password)
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Databases", folder, fileName), Ct);
        await using var sourceStream = new MemoryStream(original, writable: false);
        await using var compactedStream = new MemoryStream();
        var options = new AccessCompactOptions(password) { AllowPartialRecovery = true };
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(sourceStream, compactedStream, options, Ct);

        Assert.True(report.DatabaseValidated);
        Assert.Equal(password is not null, report.IsEncrypted);

        await using var sourceCopy = new MemoryStream(original, writable: false);
        await using AccessReader sourceReader = await AccessReader.OpenAsync(sourceCopy, new AccessReaderOptions(password), leaveOpen: true, cancellationToken: Ct);
        await using AccessReader destReader = await AccessReader.OpenAsync(compactedStream, new AccessReaderOptions(password), leaveOpen: true, cancellationToken: Ct);
        Assert.Equal(await sourceReader.ListTablesAsync(Ct), await destReader.ListTablesAsync(Ct));
        Assert.Equal(await sourceReader.GetRealRowCountAsync("MSysObjects", Ct), await destReader.GetRealRowCountAsync("MSysObjects", Ct));
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public Task CompactedDatabase_Dao120ReadsMutatesAndRecompacts(DatabaseFormat kind)
        => VerifyNativeCompactedAsync(kind);

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public Task CompactedDatabase_Dao36ReadsMutatesAndRecompacts()
        => VerifyNativeCompactedAsync(DatabaseFormat.Jet3Mdb);

    private static async Task VerifyNativeCompactedAsync(DatabaseFormat kind)
    {
        await using var native = AccessRoundTripSession.CreateEmpty(
            "JetDatabaseWriter.Tests.CompactReclaim",
            databaseExtension: kind == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");

        await using var sourceStream = new MemoryStream();
        if (kind == DatabaseFormat.Jet3Mdb)
        {
            byte[] template = await File.ReadAllBytesAsync(
                Path.Combine(AppContext.BaseDirectory, "Databases", "Jet3", "NativeDescriptorMetadata.mdb"),
                Ct);
            await sourceStream.WriteAsync(template, Ct);
            sourceStream.Position = 0;
        }

        await using (AccessWriter writer = kind == DatabaseFormat.Jet3Mdb
            ? await AccessWriter.OpenAsync(sourceStream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct)
            : await AccessWriter.CreateDatabaseAsync(sourceStream, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync(
                "Items",
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Memo", typeof(string), 20_000)],
                cancellationToken: Ct);
            await writer.CreateTableAsync(
                "DroppedTemp",
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Notes", typeof(string), 20_000)],
                cancellationToken: Ct);
            for (int i = 0; i < 30; i++)
            {
                await writer.InsertRowAsync("DroppedTemp", [i, new string('t', 2500)], Ct);
            }

            for (int i = 0; i < 20; i++)
            {
                await writer.InsertRowAsync("Items", [i, new string((char)('a' + (i % 10)), 3000)], Ct);
            }

            for (int i = 3; i < 20; i++)
            {
                Assert.Equal(1, await writer.DeleteRowsAsync("Items", "Id", i, Ct));
            }

            await writer.DropTableAsync("DroppedTemp", Ct);
        }

        await File.WriteAllBytesAsync(native.SourcePath, sourceStream.ToArray(), Ct);
        AccessCompactReport report = await AccessDatabaseCompact.CompactInPlaceAsync(native.SourcePath, cancellationToken: Ct);
        Assert.True(report.IsComplete);
        Assert.True(report.DatabaseValidated);
        Assert.True(report.ReclamationAuthorized);
        Assert.True(report.ReplacementAuthorized);
        Assert.True(report.DestinationPages < report.SourcePages);

        string sourceLiteral = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.SourcePath);
        string compactedLiteral = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.CompactedPath);
        string script = $$"""
            function Check-Items([string]$path, [int]$expectedRows) {
                $db=$engine.OpenDatabase($path)
                try {
                    $rs=$db.OpenRecordset('Items',1)
                    try {
                        $rs.Index=$db.TableDefs.Item('Items').Indexes.Item(0).Name
                        $rs.MoveFirst()
                        $count=0
                        while(!$rs.EOF) {
                            $id=[int]$rs.Fields.Item('Id').Value
                            if($id -ne $count) { throw "Identity mismatch: $id" }
                            $expected=(''+[char](97+$id))*3000
                            if(![string]::Equals([string]$rs.Fields.Item('Memo').Value,$expected,[StringComparison]::Ordinal)) { throw 'Memo mismatch' }
                            $count++
                            $rs.MoveNext()
                        }
                        if($count -ne $expectedRows) { throw "Row count mismatch: $count" }
                    } finally { $rs.Close() }
                } finally { $db.Close() }
            }
            Check-Items {{sourceLiteral}} 3
            $db=$engine.OpenDatabase({{sourceLiteral}})
            try {
                $rs=$db.OpenRecordset('Items',1)
                try {
                    $rs.AddNew()
                    $rs.Fields.Item('Id').Value=3
                    $rs.Fields.Item('Memo').Value='d'*3000
                    $rs.Update()
                } finally { $rs.Close() }
            } finally { $db.Close() }
            Check-Items {{sourceLiteral}} 4
            $engine.CompactDatabase({{sourceLiteral}},{{compactedLiteral}})
            Check-Items {{compactedLiteral}} 4
            Write-Output 'RECLAIM_COMPACT_NATIVE_OK'
            """;

        if (kind == DatabaseFormat.Jet3Mdb)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, native.WorkDir, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, $"DAO3.6 failed:\n{result.StandardOutput}\n{result.StandardError}");
        }
        else
        {
            AccessRoundTripEnvironment.CompactResult result = native.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0, $"DAO120 failed:\n{result.StdOut}\n{result.StdErr}");
        }
    }
}
