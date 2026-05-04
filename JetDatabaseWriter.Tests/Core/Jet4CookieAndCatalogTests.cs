namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Buffers.Binary;
using System.Data;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Internal.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Verifies the Jet4/ACE format-wide magic cookie <c>0x00000659</c> is
/// correctly stamped into every required location on writer-emitted TDEFs
/// (header, column descriptors, real-idx physical descriptors, logical-idx
/// entries), and that MSysObjects catalog rows carry the expected
/// <c>Owner</c>, <c>LvProp</c>, and <c>MSysACEs</c> entries.
///
/// These invariants are required for DAO Compact &amp; Repair — omitting
/// any one of them causes err 3011 "could not find the object 'MSysDb'".
/// See <c>docs/design/round-trip-test-failures.md</c>.
/// </summary>
public sealed class Jet4CookieAndCatalogTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    // ═══════════════════════════════════════════════════════════════════
    // §1  Jet4 format magic 0x00000659 in TDEF pages
    // ═══════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateTable_TdefHeader_HasJet4FormatMagicAt0x0C(DatabaseFormat format)
    {
        byte[] fileBytes = await CreateSingleTableDatabase(format);
        foreach (int tdefPage in FindTdefPages(fileBytes, format))
        {
            int off = tdefPage * PageSize(format);
            int magic = BinaryPrimitives.ReadInt32LittleEndian(fileBytes.AsSpan(off + 0x0C, 4));
            Assert.Equal(Constants.TableDefinition.Jet4FormatMagic, magic);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateTable_ColumnDescriptors_HaveJet4FormatMagicAtByte1(DatabaseFormat format)
    {
        byte[] fileBytes = await CreateSingleTableDatabase(format);
        foreach (int tdefPage in FindTdefPages(fileBytes, format))
        {
            AssertColumnDescriptorMagic(fileBytes, tdefPage, format);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateTable_RealIdxPhysicalDescriptors_HaveJet4FormatMagic(DatabaseFormat format)
    {
        byte[] fileBytes = await CreateTableWithIndex(format);
        bool foundUserTdef = false;

        foreach (int tdefPage in FindTdefPages(fileBytes, format))
        {
            int off = tdefPage * PageSize(format);
            int numRealIdx = BinaryPrimitives.ReadInt32LittleEndian(fileBytes.AsSpan(off + 51, 4));
            if (numRealIdx == 0)
            {
                continue;
            }

            int numCols = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(off + 45, 2));
            int colStart = off + 63 + (numRealIdx * 12);

            // Skip past column descriptors and names to reach real-idx phys block.
            int namePos = colStart + (numCols * 25);
            for (int c = 0; c < numCols; c++)
            {
                int nameLen = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(namePos, 2));
                namePos += 2 + nameLen;
            }

            for (int i = 0; i < numRealIdx; i++)
            {
                int phys = namePos + (i * 52);
                int magic = BinaryPrimitives.ReadInt32LittleEndian(fileBytes.AsSpan(phys, 4));
                Assert.Equal(Constants.TableDefinition.Jet4FormatMagic, magic);
            }

            foundUserTdef = true;
        }

        Assert.True(foundUserTdef, "Expected at least one TDEF with real-idx descriptors.");
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateTable_LogicalIdxEntries_HaveJet4FormatMagic(DatabaseFormat format)
    {
        byte[] fileBytes = await CreateTableWithIndex(format);
        bool foundLogical = false;

        foreach (int tdefPage in FindTdefPages(fileBytes, format))
        {
            int off = tdefPage * PageSize(format);
            int numRealIdx = BinaryPrimitives.ReadInt32LittleEndian(fileBytes.AsSpan(off + 51, 4));
            int numIdx = BinaryPrimitives.ReadInt32LittleEndian(fileBytes.AsSpan(off + 47, 4));
            if (numIdx == 0)
            {
                continue;
            }

            int numCols = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(off + 45, 2));
            int colStart = off + 63 + (numRealIdx * 12);

            // Skip past column descriptors and names.
            int namePos = colStart + (numCols * 25);
            for (int c = 0; c < numCols; c++)
            {
                int nameLen = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(namePos, 2));
                namePos += 2 + nameLen;
            }

            // Skip past real-idx physical descriptors.
            int logStart = namePos + (numRealIdx * 52);

            for (int i = 0; i < numIdx; i++)
            {
                int logEntry = logStart + (i * 28);
                int magic = BinaryPrimitives.ReadInt32LittleEndian(fileBytes.AsSpan(logEntry, 4));
                Assert.Equal(Constants.TableDefinition.Jet4FormatMagic, magic);
            }

            foundLogical = true;
        }

        Assert.True(foundLogical, "Expected at least one TDEF with logical-idx entries.");
    }

    [Fact]
    public async Task CreateTable_Jet3_DoesNotStampFormatMagicInColumnDescriptors()
    {
        byte[] fileBytes = await CreateSingleTableDatabase(DatabaseFormat.Jet3Mdb);
        foreach (int tdefPage in FindTdefPages(fileBytes, DatabaseFormat.Jet3Mdb))
        {
            int pgSz = PageSize(DatabaseFormat.Jet3Mdb);
            int off = tdefPage * pgSz;
            int numRealIdx = BinaryPrimitives.ReadInt32LittleEndian(fileBytes.AsSpan(off + 31, 4));
            int numCols = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(off + 25, 2));
            int colStart = off + 43 + (numRealIdx * 8);

            for (int c = 0; c < numCols; c++)
            {
                int o = colStart + (c * 18);

                // In Jet3, byte 1 of the column descriptor is col_num, not magic.
                // Verify it is NOT 0x00000659.
                int val = BinaryPrimitives.ReadInt32LittleEndian(fileBytes.AsSpan(o + 1, 4));
                Assert.NotEqual(Constants.TableDefinition.Jet4FormatMagic, val);
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // §2  TDEF free-space hint at offset 0x02..0x03
    // ═══════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateTable_TdefFreeSpaceHint_IsPageSizeMinusTdefLenMinus8(DatabaseFormat format)
    {
        byte[] fileBytes = await CreateSingleTableDatabase(format);
        int pgSz = PageSize(format);

        foreach (int tdefPage in FindTdefPages(fileBytes, format))
        {
            int off = tdefPage * pgSz;
            int tdefLen = BinaryPrimitives.ReadInt32LittleEndian(fileBytes.AsSpan(off + 8, 4));
            int freeSpace = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(off + 2, 2));
            int expected = Math.Max(0, pgSz - tdefLen - 8);
            Assert.Equal(expected, freeSpace);
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // §3  MSysObjects catalog row — Owner blob
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CreateTable_CatalogRow_OwnerIsNonNull()
    {
        MemoryStream ms = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, ct);

        await using (var writer = await AccessWriter.OpenAsync(
            ms, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("TestOwner", [new ColumnDefinition("A", typeof(int))], ct);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);

        DataTable catalog = await reader.ReadDataTableAsync("MSysObjects", cancellationToken: ct);
        Assert.True(catalog.Rows.Count > 0, "MSysObjects should contain rows.");

        // Find the user table row (Type=1, Name=TestOwner).
        DataRow? userRow = null;
        foreach (DataRow row in catalog.Rows)
        {
            if (row["Name"]?.ToString() == "TestOwner" &&
                Convert.ToInt16(row["Type"], CultureInfo.InvariantCulture) == Constants.SystemObjects.UserTableType)
            {
                userRow = row;
                break;
            }
        }

        Assert.NotNull(userRow);
        object ownerVal = userRow["Owner"];
        Assert.False(ownerVal is DBNull, "Owner should not be NULL on user-table catalog rows.");

        byte[] owner = (byte[])ownerVal;
        Assert.Equal(Constants.SystemObjects.DefaultOwnerBlob, owner);
    }

    // ═══════════════════════════════════════════════════════════════════
    // §4  MSysObjects catalog row — LvProp placeholder
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CreateTable_CatalogRow_LvPropIsNonNull_12Bytes()
    {
        MemoryStream ms = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, ct);

        await using (var writer = await AccessWriter.OpenAsync(
            ms, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("TestLvProp", [new ColumnDefinition("X", typeof(string), 50)], ct);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);

        DataTable catalog = await reader.ReadDataTableAsync("MSysObjects", cancellationToken: ct);

        DataRow? userRow = null;
        foreach (DataRow row in catalog.Rows)
        {
            if (row["Name"]?.ToString() == "TestLvProp" &&
                Convert.ToInt16(row["Type"], CultureInfo.InvariantCulture) == Constants.SystemObjects.UserTableType)
            {
                userRow = row;
                break;
            }
        }

        Assert.NotNull(userRow);
        object lvPropVal = userRow["LvProp"];
        Assert.False(lvPropVal is DBNull, "LvProp should not be NULL on user-table catalog rows.");

        byte[] lvProp = (byte[])lvPropVal;
        Assert.Equal(12, lvProp.Length);
    }

    // ═══════════════════════════════════════════════════════════════════
    // §5  MSysACEs — ACE rows per user table
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CreateTable_InsertsAceRows_ForUserTable()
    {
        MemoryStream ms = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, ct);

        await using (var writer = await AccessWriter.OpenAsync(
            ms, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("TestAces", [new ColumnDefinition("Id", typeof(int))], ct);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);

        CatalogEntry? entry = await reader.GetCatalogEntryAsync("TestAces", ct);
        Assert.NotNull(entry);
        int objectId = (int)entry.TDefPage;

        DataTable aces = await reader.ReadDataTableAsync("MSysACEs", cancellationToken: ct);
        Assert.True(aces.Rows.Count > 0, "MSysACEs should have rows.");

        int matchingRows = 0;
        foreach (DataRow row in aces.Rows)
        {
            if (Convert.ToInt32(row["ObjectId"], CultureInfo.InvariantCulture) == objectId)
            {
                matchingRows++;

                // ACM should be the standard permission mask.
                int acm = Convert.ToInt32(row["ACM"], CultureInfo.InvariantCulture);
                Assert.Equal(Constants.Aces.DefaultAcm, acm);

                // Inheritable should be true.
                Assert.True(Convert.ToBoolean(row["FInheritable"], CultureInfo.InvariantCulture));

                // SID should be non-null.
                object sidVal = row["SID"];
                Assert.False(sidVal is DBNull, "SID should not be NULL on ACE rows.");
            }
        }

        Assert.True(matchingRows >= 2, $"Expected at least 2 ACE rows for ObjectId={objectId}, found {matchingRows}.");
    }

    [Fact]
    public async Task CreateTable_AceRows_ContainOwnerAndUsersSids()
    {
        MemoryStream ms = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, ct);

        await using (var writer = await AccessWriter.OpenAsync(
            ms, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("TestSids", [new ColumnDefinition("V", typeof(int))], ct);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);

        CatalogEntry? entry = await reader.GetCatalogEntryAsync("TestSids", ct);
        Assert.NotNull(entry);
        int objectId = (int)entry.TDefPage;

        DataTable aces = await reader.ReadDataTableAsync("MSysACEs", cancellationToken: ct);

        bool foundOwner = false;
        bool foundUsers = false;

        foreach (DataRow row in aces.Rows)
        {
            if (Convert.ToInt32(row["ObjectId"], CultureInfo.InvariantCulture) != objectId)
            {
                continue;
            }

            byte[] sid = (byte[])row["SID"];
            if (sid.AsSpan().SequenceEqual(Constants.Aces.OwnerSid))
            {
                foundOwner = true;
            }
            else if (sid.AsSpan().SequenceEqual(Constants.Aces.UsersSid))
            {
                foundUsers = true;
            }
        }

        Assert.True(foundOwner, "Expected an ACE row with OwnerSid for the new table.");
        Assert.True(foundUsers, "Expected an ACE row with UsersSid for the new table.");
    }

    [Fact]
    public async Task CreateTable_AceRowCount_IncreasesPerUserTable()
    {
        MemoryStream ms = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, ct);

        // Count ACE rows before adding a user table.
        int aceCountBefore;
        {
            var msSnap = new MemoryStream(ms.ToArray());
            await using var reader = await AccessReader.OpenAsync(
                msSnap, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
            DataTable aces = await reader.ReadDataTableAsync("MSysACEs", cancellationToken: ct);
            aceCountBefore = aces.Rows.Count;
        }

        await using (var writer = await AccessWriter.OpenAsync(
            ms, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("UserT", [new ColumnDefinition("A", typeof(int))], ct);
        }

        ms.Position = 0;
        await using var reader2 = await AccessReader.OpenAsync(
            ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        DataTable acesAfter = await reader2.ReadDataTableAsync("MSysACEs", cancellationToken: ct);
        int aceCountAfter = acesAfter.Rows.Count;

        Assert.True(
            aceCountAfter > aceCountBefore,
            $"Expected ACE rows to increase after creating a user table. Before={aceCountBefore}, After={aceCountAfter}.");
    }

    // ═══════════════════════════════════════════════════════════════════
    // §6  Per-table usage-map page structure
    // ═══════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateTable_UsageMapPage_HasCorrectStructure(DatabaseFormat format)
    {
        byte[] fileBytes = await CreateSingleTableDatabase(format);
        int pgSz = PageSize(format);

        // Find a user-table TDEF (not the MSysObjects TDEF at page 2) and
        // read its used_pages pointer at offset 0x37..0x3A.
        foreach (int tdefPage in FindTdefPages(fileBytes, format))
        {
            if (tdefPage <= 2)
            {
                continue; // Skip system TDEFs
            }

            int off = tdefPage * pgSz;

            // used_pages pointer: row byte at 0x37, page LE3 at 0x38..0x3A.
            byte usedRow = fileBytes[off + 0x37];
            int usedPage = fileBytes[off + 0x38] | (fileBytes[off + 0x39] << 8) | (fileBytes[off + 0x3A] << 16);

            if (usedPage == 0)
            {
                continue; // System TDEFs without usage-map pages
            }

            Assert.Equal(0, usedRow); // row 0 = used_pages

            // Verify the usage-map page.
            int umOff = usedPage * pgSz;
            Assert.Equal(0x01, fileBytes[umOff]); // page_type = 0x01 (data page)
            Assert.Equal(0x01, fileBytes[umOff + 1]); // second byte = 0x01

            // Row count should be 2 (used_pages + free_pages).
            int numRows = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(umOff + 12, 2));
            Assert.Equal(2, numRows);

            // Each row is 69 bytes: 1-byte type-0 marker + 68 bitmap bytes.
            int row0Off = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(umOff + 14, 2));
            int row1Off = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(umOff + 16, 2));

            // Rows are at page tail.
            Assert.Equal(pgSz - 69, row0Off & 0x1FFF); // mask off flags
            Assert.Equal(pgSz - 138, row1Off & 0x1FFF);

            // First byte of each row is 0x00 (type-0 inline usage map).
            Assert.Equal(0x00, fileBytes[umOff + (row0Off & 0x1FFF)]);
            Assert.Equal(0x00, fileBytes[umOff + (row1Off & 0x1FFF)]);

            // free_pages pointer: row byte at 0x3B, page LE3 at 0x3C..0x3E.
            byte freeRow = fileBytes[off + 0x3B];
            int freePage = fileBytes[off + 0x3C] | (fileBytes[off + 0x3D] << 8) | (fileBytes[off + 0x3E] << 16);

            Assert.Equal(1, freeRow); // row 1 = free_pages
            Assert.Equal(usedPage, freePage); // Same usage-map page for both.
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // §7  Constants sanity checks
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public void Jet4FormatMagic_HasExpectedValue()
    {
        Assert.Equal(0x00000659, Constants.TableDefinition.Jet4FormatMagic);
    }

    [Fact]
    public void DefaultOwnerBlob_IsTwoBytes_0x71_0x10()
    {
        Assert.Equal(2, Constants.SystemObjects.DefaultOwnerBlob.Length);
        Assert.Equal(0x71, Constants.SystemObjects.DefaultOwnerBlob[0]);
        Assert.Equal(0x10, Constants.SystemObjects.DefaultOwnerBlob[1]);
    }

    [Fact]
    public void DefaultLvPropPlaceholder_IsTwelveZeroBytes()
    {
        Assert.Equal(12, Constants.SystemObjects.DefaultLvPropPlaceholder.Length);
        Assert.All(Constants.SystemObjects.DefaultLvPropPlaceholder, b => Assert.Equal(0, b));
    }

    [Fact]
    public void DefaultAcm_HasExpectedValue()
    {
        Assert.Equal(0x000FFEFF, Constants.Aces.DefaultAcm);
    }

    [Fact]
    public void OwnerSid_IsTwoBytes()
    {
        Assert.Equal(2, Constants.Aces.OwnerSid.Length);
        Assert.Equal(0x71, Constants.Aces.OwnerSid[0]);
        Assert.Equal(0x10, Constants.Aces.OwnerSid[1]);
    }

    [Fact]
    public void UsersSid_IsTwoBytes()
    {
        Assert.Equal(2, Constants.Aces.UsersSid.Length);
        Assert.Equal(0x70, Constants.Aces.UsersSid[0]);
        Assert.Equal(0x10, Constants.Aces.UsersSid[1]);
    }

    // ═══════════════════════════════════════════════════════════════════
    // §8  Multi-table creates accumulate correct ACE counts
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CreateMultipleTables_EachGetsAceRows()
    {
        MemoryStream ms = await db.CopyToStreamAsync(TestDatabases.NorthwindTraders, ct);

        await using (var writer = await AccessWriter.OpenAsync(
            ms, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("T1", [new ColumnDefinition("A", typeof(int))], ct);
            await writer.CreateTableAsync("T2", [new ColumnDefinition("B", typeof(int))], ct);
            await writer.CreateTableAsync("T3", [new ColumnDefinition("C", typeof(int))], ct);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);

        DataTable aces = await reader.ReadDataTableAsync("MSysACEs", cancellationToken: ct);

        // Each of the 3 user tables should have ACE rows.
        foreach (string tableName in new[] { "T1", "T2", "T3" })
        {
            CatalogEntry? entry = await reader.GetCatalogEntryAsync(tableName, ct);
            Assert.NotNull(entry);
            int objectId = (int)entry.TDefPage;

            int count = 0;
            foreach (DataRow row in aces.Rows)
            {
                if (Convert.ToInt32(row["ObjectId"], CultureInfo.InvariantCulture) == objectId)
                {
                    count++;
                }
            }

            Assert.True(
                count >= 2,
                $"Table '{tableName}' (ObjectId={objectId}) should have at least 2 ACE rows, found {count}.");
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════════

    private static int PageSize(DatabaseFormat format) =>
        format == DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet3 : Constants.PageSizes.Jet4;

    /// <summary>
    /// Finds all head-of-chain TDEF pages (page_type=0x02, next-chain=0) in the file.
    /// </summary>
    private static int[] FindTdefPages(byte[] fileBytes, DatabaseFormat format)
    {
        int pgSz = PageSize(format);
        var pages = new System.Collections.Generic.List<int>();

        for (int p = 1; p < fileBytes.Length / pgSz; p++)
        {
            int off = p * pgSz;
            if (fileBytes[off] == 0x02 &&
                BinaryPrimitives.ReadUInt32LittleEndian(fileBytes.AsSpan(off + 4, 4)) == 0)
            {
                pages.Add(p);
            }
        }

        return pages.ToArray();
    }

    /// <summary>
    /// Asserts every column descriptor on the given TDEF page has the
    /// Jet4 format magic at byte offset 1 within the descriptor.
    /// </summary>
    private static void AssertColumnDescriptorMagic(byte[] db, int tdefPage, DatabaseFormat format)
    {
        int pgSz = PageSize(format);
        int off = tdefPage * pgSz;
        int numRealIdx = BinaryPrimitives.ReadInt32LittleEndian(db.AsSpan(off + 51, 4));
        int numCols = BinaryPrimitives.ReadUInt16LittleEndian(db.AsSpan(off + 45, 2));
        int colStart = off + 63 + (numRealIdx * 12);

        Assert.True(numCols > 0, $"TDEF at page {tdefPage} has 0 columns.");

        for (int c = 0; c < numCols; c++)
        {
            int o = colStart + (c * 25);
            int magic = BinaryPrimitives.ReadInt32LittleEndian(db.AsSpan(o + 1, 4));
            Assert.Equal(Constants.TableDefinition.Jet4FormatMagic, magic);
        }
    }

    private async Task<byte[]> CreateSingleTableDatabase(DatabaseFormat format)
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync(
                "TestTable",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Name", typeof(string), 100),
                    new ColumnDefinition("Active", typeof(bool)),
                ],
                ct);
        }

        return ms.ToArray();
    }

    private async Task<byte[]> CreateTableWithIndex(DatabaseFormat format)
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync(
                "Indexed",
                [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Val", typeof(string), 50)],
                [new IndexDefinition("PK_Id", "Id") { IsPrimaryKey = true }],
                ct);
        }

        return ms.ToArray();
    }
}
