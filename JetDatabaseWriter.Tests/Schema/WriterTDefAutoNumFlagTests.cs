namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Tests round-trip-openrecordset hypothesis H25: Jackcess unconditionally
/// stamps TDEF byte <c>0x18</c> (the <c>autonum_flag</c>) to <c>0x01</c> on
/// every user table — even tables without an autonumber column — with the
/// comment "this makes autonumbering work in access". The writer's
/// <see cref="JetDatabaseWriter.Pages.DataPageInserter.PatchAutoNumFlag"/>
/// is conditional: it writes <c>0x01</c> only if some column carries the
/// autonumber flag (<c>0x04</c>), otherwise <c>0x00</c>.
///
/// <para>
/// Strategy: walk every user table TDEF in DAO-authored
/// <c>NorthwindTraders.accdb</c>. For each, record byte <c>0x18</c> alongside
/// whether any column descriptor has the <c>0x04</c> autonum bit set. If DAO
/// stamps <c>0x01</c> even on tables with no autonumber column, the writer's
/// conditional logic is wrong.
/// </para>
/// </summary>
public sealed class WriterTDefAutoNumFlagTests
{
    private const int Jet4ColDescSize = 25;
    private const int Jet4ColFlagsOffsetWithinDesc = 15;
    private const byte AutoNumFlag = 0x04;
    private const int TDefAutoNumFlagOffset = 0x18;

    /// <summary>
    /// DAO ground truth: surveys byte <c>0x18</c> across every user-table
    /// TDEF in <c>NorthwindTraders.accdb</c>. If at least one table without
    /// any autonumber column has byte <c>0x18 == 0x01</c>, H25 is confirmed
    /// (writer's conditional logic disagrees with DAO). If every
    /// no-autonumber table has byte <c>0x18 == 0x00</c>, H25 is disconfirmed.
    /// If every table happens to carry an autonumber column, the test is
    /// inconclusive and reports as such.
    /// </summary>
    [Fact]
    public async Task DaoAuthoredFixture_AutoNumFlagByte_BehaviorVsAutonumPresence()
    {
        Assert.True(File.Exists(TestDatabases.NorthwindTraders), "NorthwindTraders.accdb fixture not found.");

        byte[] fileBytes = await File.ReadAllBytesAsync(
            TestDatabases.NorthwindTraders,
            TestContext.Current.CancellationToken);

        await using var ms = new MemoryStream(fileBytes, writable: false);
        await using var reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        int pageSize = reader.PageSize;
        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        int tablesWithAutonum = 0;
        int tablesWithoutAutonum = 0;
        int tablesWithoutAutonum_ByteIs01 = 0;
        int tablesWithoutAutonum_ByteIs00 = 0;
        int tablesWithoutAutonum_ByteIsOther = 0;
        var perTable = new StringBuilder();

        foreach (string tableName in tables)
        {
            var entry = await reader.GetCatalogEntryAsync(tableName, TestContext.Current.CancellationToken);
            Assert.NotNull(entry);

            int tdefOff = (int)entry!.TDefPage * pageSize;
            byte autoNumByte = fileBytes[tdefOff + TDefAutoNumFlagOffset];
            bool anyAutonumColumn = HasAnyAutonumColumn(fileBytes, tdefOff);

            if (anyAutonumColumn)
            {
                tablesWithAutonum++;
            }
            else
            {
                tablesWithoutAutonum++;
                if (autoNumByte == 0x01)
                {
                    tablesWithoutAutonum_ByteIs01++;
                }
                else if (autoNumByte == 0x00)
                {
                    tablesWithoutAutonum_ByteIs00++;
                }
                else
                {
                    tablesWithoutAutonum_ByteIsOther++;
                }
            }

            perTable.AppendFormat(
                CultureInfo.InvariantCulture,
                "{0}: byte0x18=0x{1:X2} hasAutonumCol={2}; ",
                tableName,
                autoNumByte,
                anyAutonumColumn);
        }

        string summary =
            $"DAO survey: {tables.Count} tables; "
            + $"{tablesWithAutonum} with an autonum column, "
            + $"{tablesWithoutAutonum} without. "
            + $"Of the {tablesWithoutAutonum} without an autonum column: "
            + $"byte0x18==0x01 in {tablesWithoutAutonum_ByteIs01}, "
            + $"==0x00 in {tablesWithoutAutonum_ByteIs00}, "
            + $"other in {tablesWithoutAutonum_ByteIsOther}. "
            + $"Per-table: [{perTable}]";

        Assert.True(tables.Count > 0, "Expected at least one user table in NorthwindTraders.");

        if (tablesWithoutAutonum == 0)
        {
            // Inconclusive: every table happens to have an autonumber column,
            // so we can't observe DAO's behavior on a no-autonum table from
            // this fixture alone. Don't fail — report and continue.
            Assert.Skip("H25 inconclusive on this fixture: every NorthwindTraders user table has an autonumber column. " + summary);
            return;
        }

        // H25 was confirmed: across all 23 NorthwindTraders user tables
        // (19 with an autonum column, 4 without), DAO stamps byte 0x18 == 0x01
        // unconditionally. This test now serves as a regression guard for the
        // ground-truth assumption underlying DataPageInserter.PatchAutoNumFlag.
        Assert.Equal(tablesWithoutAutonum, tablesWithoutAutonum_ByteIs01);
        Assert.Equal(0, tablesWithoutAutonum_ByteIs00);
        Assert.Equal(0, tablesWithoutAutonum_ByteIsOther);
    }

    /// <summary>
    /// Writer-side check: builds two fresh tables — one with an autonumber
    /// column and one without — and asserts byte <c>0x18 == 0x01</c> on both,
    /// matching the unconditional DAO behavior confirmed by the ground-truth
    /// test above.
    /// </summary>
    [Fact]
    public async Task WriterAuthored_AutoNumFlagByte_AlwaysOne()
    {
        byte byteForNoAutonum = await BuildAndReadAutoNumByteAsync(
            "Customers",
            [
                new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = false, IsNullable = false },
                new("Name", typeof(string), maxLength: 100) { IsNullable = false },
            ]);

        byte byteForAutonum = await BuildAndReadAutoNumByteAsync(
            "Orders",
            [
                new("OrderID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                new("Total", typeof(double)) { IsNullable = true },
            ]);

        Assert.Equal(0x01, byteForNoAutonum);
        Assert.Equal(0x01, byteForAutonum);
    }

    private static async Task<byte> BuildAndReadAutoNumByteAsync(string tableName, ColumnDefinition[] columns)
    {
        await using var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
        }

        byte[] fileBytes = ms.ToArray();
        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        int pageSize = reader.PageSize;
        var entry = await reader.GetCatalogEntryAsync(tableName, TestContext.Current.CancellationToken);
        Assert.NotNull(entry);
        int tdefOff = (int)entry!.TDefPage * pageSize;
        return fileBytes[tdefOff + TDefAutoNumFlagOffset];
    }

    /// <summary>
    /// Walks the column descriptor block of a Jet4/ACE TDEF and returns
    /// <c>true</c> if any column descriptor has the autonumber bit
    /// (<c>0x04</c>) set in its flags byte.
    /// </summary>
    private static bool HasAnyAutonumColumn(byte[] fileBytes, int tdefOff)
    {
        // Jet4 TDEF: numCols at +45 (uint16), numRealIdx at +51 (int32),
        // logical-idx block starts at +63 and is numRealIdx * 12 bytes,
        // followed by numCols * 25-byte column descriptors.
        int numCols = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(tdefOff + 45, 2));
        int numRealIdx = BinaryPrimitives.ReadInt32LittleEndian(fileBytes.AsSpan(tdefOff + 51, 4));
        int colStart = tdefOff + 63 + (numRealIdx * 12);

        for (int c = 0; c < numCols; c++)
        {
            int descOff = colStart + (c * Jet4ColDescSize);
            byte flags = fileBytes[descOff + Jet4ColFlagsOffsetWithinDesc];
            if ((flags & AutoNumFlag) != 0)
            {
                return true;
            }
        }

        return false;
    }
}
