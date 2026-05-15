namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Tests round-trip-openrecordset hypothesis H22: that the writer omits the
/// "redundant <c>col_num</c>" field at column-descriptor byte offset 9-10
/// of the Jet4/ACE 25-byte column descriptor. Per mdbtools <c>HACKING.md</c>,
/// the Jet4 column descriptor stores <c>col_num</c> twice — once at offset
/// 5-6 (the primary slot) and again at offset 9-10. The writer currently
/// only writes the primary slot, leaving offset 9-10 either as the page-init
/// zero (most types) or as the literal <c>0x0001</c> stamped by the
/// TEXT/MEMO branch in <see cref="JetDatabaseWriter.Schema.TDefPageBuilder"/>.
///
/// <para>Test strategy:</para>
/// <list type="number">
///   <item><see cref="DaoAuthoredFixture_RedundantColNum_AlwaysEqualsPrimary"/>
///     — establishes ground truth from the DAO-authored
///     <c>NorthwindTraders.accdb</c> fixture: every user-table column
///     descriptor must have bytes 9-10 == bytes 5-6 (== <c>col_num</c>).</item>
///   <item><see cref="WriterAuthored_RedundantColNum_MatchesPrimary"/>
///     — runs the same byte-level check against a freshly writer-created
///     database. If H22 is correct, this test FAILS today and proves the
///     defect; once a fix lands in <c>TDefPageBuilder</c> it must pass.</item>
/// </list>
/// </summary>
public sealed class WriterColumnDescriptorRedundantColNumTests
{
    [Fact]
    public async Task DaoAuthoredFixture_RedundantColNum_AlwaysEqualsPrimary()
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

        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        int totalColumnsChecked = 0;
        foreach (string tableName in tables)
        {
            var entry = await reader.GetCatalogEntryAsync(tableName, TestContext.Current.CancellationToken);
            Assert.NotNull(entry);

            (int Primary, int Redundant)[] pairs = ReadColumnNumberPairs(fileBytes, (int)entry!.TDefPage, reader.PageSize);
            foreach (var pair in pairs)
            {
                Assert.True(
                    pair.Primary == pair.Redundant,
                    $"DAO-authored {tableName}: column descriptor offset 5-6 ({pair.Primary}) != offset 9-10 ({pair.Redundant}). Ground-truth assumption broken — re-read mdbtools HACKING.md.");
                totalColumnsChecked++;
            }
        }

        Assert.True(totalColumnsChecked > 0, "Expected at least one column to validate from NorthwindTraders.");
    }

    [Fact]
    public async Task WriterAuthored_RedundantColNum_MatchesPrimary()
    {
        await using var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            // Mix column types so we exercise BOTH branches of TDefPageBuilder:
            //   - non-text columns: writer leaves bytes 9-10 = 0x0000 (page init)
            //   - text columns:     writer stamps 0x0001 at byte 9 ("misc_flags")
            // Either pattern reproduces H22 unless col_num happens to equal 0/1.
            await writer.CreateTableAsync(
                "Customers",
                [
                    new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                    new("FirstName", typeof(string), maxLength: 100),
                    new("LastName", typeof(string), maxLength: 100),
                    new("BirthDate", typeof(DateTime)),
                    new("Balance", typeof(decimal)),
                    new("Notes", typeof(string), maxLength: int.MaxValue), // MEMO
                ],
                TestContext.Current.CancellationToken);
        }

        byte[] fileBytes = ms.ToArray();
        ms.Position = 0;

        await using var reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        var entry = await reader.GetCatalogEntryAsync("Customers", TestContext.Current.CancellationToken);
        Assert.NotNull(entry);

        (int Primary, int Redundant)[] pairs = ReadColumnNumberPairs(fileBytes, (int)entry!.TDefPage, reader.PageSize);
        Assert.NotEmpty(pairs);

        var mismatches = pairs
            .Select((p, idx) => (Index: idx, p.Primary, p.Redundant))
            .Where(t => t.Primary != t.Redundant)
            .ToArray();

        string detail = string.Join("; ", mismatches.Select(m => $"col[{m.Index}] primary={m.Primary} redundant={m.Redundant}"));
        string failureMessage = $"""
            H22 reproduced: writer-authored Customers TDEF has column descriptors whose redundant col_num
            (bytes 9-10) does not match the primary col_num (bytes 5-6). Per mdbtools HACKING.md the Jet4
            column descriptor stores col_num twice. DAO OpenRecordset reads the second copy and rejects
            the table when it disagrees with the first. Mismatches: [{detail}].
            Fix: in JetDatabaseWriter/Schema/TDefPageBuilder.cs write col.ColNum at o + 9 unconditionally
            for the Jet4/ACE branch (currently only TEXT/MEMO writes a literal 0x0001 there).
            """;

        Assert.True(mismatches.Length == 0, failureMessage);
    }

    /// <summary>
    /// Walks the column descriptors of a Jet4/ACE single-page user-table TDEF
    /// and returns each descriptor's (primary col_num at offset 5-6,
    /// redundant col_num at offset 9-10) pair. Reads only one TDEF page;
    /// callers should pick fixtures whose TDEF fits in one 4096-byte page
    /// (true for all single-table-per-test cases used here).
    /// </summary>
    private static (int Primary, int Redundant)[] ReadColumnNumberPairs(
        byte[] fileBytes,
        int tdefPage,
        int pageSize)
    {
        int off = tdefPage * pageSize;

        // Jet4 TDEF header layout (Constants.TableDefinition.Jet4):
        //   numCols at off+45 (uint16), numRealIdx at off+51 (int32),
        //   columns start at off+63 + numRealIdx*12, each descriptor = 25 bytes.
        int numCols = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(off + 45, 2));
        int numRealIdx = BinaryPrimitives.ReadInt32LittleEndian(fileBytes.AsSpan(off + 51, 4));
        int colStart = off + 63 + (numRealIdx * 12);

        var pairs = new (int, int)[numCols];
        for (int i = 0; i < numCols; i++)
        {
            int desc = colStart + (i * 25);
            int primary = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(desc + 5, 2));
            int redundant = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(desc + 9, 2));
            pairs[i] = (primary, redundant);
        }

        return pairs;
    }
}
