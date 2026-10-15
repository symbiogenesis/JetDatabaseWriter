namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Records descriptor initialization in NorthwindTraders and freshly created
/// writer tables. Offsets 5-6 and 9-10 coincide in these examples, but native
/// TestV2010 Table4 has different values; equality is not a format invariant.
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
        await using AccessReader reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);
        await using AccessReader pages = await TestDatabase.OpenReaderAsync(ms, cancellationToken: TestContext.Current.CancellationToken);

        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        int totalColumnsChecked = 0;
        foreach (string tableName in tables)
        {
            CatalogEntry? entry = await pages.GetCatalogEntryAsync(tableName, TestContext.Current.CancellationToken);
            Assert.NotNull(entry);

            (int Primary, int Redundant)[] pairs = ReadColumnNumberPairs(fileBytes, (int)entry.TDefPage, reader.PageSize);
            foreach ((int primary, int redundant) in pairs)
            {
                Assert.True(
                    primary == redundant,
                    $"DAO-authored {tableName}: column descriptor offset 5-6 ({primary}) != offset 9-10 ({redundant}). Northwind descriptor initialization changed.");
                totalColumnsChecked++;
            }
        }

        Assert.True(totalColumnsChecked > 0, "Expected at least one column to validate from NorthwindTraders.");
    }

    [Fact]
    public async Task WriterAuthored_RedundantColNum_MatchesPrimary()
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            // Mix column types so the regression guard covers descriptor branches
            // with different descriptor initialization paths.
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

        await using AccessReader reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);
        await using AccessReader pages = await TestDatabase.OpenReaderAsync(ms, cancellationToken: TestContext.Current.CancellationToken);

        CatalogEntry? entry = await pages.GetCatalogEntryAsync("Customers", TestContext.Current.CancellationToken);
        Assert.NotNull(entry);

        (int Primary, int Redundant)[] pairs = ReadColumnNumberPairs(fileBytes, (int)entry.TDefPage, reader.PageSize);
        Assert.NotEmpty(pairs);

        (int Index, int Primary, int Redundant)[] mismatches = pairs
            .Select((p, idx) => (Index: idx, p.Primary, p.Redundant))
            .Where(t => t.Primary != t.Redundant)
            .ToArray();

        string detail = string.Join("; ", mismatches.Select(m => $"col[{m.Index}] primary={m.Primary} redundant={m.Redundant}"));
        string failureMessage = $"New writer descriptors use matching values at offsets 5-6 and 9-10. Mismatches: [{detail}].";

        Assert.True(mismatches.Length == 0, failureMessage);
    }

    /// <summary>
    /// Walks the column descriptors of a Jet4/ACE single-page user-table TDEF
    /// and returns each descriptor's (primary col_num at offset 5-6,
    /// redundant col_num at offset 9-10) pair. Reads only one TDEF page;
    /// callers should pick fixtures whose TDEF fits in one 4096-byte page
    /// (true for all single-table-per-test cases used here).
    /// </summary>
    /// <param name="fileBytes">The file bytes.</param>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="pageSize">The page size.</param>
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
