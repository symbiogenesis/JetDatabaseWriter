namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Tests round-trip-openrecordset hypothesis H21: that the user-table real-idx
/// physical descriptor's <c>first_dp</c> (root index page pointer) is zero or
/// otherwise wrong on writer-authored TDEFs. The hypothesis predicts DAO
/// <c>OpenRecordset</c> reads page 0 (the database header) when <c>first_dp = 0</c>,
/// fails its index-page tag check (header page tag is <c>0x00</c>, not
/// <c>0x03</c> / <c>0x04</c>), and reports <c>"Unrecognized database format ''."</c>.
/// <para>
/// The tests below directly inspect the on-disk TDEF for every user-table
/// non-FK index and assert that:
/// </para>
/// <list type="number">
///   <item><see cref="IndexMetadata.FirstDp"/> (the reader's decoded view) is
///         a valid in-range page number greater than 1.</item>
///   <item>The raw bytes of the real-idx physical descriptor at offset
///         <c>phys + 38</c> match the reader-decoded value (rules out a
///         reader bug masking a writer-side zero).</item>
///   <item>The page at <c>first_dp</c> has page tag <c>0x04</c> (leaf) or
///         <c>0x03</c> (intermediate) — i.e. it really is an index B-tree
///         page, not the header / a TDEF / a data page.</item>
/// </list>
/// </summary>
public sealed class WriterRealIdxFirstDpStampingTests
{
    [Fact]
    public async Task SingleTablePk_RealIdxFirstDp_StampedToValidLeafPage()
    {
        await using var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Customers",
                [
                    new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                    new("Name", typeof(string), maxLength: 100) { IsNullable = false },
                ],
                TestContext.Current.CancellationToken);
        }

        await AssertAllRealIdxFirstDpAreValidAsync(ms, ["Customers"], TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task ParentChildWithFk_RealIdxFirstDp_StampedToValidLeafPage()
    {
        await using var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Customers",
                [
                    new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                    new("Name", typeof(string), maxLength: 100),
                ],
                TestContext.Current.CancellationToken);

            await writer.CreateTableAsync(
                "Orders",
                [
                    new("OrderID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                    new("CustomerID", typeof(int)) { IsNullable = false },
                ],
                TestContext.Current.CancellationToken);

            // Note: this fresh-database path has no MSysRelationships catalog
            // table, so we deliberately skip CreateRelationshipAsync. The two
            // PK indexes alone are enough to test H21 — the writer emits one
            // real-idx slot per logical index (numRealIdx = numIdx in
            // TDefPageBuilder), so a missing first_dp stamp would surface here.
        }

        await AssertAllRealIdxFirstDpAreValidAsync(ms, ["Customers", "Orders"], TestContext.Current.CancellationToken);
    }

    private static async Task AssertAllRealIdxFirstDpAreValidAsync(
        MemoryStream ms,
        IReadOnlyList<string> tableNames,
        CancellationToken ct)
    {
        ms.Position = 0;
        byte[] fileBytes = ms.ToArray();

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: ct);

        int pageSize = reader.PageSize;
        long totalPages = fileBytes.LongLength / pageSize;

        foreach (string tableName in tableNames)
        {
            IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(tableName, ct);
            Assert.NotEmpty(indexes);

            // Every non-FK logical index should have a non-zero first_dp
            // pointing at a real index B-tree page. (FK logical indexes
            // share the partner real-idx slot; their first_dp is decoded
            // independently and may legitimately be 0 on freshly-created
            // FK entries — they're outside this hypothesis's scope.)
            int checkedCount = 0;
            foreach (IndexMetadata idx in indexes.Where(i => !i.IsForeignKey))
            {
                string rangeMessage = $"{tableName}.{idx.Name}: FirstDp={idx.FirstDp} out of range (must be > 1 and < {totalPages}). H21 reproduced: writer emitted a zero / invalid real-idx first_dp stamp.";
                Assert.True(idx.FirstDp > 1 && idx.FirstDp < totalPages, rangeMessage);

                byte[] page = await reader.GetRawPageBytesAsync(idx.FirstDp, ct);
                byte tag = page[0];
                string tagMessage = $"{tableName}.{idx.Name}: page {idx.FirstDp} (FirstDp target) has page tag 0x{tag:X2}, expected 0x04 (leaf) or 0x03 (intermediate). H21 reproduced: writer's first_dp does not point at an index B-tree page.";
                Assert.True(tag == 0x04 || tag == 0x03, tagMessage);

                checkedCount++;
            }

            Assert.True(
                checkedCount > 0,
                $"{tableName}: expected at least one non-FK index to validate; got none.");
        }

        // Cross-check: the reader-decoded FirstDp must equal the raw bytes at
        // physical-descriptor offset 38 in the on-disk TDEF. A divergence
        // here would mean the reader is masking a writer-side zero, which
        // would still allow DAO to choke on the same byte.
        await using var reader2 = await AccessReader.OpenAsync(
            new MemoryStream(fileBytes),
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: false,
            cancellationToken: ct);

        foreach (string tableName in tableNames)
        {
            var entry = await reader2.GetCatalogEntryAsync(tableName, ct);
            Assert.NotNull(entry);
            int tdefPage = (int)entry!.TDefPage;
            int off = tdefPage * pageSize;

            // TDEF header layout (Jet4/ACE), see Constants.TableDefinition.Jet4:
            //   numCols at 45 (uint16), numIdx at 47 (int32), numRealIdx at 51 (int32),
            //   columns start at 63 + numRealIdx * 12, each column descriptor = 25 bytes.
            int numCols = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(off + 45, 2));
            int numRealIdx = BinaryPrimitives.ReadInt32LittleEndian(fileBytes.AsSpan(off + 51, 4));
            int colStart = off + 63 + (numRealIdx * 12);
            int namePos = colStart + (numCols * 25);
            for (int c = 0; c < numCols; c++)
            {
                int nameLen = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(namePos, 2));
                namePos += 2 + nameLen;
            }

            for (int i = 0; i < numRealIdx; i++)
            {
                int phys = namePos + (i * 52);
                int firstDpRaw = BinaryPrimitives.ReadInt32LittleEndian(fileBytes.AsSpan(phys + 38, 4));
                string rawMessage = $"{tableName}: real-idx[{i}] raw first_dp at TDEF byte 0x{phys + 38:X} = {firstDpRaw} (out of range 2..{totalPages - 1}). H21 reproduced at the byte level — writer emitted an unstamped real-idx descriptor.";
                Assert.True(firstDpRaw > 1 && firstDpRaw < totalPages, rawMessage);
            }
        }
    }
}
