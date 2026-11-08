namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Regression tests for I17 Compact &amp; Repair capabilities (1)–(7):
/// high-page root <c>TDef</c> relocation, unconstrained small external <c>LVAL</c> inline promotion,
/// <c>Unique</c>/<c>AutoNumber</c> clustering fallback and variable-width page re-bin-packing,
/// lossless Jet4/ACE Unicode compression normalization, <c>MSysObjects</c> catalog <c>DATA</c>-page salvage,
/// calculated-column 23-byte envelope field salvage, and surviving-prefix rescue from broken multi-hop <c>LVAL</c> chains.
/// </summary>
public sealed class CompactI17CapabilitiesTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactAsync_PackInteriorFreePages_RelocatesHighRootTDefIntoInteriorFreePageAndTruncatesTail(DatabaseFormat kind)
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
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Blob", typeof(string), 20_000)],
                cancellationToken: Ct);
            for (int i = 1; i <= 25; i++)
            {
                await writer.InsertRowAsync("EarlySpacer", [i, new string('S', 1800)], Ct);
            }

            await writer.CreateTableAsync(
                "HighRootTable",
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Name", typeof(string), 60)],
                [new IndexDefinition("IX_HighRoot_Name", "Name")],
                Ct);
            await writer.InsertRowAsync("HighRootTable", [1, "Alpha"], Ct);
            await writer.InsertRowAsync("HighRootTable", [2, "Beta"], Ct);

            await writer.DropTableAsync("EarlySpacer", Ct);
        }

        AccessCompactReport beforeInspect = await AccessDatabaseCompact.InspectAsync(
            sourceStream,
            new AccessCompactOptions { PackInteriorFreePages = false },
            Ct);
        long highRootBefore = Assert.Single(beforeInspect.Tables, static t => t.TableName == "HighRootTable").OwnerTDefPage;
        Assert.True(highRootBefore > 15);

        await using var unpackedStream = new MemoryStream();
        AccessCompactReport unpackedReport = await AccessDatabaseCompact.CompactAsync(
            sourceStream,
            unpackedStream,
            new AccessCompactOptions { PackInteriorFreePages = false },
            Ct);

        await using var compactedStream = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(
            sourceStream,
            compactedStream,
            new AccessCompactOptions { PackInteriorFreePages = true },
            Ct);

        Assert.True(report.IsComplete);
        Assert.True(report.DatabaseValidated);

        AccessCompactReport afterInspect = await AccessDatabaseCompact.InspectAsync(compactedStream, cancellationToken: Ct);
        long highRootAfter = Assert.Single(afterInspect.Tables, static t => t.TableName == "HighRootTable").OwnerTDefPage;
        Assert.True(highRootAfter < highRootBefore, $"Expected highRootAfter ({highRootAfter}) < highRootBefore ({highRootBefore}); Src={report.SourcePages}, Dest={report.DestinationPages}, UnpackedDest={unpackedReport.DestinationPages}, Retained={report.RetainedPrefixPages}");
        Assert.True(report.DestinationPages < unpackedReport.DestinationPages, $"Expected Dest ({report.DestinationPages}) < UnpackedDest ({unpackedReport.DestinationPages}); highRootBefore={highRootBefore}, highRootAfter={highRootAfter}");

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(compactedStream, cancellationToken: Ct);
        Assert.Equal(2, await reader.GetRealRowCountAsync("HighRootTable", Ct));
        var names = new List<string>();
        await foreach (object[] row in reader.Rows("HighRootTable", cancellationToken: Ct))
        {
            names.Add(Assert.IsType<string>(row[1]));
        }

        Assert.Equal(["Alpha", "Beta"], names);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactAsync_CoalesceLongValueChains_PromotesSmallExternalLvalInlineBySpillingFullDataPage(DatabaseFormat kind)
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
                "TightInlinePromo",
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("Memo", typeof(string), 20_000),
                    new("Pad", typeof(string), 250),
                ],
                cancellationToken: Ct);

            // Insert row 1 with a large Memo so it is stored as a Type 1 single-page external LVAL,
            // then fill the same DATA page with additional rows so the DATA page has < 30 bytes of slack.
            await writer.InsertRowAsync("TightInlinePromo", [1, new string('M', 500), new string('P', 200)], Ct);
            int fillerRows = kind == DatabaseFormat.Jet3Mdb ? 7 : 16;
            for (int i = 2; i <= fillerRows; i++)
            {
                await writer.InsertRowAsync("TightInlinePromo", [i, "inline", new string('P', 220)], Ct);
            }
        }

        // Shrink row 1's external LVAL payload length on disk from 500 chars to 32 bytes (<= 64-byte inline threshold)
        // while the DATA page remains packed so inline promotion must spill onto an additional DATA page.
        byte[] bytes = sourceStream.ToArray();
        int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        int rowsStartOff = kind == DatabaseFormat.Jet3Mdb ? 10 : 14;
        long tdefPage = FindUserTableTDefPage(bytes, pageSize);

        bool patchedDescriptor = false;
        for (long p = 3; p < bytes.Length / pageSize && !patchedDescriptor; p++)
        {
            int pageOff = checked((int)(p * pageSize));
            if (bytes[pageOff] != 0x01 || BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pageOff + 4, 4)) != (uint)tdefPage)
            {
                continue;
            }

            // Locate row 0 on the DATA page and shrink its 12-byte Type 1 LVAL descriptor's 24-bit length to 32 bytes,
            // and also shrink the target LVAL page slot to 32 bytes.
            int rowStart = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(pageOff + rowsStartOff, 2)) & 0x1FFF;
            int rowEnd = pageSize;
            Span<byte> rowSpan = bytes.AsSpan(pageOff + rowStart, rowEnd - rowStart);
            int expectedByteLen = kind == DatabaseFormat.Jet3Mdb ? 500 : 502; // Jet4/ACE has 0xFF 0xFE compressed prefix
            for (int off = 4; off + 12 <= rowSpan.Length; off++)
            {
                uint lenFlags = BinaryPrimitives.ReadUInt32LittleEndian(rowSpan.Slice(off, 4));
                uint length = lenFlags & 0x3FFFFFFFU;
                uint flags = lenFlags & 0xC0000000U;
                if (flags == 0x40000000U && length == (uint)expectedByteLen)
                {
                    uint lvalPtr = BinaryPrimitives.ReadUInt32LittleEndian(rowSpan.Slice(off + 4, 4));
                    long lvalPage = lvalPtr >> 8;
                    int lvalSlot = (int)(lvalPtr & 0xFFU);
                    if (lvalPage >= 3 && lvalPage < bytes.Length / pageSize)
                    {
                        int lvalPageOff = checked((int)(lvalPage * pageSize));
                        if (bytes[lvalPageOff] == 0x01 && BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(lvalPageOff + 4, 4)) == 0x4C41564CU)
                        {
                            const uint newLen = 32U;
                            BinaryPrimitives.WriteUInt32LittleEndian(rowSpan.Slice(off, 4), newLen | 0x40000000U);
                            int lvalRowStart = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(lvalPageOff + rowsStartOff + (lvalSlot * 2), 2)) & 0x1FFF;
                            int newLvalRowStart = pageSize - (int)newLen;
                            bytes.AsSpan(lvalPageOff + lvalRowStart, (int)newLen).CopyTo(bytes.AsSpan(lvalPageOff + newLvalRowStart, (int)newLen));
                            BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(lvalPageOff + rowsStartOff + (lvalSlot * 2), 2), (ushort)newLvalRowStart);
                            patchedDescriptor = true;
                            break;
                        }
                    }
                }
            }
        }

        Assert.True(patchedDescriptor);

        await using var modifiedSource = new MemoryStream(bytes, writable: false);
        await using var compactedDest = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(
            modifiedSource,
            compactedDest,
            new AccessCompactOptions
            {
                CoalesceLongValueChains = true,
                ClusterByPrimaryKey = false,
            },
            Ct);

        Assert.True(report.DatabaseValidated);
        Assert.True(report.CoalescedLongValueCount >= 1);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(compactedDest, cancellationToken: Ct);
        int expectedRowCount = kind == DatabaseFormat.Jet3Mdb ? 7 : 16;
        Assert.Equal(expectedRowCount, await reader.GetRealRowCountAsync("TightInlinePromo", Ct));
        string expectedMemo = new('M', kind == DatabaseFormat.Jet3Mdb ? 32 : 30);
        object[]? row1 = null;
        await foreach (object[] row in reader.Rows("TightInlinePromo", cancellationToken: Ct))
        {
            if ((int)row[0] == 1)
            {
                row1 = row;
            }
        }

        Assert.NotNull(row1);
        Assert.Equal(expectedMemo, Assert.IsType<string>(row1![1]));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactAsync_ClusterByPrimaryKey_FallsBackToUniqueIndexAndPacksVariableWidthRows(DatabaseFormat kind)
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
                "UniqueClustered",
                [
                    new("Code", typeof(int)) { IsNullable = false },
                    new("Payload", typeof(string), 250) { IsNullable = false },
                ],
                [new IndexDefinition("UQ_Code", "Code") { IsUnique = true }],
                Ct);

            for (int code = 30; code >= 1; code--)
            {
                await writer.InsertRowAsync("UniqueClustered", [code, $"row-{code:D2}-" + new string('V', 150 + (code % 40))], Ct);
            }
        }

        await using var compactedStream = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(
            sourceStream,
            compactedStream,
            new AccessCompactOptions { ClusterByPrimaryKey = true },
            Ct);

        Assert.True(report.DatabaseValidated);
        Assert.Equal(1, report.ClusteredTableCount);
        Assert.True(Assert.Single(report.Tables, static t => t.TableName == "UniqueClustered").ClusteredByPrimaryKey);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(compactedStream, cancellationToken: Ct);
        var codes = new List<int>();
        await foreach (object[] row in reader.Rows("UniqueClustered", cancellationToken: Ct))
        {
            codes.Add(Assert.IsType<int>(row[0]));
        }

        Assert.Equal(Enumerable.Range(1, 30).ToArray(), codes);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactAsync_ClusterByPrimaryKey_FallsBackToAutoNumberColumnWhenNoUniqueIndexExists(DatabaseFormat kind)
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
                "AutoClustered",
                [
                    new("Seq", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = false },
                    new("Label", typeof(string), 80),
                ],
                cancellationToken: Ct);

            await writer.InsertRowAsync("AutoClustered", [1, "First"], Ct);
            await writer.InsertRowAsync("AutoClustered", [2, "Second"], Ct);
            await writer.InsertRowAsync("AutoClustered", [3, "Third"], Ct);
        }

        // Reverse the physical slot table order on the DATA page so physical scan order is [3, 2, 1].
        byte[] bytes = sourceStream.ToArray();
        int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        int rowsStartOff = kind == DatabaseFormat.Jet3Mdb ? 10 : 14;
        long tdefPage = FindUserTableTDefPage(bytes, pageSize);
        for (long p = 3; p < bytes.Length / pageSize; p++)
        {
            int pageOff = checked((int)(p * pageSize));
            if (bytes[pageOff] == 0x01 && BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pageOff + 4, 4)) == (uint)tdefPage)
            {
                ushort slot0 = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(pageOff + rowsStartOff, 2));
                ushort slot2 = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(pageOff + rowsStartOff + 4, 2));
                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(pageOff + rowsStartOff, 2), slot2);
                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(pageOff + rowsStartOff + 4, 2), slot0);
                break;
            }
        }

        await using var outOfOrderSource = new MemoryStream(bytes, writable: false);
        await using var compactedStream = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(
            outOfOrderSource,
            compactedStream,
            new AccessCompactOptions { ClusterByPrimaryKey = true },
            Ct);

        Assert.True(report.DatabaseValidated);
        Assert.Equal(1, report.ClusteredTableCount);
        Assert.True(Assert.Single(report.Tables, static t => t.TableName == "AutoClustered").ClusteredByPrimaryKey);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(compactedStream, cancellationToken: Ct);
        var seqs = new List<int>();
        await foreach (object[] row in reader.Rows("AutoClustered", cancellationToken: Ct))
        {
            seqs.Add(Assert.IsType<int>(row[0]));
        }

        Assert.Equal([1, 2, 3], seqs);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactAsync_UncompressedUtf16TextSliceOnCompressedUnicodeColumn_NormalizesToCompressedUnicode(DatabaseFormat kind)
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
                "UnicodeNorm",
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("Title", typeof(string), 80),
                ],
                cancellationToken: Ct);

            // Insert a 12-character ASCII string ("ABCDEF012345") which compressed is 14 bytes (0xFF 0xFE + 12 ASCII bytes),
            // and then overwrite its variable-column slice in-place with a 7-character uncompressed UTF-16LE string ("UNCOMP7" = 14 bytes).
            await writer.InsertRowAsync("UnicodeNorm", [1, "ABCDEF012345"], Ct);
        }

        byte[] bytes = sourceStream.ToArray();
        byte[] compressedTarget = [0xFF, 0xFE, .. Encoding.ASCII.GetBytes("ABCDEF012345")];
        byte[] uncompressedReplacement = Encoding.Unicode.GetBytes("UNCOMP7");
        Assert.Equal(compressedTarget.Length, uncompressedReplacement.Length);

        int idx = bytes.AsSpan().IndexOf(compressedTarget);
        Assert.True(idx >= 0);
        uncompressedReplacement.CopyTo(bytes.AsSpan(idx, uncompressedReplacement.Length));

        await using var uncompressedSource = new MemoryStream(bytes, writable: false);
        await using (AccessReader preReader = await TestDatabase.OpenReaderAsync(uncompressedSource, cancellationToken: Ct))
        {
            object[] row = await preReader.Rows("UnicodeNorm", cancellationToken: Ct).SingleAsync(Ct);
            Assert.Equal("UNCOMP7", Assert.IsType<string>(row[1]));
        }

        await using var compactedStream = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(
            uncompressedSource,
            compactedStream,
            cancellationToken: Ct);

        Assert.True(report.DatabaseValidated);
        byte[] compactedBytes = compactedStream.ToArray();
        byte[] expectedCompressed = [0xFF, 0xFE, .. Encoding.ASCII.GetBytes("UNCOMP7")];
        Assert.True(compactedBytes.AsSpan().IndexOf(expectedCompressed) >= 0);
        Assert.True(compactedBytes.AsSpan().IndexOf(uncompressedReplacement) < 0);

        await using AccessReader postReader = await TestDatabase.OpenReaderAsync(compactedStream, cancellationToken: Ct);
        object[] postRow = await postReader.Rows("UnicodeNorm", cancellationToken: Ct).SingleAsync(Ct);
        Assert.Equal("UNCOMP7", Assert.IsType<string>(postRow[1]));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactAsync_DamagedMSysObjectsCatalogDataPageSlot_SalvagesPartialCatalogPageAndPreservesSurvivingTables(DatabaseFormat kind)
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
                "TableKeep",
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Val", typeof(string), 40)],
                cancellationToken: Ct);
            await writer.InsertRowAsync("TableKeep", [1, "Survived"], Ct);

            await writer.CreateTableAsync(
                "TableExtra",
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Val", typeof(string), 40)],
                cancellationToken: Ct);
            await writer.InsertRowAsync("TableExtra", [1, "AlsoSurvived"], Ct);
        }

        byte[] bytes = sourceStream.ToArray();
        int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        int rowsStartOff = kind == DatabaseFormat.Jet3Mdb ? 10 : 14;
        int numRowsHeaderOff = kind == DatabaseFormat.Jet3Mdb ? 8 : 12;

        // Find MSysObjects's DATA page (owner == 2) and append one corrupt active slot pointing out of bounds (offset 0x1FFF)
        // so the catalog DATA page fails whole-page validation and exercises slot-level catalog salvage.
        long catalogDataPage = -1;
        for (long p = 3; p < bytes.Length / pageSize; p++)
        {
            int pageOff = checked((int)(p * pageSize));
            if (bytes[pageOff] == 0x01 && BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pageOff + 4, 4)) == 2U)
            {
                catalogDataPage = p;
                int numRows = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(pageOff + numRowsHeaderOff, 2));
                int newSlotOff = pageOff + rowsStartOff + (numRows * 2);
                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(newSlotOff, 2), 0x1FFF);
                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(pageOff + numRowsHeaderOff, 2), (ushort)(numRows + 1));
                break;
            }
        }

        Assert.True(catalogDataPage >= 3);

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

        Assert.True(report.DatabaseValidated);
        Assert.Contains(
            report.Diagnostics,
            d => d.Kind == "salvaged-partial-data-page"
                && d.PageNumber == catalogDataPage
                && d.TableName == "MSysObjects");

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(compactedDest, cancellationToken: Ct);
        object[] keepRow = await reader.Rows("TableKeep", cancellationToken: Ct).SingleAsync(Ct);
        Assert.Equal("Survived", Assert.IsType<string>(keepRow[1]));
    }

    [Fact]
    public async Task CompactAsync_DamagedCalculatedColumnEnvelope_SalvagesFieldInPlaceAndPreservesRow()
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
                "CalcInvoices",
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("Qty", typeof(int)),
                    new("Price", typeof(int)),
                    new("Total", typeof(int)) { IsCalculated = true, CalculationExpression = "[Qty]*[Price]" },
                    new("Tag", typeof(string), 40),
                ],
                cancellationToken: Ct);

            await writer.InsertRowAsync("CalcInvoices", [1, 3, 10, null, "FIRST"], Ct);
            await writer.InsertRowAsync("CalcInvoices", [2, 4, 20, null, "SECOND"], Ct);
        }

        byte[] bytes = sourceStream.ToArray();
        const int pageSize = 4096;
        long tdefPage = FindUserTableTDefPage(bytes, pageSize);

        // Corrupt row 0's 23-byte calculated column envelope on the DATA page (overwrite leading 4-byte length prefix 19 with 0x77777777).
        long targetDataPage = -1;
        for (long p = 3; p < bytes.Length / pageSize; p++)
        {
            int pageOff = checked((int)(p * pageSize));
            if (bytes[pageOff] == 0x01 && BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pageOff + 4, 4)) == (uint)tdefPage)
            {
                targetDataPage = p;
                int row0Start = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(pageOff + 14, 2)) & 0x1FFF;
                Span<byte> row0Span = bytes.AsSpan(pageOff + row0Start, pageSize - row0Start);

                // Find the 23-byte envelope payload length (4 bytes == 4) followed by the int32 result 30 (0x1E, 0x00, 0x00, 0x00).
                ReadOnlySpan<byte> calcHeader = [0x04, 0x00, 0x00, 0x00, 0x1E, 0x00, 0x00, 0x00];
                int envIdx = row0Span.IndexOf(calcHeader);
                Assert.True(envIdx >= 0);
                BinaryPrimitives.WriteInt32LittleEndian(row0Span.Slice(envIdx, 4), 0x77777777);
                break;
            }
        }

        Assert.True(targetDataPage >= 3);

        await using var strictSource = new MemoryStream(bytes, writable: false);
        await using var strictDest = new MemoryStream();
        _ = await Assert.ThrowsAsync<JetCorruptDataException>(
            async () => await AccessDatabaseCompact.CompactAsync(
                strictSource,
                strictDest,
                new AccessCompactOptions { AllowPartialRecovery = false },
                Ct));

        await using var salvageSource = new MemoryStream(bytes, writable: false);
        await using var salvageDest = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(
            salvageSource,
            salvageDest,
            new AccessCompactOptions
            {
                AllowPartialRecovery = true,
                RebuildDamagedIndexes = true,
                RepairTableCounters = true,
            },
            Ct);

        Assert.True(report.DatabaseValidated);
        Assert.Contains(
            report.Diagnostics,
            d => d.Kind == "salvaged-calculated-envelope"
                && d.PageNumber == targetDataPage
                && d.RowIndex == 0
                && d.TableName == "CalcInvoices");
        Assert.Contains(
            report.Diagnostics,
            d => d.Kind == "quarantined-corrupt-calculated-field"
                && d.PageNumber == targetDataPage
                && d.RowIndex == 0
                && d.TableName == "CalcInvoices");

        AccessCompactQuarantineRecord qRec = Assert.Single(report.QuarantinedRecords);
        Assert.Equal("quarantined-corrupt-calculated-field", qRec.Kind);
        Assert.NotNull(qRec.DecodedFields);
        Assert.Equal(1, qRec.DecodedFields!["Id"]);
        Assert.Equal("FIRST", qRec.DecodedFields["Tag"]);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(salvageDest, cancellationToken: Ct);
        var rows = new List<object[]>();
        await foreach (object[] row in reader.Rows("CalcInvoices", cancellationToken: Ct))
        {
            rows.Add(row);
        }

        Assert.Equal(2, rows.Count);
        object[] row1 = Assert.Single(rows, static r => (int)r[0] == 1);
        Assert.Equal("FIRST", Assert.IsType<string>(row1[4]));
        object[] row2 = Assert.Single(rows, static r => (int)r[0] == 2);
        Assert.Equal(80, row2[3]);
        Assert.Equal("SECOND", Assert.IsType<string>(row2[4]));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompactAsync_BrokenMultiHopLvalChain_RescuesSurvivingPrefixInQuarantinedRecord(DatabaseFormat kind)
    {
        await using var sourceStream = new MemoryStream();
        string longMemo = "SURVIVING-PREFIX-HEADER-" + new string('Z', 6500);
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            sourceStream,
            kind,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                "MultiHopVictim",
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("BigMemo", typeof(string), 20_000),
                ],
                cancellationToken: Ct);

            await writer.InsertRowAsync("MultiHopVictim", [1, longMemo], Ct);
        }

        byte[] bytes = sourceStream.ToArray();
        int pageSize = kind == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        int rowsStartOff = kind == DatabaseFormat.Jet3Mdb ? 10 : 14;
        long tdefPage = FindUserTableTDefPage(bytes, pageSize);

        // Locate the first Type 2 LVAL segment pointed to by row 0 and corrupt its 4-byte nextPtr to point out-of-range (0x00FFFF00U),
        // leaving the first segment's payload bytes intact while breaking the second hop of the chain.
        bool brokeSecondHop = false;
        for (long p = 3; p < bytes.Length / pageSize && !brokeSecondHop; p++)
        {
            int pageOff = checked((int)(p * pageSize));
            if (bytes[pageOff] != 0x01 || BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(pageOff + 4, 4)) != (uint)tdefPage)
            {
                continue;
            }

            int row0Start = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(pageOff + rowsStartOff, 2)) & 0x1FFF;
            Span<byte> row0Span = bytes.AsSpan(pageOff + row0Start, pageSize - row0Start);
            for (int off = 4; off + 12 <= row0Span.Length; off++)
            {
                uint lenFlags = BinaryPrimitives.ReadUInt32LittleEndian(row0Span.Slice(off, 4));
                if ((lenFlags & 0xC0000000U) == 0U && (lenFlags & 0x3FFFFFFFU) > 4000U)
                {
                    uint firstHopPtr = BinaryPrimitives.ReadUInt32LittleEndian(row0Span.Slice(off + 4, 4));
                    long lvalPage = firstHopPtr >> 8;
                    int lvalSlot = (int)(firstHopPtr & 0xFFU);
                    if (lvalPage >= 3 && lvalPage < bytes.Length / pageSize)
                    {
                        int lvalPageOff = checked((int)(lvalPage * pageSize));
                        if (bytes[lvalPageOff] == 0x01 && BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(lvalPageOff + 4, 4)) == 0x4C41564CU)
                        {
                            int segStart = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(lvalPageOff + rowsStartOff + (lvalSlot * 2), 2)) & 0x1FFF;
                            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(lvalPageOff + segStart, 4), 0x00FFFF00U);
                            brokeSecondHop = true;
                            break;
                        }
                    }
                }
            }
        }

        Assert.True(brokeSecondHop);

        await using var corruptedSource = new MemoryStream(bytes, writable: false);
        await using var salvagedDest = new MemoryStream();
        AccessCompactReport report = await AccessDatabaseCompact.CompactAsync(
            corruptedSource,
            salvagedDest,
            new AccessCompactOptions
            {
                AllowPartialRecovery = true,
                RebuildDamagedIndexes = true,
                RepairTableCounters = true,
            },
            Ct);

        Assert.True(report.DatabaseValidated);
        AccessCompactQuarantineRecord qRec = Assert.Single(report.QuarantinedRecords, static r => r.Kind == "quarantined-corrupt-lval-field");
        Assert.NotNull(qRec.DecodedFields);
        string rescuedPrefix = Assert.IsType<string>(qRec.DecodedFields!["BigMemo"]);
        Assert.StartsWith("SURVIVING-PREFIX-HEADER-", rescuedPrefix, StringComparison.Ordinal);
        Assert.True(rescuedPrefix.Length > 500);
    }

    private static long FindUserTableTDefPage(byte[] bytes, int pageSize)
    {
        long candidate = -1;
        for (long p = 3; p < bytes.Length / pageSize; p++)
        {
            int offset = checked((int)(p * pageSize));
            if (bytes[offset] == 0x02)
            {
                int realIdxCount = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset + (pageSize == 2048 ? 28 : 40), 4));
                if (realIdxCount >= 0)
                {
                    candidate = p;
                }
            }
        }

        if (candidate < 3)
        {
            throw new InvalidOperationException("No user table TDEF page found.");
        }

        return candidate;
    }
}
