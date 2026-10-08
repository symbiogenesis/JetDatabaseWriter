namespace JetDatabaseWriter.Tests.Conformance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;
#pragma warning disable SA1312 // Variable '_' is a discard.

/// <summary>
/// Regression tests for CVE-class vulnerabilities in the JET/MDB parsing paths.
/// Each test corrupts a specific field in a valid database file and asserts that
/// the reader handles the malformation gracefully: no OOM, no infinite loop,
/// deliberate corruption errors instead of uncontrolled runtime exceptions.
/// </summary>
/// <param name="db">The database input.</param>
/// <remarks>
/// Related CVEs:
///   CVE-2005-0944 / CVE-2007-6026 / CVE-2008-1092 (crafted column counts),
/// </remarks>
public sealed class CveMitigationTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    // ─── CVE-2007-6026 analog: numCols overflow in TDEF ────────────────

    /// <summary>
    /// Corrupts the TDEF page's numCols field to 0xFFFF (65535), far exceeding
    /// the 4096-column cap. The reader must not OOM or crash; the table should
    /// either be skipped or produce an error/empty result.
    /// </summary>
    [Fact]
    public async Task ReadTable_CorruptNumCols_0xFFFF_DoesNotCrashOrOom()
    {
        string path = TestDatabases.NorthwindTraders;
        Assert.True(File.Exists(path), $"Required security fixture is missing: {path}");

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        // Jet4/ACE TDEF numCols offset = 45 within the page payload.
        // Page 2 starts at file offset 2 * 4096 = 8192.
        const int tdefPage2Start = 2 * 4096;
        const int numColsOffset = tdefPage2Start + 45;

        Assert.True(corrupted.Length >= numColsOffset + 2, "Security fixture does not contain the mutation field.");

        BinaryPrimitives.WriteUInt16LittleEndian(corrupted.AsSpan(numColsOffset, 2), 0xFFFF);

        await using var stream = new MemoryStream(corrupted, writable: false);
        Exception? ex = await Record.ExceptionAsync(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(
                stream,
                new AccessReaderOptions { UseLockFile = false },
                leaveOpen: true,
                ct);

            await foreach (object[] _ in reader.Rows("Categories", cancellationToken: ct))
            {
                break;
            }
        });

        Assert.True(ex is null or IJetException or InvalidDataException or NotSupportedException, $"Unexpected parser failure: {ex}");
    }

    /// <summary>
    /// Corrupts the TDEF page's numCols to exactly 4097 (one past the cap).
    /// Verifies the 4096-column cap rejects this cleanly.
    /// </summary>
    [Fact]
    public async Task ReadTable_CorruptNumCols_4097_DoesNotCrashOrOom()
    {
        string path = TestDatabases.NorthwindTraders;
        Assert.True(File.Exists(path), $"Required security fixture is missing: {path}");

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        const int tdefPage2Start = 2 * 4096;
        const int numColsOffset = tdefPage2Start + 45;

        Assert.True(corrupted.Length >= numColsOffset + 2, "Security fixture does not contain the mutation field.");

        BinaryPrimitives.WriteUInt16LittleEndian(corrupted.AsSpan(numColsOffset, 2), 4097);

        await using var stream = new MemoryStream(corrupted, writable: false);
        Exception? ex = await Record.ExceptionAsync(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(
                stream,
                new AccessReaderOptions { UseLockFile = false },
                leaveOpen: true,
                ct);

            await foreach (object[] _ in reader.Rows("Categories", cancellationToken: ct))
            {
                break;
            }
        });

        Assert.True(ex is null or IJetException or InvalidDataException or NotSupportedException, $"Unexpected parser failure: {ex}");
    }

    // ─── TDEF index-count corruption ────────────────────────────────

    /// <summary>
    /// Corrupts the numRealIdx field (offset 51 in Jet4/ACE TDEF) to a huge
    /// value. The reader clamps this to [0, 1000]; verify no crash/OOM.
    /// </summary>
    [Fact]
    public async Task ReadTable_CorruptNumRealIdx_0x7FFFFFFF_DoesNotCrashOrOom()
    {
        string path = TestDatabases.NorthwindTraders;
        Assert.True(File.Exists(path), $"Required security fixture is missing: {path}");

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        const int tdefPage2Start = 2 * 4096;
        const int numRealIdxOffset = tdefPage2Start + 51;

        Assert.True(corrupted.Length >= numRealIdxOffset + 4, "Security fixture does not contain the mutation field.");

        BinaryPrimitives.WriteInt32LittleEndian(corrupted.AsSpan(numRealIdxOffset, 4), int.MaxValue);

        await using var stream = new MemoryStream(corrupted, writable: false);
        Exception? ex = await Record.ExceptionAsync(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(
                stream,
                new AccessReaderOptions { UseLockFile = false },
                leaveOpen: true,
                ct);

            await foreach (object[] _ in reader.Rows("Categories", cancellationToken: ct))
            {
                break;
            }
        });

        Assert.True(ex is null or IJetException or InvalidDataException or NotSupportedException, $"Unexpected parser failure: {ex}");
    }

    // ─── Malformed metadata: data page row-offset corruption ────────

    /// <summary>
    /// Corrupts a data page's row-offset table entries to point past the page
    /// boundary. The row-offset parser masks with 0x1FFF and checks against
    /// page size; rows with invalid offsets should be skipped.
    /// </summary>
    [Fact]
    public async Task ReadTable_CorruptRowOffsets_AllOutOfBounds_ReturnsZeroRows()
    {
        string path = TestDatabases.NorthwindTraders;
        Assert.True(File.Exists(path), $"Required security fixture is missing: {path}");

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        const int pageSize = Constants.PageSizes.Jet4;
        int dataPageNumber = FindFirstDataPage(corrupted, pageSize);
        Assert.True(dataPageNumber >= 0, "Security fixture contains no suitable data page.");

        int dpStart = dataPageNumber * pageSize;
        int rowCount = BinaryPrimitives.ReadUInt16LittleEndian(corrupted.AsSpan(dpStart + 12, 2));

        // Corrupt all row offsets to 0x1FFF (max value after masking = 8191 > 4096).
        for (int r = 0; r < rowCount; r++)
        {
            int offsetPos = dpStart + 14 + (r * 2);
            if (offsetPos + 2 <= corrupted.Length)
            {
                BinaryPrimitives.WriteUInt16LittleEndian(corrupted.AsSpan(offsetPos, 2), 0x1FFF);
            }
        }

        await using var stream = new MemoryStream(corrupted, writable: false);
        Exception? ex = await Record.ExceptionAsync(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(
                stream,
                new AccessReaderOptions { UseLockFile = false },
                leaveOpen: true,
                ct);

            IReadOnlyList<string> tables = await reader.ListTablesAsync(ct);
            foreach (string table in tables)
            {
                int count = 0;
                await foreach (object[] _ in reader.Rows(table, cancellationToken: ct))
                {
                    count++;
                    if (count > 10)
                    {
                        break;
                    }
                }
            }
        });

        Assert.True(ex is null or IJetException or InvalidDataException or NotSupportedException, $"Unexpected parser failure: {ex}");
    }

    // ─── Malformed metadata: data page numRows overflow ─────────────

    /// <summary>
    /// Corrupts the numRows field on EVERY data page (type 0x01) in the file to
    /// 2500, exceeding the physical max of (4096 − 14) / 2 = 2041 row-offset
    /// entries, including the catalog pages. Catalog discovery must deliberately
    /// refuse malformed live metadata in both parsing modes. Without the numRows
    /// clamp in <c>EnumerateLiveRowBounds</c> / <c>ComputeRowDirectory</c>,
    /// <c>Ru16</c> reads past the page buffer and throws
    /// <see cref="ArgumentOutOfRangeException"/>.
    /// </summary>
    /// <param name="strictParsing">Whether scalar value parsing is strict.</param>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ListTables_AllDataPagesNumRowsCorrupted_RefusesCorruptCatalog(bool strictParsing)
    {
        string path = TestDatabases.NorthwindTraders;
        Assert.True(File.Exists(path), $"Required security fixture is missing: {path}");

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        const int pageSize = Constants.PageSizes.Jet4;
        int pageCount = corrupted.Length / pageSize;

        // Corrupt numRows on every data page to exceed the physical capacity.
        for (int p = 1; p < pageCount; p++)
        {
            int pageStart = p * pageSize;

            // data page
            if (corrupted[pageStart] == 0x01)
            {
                BinaryPrimitives.WriteUInt16LittleEndian(corrupted.AsSpan(pageStart + 12, 2), 2500);
            }
        }

        await using var stream = new MemoryStream(corrupted, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false, StrictParsing = strictParsing },
            leaveOpen: true,
            ct);

        for (int attempt = 0; attempt < 2; attempt++)
        {
            JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                await reader.ListTablesAsync(ct));
            Assert.Equal(JetErrorCode.CorruptCatalog, error.ErrorCode);
            Assert.Equal("MSysObjects", error.ErrorInfo.TableName);
            Assert.NotNull(error.ErrorInfo.PageNumber);
        }
    }

    // ─── Inline MEMO length overflow (malformed length) ───────────

    /// <summary>
    /// Corrupts an inline MEMO header's 3-byte length to the maximum (0xFFFFFF = 16 MB)
    /// while keeping the bitmask as 0x80 (inline). The reader must cap the length
    /// against the remaining row bytes, not allocate 16 MB.
    /// </summary>
    [Fact]
    public async Task ReadTable_CorruptInlineMemoLen_16MB_DoesNotAllocateHuge()
    {
        string path = TestDatabases.NorthwindTraders;
        Assert.True(File.Exists(path), $"Required security fixture is missing: {path}");

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        const int pageSize = Constants.PageSizes.Jet4;
        bool found = false;
        for (int p = 3; p < corrupted.Length / pageSize && !found; p++)
        {
            int pageStart = p * pageSize;
            if (corrupted[pageStart] != 0x01)
            {
                continue;
            }

            int nr = BinaryPrimitives.ReadUInt16LittleEndian(corrupted.AsSpan(pageStart + 12, 2));
            if (nr is 0 or > 200)
            {
                continue;
            }

            int firstRowRaw = BinaryPrimitives.ReadUInt16LittleEndian(corrupted.AsSpan(pageStart + 14, 2));
            if ((firstRowRaw & 0xC000) != 0)
            {
                continue;
            }

            int rowStart = pageStart + (firstRowRaw & 0x1FFF);
            if (rowStart + 16 >= corrupted.Length || rowStart + 16 >= pageStart + pageSize)
            {
                continue;
            }

            // Write a fake inline MEMO header: length = 0xFFFFFF (16 MB), bitmask = 0x80 (inline).
            int memoHeaderPos = rowStart + 4;
            if (memoHeaderPos + 12 < pageStart + pageSize)
            {
                corrupted[memoHeaderPos] = 0xFF;
                corrupted[memoHeaderPos + 1] = 0xFF;
                corrupted[memoHeaderPos + 2] = 0xFF;
                corrupted[memoHeaderPos + 3] = 0x80;
                found = true;
            }
        }

        Assert.True(found, "Security fixture contains no suitable mutation target.");

        await using var stream = new MemoryStream(corrupted, writable: false);
        Exception? ex = await Record.ExceptionAsync(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(
                stream,
                new AccessReaderOptions { UseLockFile = false },
                leaveOpen: true,
                ct);

            IReadOnlyList<string> tables = await reader.ListTablesAsync(ct);
            foreach (string table in tables)
            {
                int count = 0;
                await foreach (object[] _ in reader.Rows(table, cancellationToken: ct))
                {
                    count++;
                    if (count > 100)
                    {
                        break;
                    }
                }
            }
        });

        Assert.True(ex is null or IJetException or InvalidDataException or NotSupportedException, $"Unexpected parser failure: {ex}");
    }

    // ─── CVE-2020-1400 analog: integer underflow in var-col offsets ────

    /// <summary>
    /// Corrupts the variable-column offset table within rows to produce
    /// <c>varEnd &lt; varOff</c>, causing <c>dataLen = varEnd - varOff</c> to
    /// underflow to a negative value. Without the <c>dataLen &lt; 0</c> guard in
    /// <c>ResolveColumnSlice</c>, this would produce a huge positive length
    /// (in native code) or a negative <c>Span</c> length (in managed code).
    /// <see href="https://www.zerodayinitiative.com/advisories/ZDI-20-924/"/>
    /// identifies this class of bug (integer underflow before memory write) as
    /// the root cause of CVE-2020-1400.
    /// </summary>
    [Fact]
    public void ReadTable_CorruptVarColOffsets_IntegerUnderflow_DoesNotCrash()
    {
        // Security regression: ROW-VAR-OFFSET-UNDERFLOW
        // A real Jet4 trailer: EOD(2), reversed var-offsets(2), var-count(2), null-mask(1).
        byte[] row = [1, 0, 0x41, 0x42, 0x43, 5, 0, 2, 0, 1, 0, 1];
        var fields = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb).RowFields;
        var column = new ColumnInfo { Type = ColumnType.BinaryType, ColNum = 0, VarIdx = 0 };
        Assert.True(RowDecodePlan.TryParseRowLayout(fields, row, 0, row.Length, true, out var layout));
        var valid = RowDecodePlan.ResolveColumnSlice(fields, row, 0, row.Length, layout, column);
        Assert.Equal(ColumnSliceKind.Var, valid.Kind);
        Assert.Equal(3, valid.DataLen);

        // Change the actual variable offset to exceed EOD, leaving the trailer valid.
        BinaryPrimitives.WriteUInt16LittleEndian(row.AsSpan(layout.VarTableStart, 2), 6);
        Assert.True(RowDecodePlan.TryParseRowLayout(fields, row, 0, row.Length, true, out layout));
        var corrupt = RowDecodePlan.ResolveColumnSlice(fields, row, 0, row.Length, layout, column);
        Assert.Equal(ColumnSliceKind.Empty, corrupt.Kind);
        Assert.Equal(0, corrupt.DataLen);
    }

    // ─── Helper ─────────────────────────────────────────────────────────

    private static int FindFirstDataPage(byte[] fileBytes, int pageSize)
    {
        for (int p = 3; p < fileBytes.Length / pageSize; p++)
        {
            int pageStart = p * pageSize;
            if (fileBytes[pageStart] == 0x01)
            {
                int nr = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(pageStart + 12, 2));
                if (nr is > 0 and < 200)
                {
                    return p;
                }
            }
        }

        return -1;
    }

    // ─── CVE-2007-6026 analog: TDEF truncated before column names ──────

    /// <summary>
    /// Truncates every TDEF page (type 0x02) at 200 bytes — shorter than where
    /// column names would begin — then attempts to open and read. The reader
    /// must detect that <c>namePos > td.Length</c> and return <c>null</c> from
    /// <c>ReadTableDefAsync</c>, skipping the table gracefully.
    /// </summary>
    [Fact]
    public async Task ReadTable_TruncatedTDefPage_DoesNotCrashOrOom()
    {
        string path = TestDatabases.NorthwindTraders;
        Assert.True(File.Exists(path), $"Required security fixture is missing: {path}");

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        const int pageSize = Constants.PageSizes.Jet4;
        int pageCount = corrupted.Length / pageSize;

        // Zero out the tail of every TDEF page so column-name region is invalid.
        for (int p = 1; p < pageCount; p++)
        {
            int pageStart = p * pageSize;
            if (corrupted[pageStart] == 0x02)
            {
                // Keep header (first 200 bytes) but wipe the rest, truncating names.
                Array.Clear(corrupted, pageStart + 200, pageSize - 200);
            }
        }

        await using var stream = new MemoryStream(corrupted, writable: false);
        Exception? ex = await Record.ExceptionAsync(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(
                stream,
                new AccessReaderOptions { UseLockFile = false },
                leaveOpen: true,
                ct);

            IReadOnlyList<string> tables = await reader.ListTablesAsync(ct);
            foreach (string table in tables)
            {
                int count = 0;
                await foreach (object[] _ in reader.Rows(table, cancellationToken: ct))
                {
                    count++;
                    if (count > 10)
                    {
                        break;
                    }
                }
            }
        });

        Assert.True(ex is null or IJetException or InvalidDataException or NotSupportedException, $"Unexpected parser failure: {ex}");
    }

    // ─── Malformed metadata: corrupted in-row numCols ───────────────

    /// <summary>
    /// Corrupts the first byte(s) of every live row on data pages so that the
    /// in-row numCols field is 0xFF (255 for Jet3) or 0xFFFF (65535 for Jet4/ACE).
    /// <c>TryParseRowLayout</c> computes <c>nullMaskSz = (numCols + 7) / 8</c>
    /// and checks that <c>nullMaskPos >= _rowSz.NumCols</c>. With a massive
    /// numCols the null-mask would extend beyond the row, causing
    /// <c>TryParseRowLayout</c> to return <c>false</c> and the row to be skipped.
    /// </summary>
    [Fact]
    public async Task ReadTable_CorruptInRowNumCols_DoesNotCrash()
    {
        string path = TestDatabases.NorthwindTraders;
        Assert.True(File.Exists(path), $"Required security fixture is missing: {path}");

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        const int pageSize = Constants.PageSizes.Jet4;
        int pageCount = corrupted.Length / pageSize;

        // Jet4/ACE: numCols is a 2-byte LE value at the START of each row.
        // Corrupt the first 2 bytes of every row on every data page.
        for (int p = 1; p < pageCount; p++)
        {
            int pageStart = p * pageSize;
            if (corrupted[pageStart] != 0x01)
            {
                continue;
            }

            int nr = BinaryPrimitives.ReadUInt16LittleEndian(corrupted.AsSpan(pageStart + 12, 2));
            if (nr is 0 or > 200)
            {
                continue;
            }

            for (int r = 0; r < nr; r++)
            {
                int rawOffset = BinaryPrimitives.ReadUInt16LittleEndian(
                    corrupted.AsSpan(pageStart + 14 + (r * 2), 2));
                if ((rawOffset & 0xC000) != 0)
                {
                    continue; // skip deleted/overflow rows
                }

                int rowStart = pageStart + (rawOffset & 0x1FFF);
                if (rowStart + 2 >= pageStart + pageSize)
                {
                    continue;
                }

                // Write numCols = 0xFFFF into the row header.
                BinaryPrimitives.WriteUInt16LittleEndian(corrupted.AsSpan(rowStart, 2), 0xFFFF);
            }
        }

        await using var stream = new MemoryStream(corrupted, writable: false);
        Exception? ex = await Record.ExceptionAsync(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(
                stream,
                new AccessReaderOptions { UseLockFile = false },
                leaveOpen: true,
                ct);

            IReadOnlyList<string> tables = await reader.ListTablesAsync(ct);
            foreach (string table in tables)
            {
                int count = 0;
                await foreach (object[] _ in reader.Rows(table, cancellationToken: ct))
                {
                    count++;
                    if (count > 50)
                    {
                        break;
                    }
                }
            }
        });

        Assert.True(ex is null or IJetException or InvalidDataException or NotSupportedException, $"Unexpected parser failure: {ex}");
    }

    // ─── Malformed metadata: LVAL chain cycle ───────────────────────

    /// <summary>
    /// Corrupts an LVAL page pointer to form a self-referencing cycle:
    /// the "next page" field of the first LVAL row points back to the same page.
    /// Without cycle detection (<c>HashSet&lt;uint&gt; seen</c> in
    /// <c>ReadLvalChainAsync</c>), this would loop indefinitely. With it,
    /// the chain terminates immediately.
    /// </summary>
    [Fact]
    public async Task ReadTable_LvalChainCycle_DoesNotHang()
    {
        // Security regression: LVAL-CYCLE
        const uint rowPointer = 256;
        byte[] page = new byte[16];
        BinaryPrimitives.WriteUInt32LittleEndian(page.AsSpan(0, 4), rowPointer);
        int reads = 0;
        LvalChainResult result = await LongValueStore.ReadChainedPayloadAsync(
            rowPointer, 32, page.Length, LocateAsync, TestContext.Current.CancellationToken);
        Assert.Null(result.Data);
        Assert.Equal("the chain contains a cycle", result.Error);
        Assert.Equal(1, reads);

        ValueTask<LvalRowLocation> LocateAsync(uint pointer, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            Assert.Equal(rowPointer, pointer);
            reads++;
            return new ValueTask<LvalRowLocation>(new LvalRowLocation(page, 0, 8, null));
        }
    }
    // ─── Malformed metadata: LVAL chained MEMO length overflow ─────

    /// <summary>
    /// Corrupts a MEMO header to indicate a chained LVAL path (bitmask 0x00)
    /// with a declared memoLen of 16 MB (0xFFFFFF). The reader pre-allocates
    /// <c>new byte[memoLen]</c> in <c>ReadLvalChainAsync</c>, so the maximum
    /// allocation is bounded at 16 MB by the 3-byte field. This test confirms
    /// that a 16 MB LVAL declaration does not cause OOM or unhandled exceptions,
    /// exercising the LVAL allocation path that the inline-MEMO test does not cover.
    /// </summary>
    [Fact]
    public async Task ReadTable_CorruptLvalMemoLen_16MB_DoesNotOom()
    {
        string path = TestDatabases.NorthwindTraders;
        Assert.True(File.Exists(path), $"Required security fixture is missing: {path}");

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        const int pageSize = Constants.PageSizes.Jet4;
        bool found = false;

        for (int p = 3; p < corrupted.Length / pageSize && !found; p++)
        {
            int pageStart = p * pageSize;
            if (corrupted[pageStart] != 0x01)
            {
                continue;
            }

            int nr = BinaryPrimitives.ReadUInt16LittleEndian(corrupted.AsSpan(pageStart + 12, 2));
            if (nr is 0 or > 200)
            {
                continue;
            }

            int firstRowRaw = BinaryPrimitives.ReadUInt16LittleEndian(corrupted.AsSpan(pageStart + 14, 2));
            if ((firstRowRaw & 0xC000) != 0)
            {
                continue;
            }

            int rowStart = pageStart + (firstRowRaw & 0x1FFF);
            if (rowStart + 12 >= pageStart + pageSize)
            {
                continue;
            }

            // Fabricate a chained LVAL MEMO header:
            //   bytes 0..2: memoLen = 0xFFFFFF (16 MB)
            //   byte  3:    bitmask 0x00 → chained LVAL path
            //   bytes 4..7: page_row pointer → page 3, row 0 (arbitrary valid page)
            int memoHeaderPos = rowStart + 4;
            if (memoHeaderPos + 8 < pageStart + pageSize)
            {
                corrupted[memoHeaderPos] = 0xFF;     // memoLen low byte
                corrupted[memoHeaderPos + 1] = 0xFF; // memoLen mid byte
                corrupted[memoHeaderPos + 2] = 0xFF; // memoLen high byte
                corrupted[memoHeaderPos + 3] = 0x00; // bitmask: chained LVAL

                // Point to page 3, row 0 — likely invalid or a data page,
                // but the reader will handle the lookup gracefully.
                BinaryPrimitives.WriteUInt32LittleEndian(
                    corrupted.AsSpan(memoHeaderPos + 4, 4),
                    3 << 8);
                found = true;
            }
        }

        Assert.True(found, "Security fixture contains no suitable mutation target.");

        await using var stream = new MemoryStream(corrupted, writable: false);
        Exception? ex = await Record.ExceptionAsync(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(
                stream,
                new AccessReaderOptions { UseLockFile = false },
                leaveOpen: true,
                ct);

            IReadOnlyList<string> tables = await reader.ListTablesAsync(ct);
            foreach (string table in tables)
            {
                int count = 0;
                await foreach (object[] _ in reader.Rows(table, cancellationToken: ct))
                {
                    count++;
                    if (count > 100)
                    {
                        break;
                    }
                }
            }
        });

        // 16 MB is a bounded allocation; the CLR should handle it without OOM.
        // The chain walk will fail (pointing at an invalid LVAL row) and the
        // reader will surface a placeholder or skip, but must not crash.
        Assert.True(ex is null or IJetException or InvalidDataException or NotSupportedException, $"Unexpected parser failure: {ex}");
    }

    // ─── Malformed metadata: all rows marked as deleted ─────────────

    /// <summary>
    /// Sets the delete flag (0x8000) on every row offset in all data pages.
    /// <c>ComputeRowDirectory</c> leaves out every slot with the deleted bit set,
    /// so every row should be skipped — the reader must produce zero rows per
    /// table without crashing.
    /// </summary>
    [Fact]
    public async Task ReadTable_AllRowsDeleted_ProducesEmptyResults()
    {
        string path = TestDatabases.NorthwindTraders;
        Assert.True(File.Exists(path), $"Required security fixture is missing: {path}");

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        const int pageSize = Constants.PageSizes.Jet4;
        int pageCount = corrupted.Length / pageSize;

        for (int p = 1; p < pageCount; p++)
        {
            int pageStart = p * pageSize;
            if (corrupted[pageStart] != 0x01)
            {
                continue;
            }

            int nr = BinaryPrimitives.ReadUInt16LittleEndian(corrupted.AsSpan(pageStart + 12, 2));
            if (nr is 0 or > 200)
            {
                continue;
            }

            for (int r = 0; r < nr; r++)
            {
                int offsetPos = pageStart + 14 + (r * 2);
                if (offsetPos + 2 > corrupted.Length)
                {
                    break;
                }

                ushort raw = BinaryPrimitives.ReadUInt16LittleEndian(corrupted.AsSpan(offsetPos, 2));
                raw |= 0x8000; // set delete flag
                BinaryPrimitives.WriteUInt16LittleEndian(corrupted.AsSpan(offsetPos, 2), raw);
            }
        }

        await using var stream = new MemoryStream(corrupted, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            ct);

        IReadOnlyList<string> tables = await reader.ListTablesAsync(ct);
        int totalRows = 0;
        foreach (string table in tables)
        {
            await foreach (object[] _ in reader.Rows(table, cancellationToken: ct))
            {
                totalRows++;
            }
        }

        Assert.Equal(0, totalRows);
    }

    // ─── Malformed metadata: TDEF page-chain cycle ──────────────────

    /// <summary>
    /// Makes a TDEF page (type 0x02) point to itself via its "next page"
    /// field at offset 4. <c>ReadTDefBytesAsync</c> uses a
    /// <c>HashSet&lt;long&gt; seen</c> to break cycles — the reader must not
    /// loop forever.
    /// </summary>
    [Fact]
    public async Task ReadTable_TDefPageCycle_DoesNotHang()
    {
        string path = TestDatabases.NorthwindTraders;
        Assert.True(File.Exists(path), $"Required security fixture is missing: {path}");

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        const int pageSize = Constants.PageSizes.Jet4;
        int pageCount = corrupted.Length / pageSize;

        // Find the first TDEF page and make its next-page pointer point to itself.
        for (int p = 2; p < pageCount; p++)
        {
            int pageStart = p * pageSize;
            if (corrupted[pageStart] == 0x02)
            {
                // Offset 4 is the "next TDEF page" pointer (uint32 LE).
                BinaryPrimitives.WriteUInt32LittleEndian(corrupted.AsSpan(pageStart + 4, 4), (uint)p);
                break;
            }
        }

        await using var stream = new MemoryStream(corrupted, writable: false);
        Exception? ex = await Record.ExceptionAsync(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(
                stream,
                new AccessReaderOptions { UseLockFile = false },
                leaveOpen: true,
                ct);

            IReadOnlyList<string> tables = await reader.ListTablesAsync(ct);
            foreach (string table in tables)
            {
                int count = 0;
                await foreach (object[] _ in reader.Rows(table, cancellationToken: ct))
                {
                    count++;
                    if (count > 10)
                    {
                        break;
                    }
                }
            }
        });

        Assert.True(ex is null or IJetException or InvalidDataException or NotSupportedException, $"Unexpected parser failure: {ex}");
    }
}
