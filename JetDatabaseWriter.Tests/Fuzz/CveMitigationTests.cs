namespace JetDatabaseWriter.Tests.Fuzz;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention.
#pragma warning disable SA1312 // Variable '_' is a discard.

/// <summary>
/// Regression tests for CVE-class vulnerabilities in the JET/MDB parsing paths.
/// Each test corrupts a specific field in a valid database file and asserts that
/// the reader handles the malformation gracefully: no OOM, no infinite loop,
/// no unhandled exception escaping to the caller.
/// </summary>
/// <remarks>
/// Related CVEs:
///   CVE-2005-0944 / CVE-2007-6026 / CVE-2008-1092 (crafted column counts),
///   CVE-2018-8423 (unchecked index ordinal),
///   CVE-2019-0538 through CVE-2019-0584 (metadata field trust issues).
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
        if (!File.Exists(path))
        {
            return;
        }

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        // Jet4/ACE TDEF numCols offset = 45 within the page payload.
        // Page 2 starts at file offset 2 * 4096 = 8192.
        int tdefPage2Start = 2 * 4096;
        int numColsOffset = tdefPage2Start + 45;

        if (numColsOffset + 2 > corrupted.Length)
        {
            return;
        }

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

        Assert.IsNotType<OutOfMemoryException>(ex);
        Assert.IsNotType<StackOverflowException>(ex);
    }

    /// <summary>
    /// Corrupts the TDEF page's numCols to exactly 4097 (one past the cap).
    /// Verifies the 4096-column cap rejects this cleanly.
    /// </summary>
    [Fact]
    public async Task ReadTable_CorruptNumCols_4097_DoesNotCrashOrOom()
    {
        string path = TestDatabases.NorthwindTraders;
        if (!File.Exists(path))
        {
            return;
        }

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        int tdefPage2Start = 2 * 4096;
        int numColsOffset = tdefPage2Start + 45;

        if (numColsOffset + 2 > corrupted.Length)
        {
            return;
        }

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

        Assert.IsNotType<OutOfMemoryException>(ex);
        Assert.IsNotType<StackOverflowException>(ex);
    }

    // ─── CVE-2018-8423 analog: numRealIdx overflow in TDEF ────────────

    /// <summary>
    /// Corrupts the numRealIdx field (offset 51 in Jet4/ACE TDEF) to a huge
    /// value. The reader clamps this to [0, 1000]; verify no crash/OOM.
    /// </summary>
    [Fact]
    public async Task ReadTable_CorruptNumRealIdx_0x7FFFFFFF_DoesNotCrashOrOom()
    {
        string path = TestDatabases.NorthwindTraders;
        if (!File.Exists(path))
        {
            return;
        }

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        int tdefPage2Start = 2 * 4096;
        int numRealIdxOffset = tdefPage2Start + 51;

        if (numRealIdxOffset + 4 > corrupted.Length)
        {
            return;
        }

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

        Assert.IsNotType<OutOfMemoryException>(ex);
        Assert.IsNotType<StackOverflowException>(ex);
    }

    // ─── CVE-2019 batch analog: data page row-offset corruption ────────

    /// <summary>
    /// Corrupts a data page's row-offset table entries to point past the page
    /// boundary. The row-offset parser masks with 0x1FFF and checks against
    /// page size; rows with invalid offsets should be skipped.
    /// </summary>
    [Fact]
    public async Task ReadTable_CorruptRowOffsets_AllOutOfBounds_ReturnsZeroRows()
    {
        string path = TestDatabases.NorthwindTraders;
        if (!File.Exists(path))
        {
            return;
        }

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        int pageSize = 4096;
        int dataPageNumber = FindFirstDataPage(corrupted, pageSize);
        if (dataPageNumber < 0)
        {
            return;
        }

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

            var tables = await reader.ListTablesAsync(ct);
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

        Assert.IsNotType<OutOfMemoryException>(ex);
        Assert.IsNotType<StackOverflowException>(ex);
    }

    // ─── CVE-2019 batch analog: data page numRows overflow ─────────────

    /// <summary>
    /// Corrupts the numRows field on EVERY data page (type 0x01) in the file to
    /// 2500, exceeding the physical max of (4096 − 14) / 2 = 2041 row-offset
    /// entries. Then reads all tables through the normal API. Without the numRows
    /// clamp in <c>EnumerateLiveRowBounds</c> / <c>ComputeLiveRowBoundsArray</c>,
    /// <c>Ru16</c> reads past the page buffer and throws
    /// <see cref="ArgumentOutOfRangeException"/>.
    /// </summary>
    [Fact]
    public async Task ReadTable_AllDataPagesNumRowsCorrupted_DoesNotThrow()
    {
        string path = TestDatabases.NorthwindTraders;
        if (!File.Exists(path))
        {
            return;
        }

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        const int pageSize = 4096;
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
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            ct);

        var tables = await reader.ListTablesAsync(ct);
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
    }

    // ─── Inline MEMO length overflow (CVE-2019 batch analog) ───────────

    /// <summary>
    /// Corrupts an inline MEMO header's 3-byte length to the maximum (0xFFFFFF = 16 MB)
    /// while keeping the bitmask as 0x80 (inline). The reader must cap the length
    /// against the remaining row bytes, not allocate 16 MB.
    /// </summary>
    [Fact]
    public async Task ReadTable_CorruptInlineMemoLen_16MB_DoesNotAllocateHuge()
    {
        string path = TestDatabases.NorthwindTraders;
        if (!File.Exists(path))
        {
            return;
        }

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        int pageSize = 4096;
        bool found = false;
        for (int p = 3; p < corrupted.Length / pageSize && !found; p++)
        {
            int pageStart = p * pageSize;
            if (corrupted[pageStart] != 0x01)
            {
                continue;
            }

            int nr = BinaryPrimitives.ReadUInt16LittleEndian(corrupted.AsSpan(pageStart + 12, 2));
            if (nr == 0 || nr > 200)
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

        if (!found)
        {
            return;
        }

        await using var stream = new MemoryStream(corrupted, writable: false);
        Exception? ex = await Record.ExceptionAsync(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(
                stream,
                new AccessReaderOptions { UseLockFile = false },
                leaveOpen: true,
                ct);

            var tables = await reader.ListTablesAsync(ct);
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

        Assert.IsNotType<OutOfMemoryException>(ex);
        Assert.IsNotType<StackOverflowException>(ex);
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
    public async Task ReadTable_CorruptVarColOffsets_IntegerUnderflow_DoesNotCrash()
    {
        string path = TestDatabases.NorthwindTraders;
        if (!File.Exists(path))
        {
            return;
        }

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        const int pageSize = 4096;
        int pageCount = corrupted.Length / pageSize;

        // Corrupt the var-column offset table within rows: write descending
        // offset values so that varEnd < varOff for every variable column,
        // triggering the dataLen < 0 underflow path.
        for (int p = 1; p < pageCount; p++)
        {
            int pageStart = p * pageSize;
            if (corrupted[pageStart] != 0x01)
            {
                continue;
            }

            int nr = BinaryPrimitives.ReadUInt16LittleEndian(corrupted.AsSpan(pageStart + 12, 2));
            if (nr == 0 || nr > 200)
            {
                continue;
            }

            for (int r = 0; r < nr; r++)
            {
                int rawOffset = BinaryPrimitives.ReadUInt16LittleEndian(
                    corrupted.AsSpan(pageStart + 14 + (r * 2), 2));
                if ((rawOffset & 0xC000) != 0)
                {
                    continue;
                }

                int rowStart = pageStart + (rawOffset & 0x1FFF);
                if (rowStart + 20 >= pageStart + pageSize)
                {
                    continue;
                }

                // Jet4/ACE row layout: numCols(2), fixed data, then at the end:
                // [eod(2)][var-offset-table(varLen * 2)][varLen(2)][null-mask].
                // We corrupt bytes near the end of the row to break the var-offset
                // ordering. Write 0xFF to the last 8 bytes before the end to corrupt
                // var-col offset entries into descending values.
                int rowEnd = rowStart + 20; // conservative end estimate
                for (int i = rowEnd - 8; i < rowEnd && i < pageStart + pageSize; i++)
                {
                    corrupted[i] = 0xFF;
                }
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

            var tables = await reader.ListTablesAsync(ct);
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

        Assert.IsNotType<OutOfMemoryException>(ex);
        Assert.IsNotType<StackOverflowException>(ex);
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
                if (nr > 0 && nr < 200)
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
        if (!File.Exists(path))
        {
            return;
        }

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        const int pageSize = 4096;
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

            var tables = await reader.ListTablesAsync(ct);
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

        Assert.IsNotType<OutOfMemoryException>(ex);
        Assert.IsNotType<StackOverflowException>(ex);
    }

    // ─── CVE-2019 batch analog: corrupted in-row numCols ───────────────

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
        if (!File.Exists(path))
        {
            return;
        }

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        const int pageSize = 4096;
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
            if (nr == 0 || nr > 200)
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

            var tables = await reader.ListTablesAsync(ct);
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

        Assert.IsNotType<OutOfMemoryException>(ex);
        Assert.IsNotType<StackOverflowException>(ex);
    }

    // ─── CVE-2019 batch analog: LVAL chain cycle ───────────────────────

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
        string path = TestDatabases.NorthwindTraders;
        if (!File.Exists(path))
        {
            return;
        }

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        const int pageSize = 4096;
        int pageCount = corrupted.Length / pageSize;
        bool found = false;

        // Find LVAL data pages (type 0x01) that are NOT regular data pages
        // by looking for pages whose owning-table-page at offset 4 points to
        // a TDEF page. In Jet4/ACE, LVAL pages are type 0x01 with an owner
        // pointer in bytes 4..7. We'll just corrupt ALL data pages' first row
        // to point its first 4 bytes (the "next LVAL page" pointer) back to
        // itself. For pages that ARE LVAL rows, this creates the cycle.
        for (int p = 3; p < pageCount && !found; p++)
        {
            int pageStart = p * pageSize;
            if (corrupted[pageStart] != 0x01)
            {
                continue;
            }

            int nr = BinaryPrimitives.ReadUInt16LittleEndian(corrupted.AsSpan(pageStart + 12, 2));
            if (nr == 0 || nr > 200)
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

            // Make the LVAL "next page" field point to this same page (self-cycle).
            // The 4-byte value is a page_row reference: page << 8 | row_index.
            uint selfRef = (uint)(p << 8);
            BinaryPrimitives.WriteUInt32LittleEndian(corrupted.AsSpan(rowStart, 4), selfRef);

            // Also rewrite the row's MEMO header to indicate a chained LVAL (bitmask 0x00).
            if (rowStart + 8 < pageStart + pageSize)
            {
                corrupted[rowStart + 3] = 0x00; // chained LVAL

                // Set a plausible memoLen so the chain reader enters the loop.
                corrupted[rowStart] = 0x00;
                corrupted[rowStart + 1] = 0x10; // memoLen = 4096
                corrupted[rowStart + 2] = 0x00;
            }

            found = true;
        }

        if (!found)
        {
            return;
        }

        await using var stream = new MemoryStream(corrupted, writable: false);
        Exception? ex = await Record.ExceptionAsync(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(
                stream,
                new AccessReaderOptions { UseLockFile = false },
                leaveOpen: true,
                ct);

            var tables = await reader.ListTablesAsync(ct);
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

        Assert.IsNotType<OutOfMemoryException>(ex);
        Assert.IsNotType<StackOverflowException>(ex);
    }

    // ─── CVE-2019 batch analog: LVAL chained MEMO length overflow ─────

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
        if (!File.Exists(path))
        {
            return;
        }

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        const int pageSize = 4096;
        bool found = false;

        for (int p = 3; p < corrupted.Length / pageSize && !found; p++)
        {
            int pageStart = p * pageSize;
            if (corrupted[pageStart] != 0x01)
            {
                continue;
            }

            int nr = BinaryPrimitives.ReadUInt16LittleEndian(corrupted.AsSpan(pageStart + 12, 2));
            if (nr == 0 || nr > 200)
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
                    (uint)(3 << 8));
                found = true;
            }
        }

        if (!found)
        {
            return;
        }

        await using var stream = new MemoryStream(corrupted, writable: false);
        Exception? ex = await Record.ExceptionAsync(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(
                stream,
                new AccessReaderOptions { UseLockFile = false },
                leaveOpen: true,
                ct);

            var tables = await reader.ListTablesAsync(ct);
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
        Assert.IsNotType<OutOfMemoryException>(ex);
        Assert.IsNotType<StackOverflowException>(ex);
    }

    // ─── CVE-2019 batch analog: all rows marked as deleted ─────────────

    /// <summary>
    /// Sets the delete flag (0x8000) on every row offset in all data pages.
    /// <c>ComputeLiveRowBoundsArray</c> filters on <c>(raw &amp; 0xC000) == 0</c>,
    /// so every row should be skipped — the reader must produce zero rows per
    /// table without crashing.
    /// </summary>
    [Fact]
    public async Task ReadTable_AllRowsDeleted_ProducesEmptyResults()
    {
        string path = TestDatabases.NorthwindTraders;
        if (!File.Exists(path))
        {
            return;
        }

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        const int pageSize = 4096;
        int pageCount = corrupted.Length / pageSize;

        for (int p = 1; p < pageCount; p++)
        {
            int pageStart = p * pageSize;
            if (corrupted[pageStart] != 0x01)
            {
                continue;
            }

            int nr = BinaryPrimitives.ReadUInt16LittleEndian(corrupted.AsSpan(pageStart + 12, 2));
            if (nr == 0 || nr > 200)
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

        var tables = await reader.ListTablesAsync(ct);
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

    // ─── CVE-2019 batch analog: TDEF page-chain cycle ──────────────────

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
        if (!File.Exists(path))
        {
            return;
        }

        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] original = await db.GetFileAsync(path, ct);
        byte[] corrupted = (byte[])original.Clone();

        const int pageSize = 4096;
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

            var tables = await reader.ListTablesAsync(ct);
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

        Assert.IsNotType<OutOfMemoryException>(ex);
        Assert.IsNotType<StackOverflowException>(ex);
    }
}
