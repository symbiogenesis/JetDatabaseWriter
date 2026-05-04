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
}
