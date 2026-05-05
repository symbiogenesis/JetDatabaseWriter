namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Validates the reader's behaviour when an OLE long value's LVAL header
/// declares a length that does not match the actual data on disk. This
/// tests resilience against corrupt or partially-written databases where
/// the 24-bit <c>memoLen</c> field has drifted out of sync with the LVAL
/// page content. Closes §2.1 gap: "OLE long values whose header reports a
/// mismatched length vs the actual chain.".
/// </summary>
public sealed class LvalLengthMismatchTests
{
    /// <summary>Known payload size that produces single-page LVAL form (0x40).</summary>
    private const int PayloadSize = 2000;

    /// <summary>
    /// When the LVAL header declares a length smaller than the actual data on
    /// the LVAL page, the reader should return data truncated to the header
    /// length (it trusts the header as an upper bound).
    /// </summary>
    [Fact]
    public async Task Ole_HeaderLengthSmallerThanActual_ReturnsTruncatedData()
    {
        byte[] payload = BuildPayload(PayloadSize);
        byte[] dbBytes = await WriteDatabaseWithOleAsync(payload);

        // Find the LVAL header in the raw bytes: pattern is [len_low, len_mid, len_high, 0x40]
        // For 2000 bytes: 2000 = 0x0007D0 → [0xD0, 0x07, 0x00, 0x40]
        int headerOffset = FindLvalHeader(dbBytes, PayloadSize, 0x40);
        Assert.True(headerOffset >= 0, "Could not locate LVAL header in database bytes.");

        // Corrupt: set header length to 500 (smaller than actual 2000)
        const int corruptedLen = 500;
        WriteLvalLength(dbBytes, headerOffset, corruptedLen);

        // Read back — should return 500 bytes (truncated to header-declared length)
        byte[] result = await ReadOleBlobAsync(dbBytes);
        Assert.Equal(corruptedLen, result.Length);

        // The returned bytes should match the first 500 bytes of the original payload
        Assert.Equal(payload.AsSpan(0, corruptedLen).ToArray(), result);
    }

    /// <summary>
    /// When the LVAL header declares a length larger than the actual data on
    /// the LVAL page, the reader should return only the available data (the
    /// page size caps the read) without throwing.
    /// </summary>
    [Fact]
    public async Task Ole_HeaderLengthLargerThanActual_ReturnsAvailableDataWithoutCrash()
    {
        byte[] payload = BuildPayload(PayloadSize);
        byte[] dbBytes = await WriteDatabaseWithOleAsync(payload);

        int headerOffset = FindLvalHeader(dbBytes, PayloadSize, 0x40);
        Assert.True(headerOffset >= 0, "Could not locate LVAL header in database bytes.");

        // Corrupt: set header length to 8000 (larger than the ~2000 bytes actually on the LVAL page)
        const int corruptedLen = 8000;
        WriteLvalLength(dbBytes, headerOffset, corruptedLen);

        // Read back — should not throw; returns available data (capped by page content)
        byte[] result = await ReadOleBlobAsync(dbBytes);

        // The reader uses Math.Min(oleLoc.Size, memoLen) so it returns actual page data
        Assert.True(result.Length <= PayloadSize + 16, $"Expected ≤ {PayloadSize + 16} bytes, got {result.Length}.");
        Assert.True(result.Length > 0, "Expected non-empty result.");
    }

    /// <summary>
    /// When the LVAL header declares a zero length, the reader should return
    /// an empty array without crashing.
    /// </summary>
    [Fact]
    public async Task Ole_HeaderLengthZero_ReturnsEmptyArray()
    {
        byte[] payload = BuildPayload(PayloadSize);
        byte[] dbBytes = await WriteDatabaseWithOleAsync(payload);

        int headerOffset = FindLvalHeader(dbBytes, PayloadSize, 0x40);
        Assert.True(headerOffset >= 0, "Could not locate LVAL header in database bytes.");

        // Corrupt: set header length to 0
        WriteLvalLength(dbBytes, headerOffset, 0);

        byte[] result = await ReadOleBlobAsync(dbBytes);
        Assert.Empty(result);
    }

    // ════════════════════════════════════════════════════════════════
    // Helpers
    // ════════════════════════════════════════════════════════════════

    private static async Task<byte[]> WriteDatabaseWithOleAsync(byte[] payload)
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "OleTest",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Blob", typeof(byte[])),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync("OleTest", [1, payload], TestContext.Current.CancellationToken);
        }

        return ms.ToArray();
    }

    private static async Task<byte[]> ReadOleBlobAsync(byte[] dbBytes)
    {
        using var ms = new MemoryStream(dbBytes, writable: false);
        await using var reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        DataTable dt = await reader.ReadDataTableAsync(
            "OleTest",
            cancellationToken: TestContext.Current.CancellationToken);

        if (dt.Rows.Count == 0)
        {
            return [];
        }

        object val = dt.Rows[0]["Blob"];
        return val is byte[] bytes ? bytes : [];
    }

    /// <summary>
    /// Searches the raw database bytes for the 4-byte LVAL header pattern:
    /// [len_low, len_mid, len_high, formBitmask]. Returns the offset of byte 0
    /// of the header, or -1 if not found.
    /// </summary>
    private static int FindLvalHeader(byte[] data, int expectedLen, byte formBitmask)
    {
        byte b0 = unchecked((byte)(expectedLen & 0xFF));
        byte b1 = unchecked((byte)((expectedLen >> 8) & 0xFF));
        byte b2 = unchecked((byte)((expectedLen >> 16) & 0xFF));

        for (int i = 0; i < data.Length - 4; i++)
        {
            if (data[i] == b0 && data[i + 1] == b1 && data[i + 2] == b2 && data[i + 3] == formBitmask)
            {
                // Sanity: bytes 4-7 should be a non-zero LVAL page pointer
                if (i + 7 < data.Length)
                {
                    uint lvalDp = BitConverter.ToUInt32(data, i + 4);
                    if (lvalDp != 0)
                    {
                        return i;
                    }
                }
            }
        }

        return -1;
    }

    private static void WriteLvalLength(byte[] data, int headerOffset, int newLen)
    {
        data[headerOffset] = unchecked((byte)(newLen & 0xFF));
        data[headerOffset + 1] = unchecked((byte)((newLen >> 8) & 0xFF));
        data[headerOffset + 2] = unchecked((byte)((newLen >> 16) & 0xFF));
    }

    private static byte[] BuildPayload(int length)
    {
        var bytes = new byte[length];
        for (int i = 0; i < length; i++)
        {
            unchecked
            {
                bytes[i] = (byte)((i * 31) ^ (i >> 8));
            }
        }

        return bytes;
    }
}
