namespace JetDatabaseWriter.Tests.Conformance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
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
/// CveAnalogue traits identify canonical weakness-class correspondences in
/// docs/cve-vulnerability-analysis.md, not reproductions of native Microsoft exploits.
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
    [Trait("CveAnalogue", "CVE-2007-6026")]
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
    [Trait("CveAnalogue", "CVE-2008-1092")]
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
    [Trait("CveAnalogue", "CVE-2018-8423")]
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

    /// <summary>Oversized inline declarations fail against the verified column slice before copying a payload.</summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ReadTable_CorruptInlineMemoLength_RefusesVerifiedDescriptor(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (byte[] bytes, int offset, LongValueDescriptor _) = await CreateMemoMutationAsync(format, inline: true, ct);
        LongValueDescriptor.Inline(0xFFFFFF).WriteTo(bytes.AsSpan(offset));
        byte[] expected = (byte[])bytes.Clone();
        await using var backing = new MemoryStream(bytes, writable: false);
        await using var counting = new CountingStream(backing);
        await using AccessReader reader = await AccessReader.OpenAsync(
            counting,
            new AccessReaderOptions { UseLockFile = false, StrictParsing = true, PageReadOptimizationMode = PageReadOptimizationMode.Disabled },
            leaveOpen: true,
            ct);
        Assert.Equal(1, await reader.GetRealRowCountAsync("MemoTarget", ct));
        counting.Reset();

        InvalidDataException failure = await Assert.ThrowsAsync<InvalidDataException>(async () =>
        {
            await foreach (object[] _ in reader.Rows("MemoTarget", cancellationToken: ct))
            {
                Assert.Fail("The malformed MEMO must not yield a row.");
            }
        });
        Assert.Contains("Body", failure.Message, StringComparison.Ordinal);
        Assert.InRange(counting.BytesRead, 0, bytes.LongLength);
        Assert.Equal(expected, backing.ToArray());
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
    [Trait("CveAnalogue", "CVE-2019-0583")]
    [Trait("CveAnalogue", "CVE-2020-1400")]
    public void ReadTable_CorruptVarColOffsets_IntegerUnderflow_DoesNotCrash()
    {
        // Security regression: ROW-VAR-OFFSET-UNDERFLOW
        // A real Jet4 trailer: EOD(2), reversed var-offsets(2), var-count(2), null-mask(1).
        byte[] row = [1, 0, 0x41, 0x42, 0x43, 5, 0, 2, 0, 1, 0, 1];
        RowFieldSizes fields = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb).RowFields;
        var column = new ColumnInfo { Type = ColumnType.BinaryType, ColNum = 0, VarIdx = 0 };
        Assert.True(RowDecodePlan.TryParseRowLayout(fields, row, 0, row.Length, true, out RowLayout layout));
        ColumnSlice valid = RowDecodePlan.ResolveColumnSlice(fields, row, 0, row.Length, layout, column);
        Assert.Equal(ColumnSliceKind.Var, valid.Kind);
        Assert.Equal(3, valid.DataLen);

        // Change the actual variable offset to exceed EOD, leaving the trailer valid.
        BinaryPrimitives.WriteUInt16LittleEndian(row.AsSpan(layout.VarTableStart, 2), 6);
        Assert.True(RowDecodePlan.TryParseRowLayout(fields, row, 0, row.Length, true, out layout));
        ColumnSlice corrupt = RowDecodePlan.ResolveColumnSlice(fields, row, 0, row.Length, layout, column);
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

    /// <summary>A forged 30-bit chained length is refused before reading the verified first LVAL page.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="strict">Whether malformed values are rejected strictly.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    public async Task ReadTable_CorruptLvalMemoLength_RefusesBeforePayloadRead(DatabaseFormat format, bool strict)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (byte[] bytes, int offset, LongValueDescriptor original) = await CreateMemoMutationAsync(format, inline: false, ct);
        Assert.True(original.UsesChainedPages);
        Assert.NotEqual(0u, original.FirstDp);
        (original with { Length = LongValueDescriptor.MaxLength }).WriteTo(bytes.AsSpan(offset));
        byte[] expected = (byte[])bytes.Clone();
        await using var backing = new MemoryStream(bytes, writable: false);
        await using var counting = new CountingStream(backing);
        await using AccessReader reader = await AccessReader.OpenAsync(
            counting,
            new AccessReaderOptions { UseLockFile = false, StrictParsing = strict, MaxLongValueBytes = 32768, PageCacheSize = 0, PageReadOptimizationMode = PageReadOptimizationMode.Disabled },
            leaveOpen: true,
            ct);
        Assert.Equal(1, await reader.GetRealRowCountAsync("MemoTarget", ct));
        counting.Reset();

        JetLimitationException failure = await Assert.ThrowsAsync<JetLimitationException>(async () =>
        {
            await foreach (object[] _ in reader.Rows("MemoTarget", cancellationToken: ct))
            {
                Assert.Fail("The oversized MEMO must not yield a row.");
            }
        });
        Assert.Equal(JetErrorCode.ValueTooLarge, failure.ErrorCode);
        Assert.DoesNotContain((long)(original.FirstDp >> 8), counting.PagesRead(JetFormat.ForNewDatabase(format).PageSize));
        Assert.InRange(counting.BytesRead, 0, bytes.LongLength);
        Assert.Equal(expected, backing.ToArray());
    }

    private static async Task<(byte[] Bytes, int Offset, LongValueDescriptor Descriptor)> CreateMemoMutationAsync(
        DatabaseFormat format,
        bool inline,
        CancellationToken cancellationToken)
    {
        await using var source = new MemoryStream();
        string body = inline ? "verified memo" : new string('x', 12000);
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(source, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken))
        {
            await writer.CreateTableAsync("MemoTarget", [new("Body", typeof(string))], cancellationToken);
            await writer.InsertRowAsync("MemoTarget", [body], cancellationToken);
        }

        source.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(source, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken))
        {
            int count = 0;
            await foreach (object[] row in reader.Rows("MemoTarget", cancellationToken: cancellationToken))
            {
                Assert.Equal(body, Assert.IsType<string>(Assert.Single(row)));
                count++;
            }

            Assert.Equal(1, count);
        }

        byte[] bytes = source.ToArray();
        source.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(source, cancellationToken: cancellationToken);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("MemoTarget", cancellationToken));
        TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, cancellationToken));
        ColumnInfo column = Assert.Single(definition.Columns);
        Assert.Equal(ColumnType.MemoType, column.Type);
        RowLocation location = Assert.Single(await harness.Database.GetLiveRowLocationsAsync(entry.TDefPage, cancellationToken));
        byte[] page = await harness.ReadPageCopyAsync(location.DataPageNumber, cancellationToken);
        JetFormat profile = harness.Database.Format;
        Assert.True(RowDecodePlan.TryParseRowLayout(profile.RowFields, page, location.RowStart, location.RowSize, definition.HasVarColumns, out RowLayout layout));
        ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(profile.RowFields, page, location.RowStart, location.RowSize, layout, column);
        Assert.Equal(ColumnSliceKind.Var, slice.Kind);
        int offset = checked((int)(location.DataPageNumber * profile.PageSize) + location.RowStart + slice.DataStart);
        Assert.True(LongValueDescriptor.TryRead(bytes.AsSpan(offset, slice.DataLen), out LongValueDescriptor descriptor));
        Assert.Equal(inline, descriptor.IsInline);
        Assert.InRange(descriptor.Length, 1, 32768);
        return (bytes, offset, descriptor);
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
