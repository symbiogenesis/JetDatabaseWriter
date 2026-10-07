namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Table-scan read-ahead keeps a second page read in flight while the caller
/// decodes the current page. It must return the same rows as a sequential
/// scan at every page-cache size, including caches smaller than the two pages
/// in flight and no cache at all, on AES-encrypted files, where the two
/// reads can decrypt pages on two threads at once, and at the default cache
/// size with a long value longer than the cache.
/// </summary>
public sealed class TableScanReadAheadTests : IDisposable
{
    private const string PlainTable = "Plain";
    private const string LongValueTable = "LongValues";
    private const string Password = "read-ahead";
    private const string LargeMemoTable = "LargeMemo";
    private const int PlainRowCount = 400;
    private const int LongValueRowCount = 120;
    private const int LargeMemoRowCount = 30;

    /// <summary>
    /// 1,500,000 characters: at least 370 4 KB LVAL pages even with Unicode
    /// compression, and about 750 2 KB pages on Jet3.
    /// </summary>
    private const int LargeMemoLength = 1_500_000;

    private static readonly string LargeMemo = MakeLargeMemo();

    private readonly List<string> paths = [];

    public static TheoryData<DatabaseFormat, int, PageReadOptimizationMode> PlainScanCases()
    {
        var data = new TheoryData<DatabaseFormat, int, PageReadOptimizationMode>();
        foreach (DatabaseFormat format in (DatabaseFormat[])[DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb])
        {
            foreach (int cacheSize in (int[])[0, 1, 2, 8])
            {
                data.Add(format, cacheSize, PageReadOptimizationMode.Auto);
                data.Add(format, cacheSize, PageReadOptimizationMode.Enabled);
            }
        }

        return data;
    }

    public static TheoryData<DatabaseFormat, int> LongValueScanCases()
    {
        var data = new TheoryData<DatabaseFormat, int>();
        foreach (DatabaseFormat format in (DatabaseFormat[])[DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb])
        {
            foreach (int cacheSize in (int[])[1, 2, 3, 8])
            {
                data.Add(format, cacheSize);
            }
        }

        return data;
    }

    public static TheoryData<DatabaseFormat, PageReadOptimizationMode> PlainFileConcurrencyCases()
    {
        var data = new TheoryData<DatabaseFormat, PageReadOptimizationMode>();
        foreach (DatabaseFormat format in (DatabaseFormat[])[DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb])
        {
            data.Add(format, PageReadOptimizationMode.Auto);
            data.Add(format, PageReadOptimizationMode.Disabled);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(PlainScanCases))]
    public async Task Scans_TinyOrNoPageCache_ReadAheadAndReturnEveryRow(DatabaseFormat format, int cacheSize, PageReadOptimizationMode mode)
    {
        string path = await this.CreateDatabaseAsync(format, withLongValues: false, encrypt: false);
        ScanResult expected = await ReadBaselineAsync(path, PlainTable, password: null);
        var options = new AccessReaderOptions { PageCacheSize = cacheSize, PageReadOptimizationMode = mode, UseLockFile = false };

        Assert.True(await ReadsAheadAsync(path, options, PlainTable));
        await using AccessReader reader = await AccessReader.OpenAsync(path, options, TestContext.Current.CancellationToken);
        await AssertScansMatchAsync(reader, PlainTable, expected);
    }

    [Theory]
    [MemberData(nameof(LongValueScanCases))]
    public async Task Scans_LongValueTableLargerThanCache_ReturnEveryRow(DatabaseFormat format, int cacheSize)
    {
        string path = await this.CreateDatabaseAsync(format, withLongValues: true, encrypt: false);
        ScanResult expected = await ReadBaselineAsync(path, LongValueTable, password: null);
        var options = new AccessReaderOptions { PageCacheSize = cacheSize, PageReadOptimizationMode = PageReadOptimizationMode.Enabled, UseLockFile = false };

        // Long-value tables stay sequential because read-ahead measured no gain on them.
        Assert.False(await ReadsAheadAsync(path, options, LongValueTable));
        await using AccessReader reader = await AccessReader.OpenAsync(path, options, TestContext.Current.CancellationToken);
        await AssertScansMatchAsync(reader, LongValueTable, expected);
    }

    /// <summary>
    /// A MEMO chain longer than the default 256-page cache evicts the data page a
    /// scan is still on while the value is decoded. When the cache handed evicted
    /// pages back to the shared pool (bug 1), the next page read overwrote that data
    /// page and every row after the MEMO was lost. The other tests here use small
    /// caches and short chains; this one keeps the default options.
    /// </summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Scans_LongValueChainLongerThanDefaultCache_ReturnEveryRow(DatabaseFormat format)
    {
        string path = await this.CreateLargeMemoDatabaseAsync(format);
        var options = new AccessReaderOptions { UseLockFile = false };
        int lvalPages = CountLvalPages(path, format);
        Assert.True(lvalPages > options.PageCacheSize, $"The MEMO spans {lvalPages} LVAL pages, no more than the {options.PageCacheSize}-page default cache.");

        await using AccessReader reader = await AccessReader.OpenAsync(path, options, TestContext.Current.CancellationToken);
        List<string> expected = [.. Enumerable.Range(1, LargeMemoRowCount).Select(id => LargeMemoSignature(id, LargeMemoBody(id)))];

        var rows = new List<string>();
        await foreach (object[] row in reader.Rows(LargeMemoTable, cancellationToken: TestContext.Current.CancellationToken))
        {
            rows.Add(LargeMemoSignature((int)row[0], row[1] as string));
        }

        Assert.Equal(expected, rows);

        var typed = new List<string>();
        await foreach (LargeMemoRow row in reader.Rows<LargeMemoRow>(LargeMemoTable, cancellationToken: TestContext.Current.CancellationToken))
        {
            typed.Add(LargeMemoSignature(row.Id, row.Body));
        }

        Assert.Equal(expected, typed);

        var strings = new List<string>();
        await foreach (string[] row in reader.RowsAsStrings(LargeMemoTable, cancellationToken: TestContext.Current.CancellationToken))
        {
            strings.Add(LargeMemoSignature(int.Parse(row[0], CultureInfo.InvariantCulture), row[1]));
        }

        Assert.Equal(expected, strings);
        Assert.Equal(LargeMemoRowCount, await reader.GetRealRowCountAsync(LargeMemoTable, TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(8)]
    public async Task Scans_AesEncryptedFile_ReturnEveryRow(int cacheSize)
    {
        string path = await this.CreateDatabaseAsync(DatabaseFormat.AceAccdb, withLongValues: true, encrypt: true);
        ScanResult plain = await ReadBaselineAsync(path, PlainTable, Password);
        ScanResult longValues = await ReadBaselineAsync(path, LongValueTable, Password);
        var options = new AccessReaderOptions(Password) { PageCacheSize = cacheSize, UseLockFile = false };

        Assert.True(await ReadsAheadAsync(path, options, PlainTable));
        await using AccessReader reader = await AccessReader.OpenAsync(path, options, TestContext.Current.CancellationToken);
        await AssertScansMatchAsync(reader, PlainTable, plain);
        await AssertScansMatchAsync(reader, LongValueTable, longValues);
    }

    [Fact]
    public async Task Rows_ConcurrentScansOfAesEncryptedFile_ReturnEveryRow()
    {
        string path = await this.CreateDatabaseAsync(DatabaseFormat.AceAccdb, withLongValues: true, encrypt: true);
        ScanResult plain = await ReadBaselineAsync(path, PlainTable, Password);
        ScanResult longValues = await ReadBaselineAsync(path, LongValueTable, Password);

        // A fresh reader builds its AES transforms on the first page read, so
        // the first round also races that build.
        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions(Password) { PageCacheSize = 2, UseLockFile = false },
            TestContext.Current.CancellationToken);

        for (int round = 0; round < 4; round++)
        {
            Task<List<string>>[] scans = [.. Enumerable.Range(0, 8).Select(i =>
                Task.Run(() => ReadRowSignaturesAsync(reader, i % 2 == 0 ? PlainTable : LongValueTable)))];
            List<string>[] results = await Task.WhenAll(scans);
            for (int i = 0; i < results.Length; i++)
            {
                Assert.Equal(i % 2 == 0 ? plain.Rows : longValues.Rows, results[i]);
            }
        }
    }

    [Fact]
    public async Task ReadPageAsync_ConcurrentReads_ReturnEveryPage()
    {
        string path = await this.CreateDatabaseAsync(DatabaseFormat.AceAccdb, withLongValues: true, encrypt: false);
        var options = new AccessReaderOptions() { PageCacheSize = 0, UseLockFile = false };

        byte[][] expected;
        await using (ReaderHarness sequential = await ReaderHarness.OpenAsync(path, options, TestContext.Current.CancellationToken))
        {
            DatabaseFile sequentialDb = sequential.Database;
            expected = new byte[checked((int)sequentialDb.Pages.PageCount)][];
            for (int page = 1; page < expected.Length; page++)
            {
                expected[page] = await sequentialDb.Pages.ReadPageCopyAsync(page, TestContext.Current.CancellationToken);
            }
        }

        // A fresh reader starts without cached pages.
        await using ReaderHarness reader = await ReaderHarness.OpenAsync(path, options, TestContext.Current.CancellationToken);
        DatabaseFile db = reader.Database;
        for (int round = 0; round < 8; round++)
        {
            Task<byte[]>[] reads = [.. Enumerable.Range(1, expected.Length - 1).Select(page =>
                Task.Run(async () => await db.Pages.ReadPageCopyAsync(page, TestContext.Current.CancellationToken)))];
            byte[][] actual = await Task.WhenAll(reads);
            for (int page = 1; page < expected.Length; page++)
            {
                Assert.True(expected[page].AsSpan().SequenceEqual(actual[page - 1]), $"Page {page} read differently in round {round}.");
            }
        }
    }

    /// <summary>
    /// Eight scans on one path-opened reader at once, so page reads from many
    /// pool threads share the reader's file handle: positional reads in
    /// <c>Auto</c>, seek-and-read under the I/O gate in <c>Disabled</c>. The
    /// long-value table adds LVAL page reads on Jet4 and ACCDB; its CJK text
    /// does not fit Jet3's code page, so Jet3 scans the plain table only.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The page-read optimization mode.</param>
    [Theory]
    [MemberData(nameof(PlainFileConcurrencyCases))]
    public async Task Rows_ConcurrentScansOfPlainFile_ReturnEveryRow(DatabaseFormat format, PageReadOptimizationMode mode)
    {
        bool withLongValues = format != DatabaseFormat.Jet3Mdb;
        string path = await this.CreateDatabaseAsync(format, withLongValues, encrypt: false);
        ScanResult plain = await ReadBaselineAsync(path, PlainTable, password: null);
        ScanResult longValues = withLongValues ? await ReadBaselineAsync(path, LongValueTable, password: null) : plain;

        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions { PageCacheSize = 2, PageReadOptimizationMode = mode, UseLockFile = false },
            TestContext.Current.CancellationToken);

        for (int round = 0; round < 4; round++)
        {
            Task<List<string>>[] scans = [.. Enumerable.Range(0, 8).Select(i =>
                Task.Run(() => ReadRowSignaturesAsync(reader, ConcurrentScanTable(i, withLongValues))))];
            List<string>[] results = await Task.WhenAll(scans);
            for (int i = 0; i < results.Length; i++)
            {
                Assert.Equal(ConcurrentScanTable(i, withLongValues) == LongValueTable ? longValues.Rows : plain.Rows, results[i]);
            }
        }
    }

    /// <summary>
    /// Every page of an unencrypted file read at once from pool threads on one
    /// path-opened reader with no page cache matches the file's bytes.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The page-read optimization mode.</param>
    [Theory]
    [MemberData(nameof(PlainFileConcurrencyCases))]
    public async Task ReadPageAsync_ConcurrentReadsOfPlainFile_ReturnEveryPage(DatabaseFormat format, PageReadOptimizationMode mode)
    {
        string path = await this.CreateDatabaseAsync(format, withLongValues: format != DatabaseFormat.Jet3Mdb, encrypt: false);
        byte[] file = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);

        // The harness opens the path as AccessReader.OpenAsync does, so Auto
        // reads positionally and Disabled seeks and reads under the gate.
        await using ReaderHarness reader = await ReaderHarness.OpenAsync(
            path,
            new AccessReaderOptions { PageCacheSize = 0, PageReadOptimizationMode = mode, UseLockFile = false },
            TestContext.Current.CancellationToken);
        DatabaseFile db = reader.Database;
        Assert.Equal(mode != PageReadOptimizationMode.Disabled && !LibraryTarget.IsNetStandard, db.Pages.UsesRandomAccessPageReads);
        int pageSize = db.Format.PageSize;
        int pageCount = checked((int)db.Pages.PageCount);
        Assert.Equal(file.Length, pageCount * pageSize);

        for (int round = 0; round < 4; round++)
        {
            Task<byte[]>[] reads = [.. Enumerable.Range(1, pageCount - 1).Select(page =>
                Task.Run(async () => await db.Pages.ReadPageCopyAsync(page, TestContext.Current.CancellationToken)))];
            byte[][] actual = await Task.WhenAll(reads);
            for (int page = 1; page < pageCount; page++)
            {
                Assert.True(file.AsSpan(page * pageSize, pageSize).SequenceEqual(actual[page - 1]), $"Page {page} read differently in round {round}.");
            }
        }
    }

    public void Dispose()
    {
        foreach (string path in this.paths)
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
                // Best-effort cleanup.
            }
            catch (UnauthorizedAccessException)
            {
                // Best-effort cleanup.
            }
        }
    }

    private static string ConcurrentScanTable(int scan, bool withLongValues) =>
        withLongValues && scan % 2 == 1 ? LongValueTable : PlainTable;

    /// <summary>
    /// Returns whether a reader opened on <paramref name="path"/> with
    /// <paramref name="options"/> reads <paramref name="tableName"/> ahead,
    /// asking the table reader of a <see cref="ReaderHarness"/>, which opens
    /// the path as <see cref="AccessReader.OpenAsync(string, AccessReaderOptions?, System.Threading.CancellationToken)"/> does.
    /// </summary>
    /// <param name="path">The database file.</param>
    /// <param name="options">The reader options.</param>
    /// <param name="tableName">The table.</param>
    private static async Task<bool> ReadsAheadAsync(string path, AccessReaderOptions options, string tableName)
    {
        await using ReaderHarness reader = await ReaderHarness.OpenAsync(path, options, TestContext.Current.CancellationToken);
        ResolvedTable resolved = Assert.IsType<ResolvedTable>(
            await reader.Services.Catalog.ResolveTableAsync(tableName, TestContext.Current.CancellationToken));
        IReadOnlyList<long> pages = await reader.Database.OwnedPages.GetOwnedDataPagesAsync(
            resolved.Entry.TDefPage,
            TestContext.Current.CancellationToken);
        Assert.True(pages.Count >= 3, $"'{tableName}' should span at least 3 data pages; it spans {pages.Count}.");
        return reader.Services.Tables.ShouldReadAheadTablePages(resolved.Definition, pages);
    }

    private static async Task AssertScansMatchAsync(AccessReader reader, string tableName, ScanResult expected)
    {
        Assert.Equal(expected.Rows, await ReadRowSignaturesAsync(reader, tableName));

        var typed = new List<string>();
        await foreach (ScanRow row in reader.Rows<ScanRow>(tableName, cancellationToken: TestContext.Current.CancellationToken))
        {
            typed.Add(Signature(row.Id, row.Pad, row.Notes, row.Blob));
        }

        Assert.Equal(expected.Rows, typed);
        Assert.Equal(expected.Strings, await ReadStringRowsAsync(reader, tableName));
        Assert.Equal(expected.Rows.Count, await reader.GetRealRowCountAsync(tableName, TestContext.Current.CancellationToken));
    }

    private static async Task<ScanResult> ReadBaselineAsync(string path, string tableName, string? password)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions(password) { PageCacheSize = 0, PageReadOptimizationMode = PageReadOptimizationMode.Disabled, UseLockFile = false },
            TestContext.Current.CancellationToken);

        List<string> rows = await ReadRowSignaturesAsync(reader, tableName);
        bool withLongValues = tableName == LongValueTable;
        Assert.Equal(withLongValues ? LongValueRowCount : PlainRowCount, rows.Count);
        for (int i = 0; i < rows.Count; i++)
        {
            int id = i + 1;
            string prefix = Signature(id, Pad(id), withLongValues ? Notes(id) : null, blob: null);
            Assert.StartsWith(prefix[..prefix.LastIndexOf('|')], rows[i], StringComparison.Ordinal);
        }

        return new ScanResult(rows, await ReadStringRowsAsync(reader, tableName));
    }

    private static async Task<List<string>> ReadRowSignaturesAsync(AccessReader reader, string tableName)
    {
        var rows = new List<string>();
        await foreach (object[] row in reader.Rows(tableName, cancellationToken: TestContext.Current.CancellationToken).ConfigureAwait(false))
        {
            rows.Add(row.Length == 2
                ? Signature((int)row[0], (string)row[1], notes: null, blob: null)
                : Signature((int)row[0], (string)row[1], (string)row[2], (byte[])row[3]));
        }

        return rows;
    }

    private static async Task<List<string>> ReadStringRowsAsync(AccessReader reader, string tableName)
    {
        var rows = new List<string>();
        await foreach (string[] row in reader.RowsAsStrings(tableName, cancellationToken: TestContext.Current.CancellationToken).ConfigureAwait(false))
        {
            rows.Add(string.Join("|", row));
        }

        return rows;
    }

    private static string Signature(int id, string? pad, string? notes, byte[]? blob) =>
        string.Join(
            "|",
            id.ToString(CultureInfo.InvariantCulture),
            pad,
            notes ?? "<none>",
            blob is null ? "<none>" : Convert.ToHexString(blob));

    private static string Pad(int id) => "pad-" + id.ToString("D4", CultureInfo.InvariantCulture) + new string('-', 180);

    /// <summary>
    /// Cycles MEMO values that stay inline, fill one LVAL page and chain over
    /// several. CJK text defeats Unicode compression, so the chained value is
    /// about 12 KB.
    /// </summary>
    /// <param name="id">The row id.</param>
    private static string Notes(int id)
    {
        int length = (id % 3) switch
        {
            0 => 20,
            1 => 900,
            _ => 6_000,
        };

        var text = new StringBuilder(length);
        for (int i = 0; i < length; i++)
        {
            _ = text.Append((char)(0x4E00 + (((id * 131) + i) % 0x5000)));
        }

        return text.ToString();
    }

    private static string LargeMemoBody(int id) => id == 1 ? LargeMemo : "small-" + id.ToString(CultureInfo.InvariantCulture);

    /// <summary>
    /// Describes a row without copying the 1.5M-character MEMO into assertion
    /// messages: the id, then "intact" or the length of the wrong value.
    /// </summary>
    /// <param name="id">The row id.</param>
    /// <param name="body">The MEMO value read back.</param>
    private static string LargeMemoSignature(int id, string? body) =>
        id.ToString(CultureInfo.InvariantCulture) + "|"
        + (string.Equals(body, LargeMemoBody(id), StringComparison.Ordinal)
            ? "intact"
            : "wrong, " + (body?.Length ?? -1).ToString(CultureInfo.InvariantCulture) + " chars");

    private static string MakeLargeMemo()
    {
        char[] text = new char[LargeMemoLength];
        for (int i = 0; i < text.Length; i++)
        {
            text[i] = (char)('a' + (i % 26));
        }

        return new string(text);
    }

    private static int CountLvalPages(string path, DatabaseFormat format)
    {
        int pageSize = format == DatabaseFormat.Jet3Mdb ? 2048 : 4096;
        byte[] file = File.ReadAllBytes(path);
        int count = 0;
        for (int offset = 0; offset + pageSize <= file.Length; offset += pageSize)
        {
            if (LongValueStore.IsLvalPage(file.AsSpan(offset, pageSize)))
            {
                count++;
            }
        }

        return count;
    }

    private static byte[] Blob(int id)
    {
        int length = (id % 3) switch
        {
            0 => 10_000,
            1 => 40,
            _ => 3_000,
        };

        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++)
        {
            bytes[i] = unchecked((byte)((i % 200) + 1));
        }

        bytes[0] = unchecked((byte)id);
        return bytes;
    }

    private async Task<string> CreateDatabaseAsync(DatabaseFormat format, bool withLongValues, bool encrypt)
    {
        string extension = format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb";
        string path = Path.Combine(Path.GetTempPath(), $"ReadAhead_{Guid.NewGuid():N}{extension}");
        this.paths.Add(path);
        this.paths.Add(Path.ChangeExtension(path, format == DatabaseFormat.AceAccdb ? ".laccdb" : ".ldb"));

        var options = new AccessWriterOptions { UseLockFile = false };
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(path, format, options, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                PlainTable,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Pad", typeof(string), maxLength: 255),
                ],
                TestContext.Current.CancellationToken);
            _ = await writer.InsertRowsAsync(
                PlainTable,
                Enumerable.Range(1, PlainRowCount).Select(id => new object?[] { id, Pad(id) }),
                TestContext.Current.CancellationToken);

            if (withLongValues)
            {
                await writer.CreateTableAsync(
                    LongValueTable,
                    [
                        new ColumnDefinition("Id", typeof(int)),
                        new ColumnDefinition("Pad", typeof(string), maxLength: 255),
                        new ColumnDefinition("Notes", typeof(string)),
                        new ColumnDefinition("Blob", typeof(byte[])),
                    ],
                    TestContext.Current.CancellationToken);
                _ = await writer.InsertRowsAsync(
                    LongValueTable,
                    Enumerable.Range(1, LongValueRowCount).Select(id => new object?[] { id, Pad(id), Notes(id), Blob(id) }),
                    TestContext.Current.CancellationToken);
            }
        }

        if (encrypt)
        {
            await AccessWriter.EncryptAsync(path, Password.AsMemory(), AccessEncryptionFormat.AccdbAgileCfb, options, TestContext.Current.CancellationToken);
        }

        return path;
    }

    /// <summary>
    /// Creates a table whose first row holds a 1.5M-character MEMO and whose
    /// other 29 rows hold short ones, as in the bug 1 report.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>The database path.</returns>
    private async Task<string> CreateLargeMemoDatabaseAsync(DatabaseFormat format)
    {
        string extension = format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb";
        string path = Path.Combine(Path.GetTempPath(), $"ReadAheadLargeMemo_{Guid.NewGuid():N}{extension}");
        this.paths.Add(path);
        this.paths.Add(Path.ChangeExtension(path, format == DatabaseFormat.AceAccdb ? ".laccdb" : ".ldb"));

        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            path,
            format,
            new AccessWriterOptions { UseLockFile = false },
            TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(
            LargeMemoTable,
            [
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Body", typeof(string)),
            ],
            TestContext.Current.CancellationToken);
        _ = await writer.InsertRowsAsync(
            LargeMemoTable,
            Enumerable.Range(1, LargeMemoRowCount).Select(id => new object?[] { id, LargeMemoBody(id) }),
            TestContext.Current.CancellationToken);
        return path;
    }

    private sealed record ScanResult(List<string> Rows, List<string> Strings);

    private sealed class LargeMemoRow
    {
        public int Id { get; set; }

        public string? Body { get; set; }
    }

    private sealed class ScanRow
    {
        public int Id { get; set; }

        public string? Pad { get; set; }

        public string? Notes { get; set; }

        public byte[]? Blob { get; set; }
    }
}
