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
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Table-scan read-ahead keeps a second page read in flight while the caller
/// decodes the current page. It must return the same rows as a sequential
/// scan at every page-cache size, including caches smaller than the two pages
/// in flight and no cache at all, and on AES-encrypted files, where the two
/// reads can decrypt pages on two threads at once.
/// </summary>
public sealed class TableScanReadAheadTests : IDisposable
{
    private const string PlainTable = "Plain";
    private const string LongValueTable = "LongValues";
    private const string Password = "read-ahead";
    private const int PlainRowCount = 400;
    private const int LongValueRowCount = 120;

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

    [Theory]
    [MemberData(nameof(PlainScanCases))]
    public async Task Scans_TinyOrNoPageCache_ReadAheadAndReturnEveryRow(DatabaseFormat format, int cacheSize, PageReadOptimizationMode mode)
    {
        string path = await this.CreateDatabaseAsync(format, withLongValues: false, encrypt: false);
        ScanResult expected = await ReadBaselineAsync(path, PlainTable, password: null);

        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions { PageCacheSize = cacheSize, PageReadOptimizationMode = mode, UseLockFile = false },
            TestContext.Current.CancellationToken);

        Assert.True(await ReadsAheadAsync(reader, PlainTable));
        await AssertScansMatchAsync(reader, PlainTable, expected);
    }

    [Theory]
    [MemberData(nameof(LongValueScanCases))]
    public async Task Scans_LongValueTableLargerThanCache_ReturnEveryRow(DatabaseFormat format, int cacheSize)
    {
        string path = await this.CreateDatabaseAsync(format, withLongValues: true, encrypt: false);
        ScanResult expected = await ReadBaselineAsync(path, LongValueTable, password: null);

        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions { PageCacheSize = cacheSize, PageReadOptimizationMode = PageReadOptimizationMode.Enabled, UseLockFile = false },
            TestContext.Current.CancellationToken);

        // Long-value tables stay sequential because read-ahead measured no gain on them.
        Assert.False(await ReadsAheadAsync(reader, LongValueTable));
        await AssertScansMatchAsync(reader, LongValueTable, expected);
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

        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions(Password) { PageCacheSize = cacheSize, UseLockFile = false },
            TestContext.Current.CancellationToken);

        Assert.True(await ReadsAheadAsync(reader, PlainTable));
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
    public async Task ReadPageAsync_ConcurrentReadsOfAesEncryptedFile_DecryptEveryPage()
    {
        string path = await this.CreateDatabaseAsync(DatabaseFormat.AceAccdb, withLongValues: true, encrypt: true);
        var options = new AccessReaderOptions(Password) { PageCacheSize = 0, UseLockFile = false };

        byte[][] expected;
        await using (AccessReader sequential = await AccessReader.OpenAsync(path, options, TestContext.Current.CancellationToken))
        {
            DatabaseFile sequentialDb = FacadeInternals.Database(sequential);
            expected = new byte[checked((int)sequentialDb.PageCount)][];
            for (int page = 1; page < expected.Length; page++)
            {
                expected[page] = await sequentialDb.ReadPageCopyAsync(page, TestContext.Current.CancellationToken);
            }
        }

        // A fresh reader has not built its AES transforms yet, so the first
        // round also races the lazy build.
        await using AccessReader reader = await AccessReader.OpenAsync(path, options, TestContext.Current.CancellationToken);
        DatabaseFile db = FacadeInternals.Database(reader);
        for (int round = 0; round < 8; round++)
        {
            Task<byte[]>[] reads = [.. Enumerable.Range(1, expected.Length - 1).Select(page =>
                Task.Run(async () => await db.ReadPageCopyAsync(page, TestContext.Current.CancellationToken)))];
            byte[][] actual = await Task.WhenAll(reads);
            for (int page = 1; page < expected.Length; page++)
            {
                Assert.True(expected[page].AsSpan().SequenceEqual(actual[page - 1]), $"Page {page} decrypted differently in round {round}.");
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

    private static async Task<bool> ReadsAheadAsync(AccessReader reader, string tableName)
    {
        ResolvedTable resolved = Assert.IsType<ResolvedTable>(
            await FacadeInternals.Services(reader).Catalog.ResolveTableAsync(tableName, TestContext.Current.CancellationToken));
        IReadOnlyList<long> pages = await FacadeInternals.Database(reader).GetOwnedDataPagesAsync(
            resolved.Entry.TDefPage,
            TestContext.Current.CancellationToken);
        Assert.True(pages.Count >= 3, $"'{tableName}' should span at least 3 data pages; it spans {pages.Count}.");
        return FacadeInternals.Services(reader).Tables.ShouldReadAheadTablePages(resolved.Definition, pages);
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
            await AccessWriter.EncryptAsync(path, Password.AsMemory(), AccessEncryptionFormat.AccdbAesCfbWrapped, options, TestContext.Current.CancellationToken);
        }

        return path;
    }

    private sealed record ScanResult(List<string> Rows, List<string> Strings);

    private sealed class ScanRow
    {
        public int Id { get; set; }

        public string? Pad { get; set; }

        public string? Notes { get; set; }

        public byte[]? Blob { get; set; }
    }
}
