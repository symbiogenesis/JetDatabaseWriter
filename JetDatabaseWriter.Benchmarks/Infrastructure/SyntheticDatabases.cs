namespace JetDatabaseWriter.Benchmarks.Infrastructure;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;

/// <summary>
/// Builds (and caches by file existence) the synthetic .accdb files used
/// by the read-decode benchmarks. Files are written under
/// <see cref="Path.GetTempPath"/> so repeated benchmark runs reuse them.
/// Delete the files manually to force a rebuild.
/// </summary>
internal static class SyntheticDatabases
{
    /// <summary>Numeric/date-heavy table name (5 ints, currency, datetime).</summary>
    public const string NumericTable = "Numeric";

    /// <summary>Text-heavy table name (5 short-text columns).</summary>
    public const string TextTable = "TextHeavy";

    /// <summary>Wide table name (40 mixed columns).</summary>
    public const string WideTable = "Wide";

    /// <summary>MEMO-heavy table name (one int + one MEMO column with mixed payload sizes).</summary>
    public const string MemoTable = "Memos";

    /// <summary>MEMO table whose payloads stay inline in the row.</summary>
    public const string MemoInlineTable = "MemoInline";

    /// <summary>MEMO table whose payloads fit on a single LVAL page.</summary>
    public const string MemoSinglePageTable = "MemoSinglePage";

    /// <summary>MEMO table whose payloads require chained LVAL pages.</summary>
    public const string MemoChainedTable = "MemoChained";

    /// <summary>OLE table whose payloads stay inline in the row.</summary>
    public const string OleInlineTable = "OleInline";

    /// <summary>OLE table whose payloads fit on a single LVAL page.</summary>
    public const string OleSinglePageTable = "OleSinglePage";

    /// <summary>OLE table whose payloads require chained LVAL pages.</summary>
    public const string OleChainedTable = "OleChained";

    /// <summary>Small table used to isolate owned-page discovery cost.</summary>
    public const string OwnedPageDiscoveryTargetTable = "OwnedMapTarget";

    private const int NumericRows = 25_000;
    private const int TextRows = 25_000;
    private const int WideRows = 10_000;
    private const int WideColumnCount = 40;
    private const int MemoRows = 5_000;
    private const int LongValueSubmodeRows = 2_000;
    private const int InlineLongValueLength = 32;
    private const int SinglePageLongValueLength = 2_000;
    private const int ChainedLongValueLength = 16_000;
    private const int OwnedPageDiscoveryTargetRows = 128;
    private const int OwnedPageDiscoveryFillerRows = 20_000;
    private const string OwnedPageDiscoveryFillerTable = "OwnedMapFiller";

    private static readonly string TempRoot = Path.Combine(Path.GetTempPath(), "JetBench");

    public static string NumericDbPath => Path.Combine(TempRoot, $"Numeric_{NumericRows}.accdb");

    public static string TextDbPath => Path.Combine(TempRoot, $"Text_{TextRows}.accdb");

    public static string WideDbPath => Path.Combine(TempRoot, $"Wide_{WideColumnCount}c_{WideRows}.accdb");

    public static string MemoDbPath => Path.Combine(TempRoot, $"Memo_{MemoRows}_lv2.accdb");

    public static string OwnedPageDiscoveryMappedDbPath => Path.Combine(
        TempRoot,
        $"OwnedPageDiscovery_{OwnedPageDiscoveryTargetRows}_{OwnedPageDiscoveryFillerRows}_mapped_v1.accdb");

    public static string OwnedPageDiscoveryFallbackDbPath => Path.Combine(
        TempRoot,
        $"OwnedPageDiscovery_{OwnedPageDiscoveryTargetRows}_{OwnedPageDiscoveryFillerRows}_fallback_v1.accdb");

    /// <summary>
    /// Ensures all synthetic DBs exist on disk. Skips files that already
    /// exist (cache by path). Safe to call from <c>[GlobalSetup]</c>.
    /// </summary>
    public static async Task EnsureAllAsync()
    {
        Directory.CreateDirectory(TempRoot);
        await EnsureNumericAsync().ConfigureAwait(false);
        await EnsureTextAsync().ConfigureAwait(false);
        await EnsureWideAsync().ConfigureAwait(false);
        await EnsureMemoAsync().ConfigureAwait(false);
    }

    public static async Task EnsureOwnedPageDiscoveryAsync()
    {
        Directory.CreateDirectory(TempRoot);
        if (!File.Exists(OwnedPageDiscoveryMappedDbPath))
        {
            await CreateOwnedPageDiscoveryDatabaseAsync(OwnedPageDiscoveryMappedDbPath).ConfigureAwait(false);
        }

        if (!File.Exists(OwnedPageDiscoveryFallbackDbPath))
        {
            File.Copy(OwnedPageDiscoveryMappedDbPath, OwnedPageDiscoveryFallbackDbPath, overwrite: true);
            await PatchOwnedUsageMapToUnknownTypeAsync(
                OwnedPageDiscoveryFallbackDbPath,
                OwnedPageDiscoveryTargetTable).ConfigureAwait(false);
        }
    }

    private static async Task EnsureNumericAsync()
    {
        if (File.Exists(NumericDbPath))
        {
            return;
        }

        await using AccessWriter w = await AccessWriter.CreateDatabaseAsync(NumericDbPath, DatabaseFormat.AceAccdb).ConfigureAwait(false);
        await w.CreateTableAsync(
            NumericTable,
            [
                new("Id", typeof(int)),
                new("OrderId", typeof(int)),
                new("ProductId", typeof(int)),
                new("Quantity", typeof(short)),
                new("UnitPrice", typeof(decimal)),
                new("Discount", typeof(float)),
                new("StatusId", typeof(int)),
                new("AddedOn", typeof(DateTime)),
                new("ModifiedOn", typeof(DateTime)),
            ]).ConfigureAwait(false);

        var rows = new List<object[]>(NumericRows);
        var baseDate = new DateTime(2020, 1, 1);
        for (int i = 0; i < NumericRows; i++)
        {
            rows.Add(
            [
                i,
                i / 5,
                (i % 200) + 1,
                (short)((i % 50) + 1),
                (decimal)(1.99 + (i % 100)),
                (float)((i % 10) * 0.05),
                (i % 5) + 1,
                baseDate.AddMinutes(i),
                baseDate.AddMinutes(i + 30),
            ]);
        }

        await w.InsertRowsAsync(NumericTable, rows).ConfigureAwait(false);
    }

    private static async Task EnsureTextAsync()
    {
        if (File.Exists(TextDbPath))
        {
            return;
        }

        await using AccessWriter w = await AccessWriter.CreateDatabaseAsync(TextDbPath, DatabaseFormat.AceAccdb).ConfigureAwait(false);
        await w.CreateTableAsync(
            TextTable,
            [
                new("Id", typeof(int)),
                new("FirstName", typeof(string), 64),
                new("LastName", typeof(string), 64),
                new("Email", typeof(string), 128),
                new("City", typeof(string), 64),
                new("Notes", typeof(string), 255),
            ]).ConfigureAwait(false);

        var rows = new List<object[]>(TextRows);
        for (int i = 0; i < TextRows; i++)
        {
            rows.Add(
            [
                i,
                "First" + i,
                "Last" + i,
                "user" + i + "@example.com",
                "City" + (i % 100),
                "Note for row " + i + " — sample sentence with a few words to fill space.",
            ]);
        }

        await w.InsertRowsAsync(TextTable, rows).ConfigureAwait(false);
    }

    private static async Task EnsureWideAsync()
    {
        if (File.Exists(WideDbPath))
        {
            return;
        }

        var defs = new List<ColumnDefinition>(WideColumnCount)
        {
            new("Id", typeof(int)),
        };

        // 20 numeric, 19 text columns to round out the 40 total.
        for (int i = 0; i < 20; i++)
        {
            defs.Add(new ColumnDefinition("N" + i, typeof(int)));
        }

        for (int i = 0; i < 19; i++)
        {
            defs.Add(new ColumnDefinition("S" + i, typeof(string), 32));
        }

        await using AccessWriter w = await AccessWriter.CreateDatabaseAsync(WideDbPath, DatabaseFormat.AceAccdb).ConfigureAwait(false);
        await w.CreateTableAsync(WideTable, defs).ConfigureAwait(false);

        var rows = new List<object[]>(WideRows);
        for (int r = 0; r < WideRows; r++)
        {
            var row = new object[WideColumnCount];
            row[0] = r;
            for (int c = 1; c <= 20; c++)
            {
                row[c] = r * c;
            }

            for (int c = 21; c < WideColumnCount; c++)
            {
                row[c] = "v" + r + "_" + c;
            }

            rows.Add(row);
        }

        await w.InsertRowsAsync(WideTable, rows).ConfigureAwait(false);
    }

    private static async Task EnsureMemoAsync()
    {
        if (File.Exists(MemoDbPath))
        {
            return;
        }

        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(MemoDbPath, DatabaseFormat.AceAccdb).ConfigureAwait(false);
        await CreateMemoTableAsync(
            writer,
            MemoTable,
            MemoRows,
            static rowIndex => rowIndex % 3 switch
            {
                0 => InlineLongValueLength,
                1 => SinglePageLongValueLength,
                _ => ChainedLongValueLength,
            }).ConfigureAwait(false);

        await CreateMemoTableAsync(writer, MemoInlineTable, LongValueSubmodeRows, static _ => InlineLongValueLength).ConfigureAwait(false);
        await CreateMemoTableAsync(writer, MemoSinglePageTable, LongValueSubmodeRows, static _ => SinglePageLongValueLength).ConfigureAwait(false);
        await CreateMemoTableAsync(writer, MemoChainedTable, LongValueSubmodeRows, static _ => ChainedLongValueLength).ConfigureAwait(false);
        await CreateOleTableAsync(writer, OleInlineTable, LongValueSubmodeRows, InlineLongValueLength).ConfigureAwait(false);
        await CreateOleTableAsync(writer, OleSinglePageTable, LongValueSubmodeRows, SinglePageLongValueLength).ConfigureAwait(false);
        await CreateOleTableAsync(writer, OleChainedTable, LongValueSubmodeRows, ChainedLongValueLength).ConfigureAwait(false);
    }

    private static async Task CreateMemoTableAsync(AccessWriter writer, string tableName, int rowCount, Func<int, int> getLength)
    {
        await writer.CreateTableAsync(
            tableName,
            [
                new("Id", typeof(int)),
                new("Body", typeof(string)),
            ]).ConfigureAwait(false);

        var rows = new List<object[]>(rowCount);
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            int length = getLength(rowIndex);
            rows.Add([rowIndex, MakeMemoBody(rowIndex, length)]);
        }

        await writer.InsertRowsAsync(tableName, rows).ConfigureAwait(false);
    }

    private static async Task CreateOleTableAsync(AccessWriter writer, string tableName, int rowCount, int payloadLength)
    {
        await writer.CreateTableAsync(
            tableName,
            [
                new("Id", typeof(int)),
                new("Blob", typeof(byte[])),
            ]).ConfigureAwait(false);

        var rows = new List<object[]>(rowCount);
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            rows.Add([rowIndex, MakeOlePayload(rowIndex, payloadLength)]);
        }

        await writer.InsertRowsAsync(tableName, rows).ConfigureAwait(false);
    }

    private static async Task CreateOwnedPageDiscoveryDatabaseAsync(string databasePath)
    {
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(databasePath, DatabaseFormat.AceAccdb).ConfigureAwait(false);
        await writer.CreateTableAsync(OwnedPageDiscoveryTargetTable, OwnedPageDiscoverySchema()).ConfigureAwait(false);
        await writer.InsertRowsAsync(
            OwnedPageDiscoveryTargetTable,
            CreateOwnedPageDiscoveryRows(OwnedPageDiscoveryTargetRows, "T")).ConfigureAwait(false);

        await writer.CreateTableAsync(OwnedPageDiscoveryFillerTable, OwnedPageDiscoverySchema()).ConfigureAwait(false);
        await writer.InsertRowsAsync(
            OwnedPageDiscoveryFillerTable,
            CreateOwnedPageDiscoveryRows(OwnedPageDiscoveryFillerRows, "F")).ConfigureAwait(false);
    }

    private static ColumnDefinition[] OwnedPageDiscoverySchema() =>
    [
        new("Id", typeof(int)),
        new("Payload", typeof(string), maxLength: 240),
    ];

    private static List<object[]> CreateOwnedPageDiscoveryRows(int rowCount, string prefix)
    {
        var rows = new List<object[]>(rowCount);
        for (int rowNumber = 0; rowNumber < rowCount; rowNumber++)
        {
            rows.Add([rowNumber, prefix + "-" + rowNumber.ToString("D5", CultureInfo.InvariantCulture) + "-" + new string('x', 200)]);
        }

        return rows;
    }

    private static async Task PatchOwnedUsageMapToUnknownTypeAsync(string databasePath, string tableName)
    {
        int pageSize;
        long tdefPage;
        await using (AccessReader reader = await AccessReader.OpenAsync(
            databasePath,
            new AccessReaderOptions { UseLockFile = false }).ConfigureAwait(false))
        {
            pageSize = reader.PageSize;
            tdefPage = await ResolveTdefPageAsync(reader, tableName).ConfigureAwait(false);
        }

        byte[] fileBytes = await File.ReadAllBytesAsync(databasePath).ConfigureAwait(false);
        int rowAbsoluteStart = FindOwnedUsageMapRowStart(fileBytes, pageSize, tdefPage);
        if (fileBytes[rowAbsoluteStart] != 0x00)
        {
            throw new InvalidDataException("Expected an INLINE owned-pages usage-map row.");
        }

        fileBytes[rowAbsoluteStart] = 0x7F;
        await File.WriteAllBytesAsync(databasePath, fileBytes).ConfigureAwait(false);
    }

    private static async Task<long> ResolveTdefPageAsync(AccessReader reader, string tableName)
    {
        List<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync("MSysObjects").ConfigureAwait(false);
        int idIndex = metadata.FindIndex(static column => string.Equals(column.Name, "Id", StringComparison.OrdinalIgnoreCase));
        int nameIndex = metadata.FindIndex(static column => string.Equals(column.Name, "Name", StringComparison.OrdinalIgnoreCase));
        if (idIndex < 0 || nameIndex < 0)
        {
            throw new InvalidDataException("MSysObjects is missing Id or Name metadata.");
        }

        await foreach (object[] row in reader.Rows("MSysObjects").ConfigureAwait(false))
        {
            if (row[nameIndex] is string name && string.Equals(name, tableName, StringComparison.OrdinalIgnoreCase))
            {
                return Convert.ToInt64(row[idIndex], CultureInfo.InvariantCulture);
            }
        }

        throw new InvalidDataException($"Could not resolve the TDEF page for table '{tableName}'.");
    }

    private static int FindOwnedUsageMapRowStart(byte[] fileBytes, int pageSize, long tdefPage)
    {
        const int dataPageRowsStart = 14;
        const int ownedPagesPointerOffset = 0x37;
        int tdefOffset = checked((int)(tdefPage * pageSize));
        int usageMapRow = fileBytes[tdefOffset + ownedPagesPointerOffset];
        int usageMapPage = ReadUInt24(fileBytes, tdefOffset + ownedPagesPointerOffset + 1);
        int usageMapOffset = checked(usageMapPage * pageSize);
        int rowOffsetPosition = usageMapOffset + dataPageRowsStart + (usageMapRow * 2);
        int rowStart = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(rowOffsetPosition, 2)) & 0x1FFF;
        int rowAbsoluteStart = usageMapOffset + rowStart;

        if (usageMapPage <= 0 || rowAbsoluteStart < usageMapOffset || rowAbsoluteStart >= usageMapOffset + pageSize)
        {
            throw new InvalidDataException("The owned-pages usage-map pointer is outside the file bounds.");
        }

        return rowAbsoluteStart;
    }

    private static int ReadUInt24(byte[] buffer, int offset)
        => buffer[offset] | (buffer[offset + 1] << 8) | (buffer[offset + 2] << 16);

    private static string MakeMemoBody(int seed, int length)
    {
        var buffer = new char[length];
        string prefix = "row" + seed + ":";
        int prefixLength = Math.Min(prefix.Length, length);
        prefix.AsSpan(0, prefixLength).CopyTo(buffer);
        for (int index = prefixLength; index < length; index++)
        {
            buffer[index] = (char)('a' + ((index + seed) % 26));
        }

        return new string(buffer);
    }

    private static byte[] MakeOlePayload(int seed, int length)
    {
        var payload = new byte[length];
        for (int index = 0; index < payload.Length; index++)
        {
            payload[index] = (byte)(((index + seed) % 251) + 1);
        }

        return payload;
    }
}
