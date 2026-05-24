namespace JetDatabaseWriter.Benchmarks;

using System;
using System.Collections.Generic;
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

    private const int NumericRows = 25_000;
    private const int TextRows = 25_000;
    private const int WideRows = 10_000;
    private const int WideColumnCount = 40;
    private const int MemoRows = 5_000;
    private const int LongValueSubmodeRows = 2_000;
    private const int InlineLongValueLength = 32;
    private const int SinglePageLongValueLength = 2_000;
    private const int ChainedLongValueLength = 16_000;

    private static readonly string TempRoot = Path.Combine(Path.GetTempPath(), "JetBench");

    public static string NumericDbPath => Path.Combine(TempRoot, $"Numeric_{NumericRows}.accdb");

    public static string TextDbPath => Path.Combine(TempRoot, $"Text_{TextRows}.accdb");

    public static string WideDbPath => Path.Combine(TempRoot, $"Wide_{WideColumnCount}c_{WideRows}.accdb");

    public static string MemoDbPath => Path.Combine(TempRoot, $"Memo_{MemoRows}_lv2.accdb");

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

    private static async Task EnsureNumericAsync()
    {
        if (File.Exists(NumericDbPath))
        {
            return;
        }

        await using var w = await AccessWriter.CreateDatabaseAsync(NumericDbPath, DatabaseFormat.AceAccdb).ConfigureAwait(false);
        await w.CreateTableAsync(
            NumericTable,
            new ColumnDefinition[]
            {
                new("Id", typeof(int)),
                new("OrderId", typeof(int)),
                new("ProductId", typeof(int)),
                new("Quantity", typeof(short)),
                new("UnitPrice", typeof(decimal)),
                new("Discount", typeof(float)),
                new("StatusId", typeof(int)),
                new("AddedOn", typeof(DateTime)),
                new("ModifiedOn", typeof(DateTime)),
            }).ConfigureAwait(false);

        var rows = new List<object[]>(NumericRows);
        var baseDate = new DateTime(2020, 1, 1);
        for (int i = 0; i < NumericRows; i++)
        {
            rows.Add(new object[]
            {
                i,
                i / 5,
                (i % 200) + 1,
                (short)((i % 50) + 1),
                (decimal)(1.99 + (i % 100)),
                (float)((i % 10) * 0.05),
                (i % 5) + 1,
                baseDate.AddMinutes(i),
                baseDate.AddMinutes(i + 30),
            });
        }

        await w.InsertRowsAsync(NumericTable, rows).ConfigureAwait(false);
    }

    private static async Task EnsureTextAsync()
    {
        if (File.Exists(TextDbPath))
        {
            return;
        }

        await using var w = await AccessWriter.CreateDatabaseAsync(TextDbPath, DatabaseFormat.AceAccdb).ConfigureAwait(false);
        await w.CreateTableAsync(
            TextTable,
            new ColumnDefinition[]
            {
                new("Id", typeof(int)),
                new("FirstName", typeof(string), 64),
                new("LastName", typeof(string), 64),
                new("Email", typeof(string), 128),
                new("City", typeof(string), 64),
                new("Notes", typeof(string), 255),
            }).ConfigureAwait(false);

        var rows = new List<object[]>(TextRows);
        for (int i = 0; i < TextRows; i++)
        {
            rows.Add(new object[]
            {
                i,
                "First" + i,
                "Last" + i,
                "user" + i + "@example.com",
                "City" + (i % 100),
                "Note for row " + i + " — sample sentence with a few words to fill space.",
            });
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

        await using var w = await AccessWriter.CreateDatabaseAsync(WideDbPath, DatabaseFormat.AceAccdb).ConfigureAwait(false);
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

        await using var writer = await AccessWriter.CreateDatabaseAsync(MemoDbPath, DatabaseFormat.AceAccdb).ConfigureAwait(false);
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

        await CreateMemoTableAsync(writer, MemoInlineTable, LongValueSubmodeRows, static rowIndex => InlineLongValueLength).ConfigureAwait(false);
        await CreateMemoTableAsync(writer, MemoSinglePageTable, LongValueSubmodeRows, static rowIndex => SinglePageLongValueLength).ConfigureAwait(false);
        await CreateMemoTableAsync(writer, MemoChainedTable, LongValueSubmodeRows, static rowIndex => ChainedLongValueLength).ConfigureAwait(false);
        await CreateOleTableAsync(writer, OleInlineTable, LongValueSubmodeRows, InlineLongValueLength).ConfigureAwait(false);
        await CreateOleTableAsync(writer, OleSinglePageTable, LongValueSubmodeRows, SinglePageLongValueLength).ConfigureAwait(false);
        await CreateOleTableAsync(writer, OleChainedTable, LongValueSubmodeRows, ChainedLongValueLength).ConfigureAwait(false);
    }

    private static async Task CreateMemoTableAsync(AccessWriter writer, string tableName, int rowCount, Func<int, int> getLength)
    {
        await writer.CreateTableAsync(
            tableName,
            new ColumnDefinition[]
            {
                new("Id", typeof(int)),
                new("Body", typeof(string)),
            }).ConfigureAwait(false);

        var rows = new List<object[]>(rowCount);
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            int length = getLength(rowIndex);
            rows.Add(new object[] { rowIndex, MakeMemoBody(rowIndex, length) });
        }

        await writer.InsertRowsAsync(tableName, rows).ConfigureAwait(false);
    }

    private static async Task CreateOleTableAsync(AccessWriter writer, string tableName, int rowCount, int payloadLength)
    {
        await writer.CreateTableAsync(
            tableName,
            new ColumnDefinition[]
            {
                new("Id", typeof(int)),
                new("Blob", typeof(byte[])),
            }).ConfigureAwait(false);

        var rows = new List<object[]>(rowCount);
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            rows.Add(new object[] { rowIndex, MakeOlePayload(rowIndex, payloadLength) });
        }

        await writer.InsertRowsAsync(tableName, rows).ConfigureAwait(false);
    }

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
