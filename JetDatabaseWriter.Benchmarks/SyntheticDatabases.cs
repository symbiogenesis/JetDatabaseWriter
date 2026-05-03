namespace JetDatabaseWriter.Benchmarks;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
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

    private const int NumericRows = 25_000;
    private const int TextRows = 25_000;
    private const int WideRows = 10_000;
    private const int WideColumnCount = 40;
    private const int MemoRows = 5_000;

    private static readonly string TempRoot = Path.Combine(Path.GetTempPath(), "JetBench");

    public static string NumericDbPath => Path.Combine(TempRoot, $"Numeric_{NumericRows}.accdb");

    public static string TextDbPath => Path.Combine(TempRoot, $"Text_{TextRows}.accdb");

    public static string WideDbPath => Path.Combine(TempRoot, $"Wide_{WideColumnCount}c_{WideRows}.accdb");

    public static string MemoDbPath => Path.Combine(TempRoot, $"Memo_{MemoRows}.accdb");

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

        await using var w = await AccessWriter.CreateDatabaseAsync(MemoDbPath, DatabaseFormat.AceAccdb).ConfigureAwait(false);
        await w.CreateTableAsync(
            MemoTable,
            new ColumnDefinition[]
            {
                new("Id", typeof(int)),
                new("Body", typeof(string)), // maxLength=0 → MEMO (LVAL)
            }).ConfigureAwait(false);

        // Mix three payload sizes so the reader exercises all three LVAL
        // branches roughly evenly:
        //   - small  (~32 B):  inline (bitmask 0x80 — fits in row).
        //   - medium (~2 KB):  single LVAL page (bitmask 0x40).
        //   - large  (~16 KB): chained LVAL pages (default branch).
        var rows = new List<object[]>(MemoRows);
        for (int i = 0; i < MemoRows; i++)
        {
            int kind = i % 3;
            int len = kind switch
            {
                0 => 32,
                1 => 2_000,
                _ => 16_000,
            };

            rows.Add(new object[] { i, MakeBody(i, len) });
        }

        await w.InsertRowsAsync(MemoTable, rows).ConfigureAwait(false);

        static string MakeBody(int seed, int length)
        {
            // Repeating ASCII pattern with a per-row prefix so values are
            // distinct (defeats potential page-level dedup) but cheap to
            // construct.
            var buf = new char[length];
            string prefix = "row" + seed + ":";
            int p = System.Math.Min(prefix.Length, length);
            prefix.AsSpan(0, p).CopyTo(buf);
            for (int j = p; j < length; j++)
            {
                buf[j] = (char)('a' + ((j + seed) % 26));
            }

            return new string(buf);
        }
    }
}
