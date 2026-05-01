#if DIAG_ROUNDTRIP
namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

public sealed class AccessRoundTripDiagPageBytes
{
    public static bool Available => AccessRoundTripEnvironment.IsAvailable;

    [Fact(SkipUnless = nameof(Available), Skip = "diag", SkipType = typeof(AccessRoundTripDiagPageBytes))]
    public async Task DumpPg2994_FailingCase()
    {
        await Dump(true);
    }

    [Fact(SkipUnless = nameof(Available), Skip = "diag", SkipType = typeof(AccessRoundTripDiagPageBytes))]
    public async Task DumpPg2994_OriginalNorthwind()
    {
        var ct = TestContext.Current.CancellationToken;
        var sb = new System.Text.StringBuilder();
        await using FileStream fs = File.OpenRead(TestDatabases.NorthwindTraders);
        byte[] page = new byte[4096];
        for (int p = 2990; p < 3008; p++)
        {
            fs.Position = (long)p * 4096;
            await fs.ReadExactlyAsync(page.AsMemory(), ct);
            ushort numRows = BitConverter.ToUInt16(page, 12);
            int tdefPg = BitConverter.ToInt32(page, 4);
            sb.AppendLine(CultureInfo.InvariantCulture, $"orig page {p}: type=0x{page[0]:X2} tdef_pg={tdefPg} num_rows={numRows}");
        }

        Assert.Fail(sb.ToString());
    }

    [Fact(SkipUnless = nameof(Available), Skip = "diag", SkipType = typeof(AccessRoundTripDiagPageBytes))]
    public async Task DumpPg2994_PassingCase()
    {
        await Dump(false);
    }

    private static async Task Dump(bool failing)
    {
        string label = failing ? "FAIL" : "PASS";
        string work = Path.Combine(Path.GetTempPath(), "diagpg_" + label + "_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(work);
        string src = Path.Combine(work, "src.accdb");
        File.Copy(TestDatabases.NorthwindTraders, src, overwrite: true);
        File.SetAttributes(src, File.GetAttributes(src) & ~FileAttributes.ReadOnly);
        var ct = TestContext.Current.CancellationToken;
        await using (var w = await AccessWriter.OpenAsync(src, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            if (failing)
            {
                await w.CreateTableAsync("RT_Customers", [new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("Name", typeof(string), maxLength: 100) { IsNullable = false }], ct);
                await w.CreateTableAsync("RT_Orders", [new("OrderID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("CustomerID", typeof(int)) { IsNullable = false }, new("OrderDate", typeof(DateTime))], ct);
            }
            else
            {
                await w.CreateTableAsync("RT_C18", [new("CustID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("CName", typeof(string), maxLength: 100) { IsNullable = false }], ct);
                await w.CreateTableAsync("RT_O18", [new("OrdID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false }, new("CustID", typeof(int)) { IsNullable = false }, new("ODate", typeof(DateTime))], ct);
            }
        }

        var sb = new System.Text.StringBuilder();
        long fileSize = new FileInfo(src).Length;
        sb.AppendLine(CultureInfo.InvariantCulture, $"{label}: file_size={fileSize} ({fileSize / 4096} pages)");
        await using FileStream fs = File.OpenRead(src);
        byte[] page = new byte[4096];
        fs.Position = 2994L * 4096;
        await fs.ReadExactlyAsync(page.AsMemory(), ct);
        ushort freeSpace = BitConverter.ToUInt16(page, 2);
        int tdefPg = BitConverter.ToInt32(page, 4);
        ushort numRows = BitConverter.ToUInt16(page, 12);
        sb.AppendLine(CultureInfo.InvariantCulture, $"{label}: free_space={freeSpace} tdef_pg={tdefPg} num_rows={numRows}");
        for (int i = 0; i < numRows; i++)
        {
            ushort off = BitConverter.ToUInt16(page, 14 + (i * 2));
            sb.AppendLine(CultureInfo.InvariantCulture, $"  row[{i}] offset = 0x{off:X4} ({off})");
        }

        // Dump first row's bytes (the most-recently-inserted row by design) — last row in offset table is row[numRows-1]
        // but new rows are inserted at lowest index typically. Check by using highest offset (closest to end of page) and lowest offset.
        // For simplicity: dump 16 bytes around each new row (rows 21 and 22 in failing case if 20 existed before).
        // Actually let's just dump a broad area.
        sb.AppendLine(CultureInfo.InvariantCulture, $"-- {label} page 2994 hex bytes 0..2096 (16/line, only print non-zero or near-row regions) --");
        for (int row = 0; row < 256; row += 16)
        {
            var hex = string.Join(" ", new System.ArraySegment<byte>(page, row, 16).Select(b => b.ToString("X2", CultureInfo.InvariantCulture)));
            sb.AppendLine(CultureInfo.InvariantCulture, $"  {row:X4}: {hex}");
        }

        // Also dump last 384 bytes:
        sb.AppendLine(CultureInfo.InvariantCulture, $"-- tail --");
        for (int row = 4096 - 384; row < 4096; row += 16)
        {
            var hex = string.Join(" ", new System.ArraySegment<byte>(page, row, 16).Select(b => b.ToString("X2", CultureInfo.InvariantCulture)));
            sb.AppendLine(CultureInfo.InvariantCulture, $"  {row:X4}: {hex}");
        }

        // Dump the area where the new rows live (offsets 2880..3000).
        sb.AppendLine(CultureInfo.InvariantCulture, $"-- new rows region 0x0B40..0x0C00 --");
        for (int row = 0x0B40; row < 0x0C00; row += 16)
        {
            var hex = string.Join(" ", new System.ArraySegment<byte>(page, row, 16).Select(b => b.ToString("X2", CultureInfo.InvariantCulture)));
            sb.AppendLine(CultureInfo.InvariantCulture, $"  {row:X4}: {hex}");
        }

        // Dump every appended page (3008+) to see what's there
        for (int p = 3008; p < (int)(fileSize / 4096); p++)
        {
            fs.Position = (long)p * 4096;
            var newPg = new byte[4096];
            await fs.ReadExactlyAsync(newPg.AsMemory(), ct);
            sb.AppendLine(CultureInfo.InvariantCulture, $"-- {label} page {p} type=0x{newPg[0]:X2} flag=0x{newPg[1]:X2} tdef_pg={BitConverter.ToInt32(newPg, 4)} --");
            sb.AppendLine(CultureInfo.InvariantCulture, $"  hdr: {string.Join(" ", new System.ArraySegment<byte>(newPg, 0, 64).Select(b => b.ToString("X2", CultureInfo.InvariantCulture)))}");
        }

        Assert.Fail(sb.ToString());
    }
}
#endif
