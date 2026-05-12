// One-shot research probe: dumps the long-row Table11 / Table11_desc leaf
// entries from the Jackcess testIndexCodes V2000/V2003/V2007/V2010 fixtures
// and prints their raw key bytes alongside the source row values, so we can
// verify (a) the chunk separator across formats, (b) the chunk-boundary
// rule, and (c) the per-format byte budget.
//
// Invoke via: dotnet run --project JetDatabaseWriter.FormatProbe -- long-row-probe
// Legacy: set DIAG_LONG_ROW_PROBE=1 ; dotnet run --project JetDatabaseWriter.FormatProbe

namespace JetDatabaseWriter.FormatProbe;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;

internal static class LongRowProbe
{
    public static async Task<int> RunAsync(string fixturesDir, string outFile)
    {
        var sb = new StringBuilder();
        sb.AppendLine("# Long-row index leaf-entry dump");
        sb.AppendLine();
        sb.AppendLine("Tables: `Table11` (asc) and `Table11_desc` (desc) — single Memo column `data`.");
        sb.AppendLine();

        string[] fixtures =
        [
            Path.Combine(fixturesDir, "Jackcess", "V2000", "testIndexCodesV2000.mdb"),
            Path.Combine(fixturesDir, "Jackcess", "V2003", "testIndexCodesV2003.mdb"),
            Path.Combine(fixturesDir, "Jackcess", "V2007", "testIndexCodesV2007.accdb"),
            Path.Combine(fixturesDir, "Jackcess", "V2010", "testIndexCodesV2010.accdb"),
        ];

        foreach (string path in fixtures)
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"## {Path.GetFileName(path)}");
            sb.AppendLine();
            await DumpAsync(path, sb, CancellationToken.None);
            sb.AppendLine();
        }

        Directory.CreateDirectory(Path.GetDirectoryName(outFile)!);
        await File.WriteAllTextAsync(outFile, sb.ToString());
        Console.WriteLine($"Wrote {outFile}");
        return 0;
    }

    private static async Task DumpAsync(string path, StringBuilder sb, CancellationToken ct)
    {
        if (!File.Exists(path))
        {
            sb.AppendLine(CultureInfo.InvariantCulture, $"_(missing: {path})_");
            return;
        }

        await using AccessReader reader = await AccessReader.OpenAsync(
            path, new AccessReaderOptions { UseLockFile = false }, ct);

        var layout = IndexLeafPageBuilder.GetLayout(reader.DatabaseFormat);
        int pageSize = reader.PageSize;
        List<string> tables = await reader.ListTablesAsync(ct);

        foreach (string tableName in new[] { "Table11", "Table11_desc" })
        {
            if (!tables.Contains(tableName, StringComparer.OrdinalIgnoreCase))
            {
                continue;
            }

            sb.AppendLine(CultureInfo.InvariantCulture, $"### {tableName}");
            sb.AppendLine();

            List<ColumnMetadata> columns = await reader.GetColumnMetadataAsync(tableName, ct);
            int dataOrdinal = FindColumnOrdinal(columns, "data");
            var rowValues = new List<string?>();
            await foreach (string[] row in reader.RowsAsStrings(tableName, cancellationToken: ct))
            {
                rowValues.Add(dataOrdinal < row.Length ? row[dataOrdinal] : null);
            }

            IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(tableName, ct);
            foreach (IndexMetadata idx in indexes)
            {
                if (idx.Columns.Count != 1 || idx.Columns[0].Name != "data" || idx.FirstDp <= 0)
                {
                    continue;
                }

                bool asc = idx.Columns[0].IsAscending;
                sb.AppendLine(CultureInfo.InvariantCulture, $"- index `{idx.Name}` ascending={asc} firstDp={idx.FirstDp}");
                List<byte[]> keys = await CollectLeavesAsync(reader, layout, pageSize, idx.FirstDp, ct);
                sb.AppendLine(CultureInfo.InvariantCulture, $"- leaf entries: {keys.Count}");
                sb.AppendLine();

                // Sort source values the same way the leaves are sorted (unsigned bytes).
                // We can't sort by encoded key (the encoder is what we're trying to validate).
                // Just print all rows and all leaves; the human cross-references them.
                sb.AppendLine("Row values (length, first 60 chars):");
                for (int i = 0; i < rowValues.Count; i++)
                {
                    string? v = rowValues[i];
                    string display = v is null ? "<null>" : Truncate(v, 60);
                    sb.AppendLine(CultureInfo.InvariantCulture, $"  row[{i}] len={v?.Length ?? -1}  {display}");
                }

                sb.AppendLine();
                sb.AppendLine("Leaf entries (hex; trailing 4 bytes = page:3 + row:1 pointer):");
                for (int i = 0; i < keys.Count; i++)
                {
                    byte[] k = keys[i];
                    sb.AppendLine(CultureInfo.InvariantCulture, $"  leaf[{i}] len={k.Length}  {Convert.ToHexString(k)}");
                }

                sb.AppendLine();
            }
        }
    }

    private static async Task<List<byte[]>> CollectLeavesAsync(
        AccessReader reader,
        IndexLeafPageBuilder.LeafPageLayout layout,
        int pageSize,
        long rootPage,
        CancellationToken ct)
    {
        long current = rootPage;
        for (int depth = 0; depth < 32; depth++)
        {
            byte[] page = await reader.GetRawPageBytesAsync(current, ct);
            byte pageType = page[0];
            if (pageType == Constants.IndexLeafPage.PageTypeLeaf)
            {
                break;
            }

            var entries = IndexLeafIncremental.DecodeIntermediateEntries(layout, page, pageSize);
            current = entries[0].ChildPage;
        }

        var result = new List<byte[]>();
        long guard = 0;
        while (current != 0 && ++guard < 100_000)
        {
            byte[] page = await reader.GetRawPageBytesAsync(current, ct);
            var entries = IndexLeafIncremental.DecodeEntries(layout, page, pageSize);
            foreach (var e in entries)
            {
                result.Add(e.Key);
            }

            (long _, long next, long _) = IndexLeafIncremental.ReadSiblingPointers(layout, page);
            current = next;
        }

        return result;
    }

    private static int FindColumnOrdinal(List<ColumnMetadata> columns, string name)
    {
        for (int i = 0; i < columns.Count; i++)
        {
            if (string.Equals(columns[i].Name, name, StringComparison.OrdinalIgnoreCase))
            {
                return i;
            }
        }

        throw new InvalidOperationException($"Column '{name}' not found.");
    }

    private static string Truncate(string s, int n) => s.Length <= n ? s : s[..n] + "…";
}
