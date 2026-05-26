namespace JetDatabaseWriter.FormatProbe.LinkedTables;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using JetDatabaseWriter;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.FormatProbe;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;

internal static class LinkedOdbcLvPropProbe
{
    private const string ProbeSlug = "linked-odbc-lvprop";
    private const int MaxValuePreviewBytes = 72;
    private const int MaxCellCharacters = 240;

    public static async Task<int> RunAsync(string fixturesDir, string outFile)
    {
        var sb = new StringBuilder();
        AppendHeader(sb);
        AppendUpstreamFindings(sb);

        string odbcFixture = Path.Combine(fixturesDir, "Jackcess", "V2007", "odbcLinkerTestV2007.accdb");
        string accessFixture = Path.Combine(fixturesDir, "Jackcess", "V2007", "linkerTestV2007.accdb");
        string? customFixture = Environment.GetEnvironmentVariable("DIAG_LINKED_ODBC_LVPROP_PATH");

        int failures = 0;
        failures += await AnalyzeDatabaseAsync(sb, "Jackcess ODBC linked-table fixture", odbcFixture);
        failures += await AnalyzeDatabaseAsync(sb, "Jackcess Access linked-table comparison fixture", accessFixture);
        if (!string.IsNullOrWhiteSpace(customFixture))
        {
            failures += await AnalyzeDatabaseAsync(sb, "Custom DIAG_LINKED_ODBC_LVPROP_PATH fixture", customFixture);
        }

        await FormatProbeArtifacts.WriteAllTextAsync(outFile, sb.ToString());
        Console.WriteLine(FormattableString.Invariant($"[{ProbeSlug}] wrote {outFile}"));
        return failures == 0 ? 0 : 1;
    }

    private static void AppendHeader(StringBuilder sb)
    {
        _ = sb.AppendLine("# Linked ODBC LvProp schema-cache probe");
        _ = sb.AppendLine();
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Generated: {DateTimeOffset.UtcNow:u}");
        _ = sb.AppendLine("- Command: `dotnet run --project JetDatabaseWriter.FormatProbe -- linked-odbc-lvprop`");
        _ = sb.AppendLine("- Optional custom input: set `DIAG_LINKED_ODBC_LVPROP_PATH` to an Access-authored linked-table database.");
        _ = sb.AppendLine();
        _ = sb.AppendLine("This probe keeps Access-authored Type 4 ODBC `MSysObjects.LvProp` caches visible so generated writer output can be compared against real schema-cache shapes.");
        _ = sb.AppendLine();
    }

    private static void AppendUpstreamFindings(StringBuilder sb)
    {
        _ = sb.AppendLine("## Upstream implementation check");
        _ = sb.AppendLine();
        _ = sb.AppendLine("| Project | Relevant code | Finding | Upstream generation gap | ");
        _ = sb.AppendLine("|---|---|---|---|");
        _ = sb.AppendLine("| mdbtools | `src/libmdb/props.c`, `src/util/mdb-prop.c`, `src/libmdb/catalog.c` | Parses `KKD\\0`/`MR2\\0` chunks from `MSysObjects.LvProp`, exposes table/column properties, and has a property dumper. | No serializer or linked ODBC schema-cache generator was found; linked-table support is catalog listing level. |");
        _ = sb.AppendLine("| Jackcess | `DatabaseImpl`, `TableMetaData`, `PropertyMaps`, `LinkedODBCTableInfo` | Recognizes Type 4 linked ODBC tables, reads `Connect`/`ForeignName`, and reads/writes generic property maps. | Public linked-table creation targets Type 6 Access links; no Type 4 ODBC link creator or `NameMap`/schema-cache synthesizer was found. |");
        _ = sb.AppendLine();
    }

    private static async Task<int> AnalyzeDatabaseAsync(StringBuilder sb, string label, string path)
    {
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"## {EscapeMarkdown(label)}");
        _ = sb.AppendLine();
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Path: `{EscapeMarkdown(path)}`");

        if (!File.Exists(path))
        {
            _ = sb.AppendLine("- Status: missing fixture");
            _ = sb.AppendLine();
            return 1;
        }

        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions { UseLockFile = false },
            CancellationToken.None);

        DataTable catalog = await reader.ReadDataTableAsync("MSysObjects", cancellationToken: CancellationToken.None);
        List<LinkedCatalogRow> rows = ReadLinkedRows(catalog);
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Format: `{reader.DatabaseFormat}`");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Linked rows found: {rows.Count}");
        _ = sb.AppendLine();

        if (rows.Count == 0)
        {
            _ = sb.AppendLine("No Type 4/6 linked-table catalog rows were found.");
            _ = sb.AppendLine();
            return 0;
        }

        AppendCatalogSummary(sb, rows);
        foreach (LinkedCatalogRow row in rows)
        {
            AppendRowAnalysis(sb, row, reader.DatabaseFormat);
        }

        return 0;
    }

    private static List<LinkedCatalogRow> ReadLinkedRows(DataTable catalog)
    {
        var rows = new List<LinkedCatalogRow>();
        foreach (DataRow row in catalog.Rows)
        {
            int type = ToInt32(GetValue(row, "Type"));
            if (type != Constants.SystemObjects.LinkedOdbcType && type != Constants.SystemObjects.LinkedTableType)
            {
                continue;
            }

            rows.Add(new LinkedCatalogRow(
                ToInt32(GetValue(row, "Id")),
                ToText(GetValue(row, "Name")),
                type,
                ToInt32(GetValue(row, "Flags")),
                ToText(GetValue(row, "Database")),
                ToText(GetValue(row, "Connect")),
                ToText(GetValue(row, "ForeignName")),
                CopyBytes(GetValue(row, "Lv")),
                CopyBytes(GetValue(row, "LvProp")),
                CopyBytes(GetValue(row, "LvModule")),
                CopyBytes(GetValue(row, "LvExtra"))));
        }

        rows.Sort(static (left, right) =>
        {
            int typeCompare = left.Type.CompareTo(right.Type);
            return typeCompare != 0
                ? typeCompare
                : string.Compare(left.Name, right.Name, StringComparison.OrdinalIgnoreCase);
        });
        return rows;
    }

    private static object? GetValue(DataRow row, string columnName) =>
        row.Table.Columns.Contains(columnName) ? row[columnName] : DBNull.Value;

    private static string ToText(object? value) =>
        value is null or DBNull
            ? string.Empty
            : Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;

    private static int ToInt32(object? value) =>
        value is null or DBNull
            ? 0
            : Convert.ToInt32(value, CultureInfo.InvariantCulture);

    private static byte[]? CopyBytes(object? value) =>
        value is byte[] bytes ? (byte[])bytes.Clone() : null;

    private static void AppendCatalogSummary(StringBuilder sb, List<LinkedCatalogRow> rows)
    {
        _ = sb.AppendLine("### Catalog rows");
        _ = sb.AppendLine();
        _ = sb.AppendLine("| Name | Type | Flags | Database | Connect | ForeignName | Lv/LvProp/LvModule/LvExtra | ");
        _ = sb.AppendLine("|---|---:|---:|---|---|---|---|");
        foreach (LinkedCatalogRow row in rows)
        {
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| {Cell(row.Name)} | {row.Type} | `0x{row.Flags:X8}` | {Cell(row.Database)} | {Cell(row.Connect)} | {Cell(row.ForeignName)} | {BlobState(row.Lv)} / {BlobState(row.LvProp)} / {BlobState(row.LvModule)} / {BlobState(row.LvExtra)} |");
        }

        _ = sb.AppendLine();
    }

    private static void AppendRowAnalysis(StringBuilder sb, LinkedCatalogRow row, DatabaseFormat format)
    {
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"### `{EscapeMarkdown(row.Name)}` LvProp analysis");
        _ = sb.AppendLine();
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Catalog id: `{row.Id}`");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Type: `{row.Type}` ({DescribeType(row.Type)})");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Flags: `0x{row.Flags:X8}`");

        if (row.LvProp is null || row.LvProp.Length == 0)
        {
            _ = sb.AppendLine("- `LvProp`: null/empty");
            _ = sb.AppendLine();
            return;
        }

        byte[] lvProp = row.LvProp;
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- `LvProp`: {lvProp.Length} bytes, prefix `{ToHex(lvProp, 24)}`");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- SHA-256: `{Convert.ToHexString(SHA256.HashData(lvProp))}`");

        ColumnPropertyBlock? block = ColumnPropertyBlock.Parse(lvProp, format);
        if (block is null)
        {
            _ = sb.AppendLine("- Parser result: not a recognized `MR2\\0`/`KKD\\0` property block");
            _ = sb.AppendLine();
            return;
        }

        byte[]? rebuilt = ColumnPropertyBlockBuilder.FromBlock(block).ToBytes(format);
        ColumnPropertyBlock? reparsed = ColumnPropertyBlock.Parse(rebuilt, format);
        bool structuralParity = HasStructuralParity(block, reparsed);
        bool byteIdentical = rebuilt is not null && lvProp.SequenceEqual(rebuilt);

        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Parsed targets: {block.Targets.Count}");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Unknown chunks: {block.UnknownChunks.Count}");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Builder round-trip byte-identical: {byteIdentical}");
        _ = sb.AppendLine(CultureInfo.InvariantCulture, $"- Builder round-trip structural parity: {structuralParity}");
        _ = sb.AppendLine();

        AppendNameMapEntries(sb, block, format);
        AppendPropertyFrequency(sb, block);
        AppendTargetInventory(sb, block);
        AppendEntryDetails(sb, block, format);
    }

    private static string DescribeType(int type) => type switch
    {
        Constants.SystemObjects.LinkedOdbcType => "linked ODBC",
        Constants.SystemObjects.LinkedTableType => "linked Access/text",
        _ => "other",
    };

    private static bool HasStructuralParity(ColumnPropertyBlock expected, ColumnPropertyBlock? actual)
    {
        if (actual is null
            || expected.Targets.Count != actual.Targets.Count
            || expected.UnknownChunks.Count != actual.UnknownChunks.Count)
        {
            return false;
        }

        for (int targetIndex = 0; targetIndex < expected.Targets.Count; targetIndex++)
        {
            ColumnPropertyTarget expectedTarget = expected.Targets[targetIndex];
            ColumnPropertyTarget actualTarget = actual.Targets[targetIndex];
            if (!string.Equals(expectedTarget.Name, actualTarget.Name, StringComparison.Ordinal)
                || expectedTarget.ChunkType != actualTarget.ChunkType
                || expectedTarget.Entries.Count != actualTarget.Entries.Count)
            {
                return false;
            }

            for (int entryIndex = 0; entryIndex < expectedTarget.Entries.Count; entryIndex++)
            {
                ColumnPropertyEntry expectedEntry = expectedTarget.Entries[entryIndex];
                ColumnPropertyEntry actualEntry = actualTarget.Entries[entryIndex];
                if (!string.Equals(expectedEntry.Name, actualEntry.Name, StringComparison.Ordinal)
                    || expectedEntry.DataType != actualEntry.DataType
                    || expectedEntry.DdlFlag != actualEntry.DdlFlag
                    || !expectedEntry.Value.SequenceEqual(actualEntry.Value))
                {
                    return false;
                }
            }
        }

        for (int chunkIndex = 0; chunkIndex < expected.UnknownChunks.Count; chunkIndex++)
        {
            ColumnPropertyUnknownChunk expectedChunk = expected.UnknownChunks[chunkIndex];
            ColumnPropertyUnknownChunk actualChunk = actual.UnknownChunks[chunkIndex];
            if (expectedChunk.ChunkType != actualChunk.ChunkType
                || !expectedChunk.Payload.SequenceEqual(actualChunk.Payload))
            {
                return false;
            }
        }

        return true;
    }

    private static void AppendNameMapEntries(StringBuilder sb, ColumnPropertyBlock block, DatabaseFormat format)
    {
        List<(string Target, ColumnPropertyEntry Entry)> entries = block.Targets
            .SelectMany(static target => target.Entries
                .Where(static entry => string.Equals(entry.Name, "NameMap", StringComparison.OrdinalIgnoreCase))
                .Select(entry => (target.Name, entry)))
            .ToList();

        _ = sb.AppendLine("#### NameMap entries");
        _ = sb.AppendLine();
        if (entries.Count == 0)
        {
            _ = sb.AppendLine("No `NameMap` property entries were found.");
            _ = sb.AppendLine();
            return;
        }

        _ = sb.AppendLine("| Target | Type | DDL flag | Bytes | Prefix | Decoded string runs | ");
        _ = sb.AppendLine("|---|---:|---:|---:|---|---|");
        foreach ((string target, ColumnPropertyEntry entry) in entries)
        {
            string stringRuns = BuildStringRunSummary(entry.Value, format);
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| {Cell(target)} | `0x{entry.DataType:X2}` | `0x{entry.DdlFlag:X2}` | {entry.Value.Length} | `{ToHex(entry.Value, 64)}` | {Cell(stringRuns)} |");
        }

        _ = sb.AppendLine();
    }

    private static void AppendPropertyFrequency(StringBuilder sb, ColumnPropertyBlock block)
    {
        _ = sb.AppendLine("#### Property frequency");
        _ = sb.AppendLine();
        _ = sb.AppendLine("| Property | Entries | Targets | Types | ");
        _ = sb.AppendLine("|---|---:|---:|---|");

        var frequencies = block.Targets
            .SelectMany(static target => target.Entries.Select(entry => (Target: target.Name, Entry: entry)))
            .GroupBy(static item => item.Entry.Name, StringComparer.OrdinalIgnoreCase)
            .OrderByDescending(static group => group.Count())
            .ThenBy(static group => group.Key, StringComparer.OrdinalIgnoreCase);

        foreach (var group in frequencies)
        {
            string types = string.Join(", ", group.Select(static item => $"0x{item.Entry.DataType:X2}").Distinct(StringComparer.Ordinal));
            int targetCount = group.Select(static item => item.Target).Distinct(StringComparer.OrdinalIgnoreCase).Count();
            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| {Cell(group.Key)} | {group.Count()} | {targetCount} | `{types}` |");
        }

        _ = sb.AppendLine();
    }

    private static void AppendTargetInventory(StringBuilder sb, ColumnPropertyBlock block)
    {
        _ = sb.AppendLine("#### Target inventory");
        _ = sb.AppendLine();
        _ = sb.AppendLine("| # | Target | Chunk | Entries | Properties | ");
        _ = sb.AppendLine("|---:|---|---:|---:|---|");

        for (int targetIndex = 0; targetIndex < block.Targets.Count; targetIndex++)
        {
            ColumnPropertyTarget target = block.Targets[targetIndex];
            string properties = string.Join(", ", target.Entries.Select(static entry => entry.Name).Distinct(StringComparer.OrdinalIgnoreCase).Take(18));
            if (target.Entries.Select(static entry => entry.Name).Distinct(StringComparer.OrdinalIgnoreCase).Count() > 18)
            {
                properties += ", ...";
            }

            _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| {targetIndex} | {Cell(target.Name)} | `0x{(ushort)target.ChunkType:X4}` | {target.Entries.Count} | {Cell(properties)} |");
        }

        _ = sb.AppendLine();
    }

    private static void AppendEntryDetails(StringBuilder sb, ColumnPropertyBlock block, DatabaseFormat format)
    {
        _ = sb.AppendLine("#### Entry details");
        _ = sb.AppendLine();
        _ = sb.AppendLine("| Target | Property | Type | DDL flag | Bytes | Value preview | ");
        _ = sb.AppendLine("|---|---|---:|---:|---:|---|");

        foreach (ColumnPropertyTarget target in block.Targets)
        {
            foreach (ColumnPropertyEntry entry in target.Entries)
            {
                _ = sb.AppendLine(CultureInfo.InvariantCulture, $"| {Cell(target.Name)} | {Cell(entry.Name)} | `0x{entry.DataType:X2}` | `0x{entry.DdlFlag:X2}` | {entry.Value.Length} | {Cell(FormatValuePreview(entry, format))} |");
            }
        }

        _ = sb.AppendLine();
    }

    private static string FormatValuePreview(ColumnPropertyEntry entry, DatabaseFormat format)
    {
        ReadOnlySpan<byte> value = entry.Value;
        return entry.DataType switch
        {
            Constants.ColumnTypes.T_BOOL when value.Length >= 1 => value[0] == 0 ? "false" : "true",
            Constants.ColumnTypes.T_BYTE when value.Length >= 1 => value[0].ToString(CultureInfo.InvariantCulture),
            Constants.ColumnTypes.T_INT when value.Length >= sizeof(short) => BinaryPrimitives.ReadInt16LittleEndian(value).ToString(CultureInfo.InvariantCulture),
            Constants.ColumnTypes.T_LONG when value.Length >= sizeof(int) => BinaryPrimitives.ReadInt32LittleEndian(value).ToString(CultureInfo.InvariantCulture),
            Constants.ColumnTypes.T_FLOAT when value.Length >= sizeof(float) => BitConverter.Int32BitsToSingle(BinaryPrimitives.ReadInt32LittleEndian(value)).ToString("G9", CultureInfo.InvariantCulture),
            Constants.ColumnTypes.T_DOUBLE when value.Length >= sizeof(double) => BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(value)).ToString("G17", CultureInfo.InvariantCulture),
            Constants.ColumnTypes.T_DATETIME when value.Length >= sizeof(double) => FormatDateTime(value),
            Constants.ColumnTypes.T_TEXT or Constants.ColumnTypes.T_MEMO => DecodePropertyText(entry.Value, format),
            Constants.ColumnTypes.T_GUID when value.Length == 16 => new Guid(entry.Value).ToString("D", CultureInfo.InvariantCulture),
            Constants.ColumnTypes.T_BINARY when string.Equals(entry.Name, "GUID", StringComparison.OrdinalIgnoreCase) && value.Length == 16 => new Guid(entry.Value).ToString("D", CultureInfo.InvariantCulture),
            _ when string.Equals(entry.Name, "GUID", StringComparison.OrdinalIgnoreCase) && value.Length == 16 => new Guid(entry.Value).ToString("D", CultureInfo.InvariantCulture),
            _ => ToHex(entry.Value, MaxValuePreviewBytes),
        };
    }

    private static string FormatDateTime(ReadOnlySpan<byte> value)
    {
        double oaDate = BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(value));
        try
        {
            return FormattableString.Invariant($"{oaDate:G17} ({DateTime.FromOADate(oaDate):O})");
        }
        catch (ArgumentException)
        {
            return oaDate.ToString("G17", CultureInfo.InvariantCulture);
        }
    }

    private static string DecodePropertyText(byte[] value, DatabaseFormat format)
    {
        Encoding encoding = format == DatabaseFormat.Jet3Mdb ? Encoding.GetEncoding(1252) : Encoding.Unicode;
        return SanitizeText(encoding.GetString(value));
    }

    private static string BuildStringRunSummary(byte[] value, DatabaseFormat format)
    {
        List<string> runs = ExtractUtf16Runs(value);
        runs.AddRange(ExtractAsciiRuns(value));
        _ = format;

        return runs.Count == 0
            ? string.Empty
            : string.Join("; ", runs.Distinct(StringComparer.Ordinal).Select(static run => Truncate(run, 80)).Take(12));
    }

    private static List<string> ExtractUtf16Runs(byte[] value)
    {
        var runs = new List<string>();
        for (int alignment = 0; alignment < 2; alignment++)
        {
            var current = new StringBuilder();
            for (int offset = alignment; offset + 1 < value.Length; offset += 2)
            {
                char character = (char)BinaryPrimitives.ReadUInt16LittleEndian(value.AsSpan(offset));
                AppendOrFlushStringRun(runs, current, character);
            }

            FlushStringRun(runs, current);
        }

        return runs;
    }

    private static List<string> ExtractAsciiRuns(byte[] value)
    {
        var runs = new List<string>();
        var current = new StringBuilder();
        foreach (byte byteValue in value)
        {
            AppendOrFlushStringRun(runs, current, (char)byteValue);
        }

        FlushStringRun(runs, current);
        return runs;
    }

    private static void AppendOrFlushStringRun(List<string> runs, StringBuilder current, char character)
    {
        if (IsPrintableReportCharacter(character))
        {
            _ = current.Append(character);
            return;
        }

        FlushStringRun(runs, current);
    }

    private static void FlushStringRun(List<string> runs, StringBuilder current)
    {
        if (current.Length >= 3)
        {
            runs.Add(SanitizeText(current.ToString()));
        }

        _ = current.Clear();
    }

    private static bool IsPrintableReportCharacter(char character) =>
        character >= ' ' && character <= '~';

    private static string BlobState(byte[]? bytes) =>
        bytes is null || bytes.Length == 0
            ? "NULL"
            : FormattableString.Invariant($"byte[{bytes.Length}] `{ToHex(bytes, 16)}`");

    private static string ToHex(byte[] bytes, int maxBytes) =>
        ToHex(bytes.AsSpan(0, Math.Min(bytes.Length, maxBytes))) + (bytes.Length > maxBytes ? "..." : string.Empty);

    private static string ToHex(ReadOnlySpan<byte> bytes)
    {
        var sb = new StringBuilder(bytes.Length * 2);
        foreach (byte byteValue in bytes)
        {
            _ = sb.Append(byteValue.ToString("X2", CultureInfo.InvariantCulture));
        }

        return sb.ToString();
    }

    private static string Cell(string value) => EscapeMarkdown(Truncate(SanitizeText(value), MaxCellCharacters));

    private static string Truncate(string value, int maxCharacters) =>
        value.Length <= maxCharacters ? value : string.Concat(value.AsSpan(0, maxCharacters), "...");

    private static string EscapeMarkdown(string value) =>
        value.Replace("|", "\\|", StringComparison.Ordinal)
            .Replace("`", "'", StringComparison.Ordinal)
            .Replace("\r", "\\r", StringComparison.Ordinal)
            .Replace("\n", "\\n", StringComparison.Ordinal);

    private static string SanitizeText(string value)
    {
        var sb = new StringBuilder(value.Length);
        foreach (char character in value)
        {
            if (character == '\0')
            {
                _ = sb.Append("\\0");
            }
            else if (character < ' ' && character != '\t')
            {
                _ = sb.Append(CultureInfo.InvariantCulture, $"\\u{(int)character:X4}");
            }
            else if (character > '~')
            {
                _ = sb.Append(CultureInfo.InvariantCulture, $"\\u{(int)character:X4}");
            }
            else
            {
                _ = sb.Append(character);
            }
        }

        return sb.ToString();
    }

    private sealed record LinkedCatalogRow(
        int Id,
        string Name,
        int Type,
        int Flags,
        string Database,
        string Connect,
        string ForeignName,
        byte[]? Lv,
        byte[]? LvProp,
        byte[]? LvModule,
        byte[]? LvExtra);
}
