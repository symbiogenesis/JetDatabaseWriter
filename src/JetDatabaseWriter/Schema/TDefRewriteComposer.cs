namespace JetDatabaseWriter.Schema;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Combines rebuilt storage references with the original definition's uninterpreted metadata.</summary>
internal static class TDefRewriteComposer
{
    /// <summary>Retains the source header and trailer while projecting native long-value map identities.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="source">The original definition image.</param>
    /// <param name="replacement">The rebuilt definition, including its final index descriptors and maps.</param>
    /// <param name="mapColumnName">Maps surviving source column names to their replacement names.</param>
    /// <param name="maxLogicalBytes">The maximum admitted physical-chain payload, including padding.</param>
    /// <returns>The complete replacement logical bytes, with an updated declared length.</returns>
    /// <exception cref="JetCorruptDataException">A definition or surviving long-value map cannot be projected completely.</exception>
    /// <exception cref="JetLimitationException">The projected definition exceeds its configured byte budget.</exception>
    internal static byte[] Compose(JetFormat format, TDefImage source, byte[] replacement, Func<string, string?> mapColumnName, int maxLogicalBytes)
    {
        byte[] original = source.CopyBytes();
        TDefImage target = TDefCodec.Parse(format, replacement)
            ?? throw Corrupt("The replacement table definition cannot be parsed");
        if (!LongValueUsageMaps.TryRead(format, original, out List<LongValueUsageMap> sourceMaps, out int sourceSentinel)
            || !LongValueUsageMaps.TryRead(format, replacement, out List<LongValueUsageMap> targetMaps, out int targetSentinel))
        {
            throw Corrupt("The table-definition trailer cannot be read completely");
        }

        int sourceEnd = checked(Ri32(original, 8) + 8);
        int metadataEnd = targetMaps.Count == 0 ? targetSentinel : targetMaps[0].OwnedOffset - 2;
        var orderedMaps = new List<LongValueUsageMap>(targetMaps.Count);
        var remaining = new Dictionary<int, LongValueUsageMap>();
        foreach (LongValueUsageMap map in targetMaps)
        {
            remaining.Add(map.ColumnNumber, map);
        }

        foreach (LongValueUsageMap map in sourceMaps)
        {
            ColumnInfo sourceColumn = FindColumn(source.Columns, map.ColumnNumber)
                ?? throw Corrupt("A source long-value map names an undeclared column");

            string? name = mapColumnName(sourceColumn.Name);
            if (name is null)
            {
                continue;
            }

            ColumnInfo? targetColumn = FindColumn(target.Columns, name);
            if (targetColumn is null || !remaining.TryGetValue(targetColumn.ColNum, out LongValueUsageMap targetMap))
            {
                throw Corrupt("A surviving long-value column has no replacement map");
            }

            orderedMaps.Add(targetMap);
            _ = remaining.Remove(targetColumn.ColNum);
        }

        foreach (LongValueUsageMap map in targetMaps)
        {
            if (remaining.ContainsKey(map.ColumnNumber))
            {
                orderedMaps.Add(map);
            }
        }

        int trailerLength = sourceEnd - sourceSentinel;
        int usedLength = checked(metadataEnd + (orderedMaps.Count * 10) + trailerLength);
        int capacity = LogicalTDefChain.GetLogicalCapacity(format.PageSize, usedLength);
        if (capacity > maxLogicalBytes)
        {
            const string reason = "The projected table definition exceeds the configured definition-byte budget";
            throw new JetLimitationException(JetErrorCode.ValueTooLarge, reason + ".", new JetErrorInfo { PageNumber = source.TDefPageNumber, Reason = reason });
        }

        byte[] bytes = new byte[capacity];
        replacement.AsSpan(0, metadataEnd).CopyTo(bytes);
        original.AsSpan(0, format.TDef.BlockEnd).CopyTo(bytes);

        // Counts, row-layout limits, counters and storage references belong to
        // the rebuilt table. Every other source header byte remains opaque.
        CopyField(format.TDef.NumRows, sizeof(uint));
        CopyField(format.TDef.AutoNumber, sizeof(uint));
        CopyField(format.TDef.ComplexAutoNumber, sizeof(uint));
        CopyField(format.TDef.MaxCols, sizeof(ushort));
        CopyField(format.TDef.NumVarCols, sizeof(ushort));
        CopyField(format.TDef.NumCols, sizeof(ushort));
        CopyField(format.TDef.NumIdx, sizeof(uint));
        CopyField(format.TDef.NumRealIdx, sizeof(uint));
        CopyField(format.TDef.UsedPages, sizeof(uint));
        CopyField(format.TDef.FreePages, sizeof(uint));

        int position = metadataEnd;
        foreach (LongValueUsageMap map in orderedMaps)
        {
            replacement.AsSpan(map.OwnedOffset - 2, 10).CopyTo(bytes.AsSpan(position));
            position += 10;
        }

        original.AsSpan(sourceSentinel, trailerLength).CopyTo(bytes.AsSpan(position));
        Wi32(bytes, 8, usedLength - 8);
        return bytes;

        void CopyField(int offset, int length)
        {
            if (offset >= 0)
            {
                replacement.AsSpan(offset, length).CopyTo(bytes.AsSpan(offset));
            }
        }
    }

    private static ColumnInfo? FindColumn(IReadOnlyList<ColumnInfo> columns, int number)
    {
        foreach (ColumnInfo column in columns)
        {
            if (column.ColNum == number)
            {
                return column;
            }
        }

        return null;
    }

    private static ColumnInfo? FindColumn(IReadOnlyList<ColumnInfo> columns, string name)
    {
        foreach (ColumnInfo column in columns)
        {
            if (string.Equals(column.Name, name, StringComparison.OrdinalIgnoreCase))
            {
                return column;
            }
        }

        return null;
    }

    private static JetCorruptDataException Corrupt(string reason)
        => new(JetErrorCode.CorruptTableDefinition, reason + ".", new JetErrorInfo { Reason = reason });
}
