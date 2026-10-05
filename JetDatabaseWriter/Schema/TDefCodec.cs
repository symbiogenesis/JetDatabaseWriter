namespace JetDatabaseWriter.Schema;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Parses the logical TDEF image once, retaining raw bytes and tolerant parse issues.</summary>
internal static class TDefCodec
{
    /// <summary>Reads only structural counts, preserving the short-header bails of index readers.</summary>
    /// <param name="format">The database's format profile.</param>
    /// <param name="td">Logical bytes through the final index count field.</param>
    /// <returns>The declared counts.</returns>
    internal static TDefCounts ReadCounts(JetFormat format, byte[] td)
        => new(Ru16(td, format.TDef.NumCols), Ri32(td, format.TDef.NumIdx), Ri32(td, format.TDef.NumRealIdx));

    /// <summary>Reads declared header values without interpreting their validity.</summary>
    /// <param name="format">The database's format profile.</param>
    /// <param name="td">A logical TDEF with a complete header.</param>
    /// <returns>The declared header.</returns>
    internal static TDefHeader ReadHeader(JetFormat format, byte[] td)
    {
        var layout = format.TDefFormat.Header;
        return new TDefHeader(
            Ru16(td, layout.NumCols),
            Ri32(td, layout.NumIdx),
            Ri32(td, layout.NumRealIdx),
            new TableCounters(
                Ru32(td, layout.NumRows),
                Ru32(td, layout.AutoNumber),
                layout.ComplexAutoNumber < 0 ? 0 : Ru32(td, layout.ComplexAutoNumber)))
        {
            LogicalLength = Ri32(td, 8),
            TableType = td[layout.TableType],
            MaximumColumnCount = Ru16(td, layout.MaxCols),
            VariableColumnCount = Ru16(td, layout.NumVarCols),
            OwnedPagesReference = Ru32(td, layout.UsedPages),
            FreePagesReference = Ru32(td, layout.FreePages),
        };
    }

    /// <summary>Returns null for an incomplete header or column section; malformed names preserve empty-name columns.</summary>
    /// <param name="format">The database's format profile.</param>
    /// <param name="td">The logical chain bytes.</param>
    /// <returns>The owned immutable image, or null.</returns>
    internal static TDefImage? Parse(JetFormat format, byte[]? td)
    {
        List<ColumnInfo>? cols = ReadColumns(format, td, out TDefHeader header, out bool hasDeletedColumns, out TDefParseIssues issues, out int namePos);
        if (cols is null || td is null)
        {
            return null;
        }

        int numRealIdx = header.RealIndexCount;
        if (numRealIdx is < 0 or > Constants.TableDefinition.MaxIndexes)
        {
            numRealIdx = 0;
        }

        JetDatabaseWriter.Indexes.Models.IndexSectionAnchors? section = null;
        if (header.LogicalIndexCount is < 0 or > Constants.TableDefinition.MaxIndexes)
        {
            issues |= TDefParseIssues.InvalidLogicalIndexCount;
        }
        else if (namePos >= 0)
        {
            var anchors = format.Index.GetIndexSection(namePos, numRealIdx, header.LogicalIndexCount);
            if (anchors.LogIdxNamesStart <= td.Length)
            {
                section = anchors;
            }
            else
            {
                issues |= TDefParseIssues.TruncatedIndexDescriptors;
            }
        }

        var realIndexes = new List<TDefIndexDescriptor>();
        var logicalIndexes = new List<TDefLogicalIndexDescriptor>();
        if (section is { } indexSection)
        {
            for (int i = 0; i < numRealIdx; i++)
            {
                if (!format.Index.TryReadRealIdxSlotWithKeyColumns(td, indexSection.RealIdxDescStart, i, out var slot, out var keys))
                {
                    issues |= TDefParseIssues.TruncatedIndexDescriptors;
                    break;
                }

                realIndexes.Add(new TDefIndexDescriptor(slot, Ru32(td, slot.FirstDpOffset), keys, td.AsSpan(slot.PhysStart, format.Index.RealIdxPhysSize)));
            }

            int indexNamePosition = indexSection.LogIdxNamesStart;
            for (int i = 0; i < header.LogicalIndexCount; i++)
            {
                if (!format.Index.TryReadLogicalEntry(td, indexSection.LogIdxStart, i, out var entry))
                {
                    issues |= TDefParseIssues.TruncatedIndexDescriptors;
                    break;
                }

                if (format.ReadColumnName(td, ref indexNamePosition, out string indexName) < 0)
                {
                    issues |= TDefParseIssues.TruncatedIndexNames;
                }

                int offset = indexSection.LogIdxStart + (i * format.Index.LogicalEntrySize);
                logicalIndexes.Add(new TDefLogicalIndexDescriptor(entry, indexName, td.AsSpan(offset, format.Index.LogicalEntrySize)));
            }
        }

        return new TDefImage(td, header, cols, hasDeletedColumns, issues, section, realIndexes, logicalIndexes);
    }

    /// <summary>Projects columns without reading index descriptors or copying the whole logical image.</summary>
    /// <param name="format">The database's format profile.</param>
    /// <param name="td">The logical bytes.</param>
    /// <param name="header">The declared header.</param>
    /// <param name="hasDeletedColumns">Whether column numbers contain deleted-column gaps.</param>
    /// <param name="issues">The tolerant column-section issues.</param>
    /// <param name="realIndexDescriptorStart">The name-section end, or -1 for unreadable column names.</param>
    /// <returns>Owned columns, or null for an incomplete header or descriptor section.</returns>
    internal static List<ColumnInfo>? ReadColumns(
        JetFormat format,
        byte[]? td,
        out TDefHeader header,
        out bool hasDeletedColumns,
        out TDefParseIssues issues,
        out int realIndexDescriptorStart)
    {
        header = default;
        hasDeletedColumns = false;
        issues = TDefParseIssues.None;
        realIndexDescriptorStart = -1;
        if (td == null || td.Length < format.TDef.BlockEnd)
        {
            return null;
        }

        header = ReadHeader(format, td);
        int numCols = header.ColumnCount;
        int numRealIdx = header.RealIndexCount;

        // Safety: corrupt or unusual TDEFs can report absurd index counts
        if (numRealIdx is < 0 or > Constants.TableDefinition.MaxIndexes)
        {
            issues |= TDefParseIssues.InvalidRealIndexCount;
            numRealIdx = 0;
        }

        if (numCols > Constants.TableDefinition.MaxColumns)
        {
            return null;
        }

        // Column descriptors follow immediately after block + first real-idx entries
        int colStart = format.TDef.BlockEnd + (numRealIdx * format.TDef.RealIdxEntrySz);
        int namePos = colStart + (numCols * format.ColumnDescriptor.Size);

        if (namePos > td.Length)
        {
            return null;
        }

        var descriptors = new List<ParsedColumnDescriptor>(numCols);
        for (int i = 0; i < numCols; i++)
        {
            int o = colStart + (i * format.ColumnDescriptor.Size);
            if (o + format.ColumnDescriptor.Size > td.Length)
            {
                break;
            }

            var type = (ColumnType)td[o + format.ColumnDescriptor.TypeOff];

            // Extra flags byte at descriptor offset 16 (Jet4/ACE only — the
            // Jet3 18-byte descriptor has no such slot). Carries the Access
            // 2010+ calculated-column marker (Jackcess CALCULATED_EXT_FLAG_MASK
            // = 0xC0). Read unconditionally for Jet4/ACE so calc columns
            // round-trip through the schema-rewrite path; harmless for cols
            // Access wrote with the slot at zero.
            byte extraFlags = !format.IsJet3 && o + 16 < td.Length ? td[o + 16] : (byte)0;
            int misc = Ri32(td, o + format.ColumnDescriptor.MiscOff);

            // For Numeric the misc 4-byte slot reuses bytes 11/12
            // (descriptor-relative) to carry the declared precision and
            // scale Access shows in Design View. Same byte positions as
            // the Jackcess `FixedPointColumnDescriptor` parser. Other
            // column types leave these at 0.
            byte numericPrecision = type == NumericType ? td[o + format.ColumnDescriptor.MiscOff] : (byte)0;
            byte numericScale = type == NumericType ? td[o + format.ColumnDescriptor.MiscOff + 1] : (byte)0;

            descriptors.Add(new ParsedColumnDescriptor(
                type,
                Ru16(td, o + format.ColumnDescriptor.NumOff),
                Ru16(td, o + format.ColumnDescriptor.VarOff),
                Ru16(td, o + format.ColumnDescriptor.FixedOff),
                Ru16(td, o + format.ColumnDescriptor.SzOff),
                td[o + format.ColumnDescriptor.FlagsOff],
                extraFlags,
                misc,
                numericPrecision,
                numericScale,
                new JetDatabaseWriter.Indexes.Collation.TextSortOrder(
                    Ru16(td, o + (format.IsJet3 ? 9 : 11)),
                    format.IsJet3 ? (byte)0 : td[o + 14],
                    !format.IsJet3)));
        }

        // Column names follow directly after all descriptors (in TDEF / descriptor order).
        // Names MUST be read before sorting so each name maps to the correct descriptor.
        var cols = new List<ColumnInfo>(descriptors.Count);
        bool readNames = true;
        for (int i = 0; i < descriptors.Count; i++)
        {
            string name = string.Empty;
            if (readNames)
            {
                int nameLen = format.ReadColumnName(td, ref namePos, out string parsedName);
                if (nameLen >= 0)
                {
                    name = parsedName;
                }
                else
                {
                    issues |= TDefParseIssues.TruncatedColumnNames;
                    readNames = false;
                }
            }

            cols.Add(descriptors[i].ToColumnInfo(name, td.AsSpan(colStart + (i * format.ColumnDescriptor.Size), format.ColumnDescriptor.Size)));
        }

        // Sort by col_num AFTER names are assigned.
        cols.Sort((a, b) => a.ColNum.CompareTo(b.ColNum));

        // Detect deleted-column gaps: if ColNum sequence has gaps, flag it
        hasDeletedColumns = cols.Count >= 2
            && cols[^1].ColNum - cols[0].ColNum != cols.Count - 1;

        realIndexDescriptorStart = readNames ? namePos : -1;
        return cols;
    }

    /// <summary>Locates the physical index descriptors, preserving the caller's count and bail policy.</summary>
    /// <param name="format">The database's format.</param>
    /// <param name="td">The logical bytes.</param>
    /// <param name="numCols">The caller's accepted column count.</param>
    /// <param name="numRealIdx">The caller's accepted physical index count.</param>
    /// <returns>The section offset, or -1 for an unreadable name record.</returns>
    internal static int LocateRealIndexDescriptors(JetFormat format, byte[] td, int numCols, int numRealIdx)
    {
        int pos = format.TDef.BlockEnd + (numRealIdx * format.TDef.RealIdxEntrySz) + (numCols * format.ColumnDescriptor.Size);
        for (int i = 0; i < numCols; i++)
        {
            if (format.ReadColumnName(td, ref pos, out _) < 0)
            {
                return -1;
            }
        }

        return pos;
    }

    /// <summary>Decodes logical index names, preserving either the prefix or all declared slots.</summary>
    /// <param name="format">The format profile.</param>
    /// <param name="td">The logical bytes.</param>
    /// <param name="start">The name-section offset.</param>
    /// <param name="count">The caller's accepted logical index count.</param>
    /// <param name="preserveSlots">Whether malformed names leave empty slots rather than end the prefix.</param>
    /// <returns>The names in descriptor order.</returns>
    internal static List<string> ReadLogicalIndexNames(JetFormat format, byte[] td, int start, int count, bool preserveSlots = false)
    {
        var names = new List<string>(count);
        int position = start;
        for (int i = 0; i < count; i++)
        {
            if (format.ReadColumnName(td, ref position, out string name) < 0 && !preserveSlots)
            {
                break;
            }

            names.Add(name);
        }

        return names;
    }

    /// <summary>One column descriptor as stored, before its name is read.</summary>
    /// <param name="Type">The column type.</param>
    /// <param name="ColNum">The column number.</param>
    /// <param name="VarIdx">The variable-length column index.</param>
    /// <param name="FixedOff">The offset in the row's fixed area.</param>
    /// <param name="Size">The declared size.</param>
    /// <param name="Flags">The descriptor flags.</param>
    /// <param name="ExtraFlags">The Jet4/ACE extra flags byte (descriptor offset 16).</param>
    /// <param name="Misc">The 4-byte misc slot.</param>
    /// <param name="NumericPrecision">The Numeric precision, or 0.</param>
    /// <param name="NumericScale">The Numeric scale, or 0.</param>
    /// <param name="TextSortOrder">The text sort order.</param>
    private readonly record struct ParsedColumnDescriptor(
        ColumnType Type,
        int ColNum,
        int VarIdx,
        int FixedOff,
        int Size,
        byte Flags,
        byte ExtraFlags,
        int Misc,
        byte NumericPrecision,
        byte NumericScale,
        JetDatabaseWriter.Indexes.Collation.TextSortOrder TextSortOrder)
    {
        internal ColumnInfo ToColumnInfo(string name, ReadOnlySpan<byte> rawDescriptor) => new(rawDescriptor)
        {
            Name = name,
            Type = this.Type,
            ColNum = this.ColNum,
            VarIdx = this.VarIdx,
            FixedOff = this.FixedOff,
            Size = this.Size,
            Flags = this.Flags,
            ExtraFlags = this.ExtraFlags,
            Misc = this.Misc,
            TextSortOrder = this.TextSortOrder,
            NumericPrecision = this.NumericPrecision,
            NumericScale = this.NumericScale,
        };
    }
}
