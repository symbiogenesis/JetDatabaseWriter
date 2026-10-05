namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>The frozen pre-codec column parser used as an independent parity oracle.</summary>
internal sealed class LegacyTDefParsers(JetFormat format)
{
    private readonly JetFormat format = format;

    internal TableDef? Parse(byte[]? td)
    {
        if (td == null || td.Length < this.format.TDef.BlockEnd)
        {
            return null;
        }

        int numCols = Ru16(td, this.format.TDef.NumCols);
        int numRealIdx = Ri32(td, this.format.TDef.NumRealIdx);

        // Safety: corrupt or unusual TDEFs can report absurd index counts
        if (numRealIdx is < 0 or > Constants.TableDefinition.MaxIndexes)
        {
            numRealIdx = 0;
        }

        if (numCols > Constants.TableDefinition.MaxColumns)
        {
            return null;
        }

        // Column descriptors follow immediately after block + first real-idx entries
        int colStart = this.format.TDef.BlockEnd + (numRealIdx * this.format.TDef.RealIdxEntrySz);
        int namePos = colStart + (numCols * this.format.ColumnDescriptor.Size);

        if (namePos > td.Length)
        {
            return null;
        }

        var descriptors = new List<ParsedColumnDescriptor>(numCols);
        for (int i = 0; i < numCols; i++)
        {
            int o = colStart + (i * this.format.ColumnDescriptor.Size);
            if (o + this.format.ColumnDescriptor.Size > td.Length)
            {
                break;
            }

            var type = (ColumnType)td[o + this.format.ColumnDescriptor.TypeOff];

            // Extra flags byte at descriptor offset 16 (Jet4/ACE only — the
            // Jet3 18-byte descriptor has no such slot). Carries the Access
            // 2010+ calculated-column marker (Jackcess CALCULATED_EXT_FLAG_MASK
            // = 0xC0). Read unconditionally for Jet4/ACE so calc columns
            // round-trip through the schema-rewrite path; harmless for cols
            // Access wrote with the slot at zero.
            byte extraFlags = !this.format.IsJet3 && o + 16 < td.Length ? td[o + 16] : (byte)0;
            int misc = Ri32(td, o + this.format.ColumnDescriptor.MiscOff);

            // For Numeric the misc 4-byte slot reuses bytes 11/12
            // (descriptor-relative) to carry the declared precision and
            // scale Access shows in Design View. Same byte positions as
            // the Jackcess `FixedPointColumnDescriptor` parser. Other
            // column types leave these at 0.
            byte numericPrecision = type == NumericType ? td[o + this.format.ColumnDescriptor.MiscOff] : (byte)0;
            byte numericScale = type == NumericType ? td[o + this.format.ColumnDescriptor.MiscOff + 1] : (byte)0;

            descriptors.Add(new ParsedColumnDescriptor(
                type,
                Ru16(td, o + this.format.ColumnDescriptor.NumOff),
                Ru16(td, o + this.format.ColumnDescriptor.VarOff),
                Ru16(td, o + this.format.ColumnDescriptor.FixedOff),
                Ru16(td, o + this.format.ColumnDescriptor.SzOff),
                td[o + this.format.ColumnDescriptor.FlagsOff],
                extraFlags,
                misc,
                numericPrecision,
                numericScale,
                new JetDatabaseWriter.Indexes.Collation.TextSortOrder(
                    Ru16(td, o + (this.format.IsJet3 ? 9 : 11)),
                    this.format.IsJet3 ? (byte)0 : td[o + 14],
                    !this.format.IsJet3)));
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
                int nameLen = this.format.ReadColumnName(td, ref namePos, out string parsedName);
                if (nameLen >= 0)
                {
                    name = parsedName;
                }
                else
                {
                    readNames = false;
                }
            }

            cols.Add(descriptors[i].ToColumnInfo(name));
        }

        // Sort by col_num AFTER names are assigned.
        cols.Sort((a, b) => a.ColNum.CompareTo(b.ColNum));

        // Detect deleted-column gaps: if ColNum sequence has gaps, flag it
        bool hasDeletedColumns = cols.Count >= 2
            && cols[^1].ColNum - cols[0].ColNum != cols.Count - 1;

        var tableDef = new TableDef
        {
            Columns = cols,
            RowCount = Ru32(td, this.format.TDef.NumRows),
            HasDeletedColumns = hasDeletedColumns,
        };
        tableDef.InitializeColumnMetadata();
        return tableDef;
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
        internal ColumnInfo ToColumnInfo(string name) => new()
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
