namespace JetDatabaseWriter.Schema;

using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Reads table definitions (TDEFs): the page chain of one table as logical
/// bytes, and its column descriptors and names parsed into a
/// <see cref="TableDef"/>. It reads through an <see cref="IPageSource"/>, so
/// over the writer's <see cref="Pager"/> it sees a transaction's pending TDEF
/// pages, and it never writes; the in-place TDEF write-backs stay with the
/// writer.
/// </summary>
/// <param name="pages">The page source the chain is read from.</param>
/// <param name="format">The file's format profile.</param>
internal sealed class TableDefReader(IPageSource pages, JetFormat format)
{
    /// <summary>
    /// Concatenates the TDEF page chain starting at <paramref name="startPage"/>
    /// into a single byte array. Pages after the first have their 8-byte
    /// TDEF header stripped before appending. Returns <see langword="null"/>
    /// when the page is not a valid TDEF root.
    /// </summary>
    /// <param name="startPage">The start page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The logical TDEF bytes, or <see langword="null"/>.</returns>
    internal async ValueTask<byte[]?> ReadTDefBytesAsync(long startPage, CancellationToken cancellationToken = default)
    {
        LogicalTDefChain? chain = await LogicalTDefChain.ReadAsync(
            startPage,
            format.PageSize,
            pages.ReadPageAsync,
            PageBuffers.Return,
            retainPageNumbers: false,
            cancellationToken).ConfigureAwait(false);

        return chain?.Bytes;
    }

    /// <summary>
    /// Reads the TDEF page chain starting at <paramref name="startPage"/> as a
    /// logical buffer that remembers its physical pages, so fields patched at
    /// logical offsets can be written back in place
    /// (<see cref="LogicalTDefChain.WriteInPlaceAsync"/>). Throws when the
    /// page is not a TDEF root.
    /// </summary>
    /// <param name="startPage">The first TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The chain.</returns>
    /// <exception cref="InvalidDataException">The page at <paramref name="startPage"/> is not a table definition.</exception>
    internal async ValueTask<LogicalTDefChain> ReadTDefChainAsync(long startPage, CancellationToken cancellationToken = default)
        => await LogicalTDefChain.ReadAsync(
            startPage,
            format.PageSize,
            pages.ReadPageAsync,
            PageBuffers.Return,
            retainPageNumbers: true,
            cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidDataException($"The table definition at page {startPage} could not be read.");

    /// <summary>
    /// Reads and parses the table definition rooted at <paramref name="tdefPage"/>:
    /// its columns (descriptors and names, sorted by column number), its row
    /// count and whether column numbers have gaps left by deleted columns.
    /// Returns <see langword="null"/> when the chain is not a TDEF, is too
    /// short, or declares more columns than a table can have.
    /// </summary>
    /// <param name="tdefPage">The first TDEF page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The table definition, or <see langword="null"/>.</returns>
    internal async ValueTask<TableDef?> ReadTableDefAsync(long tdefPage, CancellationToken cancellationToken = default)
    {
        byte[]? td = await this.ReadTDefBytesAsync(tdefPage, cancellationToken).ConfigureAwait(false);

        if (td == null || td.Length < format.TDef.BlockEnd)
        {
            return null;
        }

        int numCols = Ru16(td, format.TDef.NumCols);
        int numRealIdx = Ri32(td, format.TDef.NumRealIdx);

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
                numericScale));
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
            RowCount = Ru32(td, format.TDef.NumRows),
            HasDeletedColumns = hasDeletedColumns,
        };
        tableDef.InitializeColumnMetadata();
        return tableDef;
    }

    /// <summary>
    /// Reads the table definition rooted at <paramref name="tdefPage"/>, or
    /// throws when it cannot be read (<see cref="ReadTableDefAsync"/>).
    /// </summary>
    /// <param name="tdefPage">The first TDEF page.</param>
    /// <param name="tableName">The table's name, for the error message.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The table definition.</returns>
    /// <exception cref="InvalidDataException">The table definition could not be read.</exception>
    internal async ValueTask<TableDef> ReadRequiredTableDefAsync(long tdefPage, string tableName, CancellationToken cancellationToken = default)
        => await this.ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidDataException($"Table definition for '{tableName}' could not be read.");

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
        byte NumericScale)
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
            NumericPrecision = this.NumericPrecision,
            NumericScale = this.NumericScale,
        };
    }
}
