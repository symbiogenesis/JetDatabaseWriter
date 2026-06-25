namespace JetDatabaseWriter.Schema;

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Builds table-definition (TDEF) pages and the bootstrap bytes for a new,
/// empty database file.
/// </summary>
/// <param name="writer">The writer.</param>
internal sealed class TDefPageBuilder(AccessWriter writer)
{
    internal static TableDef BuildTableDefinition(IReadOnlyList<ColumnDefinition> columns, DatabaseFormat format)
    {
        var result = new TableDef();
        int fixedOffset = 0;
        int nextVarIndex = 0;

        for (int i = 0; i < columns.Count; i++)
        {
            ColumnDefinition definition = columns[i];
            ValidateCalculatedColumn(definition, format);
            ColumnType type = TypeCodeFromDefinition(definition);

            if (type == BigIntType && format != DatabaseFormat.AceAccdb)
            {
                throw new NotSupportedException(
                    $"Column '{definition.Name}': Int64/Large Number columns are only supported in ACCDB databases.");
            }

            if (type == DateTimeExtendedType && format != DatabaseFormat.AceAccdb)
            {
                throw new NotSupportedException(
                    $"Column '{definition.Name}': Date/Time Extended columns are only supported in ACCDB databases.");
            }

            bool isCalculated = definition.IsCalculated;
            bool variable = isCalculated || definition.ForceVariableLengthStorage || IsAlwaysVariableLength(type);
            int declaredSize = GetDeclaredSize(type, definition.MaxLength, format);
            int size = isCalculated ? GetCalculatedDeclaredSize(type, declaredSize) : declaredSize;

            byte flags;
            bool isComplex = type is AttachmentType or ComplexType;
            if (isComplex)
            {
                flags = Constants.ColumnDescriptorFlags.ComplexColumn;
            }
            else
            {
                // 0x02 = Jackcess UNKNOWN_FF_FLAG_MASK. DAO.DBEngine.120 always sets this
                // bit on every non-complex column descriptor and refuses to open tables
                // whose columns lack it ("Unrecognized database format" on the first
                // OpenRecordset). Set unconditionally to match Access/Jackcess output.
                flags = Constants.ColumnDescriptorFlags.Unknown;
                if (!variable)
                {
                    flags |= Constants.ColumnDescriptorFlags.Fixed;
                }

                // NOTE: nullability is NOT encoded in the TDEF flag byte. DAO/Access
                // refuse to open a table whose flag byte carries any unknown bits
                // (including the 0x08 NOT NULL marker an earlier writer revision used);
                // the constraint is persisted via the Boolean `Required` property in
                // MSysObjects.LvProp instead. See JetExpressionConverter.ApplyColumn.
                if (definition.IsAutoIncrement)
                {
                    flags |= Constants.ColumnDescriptorFlags.AutoNumber;
                }

                bool wantsHyperlink = definition.IsHyperlink || definition.ClrType == typeof(Hyperlink);
                if (wantsHyperlink)
                {
                    if (type != MemoType)
                    {
                        throw new ArgumentException(
                            $"Column '{definition.Name}' has IsHyperlink = true but resolves to JET type {GetTypeDisplayName(type)}; " +
                            "hyperlink columns must be MEMO (string with no MaxLength, or typeof(Hyperlink)).",
                            nameof(columns));
                    }

                    flags |= Constants.ColumnDescriptorFlags.Hyperlink;
                }
            }

            flags = definition.DescriptorFlagsOverride ?? flags;

            var column = new ColumnInfo
            {
                Name = definition.Name,
                Type = type,
                ColNum = i,
                VarIdx = variable ? nextVarIndex : 0,
                FixedOff = variable ? 0 : fixedOffset,
                Size = size,
                Flags = flags,
                Misc = isComplex ? definition.ComplexId : definition.DescriptorMiscOverride ?? 0,
                NumericPrecision = type == NumericType ? ResolveNumericPrecision(definition) : (byte)0,
                NumericScale = type == NumericType ? ResolveNumericScale(definition) : (byte)0,
                ExtraFlags = definition.DescriptorExtraFlagsOverride ?? GetExtraFlags(definition, type, format),
            };

            result.Columns.Add(column);

            if (variable)
            {
                nextVarIndex++;
            }
            else
            {
                fixedOffset += GetFixedSize(type);
            }
        }

        result.InitializeColumnMetadata();
        return result;
    }

    public byte[] BuildTDefPage(TableDef tableDef)
        => this.BuildTDefPageWithIndexOffsets(tableDef, []).Page;

    public byte[] BuildTDefPage(TableDef tableDef, IReadOnlyList<ResolvedIndex> indexes)
        => this.BuildTDefPageWithIndexOffsets(tableDef, indexes).Page;

    public (byte[] Page, int[] FirstDpOffsets) BuildTDefPageWithIndexOffsets(TableDef tableDef, IReadOnlyList<ResolvedIndex> indexes)
    {
        (byte[][]? pages, int[]? firstDpOffsets, int[] _) = this.BuildTDefPagesWithIndexOffsets(tableDef, indexes);
        if (pages.Length != 1)
        {
            throw new NotSupportedException(
                $"Table definition produced a {pages.Length}-page TDEF chain, but the single-page builder was used. "
                + "Route this caller through BuildTDefPagesWithIndexOffsets / the multi-page write path.");
        }

        return (pages[0], firstDpOffsets);
    }

    public (byte[][] Pages, int[] FirstDpLogicalOffsets, int[] UsedPagesLogicalOffsets) BuildTDefPagesWithIndexOffsets(TableDef tableDef, IReadOnlyList<ResolvedIndex> indexes)
    {
        int logicalCapacity = Math.Max(writer.PageSizeBytes * 32, writer.PageSizeBytes);
        byte[] page = new byte[logicalCapacity];
        int numCols = tableDef.Columns.Count;
        int numIdx = indexes.Count;
        bool jet4 = writer.Format != DatabaseFormat.Jet3Mdb;
        int numRealIdx = numIdx;

        int colStart = writer.TDef.BlockEnd + (numRealIdx * writer.TDef.RealIdxEntrySz);
        int namePos = colStart + (numCols * writer.ColumnDescriptor.Size);
        int nameLenSize = jet4 ? 2 : 1;

        page[0] = Constants.PageTypes.TableDefinition;
        page[1] = 0x01;
        page[writer.TDef.NumCols - 5] = 0x4E;
        Wu16(page, writer.TDef.NumCols - 4, numCols);
        Wu16(page, writer.TDef.NumCols, numCols);
        Wi32(page, writer.TDef.NumCols + 2, numIdx);
        Wi32(page, writer.TDef.NumRealIdx, numRealIdx);

        int numVarCols = 0;
        for (int i = 0; i < numCols; i++)
        {
            ColumnInfo col = tableDef.Columns[i];
            int o = colStart + (i * writer.ColumnDescriptor.Size);

            if (!col.IsFixed)
            {
                numVarCols++;
            }

            page[o + writer.ColumnDescriptor.TypeOff] = (byte)col.Type;
            if (jet4)
            {
                Wi32(page, o + 1, Constants.TableDefinition.Jet4.FormatMagic);
            }

            Wu16(page, o + writer.ColumnDescriptor.NumOff, col.ColNum);
            Wu16(page, o + writer.ColumnDescriptor.VarOff, col.VarIdx);

            if (jet4)
            {
                // Jet4/ACE column descriptor stores col_num twice: once at offset 5-6
                // (NumOff above) and again at offset 9-10 (the "redundant col_num"
                // field per mdbtools HACKING.md). DAO OpenRecordset reads the second
                // copy and rejects the table with "Unrecognized database format ''."
                // when the two disagree. Verified against DAO-authored
                // NorthwindTraders.accdb in WriterColumnDescriptorRedundantColNumTests.
                Wu16(page, o + 9, col.ColNum);
            }

            page[o + writer.ColumnDescriptor.FlagsOff] = col.Flags;
            Wu16(page, o + writer.ColumnDescriptor.FixedOff, col.FixedOff);
            Wu16(page, o + writer.ColumnDescriptor.SzOff, col.Size);

            if (col.Type is AttachmentType or ComplexType)
            {
                Wi32(page, o + writer.ColumnDescriptor.MiscOff, col.Misc);
            }
            else if (col.Type == NumericType && writer.Format != DatabaseFormat.Jet3Mdb)
            {
                if (!col.IsCalculated)
                {
                    page[o + writer.ColumnDescriptor.MiscOff] = col.NumericPrecision;
                    page[o + writer.ColumnDescriptor.MiscOff + 1] = col.NumericScale;
                }
                else
                {
                    page[o + writer.ColumnDescriptor.FlagsOff + 1] = col.ExtraFlags;
                }
            }
            else if (jet4 && (col.Type == TextType || col.Type == MemoType))
            {
                // Jet4/ACE text columns require two extra fields that DAO populates
                // unconditionally; without them DAO refuses to OpenRecordset on the
                // table ("Unrecognized database format"). See docs/design/round-trip-openrecordset-hypothesis.md.
                //   col_desc + 11..12 (misc / sort_order, 2 bytes): collation LCID
                //                     low word. 0x0409 = "General" sort order,
                //                     en-US (LCID 1033). The 4-byte write below also
                //                     stamps misc_ext at +13..14 to zero, which DAO
                //                     accepts for the legacy "version 0" sort order.
                //   col_desc + 16     (ExtraFlags / misc_flags, 1 byte): bit 0x01 =
                //                     COMPRESSED_UNICODE_EXT_FLAG_MASK. DAO emits 0x00
                //                     for new TEXT/MEMO columns (verified via
                //                     DaoBaselineProbe), so this bit is opt-in via
                //                     ColumnDefinition.IsCompressedUnicode and surfaced
                //                     here through ColumnInfo.ExtraFlags. The reader
                //                     decodes the FF FE compressed marker regardless of
                //                     the bit.
                Wi32(page, o + writer.ColumnDescriptor.MiscOff, 0x00000409);
                page[o + writer.ColumnDescriptor.FlagsOff + 1] = col.ExtraFlags;
            }
            else if (jet4 && col.IsCalculated)
            {
                page[o + writer.ColumnDescriptor.FlagsOff + 1] = col.ExtraFlags;
            }
            else if (jet4)
            {
                if (col.Misc != 0)
                {
                    Wi32(page, o + writer.ColumnDescriptor.MiscOff, col.Misc);
                }

                if (col.ExtraFlags != 0)
                {
                    page[o + writer.ColumnDescriptor.FlagsOff + 1] = col.ExtraFlags;
                }
            }

            byte[] nameBytes = jet4 ? Encoding.Unicode.GetBytes(col.Name) : writer.AnsiEncoding.GetBytes(col.Name);
            if (namePos + nameLenSize + nameBytes.Length > page.Length)
            {
                throw new NotSupportedException(
                    "Table definition exceeds the TDEF logical-buffer capacity. Increase "
                    + "BuildTDefPagesWithIndexOffsets's logicalCapacity or reduce the column count.");
            }

            if (jet4)
            {
                Wu16(page, namePos, nameBytes.Length);
            }
            else
            {
                page[namePos] = (byte)nameBytes.Length;
            }

            namePos += nameLenSize;
            Buffer.BlockCopy(nameBytes, 0, page, namePos, nameBytes.Length);
            namePos += nameBytes.Length;
        }

        Wu16(page, writer.TDef.NumCols - 2, numVarCols);

        int[] firstDpOffsets = numIdx > 0 ? new int[numIdx] : [];
        int[] usedPagesOffsets = numIdx > 0 ? new int[numIdx] : [];
        if (numIdx > 0)
        {
            int realIdxPhysStart = namePos;
            IndexSectionAnchors anchors = writer.IndexLayoutInfo.GetIndexSection(realIdxPhysStart, numRealIdx, numIdx);
            int totalIdxBytesLowerBound = anchors.LogIdxNamesStart - realIdxPhysStart;
            if (realIdxPhysStart + totalIdxBytesLowerBound > page.Length)
            {
                throw new NotSupportedException(
                    "Table definition (with indexes) exceeds the TDEF logical-buffer capacity. Increase "
                    + "BuildTDefPagesWithIndexOffsets's logicalCapacity or reduce the index count.");
            }

            for (int i = 0; i < numIdx; i++)
            {
                ResolvedIndex ri = indexes[i];
                int phys = writer.IndexLayoutInfo.RealIdxPhysOffset(realIdxPhysStart, i);
                if (jet4)
                {
                    Wi32(page, phys, Constants.TableDefinition.Jet4.RealIdx.LeadingMagic);
                }

                for (int slot = 0; slot < Constants.TableDefinition.ColMapSlotCount; slot++)
                {
                    int so = writer.IndexLayoutInfo.ColMapSlotOffset(phys, slot);
                    if (slot < ri.ColumnNumbers.Count)
                    {
                        Wu16(page, so, ri.ColumnNumbers[slot]);
                        page[so + 2] = ri.Ascending[slot]
                            ? Constants.TableDefinition.ColMapAscendingFlag
                            : Constants.TableDefinition.ColMapDescendingFlag;
                    }
                    else
                    {
                        Wu16(page, so, Constants.TableDefinition.ColMapPaddingSlot);
                        page[so + 2] = Constants.TableDefinition.ColMapDescendingFlag;
                    }
                }

                byte flagsByte = Constants.TableDefinition.UnknownIndexFlag;
                if (ri.IsPrimaryKey)
                {
                    flagsByte |= Constants.TableDefinition.UniqueIndexFlag | Constants.TableDefinition.RequiredIndexFlag;
                }
                else if (ri.IsUnique)
                {
                    flagsByte |= Constants.TableDefinition.UniqueIndexFlag;
                }

                if (ri.IgnoreNulls)
                {
                    flagsByte |= Constants.TableDefinition.IgnoreNullsIndexFlag;
                }

                if (ri.IsRequired && !ri.IsPrimaryKey)
                {
                    flagsByte |= Constants.TableDefinition.RequiredIndexFlag;
                }

                page[writer.IndexLayoutInfo.FlagsAbsoluteOffset(phys)] = flagsByte;
                if (jet4)
                {
                    usedPagesOffsets[i] = writer.IndexLayoutInfo.FirstDpAbsoluteOffset(phys) - 4;
                }

                firstDpOffsets[i] = writer.IndexLayoutInfo.FirstDpAbsoluteOffset(phys);

                int log = writer.IndexLayoutInfo.LogicalIdxFieldsOffset(anchors.LogIdxStart, i);
                if (jet4)
                {
                    Wi32(page, log - writer.IndexLayoutInfo.LogicalEntryFieldsOffset, Constants.TableDefinition.Jet4.FormatMagic);
                }

                Wi32(page, log + Constants.TableDefinition.Jet3.LogicalIdx.IndexNumOffset, i);
                Wi32(page, log + Constants.TableDefinition.Jet3.LogicalIdx.IndexNum2Offset, i);
                Wi32(page, log + Constants.TableDefinition.Jet3.LogicalIdx.RelIdxNumOffset, -1);

                // DAO-authored TDEFs always set the cascade_ups / cascade_dels bytes
                // to 0x04 even on non-FK indexes (PK and regular). The exact semantic of
                // 0x04 is undocumented but DAO refuses to OpenRecordset on tables whose
                // PK index has 0x00 here ("Unrecognized database format").
                page[log + Constants.TableDefinition.Jet3.LogicalIdx.CascadeUpsOffset] = 0x04;
                page[log + Constants.TableDefinition.Jet3.LogicalIdx.CascadeDelsOffset] = 0x04;

                page[log + Constants.TableDefinition.Jet3.LogicalIdx.IndexTypeOffset] = (byte)(ri.IsPrimaryKey ? IndexKind.PrimaryKey : IndexKind.Normal);
            }

            int npos = anchors.LogIdxNamesStart;
            for (int i = 0; i < numIdx; i++)
            {
                byte[] nameBytes = jet4 ? Encoding.Unicode.GetBytes(indexes[i].Name) : writer.AnsiEncoding.GetBytes(indexes[i].Name);
                if (npos + nameLenSize + nameBytes.Length > page.Length)
                {
                    throw new NotSupportedException(
                        "Table definition (with indexes) exceeds the TDEF logical-buffer capacity. Increase "
                        + "BuildTDefPagesWithIndexOffsets's logicalCapacity or reduce the index count.");
                }

                if (jet4)
                {
                    Wu16(page, npos, nameBytes.Length);
                }
                else
                {
                    page[npos] = (byte)nameBytes.Length;
                }

                npos += nameLenSize;
                Buffer.BlockCopy(nameBytes, 0, page, npos, nameBytes.Length);
                npos += nameBytes.Length;
            }

            // DAO writes a single 0xFFFF "no usage-map / no-such-page" sentinel
            // immediately after the last index name. Required for OpenRecordset.
            if (jet4 && numIdx > 0)
            {
                Wu16(page, npos, 0xFFFF);
                npos += 2;
            }

            // H48 (round-trip-openrecordset-hypothesis.md §3): DAO-authored
            // RT_Customers TDEFs reserve 8 trailing zero bytes after the FFFF
            // sentinel and include them in tdef_len. Empirically the page bytes
            // there are already zero (BuildTDefPagesWithIndexOffsets's logical
            // buffer is zero-initialised); we just need to advance namePos so
            // the tdef_len/freeSpace calculations below count those 8 bytes.
            if (jet4 && numIdx > 0)
            {
                npos += 8;
            }

            namePos = npos;
        }

        Wi32(page, 8, Math.Max(0, namePos - 8));
        if (jet4)
        {
            Wi32(page, 0x0C, Constants.TableDefinition.Jet4.FormatMagic);
            int tdefLen = Math.Max(0, namePos - 8);
            Wu16(page, 2, Math.Max(0, writer.PageSizeBytes - tdefLen - 8));
        }

        (byte[][]? pages, int[]? logicalFirstDpOffsets) = this.SplitLogicalTDefIntoPages(page, namePos, firstDpOffsets);
        return (pages, logicalFirstDpOffsets, usedPagesOffsets);
    }

    /// <summary>
    /// Writes a 4-byte little-endian integer at the given LOGICAL TDEF
    /// offset, dispatching the bytes across the physical page boundary
    /// when the field straddles two pages. Used to patch <c>first_dp</c>
    /// values after their leaf pages are appended.
    /// </summary>
    /// <param name="pages">The pages.</param>
    /// <param name="logicalOffset">The logical offset.</param>
    /// <param name="value">The value.</param>
    internal void WriteLogicalTDefI32(byte[][] pages, int logicalOffset, int value)
    {
        for (int i = 0; i < 4; i++)
        {
            (int pageIdx, int pageOff) = this.LogicalToPhysicalTDefOffset(logicalOffset + i);
            pages[pageIdx][pageOff] = (byte)((value >> (i * 8)) & 0xFF);
        }
    }

    /// <summary>
    /// Writes a 3-byte little-endian unsigned integer at the given LOGICAL
    /// TDEF offset, dispatching the bytes across the physical page boundary
    /// when the field straddles two pages. Used to patch <c>used_pages</c>
    /// usage-map page pointers.
    /// </summary>
    /// <param name="pages">The pages.</param>
    /// <param name="logicalOffset">The logical offset.</param>
    /// <param name="value">The value.</param>
    internal void WriteLogicalTDefUInt24(byte[][] pages, int logicalOffset, int value)
    {
        for (int i = 0; i < 3; i++)
        {
            (int pageIdx, int pageOff) = this.LogicalToPhysicalTDefOffset(logicalOffset + i);
            pages[pageIdx][pageOff] = (byte)((value >> (i * 8)) & 0xFF);
        }
    }

    /// <summary>
    /// Adjusts the persisted row count of the table whose TDEF lives at
    /// <paramref name="tdefPage"/> by <paramref name="delta"/>, mirroring the
    /// change into every per-real-idx <c>num_idx_rows</c> counter.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="delta">The signed row-count delta.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask AdjustTDefRowCountAsync(long tdefPage, long delta, CancellationToken cancellationToken)
    {
        if (delta == 0)
        {
            return;
        }

        byte[] page = await writer.ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        long updated;

        try
        {
            uint current = Ru32(page, Constants.TableDefinition.RowCountOffset);
            updated = Math.Clamp(current + delta, 0L, uint.MaxValue);
            Wi32(page, Constants.TableDefinition.RowCountOffset, unchecked((int)(uint)updated));

            // Mirror the change into the per-real-idx `num_idx_rows` counter
            // (offset +4 of each 12-byte/8-byte slot in the leading real-idx
            // skip block at [_tdef.BlockEnd, _tdef.BlockEnd + numRealIdx *
            // _tdef.RealIdxEntrySz)). Per mdbtools HACKING.md the slot is laid
            // out as `unknown(4) + num_idx_rows(4) + unknown(4)`. DAO compares
            // num_idx_rows against the leaf-level row count when walking
            // MSysObjects; if they disagree it aborts compact with
            // "could not find the object 'MSysDb'" — see
            // docs/design/round-trip-openrecordset-hypothesis.md.
            int numRealIdx = Ri32(page, writer.TDef.NumRealIdx);
            if (numRealIdx is > 0 and <= Constants.TableDefinition.MaxIndexes)
            {
                int slotEnd = writer.TDef.BlockEnd + (numRealIdx * writer.TDef.RealIdxEntrySz);
                if (slotEnd <= page.Length)
                {
                    for (int i = 0; i < numRealIdx; i++)
                    {
                        int countOff = writer.TDef.BlockEnd + (i * writer.TDef.RealIdxEntrySz) + 4;
                        uint cur = Ru32(page, countOff);
                        long next = Math.Clamp(cur + delta, 0L, uint.MaxValue);
                        Wi32(page, countOff, unchecked((int)(uint)next));
                    }
                }
            }

            await writer.WritePageAsync(tdefPage, page, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            AccessBase.ReturnPage(page);
        }
    }

    public (int PageIndex, int PageOffset) LogicalToPhysicalTDefOffset(int logicalOffset)
        => LogicalTDefChain.LogicalToPhysicalOffset(writer.PageSizeBytes, logicalOffset);

    /// <summary>
    /// Builds a minimal, empty JET database as a byte array.
    /// The bootstrap image contains three pages (page size varies by format):
    /// page 0 (header), page 1 (global usage map), and page 2 (MSysObjects TDEF).
    /// The <see cref="AccessWriter.CreateDatabaseAsync(string, DatabaseFormat, AccessWriterOptions?, System.Threading.CancellationToken)"/> overloads add
    /// full-catalog ACCDB system tables after opening this minimal image.
    /// </summary>
    /// <param name="format">Target on-disk format.</param>
    /// <param name="fullCatalogSchema">
    /// When <see langword="true"/>, page 2 is bootstrapped with the real Access
    /// 17-column <c>MSysObjects</c> schema (matches files written by Microsoft
    /// Access across all Jet/ACE versions). When <see langword="false"/>, the
    /// historical 9-column slim schema is written instead.
    /// </param>
    /// <exception cref="NotImplementedException">Thrown when an unsupported database format is specified.</exception>
    internal static byte[] BuildEmptyDatabase(DatabaseFormat format, bool fullCatalogSchema)
    {
        int pgSz = AccessBase.GetPageSize(format);
        byte[] db = new byte[pgSz * 3];

        db[0] = 0x00;
        db[1] = 0x01;
        db[2] = 0x00;
        db[3] = 0x00;

        byte[] magic = format == DatabaseFormat.AceAccdb
            ? Encoding.ASCII.GetBytes("Standard ACE DB\0")
            : Encoding.ASCII.GetBytes("Standard Jet DB\0");
        Buffer.BlockCopy(magic, 0, db, 4, magic.Length);

        db[0x14] = format switch
        {
            DatabaseFormat.Jet3Mdb => 0x00,
            DatabaseFormat.Jet4Mdb => 0x01,
            DatabaseFormat.AceAccdb => 0x02,
            _ => throw new NotImplementedException($"Unsupported database format: {format}"),
        };

        BuildGlobalUsageMapPage(db, pgSz, format);
        BuildMSysObjectsTDef(db, pgSz * 2, format, fullCatalogSchema);

        if (format != DatabaseFormat.Jet3Mdb)
        {
            WriteJet4AceHeaderDefaults(db);
            EncryptionManager.TransformHeaderMask(db);
        }

        return db;
    }

    private static void WriteJet4AceHeaderDefaults(byte[] db)
    {
        db[0x19] = 0x01;
        db[0x1C] = 0x01;
        db[0x1D] = 0x01;
        Wi32(db, 0x20, 2);
        Wi32(db, 0x24, 3);
        Wi32(db, 0x28, 4);
        Wi32(db, 0x2C, 5);
        Wu16(db, 0x3C, 1252);

        for (int offset = 0x42; offset < 0x6A; offset += 4)
        {
            db[offset] = 0x53;
            db[offset + 1] = 0xB4;
        }

        Wu16(db, 0x6A, 0x11A6);
        Wu16(db, 0x6E, 0x0409);

        ReadOnlySpan<byte> creationDate = [0x08, 0x6E, 0x41, 0x1D, 0x7C, 0x8A, 0xE6, 0x40];
        creationDate.CopyTo(db.AsSpan(0x72));

        Wi32(db, 0x98, 0x0654);
        db[0x9C] = (byte)'4';
        db[0x9D] = (byte)'.';
        db[0x9E] = (byte)'0';
    }

    private static void BuildGlobalUsageMapPage(byte[] db, int pgSz, DatabaseFormat format)
    {
        var dataPage = DataPageLayout.For(format);
        int pageOffset = pgSz;
        int rowStart = pgSz - 69;
        int row1Start = rowStart - 69;
        int slotTableEnd = dataPage.RowsStart + 4;

        db[pageOffset] = 0x01;
        db[pageOffset + 1] = 0x01;
        Wu16(db, pageOffset + 2, row1Start - slotTableEnd);
        Wi32(db, pageOffset + dataPage.TDefOff, 1);
        Wu16(db, pageOffset + dataPage.NumRows, 2);
        Wu16(db, pageOffset + dataPage.RowsStart, rowStart);
        Wu16(db, pageOffset + dataPage.RowsStart + 2, row1Start);
        db[pageOffset + rowStart] = 0x00;
        Wi32(db, pageOffset + rowStart + 1, 0);
        db[pageOffset + row1Start] = 0x00;
        Wi32(db, pageOffset + row1Start + 1, 0);
    }

    private static int GetDeclaredSize(ColumnType type, int maxLength, DatabaseFormat format)
        => type switch
        {
            BooleanType => 0,
            ByteType => 1,
            IntegerType => 2,
            LongIntegerType => 4,
            MoneyType => 8,
            FloatType => 4,
            DoubleType => 8,
            DateTimeType => 8,
            BigIntType => 8,
            GuidType => 16,
            NumericType => 17,
            TextType => GetTextDeclaredSize(maxLength, format),
            BinaryType => maxLength > 0 ? maxLength : 255,
            AttachmentType or ComplexType => 4,
            DateTimeExtendedType => GetFixedSize(DateTimeExtendedType),
            OleType or
            MemoType or
            _ => 0,
        };

    private static int GetTextDeclaredSize(int maxLength, DatabaseFormat format)
    {
        int effectiveLength = maxLength > 0 ? maxLength : 255;
        return format switch
        {
            DatabaseFormat.Jet3Mdb => effectiveLength,
            DatabaseFormat.Jet4Mdb or DatabaseFormat.AceAccdb => Math.Max(2, effectiveLength * 2),
            _ => throw new NotSupportedException($"Unknown database format: {format}"),
        };
    }

    private static int GetCalculatedDeclaredSize(ColumnType type, int declaredSize)
    {
        if (type is MemoType or OleType)
        {
            return 0;
        }

        if (IsAlwaysVariableLength(type))
        {
            return declaredSize + Constants.CalculatedColumn.ExtraDataLen;
        }

        return Constants.CalculatedColumn.FixedFieldLen;
    }

    private static byte GetExtraFlags(ColumnDefinition definition, ColumnType type, DatabaseFormat format)
    {
        if (definition.IsCalculated)
        {
            return Constants.CalculatedColumn.ExtFlagMask;
        }

        return (format != DatabaseFormat.Jet3Mdb && (type == TextType || type == MemoType) && definition.IsCompressedUnicode)
            ? Constants.CompressedUnicodeExtFlagMask
            : (byte)0;
    }

    private static void BuildMSysObjectsTDef(byte[] db, int offset, DatabaseFormat format, bool fullCatalogSchema)
    {
        bool isJet3 = format == DatabaseFormat.Jet3Mdb;
        int tdNumCols = isJet3 ? 25 : 45;
        int tdBlockEnd = isJet3 ? 43 : 63;
        int colDescSz = isJet3 ? 18 : 25;
        const int colTypeOff = 0;
        int colNumOff = isJet3 ? 1 : 5;
        int colVarOff = isJet3 ? 3 : 7;
        int colFlagsOff = isJet3 ? 13 : 15;
        int colFixedOff = isJet3 ? 14 : 21;
        int colSzOff = isJet3 ? 16 : 23;
        int textColSize = isJet3 ? 255 : 510;

        BootstrapColumnDescriptor[] columns = fullCatalogSchema ? BuildFullCatalogColumns(textColSize) : BuildSlimCatalogColumns(textColSize);

        int numCols = columns.Length;
        int numVarCols = 0;
        for (int i = 0; i < numCols; i++)
        {
            if (IsAlwaysVariableLength(columns[i].Type))
            {
                numVarCols++;
            }
        }

        db[offset] = 0x02;
        db[offset + 1] = 0x01;
        Wi32(db, offset + 4, 0);
        db[offset + tdNumCols - 5] = 0x53;
        Wu16(db, offset + tdNumCols - 4, numCols);
        Wu16(db, offset + tdNumCols - 2, numVarCols);
        Wu16(db, offset + tdNumCols, numCols);

        int colStart = offset + tdBlockEnd;
        int namePos = colStart + (numCols * colDescSz);

        for (int i = 0; i < numCols; i++)
        {
            BootstrapColumnDescriptor col = columns[i];
            int o = colStart + (i * colDescSz);

            db[o + colTypeOff] = (byte)col.Type;
            if (!isJet3)
            {
                Wi32(db, o + 1, Constants.TableDefinition.Jet4.FormatMagic);
            }

            Wu16(db, o + colNumOff, col.ColNum);
            Wu16(db, o + colVarOff, col.VarIdx);

            if (!isJet3)
            {
                // Jet4/ACE: redundant col_num at offset 9-10 (see TDefPageBuilder
                // user-table path and WriterColumnDescriptorRedundantColNumTests).
                Wu16(db, o + 9, col.ColNum);
            }

            db[o + colFlagsOff] = col.Flags;
            Wu16(db, o + colFixedOff, col.FixedOff);
            Wu16(db, o + colSzOff, col.Size);

            byte[] nameBytes = isJet3 ? Encoding.ASCII.GetBytes(col.Name) : Encoding.Unicode.GetBytes(col.Name);
            if (isJet3)
            {
                db[namePos] = (byte)nameBytes.Length;
                namePos++;
            }
            else
            {
                Wu16(db, namePos, nameBytes.Length);
                namePos += 2;
            }

            Buffer.BlockCopy(nameBytes, 0, db, namePos, nameBytes.Length);
            namePos += nameBytes.Length;
        }

        Wi32(db, offset + 8, Math.Max(0, namePos - offset - 8));
        if (!isJet3)
        {
            Wi32(db, offset + 0x0C, Constants.TableDefinition.Jet4.FormatMagic);
            int tdefLen = Math.Max(0, namePos - offset - 8);
            int pgSz = AccessBase.GetPageSize(format);
            Wu16(db, offset + 2, Math.Max(0, pgSz - tdefLen - 8));
        }
    }

    private static BootstrapColumnDescriptor[] BuildSlimCatalogColumns(int textColSize) =>
    [
        new("Id",          LongIntegerType, 0, 0, 0,  4,           0x03),
        new("ParentId",    LongIntegerType, 1, 0, 4,  4,           0x03),
        new("Name",        TextType,        2, 0, 0,  textColSize, 0x02),
        new("Type",        IntegerType,     3, 0, 8,  2,           0x03),
        new("DateCreate",  DateTimeType,    4, 0, 10, 8,           0x03),
        new("DateUpdate",  DateTimeType,    5, 0, 18, 8,           0x03),
        new("Flags",       LongIntegerType, 6, 0, 26, 4,           0x03),
        new("ForeignName", TextType,        7, 1, 0,  textColSize, 0x02),
        new("Database",    TextType,        8, 2, 0,  textColSize, 0x02),
    ];

    private static BootstrapColumnDescriptor[] BuildFullCatalogColumns(int textColSize) =>
    [
        new("Id",           LongIntegerType, 0,  0,  0,  4,           0x13),
        new("ParentId",     LongIntegerType, 1,  0,  4,  4,           0x13),
        new("Name",         TextType,        2,  0,  0,  textColSize, 0x12),
        new("Type",         IntegerType,     3,  0,  8,  2,           0x13),
        new("DateCreate",   DateTimeType,    4,  0,  10, 8,           0x13),
        new("DateUpdate",   DateTimeType,    5,  0,  18, 8,           0x13),
        new("Owner",        BinaryType,      6,  1,  0,  textColSize, 0x32),
        new("Flags",        LongIntegerType, 7,  0,  26, 4,           0x13),
        new("Database",     MemoType,        8,  2,  0,  0,           0x12),
        new("Connect",      MemoType,        9,  3,  0,  0,           0x12),
        new("ForeignName",  TextType,        10, 4,  0,  textColSize, 0x12),
        new("RmtInfoShort", BinaryType,      11, 5,  0,  textColSize, 0x12),
        new("RmtInfoLong",  OleType,         12, 6,  0,  0,           0x12),
        new("Lv",           OleType,         13, 7,  0,  0,           0x12),
        new("LvProp",       OleType,         14, 8,  0,  0,           0x12),
        new("LvModule",     OleType,         15, 9,  0,  0,           0x12),
        new("LvExtra",      OleType,         16, 10, 0,  0,           0x12),
    ];

    private readonly record struct BootstrapColumnDescriptor(
        string Name,
        ColumnType Type,
        int ColNum,
        int VarIdx,
        int FixedOff,
        int Size,
        byte Flags);

    private (byte[][] Pages, int[] FirstDpLogicalOffsets) SplitLogicalTDefIntoPages(byte[] logical, int usedLength, int[] firstDpLogicalOffsets)
        => (LogicalTDefChain.MaterializePages(logical, usedLength, writer.PageSizeBytes), firstDpLogicalOffsets);
}
