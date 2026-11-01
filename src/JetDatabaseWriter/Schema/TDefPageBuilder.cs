namespace JetDatabaseWriter.Schema;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Builds table-definition (TDEF) pages and the bootstrap bytes for a new,
/// empty database file.
/// </summary>
/// <param name="format">The database's immutable format profile.</param>
/// <param name="pager">The writer's page file, through which a TDEF's row count is written.</param>
internal sealed class TDefPageBuilder(JetFormat format, Pager pager)
{
    /// <summary>
    /// Checks that <paramref name="format"/> can hold <paramref name="definition"/>
    /// and returns the column type it is stored as: calculated, Large Number,
    /// Date/Time Extended, Attachment and multi-value columns need a format
    /// whose capability flag allows them (ACCDB), a calculated column needs an
    /// expression, a supported result type and no AutoNumber, Attachment,
    /// multi-value or Hyperlink flag, a Hyperlink column must be a Memo, and a
    /// decimal column on a format without Numeric (Jet3) is stored as Currency
    /// (<see cref="JetTypeInfo.ResolveStorageType"/>). The expression itself
    /// is not parsed here. Every <see cref="ArgumentException"/> it throws
    /// carries <paramref name="paramName"/>, so <c>CreateTableAsync</c> reports
    /// <c>columns</c> and <c>AddColumnAsync</c> reports <c>column</c>.
    /// </summary>
    /// <param name="definition">The column definition.</param>
    /// <param name="format">The database's format profile.</param>
    /// <param name="paramName">The public parameter that carries <paramref name="definition"/>, for <see cref="ArgumentException.ParamName"/>.</param>
    /// <returns>The column's type code.</returns>
    /// <exception cref="JetNotSupportedException">The format cannot hold the column, including a Jet3 decimal column whose precision or scale Currency cannot hold.</exception>
    /// <exception cref="JetNotSupportedException">An Int64/Large Number column is declared on an MDB format.</exception>
    /// <exception cref="ArgumentException">A calculated column has no expression, the definition's flags conflict, a Hyperlink column is not a Memo, or (<see cref="ArgumentOutOfRangeException"/>) the precision or scale of a decimal column, or of a multi-value column's decimal items, is out of range.</exception>
    internal static ColumnType ValidateColumnForFormat(ColumnDefinition definition, JetFormat format, string paramName)
    {
        ValidateCalculatedColumn(definition, format, paramName);
        ColumnType type = ResolveStorageType(definition, TypeCodeFromDefinition(definition, paramName), format, paramName);

        if (definition.AllowZeroLength && type is not (TextType or MemoType))
        {
            throw new ArgumentException($"Column '{definition.Name}': AllowZeroLength requires a Text, Memo or Hyperlink field.", paramName);
        }

        if (definition.TextCollation is { } collation)
        {
            var requested = TextSortOrder.FromCollation(collation, paramName);
            if (type is not (TextType or MemoType))
            {
                throw new ArgumentException($"Column '{definition.Name}': a text collation requires a Text or Memo column.", paramName);
            }

            if (requested.HasVersion != format.DefaultTextSortOrder.HasVersion)
            {
                throw new ArgumentException($"Column '{definition.Name}': the text collation {collation} is incompatible with this database format.", paramName);
            }
        }

        if (type == BigIntType && !format.SupportsBigInt)
        {
            throw new JetNotSupportedException(
                JetErrorCode.FeatureNotSupported,
                $"Column '{definition.Name}': Int64/Large Number columns are only supported in ACCDB databases.",
                new JetErrorInfo { ColumnName = definition.Name });
        }

        if (type == DateTimeExtendedType && !format.SupportsDateTimeExtended)
        {
            throw new JetNotSupportedException(
                JetErrorCode.FeatureNotSupported,
                $"Column '{definition.Name}': Date/Time Extended columns are only supported in ACCDB databases.");
        }

        if (type == BigBinaryType)
        {
            if (!format.SupportsBigBinary)
            {
                throw new JetNotSupportedException(JetErrorCode.FeatureNotSupported, $"Column '{definition.Name}': BIGBINARY is a Jet4-only system-table type.");
            }

            int width = definition.SourceColumn?.Size ?? definition.MaxLength;
            if (width != GetFixedSize(BigBinaryType)
                || definition.SourceColumn is { IsFixed: false }
                || definition.ForceVariableLengthStorage
                || (definition.DescriptorFlagsOverride is byte binaryFlags && (binaryFlags & Constants.ColumnDescriptorFlags.Fixed) == 0))
            {
                throw new JetNotSupportedException(JetErrorCode.FeatureNotSupported, $"Column '{definition.Name}': BIGBINARY requires its native 3992-byte fixed descriptor.");
            }
        }

        if (type == ComplexType && !format.SupportsComplexColumns)
        {
            throw new JetNotSupportedException(
                JetErrorCode.FeatureNotSupported,
                $"Column '{definition.Name}': Attachment and multi-value columns are an Access 2007+ ACE feature; declare them only on .accdb databases.");
        }

        // A multi-value Decimal column's precision and scale are its items',
        // stored on the flat table's Value column, which is created after the
        // parent table; check them before anything is written.
        if (definition.IsMultiValue && definition.MultiValueElementType == typeof(decimal))
        {
            _ = ResolveNumericPrecision(definition, paramName);
            _ = ResolveNumericScale(definition, paramName);
        }

        // The Hyperlink flag marks a Memo column. A complex column carries no
        // descriptor flags, so the flag is ignored on one.
        if ((definition.IsHyperlink || definition.ClrType == typeof(Hyperlink)) && type is not (MemoType or ComplexType))
        {
            throw new ArgumentException(
                $"Column '{definition.Name}' has IsHyperlink = true but resolves to JET type {GetTypeDisplayName(type)}; " +
                "hyperlink columns must be MEMO (string with no MaxLength, or typeof(Hyperlink)).",
                paramName);
        }

        return type;
    }

    internal static TableDef BuildTableDefinition(IReadOnlyList<ColumnDefinition> columns, JetFormat format)
    {
        var descriptors = new List<ColumnInfo>(columns.Count);
        int fixedOffset = 0;
        int nextVarIndex = 0;
        var nativeIdentities = new HashSet<int>();
        int nextNativeIdentity = 0;
        if (!format.IsJet3)
        {
            foreach (ColumnDefinition definition in columns)
            {
                if (definition.SourceColumn is { } source)
                {
                    _ = nativeIdentities.Add(source.NativeIdentity);
                    nextNativeIdentity = Math.Max(nextNativeIdentity, source.NativeIdentity + 1);
                }
            }
        }

        for (int i = 0; i < columns.Count; i++)
        {
            ColumnDefinition definition = columns[i];
            ColumnType type = ValidateColumnForFormat(definition, format, nameof(columns));

            if (definition.SourceColumn is { } source)
            {
                ColumnInfo preserved = source with
                {
                    Name = definition.Name,
                    ColNum = i,
                    VarIdx = source.IsFixed ? 0 : nextVarIndex,
                    FixedOff = source.IsFixed ? fixedOffset : 0,
                };
                descriptors.Add(preserved);
                if (source.IsFixed)
                {
                    fixedOffset += GetFixedStorageSize(source.Type, source.Size);
                }
                else
                {
                    nextVarIndex++;
                }

                continue;
            }

            bool isCalculated = definition.IsCalculated;
            bool variable = isCalculated || definition.ForceVariableLengthStorage || (type != BinaryType && IsAlwaysVariableLength(type));
            if (type is TextType or BinaryType or BigBinaryType && definition.DescriptorFlagsOverride is byte storageFlags)
            {
                variable = (storageFlags & Constants.ColumnDescriptorFlags.Fixed) == 0;
            }

            int declaredSize = isCalculated && type == TextType
                ? Constants.CalculatedColumn.MaxTextResultBytes
                : GetDeclaredSize(type, definition.MaxLength, format);
            int size = definition.DescriptorSizeOverride ?? (isCalculated ? GetCalculatedDeclaredSize(type, declaredSize) : declaredSize);

            byte flags;
            bool isComplex = type is ComplexType;
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
                // the constraint is persisted via the Boolean `Required` property in
                // MSysObjects.LvProp instead. See JetExpressionConverter.ApplyColumn.
                if (definition.IsAutoIncrement)
                {
                    flags |= definition.ClrType == typeof(Guid) ? Constants.ColumnDescriptorFlags.AutoNumberGuid : Constants.ColumnDescriptorFlags.AutoNumber;
                }

                // ValidateColumnForFormat has checked that a Hyperlink column is a Memo.
                if (definition.IsHyperlink || definition.ClrType == typeof(Hyperlink))
                {
                    flags |= Constants.ColumnDescriptorFlags.Hyperlink;
                }
            }

            flags = definition.DescriptorFlagsOverride ?? flags;

            if (nextNativeIdentity > ushort.MaxValue)
            {
                nextNativeIdentity = 0;
            }

            while (nativeIdentities.Contains(nextNativeIdentity))
            {
                nextNativeIdentity++;
            }

            var column = new ColumnInfo
            {
                Name = definition.Name,
                Type = type,
                ColNum = i,
                NativeIdentity = nextNativeIdentity,
                VarIdx = variable ? nextVarIndex : 0,
                FixedOff = variable ? 0 : fixedOffset,
                Size = size,
                Flags = flags,
                TextSortOrder = definition.TextSortOrderOverride ?? (definition.TextCollation is { } collation ? TextSortOrder.FromCollation(collation, nameof(columns)) : format.DefaultTextSortOrder),
                Misc = isComplex ? definition.ComplexId : definition.DescriptorNonTextMiscOverride ?? 0,
                NumericPrecision = type == NumericType ? ResolveNumericPrecision(definition, nameof(columns)) : (byte)0,
                NumericScale = type == NumericType ? ResolveNumericScale(definition, nameof(columns)) : (byte)0,
                ExtraFlags = definition.DescriptorExtraFlagsOverride ?? GetExtraFlags(definition, type, format),
            };

            descriptors.Add(column);
            _ = nativeIdentities.Add(nextNativeIdentity++);

            if (variable)
            {
                nextVarIndex++;
            }
            else
            {
                fixedOffset += GetFixedStorageSize(type, size);
            }
        }

        return new TableDef { Columns = descriptors };
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
            throw new JetNotSupportedException(
                JetErrorCode.FeatureNotSupported,
                $"Table definition produced a {pages.Length}-page TDEF chain, but the single-page builder was used. Route this caller through BuildTDefPagesWithIndexOffsets / the multi-page write path.");
        }

        return (pages[0], firstDpOffsets);
    }

    public (byte[][] Pages, int[] FirstDpLogicalOffsets, int[] UsedPagesLogicalOffsets) BuildTDefPagesWithIndexOffsets(TableDef tableDef, IReadOnlyList<ResolvedIndex> indexes)
    {
        int logicalCapacity = Math.Max(format.PageSize * 32, format.PageSize);
        byte[] page = new byte[logicalCapacity];
        int numCols = tableDef.Columns.Count;
        int numIdx = indexes.Count;
        bool jet4 = !format.IsJet3;
        bool formatMagic = format.WritesTDefFormatMagic;
        int numRealIdx = numIdx;

        int colStart = format.TDef.BlockEnd + (numRealIdx * format.TDef.RealIdxEntrySz);
        int namePos = colStart + (numCols * format.ColumnDescriptor.Size);

        page[0] = Constants.PageTypes.TableDefinition;
        page[1] = 0x01;
        page[format.TDef.TableType] = Constants.TableDefinition.UserTableType;
        Wu16(page, format.TDef.MaxCols, numCols);
        Wu16(page, format.TDef.NumCols, numCols);
        Wi32(page, format.TDef.NumIdx, numIdx);
        Wi32(page, format.TDef.NumRealIdx, numRealIdx);

        int numVarCols = 0;
        for (int i = 0; i < numCols; i++)
        {
            ColumnInfo col = tableDef.Columns[i];
            int o = colStart + (i * format.ColumnDescriptor.Size);

            if (!col.IsFixed)
            {
                numVarCols++;
            }

            page[o + format.ColumnDescriptor.TypeOff] = (byte)col.Type;
            if (formatMagic)
            {
                Wi32(page, o + 1, Constants.TableDefinition.Jet4.FormatMagic);
            }

            Wu16(page, o + format.ColumnDescriptor.NumOff, col.ColNum);
            Wu16(page, o + format.ColumnDescriptor.VarOff, col.VarIdx);

            if (jet4)
            {
                // Native identities need not equal the row-layout column number.
                Wu16(page, o + format.ColumnDescriptor.NativeIdentityOff, col.NativeIdentity);
            }

            if (!jet4)
            {
                Wu16(page, o + format.ColumnDescriptor.NativeIdentityOff, col.ColNum);
                Wu16(page, o + format.ColumnDescriptor.MiscOff, 1);
                Wu16(page, o + format.ColumnDescriptor.SortOrderOff, 0x0409);
                Wu16(page, o + format.ColumnDescriptor.CodePageOff, format.CodePage);
            }

            page[o + format.ColumnDescriptor.FlagsOff] = col.Flags;
            Wu16(page, o + format.ColumnDescriptor.FixedOff, col.FixedOff);
            Wu16(page, o + format.ColumnDescriptor.SzOff, col.Size);

            if (col.Type is ComplexType)
            {
                Wi32(page, o + format.ColumnDescriptor.MiscOff, col.Misc);
            }
            else if (col.Type == NumericType && format.SupportsNumeric)
            {
                // Numeric descriptors are supported only by Jet4 and ACE.
                if (!col.IsCalculated)
                {
                    page[o + format.ColumnDescriptor.MiscOff] = col.NumericPrecision;
                    page[o + format.ColumnDescriptor.MiscOff + 1] = col.NumericScale;
                }
                else
                {
                    page[o + format.ColumnDescriptor.ExtraFlagsOff] = col.ExtraFlags;
                }
            }
            else if (col.Type is TextType or MemoType)
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
                Wu16(page, o + format.ColumnDescriptor.SortOrderOff, col.TextSortOrder.Value);
                if (jet4)
                {
                    page[o + format.ColumnDescriptor.SortVersionOff] = col.TextSortOrder.Version;
                }
                else
                {
                    Wu16(page, o + format.ColumnDescriptor.CodePageOff, format.CodePage);
                }

                if (jet4)
                {
                    page[o + format.ColumnDescriptor.ExtraFlagsOff] = col.ExtraFlags;
                }
            }
            else if (jet4 && col.IsCalculated)
            {
                page[o + format.ColumnDescriptor.ExtraFlagsOff] = col.ExtraFlags;
            }
            else if (jet4)
            {
                if (col.Misc != 0)
                {
                    Wi32(page, o + format.ColumnDescriptor.MiscOff, col.Misc);
                }

                if (col.ExtraFlags != 0)
                {
                    page[o + format.ColumnDescriptor.ExtraFlagsOff] = col.ExtraFlags;
                }
            }

            if (col.RawDescriptor.Count != 0)
            {
                if (col.RawDescriptor.Count != format.ColumnDescriptor.Size)
                {
                    throw new JetNotSupportedException(JetErrorCode.FeatureNotSupported, "The source column descriptor does not match the database format's descriptor width.");
                }

                for (int byteIndex = 0; byteIndex < col.RawDescriptor.Count; byteIndex++)
                {
                    page[o + byteIndex] = col.RawDescriptor[byteIndex];
                }

                Wu16(page, o + format.ColumnDescriptor.NumOff, col.ColNum);
                Wu16(page, o + format.ColumnDescriptor.VarOff, col.VarIdx);
                Wu16(page, o + format.ColumnDescriptor.FixedOff, col.FixedOff);
                Wu16(page, o + format.ColumnDescriptor.NativeIdentityOff, jet4 ? col.NativeIdentity : col.ColNum);
            }

            byte[] nameRecord = format.EncodeTDefNameRecord(col.Name);
            if (namePos + nameRecord.Length > page.Length)
            {
                throw new JetNotSupportedException(
                    JetErrorCode.FeatureNotSupported,
                    "Table definition exceeds the TDEF logical-buffer capacity. Increase the logical capacity or reduce column-name lengths.");
            }

            Buffer.BlockCopy(nameRecord, 0, page, namePos, nameRecord.Length);
            namePos += nameRecord.Length;
        }

        Wu16(page, format.TDef.NumVarCols, numVarCols);

        int[] firstDpOffsets = numIdx > 0 ? new int[numIdx] : [];
        int[] usedPagesOffsets = numIdx > 0 ? new int[numIdx] : [];
        if (numIdx > 0)
        {
            int realIdxPhysStart = namePos;
            IndexSectionAnchors anchors = format.Index.GetIndexSection(realIdxPhysStart, numRealIdx, numIdx);
            int totalIdxBytesLowerBound = anchors.LogIdxNamesStart - realIdxPhysStart;
            if (realIdxPhysStart + totalIdxBytesLowerBound > page.Length)
            {
                throw new JetNotSupportedException(
                    JetErrorCode.FeatureNotSupported,
                    "Table definition (with indexes) exceeds the TDEF logical-buffer capacity. Increase BuildTDefPagesWithIndexOffsets's logicalCapacity or reduce the index count.");
            }

            for (int i = 0; i < numIdx; i++)
            {
                ResolvedIndex ri = indexes[i];
                int phys = format.Index.RealIdxPhysOffset(realIdxPhysStart, i);
                if (formatMagic)
                {
                    Wi32(page, phys, Constants.TableDefinition.Jet4.RealIdx.LeadingMagic);
                }

                for (int slot = 0; slot < Constants.TableDefinition.ColMapSlotCount; slot++)
                {
                    int so = format.Index.ColMapSlotOffset(phys, slot);
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

                byte flagsByte = jet4 ? Constants.TableDefinition.UnknownIndexFlag : (byte)0;
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

                page[format.Index.FlagsAbsoluteOffset(phys)] = flagsByte;
                usedPagesOffsets[i] = format.Index.FirstDpAbsoluteOffset(phys) - 4;

                firstDpOffsets[i] = format.Index.FirstDpAbsoluteOffset(phys);

                int log = format.Index.LogicalIdxFieldsOffset(anchors.LogIdxStart, i);
                if (formatMagic)
                {
                    Wi32(page, log - format.Index.LogicalEntryFieldsOffset, Constants.TableDefinition.Jet4.FormatMagic);
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
                byte[] nameRecord = format.EncodeTDefNameRecord(indexes[i].Name);
                if (npos + nameRecord.Length > page.Length)
                {
                    throw new JetNotSupportedException(
                        JetErrorCode.FeatureNotSupported,
                        "Table definition exceeds the TDEF logical-buffer capacity. Increase the logical capacity or reduce index-name lengths.");
                }

                Buffer.BlockCopy(nameRecord, 0, page, npos, nameRecord.Length);
                npos += nameRecord.Length;
            }

            namePos = npos;
        }

        // Native tables terminate metadata with a sentinel and eight reserved bytes,
        // including Jet3 scalar tables with no indexes.
        Wu16(page, namePos, ushort.MaxValue);
        namePos += 10;
        if (!jet4)
        {
            // DAO3.6 writes the VC signature rather than a free-space count.
            page[2] = 0x56;
            page[3] = 0x43;
        }

        Wi32(page, 8, Math.Max(0, namePos - 8));
        if (formatMagic)
        {
            Wi32(page, 0x0C, Constants.TableDefinition.Jet4.FormatMagic);
        }

        if (format.WritesTDefFreeSpace)
        {
            int tdefLen = Math.Max(0, namePos - 8);
            Wu16(page, 2, Math.Max(0, format.PageSize - tdefLen - 8));
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
    /// Writes a real index's 4-byte <c>used_pages</c> usage-map pointer (row
    /// byte, then the 3-byte page number) at the given LOGICAL TDEF offset,
    /// mapping every byte to the physical page that holds it. In a wide table
    /// the real-idx descriptors sit on a continuation page, whose body starts
    /// after an 8-byte header, so the offset cannot be split with a plain
    /// divide by the page size.
    /// </summary>
    /// <param name="pages">The pages.</param>
    /// <param name="logicalOffset">The logical offset of the <c>used_pages</c> field.</param>
    /// <param name="rowIndex">The usage-map row that lists the index's pages.</param>
    /// <param name="usageMapPage">The usage-map page number.</param>
    internal void WriteLogicalUsedPagesPointer(byte[][] pages, int logicalOffset, int rowIndex, long usageMapPage)
    {
        (int pageIdx, int pageOff) = this.LogicalToPhysicalTDefOffset(logicalOffset);
        pages[pageIdx][pageOff] = checked((byte)rowIndex);
        this.WriteLogicalTDefUInt24(pages, logicalOffset + 1, checked((int)usageMapPage));
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

        byte[] page = await pager.ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        long updated;

        try
        {
            uint current = Ru32(page, format.TDef.NumRows);
            updated = Math.Clamp(current + delta, 0L, uint.MaxValue);
            Wi32(page, format.TDef.NumRows, unchecked((int)(uint)updated));

            // Jet3 slots contain optimizer statistics, including an insertion-time
            // distinct-key estimate at +4. Native deletion leaves it stale and
            // Compact recalculates it; preserve it rather than applying row deltas.
            // Mirror Jet4/ACE changes into the per-real-idx `num_idx_rows` counter
            // (offset +4 of each 12-byte/8-byte slot in the leading real-idx
            // skip block at [_tdef.BlockEnd, _tdef.BlockEnd + numRealIdx *
            // _tdef.RealIdxEntrySz)). Per mdbtools HACKING.md the slot is laid
            // out as `unknown(4) + num_idx_rows(4) + unknown(4)`. DAO compares
            // num_idx_rows against the leaf-level row count when walking
            // MSysObjects; if they disagree it aborts compact with
            // "could not find the object 'MSysDb'" — see
            // docs/design/round-trip-openrecordset-hypothesis.md.
            int numRealIdx = TDefCodec.ReadCounts(format, page).RealIndexCount;
            if (!format.IsJet3 && numRealIdx is > 0 and <= Constants.TableDefinition.MaxIndexes)
            {
                int slotEnd = format.TDef.BlockEnd + (numRealIdx * format.TDef.RealIdxEntrySz);
                if (slotEnd <= page.Length)
                {
                    for (int i = 0; i < numRealIdx; i++)
                    {
                        int countOff = format.TDef.BlockEnd + (i * format.TDef.RealIdxEntrySz) + 4;
                        uint cur = Ru32(page, countOff);
                        long next = Math.Clamp(cur + delta, 0L, uint.MaxValue);
                        Wi32(page, countOff, unchecked((int)(uint)next));
                    }
                }
            }

            await pager.WritePageAsync(tdefPage, page, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            PageBuffers.Return(page);
        }
    }

    /// <summary>Sets the live row count and the Jet4/ACE real-index row statistics.</summary>
    /// <param name="tdefPage">The TDEF root page.</param>
    /// <param name="rowCount">The rewritten row count.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    internal async ValueTask SetTDefRowCountAsync(long tdefPage, uint rowCount, CancellationToken cancellationToken)
    {
        byte[] page = await pager.ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            Wi32(page, format.TDef.NumRows, unchecked((int)rowCount));
            int count = TDefCodec.ReadCounts(format, page).RealIndexCount;
            if (!format.IsJet3 && count is > 0 and <= Constants.TableDefinition.MaxIndexes
                && format.TDef.BlockEnd + (count * format.TDef.RealIdxEntrySz) <= page.Length)
            {
                for (int i = 0; i < count; i++)
                {
                    Wi32(page, format.TDef.BlockEnd + (i * format.TDef.RealIdxEntrySz) + 4, unchecked((int)rowCount));
                }
            }

            await pager.WritePageAsync(tdefPage, page, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            PageBuffers.Return(page);
        }
    }

    public (int PageIndex, int PageOffset) LogicalToPhysicalTDefOffset(int logicalOffset)
        => LogicalTDefChain.LogicalToPhysicalOffset(format.PageSize, logicalOffset);

    /// <summary>
    /// Builds a minimal, empty JET database as a byte array.
    /// The bootstrap image contains three pages (page size varies by format):
    /// page 0 (header), page 1 (global usage map), and page 2 (MSysObjects TDEF).
    /// The <see cref="AccessWriter.CreateDatabaseAsync(string, DatabaseFormat, AccessWriterOptions?, System.Threading.CancellationToken)"/> overloads add
    /// system tables and security metadata after opening this minimal image.
    /// </summary>
    /// <param name="format">Target on-disk format.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="format"/> is not a defined format.</exception>
    internal static byte[] BuildEmptyDatabase(DatabaseFormat format)
    {
        var profile = JetFormat.ForNewDatabase(format);
        int pgSz = profile.PageSize;
        byte[] db = new byte[pgSz * 3];

        BuildGlobalUsageMapPage(db, pgSz, profile);
        BuildMSysObjectsTDef(db, pgSz * 2, profile);
        profile.InitializeNewHeader(db);
        return db;
    }

    private static void BuildGlobalUsageMapPage(byte[] db, int pgSz, JetFormat format)
    {
        DataPageLayout dataPage = format.DataPage;
        int pageOffset = pgSz;

        // Native Jet3 uses 133-byte global rows; shorter rows read but refuse native allocation and LVAL mutation.
        int rowStart = pgSz - format.UsageMapRowSize;
        int row1Start = rowStart - format.UsageMapRowSize;
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

    private static int GetDeclaredSize(ColumnType type, int maxLength, JetFormat format)
    {
        if (type == TextType)
        {
            return format.GetTextDeclaredSize(maxLength);
        }

        if (type == BinaryType)
        {
            return maxLength > 0 ? maxLength : 255;
        }

        return GetFixedSize(type);
    }

    private static int GetCalculatedDeclaredSize(ColumnType type, int declaredSize)
    {
        if (type == BinaryType)
        {
            return checked(declaredSize * 2);
        }

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

    private static byte GetExtraFlags(ColumnDefinition definition, ColumnType type, JetFormat format)
    {
        if (definition.IsCalculated)
        {
            return Constants.CalculatedColumn.ExtFlagMask;
        }

        return (format.ColumnDescriptor.ExtraFlagsOff >= 0 && (type == TextType || type == MemoType) && definition.IsCompressedUnicode)
            ? Constants.CompressedUnicodeExtFlagMask
            : (byte)0;
    }

    private static void BuildMSysObjectsTDef(byte[] db, int offset, JetFormat format)
    {
        bool isJet3 = format.IsJet3;
        TDefHeaderLayout tdef = format.TDef;
        ColumnDescriptorLayout descriptor = format.ColumnDescriptor;
        int textColSize = format.GetTextDeclaredSize(255);

        BootstrapColumnDescriptor[] columns = BuildFullCatalogColumns(textColSize);

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
        db[offset + tdef.TableType] = Constants.TableDefinition.SystemTableType;
        Wu16(db, offset + tdef.MaxCols, numCols);
        Wu16(db, offset + tdef.NumVarCols, numVarCols);
        Wu16(db, offset + tdef.NumCols, numCols);

        int colStart = offset + tdef.BlockEnd;
        int namePos = colStart + (numCols * descriptor.Size);

        for (int i = 0; i < numCols; i++)
        {
            BootstrapColumnDescriptor col = columns[i];
            int o = colStart + (i * descriptor.Size);

            db[o + descriptor.TypeOff] = (byte)col.Type;
            if (format.WritesTDefFormatMagic)
            {
                Wi32(db, o + 1, Constants.TableDefinition.Jet4.FormatMagic);
            }

            Wu16(db, o + descriptor.NumOff, col.ColNum);
            Wu16(db, o + descriptor.VarOff, col.VarIdx);

            if (!isJet3)
            {
                // Jet4/ACE: new descriptors initialize offset 9-10 from col_num (see
                // the user-table path and its descriptor initialization tests).
                Wu16(db, o + descriptor.NativeIdentityOff, col.ColNum);
            }

            if (isJet3)
            {
                Wu16(db, o + format.ColumnDescriptor.NativeIdentityOff, col.ColNum);
                Wu16(db, o + format.ColumnDescriptor.SortOrderOff, 0x0409);
                Wu16(db, o + descriptor.CodePageOff, format.CodePage);
            }

            db[o + descriptor.FlagsOff] = col.Flags;
            Wu16(db, o + descriptor.FixedOff, col.FixedOff);
            Wu16(db, o + descriptor.SzOff, col.Size);

            byte[] nameRecord = format.EncodeTDefNameRecord(col.Name);
            Buffer.BlockCopy(nameRecord, 0, db, namePos, nameRecord.Length);
            namePos += nameRecord.Length;
        }

        Wi32(db, offset + 8, Math.Max(0, namePos - offset - 8));
        if (format.WritesTDefFormatMagic)
        {
            Wi32(db, offset + 0x0C, Constants.TableDefinition.Jet4.FormatMagic);
        }

        if (format.WritesTDefFreeSpace)
        {
            int tdefLen = Math.Max(0, namePos - offset - 8);
            Wu16(db, offset + 2, Math.Max(0, format.PageSize - tdefLen - 8));
        }
    }

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
        => (LogicalTDefChain.MaterializePages(logical, usedLength, format.PageSize), firstDpLogicalOffsets);
}
