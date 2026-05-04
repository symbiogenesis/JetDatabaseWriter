namespace JetDatabaseWriter.Internal.Builders;

using System;
using System.Collections.Generic;
using System.Text;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Internal.Models;
using JetDatabaseWriter.Models;
using static JetDatabaseWriter.Constants.ColumnTypes;

#pragma warning disable SA1204

/// <summary>
/// Builds table-definition (TDEF) pages and the bootstrap bytes for a new,
/// empty database file. Owned by <see cref="AccessWriter"/>, which keeps thin
/// compatibility forwarders for existing call sites.
/// </summary>
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
            byte type = AccessWriter.TypeCodeFromDefinition(definition);
            bool variable = AccessWriter.IsVariableType(type);
            int size = GetDeclaredSize(type, definition.MaxLength, format);

            byte flags;
            bool isComplex = type == T_ATTACHMENT || type == T_COMPLEX;
            if (isComplex)
            {
                flags = 0x07;
            }
            else
            {
                flags = 0;
                if (!variable)
                {
                    flags |= 0x01;
                }

                if (definition.IsNullable)
                {
                    flags |= 0x02;
                }

                if (definition.IsAutoIncrement)
                {
                    flags |= 0x04;
                }

                bool wantsHyperlink = definition.IsHyperlink || definition.ClrType == typeof(Hyperlink);
                if (wantsHyperlink)
                {
                    if (type != T_MEMO)
                    {
                        throw new ArgumentException(
                            $"Column '{definition.Name}' has IsHyperlink = true but resolves to JET type 0x{type:X2}; " +
                            "hyperlink columns must be MEMO (string with no MaxLength, or typeof(Hyperlink)).",
                            nameof(columns));
                    }

                    flags |= 0x80;
                }
            }

            var column = new ColumnInfo
            {
                Name = definition.Name,
                Type = type,
                ColNum = i,
                VarIdx = variable ? nextVarIndex : 0,
                FixedOff = variable ? 0 : fixedOffset,
                Size = size,
                Flags = flags,
                Misc = isComplex ? definition.ComplexId : 0,
                NumericPrecision = type == T_NUMERIC ? AccessWriter.ResolveNumericPrecision(definition) : (byte)0,
                NumericScale = type == T_NUMERIC ? AccessWriter.ResolveNumericScale(definition) : (byte)0,
            };

            result.Columns.Add(column);

            if (variable)
            {
                nextVarIndex++;
            }
            else
            {
                fixedOffset += JetTypeInfo.GetFixedSize(type);
            }
        }

        result.InitializeColumnMetadata();
        return result;
    }

    public byte[] BuildTDefPage(TableDef tableDef)
        => BuildTDefPageWithIndexOffsets(tableDef, []).Page;

    public byte[] BuildTDefPage(TableDef tableDef, IReadOnlyList<ResolvedIndex> indexes)
        => BuildTDefPageWithIndexOffsets(tableDef, indexes).Page;

    public (byte[] Page, int[] FirstDpOffsets) BuildTDefPageWithIndexOffsets(TableDef tableDef, IReadOnlyList<ResolvedIndex> indexes)
    {
        var (pages, firstDpOffsets, _) = BuildTDefPagesWithIndexOffsets(tableDef, indexes);
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
        int logicalCapacity = Math.Max(writer._pgSz * 32, writer._pgSz);
        byte[] page = new byte[logicalCapacity];
        int numCols = tableDef.Columns.Count;
        int numIdx = indexes.Count;
        bool jet4 = writer._format != DatabaseFormat.Jet3Mdb;
        int numRealIdx = numIdx;

        int colStart = writer._tdef.BlockEnd + (numRealIdx * writer._tdef.RealIdxEntrySz);
        int namePos = colStart + (numCols * writer._colDesc.Size);
        int nameLenSize = jet4 ? 2 : 1;

        page[0] = 0x02;
        page[1] = 0x01;
        page[writer._tdef.NumCols - 5] = 0x4E;
        AccessBase.Wu16(page, writer._tdef.NumCols - 4, numCols);
        AccessBase.Wu16(page, writer._tdef.NumCols, numCols);
        AccessBase.Wi32(page, writer._tdef.NumCols + 2, numIdx);
        AccessBase.Wi32(page, writer._tdef.NumRealIdx, numRealIdx);

        int numVarCols = 0;
        for (int i = 0; i < numCols; i++)
        {
            ColumnInfo col = tableDef.Columns[i];
            int o = colStart + (i * writer._colDesc.Size);

            if (AccessWriter.IsVariableType(col.Type))
            {
                numVarCols++;
            }

            page[o + writer._colDesc.TypeOff] = col.Type;
            if (jet4)
            {
                AccessBase.Wi32(page, o + 1, Constants.TableDefinition.Jet4FormatMagic);
            }

            AccessBase.Wu16(page, o + writer._colDesc.NumOff, col.ColNum);
            AccessBase.Wu16(page, o + writer._colDesc.VarOff, col.VarIdx);
            page[o + writer._colDesc.FlagsOff] = col.Flags;
            AccessBase.Wu16(page, o + writer._colDesc.FixedOff, col.FixedOff);
            AccessBase.Wu16(page, o + writer._colDesc.SzOff, col.Size);

            if (col.Type == T_ATTACHMENT || col.Type == T_COMPLEX)
            {
                AccessBase.Wi32(page, o + writer._colDesc.MiscOff, col.Misc);
            }
            else if (col.Type == T_NUMERIC && writer._format != DatabaseFormat.Jet3Mdb)
            {
                page[o + writer._colDesc.MiscOff] = col.NumericPrecision;
                page[o + writer._colDesc.MiscOff + 1] = col.NumericScale;
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
                AccessBase.Wu16(page, namePos, nameBytes.Length);
            }
            else
            {
                page[namePos] = (byte)nameBytes.Length;
            }

            namePos += nameLenSize;
            Buffer.BlockCopy(nameBytes, 0, page, namePos, nameBytes.Length);
            namePos += nameBytes.Length;
        }

        AccessBase.Wu16(page, writer._tdef.NumCols - 2, numVarCols);

        int[] firstDpOffsets = numIdx > 0 ? new int[numIdx] : [];
        int[] usedPagesOffsets = numIdx > 0 ? new int[numIdx] : [];
        if (numIdx > 0)
        {
            int realIdxPhysStart = namePos;
            var (_, logIdxStart, logIdxNameStart, _, _) = writer._indexLayout.GetIndexSection(realIdxPhysStart, numRealIdx, numIdx);
            int totalIdxBytesLowerBound = logIdxNameStart - realIdxPhysStart;
            if (realIdxPhysStart + totalIdxBytesLowerBound > page.Length)
            {
                throw new NotSupportedException(
                    "Table definition (with indexes) exceeds the TDEF logical-buffer capacity. Increase "
                    + "BuildTDefPagesWithIndexOffsets's logicalCapacity or reduce the index count.");
            }

            for (int i = 0; i < numIdx; i++)
            {
                ResolvedIndex ri = indexes[i];
                int phys = writer._indexLayout.RealIdxPhysOffset(realIdxPhysStart, i);
                if (jet4)
                {
                    AccessBase.Wi32(page, phys, Constants.TableDefinition.Jet4FormatMagic);
                }

                for (int slot = 0; slot < IndexLayout.ColMapSlotCount; slot++)
                {
                    int so = writer._indexLayout.ColMapSlotOffset(phys, slot);
                    if (slot < ri.ColumnNumbers.Count)
                    {
                        AccessBase.Wu16(page, so, ri.ColumnNumbers[slot]);
                        page[so + 2] = ri.Ascending[slot] ? (byte)0x01 : (byte)0x02;
                    }
                    else
                    {
                        AccessBase.Wu16(page, so, IndexLayout.ColMapPaddingSlot);
                        page[so + 2] = 0x00;
                    }
                }

                byte flagsByte = Constants.TableDefinition.UnknownIndexFlag;
                if (ri.IsPrimaryKey)
                {
                    flagsByte |= (byte)(Constants.TableDefinition.UniqueIndexFlag | Constants.TableDefinition.RequiredIndexFlag);
                }
                else if (ri.IsUnique)
                {
                    flagsByte |= Constants.TableDefinition.UniqueIndexFlag;
                }

                page[writer._indexLayout.FlagsAbsoluteOffset(phys)] = flagsByte;
                if (jet4)
                {
                    usedPagesOffsets[i] = writer._indexLayout.FirstDpAbsoluteOffset(phys) - 4;
                }

                firstDpOffsets[i] = writer._indexLayout.FirstDpAbsoluteOffset(phys);

                int log = writer._indexLayout.LogicalIdxFieldsOffset(logIdxStart, i);
                if (jet4)
                {
                    AccessBase.Wi32(page, log - writer._indexLayout.LogicalEntryFieldsOffset, Constants.TableDefinition.Jet4FormatMagic);
                }

                AccessBase.Wi32(page, log + IndexLayout.IndexNumFieldOffset, i);
                AccessBase.Wi32(page, log + IndexLayout.IndexNum2FieldOffset, i);
                AccessBase.Wi32(page, log + IndexLayout.RelIdxNumFieldOffset, -1);
                page[log + IndexLayout.IndexTypeFieldOffset] = (byte)(ri.IsPrimaryKey ? IndexKind.PrimaryKey : IndexKind.Normal);
            }

            int npos = logIdxNameStart;
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
                    AccessBase.Wu16(page, npos, nameBytes.Length);
                }
                else
                {
                    page[npos] = (byte)nameBytes.Length;
                }

                npos += nameLenSize;
                Buffer.BlockCopy(nameBytes, 0, page, npos, nameBytes.Length);
                npos += nameBytes.Length;
            }

            namePos = npos;
        }

        AccessBase.Wi32(page, 8, Math.Max(0, namePos - 8));
        if (jet4)
        {
            AccessBase.Wi32(page, 0x0C, Constants.TableDefinition.Jet4FormatMagic);
            int tdefLen = Math.Max(0, namePos - 8);
            AccessBase.Wu16(page, 2, Math.Max(0, writer._pgSz - tdefLen - 8));
        }

        var (pages, logicalFirstDpOffsets) = SplitLogicalTDefIntoPages(page, namePos, firstDpOffsets);
        return (pages, logicalFirstDpOffsets, usedPagesOffsets);
    }

    public (int PageIndex, int PageOffset) LogicalToPhysicalTDefOffset(int logicalOffset)
    {
        if (logicalOffset < writer._pgSz)
        {
            return (0, logicalOffset);
        }

        int bodyPerCont = writer._pgSz - 8;
        int rest = logicalOffset - writer._pgSz;
        int contIdx = rest / bodyPerCont;
        int contOff = rest % bodyPerCont;
        return (1 + contIdx, 8 + contOff);
    }

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
            DatabaseFormat.AceAccdb => 0x02,
            _ => 0x01,
        };

        BuildMSysObjectsTDef(db, pgSz * 2, format, fullCatalogSchema);
        return db;
    }

    private static int GetDeclaredSize(byte type, int maxLength, DatabaseFormat format)
        => type switch
        {
            T_BOOL => 0,
            T_BYTE => 1,
            T_INT => 2,
            T_LONG => 4,
            T_MONEY => 8,
            T_FLOAT => 4,
            T_DOUBLE => 8,
            T_DATETIME => 8,
            T_GUID => 16,
            T_NUMERIC => 17,
            T_TEXT => format != DatabaseFormat.Jet3Mdb ? Math.Max(2, (maxLength > 0 ? maxLength : 255) * 2) : (maxLength > 0 ? maxLength : 255),
            T_BINARY => maxLength > 0 ? maxLength : 255,
            T_ATTACHMENT or T_COMPLEX => 4,
            _ => 0,
        };

    private static void BuildMSysObjectsTDef(byte[] db, int offset, DatabaseFormat format, bool fullCatalogSchema)
    {
        bool isJet3 = format == DatabaseFormat.Jet3Mdb;
        int tdNumCols = isJet3 ? 25 : 45;
        int tdBlockEnd = isJet3 ? 43 : 63;
        int colDescSz = isJet3 ? 18 : 25;
        int colTypeOff = 0;
        int colNumOff = isJet3 ? 1 : 5;
        int colVarOff = isJet3 ? 3 : 7;
        int colFlagsOff = isJet3 ? 13 : 15;
        int colFixedOff = isJet3 ? 14 : 21;
        int colSzOff = isJet3 ? 16 : 23;
        int textColSize = isJet3 ? 255 : 510;

        var columns = fullCatalogSchema ? BuildFullCatalogColumns(textColSize) : BuildSlimCatalogColumns(textColSize);

        int numCols = columns.Length;
        int numVarCols = 0;
        for (int i = 0; i < numCols; i++)
        {
            if (AccessWriter.IsVariableType(columns[i].Type))
            {
                numVarCols++;
            }
        }

        db[offset] = 0x02;
        db[offset + 1] = 0x01;
        AccessBase.Wi32(db, offset + 4, 0);
        db[offset + tdNumCols - 5] = 0x4E;
        AccessBase.Wu16(db, offset + tdNumCols - 4, numCols);
        AccessBase.Wu16(db, offset + tdNumCols - 2, numVarCols);
        AccessBase.Wu16(db, offset + tdNumCols, numCols);

        int colStart = offset + tdBlockEnd;
        int namePos = colStart + (numCols * colDescSz);

        for (int i = 0; i < numCols; i++)
        {
            var col = columns[i];
            int o = colStart + (i * colDescSz);

            db[o + colTypeOff] = col.Type;
            if (!isJet3)
            {
                AccessBase.Wi32(db, o + 1, Constants.TableDefinition.Jet4FormatMagic);
            }

            AccessBase.Wu16(db, o + colNumOff, col.ColNum);
            AccessBase.Wu16(db, o + colVarOff, col.VarIdx);
            db[o + colFlagsOff] = col.Flags;
            AccessBase.Wu16(db, o + colFixedOff, col.FixedOff);
            AccessBase.Wu16(db, o + colSzOff, col.Size);

            byte[] nameBytes = isJet3 ? Encoding.ASCII.GetBytes(col.Name) : Encoding.Unicode.GetBytes(col.Name);
            if (isJet3)
            {
                db[namePos] = (byte)nameBytes.Length;
                namePos += 1;
            }
            else
            {
                AccessBase.Wu16(db, namePos, nameBytes.Length);
                namePos += 2;
            }

            Buffer.BlockCopy(nameBytes, 0, db, namePos, nameBytes.Length);
            namePos += nameBytes.Length;
        }

        AccessBase.Wi32(db, offset + 8, Math.Max(0, namePos - offset - 8));
        if (!isJet3)
        {
            AccessBase.Wi32(db, offset + 0x0C, Constants.TableDefinition.Jet4FormatMagic);
            int tdefLen = Math.Max(0, namePos - offset - 8);
            int pgSz = AccessBase.GetPageSize(format);
            AccessBase.Wu16(db, offset + 2, Math.Max(0, pgSz - tdefLen - 8));
        }
    }

    private (byte[][] Pages, int[] FirstDpLogicalOffsets) SplitLogicalTDefIntoPages(byte[] logical, int usedLength, int[] firstDpLogicalOffsets)
    {
        if (usedLength <= writer._pgSz)
        {
            byte[] only = new byte[writer._pgSz];
            Buffer.BlockCopy(logical, 0, only, 0, writer._pgSz);
            return ([only], firstDpLogicalOffsets);
        }

        int bodyPerCont = writer._pgSz - 8;
        int continuationBodyBytes = usedLength - writer._pgSz;
        int continuationCount = (continuationBodyBytes + bodyPerCont - 1) / bodyPerCont;
        int totalPages = 1 + continuationCount;
        byte[][] pages = new byte[totalPages][];

        pages[0] = new byte[writer._pgSz];
        Buffer.BlockCopy(logical, 0, pages[0], 0, writer._pgSz);

        for (int p = 1; p < totalPages; p++)
        {
            byte[] cont = new byte[writer._pgSz];
            cont[0] = 0x02;
            cont[1] = 0x01;

            int srcOffset = writer._pgSz + ((p - 1) * bodyPerCont);
            int copyLen = Math.Min(bodyPerCont, usedLength - srcOffset);
            Buffer.BlockCopy(logical, srcOffset, cont, 8, copyLen);
            pages[p] = cont;
        }

        return (pages, firstDpLogicalOffsets);
    }

    private static (string Name, byte Type, int ColNum, int VarIdx, int FixedOff, int Size, byte Flags)[] BuildSlimCatalogColumns(int textColSize) =>
    [
        ("Id",          T_LONG,     0, 0, 0,  4,           0x03),
        ("ParentId",    T_LONG,     1, 0, 4,  4,           0x03),
        ("Name",        T_TEXT,     2, 0, 0,  textColSize, 0x02),
        ("Type",        T_INT,      3, 0, 8,  2,           0x03),
        ("DateCreate",  T_DATETIME, 4, 0, 10, 8,           0x03),
        ("DateUpdate",  T_DATETIME, 5, 0, 18, 8,           0x03),
        ("Flags",       T_LONG,     6, 0, 26, 4,           0x03),
        ("ForeignName", T_TEXT,     7, 1, 0,  textColSize, 0x02),
        ("Database",    T_TEXT,     8, 2, 0,  textColSize, 0x02),
    ];

    private static (string Name, byte Type, int ColNum, int VarIdx, int FixedOff, int Size, byte Flags)[] BuildFullCatalogColumns(int textColSize) =>
    [
        ("Id",           T_LONG,     0,  0, 0,  4,           0x13),
        ("ParentId",     T_LONG,     1,  0, 4,  4,           0x13),
        ("Name",         T_TEXT,     2,  0, 0,  textColSize, 0x12),
        ("Type",         T_INT,      3,  0, 8,  2,           0x13),
        ("DateCreate",   T_DATETIME, 4,  0, 10, 8,           0x13),
        ("DateUpdate",   T_DATETIME, 5,  0, 18, 8,           0x13),
        ("Owner",        T_BINARY,   6,  1, 0,  textColSize, 0x32),
        ("Flags",        T_LONG,     7,  0, 26, 4,           0x13),
        ("Database",     T_MEMO,     8,  2, 0,  0,           0x12),
        ("Connect",      T_MEMO,     9,  3, 0,  0,           0x12),
        ("ForeignName",  T_TEXT,     10, 4, 0,  textColSize, 0x12),
        ("RmtInfoShort", T_BINARY,   11, 5, 0,  textColSize, 0x12),
        ("RmtInfoLong",  T_OLE,      12, 6, 0,  0,           0x12),
        ("Lv",           T_OLE,      13, 7, 0,  0,           0x12),
        ("LvProp",       T_OLE,      14, 8, 0,  0,           0x12),
        ("LvModule",     T_OLE,      15, 9, 0,  0,           0x12),
        ("LvExtra",      T_OLE,      16, 10, 0, 0,           0x12),
    ];
}
