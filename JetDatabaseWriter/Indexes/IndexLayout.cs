namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Encapsulates the per-format byte offsets and entry sizes for a TDEF
/// page's real-index physical descriptor (§3.1) and logical-index entry
/// (§3.2) sections. The Jet4 / ACE layouts differ from Jet3 by:
/// <list type="bullet">
/// <item>a 4-byte leading cookie prepended to every logical-idx entry, and</item>
/// <item>a 4-byte <c>used_pages</c> slot inserted between <c>col_map</c> and
/// <c>first_dp</c> in the real-idx physical descriptor.</item>
/// </list>
/// Both shifts are folded into <see cref="LogicalEntryFieldsOffset"/> /
/// <see cref="RealIdxFieldsOffset"/>; consumers add those to a slot start to
/// reach the post-cookie field block whose offsets are format-invariant.
/// </summary>
internal readonly struct IndexLayout
{
    private IndexLayout(
        DatabaseFormat format,
        int realIdxPhysSize,
        int logicalEntrySize,
        int realIdxFieldsOffset,
        int logicalEntryFieldsOffset,
        int flagsOffsetWithinPhys,
        int colMapStartWithinPhys)
    {
        Format = format;
        RealIdxPhysSize = realIdxPhysSize;
        LogicalEntrySize = logicalEntrySize;
        RealIdxFieldsOffset = realIdxFieldsOffset;
        LogicalEntryFieldsOffset = logicalEntryFieldsOffset;
        FlagsOffsetWithinPhys = flagsOffsetWithinPhys;
        ColMapStartWithinPhys = colMapStartWithinPhys;
    }

    /// <summary>Gets the database format this layout describes.</summary>
    public DatabaseFormat Format { get; }

    /// <summary>Gets the size in bytes of one real-idx physical descriptor (Jet3: 39, Jet4/ACE: 52).</summary>
    public int RealIdxPhysSize { get; }

    /// <summary>Gets the size in bytes of one logical-idx entry (Jet3: 20, Jet4/ACE: 28).</summary>
    public int LogicalEntrySize { get; }

    /// <summary>
    /// Gets the byte offset within a real-idx physical descriptor at which
    /// the post-<c>col_map</c> field block begins (Jet3: 0, Jet4/ACE: 4).
    /// Add to a phys slot start, then offset by
    /// <see cref="Constants.TableDefinition.Jet3.RealIdx.FirstDpOffset"/>.
    /// (For the <c>flags</c> byte, use <see cref="FlagsOffsetWithinPhys"/>
    /// instead — its placement is not a fixed shift on top of this offset.)
    /// </summary>
    public int RealIdxFieldsOffset { get; }

    /// <summary>
    /// Gets the byte offset within a logical-idx entry at which the
    /// format-invariant field block begins (Jet3: 0, Jet4/ACE: 4 to skip the
    /// leading cookie). Add to an entry start, then offset by one of
    /// the <c>*FieldOffset</c> constants below.
    /// </summary>
    public int LogicalEntryFieldsOffset { get; }

    /// <summary>
    /// Gets the absolute offset of the real-idx <c>flags</c> byte from the
    /// start of the physical descriptor. Format-dependent because Jet4
    /// inserts a 4-byte “unknown” gap between <c>first_dp</c> and
    /// <c>flags</c> that Jet3 does not (Jet3: 38, Jet4/ACE: 46). This cannot
    /// be expressed by adding a fixed constant to <see cref="RealIdxFieldsOffset"/>
    /// because the gap is independent of the leading-cookie shift.
    /// </summary>
    public int FlagsOffsetWithinPhys { get; }

    /// <summary>
    /// Gets the byte offset of the <c>col_map</c> block within a real-idx
    /// physical descriptor. Format-dependent because Jet4/ACE prepends a
    /// 4-byte “magic” cookie before <c>col_map</c> that Jet3 does not
    /// (Jet3: 0, Jet4/ACE: 4). Per mdbtools <see href="HACKING.md" />: Jet3 phys
    /// descriptor is <c>col_map(30) + used_pages(4) + first_dp(4) + flags(1)</c>;
    /// Jet4 phys descriptor is <c>magic(4) + col_map(30) + …</c>.
    /// </summary>
    public int ColMapStartWithinPhys { get; }

    /// <summary>Returns the layout matching <paramref name="format"/>.</summary>
    /// <param name="format">The format.</param>
    public static IndexLayout For(DatabaseFormat format) => format == DatabaseFormat.Jet3Mdb
        ? new IndexLayout(
            format,
            realIdxPhysSize: Constants.TableDefinition.Jet3.RealIdx.PhysSize,
            logicalEntrySize: Constants.TableDefinition.Jet3.LogicalIdx.EntrySize,
            realIdxFieldsOffset: 0,
            logicalEntryFieldsOffset: 0,
            flagsOffsetWithinPhys: Constants.TableDefinition.Jet3.RealIdx.FlagsOffset,
            colMapStartWithinPhys: Constants.TableDefinition.Jet3.RealIdx.ColMapOffset)
        : new IndexLayout(
            format,
            realIdxPhysSize: Constants.TableDefinition.Jet4.RealIdx.PhysSize,
            logicalEntrySize: Constants.TableDefinition.Jet4.LogicalIdx.EntrySize,
            realIdxFieldsOffset: 4,
            logicalEntryFieldsOffset: 4,
            flagsOffsetWithinPhys: Constants.TableDefinition.Jet4.RealIdx.FlagsOffset,
            colMapStartWithinPhys: Constants.TableDefinition.Jet4.RealIdx.ColMapOffset);

    /// <summary>Walks the 10-slot <c>col_map</c> in a real-idx physical descriptor, invoking <paramref name="onColumn"/> for each populated slot.</summary>
    /// <param name="td">TDEF byte buffer.</param>
    /// <param name="physStart">Absolute byte offset of the real-idx physical descriptor within <paramref name="td"/>.</param>
    /// <param name="onColumn">Callback receiving each non-padding (column, ascending) pair.</param>
    public void ReadColMap(ReadOnlySpan<byte> td, int physStart, Action<KeyColumn> onColumn)
    {
        int colMapStart = physStart + ColMapStartWithinPhys;
        for (int slot = 0; slot < Constants.TableDefinition.ColMapSlotCount; slot++)
        {
            int so = colMapStart + (slot * Constants.TableDefinition.ColMapSlotSize);
            int colNum = Ru16(td, so);
            if (colNum == Constants.TableDefinition.ColMapPaddingSlot)
            {
                continue;
            }

            // Per Jackcess `IndexData.ASCENDING_COLUMN_FLAG = 0x01`: bit 0x01
            // set in the col_map flag byte means ascending; clear means
            // descending. (Microsoft Access writes 0x01 / 0x00; this library's
            // writer historically wrote 0x01 / 0x02 — both readings of the
            // 0x01 bit yield the correct result, so this masks both writers'
            // conventions correctly.)
            onColumn(new KeyColumn(colNum, (td[so + 2] & 0x01) != 0));
        }
    }

    /// <summary>Returns the absolute byte offset of a <c>col_map</c> slot's <c>col_num</c> within a TDEF buffer.</summary>
    /// <param name="physStart">The phys start.</param>
    /// <param name="slot">The zero-based slot index.</param>
    public int ColMapSlotOffset(int physStart, int slot)
        => physStart + ColMapStartWithinPhys + (slot * Constants.TableDefinition.ColMapSlotSize);

    /// <summary>
    /// Walks the 10-slot <c>col_map</c> in a real-idx physical descriptor and
    /// returns each populated slot as a <see cref="KeyColumn"/>. Equivalent
    /// to <see cref="ReadColMap"/> but materialises the result as a list,
    /// which is what every consumer that decodes a real-idx slot ultimately
    /// builds.
    /// </summary>
    /// <param name="td">The table-definition buffer.</param>
    /// <param name="physStart">The phys start.</param>
    public List<KeyColumn> ReadColMapEntries(ReadOnlySpan<byte> td, int physStart)
    {
        var result = new List<KeyColumn>(Constants.TableDefinition.ColMapSlotCount);
        int colMapStart = physStart + ColMapStartWithinPhys;
        for (int slot = 0; slot < Constants.TableDefinition.ColMapSlotCount; slot++)
        {
            int so = colMapStart + (slot * Constants.TableDefinition.ColMapSlotSize);
            int colNum = Ru16(td, so);
            if (colNum == Constants.TableDefinition.ColMapPaddingSlot)
            {
                continue;
            }

            // See ReadColMap for the 0x01-bit ascending-flag rationale.
            result.Add(new KeyColumn(colNum, (td[so + 2] & 0x01) != 0));
        }

        return result;
    }

    /// <summary>
    /// Returns the absolute byte offset within a TDEF buffer at which a real-idx
    /// physical descriptor's <c>first_dp</c> field begins.
    /// </summary>
    /// <param name="physStart">The phys start.</param>
    public int FirstDpAbsoluteOffset(int physStart)
        => physStart + RealIdxFieldsOffset + Constants.TableDefinition.Jet3.RealIdx.FirstDpOffset;

    /// <summary>
    /// Returns the absolute byte offset within a TDEF buffer at which a real-idx
    /// physical descriptor's <c>flags</c> byte begins.
    /// </summary>
    /// <param name="physStart">The phys start.</param>
    public int FlagsAbsoluteOffset(int physStart)
        => physStart + FlagsOffsetWithinPhys;

    /// <summary>
    /// Returns the absolute byte offset within a TDEF buffer of the
    /// <paramref name="slot"/>-th real-idx physical descriptor, given the
    /// start of the real-idx descriptor block.
    /// </summary>
    /// <param name="realIdxDescStart">The real index desc start.</param>
    /// <param name="slot">The zero-based slot index.</param>
    public int RealIdxPhysOffset(int realIdxDescStart, int slot)
        => realIdxDescStart + (slot * RealIdxPhysSize);

    /// <summary>
    /// Returns the absolute byte offset within a TDEF buffer at which the
    /// logical-idx entry block begins (immediately after the
    /// <paramref name="numRealIdx"/> real-idx physical descriptors).
    /// </summary>
    /// <param name="realIdxDescStart">The real index desc start.</param>
    /// <param name="numRealIdx">The number of real index.</param>
    public int LogicalIdxStart(int realIdxDescStart, int numRealIdx)
        => realIdxDescStart + (numRealIdx * RealIdxPhysSize);

    /// <summary>
    /// Returns the absolute byte offset within a TDEF buffer of the
    /// <paramref name="slot"/>-th logical-idx entry (start of the entry,
    /// including the leading 4-byte cookie on Jet4/ACE).
    /// </summary>
    /// <param name="logIdxStart">The log index start.</param>
    /// <param name="slot">The zero-based slot index.</param>
    public int LogicalIdxEntryOffset(int logIdxStart, int slot)
        => logIdxStart + (slot * LogicalEntrySize);

    /// <summary>
    /// Returns the absolute byte offset within a TDEF buffer at which the
    /// <paramref name="slot"/>-th logical-idx entry's format-invariant field
    /// block begins. Add one of
    /// <see cref="Constants.TableDefinition.Jet3.LogicalIdx"/> member offsets
    /// (e.g. <see cref="Constants.TableDefinition.Jet3.LogicalIdx.IndexTypeOffset"/>)
    /// to reach a specific field.
    /// </summary>
    /// <param name="logIdxStart">The log index start.</param>
    /// <param name="slot">The zero-based slot index.</param>
    public int LogicalIdxFieldsOffset(int logIdxStart, int slot)
        => logIdxStart + (slot * LogicalEntrySize) + LogicalEntryFieldsOffset;

    /// <summary>
    /// Returns the absolute byte offset within a TDEF buffer at which the
    /// logical-idx names block begins (immediately after the
    /// <paramref name="numIdx"/> logical-idx entries).
    /// </summary>
    /// <param name="logIdxStart">The log index start.</param>
    /// <param name="numIdx">The number of index.</param>
    public int LogicalIdxNamesStart(int logIdxStart, int numIdx)
        => logIdxStart + (numIdx * LogicalEntrySize);

    /// <summary>
    /// Combines <see cref="RealIdxPhysOffset"/> with a bounds check against
    /// <paramref name="bufferLength"/>. Returns <see langword="true"/> and
    /// emits <paramref name="physStart"/> when the slot fits entirely within
    /// the buffer; otherwise returns <see langword="false"/>.
    /// </summary>
    /// <param name="realIdxDescStart">The real index desc start.</param>
    /// <param name="slot">The zero-based slot index.</param>
    /// <param name="bufferLength">The buffer length.</param>
    /// <param name="physStart">The phys start.</param>
    public bool TryGetRealIdxPhysOffset(int realIdxDescStart, int slot, int bufferLength, out int physStart)
    {
        physStart = realIdxDescStart + (slot * RealIdxPhysSize);
        return physStart + RealIdxPhysSize <= bufferLength;
    }

    /// <summary>
    /// Combines <see cref="LogicalIdxFieldsOffset"/> with a bounds check
    /// against <paramref name="bufferLength"/>. Returns <see langword="true"/>
    /// and emits <paramref name="fieldsOffset"/> (the start of the
    /// format-invariant field block; add a
    /// <see cref="Constants.TableDefinition.Jet3.LogicalIdx"/> offset to
    /// reach a specific field) when the slot fits entirely within the
    /// buffer; otherwise returns <see langword="false"/>.
    /// </summary>
    /// <param name="logIdxStart">The log index start.</param>
    /// <param name="slot">The zero-based slot index.</param>
    /// <param name="bufferLength">The buffer length.</param>
    /// <param name="fieldsOffset">The fields offset.</param>
    public bool TryGetLogicalIdxFieldsOffset(int logIdxStart, int slot, int bufferLength, out int fieldsOffset)
    {
        int entryStart = logIdxStart + (slot * LogicalEntrySize);
        if (entryStart + LogicalEntrySize > bufferLength)
        {
            fieldsOffset = 0;
            return false;
        }

        fieldsOffset = entryStart + LogicalEntryFieldsOffset;
        return true;
    }

    /// <summary>
    /// Decoded view of a single populated <c>col_map</c> entry: the column
    /// number and ascending/descending direction.
    /// </summary>
    /// <param name="ColNum">The col number of.</param>
    /// <param name="Ascending">The ascending.</param>
    public readonly record struct KeyColumn(int ColNum, bool Ascending);

    /// <summary>
    /// Computes the three anchors that drive every TDEF index-section walk:
    /// the real-idx descriptor block start, the logical-idx entry block start,
    /// and the logical-idx names block start. Combines
    /// <see cref="LogicalIdxStart"/> + <see cref="LogicalIdxNamesStart"/>.
    /// </summary>
    /// <param name="realIdxDescStart">The real index desc start.</param>
    /// <param name="numRealIdx">The number of real index.</param>
    /// <param name="numIdx">The number of index.</param>
    public IndexSectionAnchors GetIndexSection(int realIdxDescStart, int numRealIdx, int numIdx)
    {
        int logIdxStart = LogicalIdxStart(realIdxDescStart, numRealIdx);
        int logIdxNamesStart = LogicalIdxNamesStart(logIdxStart, numIdx);
        return new IndexSectionAnchors(realIdxDescStart, logIdxStart, logIdxNamesStart, numRealIdx, numIdx);
    }

    /// <summary>
    /// Locates the <paramref name="slot"/>-th real-idx physical descriptor and
    /// reads its <c>flags</c> byte and <c>first_dp</c> field offset in one call.
    /// Combines <see cref="TryGetRealIdxPhysOffset"/> with
    /// <see cref="FlagsAbsoluteOffset"/> + <see cref="FirstDpAbsoluteOffset"/>.
    /// Returns <see langword="false"/> when the slot does not fit within
    /// <paramref name="td"/>.
    /// </summary>
    /// <param name="td">The table-definition buffer.</param>
    /// <param name="realIdxDescStart">The real index desc start.</param>
    /// <param name="slot">The zero-based slot index.</param>
    /// <param name="info">The index metadata.</param>
    public bool TryReadRealIdxSlot(ReadOnlySpan<byte> td, int realIdxDescStart, int slot, out RealIdxSlot info)
    {
        if (!TryGetRealIdxPhysOffset(realIdxDescStart, slot, td.Length, out int phys))
        {
            info = default;
            return false;
        }

        int firstDpOffset = phys + RealIdxFieldsOffset + Constants.TableDefinition.Jet3.RealIdx.FirstDpOffset;
        int flagsOffset = phys + FlagsOffsetWithinPhys;
        info = new RealIdxSlot(phys, firstDpOffset, td[flagsOffset]);
        return true;
    }

    /// <summary>
    /// Combines <see cref="TryReadRealIdxSlot"/> with
    /// <see cref="ReadColMapEntries"/>: locates the <paramref name="slot"/>-th
    /// real-idx physical descriptor, decodes its <c>flags</c> /
    /// <c>first_dp</c> offsets, and materialises every populated
    /// <c>col_map</c> entry into <paramref name="keyColumns"/>. Returns
    /// <see langword="false"/> when the slot does not fit within
    /// <paramref name="td"/>; <paramref name="keyColumns"/> is then an
    /// empty list.
    /// </summary>
    /// <param name="td">The table-definition buffer.</param>
    /// <param name="realIdxDescStart">The real index desc start.</param>
    /// <param name="slot">The zero-based slot index.</param>
    /// <param name="info">The index metadata.</param>
    /// <param name="keyColumns">The key columns.</param>
    public bool TryReadRealIdxSlotWithKeyColumns(
        ReadOnlySpan<byte> td,
        int realIdxDescStart,
        int slot,
        out RealIdxSlot info,
        out List<KeyColumn> keyColumns)
    {
        if (!TryReadRealIdxSlot(td, realIdxDescStart, slot, out info))
        {
            keyColumns = [];
            return false;
        }

        keyColumns = ReadColMapEntries(td, info.PhysStart);
        return true;
    }

    /// <summary>
    /// Locates the <paramref name="slot"/>-th logical-idx entry and decodes
    /// every format-invariant field into a <see cref="LogicalIdxEntry"/>.
    /// Combines <see cref="TryGetLogicalIdxFieldsOffset"/> with the
    /// per-field reads that all consumers repeat. Returns
    /// <see langword="false"/> when the slot does not fit within
    /// <paramref name="td"/>.
    /// </summary>
    /// <param name="td">The table-definition buffer.</param>
    /// <param name="logIdxStart">The log index start.</param>
    /// <param name="slot">The zero-based slot index.</param>
    /// <param name="entry">The entry.</param>
    public bool TryReadLogicalEntry(ReadOnlySpan<byte> td, int logIdxStart, int slot, out LogicalIdxEntry entry)
    {
        if (!TryGetLogicalIdxFieldsOffset(logIdxStart, slot, td.Length, out int e))
        {
            entry = default;
            return false;
        }

        entry = new LogicalIdxEntry(
            e,
            Ri32(td, e + Constants.TableDefinition.Jet3.LogicalIdx.IndexNumOffset),
            Ri32(td, e + Constants.TableDefinition.Jet3.LogicalIdx.IndexNum2Offset),
            Ri32(td, e + Constants.TableDefinition.Jet3.LogicalIdx.RelIdxNumOffset),
            Ri32(td, e + Constants.TableDefinition.Jet3.LogicalIdx.RelTblPageOffset),
            td[e + Constants.TableDefinition.Jet3.LogicalIdx.CascadeUpsOffset],
            td[e + Constants.TableDefinition.Jet3.LogicalIdx.CascadeDelsOffset],
            (IndexKind)td[e + Constants.TableDefinition.Jet3.LogicalIdx.IndexTypeOffset]);
        return true;
    }

    /// <summary>
    /// Anchors of the three index-related blocks within a TDEF buffer,
    /// plus the populated real-idx / logical-idx slot counts (TDEF header
    /// fields), returned by <see cref="GetIndexSection"/>. Bundles every
    /// piece of state a catalog walker needs so callers pass a single value
    /// instead of four parallel arguments.
    /// </summary>
    /// <param name="RealIdxDescStart">The real index desc start.</param>
    /// <param name="LogIdxStart">The log index start.</param>
    /// <param name="LogIdxNamesStart">The log index names start.</param>
    /// <param name="NumRealIdx">The number of real index.</param>
    /// <param name="NumIdx">The number of index.</param>
    public readonly record struct IndexSectionAnchors(
        int RealIdxDescStart,
        int LogIdxStart,
        int LogIdxNamesStart,
        int NumRealIdx,
        int NumIdx);

    /// <summary>
    /// Decoded view of a single real-idx physical descriptor's
    /// <c>flags</c> byte and <c>first_dp</c> field offset, returned by
    /// <see cref="TryReadRealIdxSlot"/>.
    /// </summary>
    /// <param name="PhysStart">The phys start.</param>
    /// <param name="FirstDpOffset">The first data page offset.</param>
    /// <param name="Flags">The flags.</param>
    public readonly record struct RealIdxSlot(int PhysStart, int FirstDpOffset, byte Flags)
    {
        /// <summary>Gets a value indicating whether the unique flag bit (0x01) is set.</summary>
        public bool IsUnique => (Flags & Constants.TableDefinition.UniqueIndexFlag) != 0;

        /// <summary>
        /// Lifts this raw slot into a <see cref="RealIdxEntry"/> by attaching
        /// the decoded <paramref name="keyColumns"/>. By default the entry's
        /// <see cref="RealIdxEntry.IsUnique"/> mirrors this slot's
        /// <see cref="IsUnique"/> (the real-idx <c>flags &amp; 0x01</c> bit);
        /// pass <paramref name="overrideUnique"/> to substitute (e.g.
        /// <c>false</c> when the caller will resolve uniqueness later, or
        /// <c>true</c> when an associated logical-idx PK promotes the slot).
        /// </summary>
        /// <param name="keyColumns">The key columns.</param>
        /// <param name="overrideUnique">The override unique.</param>
        public RealIdxEntry ToEntry(IReadOnlyList<KeyColumn> keyColumns, bool? overrideUnique = null)
            => new(keyColumns, FirstDpOffset, overrideUnique ?? IsUnique);
    }

    /// <summary>
    /// Decoded view of a single logical-idx entry's format-invariant fields,
    /// returned by <see cref="TryReadLogicalEntry"/>. <see cref="IndexType"/>
    /// is exposed as <see cref="IndexKind"/> rather than the raw byte so
    /// consumers compare against enum values directly.
    /// </summary>
    /// <param name="FieldsOffset">The fields offset.</param>
    /// <param name="IndexNum">The index number of.</param>
    /// <param name="IndexNum2">The index num2.</param>
    /// <param name="RelIdxNum">The relationship index number of.</param>
    /// <param name="RelTblPage">The relationship table page.</param>
    /// <param name="CascadeUps">The cascade ups.</param>
    /// <param name="CascadeDels">The cascade dels.</param>
    /// <param name="IndexType">The index type.</param>
    public readonly record struct LogicalIdxEntry(
        int FieldsOffset,
        int IndexNum,
        int IndexNum2,
        int RelIdxNum,
        int RelTblPage,
        byte CascadeUps,
        byte CascadeDels,
        IndexKind IndexType);

    /// <summary>
    /// One real-idx slot decoded into its full <c>col_map</c> key list, the
    /// absolute byte offset of its <c>first_dp</c> field within the TDEF
    /// buffer, and the resolved unique flag (real-idx <c>flags &amp; 0x01</c>
    /// OR an associated logical-idx with <c>index_type = 0x01</c>). Used by
    /// the writer's index-maintenance and unique-check paths to carry
    /// per-real-idx state without re-decoding the TDEF block.
    /// </summary>
    /// <param name="IndexKeyColumns">The index key columns.</param>
    /// <param name="FirstDpOffset">The first data page offset.</param>
    /// <param name="IsUnique">A value indicating whether is unique.</param>
    public readonly record struct RealIdxEntry(
        IReadOnlyList<KeyColumn> IndexKeyColumns,
        int FirstDpOffset,
        bool IsUnique);

    /// <summary>
    /// One real-idx key column resolved against the table's
    /// <see cref="ColumnInfo"/> list: the column descriptor, the row-snapshot
    /// index (which differs from <c>ColNum</c> when columns have been
    /// deleted), and the ascending/descending direction copied from the
    /// originating <c>col_map</c> slot.
    /// </summary>
    /// <param name="Col">The column descriptor.</param>
    /// <param name="SnapIdx">The snap index.</param>
    /// <param name="Ascending">The ascending.</param>
    public readonly record struct KeyColumnInfo(ColumnInfo Col, int SnapIdx, bool Ascending);

    /// <summary>
    /// One unique real-idx slot bundled with a best-effort logical-idx name
    /// (used in error messages) and the resolved <see cref="KeyColumnInfo"/>
    /// list. Produced by the writer's pre-write unique-index loader and
    /// consumed by the composite-key encoder + collision detector.
    /// </summary>
    /// <param name="RealIdxNum">The real index number of.</param>
    /// <param name="Name">The name.</param>
    /// <param name="KeyColumns">The key columns.</param>
    public readonly record struct UniqueIndexDescriptor(
        int RealIdxNum,
        string Name,
        IReadOnlyList<KeyColumnInfo> KeyColumns);

    /// <summary>
    /// Resolves an index's <see cref="KeyColumn"/> list against a table's
    /// <see cref="ColumnInfo"/> list, producing one <see cref="KeyColumnInfo"/>
    /// per key column. Returns <see langword="false"/> when any
    /// <c>ColNum</c> cannot be located (e.g. snapshot reflects a column
    /// deletion that has not yet been propagated to the index slot); on
    /// failure <paramref name="keyColInfos"/> is set to an empty list.
    /// </summary>
    /// <param name="indexKeyColumns">The index key columns.</param>
    /// <param name="tableColumns">The table columns.</param>
    /// <param name="snapshotIndexByColNum">The snapshot index by col number of.</param>
    /// <param name="keyColInfos">The key col infos.</param>
    public static bool TryResolveKeyColumnInfos(
        IReadOnlyList<KeyColumn> indexKeyColumns,
        IReadOnlyList<ColumnInfo> tableColumns,
        IReadOnlyDictionary<int, int> snapshotIndexByColNum,
        out List<KeyColumnInfo> keyColInfos)
    {
        var infos = new List<KeyColumnInfo>(indexKeyColumns.Count);
        for (int i = 0; i < indexKeyColumns.Count; i++)
        {
            (int colNum, bool ascending) = indexKeyColumns[i];
            if (!snapshotIndexByColNum.TryGetValue(colNum, out int snapIdx))
            {
                keyColInfos = [];
                return false;
            }

            ColumnInfo? col = null;
            for (int c = 0; c < tableColumns.Count; c++)
            {
                if (tableColumns[c].ColNum == colNum)
                {
                    col = tableColumns[c];
                    break;
                }
            }

            if (col is null)
            {
                keyColInfos = [];
                return false;
            }

            infos.Add(new KeyColumnInfo(tableColumns[snapIdx], snapIdx, ascending));
        }

        keyColInfos = infos;
        return true;
    }
}
