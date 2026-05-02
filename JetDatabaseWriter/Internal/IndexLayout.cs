namespace JetDatabaseWriter.Internal;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Internal.Models;

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
    /// <summary>Number of <c>col_map</c> slots in a real-idx physical descriptor (always 10).</summary>
    public const int ColMapSlotCount = Constants.TableDefinition.ColMapSlotCount;

    /// <summary>Sentinel <c>col_num</c> value marking an unused col_map slot.</summary>
    public const ushort ColMapPaddingSlot = Constants.TableDefinition.ColMapPaddingSlot;

    /// <summary>Offset (post <see cref="RealIdxFieldsOffset"/>) of <c>first_dp</c>: root page of the index B-tree.</summary>
    public const int FirstDpFieldOffset = Constants.TableDefinition.Jet3.RealIdx.FirstDpOffset;

    /// <summary>Offset (post <see cref="RealIdxFieldsOffset"/>) of the real-idx <c>flags</c> byte.</summary>
    public const int FlagsFieldOffset = Constants.TableDefinition.Jet3.RealIdx.FlagsOffset;

    /// <summary>Offset (post <see cref="LogicalEntryFieldsOffset"/>) of <c>index_num</c>.</summary>
    public const int IndexNumFieldOffset = Constants.TableDefinition.Jet3.LogicalIdx.IndexNumOffset;

    /// <summary>Offset (post <see cref="LogicalEntryFieldsOffset"/>) of <c>index_num2</c> (backing real-idx slot).</summary>
    public const int IndexNum2FieldOffset = Constants.TableDefinition.Jet3.LogicalIdx.IndexNum2Offset;

    /// <summary>Offset (post <see cref="LogicalEntryFieldsOffset"/>) of <c>rel_idx_num</c>.</summary>
    public const int RelIdxNumFieldOffset = Constants.TableDefinition.Jet3.LogicalIdx.RelIdxNumOffset;

    /// <summary>Offset (post <see cref="LogicalEntryFieldsOffset"/>) of <c>rel_tbl_page</c>.</summary>
    public const int RelTblPageFieldOffset = Constants.TableDefinition.Jet3.LogicalIdx.RelTblPageOffset;

    /// <summary>Offset (post <see cref="LogicalEntryFieldsOffset"/>) of <c>cascade_ups</c>.</summary>
    public const int CascadeUpsFieldOffset = Constants.TableDefinition.Jet3.LogicalIdx.CascadeUpsOffset;

    /// <summary>Offset (post <see cref="LogicalEntryFieldsOffset"/>) of <c>cascade_dels</c>.</summary>
    public const int CascadeDelsFieldOffset = Constants.TableDefinition.Jet3.LogicalIdx.CascadeDelsOffset;

    /// <summary>Offset (post <see cref="LogicalEntryFieldsOffset"/>) of <c>index_type</c>.</summary>
    public const int IndexTypeFieldOffset = Constants.TableDefinition.Jet3.LogicalIdx.IndexTypeOffset;

    /// <summary>
    /// Byte offset of the <c>col_map</c> block within a real-idx physical
    /// descriptor. Starts immediately after the 4-byte umap pointer; identical
    /// between Jet3 (PhysSize 39) and Jet4/ACE (PhysSize 52).
    /// </summary>
    public const int ColMapStartWithinPhys = Constants.TableDefinition.Jet3.RealIdx.ColMapOffset;

    /// <summary>Size in bytes of one <c>col_map</c> slot: <c>{col_num(2), col_order(1)}</c>.</summary>
    public const int ColMapSlotSize = Constants.TableDefinition.ColMapSlotSize;

    private IndexLayout(
        DatabaseFormat format,
        int realIdxPhysSize,
        int logicalEntrySize,
        int realIdxFieldsOffset,
        int logicalEntryFieldsOffset)
    {
        Format = format;
        RealIdxPhysSize = realIdxPhysSize;
        LogicalEntrySize = logicalEntrySize;
        RealIdxFieldsOffset = realIdxFieldsOffset;
        LogicalEntryFieldsOffset = logicalEntryFieldsOffset;
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
    /// Add to a phys slot start, then offset by <see cref="FirstDpFieldOffset"/>
    /// or <see cref="FlagsFieldOffset"/>.
    /// </summary>
    public int RealIdxFieldsOffset { get; }

    /// <summary>
    /// Gets the byte offset within a logical-idx entry at which the
    /// format-invariant field block begins (Jet3: 0, Jet4/ACE: 4 to skip the
    /// leading cookie). Add to an entry start, then offset by one of
    /// the <c>*FieldOffset</c> constants below.
    /// </summary>
    public int LogicalEntryFieldsOffset { get; }

    /// <summary>Returns the layout matching <paramref name="format"/>.</summary>
    public static IndexLayout For(DatabaseFormat format) => format == DatabaseFormat.Jet3Mdb
        ? new IndexLayout(
            format,
            realIdxPhysSize: Constants.TableDefinition.Jet3.RealIdx.PhysSize,
            logicalEntrySize: Constants.TableDefinition.Jet3.LogicalIdx.EntrySize,
            realIdxFieldsOffset: 0,
            logicalEntryFieldsOffset: 0)
        : new IndexLayout(
            format,
            realIdxPhysSize: Constants.TableDefinition.Jet4.RealIdx.PhysSize,
            logicalEntrySize: Constants.TableDefinition.Jet4.LogicalIdx.EntrySize,
            realIdxFieldsOffset: 4,
            logicalEntryFieldsOffset: 4);

    /// <summary>Walks the 10-slot <c>col_map</c> in a real-idx physical descriptor, invoking <paramref name="onColumn"/> for each populated slot.</summary>
    /// <param name="td">TDEF byte buffer.</param>
    /// <param name="physStart">Absolute byte offset of the real-idx physical descriptor within <paramref name="td"/>.</param>
    /// <param name="onColumn">Callback receiving each non-padding (column, ascending) pair.</param>
    public static void ReadColMap(ReadOnlySpan<byte> td, int physStart, Action<KeyColumn> onColumn)
    {
        int colMapStart = physStart + ColMapStartWithinPhys;
        for (int slot = 0; slot < ColMapSlotCount; slot++)
        {
            int so = colMapStart + (slot * ColMapSlotSize);
            int colNum = BinaryPrimitives.ReadUInt16LittleEndian(td.Slice(so, 2));
            if (colNum == ColMapPaddingSlot)
            {
                continue;
            }

            onColumn(new KeyColumn(colNum, td[so + 2] != 0x02));
        }
    }

    /// <summary>Returns the absolute byte offset of a <c>col_map</c> slot's <c>col_num</c> within a TDEF buffer.</summary>
    public static int ColMapSlotOffset(int physStart, int slot)
        => physStart + ColMapStartWithinPhys + (slot * ColMapSlotSize);

    /// <summary>
    /// Walks the 10-slot <c>col_map</c> in a real-idx physical descriptor and
    /// returns each populated slot as a <see cref="KeyColumn"/>. Equivalent
    /// to <see cref="ReadColMap"/> but materialises the result as a list,
    /// which is what every consumer that decodes a real-idx slot ultimately
    /// builds.
    /// </summary>
    public static List<KeyColumn> ReadColMapEntries(ReadOnlySpan<byte> td, int physStart)
    {
        var result = new List<KeyColumn>(ColMapSlotCount);
        int colMapStart = physStart + ColMapStartWithinPhys;
        for (int slot = 0; slot < ColMapSlotCount; slot++)
        {
            int so = colMapStart + (slot * ColMapSlotSize);
            int colNum = BinaryPrimitives.ReadUInt16LittleEndian(td.Slice(so, 2));
            if (colNum == ColMapPaddingSlot)
            {
                continue;
            }

            result.Add(new KeyColumn(colNum, td[so + 2] != 0x02));
        }

        return result;
    }

    /// <summary>
    /// Returns the absolute byte offset within a TDEF buffer at which a real-idx
    /// physical descriptor's <c>first_dp</c> field begins.
    /// </summary>
    public int FirstDpAbsoluteOffset(int physStart)
        => physStart + RealIdxFieldsOffset + FirstDpFieldOffset;

    /// <summary>
    /// Returns the absolute byte offset within a TDEF buffer at which a real-idx
    /// physical descriptor's <c>flags</c> byte begins.
    /// </summary>
    public int FlagsAbsoluteOffset(int physStart)
        => physStart + RealIdxFieldsOffset + FlagsFieldOffset;

    /// <summary>
    /// Returns the absolute byte offset within a TDEF buffer of the
    /// <paramref name="slot"/>-th real-idx physical descriptor, given the
    /// start of the real-idx descriptor block.
    /// </summary>
    public int RealIdxPhysOffset(int realIdxDescStart, int slot)
        => realIdxDescStart + (slot * RealIdxPhysSize);

    /// <summary>
    /// Returns the absolute byte offset within a TDEF buffer at which the
    /// logical-idx entry block begins (immediately after the
    /// <paramref name="numRealIdx"/> real-idx physical descriptors).
    /// </summary>
    public int LogicalIdxStart(int realIdxDescStart, int numRealIdx)
        => realIdxDescStart + (numRealIdx * RealIdxPhysSize);

    /// <summary>
    /// Returns the absolute byte offset within a TDEF buffer of the
    /// <paramref name="slot"/>-th logical-idx entry (start of the entry,
    /// including the leading 4-byte cookie on Jet4/ACE).
    /// </summary>
    public int LogicalIdxEntryOffset(int logIdxStart, int slot)
        => logIdxStart + (slot * LogicalEntrySize);

    /// <summary>
    /// Returns the absolute byte offset within a TDEF buffer at which the
    /// <paramref name="slot"/>-th logical-idx entry's format-invariant field
    /// block begins. Add one of the <c>*FieldOffset</c> constants
    /// (e.g. <see cref="IndexTypeFieldOffset"/>) to reach a specific field.
    /// </summary>
    public int LogicalIdxFieldsOffset(int logIdxStart, int slot)
        => logIdxStart + (slot * LogicalEntrySize) + LogicalEntryFieldsOffset;

    /// <summary>
    /// Returns the absolute byte offset within a TDEF buffer at which the
    /// logical-idx names block begins (immediately after the
    /// <paramref name="numIdx"/> logical-idx entries).
    /// </summary>
    public int LogicalIdxNamesStart(int logIdxStart, int numIdx)
        => logIdxStart + (numIdx * LogicalEntrySize);

    /// <summary>
    /// Combines <see cref="RealIdxPhysOffset"/> with a bounds check against
    /// <paramref name="bufferLength"/>. Returns <see langword="true"/> and
    /// emits <paramref name="physStart"/> when the slot fits entirely within
    /// the buffer; otherwise returns <see langword="false"/>.
    /// </summary>
    public bool TryGetRealIdxPhysOffset(int realIdxDescStart, int slot, int bufferLength, out int physStart)
    {
        physStart = realIdxDescStart + (slot * RealIdxPhysSize);
        return physStart + RealIdxPhysSize <= bufferLength;
    }

    /// <summary>
    /// Combines <see cref="LogicalIdxFieldsOffset"/> with a bounds check
    /// against <paramref name="bufferLength"/>. Returns <see langword="true"/>
    /// and emits <paramref name="fieldsOffset"/> (the start of the
    /// format-invariant field block; add a <c>*FieldOffset</c> constant to
    /// reach a specific field) when the slot fits entirely within the
    /// buffer; otherwise returns <see langword="false"/>.
    /// </summary>
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
    public readonly record struct KeyColumn(int ColNum, bool Ascending);

    /// <summary>
    /// Computes the three anchors that drive every TDEF index-section walk:
    /// the real-idx descriptor block start, the logical-idx entry block start,
    /// and the logical-idx names block start. Combines
    /// <see cref="LogicalIdxStart"/> + <see cref="LogicalIdxNamesStart"/>.
    /// </summary>
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
    public bool TryReadRealIdxSlot(ReadOnlySpan<byte> td, int realIdxDescStart, int slot, out RealIdxSlot info)
    {
        if (!TryGetRealIdxPhysOffset(realIdxDescStart, slot, td.Length, out int phys))
        {
            info = default;
            return false;
        }

        int firstDpOffset = phys + RealIdxFieldsOffset + FirstDpFieldOffset;
        int flagsOffset = phys + RealIdxFieldsOffset + FlagsFieldOffset;
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
    public bool TryReadLogicalEntry(ReadOnlySpan<byte> td, int logIdxStart, int slot, out LogicalIdxEntry entry)
    {
        if (!TryGetLogicalIdxFieldsOffset(logIdxStart, slot, td.Length, out int e))
        {
            entry = default;
            return false;
        }

        entry = new LogicalIdxEntry(
            e,
            BinaryPrimitives.ReadInt32LittleEndian(td.Slice(e + IndexNumFieldOffset, 4)),
            BinaryPrimitives.ReadInt32LittleEndian(td.Slice(e + IndexNum2FieldOffset, 4)),
            BinaryPrimitives.ReadInt32LittleEndian(td.Slice(e + RelIdxNumFieldOffset, 4)),
            BinaryPrimitives.ReadInt32LittleEndian(td.Slice(e + RelTblPageFieldOffset, 4)),
            td[e + CascadeUpsFieldOffset],
            td[e + CascadeDelsFieldOffset],
            (IndexKind)td[e + IndexTypeFieldOffset]);
        return true;
    }

    /// <summary>
    /// Anchors of the three index-related blocks within a TDEF buffer,
    /// plus the populated real-idx / logical-idx slot counts (TDEF header
    /// fields), returned by <see cref="GetIndexSection"/>. Bundles every
    /// piece of state a catalog walker needs so callers pass a single value
    /// instead of four parallel arguments.
    /// </summary>
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
    public readonly record struct RealIdxSlot(int PhysStart, int FirstDpOffset, byte Flags)
    {
        /// <summary>Gets a value indicating whether the unique flag bit (0x01) is set.</summary>
        public bool IsUnique => (Flags & 0x01) != 0;

        /// <summary>
        /// Lifts this raw slot into a <see cref="RealIdxEntry"/> by attaching
        /// the decoded <paramref name="keyColumns"/>. By default the entry's
        /// <see cref="RealIdxEntry.IsUnique"/> mirrors this slot's
        /// <see cref="IsUnique"/> (the real-idx <c>flags &amp; 0x01</c> bit);
        /// pass <paramref name="overrideUnique"/> to substitute (e.g.
        /// <c>false</c> when the caller will resolve uniqueness later, or
        /// <c>true</c> when an associated logical-idx PK promotes the slot).
        /// </summary>
        public RealIdxEntry ToEntry(IReadOnlyList<KeyColumn> keyColumns, bool? overrideUnique = null)
            => new(keyColumns, FirstDpOffset, overrideUnique ?? IsUnique);
    }

    /// <summary>
    /// Decoded view of a single logical-idx entry's format-invariant fields,
    /// returned by <see cref="TryReadLogicalEntry"/>. <see cref="IndexType"/>
    /// is exposed as <see cref="IndexKind"/> rather than the raw byte so
    /// consumers compare against enum values directly.
    /// </summary>
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
    public readonly record struct KeyColumnInfo(ColumnInfo Col, int SnapIdx, bool Ascending);

    /// <summary>
    /// One unique real-idx slot bundled with a best-effort logical-idx name
    /// (used in error messages) and the resolved <see cref="KeyColumnInfo"/>
    /// list. Produced by the writer's pre-write unique-index loader and
    /// consumed by the composite-key encoder + collision detector.
    /// </summary>
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
