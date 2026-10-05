namespace JetDatabaseWriter.Relationships;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Schema;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

internal sealed class RelationshipSeekPlanner(JetFormat format, TableDefReader tableDefs, TableCatalog tableCatalog)
{
    private readonly record struct SeekIndexCore(
        long FirstDp,
        ColumnType[] ColTypes,
        byte[] NumericScales,
        TextSortOrder[] TextSortOrders,
        IReadOnlyList<bool> Ascending,
        bool LegacyNumeric);

    public async ValueTask<ParentSeekIndex?> ResolveParentSeekIndexAsync(
        FkRelationship rel,
        FkContext ctx,
        CancellationToken cancellationToken)
    {
        if (ctx.SeekIndexes.TryGetValue(rel.Name, out ParentSeekIndex? cached))
        {
            return cached;
        }

        ParentSeekIndex? resolved = null;
        try
        {
            SeekIndexCore? core = await this.TryResolveSeekIndexCoreAsync(
                rel.PrimaryTable,
                rel.PrimaryColumns,
                cancellationToken).ConfigureAwait(false);
            if (core == null)
            {
                return null;
            }

            CatalogEntry? foreignEntry = await tableCatalog.GetCatalogEntryAsync(rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            if (foreignEntry == null)
            {
                return null;
            }

            TableDef foreignDef = await tableDefs.ReadRequiredTableDefAsync(foreignEntry.TDefPage, rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            int[] foreignRowIndexes = new int[rel.ForeignColumns.Count];
            for (int index = 0; index < rel.ForeignColumns.Count; index++)
            {
                foreignRowIndexes[index] = foreignDef.FindColumnIndex(rel.ForeignColumns[index]);
                if (foreignRowIndexes[index] < 0)
                {
                    return null;
                }
            }

            var keyColumns = new ParentSeekKeyColumn[core.Value.ColTypes.Length];
            for (int index = 0; index < keyColumns.Length; index++)
            {
                keyColumns[index] = new ParentSeekKeyColumn(
                    core.Value.ColTypes[index],
                    core.Value.Ascending[index],
                    foreignRowIndexes[index],
                    core.Value.NumericScales[index],
                    core.Value.LegacyNumeric,
                    core.Value.TextSortOrders[index]);
            }

            resolved = new ParentSeekIndex(core.Value.FirstDp, keyColumns);
            return resolved;
        }
        finally
        {
            ctx.SeekIndexes[rel.Name] = resolved;
        }
    }

    public async ValueTask<ChildSeekIndex?> ResolveChildSeekIndexAsync(
        FkRelationship rel,
        FkContext ctx,
        CancellationToken cancellationToken)
    {
        if (ctx.ChildSeekIndexes.TryGetValue(rel.Name, out ChildSeekIndex? cached))
        {
            return cached;
        }

        ChildSeekIndex? resolved = null;
        try
        {
            SeekIndexCore? core = await this.TryResolveSeekIndexCoreAsync(
                rel.ForeignTable,
                rel.ForeignColumns,
                cancellationToken).ConfigureAwait(false);
            if (core == null)
            {
                return null;
            }

            var keyColumns = new ChildSeekKeyColumn[core.Value.ColTypes.Length];
            for (int index = 0; index < keyColumns.Length; index++)
            {
                keyColumns[index] = new ChildSeekKeyColumn(
                    core.Value.ColTypes[index],
                    core.Value.Ascending[index],
                    core.Value.NumericScales[index],
                    core.Value.LegacyNumeric,
                    core.Value.TextSortOrders[index]);
            }

            resolved = new ChildSeekIndex(core.Value.FirstDp, keyColumns);
            return resolved;
        }
        finally
        {
            ctx.ChildSeekIndexes[rel.Name] = resolved;
        }
    }

    private async ValueTask<SeekIndexCore?> TryResolveSeekIndexCoreAsync(
        string tableName,
        IReadOnlyList<string> columnNames,
        CancellationToken cancellationToken)
    {
        if (!format.SupportsIndexSeeks)
        {
            return null;
        }

        CatalogEntry? entry = await tableCatalog.GetCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (entry == null)
        {
            return null;
        }

        TableDef definition = await tableDefs.ReadRequiredTableDefAsync(entry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);

        int[] columnNumbers = new int[columnNames.Count];
        var columnTypes = new ColumnType[columnNames.Count];
        byte[] numericScales = new byte[columnNames.Count];
        var textSortOrders = new TextSortOrder[columnNames.Count];
        for (int index = 0; index < columnNames.Count; index++)
        {
            int columnIndex = definition.FindColumnIndex(columnNames[index]);
            if (columnIndex < 0)
            {
                return null;
            }

            columnNumbers[index] = definition.Columns[columnIndex].ColNum;
            columnTypes[index] = definition.Columns[columnIndex].Type;
            numericScales[index] = definition.Columns[columnIndex].NumericScale;
            textSortOrders[index] = definition.Columns[columnIndex].TextSortOrder;
            if (columnTypes[index] is TextType or MemoType && !textSortOrders[index].IsSupported)
            {
                return null;
            }
        }

        (long FirstDp, IReadOnlyList<bool> AscendingFlags)? hit = await this.TryFindCoveringRealIdxAsync(
            entry.TDefPage,
            columnNumbers,
            cancellationToken).ConfigureAwait(false);
        if (hit == null)
        {
            return null;
        }

        for (int index = 0; index < columnTypes.Length; index++)
        {
            if (!IndexKeyEncoder.IsColumnTypeSeekable(columnTypes[index]))
            {
                return null;
            }
        }

        return new SeekIndexCore(
            hit.Value.FirstDp,
            columnTypes,
            numericScales,
            textSortOrders,
            hit.Value.AscendingFlags,
            format.LegacyNumericIndexKeys);
    }

    private async ValueTask<(long FirstDp, IReadOnlyList<bool> AscendingFlags)?> TryFindCoveringRealIdxAsync(
        long tdefPage,
        int[] targetColumnNumbers,
        CancellationToken cancellationToken)
    {
        // Read the whole TDEF chain: a wide table's real-idx descriptors sit
        // on a continuation page.
        byte[]? tableDefinition = await tableDefs.ReadTDefBytesAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (tableDefinition is null)
        {
            return null;
        }

        var header = TDefCodec.ReadCounts(format, tableDefinition);
        int numColumns = header.ColumnCount;
        int numRealIndexes = header.RealIndexCount;
        if (numColumns < 0 || numColumns > Constants.TableDefinition.MaxColumns
            || numRealIndexes <= 0 || numRealIndexes > Constants.TableDefinition.MaxIndexes)
        {
            return null;
        }

        int realIndexDescriptorStart = IndexCatalogReader.LocateRealIdxDescStart(format, tableDefinition, numColumns, numRealIndexes);
        if (realIndexDescriptorStart < 0)
        {
            return null;
        }

        IndexLayout layout = format.Index;
        for (int realIndex = 0; realIndex < numRealIndexes; realIndex++)
        {
            int physicalDescriptorOffset = layout.RealIdxPhysOffset(realIndexDescriptorStart, realIndex);
            if (!IndexHelpers.RealIdxColMapMatches(layout, tableDefinition, physicalDescriptorOffset, targetColumnNumbers))
            {
                continue;
            }

            bool[] ascending = new bool[targetColumnNumbers.Length];
            for (int slot = 0; slot < targetColumnNumbers.Length; slot++)
            {
                ascending[slot] = (tableDefinition[layout.ColMapSlotOffset(physicalDescriptorOffset, slot) + 2] & 0x01) != 0;
            }

            int firstDataPage = Ri32(tableDefinition, layout.FirstDpAbsoluteOffset(physicalDescriptorOffset));
            if (firstDataPage <= 0)
            {
                continue;
            }

            return (firstDataPage, ascending);
        }

        return null;
    }
}
