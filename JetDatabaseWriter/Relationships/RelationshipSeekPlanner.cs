namespace JetDatabaseWriter.Relationships;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

internal sealed class RelationshipSeekPlanner(AccessWriter writer)
{
    private readonly record struct SeekIndexCore(
        long FirstDp,
        byte[] ColTypes,
        byte[] NumericScales,
        IReadOnlyList<bool> Ascending,
        bool LegacyNumeric);

    public async ValueTask<ParentSeekIndex?> ResolveParentSeekIndexAsync(
        FkRelationship rel,
        FkContext ctx,
        CancellationToken cancellationToken)
    {
        if (ctx.SeekIndexes.TryGetValue(rel.Name, out var cached))
        {
            return cached;
        }

        ParentSeekIndex? resolved = null;
        try
        {
            var core = await TryResolveSeekIndexCoreAsync(
                rel.PrimaryTable,
                rel.PrimaryColumns,
                cancellationToken).ConfigureAwait(false);
            if (core == null)
            {
                return null;
            }

            var foreignEntry = await writer.GetCatalogEntryAsync(rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            if (foreignEntry == null)
            {
                return null;
            }

            var foreignDef = await writer.ReadRequiredTableDefAsync(foreignEntry.TDefPage, rel.ForeignTable, cancellationToken).ConfigureAwait(false);
            var foreignRowIndexes = new int[rel.ForeignColumns.Count];
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
                    core.Value.LegacyNumeric);
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
        if (ctx.ChildSeekIndexes.TryGetValue(rel.Name, out var cached))
        {
            return cached;
        }

        ChildSeekIndex? resolved = null;
        try
        {
            var core = await TryResolveSeekIndexCoreAsync(
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
                    core.Value.LegacyNumeric);
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
        if (writer._format == DatabaseFormat.Jet3Mdb)
        {
            return null;
        }

        var entry = await writer.GetCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (entry == null)
        {
            return null;
        }

        var definition = await writer.ReadRequiredTableDefAsync(entry.TDefPage, tableName, cancellationToken).ConfigureAwait(false);

        var columnNumbers = new int[columnNames.Count];
        var columnTypes = new byte[columnNames.Count];
        var numericScales = new byte[columnNames.Count];
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
        }

        var hit = await TryFindCoveringRealIdxAsync(
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
            hit.Value.AscendingFlags,
            writer._format == DatabaseFormat.Jet4Mdb);
    }

    private async ValueTask<(long FirstDp, IReadOnlyList<bool> AscendingFlags)?> TryFindCoveringRealIdxAsync(
        long tdefPage,
        int[] targetColumnNumbers,
        CancellationToken cancellationToken)
    {
        byte[] tableDefinition = await RelationshipPageReader.ReadOwnedAsync(writer, tdefPage, cancellationToken).ConfigureAwait(false);

        if (tableDefinition[0] != Constants.PageTypes.TableDefinition || Ru32(tableDefinition, 4) != 0)
        {
            return null;
        }

        int numColumns = Ru16(tableDefinition, writer._tdef.NumCols);
        int numRealIndexes = Ri32(tableDefinition, writer._tdef.NumRealIdx);
        if (numColumns < 0 || numColumns > Constants.TableDefinition.MaxColumns
            || numRealIndexes <= 0 || numRealIndexes > Constants.TableDefinition.MaxIndexes)
        {
            return null;
        }

        int realIndexDescriptorStart = LocateRealIdxDescStart(tableDefinition, numColumns, numRealIndexes);
        if (realIndexDescriptorStart < 0)
        {
            return null;
        }

        for (int realIndex = 0; realIndex < numRealIndexes; realIndex++)
        {
            int physicalDescriptorOffset = realIndexDescriptorStart + (realIndex * Constants.TableDefinition.Jet4.RealIdx.PhysSize);
            if (!IndexHelpers.RealIdxColMapMatches(tableDefinition, physicalDescriptorOffset, targetColumnNumbers))
            {
                continue;
            }

            var ascending = new bool[targetColumnNumbers.Length];
            for (int slot = 0; slot < targetColumnNumbers.Length; slot++)
            {
                ascending[slot] = (tableDefinition[physicalDescriptorOffset + 4 + (slot * 3) + 2] & 0x01) != 0;
            }

            int firstDataPage = Ri32(tableDefinition, physicalDescriptorOffset + 38);
            if (firstDataPage <= 0)
            {
                continue;
            }

            return (firstDataPage, ascending);
        }

        return null;
    }

    private int LocateRealIdxDescStart(byte[] tableDefinition, int numColumns, int numRealIndexes)
    {
        int columnStart = writer._tdef.BlockEnd + (numRealIndexes * writer._tdef.RealIdxEntrySz);
        int position = columnStart + (numColumns * writer._colDesc.Size);
        for (int column = 0; column < numColumns; column++)
        {
            if (writer.ReadColumnName(tableDefinition, ref position, out _) < 0)
            {
                return -1;
            }
        }

        return position;
    }
}
