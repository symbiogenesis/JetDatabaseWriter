namespace JetDatabaseWriter.ComplexColumns;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tables;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Retargets the native history payload when its append-only Memo is renamed or dropped.</summary>
/// <param name="format">The database format.</param>
/// <param name="pager">The transaction-aware page store.</param>
/// <param name="tableDefs">The logical definition reader.</param>
/// <param name="allocator">The continuation-page allocator.</param>
/// <param name="catalog">The parent catalog and schema cache.</param>
/// <param name="complexColumns">The validated complex descriptors.</param>
/// <param name="snapshots">The hidden-table and property reader.</param>
/// <param name="catalogWriter">The property writer.</param>
/// <param name="catalogRows">The physical catalog identity reader.</param>
internal sealed class AppendOnlyHistorySchemaEditor(
    JetFormat format,
    Pager pager,
    TableDefReader tableDefs,
    PageAllocator allocator,
    TableCatalog catalog,
    ComplexColumnReader complexColumns,
    TableSnapshotReader snapshots,
    CatalogWriter catalogWriter,
    CatalogRowReader catalogRows)
{
    private static JetCorruptDataException InvalidHistory(string tableName, string columnName)
        => new(JetErrorCode.CorruptComplexColumn, "The append-only Memo has no unambiguous native history payload.", new JetErrorInfo { TableName = tableName, ColumnName = columnName });

    /// <summary>Prepares the flat and template edits without changing any page.</summary>
    /// <param name="tableName">The parent table.</param>
    /// <param name="oldName">The source Memo name.</param>
    /// <param name="newName">The new name, or null for a drop.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The prepared hidden-table changes.</returns>
    /// <exception cref="JetCorruptDataException">The history payload or its private ownership cannot be verified.</exception>
    internal async ValueTask<HistorySchemaPlan> PrepareAsync(string tableName, string oldName, string? newName, CancellationToken cancellationToken)
    {
        var changes = new List<HistorySchemaChange>();
        string? droppedReference = null;
        ResolvedTable? droppedTemplate = null;
        if (!format.SupportsComplexColumns)
        {
            return new(changes, null, null);
        }

        ResolvedTable parent = await catalog.ResolveRequiredTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        ColumnInfo? source = parent.Definition.FindColumn(oldName);
        ColumnPropertyBlock? properties = await snapshots.ReadLvPropBlockAsync(parent.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        if (source?.Type != ColumnType.MemoType || properties?.FindPropertyTarget(source.Name, "AppendOnly")?.GetBooleanValue("AppendOnly") != true)
        {
            return new(changes, null, null);
        }

        bool found = false;
        foreach (ComplexColumnInfo descriptor in await complexColumns.GetComplexColumnsAsync(tableName, cancellationToken).ConfigureAwait(false))
        {
            if (descriptor.Kind != ComplexColumnKind.VersionHistory)
            {
                continue;
            }

            ResolvedTable? flat = await snapshots.ResolveTableAsync(descriptor.FlatTableName, cancellationToken).ConfigureAwait(false);
            if (flat?.Definition.FindColumn(source.Name)?.Type != ColumnType.MemoType)
            {
                continue;
            }

            if (found)
            {
                throw InvalidHistory(tableName, source.Name);
            }

            found = true;
            bool dropHistory = newName is null && flat.Definition.Columns.Count(column => column.Type == ColumnType.MemoType) == 1;
            if (dropHistory)
            {
                droppedReference = descriptor.ColumnName;
            }
            else
            {
                changes.Add(await this.PrepareTableAsync(flat, source.Name, newName, cancellationToken).ConfigureAwait(false));
            }

            if (descriptor.ComplexTypeObjectId != 0)
            {
                ResolvedTable? template = await snapshots.ResolveTableAsync(descriptor.ComplexTypeName, cancellationToken).ConfigureAwait(false);
                if (template?.Definition.FindColumn(source.Name)?.Type != ColumnType.MemoType
                    || template.Entry.TDefPage != CatalogValueReader.TdefPageFromId(descriptor.ComplexTypeObjectId))
                {
                    throw InvalidHistory(tableName, source.Name);
                }

                await this.ValidatePrivateTemplateAsync(template, cancellationToken).ConfigureAwait(false);
                ResolvedTable mappings = await snapshots.ResolveTableAsync(Constants.SystemTableNames.ComplexColumns, cancellationToken).ConfigureAwait(false)
                    ?? throw InvalidHistory(tableName, source.Name);
                int typeIndex = mappings.Definition.FindColumnIndex("ComplexTypeObjectID");
                if (typeIndex < 0)
                {
                    throw InvalidHistory(tableName, source.Name);
                }

                int references = 0;
                foreach (LocatedRow row in await snapshots.ReadRowsAsync(mappings.Entry.TDefPage, cancellationToken).ConfigureAwait(false))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (row.Values[typeIndex] is not int type)
                    {
                        throw InvalidHistory(tableName, source.Name);
                    }

                    if (type == descriptor.ComplexTypeObjectId)
                    {
                        references++;
                    }
                }

                if (dropHistory)
                {
                    droppedTemplate = references == 1 ? template : null;
                }
                else
                {
                    if (references != 1)
                    {
                        throw InvalidHistory(tableName, source.Name);
                    }

                    changes.Add(await this.PrepareTableAsync(template, source.Name, newName, cancellationToken).ConfigureAwait(false));
                }
            }
        }

        return !found ? throw InvalidHistory(tableName, source.Name) : new(changes, droppedReference, droppedTemplate);
    }

    /// <summary>Applies all prepared edits in the parent schema statement.</summary>
    /// <param name="plan">The validated changes.</param>
    /// <param name="dropTemplate">Reclaims an unreferenced private template.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    internal async ValueTask ApplyAsync(HistorySchemaPlan plan, Func<ResolvedTable, CancellationToken, ValueTask> dropTemplate, CancellationToken cancellationToken)
    {
        foreach (HistorySchemaChange change in plan.Changes)
        {
            await change.Chain.WriteAsync(change.Bytes, change.UsedLength, allocator.AllocatePageAsync, pager.WritePageAsync, allocator.DeallocatePageAsync, format.WritesTDefFreeSpace, cancellationToken).ConfigureAwait(false);
            if (change.HasPropertyChanges)
            {
                await catalogWriter.UpdateTablePropertiesAsync(change.TableName, change.Root, change.Properties, cancellationToken: cancellationToken).ConfigureAwait(false);
            }
        }

        if (plan.DroppedTemplate is { } template)
        {
            await dropTemplate(template, cancellationToken).ConfigureAwait(false);
        }

        if (plan.Changes.Count != 0 || plan.DroppedReferenceColumn is not null)
        {
            catalog.Invalidate();
        }
    }

    private async ValueTask ValidatePrivateTemplateAsync(ResolvedTable template, CancellationToken cancellationToken)
    {
        TableDef objects = await tableDefs.ReadRequiredTableDefAsync(2, Constants.SystemTableNames.Objects, cancellationToken).ConfigureAwait(false);
        List<CatalogRow> identities = await catalogRows.GetCatalogRowsAsync(objects, cancellationToken).ConfigureAwait(false);
        List<CatalogRow> matches = identities.FindAll(row => row.TDefPage == template.Entry.TDefPage);
        TDefImage? image = await tableDefs.ReadImageAsync(template.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        if (identities.Exists(row => !row.IsDecoded) || matches.Count != 1 || !matches[0].IsDecoded
            || matches[0].ObjectType != Constants.SystemObjects.UserTableType
            || (matches[0].Flags & Constants.SystemObjects.ComplexTypeTemplateFlags) != Constants.SystemObjects.ComplexTypeTemplateFlags
            || !string.Equals(matches[0].Name, template.Entry.Name, StringComparison.Ordinal)
            || !template.Entry.Name.StartsWith(Constants.ComplexTypeNames.VersionHistoryPrefix, StringComparison.OrdinalIgnoreCase)
            || image is null || image.Header.RealIndexCount != 0 || image.Header.LogicalIndexCount != 0 || image.Header.Counters.RowCount != 0
            || (await snapshots.ReadRowsAsync(template.Entry.TDefPage, cancellationToken).ConfigureAwait(false)).Count != 0)
        {
            throw InvalidHistory(template.Entry.Name, string.Empty);
        }
    }

    private async ValueTask<HistorySchemaChange> PrepareTableAsync(ResolvedTable table, string oldName, string? newName, CancellationToken cancellationToken)
    {
        LogicalTDefChain chain = await tableDefs.ReadTDefChainAsync(table.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        TDefImage image = TDefCodec.Parse(format, chain.Bytes) ?? throw InvalidHistory(table.Entry.Name, oldName);
        int usedLength = checked(image.Header.LogicalLength + 8);
        int descriptorStart = checked(format.TDef.BlockEnd + (image.Header.RealIndexCount * format.TDef.RealIdxEntrySz));
        int namePosition = checked(descriptorStart + (image.Header.ColumnCount * format.ColumnDescriptor.Size));
        if (usedLength > chain.Bytes.Length || namePosition > usedLength || (newName is not null && table.Definition.FindColumn(newName) is { } collision && !string.Equals(collision.Name, oldName, StringComparison.OrdinalIgnoreCase)))
        {
            throw InvalidHistory(table.Entry.Name, oldName);
        }

        int nameStart = -1;
        int nameEnd = -1;
        int descriptorOffset = -1;
        for (int ordinal = 0; ordinal < image.Header.ColumnCount; ordinal++)
        {
            int start = namePosition;
            if (format.ReadColumnName(chain.Bytes, ref namePosition, out string name) < 0 || namePosition > usedLength)
            {
                throw InvalidHistory(table.Entry.Name, oldName);
            }

            if (string.Equals(name, oldName, StringComparison.OrdinalIgnoreCase))
            {
                nameStart = start;
                nameEnd = namePosition;
                descriptorOffset = descriptorStart + (ordinal * format.ColumnDescriptor.Size);
            }
        }

        if (nameStart < 0)
        {
            throw InvalidHistory(table.Entry.Name, oldName);
        }

        if (newName is null && image.RealIndexes.Any(index => index.Columns.Any(column => column.ColNum == table.Definition.FindColumn(oldName)!.ColNum)))
        {
            throw InvalidHistory(table.Entry.Name, oldName);
        }

        int removedMapOffset = -1;
        if (newName is null)
        {
            if (!LongValueUsageMaps.TryRead(format, chain.Bytes, out List<LongValueUsageMap> maps, out _))
            {
                throw InvalidHistory(table.Entry.Name, oldName);
            }

            int columnNumber = table.Definition.FindColumn(oldName)!.ColNum;
            foreach (LongValueUsageMap map in maps)
            {
                if (map.ColumnNumber == columnNumber)
                {
                    removedMapOffset = map.OwnedOffset - 2;
                    break;
                }
            }
        }

        byte[] replacementName = newName is null ? [] : format.EncodeTDefNameRecord(newName);
        int removedDescriptor = newName is null ? format.ColumnDescriptor.Size : 0;
        int nextLength = checked(usedLength + replacementName.Length - (nameEnd - nameStart) - removedDescriptor);
        byte[] bytes = new byte[LogicalTDefChain.GetLogicalCapacity(format.PageSize, nextLength)];
        int prefixEnd = newName is null ? descriptorOffset : nameStart;
        chain.Bytes.AsSpan(0, prefixEnd).CopyTo(bytes);
        if (newName is null)
        {
            chain.Bytes.AsSpan(descriptorOffset + removedDescriptor, nameStart - descriptorOffset - removedDescriptor).CopyTo(bytes.AsSpan(descriptorOffset));
            Wu16(bytes, format.TDef.NumCols, image.Header.ColumnCount - 1);
        }

        replacementName.CopyTo(bytes.AsSpan(nameStart - removedDescriptor));
        chain.Bytes.AsSpan(nameEnd, usedLength - nameEnd).CopyTo(bytes.AsSpan(nameStart - removedDescriptor + replacementName.Length));
        if (removedMapOffset >= 0)
        {
            int offset = checked(removedMapOffset + replacementName.Length - (nameEnd - nameStart) - removedDescriptor);
            bytes.AsSpan(offset + 10, nextLength - offset - 10).CopyTo(bytes.AsSpan(offset));
            bytes.AsSpan(nextLength - 10, 10).Clear();
            nextLength -= 10;
        }

        ColumnPropertyBlock? properties = await snapshots.ReadLvPropBlockAsync(table.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        bool propertyChanges = properties?.Targets.Any(target => string.Equals(target.Name, oldName, StringComparison.OrdinalIgnoreCase)) == true;
        byte[]? projectedProperties = null;
        if (propertyChanges)
        {
            var builder = ColumnPropertyBlockBuilder.FromBlock(properties!);
            if (newName is null)
            {
                builder.Targets.RemoveAll(target => string.Equals(target.Name, oldName, StringComparison.OrdinalIgnoreCase));
            }
            else
            {
                foreach (ColumnPropertyTargetBuilder target in builder.Targets.Where(target => string.Equals(target.Name, oldName, StringComparison.OrdinalIgnoreCase)))
                {
                    target.Name = newName;
                }
            }

            projectedProperties = builder.ToBytes(format);
        }

        return new HistorySchemaChange(table.Entry.Name, table.Entry.TDefPage, chain, bytes, nextLength, propertyChanges, projectedProperties);
    }

    /// <summary>A private native definition edit, applied with its parent schema change.</summary>
    /// <param name="TableName">The hidden table name.</param>
    /// <param name="Root">Its retained root.</param>
    /// <param name="Chain">Its retained definition chain.</param>
    /// <param name="Bytes">The projected bytes.</param>
    /// <param name="UsedLength">The projected logical length.</param>
    /// <param name="HasPropertyChanges">Whether matching property targets changed.</param>
    /// <param name="Properties">The projected property blob.</param>
    internal sealed record HistorySchemaChange(string TableName, long Root, LogicalTDefChain Chain, byte[] Bytes, int UsedLength, bool HasPropertyChanges, byte[]? Properties);

    /// <summary>The parent projection and its dependent private edits.</summary>
    /// <param name="Changes">The retained payload definitions.</param>
    /// <param name="DroppedReferenceColumn">The hidden parent field removed with its last Memo.</param>
    /// <param name="DroppedTemplate">An unreferenced private template to remove.</param>
    internal sealed record HistorySchemaPlan(List<HistorySchemaChange> Changes, string? DroppedReferenceColumn, ResolvedTable? DroppedTemplate);
}
