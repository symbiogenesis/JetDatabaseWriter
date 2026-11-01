namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tables;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Changes logical index names while retaining every physical storage identity.</summary>
/// <param name="format">The native format.</param>
/// <param name="pager">The statement's transactional page store.</param>
/// <param name="tableDefs">The bounded table-definition reader.</param>
/// <param name="allocator">Allocates or releases definition continuation pages.</param>
/// <param name="catalog">Resolves local tables and invalidates derived schemas.</param>
/// <param name="artifacts">Checks the native security mutation policy.</param>
/// <param name="catalogWriter">Updates persisted index property targets.</param>
/// <param name="snapshots">Reads the original property block.</param>
internal sealed class IndexSchemaEditor(JetFormat format, Pager pager, TableDefReader tableDefs, PageAllocator allocator, TableCatalog catalog, CatalogArtifactWriter artifacts, CatalogWriter catalogWriter, TableSnapshotReader snapshots)
{
    /// <summary>Renames an ordinary or primary-key index without rebuilding its rows or trees.</summary>
    /// <param name="tableName">The local table.</param>
    /// <param name="oldName">The existing logical name.</param>
    /// <param name="newName">The validated replacement name.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="JetOperationException">The index is absent or belongs to a relationship.</exception>
    /// <exception cref="JetObjectExistsException">Another index has the requested name.</exception>
    /// <exception cref="JetCorruptDataException">The original definition cannot be preserved completely.</exception>
    /// <exception cref="JetLimitationException">The replacement exceeds the definition-byte budget.</exception>
    internal async ValueTask RenameIndexAsync(string tableName, string oldName, string newName, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(oldName, nameof(oldName));
        AccessObjectName.ThrowIfInvalid(newName, nameof(newName), "index");
        AccessObjectName.ThrowIfNotStorable(format, newName, nameof(newName), "index");
        pager.ThrowIfDisposedOrCancelled(cancellationToken);
        await artifacts.ThrowIfNativeSecurityUnmaintainableAsync(relationships: false, cancellationToken).ConfigureAwait(false);
        ResolvedTable table = await catalog.ResolveRequiredTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        LogicalTDefChain chain = await tableDefs.ReadTDefChainAsync(table.Entry.TDefPage, cancellationToken).ConfigureAwait(false);
        TDefImage image = TDefCodec.Parse(format, chain.Bytes) ?? throw Corrupt();
        if (image.Issues != TDefParseIssues.None || image.IndexSection is not IndexSectionAnchors anchors)
        {
            throw Corrupt();
        }

        int selected = -1;
        for (int index = 0; index < image.LogicalIndexes.Count; index++)
        {
            string name = image.LogicalIndexes[index].Name;
            if (string.Equals(name, oldName, StringComparison.OrdinalIgnoreCase))
            {
                if (selected >= 0)
                {
                    throw Corrupt();
                }

                selected = index;
            }
            else if (string.Equals(name, newName, StringComparison.OrdinalIgnoreCase))
            {
                throw new JetObjectExistsException(JetErrorCode.ObjectExists, $"Index '{newName}' already exists in table '{tableName}'.", new JetErrorInfo { TableName = tableName, IndexName = newName });
            }
        }

        if (selected < 0)
        {
            throw new JetOperationException(JetErrorCode.IndexNotFound, $"Index '{oldName}' does not exist in table '{tableName}'.", new JetErrorInfo { TableName = tableName, IndexName = oldName });
        }

        TDefLogicalIndexDescriptor target = image.LogicalIndexes[selected];
        if (target.Entry.IndexType is not (IndexKind.Normal or IndexKind.PrimaryKey))
        {
            throw new JetOperationException(JetErrorCode.InvalidOperationState, "Use RenameRelationshipAsync to rename a relationship index.", new JetErrorInfo { TableName = tableName, IndexName = oldName });
        }

        if (string.Equals(target.Name, newName, StringComparison.Ordinal))
        {
            return;
        }

        byte[]? properties = await this.RenameIndexPropertiesAsync(table.Entry.TDefPage, target.Name, newName, cancellationToken).ConfigureAwait(false);
        long declaredLength = (long)image.Header.LogicalLength + 8;
        if (declaredLength < anchors.LogIdxNamesStart || declaredLength > chain.Bytes.Length)
        {
            throw Corrupt();
        }

        int usedLength = checked((int)declaredLength);
        int position = anchors.LogIdxNamesStart;
        var names = new List<(string Name, byte[] Record, byte[] Entry)>(image.LogicalIndexes.Count);
        for (int index = 0; index < image.LogicalIndexes.Count; index++)
        {
            int start = position;
            if (format.ReadColumnName(chain.Bytes, ref position, out string name) < 0 || position > usedLength)
            {
                throw Corrupt();
            }

            byte[] record = index == selected ? format.EncodeTDefNameRecord(newName) : chain.Bytes.AsSpan(start, position - start).ToArray();
            byte[] entry = chain.Bytes.AsSpan(anchors.LogIdxStart + (index * format.Index.LogicalEntrySize), format.Index.LogicalEntrySize).ToArray();
            names.Add((index == selected ? newName : name, record, entry));
        }

        var collation = new JetTextCollation(format.DefaultTextSortOrder);

        // Move only the renamed entry. Equal native names retain their original
        // relative order, and index_num remains the identity used by FK partners.
        (string Name, byte[] Record, byte[] Entry) renamed = names[selected];
        names.RemoveAt(selected);
        int insertion = 0;
        while (insertion < names.Count && collation.Compare(names[insertion].Name, newName) <= 0)
        {
            insertion++;
        }

        names.Insert(insertion, renamed);
        int finalLength = usedLength - (position - anchors.LogIdxNamesStart);
        foreach ((_, byte[] record, _) in names)
        {
            finalLength = checked(finalLength + record.Length);
        }

        int capacity = LogicalTDefChain.GetLogicalCapacity(format.PageSize, finalLength);
        if (capacity > tableDefs.MaxSchemaTraversalBytes)
        {
            throw new JetLimitationException(JetErrorCode.SchemaTraversalBudgetExceeded, "The renamed index exceeds MaxSchemaTraversalBytes.", new JetErrorInfo { TableName = tableName, IndexName = newName });
        }

        if (capacity > tableDefs.MaxLogicalBytes)
        {
            throw new JetLimitationException(JetErrorCode.ValueTooLarge, "The renamed index exceeds the configured table-definition byte budget.", new JetErrorInfo { TableName = tableName, IndexName = newName });
        }

        byte[] bytes = chain.EnsureCapacity(finalLength);
        int namesEnd = finalLength - (usedLength - position);
        bytes.AsSpan(position, usedLength - position).CopyTo(bytes.AsSpan(namesEnd));
        int entryOffset = anchors.LogIdxStart;
        int nameOffset = anchors.LogIdxNamesStart;
        foreach ((_, byte[] record, byte[] entry) in names)
        {
            entry.CopyTo(bytes, entryOffset);
            record.CopyTo(bytes, nameOffset);
            entryOffset += entry.Length;
            nameOffset += record.Length;
        }

        Wi32(bytes, 8, finalLength - 8);
        await chain.WriteAsync(bytes, finalLength, allocator.AllocatePageAsync, pager.WritePageAsync, allocator.DeallocatePageAsync, format.WritesTDefFreeSpace, cancellationToken).ConfigureAwait(false);
        if (properties is not null)
        {
            await catalogWriter.UpdateTablePropertiesAsync(tableName, table.Entry.TDefPage, properties, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        catalog.Invalidate();

        JetCorruptDataException Corrupt()
            => new(JetErrorCode.CorruptTableDefinition, $"The index names of table '{tableName}' cannot be changed without losing metadata.", new JetErrorInfo { TableName = tableName, IndexName = oldName });
    }

    private async ValueTask<byte[]?> RenameIndexPropertiesAsync(long tdefPage, string oldName, string newName, CancellationToken cancellationToken)
    {
        ColumnPropertyBlock? properties = await snapshots.ReadLvPropBlockAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (properties is null)
        {
            return null;
        }

        var builder = ColumnPropertyBlockBuilder.FromBlock(properties);
        bool changed = false;
        foreach (ColumnPropertyTargetBuilder target in builder.Targets)
        {
            if (target.ChunkType != ColumnPropertyChunkType.PropertyBlockAlt2)
            {
                continue;
            }

            if (string.Equals(target.Name, oldName, StringComparison.OrdinalIgnoreCase))
            {
                target.Name = newName;
                changed = true;
            }
            else if (string.Equals(target.Name, newName, StringComparison.OrdinalIgnoreCase))
            {
                throw new JetOperationException(JetErrorCode.InvalidOperationState, $"Persisted index metadata already uses the name '{newName}'.", new JetErrorInfo { IndexName = newName, PageNumber = tdefPage });
            }
        }

        return changed ? builder.ToBytes(format) : null;
    }
}
