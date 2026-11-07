namespace JetDatabaseWriter.Maintenance;

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;

/// <summary>
/// Regenerates canonical <see cref="IndexEntry"/> streams directly from verified active table rows
/// when a stored physical index B-tree or index ownership map is damaged.
/// </summary>
internal static class CompactIndexKeyRegenerator
{
    internal static async ValueTask<IAsyncEnumerable<IndexEntry>> RegenerateAsync(
        JetFormat format,
        long owner,
        TDefImage definition,
        int descriptorIndex,
        IReadOnlyList<(RowLocation Location, byte[] Payload)> activeRows,
        int maxKeyBytes,
        CompactDiscoveryAccounting? accounting,
        CompactDiscoverySession? memoSession,
        CompactDiscoveryBudget memoBudget,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(definition, nameof(definition));
        Guard.NotNull(activeRows, nameof(activeRows));
        Guard.NotNull(memoBudget, nameof(memoBudget));
        cancellationToken.ThrowIfCancellationRequested();

        if (!CompactIndexRowAudit.SupportsDescriptor(definition, descriptorIndex))
        {
            throw new JetCorruptDataException(
                JetErrorCode.CorruptIndex,
                $"Damaged physical index descriptor {descriptorIndex} on table owner {owner} uses an unsupported key column or collation and cannot be regenerated.");
        }

        TDefIndexDescriptor descriptor = definition.RealIndexes[descriptorIndex];
        bool isPrimaryKey = IsPrimaryKey(definition, descriptorIndex);
        bool isUnique = isPrimaryKey || (descriptor.Slot.Flags & Constants.TableDefinition.UniqueIndexFlag) != 0;
        bool isRequired = isPrimaryKey || (descriptor.Slot.Flags & Constants.TableDefinition.RequiredIndexFlag) != 0;
        bool ignoreNulls = (descriptor.Slot.Flags & Constants.TableDefinition.IgnoreNullsIndexFlag) != 0;

        var keyColumnNumbers = new HashSet<int>(descriptor.Columns.Count);
        foreach (KeyColumn keyCol in descriptor.Columns)
        {
            _ = keyColumnNumbers.Add(keyCol.ColNum);
        }

        var entries = new List<IndexEntry>(activeRows.Count);
        foreach ((RowLocation location, byte[] payload) in activeRows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (isRequired)
            {
                bool hasNullComponent = false;
                await CompactRawFieldReader.ScanAsync(
                    format,
                    definition,
                    location,
                    payload,
                    (field, _) =>
                    {
                        if (keyColumnNumbers.Contains(field.Column.ColNum)
                            && field.Column.Type != ColumnType.BooleanType
                            && field.Kind is CompactRawFieldKind.Null or CompactRawFieldKind.Absent)
                        {
                            hasNullComponent = true;
                        }

                        return default;
                    },
                    cancellationToken).ConfigureAwait(false);
                if (hasNullComponent)
                {
                    throw new JetCorruptDataException(
                        JetErrorCode.NotNullViolation,
                        $"Cannot regenerate required or primary-key index descriptor {descriptorIndex} on table owner {owner} because active row {location.PageNumber}:{location.RowIndex} contains a null key value.");
                }
            }

            (byte[] key, bool allNull) = await CompactIndexRowAudit.ReadKeyAsync(
                format,
                definition,
                descriptorIndex,
                location,
                payload,
                maxKeyBytes,
                accounting,
                memoSession,
                memoBudget,
                cancellationToken).ConfigureAwait(false);
            if (allNull && ignoreNulls)
            {
                continue;
            }

            entries.Add(new IndexEntry(key, location.PageNumber, checked((byte)location.RowIndex)));
        }

        entries.Sort(CompareEntries);
        if (isUnique)
        {
            for (int i = 1; i < entries.Count; i++)
            {
                if (entries[i - 1].Key.AsSpan().SequenceEqual(entries[i].Key.AsSpan()))
                {
                    throw new JetCorruptDataException(
                        JetErrorCode.UniqueViolation,
                        $"Cannot regenerate unique or primary-key index descriptor {descriptorIndex} on table owner {owner} because active rows {entries[i - 1].DataPage}:{entries[i - 1].DataRow} and {entries[i].DataPage}:{entries[i].DataRow} have duplicate keys.");
                }
            }
        }

        return StreamEntriesAsync(entries, CancellationToken.None);
    }

    private static int CompareEntries(IndexEntry left, IndexEntry right)
    {
        int key = left.Key.AsSpan().SequenceCompareTo(right.Key);
        if (key != 0)
        {
            return key;
        }

        int page = left.DataPage.CompareTo(right.DataPage);
        return page != 0 ? page : left.DataRow.CompareTo(right.DataRow);
    }

    private static bool IsPrimaryKey(TDefImage definition, int descriptorIndex)
    {
        foreach (TDefLogicalIndexDescriptor logical in definition.LogicalIndexes)
        {
            if (logical.Entry.IndexNum2 == descriptorIndex && logical.Entry.IndexType == IndexKind.PrimaryKey)
            {
                return true;
            }
        }

        return false;
    }

    private static async IAsyncEnumerable<IndexEntry> StreamEntriesAsync(
        List<IndexEntry> entries,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.CompletedTask.ConfigureAwait(false);
        for (int i = 0; i < entries.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return entries[i];
        }
    }
}
