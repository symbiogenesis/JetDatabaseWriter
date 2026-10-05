namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tables;
using static JetDatabaseWriter.Enums.ColumnType;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Pre-write unique-index enforcement: detects duplicate keys before any
/// disk page is mutated. Owned by <see cref="AccessWriter"/>.
/// </summary>
/// <param name="format">The database's immutable format profile.</param>
/// <param name="pageSource">The database's page source.</param>
/// <param name="tableDefs">The table-definition reader.</param>
/// <param name="snapshots">Reads decoded table rows when index pages cannot answer a uniqueness probe.</param>
internal sealed class UniqueIndexChecker(JetFormat format, IPageSource pageSource, TableDefReader tableDefs, TableSnapshotReader snapshots)
{
    /// <summary>
    /// Loads all unique / primary-key index descriptors for the table whose
    /// TDEF chain starts at <paramref name="tdefPage"/>. The whole chain is
    /// read, because a wide table's index section sits on a continuation page.
    /// Returns an empty list when the TDEF declares no indexes, and throws
    /// rather than skip a unique index it cannot enforce.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="tableName">The table name, for error messages.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidDataException">The page at <paramref name="tdefPage"/> is not a table definition.</exception>
    /// <exception cref="JetLimitationException">The index section cannot be parsed, or a unique index names a column the table does not have.</exception>
    internal async ValueTask<List<UniqueIndexDescriptor>> LoadUniqueIndexDescriptorsAsync(
        long tdefPage, TableDef tableDef, string tableName, CancellationToken cancellationToken)
    {
        var result = new List<UniqueIndexDescriptor>();

        byte[] tdefBuffer = await tableDefs.ReadTDefBytesAsync(tdefPage, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidDataException($"The table definition of '{tableName}' at page {tdefPage} could not be read.");

        int numCols = Ru16(tdefBuffer, format.TDef.NumCols);
        int numIdx = Ri32(tdefBuffer, format.TDef.NumIdx);
        int numRealIdx = Ri32(tdefBuffer, format.TDef.NumRealIdx);
        if (numIdx <= 0 || numRealIdx <= 0)
        {
            return result;
        }

        if (numIdx > Constants.TableDefinition.MaxIndexes || numRealIdx > Constants.TableDefinition.MaxIndexes)
        {
            throw CreateUnenforceableException(tableName, $"its table definition declares {numIdx} logical and {numRealIdx} real indexes");
        }

        int realIdxDescStart = IndexCatalogReader.LocateRealIdxDescStart(format, tdefBuffer, numCols, numRealIdx);
        if (realIdxDescStart < 0)
        {
            throw CreateUnenforceableException(tableName, "the column-name section of its table definition could not be walked");
        }

        IndexSectionAnchors anchors = format.Index.GetIndexSection(realIdxDescStart, numRealIdx, numIdx);
        List<string> logIdxNames = IndexCatalogReader.ReadLogicalIdxNames(format, tdefBuffer, anchors.LogIdxNamesStart, numIdx);

        IndexCatalogReader.ResolvedIndexCatalog catalog = IndexCatalogReader.ReadResolved(
            tdefBuffer, format.Index, anchors, tableDef.Columns, logIdxNames);

        foreach ((int realIdxNum, RealIdxEntry slot) in catalog.RealIdxByNum)
        {
            if (!catalog.Catalog.IsUniqueOrPk(realIdxNum))
            {
                continue;
            }

            if (!catalog.TryGetKeyColumnInfos(realIdxNum, out List<KeyColumnInfo>? keyColInfos))
            {
                throw CreateUnenforceableException(
                    tableName,
                    $"unique index '{catalog.Catalog.GetNameOrFallback(realIdxNum)}' names a column the table does not have");
            }

            long rootPage = (uint)Ri32(tdefBuffer, slot.FirstDpOffset);
            result.Add(new UniqueIndexDescriptor(realIdxNum, catalog.Catalog.GetNameOrFallback(realIdxNum), keyColInfos, rootPage));
        }

        return result;
    }

    private static JetLimitationException CreateUnenforceableException(string tableName, string reason)
        => new($"Unique indexes on table '{tableName}' cannot be enforced: {reason}. The table is unchanged.");

    /// <summary>
    /// Encodes the composite index key for one row using a previously
    /// computed canonical numeric scale per key column.
    /// </summary>
    /// <param name="descriptor">The descriptor.</param>
    /// <param name="row">The row values or row bytes.</param>
    /// <param name="numericTargetScales">The numeric target scales.</param>
    internal byte[] EncodeCompositeKeyForUniqueCheck(
        UniqueIndexDescriptor descriptor,
        object[] row,
        int[] numericTargetScales)
    {
        bool legacyNumeric = format.LegacyNumericIndexKeys;
        int keyCount = descriptor.KeyColumns.Count;

        // Single-column fast path: avoid the per-column array + copy.
        if (keyCount == 1)
        {
            (ColumnInfo? col, int snapIdx, bool ascending) = descriptor.KeyColumns[0];
            object cell = snapIdx < row.Length ? row[snapIdx] : DBNull.Value;
            object? value = cell is null or DBNull ? null : cell;
            return col.Type == NumericType
                ? IndexKeyEncoder.EncodeNumericEntryAtDeclaredScale(value, ascending, (byte)numericTargetScales[0], legacyNumeric)
                : IndexKeyEncoder.EncodeColumnEntry(format, col, value, ascending);
        }

        // Multi-column: encode into per-column spans then concatenate.
        Span<int> lengths = stackalloc int[keyCount];
        byte[][] perColumn = new byte[keyCount][];
        int totalLen = 0;
        for (int k = 0; k < keyCount; k++)
        {
            (ColumnInfo? col, int snapIdx, bool ascending) = descriptor.KeyColumns[k];
            object cell = snapIdx < row.Length ? row[snapIdx] : DBNull.Value;
            object? value = cell is null or DBNull ? null : cell;
            perColumn[k] = col.Type == NumericType
                ? IndexKeyEncoder.EncodeNumericEntryAtDeclaredScale(value, ascending, (byte)numericTargetScales[k], legacyNumeric)
                : IndexKeyEncoder.EncodeColumnEntry(format, col, value, ascending);
            lengths[k] = perColumn[k].Length;
            totalLen += perColumn[k].Length;
        }

        byte[] composite = new byte[totalLen];
        int offset = 0;
        for (int k = 0; k < keyCount; k++)
        {
            perColumn[k].AsSpan().CopyTo(composite.AsSpan(offset));
            offset += lengths[k];
        }

        return composite;
    }

    /// <summary>
    /// Pre-write unique-index validation for an insert batch.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="pendingRows">The pending rows.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask CheckUniqueIndexesPreInsertAsync(
        long tdefPage,
        TableDef tableDef,
        string tableName,
        List<object[]> pendingRows,
        CancellationToken cancellationToken)
    {
        if (pendingRows.Count == 0)
        {
            return;
        }

        List<UniqueIndexDescriptor> descriptors = await this.LoadUniqueIndexDescriptorsAsync(tdefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
        if (descriptors.Count == 0)
        {
            return;
        }

        // Cursor fast path: encode pending keys exactly as index maintenance
        // writes them, then probe the existing B-tree through IndexCursor.
        // Keep Numeric and Memo indexes on the full-table snapshot path:
        // Numeric needs descriptor-scale validation, and Memo/Hyperlink
        // values may require LVAL traversal outside the fast path.
        if (RequiresSnapshotForPreInsert(descriptors) || !CanUseCursorFastPath(descriptors))
        {
            List<LocatedRow> existingRows = await snapshots.ReadRowsAsync(tdefPage, cancellationToken).ConfigureAwait(false);
            this.CheckUniqueIndexesCore(tableName, descriptors, existingRows, pendingRows, replaceAtRowIndex: null);
            return;
        }

        await this.CheckUniqueIndexesFastPathAsync(tableName, descriptors, pendingRows, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Fast-path uniqueness check: probes existing encoded keys through the
    /// index cursor and validates pending-row collisions in memory.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="descriptors">The descriptors.</param>
    /// <param name="pendingRows">The pending rows.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidOperationException">Thrown when a pending insert would duplicate a unique key.</exception>
    private async ValueTask CheckUniqueIndexesFastPathAsync(
        string tableName,
        List<UniqueIndexDescriptor> descriptors,
        List<object[]> pendingRows,
        CancellationToken cancellationToken)
    {
        var cursor = new IndexCursor(
            format.IndexPage,
            this.ReadIndexPageOwnedAsync,
            format.PageSize);

        int[][] numericScales = new int[descriptors.Count][];
        var seenSets = new HashSet<byte[]>[descriptors.Count];
        for (int d = 0; d < descriptors.Count; d++)
        {
            numericScales[d] = BuildNumericScales(descriptors[d]);
            seenSets[d] = new HashSet<byte[]>(ByteArrayEqualityComparer.Instance);
        }

        for (int p = 0; p < pendingRows.Count; p++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            for (int d = 0; d < descriptors.Count; d++)
            {
                UniqueIndexDescriptor descriptor = descriptors[d];
                byte[] key = this.EncodeCompositeKeyForUniqueCheck(descriptor, pendingRows[p], numericScales[d]);

                if (!seenSets[d].Add(key)
                    || await cursor.ContainsKeyAsync(descriptor.RootPage, key, cancellationToken).ConfigureAwait(false))
                {
                    throw new InvalidOperationException(
                        $"Unique index violation on table '{tableName}': duplicate key for index '{descriptor.Name}'. " +
                        "The conflict was detected before any row was written; the table is unchanged.");
                }
            }
        }
    }

    private async ValueTask<byte[]> ReadIndexPageOwnedAsync(long pageNumber, CancellationToken cancellationToken)
    {
        byte[] page = await pageSource.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            return (byte[])page.Clone();
        }
        finally
        {
            PageBuffers.Return(page);
        }
    }

    /// <summary>
    /// Pre-write unique-index validation for an update batch.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="existingRows">The table's live rows, as read for the update.</param>
    /// <param name="updates">The updates; each <c>Index</c> is a position in <paramref name="existingRows"/>.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask CheckUniqueIndexesPreUpdateAsync(
        long tdefPage,
        TableDef tableDef,
        string tableName,
        IReadOnlyList<LocatedRow> existingRows,
        List<(int Index, object[] OldRow, object[] NewRow)> updates,
        CancellationToken cancellationToken)
    {
        if (updates.Count == 0)
        {
            return;
        }

        List<UniqueIndexDescriptor> descriptors = await this.LoadUniqueIndexDescriptorsAsync(tdefPage, tableDef, tableName, cancellationToken).ConfigureAwait(false);
        if (descriptors.Count == 0)
        {
            return;
        }

        Dictionary<int, object[]> replaceAt = new(updates.Count);
        foreach ((int idx, _, object[] newRow) in updates)
        {
            replaceAt[idx] = newRow;
        }

        this.CheckUniqueIndexesCore(tableName, descriptors, existingRows, pendingInsertRows: [], replaceAtRowIndex: replaceAt);
    }

    private static bool RequiresSnapshotForPreInsert(IReadOnlyList<UniqueIndexDescriptor> descriptors)
    {
        foreach (UniqueIndexDescriptor descriptor in descriptors)
        {
            foreach (KeyColumnInfo keyColumn in descriptor.KeyColumns)
            {
                if (keyColumn.Col.Type is NumericType or MemoType)
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static bool CanUseCursorFastPath(IReadOnlyList<UniqueIndexDescriptor> descriptors)
    {
        foreach (UniqueIndexDescriptor descriptor in descriptors)
        {
            if (descriptor.RootPage <= 0)
            {
                return false;
            }
        }

        return true;
    }

    private static int[] BuildNumericScales(UniqueIndexDescriptor descriptor)
    {
        int[] scales = new int[descriptor.KeyColumns.Count];
        for (int k = 0; k < descriptor.KeyColumns.Count; k++)
        {
            ColumnInfo kCol = descriptor.KeyColumns[k].Col;
            scales[k] = kCol.Type == NumericType ? kCol.NumericScale : -1;
        }

        return scales;
    }

    /// <summary>
    /// Core: builds the post-mutation effective row set and detects any
    /// unique-key collision. Throws <see cref="InvalidOperationException"/>
    /// on first violation.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="descriptors">The descriptors.</param>
    /// <param name="existingRows">The table's live rows.</param>
    /// <param name="pendingInsertRows">The pending insert rows.</param>
    /// <param name="replaceAtRowIndex">Post-update rows keyed by their position in <paramref name="existingRows"/>.</param>
    /// <exception cref="InvalidOperationException">Thrown when the effective post-mutation row set contains a duplicate unique key.</exception>
    private void CheckUniqueIndexesCore(
        string tableName,
        List<UniqueIndexDescriptor> descriptors,
        IReadOnlyList<LocatedRow> existingRows,
        List<object[]> pendingInsertRows,
        Dictionary<int, object[]>? replaceAtRowIndex)
    {
        int existingRowCount = existingRows.Count;
        int pendingCount = pendingInsertRows.Count;

        foreach (UniqueIndexDescriptor descriptor in descriptors)
        {
            int[] numericTargetScales = new int[descriptor.KeyColumns.Count];
            for (int k = 0; k < descriptor.KeyColumns.Count; k++)
            {
                ColumnInfo kCol = descriptor.KeyColumns[k].Col;
                numericTargetScales[k] = kCol.Type == NumericType ? kCol.NumericScale : -1;
            }

            var seen = new HashSet<byte[]>(ByteArrayEqualityComparer.Instance);

            for (int r = 0; r < existingRowCount; r++)
            {
                object[] effectiveRow = replaceAtRowIndex != null && replaceAtRowIndex.TryGetValue(r, out object[]? rep)
                    ? rep
                    : existingRows[r].Values;

                byte[] key = this.EncodeCompositeKeyForUniqueCheck(descriptor, effectiveRow, numericTargetScales);

                if (!seen.Add(key))
                {
                    throw new InvalidOperationException(
                        $"Unique index violation on table '{tableName}': duplicate key for index '{descriptor.Name}'. " +
                        "The conflict was detected before any row was written; the table is unchanged.");
                }
            }

            for (int p = 0; p < pendingCount; p++)
            {
                byte[] key = this.EncodeCompositeKeyForUniqueCheck(descriptor, pendingInsertRows[p], numericTargetScales);

                if (!seen.Add(key))
                {
                    throw new InvalidOperationException(
                        $"Unique index violation on table '{tableName}': duplicate key for index '{descriptor.Name}'. " +
                        "The conflict was detected before any row was written; the table is unchanged.");
                }
            }
        }
    }
}
