namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Models;
using static JetDatabaseWriter.Constants.ColumnTypes;
using static JetDatabaseWriter.Schema.JetTypeInfo;
using UniqueIndexDescriptor = JetDatabaseWriter.Indexes.IndexLayout.UniqueIndexDescriptor;

/// <summary>
/// Pre-write unique-index enforcement: detects duplicate keys before any
/// disk page is mutated. Owned by <see cref="AccessWriter"/>.
/// </summary>
/// <param name="writer">The writer.</param>
internal sealed class UniqueIndexChecker(AccessWriter writer)
{
    /// <summary>
    /// Loads all unique / primary-key index descriptors for the given TDEF page.
    /// Returns an empty list on Jet3 (no index emission) or when the TDEF
    /// declares no indexes.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    internal async ValueTask<List<UniqueIndexDescriptor>> LoadUniqueIndexDescriptorsAsync(
        long tdefPage, TableDef tableDef, CancellationToken cancellationToken)
    {
        var result = new List<UniqueIndexDescriptor>();

        byte[] tdefPageBytes = await writer.ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        byte[] tdefBuffer;
        try
        {
            tdefBuffer = (byte[])tdefPageBytes.Clone();
        }
        finally
        {
            AccessBase.ReturnPage(tdefPageBytes);
        }

        int numCols = Ru16(tdefBuffer, writer._tdef.NumCols);
        int numIdx = Ri32(tdefBuffer, writer._tdef.NumCols + 2);
        int numRealIdx = Ri32(tdefBuffer, writer._tdef.NumRealIdx);
        if (numIdx <= 0 || numRealIdx <= 0
            || numIdx > Constants.TableDefinition.MaxIndexes
            || numRealIdx > Constants.TableDefinition.MaxIndexes)
        {
            return result;
        }

        int colStart = writer._tdef.BlockEnd + (numRealIdx * writer._tdef.RealIdxEntrySz);
        int namePos = colStart + (numCols * writer._colDesc.Size);
        for (int i = 0; i < numCols; i++)
        {
            if (writer.ReadColumnName(tdefBuffer, ref namePos, out _) < 0)
            {
                return result;
            }
        }

        int realIdxDescStart = namePos;
        var anchors = writer._indexLayout.GetIndexSection(realIdxDescStart, numRealIdx, numIdx);
        var logIdxNames = writer.Relationships.ReadLogicalIdxNames(tdefBuffer, anchors.LogIdxNamesStart, numIdx);

        var catalog = IndexCatalogReader.ReadResolved(
            tdefBuffer, writer._indexLayout, anchors, tableDef.Columns, logIdxNames);

        foreach ((int realIdxNum, var slot) in catalog.RealIdxByNum)
        {
            if (!catalog.Catalog.IsUniqueOrPk(realIdxNum))
            {
                continue;
            }

            if (!catalog.TryGetKeyColumnInfos(realIdxNum, out var keyColInfos))
            {
                continue;
            }

            result.Add(new UniqueIndexDescriptor(realIdxNum, catalog.Catalog.GetNameOrFallback(realIdxNum), keyColInfos));
        }

        return result;
    }

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
        bool legacyNumeric = writer._format == Enums.DatabaseFormat.Jet4Mdb;
        int keyCount = descriptor.KeyColumns.Count;

        // Single-column fast path: avoid the per-column array + copy.
        if (keyCount == 1)
        {
            (var col, int snapIdx, bool ascending) = descriptor.KeyColumns[0];
            object cell = snapIdx < row.Length ? row[snapIdx] : DBNull.Value;
            object? value = cell is null or DBNull ? null : cell;
            return col.Type == NumericType
                ? IndexKeyEncoder.EncodeNumericEntryAtDeclaredScale(value, ascending, (byte)numericTargetScales[0], legacyNumeric)
                : IndexKeyEncoder.EncodeEntry(col.Type, value, ascending);
        }

        // Multi-column: encode into per-column spans then concatenate.
        Span<int> lengths = stackalloc int[keyCount];
        byte[][] perColumn = new byte[keyCount][];
        int totalLen = 0;
        for (int k = 0; k < keyCount; k++)
        {
            (var col, int snapIdx, bool ascending) = descriptor.KeyColumns[k];
            object cell = snapIdx < row.Length ? row[snapIdx] : DBNull.Value;
            object? value = cell is null or DBNull ? null : cell;
            perColumn[k] = col.Type == NumericType
                ? IndexKeyEncoder.EncodeNumericEntryAtDeclaredScale(value, ascending, (byte)numericTargetScales[k], legacyNumeric)
                : IndexKeyEncoder.EncodeEntry(col.Type, value, ascending);
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
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
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

        var descriptors = await LoadUniqueIndexDescriptorsAsync(tdefPage, tableDef, cancellationToken).ConfigureAwait(false);
        if (descriptors.Count == 0)
        {
            return;
        }

        // Fast path: read only the key columns directly from data pages via
        // TryReadColumnValuesTypedAsync. Keep Numeric indexes on the full-table
        // snapshot path until the byte-for-byte key normalization is validated
        // against the descriptor scale and Jet4/ACE numeric encoding variants.
        bool needsSnapshot = false;
        foreach (var desc in descriptors)
        {
            foreach ((var col, _, _) in desc.KeyColumns)
            {
                if (col.Type == NumericType)
                {
                    needsSnapshot = true;
                    break;
                }
            }

            if (needsSnapshot)
            {
                break;
            }
        }

        if (needsSnapshot)
        {
            using var snapshot = await writer.ReadTableSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);
            CheckUniqueIndexesCore(tableName, descriptors, snapshot, pendingRows, replaceAtSnapshotIndex: null);
            return;
        }

        // Gather existing key-column values directly from data pages.
        var locations = await writer.GetLiveRowLocationsAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        await CheckUniqueIndexesFastPathAsync(tableName, descriptors, tableDef, locations, pendingRows, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Fast-path uniqueness check: reads only the key columns from existing
    /// rows (no DataTable materialization) and validates the pending batch.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="descriptors">The descriptors.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="locations">The locations.</param>
    /// <param name="pendingRows">The pending rows.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    /// <exception cref="InvalidOperationException">Thrown when a pending insert would duplicate a unique key.</exception>
    private async ValueTask CheckUniqueIndexesFastPathAsync(
        string tableName,
        List<UniqueIndexDescriptor> descriptors,
        TableDef tableDef,
        List<RowLocation> locations,
        List<object[]> pendingRows,
        CancellationToken cancellationToken)
    {
        // Build ordered column indices for each descriptor.
        var descriptorOrdinals = new int[descriptors.Count][];
        for (int d = 0; d < descriptors.Count; d++)
        {
            descriptorOrdinals[d] = new int[descriptors[d].KeyColumns.Count];
            for (int k = 0; k < descriptors[d].KeyColumns.Count; k++)
            {
                descriptorOrdinals[d][k] = descriptors[d].KeyColumns[k].SnapIdx;
            }
        }

        // Collect unique set of all key column ordinals to read.
        var allOrdinals = new HashSet<int>();
        foreach (int[] ords in descriptorOrdinals)
        {
            foreach (int ord in ords)
            {
                allOrdinals.Add(ord);
            }
        }

        int[] columnOrdinalsArray = [.. allOrdinals];

        // Per-descriptor seen sets.
        var seenSets = new HashSet<byte[]>[descriptors.Count];
        for (int d = 0; d < descriptors.Count; d++)
        {
            seenSets[d] = new HashSet<byte[]>(ByteArrayEqualityComparer.Instance);
        }

        // Read existing rows (key columns only).
        foreach (var loc in locations)
        {
            cancellationToken.ThrowIfCancellationRequested();

            object?[]? values = await writer.TryReadColumnValuesTypedAsync(loc, tableDef, columnOrdinalsArray, cancellationToken).ConfigureAwait(false);
            if (values == null)
            {
                continue;
            }

            for (int d = 0; d < descriptors.Count; d++)
            {
                var descriptor = descriptors[d];
                int[] numericTargetScales = UniqueIndexChecker.BuildNumericScales(descriptor);

                // Map from the columnOrdinalsArray position back to the full row object[].
                object[] fullRow = UniqueIndexChecker.BuildRowFromPartialValues(descriptor, values, columnOrdinalsArray);
                byte[] key = EncodeCompositeKeyForUniqueCheck(descriptor, fullRow, numericTargetScales);
                _ = seenSets[d].Add(key);
            }
        }

        // Check pending rows.
        for (int p = 0; p < pendingRows.Count; p++)
        {
            for (int d = 0; d < descriptors.Count; d++)
            {
                var descriptor = descriptors[d];
                int[] numericTargetScales = UniqueIndexChecker.BuildNumericScales(descriptor);
                byte[] key = EncodeCompositeKeyForUniqueCheck(descriptor, pendingRows[p], numericTargetScales);

                if (!seenSets[d].Add(key))
                {
                    throw new InvalidOperationException(
                        $"Unique index violation on table '{tableName}': duplicate key for index '{descriptor.Name}'. " +
                        "The conflict was detected before any row was written; the table is unchanged.");
                }
            }
        }
    }

    /// <summary>
    /// Pre-write unique-index validation for an update batch.
    /// </summary>
    /// <param name="tdefPage">The TDEF page.</param>
    /// <param name="tableDef">The table def.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="snapshot">The snapshot.</param>
    /// <param name="updates">The updates.</param>
    /// <param name="cancellationToken">A value indicating whether cancellation token.</param>
    internal async ValueTask CheckUniqueIndexesPreUpdateAsync(
        long tdefPage,
        TableDef tableDef,
        string tableName,
        DataTable snapshot,
        List<(int Index, object[] NewRow)> updates,
        CancellationToken cancellationToken)
    {
        if (updates.Count == 0)
        {
            return;
        }

        var descriptors = await LoadUniqueIndexDescriptorsAsync(tdefPage, tableDef, cancellationToken).ConfigureAwait(false);
        if (descriptors.Count == 0)
        {
            return;
        }

        var replaceAt = new Dictionary<int, object[]>(updates.Count);
        foreach ((int idx, object[] newRow) in updates)
        {
            replaceAt[idx] = newRow;
        }

        CheckUniqueIndexesCore(tableName, descriptors, snapshot, pendingInsertRows: [], replaceAtSnapshotIndex: replaceAt);
    }

    private static int[] BuildNumericScales(UniqueIndexDescriptor descriptor)
    {
        int[] scales = new int[descriptor.KeyColumns.Count];
        for (int k = 0; k < descriptor.KeyColumns.Count; k++)
        {
            var kCol = descriptor.KeyColumns[k].Col;
            scales[k] = kCol.Type == NumericType ? kCol.NumericScale : -1;
        }

        return scales;
    }

    private static object[] BuildRowFromPartialValues(UniqueIndexDescriptor descriptor, object?[] partialValues, int[] columnOrdinalsArray)
    {
        // Build a row array sized to cover all key column snap indices.
        int maxIdx = 0;
        foreach ((_, int snapIdx, _) in descriptor.KeyColumns)
        {
            if (snapIdx > maxIdx)
            {
                maxIdx = snapIdx;
            }
        }

        var row = new object[maxIdx + 1];
        for (int i = 0; i < row.Length; i++)
        {
            row[i] = DBNull.Value;
        }

        // Map: columnOrdinalsArray[i] → partialValues[i]
        for (int i = 0; i < columnOrdinalsArray.Length; i++)
        {
            int ord = columnOrdinalsArray[i];
            if (ord < row.Length)
            {
                row[ord] = partialValues[i] ?? DBNull.Value;
            }
        }

        return row;
    }

    /// <summary>
    /// Core: builds the post-mutation effective row set and detects any
    /// unique-key collision. Throws <see cref="InvalidOperationException"/>
    /// on first violation.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="descriptors">The descriptors.</param>
    /// <param name="snapshot">The snapshot.</param>
    /// <param name="pendingInsertRows">The pending insert rows.</param>
    /// <param name="replaceAtSnapshotIndex">The replace at snapshot index.</param>
    /// <exception cref="InvalidOperationException">Thrown when the effective post-mutation row set contains a duplicate unique key.</exception>
    private void CheckUniqueIndexesCore(
        string tableName,
        List<UniqueIndexDescriptor> descriptors,
        DataTable snapshot,
        List<object[]> pendingInsertRows,
        Dictionary<int, object[]>? replaceAtSnapshotIndex)
    {
        int snapshotRowCount = snapshot.Rows.Count;
        int pendingCount = pendingInsertRows.Count;
        int totalRows = snapshotRowCount + pendingCount;

        foreach (var descriptor in descriptors)
        {
            int[] numericTargetScales = new int[descriptor.KeyColumns.Count];
            for (int k = 0; k < descriptor.KeyColumns.Count; k++)
            {
                var kCol = descriptor.KeyColumns[k].Col;
                numericTargetScales[k] = kCol.Type == NumericType ? kCol.NumericScale : -1;
            }

            var seen = new HashSet<byte[]>(ByteArrayEqualityComparer.Instance);

            for (int r = 0; r < snapshotRowCount; r++)
            {
                object[] effectiveRow;
                if (replaceAtSnapshotIndex != null && replaceAtSnapshotIndex.TryGetValue(r, out object[]? rep))
                {
                    effectiveRow = rep;
                }
                else
                {
                    effectiveRow = snapshot.Rows[r].ItemArray;
                }

                byte[] key = EncodeCompositeKeyForUniqueCheck(descriptor, effectiveRow, numericTargetScales);

                if (!seen.Add(key))
                {
                    throw new InvalidOperationException(
                        $"Unique index violation on table '{tableName}': duplicate key for index '{descriptor.Name}'. " +
                        "The conflict was detected before any row was written; the table is unchanged.");
                }
            }

            for (int p = 0; p < pendingCount; p++)
            {
                byte[] key = EncodeCompositeKeyForUniqueCheck(descriptor, pendingInsertRows[p], numericTargetScales);

                if (!seen.Add(key))
                {
                    throw new InvalidOperationException(
                        $"Unique index violation on table '{tableName}': duplicate key for index '{descriptor.Name}'. " +
                        "The conflict was detected before any row was written; the table is unchanged.");
                }
            }

            _ = totalRows;
        }
    }
}
