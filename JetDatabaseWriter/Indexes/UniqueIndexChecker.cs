namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Constants.ColumnTypes;
using KeyColumnInfo = JetDatabaseWriter.Indexes.IndexLayout.KeyColumnInfo;
using RealIdxEntry = JetDatabaseWriter.Indexes.IndexLayout.RealIdxEntry;
using UniqueIndexDescriptor = JetDatabaseWriter.Indexes.IndexLayout.UniqueIndexDescriptor;

#pragma warning disable CA1822 // Mark members as static

/// <summary>
/// Pre-write unique-index enforcement: detects duplicate keys before any
/// disk page is mutated. Owned by <see cref="AccessWriter"/>.
/// </summary>
internal sealed class UniqueIndexChecker(AccessWriter writer)
{
    /// <summary>
    /// Loads all unique / primary-key index descriptors for the given TDEF page.
    /// Returns an empty list on Jet3 (no index emission) or when the TDEF
    /// declares no indexes.
    /// </summary>
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

        int numCols = AccessBase.Ru16(tdefBuffer, writer._tdef.NumCols);
        int numIdx = AccessBase.Ri32(tdefBuffer, writer._tdef.NumCols + 2);
        int numRealIdx = AccessBase.Ri32(tdefBuffer, writer._tdef.NumRealIdx);
        if (numIdx <= 0 || numRealIdx <= 0 || numIdx > 1000 || numRealIdx > 1000)
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
        List<string> logIdxNames = writer.Relationships.ReadLogicalIdxNames(tdefBuffer, anchors.LogIdxNamesStart, numIdx);

        IndexCatalogReader.ResolvedIndexCatalog catalog = IndexCatalogReader.ReadResolved(
            tdefBuffer, writer._indexLayout, anchors, tableDef.Columns, logIdxNames);

        foreach ((int realIdxNum, RealIdxEntry slot) in catalog.RealIdxByNum)
        {
            if (!catalog.Catalog.IsUniqueOrPk(realIdxNum))
            {
                continue;
            }

            if (!catalog.TryGetKeyColumnInfos(realIdxNum, out List<KeyColumnInfo> keyColInfos))
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
    internal byte[] EncodeCompositeKeyForUniqueCheck(
        UniqueIndexDescriptor descriptor,
        object[] row,
        int[] numericTargetScales)
    {
        bool legacyNumeric = writer._format == Enums.DatabaseFormat.Jet4Mdb;
        byte[][] perColumn = new byte[descriptor.KeyColumns.Count][];
        int totalLen = 0;
        for (int k = 0; k < descriptor.KeyColumns.Count; k++)
        {
            (ColumnInfo col, int snapIdx, bool ascending) = descriptor.KeyColumns[k];
            object cell = snapIdx < row.Length ? row[snapIdx] : DBNull.Value;
            object? value = cell is null or DBNull ? null : cell;
            perColumn[k] = col.Type == T_NUMERIC
                ? IndexKeyEncoder.EncodeNumericEntryAtDeclaredScale(value, ascending, (byte)numericTargetScales[k], legacyNumeric)
                : IndexKeyEncoder.EncodeEntry(col.Type, value, ascending);
            totalLen += perColumn[k].Length;
        }

        byte[] composite = new byte[totalLen];
        int offset = 0;
        for (int k = 0; k < perColumn.Length; k++)
        {
            Buffer.BlockCopy(perColumn[k], 0, composite, offset, perColumn[k].Length);
            offset += perColumn[k].Length;
        }

        return composite;
    }

    /// <summary>
    /// Pre-write unique-index validation for an insert batch.
    /// </summary>
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

        List<UniqueIndexDescriptor> descriptors = await LoadUniqueIndexDescriptorsAsync(tdefPage, tableDef, cancellationToken).ConfigureAwait(false);
        if (descriptors.Count == 0)
        {
            return;
        }

        using DataTable snapshot = await writer.ReadTableSnapshotAsync(tableName, cancellationToken).ConfigureAwait(false);
        CheckUniqueIndexesCore(tableName, descriptors, snapshot, pendingRows, replaceAtSnapshotIndex: null);
    }

    /// <summary>
    /// Pre-write unique-index validation for an update batch.
    /// </summary>
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

        List<UniqueIndexDescriptor> descriptors = await LoadUniqueIndexDescriptorsAsync(tdefPage, tableDef, cancellationToken).ConfigureAwait(false);
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

    /// <summary>
    /// Core: builds the post-mutation effective row set and detects any
    /// unique-key collision. Throws <see cref="InvalidOperationException"/>
    /// on first violation.
    /// </summary>
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

        foreach (UniqueIndexDescriptor descriptor in descriptors)
        {
            int[] numericTargetScales = new int[descriptor.KeyColumns.Count];
            for (int k = 0; k < descriptor.KeyColumns.Count; k++)
            {
                ColumnInfo kCol = descriptor.KeyColumns[k].Col;
                numericTargetScales[k] = kCol.Type == T_NUMERIC ? kCol.NumericScale : -1;
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
