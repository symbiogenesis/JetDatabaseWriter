namespace JetDatabaseWriter.ComplexColumns;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.ComplexColumns.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tables;

/// <summary>
/// Reads the items stored behind an Access 2007+ complex column: the
/// attachments of an Attachment column and the values of a Multi-value column.
/// Each lives in a hidden flat child table, which is read through the table
/// reader and decoded here. Each operation enters the reader's operation gate
/// so disposal waits for it.
/// </summary>
/// <param name="complexColumns">Finds the column's flat child table.</param>
/// <param name="tables">Reads the flat child table.</param>
/// <param name="operations">The reader's operation gate.</param>
internal sealed class ComplexItemReader(ComplexColumnReader complexColumns, TableReader tables, AsyncReentrantOperationGate operations)
{
    /// <summary>
    /// Returns every attachment row stored in the hidden flat child table backing
    /// the Attachment column <paramref name="columnName"/> on <paramref name="tableName"/>.
    /// </summary>
    /// <param name="tableName">Parent table name (case-insensitive).</param>
    /// <param name="columnName">Attachment column name (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<IReadOnlyList<AttachmentRecord>> GetAttachmentsAsync(string tableName, string columnName, CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(columnName, nameof(columnName));
        cancellationToken.ThrowIfCancellationRequested();

        ComplexColumnInfo? info = await this.FindComplexColumnAsync(tableName, columnName, cancellationToken).ConfigureAwait(false);
        if (info == null || string.IsNullOrEmpty(info.FlatTableName))
        {
            return [];
        }

        DataTable flat = await tables.ReadTableAsync(info.FlatTableName, maxRows: null, progress: null, cancellationToken).ConfigureAwait(false);
        if (flat.Rows.Count == 0)
        {
            return [];
        }

        int idxFk = FindFlatLongFkIndex(flat);
        int idxFileUrl = flat.Columns.IndexOf("FileURL");
        int idxFileName = flat.Columns.IndexOf("FileName");
        int idxFileType = flat.Columns.IndexOf("FileType");
        int idxFileTime = flat.Columns.IndexOf("FileTimeStamp");
        int idxFileData = flat.Columns.IndexOf("FileData");

        var result = new List<AttachmentRecord>(flat.Rows.Count);
        foreach (DataRow row in flat.Rows)
        {
            int fk = idxFk >= 0 && row[idxFk] is not DBNull ? Convert.ToInt32(row[idxFk], CultureInfo.InvariantCulture) : 0;
            byte[] rawData = ExtractOleBytesBestEffort(idxFileData >= 0 ? row[idxFileData] : null);
            byte[] decoded = rawData;
            string ext = idxFileType >= 0 && row[idxFileType] is not DBNull ? Convert.ToString(row[idxFileType], CultureInfo.InvariantCulture) ?? string.Empty : string.Empty;
            if (rawData.Length > 0 && AttachmentWrapper.TryDecode(rawData, out string decodedExt, out byte[] payload))
            {
                decoded = payload;
                if (string.IsNullOrEmpty(ext))
                {
                    ext = decodedExt;
                }
            }

            result.Add(new AttachmentRecord
            {
                ConceptualTableId = fk,
                FileName = idxFileName >= 0 && row[idxFileName] is not DBNull ? Convert.ToString(row[idxFileName], CultureInfo.InvariantCulture) ?? string.Empty : string.Empty,
                FileType = ext,
                FileURL = idxFileUrl >= 0 && row[idxFileUrl] is not DBNull ? Convert.ToString(row[idxFileUrl], CultureInfo.InvariantCulture) : null,
                FileTimeStamp = idxFileTime >= 0 && row[idxFileTime] is DateTime dt ? dt : null,
                FileData = decoded,
            });
        }

        return result;
    }

    /// <summary>
    /// Returns every value stored in the hidden flat child table backing the
    /// Multi-value column <paramref name="columnName"/> on <paramref name="tableName"/>.
    /// </summary>
    /// <param name="tableName">Parent table name (case-insensitive).</param>
    /// <param name="columnName">Multi-value column name (case-insensitive).</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<IReadOnlyList<MultiValueItem>> GetMultiValueItemsAsync(string tableName, string columnName, CancellationToken cancellationToken)
    {
        using AsyncReentrantOperationGate.Lease operation = operations.Enter();
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(columnName, nameof(columnName));
        cancellationToken.ThrowIfCancellationRequested();

        ComplexColumnInfo? info = await this.FindComplexColumnAsync(tableName, columnName, cancellationToken).ConfigureAwait(false);
        if (info == null || string.IsNullOrEmpty(info.FlatTableName))
        {
            return [];
        }

        DataTable flat = await tables.ReadTableAsync(info.FlatTableName, maxRows: null, progress: null, cancellationToken).ConfigureAwait(false);
        if (flat.Rows.Count == 0)
        {
            return [];
        }

        int idxFk = FindFlatLongFkIndex(flat);
        int idxValue = flat.Columns.IndexOf("value");
        if (idxValue < 0)
        {
            for (int i = 0; i < flat.Columns.Count; i++)
            {
                if (i != idxFk)
                {
                    idxValue = i;
                    break;
                }
            }
        }

        var result = new List<MultiValueItem>(flat.Rows.Count);
        foreach (DataRow row in flat.Rows)
        {
            int fk = idxFk >= 0 && row[idxFk] is not DBNull ? Convert.ToInt32(row[idxFk], CultureInfo.InvariantCulture) : 0;
            object? value = idxValue >= 0 && row[idxValue] is not DBNull ? row[idxValue] : null;
            result.Add(new MultiValueItem
            {
                ConceptualTableId = fk,
                Value = value,
            });
        }

        return result;
    }

    private static byte[] ExtractOleBytesBestEffort(object? cell)
    {
        if (cell is null or DBNull)
        {
            return [];
        }

        if (cell is byte[] b)
        {
            return b;
        }

        if (cell is string s)
        {
            return BinaryStringParser.TryDecodeBase64DataUri(s, out byte[] bytes) ? bytes : [];
        }

        return [];
    }

    private static int FindFlatLongFkIndex(DataTable flat)
    {
        for (int i = 0; i < flat.Columns.Count; i++)
        {
            DataColumn c = flat.Columns[i];
            if (c.DataType == typeof(int) && c.ColumnName.StartsWith('_'))
            {
                return i;
            }
        }

        for (int i = 0; i < flat.Columns.Count; i++)
        {
            if (flat.Columns[i].DataType == typeof(int))
            {
                return i;
            }
        }

        return -1;
    }

    private async ValueTask<ComplexColumnInfo?> FindComplexColumnAsync(string tableName, string columnName, CancellationToken cancellationToken)
    {
        IReadOnlyList<ComplexColumnInfo> complex = await complexColumns.GetComplexColumnsAsync(tableName, cancellationToken).ConfigureAwait(false);
        foreach (ComplexColumnInfo column in complex)
        {
            if (string.Equals(column.ColumnName, columnName, StringComparison.OrdinalIgnoreCase))
            {
                return column;
            }
        }

        return null;
    }
}
