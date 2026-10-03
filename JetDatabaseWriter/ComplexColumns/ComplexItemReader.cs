namespace JetDatabaseWriter.ComplexColumns;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;

/// <summary>
/// Reads the items stored behind an Access 2007+ complex column: the
/// attachments of an Attachment column and the values of a Multi-value column.
/// Each lives in a hidden flat child table, which the complex-column reader
/// decodes with the same code that builds the row-read cells. Each operation
/// enters the reader's operation gate so disposal waits for it.
/// </summary>
/// <param name="complexColumns">Finds and decodes the column's flat child table.</param>
/// <param name="operations">The reader's operation gate.</param>
internal sealed class ComplexItemReader(ComplexColumnReader complexColumns, AsyncReentrantOperationGate operations)
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
        return info == null
            ? []
            : await complexColumns.ReadAttachmentsAsync(info, cancellationToken).ConfigureAwait(false);
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
        return info == null
            ? []
            : await complexColumns.ReadMultiValueItemsAsync(info, cancellationToken).ConfigureAwait(false);
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
