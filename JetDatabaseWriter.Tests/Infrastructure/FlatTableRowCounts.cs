namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;

/// <summary>
/// Reads the row counts of a complex column's hidden flat table, for tests
/// that check a delete keeps the flat table's TDEF row count in step with its
/// rows.
/// </summary>
internal static class FlatTableRowCounts
{
    private static readonly AccessReaderOptions ReaderOptions = new() { UseLockFile = false };

    /// <summary>
    /// Returns the row count the TDEF of the flat table behind
    /// <paramref name="tableName"/>.<paramref name="columnName"/> declares, and
    /// the number of live rows its data pages hold.
    /// </summary>
    /// <param name="ms">The database.</param>
    /// <param name="tableName">The parent table.</param>
    /// <param name="columnName">The Attachment or multi-value column.</param>
    /// <param name="cancellationToken">A token used to cancel the reads.</param>
    /// <returns>The declared and the live row counts.</returns>
    public static async Task<(long Declared, int Live)> ReadAsync(MemoryStream ms, string tableName, string columnName, CancellationToken cancellationToken)
    {
        long flatTdefPage;
        ms.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, cancellationToken))
        {
            IReadOnlyList<ComplexColumnInfo> columns = await reader.GetComplexColumnsAsync(tableName, cancellationToken);
            flatTdefPage = columns.Single(column => column.ColumnName == columnName).FlatTableId;
        }

        ms.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(ms, ReaderOptions, cancellationToken: cancellationToken);
        byte[] tdef = await harness.ReadPageCopyAsync(flatTdefPage, cancellationToken);
        List<RowLocation> live = await harness.Database.GetLiveRowLocationsAsync(flatTdefPage, cancellationToken);
        ms.Position = 0;
        return (BinaryPrimitives.ReadUInt32LittleEndian(tdef.AsSpan(harness.Database.TDef.NumRows)), live.Count);
    }
}
