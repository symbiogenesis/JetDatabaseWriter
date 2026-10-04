namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// A row must fit on one data page: the page size less the data-page header
/// and one row-offset slot, 2,036 bytes on Jet3 and 4,080 on Jet4/ACE. Long
/// values over the inline caps (1,024 MEMO and 256 OLE bytes) move to LVAL
/// pages first, so only many inline values can exceed it. The writer used to
/// append an empty data page for such a row and then throw
/// <see cref="ArgumentOutOfRangeException"/> from the page copy (or, on Jet3
/// before the jump table was written, <see cref="OverflowException"/>).
/// </summary>
public sealed class RowSizeLimitTests
{
    private const string TableName = "T";

    /// <summary>
    /// Inserts a row of <c>Id</c> plus inline 250-byte OLE values that adds up
    /// to more than a page. The insert throws before anything is written: the
    /// file is byte-for-byte what it was, in every write mode (an explicit
    /// transaction is committed after the failure).
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">"direct", "transactional" or "explicit".</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "direct")]
    [InlineData(DatabaseFormat.Jet3Mdb, "transactional")]
    [InlineData(DatabaseFormat.Jet3Mdb, "explicit")]
    [InlineData(DatabaseFormat.Jet4Mdb, "direct")]
    [InlineData(DatabaseFormat.Jet4Mdb, "transactional")]
    [InlineData(DatabaseFormat.Jet4Mdb, "explicit")]
    [InlineData(DatabaseFormat.AceAccdb, "direct")]
    [InlineData(DatabaseFormat.AceAccdb, "transactional")]
    [InlineData(DatabaseFormat.AceAccdb, "explicit")]
    public async Task RowLargerThanAPage_ThrowsJetLimitationException_AndWritesNothing(DatabaseFormat format, string mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        int oleColumns = format == DatabaseFormat.Jet3Mdb ? 9 : 17;
        await using MemoryStream ms = await CreateDatabaseAsync(format, oleColumns, memo: false, ct);
        object[] row = [1, .. Enumerable.Range(0, oleColumns).Select(i => (object)Ole(250, i))];

        await AssertOversizedInsertWritesNothingAsync(ms, format, mode, row, ct);
    }

    /// <summary>
    /// The same oversized row with a MEMO value too long to stay inline, so it
    /// is bound for LVAL pages. The writer used to write those pages before it
    /// measured the row, and the failed insert left them behind with nothing
    /// pointing at them: appended to the file, or, once a dropped table had
    /// freed pages, written over the free pages and marked used, with the file
    /// length unchanged. The row is now measured first, with a 12-byte
    /// placeholder for the value's LVAL header.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">"direct", "transactional" or "explicit".</param>
    /// <param name="freePages">Whether a dropped table has left free pages for the LVAL pages to reuse.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "direct", false)]
    [InlineData(DatabaseFormat.Jet3Mdb, "direct", true)]
    [InlineData(DatabaseFormat.Jet3Mdb, "explicit", true)]
    [InlineData(DatabaseFormat.Jet4Mdb, "direct", false)]
    [InlineData(DatabaseFormat.Jet4Mdb, "direct", true)]
    [InlineData(DatabaseFormat.Jet4Mdb, "transactional", false)]
    [InlineData(DatabaseFormat.AceAccdb, "direct", false)]
    [InlineData(DatabaseFormat.AceAccdb, "direct", true)]
    [InlineData(DatabaseFormat.AceAccdb, "explicit", false)]
    public async Task RowLargerThanAPage_WithAMemoBoundForLvalPages_WritesNothing(DatabaseFormat format, string mode, bool freePages)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        int oleColumns = format == DatabaseFormat.Jet3Mdb ? 9 : 17;
        await using MemoryStream ms = await CreateDatabaseAsync(format, oleColumns, memo: true, ct);
        if (freePages)
        {
            await using AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct);
            await writer.CreateTableAsync("Scratch", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Notes", typeof(string))], ct);
            await writer.InsertRowAsync("Scratch", [1, new string('s', 20_000)], ct);
            await writer.DropTableAsync("Scratch", ct);
        }

        object[] row = [1, .. Enumerable.Range(0, oleColumns).Select(i => (object)Ole(250, i)), new string('m', 5_000)];

        await AssertOversizedInsertWritesNothingAsync(ms, format, mode, row, ct);
    }

    /// <summary>
    /// A row of exactly the page's capacity (Id plus inline OLE values) is
    /// written and reads back; one byte more is refused.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RowAtExactPageCapacity_IsWritten(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // Jet3: 1 + 4 + 8 x (12 + len) + EOD + 8 offsets + 7 jump entries + var_len + 2 mask bytes = 2,036 with 1,916 bytes of values.
        // Jet4/ACE: 2 + 4 + 16 x (12 + len) + 2 + 32 + 2 + 3 mask bytes = 4,080 with 3,843 bytes of values.
        int[] lengths = format == DatabaseFormat.Jet3Mdb
            ? [.. Enumerable.Repeat(240, 7), 236]
            : [.. Enumerable.Repeat(240, 15), 243];
        await using MemoryStream ms = await CreateDatabaseAsync(format, lengths.Length, memo: false, ct);

        object[] fits = [1, .. lengths.Select((length, i) => (object)Ole(length, i))];
        object[] tooLong = [2, .. lengths.Select((length, i) => (object)Ole(i == 0 ? length + 1 : length, i))];
        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            await writer.InsertRowAsync(TableName, fits, ct);
            JetLimitationException ex = await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.InsertRowAsync(TableName, tooLong, ct));
            Assert.Contains($"{MaxRowLength(format) + 1} bytes", ex.Message, StringComparison.Ordinal);
        }

        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        object[] row = Assert.Single(await reader.Rows(TableName, cancellationToken: ct).ToListAsync(ct));
        Assert.Equal(fits, row);
    }

    /// <summary>
    /// An update that grows a row past a page throws the same exception. The
    /// update rewrites a row by deleting it and inserting the new version, and
    /// it used to encode the new version only after the delete, so without a
    /// transaction the row was lost. Every new version is now encoded and
    /// measured first: the row keeps its old values and the file is
    /// byte-for-byte what it was, in every write mode (an explicit transaction
    /// is committed after the failure).
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">"direct", "transactional" or "explicit".</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "direct")]
    [InlineData(DatabaseFormat.Jet3Mdb, "transactional")]
    [InlineData(DatabaseFormat.Jet3Mdb, "explicit")]
    [InlineData(DatabaseFormat.Jet4Mdb, "direct")]
    [InlineData(DatabaseFormat.Jet4Mdb, "transactional")]
    [InlineData(DatabaseFormat.Jet4Mdb, "explicit")]
    [InlineData(DatabaseFormat.AceAccdb, "direct")]
    [InlineData(DatabaseFormat.AceAccdb, "transactional")]
    [InlineData(DatabaseFormat.AceAccdb, "explicit")]
    public async Task UpdateGrowingRowPastAPage_ThrowsJetLimitationException_AndKeepsTheRow(DatabaseFormat format, string mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        int oleColumns = format == DatabaseFormat.Jet3Mdb ? 9 : 17;
        await using MemoryStream ms = await CreateDatabaseAsync(format, oleColumns, memo: false, ct);
        object[] original = [1, .. Enumerable.Range(0, oleColumns).Select(i => (object)Ole(20, i))];
        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            await writer.InsertRowAsync(TableName, original, ct);
        }

        var changes = new RowValues();
        for (int i = 0; i < oleColumns; i++)
        {
            changes[$"Blob{i}"] = Ole(250, i);
        }

        await AssertOversizedUpdateWritesNothingAsync(ms, format, mode, RowCriteria.Where("Id", 1), changes, [original], ct);
    }

    /// <summary>
    /// A multi-row update whose later row grows past a page changes no row,
    /// not even the earlier one, whose new version fits. The rows hold inline
    /// MEMO values, as in the README's "Table and row size" limitation: on
    /// Jet3 three MEMO columns, where the second row holds two 900-character
    /// values and the update sets the third to 1,000 characters (2,858 bytes),
    /// and on Jet4 and ACCDB five, four of them 900 characters long (4,691 bytes).
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">"direct", "transactional" or "explicit".</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "direct")]
    [InlineData(DatabaseFormat.Jet3Mdb, "transactional")]
    [InlineData(DatabaseFormat.Jet3Mdb, "explicit")]
    [InlineData(DatabaseFormat.Jet4Mdb, "direct")]
    [InlineData(DatabaseFormat.Jet4Mdb, "transactional")]
    [InlineData(DatabaseFormat.Jet4Mdb, "explicit")]
    [InlineData(DatabaseFormat.AceAccdb, "direct")]
    [InlineData(DatabaseFormat.AceAccdb, "transactional")]
    [InlineData(DatabaseFormat.AceAccdb, "explicit")]
    public async Task UpdateGrowingALaterRowPastAPage_ChangesNoRow(DatabaseFormat format, string mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        int memoColumns = format == DatabaseFormat.Jet3Mdb ? 3 : 5;
        var columns = new List<ColumnDefinition> { new("Id", typeof(int)) };
        for (int i = 0; i < memoColumns; i++)
        {
            columns.Add(new ColumnDefinition($"Memo{i}", typeof(string)));
        }

        await using MemoryStream ms = await CreateDatabaseAsync(format, columns, ct);
        object[] fits = [1, .. Enumerable.Repeat<object>(DBNull.Value, memoColumns)];
        object[] grows = [2, DBNull.Value, .. Enumerable.Repeat<object>(new string('b', 900), memoColumns - 1)];
        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            Assert.Equal(2, await writer.InsertRowsAsync(TableName, [fits, grows], ct));
        }

        await AssertOversizedUpdateWritesNothingAsync(
            ms,
            format,
            mode,
            RowCriteria.All(),
            new RowValues { ["Memo0"] = new string('a', 1_000) },
            [fits, grows],
            ct);
    }

    private static int MaxRowLength(DatabaseFormat format) => format == DatabaseFormat.Jet3Mdb ? 2036 : 4080;

    /// <summary>
    /// Runs <paramref name="changes"/> on the rows <paramref name="criteria"/>
    /// matches in <paramref name="mode"/>, expects the row-size
    /// <see cref="JetLimitationException"/>, and checks that the file is
    /// byte-for-byte unchanged, the table holds <paramref name="expectedRows"/>
    /// and its stored row count is unchanged.
    /// </summary>
    /// <param name="ms">The database.</param>
    /// <param name="format">The database format.</param>
    /// <param name="mode">"direct", "transactional" or "explicit".</param>
    /// <param name="criteria">The rows to update.</param>
    /// <param name="changes">Changes that grow a matching row past a page.</param>
    /// <param name="expectedRows">The table's rows, in table order.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static async Task AssertOversizedUpdateWritesNothingAsync(
        MemoryStream ms,
        DatabaseFormat format,
        string mode,
        RowCriteria criteria,
        RowValues changes,
        object[][] expectedRows,
        CancellationToken cancellationToken)
    {
        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = mode == "transactional" }, cancellationToken))
        {
            JetTransaction? transaction = mode == "explicit" ? await writer.BeginTransactionAsync(cancellationToken) : null;
            JetLimitationException ex = await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.UpdateRowsAsync(TableName, criteria, changes, cancellationToken));
            Assert.Contains($"{MaxRowLength(format)}-byte maximum", ex.Message, StringComparison.Ordinal);

            if (transaction is not null)
            {
                await transaction.CommitAsync(cancellationToken);
                await transaction.DisposeAsync();
            }
        }

        Assert.Equal(before, ms.ToArray());
        await using AccessReader reader = await OpenReaderAsync(ms, cancellationToken);
        Assert.Equal(expectedRows, await reader.Rows(TableName, cancellationToken: cancellationToken).ToListAsync(cancellationToken));
        TableStat stat = Assert.Single(await reader.GetTableStatsAsync(cancellationToken), s => s.Name == TableName);
        Assert.Equal(expectedRows.Length, stat.RowCount);
    }

    /// <summary>
    /// Inserts <paramref name="row"/> in <paramref name="mode"/>, expects the
    /// row-size <see cref="JetLimitationException"/>, and checks that the file
    /// is byte-for-byte unchanged and the table still empty.
    /// </summary>
    /// <param name="ms">The database.</param>
    /// <param name="format">The database format.</param>
    /// <param name="mode">"direct", "transactional" or "explicit".</param>
    /// <param name="row">A row larger than a page.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static async Task AssertOversizedInsertWritesNothingAsync(MemoryStream ms, DatabaseFormat format, string mode, object[] row, CancellationToken cancellationToken)
    {
        byte[] before = ms.ToArray();
        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = mode == "transactional" }, cancellationToken))
        {
            JetTransaction? transaction = mode == "explicit" ? await writer.BeginTransactionAsync(cancellationToken) : null;
            JetLimitationException ex = await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.InsertRowAsync(TableName, row, cancellationToken));
            Assert.Contains($"{MaxRowLength(format)}-byte maximum", ex.Message, StringComparison.Ordinal);

            if (transaction is not null)
            {
                await transaction.CommitAsync(cancellationToken);
                await transaction.DisposeAsync();
            }
        }

        Assert.Equal(before.Length, ms.Length);
        Assert.Equal(before, ms.ToArray());
        await using AccessReader reader = await OpenReaderAsync(ms, cancellationToken);
        Assert.Equal(0, await reader.GetRealRowCountAsync(TableName, cancellationToken));
        using DataTable table = await reader.ReadDataTableAsync(TableName, cancellationToken: cancellationToken);
        Assert.Empty(table.Rows);
    }

    /// <summary>OLE bytes that start <c>11 22</c> and carry no file signature, so the read API returns them unchanged.</summary>
    /// <param name="length">The length.</param>
    /// <param name="salt">Varies the bytes per column.</param>
    /// <returns>The OLE bytes.</returns>
    private static byte[] Ole(int length, int salt)
    {
        byte[] bytes = new byte[length];
        for (int i = 0; i < bytes.Length; i++)
        {
            bytes[i] = unchecked((byte)((i * 7) + salt));
        }

        bytes[0] = 0x11;
        bytes[1] = 0x22;
        return bytes;
    }

    private static Task<MemoryStream> CreateDatabaseAsync(DatabaseFormat format, int oleColumns, bool memo, CancellationToken cancellationToken)
    {
        var columns = new List<ColumnDefinition> { new("Id", typeof(int)) };
        for (int i = 0; i < oleColumns; i++)
        {
            columns.Add(new ColumnDefinition($"Blob{i}", typeof(byte[])));
        }

        if (memo)
        {
            columns.Add(new ColumnDefinition("Notes", typeof(string)));
        }

        return CreateDatabaseAsync(format, columns, cancellationToken);
    }

    private static async Task<MemoryStream> CreateDatabaseAsync(DatabaseFormat format, List<ColumnDefinition> columns, CancellationToken cancellationToken)
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken))
        {
            await writer.CreateTableAsync(TableName, columns, cancellationToken);
        }

        return ms;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream ms, AccessWriterOptions options, CancellationToken cancellationToken)
    {
        ms.Position = 0;
        return AccessWriter.OpenAsync(ms, options, leaveOpen: true, cancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream ms, CancellationToken cancellationToken)
    {
        ms.Position = 0;
        return AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken);
    }
}
