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
    /// to more than a page. The insert throws before anything is written.
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
        await using MemoryStream ms = await CreateDatabaseAsync(format, oleColumns, ct);
        byte[] before = ms.ToArray();

        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = mode == "transactional" }, ct))
        {
            JetTransaction? transaction = mode == "explicit" ? await writer.BeginTransactionAsync(ct) : null;
            object[] row = [1, .. Enumerable.Range(0, oleColumns).Select(i => (object)Ole(250, i))];
            JetLimitationException ex = await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.InsertRowAsync(TableName, row, ct));
            Assert.Contains($"{MaxRowLength(format)}-byte maximum", ex.Message, StringComparison.Ordinal);

            if (transaction is not null)
            {
                await transaction.CommitAsync(ct);
                await transaction.DisposeAsync();
            }
        }

        Assert.Equal(before.Length, ms.Length);
        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        Assert.Equal(0, await reader.GetRealRowCountAsync(TableName, ct));
        using DataTable table = await reader.ReadDataTableAsync(TableName, cancellationToken: ct);
        Assert.Empty(table.Rows);
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
        await using MemoryStream ms = await CreateDatabaseAsync(format, lengths.Length, ct);

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
    /// update deletes the old row before it re-inserts the new one, so this
    /// checks the two modes that undo the delete: <see cref="AccessWriterOptions.UseTransactionalWrites"/>
    /// and a rolled-back explicit transaction.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">"transactional" or "explicit".</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "transactional")]
    [InlineData(DatabaseFormat.Jet3Mdb, "explicit")]
    [InlineData(DatabaseFormat.Jet4Mdb, "transactional")]
    [InlineData(DatabaseFormat.AceAccdb, "explicit")]
    public async Task UpdateGrowingRowPastAPage_ThrowsJetLimitationException_AndKeepsTheRow(DatabaseFormat format, string mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        int oleColumns = format == DatabaseFormat.Jet3Mdb ? 9 : 17;
        await using MemoryStream ms = await CreateDatabaseAsync(format, oleColumns, ct);
        object[] original = [1, .. Enumerable.Range(0, oleColumns).Select(i => (object)Ole(20, i))];
        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            await writer.InsertRowAsync(TableName, original, ct);
        }

        var changes = new Dictionary<string, object?>();
        for (int i = 0; i < oleColumns; i++)
        {
            changes[$"Blob{i}"] = Ole(250, i);
        }

        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = mode == "transactional" }, ct))
        {
            JetTransaction? transaction = mode == "explicit" ? await writer.BeginTransactionAsync(ct) : null;
            JetLimitationException ex = await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.UpdateRowsAsync(TableName, "Id", 1, changes, ct));
            Assert.Contains($"{MaxRowLength(format)}-byte maximum", ex.Message, StringComparison.Ordinal);

            if (transaction is not null)
            {
                await transaction.RollbackAsync(ct);
                await transaction.DisposeAsync();
            }
        }

        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        object[] row = Assert.Single(await reader.Rows(TableName, cancellationToken: ct).ToListAsync(ct));
        Assert.Equal(original, row);
    }

    private static int MaxRowLength(DatabaseFormat format) => format == DatabaseFormat.Jet3Mdb ? 2036 : 4080;

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

    private static async Task<MemoryStream> CreateDatabaseAsync(DatabaseFormat format, int oleColumns, CancellationToken cancellationToken)
    {
        var columns = new List<ColumnDefinition> { new("Id", typeof(int)) };
        for (int i = 0; i < oleColumns; i++)
        {
            columns.Add(new ColumnDefinition($"Blob{i}", typeof(byte[])));
        }

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
