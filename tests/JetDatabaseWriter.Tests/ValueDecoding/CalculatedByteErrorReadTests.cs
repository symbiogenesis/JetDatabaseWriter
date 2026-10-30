namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;

#pragma warning disable CA1812 // Public row materialization constructs the projection target.

/// <summary>The measured native Byte error envelope must never become an ordinary Null.</summary>
public sealed class CalculatedByteErrorReadTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task NativeLayoutByteError_PublicReadsRefuseInsteadOfReturningNull(bool strict)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] envelope = new byte[39];
        BinaryPrimitives.WriteInt32LittleEndian(envelope, 6);
        await using MemoryStream stream = await WithEnvelopeAsync(envelope, ct);
        byte[] original = stream.ToArray();
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ct))
        {
            JetLimitationException typed = await Assert.ThrowsAsync<JetLimitationException>(async () => await reader.Rows("Values", cancellationToken: ct).ToListAsync(ct));
            JetLimitationException strings = await Assert.ThrowsAsync<JetLimitationException>(async () => await reader.RowsAsStrings("Values", cancellationToken: ct).ToListAsync(ct));
            JetLimitationException poco = await Assert.ThrowsAsync<JetLimitationException>(async () => await reader.Rows<ValueRow>("Values", cancellationToken: ct).ToListAsync(ct));
            JetLimitationException table = await Assert.ThrowsAsync<JetLimitationException>(async () => await reader.ReadTableAsync("Values", cancellationToken: ct));
            foreach (JetLimitationException error in new[] { typed, strings, poco, table })
            {
                Assert.Equal(JetErrorCode.NumericOverflow, error.ErrorCode);
                Assert.Equal("Value", error.ErrorInfo.ColumnName);
                Assert.Contains("native calculation error", error.Message, StringComparison.Ordinal);
                Assert.Equal(typed.Message, error.Message);
            }
        }

        stream.Position = 0;
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.UpdateRowsAsync("Values", RowCriteria.All(), new RowValues { ["Id"] = 2 }, ct));
        Assert.Equal(original, stream.ToArray());
    }

    [Theory]
    [InlineData(true, -1)]
    [InlineData(false, -1)]
    [InlineData(true, 0)]
    [InlineData(false, 0)]
    [InlineData(true, 255)]
    [InlineData(false, 255)]
    public async Task ByteAndNullControls_PublicReadsPreserveValues(bool strict, int value)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] envelope = CalculatedColumnUtil.Wrap(value < 0 ? [] : [checked((byte)value)]);
        await using MemoryStream stream = await WithEnvelopeAsync(envelope, ct);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ct);
        Assert.Equal(value < 0 ? DBNull.Value : checked((byte)value), Assert.Single(await reader.Rows("Values", cancellationToken: ct).ToListAsync(ct))[1]);
        Assert.Equal(value < 0 ? string.Empty : value.ToString(System.Globalization.CultureInfo.InvariantCulture), Assert.Single(await reader.RowsAsStrings("Values", cancellationToken: ct).ToListAsync(ct))[1]);
        Assert.Equal(value < 0 ? null : checked((byte)value), Assert.Single(await reader.Rows<ValueRow>("Values", cancellationToken: ct).ToListAsync(ct)).Value);
    }

    [Theory]
    [InlineData(true, true)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(false, false)]
    public async Task MalformedEnvelope_RemainsCorruptionRatherThanCalculationError(bool strict, bool truncated)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] envelope = new byte[truncated ? 22 : 39];
        BinaryPrimitives.WriteInt32LittleEndian(envelope, 6);
        BinaryPrimitives.WriteInt32LittleEndian(envelope.AsSpan(Constants.CalculatedColumn.DataLenOffset), truncated ? 0 : 40);
        await using MemoryStream stream = await WithEnvelopeAsync(envelope, ct);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ct);
        if (strict)
        {
            JetCorruptDataException typed = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows("Values", cancellationToken: ct).ToListAsync(ct));
            Assert.Equal(JetErrorCode.MalformedValue, typed.ErrorCode);
            Assert.Equal("Value", typed.ErrorInfo.ColumnName);
        }
        else
        {
            Assert.Equal(DBNull.Value, Assert.Single(await reader.Rows("Values", cancellationToken: ct).ToListAsync(ct))[1]);
        }
    }

    private static async Task<MemoryStream> WithEnvelopeAsync(byte[] envelope, CancellationToken ct)
    {
        await using var source = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(source, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct))
        {
            await writer.CreateTableAsync("Values", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Value", typeof(byte)) { IsCalculated = true, CalculationExpression = "0" }], ct);
            await writer.InsertRowAsync("Values", [1, (byte)0], ct);
        }

        byte[] bytes = source.ToArray();
        source.Position = 0;
        await using AccessReader harness = await TestDatabase.OpenReaderAsync(source, cancellationToken: ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Values", ct));
        TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, ct));
        ColumnInfo column = definition.Columns[1];
        JetFormat format = harness.Database.Format;
        for (long number = 1; number < harness.Database.Pages.PageCount; number++)
        {
            byte[] page = await harness.ReadPageCopyAsync(number, ct);
            if (page[0] != Constants.PageTypes.Data || JetTypeInfo.Ri32(page, format.DataPage.TDefOff) != entry.TDefPage)
            {
                continue;
            }

            RowBound bound = Assert.Single(DataPageRows.EnumerateLiveRowBounds(format, page));
            Assert.True(RowDecodePlan.TryParseRowLayout(format.RowFields, page, bound.RowStart, bound.RowSize, definition.HasVarColumns, out RowLayout layout));
            ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(format.RowFields, page, bound.RowStart, bound.RowSize, layout, column);
            Assert.Equal(slice.DataStart + slice.DataLen, layout.Eod);
            int delta = envelope.Length - slice.DataLen;
            byte[] row = new byte[bound.RowSize + delta];
            page.AsSpan(bound.RowStart, slice.DataStart).CopyTo(row);
            envelope.CopyTo(row, slice.DataStart);
            page.AsSpan(bound.RowStart + layout.Eod, bound.RowSize - layout.Eod).CopyTo(row.AsSpan(layout.Eod + delta));
            JetTypeInfo.Wu16(row, layout.Eod + delta, layout.Eod + delta);
            int newStart = format.PageSize - row.Length;
            row.CopyTo(page, newStart);
            JetTypeInfo.Wu16(page, format.DataPage.RowsStart + (bound.RowIndex * 2), newStart);
            JetTypeInfo.Wu16(page, 2, JetTypeInfo.Ru16(page, 2) - delta);
            page.CopyTo(bytes, checked((int)(number * format.PageSize)));
            return new MemoryStream(bytes);
        }

        throw new InvalidOperationException("The created database must contain its calculated Byte row.");
    }

    private sealed class ValueRow
    {
        public byte? Value { get; set; }
    }
}
