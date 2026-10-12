namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
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

/// <summary>Malformed variable values have the same failure policy on public read surfaces.</summary>
public sealed class RowDecodeParityTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false, true, false, 1)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false, false, false, 1)]
    [InlineData(DatabaseFormat.AceAccdb, false, false, true, false, 1)]
    [InlineData(DatabaseFormat.AceAccdb, false, false, false, false, 1)]
    [InlineData(DatabaseFormat.AceAccdb, true, false, true, false, 1)]
    [InlineData(DatabaseFormat.AceAccdb, true, false, false, false, 1)]
    [InlineData(DatabaseFormat.AceAccdb, true, true, true, false, 1)]
    [InlineData(DatabaseFormat.AceAccdb, true, true, false, false, 1)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false, true, true, 1)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false, false, true, 1)]
    [InlineData(DatabaseFormat.AceAccdb, false, false, true, true, 1)]
    [InlineData(DatabaseFormat.AceAccdb, false, false, false, true, 1)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false, true, false, 0)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false, false, false, 0)]
    [InlineData(DatabaseFormat.AceAccdb, false, false, true, false, 0)]
    [InlineData(DatabaseFormat.AceAccdb, false, false, false, false, 0)]
    [InlineData(DatabaseFormat.AceAccdb, true, false, true, false, 0)]
    [InlineData(DatabaseFormat.AceAccdb, true, false, false, false, 0)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false, true, false, -1)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false, false, false, -1)]
    [InlineData(DatabaseFormat.AceAccdb, false, false, true, false, -1)]
    [InlineData(DatabaseFormat.AceAccdb, false, false, false, false, -1)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false, true, true, -1)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false, false, true, -1)]
    [InlineData(DatabaseFormat.AceAccdb, false, false, true, true, -1)]
    [InlineData(DatabaseFormat.AceAccdb, false, false, false, true, -1)]
    [InlineData(DatabaseFormat.AceAccdb, true, false, true, false, -1)]
    [InlineData(DatabaseFormat.AceAccdb, true, false, false, false, -1)]
    public async Task MalformedPayload_PublicReadsAgree(DatabaseFormat format, bool calculated, bool invalidEnvelope, bool strict, bool numeric, int slotLength)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var original = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(original, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct))
        {
            ColumnDefinition column = calculated
                ? new("Value", typeof(int)) { IsCalculated = true, CalculationExpression = "42" }
                : new("Value", numeric ? typeof(decimal) : typeof(int)) { ForceVariableLengthStorage = true };
            await writer.CreateTableAsync("Values", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Payload", typeof(byte[])), column], ct);
            object value = numeric ? 42m : 42;
            await writer.InsertRowAsync("Values", [1, new byte[] { 1, 2, 3 }, calculated ? null : value], ct);
        }

        byte[] bytes = original.ToArray();
        original.Position = 0;
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(original, cancellationToken: ct))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Values", ct));
            TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, ct));
            ColumnInfo column = definition.Columns[2];
            bool damaged = false;
            for (long number = 1; number < harness.Database.Pages.PageCount && !damaged; number++)
            {
                byte[] page = await harness.ReadPageCopyAsync(number, ct);
                JetFormat profile = harness.Database.Format;
                if (page[0] != Constants.PageTypes.Data || JetTypeInfo.Ri32(page, profile.DataPage.TDefOff) != entry.TDefPage)
                {
                    continue;
                }

                RowBound bound = Assert.Single(DataPageRows.EnumerateLiveRowBounds(profile, page));
                Assert.True(RowDecodePlan.TryParseRowLayout(profile.RowFields, page, bound.RowStart, bound.RowSize, definition.HasVarColumns, out RowLayout layout));
                ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(profile.RowFields, page, bound.RowStart, bound.RowSize, layout, column);
                Assert.Equal(ColumnSliceKind.Var, slice.Kind);
                int rowOffset = checked((int)(number * profile.PageSize)) + bound.RowStart;
                if (calculated && slotLength > 0)
                {
                    BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(rowOffset + slice.DataStart + Constants.CalculatedColumn.DataLenOffset, 4), invalidEnvelope ? int.MaxValue : 1);
                }
                else
                {
                    BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(rowOffset + layout.VarTableStart - profile.RowFields.Eod, 2), checked((ushort)(slice.DataStart + slotLength)));
                }

                damaged = true;
            }

            Assert.True(damaged);
        }

        await using (var mutationStream = new MemoryStream())
        {
            await mutationStream.WriteAsync(bytes, ct);
            mutationStream.Position = 0;
            await using AccessWriter writer = await AccessWriter.OpenAsync(mutationStream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct);
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await writer.UpdateRowsAsync("Values", RowCriteria.All(), new RowValues { ["Id"] = 2 }, ct));
            Assert.Equal(bytes, mutationStream.ToArray());
        }

        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ct);
        if (strict)
        {
            JetCorruptDataException strings = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CollectAsync(reader.RowsAsStrings("Values", cancellationToken: ct)));
            JetCorruptDataException typed = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CollectAsync(reader.Rows("Values", cancellationToken: ct)));
            JetCorruptDataException poco = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CollectAsync(reader.Rows<ValueRow>("Values", cancellationToken: ct)));
            JetCorruptDataException hybrid = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CollectAsync(reader.Rows<HybridValueRow>("Values", cancellationToken: ct)));
            JetCorruptDataException table = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.ReadTableAsync("Values", cancellationToken: ct));
            Assert.Equal(typed.Message, hybrid.Message);
            Assert.Equal(typed.Message, table.Message);
            Assert.Equal(JetErrorCode.MalformedValue, strings.ErrorCode);
            Assert.Equal("Value", strings.ErrorInfo.ColumnName);
            Assert.IsType<ArgumentException>(strings.InnerException, exactMatch: false);
            Assert.Contains("Value", strings.Message, StringComparison.Ordinal);
            Assert.Equal(strings.Message, typed.Message);
            Assert.Equal(typed.Message, poco.Message);
        }
        else
        {
            Assert.Equal(string.Empty, Assert.Single(await CollectAsync(reader.RowsAsStrings("Values", cancellationToken: ct)))[2]);
            Assert.Equal(DBNull.Value, Assert.Single(await CollectAsync(reader.Rows("Values", cancellationToken: ct)))[2]);
            Assert.Null(Assert.Single(await CollectAsync(reader.Rows<ValueRow>("Values", cancellationToken: ct))).Value);
            HybridValueRow hybrid = Assert.Single(await CollectAsync(reader.Rows<HybridValueRow>("Values", cancellationToken: ct)));
            Assert.Null(hybrid.Value);
            Assert.Equal(new byte[] { 1, 2, 3 }, hybrid.Payload);
            Assert.Equal(DBNull.Value, Assert.Single((await reader.ReadTableAsync("Values", cancellationToken: ct)).Rows.Cast<System.Data.DataRow>())[2]);
        }
    }

    [Theory]
    [InlineData(false, true)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(true, false)]
    public async Task ValidScalarNull_PublicReadsPreserveMissingValue(bool calculated, bool strict)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct))
        {
            await writer.CreateTableAsync("Values", [new ColumnDefinition("Value", typeof(int)) { ForceVariableLengthStorage = true, IsCalculated = calculated, CalculationExpression = calculated ? "Null" : null }], ct);
            await writer.InsertRowAsync("Values", [null], ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ct);
        Assert.Equal(string.Empty, Assert.Single(await CollectAsync(reader.RowsAsStrings("Values", cancellationToken: ct)))[0]);
        Assert.Equal(DBNull.Value, Assert.Single(await CollectAsync(reader.Rows("Values", cancellationToken: ct)))[0]);
        Assert.Null(Assert.Single(await CollectAsync(reader.Rows<ValueRow>("Values", cancellationToken: ct))).Value);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RealIoFailure_PublicReadsPropagate(bool strict)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var original = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(original, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct))
        {
            await writer.CreateTableAsync("Values", [new ColumnDefinition("Value", typeof(int)) { ForceVariableLengthStorage = true }], ct);
            await writer.InsertRowAsync("Values", [42], ct);
        }

        await using var stream = new FailingReadStream(original.ToArray());
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ct);
        stream.FailReads = true;
        Assert.Same(stream.Failure, await Assert.ThrowsAsync<IOException>(async () => await CollectAsync(reader.RowsAsStrings("Values", cancellationToken: ct))));
        Assert.Same(stream.Failure, await Assert.ThrowsAsync<IOException>(async () => await CollectAsync(reader.Rows("Values", cancellationToken: ct))));
        Assert.Same(stream.Failure, await Assert.ThrowsAsync<IOException>(async () => await CollectAsync(reader.Rows<ValueRow>("Values", cancellationToken: ct))));
    }

    private sealed class FailingReadStream(byte[] bytes) : MemoryStream(bytes, writable: false)
    {
        internal IOException Failure { get; } = new("disk failure");

        internal bool FailReads { get; set; }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => this.FailReads ? ValueTask.FromException<int>(this.Failure) : base.ReadAsync(buffer, cancellationToken);
    }

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> rows)
    {
        var result = new List<T>();
        await foreach (T row in rows)
        {
            result.Add(row);
        }

        return result;
    }

    private sealed class HybridValueRow
    {
        public decimal? Value { get; set; }

        public byte[]? Payload { get; set; }
    }

    private sealed class ValueRow
    {
        public decimal? Value { get; set; }
    }
}
