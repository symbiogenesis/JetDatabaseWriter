namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
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

/// <summary>Malformed fixed dates have the same failure policy on public read surfaces.</summary>
public sealed class FixedDateDecodePolicyTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    public async Task MalformedFixedDate_PublicReadsAgree(DatabaseFormat format, bool strict)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var original = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(original, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct))
        {
            await writer.CreateTableAsync("Values", [new ColumnDefinition("Value", typeof(DateTime)), new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Payload", typeof(byte[]))], ct);
            await writer.InsertRowAsync("Values", [new DateTime(2026, 1, 1), 1, new byte[] { 1, 2, 3 }], ct);
        }

        byte[] bytes = original.ToArray();
        original.Position = 0;
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(original, cancellationToken: ct))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Values", ct));
            TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, ct));
            ColumnInfo column = definition.Columns[0];
            Assert.NotNull(DirectRowDecoderBuilder.TryBuildHybrid<HybridValueRow>(["Value", "Id", "Payload"], definition.Columns, definition.ClrTypes));
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
                Assert.Equal(ColumnSliceKind.Fixed, slice.Kind);
                int rowOffset = checked((int)(number * profile.PageSize)) + bound.RowStart;
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(rowOffset + slice.DataStart, 8), BitConverter.DoubleToInt64Bits(double.NaN));
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
            Assert.Equal(poco.Message, hybrid.Message);
            Assert.Equal(JetErrorCode.MalformedValue, strings.ErrorCode);
            Assert.Equal("Value", strings.ErrorInfo.ColumnName);
            Assert.IsType<ArgumentException>(strings.InnerException, exactMatch: false);
            Assert.Contains("Value", strings.Message, StringComparison.Ordinal);
            Assert.Equal(strings.Message, typed.Message);
            Assert.Equal(typed.Message, poco.Message);
        }
        else
        {
            Assert.Equal(string.Empty, Assert.Single(await CollectAsync(reader.RowsAsStrings("Values", cancellationToken: ct)))[0]);
            Assert.Equal(DBNull.Value, Assert.Single(await CollectAsync(reader.Rows("Values", cancellationToken: ct)))[0]);
            Assert.Null(Assert.Single(await CollectAsync(reader.Rows<ValueRow>("Values", cancellationToken: ct))).Value);
            HybridValueRow hybrid = Assert.Single(await CollectAsync(reader.Rows<HybridValueRow>("Values", cancellationToken: ct)));
            Assert.Null(hybrid.Value);
            Assert.Equal(new byte[] { 1, 2, 3 }, hybrid.Payload);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ValidDatesAndNull_PublicReadsAgree(bool strict)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        DateTime[] dates = [DateTime.FromOADate(-657434), DateTime.FromOADate(-1.25), DateTime.FromOADate(2958465)];
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct))
        {
            await writer.CreateTableAsync("Values", [new ColumnDefinition("Value", typeof(DateTime))], ct);
            foreach (DateTime date in dates)
            {
                await writer.InsertRowAsync("Values", [date], ct);
            }

            await writer.InsertRowAsync("Values", [null], ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ct);
        List<object[]> typed = await CollectAsync(reader.Rows("Values", cancellationToken: ct));
        List<string[]> strings = await CollectAsync(reader.RowsAsStrings("Values", cancellationToken: ct));
        List<ValueRow> pocos = await CollectAsync(reader.Rows<ValueRow>("Values", cancellationToken: ct));
        for (int index = 0; index < dates.Length; index++)
        {
            Assert.Equal(dates[index], typed[index][0]);
            Assert.Equal(dates[index], pocos[index].Value);
            Assert.Equal(dates[index].ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture), strings[index][0]);
        }

        Assert.Equal(DBNull.Value, typed[3][0]);
        Assert.Equal(string.Empty, strings[3][0]);
        Assert.Null(pocos[3].Value);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RealIoFailure_PublicReadsPropagate(bool strict)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var original = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(original, DatabaseFormat.Jet4Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct))
        {
            await writer.CreateTableAsync("Values", [new ColumnDefinition("Value", typeof(DateTime))], ct);
            await writer.InsertRowAsync("Values", [new DateTime(2026, 1, 1)], ct);
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

    [Fact(DisableParallelization = true)]
    public void LenientDateFault_EmitsDiagnosticAndToleratesListenerFailure()
    {
        using var output = new StringWriter(System.Globalization.CultureInfo.InvariantCulture);
        using var listener = new TextWriterTraceListener(output);
        Trace.Listeners.Add(listener);
        try
        {
            Assert.Equal(DBNull.Value, RowValueDecodePolicy.MalformedFixedDate(new ColumnInfo { Name = "Value" }, new ArgumentException("bad date"), strictParsing: false));
            Assert.Contains("Value", output.ToString(), StringComparison.Ordinal);
            Assert.Contains("missing value", output.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            Trace.Listeners.Remove(listener);
        }

        using var throwing = new ThrowingTraceListener();
        Trace.Listeners.Add(throwing);
        try
        {
            Assert.Equal(DBNull.Value, RowValueDecodePolicy.MalformedFixedDate(new ColumnInfo { Name = "Value" }, new ArgumentException("bad date"), strictParsing: false));
        }
        finally
        {
            Trace.Listeners.Remove(throwing);
        }
    }

    private sealed class ThrowingTraceListener : TraceListener
    {
        public override void Write(string? message) => throw new IOException("diagnostic failure");

        public override void WriteLine(string? message) => throw new IOException("diagnostic failure");
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
        public DateTime? Value { get; set; }

        public byte[]? Payload { get; set; }
    }

    private sealed class ValueRow
    {
        public DateTime? Value { get; set; }
    }
}
