namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
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

#pragma warning disable CA1812 // Public row materialization constructs the projection targets.

/// <summary>Fixed column slices must not read row trailers or collapse corrupt assigned values to null.</summary>
public sealed class FixedSlotBoundsDecodePolicyTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, true, true, true)]
    [InlineData(DatabaseFormat.Jet3Mdb, true, false, true)]
    [InlineData(DatabaseFormat.Jet3Mdb, false, true, true)]
    [InlineData(DatabaseFormat.Jet3Mdb, false, false, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true, true, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true, false, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, true, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false, true)]
    [InlineData(DatabaseFormat.AceAccdb, true, true, true)]
    [InlineData(DatabaseFormat.AceAccdb, true, false, true)]
    [InlineData(DatabaseFormat.AceAccdb, false, true, true)]
    [InlineData(DatabaseFormat.AceAccdb, false, false, true)]
    [InlineData(DatabaseFormat.Jet3Mdb, true, true, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true, false, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true, true, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true, false, false)]
    [InlineData(DatabaseFormat.AceAccdb, true, true, false)]
    [InlineData(DatabaseFormat.AceAccdb, true, false, false)]
    public async Task OutOfPayloadFixedOffset_PublicReadsAgree(DatabaseFormat format, bool intoTrailer, bool strict, bool withPayload)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var original = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(original, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct))
        {
            ColumnDefinition[] columns = withPayload
                ? [new ColumnDefinition("Value", typeof(int)), new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Payload", typeof(byte[]))]
                : [new ColumnDefinition("Value", typeof(int)), new ColumnDefinition("Id", typeof(int))];
            await writer.CreateTableAsync("Values", columns, ct);
            await writer.InsertRowAsync("Values", withPayload ? [42, 1, new byte[] { 1, 2, 3 }] : [42, 1], ct);
        }

        byte[] bytes = original.ToArray();
        original.Position = 0;
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(original, cancellationToken: ct))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Values", ct));
            TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, ct));
            Assert.NotNull(DirectRowDecoderBuilder.TryBuild<ValueRow>(withPayload ? ["Value", "Id", "Payload"] : ["Value", "Id"], definition.Columns, definition.ClrTypes));
            if (withPayload)
            {
                Assert.NotNull(DirectRowDecoderBuilder.TryBuildHybrid<HybridValueRow>(["Value", "Id", "Payload"], definition.Columns, definition.ClrTypes));
            }

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
                int fixedOffset = intoTrailer ? layout.Eod - profile.RowFields.NumCols - 3 : bound.RowSize;
                int descriptorOffset = checked((int)(entry.TDefPage * profile.PageSize)) + profile.TDef.BlockEnd + profile.ColumnDescriptor.FixedOff;
                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(descriptorOffset, 2), checked((ushort)fixedOffset));
                damaged = true;
            }

            Assert.True(damaged);
        }

        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ct);
        if (strict)
        {
            JetCorruptDataException strings = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CollectAsync(reader.RowsAsStrings("Values", cancellationToken: ct)));
            JetCorruptDataException typed = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CollectAsync(reader.Rows("Values", cancellationToken: ct)));
            JetCorruptDataException poco = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CollectAsync(reader.Rows<ValueRow>("Values", cancellationToken: ct)));
            if (withPayload)
            {
                JetCorruptDataException hybrid = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CollectAsync(reader.Rows<HybridValueRow>("Values", cancellationToken: ct)));
                Assert.Equal(strings.Message, hybrid.Message);
            }

            JetCorruptDataException table = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.ReadTableAsync("Values", cancellationToken: ct));
            Assert.Equal(JetErrorCode.MalformedValue, strings.ErrorCode);
            Assert.Equal("Value", strings.ErrorInfo.ColumnName);
            Assert.IsType<ArgumentException>(strings.InnerException);
            Assert.Equal(strings.Message, typed.Message);
            Assert.Equal(strings.Message, poco.Message);
            Assert.Equal(strings.Message, table.Message);
        }
        else
        {
            Assert.Equal(string.Empty, Assert.Single(await CollectAsync(reader.RowsAsStrings("Values", cancellationToken: ct)))[0]);
            Assert.Equal(DBNull.Value, Assert.Single(await CollectAsync(reader.Rows("Values", cancellationToken: ct)))[0]);
            Assert.Null(Assert.Single(await CollectAsync(reader.Rows<ValueRow>("Values", cancellationToken: ct))).Value);
            if (withPayload)
            {
                HybridValueRow hybrid = Assert.Single(await CollectAsync(reader.Rows<HybridValueRow>("Values", cancellationToken: ct)));
                Assert.Null(hybrid.Value);
                Assert.Equal(new byte[] { 1, 2, 3 }, hybrid.Payload);
            }

            using System.Data.DataTable table = await reader.ReadTableAsync("Values", cancellationToken: ct);
            Assert.Equal(DBNull.Value, table.Rows[0][0]);
        }

        await using var mutationStream = new MemoryStream();
        await mutationStream.WriteAsync(bytes, ct);
        mutationStream.Position = 0;
        await using AccessWriter mutation = await AccessWriter.OpenAsync(mutationStream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await mutation.UpdateRowsAsync("Values", RowCriteria.All(), new RowValues { ["Id"] = 2 }, ct));
        Assert.Equal(bytes, mutationStream.ToArray());
    }

    [Theory]
    [InlineData(ColumnType.ByteType)]
    [InlineData(ColumnType.IntegerType)]
    [InlineData(ColumnType.LongIntegerType)]
    [InlineData(ColumnType.MoneyType)]
    [InlineData(ColumnType.FloatType)]
    [InlineData(ColumnType.DoubleType)]
    [InlineData(ColumnType.DateTimeType)]
    [InlineData(ColumnType.GuidType)]
    [InlineData(ColumnType.NumericType)]
    [InlineData(ColumnType.BigBinaryType)]
    [InlineData(ColumnType.ComplexType)]
    [InlineData(ColumnType.BigIntType)]
    [InlineData(ColumnType.DateTimeExtendedType)]
    [InlineData(ColumnType.TextType)]
    [InlineData(ColumnType.BinaryType)]
    public void FixedSlotCrossingTrailer_IsUnreadableButNullIsPreserved(ColumnType type)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        int size = type is ColumnType.TextType or ColumnType.BinaryType ? 4 : JetTypeInfo.GetFixedSize(type);
        int payloadEnd = size + 16;
        int rowSize = payloadEnd + 8;
        var column = new ColumnInfo { Name = "Value", Type = type, Flags = Constants.ColumnDescriptorFlags.Fixed, Size = size, FixedOff = payloadEnd - format.RowFields.NumCols - size + 1 };
        byte[] page = new byte[rowSize];
        var layout = new RowLayout(1, rowSize - 1, 0, rowSize - 1, payloadEnd);
        Assert.Equal(ColumnSliceKind.Null, RowDecodePlan.ResolveColumnSlice(format.RowFields, page, 0, rowSize, layout, column).Kind);
        page[^1] = 1;
        Assert.Equal(ColumnSliceKind.Empty, RowDecodePlan.ResolveColumnSlice(format.RowFields, page, 0, rowSize, layout, column).Kind);
        ColumnSlice valid = RowDecodePlan.ResolveColumnSlice(format.RowFields, page, 0, rowSize, layout, column with { FixedOff = column.FixedOff - 1 });
        Assert.Equal(size, valid.DataLen);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void FixedSlotPointingIntoVariableData_IsUnreadable(DatabaseFormat databaseFormat)
    {
        var format = JetFormat.ForNewDatabase(databaseFormat);
        var column = new ColumnInfo { Name = "Value", Type = ColumnType.LongIntegerType, Flags = Constants.ColumnDescriptorFlags.Fixed, FixedOff = 8 - format.RowFields.NumCols };
        byte[] page = new byte[24];
        var layout = new RowLayout(2, 23, 1, 18, 16);
        page[23] = 3;
        page[18] = 8;
        Assert.Equal(ColumnSliceKind.Empty, RowDecodePlan.ResolveColumnSlice(format.RowFields, page, 0, page.Length, layout, column).Kind);
        Assert.Equal(ColumnSliceKind.Fixed, RowDecodePlan.ResolveColumnSlice(format.RowFields, page, 0, page.Length, layout, column with { FixedOff = column.FixedOff - 4 }).Kind);
    }

    [Fact]
    public void FixedCalculatedEnvelope_CannotConsumeTrailerBytes()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        const int envelopeSize = Constants.CalculatedColumn.DataOffset + 4;
        var column = new ColumnInfo
        {
            Name = "Value",
            Type = ColumnType.LongIntegerType,
            Flags = Constants.ColumnDescriptorFlags.Fixed,
            ExtraFlags = Constants.CalculatedColumn.ExtFlagMask,
            Size = envelopeSize,
            FixedOff = 5,
        };
        byte[] page = new byte[envelopeSize + 7];
        page[^1] = 1;
        var layout = new RowLayout(1, page.Length - 1, 0, page.Length - 1, page.Length - 1);
        Assert.Equal(ColumnSliceKind.Empty, RowDecodePlan.ResolveColumnSlice(format.RowFields, page, 0, page.Length, layout, column).Kind);
        Assert.Equal(ColumnSliceKind.Var, RowDecodePlan.ResolveColumnSlice(format.RowFields, page, 0, page.Length, layout, column with { FixedOff = 4 }).Kind);
    }

    [Fact(DisableParallelization = true)]
    public void LenientFixedBounds_EmitDiagnosticWithoutValues()
    {
        using var output = new StringWriter(CultureInfo.InvariantCulture);
        using var listener = new TextWriterTraceListener(output);
        Trace.Listeners.Add(listener);
        try
        {
            var definition = new TableDef { Columns = [new ColumnInfo { Name = "Value", Type = ColumnType.LongIntegerType, Flags = Constants.ColumnDescriptorFlags.Fixed, FixedOff = 100 }] };
            var plan = RowDecodePlan.CreateTyped(definition, wantedColumns: null, strictParsing: false);
            byte[] page = [1, 0, 0, 0, 0, 0, 1];
            object?[] values = new object?[1];
            Assert.True(plan.TryDecodeTypedIntoBuffer(JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb), page, 0, page.Length, null!, values, out _));
            Assert.Equal(DBNull.Value, values[0]);
            Assert.Contains("Value", output.ToString(), StringComparison.Ordinal);
            Assert.Contains("missing value", output.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            Trace.Listeners.Remove(listener);
        }
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

    private sealed class ValueRow
    {
        public int? Value { get; set; }
    }

    private sealed class HybridValueRow
    {
        public int? Value { get; set; }

        public byte[]? Payload { get; set; }
    }
}
