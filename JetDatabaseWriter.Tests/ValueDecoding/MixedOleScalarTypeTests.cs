namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;
using static JetDatabaseWriter.Enums.ColumnType;

#pragma warning disable CA1812 // Compiled row mappers construct these projection targets.

/// <summary>Mixed OLE plans preserve generic JET/ACE scalar semantics and lossless widening.</summary>
public sealed class MixedOleScalarTypeTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task MixedScalarsAndOle_MatchProjectionWithNullsAndLosslessWidening(DatabaseFormat format)
    {
        byte[] payload = new byte[8500];
        for (int i = 0; i < payload.Length; i++)
        {
            payload[i] = unchecked((byte)(i * 29));
        }

        byte[] binary = [0, 0xFF, 0x80];
        var guid = Guid.Parse("12345678-9abc-def0-1234-56789abcdef0");
        var date = new DateTime(2021, 6, 14, 22, 45, 12, DateTimeKind.Unspecified);
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                "Values",
                [
                    new("ByteValue", typeof(byte)),
                    new("ShortValue", typeof(short)),
                    new("IntValue", typeof(int)),
                    new("FloatValue", typeof(float)),
                    new("DoubleValue", typeof(double)),
                    new("CurrencyValue", typeof(decimal)) { IsCurrency = true },
                    new("DateValue", typeof(DateTime)),
                    new("GuidValue", typeof(Guid)),
                    new("BoolValue", typeof(bool)),
                    new("TextValue", typeof(string), 100),
                    new("BinaryValue", typeof(byte[]), 30) { ColumnTypeOverride = BinaryType },
                    new("OleValue", typeof(byte[])),
                ],
                Ct);
            await writer.InsertRowsAsync(
                "Values",
                [
                    [(byte)255, short.MinValue, int.MaxValue, 123.25f, -456.125d, -12345.6789m, date, guid, true, "Bounded text", binary, payload],
                    [(byte)0, short.MaxValue, int.MinValue, -0.5f, double.MaxValue, 0.0001m, date.AddDays(-500), Guid.Empty, false, string.Empty, Array.Empty<byte>(), Array.Empty<byte>()],
                    [DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, false, DBNull.Value, DBNull.Value, DBNull.Value],
                ],
                Ct);
        }

        stream.Position = 0;
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: Ct))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Values", Ct));
            TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, Ct));
            Assert.NotNull(DirectRowDecoderBuilder.TryBuildHybrid<ScalarRow>(definition));
            Assert.NotNull(DirectRowDecoderBuilder.TryBuildHybrid<WidenedRow>(definition));
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, PageCacheSize = 1 }, leaveOpen: true, cancellationToken: Ct);
        List<ScalarRow> expected = await CollectAsync(Rows<ScalarRow>(reader, hybrid: false));
        List<ScalarRow> actual = await CollectAsync(Rows<ScalarRow>(reader, hybrid: true));
        Assert.Equal(3, actual.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equivalent(expected[i], actual[i], strict: true);
        }

        Assert.Equal((byte)255, actual[0].ByteValue);
        Assert.Equal(short.MinValue, actual[0].ShortValue);
        Assert.Equal(int.MaxValue, actual[0].IntValue);
        Assert.Equal(-12345.6789m, actual[0].CurrencyValue);
        Assert.Equal(date, actual[0].DateValue);
        Assert.Equal(guid, actual[0].GuidValue);
        Assert.Equal(payload, actual[0].OleValue);
        Assert.Null(actual[2].CurrencyValue);
        Assert.Null(actual[2].DateValue);
        Assert.Null(actual[2].GuidValue);
        Assert.Null(actual[2].OleValue);

        List<WidenedRow> widenedExpected = await CollectAsync(Rows<WidenedRow>(reader, hybrid: false));
        List<WidenedRow> widened = await CollectAsync(Rows<WidenedRow>(reader, hybrid: true));
        Assert.Equal(widenedExpected.Count, widened.Count);
        for (int i = 0; i < widened.Count; i++)
        {
            Assert.Equivalent(widenedExpected[i], widened[i], strict: true);
        }

        Assert.Equal(255L, widened[0].ByteValue);
        Assert.Equal((decimal)short.MinValue, widened[0].ShortValue);
        Assert.Equal((long)int.MaxValue, widened[0].IntValue);
        Assert.Equal(123.25d, widened[0].FloatValue);
        Assert.Equal(payload, widened[0].OleValue);
        Assert.Null(widened[2].ByteValue);
        Assert.Null(widened[2].ShortValue);
        Assert.Null(widened[2].IntValue);
        Assert.Null(widened[2].FloatValue);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    public async Task NumericAndOle_UnsupportedNumericPreservesStrictAndLenientFallback(DatabaseFormat format, bool strict)
    {
        byte[] payload = [0xFF, 0, 0x80, 12, 34];
        await using var original = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(original, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("Values", [new("Amount", typeof(decimal)) { NumericPrecision = 28, NumericScale = 2 }, new("OleValue", typeof(byte[]))], Ct);
            await writer.InsertRowAsync("Values", [123.45m, payload], Ct);
        }

        byte[] bytes = original.ToArray();
        original.Position = 0;
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(original, cancellationToken: Ct))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Values", Ct));
            TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, Ct));
            Assert.Null(DirectRowDecoderBuilder.TryBuildHybrid<NumericRow>(definition));
            ColumnInfo amount = definition.Columns.Single(column => column.Name == "Amount");
            bool corrupted = false;
            for (long number = 1; number < harness.Database.Pages.PageCount && !corrupted; number++)
            {
                byte[] page = await harness.ReadPageCopyAsync(number, Ct);
                JetFormat profile = harness.Database.Format;
                if (page[0] != Constants.PageTypes.Data || JetTypeInfo.Ri32(page, profile.DataPage.TDefOff) != entry.TDefPage)
                {
                    continue;
                }

                RowBound bound = Assert.Single(DataPageRows.EnumerateLiveRowBounds(profile, page));
                Assert.True(RowDecodePlan.TryParseRowLayout(profile.RowFields, page, bound.RowStart, bound.RowSize, definition.HasVarColumns, out RowLayout layout));
                ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(profile.RowFields, page, bound.RowStart, bound.RowSize, layout, amount);
                Assert.Equal(17, slice.DataLen);
                int offset = checked((int)(number * profile.PageSize)) + bound.RowStart + slice.DataStart;
                bytes.AsSpan(offset + 1, 16).Fill(0xFF);
                corrupted = true;
            }

            Assert.True(corrupted);
        }

        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: Ct);
        if (strict)
        {
            JetLimitationException expected = await Assert.ThrowsAsync<JetLimitationException>(async () => await CollectAsync(Rows<NumericRow>(reader, hybrid: false)));
            JetLimitationException actual = await Assert.ThrowsAsync<JetLimitationException>(async () => await CollectAsync(Rows<NumericRow>(reader, hybrid: true)));
            Assert.Equal(expected.ErrorCode, actual.ErrorCode);
            Assert.Equal(expected.Message, actual.Message);
        }
        else
        {
            NumericRow expected = Assert.Single(await CollectAsync(Rows<NumericRow>(reader, hybrid: false)));
            NumericRow actual = Assert.Single(await CollectAsync(Rows<NumericRow>(reader, hybrid: true)));
            Assert.Null(actual.Amount);
            Assert.Equivalent(expected, actual, strict: true);
            Assert.Equal(payload, actual.OleValue);
        }
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static IAsyncEnumerable<T> Rows<T>(AccessReader reader, bool hybrid)
        where T : class, new()
        => reader.ReadPrivateField<ReaderServices>("services").Tables.RowsWithDecoder<T>("Values", enableHybridOle: hybrid, forceProjection: !hybrid, progress: null, cancellationToken: Ct);

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> rows)
    {
        var result = new List<T>();
        await foreach (T row in rows)
        {
            result.Add(row);
        }

        return result;
    }

    [SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "OLE bindings preserve stored bytes.")]
    private sealed class NumericRow
    {
        public decimal? Amount { get; set; }

        public byte[]? OleValue { get; set; }
    }

    [SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "Binary and OLE bindings preserve stored bytes.")]
    private sealed class ScalarRow
    {
        public byte? ByteValue { get; set; }

        public short? ShortValue { get; set; }

        public int? IntValue { get; set; }

        public float? FloatValue { get; set; }

        public double? DoubleValue { get; set; }

        public decimal? CurrencyValue { get; set; }

        public DateTime? DateValue { get; set; }

        public Guid? GuidValue { get; set; }

        public bool? BoolValue { get; set; }

        public string? TextValue { get; set; }

        public byte[]? BinaryValue { get; set; }

        public byte[]? OleValue { get; set; }
    }

    [SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "OLE bindings preserve stored bytes.")]
    private sealed class WidenedRow
    {
        public long? ByteValue { get; set; }

        public decimal? ShortValue { get; set; }

        public long? IntValue { get; set; }

        public double? FloatValue { get; set; }

        public byte[]? OleValue { get; set; }
    }
}
