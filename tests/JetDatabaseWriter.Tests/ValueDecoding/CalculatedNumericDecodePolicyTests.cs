namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
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

#pragma warning disable CA1812 // Public row materialization constructs the projection targets.

/// <summary>Calculated Decimal mantissa bounds follow the public decode-fault policy.</summary>
public sealed class CalculatedNumericDecodePolicyTests
{
    [Theory]
    [InlineData((short)14, 4, true)]
    [InlineData((short)14, 4, false)]
    [InlineData((short)14, 5, true)]
    [InlineData((short)14, 5, false)]
    [InlineData((short)14, 7, true)]
    [InlineData((short)14, 7, false)]
    [InlineData((short)18, 16, true)]
    [InlineData((short)18, 16, false)]
    [InlineData((short)10, 8, true)]
    [InlineData((short)10, 8, false)]
    [InlineData((short)6, 7, true)]
    [InlineData((short)6, 7, false)]
    public async Task MalformedMantissaLength_PublicReadsAgree(short storedLength, int payloadLength, bool strict)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream original = await CreateDatabaseAsync(42m, ct);
        byte[] bytes = original.ToArray();
        await RewriteNumericPayloadAsync(original, bytes, storedLength, payloadLength, ct);

        await using var traceOutput = new StringWriter(CultureInfo.InvariantCulture);
        using var listener = new TextWriterTraceListener(traceOutput);
        var traceScope = new TestTraceScope(listener);
        try
        {
            await using var stream = new MemoryStream(bytes, writable: false);
            await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ct);
            if (strict)
            {
                JetCorruptDataException strings = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.RowsAsStrings("Values", cancellationToken: ct).ToListAsync(ct));
                JetCorruptDataException typed = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows("Values", cancellationToken: ct).ToListAsync(ct));
                JetCorruptDataException poco = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows<ValueRow>("Values", cancellationToken: ct).ToListAsync(ct));
                JetCorruptDataException hybrid = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows<HybridValueRow>("Values", cancellationToken: ct).ToListAsync(ct));
                JetCorruptDataException table = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.ReadTableAsync("Values", cancellationToken: ct));
                Assert.Equal(JetErrorCode.MalformedValue, strings.ErrorCode);
                Assert.Equal("Value", strings.ErrorInfo.ColumnName);
                Assert.IsType<ArgumentException>(strings.InnerException, exactMatch: false);
                Assert.Contains("Value", strings.Message, StringComparison.Ordinal);
                Assert.Equal(strings.Message, typed.Message);
                Assert.Equal(typed.Message, poco.Message);
                Assert.Equal(poco.Message, hybrid.Message);
                Assert.Equal(typed.Message, table.Message);
            }
            else
            {
                Assert.Equal(string.Empty, Assert.Single(await reader.RowsAsStrings("Values", cancellationToken: ct).ToListAsync(ct))[0]);
                Assert.Equal(DBNull.Value, Assert.Single(await reader.Rows("Values", cancellationToken: ct).ToListAsync(ct))[0]);
                Assert.Null(Assert.Single(await reader.Rows<ValueRow>("Values", cancellationToken: ct).ToListAsync(ct)).Value);
                HybridValueRow hybrid = Assert.Single(await reader.Rows<HybridValueRow>("Values", cancellationToken: ct).ToListAsync(ct));
                Assert.Null(hybrid.Value);
                Assert.Equal(new byte[] { 1, 2, 3 }, hybrid.Payload);
                Assert.Equal(DBNull.Value, Assert.Single((await reader.ReadTableAsync("Values", cancellationToken: ct)).Rows.Cast<System.Data.DataRow>())[0]);
                string diagnostic = traceOutput.ToString();
                Assert.Contains("calculated", diagnostic, StringComparison.Ordinal);
                Assert.Contains("Decimal", diagnostic, StringComparison.Ordinal);
                Assert.Contains("Value", diagnostic, StringComparison.Ordinal);
                Assert.Contains("missing value", diagnostic, StringComparison.Ordinal);
                Assert.DoesNotContain("mantissa", diagnostic, StringComparison.Ordinal);
            }
        }
        finally
        {
            traceScope.Dispose();
        }

        await using var mutationStream = new MemoryStream();
        await mutationStream.WriteAsync(bytes, ct);
        mutationStream.Position = 0;
        await using AccessWriter writer = await AccessWriter.OpenAsync(mutationStream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await writer.UpdateRowsAsync("Values", RowCriteria.All(), new RowValues { ["Id"] = 2 }, ct));
        Assert.Equal(bytes, mutationStream.ToArray());
    }

    [Theory]
    [InlineData(4, true)]
    [InlineData(4, false)]
    [InlineData(8, true)]
    [InlineData(8, false)]
    public void AcceptedShortMantissa_ZeroExtendsToDecimal(int mantissaLength, bool strict)
    {
        byte[] payload = new byte[mantissaLength + 4];
        BinaryPrimitives.WriteInt16LittleEndian(payload, checked((short)(mantissaLength + 2)));
        Assert.Equal(0m, CalculatedColumnUtil.ReadPayloadTyped(payload, ColumnType.NumericType, strict));
        Assert.Equal("0", CalculatedColumnUtil.ReadPayloadString(payload, ColumnType.NumericType, strict));

        payload[2] = 2;
        payload[3] = 0x80;
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(4, 4), 42);
        Assert.Equal(-0.42m, CalculatedColumnUtil.ReadPayloadTyped(payload, ColumnType.NumericType, strict));
        Assert.Equal("-0.42", CalculatedColumnUtil.ReadPayloadString(payload, ColumnType.NumericType, strict));
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(false, true)]
    public async Task ZeroAndNullCachedNumeric_PublicReadsPreserveValues(bool strict, bool nullValue)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateDatabaseAsync(nullValue ? null : 0m, ct);
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ct);
        Assert.Equal(nullValue ? string.Empty : "0", Assert.Single(await reader.RowsAsStrings("Values", cancellationToken: ct).ToListAsync(ct))[0]);
        Assert.Equal(nullValue ? DBNull.Value : 0m, Assert.Single(await reader.Rows("Values", cancellationToken: ct).ToListAsync(ct))[0]);
        Assert.Equal(nullValue ? null : 0m, Assert.Single(await reader.Rows<ValueRow>("Values", cancellationToken: ct).ToListAsync(ct)).Value);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task NativeCalculatedNumericPayloads_PreserveZeroAndNonzeroValues(bool strict)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        decimal[] expected = [19230.7692307692m, -0.0192307692307692m, 0m, 1.92307692307692m];
        await using AccessReader reader = await AccessReader.OpenAsync(TestDatabases.CalcFieldTestV2010, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, ct);
        List<object[]> rows = await reader.Rows("Table1", cancellationToken: ct).ToListAsync(ct);
        List<string[]> strings = await reader.RowsAsStrings("Table1", cancellationToken: ct).ToListAsync(ct);
        List<NativeValueRow> pocos = await reader.Rows<NativeValueRow>("Table1", cancellationToken: ct).ToListAsync(ct);
        IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync("Table1", ct);
        ColumnMetadata weeklySalary = Assert.Single(columns, column => column.Name == "WeeklySalary");
        Assert.True(weeklySalary.IsCalculated);
        Assert.Equal((byte)ColumnType.NumericType, weeklySalary.CalculatedResultType);
        Assert.Equal(expected, rows.Select(row => Assert.IsType<decimal>(row[weeklySalary.Ordinal])));
        Assert.Equal(expected.Select(value => value.ToString("G", CultureInfo.InvariantCulture)), strings.Select(row => row[weeklySalary.Ordinal]));
        Assert.Equal(expected.Select(value => (decimal?)value), pocos.Select(row => row.WeeklySalary));

        await using AccessReader harness = await TestDatabase.OpenReaderAsync(TestDatabases.CalcFieldTestV2010, cancellationToken: ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Table1", ct));
        TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, ct));
        ColumnInfo column = Assert.IsType<ColumnInfo>(definition.FindColumn("WeeklySalary"));
        int payloadCount = 0;
        int zeroCount = 0;
        for (long number = 1; number < harness.Database.Pages.PageCount; number++)
        {
            byte[] page = await harness.ReadPageCopyAsync(number, ct);
            JetFormat profile = harness.Database.Format;
            if (page[0] != Constants.PageTypes.Data || JetTypeInfo.Ri32(page, profile.DataPage.TDefOff) != entry.TDefPage)
            {
                continue;
            }

            foreach (RowBound bound in DataPageRows.EnumerateLiveRowBounds(profile, page))
            {
                Assert.True(RowDecodePlan.TryParseRowLayout(profile.RowFields, page, bound.RowStart, bound.RowSize, definition.HasVarColumns, out RowLayout layout));
                ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(profile.RowFields, page, bound.RowStart, bound.RowSize, layout, column);
                Assert.Equal(ColumnSliceKind.Var, slice.Kind);
                byte[] payload = CalculatedColumnUtil.Unwrap(page.AsSpan(bound.RowStart + slice.DataStart, slice.DataLen));
                Assert.Equal(16, payload.Length);
                Assert.Equal(Constants.CalculatedColumn.ExtraDataLen + payload.Length, slice.DataLen);
                Assert.Equal(14, BinaryPrimitives.ReadInt16LittleEndian(payload));
                decimal value = Assert.IsType<decimal>(CalculatedColumnUtil.ReadPayloadTyped(payload, ColumnType.NumericType, strictNumeric: true));
                Assert.Equal(expected[payloadCount], value);
                if (value == 0)
                {
                    // The Access-authored zero retains all twelve mantissa bytes.
                    Assert.Equal("0E000000000000000000000000000000", Convert.ToHexString(payload));
                    zeroCount++;
                }

                payloadCount++;
            }
        }

        Assert.Equal(rows.Count, payloadCount);
        Assert.Equal(1, zeroCount);
    }

    private static async Task<MemoryStream> CreateDatabaseAsync(decimal? value, CancellationToken ct)
    {
        var stream = new MemoryStream();
        try
        {
            await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct);
            await writer.CreateTableAsync("Values", [new ColumnDefinition("Value", typeof(decimal)) { IsCalculated = true, CalculationExpression = value?.ToString(CultureInfo.InvariantCulture) ?? "Null" }, new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Payload", typeof(byte[]))], ct);
            await writer.InsertRowAsync("Values", [value, 1, new byte[] { 1, 2, 3 }], ct);
            stream.Position = 0;
            return stream;
        }
        catch
        {
            await stream.DisposeAsync();
            throw;
        }
    }

    private static async Task RewriteNumericPayloadAsync(MemoryStream original, byte[] bytes, short storedLength, int payloadLength, CancellationToken ct)
    {
        original.Position = 0;
        await using AccessReader harness = await TestDatabase.OpenReaderAsync(original, cancellationToken: ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Values", ct));
        TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, ct));
        ColumnInfo column = definition.Columns[0];
        for (long number = 1; number < harness.Database.Pages.PageCount; number++)
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
            int slotOffset = checked((int)(number * profile.PageSize)) + bound.RowStart + slice.DataStart;
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(slotOffset + Constants.CalculatedColumn.DataLenOffset, 4), payloadLength);
            BinaryPrimitives.WriteInt16LittleEndian(bytes.AsSpan(slotOffset + Constants.CalculatedColumn.DataOffset, 2), storedLength);
            return;
        }

        Assert.Fail("The created database must contain the calculated Numeric row.");
    }

    private sealed class HybridValueRow
    {
        public decimal? Value { get; set; }

        public byte[]? Payload { get; set; }
    }

    private sealed class NativeValueRow
    {
        public decimal? WeeklySalary { get; set; }
    }

    private sealed class ValueRow
    {
        public decimal? Value { get; set; }
    }
}
