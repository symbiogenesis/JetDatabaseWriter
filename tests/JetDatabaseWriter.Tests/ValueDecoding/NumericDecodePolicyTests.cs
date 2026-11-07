namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
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

/// <summary>
/// Stored Decimal values that Access cannot author follow the public decode-fault
/// policy: strict reads report contextual corruption and explicitly lenient reads
/// return a diagnosed missing value on every public surface.
/// </summary>
public sealed class NumericDecodePolicyTests
{
    /// <summary>The kind of stored Decimal damage.</summary>
    [SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "Theory parameters of public xUnit test methods must be public.")]
    public enum NumericFault
    {
        /// <summary>A fixed 17-byte slot whose coefficient exceeds 96 bits.</summary>
        FixedCoefficient = 0,

        /// <summary>A variable 17-byte slot whose coefficient exceeds 96 bits.</summary>
        VariableCoefficient = 1,

        /// <summary>A cached calculated Decimal whose scale byte exceeds 28.</summary>
        CalculatedScale = 2,
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, NumericFault.FixedCoefficient, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, NumericFault.FixedCoefficient, false)]
    [InlineData(DatabaseFormat.AceAccdb, NumericFault.FixedCoefficient, true)]
    [InlineData(DatabaseFormat.AceAccdb, NumericFault.FixedCoefficient, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, NumericFault.VariableCoefficient, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, NumericFault.VariableCoefficient, false)]
    [InlineData(DatabaseFormat.AceAccdb, NumericFault.VariableCoefficient, true)]
    [InlineData(DatabaseFormat.AceAccdb, NumericFault.VariableCoefficient, false)]
    [InlineData(DatabaseFormat.AceAccdb, NumericFault.CalculatedScale, true)]
    [InlineData(DatabaseFormat.AceAccdb, NumericFault.CalculatedScale, false)]
    public async Task UnrepresentableStoredDecimal_PublicReadsAgree(DatabaseFormat format, NumericFault fault, bool strict)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] bytes = await CreateDamagedDatabaseAsync(format, fault, ct);

        await using (var mutationStream = new MemoryStream())
        {
            await mutationStream.WriteAsync(bytes, ct);
            mutationStream.Position = 0;
            await using AccessWriter writer = await AccessWriter.OpenAsync(mutationStream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct);
            JetCorruptDataException refused = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await writer.UpdateRowsAsync("Values", RowCriteria.All(), new RowValues { ["Id"] = 2 }, ct));
            Assert.Equal(JetErrorCode.MalformedValue, refused.ErrorCode);
            Assert.Equal(bytes, mutationStream.ToArray());
        }

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
                JetCorruptDataException wide = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows<WideValueRow>("Values", cancellationToken: ct).ToListAsync(ct));
                JetCorruptDataException projected = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Services.Tables.RowsWithDecoder<WideValueRow>("Values", forceProjection: true, progress: null, cancellationToken: ct).ToListAsync(ct));
                JetCorruptDataException table = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.ReadTableAsync("Values", cancellationToken: ct));
                foreach (JetCorruptDataException failure in new[] { strings, typed, poco, wide, projected, table })
                {
                    Assert.Equal(JetErrorCode.MalformedValue, failure.ErrorCode);
                    Assert.Equal("Value", failure.ErrorInfo.ColumnName);
                    Assert.Contains("Value", failure.Message, StringComparison.Ordinal);
                    Assert.NotNull(failure.InnerException);
                }

                Assert.Equal(strings.Message, typed.Message);
                Assert.Equal(typed.Message, poco.Message);
                Assert.Equal(poco.Message, wide.Message);
                Assert.Equal(wide.Message, projected.Message);
                Assert.Equal(projected.Message, table.Message);
                Assert.Empty(traceOutput.ToString());
            }
            else
            {
                string[] strings = Assert.Single(await reader.RowsAsStrings("Values", cancellationToken: ct).ToListAsync(ct));
                Assert.Equal(string.Empty, strings[0]);
                Assert.Equal("1", strings[1]);
                object[] typed = Assert.Single(await reader.Rows("Values", cancellationToken: ct).ToListAsync(ct));
                Assert.Equal(DBNull.Value, typed[0]);
                Assert.Equal(1, typed[1]);
                Assert.Null(Assert.Single(await reader.Rows<ValueRow>("Values", cancellationToken: ct).ToListAsync(ct)).Value);
                foreach (WideValueRow row in new[]
                {
                    Assert.Single(await reader.Rows<WideValueRow>("Values", cancellationToken: ct).ToListAsync(ct)),
                    Assert.Single(await reader.Services.Tables.RowsWithDecoder<WideValueRow>("Values", forceProjection: true, progress: null, cancellationToken: ct).ToListAsync(ct)),
                })
                {
                    Assert.Null(row.Value);
                    Assert.Equal(1, row.Id);
                    Assert.Equal(new byte[] { 1, 2, 3 }, row.Payload);
                }

                System.Data.DataRow materialized = Assert.Single((await reader.ReadTableAsync("Values", cancellationToken: ct)).Rows.Cast<System.Data.DataRow>());
                Assert.Equal(DBNull.Value, materialized[0]);
                Assert.Equal(1, materialized[1]);

                string diagnostic = traceOutput.ToString();
                Assert.Contains("Value", diagnostic, StringComparison.Ordinal);
                Assert.Contains("Decimal", diagnostic, StringComparison.Ordinal);
                Assert.Contains("missing value", diagnostic, StringComparison.Ordinal);
                Assert.DoesNotContain("mantissa", diagnostic, StringComparison.Ordinal);
                Assert.DoesNotContain("exceeds", diagnostic, StringComparison.Ordinal);
            }
        }
        finally
        {
            traceScope.Dispose();
        }
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(false, true)]
    public async Task BoundaryDecimals_PublicReadsPreserveValues(bool strict, bool variable)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        decimal[] values = [0m, -1.5m, 9999999999999999999999999999m, -9999999999999999999999999999m];
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct))
        {
            await writer.CreateTableAsync("Values", [new ColumnDefinition("Value", typeof(decimal)) { NumericPrecision = 28, NumericScale = 0, ForceVariableLengthStorage = variable }], ct);
            foreach (decimal value in values)
            {
                await writer.InsertRowAsync("Values", [decimal.Truncate(value)], ct);
            }

            await writer.InsertRowAsync("Values", [null], ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ct);
        decimal?[] expected = [.. values.Select(value => (decimal?)decimal.Truncate(value)), null];
        Assert.Equal(expected, (await reader.Rows("Values", cancellationToken: ct).ToListAsync(ct)).Select(row => row[0] is DBNull ? (decimal?)null : (decimal)row[0]));
        Assert.Equal(expected, (await reader.Rows<ValueRow>("Values", cancellationToken: ct).ToListAsync(ct)).Select(row => row.Value));
        Assert.Equal(
            expected.Select(value => value?.ToString("G", CultureInfo.InvariantCulture) ?? string.Empty),
            (await reader.RowsAsStrings("Values", cancellationToken: ct).ToListAsync(ct)).Select(row => row[0]));
    }

    [Fact]
    public void StoredDecimalFaults_AreMalformedValues()
    {
        byte[] coefficient = new byte[17];
        coefficient.AsSpan(1).Fill(0xFF);
        ColumnInfo column = new() { Name = "Value", Type = ColumnType.NumericType, Flags = Constants.ColumnDescriptorFlags.Fixed, NumericPrecision = 28 };
        Assert.Equal(JetErrorCode.MalformedValue, Assert.Throws<JetCorruptDataException>(() => JetTypeInfo.ReadFixedTyped(coefficient, 0, column, 17, strictNumeric: true)).ErrorCode);
        Assert.Equal(JetErrorCode.MalformedValue, Assert.Throws<JetCorruptDataException>(() => JetTypeInfo.ReadFixedString(coefficient, 0, column, 17, strictNumeric: true)).ErrorCode);
        Assert.Equal(DBNull.Value, JetTypeInfo.ReadFixedTyped(coefficient, 0, column, 17, strictNumeric: false));

        byte[] valid = new byte[17];
        valid[1] = 1;
        ColumnInfo scaled = new() { Name = "Value", Type = ColumnType.NumericType, Flags = Constants.ColumnDescriptorFlags.Fixed, NumericPrecision = 28, NumericScale = 29 };
        Assert.Equal(JetErrorCode.MalformedValue, Assert.Throws<JetCorruptDataException>(() => JetTypeInfo.ReadFixedTyped(valid, 0, scaled, 17, strictNumeric: true)).ErrorCode);

        byte[] calculated = new byte[16];
        calculated[2] = 29;
        Assert.Equal(JetErrorCode.MalformedValue, Assert.Throws<JetCorruptDataException>(() => CalculatedColumnUtil.ReadPayloadTyped(calculated, ColumnType.NumericType, strictNumeric: true)).ErrorCode);
        Assert.Equal(JetErrorCode.MalformedValue, Assert.Throws<JetCorruptDataException>(() => CalculatedColumnUtil.ReadPayloadTyped(new byte[17], ColumnType.NumericType, strictNumeric: true)).ErrorCode);
        Assert.Equal(DBNull.Value, CalculatedColumnUtil.ReadPayloadTyped(calculated, ColumnType.NumericType, strictNumeric: false));
    }

    private static async Task<byte[]> CreateDamagedDatabaseAsync(DatabaseFormat format, NumericFault fault, CancellationToken ct)
    {
        bool calculated = fault == NumericFault.CalculatedScale;
        await using var original = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(original, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct))
        {
            ColumnDefinition value = calculated
                ? new ColumnDefinition("Value", typeof(decimal)) { IsCalculated = true, CalculationExpression = "42" }
                : new ColumnDefinition("Value", typeof(decimal)) { NumericPrecision = 28, NumericScale = 2, ForceVariableLengthStorage = fault == NumericFault.VariableCoefficient };
            await writer.CreateTableAsync("Values", [value, new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Payload", typeof(byte[]))], ct);
            await writer.InsertRowAsync("Values", [42m, 1, new byte[] { 1, 2, 3 }], ct);
        }

        byte[] bytes = original.ToArray();
        original.Position = 0;
        await using AccessReader harness = await TestDatabase.OpenReaderAsync(original, cancellationToken: ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Values", ct));
        TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, ct));
        ColumnInfo column = definition.Columns[0];
        Assert.Equal(fault == NumericFault.FixedCoefficient, column.IsFixed);
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
            Assert.Equal(fault == NumericFault.FixedCoefficient ? ColumnSliceKind.Fixed : ColumnSliceKind.Var, slice.Kind);
            int offset = checked((int)(number * profile.PageSize)) + bound.RowStart + slice.DataStart;
            if (calculated)
            {
                Assert.Equal(16, BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset + Constants.CalculatedColumn.DataLenOffset, 4)));
                bytes[offset + Constants.CalculatedColumn.DataOffset + 2] = 29;
            }
            else
            {
                Assert.True(slice.DataLen >= 17);
                bytes.AsSpan(offset + 1, 16).Fill(0xFF);
            }

            return bytes;
        }

        Assert.Fail("The created database must contain the Decimal row.");
        return bytes;
    }

    private sealed class ValueRow
    {
        public decimal? Value { get; set; }
    }

    private sealed class WideValueRow
    {
        public decimal? Value { get; set; }

        public int? Id { get; set; }

        public byte[]? Payload { get; set; }
    }
}
