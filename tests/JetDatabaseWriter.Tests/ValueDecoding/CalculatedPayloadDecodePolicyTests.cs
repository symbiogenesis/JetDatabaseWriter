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
#pragma warning disable IDE0010, IDE0072 // Only calculated scalar and Text result types are exercised here.

/// <summary>
/// Type-specific calculated scalar and Text payload faults follow the public
/// decode-fault policy across strict, lenient, typed, string, POCO, DataTable and write-back paths.
/// </summary>
public sealed class CalculatedPayloadDecodePolicyTests
{
    /// <summary>The kind of calculated inner-payload corruption.</summary>
    [SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "Theory parameters of public xUnit test methods must be public.")]
    public enum CalculatedPayloadFault
    {
        /// <summary>Non-empty fixed-width scalar payload shorter than its required byte width.</summary>
        TruncatedScalar = 0,

        /// <summary>Non-empty fixed-width scalar payload longer than its required byte width.</summary>
        OversizedScalar = 1,

        /// <summary>Boolean payload with a non-canonical byte (neither 0x00 nor 0xFF).</summary>
        InvalidBooleanByte = 2,

        /// <summary>Plain UTF-16LE calculated Text payload with an odd byte count.</summary>
        OddPlainText = 3,
    }

    public static TheoryData<ColumnType, CalculatedPayloadFault, bool> FaultCases()
    {
        var data = new TheoryData<ColumnType, CalculatedPayloadFault, bool>();
        ColumnType[] truncatedTypes =
        [
            ColumnType.IntegerType,
            ColumnType.LongIntegerType,
            ColumnType.BigIntType,
            ColumnType.FloatType,
            ColumnType.DoubleType,
            ColumnType.MoneyType,
            ColumnType.DateTimeType,
            ColumnType.GuidType,
        ];
        foreach (ColumnType type in truncatedTypes)
        {
            data.Add(type, CalculatedPayloadFault.TruncatedScalar, false);
            data.Add(type, CalculatedPayloadFault.TruncatedScalar, true);
        }

        ColumnType[] oversizedTypes =
        [
            ColumnType.BooleanType,
            ColumnType.ByteType,
            ColumnType.IntegerType,
            ColumnType.LongIntegerType,
            ColumnType.BigIntType,
            ColumnType.FloatType,
            ColumnType.DoubleType,
            ColumnType.MoneyType,
            ColumnType.DateTimeType,
            ColumnType.GuidType,
        ];
        foreach (ColumnType type in oversizedTypes)
        {
            data.Add(type, CalculatedPayloadFault.OversizedScalar, false);
            data.Add(type, CalculatedPayloadFault.OversizedScalar, true);
        }

        data.Add(ColumnType.BooleanType, CalculatedPayloadFault.InvalidBooleanByte, false);
        data.Add(ColumnType.BooleanType, CalculatedPayloadFault.InvalidBooleanByte, true);
        data.Add(ColumnType.TextType, CalculatedPayloadFault.OddPlainText, false);
        data.Add(ColumnType.TextType, CalculatedPayloadFault.OddPlainText, true);
        return data;
    }

    public static TheoryData<ColumnType, CalculatedPayloadFault> WriteBackCases()
    {
        var data = new TheoryData<ColumnType, CalculatedPayloadFault>();
        foreach (TheoryDataRow<ColumnType, CalculatedPayloadFault, bool> row in FaultCases())
        {
            if (row.Data.Item3)
            {
                data.Add(row.Data.Item1, row.Data.Item2);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(FaultCases))]
    public async Task MalformedCalculatedPayload_PublicReadsAgree(ColumnType resultType, CalculatedPayloadFault fault, bool strict)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] bytes = await CreateDamagedDatabaseAsync(resultType, fault, ct);
        await using var messages = new StringWriter(CultureInfo.InvariantCulture);
        using var listener = new TextWriterTraceListener(messages);
        var traceScope = new TestTraceScope(listener);
        try
        {
            await using var stream = new MemoryStream(bytes, writable: false);
            await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ct);
            if (strict)
            {
                JetCorruptDataException typed = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows("Values", cancellationToken: ct).ToListAsync(ct));
                JetCorruptDataException strings = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.RowsAsStrings("Values", cancellationToken: ct).ToListAsync(ct));
                JetCorruptDataException poco = await AssertToListThrowsAsync(reader, resultType, ct);
                JetCorruptDataException table = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.ReadTableAsync("Values", cancellationToken: ct));
                JetCorruptDataException stringTable = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.ReadTableAsStringsAsync("Values", cancellationToken: ct));
                foreach (JetCorruptDataException failure in new[] { typed, strings, poco, table, stringTable })
                {
                    Assert.Equal(JetErrorCode.MalformedValue, failure.ErrorCode);
                    Assert.Equal("Value", failure.ErrorInfo.ColumnName);
                    Assert.NotNull(failure.InnerException);
                }
            }
            else
            {
                Assert.Equal(DBNull.Value, Assert.Single(await reader.Rows("Values", cancellationToken: ct).ToListAsync(ct))[1]);
                Assert.Equal(string.Empty, Assert.Single(await reader.RowsAsStrings("Values", cancellationToken: ct).ToListAsync(ct))[1]);
                await AssertPocoMissingAsync(reader, resultType, ct);
                Assert.Equal(DBNull.Value, Assert.Single((await reader.ReadTableAsync("Values", cancellationToken: ct)).Rows.Cast<System.Data.DataRow>())[1]);
                Assert.Equal(string.Empty, Assert.Single((await reader.ReadTableAsStringsAsync("Values", cancellationToken: ct)).Rows.Cast<System.Data.DataRow>())[1]);
                listener.Flush();
                Assert.Contains("calculated", messages.ToString(), StringComparison.Ordinal);
                Assert.Contains("column 'Value' is unreadable and was returned as a missing value", messages.ToString(), StringComparison.Ordinal);
            }
        }
        finally
        {
            traceScope.Dispose();
        }
    }

    [Theory]
    [MemberData(nameof(WriteBackCases))]
    public async Task MalformedCalculatedPayload_WriteBackRefusesWithoutMutatingDatabase(ColumnType resultType, CalculatedPayloadFault fault)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] before = await CreateDamagedDatabaseAsync(resultType, fault, ct);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(before, ct);
        stream.Position = 0;
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct);
        JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await writer.UpdateRowsAsync("Values", RowCriteria.All(), new RowValues { ["Id"] = 2 }, ct));
        Assert.Equal(JetErrorCode.MalformedValue, failure.ErrorCode);
        Assert.Contains("Value", failure.Message, StringComparison.Ordinal);
        Assert.Equal(before, stream.ToArray());
    }

    [Fact]
    public void CalculatedColumnUtil_StrictInvalidPayloads_ThrowMalformedValue()
    {
        Assert.Equal(
            JetErrorCode.MalformedValue,
            Assert.Throws<JetCorruptDataException>(() => CalculatedColumnUtil.ReadPayloadTyped([0x02], ColumnType.BooleanType, strictNumeric: true)).ErrorCode);
        Assert.Equal(
            JetErrorCode.MalformedValue,
            Assert.Throws<JetCorruptDataException>(() => CalculatedColumnUtil.ReadPayloadString([0xFF, 0x00], ColumnType.BooleanType, strictNumeric: true)).ErrorCode);
        Assert.Equal(
            JetErrorCode.MalformedValue,
            Assert.Throws<JetCorruptDataException>(() => CalculatedColumnUtil.ReadPayloadTyped([1, 2, 3, 4, 5], ColumnType.LongIntegerType, strictNumeric: true)).ErrorCode);
        Assert.Equal(
            JetErrorCode.MalformedValue,
            Assert.Throws<JetCorruptDataException>(() => CalculatedColumnUtil.ReadPayloadString([1, 2, 3], ColumnType.LongIntegerType, strictNumeric: true)).ErrorCode);
    }

    private static async Task<JetCorruptDataException> AssertToListThrowsAsync(AccessReader reader, ColumnType resultType, CancellationToken ct) =>
        resultType switch
        {
            ColumnType.BooleanType => await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows<BoolRow>("Values", cancellationToken: ct).ToListAsync(ct)),
            ColumnType.ByteType => await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows<ByteRow>("Values", cancellationToken: ct).ToListAsync(ct)),
            ColumnType.IntegerType => await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows<Int16Row>("Values", cancellationToken: ct).ToListAsync(ct)),
            ColumnType.LongIntegerType => await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows<Int32Row>("Values", cancellationToken: ct).ToListAsync(ct)),
            ColumnType.BigIntType => await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows<Int64Row>("Values", cancellationToken: ct).ToListAsync(ct)),
            ColumnType.FloatType => await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows<SingleRow>("Values", cancellationToken: ct).ToListAsync(ct)),
            ColumnType.DoubleType => await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows<DoubleRow>("Values", cancellationToken: ct).ToListAsync(ct)),
            ColumnType.MoneyType => await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows<DecimalRow>("Values", cancellationToken: ct).ToListAsync(ct)),
            ColumnType.DateTimeType => await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows<DateRow>("Values", cancellationToken: ct).ToListAsync(ct)),
            ColumnType.GuidType => await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows<GuidRow>("Values", cancellationToken: ct).ToListAsync(ct)),
            ColumnType.TextType => await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows<StringRow>("Values", cancellationToken: ct).ToListAsync(ct)),
            _ => throw new ArgumentOutOfRangeException(nameof(resultType)),
        };

    private static async Task AssertPocoMissingAsync(AccessReader reader, ColumnType resultType, CancellationToken ct)
    {
        switch (resultType)
        {
            case ColumnType.BooleanType:
                Assert.Null(Assert.Single(await reader.Rows<BoolRow>("Values", cancellationToken: ct).ToListAsync(ct)).Value);
                break;
            case ColumnType.ByteType:
                Assert.Null(Assert.Single(await reader.Rows<ByteRow>("Values", cancellationToken: ct).ToListAsync(ct)).Value);
                break;
            case ColumnType.IntegerType:
                Assert.Null(Assert.Single(await reader.Rows<Int16Row>("Values", cancellationToken: ct).ToListAsync(ct)).Value);
                break;
            case ColumnType.LongIntegerType:
                Assert.Null(Assert.Single(await reader.Rows<Int32Row>("Values", cancellationToken: ct).ToListAsync(ct)).Value);
                break;
            case ColumnType.BigIntType:
                Assert.Null(Assert.Single(await reader.Rows<Int64Row>("Values", cancellationToken: ct).ToListAsync(ct)).Value);
                break;
            case ColumnType.FloatType:
                Assert.Null(Assert.Single(await reader.Rows<SingleRow>("Values", cancellationToken: ct).ToListAsync(ct)).Value);
                break;
            case ColumnType.DoubleType:
                Assert.Null(Assert.Single(await reader.Rows<DoubleRow>("Values", cancellationToken: ct).ToListAsync(ct)).Value);
                break;
            case ColumnType.MoneyType:
                Assert.Null(Assert.Single(await reader.Rows<DecimalRow>("Values", cancellationToken: ct).ToListAsync(ct)).Value);
                break;
            case ColumnType.DateTimeType:
                Assert.Null(Assert.Single(await reader.Rows<DateRow>("Values", cancellationToken: ct).ToListAsync(ct)).Value);
                break;
            case ColumnType.GuidType:
                Assert.Null(Assert.Single(await reader.Rows<GuidRow>("Values", cancellationToken: ct).ToListAsync(ct)).Value);
                break;
            case ColumnType.TextType:
                Assert.Null(Assert.Single(await reader.Rows<StringRow>("Values", cancellationToken: ct).ToListAsync(ct)).Value);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(resultType));
        }
    }

    private static async Task<byte[]> CreateDamagedDatabaseAsync(ColumnType resultType, CalculatedPayloadFault fault, CancellationToken ct)
    {
        await using var original = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(original, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct))
        {
            (ColumnDefinition column, object initialValue) = BuildColumnAndInitialValue(resultType);
            await writer.CreateTableAsync("Values", [new ColumnDefinition("Id", typeof(int)), column], ct);
            await writer.InsertRowAsync("Values", [1, initialValue], ct);
        }

        byte[] bytes = original.ToArray();
        original.Position = 0;
        await using AccessReader harness = await TestDatabase.OpenReaderAsync(original, cancellationToken: ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Values", ct));
        TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, ct));
        ColumnInfo columnInfo = definition.Columns[1];
        Assert.True(columnInfo.IsCalculated);
        Assert.Equal(resultType, JetTypeInfo.ResolveValueType(columnInfo));

        JetFormat profile = harness.Database.Format;
        for (long number = 1; number < harness.Database.Pages.PageCount; number++)
        {
            byte[] page = await harness.ReadPageCopyAsync(number, ct);
            if (page[0] != Constants.PageTypes.Data || JetTypeInfo.Ri32(page, profile.DataPage.TDefOff) != entry.TDefPage)
            {
                continue;
            }

            RowBound bound = Assert.Single(DataPageRows.EnumerateLiveRowBounds(profile, page));
            Assert.True(RowDecodePlan.TryParseRowLayout(profile.RowFields, page, bound.RowStart, bound.RowSize, definition.HasVarColumns, out RowLayout layout));
            ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(profile.RowFields, page, bound.RowStart, bound.RowSize, layout, columnInfo);
            Assert.Equal(ColumnSliceKind.Var, slice.Kind);
            int pageOffset = checked((int)(number * profile.PageSize));
            int rowOffset = pageOffset + bound.RowStart;
            int envelopeOffset = rowOffset + slice.DataStart;
            int declaredLen = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(envelopeOffset + Constants.CalculatedColumn.DataLenOffset, sizeof(int)));
            Assert.True(declaredLen > 0);

            switch (fault)
            {
                case CalculatedPayloadFault.TruncatedScalar:
                    BinaryPrimitives.WriteInt32LittleEndian(
                        bytes.AsSpan(envelopeOffset + Constants.CalculatedColumn.DataLenOffset, sizeof(int)),
                        declaredLen - 1);
                    break;

                case CalculatedPayloadFault.OversizedScalar:
                    byte[] oversizedPayload = new byte[declaredLen + 1];
                    bytes.AsSpan(envelopeOffset + Constants.CalculatedColumn.DataOffset, declaredLen).CopyTo(oversizedPayload);
                    byte[] oversizedEnvelope = CalculatedColumnUtil.Wrap(oversizedPayload);
                    ReplaceVariableSlotInPage(bytes.AsSpan(pageOffset, profile.PageSize), profile, bound, layout, columnInfo, oversizedEnvelope);
                    break;

                case CalculatedPayloadFault.InvalidBooleanByte:
                    bytes[envelopeOffset + Constants.CalculatedColumn.DataOffset] = 0x02;
                    break;

                case CalculatedPayloadFault.OddPlainText:
                    bytes[envelopeOffset + Constants.CalculatedColumn.DataOffset] = 0x41;
                    BinaryPrimitives.WriteInt32LittleEndian(
                        bytes.AsSpan(envelopeOffset + Constants.CalculatedColumn.DataLenOffset, sizeof(int)),
                        1);
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(fault));
            }

            return bytes;
        }

        throw new InvalidOperationException("The created database must contain the calculated row.");
    }

    private static void ReplaceVariableSlotInPage(
        Span<byte> page,
        JetFormat profile,
        RowBound bound,
        RowLayout layout,
        ColumnInfo column,
        ReadOnlySpan<byte> replacement)
    {
        ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(profile.RowFields, page, bound.RowStart, bound.RowSize, layout, column);
        int delta = replacement.Length - slice.DataLen;
        int newRowSize = bound.RowSize + delta;
        int newRowStart = bound.RowStart - delta;
        Span<byte> newRow = new byte[newRowSize];

        int prefixLen = slice.DataStart;
        page.Slice(bound.RowStart, prefixLen).CopyTo(newRow);
        replacement.CopyTo(newRow.Slice(prefixLen, replacement.Length));
        int suffixStart = slice.DataStart + slice.DataLen;
        int suffixLen = bound.RowSize - suffixStart;
        page.Slice(bound.RowStart + suffixStart, suffixLen).CopyTo(newRow.Slice(prefixLen + replacement.Length, suffixLen));

        int newEodPos = layout.VarTableStart - profile.RowFields.Eod + delta;
        BinaryPrimitives.WriteUInt16LittleEndian(newRow.Slice(newEodPos, sizeof(ushort)), checked((ushort)(layout.Eod + delta)));
        newRow.CopyTo(page.Slice(newRowStart, newRowSize));

        int dirOffset = profile.DataPage.RowsStart + (bound.RowIndex * sizeof(ushort));
        BinaryPrimitives.WriteUInt16LittleEndian(page.Slice(dirOffset, sizeof(ushort)), checked((ushort)newRowStart));
    }

    private static (ColumnDefinition Column, object InitialValue) BuildColumnAndInitialValue(ColumnType resultType) =>
        resultType switch
        {
            ColumnType.BooleanType => (new ColumnDefinition("Value", typeof(bool)) { IsCalculated = true, CalculatedResultType = (byte)resultType, CalculationExpression = "True" }, true),
            ColumnType.ByteType => (new ColumnDefinition("Value", typeof(byte)) { IsCalculated = true, CalculatedResultType = (byte)resultType, CalculationExpression = "7" }, (byte)7),
            ColumnType.IntegerType => (new ColumnDefinition("Value", typeof(short)) { IsCalculated = true, CalculatedResultType = (byte)resultType, CalculationExpression = "7" }, (short)7),
            ColumnType.LongIntegerType => (new ColumnDefinition("Value", typeof(int)) { IsCalculated = true, CalculatedResultType = (byte)resultType, CalculationExpression = "7" }, 7),
            ColumnType.BigIntType => (new ColumnDefinition("Value", typeof(long)) { IsCalculated = true, CalculatedResultType = (byte)resultType, CalculationExpression = "7" }, 7L),
            ColumnType.FloatType => (new ColumnDefinition("Value", typeof(float)) { IsCalculated = true, CalculatedResultType = (byte)resultType, CalculationExpression = "7.5" }, 7.5f),
            ColumnType.DoubleType => (new ColumnDefinition("Value", typeof(double)) { IsCalculated = true, CalculatedResultType = (byte)resultType, CalculationExpression = "7.5" }, 7.5d),
            ColumnType.MoneyType => (new ColumnDefinition("Value", typeof(decimal)) { IsCurrency = true, IsCalculated = true, CalculatedResultType = (byte)resultType, CalculationExpression = "7.5" }, 7.5m),
            ColumnType.DateTimeType => (new ColumnDefinition("Value", typeof(DateTime)) { IsCalculated = true, CalculatedResultType = (byte)resultType, CalculationExpression = "DateSerial(2026,1,1)" }, new DateTime(2026, 1, 1)),
            ColumnType.GuidType => (new ColumnDefinition("Value", typeof(Guid)) { IsCalculated = true, CalculatedResultType = (byte)resultType, CalculationExpression = "\"{11111111-2222-3333-4444-555555555555}\"" }, Guid.Parse("11111111-2222-3333-4444-555555555555")),
            ColumnType.TextType => (new ColumnDefinition("Value", typeof(string), maxLength: 40) { IsCalculated = true, CalculatedResultType = (byte)resultType, CalculationExpression = "\"abc\"" }, "abc"),
            _ => throw new ArgumentOutOfRangeException(nameof(resultType)),
        };

    private sealed class BoolRow
    {
        public bool? Value { get; set; }
    }

    private sealed class ByteRow
    {
        public byte? Value { get; set; }
    }

    private sealed class Int16Row
    {
        public short? Value { get; set; }
    }

    private sealed class Int32Row
    {
        public int? Value { get; set; }
    }

    private sealed class Int64Row
    {
        public long? Value { get; set; }
    }

    private sealed class SingleRow
    {
        public float? Value { get; set; }
    }

    private sealed class DoubleRow
    {
        public double? Value { get; set; }
    }

    private sealed class DecimalRow
    {
        public decimal? Value { get; set; }
    }

    private sealed class DateRow
    {
        public DateTime? Value { get; set; }
    }

    private sealed class GuidRow
    {
        public Guid? Value { get; set; }
    }

    private sealed class StringRow
    {
        public string? Value { get; set; }
    }
}
