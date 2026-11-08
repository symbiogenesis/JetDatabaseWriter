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
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
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
/// Long-value descriptor and chain faults follow the public decode-fault policy
/// across ordinary and calculated Memo, Binary and OLE columns on every read and write-back surface.
/// </summary>
public sealed class LongValueDescriptorAndChainDecodePolicyTests
{
    /// <summary>The long-value descriptor or chain corruption kind.</summary>
    [SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "Theory parameters of public xUnit test methods must be public.")]
    public enum LongValueFault
    {
        /// <summary>Variable slot shorter than the 12-byte LVAL header (1..11 bytes).</summary>
        TruncatedDescriptor = 0,

        /// <summary>Reserved storage mode 0xC0 in byte 3 of the descriptor.</summary>
        InvalidStorageMode = 1,

        /// <summary>Single-page descriptor with positive length and null FirstDp pointer.</summary>
        SinglePageNullPointer = 2,

        /// <summary>Chained descriptor with positive length and null FirstDp pointer.</summary>
        ChainedNullPointer = 3,

        /// <summary>External pointer targeting a page number beyond the end of the database.</summary>
        OutOfRangePage = 4,

        /// <summary>External pointer targeting a non-DATA page header.</summary>
        NonDataPage = 5,

        /// <summary>External pointer targeting a deleted LVAL row slot.</summary>
        DeletedSlot = 6,

        /// <summary>External pointer targeting an overflow LVAL row slot.</summary>
        OverflowSlot = 7,

        /// <summary>External pointer targeting a row index outside the LVAL page's row directory.</summary>
        MissingSlot = 8,

        /// <summary>Single-page LVAL row shorter than the descriptor's declared byte length.</summary>
        SinglePageRowTooShort = 9,

        /// <summary>Chained LVAL row containing only the 4-byte next-pointer header and zero payload bytes.</summary>
        ChainedZeroProgressRow = 10,

        /// <summary>Chained LVAL row terminating with nextDp = 0 before the declared byte length is reached.</summary>
        ChainedPrematureEnd = 11,

        /// <summary>Chained LVAL row whose nextDp pointer cycles back to an already-visited chunk.</summary>
        ChainedCycle = 12,

        /// <summary>Plain UTF-16LE inline Memo payload with an odd declared byte length.</summary>
        OddPlainInlineMemo = 13,

        /// <summary>Plain UTF-16LE single-page Memo payload with an odd declared byte length.</summary>
        OddPlainSinglePageMemo = 14,
    }

    public static TheoryData<LongValueFault, bool, ColumnType, bool> FaultCases()
    {
        var data = new TheoryData<LongValueFault, bool, ColumnType, bool>();
        foreach (LongValueFault fault in Enum.GetValues<LongValueFault>())
        {
            foreach ((bool calculated, ColumnType type) in new[]
            {
                (false, ColumnType.MemoType),
                (false, ColumnType.OleType),
                (true, ColumnType.MemoType),
                (true, ColumnType.BinaryType),
                (true, ColumnType.OleType),
            })
            {
                if (fault is LongValueFault.OddPlainInlineMemo or LongValueFault.OddPlainSinglePageMemo
                    && (calculated || type != ColumnType.MemoType))
                {
                    continue;
                }

                foreach (bool strict in new[] { false, true })
                {
                    data.Add(fault, calculated, type, strict);
                }
            }
        }

        return data;
    }

    public static TheoryData<LongValueFault, bool, ColumnType> WriteBackCases()
    {
        var data = new TheoryData<LongValueFault, bool, ColumnType>();
        foreach (LongValueFault fault in Enum.GetValues<LongValueFault>())
        {
            foreach ((bool calculated, ColumnType type) in new[]
            {
                (false, ColumnType.MemoType),
                (false, ColumnType.OleType),
                (true, ColumnType.MemoType),
                (true, ColumnType.BinaryType),
                (true, ColumnType.OleType),
            })
            {
                if (fault is LongValueFault.OddPlainInlineMemo or LongValueFault.OddPlainSinglePageMemo
                    && (calculated || type != ColumnType.MemoType))
                {
                    continue;
                }

                data.Add(fault, calculated, type);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(FaultCases))]
    public async Task DescriptorOrChainFault_PublicReadsAgree(LongValueFault fault, bool calculated, ColumnType columnType, bool strict)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] bytes = await CreateDamagedDatabaseAsync(fault, calculated, columnType, ct);
        await using var messages = new StringWriter(CultureInfo.InvariantCulture);
        using var listener = new TextWriterTraceListener(messages);
        var traceScope = new TestTraceScope(listener);
        try
        {
            await using var stream = new MemoryStream(bytes, writable: false);
            await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ct);
            bool isBinarySurface = columnType is ColumnType.BinaryType or ColumnType.OleType;
            if (strict)
            {
                JetCorruptDataException typed = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows("Values", cancellationToken: ct).ToListAsync(ct));
                JetCorruptDataException strings = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.RowsAsStrings("Values", cancellationToken: ct).ToListAsync(ct));
                JetCorruptDataException stringPoco = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows<StringValueRow>("Values", cancellationToken: ct).ToListAsync(ct));
                JetCorruptDataException table = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.ReadTableAsync("Values", cancellationToken: ct));
                JetCorruptDataException stringTable = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.ReadTableAsStringsAsync("Values", cancellationToken: ct));
                foreach (JetCorruptDataException failure in new[] { typed, strings, stringPoco, table, stringTable })
                {
                    Assert.Equal(JetErrorCode.UnreadableLongValue, failure.ErrorCode);
                    Assert.Equal("Value", failure.ErrorInfo.ColumnName);
                    Assert.NotNull(failure.InnerException);
                }

                if (isBinarySurface)
                {
                    JetCorruptDataException binaryPoco = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows<BinaryValueRow>("Values", cancellationToken: ct).ToListAsync(ct));
                    JetCorruptDataException projectedBinaryPoco = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Services.Tables.RowsWithDecoder<BinaryValueRow>("Values", forceProjection: true, progress: null, cancellationToken: ct).ToListAsync(ct));
                    Assert.Equal(JetErrorCode.UnreadableLongValue, binaryPoco.ErrorCode);
                    Assert.Equal("Value", binaryPoco.ErrorInfo.ColumnName);
                    Assert.Equal(binaryPoco.Message, projectedBinaryPoco.Message);
                }
            }
            else
            {
                Assert.Equal(DBNull.Value, Assert.Single(await reader.Rows("Values", cancellationToken: ct).ToListAsync(ct))[1]);
                Assert.Null(Assert.Single(await reader.RowsAsStrings("Values", cancellationToken: ct).ToListAsync(ct))[1]);
                Assert.Null(Assert.Single(await reader.Rows<StringValueRow>("Values", cancellationToken: ct).ToListAsync(ct)).Value);
                if (isBinarySurface)
                {
                    Assert.Null(Assert.Single(await reader.Rows<BinaryValueRow>("Values", cancellationToken: ct).ToListAsync(ct)).Value);
                    Assert.Null(Assert.Single(await reader.Services.Tables.RowsWithDecoder<BinaryValueRow>("Values", forceProjection: true, progress: null, cancellationToken: ct).ToListAsync(ct)).Value);
                }

                Assert.Equal(DBNull.Value, Assert.Single((await reader.ReadTableAsync("Values", cancellationToken: ct)).Rows.Cast<System.Data.DataRow>())[1]);
                Assert.Equal(DBNull.Value, Assert.Single((await reader.ReadTableAsStringsAsync("Values", cancellationToken: ct)).Rows.Cast<System.Data.DataRow>())[1]);
                listener.Flush();
                Assert.Contains("The stored long value for column 'Value' is unreadable and was returned as a missing value.", messages.ToString(), StringComparison.Ordinal);
            }
        }
        finally
        {
            traceScope.Dispose();
        }
    }

    [Theory]
    [MemberData(nameof(WriteBackCases))]
    public async Task DescriptorOrChainFault_WriteBackRefusesWithoutMutatingDatabase(LongValueFault fault, bool calculated, ColumnType columnType)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] before = await CreateDamagedDatabaseAsync(fault, calculated, columnType, ct);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(before, ct);
        stream.Position = 0;
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct);
        JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await writer.UpdateRowsAsync("Values", RowCriteria.All(), new RowValues { ["Id"] = 2 }, ct));
        Assert.Contains("Value", failure.Message, StringComparison.Ordinal);
        Assert.Equal(before, stream.ToArray());
    }

    private static async Task<byte[]> CreateDamagedDatabaseAsync(LongValueFault fault, bool calculated, ColumnType columnType, CancellationToken ct)
    {
        bool needsChain = fault is LongValueFault.ChainedNullPointer
            or LongValueFault.ChainedZeroProgressRow
            or LongValueFault.ChainedPrematureEnd
            or LongValueFault.ChainedCycle;
        bool needsInline = fault is LongValueFault.TruncatedDescriptor
            or LongValueFault.InvalidStorageMode
            or LongValueFault.OddPlainInlineMemo;
        int charLength = 300;
        if (needsInline)
        {
            charLength = 10;
        }
        else if (needsChain)
        {
            charLength = 3000;
        }

        await using var original = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(original, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct))
        {
            string textValue = new('\u03B1', charLength);
            byte[] binaryValue = Enumerable.Range(0, charLength * 2).Select(index => unchecked((byte)(index + 1))).ToArray();
            ColumnDefinition valueColumn = calculated
                ? new ColumnDefinition("Value", columnType == ColumnType.MemoType ? typeof(string) : typeof(byte[]))
                {
                    IsCalculated = true,
                    CalculatedResultType = (byte)columnType,
                    CalculationExpression = "\"" + new string('v', charLength) + "\"",
                }
                : new ColumnDefinition("Value", columnType == ColumnType.MemoType ? typeof(string) : typeof(byte[]))
                {
                    IsCompressedUnicode = fault is not (LongValueFault.OddPlainInlineMemo or LongValueFault.OddPlainSinglePageMemo),
                };

            await writer.CreateTableAsync("Values", [new ColumnDefinition("Id", typeof(int)), valueColumn], ct);
            object inserted = binaryValue;
            if (calculated)
            {
                inserted = new string('v', charLength);
            }
            else if (columnType == ColumnType.MemoType)
            {
                inserted = textValue;
            }

            await writer.InsertRowAsync("Values", [1, inserted], ct);
        }

        byte[] bytes = original.ToArray();
        original.Position = 0;
        await using AccessReader harness = await TestDatabase.OpenReaderAsync(original, cancellationToken: ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Values", ct));
        TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, ct));
        ColumnInfo column = definition.Columns[1];
        if (!calculated && columnType == ColumnType.OleType)
        {
            Assert.NotNull(DirectRowDecoderBuilder.TryBuildHybrid<BinaryValueRow>(definition));
        }

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
            ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(profile.RowFields, page, bound.RowStart, bound.RowSize, layout, column);
            Assert.Equal(ColumnSliceKind.Var, slice.Kind);
            int pageOffset = checked((int)(number * profile.PageSize));
            int descriptorOffset = pageOffset + bound.RowStart + slice.DataStart;
            Assert.True(LongValueDescriptor.TryRead(bytes.AsSpan(descriptorOffset, slice.DataLen), out LongValueDescriptor descriptor));

            int lvalPage = descriptor.IsInline ? 0 : LongValueStore.PageNumber(descriptor.FirstDp);
            int lvalRow = descriptor.IsInline ? 0 : LongValueStore.RowIndex(descriptor.FirstDp);
            int lvalPageOffset = lvalPage * profile.PageSize;

            switch (fault)
            {
                case LongValueFault.TruncatedDescriptor:
                    BinaryPrimitives.WriteUInt16LittleEndian(
                        bytes.AsSpan(pageOffset + bound.RowStart + layout.VarTableStart - profile.RowFields.Eod, sizeof(ushort)),
                        checked((ushort)(slice.DataStart + 7)));
                    break;

                case LongValueFault.InvalidStorageMode:
                    bytes[descriptorOffset + 3] = (byte)((bytes[descriptorOffset + 3] & 0x3F) | 0xC0);
                    break;

                case LongValueFault.SinglePageNullPointer:
                    new LongValueDescriptor(descriptor.Length, Constants.LongValue.SinglePageStorageMode, 0, descriptor.Token)
                        .WriteTo(bytes.AsSpan(descriptorOffset, Constants.LongValue.HeaderSize));
                    break;

                case LongValueFault.ChainedNullPointer:
                    new LongValueDescriptor(descriptor.Length, Constants.LongValue.ChainedStorageMode, 0, descriptor.Token)
                        .WriteTo(bytes.AsSpan(descriptorOffset, Constants.LongValue.HeaderSize));
                    break;

                case LongValueFault.OutOfRangePage:
                    (descriptor with { FirstDp = LongValueStore.MakeRowPointer(harness.Database.Pages.PageCount + 5, 0) })
                        .WriteTo(bytes.AsSpan(descriptorOffset, Constants.LongValue.HeaderSize));
                    break;

                case LongValueFault.NonDataPage:
                    bytes[lvalPageOffset] = 0x02;
                    break;

                case LongValueFault.DeletedSlot:
                {
                    int entryOffset = lvalPageOffset + profile.DataPage.RowsStart + (lvalRow * sizeof(ushort));
                    ushort raw = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(entryOffset, sizeof(ushort)));
                    BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(entryOffset, sizeof(ushort)), (ushort)(raw | Constants.DataPage.DeletedRowFlag));
                    break;
                }

                case LongValueFault.OverflowSlot:
                {
                    int entryOffset = lvalPageOffset + profile.DataPage.RowsStart + (lvalRow * sizeof(ushort));
                    ushort raw = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(entryOffset, sizeof(ushort)));
                    BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(entryOffset, sizeof(ushort)), (ushort)(raw | Constants.DataPage.OverflowRowFlag));
                    break;
                }

                case LongValueFault.MissingSlot:
                    (descriptor with { FirstDp = LongValueStore.MakeRowPointer(lvalPage, 9) })
                        .WriteTo(bytes.AsSpan(descriptorOffset, Constants.LongValue.HeaderSize));
                    break;

                case LongValueFault.SinglePageRowTooShort:
                    (descriptor with { Length = descriptor.Length + 8 })
                        .WriteTo(bytes.AsSpan(descriptorOffset, Constants.LongValue.HeaderSize));
                    break;

                case LongValueFault.ChainedZeroProgressRow:
                {
                    int entryOffset = lvalPageOffset + profile.DataPage.RowsStart + (lvalRow * sizeof(ushort));
                    BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(entryOffset, sizeof(ushort)), checked((ushort)(profile.PageSize - sizeof(uint))));
                    break;
                }

                case LongValueFault.ChainedPrematureEnd:
                {
                    byte[] lvalBytes = await harness.ReadPageCopyAsync(lvalPage, ct);
                    RowBound lvalBound = Assert.Single(DataPageRows.EnumerateLiveRowBounds(profile, lvalBytes));
                    BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(lvalPageOffset + lvalBound.RowStart, sizeof(uint)), 0u);
                    break;
                }

                case LongValueFault.ChainedCycle:
                {
                    byte[] lvalBytes = await harness.ReadPageCopyAsync(lvalPage, ct);
                    RowBound lvalBound = Assert.Single(DataPageRows.EnumerateLiveRowBounds(profile, lvalBytes));
                    BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(lvalPageOffset + lvalBound.RowStart, sizeof(uint)), descriptor.FirstDp);
                    break;
                }

                case LongValueFault.OddPlainInlineMemo:
                case LongValueFault.OddPlainSinglePageMemo:
                    (descriptor with { Length = descriptor.Length - 1 })
                        .WriteTo(bytes.AsSpan(descriptorOffset, Constants.LongValue.HeaderSize));
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(fault));
            }

            return bytes;
        }

        throw new InvalidOperationException("The created database must contain the target row.");
    }

    private sealed class StringValueRow
    {
        public int Id { get; set; }

        public string? Value { get; set; }
    }

    private sealed class BinaryValueRow
    {
        public int Id { get; set; }

        public byte[]? Value { get; set; }
    }
}
