namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Buffers.Binary;
using System.Diagnostics;
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

#pragma warning disable CA1812 // Public row materialization constructs the projection target.

/// <summary>Missing calculated long-value descriptors cannot become empty strings during read or write-back.</summary>
public sealed class CalculatedLongValueDecodePolicyTests
{
    public static TheoryData<int, bool, bool> EnvelopeCases()
    {
        var data = new TheoryData<int, bool, bool>();
        for (int fault = 0; fault < 4; fault++)
        {
            foreach (bool external in new[] { false, true })
            {
                foreach (bool strict in new[] { false, true })
                {
                    data.Add(fault, external, strict);
                }
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(EnvelopeCases))]
    public async Task MalformedCalculatedMemo_PublicReadsDiagnoseMissingValue(int fault, bool external, bool strict)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream original = await CreateDamagedDatabaseAsync(fault, external, ct);
        await using var messages = new StringWriter(CultureInfo.InvariantCulture);
        using var listener = new TextWriterTraceListener(messages);
        var traceScope = new TestTraceScope(listener);
        try
        {
            await using AccessReader reader = await AccessReader.OpenAsync(original, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ct);
            if (strict)
            {
                JetCorruptDataException typed = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows("Values", cancellationToken: ct).ToListAsync(ct));
                JetCorruptDataException strings = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.RowsAsStrings("Values", cancellationToken: ct).ToListAsync(ct));
                JetCorruptDataException poco = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Rows<ValueRow>("Values", cancellationToken: ct).ToListAsync(ct));
                JetCorruptDataException table = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.ReadTableAsync("Values", cancellationToken: ct));
                Assert.Equal(JetErrorCode.MalformedValue, typed.ErrorCode);
                Assert.Equal("Value", typed.ErrorInfo.ColumnName);
                Assert.Equal(typed.Message, strings.Message);
                Assert.Equal(typed.Message, poco.Message);
                Assert.Equal(typed.Message, table.Message);
            }
            else
            {
                Assert.Equal(DBNull.Value, Assert.Single(await reader.Rows("Values", cancellationToken: ct).ToListAsync(ct))[1]);
                Assert.Null(Assert.Single(await reader.RowsAsStrings("Values", cancellationToken: ct).ToListAsync(ct))[1]);
                Assert.Null(Assert.Single(await reader.Rows<ValueRow>("Values", cancellationToken: ct).ToListAsync(ct)).Value);
                Assert.Equal(DBNull.Value, Assert.Single((await reader.ReadTableAsync("Values", cancellationToken: ct)).Rows.Cast<System.Data.DataRow>())[1]);
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
    [InlineData(0, false)]
    [InlineData(0, true)]
    [InlineData(1, false)]
    [InlineData(1, true)]
    [InlineData(2, false)]
    [InlineData(2, true)]
    [InlineData(3, false)]
    [InlineData(3, true)]
    public async Task MalformedCalculatedMemo_WriteBackRefusesLoss(int fault, bool external)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateDamagedDatabaseAsync(fault, external, ct);
        byte[] before = stream.ToArray();
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct);
        JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await writer.UpdateRowsAsync("Values", RowCriteria.All(), new RowValues { ["Id"] = 2 }, ct));
        Assert.Contains("Value", failure.Message, StringComparison.Ordinal);
        Assert.Equal(before, stream.ToArray());
    }

    private static async Task<MemoryStream> CreateDamagedDatabaseAsync(int fault, bool external, CancellationToken ct)
    {
        await using var original = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(original, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct))
        {
            string value = external ? new string('v', 300) : "visible";
            await writer.CreateTableAsync("Values", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Value", typeof(string)) { IsCalculated = true, CalculationExpression = "\"" + value + "\"" }], ct);
            await writer.InsertRowAsync("Values", [1, value], ct);
        }

        byte[] bytes = original.ToArray();
        original.Position = 0;
        await using AccessReader harness = await TestDatabase.OpenReaderAsync(original, cancellationToken: ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Values", ct));
        TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, ct));
        ColumnInfo column = definition.Columns[1];
        Assert.True(column.IsCalculated);
        Assert.Equal(ColumnType.MemoType, JetTypeInfo.ResolveValueType(column));
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
            Assert.Equal(external, !descriptor.IsInline);
            int payloadOffset = descriptorOffset + Constants.LongValue.HeaderSize;
            if (external)
            {
                Assert.True(descriptor.IsSinglePage);
                int lvalPage = LongValueStore.PageNumber(descriptor.FirstDp);
                byte[] lvalBytes = await harness.ReadPageCopyAsync(lvalPage, ct);
                RowBound lvalBound = Assert.Single(DataPageRows.EnumerateLiveRowBounds(profile, lvalBytes));
                Assert.Equal(LongValueStore.RowIndex(descriptor.FirstDp), lvalBound.RowIndex);
                payloadOffset = (lvalPage * profile.PageSize) + lvalBound.RowStart;
            }

            switch (fault)
            {
                case 0:
                    BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(pageOffset + bound.RowStart + layout.VarTableStart - profile.RowFields.Eod, sizeof(ushort)), checked((ushort)slice.DataStart));
                    Assert.True(RowDecodePlan.TryParseRowLayout(profile.RowFields, bytes.AsSpan(pageOffset, profile.PageSize), bound.RowStart, bound.RowSize, definition.HasVarColumns, out RowLayout damagedLayout));
                    ColumnSlice damagedSlice = RowDecodePlan.ResolveColumnSlice(profile.RowFields, bytes.AsSpan(pageOffset, profile.PageSize), bound.RowStart, bound.RowSize, damagedLayout, column);
                    Assert.Equal(ColumnSliceKind.Var, damagedSlice.Kind);
                    Assert.Equal(0, damagedSlice.DataLen);
                    break;
                case 1:
                    (descriptor with { Length = Constants.CalculatedColumn.DataOffset - 1 }).WriteTo(bytes.AsSpan(descriptorOffset));
                    break;
                case 2:
                    BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(payloadOffset + Constants.CalculatedColumn.DataLenOffset), -1);
                    break;
                case 3:
                    BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(payloadOffset + Constants.CalculatedColumn.DataLenOffset), descriptor.Length);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(fault));
            }

            return new MemoryStream(bytes);
        }

        throw new InvalidOperationException("The created database must contain the calculated value.");
    }

    private sealed class ValueRow
    {
        public string? Value { get; set; }
    }
}
