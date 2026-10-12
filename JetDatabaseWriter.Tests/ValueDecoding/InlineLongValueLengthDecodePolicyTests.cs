namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
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

/// <summary>Inline typed decoding must honor all native long-value length bits.</summary>
public sealed class InlineLongValueLengthDecodePolicyTests
{
    public static TheoryData<DatabaseFormat, bool, bool, bool> InlineLongValueCases()
    {
        var data = new TheoryData<DatabaseFormat, bool, bool, bool>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (bool ole in new[] { false, true })
            {
                foreach (bool strict in new[] { false, true })
                {
                    foreach (bool withinBudget in new[] { false, true })
                    {
                        data.Add(format, ole, strict, withinBudget);
                    }
                }
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(InlineLongValueCases))]
    public async Task HighLengthBits_PublicReadsHonorBudgetAndInlineBounds(DatabaseFormat format, bool ole, bool strict, bool withinBudget)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var original = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(original, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct))
        {
            await writer.CreateTableAsync("Values", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Value", ole ? typeof(byte[]) : typeof(string))], ct);
            await writer.InsertRowAsync("Values", [1, ole ? new byte[] { 1, 2, 3 } : "visible"], ct);
        }

        byte[] bytes = original.ToArray();
        await SetHighLengthBitAsync(original, bytes, ole, ct);
        await using var messages = new StringWriter(CultureInfo.InvariantCulture);
        using var listener = new TextWriterTraceListener(messages);
        var traceScope = new TestTraceScope(listener);
        try
        {
            await using var stream = new MemoryStream(bytes, writable: false);
            await using AccessReader reader = await AccessReader.OpenAsync(
                stream,
                new AccessReaderOptions
                {
                    UseLockFile = false,
                    StrictParsing = strict,
                    MaxLongValueBytes = withinBudget ? 0x1FFFFFF : 0xFFFFFF,
                },
                leaveOpen: true,
                cancellationToken: ct);
            if (!withinBudget)
            {
                JetLimitationException typed = await Assert.ThrowsAsync<JetLimitationException>(async () => await CollectAsync(reader.Rows("Values", cancellationToken: ct)));
                JetLimitationException strings = await Assert.ThrowsAsync<JetLimitationException>(async () => await CollectAsync(reader.RowsAsStrings("Values", cancellationToken: ct)));
                JetLimitationException poco = await Assert.ThrowsAsync<JetLimitationException>(async () => await CollectAsync(reader.Rows<ValueRow>("Values", cancellationToken: ct)));
                JetLimitationException natural = await Assert.ThrowsAsync<JetLimitationException>(async () => await ReadNaturalProjectionAsync(reader, ole, ct));
                JetLimitationException table = await Assert.ThrowsAsync<JetLimitationException>(async () => await reader.ReadTableAsync("Values", cancellationToken: ct));
                Assert.Equal(JetErrorCode.ValueTooLarge, typed.ErrorCode);
                Assert.Equal(typed.Message, strings.Message);
                Assert.Equal(typed.Message, poco.Message);
                Assert.Equal(typed.Message, natural.Message);
                Assert.Equal(typed.Message, table.Message);
                Assert.Empty(messages.ToString());
            }
            else if (strict)
            {
                InvalidDataException typed = await Assert.ThrowsAsync<InvalidDataException>(async () => await CollectAsync(reader.Rows("Values", cancellationToken: ct)));
                InvalidDataException strings = await Assert.ThrowsAsync<InvalidDataException>(async () => await CollectAsync(reader.RowsAsStrings("Values", cancellationToken: ct)));
                InvalidDataException poco = await Assert.ThrowsAsync<InvalidDataException>(async () => await CollectAsync(reader.Rows<ValueRow>("Values", cancellationToken: ct)));
                InvalidDataException natural = await Assert.ThrowsAsync<InvalidDataException>(async () => await ReadNaturalProjectionAsync(reader, ole, ct));
                InvalidDataException table = await Assert.ThrowsAsync<InvalidDataException>(async () => await reader.ReadTableAsync("Values", cancellationToken: ct));
                Assert.Contains("Value", typed.Message, StringComparison.Ordinal);
                Assert.Equal(typed.Message, strings.Message);
                Assert.Equal(typed.Message, poco.Message);
                Assert.Equal(typed.Message, natural.Message);
                Assert.Equal(typed.Message, table.Message);
            }
            else
            {
                Assert.Equal(DBNull.Value, Assert.Single(await CollectAsync(reader.Rows("Values", cancellationToken: ct)))[1]);
                Assert.Null(Assert.Single(await CollectAsync(reader.RowsAsStrings("Values", cancellationToken: ct)))[1]);
                Assert.Null(Assert.Single(await CollectAsync(reader.Rows<ValueRow>("Values", cancellationToken: ct))).Value);
                Assert.Null(await ReadNaturalProjectionAsync(reader, ole, ct));
                Assert.Equal(DBNull.Value, Assert.Single((await reader.ReadTableAsync("Values", cancellationToken: ct)).Rows.Cast<System.Data.DataRow>())[1]);
                listener.Flush();
                Assert.Contains("column 'Value' is unreadable and was returned as a missing value", messages.ToString(), StringComparison.Ordinal);
                Assert.DoesNotContain("visible", messages.ToString(), StringComparison.Ordinal);
            }
        }
        finally
        {
            traceScope.Dispose();
        }

        await using var mutationStream = new MemoryStream();
        await mutationStream.WriteAsync(bytes, ct);
        mutationStream.Position = 0;
        await using AccessWriter mutation = await AccessWriter.OpenAsync(mutationStream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct);
        JetLimitationException refused = await Assert.ThrowsAsync<JetLimitationException>(async () => await mutation.UpdateRowsAsync("Values", RowCriteria.All(), new RowValues { ["Id"] = 2 }, ct));
        Assert.Equal(JetErrorCode.ValueTooLarge, refused.ErrorCode);
        Assert.Equal(bytes, mutationStream.ToArray());
    }

    private static async Task SetHighLengthBitAsync(MemoryStream original, byte[] bytes, bool ole, CancellationToken ct)
    {
        original.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(original, cancellationToken: ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Values", ct));
        TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, ct));
        ColumnInfo column = definition.Columns[1];
        Assert.Equal(ole ? ColumnType.OleType : ColumnType.MemoType, column.Type);
        if (ole)
        {
            Assert.NotNull(DirectRowDecoderBuilder.TryBuildHybrid<OleRow>(definition));
        }

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
            Assert.True(LongValueDescriptor.TryRead(page.AsSpan(bound.RowStart + slice.DataStart, slice.DataLen), out LongValueDescriptor descriptor));
            Assert.True(descriptor.IsInline);
            int offset = checked((int)(number * profile.PageSize)) + bound.RowStart + slice.DataStart;
            bytes[offset + 3] |= 1;
            Assert.True(LongValueDescriptor.TryRead(bytes.AsSpan(offset, slice.DataLen), out LongValueDescriptor damaged));
            Assert.Equal(descriptor.StorageMode, damaged.StorageMode);
            Assert.Equal(descriptor.Length + 0x1000000, damaged.Length);
            return;
        }

        Assert.Fail("The created database must contain the inline long value.");
    }

    private static async Task<object?> ReadNaturalProjectionAsync(AccessReader reader, bool ole, CancellationToken ct)
        => ole
            ? Assert.Single(await CollectAsync(reader.Rows<OleRow>("Values", cancellationToken: ct))).Value
            : Assert.Single(await CollectAsync(reader.Rows<MemoRow>("Values", cancellationToken: ct))).Value;

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
        public object? Value { get; set; }
    }

    private sealed class OleRow
    {
        public byte[]? Value { get; set; }
    }

    private sealed class MemoRow
    {
        public string? Value { get; set; }
    }
}
