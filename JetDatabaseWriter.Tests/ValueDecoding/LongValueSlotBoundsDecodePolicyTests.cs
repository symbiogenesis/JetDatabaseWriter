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

/// <summary>Assigned long-value and complex slots cannot silently lose missing or malformed offsets.</summary>
public sealed class LongValueSlotBoundsDecodePolicyTests
{
    public static TheoryData<DatabaseFormat, bool, bool, bool, bool> LongValueSlots()
    {
        var data = new TheoryData<DatabaseFormat, bool, bool, bool, bool>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (bool ole in new[] { false, true })
            {
                foreach (bool missingSlot in new[] { false, true })
                {
                    foreach (bool strict in new[] { false, true })
                    {
                        data.Add(format, ole, false, missingSlot, strict);
                        if (format == DatabaseFormat.AceAccdb && !ole)
                        {
                            data.Add(format, ole, true, missingSlot, strict);
                        }
                    }
                }
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(LongValueSlots))]
    public async Task AssignedLongValueSlot_PublicReadsDiagnoseMissingOffsets(DatabaseFormat format, bool ole, bool calculated, bool missingSlot, bool strict)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var original = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(original, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct))
        {
            var valueColumn = new ColumnDefinition("Value", ole ? typeof(byte[]) : typeof(string))
            {
                IsCalculated = calculated,
                CalculationExpression = calculated ? "\"visible\"" : null,
            };
            await writer.CreateTableAsync("Values", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Padding", typeof(string), maxLength: 50), valueColumn], ct);
            await writer.InsertRowAsync("Values", [1, "padding", ole ? new byte[] { 1, 2, 3 } : "visible"], ct);
        }

        byte[] bytes = original.ToArray();
        await DamageLastVariableSlotAsync(original, bytes, missingSlot, ct);
        await using var messages = new StringWriter(CultureInfo.InvariantCulture);
        using var listener = new TextWriterTraceListener(messages);
        var traceScope = new TestTraceScope(listener);
        try
        {
            await using var stream = new MemoryStream(bytes, writable: false);
            await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ct);
            if (strict)
            {
                InvalidDataException strings = await Assert.ThrowsAsync<InvalidDataException>(async () => await reader.RowsAsStrings("Values", cancellationToken: ct).ToListAsync(ct));
                InvalidDataException typed = await Assert.ThrowsAsync<InvalidDataException>(async () => await reader.Rows("Values", cancellationToken: ct).ToListAsync(ct));
                InvalidDataException poco = await Assert.ThrowsAsync<InvalidDataException>(async () => await reader.Rows<ValueRow>("Values", cancellationToken: ct).ToListAsync(ct));
                InvalidDataException table = await Assert.ThrowsAsync<InvalidDataException>(async () => await reader.ReadTableAsync("Values", cancellationToken: ct));
                Assert.Contains("Value", strings.Message, StringComparison.Ordinal);
                Assert.IsType<InvalidDataException>(strings.InnerException);
                Assert.Equal(strings.Message, typed.Message);
                Assert.Equal(typed.Message, poco.Message);
                Assert.Equal(poco.Message, table.Message);
                if (ole)
                {
                    InvalidDataException hybrid = await Assert.ThrowsAsync<InvalidDataException>(async () => await reader.Rows<OleRow>("Values", cancellationToken: ct).ToListAsync(ct));
                    Assert.Equal(typed.Message, hybrid.Message);
                }
            }
            else
            {
                Assert.Null(Assert.Single(await reader.RowsAsStrings("Values", cancellationToken: ct).ToListAsync(ct))[2]);
                Assert.Equal(DBNull.Value, Assert.Single(await reader.Rows("Values", cancellationToken: ct).ToListAsync(ct))[2]);
                Assert.Null(Assert.Single(await reader.Rows<ValueRow>("Values", cancellationToken: ct).ToListAsync(ct)).Value);
                Assert.Equal(DBNull.Value, Assert.Single((await reader.ReadTableAsync("Values", cancellationToken: ct)).Rows.Cast<System.Data.DataRow>())[2]);
                if (ole)
                {
                    Assert.Null(Assert.Single(await reader.Rows<OleRow>("Values", cancellationToken: ct).ToListAsync(ct)).Value);
                }

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
        await Assert.ThrowsAsync<InvalidDataException>(async () => await mutation.UpdateRowsAsync("Values", RowCriteria.All(), new RowValues { ["Id"] = 2 }, ct));
        Assert.Equal(bytes, mutationStream.ToArray());
    }

    [Theory]
    [InlineData(true, 0)]
    [InlineData(false, 0)]
    [InlineData(true, 1)]
    [InlineData(false, 1)]
    [InlineData(true, 2)]
    [InlineData(false, 2)]
    public async Task MissingComplexSlot_RespectsAssignmentAndReportsCorruption(bool strict, int unassignedKind)
    {
        var column = new ColumnInfo { Name = "Value", Type = ColumnType.ComplexType, ColNum = unassignedKind == 2 ? 1 : 0 };
        var definition = new TableDef { Columns = [column] };
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        byte[] page = [1, 0, 2, 0, 0, 0, unassignedKind == 1 ? (byte)0 : (byte)1];
        var typed = RowDecodePlan.CreateTyped(definition, wantedColumns: null, strictParsing: strict);
        var strings = RowDecodePlan.CreateStrings(definition, strictParsing: strict);
        object?[] values = new object?[1];
        await using var messages = new StringWriter(CultureInfo.InvariantCulture);
        using var listener = new TextWriterTraceListener(messages);
        var traceScope = new TestTraceScope(listener);
        try
        {
            if (strict && unassignedKind == 0)
            {
                JetCorruptDataException failure = Assert.Throws<JetCorruptDataException>(() => typed.TryDecodeTypedIntoBuffer(format, page, 0, page.Length, null!, values, out _));
                JetCorruptDataException stringFailure = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await strings.TryDecodeStringRowAsync(format, page, 0, page.Length, null!, TestContext.Current.CancellationToken));
                Assert.Equal(JetErrorCode.MalformedValue, failure.ErrorCode);
                Assert.Equal("Value", failure.ErrorInfo.ColumnName);
                Assert.Equal(failure.Message, stringFailure.Message);
            }
            else
            {
                Assert.True(typed.TryDecodeTypedIntoBuffer(format, page, 0, page.Length, null!, values, out _));
                Assert.Equal(DBNull.Value, values[0]);
                Assert.Equal(string.Empty, Assert.Single((await strings.TryDecodeStringRowAsync(format, page, 0, page.Length, null!, TestContext.Current.CancellationToken))!));
                listener.Flush();
                if (unassignedKind == 0)
                {
                    Assert.Contains("variable-area Complex value for column 'Value' is unreadable", messages.ToString(), StringComparison.Ordinal);
                }
                else
                {
                    Assert.Empty(messages.ToString());
                }
            }

            var writeBack = RowDecodePlan.CreateTypedForWriteBack(definition, strictParsing: false);
            if (unassignedKind == 0)
            {
                Assert.Throws<JetCorruptDataException>(() => writeBack.TryDecodeTypedIntoBuffer(format, page, 0, page.Length, null!, values, out _));
            }
            else
            {
                Assert.True(writeBack.TryDecodeTypedIntoBuffer(format, page, 0, page.Length, null!, values, out _));
                Assert.Equal(DBNull.Value, values[0]);
            }
        }
        finally
        {
            traceScope.Dispose();
        }
    }

    [Theory]
    [InlineData(ColumnType.MemoType)]
    [InlineData(ColumnType.OleType)]
    public async Task MissingLongValueSlot_UnassignedColumnsDoNotEmitDiagnostics(ColumnType type)
    {
        await using var messages = new StringWriter(CultureInfo.InvariantCulture);
        using var listener = new TextWriterTraceListener(messages);
        var traceScope = new TestTraceScope(listener);
        try
        {
            foreach (bool absentColumn in new[] { false, true })
            {
                var definition = new TableDef { Columns = [new ColumnInfo { Name = "Value", Type = type, ColNum = absentColumn ? 1 : 0 }] };
                byte[] page = [1, 0, 2, 0, 0, 0, absentColumn ? (byte)1 : (byte)0];
                var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
                object?[] values = new object?[1];
                Assert.True(RowDecodePlan.CreateTyped(definition, wantedColumns: null, strictParsing: true).TryDecodeTypedIntoBuffer(format, page, 0, page.Length, null!, values, out _));
                Assert.Equal(DBNull.Value, values[0]);
                Assert.True(RowDecodePlan.CreateTypedForWriteBack(definition, strictParsing: false).TryDecodeTypedIntoBuffer(format, page, 0, page.Length, null!, values, out _));
                Assert.Equal(DBNull.Value, values[0]);
                Assert.Equal(string.Empty, Assert.Single((await RowDecodePlan.CreateStrings(definition, strictParsing: true).TryDecodeStringRowAsync(format, page, 0, page.Length, null!, TestContext.Current.CancellationToken))!));
            }

            listener.Flush();
            Assert.Empty(messages.ToString());
        }
        finally
        {
            traceScope.Dispose();
        }
    }

    [Fact]
    public async Task LenientMissingLongValueSlot_ThrowingListenerPreservesMissingValues()
    {
        var definition = new TableDef { Columns = [new ColumnInfo { Name = "Value", Type = ColumnType.MemoType }] };
        byte[] page = [1, 0, 2, 0, 0, 0, 1];
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        object?[] values = new object?[1];
        using var listener = new ThrowingTraceListener();
        var traceScope = new TestTraceScope(listener);
        try
        {
            Assert.True(RowDecodePlan.CreateTyped(definition, wantedColumns: null, strictParsing: false).TryDecodeTypedIntoBuffer(format, page, 0, page.Length, null!, values, out _));
            Assert.Equal(DBNull.Value, values[0]);
            Assert.Null(Assert.Single((await RowDecodePlan.CreateStrings(definition, strictParsing: false).TryDecodeStringRowAsync(format, page, 0, page.Length, null!, TestContext.Current.CancellationToken))!));
        }
        finally
        {
            traceScope.Dispose();
        }
    }

    private static async Task DamageLastVariableSlotAsync(MemoryStream original, byte[] bytes, bool missingSlot, CancellationToken ct)
    {
        original.Position = 0;
        await using AccessReader harness = await TestDatabase.OpenReaderAsync(original, cancellationToken: ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Values", ct));
        TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, ct));
        ColumnInfo column = definition.Columns[2];
        Assert.False(column.IsFixed);
        if (column.Type == ColumnType.OleType)
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
            int rowOffset = checked((int)(number * profile.PageSize)) + bound.RowStart;
            if (missingSlot)
            {
                int offset = rowOffset + layout.NullMaskPos - profile.RowFields.VarLen;
                if (profile.RowFields.VarLen == 1)
                {
                    bytes[offset] = checked((byte)(layout.VarLen - 1));
                }
                else
                {
                    BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(offset, profile.RowFields.VarLen), checked((ushort)(layout.VarLen - 1)));
                }
            }
            else if (profile.RowFields.Eod == 1)
            {
                bytes[rowOffset + layout.VarTableStart - profile.RowFields.Eod] = checked((byte)(slice.DataStart - 1));
            }
            else
            {
                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(rowOffset + layout.VarTableStart - profile.RowFields.Eod, profile.RowFields.Eod), checked((ushort)(slice.DataStart - 1)));
            }

            ReadOnlySpan<byte> damagedPage = bytes.AsSpan(checked((int)(number * profile.PageSize)), profile.PageSize);
            Assert.True(RowDecodePlan.TryParseRowLayout(profile.RowFields, damagedPage, bound.RowStart, bound.RowSize, definition.HasVarColumns, out RowLayout damagedLayout));
            Assert.Equal(ColumnSliceKind.Empty, RowDecodePlan.ResolveColumnSlice(profile.RowFields, damagedPage, bound.RowStart, bound.RowSize, damagedLayout, column).Kind);
            return;
        }

        Assert.Fail("The created database must contain the long-value row.");
    }

    private sealed class ThrowingTraceListener : TraceListener
    {
        public override void Write(string? message) => throw new IOException("diagnostic failure");

        public override void WriteLine(string? message) => throw new IOException("diagnostic failure");
    }

    private sealed class ValueRow
    {
        public object? Value { get; set; }
    }

    private sealed class OleRow
    {
        public byte[]? Value { get; set; }
    }
}
