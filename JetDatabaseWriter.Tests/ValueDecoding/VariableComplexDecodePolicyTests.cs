namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Linq;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;

/// <summary>Synthetic ACE variable Complex slots follow the public unreadable-value policy.</summary>
public sealed class VariableComplexDecodePolicyTests
{
    [Theory]
    [InlineData(true, -2, 1)]
    [InlineData(false, -2, 1)]
    [InlineData(true, -1, 1)]
    [InlineData(false, -1, 1)]
    [InlineData(true, 0, 1)]
    [InlineData(false, 0, 1)]
    [InlineData(true, 1, 1)]
    [InlineData(false, 1, 1)]
    [InlineData(true, 3, 1)]
    [InlineData(false, 3, 1)]
    [InlineData(true, 5, 1)]
    [InlineData(false, 5, 1)]
    [InlineData(true, 4, 0)]
    [InlineData(false, 4, 0)]
    [InlineData(true, 4, -1)]
    [InlineData(false, 4, -1)]
    [InlineData(true, 4, int.MinValue)]
    [InlineData(false, 4, int.MinValue)]
    public async Task MalformedAssignedSlot_PublicReadsAndWriteBackRespectPolicy(bool strict, int slotLength, int reference)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream original = await CreateFixtureAsync(withItem: true, ct);
        byte[] bytes = original.ToArray();
        await DamageSlotAsync(original, bytes, slotLength, reference, unassignedKind: 0, ct);

        await using (var mutationStream = new MemoryStream())
        {
            await mutationStream.WriteAsync(bytes, ct);
            mutationStream.Position = 0;
            await using AccessWriter writer = await AccessWriter.OpenAsync(mutationStream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct);
            JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await writer.UpdateRowsAsync("Values", RowCriteria.All(), new RowValues { ["Id"] = 2 }, ct));
            Assert.Equal(JetErrorCode.MalformedValue, failure.ErrorCode);
            Assert.Equal("Value", failure.ErrorInfo.ColumnName);
            Assert.Equal(bytes, mutationStream.ToArray());
        }

        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await OpenAsync(stream, strict, ct);
        if (strict)
        {
            JetCorruptDataException typed = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CollectAsync(reader.Rows("Values", cancellationToken: ct)));
            JetCorruptDataException strings = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CollectAsync(reader.RowsAsStrings("Values", cancellationToken: ct)));
            JetCorruptDataException poco = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CollectAsync(reader.Rows<ValueRow>("Values", cancellationToken: ct)));
            JetCorruptDataException table = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.ReadTableAsync("Values", cancellationToken: ct));
            JetCorruptDataException stringTable = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.ReadTableAsStringsAsync("Values", cancellationToken: ct));
            JetCorruptDataException query = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.Query<ValueRow>("Values").Where(row => row.Id == 1).ToListAsync(ct));
            JetCorruptDataException seek = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CollectAsync(reader.SeekRowsAsync("Values", "PrimaryKey", [1], ct)));
            JetCorruptDataException typedSeek = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CollectAsync(reader.FromIndex<ValueRow>("Values", "PrimaryKey").WhereEquals(1).ToRowsAsync(ct)));
            Assert.Equal(JetErrorCode.MalformedValue, typed.ErrorCode);
            Assert.Equal("Value", typed.ErrorInfo.ColumnName);
            Assert.Equal(typed.Message, strings.Message);
            Assert.Equal(typed.Message, poco.Message);
            Assert.Equal(typed.Message, table.Message);
            Assert.Equal(typed.Message, stringTable.Message);
            Assert.Equal(typed.Message, query.Message);
            Assert.Equal(typed.Message, seek.Message);
            Assert.Equal(typed.Message, typedSeek.Message);
        }
        else
        {
            await using var messages = new StringWriter(CultureInfo.InvariantCulture);
            using var listener = new TextWriterTraceListener(messages);
            var traceScope = new TestTraceScope(listener);
            try
            {
                await AssertMissingAsync(reader, ct);
                listener.Flush();
                string diagnostic = messages.ToString();
                Assert.Contains("variable-area Complex value for column 'Value' is unreadable", diagnostic, StringComparison.Ordinal);
                Assert.Contains("missing value", diagnostic, StringComparison.Ordinal);
                Assert.DoesNotContain("payload", diagnostic, StringComparison.Ordinal);
                Assert.DoesNotContain("-2147483648", diagnostic, StringComparison.Ordinal);
            }
            finally
            {
                traceScope.Dispose();
            }
        }
    }

    [Theory]
    [InlineData(true, 0)]
    [InlineData(false, 0)]
    [InlineData(true, 1)]
    [InlineData(false, 1)]
    [InlineData(true, 2)]
    [InlineData(false, 2)]
    public async Task ValidEmptyAndUnassignedSlot_PublicReadsPreserveMissingWithoutDiagnostic(bool strict, int unassignedKind)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream original = await CreateFixtureAsync(withItem: false, ct);
        byte[] bytes = original.ToArray();
        await DamageSlotAsync(original, bytes, unassignedKind == 0 ? 4 : -1, reference: 1, unassignedKind, ct);
        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await OpenAsync(stream, strict, ct);
        await using var messages = new StringWriter(CultureInfo.InvariantCulture);
        using var listener = new TextWriterTraceListener(messages);
        var traceScope = new TestTraceScope(listener);
        try
        {
            await AssertMissingAsync(reader, ct);
            listener.Flush();
            Assert.Empty(messages.ToString());
        }
        finally
        {
            traceScope.Dispose();
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ValidVariableReference_PublicReadsJoinComplexItems(bool strict)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateFixtureAsync(withItem: true, ct);
        await using AccessReader reader = await OpenAsync(stream, strict, ct);
        byte[] value = Assert.IsType<byte[]>(Assert.Single(await CollectAsync(reader.Rows("Values", cancellationToken: ct)))[1]);
        AttachmentRecord item = Assert.Single(ComplexCellValue.ReadAttachments(value));
        Assert.Equal("value.txt", item.FileName);
        Assert.Equal(new byte[] { 11, 22, 33 }, item.FileData);
        Assert.Equal(value, Assert.Single(await CollectAsync(reader.Rows<ValueRow>("Values", cancellationToken: ct))).Value);
        Assert.Equal("data:application/octet-stream;base64," + Convert.ToBase64String(value), Assert.Single(await CollectAsync(reader.RowsAsStrings("Values", cancellationToken: ct)))[1]);
    }

    private static async Task<MemoryStream> CreateFixtureAsync(bool withItem, CancellationToken ct)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct))
        {
            // Native fixtures have fixed references. Force variable storage only
            // to exercise public parsing with a complete writer-created complex catalog.
            var column = new ColumnDefinition("Value", typeof(byte[]))
            {
                IsAttachment = true,
                ForceVariableLengthStorage = true,
                DescriptorFlagsOverride = Constants.ColumnDescriptorFlags.ComplexColumn & ~Constants.ColumnDescriptorFlags.Fixed,
            };
            var payload = new ColumnDefinition("Payload", typeof(byte[]), maxLength: 8) { ColumnTypeOverride = ColumnType.BinaryType, ForceVariableLengthStorage = true };
            await writer.CreateTableAsync("Values", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, column, payload], ct);
            await writer.InsertRowAsync("Values", [1, DBNull.Value, new byte[] { 101, 102, 0, 104 }], ct);
            if (withItem)
            {
                await writer.AddAttachmentAsync("Values", "Value", new Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("value.txt", [11, 22, 33]), ct);
            }
        }

        stream.Position = 0;
        return stream;
    }

    private static async Task DamageSlotAsync(MemoryStream original, byte[] bytes, int slotLength, int reference, int unassignedKind, CancellationToken ct)
    {
        original.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(original, cancellationToken: ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Values", ct));
        TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, ct));
        ColumnInfo column = definition.Columns[1];
        Assert.Equal(ColumnType.ComplexType, column.Type);
        Assert.False(column.IsFixed);
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
            Assert.Equal(4, slice.DataLen);
            int rowOffset = checked((int)(number * profile.PageSize)) + bound.RowStart;
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(rowOffset + slice.DataStart, 4), reference);
            if (slotLength == -2)
            {
                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(rowOffset + layout.NullMaskPos - profile.RowFields.VarLen, 2), 0);
            }
            else
            {
                int nextEntry = rowOffset + layout.VarTableStart + ((layout.VarLen - 2 - column.VarIdx) * profile.RowFields.VarEntry);
                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(nextEntry, 2), checked((ushort)(slice.DataStart + slotLength)));
            }

            if (unassignedKind == 1)
            {
                int maskOffset = rowOffset + layout.NullMaskPos + (column.ColNum / 8);
                bytes[maskOffset] &= checked((byte)(0xFF ^ (1 << (column.ColNum % 8))));
            }
            else if (unassignedKind == 2)
            {
                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(rowOffset, profile.RowFields.NumCols), checked((ushort)column.ColNum));
            }

            return;
        }

        Assert.Fail("The fixture must contain the parent row.");
    }

    private static ValueTask<AccessReader> OpenAsync(MemoryStream stream, bool strict, CancellationToken ct)
        => AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ct);

    private static async Task AssertMissingAsync(AccessReader reader, CancellationToken ct)
    {
        Assert.Equal(DBNull.Value, Assert.Single(await CollectAsync(reader.Rows("Values", cancellationToken: ct)))[1]);
        Assert.Equal(string.Empty, Assert.Single(await CollectAsync(reader.RowsAsStrings("Values", cancellationToken: ct)))[1]);
        Assert.Null(Assert.Single(await CollectAsync(reader.Rows<ValueRow>("Values", cancellationToken: ct))).Value);
        Assert.Null(Assert.Single(await reader.Query<ValueRow>("Values").Where(row => row.Id == 1).ToListAsync(ct)).Value);
        Assert.Equal(DBNull.Value, Assert.Single(await CollectAsync(reader.SeekRowsAsync("Values", "PrimaryKey", [1], ct)))[1]);
        Assert.Null(Assert.Single(await CollectAsync(reader.FromIndex<ValueRow>("Values", "PrimaryKey").WhereEquals(1).ToRowsAsync(ct))).Value);
        using DataTable table = await reader.ReadTableAsync("Values", cancellationToken: ct);
        Assert.Equal(DBNull.Value, Assert.Single(table.Rows.Cast<DataRow>())[1]);
        using DataTable stringTable = await reader.ReadTableAsStringsAsync("Values", cancellationToken: ct);
        Assert.Equal(string.Empty, Assert.Single(stringTable.Rows.Cast<DataRow>())[1]);
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

#pragma warning disable CA1812 // Public row materialization constructs the projection target.
    private sealed class ValueRow
    {
        public int Id { get; set; }

        public byte[]? Value { get; set; }
    }
#pragma warning restore CA1812
}
