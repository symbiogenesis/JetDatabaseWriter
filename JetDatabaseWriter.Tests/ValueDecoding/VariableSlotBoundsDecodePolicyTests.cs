namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
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

/// <summary>Malformed assigned variable slices must not collapse to null; unassigned columns remain missing.</summary>
public sealed class VariableSlotBoundsDecodePolicyTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, true, -1)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, -1)]
    [InlineData(DatabaseFormat.AceAccdb, true, -1)]
    [InlineData(DatabaseFormat.AceAccdb, false, -1)]
    [InlineData(DatabaseFormat.Jet4Mdb, true, 0)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, 0)]
    [InlineData(DatabaseFormat.AceAccdb, true, 0)]
    [InlineData(DatabaseFormat.AceAccdb, false, 0)]
    [InlineData(DatabaseFormat.Jet4Mdb, true, 1)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, 1)]
    [InlineData(DatabaseFormat.AceAccdb, true, 1)]
    [InlineData(DatabaseFormat.AceAccdb, false, 1)]
    public async Task InvalidVariableTextOffsets_PublicReadsRespectAssignment(DatabaseFormat format, bool strict, int unassignedKind)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var original = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(original, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct))
        {
            await writer.CreateTableAsync("Values", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Payload", typeof(byte[])), new ColumnDefinition("Value", typeof(string), maxLength: 50)], ct);
            await writer.InsertRowAsync("Values", [1, new byte[] { 1, 2, 3 }, "visible"], ct);
        }

        byte[] bytes = original.ToArray();
        original.Position = 0;
        await using (AccessReader harness = await TestDatabase.OpenReaderAsync(original, cancellationToken: ct))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Values", ct));
            TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, ct));
            ColumnInfo column = definition.Columns[2];
            Assert.NotNull(DirectRowDecoderBuilder.TryBuild<ValueRow>(definition));
            Assert.NotNull(DirectRowDecoderBuilder.TryBuildHybrid<HybridValueRow>(definition));
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
                ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(profile.RowFields, page, bound.RowStart, bound.RowSize, layout, column);
                Assert.Equal(ColumnSliceKind.Var, slice.Kind);
                int rowOffset = checked((int)(number * profile.PageSize)) + bound.RowStart;
                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(rowOffset + layout.VarTableStart - profile.RowFields.Eod, 2), checked((ushort)(slice.DataStart - 1)));
                if (unassignedKind == 0)
                {
                    int maskOffset = rowOffset + layout.NullMaskPos + (column.ColNum / 8);
                    bytes[maskOffset] &= checked((byte)(0xFF ^ (1 << (column.ColNum % 8))));
                }
                else if (unassignedKind == 1)
                {
                    BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(rowOffset, profile.RowFields.NumCols), checked((ushort)column.ColNum));
                }

                Assert.True(RowDecodePlan.TryParseRowLayout(profile.RowFields, bytes.AsSpan(checked((int)(number * profile.PageSize)), profile.PageSize), bound.RowStart, bound.RowSize, definition.HasVarColumns, out RowLayout damagedLayout));
                Assert.Equal(unassignedKind < 0 ? ColumnSliceKind.Empty : ColumnSliceKind.Null, RowDecodePlan.ResolveColumnSlice(profile.RowFields, bytes.AsSpan(checked((int)(number * profile.PageSize)), profile.PageSize), bound.RowStart, bound.RowSize, damagedLayout, column).Kind);
                damaged = true;
            }

            Assert.True(damaged);
        }

        if (unassignedKind < 0)
        {
            await using var mutationStream = new MemoryStream();
            await mutationStream.WriteAsync(bytes, ct);
            mutationStream.Position = 0;
            await using AccessWriter writer = await AccessWriter.OpenAsync(mutationStream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct);
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await writer.UpdateRowsAsync("Values", RowCriteria.All(), new RowValues { ["Id"] = 2 }, ct));
            Assert.Equal(bytes, mutationStream.ToArray());
        }

        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ct);
        if (strict && unassignedKind < 0)
        {
            JetCorruptDataException strings = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CollectAsync(reader.RowsAsStrings("Values", cancellationToken: ct)));
            JetCorruptDataException typed = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CollectAsync(reader.Rows("Values", cancellationToken: ct)));
            JetCorruptDataException direct = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CollectAsync(reader.Rows<ValueRow>("Values", cancellationToken: ct)));
            JetCorruptDataException hybrid = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CollectAsync(reader.Rows<HybridValueRow>("Values", cancellationToken: ct)));
            JetCorruptDataException table = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.ReadTableAsync("Values", cancellationToken: ct));
            Assert.Equal(JetErrorCode.MalformedValue, strings.ErrorCode);
            Assert.Equal("Value", strings.ErrorInfo.ColumnName);
            Assert.Equal(strings.Message, typed.Message);
            Assert.Equal(typed.Message, direct.Message);
            Assert.Equal(direct.Message, hybrid.Message);
            Assert.Equal(typed.Message, table.Message);
        }
        else
        {
            Assert.Equal(string.Empty, Assert.Single(await CollectAsync(reader.RowsAsStrings("Values", cancellationToken: ct)))[2]);
            Assert.Equal(DBNull.Value, Assert.Single(await CollectAsync(reader.Rows("Values", cancellationToken: ct)))[2]);
            Assert.Null(Assert.Single(await CollectAsync(reader.Rows<ValueRow>("Values", cancellationToken: ct))).Value);
            HybridValueRow hybrid = Assert.Single(await CollectAsync(reader.Rows<HybridValueRow>("Values", cancellationToken: ct)));
            Assert.Null(hybrid.Value);
            Assert.Equal(new byte[] { 1, 2, 3 }, hybrid.Payload);
            Assert.Equal(DBNull.Value, Assert.Single((await reader.ReadTableAsync("Values", cancellationToken: ct)).Rows.Cast<System.Data.DataRow>())[2]);
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

    private sealed class HybridValueRow
    {
        public string? Value { get; set; }

        public byte[]? Payload { get; set; }
    }

    private sealed class ValueRow
    {
        public string? Value { get; set; }
    }
}
