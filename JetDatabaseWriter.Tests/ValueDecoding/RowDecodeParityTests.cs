namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
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

/// <summary>Malformed variable values have the same failure policy on public read surfaces.</summary>
public sealed class RowDecodeParityTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false, false)]
    [InlineData(DatabaseFormat.AceAccdb, false, false, true)]
    [InlineData(DatabaseFormat.AceAccdb, false, false, false)]
    [InlineData(DatabaseFormat.AceAccdb, true, false, true)]
    [InlineData(DatabaseFormat.AceAccdb, true, false, false)]
    [InlineData(DatabaseFormat.AceAccdb, true, true, true)]
    [InlineData(DatabaseFormat.AceAccdb, true, true, false)]
    public async Task MalformedPayload_PublicReadsAgree(DatabaseFormat format, bool calculated, bool invalidEnvelope, bool strict)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var original = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(original, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ct))
        {
            ColumnDefinition column = calculated
                ? new("Value", typeof(int)) { IsCalculated = true, CalculationExpression = "42" }
                : new("Value", typeof(decimal));
            await writer.CreateTableAsync("Values", [column], ct);
            await writer.InsertRowAsync("Values", calculated ? [null] : [42m], ct);
        }

        byte[] bytes = original.ToArray();
        original.Position = 0;
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(original, cancellationToken: ct))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Values", ct));
            TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, ct));
            ColumnInfo column = Assert.Single(definition.Columns);
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
                if (calculated)
                {
                    BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(rowOffset + slice.DataStart + Constants.CalculatedColumn.DataLenOffset, 4), invalidEnvelope ? int.MaxValue : 1);
                }
                else
                {
                    BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(rowOffset + layout.VarTableStart - profile.RowFields.Eod, 2), checked((ushort)(slice.DataStart + 1)));
                }

                damaged = true;
            }

            Assert.True(damaged);
        }

        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ct);
        if (strict)
        {
            InvalidDataException strings = await Assert.ThrowsAsync<InvalidDataException>(async () => await CollectAsync(reader.RowsAsStrings("Values", cancellationToken: ct)));
            InvalidDataException typed = await Assert.ThrowsAsync<InvalidDataException>(async () => await CollectAsync(reader.Rows("Values", cancellationToken: ct)));
            InvalidDataException poco = await Assert.ThrowsAsync<InvalidDataException>(async () => await CollectAsync(reader.Rows<ValueRow>("Values", cancellationToken: ct)));
            Assert.Contains("Value", strings.Message, StringComparison.Ordinal);
            Assert.Equal(strings.Message, typed.Message);
            Assert.Equal(typed.Message, poco.Message);
        }
        else
        {
            Assert.Equal(string.Empty, Assert.Single(await CollectAsync(reader.RowsAsStrings("Values", cancellationToken: ct)))[0]);
            Assert.Equal(DBNull.Value, Assert.Single(await CollectAsync(reader.Rows("Values", cancellationToken: ct)))[0]);
            Assert.Null(Assert.Single(await CollectAsync(reader.Rows<ValueRow>("Values", cancellationToken: ct))).Value);
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

    private sealed class ValueRow
    {
        public decimal? Value { get; set; }
    }
}