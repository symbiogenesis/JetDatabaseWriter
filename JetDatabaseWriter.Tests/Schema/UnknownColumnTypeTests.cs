namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using JetDatabaseWriter.ValueEncoding;
using Xunit;

/// <summary>Unknown descriptor codes do not authorize decoding or destructive write-back.</summary>
public sealed class UnknownColumnTypeTests
{
    private const byte UnknownCode = 0x48;

    public static TheoryData<DatabaseFormat, WriteMode, bool> StorageCases
    {
        get
        {
            var cases = new TheoryData<DatabaseFormat, WriteMode, bool>();
            foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
            {
                foreach (WriteMode mode in Enum.GetValues<WriteMode>())
                {
                    cases.Add(format, mode, false);
                    cases.Add(format, mode, true);
                }
            }

            return cases;
        }
    }

    [Theory]
    [MemberData(nameof(StorageCases))]
    public async Task UpdateKnownColumn_UnknownAssignedValue_RefusesWithoutChangingBytes(DatabaseFormat kind, WriteMode mode, bool variable)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateUnknownColumnAsync(kind, variable, ct);
        byte[] original = stream.ToArray();
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, ct);
        await WriteModes.RunAsync(
            writer,
            mode,
            async () =>
            {
                Task UpdateAsync() => writer.UpdateRowsAsync("Samples", "Known", 7, new Dictionary<string, object?> { ["Known"] = 8 }, ct).AsTask();
                JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(UpdateAsync);
                Assert.Contains("0x48", error.Message, StringComparison.Ordinal);
                Assert.Contains("Opaque", error.Message, StringComparison.Ordinal);
                Assert.Equal(original, stream.ToArray());
            },
            ct);
        Assert.Equal(original, stream.ToArray());
    }

    [Theory]
    [MemberData(nameof(StorageCases))]
    public async Task InsertUnknownColumn_NullOrAssigned_RefusesWithoutChangingBytes(DatabaseFormat kind, WriteMode mode, bool variable)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateUnknownColumnAsync(kind, variable, ct);
        byte[] original = stream.ToArray();
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, ct);
        await WriteModes.RunAsync(
            writer,
            mode,
            async () =>
            {
                foreach (object value in new object[] { DBNull.Value, 17 })
                {
                    NotSupportedException error = await Assert.ThrowsAsync<NotSupportedException>(async () =>
                        await writer.InsertRowAsync("Samples", [value, 8], ct));
                    Assert.Contains("0x48", error.Message, StringComparison.Ordinal);
                    Assert.Contains("Opaque", error.Message, StringComparison.Ordinal);
                    Assert.Equal(original, stream.ToArray());
                }
            },
            ct);
        Assert.Equal(original, stream.ToArray());

        // A failed serialization leaves the writer and any caller transaction usable.
        await WriteModes.RunAsync(
            writer,
            mode,
            async () =>
            {
                await Assert.ThrowsAsync<NotSupportedException>(async () => await writer.InsertRowAsync("Samples", [DBNull.Value, 8], ct));
                Assert.Equal(original, stream.ToArray());
                await writer.InsertRowAsync("Control", [8], ct);
            },
            ct);
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        using DataTable control = await reader.ReadTableAsync("Control", cancellationToken: ct);
        Assert.Equal(8, Assert.Single(control.AsEnumerable())["Value"]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task UnknownColumn_RawDescriptorAndPayloadRemainAvailable_ProjectionIsDiagnostic(DatabaseFormat kind, bool variable)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateUnknownColumnAsync(kind, variable, ct);
        byte[] original = stream.ToArray();
        await using (AccessReader harness = await TestDatabase.OpenReaderAsync(stream, cancellationToken: ct))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Samples", ct));
            TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, ct));
            ColumnInfo column = definition.Columns[0];
            Assert.Equal((ColumnType)UnknownCode, column.Type);
            Assert.Equal(UnknownCode, column.RawDescriptor[harness.Database.Format.ColumnDescriptor.TypeOff]);
            Assert.Equal(sizeof(int), column.Size);
            Assert.Equal(!variable, column.IsFixed);
            Assert.Null(JetTypeInfo.GetClrType(column.Type));
            Assert.Equal(typeof(string), JetTypeInfo.ResolveClrType(column));
            bool visited = false;
            await harness.Database.OwnedPages.ForEachLiveTableRowAsync(
                entry.TDefPage,
                (row, _) =>
                {
                    int offset;
                    if (variable)
                    {
                        Assert.True(RowDecodePlan.TryParseRowLayout(harness.Database.Format.RowFields, row.Page, row.Location.RowStart, row.Location.RowSize, hasVarColumns: true, out RowLayout layout));
                        ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(harness.Database.Format.RowFields, row.Page, row.Location.RowStart, row.Location.RowSize, layout, column);
                        Assert.Equal(ColumnSliceKind.Var, slice.Kind);
                        Assert.Equal(sizeof(int), slice.DataLen);
                        offset = row.Location.RowStart + slice.DataStart;
                    }
                    else
                    {
                        offset = row.Location.RowStart + harness.Database.Format.RowFields.NumCols + column.FixedOff;
                    }

                    Assert.Equal(0x12345678, BinaryPrimitives.ReadInt32LittleEndian(row.Page.AsSpan(offset, sizeof(int))));
                    visited = true;
                    return new ValueTask<bool>(true);
                },
                ct);
            Assert.True(visited);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        ColumnMetadata metadata = Assert.Single(await reader.GetColumnMetadataAsync("Samples", ct), column => column.Name == "Opaque");
        Assert.Equal("0x48", metadata.TypeName);
        Assert.Equal(typeof(string), metadata.ClrType);
        Assert.Equal(ColumnSize.FromBytes(sizeof(int)), metadata.Size);
        Assert.Equal(!variable, metadata.IsFixedLength);
        Assert.Equal(original, stream.ToArray());
    }

    [Fact]
    public void DecodePlan_UnselectedUnknownColumn_DoesNotBlockKnownValues()
    {
        var definition = new TableDef
        {
            Columns =
            [
                new ColumnInfo { Name = "Opaque", Type = (ColumnType)UnknownCode, Flags = Constants.ColumnDescriptorFlags.Fixed, Size = sizeof(int), ColNum = 0 },
                new ColumnInfo { Name = "Known", Type = ColumnType.LongIntegerType, Flags = Constants.ColumnDescriptorFlags.Fixed, Size = sizeof(int), ColNum = 1, FixedOff = sizeof(int) },
            ],
        };
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        byte[] row = [2, 0, 0x78, 0x56, 0x34, 0x12, 7, 0, 0, 0, 3];
        var typed = RowDecodePlan.CreateTyped(definition, [false, true], strictParsing: true);
        object?[] buffer = new object?[2];
        Assert.True(typed.TryDecodeTypedIntoBuffer(format, row, 0, row.Length, longValueDecoder: null!, buffer, out bool needsLongValue));
        Assert.False(needsLongValue);
        Assert.Null(buffer[0]);
        Assert.Equal(7, buffer[1]);

        var partial = RowDecodePlan.CreatePartial(definition, [1]);
        object?[] projected = new object?[1];
        Assert.True(partial.TryDecodePartialColumns(format, row, 0, row.Length, projected));
        Assert.Equal(7, Assert.Single(projected));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void SerializeUnknownPhysicalOrCalculatedType_EveryUnknownCodeIsRefusedEvenForNull(bool calculated)
    {
        var encoder = new RowEncoder(JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb));
        for (int code = 0; code <= byte.MaxValue; code++)
        {
            var type = (ColumnType)code;
            if (JetTypeInfo.GetClrType(type) is not null || (calculated && code == 0))
            {
                continue;
            }

            var column = new ColumnInfo
            {
                Name = "Opaque",
                Type = calculated ? ColumnType.LongIntegerType : type,
                CalculatedResultType = calculated ? type : default,
                ExtraFlags = calculated ? Constants.CalculatedColumn.ExtFlagMask : (byte)0,
                Flags = Constants.ColumnDescriptorFlags.Fixed,
                Size = sizeof(int),
            };
            var definition = new TableDef { Columns = [column] };
            NotSupportedException error = Assert.Throws<NotSupportedException>(() => encoder.SerializeRow(definition, [DBNull.Value]));
            Assert.Contains($"0x{code:X2}", error.Message, StringComparison.Ordinal);
            Assert.Contains("Opaque", error.Message, StringComparison.Ordinal);
        }
    }

    private static async Task<MemoryStream> CreateUnknownColumnAsync(DatabaseFormat kind, bool variable, CancellationToken cancellationToken)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, kind, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, cancellationToken))
        {
            await writer.CreateTableAsync("Samples", [new ColumnDefinition("Opaque", typeof(int)) { ForceVariableLengthStorage = variable }, new ColumnDefinition("Known", typeof(int))], cancellationToken);
            await writer.InsertRowAsync("Samples", [0x12345678, 7], cancellationToken);
            await writer.CreateTableAsync("Control", [new ColumnDefinition("Value", typeof(int))], cancellationToken);
        }

        stream.Position = 0;
        await using (AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: cancellationToken))
        {
            ResolvedTable table = await harness.Services.Catalog.ResolveRequiredTableAsync("Samples", cancellationToken);
            long pageNumber = table.Entry.TDefPage;
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(pageNumber, cancellationToken);
            int realIndexes = BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(harness.Database.Format.TDef.NumRealIdx, 4));
            int firstDescriptor = harness.Database.Format.TDef.BlockEnd + (realIndexes * harness.Database.Format.TDef.RealIdxEntrySz);
            page[firstDescriptor + harness.Database.Format.ColumnDescriptor.TypeOff] = UnknownCode;
            await harness.Pager.WritePageAsync(pageNumber, page, cancellationToken);
        }

        stream.Position = 0;
        return stream;
    }
}
