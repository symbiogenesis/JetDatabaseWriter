namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
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

/// <summary>Native Jet4 BIGBINARY payloads are fixed binary bytes rather than complex references.</summary>
public sealed class NativeBigBinaryTests
{
    private const ColumnType NativeType = ColumnType.BigBinaryType;
    private const int NativeWidth = 3992;

    [Fact]
    public async Task AccessAuthoredBigBinary_CatalogVisibilityAdapter_ReadsEveryStoredByteThroughPublicApis()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        // Public readers resolve user tables only. Adapt catalog visibility in a copy;
        // native table definitions and payload pages remain byte-for-byte unchanged.
        byte[] original = await File.ReadAllBytesAsync(TestDatabases.TestV2000, ct);
        byte[] adapted = (byte[])original.Clone();
        await using (var source = new MemoryStream(original, writable: false))
        await using (ReaderHarness catalogReader = await ReaderHarness.OpenAsync(source, cancellationToken: ct))
        {
            TableDef catalog = Assert.IsType<TableDef>(await catalogReader.ReadTableDefAsync(2, ct));
            var rows = new CatalogRowReader(catalogReader.Database.Format, catalogReader.Database.TableDefs, catalogReader.Database.OwnedPages);
            CatalogRow entry = Assert.Single(await rows.GetCatalogRowsAsync(catalog, ct), catalogRow => catalogRow.Name == "MSysAccessObjects");
            ColumnInfo flags = Assert.IsType<ColumnInfo>(catalog.FindColumn("Flags"));
            byte[] page = await catalogReader.ReadPageCopyAsync(entry.PageNumber, ct);
            RowBound row = DataPageRows.EnumerateLiveRowBounds(catalogReader.Database.Format, page).Single(bound => bound.RowIndex == entry.RowIndex);
            int offset = checked((int)(entry.PageNumber * catalogReader.Database.Format.PageSize)) + row.RowStart + catalogReader.Database.Format.RowFields.NumCols + flags.FixedOff;
            BinaryPrimitives.WriteInt32LittleEndian(adapted.AsSpan(offset, sizeof(int)), 0);
            Assert.Equal(original.AsSpan(0, offset).ToArray(), adapted.AsSpan(0, offset).ToArray());
            Assert.Equal(original.AsSpan(offset + sizeof(int)).ToArray(), adapted.AsSpan(offset + sizeof(int)).ToArray());
        }

        await using var fixture = new MemoryStream(adapted, writable: false);
        var expected = new List<byte[]>();
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(fixture, cancellationToken: ct))
        {
            DatabaseFile database = harness.Database;
            var catalogRows = new CatalogRowReader(database.Format, database.TableDefs, database.OwnedPages);
            long tdefPage = await catalogRows.FindSystemTableTdefPageAsync("MSysAccessObjects", ct);
            Assert.True(tdefPage > 0);
            TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(tdefPage, ct));
            ColumnInfo column = Assert.Single(definition.Columns, c => c.Type == NativeType);
            Assert.Equal("Data", column.Name);
            Assert.Equal(NativeWidth, column.Size);
            Assert.True(column.IsFixed);
            await harness.Database.OwnedPages.ForEachLiveTableRowAsync(
                tdefPage,
                (row, _) =>
                {
                    int offset = row.Location.RowStart + harness.Database.Format.RowFields.NumCols + column.FixedOff;
                    Assert.True(offset + NativeWidth <= row.Location.RowStart + row.Location.RowSize);
                    expected.Add(row.Page.AsSpan(offset, NativeWidth).ToArray());
                    return new ValueTask<bool>(true);
                },
                ct);
        }

        Assert.NotEmpty(expected);
        await using AccessReader reader = await AccessReader.OpenAsync(fixture, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        ColumnMetadata metadata = Assert.Single(await reader.GetColumnMetadataAsync("MSysAccessObjects", ct), c => c.Name == "Data");
        Assert.Equal("Big Binary", metadata.TypeName);
        Assert.Equal(typeof(byte[]), metadata.ClrType);
        Assert.True(metadata.IsFixedLength);
        Assert.Equal(ColumnSize.FromBytes(NativeWidth), metadata.Size);
        using DataTable table = await reader.ReadTableAsync("MSysAccessObjects", cancellationToken: ct);
        Assert.Equal(expected, table.AsEnumerable().Select(row => Assert.IsType<byte[]>(row["Data"])), ByteArrayComparer.Instance);
        Assert.Equal(expected, (await reader.Rows("MSysAccessObjects", cancellationToken: ct).ToListAsync(ct)).Select(row => Assert.IsType<byte[]>(row[0])), ByteArrayComparer.Instance);
        Assert.Equal(expected.Select(Convert.ToHexString), (await reader.RowsAsStrings("MSysAccessObjects", cancellationToken: ct).ToListAsync(ct)).Select(row => row[0]));
        Assert.Equal(expected, (await reader.Rows<BinaryRow>("MSysAccessObjects", cancellationToken: ct).ToListAsync(ct)).Select(row => row.Data), ByteArrayComparer.Instance);
    }

    [Fact]
    public async Task NativeBigBinary_SchemaRewritePreservesDescriptorAndPayload()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] payload = Enumerable.Range(0, NativeWidth).Select(i => unchecked((byte)i)).ToArray();
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("Samples", [new ColumnDefinition("Data", typeof(byte[]), NativeWidth) { ColumnTypeOverride = NativeType }, new ColumnDefinition("Id", typeof(int))], ct);
            await writer.InsertRowAsync("Samples", [payload, 7], ct);
            await writer.AddColumnAsync("Samples", new ColumnDefinition("Extra", typeof(int)), ct);
            await writer.RenameColumnAsync("Samples", "Data", "Payload", ct);
        }

        stream.Position = 0;
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: ct))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Samples", ct));
            TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, ct));
            ColumnInfo column = Assert.Single(definition.Columns, c => c.Name == "Payload");
            Assert.Equal(NativeType, column.Type);
            Assert.Equal(NativeWidth, column.Size);
            Assert.True(column.IsFixed);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        using DataTable table = await reader.ReadTableAsync("Samples", cancellationToken: ct);
        DataRow row = Assert.Single(table.AsEnumerable());
        Assert.Equal(payload, Assert.IsType<byte[]>(row["Payload"]));
        Assert.Equal(7, row["Id"]);
        Assert.Equal(DBNull.Value, row["Extra"]);
    }

    [Theory]
    [InlineData(WriteMode.Direct, false)]
    [InlineData(WriteMode.Direct, true)]
    [InlineData(WriteMode.AutoCommit, false)]
    [InlineData(WriteMode.AutoCommit, true)]
    [InlineData(WriteMode.ExplicitCommit, false)]
    [InlineData(WriteMode.ExplicitCommit, true)]
    public async Task NativeBigBinary_TruncatedSlotRefusesWriteBackWithoutChangingFile(WriteMode mode, bool schemaEdit)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] payload = Enumerable.Range(0, NativeWidth).Select(i => unchecked((byte)i)).ToArray();
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, WriteModes.WriterOptions(mode), leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("Samples", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Data", typeof(byte[]), NativeWidth) { ColumnTypeOverride = NativeType }], ct);
            await writer.InsertRowAsync("Samples", [7, payload], ct);
        }

        stream.Position = 0;
        RowLocation location;
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: ct))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("Samples", ct));
            location = Assert.Single(await harness.Database.GetLiveRowLocationsAsync(entry.TDefPage, ct));
        }

        Assert.False(location.IsOverflow);
        Assert.Equal(0, location.RowIndex);
        byte[] malformedRow = new byte[NativeWidth + 6];
        BinaryPrimitives.WriteUInt16LittleEndian(malformedRow, 2);
        BinaryPrimitives.WriteInt32LittleEndian(malformedRow.AsSpan(2), 7);
        payload.AsSpan(0, NativeWidth - 1).CopyTo(malformedRow.AsSpan(6));
        malformedRow[^1] = 3;
        int pageStart = checked((int)(location.PageNumber * 4096));
        byte[] file = stream.GetBuffer();
        Assert.Equal(1, BinaryPrimitives.ReadUInt16LittleEndian(file.AsSpan(pageStart + 12, 2)));
        file.AsSpan(pageStart + 16, 4096 - 16).Clear();
        int rowStart = 4096 - malformedRow.Length;
        malformedRow.CopyTo(file.AsSpan(pageStart + rowStart));
        BinaryPrimitives.WriteUInt16LittleEndian(file.AsSpan(pageStart + 14, 2), checked((ushort)rowStart));
        BinaryPrimitives.WriteUInt16LittleEndian(file.AsSpan(pageStart + 2, 2), checked((ushort)(rowStart - 16)));
        byte[] before = stream.ToArray();

        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            using DataTable table = await reader.ReadTableAsync("Samples", cancellationToken: ct);
            DataRow row = Assert.Single(table.AsEnumerable());
            Assert.Equal(7, row["Id"]);
            Assert.Equal(DBNull.Value, row["Data"]);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, ct))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
                {
                    JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                    {
                        if (schemaEdit)
                        {
                            await writer.AddColumnAsync("Samples", new ColumnDefinition("Extra", typeof(int)), ct);
                        }
                        else
                        {
                            await writer.UpdateRowsAsync("Samples", "Id", 7, new Dictionary<string, object?> { ["Id"] = 8 }, ct);
                        }
                    });
                    Assert.Equal(JetErrorCode.MalformedValue, failure.ErrorCode);
                    Assert.Equal("Data", failure.ErrorInfo.ColumnName);
                    Assert.NotEmpty(Assert.IsType<string>(failure.ErrorInfo.Reason));
                    Assert.Equal(before, stream.ToArray());
                },
                ct);
        }

        Assert.Equal(before, stream.ToArray());
    }

    [Fact]
    public void NativeBigBinary_TruncatedFixedSlotReturnsMissingValue()
    {
        byte[] row = [1, 0, 42, 0, 0, 0, 1];
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var column = new ColumnInfo { Type = NativeType, Size = NativeWidth, Flags = Constants.ColumnDescriptorFlags.Fixed, ColNum = 0 };
        Assert.Equal(DBNull.Value, JetTypeInfo.ReadFixedTyped(row, start: 2, NativeType, NativeWidth));
        Assert.Empty(JetTypeInfo.ReadFixedString(row, start: 2, NativeType, NativeWidth));
        Assert.True(RowDecodePlan.TryParseRowLayout(format.RowFields, row, rowStart: 0, row.Length, hasVarColumns: false, out RowLayout layout));
        Assert.Equal(ColumnSliceKind.Empty, RowDecodePlan.ResolveColumnSlice(format.RowFields, row, rowStart: 0, row.Length, layout, column).Kind);
    }

    [Fact]
    public void NativeBigBinary_PayloadCannotConsumeTheNullMask()
    {
        byte[] row = new byte[NativeWidth + 2];
        row[0] = 1;
        row[^1] = 1;
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var column = new ColumnInfo { Type = NativeType, Size = NativeWidth, Flags = Constants.ColumnDescriptorFlags.Fixed, ColNum = 0 };
        Assert.True(RowDecodePlan.TryParseRowLayout(format.RowFields, row, rowStart: 0, row.Length, hasVarColumns: false, out RowLayout layout));
        Assert.Equal(ColumnSliceKind.Empty, RowDecodePlan.ResolveColumnSlice(format.RowFields, row, rowStart: 0, row.Length, layout, column).Kind);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(4)]
    [InlineData(4000)]
    public void BigBinaryDescriptor_MalformedWidthIsRefused(int width)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var column = new ColumnInfo { Type = NativeType, Size = width, Flags = Constants.ColumnDescriptorFlags.Fixed, ColNum = 0 };
        Assert.Throws<NotSupportedException>(() => TDefPageBuilder.BuildTableDefinition([new ColumnDefinition("Data", typeof(byte[]), width) { ColumnTypeOverride = NativeType }], format));
        Assert.Equal(DBNull.Value, JetTypeInfo.ReadFixedTyped(new byte[NativeWidth], start: 0, column, width));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void BigBinaryDescriptor_UnsupportedFormatIsRefused(DatabaseFormat databaseFormat)
    {
        var format = JetFormat.ForNewDatabase(databaseFormat);
        Assert.Throws<NotSupportedException>(() => TDefPageBuilder.BuildTableDefinition([new ColumnDefinition("Data", typeof(byte[]), NativeWidth) { ColumnTypeOverride = NativeType }], format));
    }

    [SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Rows<T> instantiates this POCO through its materializer.")]
    private sealed class BinaryRow
    {
        public byte[] Data { get; set; } = [];
    }

    private sealed class ByteArrayComparer : IEqualityComparer<byte[]>
    {
        internal static ByteArrayComparer Instance { get; } = new();

        public bool Equals(byte[]? x, byte[]? y) => ReferenceEquals(x, y) || (x is not null && y is not null && x.AsSpan().SequenceEqual(y));

        public int GetHashCode(byte[] obj) => obj.Length;
    }
}
