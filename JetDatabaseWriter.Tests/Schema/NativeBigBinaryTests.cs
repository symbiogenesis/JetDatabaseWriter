namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;

/// <summary>Native Jet4 BIGBINARY payloads are fixed binary bytes rather than complex references.</summary>
public sealed class NativeBigBinaryTests
{
    private const ColumnType NativeType = (ColumnType)0x11;
    private const int NativeWidth = 3992;

    [Fact]
    public async Task AccessAuthoredBigBinary_ReadsEveryStoredByteThroughPublicApis()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var expected = new List<byte[]>();
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(TestDatabases.TestV2000, cancellationToken: ct))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("MSysAccessObjects", ct));
            TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, ct));
            ColumnInfo column = Assert.Single(definition.Columns, c => c.Type == NativeType);
            Assert.Equal("Data", column.Name);
            Assert.Equal(NativeWidth, column.Size);
            Assert.True(column.IsFixed);
            await harness.Database.OwnedPages.ForEachLiveTableRowAsync(
                entry.TDefPage,
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
        await using AccessReader reader = await AccessReader.OpenAsync(TestDatabases.TestV2000, new AccessReaderOptions { UseLockFile = false }, ct);
        ColumnMetadata metadata = Assert.Single(await reader.GetColumnMetadataAsync("MSysAccessObjects", ct), c => c.Name == "Data");
        Assert.Equal("Big Binary", metadata.TypeName);
        Assert.Equal(typeof(byte[]), metadata.ClrType);
        Assert.True(metadata.IsFixedLength);
        Assert.Equal(ColumnSize.FromBytes(NativeWidth), metadata.Size);
        using DataTable table = await reader.ReadDataTableAsync("MSysAccessObjects", cancellationToken: ct);
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
        using DataTable table = await reader.ReadDataTableAsync("Samples", cancellationToken: ct);
        DataRow row = Assert.Single(table.AsEnumerable());
        Assert.Equal(payload, Assert.IsType<byte[]>(row["Payload"]));
        Assert.Equal(7, row["Id"]);
        Assert.Equal(DBNull.Value, row["Extra"]);
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
