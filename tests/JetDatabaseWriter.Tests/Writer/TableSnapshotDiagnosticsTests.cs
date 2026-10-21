namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;

/// <summary>Streaming writer snapshots locate scalar failures without obscuring their cause or I/O failures.</summary>
public sealed class TableSnapshotDiagnosticsTests
{
    private const string TableName = "Items";

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false, OverflowRowLayout.CrossPage)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, OverflowRowLayout.CrossPage)]
    [InlineData(DatabaseFormat.AceAccdb, false, OverflowRowLayout.CrossPage)]
    [InlineData(DatabaseFormat.Jet3Mdb, true, OverflowRowLayout.CrossPage)]
    [InlineData(DatabaseFormat.Jet4Mdb, true, OverflowRowLayout.CrossPage)]
    [InlineData(DatabaseFormat.AceAccdb, true, OverflowRowLayout.CrossPage)]
    [InlineData(DatabaseFormat.AceAccdb, true, OverflowRowLayout.SamePage)]
    [InlineData(DatabaseFormat.AceAccdb, true, OverflowRowLayout.TwoHop)]
    public async Task MalformedSelectedScalar_ReportsPhysicalRowContext(DatabaseFormat format, bool overflow, OverflowRowLayout layout)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] image = await CreateImageAsync(format, ct);
        SyntheticOverflowRow? moved = overflow ? await SyntheticOverflowRows.MoveRowAsync(image, TableName, layout, ct) : null;
        await using var stream = new MemoryStream(image, 0, image.Length, writable: true, publiclyVisible: true);
        RowLocation damaged = await DamageRowAsync(stream, moved, ct);
        byte[] before = stream.ToArray();
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: ct);
        ResolvedTable resolved = Assert.IsType<ResolvedTable>(await harness.Services.Snapshots.ResolveTableAsync(TableName, ct));
        JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await harness.Services.Snapshots.ForEachTypedRowAsync(
                resolved.Entry.TDefPage,
                resolved.Definition,
                [false, false, true],
                (location, _, _) =>
                {
                    Assert.NotEqual(damaged, location);
                    return new ValueTask<bool>(true);
                },
                ct));

        Assert.Equal(JetErrorCode.MalformedValue, failure.ErrorCode);
        Assert.Equal(TableName, failure.ErrorInfo.TableName);
        Assert.Equal("Value", failure.ErrorInfo.ColumnName);
        Assert.Equal(damaged.DataPageNumber, failure.ErrorInfo.PageNumber);
        string reason = Assert.IsType<string>(failure.ErrorInfo.Reason);
        Assert.Contains($"Row slot {damaged.DataRowIndex} at offset {damaged.RowStart} occupies {damaged.RowSize} bytes.", reason, StringComparison.Ordinal);
        JetCorruptDataException scalar = Assert.IsType<JetCorruptDataException>(failure.InnerException!.InnerException);
        Assert.Null(scalar.ErrorInfo.PageNumber);
        Assert.Equal(scalar.Message, failure.Message);
        Assert.EndsWith(Assert.IsType<string>(scalar.ErrorInfo.Reason), reason, StringComparison.Ordinal);
        Assert.IsType<ArgumentException>(scalar.InnerException, exactMatch: false);
        JetCorruptDataException buffered = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await harness.Services.Snapshots.ReadRowsAsync(resolved.Entry.TDefPage, ct));
        Assert.Equal(failure.ErrorInfo, buffered.ErrorInfo);
        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [InlineData(OverflowRowLayout.CrossPage)]
    [InlineData(OverflowRowLayout.SamePage)]
    public async Task UndecodableOverflowLayout_ReportsPhysicalRowContext(OverflowRowLayout layout)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] image = await CreateImageAsync(DatabaseFormat.AceAccdb, ct);
        SyntheticOverflowRow moved = await SyntheticOverflowRows.MoveRowAsync(image, TableName, layout, ct);
        await using var stream = new MemoryStream(image, 0, image.Length, writable: true, publiclyVisible: true);
        RowLocation damaged = await DamageRowAsync(stream, moved, ct, invalidLayout: true);
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: ct);
        ResolvedTable resolved = Assert.IsType<ResolvedTable>(await harness.Services.Snapshots.ResolveTableAsync(TableName, ct));
        JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await harness.Services.Snapshots.ForEachTypedRowAsync(resolved.Entry.TDefPage, resolved.Definition, [false, false, true], (_, _, _) => new ValueTask<bool>(true), ct));
        Assert.Equal(JetErrorCode.MalformedValue, failure.ErrorCode);
        Assert.Equal(TableName, failure.ErrorInfo.TableName);
        Assert.Equal(damaged.DataPageNumber, failure.ErrorInfo.PageNumber);
        Assert.Contains($"Row slot {damaged.DataRowIndex} at offset {damaged.RowStart}", Assert.IsType<string>(failure.ErrorInfo.Reason), StringComparison.Ordinal);
    }

    [Fact]
    public async Task UnreadableOverflowLongValue_ReportsPhysicalPage()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] image = await SyntheticOverflowRows.CreateTableAsync(DatabaseFormat.AceAccdb, TableName, 80, primaryKey: false, [new ColumnDefinition("Value", typeof(string)) { ColumnTypeOverride = ColumnType.MemoType }], _ => ["stored content"], ct);
        SyntheticOverflowRow moved = await SyntheticOverflowRows.MoveRowAsync(image, TableName, OverflowRowLayout.CrossPage, ct);
        await using var stream = new MemoryStream(image, 0, image.Length, writable: true, publiclyVisible: true);
        RowLocation damaged = await DamageRowAsync(stream, moved, ct);
        byte[] before = stream.ToArray();
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: ct);
        ResolvedTable resolved = Assert.IsType<ResolvedTable>(await harness.Services.Snapshots.ResolveTableAsync(TableName, ct));
        JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await harness.Services.Snapshots.ForEachTypedRowAsync(resolved.Entry.TDefPage, resolved.Definition, [false, false, true], (_, _, _) => new ValueTask<bool>(true), ct));
        Assert.Equal(JetErrorCode.UnreadableLongValue, failure.ErrorCode);
        Assert.Equal("Value", failure.ErrorInfo.ColumnName);
        Assert.Equal(damaged.DataPageNumber, failure.ErrorInfo.PageNumber);
        Assert.DoesNotContain("stored content", failure.Message, StringComparison.Ordinal);
        Assert.Equal(before, stream.ToArray());
    }

    [Fact]
    public async Task UnselectedMalformedScalar_PreservesProjectionAndCallbackContext()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] image = await CreateImageAsync(DatabaseFormat.AceAccdb, ct);
        await using var stream = new MemoryStream(image, 0, image.Length, writable: true, publiclyVisible: true);
        _ = await DamageRowAsync(stream, moved: null, ct);
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: ct);
        ResolvedTable resolved = Assert.IsType<ResolvedTable>(await harness.Services.Snapshots.ResolveTableAsync(TableName, ct));
        var expected = new JetCorruptDataException(JetErrorCode.MalformedValue, "callback failure", new JetErrorInfo { TableName = "Callback", PageNumber = 999, Reason = "original reason" });
        JetCorruptDataException actual = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await harness.Services.Snapshots.ForEachTypedRowAsync(
                resolved.Entry.TDefPage,
                resolved.Definition,
                [true, false, false],
                (_, values, _) =>
                {
                    Assert.Equal(1, values[0]);
                    Assert.Null(values[2]);
                    throw expected;
                },
                ct));
        Assert.Same(expected, actual);
    }

    [Fact]
    public async Task RealIoFailure_PreservesOriginalException()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] image = await CreateImageAsync(DatabaseFormat.AceAccdb, ct);
        await using var stream = new FailingReadStream(image);
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: ct);
        ResolvedTable resolved = Assert.IsType<ResolvedTable>(await harness.Services.Snapshots.ResolveTableAsync(TableName, ct));
        stream.FailReads = true;
        IOException actual = await Assert.ThrowsAsync<IOException>(async () =>
            await harness.Services.Snapshots.ForEachTypedRowAsync(resolved.Entry.TDefPage, resolved.Definition, [false, false, true], (_, _, _) => new ValueTask<bool>(true), ct));
        Assert.Same(stream.Failure, actual);
    }

    private static ValueTask<byte[]> CreateImageAsync(DatabaseFormat format, CancellationToken ct)
        => SyntheticOverflowRows.CreateTableAsync(format, TableName, 80, primaryKey: false, [new ColumnDefinition("Value", typeof(DateTime))], _ => [new DateTime(2026, 1, 1)], ct);

    private static async Task<RowLocation> DamageRowAsync(MemoryStream stream, SyntheticOverflowRow? moved, CancellationToken ct, bool invalidLayout = false)
    {
        stream.Position = 0;
        RowLocation location;
        await using (AccessReader harness = await TestDatabase.OpenReaderAsync(stream, cancellationToken: ct))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync(TableName, ct));
            TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, ct));
            RowLocation[] locations = [.. await harness.Database.GetLiveRowLocationsAsync(entry.TDefPage, ct)];
            location = moved is SyntheticOverflowRow overflow
                ? Assert.Single(locations, row => row.PageNumber == overflow.HeaderPage && row.RowIndex == overflow.HeaderRow)
                : locations[0];
            Assert.Equal(moved.HasValue, location.IsOverflow);
            byte[] page = await harness.ReadPageCopyAsync(location.DataPageNumber, ct);
            JetFormat profile = harness.Database.Format;
            Assert.True(RowDecodePlan.TryParseRowLayout(profile.RowFields, page, location.RowStart, location.RowSize, definition.HasVarColumns, out RowLayout rowLayout));
            ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(profile.RowFields, page, location.RowStart, location.RowSize, rowLayout, definition.Columns[2]);
            int valueOffset = checked((int)(location.DataPageNumber * profile.PageSize)) + location.RowStart + slice.DataStart;
            if (invalidLayout)
            {
                int rowOffset = checked((int)(location.DataPageNumber * profile.PageSize)) + location.RowStart;
                stream.GetBuffer().AsSpan(rowOffset, profile.RowFields.NumCols).Clear();
            }
            else if (definition.Columns[2].Type == ColumnType.MemoType)
            {
                stream.GetBuffer()[valueOffset + 3] |= Constants.LongValue.StorageModeMask;
            }
            else
            {
                BinaryPrimitives.WriteInt64LittleEndian(stream.GetBuffer().AsSpan(valueOffset, sizeof(double)), BitConverter.DoubleToInt64Bits(double.NaN));
            }
        }

        stream.Position = 0;
        return location;
    }

    private sealed class FailingReadStream(byte[] bytes) : MemoryStream(bytes)
    {
        internal IOException Failure { get; } = new("disk failure");

        internal bool FailReads { get; set; }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => this.FailReads ? ValueTask.FromException<int>(this.Failure) : base.ReadAsync(buffer, cancellationToken);
    }
}
