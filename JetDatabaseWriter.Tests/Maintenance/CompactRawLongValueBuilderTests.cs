namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;

public sealed class CompactRawLongValueBuilderTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("Employees", "Notes")]
    [InlineData("Employees", "Photo")]
    [InlineData("Categories", "Picture")]
    public async Task Build_AccessAuthoredJet3PackedAndChainedRowsKeepExactRawContents(string tableName, string columnName)
    {
        await using AccessReader reader = await TestDatabases.OpenAsync(TestDatabases.MdbtoolsNwind, cancellationToken: Ct);
        DatabaseFile database = reader.Database;
        CatalogEntry? entry = await reader.GetCatalogEntryAsync(tableName, Ct);
        Assert.NotNull(entry);
        TableDef definition = Assert.IsType<TableDef>(await database.TableDefs.ReadTableDefAsync(entry.TDefPage, Ct));
        ColumnInfo column = Assert.Single(definition.Columns, item => item.Name == columnName);
        bool hasVariable = definition.Columns.Any(item => !item.IsFixed);
        int copied = 0;
        long copiedRows = 0;
        await database.OwnedPages.ForEachLiveTableRowAsync(
            entry.TDefPage,
            async (stored, token) =>
        {
            RowLocation location = stored.Location;
            Assert.True(RowDecodePlan.TryParseRowLayout(database.Format.RowFields, stored.Page, location.RowStart, location.RowSize, hasVariable, out RowLayout layout));
            ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(database.Format.RowFields, stored.Page, location.RowStart, location.RowSize, layout, column);
            if (slice.Kind is ColumnSliceKind.Fixed or ColumnSliceKind.Var)
            {
                ReadOnlyMemory<byte> original = stored.Page.AsMemory(location.RowStart + slice.DataStart, slice.DataLen);
                Assert.True(LongValueDescriptor.TryRead(original.Span, out LongValueDescriptor descriptor));
                var expected = new List<byte[]>();
                var discovery = new CompactDiscoverySession(database.Pages, database.Format, new());
                await discovery.AuditLongValueAsync(
                    original,
                    (record, _) =>
                {
                    if (record.Kind == "lval-row")
                    {
                        expected.Add(record.RawBytes.ToArray());
                    }

                    return ValueTask.CompletedTask;
                },
                    token);
                await using var destination = new MemoryStream();
                destination.SetLength(3L * database.Format.PageSize);
                CompactRawLongValueBuilderResult result = await CompactRawLongValueBuilder.BuildAsync(discovery, database.Format, original, destination, new(), token);
                List<byte[]> actual = await ReadRowsAsync(new ImagePages(DatabaseFormat.Jet3Mdb, destination.ToArray()), result.Column);
                Assert.Equal(expected.Count, actual.Count);
                int prefix = descriptor.UsesChainedPages ? 4 : 0;
                for (int i = 0; i < actual.Count; i++)
                {
                    Assert.Equal<byte>(expected[i].AsSpan(prefix), actual[i].AsSpan(prefix));
                }

                Assert.Equal<byte>(original.Span[..4], result.Column.Span[..4]);
                Assert.Equal<byte>(original.Span[8..], result.Column.Span[8..]);
                copied++;
                copiedRows += result.RowCount;
            }

            return true;
        },
            Ct);
        Assert.True(copied > 0);
        Assert.True(copiedRows > 0);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Build_RelocatesWholeChainIncludingTrailingStorageWithoutDecoding(DatabaseFormat kind)
    {
        var source = new ImagePages(kind, 9);
        source.SetRows(3, Chain(0x701, [0xFE, 0xFF, 0x80]));
        source.SetRows(7, [0xAA], Chain(0x800, [0xEE]));
        source.SetRows(8, Chain(0, [0x99, 0x98]));
        byte[] column = Column(LongValueDescriptor.Chained(2, 0x300, 0xFEEDABCD), [0xCC, 0xDD]);
        byte[] originalColumn = (byte[])column.Clone();
        byte[][] originalPages = Array.ConvertAll(source.Pages, static page => (byte[])page.Clone());
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(5L * source.PageSize);

        CompactRawLongValueBuilderResult result = await CompactRawLongValueBuilder.BuildAsync(session, source.Format, column, destination, new(), Ct);

        Assert.False(result.IsValidated);
        Assert.False(result.OwnershipProven);
        Assert.Equal(5, result.FirstPageNumber);
        Assert.Equal(1, result.PageCount);
        Assert.Equal(3, result.RowCount);
        Assert.Equal(column.Length + 18, result.RawBytes);
        Assert.Equal<byte>(column.AsSpan(0, 4), result.Column.Span[..4]);
        Assert.Equal<byte>(column.AsSpan(8), result.Column.Span[8..]);
        Assert.Equal(0x500U, BinaryPrimitives.ReadUInt32LittleEndian(result.Column.Span[4..]));
        var staged = new ImagePages(kind, destination.ToArray());
        List<byte[]> rows = await ReadRowsAsync(staged, result.Column);
        Assert.Equal(3, rows.Count);
        Assert.Equal(Chain(0x501, [0xFE, 0xFF, 0x80]), rows[0]);
        Assert.Equal(Chain(0x502, [0xEE]), rows[1]);
        Assert.Equal(Chain(0, [0x99, 0x98]), rows[2]);
        Assert.Equal(originalColumn, column);
        for (int i = 0; i < originalPages.Length; i++)
        {
            Assert.Equal(originalPages[i], source.Pages[i]);
        }

        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Build_PatchesPreviousPageBeforeFlushingAndKeepsMaximumRawRows(DatabaseFormat kind)
    {
        var source = new ImagePages(kind, 5);
        byte[] first = new byte[source.PageSize - source.Format.DataPage.RowsStart - 2];
        first.AsSpan().Fill(0xFE);
        BinaryPrimitives.WriteUInt32LittleEndian(first, 0x400);
        source.SetRows(3, first);
        source.SetRows(4, Chain(0, [0xFF]));
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(5L * source.PageSize);

        CompactRawLongValueBuilderResult result = await CompactRawLongValueBuilder.BuildAsync(session, source.Format, Column(LongValueDescriptor.Chained(1, 0x300, 7)), destination, new(), Ct);

        Assert.Equal(2, result.PageCount);
        List<byte[]> rows = await ReadRowsAsync(new ImagePages(kind, destination.ToArray()), result.Column);
        BinaryPrimitives.WriteUInt32LittleEndian(first, 0x600);
        Assert.Equal(first, rows[0]);
        Assert.Equal(Chain(0, [0xFF]), rows[1]);
    }

    [Fact]
    public async Task Build_RowSlotsRespectNativeLimitWithoutDroppingTrailingRows()
    {
        var source = new ImagePages(DatabaseFormat.Jet4Mdb, 260);
        for (int i = 3; i < 260; i++)
        {
            source.SetRows(i, Chain(i == 259 ? 0U : checked((uint)(i + 1) << 8), []));
        }

        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(3L * source.PageSize);
        CompactRawLongValueBuilderResult result = await CompactRawLongValueBuilder.BuildAsync(session, source.Format, Column(LongValueDescriptor.Chained(0, 0x300, 0)), destination, new(), Ct);

        Assert.Equal(257, result.RowCount);
        Assert.Equal(2, result.PageCount);
        Assert.Equal(257, (await ReadRowsAsync(new ImagePages(DatabaseFormat.Jet4Mdb, destination.ToArray()), result.Column)).Count);
        Assert.Equal(255, BinaryPrimitives.ReadUInt16LittleEndian(destination.ToArray().AsSpan((3 * source.PageSize) + source.Format.DataPage.NumRows)));
    }

    [Fact]
    public async Task Build_ZeroLengthSingleRowKeepsTrailingBytesAndToken()
    {
        var source = new ImagePages(DatabaseFormat.Jet4Mdb, 4);
        source.SetRows(3, [0xFE, 0xFF, 0x80]);
        byte[] column = Column(LongValueDescriptor.SinglePage(0, 0x300, 0xDEADBEEF), [0xFA]);
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(6L * source.PageSize);

        CompactRawLongValueBuilderResult result = await CompactRawLongValueBuilder.BuildAsync(session, source.Format, column, destination, new(), Ct);

        Assert.Equal(0x600U, BinaryPrimitives.ReadUInt32LittleEndian(result.Column.Span[4..]));
        Assert.Equal<byte>(column.AsSpan(8), result.Column.Span[8..]);
        List<byte[]> rows = await ReadRowsAsync(new ImagePages(DatabaseFormat.Jet4Mdb, destination.ToArray()), result.Column);
        Assert.Equal<byte>([0xFE, 0xFF, 0x80], Assert.Single(rows));
    }

    [Fact]
    public async Task Build_InlineAndNullExternalKeepWholeColumnWithoutAppendingPages()
    {
        var source = new ImagePages(DatabaseFormat.Jet4Mdb, 4);
        foreach (byte[] column in (byte[][])[Column(new LongValueDescriptor(1, Constants.LongValue.InlineStorageMode, 0xFEFEFEFE, 0xDDDDDDDD), [0xFE, 0x80]), Column(LongValueDescriptor.Chained(0, 0, 0xAABBCCDD), [0xAA])])
        {
            var session = new CompactDiscoverySession(source, source.Format, new());
            await using var destination = new MemoryStream();
            destination.SetLength(3L * source.PageSize);
            CompactRawLongValueBuilderResult result = await CompactRawLongValueBuilder.BuildAsync(session, source.Format, column, destination, new() { MaxRows = 0, MaxPages = 0, MaxOutputBytes = 0 }, Ct);
            Assert.Equal(column, result.Column.ToArray());
            Assert.Equal(0, result.PageCount);
            Assert.Equal(0, result.RowCount);
            Assert.Equal(3L * source.PageSize, destination.Length);
        }

        Assert.Equal(0, source.Reads);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public async Task Build_LimitsRefuseAndReleaseStorage(int kind)
    {
        var source = new ImagePages(DatabaseFormat.Jet4Mdb, 5);
        source.SetRows(3, Chain(0x400, [1]));
        source.SetRows(4, Chain(0, [2]));
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(3L * source.PageSize);
        CompactRawLongValueBuilderBudget budget = kind switch
        {
            0 => new() { MaxWorkingBytes = 1 },
            1 => new() { MaxRows = 1 },
            2 => new() { MaxPages = 0 },
            3 => new() { MaxOutputBytes = 1 },
            _ => new() { MaxRawBytes = 21 },
        };

        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactRawLongValueBuilder.BuildAsync(session, source.Format, Column(LongValueDescriptor.Chained(1, 0x300, 0)), destination, budget, Ct));

        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
        Assert.Equal(new byte[3 * source.PageSize], destination.ToArray().AsSpan(0, 3 * source.PageSize).ToArray());
        if (kind == 0)
        {
            Assert.Equal(0, source.Reads);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Build_TrailingCycleOrDanglingPointerCannotProduceAResult(bool cycle)
    {
        var source = new ImagePages(DatabaseFormat.Jet4Mdb, 4);
        source.SetRows(3, Chain(cycle ? 0x300U : 0xFF00U, [1]));
        var session = new CompactDiscoverySession(source, source.Format, new());
        await using var destination = new MemoryStream();
        destination.SetLength(3L * source.PageSize);

        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactRawLongValueBuilder.BuildAsync(session, source.Format, Column(LongValueDescriptor.Chained(1, 0x300, 0)), destination, new(), Ct));

        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task Build_FlushFailurePropagatesAndCancellationNeverReturnsCompletion()
    {
        var source = new ImagePages(DatabaseFormat.Jet4Mdb, 4);
        source.SetRows(3, Chain(0, [1]));
        var session = new CompactDiscoverySession(source, source.Format, new());
        var failure = new IOException("private LVAL flush failed");
        await using var destination = new FlushFailureStream(failure);
        destination.SetLength(3L * source.PageSize);
        byte[] column = Column(LongValueDescriptor.Chained(1, 0x300, 0));
        IOException actual = await Assert.ThrowsAsync<IOException>(async () => await CompactRawLongValueBuilder.BuildAsync(session, source.Format, column, destination, new(), Ct));
        Assert.Same(failure, actual);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);

        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await using var cancelling = new CancellingStream(cancellation);
        cancelling.SetLength(3L * source.PageSize);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactRawLongValueBuilder.BuildAsync(session, source.Format, column, cancelling, new(), cancellation.Token));
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task Build_InvalidAdmissionAndPreCancellationReadNothing()
    {
        var source = new ImagePages(DatabaseFormat.Jet4Mdb, 4);
        var session = new CompactDiscoverySession(source, source.Format, new());
        byte[] column = Column(LongValueDescriptor.Inline(0));
        await using var destination = new MemoryStream();
        destination.SetLength(3L * source.PageSize);
        await Assert.ThrowsAsync<ArgumentException>(async () => await CompactRawLongValueBuilder.BuildAsync(session, JetFormat.ForNewDatabase(DatabaseFormat.Jet3Mdb), column, destination, new(), Ct));
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactRawLongValueBuilder.BuildAsync(session, source.Format, column, destination, new(), cancellation.Token));
        destination.SetLength(destination.Length - 1);
        await Assert.ThrowsAsync<ArgumentException>(async () => await CompactRawLongValueBuilder.BuildAsync(session, source.Format, column, destination, new(), Ct));
        Assert.Equal(0, source.Reads);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task Build_RepeatedValuesConsumeSharedReadAllowance()
    {
        var source = new ImagePages(DatabaseFormat.Jet4Mdb, 4);
        source.SetRows(3, Chain(0, [1]));
        var session = new CompactDiscoverySession(source, source.Format, new() { MaxPageReads = 1 });
        await using var destination = new MemoryStream();
        destination.SetLength(3L * source.PageSize);
        byte[] column = Column(LongValueDescriptor.Chained(1, 0x300, 0));
        _ = await CompactRawLongValueBuilder.BuildAsync(session, source.Format, column, destination, new(), Ct);

        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactRawLongValueBuilder.BuildAsync(session, source.Format, column, destination, new(), Ct));

        Assert.Equal(1, source.Reads);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    private static async Task<List<byte[]>> ReadRowsAsync(ImagePages source, ReadOnlyMemory<byte> column)
    {
        var rows = new List<byte[]>();
        await CompactLongValueAudit.AuditAsync(
            source,
            source.Format,
            column,
            new(),
            (record, _) =>
        {
            if (record.Kind == "lval-row")
            {
                rows.Add(record.RawBytes.ToArray());
            }

            return ValueTask.CompletedTask;
        },
            Ct);
        return rows;
    }

    private static byte[] Column(LongValueDescriptor descriptor, byte[]? trailing = null)
    {
        byte[] column = new byte[12 + (trailing?.Length ?? 0)];
        descriptor.WriteTo(column);
        trailing?.CopyTo(column, 12);
        return column;
    }

    private static byte[] Chain(uint next, byte[] payload)
    {
        byte[] bytes = new byte[4 + payload.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, next);
        payload.CopyTo(bytes, 4);
        return bytes;
    }

    private sealed class FlushFailureStream(IOException failure) : MemoryStream
    {
        public override Task FlushAsync(CancellationToken cancellationToken) => Task.FromException(failure);
    }

    private sealed class CancellingStream(CancellationTokenSource cancellation) : MemoryStream
    {
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await base.WriteAsync(buffer, cancellationToken);
            await cancellation.CancelAsync();
        }
    }

    private sealed class ImagePages : IPageSource
    {
        internal ImagePages(DatabaseFormat kind, int count)
        {
            this.Format = JetFormat.ForNewDatabase(kind);
            this.Pages = new byte[count][];
            for (int i = 0; i < count; i++)
            {
                this.Pages[i] = new byte[this.PageSize];
            }
        }

        internal ImagePages(DatabaseFormat kind, byte[] image)
            : this(kind, image.Length / JetFormat.ForNewDatabase(kind).PageSize)
        {
            for (int i = 0; i < this.Pages.Length; i++)
            {
                image.AsSpan(i * this.PageSize, this.PageSize).CopyTo(this.Pages[i]);
            }
        }

        public int PageSize => this.Format.PageSize;

        public long PageCount => this.Pages.Length;

        public bool IsDisposed => false;

        internal JetFormat Format { get; }

        internal byte[][] Pages { get; }

        internal int Reads { get; private set; }

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            this.Reads++;
            byte[] rented = ArrayPool<byte>.Shared.Rent(this.PageSize);
            this.Pages[checked((int)pageNumber)].CopyTo(rented, 0);
            return ValueTask.FromResult(rented);
        }

        public void ThrowIfDisposed()
        {
        }

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => cancellationToken.ThrowIfCancellationRequested();

        internal void SetRows(int number, params byte[][] rows)
        {
            byte[] page = this.Pages[number];
            page[0] = 1;
            "LVAL"u8.CopyTo(page.AsSpan(4));
            BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(this.Format.DataPage.NumRows), checked((ushort)rows.Length));
            int start = this.PageSize;
            for (int i = 0; i < rows.Length; i++)
            {
                start -= rows[i].Length;
                rows[i].CopyTo(page, start);
                BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(this.Format.DataPage.RowsStart + (i * 2)), checked((ushort)start));
            }
        }
    }
}
