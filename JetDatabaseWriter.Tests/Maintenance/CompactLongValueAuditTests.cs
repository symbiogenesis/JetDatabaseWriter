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

public sealed class CompactLongValueAuditTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("Employees", "Notes")]
    [InlineData("Employees", "Photo")]
    [InlineData("Categories", "Picture")]
    public async Task Audit_AccessAuthoredJet3PackedAndChainedValuesRetainExactStorage(string tableName, string columnName)
    {
        await using AccessReader reader = await TestDatabases.OpenAsync(TestDatabases.MdbtoolsNwind, cancellationToken: Ct);
        DatabaseFile database = reader.Database;
        CatalogEntry? entry = await reader.GetCatalogEntryAsync(tableName, Ct);
        Assert.NotNull(entry);
        TableDef definition = Assert.IsType<TableDef>(await database.TableDefs.ReadTableDefAsync(entry.TDefPage, Ct));
        ColumnInfo column = Assert.Single(definition.Columns, item => item.Name == columnName);
        bool hasVariable = definition.Columns.Any(item => !item.IsFixed);
        int audited = 0;
        int externalRows = 0;
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
                    int declaredBytes = 0;
                    await CompactLongValueAudit.AuditAsync(
                        database.Pages,
                        database.Format,
                        original,
                        new(),
                        async (record, recordToken) =>
                        {
                            declaredBytes += record.DeclaredPayloadBytes;
                            if (record.Kind == "lval-column")
                            {
                                Assert.Equal<byte>(original.Span, record.RawBytes.Span);
                            }
                            else if (record.Kind == "lval-row")
                            {
                                byte[] physical = await reader.ReadPageCopyAsync(record.PageNumber, recordToken);
                                Assert.Equal<byte>(physical.AsSpan(record.RowStart, record.RawBytes.Length), record.RawBytes.Span);
                                externalRows++;
                            }
                        },
                        token);
                    Assert.Equal(descriptor.Length, declaredBytes);
                    audited++;
                }

                return true;
            },
            Ct);
        Assert.True(audited > 0);
        Assert.True(externalRows > 0);
    }

    [Fact]
    public async Task Audit_PreservesWholeInlineSliceIncludingDescriptorAndTrailingBytes()
    {
        var source = new SyntheticPages(DatabaseFormat.Jet4Mdb);
        byte[] column = Column(LongValueDescriptor.Inline(2), [0xFE, 0xFF, 0x80, 0xAA]);
        List<CompactLongValueRecord> records = await AuditAsync(source, column);

        CompactLongValueRecord stored = Assert.Single(records, item => item.Kind == "lval-column");
        Assert.Equal(column, stored.RawBytes.ToArray());
        Assert.Equal(12, stored.PayloadOffset);
        Assert.Equal(2, stored.DeclaredPayloadBytes);
        Assert.Equal(2, stored.TrailingBytes);
        Assert.Equal(0, source.Reads);
        Assert.All(records, item => Assert.True(item.IsPinned));
        Assert.False(records[^1].OwnershipProven);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Audit_ObservesCompleteRowsAndReferencesBeyondDeclaredPayload(DatabaseFormat format)
    {
        var source = new SyntheticPages(format);
        source.SetRow(3, Chained(1024, [0x11, 0x22, 0x33]));
        source.SetRow(4, Chained(0, [0x80, 0xFF]));
        byte[] column = Column(LongValueDescriptor.Chained(2, 768, 0xFEEDABCD), [0x55]);
        List<CompactLongValueRecord> records = await AuditAsync(source, column);

        CompactLongValueRecord[] rows = records.FindAll(item => item.Kind == "lval-row").ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(Chained(1024, [0x11, 0x22, 0x33]), rows[0].RawBytes.ToArray());
        Assert.Equal(2, rows[0].DeclaredPayloadBytes);
        Assert.Equal(1, rows[0].TrailingBytes);
        Assert.Equal(0, rows[1].DeclaredPayloadBytes);
        Assert.Equal(2, rows[1].TrailingBytes);
        Assert.Equal(1024u, rows[0].NextPointer);
        Assert.Equal(2, records.FindAll(item => item.Kind == "lval-reference").Count);
        Assert.Equal(2, source.Reads);
        Assert.Equal("lval-terminal", records[^1].Kind);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Audit_ZeroDeclaredLengthStillPinsExternalStorage(bool singlePage)
    {
        var source = new SyntheticPages(DatabaseFormat.Jet4Mdb);
        byte[] bytes = singlePage ? [0xFE, 0xFF] : Chained(0, [0xFE, 0xFF]);
        source.SetRow(3, bytes);
        byte[] column = Column(singlePage ? LongValueDescriptor.SinglePage(0, 768, 7) : LongValueDescriptor.Chained(0, 768, 7));

        CompactLongValueRecord row = Assert.Single(await AuditAsync(source, column), item => item.Kind == "lval-row");

        Assert.Equal(bytes, row.RawBytes.ToArray());
        Assert.Equal(0, row.DeclaredPayloadBytes);
        Assert.Equal(2, row.TrailingBytes);
    }

    [Fact]
    public async Task Audit_ObservesDanglingTrailingReferenceThenRefusesWithoutTerminal()
    {
        var source = new SyntheticPages(DatabaseFormat.Jet4Mdb);
        source.SetRow(3, Chained(0xFFFF00, [1]));
        var records = new List<CompactLongValueRecord>();

        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await AuditAsync(source, Column(LongValueDescriptor.Chained(1, 768, 0)), records: records));

        Assert.Equal(0xFFFF, error.ErrorInfo.PageNumber);
        Assert.Equal(0xFFFF00u, records[^1].Pointer);
        Assert.Equal("lval-reference", records[^1].Kind);
        Assert.DoesNotContain(records, item => item.Kind == "lval-terminal");
        Assert.Equal(1, source.Reads);
    }

    [Fact]
    public async Task Audit_FollowsTrailingCycleAndRefusesInsteadOfTruncatingIt()
    {
        var source = new SyntheticPages(DatabaseFormat.Jet4Mdb);
        source.SetRow(3, Chained(768, [1]));
        var records = new List<CompactLongValueRecord>();

        await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await AuditAsync(source, Column(LongValueDescriptor.Chained(1, 768, 0)), records: records));

        Assert.Equal(1, source.Reads);
        Assert.Equal(2, records.FindAll(item => item.Kind == "lval-reference").Count);
    }

    [Theory]
    [InlineData(0, "MaxPages")]
    [InlineData(1, "MaxWorkingBytes")]
    [InlineData(2, "MaxRawBytes")]
    public async Task Audit_AdmissionRefusesBeforeIo(int kind, string reason)
    {
        var source = new SyntheticPages(DatabaseFormat.Jet4Mdb);
        CompactLongValueBudget budget = kind switch
        {
            0 => new() { MaxPages = 1 },
            1 => new() { MaxWorkingBytes = 1 },
            _ => new() { MaxRawBytes = 1 },
        };

        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await AuditAsync(source, Column(LongValueDescriptor.Chained(1, 768, 0)), budget));

        Assert.Contains(reason, error.ErrorInfo.Reason, StringComparison.Ordinal);
        Assert.Equal(0, source.Reads);
    }

    [Theory]
    [InlineData(0, "MaxChainRows")]
    [InlineData(1, "MaxPageReads")]
    [InlineData(2, "MaxDirectorySlots")]
    [InlineData(3, "MaxRawBytes")]
    public async Task Audit_TraversalBudgetIncludesTrailingStorage(int kind, string reason)
    {
        var source = new SyntheticPages(DatabaseFormat.Jet4Mdb);
        source.SetRow(3, Chained(1024, [1]));
        source.SetRow(4, Chained(0, [2]));
        CompactLongValueBudget budget = kind switch
        {
            0 => new() { MaxChainRows = 1 },
            1 => new() { MaxPageReads = 1 },
            2 => new() { MaxDirectorySlots = 1 },
            _ => new() { MaxRawBytes = 17 },
        };

        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await AuditAsync(source, Column(LongValueDescriptor.Chained(1, 768, 0)), budget));

        Assert.Contains(reason, error.ErrorInfo.Reason, StringComparison.Ordinal);
        Assert.Equal(kind is 2 or 3 ? 2 : 1, source.Reads);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    public async Task Audit_RefusesDamagedRowsWithContext(int damage)
    {
        var source = new SyntheticPages(DatabaseFormat.Jet4Mdb);
        source.SetRow(3, Chained(0, [1]));
        byte[] page = source.Pages[3];
        int directory = source.Format.DataPage.RowsStart;
        switch (damage)
        {
            case 0:
                page[0] = 0xFF;
                break;
            case 1:
                page[4] = 0;
                break;
            case 2:
                BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(source.Format.DataPage.NumRows), ushort.MaxValue);
                break;
            case 3:
                BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(directory), 1);
                break;
            case 4:
                page[directory + 1] |= 0x80;
                break;
            case 5:
                page[directory + 1] |= 0x40;
                break;
            default:
                BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(source.Format.DataPage.NumRows), 2);
                page.AsSpan(directory, 2).CopyTo(page.AsSpan(directory + 2));
                break;
        }

        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await AuditAsync(source, Column(LongValueDescriptor.Chained(1, 768, 0))));
        Assert.Equal(3, error.ErrorInfo.PageNumber);
        Assert.Equal(JetErrorCode.MalformedValue, error.ErrorCode);
    }

    [Fact]
    public async Task Audit_PreservesSourceIoFailureIdentity()
    {
        var expected = new IOException("source read failure");
        var source = new SyntheticPages(DatabaseFormat.Jet4Mdb) { Failure = expected };

        IOException actual = await Assert.ThrowsAsync<IOException>(async () =>
            await AuditAsync(source, Column(LongValueDescriptor.Chained(1, 768, 0))));
        Assert.Same(expected, actual);
    }

    [Fact]
    public async Task Audit_ObservesShortChainRowBeforeRefusingMissingPointerPrefix()
    {
        var source = new SyntheticPages(DatabaseFormat.Jet4Mdb);
        source.SetRow(3, [0xFE, 0xFF, 0x80]);
        var records = new List<CompactLongValueRecord>();

        await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await AuditAsync(source, Column(LongValueDescriptor.Chained(1, 768, 0)), records: records));

        CompactLongValueRecord row = Assert.Single(records, item => item.Kind == "lval-row");
        Assert.Equal(new byte[] { 0xFE, 0xFF, 0x80 }, row.RawBytes.ToArray());
        Assert.Null(row.NextPointer);
        Assert.Null(row.Coverage);
        Assert.DoesNotContain(records, item => item.Kind == "lval-terminal");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Audit_ObservesOriginalMalformedColumnBeforeRefusing(bool truncated)
    {
        var source = new SyntheticPages(DatabaseFormat.Jet4Mdb);
        byte[] column = truncated ? [0xFF, 0xFE] : Column(new LongValueDescriptor(1, Constants.LongValue.StorageModeMask, 768, 7), [0x80]);
        var records = new List<CompactLongValueRecord>();

        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await AuditAsync(source, column, records: records));

        CompactLongValueRecord observed = Assert.Single(records);
        Assert.Equal("lval-column", observed.Kind);
        Assert.Equal(column, observed.RawBytes.ToArray());
        Assert.Equal(0, source.Reads);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Audit_RefusesTruncatedPageOrChangingSource(bool changing)
    {
        var source = new SyntheticPages(DatabaseFormat.Jet4Mdb) { ShortRead = !changing, ChangingGeometry = changing };
        source.SetRow(3, Chained(0, [1]));

        await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await AuditAsync(source, Column(LongValueDescriptor.Chained(1, 768, 0))));

        Assert.Equal(1, source.Reads);
    }

    [Fact]
    public async Task Audit_VisitorIoFailurePropagatesBeforeNextReference()
    {
        var source = new SyntheticPages(DatabaseFormat.Jet4Mdb);
        source.SetRow(3, Chained(1024, [1]));
        var expected = new IOException("visitor failed to retain raw row");

        IOException actual = await Assert.ThrowsAsync<IOException>(async () =>
            await CompactLongValueAudit.AuditAsync(
                source,
                source.Format,
                Column(LongValueDescriptor.Chained(1, 768, 0)),
                new(),
                (record, _) => record.Kind == "lval-row" ? ValueTask.FromException(expected) : ValueTask.CompletedTask,
                Ct));

        Assert.Same(expected, actual);
        Assert.Equal(1, source.Reads);
    }

    [Fact]
    public async Task Audit_AwaitsBorrowedRowVisitorBeforeAdvancing()
    {
        var source = new SyntheticPages(DatabaseFormat.Jet4Mdb);
        source.SetRow(3, Chained(1024, [0xFE]));
        source.SetRow(4, Chained(0, [0xFF]));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var resume = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task audit = CompactLongValueAudit.AuditAsync(
            source,
            source.Format,
            Column(LongValueDescriptor.Chained(1, 768, 0)),
            new(),
            async (record, token) =>
            {
                if (record.Kind == "lval-row" && record.PageNumber == 3)
                {
                    entered.SetResult();
                    await resume.Task.WaitAsync(token);
                    Assert.Equal(Chained(1024, [0xFE]), record.RawBytes.ToArray());
                }
            },
            Ct).AsTask();

        await entered.Task.WaitAsync(TimeSpan.FromSeconds(30), Ct);
        Assert.Equal(1, source.Reads);
        resume.SetResult();
        await audit;
        Assert.Equal(2, source.Reads);
    }

    [Fact]
    public async Task Audit_CancellationAfterBorrowedRowStopsBeforeNextRead()
    {
        var source = new SyntheticPages(DatabaseFormat.Jet4Mdb);
        source.SetRow(3, Chained(1024, [1]));
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await CompactLongValueAudit.AuditAsync(
                source,
                source.Format,
                Column(LongValueDescriptor.Chained(1, 768, 0)),
                new(),
                async (record, _) =>
                {
                    if (record.Kind == "lval-row")
                    {
                        await cancellation.CancelAsync();
                    }
                },
                cancellation.Token));
        Assert.Equal(1, source.Reads);
    }

    private static async Task<List<CompactLongValueRecord>> AuditAsync(
        SyntheticPages source,
        byte[] column,
        CompactLongValueBudget? budget = null,
        List<CompactLongValueRecord>? records = null)
    {
        records ??= [];
        await CompactLongValueAudit.AuditAsync(
            source,
            source.Format,
            column,
            budget ?? new(),
            (record, _) =>
            {
                records.Add(record with { RawBytes = record.RawBytes.ToArray() });
                return ValueTask.CompletedTask;
            },
            Ct);
        return records;
    }

    private static byte[] Column(LongValueDescriptor descriptor, byte[]? following = null)
    {
        byte[] bytes = new byte[12 + (following?.Length ?? 0)];
        descriptor.WriteTo(bytes);
        following?.CopyTo(bytes, 12);
        return bytes;
    }

    private static byte[] Chained(uint next, byte[] payload)
    {
        byte[] bytes = new byte[4 + payload.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, next);
        payload.CopyTo(bytes, 4);
        return bytes;
    }

    private sealed class SyntheticPages(DatabaseFormat format) : IPageSource
    {
        internal JetFormat Format { get; } = JetFormat.ForNewDatabase(format);

        internal byte[][] Pages { get; } = new byte[6][];

        internal int Reads { get; private set; }

        internal Exception? Failure { get; init; }

        internal bool ShortRead { get; init; }

        internal bool ChangingGeometry { get; init; }

        public int PageSize => this.Format.PageSize;

        public long PageCount => this.Pages.Length + (this.ChangingGeometry && this.Reads > 0 ? 1 : 0);

        public bool IsDisposed => false;

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            this.Reads++;
            if (this.Failure is { } error)
            {
                return ValueTask.FromException<byte[]>(error);
            }

            byte[] page = ArrayPool<byte>.Shared.Rent(this.ShortRead ? 128 : this.PageSize);
            if (!this.ShortRead)
            {
                this.Pages[checked((int)pageNumber)].CopyTo(page, 0);
            }

            return ValueTask.FromResult(page);
        }

        public void ThrowIfDisposed()
        {
        }

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => cancellationToken.ThrowIfCancellationRequested();

        internal void SetRow(int number, byte[] row)
        {
            byte[] page = new byte[this.PageSize];
            page[0] = 1;
            "LVAL"u8.CopyTo(page.AsSpan(4));
            BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(this.Format.DataPage.NumRows), 1);
            BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(this.Format.DataPage.RowsStart), checked((ushort)(this.PageSize - row.Length)));
            row.CopyTo(page, this.PageSize - row.Length);
            this.Pages[number] = page;
        }
    }
}
