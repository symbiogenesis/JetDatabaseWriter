namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;
using static JetDatabaseWriter.Tests.ComplexColumns.ComplexColumnTestSupport;

/// <summary>Demand-driven cells retain flat-table-wide identity admission without decoding unrelated payloads.</summary>
public sealed class ComplexItemPreflightTests
{
    /// <summary>Duplicate item identities in different parent groups refuse a complete strict parent read.</summary>
    /// <param name="kind">The complex item representation.</param>
    [Theory]
    [InlineData(ComplexColumnKind.Attachment)]
    [InlineData(ComplexColumnKind.MultiValue)]
    [InlineData(ComplexColumnKind.VersionHistory)]
    public async Task CrossParentDuplicateIds_StrictReadsRefuse(ComplexColumnKind kind)
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.ComplexDataTestV2007);
        _ = await DuplicateAcrossParentsAsync(stream, kind);
        await using AccessReader reader = await OpenReaderAsync(stream);

        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await reader.Rows("Table1", cancellationToken: Ct).ToListAsync(Ct));

        Assert.Equal(JetErrorCode.CorruptComplexColumn, error.ErrorCode);
        Assert.NotNull(error.ErrorInfo.PageNumber);
        Assert.NotNull(error.ErrorInfo.ColumnName);
    }

    /// <summary>Lenient reads omit both ambiguous identities before either parent's cell is returned.</summary>
    /// <param name="kind">The complex item representation.</param>
    [Theory]
    [InlineData(ComplexColumnKind.Attachment)]
    [InlineData(ComplexColumnKind.MultiValue)]
    [InlineData(ComplexColumnKind.VersionHistory)]
    public async Task CrossParentDuplicateIds_LenientReadsOmitBoth(ComplexColumnKind kind)
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.ComplexDataTestV2007);
        (ComplexColumnInfo column, int duplicate) = await DuplicateAcrossParentsAsync(stream, kind);
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = false }, leaveOpen: true, Ct);
        int ordinal = (await reader.GetColumnMetadataAsync("Table1", Ct)).Single(metadata => metadata.Name == column.ColumnName).Ordinal;
        List<object[]> parents = await reader.Rows("Table1", cancellationToken: Ct).ToListAsync(Ct);
        IEnumerable<int> ids = parents.Where(parent => parent[ordinal] is byte[]).SelectMany(parent => kind == ComplexColumnKind.Attachment
            ? ComplexCellValue.ReadAttachments((byte[])parent[ordinal]).Select(item => item.Id)
            : ComplexCellValue.ReadMultiValueItems((byte[])parent[ordinal]).Select(item => item.Id));

        Assert.DoesNotContain(duplicate, ids);
    }

    /// <summary>Identity inventory limits apply before payload decoding under either parsing policy.</summary>
    /// <param name="strict">Whether value decoding is strict.</param>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task IdentityBudget_RefusesAggregateSelectedFlats(bool strict)
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.ComplexDataTestV2007);
        int records = await CountFlatRowsAsync(stream);
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions
            {
                UseLockFile = false,
                StrictParsing = strict,
                MaxComplexItemIdentityEntriesPerOperation = records - 1,
                MaxAttachmentContentBytesPerOperation = 1,
            },
            leaveOpen: true,
            Ct);

        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await reader.Rows("Table1", cancellationToken: Ct).FirstAsync(Ct));

        Assert.Equal(JetErrorCode.ComplexItemIdentityBudgetExceeded, error.ErrorCode);
    }

    /// <summary>Independent operations receive fresh budgets rather than consuming a reader-lifetime allowance.</summary>
    [Fact]
    public async Task IdentityBudget_ResetsAcrossIndependentReads()
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.ComplexDataTestV2007);
        int records = await CountFlatRowsAsync(stream);
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, MaxComplexItemIdentityEntriesPerOperation = records }, leaveOpen: true, Ct);

        List<object[]> first = await reader.Rows("Table1", cancellationToken: Ct).ToListAsync(Ct);
        List<object[]> second = await reader.Rows("Table1", cancellationToken: Ct).ToListAsync(Ct);

        Assert.NotEmpty(first);
        Assert.Equal(first.Count, second.Count);
    }

    /// <summary>Whole-column APIs retain global preflight and apply the same named allowance.</summary>
    /// <param name="kind">The complex item representation.</param>
    /// <param name="strict">Whether value decoding is strict.</param>
    [Theory]
    [InlineData(ComplexColumnKind.Attachment, true)]
    [InlineData(ComplexColumnKind.Attachment, false)]
    [InlineData(ComplexColumnKind.MultiValue, true)]
    [InlineData(ComplexColumnKind.MultiValue, false)]
    [InlineData(ComplexColumnKind.VersionHistory, true)]
    [InlineData(ComplexColumnKind.VersionHistory, false)]
    public async Task IdentityBudget_RefusesDirectItemReads(ComplexColumnKind kind, bool strict)
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.ComplexDataTestV2007);
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions
            {
                UseLockFile = false,
                StrictParsing = strict,
                MaxComplexItemIdentityEntriesPerOperation = 1,
                MaxAttachmentContentBytesPerOperation = 1,
            },
            leaveOpen: true,
            Ct);
        ComplexColumnInfo column = (await reader.GetComplexColumnsAsync("Table1", Ct)).First(candidate => candidate.Kind == kind);

        JetLimitationException failure = kind == ComplexColumnKind.Attachment
            ? await Assert.ThrowsAsync<JetLimitationException>(async () => await reader.GetAttachmentsAsync("Table1", column.ColumnName, Ct))
            : await Assert.ThrowsAsync<JetLimitationException>(async () => await reader.GetMultiValueItemsAsync("Table1", column.ColumnName, Ct));

        Assert.Equal(JetErrorCode.ComplexItemIdentityBudgetExceeded, failure.ErrorCode);
    }

    /// <summary>The configured inventory limit must be positive.</summary>
    /// <param name="limit">The invalid limit.</param>
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task IdentityBudget_RejectsNonPositiveConfiguration(int limit)
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.ComplexDataTestV2007);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, MaxComplexItemIdentityEntriesPerOperation = limit }, leaveOpen: true, Ct);
            _ = reader.DatabaseFormat;
        });
    }

    /// <summary>Nested and linked operations share an allowance while unrelated roots remain independent.</summary>
    [Fact]
    public void IdentityBudget_SharesNestedAndLinkedContexts()
    {
        var options = new AccessReaderOptions { MaxComplexItemIdentityEntriesPerOperation = 1 };
        var root = ReaderOpenContext.CreateRoot(options);
        ReaderOpenContext linked = root.ForLinkedSource("source.accdb", new LinkedSourceTraversal(1, []));
        var independent = ReaderOpenContext.CreateRoot(options);
        Assert.Same(root.ComplexItemReads, linked.ComplexItemReads);
        Assert.NotSame(root.ComplexItemReads, independent.ComplexItemReads);
        var rootGate = new AsyncReentrantOperationGate(typeof(AccessReader), complexItemReads: root.ComplexItemReads);
        var linkedGate = new AsyncReentrantOperationGate(typeof(AccessReader), complexItemReads: linked.ComplexItemReads);
        var independentGate = new AsyncReentrantOperationGate(typeof(AccessReader), complexItemReads: independent.ComplexItemReads);
        using (rootGate.Enter())
        {
            root.ComplexItemReads.Budget.Observe();
            using (linkedGate.Enter())
            {
                JetLimitationException failure = Assert.Throws<JetLimitationException>(linked.ComplexItemReads.Budget.Observe);
                Assert.Equal(JetErrorCode.ComplexItemIdentityBudgetExceeded, failure.ErrorCode);
            }

            using (independentGate.Enter())
            {
                independent.ComplexItemReads.Budget.Observe();
            }
        }

        using (rootGate.Enter())
        {
            root.ComplexItemReads.Budget.Observe();
        }
    }

    /// <summary>Concurrent observations cannot over-admit the named record allowance.</summary>
    [Fact]
    public async Task IdentityBudget_AdmitsExactlyItsConcurrentLimit()
    {
        var budget = new ComplexItemIdentityBudget(3);
        int accepted = 0;
        var attempts = new Task[32];
        for (int index = 0; index < attempts.Length; index++)
        {
            attempts[index] = Task.Run(
                () =>
                {
                    try
                    {
                        budget.Observe();
                        _ = Interlocked.Increment(ref accepted);
                    }
                    catch (JetLimitationException failure)
                    {
                        Assert.Equal(JetErrorCode.ComplexItemIdentityBudgetExceeded, failure.ErrorCode);
                    }
                },
                Ct);
        }

        await Task.WhenAll(attempts);
        Assert.Equal(3, accepted);
    }

    private static async ValueTask<int> CountFlatRowsAsync(MemoryStream stream)
    {
        IReadOnlyList<ComplexColumnInfo> columns;
        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            columns = await reader.GetComplexColumnsAsync("Table1", Ct);
        }

        stream.Position = 0;
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        int count = 0;
        foreach (int id in columns.Select(column => column.FlatTableId).Distinct())
        {
            count += (await harness.Services.Snapshots.ReadRowsAsync(CatalogValueReader.TdefPageFromId(id), Ct)).Count;
        }

        return count;
    }

    private static async ValueTask<(ComplexColumnInfo Column, int Duplicate)> DuplicateAcrossParentsAsync(MemoryStream stream, ComplexColumnKind kind)
    {
        ComplexColumnInfo descriptor;
        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            descriptor = (await reader.GetComplexColumnsAsync("Table1", Ct)).First(column => column.Kind == kind);
        }

        stream.Position = 0;
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        long pageNumber = CatalogValueReader.TdefPageFromId(descriptor.FlatTableId);
        TableDef flat = await harness.Database.TableDefs.ReadRequiredTableDefAsync(pageNumber, descriptor.FlatTableName, Ct);
        List<LocatedRow> items = await harness.Services.Snapshots.ReadRowsAsync(pageNumber, Ct);
        int identity = flat.FindColumnIndex(column => column.IsAutoNumber);
        int reference = flat.FindColumnIndex(column => column.Type == ColumnType.LongIntegerType && !column.IsAutoNumber && column.Name.StartsWith('_'));
        Assert.True(identity >= 0 && reference >= 0);
        LocatedRow first = items[0];
        LocatedRow other = items.First(item => !Equals(item.Values[reference], first.Values[reference]));
        int duplicate = Assert.IsType<int>(other.Values[identity]);
        byte[] bytes = await harness.Database.Pages.ReadPageCopyAsync(first.Location.DataPageNumber, Ct);
        Assert.True(RowDecodePlan.TryParseRowLayout(harness.Database.Format.RowFields, bytes, first.Location.RowStart, first.Location.RowSize, flat.HasVarColumns, out RowLayout layout));
        ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(harness.Database.Format.RowFields, bytes, first.Location.RowStart, first.Location.RowSize, layout, flat.Columns[identity]);
        Assert.Equal(4, slice.DataLen);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(first.Location.RowStart + slice.DataStart, 4), duplicate);
        await harness.Pager.WritePageAsync(first.Location.DataPageNumber, bytes, Ct);
        return (descriptor, duplicate);
    }
}
