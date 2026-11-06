namespace JetDatabaseWriter.Tests.Queries;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Linq;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Queries;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Verifies <see cref="QueryPlanTemplateCache"/> reuse across changing captures, safe column projection pushdown,
/// and end-to-end inward-rounded index seek endpoint rewrites.
/// </summary>
public sealed class QueryPlanCacheAndProjectionTests
{
    private const string Table = "Items";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task QueryPlanTemplateCache_ReusesPlanAcrossChangingCaptures_AndPreservesShortCircuit()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                Table,
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Code", typeof(int)), new("Bio", typeof(string))],
                [new IndexDefinition("IX_Code", "Code")],
                Ct);
            await writer.InsertRowsAsync(
                Table,
                [
                    [1, 10, "alpha"],
                    [2, 20, "beta"],
                    [3, 30, "gamma"],
                    [4, 40, "delta"],
                ],
                Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);

        int minCode = 15;
        int skipCount = 0;
        int takeCount = 2;
        List<int> first = await reader.Query<ProjectionRow>(Table)
            .Where(r => r.Code >= minCode)
            .OrderByDescending(r => r.Id)
            .Skip(skipCount)
            .Take(takeCount)
            .Select(r => r.Id)
            .ToListAsync(Ct);
        Assert.Equal([4, 3], first);
        Assert.True(QueryPlanTemplateCache.CachedPlanCount >= 1);

        minCode = 25;
        skipCount = 1;
        takeCount = 1;
        List<int> second = await reader.Query<ProjectionRow>(Table)
            .Where(r => r.Code >= minCode)
            .OrderByDescending(r => r.Id)
            .Skip(skipCount)
            .Take(takeCount)
            .Select(r => r.Id)
            .ToListAsync(Ct);
        Assert.Equal([3], second);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SelectAndCount_PushProjectedColumns_AndSkipUnreferencedLongValues(bool useIndexSeek)
    {
        string largeMemo = new('M', 24_000);
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            IndexDefinition[] indexes = useIndexSeek ? [new IndexDefinition("IX_Code", "Code")] : [];
            await writer.CreateTableAsync(
                Table,
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Code", typeof(int)), new("Bio", typeof(string))],
                indexes,
                Ct);
            await writer.InsertRowsAsync(
                Table,
                Enumerable.Range(1, 20).Select(id => new object?[] { id, id * 10, largeMemo }),
                Ct);
        }

        stream.Position = 0;
        await using var counting = new CountingStream(stream);
        await using AccessReader reader = await AccessReader.OpenAsync(counting, new AccessReaderOptions { UseLockFile = false, PageCacheSize = 0 }, leaveOpen: true, cancellationToken: Ct);
        _ = await reader.ListIndexesAsync(Table, Ct);
        _ = await reader.GetColumnMetadataAsync(Table, Ct);
        _ = await reader.ListLinkedTablesAsync(Ct);

        counting.Reset();
        List<int> projectedIds = await reader.Query<ProjectionRow>(Table)
            .Where(r => r.Code >= 50)
            .Select(r => r.Id)
            .ToListAsync(Ct);
        long projectedBytes = counting.BytesRead;
        Assert.Equal(16, projectedIds.Count);

        counting.Reset();
        int count = await reader.Query<ProjectionRow>(Table)
            .Where(r => r.Code >= 50)
            .CountAsync(Ct);
        long countBytes = counting.BytesRead;
        Assert.Equal(16, count);

        counting.Reset();
        List<ProjectionRow> fullEntities = await reader.Query<ProjectionRow>(Table)
            .Where(r => r.Code >= 50)
            .ToListAsync(Ct);
        long fullBytes = counting.BytesRead;
        Assert.Equal(16, fullEntities.Count);
        Assert.All(fullEntities, row => Assert.Equal(largeMemo.Length, row.Bio.Length));

        // Projecting only Id/Code must avoid reading the external LVAL Memo pages that full entity materialization reads.
        Assert.True(projectedBytes * 3 < fullBytes, $"Expected projectedBytes ({projectedBytes}) * 3 < fullBytes ({fullBytes}).");
        Assert.True(countBytes * 3 < fullBytes, $"Expected countBytes ({countBytes}) * 3 < fullBytes ({fullBytes}).");
    }

    [Fact]
    public async Task FallibleUnreferencedScalarMapping_DisablesProjectionPushdown_AndPreservesCastException()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                Table,
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Payload", typeof(int))],
                Ct);
            await writer.InsertRowsAsync(Table, [[1, 500]], Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);

        await Assert.ThrowsAsync<InvalidCastException>(async () =>
            await reader.Query<NarrowProjectionRow>(Table).Select(r => r.Id).ToListAsync(Ct));
        await Assert.ThrowsAsync<InvalidCastException>(async () =>
            await reader.Query<NarrowProjectionRow>(Table).Where(r => r.Id >= 1).CountAsync(Ct));
    }

    [Fact]
    public async Task InwardRoundedStrictBounds_SeekIndexWithoutLosingBoundaryRows()
    {
        const int rowCount = 3000;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync(
                Table,
                [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Code", typeof(int)), new("Bio", typeof(string))],
                [new IndexDefinition("IX_Code", "Code")],
                Ct);
            await writer.InsertRowsAsync(
                Table,
                Enumerable.Range(1, rowCount).Select(id => new object?[] { id, id, "item" }),
                Ct);
        }

        stream.Position = 0;
        await using var counting = new CountingStream(stream);
        await using AccessReader reader = await AccessReader.OpenAsync(counting, new AccessReaderOptions { UseLockFile = false, PageCacheSize = 0 }, leaveOpen: true, cancellationToken: Ct);
        _ = await reader.ListIndexesAsync(Table, Ct);
        _ = await reader.GetColumnMetadataAsync(Table, Ct);
        _ = await reader.ListLinkedTablesAsync(Ct);

        // 1.5 rounds to even (2, inward for >) and 4.5 rounds to even (4, inward for <).
        // Rewriting > 1.5 to >= 2 and < 4.5 to <= 4 seeks IX_Code without scanning the 3000-row table and retains 2, 3, 4.
        counting.Reset();
        List<int> ids = await reader.Query<ProjectionRow>(Table)
            .Where(r => r.Code > 1.5 && r.Code < 4.5)
            .Select(r => r.Id)
            .ToListAsync(Ct);
        Assert.Equal([2, 3, 4], ids);
        Assert.InRange(counting.BytesRead, 1, 16L * 4096);
    }

#pragma warning disable CA1812 // Instantiated by typed row mappers and query expressions.
    private sealed class ProjectionRow
    {
        public int Id { get; set; }

        public int Code { get; set; }

        public string Bio { get; set; } = string.Empty;
    }

    private sealed class NarrowProjectionRow
    {
        public int Id { get; set; }

        public byte Payload { get; set; }
    }
#pragma warning restore CA1812
}
